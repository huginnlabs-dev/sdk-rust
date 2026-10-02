//! actix-web 4 server middleware (feature `actix`).
//!
//! An [`actix_web::middleware::Transform`] opening the same `HTTP_SERVER`
//! span as the axum integration per request — name `"METHOD path"`,
//! `status_code` from the response, incoming `X-Dataflow-Trace-Id` header
//! adopted, `error_message` = `HTTP 5xx` for server-error responses and
//! the handler error's `Display` for failed handler futures:
//!
//! ```no_run
//! use actix_web::{App, web};
//!
//! let app = App::new()
//!     .wrap(dataflow_rs::DataflowMiddleware)
//!     .route("/orders", web::get().to(|| async { "orders" }));
//! # let _ = app;
//! ```
//!
//! actix workers poll each request task on a single thread, so the
//! thread-local current-span context works as designed: handler code sees
//! the server span as current, nested [`trace`](crate::trace) scopes become
//! children and log lines carry the request's ids.

use std::future::{ready, Future, Ready};
use std::pin::Pin;

use ::actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use ::actix_web::Error;

/// The propagation header the middleware adopts (`X-Dataflow-Trace-Id`).
const TRACE_HEADER: &str = "x-dataflow-trace-id";

/// The actix outcome: `Ok(response)` records the response status; `Err(_)`
/// marks the span failed (500 + the error's message) and keeps flowing.
impl<B> crate::integrations::SpanOutcome for Result<ServiceResponse<B>, Error> {
    fn df_status(&self) -> i32 {
        match self {
            Ok(res) => res.status().as_u16() as i32,
            Err(_) => 500,
        }
    }
    fn df_is_error(&self) -> bool {
        match self {
            Ok(res) => res.status().is_server_error(),
            Err(_) => true,
        }
    }
    fn df_error_message(&self) -> String {
        match self {
            Ok(res) => format!("HTTP {}", res.status().as_u16()),
            Err(e) => e.to_string(),
        }
    }
}

/// Server-span middleware for actix-web 4 — use with
/// [`App::wrap`](::actix_web::App::wrap):
///
/// ```no_run
/// use actix_web::{App, web};
///
/// let app = App::new()
///     .wrap(dataflow_rs::DataflowMiddleware)
///     .route("/orders", web::get().to(|| async { "orders" }));
/// # let _ = app;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct DataflowMiddleware;

impl<S, B> Transform<S, ServiceRequest> for DataflowMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = DataflowMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(DataflowMiddlewareService { service }))
    }
}

/// The wrapped service [`DataflowMiddleware`] produces.
#[derive(Debug, Clone)]
pub struct DataflowMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for DataflowMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<ServiceResponse<B>, Error>>>>;

    fn poll_ready(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let name = format!("{} {}", req.method().as_str().to_ascii_uppercase(), req.path());
        let incoming = req
            .headers()
            .get(TRACE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let fut = self.service.call(req);
        Box::pin(async move {
            crate::integrations::server_span_around(&name, &incoming, fut).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::actix_web::{test, web, App, HttpResponse};
    use crate::transport::tests::{ensure_configured, EMIT_LOCK};

    fn json_str_field(body: &str, field: &str) -> String {
        let needle = format!("\"{}\":\"", field);
        let pos = body
            .find(&needle)
            .unwrap_or_else(|| panic!("missing {} in {}", field, body));
        let rest = &body[pos + needle.len()..];
        let mut out = String::new();
        for c in rest.chars() {
            match c {
                '"' => break,
                '\\' => continue,
                _ => out.push(c),
            }
        }
        out
    }

    #[actix_web::test]
    async fn middleware_opens_inherited_server_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let app = test::init_service(
            App::new()
                .wrap(DataflowMiddleware)
                .route("/hello", web::get().to(|| async { "ok" })),
        )
        .await;
        let req = test::TestRequest::get()
            .uri("/hello")
            .insert_header(("X-Dataflow-Trace-Id", "trace-actix"))
            .to_request();
        let res = test::call_service(&app, req).await;
        assert!(res.status().is_success(), "{}", res.status());
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "one HTTP_SERVER event expected");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"HTTP_SERVER\""), "{}", ev);
        assert!(ev.contains("\"name\":\"GET /hello\""), "{}", ev);
        assert!(ev.contains("\"trace_id\":\"trace-actix\""), "{}", ev);
        assert!(ev.contains("\"status_code\":200"), "{}", ev);
        assert!(
            ev.contains(&format!("\"agent.sdk\":\"rust-sdk/{}\"", crate::SDK_VERSION)),
            "{}",
            ev
        );
        assert!(ev.contains("\"error_message\":\"\""), "{}", ev);
    }

    #[actix_web::test]
    async fn server_error_marks_the_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let app = test::init_service(
            App::new()
                .wrap(DataflowMiddleware)
                .route(
                    "/boom",
                    web::get().to(|| async {
                        HttpResponse::InternalServerError().body("boom")
                    }),
                ),
        )
        .await;
        let req = test::TestRequest::get().uri("/boom").to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), 500);
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"name\":\"GET /boom\""), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(ev.contains("\"error_message\":\"HTTP 500\""), "{}", ev);
        // No incoming header -> a fresh trace id (never empty).
        assert!(!json_str_field(ev, "trace_id").is_empty(), "{}", ev);
    }

    #[actix_web::test]
    async fn handler_traces_nest_under_server_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let app = test::init_service(
            App::new().wrap(DataflowMiddleware).route(
                "/nested",
                web::get().to(|| async {
                    crate::trace("orders.Load", |s| {
                        s.data("id", 7i64);
                    });
                    "ok"
                }),
            ),
        )
        .await;
        let req = test::TestRequest::get()
            .uri("/nested")
            .insert_header(("X-Dataflow-Trace-Id", "trace-nest"))
            .to_request();
        let res = test::call_service(&app, req).await;
        assert!(res.status().is_success());
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 2, "child + server spans expected");
        let child = &events[events.len() - 2];
        let server = &events[events.len() - 1];
        assert!(child.contains("\"type\":\"FUNCTION_CALL\""), "{}", child);
        assert!(child.contains("\"name\":\"orders.Load\""), "{}", child);
        assert_eq!(json_str_field(child, "trace_id"), "trace-nest", "{}", child);
        assert_eq!(
            json_str_field(child, "parent_span_id"),
            json_str_field(server, "span_id"),
            "child parent must be the server span"
        );
    }
}
