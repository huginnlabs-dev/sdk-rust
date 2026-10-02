//! axum/tower server middleware (feature `axum`; axum 0.7, tower 0.4).
//!
//! Two shapes, one behavior — an `HTTP_SERVER` span named `"METHOD path"`
//! per request, `status_code` from the response status, the incoming
//! `X-Dataflow-Trace-Id` header adopted as the trace id, `error_message`
//! `HTTP 5xx` on server errors:
//!
//! ```no_run
//! use axum::{routing::get, Router};
//!
//! let app: Router = Router::new()
//!     .route("/orders", get(|| async { "orders" }))
//!     .layer(dataflow_rs::TraceLayer); // tower Layer form …
//! # let _ = app;
//! ```
//!
//! ```no_run
//! use axum::{middleware, routing::get, Router};
//!
//! let app: Router = Router::new()
//!     .route("/orders", get(|| async { "orders" }))
//!     .layer(middleware::from_fn(dataflow_rs::axum_middleware)); // … or from_fn form
//! ```
//!
//! While the handler runs the server span is the thread's current span, so
//! nested [`trace`](crate::trace) scopes become children and log lines
//! carry the request's ids. Caveat inherited from the thread-local context
//! model: on multi-threaded tokio runtimes a task that hops threads mid-
//! request detaches its nested spans (the server span itself is unaffected
//! — it lives in the middleware future).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ::axum::extract::Request;
use ::axum::response::Response;
use ::tower::Service;

/// The propagation header the middleware adopts (`X-Dataflow-Trace-Id`).
const TRACE_HEADER: &str = "x-dataflow-trace-id";

impl crate::integrations::SpanOutcome for Response {
    fn df_status(&self) -> i32 {
        self.status().as_u16() as i32
    }
    fn df_is_error(&self) -> bool {
        self.status().is_server_error()
    }
    fn df_error_message(&self) -> String {
        format!("HTTP {}", self.status().as_u16())
    }
}

/// The error side of any tower service is displayable enough to record;
/// error responses never replace the handler's own (we only observe).
impl<E: std::fmt::Display> crate::integrations::SpanOutcome for Result<Response, E> {
    fn df_status(&self) -> i32 {
        match self {
            Ok(r) => r.df_status(),
            Err(_) => 500,
        }
    }
    fn df_is_error(&self) -> bool {
        match self {
            Ok(r) => r.df_is_error(),
            Err(_) => true,
        }
    }
    fn df_error_message(&self) -> String {
        match self {
            Ok(r) => r.df_error_message(),
            Err(e) => e.to_string(),
        }
    }
}

/// The `axum::middleware::from_fn` form of the server-span middleware:
///
/// ```no_run
/// use axum::{middleware, Router};
///
/// let app: Router = Router::new()
///     .layer(middleware::from_fn(dataflow_rs::axum_middleware));
/// # let _ = app;
/// ```
pub async fn axum_middleware(req: Request, next: ::axum::middleware::Next) -> Response {
    let name = request_name(req.method().as_str(), req.uri().path());
    let incoming = incoming_trace_id(req.headers()).to_string();
    crate::integrations::server_span_around(&name, &incoming, next.run(req)).await
}

/// The tower [`Layer`](::tower::Layer) form of [`axum_middleware`] —
/// equivalent to `middleware::from_fn(axum_middleware)`; use wherever a
/// layer is expected:
///
/// ```no_run
/// use axum::{routing::get, Router};
///
/// let app: Router = Router::new()
///     .route("/orders", get(|| async { "orders" }))
///     .layer(dataflow_rs::TraceLayer);
/// # let _ = app;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TraceLayer;

impl<S> ::tower::Layer<S> for TraceLayer
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
{
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService { inner }
    }
}

/// The wrapped service [`TraceLayer`] produces (error type is preserved,
/// so axum's `Infallible` keeps flowing through untouched).
#[derive(Debug, Clone)]
pub struct TraceService<S> {
    inner: S,
}

impl<S> Service<Request> for TraceService<S>
where
    S: Service<Request, Response = Response>,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let name = request_name(req.method().as_str(), req.uri().path());
        let incoming = incoming_trace_id(req.headers()).to_string();
        let fut = self.inner.call(req);
        Box::pin(async move {
            crate::integrations::server_span_around(&name, &incoming, fut).await
        })
    }
}

/// Span name per the wire contract: `"METHOD path"` (query/fragment
/// dropped by using only the URI path).
fn request_name(method: &str, path: &str) -> String {
    format!("{} {}", method.to_ascii_uppercase(), path)
}

fn incoming_trace_id(headers: &::axum::http::HeaderMap) -> &str {
    headers
        .get(TRACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::axum::http::StatusCode;
    use ::axum::routing::get;
    use ::tower::ServiceExt;
    use crate::transport::tests::{ensure_configured, EMIT_LOCK};

    fn app() -> ::axum::Router {
        ::axum::Router::new()
            .route("/hello", get(|| async { "ok" }))
            .route(
                "/nested",
                get(|| async {
                    crate::trace("orders.Load", |s| {
                        s.data("id", 7i64);
                    });
                    "ok"
                }),
            )
            .route("/boom", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
            .layer(TraceLayer)
    }

    fn get_req(path: &str, trace_id: Option<&str>) -> Request {
        let mut b = Request::builder().method("GET").uri(path);
        if let Some(tid) = trace_id {
            b = b.header(TRACE_HEADER, tid);
        }
        b.body(::axum::body::Body::empty()).unwrap()
    }

    /// Pulls one quoted string field out of an event JSON line (test-only).
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

    #[tokio::test]
    async fn layer_opens_inherited_server_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let res = ServiceExt::oneshot(app(), get_req("/hello", Some("trace-abc")))
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "one HTTP_SERVER event expected");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"HTTP_SERVER\""), "{}", ev);
        assert!(ev.contains("\"name\":\"GET /hello\""), "{}", ev);
        assert!(ev.contains("\"trace_id\":\"trace-abc\""), "{}", ev);
        assert!(ev.contains("\"status_code\":200"), "{}", ev);
        // Agent metadata stamped on entry-point spans.
        assert!(
            ev.contains(&format!("\"agent.sdk\":\"rust-sdk/{}\"", crate::SDK_VERSION)),
            "{}",
            ev
        );
        assert!(ev.contains("\"error_message\":\"\""), "{}", ev);
    }

    #[tokio::test]
    async fn handler_traces_nest_under_server_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let res = ServiceExt::oneshot(app(), get_req("/nested", Some("trace-nest")))
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 2, "child + server spans expected");
        let child = &events[events.len() - 2];
        let server = &events[events.len() - 1];
        // The handler's FUNCTION_CALL span joins the same trace and nests
        // under the HTTP_SERVER span.
        assert!(child.contains("\"type\":\"FUNCTION_CALL\""), "{}", child);
        assert!(child.contains("\"name\":\"orders.Load\""), "{}", child);
        assert_eq!(
            json_str_field(child, "trace_id"),
            "trace-nest",
            "{}",
            child
        );
        assert_eq!(
            json_str_field(child, "parent_span_id"),
            json_str_field(server, "span_id"),
            "child parent must be the server span"
        );
    }

    #[tokio::test]
    async fn server_error_marks_the_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let res = ServiceExt::oneshot(app(), get_req("/boom", None)).await.unwrap();
        assert_eq!(res.status(), 500);
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(ev.contains("\"error_message\":\"HTTP 500\""), "{}", ev);
        // No incoming header -> a fresh trace id (never empty).
        assert!(!json_str_field(ev, "trace_id").is_empty(), "{}", ev);
    }

    #[tokio::test]
    async fn from_fn_form_matches_layer_form() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let router = ::axum::Router::new()
            .route("/hello", get(|| async { "ok" }))
            .layer(::axum::middleware::from_fn(axum_middleware));
        let before = crate::pipeline::buffered_events().len();
        let res = ServiceExt::oneshot(router, get_req("/hello", Some("trace-fn")))
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"HTTP_SERVER\""), "{}", ev);
        assert!(ev.contains("\"name\":\"GET /hello\""), "{}", ev);
        assert!(ev.contains("\"trace_id\":\"trace-fn\""), "{}", ev);
    }
}
