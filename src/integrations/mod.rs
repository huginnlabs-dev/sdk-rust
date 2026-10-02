//! Optional framework integrations — compiled only when their feature is
//! enabled. The core SDK stays dependency-free; enabling a feature pulls in
//! exactly that framework:
//!
//! | Feature | Module | What you get |
//! | --- | --- | --- |
//! | `axum` | `integrations::axum` | `axum_middleware` (use with `axum::middleware::from_fn`) and `TraceLayer` (tower `Layer`) for axum 0.7 |
//! | `actix` | `integrations::actix` | `DataflowMiddleware` for actix-web 4 (`.wrap(DataflowMiddleware)`) |
//! | `sqlx` | `integrations::sqlx` | `query_span` — run one statement inside a `DB_QUERY` span |
//!
//! All three share the server-span semantics documented on
//! [`crate::start_server_span_inherited`]: the middleware opens an
//! `HTTP_SERVER` span named `"METHOD path"` (agent metadata stamped,
//! incoming `X-Dataflow-Trace-Id` adopted so the caller joins the trace),
//! makes it the thread's current span while the rest of the stack runs,
//! records the response status (and an `error_message` for 5xx or failed
//! handler futures) and ends the span when the response is ready. With the
//! SDK disabled the middleware is pure pass-through — no span is opened
//! and nothing is recorded.

use crate::Trace;

#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "actix")]
pub mod actix;
#[cfg(feature = "sqlx")]
pub mod sqlx;

/// Opens the `HTTP_SERVER` entry span (`name` should be `"METHOD path"`)
/// with agent attributes and the adopted incoming trace id, and makes it
/// the thread's current one. `None` when the SDK is disabled — callers
/// then run the plain stack (pass-through, zero overhead beyond the check).
pub(crate) fn open_server_span(name: &str, incoming_trace_id: &str) -> Option<Trace> {
    if !crate::enabled() {
        return None;
    }
    let span = crate::start_server_span_inherited(name, incoming_trace_id);
    Some(Trace::attach(span))
}

/// Extracts the span outcome from a middleware stack's result. Implemented
/// for plain responses (axum's `Next` future cannot fail) and for
/// `Result<ServiceResponse<B>, Error>` (actix handlers fail as `Error`).
pub(crate) trait SpanOutcome {
    /// Status stamped on the span (`status_code` on the wire).
    fn df_status(&self) -> i32;
    /// Whether the span must be marked failed (`error_message` recorded).
    fn df_is_error(&self) -> bool;
    /// The `error_message` recorded when [`SpanOutcome::df_is_error`] holds.
    fn df_error_message(&self) -> String;
}

/// Runs the rest of the middleware stack inside one `HTTP_SERVER` span and
/// records the outcome — the single body shared by the axum and actix
/// middleware implementations.
pub(crate) async fn server_span_around<F, T>(name: &str, incoming_trace_id: &str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
    T: SpanOutcome,
{
    let Some(mut t) = open_server_span(name, incoming_trace_id) else {
        return fut.await;
    };
    let out = fut.await;
    let status = out.df_status();
    t.span().status(status);
    if out.df_is_error() {
        t.span().record_error(&out.df_error_message());
    }
    t.end_now();
    out
}
