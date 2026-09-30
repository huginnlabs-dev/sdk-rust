//! HuginnLabs Dataflow SDK for Rust — runtime tracing with E2E-encrypted
//! payloads.
//!
//! RAII spans (mirroring the C++ SDK), a thread-local trace context and a
//! background sender shipping REST ingest batches (`POST /api/v1/ingest`).
//! Payload field names travel as plaintext metadata (lineage + PII
//! categories); values are AES-256-GCM encrypted with a PBKDF2-derived key
//! that never leaves the process.
//!
//! ```no_run
//! dataflow_rs::configure();
//! dataflow_rs::trace("dispatch.Route", |span| {
//!     span.data("city", "Riga");
//!     dataflow_rs::trace("geo.Resolve", |s| { s.data("city", "Riga"); });
//! }); // span ends and is queued here
//! ```
//!
//! Transport guards ([`http_span`] / [`db_span`]) wrap outgoing HTTP calls
//! and database queries as HTTP_CLIENT / DB_QUERY spans — RAII, transport
//! agnostic, no client-library dependencies.
//!
//! Env: `DATAFLOW_ENDPOINT` (http://host:port — plaintext HTTP, front it
//! with a TLS-terminating proxy for WAN), `DATAFLOW_API_KEY`,
//! `DATAFLOW_SERVICE_NAME`, `DATAFLOW_ENCRYPTION_KEY`,
//! `DATAFLOW_SAMPLE_RATIO`, `DATAFLOW_BUFFER_SIZE`, `DATAFLOW_DISABLED`,
//! `DATAFLOW_HTTP_URL` (HTTP API base for manifest reporting when the
//! endpoint is a bare gRPC host:port), `DATAFLOW_APP_VERSION`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod crypto;
pub mod json;
mod pii;
mod pipeline;
pub mod transport;

pub use transport::{db_span, http_span, DbSpan, HttpSpan};

/// SDK version stamped into agent metadata and the service manifest.
pub const SDK_VERSION: &str = "0.3.0";

/// Payload field value: a small JSON-ready enum (no serde dependency).
#[derive(Clone, Debug)]
pub enum Value {
    Str(String),
    Num(f64),
    Bool(bool),
    Raw(String), // pre-serialized JSON
}

impl From<&str> for Value {
    fn from(v: &str) -> Self { Value::Str(v.to_string()) }
}
impl From<String> for Value {
    fn from(v: String) -> Self { Value::Str(v) }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self { Value::Num(v as f64) }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self { Value::Num(v) }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self { Value::Bool(v) }
}

/// Immutable SDK settings.
#[derive(Clone)]
pub struct Settings {
    pub endpoint: String,
    pub api_key: String,
    pub service_name: String,
    pub encryption_key: String,
    pub sample_ratio: f64,
    pub buffer_size: usize,
    pub disabled: bool,
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn env_num(key: &str, fallback: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();
static SENDER_STARTED: AtomicBool = AtomicBool::new(false);
static HOSTNAME: OnceLock<String> = OnceLock::new();

/// Configures the SDK from `DATAFLOW_*` environment variables and starts
/// the background sender. Idempotent: repeated calls update settings but
/// never spawn a second sender.
pub fn configure() {
    let disabled = env_or("DATAFLOW_DISABLED", "false") == "true";
    let s = Settings {
        endpoint: env_or("DATAFLOW_ENDPOINT", ""),
        api_key: env_or("DATAFLOW_API_KEY", ""),
        service_name: env_or("DATAFLOW_SERVICE_NAME", ""),
        encryption_key: env_or("DATAFLOW_ENCRYPTION_KEY", ""),
        sample_ratio: env_num("DATAFLOW_SAMPLE_RATIO", 1.0),
        buffer_size: env_num("DATAFLOW_BUFFER_SIZE", 10_000.0) as usize,
        disabled,
    };
    let _ = SETTINGS.set(s.clone());
    // compare_exchange yields Ok(previous): a successful first start reads
    // Ok(false), so test is_ok() — NOT Ok(true).
    let not_yet_started = SENDER_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok();
    if SETTINGS.get().map(|s| !s.disabled && !s.endpoint.is_empty() && !s.api_key.is_empty()) == Some(true)
        && not_yet_started
    {
        pipeline::start();
    }
}

/// True when spans are collected and shipped.
pub fn enabled() -> bool {
    match SETTINGS.get() {
        Some(s) => !s.disabled && !s.endpoint.is_empty() && !s.api_key.is_empty(),
        None => false,
    }
}

fn settings() -> &'static Settings {
    static PASSIVE: OnceLock<Settings> = OnceLock::new();
    SETTINGS.get().unwrap_or_else(|| {
        PASSIVE.get_or_init(|| Settings {
            endpoint: String::new(),
            api_key: String::new(),
            service_name: String::new(),
            encryption_key: String::new(),
            sample_ratio: 1.0,
            buffer_size: 10_000,
            disabled: true,
        })
    })
}

fn service_name() -> String {
    let n = settings().service_name.clone();
    if !n.is_empty() {
        return n;
    }
    HOSTNAME
        .get_or_init(|| {
            std::fs::read_to_string("/etc/hostname")
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        })
        .clone()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn new_id() -> String {
    // UUIDv4-shaped id from the OS RNG.
    let mut b = [0u8; 16];
    fill_random(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{:02x}", x)).collect();
    hex.to_string()
}

pub(crate) fn fill_random(buf: &mut [u8]) {
    use std::io::Read;
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(buf);
        return;
    }
    // Fallback: hash of time + address entropy (never used on Linux).
    let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut state = seed as u64 ^ (&buf as *const _ as u64);
    for slot in buf.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *slot = (state >> 33) as u8;
    }
}

// ---------------------------------------------------------------------------
// Service manifest

/// Builds the startup service-manifest body (`POST /api/v1/manifest`) with
/// the existing JSON writer; field order is fixed by the wire contract.
///
/// `runtime_version` is empty — Rust binaries are statically linked and
/// carry no discoverable runtime; a build script can opt in by setting
/// `DATAFLOW_RUNTIME` at compile time. `framework` stays empty for v1
/// (no reliable runtime detection in Rust) and `dependencies` stays empty:
/// statically linked binaries have no runtime dependency inventory
/// (build-time generation, e.g. from Cargo.lock via a build script, may
/// come later).
pub fn build_manifest(service_name: &str, sdk_version: &str) -> String {
    build_manifest_with(
        service_name,
        sdk_version,
        &std::env::var("DATAFLOW_APP_VERSION").unwrap_or_default(),
    )
}

/// [`build_manifest`] with the app version injected (testability).
pub(crate) fn build_manifest_with(service_name: &str, sdk_version: &str, app_version: &str) -> String {
    let mut sb = String::with_capacity(256);
    sb.push('{');
    sb.push_str("\"service_name\":");
    json::escape(&mut sb, service_name);
    sb.push_str(",\"language\":");
    json::escape(&mut sb, "rust");
    sb.push_str(",\"sdk_version\":");
    json::escape(&mut sb, sdk_version);
    sb.push_str(",\"runtime_version\":");
    json::escape(&mut sb, option_env!("DATAFLOW_RUNTIME").unwrap_or(""));
    sb.push_str(",\"framework\":");
    json::escape(&mut sb, "");
    sb.push_str(",\"os_arch\":");
    json::escape(&mut sb, &format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH));
    sb.push_str(",\"app_version\":");
    json::escape(&mut sb, app_version);
    // No runtime dependency inventory for statically linked binaries (v1).
    sb.push_str(",\"dependencies\":[]");
    sb.push('}');
    sb
}

/// Resolves the HTTP API base for manifest reporting: the
/// `DATAFLOW_HTTP_URL` value (`http_url_env`, "" when unset) wins — needed
/// when the endpoint is a bare gRPC host:port; URL-form endpoints map
/// directly; anything else has no derivable HTTP base and reporting is
/// skipped. Trailing slashes are trimmed.
pub(crate) fn http_base(endpoint: &str, http_url_env: &str) -> Option<String> {
    let override_url = http_url_env.trim();
    if !override_url.is_empty() {
        return Some(override_url.trim_end_matches('/').to_string());
    }
    if endpoint.starts_with("https://") || endpoint.starts_with("http://") {
        return Some(endpoint.trim_end_matches('/').to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// Span

/// One measured unit of work. Cloneable handle over shared state; ends
/// automatically when the owning [`Trace`] scope drops.
#[derive(Clone)]
pub struct Span {
    inner: Arc<SpanInner>,
}

struct SpanInner {
    state: Mutex<SpanState>,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    caller: String,
    name: String,
    kind: &'static str,
    start_ms: i64,
    start_nanos: std::time::Instant,
    sampled: bool,
}

#[derive(Default)]
struct SpanState {
    attrs: BTreeMap<String, String>,
    payload: BTreeMap<String, Value>,
    error: String,
    status: i32,
    callee: String,
    ended: bool,
}

impl Span {
    /// Crate-internal constructor: the transport guards open client-side
    /// spans (HTTP_CLIENT, DB_QUERY) parented to the current span without
    /// becoming the thread's current one.
    pub(crate) fn new(name: &str, kind: &'static str, parent: Option<&Span>) -> Span {
        Self::with_trace(name, kind, parent, None)
    }

    fn with_trace(
        name: &str,
        kind: &'static str,
        parent: Option<&Span>,
        trace_override: Option<String>,
    ) -> Span {
        let sampled = settings().sample_ratio >= 1.0
            || rand_float() < settings().sample_ratio;
        let callee = if kind == "FUNCTION_CALL" {
            name.split('.').next().unwrap_or("").to_string()
        } else {
            String::new()
        };
        Span {
            inner: Arc::new(SpanInner {
                state: Mutex::new(SpanState {
                    callee,
                    ..Default::default()
                }),
                trace_id: trace_override
                    .or_else(|| parent.map(|p| p.inner.trace_id.clone()))
                    .unwrap_or_else(new_id),
                span_id: new_id(),
                parent_span_id: parent.map(|p| p.inner.span_id.clone()).unwrap_or_default(),
                // Caller attribution mirrors the other SDKs: the enclosing
                // span's package; roots stay empty (no self-edges).
                caller: parent.map(|p| p.inner.state.lock().unwrap().callee.clone()).unwrap_or_default(),
                name: name.to_string(),
                kind,
                start_ms: now_ms(),
                start_nanos: std::time::Instant::now(),
                sampled,
            }),
        }
    }

    /// Plaintext attribute (metadata entry).
    pub fn attr(&self, key: &str, value: &str) -> &Self {
        self.inner.state.lock().unwrap().attrs.insert(key.into(), value.into());
        self
    }

    /// Payload field — values are encrypted at end when a key is set.
    pub fn data(&self, key: &str, value: impl Into<Value>) -> &Self {
        self.inner.state.lock().unwrap().payload.insert(key.into(), value.into());
        self
    }

    /// Payload field holding a pre-serialized JSON document.
    pub fn data_json(&self, key: &str, raw_json: &str) -> &Self {
        self.data(key, Value::Raw(raw_json.into()))
    }

    /// Marks this span's package (or host) for the data-flow graph.
    pub fn callee(&self, pkg: &str) -> &Self {
        self.inner.state.lock().unwrap().callee = pkg.into();
        self
    }

    pub fn record_error(&self, message: &str) -> &Self {
        let mut st = self.inner.state.lock().unwrap();
        if st.error.is_empty() {
            st.error = message.into();
        } else {
            st.error = format!("{}; {}", st.error, message);
        }
        st.status = 500;
        self
    }

    /// HTTP status or gRPC code.
    pub fn status(&self, code: i32) -> &Self {
        self.inner.state.lock().unwrap().status = code;
        self
    }

    pub fn trace_id(&self) -> String { self.inner.trace_id.clone() }

    /// Ends the span and queues it for delivery (idempotent).
    pub fn end(&self) {
        if !self.inner.sampled {
            return;
        }
        let (meta, payload, error, status, callee) = {
            let mut st = self.inner.state.lock().unwrap();
            if st.ended {
                return;
            }
            st.ended = true;
            (
                st.attrs.clone(),
                st.payload.clone(),
                st.error.clone(),
                st.status,
                st.callee.clone(),
            )
        };
        let duration_ms = self.inner.start_nanos.elapsed().as_millis() as i64;

        let mut fields: Vec<String> = payload.keys().cloned().collect();
        fields.sort();
        let mut meta = meta;
        if !fields.is_empty() {
            meta.insert("data.fields".into(), fields.join(","));
            let pii = pii::classify(&fields);
            if !pii.is_empty() {
                meta.insert("data.pii".into(), pii);
            }
        }

        let payload_json = if payload.is_empty() {
            "null".to_string()
        } else {
            crypto::seal(&payload)
        };
        pipeline::enqueue(json::event_json(
            &new_id(),
            pipeline::next_seq(),
            &self.inner.trace_id,
            &self.inner.span_id,
            &self.inner.parent_span_id,
            self.inner.kind,
            &service_name(),
            &self.inner.name,
            &self.inner.caller,
            &callee,
            error.as_str(),
            status,
            self.inner.start_ms,
            duration_ms,
            &meta,
            &payload_json,
        ));
    }
}

fn rand_float() -> f64 {
    let mut b = [0u8; 8];
    fill_random(&mut b);
    u64::from_le_bytes(b) as f64 / u64::MAX as f64
}

// ---------------------------------------------------------------------------
// Trace scope + thread-local context

thread_local! {
    static CURRENT: RefCell<Option<Span>> = const { RefCell::new(None) };
}

/// The span active on this thread, if any.
pub fn current() -> Option<Span> { CURRENT.with(|c| c.borrow().clone()) }

/// RAII scope: opens a span named `name`, makes it the thread's current
/// one and ends it on drop — the Rust analogue of the C++ Trace and the
/// Java try-with-resources.
pub struct Trace {
    span: Span,
    previous: Option<Span>,
    skip_drop: bool,
}

impl Trace {
    /// Opens a FUNCTION_CALL child of the current span (or a new trace).
    pub fn new(name: &str) -> Trace { Trace::with_kind(name, "FUNCTION_CALL") }

    /// Opens a span of an explicit event type (HTTP_SERVER, HTTP_CLIENT…).
    pub fn with_kind(name: &str, kind: &'static str) -> Trace {
        let previous = current();
        let span = Span::new(name, kind, previous.as_ref());
        CURRENT.with(|c| *c.borrow_mut() = Some(span.clone()));
        Trace { span, previous, skip_drop: false }
    }

    /// The active span.
    pub fn span(&self) -> &Span { &self.span }

    /// Ends the span without waiting for the scope to drop.
    pub fn end_now(&mut self) {
        self.span.end();
        self.skip_drop = true;
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        if !self.skip_drop {
            self.span.end();
        }
        CURRENT.with(|c| *c.borrow_mut() = self.previous.clone());
    }
}

/// Runs `body` inside a named span (convenience over [`Trace`]).
pub fn trace<T>(name: &str, body: impl FnOnce(&Span) -> T) -> T {
    let mut t = Trace::new(name);
    let out = body(t.span());
    t.end_now();
    out
}

/// Opens an entry-point (HTTP_SERVER) root span and stamps agent metadata.
pub fn start_server_span(route: &str) -> Span {
    let span = Span::with_trace(route, "HTTP_SERVER", None, None);
    for (k, v) in agent_attrs() {
        span.attr(&k, &v);
    }
    span
}

/// Same as [`start_server_span`], adopting an incoming
/// `X-Dataflow-Trace-Id` header so the caller service joins the trace.
pub fn start_server_span_inherited(route: &str, incoming_trace_id: &str) -> Span {
    let span = Span::with_trace(
        route,
        "HTTP_SERVER",
        None,
        if incoming_trace_id.is_empty() { None } else { Some(incoming_trace_id.to_string()) },
    );
    for (k, v) in agent_attrs() {
        span.attr(&k, &v);
    }
    span
}

/// Host/process descriptor stamped onto entry-point spans.
pub fn agent_attrs() -> Vec<(String, String)> {
    static AGENT: OnceLock<Vec<(String, String)>> = OnceLock::new();
    AGENT
        .get_or_init(|| {
            let mut v: Vec<(String, String)> = vec![
                ("agent.os".into(), format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)),
                ("agent.runtime".into(), "rust".into()),
                ("agent.sdk".into(), format!("rust-sdk/{}", SDK_VERSION)),
                ("agent.cpu".into(), std::thread::available_parallelism().map(|n| n.get().to_string()).unwrap_or_else(|_| "1".into())),
                ("agent.pid".into(), std::process::id().to_string()),
                ("agent.started".into(), now_ms().to_string()),
            ];
            if let Ok(env) = std::env::var("DATAFLOW_ENV") {
                if !env.is_empty() { v.push(("agent.env".into(), env)); }
            }
            if let Ok(ver) = std::env::var("DATAFLOW_APP_VERSION") {
                if !ver.is_empty() { v.push(("agent.app_version".into(), ver)); }
            }
            v
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_fields_match_wire_contract() {
        let body = build_manifest_with("payments", SDK_VERSION, "1.4.2");
        assert!(body.contains("\"service_name\":\"payments\""), "{}", body);
        assert!(body.contains("\"language\":\"rust\""), "{}", body);
        assert!(
            body.contains(&format!("\"sdk_version\":\"{}\"", SDK_VERSION)),
            "{}",
            body
        );
        // os_arch mirrors the agent.os attribute format.
        assert!(
            body.contains(&format!(
                "\"os_arch\":\"{}/{}\"",
                std::env::consts::OS,
                std::env::consts::ARCH
            )),
            "{}",
            body
        );
        assert!(body.contains("\"framework\":\"\""), "{}", body);
        assert!(body.contains("\"app_version\":\"1.4.2\""), "{}", body);
        assert!(body.contains("\"dependencies\":[]"), "{}", body);
    }

    #[test]
    fn manifest_field_order_is_stable() {
        let body = build_manifest_with("svc", SDK_VERSION, "");
        let order: Vec<&str> = [
            "service_name",
            "language",
            "sdk_version",
            "runtime_version",
            "framework",
            "os_arch",
            "app_version",
            "dependencies",
        ]
        .to_vec();
        let mut pos = 0;
        for field in order {
            let at = body.find(&format!("\"{}\"", field)).unwrap_or_else(|| panic!("missing {}", field));
            assert!(at > pos, "{} out of order in {}", field, body);
            pos = at;
        }
    }

    #[test]
    fn manifest_escapes_values() {
        let body = build_manifest_with("pay\"ments\n", SDK_VERSION, "");
        assert!(
            body.contains("\"service_name\":\"pay\\\"ments\\n\""),
            "{}",
            body
        );
    }

    #[test]
    fn manifest_wrapper_shape() {
        // The env-reading wrapper keeps the same structure (app version not
        // asserted: it depends on the ambient DATAFLOW_APP_VERSION).
        let body = build_manifest("svc", SDK_VERSION);
        assert!(body.starts_with('{') && body.ends_with('}'), "{}", body);
        assert!(body.contains("\"language\":\"rust\""), "{}", body);
        assert!(body.contains("\"dependencies\":[]"), "{}", body);
    }

    #[test]
    fn http_base_resolution() {
        // Explicit DATAFLOW_HTTP_URL wins, whitespace trimmed, trailing
        // slash trimmed — even for a bare host:port endpoint.
        assert_eq!(
            http_base("api:9090", "http://api:8080/"),
            Some("http://api:8080".to_string())
        );
        assert_eq!(
            http_base("api:9090", "  https://ingest.example.com  "),
            Some("https://ingest.example.com".to_string())
        );
        // URL-form endpoints map directly (trailing slash trimmed).
        assert_eq!(
            http_base("https://ingest.example.com/", ""),
            Some("https://ingest.example.com".to_string())
        );
        assert_eq!(
            http_base("http://ingest.example.com", ""),
            Some("http://ingest.example.com".to_string())
        );
        // Bare gRPC endpoint without an HTTP override: nothing to report to.
        assert_eq!(http_base("api:9090", ""), None);
        assert_eq!(http_base("localhost:50051", ""), None);
    }
}
