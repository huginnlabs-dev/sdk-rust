//! Application log shipping with trace correlation.
//!
//! [`log`] (and the shorthands [`debug`] / [`info`] / [`warn`] /
//! [`error`], plus the field-carrying [`info_with`] / [`error_with`])
//! records a log line annotated with the thread's current span ids — the
//! same correlation the spans use, so log lines and traces line up in the
//! server. Recorded outside any span, both ids ship empty.
//!
//! Lines are serialized eagerly (hand-rolled JSON, no dependencies),
//! buffered in a bounded queue (1024 entries, drop-oldest; the dropped
//! count is tracked in an atomic) and shipped by a background flusher
//! thread to `POST {base}/api/v1/logs` in batches of at most 1000: woken
//! by the queue (50-line threshold) or on a 500 ms tick. A failed POST is
//! retried once, then the batch is dropped — logging is best-effort and
//! never blocks or panics the caller. [`flush_logs`] drains and ships
//! synchronously (call it before process exit; the queue does not survive
//! a restart).
//!
//! The base is resolved manifest-style ([`crate::http_base`]): a
//! URL-form `DATAFLOW_ENDPOINT` maps directly, `DATAFLOW_HTTP_URL`
//! overrides, and a bare host:port (gRPC) endpoint has no derivable HTTP
//! base — logging stays off. With the SDK disabled every helper is a
//! no-op.
//!
//! The message is clipped to 8192 chars (UTF-8 boundary safe, top kept)
//! and at most 50 `fields` entries ship per line. Levels are normalized
//! to the wire set `debug | info | warn | error`: case and whitespace are
//! trimmed, common aliases map onto the nearest level (`trace` →
//! `debug`, `warning` → `warn`, `err` / `fatal` / `critical` → `error`)
//! and anything unknown ships as `info`.
//!
//! v1 is an explicit API only — a facade over the ecosystem `log` crate
//! may come later.
//!
//! ```no_run
//! dataflow_rs::configure();
//! dataflow_rs::info("orders processed");
//! dataflow_rs::info_with("cache miss", &[("key", "user:42".to_string())]);
//! dataflow_rs::log("warning", "retries running low", &[("left", "3".to_string())]);
//! dataflow_rs::error_with("payment failed", &[("provider", "stripe".to_string())]);
//! dataflow_rs::flush_logs(); // optional: ship the remainder, e.g. before exit
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

/// Buffered log lines before the flusher drains them (drop-oldest past
/// this; each entry is a ready-to-ship JSON object).
const QUEUE_CAP: usize = 1024;

/// The flusher ships as soon as this many lines are queued; otherwise it
/// ships whatever accumulated on the tick.
const FLUSH_THRESHOLD: usize = 50;

/// Idle wait between flush opportunities.
const FLUSH_TICK: Duration = Duration::from_millis(500);

/// Wire contract: at most 1000 log lines per batch.
const BATCH_MAX: usize = 1000;

/// At most 50 fields per line (first 50 win).
const FIELDS_MAX: usize = 50;

/// Upper bound for the message (chars, UTF-8 safe clipping, top kept).
const MAX_MESSAGE_CHARS: usize = 8192;

/// Log POST budget — shorter than the ingest timeout: logs are
/// best-effort and must not park the flusher long on a dead network.
const LOG_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

/// Pause between the first failed POST and the single retry.
const RETRY_DELAY: Duration = Duration::from_millis(500);

struct Shared {
    queue: Mutex<VecDeque<String>>,
    wake: Condvar,
}

static SHARED: OnceLock<Shared> = OnceLock::new();
static DROPPED: AtomicU64 = AtomicU64::new(0);

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| Shared {
        queue: Mutex::new(VecDeque::new()),
        wake: Condvar::new(),
    })
}

/// Test-freeze switch: when set, the background flusher never drains the
/// queue. Drain decisions are made under the queue lock, so a test that
/// sets this before recording can never race a concurrent flush.
#[cfg(test)]
static TEST_PAUSE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
fn paused() -> bool {
    TEST_PAUSE.load(Ordering::SeqCst)
}

#[cfg(not(test))]
fn paused() -> bool {
    false
}

/// Records one application log line at `level` with optional structured
/// fields. Best-effort: a no-op when logging is off (SDK disabled or no
/// derivable HTTP base), never blocks on I/O and never panics. The line
/// is stamped with the thread's current span ids (empty without one) and
/// queued for the background flusher.
pub fn log(level: &str, message: &str, fields: &[(&str, String)]) {
    if !logging_on() {
        return;
    }
    ensure_flusher();
    let (trace_id, span_id) = match crate::current() {
        Some(span) => (span.trace_id(), span.span_id()),
        None => (String::new(), String::new()),
    };
    let line = line_json(
        crate::now_ms(),
        normalize_level(level),
        &clip_message(message),
        &trace_id,
        &span_id,
        &crate::service_name(),
        fields,
    );
    let sh = shared();
    let mut q = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
    q.push_back(line);
    while q.len() > QUEUE_CAP {
        q.pop_front();
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
    sh.wake.notify_one();
}

/// Records a `debug` line.
pub fn debug(message: &str) {
    log("debug", message, &[]);
}

/// Records an `info` line.
pub fn info(message: &str) {
    log("info", message, &[]);
}

/// Records a `warn` line.
pub fn warn(message: &str) {
    log("warn", message, &[]);
}

/// Records an `error` line.
pub fn error(message: &str) {
    log("error", message, &[]);
}

/// [`info`] with structured fields.
pub fn info_with(message: &str, fields: &[(&str, String)]) {
    log("info", message, fields);
}

/// [`error`] with structured fields.
pub fn error_with(message: &str, fields: &[(&str, String)]) {
    log("error", message, fields);
}

/// Drains the queue and ships it synchronously (same one-retry-then-drop
/// policy as the background flusher). Use before process exit — the queue
/// does not survive a restart. A no-op when logging is off or the queue
/// is empty; every failure is silent.
pub fn flush_logs() {
    if let Some((base, api_key)) = shipping_target() {
        ship(&base, &api_key);
    }
}

/// Logging is on when the SDK is enabled AND an HTTP base is derivable
/// (URL-form endpoint or `DATAFLOW_HTTP_URL`); a bare host:port endpoint
/// leaves logging off — the manifest-style resolution.
fn logging_on() -> bool {
    shipping_target().is_some()
}

/// `(base, api_key)` for the logs endpoint, or `None` when logging is off.
fn shipping_target() -> Option<(String, String)> {
    #[cfg(test)]
    if let Some(t) = TEST_TARGET.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        return Some(t.clone());
    }
    shipping_target_for(
        &crate::settings(),
        &std::env::var("DATAFLOW_HTTP_URL").unwrap_or_default(),
    )
}

/// [`shipping_target`] from explicit inputs (testability — the pattern of
/// [`crate::build_manifest_with`]: process-global settings are
/// first-configure-wins and cannot be re-pointed by a unit test).
fn shipping_target_for(s: &crate::Settings, http_url_env: &str) -> Option<(String, String)> {
    if s.disabled || s.endpoint.is_empty() || s.api_key.is_empty() {
        return None;
    }
    let base = crate::http_base(&s.endpoint, http_url_env)?;
    Some((base, s.api_key.clone()))
}

/// Test-only shipping target override: unit tests cannot rely on
/// `configure()` (global settings are first-configure-wins), so the
/// network tests inject the stub address here.
#[cfg(test)]
static TEST_TARGET: Mutex<Option<(String, String)>> = Mutex::new(None);

/// Normalizes a caller-supplied level to the wire set. Unknown levels
/// ship as `info` (the server only accepts the four canonical values).
fn normalize_level(level: &str) -> &'static str {
    match level.trim().to_ascii_lowercase().as_str() {
        "debug" | "trace" => "debug",
        "info" | "notice" => "info",
        "warn" | "warning" => "warn",
        "error" | "err" | "fatal" | "critical" => "error",
        _ => "info",
    }
}

/// Serializes one log line as the wire JSON object (field order fixed by
/// the contract). Field values are escaped strings.
fn line_json(
    timestamp: i64,
    level: &str,
    message: &str,
    trace_id: &str,
    span_id: &str,
    service: &str,
    fields: &[(&str, String)],
) -> String {
    let mut sb = String::with_capacity(128 + message.len());
    sb.push('{');
    sb.push_str("\"timestamp\":");
    sb.push_str(&timestamp.to_string());
    sb.push_str(",\"level\":");
    crate::json::escape(&mut sb, level);
    sb.push_str(",\"message\":");
    crate::json::escape(&mut sb, message);
    sb.push_str(",\"trace_id\":");
    crate::json::escape(&mut sb, trace_id);
    sb.push_str(",\"span_id\":");
    crate::json::escape(&mut sb, span_id);
    sb.push_str(",\"service_name\":");
    crate::json::escape(&mut sb, service);
    sb.push_str(",\"fields\":{");
    let mut first = true;
    for (k, v) in fields.iter().take(FIELDS_MAX) {
        if !first {
            sb.push(',');
        }
        first = false;
        crate::json::escape(&mut sb, k);
        sb.push(':');
        crate::json::escape(&mut sb, v);
    }
    sb.push_str("}}");
    sb
}

/// Clips the message to [`MAX_MESSAGE_CHARS`] chars (UTF-8 safe, top
/// kept) — the same char-counted clipping style as the crash capture.
fn clip_message(s: &str) -> String {
    if s.chars().count() <= MAX_MESSAGE_CHARS {
        return s.to_string();
    }
    s.chars().take(MAX_MESSAGE_CHARS).collect()
}

/// Spawns the background flusher exactly once (lazily, on the first
/// recorded line). A failed spawn is ignored — [`flush_logs`] remains
/// available.
fn ensure_flusher() {
    static STARTED: AtomicBool = AtomicBool::new(false);
    if STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let _ = std::thread::Builder::new()
            .name("dataflow-log-flush".into())
            .spawn(flusher_loop);
    }
}

fn flusher_loop() {
    loop {
        let should_flush = {
            let sh = shared();
            let mut q = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
            if paused() {
                // Test freeze: park (bounded by the tick) and never drain.
                // Re-checked under the lock, so this cannot race a test
                // that sets the pause before recording.
                let _ = sh
                    .wake
                    .wait_timeout(q, FLUSH_TICK)
                    .unwrap_or_else(|p| p.into_inner());
                continue;
            }
            if q.len() < FLUSH_THRESHOLD {
                let (guard, timeout) = sh
                    .wake
                    .wait_timeout(q, FLUSH_TICK)
                    .unwrap_or_else(|p| p.into_inner());
                q = guard;
                // Re-check after the wait: the pause may have been set
                // while we slept, and an early wake below the threshold
                // (another notify) goes back to sleep — only the tick
                // ships a partial queue.
                if paused() || (!timeout.timed_out() && q.len() < FLUSH_THRESHOLD) {
                    continue;
                }
            }
            !q.is_empty()
        };
        if should_flush {
            if let Some((base, api_key)) = shipping_target() {
                ship(&base, &api_key);
            }
        }
    }
}

/// Ships one batch of up to [`BATCH_MAX`] lines: snapshot the batch under
/// the lock, POST it (network I/O outside the lock), retry once, then
/// drop the batch — logging is best-effort by contract.
fn ship(base: &str, api_key: &str) {
    let batch = drain(BATCH_MAX);
    if batch.is_empty() {
        return;
    }
    let body = format!("{{\"logs\":[{}]}}", batch.join(","));
    for attempt in 0..2 {
        let sent = matches!(
            crate::pipeline::http_post(base, "/api/v1/logs", api_key, body.as_bytes(), LOG_HTTP_TIMEOUT),
            Ok(resp) if resp.status.contains(" 200")
        );
        if sent {
            return;
        }
        if attempt == 0 {
            std::thread::sleep(RETRY_DELAY);
        }
    }
    // Both attempts failed: the batch is dropped.
}

/// Removes and returns up to `max` queued lines (oldest first).
fn drain(max: usize) -> Vec<String> {
    let sh = shared();
    let mut q = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
    let n = max.min(q.len());
    q.drain(..n).collect()
}

#[cfg(test)]
pub(crate) fn drain_for_test() -> Vec<String> {
    let sh = shared();
    let mut q = sh.queue.lock().unwrap_or_else(|p| p.into_inner());
    q.drain(..).collect()
}

#[cfg(test)]
pub(crate) fn dropped_for_test() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tests::{ensure_configured, EMIT_LOCK};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Instant;

    /// Injects the stub as the shipping target for the duration of a
    /// network test: unit tests cannot rely on `configure()` (global
    /// settings are first-configure-wins), so the target is set directly
    /// and restored on drop.
    struct TestTarget(String, String);
    impl TestTarget {
        fn at(addr: &str) -> Self {
            let t = TestTarget(format!("http://{}", addr), "logs-test-key".to_string());
            *TEST_TARGET.lock().unwrap_or_else(|p| p.into_inner()) =
                Some((t.0.clone(), t.1.clone()));
            t
        }
    }
    impl Drop for TestTarget {
        fn drop(&mut self) {
            *TEST_TARGET.lock().unwrap_or_else(|p| p.into_inner()) = None;
        }
    }

    /// Freezes the background flusher and clears leftovers from earlier
    /// tests: the flusher re-checks the pause under the queue lock, so a
    /// test that sets it before recording can never race a drain.
    fn freeze_and_clear() {
        TEST_PAUSE.store(true, Ordering::SeqCst);
        drain_for_test();
    }

    /// Tiny reader for one integer field of a log line (test-only).
    fn json_i64_field(body: &str, field: &str) -> i64 {
        let needle = format!("\"{}\":", field);
        let pos = body
            .find(&needle)
            .unwrap_or_else(|| panic!("missing {} in {}", field, body));
        let rest = &body[pos + needle.len()..];
        let num: String = rest
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '-')
            .collect();
        num.parse()
            .unwrap_or_else(|_| panic!("bad {} in {}", field, body))
    }

    /// Reads one HTTP request off `stream` (headers by delimiter, body by
    /// Content-Length — the client keeps the write side open). `None` on
    /// a closed or timed-out connection.
    fn read_request(stream: &mut TcpStream) -> Option<(String, String)> {
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            match stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                }
                Err(_) => return None,
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let clen: usize = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split_once(':'))
            .and_then(|(_, v)| v.trim().parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + clen {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let at = head_end.min(buf.len());
        Some((head, String::from_utf8_lossy(&buf[at..]).to_string()))
    }

    /// Accepts connections until `min_entries` log lines have arrived on
    /// `POST /api/v1/logs` (or the deadline passes). Every request is
    /// answered 200; other traffic (the pipeline sender or the manifest
    /// reporter may target the same stub) is consumed and skipped.
    /// Returns the first logs request head and all log bodies joined.
    fn serve_logs_until(
        listener: &TcpListener,
        min_entries: usize,
        deadline: Instant,
    ) -> (String, String) {
        let _ = listener.set_nonblocking(true);
        let mut first_head = String::new();
        let mut all_bodies = String::new();
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(LOG_HTTP_TIMEOUT));
                    let request = read_request(&mut stream);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                    let _ = stream.flush();
                    drop(stream);
                    if let Some((head, body)) = request {
                        if head.starts_with("POST /api/v1/logs ") {
                            if first_head.is_empty() {
                                first_head = head;
                            }
                            all_bodies.push_str(&body);
                        }
                    }
                    if all_bodies.match_indices("\"timestamp\":").count() >= min_entries {
                        return (first_head, all_bodies);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("accept failed: {}", e),
            }
        }
        panic!(
            "only {} log entries arrived before the deadline",
            all_bodies.match_indices("\"timestamp\":").count()
        );
    }

    #[test]
    fn level_normalization() {
        assert_eq!(normalize_level("info"), "info");
        assert_eq!(normalize_level(" INFO "), "info");
        assert_eq!(normalize_level("Debug"), "debug");
        assert_eq!(normalize_level("TRACE"), "debug");
        assert_eq!(normalize_level("warn"), "warn");
        assert_eq!(normalize_level("warning"), "warn");
        assert_eq!(normalize_level("err"), "error");
        assert_eq!(normalize_level("Fatal"), "error");
        assert_eq!(normalize_level("critical"), "error");
        assert_eq!(normalize_level("bogus"), "info");
        assert_eq!(normalize_level(""), "info");
    }

    #[test]
    fn shipping_target_resolution() {
        let on = crate::Settings {
            endpoint: "http://logs.example.com".to_string(),
            api_key: "k".to_string(),
            service_name: "svc".to_string(),
            encryption_key: String::new(),
            sample_ratio: 1.0,
            buffer_size: 100,
            disabled: false,
        };
        // URL-form endpoint maps directly (trailing slash trimmed).
        assert_eq!(
            shipping_target_for(&on, "").map(|(b, _)| b),
            Some("http://logs.example.com".to_string())
        );
        // DATAFLOW_HTTP_URL wins over the endpoint form.
        assert_eq!(
            shipping_target_for(&on, "https://ingest.example.com/").map(|(b, _)| b),
            Some("https://ingest.example.com".to_string())
        );
        // A bare host:port (gRPC) endpoint without an override: logging
        // off — no derivable HTTP base.
        let mut bare = on.clone();
        bare.endpoint = "collector.internal:9090".to_string();
        assert!(shipping_target_for(&bare, "").is_none());
        // ...but the override enables it even there.
        assert_eq!(
            shipping_target_for(&bare, "http://logs.example.com").map(|(b, _)| b),
            Some("http://logs.example.com".to_string())
        );
        // Disabled SDK, missing endpoint or missing key: logging off.
        let mut off = on.clone();
        off.disabled = true;
        assert!(shipping_target_for(&off, "").is_none());
        let mut no_endpoint = on.clone();
        no_endpoint.endpoint = String::new();
        assert!(shipping_target_for(&no_endpoint, "").is_none());
        let mut no_key = on;
        no_key.api_key = String::new();
        assert!(shipping_target_for(&no_key, "").is_none());
    }

    #[test]
    fn records_correlate_with_current_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        freeze_and_clear();
        ensure_configured();

        log("error", "outside span", &[]);
        let mut inside_trace = String::new();
        let mut inside_span = String::new();
        crate::trace("handler", |span| {
            inside_trace = span.trace_id();
            inside_span = span.span_id();
            debug("inside span");
        });

        let service = format!("\"service_name\":\"{}\"", crate::service_name());
        let lines = drain_for_test();
        assert_eq!(lines.len(), 2, "{:?}", lines);
        assert!(lines[0].contains("\"message\":\"outside span\""), "{}", lines[0]);
        assert!(lines[0].contains("\"level\":\"error\""), "{}", lines[0]);
        assert!(lines[0].contains("\"trace_id\":\"\",\"span_id\":\"\""), "{}", lines[0]);

        assert!(lines[1].contains("\"message\":\"inside span\""), "{}", lines[1]);
        assert!(lines[1].contains("\"level\":\"debug\""), "{}", lines[1]);
        assert!(
            lines[1].contains(&format!("\"trace_id\":\"{}\"", inside_trace)),
            "{}",
            lines[1]
        );
        assert!(
            lines[1].contains(&format!("\"span_id\":\"{}\"", inside_span)),
            "{}",
            lines[1]
        );
        assert!(lines[1].contains(&service), "{}", lines[1]);
        assert!(lines[1].contains("\"fields\":{}"), "{}", lines[1]);
        let ts = json_i64_field(&lines[1], "timestamp");
        assert!(ts > 1_600_000_000_000, "unix ms timestamp: {}", ts);
    }

    #[test]
    fn fields_capped_at_50() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        freeze_and_clear();
        ensure_configured();

        let keys: Vec<String> = (0..60).map(|i| format!("f{}", i)).collect();
        let fields: Vec<(&str, String)> =
            keys.iter().map(|k| (k.as_str(), "v".to_string())).collect();
        info_with("capped", &fields);

        let lines = drain_for_test();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"f49\":\"v\""), "{}", lines[0]);
        assert!(!lines[0].contains("\"f50\""), "{}", lines[0]);
    }

    #[test]
    fn message_clipped_utf8_safe() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        freeze_and_clear();
        ensure_configured();

        let long = "ü".repeat(10_000);
        log("info", &long, &[]);

        let lines = drain_for_test();
        assert_eq!(lines.len(), 1);
        let clipped = clip_message(&long);
        assert_eq!(clipped.chars().count(), MAX_MESSAGE_CHARS);
        assert!(clipped.is_char_boundary(clipped.len()));
        assert!(
            lines[0].contains(&format!("\"message\":\"{}\"", clipped)),
            "clipped message must be the shipped message"
        );
    }

    #[test]
    fn queue_drop_oldest_at_capacity() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        freeze_and_clear();
        ensure_configured();

        let dropped_before = dropped_for_test();
        for i in 0..1030 {
            log("info", &format!("m{}", i), &[]);
        }
        let lines = drain_for_test();
        assert_eq!(lines.len(), QUEUE_CAP, "queue is capped at 1024");
        assert!(lines[0].contains("\"message\":\"m6\""), "{}", lines[0]);
        assert!(
            lines[QUEUE_CAP - 1].contains("\"message\":\"m1029\""),
            "{}",
            lines[QUEUE_CAP - 1]
        );
        assert_eq!(dropped_for_test() - dropped_before, 6, "oldest six dropped");
    }

    #[test]
    fn disabled_settings_close_the_log_gate() {
        // The record path starts with the gate: a disabled Settings closes
        // it, so every helper is a no-op before anything is recorded. (The
        // live disabled pass-through runs in tests/crash_disabled.rs's own
        // process — global settings are first-configure-wins here.)
        let mut settings = crate::Settings {
            endpoint: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            service_name: "svc".to_string(),
            encryption_key: String::new(),
            sample_ratio: 1.0,
            buffer_size: 100,
            disabled: true,
        };
        assert!(shipping_target_for(&settings, "").is_none(), "gate closed");
        settings.disabled = false;
        assert!(shipping_target_for(&settings, "").is_some(), "gate open");
    }

    #[test]
    fn flush_logs_posts_batch_with_header_and_trace_ids() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        freeze_and_clear();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("addr");
        let _target = TestTarget::at(&addr.to_string());

        let mut trace_id = String::new();
        let mut span_id = String::new();
        crate::trace("log-span", |span| {
            trace_id = span.trace_id();
            span_id = span.span_id();
            info_with("order shipped", &[("order_id", "o-77".to_string())]);
        });
        log("WARN", "stock low", &[]);
        flush_logs();

        let service = format!("\"service_name\":\"{}\"", crate::service_name());
        let deadline = Instant::now() + Duration::from_secs(10);
        let (head, body) = serve_logs_until(&listener, 2, deadline);

        assert!(head.starts_with("POST /api/v1/logs HTTP/1.1"), "{}", head);
        assert!(head.contains("X-Api-Key: logs-test-key"), "{}", head);
        assert!(body.starts_with("{\"logs\":["), "{}", body);
        assert_eq!(
            body.match_indices("\"timestamp\":").count(),
            2,
            "one batch with both lines: {}",
            body
        );
        assert!(body.contains("\"level\":\"info\""), "{}", body);
        assert!(body.contains("\"message\":\"order shipped\""), "{}", body);
        assert!(body.contains("\"fields\":{\"order_id\":\"o-77\"}"), "{}", body);
        assert!(
            body.contains(&format!("\"trace_id\":\"{}\"", trace_id)),
            "{}",
            body
        );
        assert!(
            body.contains(&format!("\"span_id\":\"{}\"", span_id)),
            "{}",
            body
        );
        assert!(body.contains(&service), "{}", body);
        assert!(body.contains("\"level\":\"warn\""), "{}", body);
        assert!(body.contains("\"message\":\"stock low\""), "{}", body);
        assert!(body.contains("\"trace_id\":\"\",\"span_id\":\"\""), "{}", body);
    }

    #[test]
    fn background_flusher_ships_past_threshold() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        TEST_PAUSE.store(false, Ordering::SeqCst);
        drain_for_test();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("addr");
        let _target = TestTarget::at(&addr.to_string());

        for i in 0..60 {
            log("debug", &format!("bg-{}", i), &[]);
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        let (head, body) = serve_logs_until(&listener, 60, deadline);

        assert!(head.starts_with("POST /api/v1/logs HTTP/1.1"), "{}", head);
        assert!(head.contains("X-Api-Key: logs-test-key"), "{}", head);
        assert!(body.contains("\"message\":\"bg-0\""), "{}", body);
        assert!(body.contains("\"message\":\"bg-59\""), "{}", body);
        assert!(body.contains("\"level\":\"debug\""), "{}", body);

        // Teardown: freeze the flusher and clear any remainder so later
        // tests start from an empty queue.
        TEST_PAUSE.store(true, Ordering::SeqCst);
        drain_for_test();
    }
}
