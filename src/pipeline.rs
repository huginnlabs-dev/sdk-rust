//! Delivery path: spans land in a replay buffer; a background sender posts
//! batches to the REST ingest endpoint and trims the buffer up to the
//! acked `last_seq`. Failed batches stay buffered and are retried.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::json;

struct Shared {
    buffer: Mutex<Buffer>,
    wake: Condvar,
}

struct Buffer {
    events: VecDeque<String>,
    base: i64, // seq of events[0]
}

static SHARED: OnceLock<Shared> = OnceLock::new();
static SEQ: AtomicI64 = AtomicI64::new(0);

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| Shared {
        buffer: Mutex::new(Buffer { events: VecDeque::new(), base: 1 }),
        wake: Condvar::new(),
    })
}

pub fn next_seq() -> i64 { SEQ.fetch_add(1, Ordering::SeqCst) + 1 }

pub fn enqueue(event_json: String) {
    let sh = shared();
    let mut buf = sh.buffer.lock().unwrap();
    buf.events.push_back(event_json);
    let cap = crate::settings().buffer_size;
    while buf.events.len() > cap {
        buf.events.pop_front();
        buf.base += 1;
    }
    sh.wake.notify_one();
}

/// Test hook: snapshot of the buffered event JSON lines (letting guard
/// tests assert emission without a live endpoint).
#[cfg(test)]
pub(crate) fn buffered_events() -> Vec<String> {
    shared().buffer.lock().unwrap().events.iter().cloned().collect()
}

fn trim_acked(acked: i64) {
    let sh = shared();
    let mut buf = sh.buffer.lock().unwrap();
    let mut drop = acked + 1 - buf.base;
    if drop < 0 { drop = 0; }
    let drop = (drop as usize).min(buf.events.len());
    for _ in 0..drop {
        buf.events.pop_front();
    }
    buf.base += drop as i64;
}

pub fn start() {
    eprintln!("dataflow: sender starting");
    if crate::settings().encryption_key.is_empty() {
        eprintln!("dataflow: warning: no encryption key set; captured payloads are sent as plaintext");
    }
    // Report the service manifest (language + platform profile) once;
    // best-effort, independent of the tracing pipeline.
    send_manifest();
    std::thread::Builder::new()
        .name("dataflow-sender".into())
        .spawn(sender_loop)
        .expect("dataflow: spawn sender");
}

fn sender_loop() {
    let endpoint = crate::settings().endpoint.clone();
    let api_key = crate::settings().api_key.clone();
    let mut backoff_ms: u64 = 500;
    loop {
        // Wait for work (or the flush tick).
        {
            let sh = shared();
            let buf = sh.buffer.lock().unwrap();
            if buf.events.is_empty() {
                let (guard, timeout) = sh
                    .wake
                    .wait_timeout(buf, Duration::from_millis(300))
                    .unwrap();
                if guard.events.is_empty() && !timeout.timed_out() {
                    drop(guard);
                    continue;
                }
            }
        }

        match flush(&endpoint, &api_key) {
            Ok(_) => backoff_ms = 500,
            Err(e) => {
                eprintln!("dataflow: send failed, retrying: {}", e);
                std::thread::sleep(Duration::from_millis(backoff_ms.min(10_000)));
                backoff_ms = backoff_ms.saturating_mul(2);
            }
        }
    }
}

fn flush(endpoint: &str, api_key: &str) -> Result<(), String> {
    // Snapshot under the lock; network I/O happens outside it.
    let batch: Vec<String> = {
        let sh = shared();
        let buf = sh.buffer.lock().unwrap();
        buf.events.iter().take(500).cloned().collect()
    };
    if batch.is_empty() {
        return Ok(());
    }

    let body = format!("{{\"events\":[{}]}}", batch.join(","));
    let response = http_post(endpoint, "/api/v1/ingest", api_key, body.as_bytes(), HTTP_TIMEOUT)?;
    if !response.status.is_empty() && response.status.contains(" 200") {
        let acked = json::extract_last_seq(&response.body);
        if acked > 0 {
            trim_acked(acked);
        }
        Ok(())
    } else {
        Err(format!("ingest status {}", response.status))
    }
}

pub(crate) struct Response {
    pub(crate) status: String,
    pub(crate) body: String,
}

/// Ingest batch timeout (the SDK's long-standing HTTP timeout).
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Manifest reporting budget: one short attempt, never gates tracing.
const MANIFEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Reports the service manifest once per process (`POST /api/v1/manifest`).
/// Best-effort: own thread, short timeout, every failure silent — startup
/// and the tracing sender are never delayed or blocked. Needs an API key
/// and a derivable HTTP base (DATAFLOW_HTTP_URL or a URL-form endpoint).
pub fn send_manifest() {
    let s = crate::settings();
    if s.api_key.is_empty() {
        return;
    }
    let http_url = std::env::var("DATAFLOW_HTTP_URL").unwrap_or_default();
    let base = match crate::http_base(&s.endpoint, &http_url) {
        Some(b) => b,
        None => return,
    };
    let api_key = s.api_key.clone();
    let _ = std::thread::Builder::new()
        .name("dataflow-manifest".into())
        .spawn(move || {
            let body = crate::build_manifest(&crate::service_name(), crate::SDK_VERSION);
            // Any failure (connect refused, non-200, TLS-terminated base the
            // plaintext client cannot speak) is silently ignored.
            let _ = http_post(&base, "/api/v1/manifest", &api_key, body.as_bytes(), MANIFEST_TIMEOUT);
        });
}

/// Minimal HTTP/1.1 POST over a plain TCP stream — no dependencies. For
/// WAN deployments put a TLS-terminating proxy in front of the endpoint.
/// Crate-internal: shared by the ingest sender, the manifest reporter and
/// the `dataflow-scan` catalog post.
pub(crate) fn http_post(
    endpoint: &str,
    path: &str,
    api_key: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<Response, String> {
    let authority = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string();
    let mut parts = authority.split(':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(80);

    let mut stream = TcpStream::connect((host.as_str(), port))
        .map_err(|e| format!("connect {}: {}", authority, e))?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|_| stream.set_write_timeout(Some(timeout)))
        .map_err(|e| e.to_string())?;

    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nX-Api-Key: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        path, authority, api_key, body.len()
    );
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .map(|(h, b)| (h.to_string(), b.to_string()))
        .unwrap_or((text.to_string(), String::new()));
    let status = head.lines().next().unwrap_or("").to_string();
    Ok(Response { status, body })
}
