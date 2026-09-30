//! Crash capture: panic recording with stack traces.
//!
//! [`capture_panic`] wraps a closure with [`std::panic::catch_unwind`]:
//! when the closure panics, the crash is recorded on the thread's current
//! span (a synthetic `"panic"` span, ended immediately, when there is
//! none) and the panic is **resumed** — recording never swallows it, the
//! original payload travels unchanged to the framework's own catcher or
//! the caller's `catch_unwind`. [`capture_uncaught`] installs a
//! process-wide hook for threads you do not spawn yourself; it records a
//! synthetic `"uncaught panic"` span and then chains to the previously
//! installed hook. [`ignore_uncaught`] restores it.
//!
//! Wire contract: the crashing span carries `status_code` 500,
//! `error_message` = the formatted panic payload (clipped to 500 chars,
//! top kept) and metadata `error.stack` = the rendered
//! [`std::backtrace::Backtrace`] clipped to its first 8192 bytes (UTF-8
//! boundary safe, top kept).
//!
//! All recording is best-effort and gated on [`crate::enabled`]; every
//! recording step is itself wrapped in `catch_unwind` and contains no
//! unwraps, so a panic while handling a panic can never escalate — the
//! in-flight panic always resumes.
//!
//! # Examples
//!
//! ```
//! // Success passes straight through — nothing recorded.
//! let value = dataflow_rs::capture_panic(|| 21 * 2);
//! assert_eq!(value, 42);
//! ```
//!
//! ```
//! // A panic records on the active span, then keeps unwinding:
//! let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
//!     dataflow_rs::capture_panic(|| -> u8 { std::panic::panic_any("boom".to_string()) })
//! }));
//! assert!(resumed.is_err()); // the original payload is intact
//! ```
//!
//! ```no_run
//! // Threads you do not spawn yourself (framework workers, thread
//! // pools): install the process-wide hook once at startup; it chains to
//! // the previously installed hook.
//! dataflow_rs::capture_uncaught();
//! ```

use std::any::Any;
use std::panic::{AssertUnwindSafe, PanicHookInfo, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::Span;

/// Upper bound for the formatted panic message (chars, UTF-8 safe).
const MAX_MESSAGE_CHARS: usize = 500;
/// Upper bound for the `error.stack` attribute (bytes, top kept).
const MAX_STACK_BYTES: usize = 8192;

/// The previously installed panic hook, restored by [`ignore_uncaught`].
/// Both lifetimes are higher-ranked, matching `std::panic::set_hook`'s
/// expected `Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send>`.
type Hook = Box<dyn for<'a, 'b> Fn(&'a PanicHookInfo<'b>) + Send + Sync>;

static HOOK_INSTALLED: AtomicBool = AtomicBool::new(false);
static PREV_HOOK: Mutex<Option<Hook>> = Mutex::new(None);

/// Runs `f`, recording any panic on the current span before resuming it.
///
/// On success the value is returned unchanged and nothing is recorded.
/// On panic the crashing span gets `status_code` 500, `error_message`
/// (formatted payload, ≤500 chars) and metadata `error.stack`
/// (backtrace, ≤8192 bytes, top kept) — then the panic resumes with the
/// original payload, so crash handling stays with the framework. When the
/// thread has no current span, a synthetic `"panic"` span is created and
/// ended immediately. A no-op recording path when the SDK is disabled.
pub fn capture_panic<R>(f: impl FnOnce() -> R + UnwindSafe) -> R {
    match std::panic::catch_unwind(f) {
        Ok(value) => value,
        Err(payload) => {
            // Disabled-state check first: no message formatting, no
            // backtrace capture, no spans when the SDK is off.
            if crate::enabled() {
                let message = format_payload(payload.as_ref());
                let stack = capture_stack();
                record_panic(&message, &stack);
            }
            // The original payload travels untouched: callers and
            // frameworks catch exactly what was thrown.
            std::panic::resume_unwind(payload);
        }
    }
}

/// Installs the process-wide uncaught-panic hook (idempotent).
///
/// On every panic a synthetic `uncaught panic` span is recorded (message
/// plus panic location, and the captured backtrace) and the previously
/// installed hook is invoked with the same info, so existing reporting
/// keeps working. Repeated calls are no-ops — the stored previous hook is
/// never overwritten with our own. With the SDK disabled the hook only
/// chains. Pair with [`ignore_uncaught`] to restore the previous hook.
pub fn capture_uncaught() {
    // Idempotent install: a second call must not swap the stored previous
    // hook for our own (that would break the chain).
    if HOOK_INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    let previous = std::panic::take_hook();
    {
        let mut slot = PREV_HOOK.lock().unwrap_or_else(|p| p.into_inner());
        *slot = Some(previous);
    }
    std::panic::set_hook(Box::new(|info: &PanicHookInfo| uncaught_hook(info)));
}

/// Removes the hook installed by [`capture_uncaught`] and restores the
/// previously installed one. No-op when nothing is installed.
pub fn ignore_uncaught() {
    if !HOOK_INSTALLED.swap(false, Ordering::SeqCst) {
        return;
    }
    let mut slot = PREV_HOOK.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(previous) = slot.take() {
        std::panic::set_hook(previous);
    }
    // A missing previous hook cannot happen (`take_hook` always yields
    // one); if it ever did, our hook simply stays installed until the
    // next `capture_uncaught` re-install.
}

/// The installed hook body: record (when enabled), then chain.
fn uncaught_hook(info: &PanicHookInfo<'_>) {
    if crate::enabled() {
        let message = hook_message(info);
        let stack = capture_stack();
        record_synthetic("uncaught panic", &message, &stack);
    }
    call_previous(info);
}

/// Hook-path message: formatted payload plus the panic location — an
/// uncaught panic has no span context to carry the origin, so it travels
/// in the message. Still clipped (payload first, top kept).
fn hook_message(info: &PanicHookInfo<'_>) -> String {
    match info.location() {
        Some(loc) => clip_message(&format!("{} ({})", format_payload(info.payload()), loc)),
        None => format_payload(info.payload()),
    }
}

/// Invokes the stored previous hook. Foreign code — a panicking hook must
/// not turn the in-flight panic into a double panic (process abort), so
/// the call is swallowed on panic. The lock is held across the call;
/// hooks re-entering `capture_uncaught` are fine (AtomicBool no-op).
fn call_previous(info: &PanicHookInfo<'_>) {
    let slot = PREV_HOOK.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(previous) = slot.as_ref() {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| previous(info)));
    }
}

/// Records a panic on the thread's current span; without one, a synthetic
/// `"panic"` span is created and ended immediately.
fn record_panic(message: &str, stack: &str) {
    if !crate::enabled() {
        return;
    }
    // Recording must never panic while handling one: the whole step is
    // wrapped and a panicking step is dropped (best-effort by contract).
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        match crate::current() {
            // The current span is owned by its Trace scope — attach the
            // error but do not end it (the scope measures the duration).
            Some(span) => attach_error(&span, message, stack),
            None => record_synthetic("panic", message, stack),
        }
    }));
}

/// Records a panic on a synthetic span created and ended immediately,
/// parented to the thread's current span when one exists.
fn record_synthetic(name: &str, message: &str, stack: &str) {
    if !crate::enabled() {
        return;
    }
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let span = Span::new(name, "FUNCTION_CALL", crate::current().as_ref());
        attach_error(&span, message, stack);
        span.end();
    }));
}

/// Stamps the wire contract on a span: `record_error` (message, status
/// 500) plus the `error.stack` backtrace attribute.
fn attach_error(span: &Span, message: &str, stack: &str) {
    span.record_error(message);
    if !stack.is_empty() {
        span.attr("error.stack", stack);
    }
}

/// Formats a panic payload for `error_message`: `&str` and `String` are
/// used verbatim, anything else falls back to the payload's `Any` debug
/// form (the only safely printable representation of an opaque payload);
/// the result is clipped to [`MAX_MESSAGE_CHARS`], keeping the top.
fn format_payload(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        clip_message(s)
    } else if let Some(s) = payload.downcast_ref::<String>() {
        clip_message(s)
    } else {
        clip_message(&format!("{:?}", payload))
    }
}

/// Clips to [`MAX_MESSAGE_CHARS`] chars (UTF-8 safe, top kept) — the same
/// char-counted clipping style as the transport `clip_statement`.
fn clip_message(s: &str) -> String {
    if s.chars().count() <= MAX_MESSAGE_CHARS {
        return s.to_string();
    }
    s.chars().take(MAX_MESSAGE_CHARS).collect()
}

/// Captures the backtrace for `error.stack`. `std::backtrace::Backtrace`
/// is stable since Rust 1.65 and the crate already requires newer std
/// (`OnceLock` needs 1.70) with no `rust-version` floor declared below
/// that — the real backtrace path compiles unconditionally, so there is
/// no "backtrace unavailable" fallback branch.
fn capture_stack() -> String {
    // force_capture: a backtrace even when RUST_BACKTRACE is unset.
    clip_stack(&std::backtrace::Backtrace::force_capture().to_string())
}

/// Clips the rendered backtrace to its first [`MAX_STACK_BYTES`] bytes —
/// the top frames are the crash site, so the top is kept — cutting on a
/// UTF-8 boundary.
fn clip_stack(s: &str) -> String {
    if s.len() <= MAX_STACK_BYTES {
        return s.to_string();
    }
    let mut end = MAX_STACK_BYTES;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tests::{ensure_configured, EMIT_LOCK};
    use std::sync::Arc;

    /// Asserts that `ev` carries a non-empty `error.stack` attribute.
    fn assert_stack_present(ev: &str) {
        const NEEDLE: &str = "\"error.stack\":\"";
        let at = ev
            .find(NEEDLE)
            .unwrap_or_else(|| panic!("missing error.stack in {}", ev));
        assert!(
            !ev[at + NEEDLE.len()..].starts_with('"'),
            "error.stack must be non-empty: {}",
            ev
        );
    }

    #[test]
    fn payload_formatting_variants() {
        let s: Box<dyn Any + Send> = Box::new("literal &str panic");
        assert_eq!(format_payload(&*s), "literal &str panic");
        let owned: Box<dyn Any + Send> = Box::new(format!("owned {}", 7));
        assert_eq!(format_payload(&*owned), "owned 7");
        // Opaque payload: the dyn Any debug form is the fallback.
        let other: Box<dyn Any + Send> = Box::new(1234_i32);
        assert_eq!(format_payload(&*other), "Any { .. }");
    }

    #[test]
    fn clip_message_caps_at_500_chars() {
        let long = "x".repeat(3000);
        assert_eq!(clip_message(&long).chars().count(), MAX_MESSAGE_CHARS);
        assert!(clip_message(&long).starts_with(&"x".repeat(50)), "top kept");
        // Clipping counts chars, never splits codepoints.
        let multi = "ü".repeat(600);
        let clipped = clip_message(&multi);
        assert_eq!(clipped.chars().count(), MAX_MESSAGE_CHARS);
        assert!(clipped.is_char_boundary(clipped.len()));
        assert_eq!(clip_message("short"), "short");
    }

    #[test]
    fn clip_stack_keeps_top_8192_bytes() {
        let short = "stack\nframes";
        assert_eq!(clip_stack(short), short);
        // 8191 ASCII bytes + a two-byte codepoint crossing the cap: the
        // cut lands on the last whole codepoint under 8192 bytes.
        let s = format!("{}ü{}", "a".repeat(8191), "b".repeat(100));
        let clipped = clip_stack(&s);
        assert!(clipped.len() <= MAX_STACK_BYTES, "{} bytes", clipped.len());
        assert!(clipped.is_char_boundary(clipped.len()));
        assert!(s.starts_with(&clipped), "top must be kept");
    }

    #[test]
    fn capture_panic_returns_value_without_events() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        assert_eq!(capture_panic(|| 21 * 2), 42);
        assert_eq!(
            crate::pipeline::buffered_events().len(),
            before,
            "success must not record events"
        );
    }

    /// The panic payload as text. Formatted panics carry `&str` payloads
    /// on current toolchains (older ones carried `String`), while
    /// `panic_any` keeps the type it was given — accept both.
    fn payload_text(payload: &(dyn Any + Send)) -> Option<String> {
        payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
    }

    #[test]
    fn capture_panic_records_on_current_span_and_resumes() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        crate::trace("handler", |_span| {
            let resumed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                capture_panic(|| -> i32 { panic!("boom {}", 42) })
            }));
            let payload = resumed.err().expect("capture_panic must resume unwinding");
            assert_eq!(
                payload_text(&*payload).as_deref(),
                Some("boom 42"),
                "the original payload must travel unchanged"
            );
        });
        // The error was attached to the current span; its Trace scope ends
        // it — exactly one event carrying the crash.
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "the crashing span is recorded once");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"name\":\"handler\""), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(ev.contains("\"error_message\":\"boom 42\""), "{}", ev);
        assert_stack_present(ev);
    }

    #[test]
    fn capture_panic_synthetic_span_without_current() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let handle = std::thread::spawn(|| {
            // A fresh thread has no current span: a synthetic "panic" span
            // is created, recorded and ended immediately.
            let resumed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                capture_panic(|| -> () { std::panic::panic_any("thread boom".to_string()) })
            }));
            assert!(resumed.is_err());
        });
        handle.join().unwrap();
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"name\":\"panic\""), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(ev.contains("\"error_message\":\"thread boom\""), "{}", ev);
        assert_stack_present(ev);
    }

    #[test]
    fn capture_uncaught_chains_and_records() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();

        // Always restore the previous hook, even on a failing assertion.
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                crate::ignore_uncaught();
            }
        }
        let _restore = Restore;

        // The "previous" hook: a recording one, so chaining is observable.
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recording = Arc::clone(&seen);
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo| {
            let msg = info
                .payload()
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| info.payload().downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            recording.lock().unwrap().push(msg);
        }));

        let before = crate::pipeline::buffered_events().len();
        crate::capture_uncaught();
        crate::capture_uncaught(); // idempotent: must not chain ours into itself

        let handle = std::thread::spawn(|| {
            let resumed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                std::panic::panic_any("uncaught boom".to_string())
            }));
            assert!(resumed.is_err());
        });
        handle.join().unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["uncaught boom".to_string()],
            "the previous hook must see the panic exactly once (idempotent install)"
        );
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "one synthetic uncaught-panic span");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"name\":\"uncaught panic\""), "{}", ev);
        assert!(ev.contains("\"error_message\":\"uncaught boom"), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert_stack_present(ev);

        // After ignore_uncaught the previous hook is active again (it
        // still records into `seen`) but our synthetic span is gone.
        crate::ignore_uncaught();
        let after = crate::pipeline::buffered_events().len();
        let handle = std::thread::spawn(|| {
            let resumed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                std::panic::panic_any("after restore".to_string())
            }));
            assert!(resumed.is_err());
        });
        handle.join().unwrap();
        assert_eq!(
            crate::pipeline::buffered_events().len(),
            after,
            "no uncaught-panic events after ignore_uncaught"
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "the restored previous hook still runs"
        );
    }

    #[test]
    fn ignore_uncaught_without_install_is_noop() {
        // Must not panic and must not touch the ambient hook chain.
        crate::ignore_uncaught();
    }
}
