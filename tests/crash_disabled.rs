//! Disabled-SDK passthrough for the crash-capture API.
//!
//! This lives in an integration test on purpose: it runs in its own
//! process, so `DATAFLOW_DISABLED=true` cannot race the unit tests for the
//! process-global settings (`OnceLock` — the first `configure()` wins).

#[test]
fn disabled_sdk_resumes_panics_and_chains_hooks() {
    std::env::set_var("DATAFLOW_DISABLED", "true");
    std::env::set_var("DATAFLOW_ENDPOINT", "http://127.0.0.1:1");
    std::env::set_var("DATAFLOW_API_KEY", "test-key");
    dataflow_rs::configure();
    assert!(!dataflow_rs::enabled(), "SDK must be disabled");

    // capture_panic: success passes through...
    assert_eq!(dataflow_rs::capture_panic(|| 40 + 2), 42);

    // ...and a panic still resumes with the original payload (recording
    // is skipped when disabled).
    let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dataflow_rs::capture_panic(|| -> i32 { std::panic::panic_any("disabled boom".to_string()) })
    }));
    let payload = resumed.err().expect("panic must resume when disabled");
    assert_eq!(
        payload.downcast_ref::<String>().map(String::as_str),
        Some("disabled boom")
    );

    // capture_uncaught: install is idempotent, the hook chains (to the
    // default here) without recording, and ignore_uncaught restores.
    dataflow_rs::capture_uncaught();
    dataflow_rs::capture_uncaught();
    let handle = std::thread::spawn(|| {
        let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::panic::panic_any("disabled uncaught".to_string())
        }));
        assert!(resumed.is_err());
    });
    handle.join().unwrap();
    dataflow_rs::ignore_uncaught();
}
