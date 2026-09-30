//! Transport spans: RAII guards for outgoing HTTP calls and database
//! queries — the client-side counterpart of [`crate::start_server_span`].
//!
//! The SDK itself stays transport-agnostic (no reqwest, no SQLx): the
//! caller opens a guard around any client call and the guard ends the
//! span — measuring the duration — when it drops. Emission goes through
//! the regular pipeline; the guards are no-ops unless the SDK is enabled
//! (mirroring the Go SDK's `Enabled()` gate) and `Drop` never panics.
//!
//! ```no_run
//! let span = dataflow_rs::http_span("GET", "https://api.example.com/v1/orders");
//! // Attach the propagation header to the outgoing request:
//! let header = format!("X-Dataflow-Trace-Id: {}", span.trace_id());
//! // ... perform the request with any HTTP client ...
//! span.set_status(200);
//! // the HTTP_CLIENT event is queued when the guard drops
//! ```
//!
//! Wire contract: an HTTP guard emits type `HTTP_CLIENT`, name
//! `METHOD host/path`, `callee_package` = host (port kept, userinfo
//! stripped) and metadata `http.method` / `http.url`; a DB guard emits
//! type `DB_QUERY`, name `<VERB> <table>` (see [`stmt_summary`]),
//! `callee_package` = system (`postgres`, `mysql`, `sqlite`, `redis`,
//! `mongo`) and metadata `db.system` / `db.statement`. Bind values are
//! never captured — only the statement text, clipped to 200 chars.

use crate::Span;

/// Upper bound for the `db.statement` attribute (chars, not bytes — UTF-8
/// safe clipping, never mid-codepoint).
const MAX_STATEMENT_CHARS: usize = 200;

/// Opens an `HTTP_CLIENT` span for one outgoing HTTP call. The guard
/// records the duration and queues the event when it drops; attach the
/// propagation header `X-Dataflow-Trace-Id: <trace_id()>` to the request
/// so the callee service joins the trace. No-op when the SDK is disabled
/// ([`trace_id`] then returns an empty string — skip the header too).
///
/// [`trace_id`]: HttpSpan::trace_id
///
/// ```
/// let span = dataflow_rs::http_span("GET", "https://api.example.com/v1/orders");
/// span.set_status(200);
/// // drops at the end of scope -> event queued
/// ```
pub fn http_span(method: &str, url: &str) -> HttpSpan {
    if !crate::enabled() {
        return HttpSpan { span: None };
    }
    let (host, path) = split_url(url);
    let method = method.trim().to_ascii_uppercase();
    let name = format!("{} {}{}", method, host, path);
    let span = Span::new(&name, "HTTP_CLIENT", crate::current().as_ref());
    span.callee(&host);
    span.attr("http.method", &method);
    span.attr("http.url", url);
    HttpSpan { span: Some(span) }
}

/// Opens a `DB_QUERY` span for one database call against `system`
/// (`postgres`, `mysql`, `sqlite`, `redis`, `mongo`). Same RAII pattern as
/// [`http_span`]; the span name comes from [`stmt_summary`] and the
/// statement attribute from [`clip_statement`] — bind values are never
/// captured.
///
/// ```
/// let span = dataflow_rs::db_span("postgres", "SELECT id FROM public.items WHERE id = $1");
/// span.set_status(200);
/// // drops at the end of scope -> event queued
/// ```
pub fn db_span(system: &str, statement: &str) -> DbSpan {
    if !crate::enabled() {
        return DbSpan { span: None };
    }
    let name = stmt_summary(statement);
    let span = Span::new(&name, "DB_QUERY", crate::current().as_ref());
    span.callee(system);
    span.attr("db.system", system);
    let clipped = clip_statement(statement);
    if !clipped.is_empty() {
        span.attr("db.statement", &clipped);
    }
    DbSpan { span: Some(span) }
}

/// RAII guard for one outgoing HTTP call (`HTTP_CLIENT` span). Ends the
/// span on drop; every method is a no-op when the SDK is disabled.
pub struct HttpSpan {
    span: Option<Span>,
}

impl HttpSpan {
    /// Records the HTTP response status (wire `status_code`).
    pub fn set_status(&self, code: u16) -> &Self {
        if let Some(s) = &self.span {
            s.status(code as i32);
        }
        self
    }

    /// Records a transport/client failure; also marks the span failed
    /// (status 500), matching [`Span::record_error`].
    pub fn record_error(&self, message: &str) -> &Self {
        if let Some(s) = &self.span {
            s.record_error(message);
        }
        self
    }

    /// Trace id for the `X-Dataflow-Trace-Id` propagation header. Empty
    /// when the SDK is disabled (skip the header as well).
    pub fn trace_id(&self) -> String {
        self.span.as_ref().map(|s| s.trace_id()).unwrap_or_default()
    }
}

impl Drop for HttpSpan {
    fn drop(&mut self) {
        // Span::end is idempotent and never panics on the disabled path.
        if let Some(s) = &self.span {
            s.end();
        }
    }
}

/// RAII guard for one database call (`DB_QUERY` span). Ends the span on
/// drop; every method is a no-op when the SDK is disabled.
pub struct DbSpan {
    span: Option<Span>,
}

impl DbSpan {
    /// Records the outcome status (the Go SDK stamps 200 on success and
    /// 500 on failure).
    pub fn set_status(&self, code: i32) -> &Self {
        if let Some(s) = &self.span {
            s.status(code);
        }
        self
    }

    /// Records a query failure; also marks the span failed (status 500).
    pub fn record_error(&self, message: &str) -> &Self {
        if let Some(s) = &self.span {
            s.record_error(message);
        }
        self
    }

    /// Trace id for the `X-Dataflow-Trace-Id` propagation header. Empty
    /// when the SDK is disabled.
    pub fn trace_id(&self) -> String {
        self.span.as_ref().map(|s| s.trace_id()).unwrap_or_default()
    }
}

impl Drop for DbSpan {
    fn drop(&mut self) {
        if let Some(s) = &self.span {
            s.end();
        }
    }
}

/// Short human name for a statement: the verb plus the first table
/// reference when one exists (`"SELECT orders"`, `"INSERT users"`); bare
/// verbs and non-SQL fall back to the upcased first word. Pure — safe to
/// call anywhere.
pub fn stmt_summary(sql: &str) -> String {
    let one = single_spaced(sql);
    let verb = leading_verb(&one);
    match table_ref(&one) {
        Some(table) => format!("{} {}", verb, table),
        None => verb,
    }
}

/// The statement text for the `db.statement` attribute: whitespace
/// collapsed to single spaces, clipped to [`MAX_STATEMENT_CHARS`]. The
/// statement only — bind values never travel through this SDK.
pub fn clip_statement(sql: &str) -> String {
    let one = single_spaced(sql);
    if one.chars().count() <= MAX_STATEMENT_CHARS {
        one
    } else {
        one.chars().take(MAX_STATEMENT_CHARS).collect()
    }
}

/// Collapses every whitespace run to a single space (shared by the
/// summary and the clip so both see the same normalized statement).
fn single_spaced(s: &str) -> String {
    s.split_whitespace().collect::<Vec<&str>>().join(" ")
}

/// The upcased leading verb. A leading paren (`"(SELECT …)"`) or trailing
/// punctuation (`"VACUUM;"`) does not hide it; an empty statement falls
/// back to `QUERY`. Mirrors the Go SDK, whose verb list turns out to be
/// cosmetic: the anchored regex and the fallback both yield the first
/// word upcased.
fn leading_verb(one: &str) -> String {
    let body = one.trim_start_matches(|c: char| c == '(' || c == ' ');
    let word = body.split(' ').next().unwrap_or("");
    let word = word.trim_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '_'));
    if word.is_empty() {
        return "QUERY".to_string();
    }
    word.to_ascii_uppercase()
}

/// First table reference after the first of FROM|INTO|UPDATE|TABLE|JOIN:
/// an optional `IF [NOT] EXISTS` clause is skipped and schema-qualified
/// names (`public.items`) report the bare table.
fn table_ref(one: &str) -> Option<String> {
    const KEYWORDS: [&str; 5] = ["FROM", "INTO", "UPDATE", "TABLE", "JOIN"];
    let tokens: Vec<&str> = one.split(' ').collect();
    for (i, tok) in tokens.iter().enumerate() {
        if !KEYWORDS.contains(&tok.to_ascii_uppercase().as_str()) {
            continue;
        }
        // Optional "IF [NOT] EXISTS" between the keyword and the table
        // (CREATE TABLE IF NOT EXISTS…, DROP TABLE IF EXISTS…).
        let mut j = i + 1;
        if tokens.get(j).map(|t| t.eq_ignore_ascii_case("IF")) == Some(true) {
            j += 1;
            if tokens.get(j).map(|t| t.eq_ignore_ascii_case("NOT")) == Some(true) {
                j += 1;
            }
            if tokens.get(j).map(|t| t.eq_ignore_ascii_case("EXISTS")) == Some(true) {
                j += 1;
            }
        }
        return tokens
            .get(j)
            .and_then(|t| ident_of(t))
            .map(|ident| match ident.rfind(|c: char| c == '.' || c == '$') {
                Some(dot) => ident[dot + 1..].to_string(),
                None => ident,
            });
    }
    None
}

/// The identifier at the start of `tok`: optional wrapping quote
/// (backtick, double/single quote, brackets), then word characters, dots
/// and dollar signs. Trailing punctuation (`orders;`) is fine — the scan
/// stops there; a leading `(` is not (mirrors the Go regex, so
/// `"FROM (SELECT …)"` yields no table).
fn ident_of(tok: &str) -> Option<String> {
    let stripped = tok.trim_start_matches(|c: char| matches!(c, '"' | '\'' | '`' | '[' | ']'));
    let ident: String = stripped
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.' || *c == '$')
        .collect();
    match ident.chars().next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => Some(ident),
        _ => None,
    }
}

/// Splits a call target URL into `(authority, path)` for the span name and
/// `callee_package`: scheme stripped, userinfo (`user:pass@`) dropped, the
/// authority kept verbatim including the port (mirrors the Go SDK's
/// `URL.Host`), path defaulting to `/` with query and fragment removed.
fn split_url(url: &str) -> (String, String) {
    let url = url.trim();
    let rest = match url.find("://") {
        Some(i) => &url[i + "://".len()..],
        None => url,
    };
    let split = rest
        .find(|c: char| c == '/' || c == '?' || c == '#')
        .unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(split);
    let authority = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    let path_len = tail
        .find(|c: char| c == '?' || c == '#')
        .unwrap_or(tail.len());
    let mut path = tail[..path_len].to_string();
    if path.is_empty() {
        path.push('/');
    }
    (authority.to_string(), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes the emission tests: the pipeline buffer is process-global
    /// and cargo runs unit tests on parallel threads.
    static EMIT_LOCK: Mutex<()> = Mutex::new(());

    /// Points the SDK at a port nothing listens on (connect refused, events
    /// stay buffered) so the enabled path can be exercised without network.
    fn ensure_configured() {
        std::env::set_var("DATAFLOW_ENDPOINT", "http://127.0.0.1:1");
        std::env::set_var("DATAFLOW_API_KEY", "test-key");
        std::env::set_var("DATAFLOW_SERVICE_NAME", "sdk-rust-tests");
        crate::configure();
    }

    #[test]
    fn stmt_summary_cases() {
        let cases = [
            ("SELECT id, total FROM orders WHERE id = $1", "SELECT orders"),
            ("  insert into users (email) values ($1)", "INSERT users"),
            ("UPDATE public.items SET total = total - 1", "UPDATE items"),
            ("DELETE FROM sessions WHERE expires < now()", "DELETE sessions"),
            ("CREATE TABLE IF NOT EXISTS migrations (id int)", "CREATE migrations"),
            ("DROP TABLE IF EXISTS logs", "DROP logs"),
            ("select u.id\nfrom users u\njoin orders o on o.user_id = u.id", "SELECT users"),
            ("WITH cte AS (SELECT 1) SELECT * FROM t", "WITH t"),
            ("PRAGMA journal_mode=WAL", "PRAGMA"),
            ("SELECT 1", "SELECT"),
            ("COMMIT", "COMMIT"),
            ("", "QUERY"),
            ("INSERT INTO public.orders (id) VALUES (1)", "INSERT orders"),
        ];
        for (sql, want) in cases {
            assert_eq!(stmt_summary(sql), want, "stmt_summary({:?})", sql);
        }
    }

    #[test]
    fn clip_statement_cases() {
        assert_eq!(clip_statement("SELECT  *\tFROM\n  orders"), "SELECT * FROM orders");
        assert_eq!(clip_statement("   "), "");
        // Clipping counts chars, staying on UTF-8 boundaries.
        let long = format!("SELECT '{}'", "ü".repeat(300));
        let clipped = clip_statement(&long);
        assert_eq!(clipped.chars().count(), MAX_STATEMENT_CHARS);
        assert!(clipped.is_char_boundary(clipped.len()));
    }

    #[test]
    fn split_url_cases() {
        let cases = [
            ("https://api.example.com/v1/users?limit=5", ("api.example.com", "/v1/users")),
            ("http://localhost:8080/health", ("localhost:8080", "/health")),
            ("https://api.example.com", ("api.example.com", "/")),
            ("api.example.com/v2/items", ("api.example.com", "/v2/items")),
            ("https://user:pw@api.example.com:8443/x#frag", ("api.example.com:8443", "/x")),
            ("  https://h.test  ", ("h.test", "/")),
            ("", ("", "/")),
        ];
        for (url, want) in cases {
            let (host, path) = split_url(url);
            assert_eq!((host.as_str(), path.as_str()), want, "split_url({:?})", url);
        }
    }

    #[test]
    fn http_guard_drop_emits_client_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let trace_id;
        {
            let s = crate::http_span("GET", "https://api.example.com/v1/users?limit=5");
            trace_id = s.trace_id();
            assert!(!trace_id.is_empty());
            s.set_status(200);
            std::thread::sleep(std::time::Duration::from_millis(12));
        } // drop ends the span and queues the event
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "drop must queue exactly one event");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"HTTP_CLIENT\""), "{}", ev);
        assert!(ev.contains("\"name\":\"GET api.example.com/v1/users\""), "{}", ev);
        assert!(ev.contains("\"callee_package\":\"api.example.com\""), "{}", ev);
        assert!(ev.contains("\"status_code\":200"), "{}", ev);
        assert!(ev.contains("\"http.method\":\"GET\""), "{}", ev);
        assert!(ev.contains("\"http.url\":\"https://api.example.com/v1/users?limit=5\""), "{}", ev);
        assert!(ev.contains(&format!("\"trace_id\":\"{}\"", trace_id)), "{}", ev);
        let duration = json_i64_field(ev, "duration_ms");
        assert!(duration >= 5, "duration_ms {} should record elapsed time", duration);
    }

    #[test]
    fn db_guard_drop_emits_query_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        {
            let s = crate::db_span("postgres", "SELECT id FROM public.items WHERE id = $1");
            s.record_error("connection reset");
        }
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "drop must queue exactly one event");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"DB_QUERY\""), "{}", ev);
        assert!(ev.contains("\"name\":\"SELECT items\""), "{}", ev);
        assert!(ev.contains("\"callee_package\":\"postgres\""), "{}", ev);
        assert!(ev.contains("\"error_message\":\"connection reset\""), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(ev.contains("\"db.system\":\"postgres\""), "{}", ev);
        assert!(
            ev.contains("\"db.statement\":\"SELECT id FROM public.items WHERE id = $1\""),
            "{}",
            ev
        );
    }

    #[test]
    fn db_statement_is_clipped_in_attribute() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let before = crate::pipeline::buffered_events().len();
        let sql = format!("SELECT id FROM orders WHERE note = '{}'", "x".repeat(300));
        {
            let _s = crate::db_span("mysql", &sql);
        }
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        let clipped = clip_statement(&sql);
        assert_eq!(clipped.chars().count(), MAX_STATEMENT_CHARS);
        assert!(
            ev.contains(&format!("\"db.statement\":\"{}\"", clipped)),
            "{}",
            ev
        );
        assert!(ev.contains("\"name\":\"SELECT orders\""), "{}", ev);
    }

    /// Tiny reader for one integer field of an event JSON line (test-only).
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
}
