# dataflow-rs — HuginnLabs Dataflow SDK for Rust

Runtime tracing for Rust services: RAII spans (mirroring the C++ SDK), a
thread-local trace context, and a background sender shipping E2E-encrypted
batches to the Dataflow ingest API (`POST /api/v1/ingest`). No async
runtime, no HTTP-client dependency — delivery is a minimal blocking POST,
and payload values are sealed with AES-256-GCM before they leave the
process.

## Quick start

```rust
dataflow_rs::configure(); // reads DATAFLOW_* env, starts the sender once

dataflow_rs::trace("dispatch.Route", |span| {
    span.data("city", "Riga");
}); // span ends and is queued here
```

Entry-point spans join an incoming trace via
`start_server_span_inherited(route, incoming_trace_id)` — feed it the
request's `X-Dataflow-Trace-Id` header.

## Transport spans — outgoing HTTP and database calls

`http_span` / `db_span` (SDK 0.3.0) turn outgoing HTTP calls and SQL
execution into `HTTP_CLIENT` / `DB_QUERY` spans — the callee side of the
service graph. The SDK stays transport-agnostic: **no reqwest, no SQLx** —
you call two helpers around your existing client calls and the guard
measures the call by scope (RAII: the event is queued when the guard
drops; `Drop` never panics).

When the SDK is disabled (no `DATAFLOW_*` configuration) both guards are
no-ops and `trace_id()` returns an empty string — skip the propagation
header in that case.

### Outgoing HTTP (reqwest sketch)

```rust,ignore
use dataflow_rs::http_span;

async fn call_orders(base: &str, path: &str) -> Result<String, reqwest::Error> {
    let span = http_span("GET", &format!("{}{}", base, path)); // "GET api.example.com/v1/orders"
    let resp = reqwest::Client::new()
        .get(format!("{}{}", base, path))
        .header("X-Dataflow-Trace-Id", span.trace_id()) // propagation header
        .send()
        .await?;
    span.set_status(resp.status().as_u16());
    let body = resp.text().await;
    if body.is_err() {
        span.record_error("read body failed");
    }
    drop(span); // explicit end; implicit on scope exit
    body
}
```

Emits: type `HTTP_CLIENT`, name `METHOD host/path` (host keeps the port,
userinfo is stripped), `callee_package` = host, `status_code` = HTTP
status, metadata `http.method` and `http.url` (the full target URL).

### Database queries (SQLx sketch)

```rust,ignore
use dataflow_rs::db_span;

async fn load_items(pool: &sqlx::PgPool, id: i64) -> Result<Vec<Item>, sqlx::Error> {
    let sql = "SELECT id, total FROM public.items WHERE id = $1";
    let span = db_span("postgres", sql); // name "SELECT items", callee_package "postgres"
    let rows = sqlx::query_as::<_, Item>(sql)
        .bind(id)
        .fetch_all(pool)
        .await;
    match &rows {
        Ok(_) => span.set_status(200),
        Err(e) => span.record_error(&e.to_string()),
    }
    drop(span);
    rows
}
```

Emits: type `DB_QUERY`, name `<VERB> <table>` derived from the statement —
verb is the upcased first keyword, the table comes from the first
`FROM | INTO | UPDATE | TABLE | JOIN` (an `IF [NOT] EXISTS` clause is
skipped and schema qualifiers reduce to the bare table, so
`public.items` reports `items`), `callee_package` = system (`postgres`,
`mysql`, `sqlite`, `redis`, `mongo`), metadata `db.system` and
`db.statement` (single-spaced, clipped to 200 chars). **Bind values are
never captured** — only the statement text.

## Configuration

| Variable | Meaning |
| --- | --- |
| `DATAFLOW_ENDPOINT` | `http://host:port` ingest base (plaintext HTTP; front with a TLS-terminating proxy for WAN) |
| `DATAFLOW_API_KEY` | ingest API key |
| `DATAFLOW_SERVICE_NAME` | service name stamped on spans |
| `DATAFLOW_ENCRYPTION_KEY` | PBKDF2 source for the AES-256-GCM payload key |
| `DATAFLOW_SAMPLE_RATIO` | 0.0–1.0 span sampling (default 1.0) |
| `DATAFLOW_BUFFER_SIZE` | replay buffer cap (default 10000) |
| `DATAFLOW_DISABLED` | `true` disables collection |
| `DATAFLOW_HTTP_URL` | HTTP API base for manifest reporting when the endpoint is a bare gRPC host:port |
| `DATAFLOW_APP_VERSION` | application version for the manifest |

## Testing

```sh
cargo test
```
