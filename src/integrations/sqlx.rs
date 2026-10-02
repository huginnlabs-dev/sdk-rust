//! SQLx query helpers (feature `sqlx`; sqlx 0.8, tokio runtime).
//!
//! A **pragmatic convenience, not driver instrumentation**: sqlx exposes no
//! portable query callbacks (its `ConnectOptions::log_statements` hook is
//! log-level based and carries no success/failure outcome), so instead of
//! patching the driver this module provides first-class helpers that wrap a
//! statement execution in a `DB_QUERY` span — the same shape as the core
//! [`db_span`](crate::db_span) guard: name from
//! [`stmt_summary`](crate::transport::stmt_summary) (`"SELECT orders"`), `db.system` derived from the pool's URL scheme,
//! `db.statement` clipped to 200 chars, bind values never captured.
//!
//! ```no_run
//! # async fn demo(pool: &sqlx::SqlitePool) -> sqlx::Result<()> {
//! dataflow_rs::query_span(pool, "SELECT id, total FROM orders WHERE id = 1").await?;
//! # Ok(())
//! # }
//! ```
//!
//! The helper covers plain `execute` round-trips (`INSERT`/`UPDATE`/`DDL`
//! and scalar `SELECT`s). For `query_as`/`fetch_all` flows keep using the
//! core [`db_span`](crate::db_span) guard around the call — RAII, any
//! executor. With the SDK disabled both are no-ops.

use ::sqlx::Pool;

/// Executes `sql` on `pool` inside a `DB_QUERY` span. The span records
/// status 200 on success and the driver error message + status 500 on
/// failure; the driver result is returned unchanged.
///
/// ```no_run
/// # async fn demo(pool: &sqlx::SqlitePool) -> sqlx::Result<()> {
/// dataflow_rs::query_span(pool, "INSERT INTO orders (id) VALUES (1)").await?;
/// # Ok(())
/// # }
/// ```
pub async fn query_span<DB>(
    pool: &Pool<DB>,
    sql: &str,
) -> ::sqlx::Result<<DB as ::sqlx::Database>::QueryResult>
where
    DB: ::sqlx::Database,
    for<'c> &'c mut <DB as ::sqlx::Database>::Connection: ::sqlx::Executor<'c, Database = DB>,
    for<'a> <DB as ::sqlx::Database>::Arguments<'a>: ::sqlx::IntoArguments<'a, DB>,
{
    let span = if crate::enabled() {
        Some(crate::db_span(&db_system(pool), sql))
    } else {
        None
    };
    let result = ::sqlx::query(sql).execute(pool).await;
    if let Some(s) = &span {
        match &result {
            Ok(_) => {
                s.set_status(200);
            }
            Err(e) => {
                s.record_error(&e.to_string());
            }
        }
    }
    result
}

/// The `db.system` label for a pool, taken from the pool's driver
/// (`Database::NAME`): `PostgreSQL` → `postgres`, `MySQL` → `mysql`,
/// `SQLite` → `sqlite`, `MSSQL` → `mssql`.
///
/// Deliberately **not** derived from the pool's connect URL:
/// `ConnectOptions::to_url_lossy` — the only generic URL accessor sqlx
/// offers — panics for `sqlite::memory:` pools (the driver cannot rebuild
/// a parseable URL from the `:memory:` filename), and a tracing helper
/// must never panic. The driver name carries the same information; users
/// holding a raw URL can use [`db_system_from_url`].
pub fn db_system<DB: ::sqlx::Database>(_pool: &Pool<DB>) -> String {
    db_system_from_url(&format!("{}:", DB::NAME)).to_string()
}

/// Normalizes a driver name or database URL scheme to the `db.system`
/// wire label — case-insensitive (`Database::NAME` is `"PostgreSQL"` /
/// `"MySQL"` / `"SQLite"` / `"MSSQL"`; `postgresql` folds into `postgres`
/// and `sqlite::memory:` — an opaque URL, no `://` — still yields
/// `sqlite`). Borrowed: unknown labels pass through as a slice of the
/// input's scheme.
pub fn db_system_from_url(url: &str) -> &str {
    let scheme = url.split(':').next().unwrap_or("");
    if scheme.eq_ignore_ascii_case("postgres") || scheme.eq_ignore_ascii_case("postgresql") {
        return "postgres";
    }
    if scheme.eq_ignore_ascii_case("mysql") || scheme.eq_ignore_ascii_case("mariadb") {
        return "mysql";
    }
    if scheme.eq_ignore_ascii_case("sqlite") {
        return "sqlite";
    }
    if scheme.eq_ignore_ascii_case("mssql") || scheme.eq_ignore_ascii_case("sqlserver") {
        return "mssql";
    }
    scheme
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::sqlx::sqlite::SqlitePoolOptions;
    use crate::transport::tests::{ensure_configured, EMIT_LOCK};

    fn memory_pool() -> ::sqlx::sqlite::SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_lazy("sqlite::memory:")
            .expect("sqlite options")
    }

    #[test]
    fn db_system_from_url_cases() {
        let cases = [
            ("postgres://u:p@db:5432/app", "postgres"),
            ("postgresql://db/app", "postgres"),
            ("mysql://root@localhost:3306/app", "mysql"),
            ("mariadb://db/app", "mysql"),
            ("sqlite::memory:", "sqlite"),
            ("sqlite://orders.db?mode=rw", "sqlite"),
            ("sqlite:data.db", "sqlite"),
            ("mssql://sa@srv/app", "mssql"),
            ("cass://node:9042", "cass"),
        ];
        for (url, want) in cases {
            assert_eq!(db_system_from_url(url), want, "db_system_from_url({:?})", url);
        }
    }

    #[tokio::test]
    async fn db_system_matches_the_pool_driver() {
        // Derived from the driver (Database::NAME), never from the URL —
        // sqlx's to_url_lossy panics for sqlite::memory: pools.
        let pool = memory_pool();
        assert_eq!(db_system(&pool), "sqlite");
    }

    #[tokio::test]
    async fn query_span_emits_db_query_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let pool = memory_pool();
        let before = crate::pipeline::buffered_events().len();
        let res = query_span(&pool, "CREATE TABLE items (id INTEGER PRIMARY KEY, total INTEGER)").await;
        assert!(res.is_ok(), "{res:?}");
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1, "one DB_QUERY event expected");
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"type\":\"DB_QUERY\""), "{}", ev);
        // stmt_summary reused for the name; db.system from the URL scheme.
        assert!(ev.contains("\"name\":\"CREATE items\""), "{}", ev);
        assert!(ev.contains("\"callee_package\":\"sqlite\""), "{}", ev);
        assert!(ev.contains("\"db.system\":\"sqlite\""), "{}", ev);
        assert!(
            ev.contains("\"db.statement\":\"CREATE TABLE items (id INTEGER PRIMARY KEY, total INTEGER)\""),
            "{}",
            ev
        );
        assert!(ev.contains("\"status_code\":200"), "{}", ev);
        assert!(ev.contains("\"error_message\":\"\""), "{}", ev);
    }

    #[tokio::test]
    async fn query_span_records_driver_errors() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let pool = memory_pool();
        let before = crate::pipeline::buffered_events().len();
        let res = query_span(&pool, "SELEC * FROM items").await;
        assert!(res.is_err(), "broken SQL must surface to the caller");
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 1);
        let ev = &events[events.len() - 1];
        assert!(ev.contains("\"name\":\"SELEC items\""), "{}", ev);
        assert!(ev.contains("\"status_code\":500"), "{}", ev);
        assert!(
            ev.contains("\"error_message\":\"") && !ev.contains("\"error_message\":\"\""),
            "driver error must be recorded: {}",
            ev
        );
    }

    #[test]
    fn query_span_parents_to_the_current_span() {
        let _serial = EMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        ensure_configured();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Pool creation needs a tokio context (the pool spawns a reaper);
        // each `sqlite::memory:` connection starts empty, so create the
        // table up front (max_connections(1) keeps it on the same db).
        let pool = rt.block_on(async {
            let pool = memory_pool();
            ::sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY)")
                .execute(&pool)
                .await
                .expect("create items table");
            pool
        });
        let before = crate::pipeline::buffered_events().len();
        crate::trace("repo.Load", |_s| {
            // block_on inside the (sync) trace scope: the future polls on
            // this thread, so the DB span sees the current span.
            let res = rt.block_on(query_span(&pool, "SELECT id FROM items"));
            assert!(res.is_ok(), "{res:?}");
        });
        let events = crate::pipeline::buffered_events();
        assert_eq!(events.len(), before + 2, "child DB + parent spans expected");
        let db = &events[events.len() - 2];
        let parent = &events[events.len() - 1];
        assert!(db.contains("\"type\":\"DB_QUERY\""), "{}", db);
        assert!(parent.contains("\"name\":\"repo.Load\""), "{}", parent);
        assert_eq!(
            json_str_field(db, "parent_span_id"),
            json_str_field(parent, "span_id"),
            "DB span must nest under the current span"
        );
        assert_eq!(json_str_field(db, "trace_id"), json_str_field(parent, "trace_id"));
    }

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
}
