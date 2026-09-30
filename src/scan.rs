//! Static route scanner: extracts declared HTTP endpoints from Rust
//! sources (actix-web attribute macros and axum `.route` chains) and
//! reports them to the server catalog (`POST /api/v1/catalog`). Exposed as
//! the `dataflow-scan` binary — std only, no clap, no reqwest: argument
//! parsing is hand-rolled and delivery reuses the pipeline's plaintext
//! POST helper.
//!
//! The logic lives in pure helpers (extraction over `&str` fixtures, JSON
//! body construction, argument parsing, base-URL resolution) so it is fully
//! unit-testable; `run` only wires them to the filesystem, the environment
//! and the network. Warp is deliberately not scanned — its filter chains
//! have no declarative route syntax a line scanner can anchor on.
//!
//! Wire contract: `{"service_name":"…","routes":[{"method","path",
//! "handler","source_file"}]}` — at most 1000 routes, paths rooted at `/`,
//! methods uppercased. The server deduplicates on `METHOD path` and a
//! re-scan replaces the service's route set.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Catalog POST budget (one request, same envelope as the ingest client).
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Server contract: at most 1000 routes per service.
const MAX_ROUTES: usize = 1000;

/// How many lines past an actix attribute the handler `fn` is looked up
/// (doc comments and other attributes may sit in between).
const ACTIX_FN_LOOKAHEAD: usize = 12;

/// One statically extracted route (framework marker is local-only; it is
/// not part of the wire contract).
#[derive(Clone, Debug, PartialEq)]
struct Route {
    method: String,
    path: String,
    handler: String,
    source_file: String,
    framework: &'static str,
}

/// Parsed CLI arguments.
struct Args {
    dir: String,
    service: Option<String>,
    url: Option<String>,
    api_key: Option<String>,
    print: bool,
}

const USAGE: &str = "\
dataflow-scan — extract declared HTTP routes from Rust sources and post
them to the Dataflow catalog (POST /api/v1/catalog).

USAGE:
    dataflow-scan [--dir DIR] [--service NAME] [--url BASE] [--api-key KEY] [--print]

FLAGS:
    --dir DIR       source root to scan (default \".\")
    --service NAME  service name (default: DATAFLOW_SERVICE_NAME or dir basename)
    --url BASE      HTTP API base (default: DATAFLOW_HTTP_URL, then URL-form DATAFLOW_ENDPOINT)
    --api-key KEY   API key (default: DATAFLOW_API_KEY)
    --print         print the catalog JSON to stdout instead of posting
    -h, --help      show this help

EXIT CODES: 0 success, 1 runtime failure, 2 usage error.
Frameworks: actix-web attribute macros and axum .route chains (warp is not
scanned); plaintext HTTP like the SDK transport — front the endpoint with a
TLS-terminating proxy for WAN.";

/// CLI entry point. Returns the process exit code: 0 success, 1 runtime
/// failure (bad directory, no derivable HTTP base, missing API key, server
/// error), 2 usage error.
pub fn run(argv: &[String]) -> i32 {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        println!("{}", USAGE);
        return 0;
    }
    let args = match parse_args(argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("dataflow-scan: {}", e);
            eprintln!("{}", USAGE);
            return 2;
        }
    };

    if !Path::new(&args.dir).is_dir() {
        eprintln!("dataflow-scan: not a directory: {}", args.dir);
        return 1;
    }

    let mut files = Vec::new();
    if let Err(e) = collect_files(Path::new(&args.dir), 0, &mut files) {
        eprintln!("dataflow-scan: {}", e);
        return 1;
    }

    let mut routes = Vec::new();
    for path in &files {
        let source = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("dataflow-scan: skipping {}: {}", path.display(), e);
                continue;
            }
        };
        routes.extend(extract_routes(&source, &rel_path(path, &args.dir)));
    }
    let scanned = files.len();

    let before_dedupe = routes.len();
    let routes = dedupe_routes(routes);
    let deduped = before_dedupe - routes.len();
    let (routes, dropped) = limit_routes(routes);
    if dropped > 0 {
        eprintln!(
            "dataflow-scan: warning: {} routes over the {}-route limit were dropped",
            dropped, MAX_ROUTES
        );
    }
    if routes.is_empty() {
        eprintln!(
            "dataflow-scan: warning: no routes found under {} — an empty catalog replaces any previous one",
            args.dir
        );
    }

    let body = catalog_json(&service_name(&args), &routes);
    let actix = routes.iter().filter(|r| r.framework == "actix").count();
    let axum = routes.iter().filter(|r| r.framework == "axum").count();

    if args.print {
        println!("{}", body);
        eprintln!(
            "dataflow-scan: scanned {} Rust files under {}; {} routes (actix {}, axum {}{}) — printed JSON",
            scanned,
            args.dir,
            routes.len(),
            actix,
            axum,
            if deduped > 0 {
                format!(", {} duplicates dropped", deduped)
            } else {
                String::new()
            }
        );
        return 0;
    }

    let base = match base_url(args.url.as_deref(), &env("DATAFLOW_ENDPOINT"), &env("DATAFLOW_HTTP_URL")) {
        Some(b) => b,
        None => {
            eprintln!(
                "dataflow-scan: no HTTP base URL: DATAFLOW_ENDPOINT is a bare host:port — set DATAFLOW_HTTP_URL or pass --url"
            );
            return 1;
        }
    };
    let api_key = match args.api_key.as_deref().map(str::trim).filter(|k| !k.is_empty()) {
        Some(k) => k.to_string(),
        None => match env("DATAFLOW_API_KEY") {
            k if !k.is_empty() => k,
            _ => {
                eprintln!("dataflow-scan: no API key: pass --api-key or set DATAFLOW_API_KEY");
                return 1;
            }
        },
    };

    match crate::pipeline::http_post(&base, "/api/v1/catalog", &api_key, body.as_bytes(), HTTP_TIMEOUT) {
        Ok(resp) => {
            if resp.status.contains(" 200") {
                let accepted = extract_accepted(&resp.body).unwrap_or(routes.len() as i64);
                eprintln!(
                    "dataflow-scan: scanned {} Rust files under {}; {} routes (actix {}, axum {}) — accepted {} ({}/api/v1/catalog)",
                    scanned, args.dir, routes.len(), actix, axum, accepted, base
                );
                0
            } else {
                eprintln!("dataflow-scan: catalog post failed: {}", resp.status);
                1
            }
        }
        Err(e) => {
            eprintln!("dataflow-scan: catalog post failed: {}", e);
            1
        }
    }
}

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Argument parsing (hand-rolled; supports both "--flag value" and "--flag=value")

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        dir: ".".to_string(),
        service: None,
        url: None,
        api_key: None,
        print: false,
    };
    let mut i = 0;
    while i < argv.len() {
        let raw = argv[i].as_str();
        let (flag, inline) = match raw.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f, Some(v)),
            _ => (raw, None),
        };
        match flag {
            "--dir" | "--service" | "--url" | "--api-key" => {
                let value = match inline {
                    Some(v) => v.to_string(),
                    None => {
                        i += 1;
                        match argv.get(i) {
                            Some(v) => v.clone(),
                            None => return Err(format!("missing value for {}", flag)),
                        }
                    }
                };
                match flag {
                    "--dir" => args.dir = value,
                    "--service" => args.service = Some(value),
                    "--url" => args.url = Some(value),
                    _ => args.api_key = Some(value),
                }
            }
            "--print" => {
                if inline.is_some() {
                    return Err("--print does not take a value".to_string());
                }
                args.print = true;
            }
            _ => return Err(format!("unknown argument: {}", raw)),
        }
        i += 1;
    }
    Ok(args)
}

// ---------------------------------------------------------------------------
// Base URL + service name

/// `--url` wins, then `DATAFLOW_HTTP_URL`, then a URL-form
/// `DATAFLOW_ENDPOINT` — a bare host:port endpoint has no derivable HTTP
/// base and yields None (reporting is skipped with a message). Pure over
/// the env values, so it is testable without mutating the environment.
fn base_url(cli_url: Option<&str>, endpoint_env: &str, http_url_env: &str) -> Option<String> {
    if let Some(u) = cli_url.map(str::trim).filter(|u| !u.is_empty()) {
        return Some(u.trim_end_matches('/').to_string());
    }
    crate::http_base(endpoint_env, http_url_env)
}

/// Service name: `--service`, then `DATAFLOW_SERVICE_NAME`, then the scan
/// root's basename ("." canonicalizes first; a bare fallback keeps the
/// catalog entry addressable).
fn service_name(args: &Args) -> String {
    if let Some(s) = args.service.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        return s.to_string();
    }
    let from_env = env("DATAFLOW_SERVICE_NAME");
    if !from_env.is_empty() {
        return from_env;
    }
    dir_basename(&args.dir)
}

fn dir_basename(dir: &str) -> String {
    if let Some(name) = Path::new(dir).file_name().map(|n| n.to_string_lossy().to_string()) {
        if !name.is_empty() {
            return name;
        }
    }
    std::fs::canonicalize(dir)
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .unwrap_or_else(|| "default".to_string())
}

// ---------------------------------------------------------------------------
// Source tree walk

/// Collects `*.rs` files under `root`, skipping `target/` and `.git/`
/// directories at any depth; entries are sorted for deterministic output.
fn collect_files(root: &Path, depth: usize, out: &mut Vec<PathBuf>) -> Result<(), String> {
    if depth > 64 {
        return Ok(()); // pathological tree — stop descending
    }
    let mut entries: Vec<PathBuf> = std::fs::read_dir(root)
        .map_err(|e| format!("cannot read directory {}: {}", root.display(), e))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        if path.is_dir() {
            if name == "target" || name == ".git" {
                continue;
            }
            collect_files(&path, depth + 1, out)?;
        } else if name.ends_with(".rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Repo-relative path of `path` below `root`, always with `/` separators.
fn rel_path(path: &Path, root: &str) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

// ---------------------------------------------------------------------------
// Extraction (regex-free line scanning)

/// Extracts routes from one Rust source. Two frameworks:
///
/// - actix-web attribute macros `#[get("/path")]` (also post/put/delete/
///   patch) — the handler is the first `fn` name that follows the
///   attribute;
/// - axum `.route("/path", get(handler))` chains — the method token right
///   after the path literal, handler inside its parens.
///
/// Path parameters keep their source syntax (`{id}`, `:id`) verbatim.
/// Occurrences behind a `//` line comment and non-rooted paths are
/// skipped.
fn extract_routes(source: &str, source_file: &str) -> Vec<Route> {
    let lines: Vec<&str> = source.lines().collect();
    let mut routes = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        for (pos, _) in line.match_indices(".route(") {
            if commented_before(line, pos) {
                continue;
            }
            if let Some(r) = axum_route(line, pos, source_file) {
                routes.push(r);
            }
        }
        for verb in ["get", "post", "put", "delete", "patch"] {
            let pattern = format!("#[{}(", verb);
            for (pos, _) in line.match_indices(&pattern) {
                if commented_before(line, pos) {
                    continue;
                }
                if let Some(r) = actix_route(&lines, idx, pos + pattern.len(), verb, source_file) {
                    routes.push(r);
                }
            }
        }
    }
    routes
}

/// One axum `.route(` occurrence. The path literal must appear before the
/// first `(` of the argument list (skips `&format!(...)` paths); the
/// method token right after the path's comma must be a known verb, and the
/// handler is whatever sits inside its parens (closures report empty).
fn axum_route(line: &str, pos: usize, source_file: &str) -> Option<Route> {
    const CALL: &str = ".route(";
    let rest = &line[pos + CALL.len()..];
    let (path, open, after) = first_quoted(rest)?;
    if rest.find('(').map_or(false, |p| p < open) {
        return None;
    }
    if !path.starts_with('/') {
        return None;
    }
    let comma = rest[after..].find(',')? + after;
    let tail = rest[comma + 1..].trim_start();
    let method_end = ident_end(tail)?;
    let method = normalize_method(&tail[..method_end])?;
    let handler = tail[method_end..]
        .trim_start()
        .strip_prefix('(')
        .map(|inner| inner.split(')').next().unwrap_or("").trim())
        .map(|inner| if inner.starts_with('|') { "" } else { inner })
        .unwrap_or("")
        .to_string();
    Some(Route {
        method: method.to_string(),
        path,
        handler,
        source_file: source_file.to_string(),
        framework: "axum",
    })
}

/// One actix attribute occurrence (`col` points just past `#[get(`). The
/// handler is the first non-commented `fn` name after the attribute.
fn actix_route(
    lines: &[&str],
    idx: usize,
    col: usize,
    verb: &str,
    source_file: &str,
) -> Option<Route> {
    let line = lines[idx];
    let rest = &line[col..];
    let (path, open, after) = first_quoted(rest)?;
    if rest.find('(').map_or(false, |p| p < open) {
        return None;
    }
    if !path.starts_with('/') {
        return None;
    }
    let handler = fn_name_after(lines, idx, col + after, ACTIX_FN_LOOKAHEAD)?;
    Some(Route {
        method: verb.to_ascii_uppercase(),
        path,
        handler,
        source_file: source_file.to_string(),
        framework: "actix",
    })
}

/// First double-quoted string in `s`: returns `(content, index of the
/// opening quote, index just past the closing quote)`. Handles `\"` escapes.
fn first_quoted(s: &str) -> Option<(String, usize, usize)> {
    let open = s.find('"')?;
    let rest = &s[open + 1..];
    let mut escaped = false;
    for (i, c) in rest.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '"' => return Some((rest[..i].to_string(), open, open + 1 + i + 1)),
            _ => {}
        }
    }
    None
}

/// Length of the leading ASCII identifier in `s` (starts with a letter or
/// `_`); None when there is none.
fn ident_end(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return None;
    }
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Some(i)
}

/// The route method verb, upcased; None for anything that is not one of
/// the five scanned verbs (e.g. `.route("/x", some_router)`).
fn normalize_method(token: &str) -> Option<&'static str> {
    match token.to_ascii_lowercase().as_str() {
        "get" => Some("GET"),
        "post" => Some("POST"),
        "put" => Some("PUT"),
        "delete" => Some("DELETE"),
        "patch" => Some("PATCH"),
        _ => None,
    }
}

/// The first non-commented `fn NAME` at or after `(start_line, col)`,
/// scanning up to `max` lines (doc comments and other attributes may sit
/// between an actix attribute and its function).
fn fn_name_after(lines: &[&str], start_line: usize, col: usize, max: usize) -> Option<String> {
    let end = (start_line + max).min(lines.len());
    for (offset, li) in (start_line..end).enumerate() {
        let line = lines[li];
        let from = if offset == 0 { col } else { 0 };
        for (p, _) in line.match_indices("fn") {
            if p < from || commented_before(line, p) {
                continue;
            }
            // Keyword boundary: "fn" must not continue an identifier and
            // must be followed by whitespace (skips `fn(u8)` type refs).
            let continues_ident = line[..p]
                .chars()
                .next_back()
                .map_or(false, |c| c.is_ascii_alphanumeric() || c == '_');
            let followed_by_space = line[p + 2..]
                .chars()
                .next()
                .map_or(false, |c| c == ' ' || c == '\t');
            if continues_ident || !followed_by_space {
                continue;
            }
            let name: String = line[p + 2..]
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                return Some(name);
            }
        }
    }
    None
}

/// True when a `//` line comment precedes `pos` (a `://` scheme separator
/// is not a comment).
fn commented_before(line: &str, pos: usize) -> bool {
    let prefix = &line[..pos.min(line.len())];
    let mut from = 0;
    while let Some(i) = prefix[from..].find("//") {
        let at = from + i;
        if at > 0 && prefix.as_bytes()[at - 1] == b':' {
            from = at + 2;
            continue;
        }
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Catalog assembly

/// Exact duplicates (same method/path/handler/file) collapse to one.
fn dedupe_routes(routes: Vec<Route>) -> Vec<Route> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(routes.len());
    for r in routes {
        let key = (
            r.method.clone(),
            r.path.clone(),
            r.handler.clone(),
            r.source_file.clone(),
        );
        if seen.insert(key) {
            out.push(r);
        }
    }
    out
}

/// Enforces the server's 1000-route ceiling; returns the kept routes and
/// how many were dropped.
fn limit_routes(mut routes: Vec<Route>) -> (Vec<Route>, usize) {
    let total = routes.len();
    routes.truncate(MAX_ROUTES);
    let dropped = total - routes.len();
    (routes, dropped)
}

/// Serializes the catalog body (field order fixed by the wire contract,
/// escaping via the shared JSON writer).
fn catalog_json(service: &str, routes: &[Route]) -> String {
    let mut sb = String::with_capacity(64 + routes.len() * 128);
    sb.push('{');
    sb.push_str("\"service_name\":");
    crate::json::escape(&mut sb, service);
    sb.push_str(",\"routes\":[");
    for (i, r) in routes.iter().enumerate() {
        if i > 0 {
            sb.push(',');
        }
        sb.push('{');
        sb.push_str("\"method\":");
        crate::json::escape(&mut sb, &r.method);
        sb.push_str(",\"path\":");
        crate::json::escape(&mut sb, &r.path);
        sb.push_str(",\"handler\":");
        crate::json::escape(&mut sb, &r.handler);
        sb.push_str(",\"source_file\":");
        crate::json::escape(&mut sb, &r.source_file);
        sb.push('}');
    }
    sb.push_str("]}");
    sb
}

/// Extracts `accepted` from the catalog response (tiny targeted parse,
/// mirroring `json::extract_last_seq`).
fn extract_accepted(body: &str) -> Option<i64> {
    let pos = body.find("\"accepted\"")?;
    let rest = &body[pos + "\"accepted\"".len()..];
    let rest = rest.trim_start().strip_prefix(':').unwrap_or(rest);
    let num: String = rest.trim_start().chars().take_while(|c| c.is_ascii_digit()).collect();
    num.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACTIX_SRC: &str = "\
use actix_web::{get, post, web, HttpResponse};

#[get(\"/health\")]
async fn health() -> HttpResponse { HttpResponse::Ok().finish() }

#[post(\"/orders\")]
async fn create_order(
    body: web::Json<NewOrder>,
) -> HttpResponse { HttpResponse::Created().finish() }

// #[get(\"/ghost\")]
// async fn ghost() {}

#[get(\"relative\")]
async fn not_rooted() {}

#[put(\"/orders/{id}\")]
async fn replace_order(path: web::Path<u32>) -> HttpResponse { todo!() }

#[delete(\"/orders/:id\")]
async fn remove_order(path: web::Path<u32>) -> HttpResponse { todo!() }

#[patch(\"/orders/{id}\")]
async fn patch_order(path: web::Path<u32>) -> HttpResponse { todo!() }

#[get(\"/inline\")] async fn inline_handler() {}
";

    #[test]
    fn extracts_actix_attributes() {
        let routes = extract_routes(ACTIX_SRC, "src/orders.rs");
        let want = [
            ("GET", "/health", "health"),
            ("POST", "/orders", "create_order"),
            ("PUT", "/orders/{id}", "replace_order"),
            ("DELETE", "/orders/:id", "remove_order"),
            ("PATCH", "/orders/{id}", "patch_order"),
            ("GET", "/inline", "inline_handler"),
        ];
        assert_eq!(routes.len(), want.len(), "{:?}", routes);
        for (r, (method, path, handler)) in routes.iter().zip(want) {
            assert_eq!(r.method, method);
            assert_eq!(r.path, path);
            assert_eq!(r.handler, handler);
            assert_eq!(r.source_file, "src/orders.rs");
            assert_eq!(r.framework, "actix");
        }
    }

    const AXUM_SRC: &str = "\
let app = Router::new()
    .route(\"/users\", get(users_list))
    .route(\"/users\", post(user::create))
    .route(\"/users/{id}\", put(update_user))
    .route(\"/orders/:id\", delete(remove_order))
    .route(\"/health\", patch(health_check))
    .route(\"/admin\", handler_without_method)
    .route(\"/closure\", get(|| async { \"ok\" }))
    .route(&format!(\"/dyn/{}\", id), get(dynamic))
    // .route(\"/ghost\", get(ghost_handler))
    ;
let chained = Router::new().route(\"/a\", get(a_handler)).route(\"/b\", post(b_handler));
";

    #[test]
    fn extracts_axum_route_chains() {
        let routes = extract_routes(AXUM_SRC, "src/app.rs");
        let want = [
            ("GET", "/users", "users_list"),
            ("POST", "/users", "user::create"),
            ("PUT", "/users/{id}", "update_user"),
            ("DELETE", "/orders/:id", "remove_order"),
            ("PATCH", "/health", "health_check"),
            ("GET", "/closure", ""),
            ("GET", "/a", "a_handler"),
            ("POST", "/b", "b_handler"),
        ];
        assert_eq!(routes.len(), want.len(), "{:?}", routes);
        for (r, (method, path, handler)) in routes.iter().zip(want) {
            assert_eq!((r.method.as_str(), r.path.as_str(), r.handler.as_str()), (method, path, handler), "{:?}", r);
            assert_eq!(r.source_file, "src/app.rs");
            assert_eq!(r.framework, "axum");
        }
    }

    #[test]
    fn nested_sources_without_routes_yield_nothing() {
        let src = "\
mod nested {
    mod deeper {
        pub fn helper() -> u32 { 1 }
    }
    const S: &str = \"plain text\";
    // just a comment mentioning .route(\"/x\", get(x))
}
";
        assert!(extract_routes(src, "src/nested.rs").is_empty());
    }

    #[test]
    fn commented_before_cases() {
        assert!(!commented_before("#[get(\"/x\")]", 0));
        assert!(commented_before("// #[get(\"/x\")]", 3));
        // A "://" scheme separator is not a comment; a real one still is.
        assert!(!commented_before("let url = \"https://x\";", 11));
        assert!(commented_before("let a = 1; // #[get(\"/y\")]", 16));
    }

    #[test]
    fn catalog_json_shape() {
        let routes = vec![Route {
            method: "GET".into(),
            path: "/api/orders/{id}".into(),
            handler: "orders::list".into(),
            source_file: "src/orders.rs".into(),
            framework: "actix",
        }];
        assert_eq!(
            catalog_json("svc", &routes),
            "{\"service_name\":\"svc\",\"routes\":[{\"method\":\"GET\",\"path\":\"/api/orders/{id}\",\"handler\":\"orders::list\",\"source_file\":\"src/orders.rs\"}]}"
        );
        assert_eq!(catalog_json("svc", &[]), "{\"service_name\":\"svc\",\"routes\":[]}");
        // escaping rides on the shared JSON writer
        assert_eq!(
            catalog_json("pa\"y\n", &[]),
            "{\"service_name\":\"pa\\\"y\\n\",\"routes\":[]}"
        );
    }

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_cases() {
        let a = parse_args(&argv(&[])).unwrap();
        assert_eq!(a.dir, ".");
        assert!(!a.print);
        assert!(a.service.is_none() && a.url.is_none() && a.api_key.is_none());

        let a = parse_args(&argv(&["--dir", "src", "--print"])).unwrap();
        assert_eq!(a.dir, "src");
        assert!(a.print);

        let a = parse_args(&argv(&["--dir=src", "--service=svc", "--url=http://x:1", "--api-key=k"])).unwrap();
        assert_eq!(a.dir, "src");
        assert_eq!(a.service.as_deref(), Some("svc"));
        assert_eq!(a.url.as_deref(), Some("http://x:1"));
        assert_eq!(a.api_key.as_deref(), Some("k"));

        assert!(parse_args(&argv(&["--nope"])).is_err());
        assert!(parse_args(&argv(&["--dir"])).is_err());
        assert!(parse_args(&argv(&["--print=1"])).is_err());
        assert!(parse_args(&argv(&["positional"])).is_err());
    }

    #[test]
    fn base_url_cases() {
        // --url wins over everything; trailing slash trimmed.
        assert_eq!(
            base_url(Some("http://cli:1/"), "http://endpoint:2", "http://env:3"),
            Some("http://cli:1".to_string())
        );
        // DATAFLOW_HTTP_URL beats a URL-form endpoint.
        assert_eq!(
            base_url(None, "http://endpoint:2/", "http://env:3"),
            Some("http://env:3".to_string())
        );
        // URL-form endpoint used when no override; bare host:port skipped.
        assert_eq!(
            base_url(None, "https://endpoint:2/x", ""),
            Some("https://endpoint:2/x".to_string())
        );
        assert_eq!(base_url(None, "grpc:9090", ""), None);
        assert_eq!(base_url(None, "localhost:50051", ""), None);
        // An empty --url falls through to the environment.
        assert_eq!(
            base_url(Some(""), "http://endpoint:2", ""),
            Some("http://endpoint:2".to_string())
        );
    }

    #[test]
    fn dir_basename_cases() {
        assert_eq!(dir_basename("src"), "src");
        assert_eq!(dir_basename("some/dir/app"), "app");
        assert_eq!(dir_basename("app/"), "app");
        // "." canonicalizes to the real directory name (machine-dependent,
        // but always non-empty).
        assert!(!dir_basename(".").is_empty());
    }

    #[test]
    fn dedupe_and_limit() {
        let r = |path: &str| Route {
            method: "GET".into(),
            path: path.into(),
            handler: "h".into(),
            source_file: "src/x.rs".into(),
            framework: "axum",
        };
        let dupes = vec![r("/a"), r("/a"), r("/b")];
        let kept = dedupe_routes(dupes);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].path, "/a");
        assert_eq!(kept[1].path, "/b");

        let many: Vec<Route> = (0..MAX_ROUTES + 7).map(|i| r(&format!("/p{}", i))).collect();
        let (kept, dropped) = limit_routes(many);
        assert_eq!(kept.len(), MAX_ROUTES);
        assert_eq!(dropped, 7);
    }

    #[test]
    fn extract_accepted_cases() {
        assert_eq!(extract_accepted("{\"accepted\":3}"), Some(3));
        assert_eq!(extract_accepted("{\"accepted\": 12}"), Some(12));
        assert_eq!(extract_accepted("{}"), None);
        assert_eq!(extract_accepted("not json"), None);
    }

    #[test]
    fn rel_path_uses_forward_slashes() {
        let p = Path::new("src").join("nested").join("mod.rs");
        assert_eq!(rel_path(&p, "src"), "nested/mod.rs");
        assert_eq!(rel_path(Path::new("outside.rs"), "src"), "outside.rs");
    }

    #[test]
    fn collect_files_skips_target_and_git() {
        let root = std::env::temp_dir().join(format!("dataflow-scan-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("src/nested")).unwrap();
        std::fs::create_dir_all(root.join("target/debug")).unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join("src/lib.rs"), "").unwrap();
        std::fs::write(root.join("src/nested/a.rs"), "").unwrap();
        std::fs::write(root.join("target/debug/junk.rs"), "").unwrap();
        std::fs::write(root.join(".git/config.rs"), "").unwrap();
        std::fs::write(root.join("notes.txt"), "").unwrap();

        let mut files = Vec::new();
        collect_files(&root, 0, &mut files).unwrap();
        let names: Vec<String> = files.iter().map(|f| rel_path(f, root.to_str().unwrap())).collect();
        assert_eq!(names, vec!["src/lib.rs".to_string(), "src/nested/a.rs".to_string()]);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
