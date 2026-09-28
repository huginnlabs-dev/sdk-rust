//! Minimal JSON writer — no serde, no dependencies.

use crate::Value;
use std::collections::BTreeMap;

pub fn escape(sb: &mut String, s: &str) {
    sb.push('"');
    for c in s.chars() {
        match c {
            '"' => sb.push_str("\\\""),
            '\\' => sb.push_str("\\\\"),
            '\n' => sb.push_str("\\n"),
            '\r' => sb.push_str("\\r"),
            '\t' => sb.push_str("\\t"),
            c if (c as u32) < 0x20 => sb.push_str(&format!("\\u{:04x}", c as u32)),
            c => sb.push(c),
        }
    }
    sb.push('"');
}

fn quoted(s: &str) -> String {
    let mut sb = String::with_capacity(s.len() + 2);
    escape(&mut sb, s);
    sb
}

pub fn write_value(sb: &mut String, v: &Value) {
    match v {
        Value::Str(s) => escape(sb, s),
        Value::Num(n) => {
            if n.fract() == 0.0 && n.abs() < 9e15 {
                sb.push_str(&format!("{}", *n as i64));
            } else {
                sb.push_str(&format!("{}", n));
            }
        }
        Value::Bool(b) => sb.push_str(if *b { "true" } else { "false" }),
        Value::Raw(r) => sb.push_str(r),
    }
}

pub fn payload_object(payload: &BTreeMap<String, Value>) -> String {
    let mut sb = String::from("{");
    let mut first = true;
    for (k, v) in payload {
        if !first { sb.push(','); }
        first = false;
        escape(&mut sb, k);
        sb.push(':');
        write_value(&mut sb, v);
    }
    sb.push('}');
    sb
}

pub fn metadata_object(meta: &BTreeMap<String, String>) -> String {
    let mut sb = String::from("{");
    let mut first = true;
    for (k, v) in meta {
        if !first { sb.push(','); }
        first = false;
        escape(&mut sb, k);
        sb.push(':');
        escape(&mut sb, v);
    }
    sb.push('}');
    sb
}

/// Serializes one TraceEvent as the REST-ingest JSON object. `payload_json`
/// is the already-serialized payload segment (encrypted envelope or null).
#[allow(clippy::too_many_arguments)]
pub fn event_json(
    event_id: &str,
    seq: i64,
    trace_id: &str,
    span_id: &str,
    parent_span_id: &str,
    kind: &str,
    service: &str,
    name: &str,
    caller: &str,
    callee: &str,
    error: &str,
    status: i32,
    ts_ms: i64,
    duration_ms: i64,
    meta: &BTreeMap<String, String>,
    payload_json: &str,
) -> String {
    let mut sb = String::with_capacity(512);
    sb.push('{');
    sb.push_str(&format!("\"event_id\":{},", quoted(event_id)));
    sb.push_str(&format!("\"seq\":{},", seq));
    sb.push_str(&format!("\"trace_id\":{},", quoted(trace_id)));
    sb.push_str(&format!("\"span_id\":{},", quoted(span_id)));
    sb.push_str(&format!("\"parent_span_id\":{},", quoted(parent_span_id)));
    sb.push_str(&format!("\"type\":{},", quoted(kind)));
    sb.push_str(&format!("\"service_name\":{},", quoted(service)));
    sb.push_str(&format!("\"name\":{},", quoted(name)));
    sb.push_str(&format!("\"caller_package\":{},", quoted(caller)));
    sb.push_str(&format!("\"callee_package\":{},", quoted(callee)));
    sb.push_str(&format!("\"function_name\":{},", quoted(name)));
    sb.push_str(&format!("\"timestamp\":{},", ts_ms));
    sb.push_str(&format!("\"duration_ms\":{},", duration_ms));
    sb.push_str(&format!("\"status_code\":{},", status));
    sb.push_str(&format!("\"error_message\":{},", quoted(error)));
    sb.push_str(&format!("\"payload\":{},", payload_json));
    sb.push_str(&format!("\"metadata\":{}", metadata_object(meta)));
    sb.push('}');
    sb
}

/// Extracts `last_seq` from the ack response body (tiny targeted parse).
pub fn extract_last_seq(body: &str) -> i64 {
    if let Some(pos) = body.find("\"last_seq\"") {
        let rest = &body[pos + "\"last_seq\"".len()..];
        let rest = rest.trim_start().strip_prefix(':').unwrap_or(rest);
        let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(v) = num.parse::<i64>() {
            return v;
        }
    }
    0
}
