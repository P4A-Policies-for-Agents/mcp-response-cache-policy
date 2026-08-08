// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// MCP / JSON-RPC 2.0 envelope parsing and method vocabulary.
//
// MCP traffic on the gateway is JSON-RPC 2.0 over HTTP POST. The response
// carries no method name, so a response-side cache must thread the method from
// the request side. This module owns the protocol facts; the cache lifecycle
// (key construction, lookup/store, guardrails) builds on top of it.
#![allow(dead_code)]

use serde_json::Value;

// --- Method vocabulary (wire strings) ---------------------------------------

pub const TOOLS_LIST: &str = "tools/list";
pub const RESOURCES_LIST: &str = "resources/list";
pub const PROMPTS_LIST: &str = "prompts/list";
pub const TOOLS_CALL: &str = "tools/call";

/// Discovery methods whose full result is cacheable as a unit.
pub fn is_discovery_method(method: &str) -> bool {
    matches!(method, TOOLS_LIST | RESOURCES_LIST | PROMPTS_LIST)
}

/// JSON-RPC notifications have no `id` and never receive a response.
pub fn is_notification(method: &str) -> bool {
    method.starts_with("notifications/")
}

// --- Envelope ---------------------------------------------------------------

/// A JSON-RPC request id. Preserved verbatim so a cached result can be
/// re-stamped onto the live request's id on a cache hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestId {
    Number(i64),
    String(String),
    Null,
}

/// The subset of a JSON-RPC request this policy needs to make a cache decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequest {
    pub id: RequestId,
    pub method: String,
    /// Tool name for `tools/call` (`params.name`), if present.
    pub tool_name: Option<String>,
}

/// Parse a JSON-RPC 2.0 request envelope. Returns `None` for non-JSON-RPC
/// bodies (fail-open: the caller passes such traffic through untouched).
pub fn parse_request(body: &[u8]) -> Option<McpRequest> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let obj = v.as_object()?;
    if obj.get("jsonrpc")?.as_str()? != "2.0" {
        return None;
    }
    let method = obj.get("method")?.as_str()?;
    if method.is_empty() {
        return None;
    }

    let id = match obj.get("id") {
        Some(Value::Number(n)) => n.as_i64().map(RequestId::Number).unwrap_or(RequestId::Null),
        Some(Value::String(s)) => RequestId::String(s.clone()),
        _ => RequestId::Null,
    };

    let tool_name = if method == TOOLS_CALL {
        obj.get("params")
            .and_then(Value::as_object)
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    } else {
        None
    };

    Some(McpRequest {
        id,
        method: method.to_string(),
        tool_name,
    })
}

// --- Response side ----------------------------------------------------------

/// Classification of one JSON-RPC event parsed out of an SSE stream.
enum RpcEvent {
    /// A terminal success response (`id` + `result`, no `error`) — the payload
    /// a cache would store. Carries the raw object bytes.
    Terminal(Vec<u8>),
    /// A fire-and-forget notification (`method`, no `id`) — droppable on replay.
    Notification,
    /// Anything that makes the stream unsafe to collapse: a server→client
    /// request (`method` + `id`), a terminal error (`id` + `error`), a second
    /// terminal, or a non-object / unrecognized payload.
    Unsafe,
}

/// Classify a single SSE `data:` payload as a JSON-RPC event.
fn classify_event(payload: &str) -> RpcEvent {
    let v: Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => return RpcEvent::Unsafe,
    };
    let obj = match v.as_object() {
        Some(o) => o,
        None => return RpcEvent::Unsafe,
    };
    let has_id = obj.get("id").map(|id| !id.is_null()).unwrap_or(false);
    let has_method = obj.contains_key("method");

    match (has_id, has_method) {
        // Notification: method, no id — fire-and-forget, safe to drop on replay.
        (false, true) => RpcEvent::Notification,
        // Server→client request: method + id — needs a client response;
        // collapsing to a cached result would skip that round-trip.
        (true, true) => RpcEvent::Unsafe,
        // Response: id, no method. Cacheable only if it is a clean success.
        (true, false) => {
            if obj.contains_key("error") || !obj.contains_key("result") {
                RpcEvent::Unsafe
            } else {
                RpcEvent::Terminal(payload.as_bytes().to_vec())
            }
        }
        // Neither id nor method — not a well-formed JSON-RPC event.
        (false, false) => RpcEvent::Unsafe,
    }
}

/// Extract the cacheable JSON-RPC payload from a streamable-HTTP MCP response
/// body, normalizing transport framing.
///
/// MCP's streamable-HTTP transport frames responses as Server-Sent Events, even
/// the non-streaming single-shot results of the cacheable methods. This returns
/// the raw JSON bytes of the terminal success response so the rest of the
/// response path (cacheability check, storage) treats SSE-framed and bare-JSON
/// responses uniformly. Returns `None` when the stream is not safe to collapse.
///
/// Cases:
/// - **Bare JSON object** (no SSE framing) → returned as-is.
/// - **Single-event SSE frame** whose data is a success response → unwrapped.
/// - **Multi-event stream** of `(notification)* + exactly one success terminal`
///   → the terminal is extracted; the fire-and-forget notifications are dropped
///   (a "completed op that emitted progress"). This is safe because a cache hit
///   replays only the terminal result, and the client never needed the
///   progress frames.
/// - **Unsafe stream** → `None`: any server→client request in the stream (a
///   `method` **with** an `id` — sampling/elicitation/roots — needs a client
///   round-trip), a terminal error, more than one terminal response, a
///   notifications-only stream (no result to cache), or a non-object payload.
///
/// Per the SSE spec, a `data:` value may span multiple consecutive `data:`
/// lines within one event (joined with `\n`); this handles that. Comment lines
/// (`:`), the `event:`/`id:`/`retry:` fields, and blank separators are ignored.
pub fn extract_cacheable_json(body: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(body).ok()?;

    // Bare JSON (not SSE-framed): accept iff it is a JSON object.
    let looks_like_sse = text
        .lines()
        .any(|l| l.starts_with("data:") || l.starts_with("event:"));
    if !looks_like_sse {
        return serde_json::from_slice::<Value>(body)
            .ok()
            .filter(Value::is_object)
            .map(|_| body.to_vec());
    }

    // SSE-framed: collect the payload of each event. An event ends at a blank
    // line; its data is the concatenation (by `\n`) of its `data:` lines.
    let mut events: Vec<String> = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        if line.is_empty() {
            if let Some(data) = current.take() {
                events.push(data);
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            // A single leading space after the colon is part of the framing.
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            match current.as_mut() {
                Some(buf) => {
                    buf.push('\n');
                    buf.push_str(rest);
                }
                None => current = Some(rest.to_string()),
            }
        }
        // event:/id:/retry:/comment lines carry no payload — ignore.
    }
    if let Some(data) = current.take() {
        events.push(data);
    }

    // Reduce the stream to a single terminal result, requiring every other
    // event to be a droppable notification. Any unsafe event, or a second
    // terminal, aborts (None).
    let mut terminal: Option<Vec<u8>> = None;
    for payload in &events {
        match classify_event(payload) {
            RpcEvent::Notification => {}
            RpcEvent::Terminal(bytes) => {
                if terminal.is_some() {
                    return None; // more than one terminal response
                }
                terminal = Some(bytes);
            }
            RpcEvent::Unsafe => return None,
        }
    }
    terminal
}

/// True iff the response is a cacheable JSON-RPC success: parses as an object,
/// carries a `result`, has no `error`, and is not a tool-level error
/// (`result.isError == true`).
pub fn is_cacheable_response(body: &[u8]) -> bool {
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let obj = match v.as_object() {
        Some(o) => o,
        None => return false,
    };
    if obj.contains_key("error") {
        return false;
    }
    let result = match obj.get("result") {
        Some(r) => r,
        None => return false,
    };
    // Tool-level error inside a successful envelope.
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        return false;
    }
    true
}

/// Replace the stored response's `id` with the live request's id and
/// re-serialize. Returns `None` if the stored body is not a JSON object.
pub fn restamp_id(body: &[u8], id: &RequestId) -> Option<Vec<u8>> {
    let mut v: Value = serde_json::from_slice(body).ok()?;
    let obj = v.as_object_mut()?;
    let id_value = match id {
        RequestId::Number(n) => Value::from(*n),
        RequestId::String(s) => Value::from(s.clone()),
        RequestId::Null => Value::Null,
    };
    obj.insert("id".to_string(), id_value);
    serde_json::to_vec(&v).ok()
}

/// From a `tools/list` result body, derive per-tool safety. A tool is "safe"
/// (cacheable, subject to the operator allowlist) only if it declares
/// `readOnlyHint: true` and does not declare `destructiveHint: true`. Tools
/// with no/unknown annotations are reported unsafe here; the caller decides how
/// to treat absence (defer to allowlist).
pub fn tool_safety_from_list(result_body: &[u8]) -> Vec<(String, bool)> {
    let v: Value = match serde_json::from_slice(result_body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let tools = match v.pointer("/result/tools").and_then(Value::as_array) {
        Some(t) => t,
        None => return Vec::new(),
    };
    tools
        .iter()
        .filter_map(|t| {
            let name = t.get("name")?.as_str()?.to_string();
            let ann = t.get("annotations");
            let read_only = ann
                .and_then(|a| a.get("readOnlyHint"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let destructive = ann
                .and_then(|a| a.get("destructiveHint"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some((name, read_only && !destructive))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tools_list() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
        let req = parse_request(body).expect("must parse");
        assert_eq!(req.id, RequestId::Number(1));
        assert_eq!(req.method, TOOLS_LIST);
        assert!(req.tool_name.is_none());
        assert!(is_discovery_method(&req.method));
    }

    #[test]
    fn parses_tools_call_with_name() {
        let body =
            br#"{"jsonrpc":"2.0","id":"a","method":"tools/call","params":{"name":"search"}}"#;
        let req = parse_request(body).expect("must parse");
        assert_eq!(req.id, RequestId::String("a".into()));
        assert_eq!(req.method, TOOLS_CALL);
        assert_eq!(req.tool_name.as_deref(), Some("search"));
        assert!(!is_discovery_method(&req.method));
    }

    #[test]
    fn notification_detected() {
        let body = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let req = parse_request(body).expect("must parse");
        assert!(is_notification(&req.method));
        assert_eq!(req.id, RequestId::Null);
    }

    #[test]
    fn rejects_non_jsonrpc() {
        assert!(parse_request(br#"{"foo":"bar"}"#).is_none());
        assert!(parse_request(b"not json").is_none());
    }

    #[test]
    fn cacheable_response_accepts_result() {
        let ok = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        assert!(is_cacheable_response(ok));
    }

    #[test]
    fn extract_cacheable_json_from_sse_event() {
        // The exact shape a streamable-HTTP MCP server returns for a single-shot
        // result: one `event: message` block, one `data:` line.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"ok\":true}}\n\n";
        let json = extract_cacheable_json(body).expect("single event must extract");
        let v: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["id"], serde_json::json!(3));
        assert_eq!(v["result"]["ok"], serde_json::json!(true));
        // And the extracted JSON flows through the cacheability gate.
        assert!(is_cacheable_response(&json));
    }

    #[test]
    fn extract_cacheable_json_passes_bare_json_through() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        let json = extract_cacheable_json(body).expect("bare json object passes through");
        assert_eq!(json, body.to_vec());
    }

    #[test]
    fn extract_cacheable_json_joins_multiline_data() {
        // Per the SSE spec a single event's data can span consecutive data:
        // lines, joined by \n. This is still ONE event → one JSON payload.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\ndata: \"result\":{\"v\":2}}\n\n";
        let json = extract_cacheable_json(body).expect("multiline single event extracts");
        let v: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["result"]["v"], serde_json::json!(2));
    }

    #[test]
    fn extract_cacheable_json_multi_event_progress_then_result() {
        // A "completed op that emitted progress" stream: one or more fire-and-
        // forget progress notifications (method, NO id) followed by the single
        // terminal result. The notifications are droppable on replay, so the
        // terminal result IS cacheable — extract exactly it.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":0.3}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":0.7}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n";
        let json = extract_cacheable_json(body).expect("progress+result stream extracts terminal");
        let v: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["id"], serde_json::json!(1));
        assert_eq!(v["result"]["ok"], serde_json::json!(true));
        assert!(is_cacheable_response(&json));
    }

    #[test]
    fn extract_cacheable_json_rejects_server_request_in_stream() {
        // An interactive stream: the server issues a request (method + non-null
        // id → sampling/elicitation/roots) that needs a client response before
        // the terminal. Collapsing to a cached result would skip that
        // round-trip, so the whole stream is unsafe → None.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"sampling/createMessage\",\"params\":{}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n";
        assert!(extract_cacheable_json(body).is_none());
    }

    #[test]
    fn extract_cacheable_json_rejects_multiple_results() {
        // Two terminal responses in one stream is ambiguous — never collapse.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"a\":1}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"b\":2}}\n\n";
        assert!(extract_cacheable_json(body).is_none());
    }

    #[test]
    fn extract_cacheable_json_rejects_notifications_only() {
        // A stream carrying only notifications and no terminal response is not a
        // cacheable result.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":0.5}}\n\n";
        assert!(extract_cacheable_json(body).is_none());
    }

    #[test]
    fn extract_cacheable_json_rejects_non_object_payload() {
        // A data: line that isn't a JSON object (array / scalar / garbage).
        let arr = b"event: message\ndata: [1,2,3]\n\n";
        let scalar = b"event: message\ndata: 42\n\n";
        let garbage = b"event: message\ndata: not json\n\n";
        assert!(extract_cacheable_json(arr).is_none());
        assert!(extract_cacheable_json(scalar).is_none());
        assert!(extract_cacheable_json(garbage).is_none());
    }

    #[test]
    fn cacheable_response_rejects_error_and_iserror() {
        let err = br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"x"}}"#;
        let tool_err = br#"{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[]}}"#;
        let no_result = br#"{"jsonrpc":"2.0","id":1}"#;
        assert!(!is_cacheable_response(err));
        assert!(!is_cacheable_response(tool_err));
        assert!(!is_cacheable_response(no_result));
    }

    #[test]
    fn restamp_replaces_id() {
        let stored = br#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#;
        let out = restamp_id(stored, &RequestId::String("live".into())).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["id"], serde_json::json!("live"));
        assert_eq!(v["result"]["ok"], serde_json::json!(true));
    }

    #[test]
    fn restamp_null_id() {
        let stored = br#"{"jsonrpc":"2.0","id":7,"result":{}}"#;
        let out = restamp_id(stored, &RequestId::Null).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["id"], Value::Null);
    }

    #[test]
    fn tool_safety_extraction() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[
            {"name":"safe","annotations":{"readOnlyHint":true}},
            {"name":"writer","annotations":{"readOnlyHint":true,"destructiveHint":true}},
            {"name":"unknown"}
        ]}}"#;
        let map: std::collections::HashMap<_, _> =
            tool_safety_from_list(body).into_iter().collect();
        assert_eq!(map.get("safe"), Some(&true));
        assert_eq!(map.get("writer"), Some(&false));
        assert_eq!(map.get("unknown"), Some(&false));
    }
}
