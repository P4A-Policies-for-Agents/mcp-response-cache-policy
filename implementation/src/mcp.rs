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
