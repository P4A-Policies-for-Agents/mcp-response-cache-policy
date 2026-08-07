// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// MCP / JSON-RPC 2.0 envelope parsing and method vocabulary.
//
// MCP traffic on the gateway is JSON-RPC 2.0 over HTTP POST. The response
// carries no method name, so a response-side cache must thread the method from
// the request side. This module owns the protocol facts; the cache lifecycle
// (key construction, lookup/store, guardrails) builds on top of it.
//
// Scaffold: the parser + vocabulary are defined ahead of the cache lifecycle
// that consumes them (implementation phase). Tests exercise them today.
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
}
