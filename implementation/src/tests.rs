// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Unit tests for the entrypoint / config layer. Protocol-level parsing tests
// live in `mcp.rs`. Cache-lifecycle tests are added with the lifecycle in the
// implementation phase.

use crate::generated::config::{CacheScope, Config};

fn parse(json: &str) -> Config {
    serde_json::from_str(json).expect("config must parse")
}

#[test]
fn parses_minimal_config_with_defaults() {
    let cfg = parse(r#"{"discovery":{}}"#);
    assert!(cfg.discovery.cacheable);
    assert_eq!(cfg.discovery.ttl, 60);
    assert_eq!(cfg.max_entries, 1000);
    assert!(!cfg.distributed);
    assert!(cfg.tools.is_empty());
}

#[test]
fn parses_full_config() {
    let cfg = parse(
        r#"{
            "discovery": {"cacheable": false, "ttl": 30},
            "tools": [{"name": "search", "cacheable": true, "ttl": 120, "scope": "identity"}],
            "maxEntries": 500,
            "distributed": true
        }"#,
    );
    assert!(!cfg.discovery.cacheable);
    assert_eq!(cfg.discovery.ttl, 30);
    assert_eq!(cfg.max_entries, 500);
    assert!(cfg.distributed);
    assert_eq!(cfg.tools.len(), 1);
    assert_eq!(cfg.tools[0].name, "search");
    assert!(cfg.tools[0].cacheable);
    assert_eq!(cfg.tools[0].ttl, 120);
    assert_eq!(cfg.tools[0].scope, CacheScope::Identity);
}

#[test]
fn tool_scope_defaults_to_shared() {
    let cfg = parse(r#"{"discovery":{},"tools":[{"name":"t","ttl":10}]}"#);
    assert_eq!(cfg.tools[0].scope, CacheScope::Shared);
    assert!(!cfg.tools[0].cacheable);
}
