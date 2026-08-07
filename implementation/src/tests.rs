// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Unit tests for the entrypoint / config layer. Protocol-level parsing tests
// live in `mcp.rs`. Cache-lifecycle tests are added with the lifecycle in the
// implementation phase.

use crate::generated::config::Config;
use crate::Policy;

/// Parse a gcl config JSON (the generated, Option-wrapped shape) and resolve it
/// through `Policy::new`, which re-applies the gcl `default:` clauses that
/// `cargo anypoint config-gen` strips.
fn policy(json: &str) -> Policy {
    let cfg: Config = serde_json::from_str(json).expect("config must parse");
    Policy::new(&cfg)
}

#[test]
fn parses_minimal_config_with_defaults() {
    let p = policy(r#"{"discovery":{}}"#);
    assert!(p.discovery_cacheable);
    assert_eq!(p.discovery_ttl, 60);
    assert!(p.tools.is_empty());
}

#[test]
fn parses_full_config() {
    let p = policy(
        r#"{
            "discovery": {"cacheable": false, "ttl": 30},
            "tools": [{"name": "search", "cacheable": true, "ttl": 120, "scope": "identity"}],
            "maxEntries": 500,
            "distributed": true
        }"#,
    );
    // discovery.cacheable=false collapses discovery_cacheable to false.
    assert!(!p.discovery_cacheable);
    assert_eq!(p.discovery_ttl, 30);
    assert_eq!(p.tools.len(), 1);
    let search = p.tools.get("search").expect("search tool resolved");
    assert!(search.cacheable);
    assert_eq!(search.ttl, 120);
    assert_eq!(search.scope, CacheScope::Identity);
}

#[test]
fn tool_scope_defaults_to_shared() {
    let p = policy(r#"{"discovery":{},"tools":[{"name":"t","ttl":10}]}"#);
    let t = p.tools.get("t").expect("tool resolved");
    assert_eq!(t.scope, CacheScope::Shared);
    assert!(!t.cacheable);
}

#[test]
fn discovery_ttl_zero_disables_discovery() {
    // ttl=0 forces discovery_cacheable false even when cacheable=true.
    let p = policy(r#"{"discovery":{"cacheable":true,"ttl":0}}"#);
    assert!(!p.discovery_cacheable);
}

// --- key.rs -----------------------------------------------------------------

use crate::key::{cache_key, canonicalize, CacheScope, Identity};
use serde_json::json;

#[test]
fn canonicalize_is_order_independent() {
    let a = json!({"b": 1, "a": 2, "nested": {"y": 1, "x": 2}});
    let b = json!({"a": 2, "b": 1, "nested": {"x": 2, "y": 1}});
    assert_eq!(canonicalize(&a), canonicalize(&b));
}

#[test]
fn canonicalize_distinguishes_values() {
    assert_ne!(canonicalize(&json!({"a": 1})), canonicalize(&json!({"a": 2})));
}

#[test]
fn shared_key_is_deterministic_and_prefixed() {
    let p = json!({"q": "hi"});
    let id = Identity { principal: None, session: None };
    let k1 = cache_key("tools/call", &p, CacheScope::Shared, &id).unwrap();
    let k2 = cache_key("tools/call", &p, CacheScope::Shared, &id).unwrap();
    assert_eq!(k1, k2);
    assert!(k1.starts_with("tools/call:"));
}

#[test]
fn identity_key_partitions_by_principal() {
    let p = json!({});
    let a = Identity { principal: Some("alice"), session: None };
    let b = Identity { principal: Some("bob"), session: None };
    let ka = cache_key("tools/call", &p, CacheScope::Identity, &a).unwrap();
    let kb = cache_key("tools/call", &p, CacheScope::Identity, &b).unwrap();
    assert_ne!(ka, kb);
}

#[test]
fn identity_key_absent_when_no_principal_or_session() {
    let id = Identity { principal: None, session: None };
    assert!(cache_key("tools/call", &json!({}), CacheScope::Identity, &id).is_none());
}

#[test]
fn identity_key_present_with_only_session() {
    let id = Identity { principal: None, session: Some("s1") };
    assert!(cache_key("tools/call", &json!({}), CacheScope::Identity, &id).is_some());
}

// --- store.rs ---------------------------------------------------------------

use crate::store::{now_secs, CacheStore, CachedEntry, LocalStore};
use pdk::cache::{Cache, CacheError};
use std::collections::HashMap;
use std::sync::Mutex;

pub(crate) struct MockCache {
    data: Mutex<HashMap<String, Vec<u8>>>,
}
impl MockCache {
    pub(crate) fn new() -> Self {
        Self { data: Mutex::new(HashMap::new()) }
    }
}
impl Cache for MockCache {
    fn save(&self, key: &str, value: Vec<u8>) -> Result<(), CacheError> {
        self.data.lock().unwrap().insert(key.to_string(), value);
        Ok(())
    }
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.lock().unwrap().get(key).cloned()
    }
    fn delete(&self, key: &str) -> Option<Vec<u8>> {
        self.data.lock().unwrap().remove(key)
    }
    fn purge(&self) {
        self.data.lock().unwrap().clear();
    }
}

#[test]
fn cached_entry_freshness() {
    let now = 1000;
    let fresh = CachedEntry { written_at: now, valid_until: now + 10, body: vec![1] };
    let stale = CachedEntry { written_at: now, valid_until: now, body: vec![1] };
    assert!(fresh.is_fresh(now + 5));
    assert!(!stale.is_fresh(now + 1));
}

#[tokio::test]
async fn local_store_roundtrip_and_expiry() {
    let store = LocalStore::new(MockCache::new());
    let now = now_secs();
    let entry = CachedEntry { written_at: now, valid_until: now + 60, body: b"hi".to_vec() };
    store.put("k", &entry).await;
    let got = store.get("k").await.expect("hit");
    assert_eq!(got.body, b"hi");

    // Stale entry is evicted on read (local mode).
    let stale = CachedEntry { written_at: now - 100, valid_until: now - 1, body: b"x".to_vec() };
    store.put("s", &stale).await;
    assert!(store.get("s").await.is_none());
}

// --- annotations.rs ---------------------------------------------------------

use crate::annotations::{is_known_unsafe, record_from_list};

#[tokio::test]
async fn records_and_reads_unsafe_tools() {
    let store = LocalStore::new(MockCache::new());
    let list = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[
        {"name":"safe","annotations":{"readOnlyHint":true}},
        {"name":"writer","annotations":{"destructiveHint":true}}
    ]}}"#;
    record_from_list(&store, list).await;
    assert!(is_known_unsafe(&store, "writer").await);
    assert!(!is_known_unsafe(&store, "safe").await);
    assert!(!is_known_unsafe(&store, "never-seen").await);
}
