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

// --- key.rs -----------------------------------------------------------------

use crate::key::{cache_key, canonicalize, Identity};
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
