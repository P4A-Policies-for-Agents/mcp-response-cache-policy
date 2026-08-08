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

// --- decide() lifecycle decision matrix -------------------------------------
//
// `decide` is the heart of the request filter: it maps a parsed MCP request +
// resolved config + observed-annotation state to Cache|Bypass. These cover
// every branch (notification, discovery on/off, tools/call allowlist +
// cacheable + missing-name + observed-unsafe, identity scope, unknown method)
// without the Docker harness.

use crate::mcp::{McpRequest, RequestId};
use crate::{decide, Decision};

/// Build a parsed request with a fixed numeric id.
fn req(method: &str, tool: Option<&str>) -> McpRequest {
    McpRequest {
        id: RequestId::Number(1),
        method: method.to_string(),
        tool_name: tool.map(String::from),
    }
}

fn is_cache(d: &Decision) -> bool {
    matches!(d, Decision::Cache { .. })
}

/// Identity with neither principal nor session.
fn anon() -> Identity<'static> {
    Identity { principal: None, session: None }
}

#[tokio::test]
async fn decide_notification_bypasses() {
    let p = policy(r#"{"discovery":{"cacheable":true,"ttl":60}}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("notifications/initialized", None), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_unknown_method_bypasses() {
    let p = policy(r#"{"discovery":{"cacheable":true,"ttl":60}}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("initialize", None), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_discovery_caches_when_enabled() {
    let p = policy(r#"{"discovery":{"cacheable":true,"ttl":45}}"#);
    let store = LocalStore::new(MockCache::new());
    for method in ["tools/list", "resources/list", "prompts/list"] {
        match decide(&p, &store, &req(method, None), &json!({}), &anon()).await {
            Decision::Cache { key, ttl } => {
                assert_eq!(ttl, 45, "{method} uses discovery ttl");
                assert!(key.starts_with(&format!("{method}:")), "{method} key prefixed");
            }
            Decision::Bypass => panic!("{method} should cache when discovery enabled"),
        }
    }
}

#[tokio::test]
async fn decide_discovery_bypasses_when_disabled() {
    let p = policy(r#"{"discovery":{"cacheable":false,"ttl":60}}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/list", None), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_discovery_bypasses_when_ttl_zero() {
    // ttl=0 collapses discovery_cacheable to false in Policy::new.
    let p = policy(r#"{"discovery":{"cacheable":true,"ttl":0}}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/list", None), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_tools_call_allowlisted_caches() {
    let p = policy(
        r#"{"discovery":{},"tools":[{"name":"search","cacheable":true,"ttl":90,"scope":"shared"}]}"#,
    );
    let store = LocalStore::new(MockCache::new());
    match decide(&p, &store, &req("tools/call", Some("search")), &json!({"q": "x"}), &anon()).await {
        Decision::Cache { ttl, .. } => assert_eq!(ttl, 90),
        Decision::Bypass => panic!("allowlisted cacheable tool should cache"),
    }
}

#[tokio::test]
async fn decide_tools_call_not_in_allowlist_bypasses() {
    let p = policy(r#"{"discovery":{},"tools":[{"name":"search","cacheable":true,"ttl":90}]}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/call", Some("other")), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_tools_call_allowlisted_but_not_cacheable_bypasses() {
    let p = policy(r#"{"discovery":{},"tools":[{"name":"search","cacheable":false,"ttl":90}]}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/call", Some("search")), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_tools_call_without_name_bypasses() {
    let p = policy(r#"{"discovery":{},"tools":[{"name":"search","cacheable":true,"ttl":90}]}"#);
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/call", None), &json!({}), &anon()).await;
    assert!(!is_cache(&d));
}

#[tokio::test]
async fn decide_tools_call_observed_unsafe_bypasses_even_if_allowlisted() {
    let p = policy(r#"{"discovery":{},"tools":[{"name":"writer","cacheable":true,"ttl":90}]}"#);
    let store = LocalStore::new(MockCache::new());
    // Observe a destructive annotation for `writer` via a passed-through list.
    let list = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[
        {"name":"writer","annotations":{"destructiveHint":true}}
    ]}}"#;
    record_from_list(&store, list).await;
    let d = decide(&p, &store, &req("tools/call", Some("writer")), &json!({}), &anon()).await;
    assert!(!is_cache(&d), "observed-destructive tool must bypass despite allowlist");
}

#[tokio::test]
async fn decide_identity_scope_bypasses_without_principal_or_session() {
    let p = policy(
        r#"{"discovery":{},"tools":[{"name":"me","cacheable":true,"ttl":90,"scope":"identity"}]}"#,
    );
    let store = LocalStore::new(MockCache::new());
    let d = decide(&p, &store, &req("tools/call", Some("me")), &json!({}), &anon()).await;
    assert!(!is_cache(&d), "identity scope with no principal/session must bypass");
}

#[tokio::test]
async fn decide_identity_scope_caches_with_principal() {
    let p = policy(
        r#"{"discovery":{},"tools":[{"name":"me","cacheable":true,"ttl":90,"scope":"identity"}]}"#,
    );
    let store = LocalStore::new(MockCache::new());
    let id = Identity { principal: Some("alice"), session: None };
    let d = decide(&p, &store, &req("tools/call", Some("me")), &json!({}), &id).await;
    assert!(is_cache(&d), "identity scope with a principal should cache");
}

// --- GossipStore (distributed backend) --------------------------------------
//
// Mirrors the LocalStore tests but exercises the gossip-safety rules: stale
// entries are NOT deleted on read (tombstones can race a re-populate), and
// puts are first-writer-wins via StoreMode::Absent.

use crate::store::GossipStore;
use pdk::data_storage::{DataStorage, DataStorageError, StoreMode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cloneable mock: clones share the same backing map, so a probe handle kept
/// outside the store can inspect what the store did (e.g. that a stale read did
/// NOT delete the row).
#[derive(Clone)]
struct MockDataStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    fail_get: Arc<AtomicBool>,
}
impl MockDataStorage {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            fail_get: Arc::new(AtomicBool::new(false)),
        }
    }
    fn contains(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }
}
impl DataStorage for MockDataStorage {
    async fn get_keys(&self) -> Result<Vec<String>, DataStorageError> {
        Ok(self.data.lock().unwrap().keys().cloned().collect())
    }
    async fn store<T: Serialize>(
        &self,
        key: &str,
        mode: &StoreMode,
        item: &T,
    ) -> Result<(), DataStorageError> {
        let bytes = serde_json::to_vec(item)
            .map_err(|e| DataStorageError::Unexpected(e.to_string()))?;
        let mut map = self.data.lock().unwrap();
        match mode {
            StoreMode::Always => {
                map.insert(key.to_string(), bytes);
            }
            StoreMode::Absent => {
                if map.contains_key(key) {
                    return Err(DataStorageError::CasMismatch);
                }
                map.insert(key.to_string(), bytes);
            }
            StoreMode::Cas(_) => {
                map.insert(key.to_string(), bytes);
            }
        }
        Ok(())
    }
    async fn get<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<(T, String)>, DataStorageError> {
        if self.fail_get.load(Ordering::SeqCst) {
            return Err(DataStorageError::Unexpected("injected".into()));
        }
        match self.data.lock().unwrap().get(key) {
            Some(bytes) => {
                let item: T = serde_json::from_slice(bytes)
                    .map_err(|e| DataStorageError::Unexpected(e.to_string()))?;
                Ok(Some((item, "1".to_string())))
            }
            None => Ok(None),
        }
    }
    async fn delete(&self, key: &str) -> Result<(), DataStorageError> {
        self.data.lock().unwrap().remove(key);
        Ok(())
    }
    async fn delete_all(&self) -> Result<(), DataStorageError> {
        self.data.lock().unwrap().clear();
        Ok(())
    }
}

#[tokio::test]
async fn gossip_store_roundtrip() {
    let store = GossipStore::new(MockDataStorage::new());
    let now = now_secs();
    let entry = CachedEntry { written_at: now, valid_until: now + 60, body: b"hi".to_vec() };
    store.put("k", &entry).await;
    let got = store.get("k").await.expect("hit");
    assert_eq!(got.body, b"hi");
}

#[tokio::test]
async fn gossip_store_stale_returns_none_without_tombstone() {
    // Keep a probe handle (clone shares backing) to inspect the row afterwards.
    let probe = MockDataStorage::new();
    let now = now_secs();
    let stale = CachedEntry { written_at: now - 100, valid_until: now - 1, body: b"x".to_vec() };
    probe.store("s", &StoreMode::Always, &stale).await.unwrap();

    let store = GossipStore::new(probe.clone());
    assert!(store.get("s").await.is_none(), "stale entry is a miss");
    // Gossip safety: no proactive delete on a stale read — the row must remain
    // (namespace TTL evicts it) so a delete tombstone can't race a re-populate.
    assert!(probe.contains("s"), "stale entry left in place for TTL eviction");
}

#[tokio::test]
async fn gossip_store_first_writer_wins() {
    let store = GossipStore::new(MockDataStorage::new());
    let now = now_secs();
    let first = CachedEntry { written_at: now, valid_until: now + 60, body: b"first".to_vec() };
    let second = CachedEntry { written_at: now, valid_until: now + 60, body: b"second".to_vec() };
    store.put("k", &first).await;
    // Second put with StoreMode::Absent hits CasMismatch and is swallowed.
    store.put("k", &second).await;
    let got = store.get("k").await.expect("hit");
    assert_eq!(got.body, b"first", "first writer wins; concurrent put does not clobber");
}

#[tokio::test]
async fn gossip_store_get_error_is_miss() {
    let mock = MockDataStorage::new();
    mock.fail_get.store(true, Ordering::SeqCst);
    let store = GossipStore::new(mock);
    assert!(store.get("k").await.is_none(), "storage error falls open to a miss");
}
