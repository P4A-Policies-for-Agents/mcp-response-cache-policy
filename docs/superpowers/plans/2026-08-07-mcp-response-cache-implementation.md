# MCP Response Cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the approved MCP Response Cache scaffold into a working policy that serves side-effect-free MCP responses (discovery methods + allowlisted read-only `tools/call`) from a gateway-local cache instead of re-hitting the upstream MCP server.

**Architecture:** A `CacheStore` trait with two backends (`LocalStore` over the sync PDK `Cache`, `GossipStore` over the async PDK `DataStorage` remote) is selected once in `configure()` by the `distributed` flag; all filter code is generic over the trait. The request filter parses the JSON-RPC envelope, decides cacheability (config allowlist + discovery toggle + guardrails), builds a SHA-256 key, and on a hit returns `Flow::Break` with the stored result re-stamped to the live request `id`; on a miss it threads a `MissCtx` to the response filter via `Flow::Continue`, which parses the JSON-RPC response and stores it. Tool annotations observed on passing-through `tools/list` results add a defense-in-depth non-cacheable set.

**Tech Stack:** Rust → `wasm32-wasip1`, PDK 1.9.2 (`pdk::hl`, `pdk::cache::Cache`/`CacheBuilder`, `pdk::data_storage::{DataStorage,DataStorageBuilder,StoreMode}`), `serde`/`serde_json` (with `raw_value`), `sha2`, `hex`, `anyhow`. Unit tests via `pdk-unit`; integration via `pdk-test` + `httpmock` + `reqwest`.

## Global Constraints

- **PDK floor:** `pdk = { version = "1.9.2" }`; `MIN_FLEX_VERSION := 1.9.3` (already set). Repo must stay public.
- **PDK-first:** prefer PDK primitives over std/third-party. Allowed non-PDK crates are only those already in `Cargo.toml` (`serde`, `serde_json`, `anyhow`, `mime`, `sha2`, `hex`); do **not** add dependencies without updating this plan.
- **Build/test via `make`, never raw `cargo`** at the workflow level: `make build` (compiles wasm + packages), `make test` (builds then `cargo test -- --nocapture`). Per-test iteration may use `cargo test <name>` locally, but a task's "verify" step is not done until `make build` / `make test` pass.
- **No time/random gotchas:** PDK has no `pdk::time`; use `std::time::SystemTime` for timestamps (works under wasm). Never `unwrap`/`panic` on context-derived values (no control plane at t=0 must not crash).
- **`Cache` is synchronous** (`&self`, no `.await`); **`DataStorage` is async** (every method `.await`s). The `CacheStore` trait is async; `LocalStore` wraps the sync `Cache` in async methods that don't actually await.
- **Body reads:** `state.handler().body()` returns `Vec<u8>`; header reads `handler().header(name)` return `Option<String>`; header names are lowercase.
- **`Response::with_body`** takes `impl Into<Vec<u8>>`; **`with_headers`** takes `Vec<(String, String)>`.
- **`config.rs` is regenerated** by the pipeline from `gcl.yaml`; keep the hand-maintained copy in sync but do not rely on hand edits surviving deploy. **Do not change `gcl.yaml` property names** (Rust-keyword collision + serde-alias contract).
- **Gossip safety (distributed mode):** never `delete` before re-create (tombstone race); no proactive deletes on expiry (rely on namespace TTL); `put` uses `StoreMode::Absent` (first-writer-wins); treat `DataStorageError::CasMismatch` as "already populated", not an error.
- **Fail-open always:** any parse/cache error, unknown method, or missing body → pass through (`Flow::Continue`) and never block traffic.
- **Copy/branding:** display name "MCP Response Cache"; slugs unchanged (`mcp-response-cache-policy`, assetId `mcp-response-cache`, crate `mcp_response_cache_policy`). Keep "Flex" terminology in code/config/tests (toolchain not yet rebranded).
- **Observability:** every handled response carries `x-mcp-cache: hit | miss | bypass`.

---

## File Structure

Under `implementation/src/` (crate `mcp_response_cache_policy`):

| File | Responsibility | Status |
|---|---|---|
| `lib.rs` | `#[entrypoint] configure()`: parse config, build the selected `CacheStore`, wire `on_request`/`on_response`, launch. Request filter (recognition → cacheability → guardrails → key → get → hit/miss). Response filter (parse response → store; observe annotations). | exists (scaffold) — rewrite filters |
| `mcp.rs` | JSON-RPC vocabulary + request parse (`parse_request`) [exists] + **response parse** (`parse_response`) + **hit-response envelope builder** (`build_hit_response_body`) + `readOnlyHint`/`destructiveHint` extraction from a `tools/list` result. | exists — extend |
| `key.rs` | Canonicalize `params` (recursive key sort), SHA-256 base hash, scope-aware key assembly (`shared` vs `identity` with principal/session hashing). | **new** |
| `store.rs` | `CacheStore` async trait; `CachedEntry` (serde); `LocalStore` (sync `Cache`, embedded-expiry lazy eviction); `GossipStore` (`DataStorage` remote, `Absent` put, no proactive delete). Lazy-expiry helper. | **new** |
| `annotations.rs` | Observed-annotation store: derive tool safety from a `tools/list` result, persist a short-TTL non-cacheable marker via `CacheStore`, check it before caching a `tools/call`. | **new** |
| `generated/config.rs` | Config structs (mirrors `gcl.yaml`). | exists — unchanged |
| `tests.rs` | Unit tests: config [exists], key canonicalization/scope, guardrail matrix, `CachedEntry` expiry, hit re-stamp, `MockCacheStore`. | exists — extend |
| `tests/requests.rs` | Integration (`pdk-test`): miss→store→hit round-trip with `mock.assert_hits`, bypass header, discovery TTL, non-allowlisted pass-through. | **new** |
| `tests/common/mod.rs` | Shared test constants (`POLICY_NAME`, `POLICY_DIR`). | **new** |

Build order (dependency-respecting): `key.rs` → `store.rs` → `mcp.rs` extensions → `annotations.rs` → `lib.rs` filters → integration tests. Each task compiles and tests green on its own.

---

### Task 1: Key construction (`key.rs`)

**Files:**
- Create: `implementation/src/key.rs`
- Modify: `implementation/src/lib.rs` (add `mod key;` near the other `mod` lines)
- Test: `implementation/src/tests.rs` (append key tests)

**Interfaces:**
- Consumes: `crate::generated::config::CacheScope` (existing enum `Shared | Identity`).
- Produces:
  - `pub fn canonicalize(params: &serde_json::Value) -> String` — deterministic serialization with recursively sorted object keys.
  - `pub struct Identity<'a> { pub principal: Option<&'a str>, pub session: Option<&'a str> }`
  - `pub fn cache_key(method: &str, params: &serde_json::Value, scope: CacheScope, identity: &Identity) -> Option<String>` — returns `None` when `scope == Identity` but neither principal nor session is present (caller must bypass).

- [ ] **Step 1: Write the failing tests**

Append to `implementation/src/tests.rs`:

```rust
use crate::key::{canonicalize, cache_key, Identity};
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd implementation && cargo test --lib key`
Expected: FAIL — `unresolved import crate::key` / functions not defined.

- [ ] **Step 3: Implement `key.rs`**

Create `implementation/src/key.rs`:

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Cache-key construction: canonicalize JSON-RPC params (order-independent),
// SHA-256 the (method, params) tuple, and assemble a scope-aware key. Sensitive
// identity values participate only as hashes, never raw.

use crate::generated::config::CacheScope;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// Record separator between method and params inside the hash preimage.
const RS: u8 = 0x1e;

/// Caller identity for `identity`-scoped keys. Both fields optional; the key
/// degrades to whichever is present.
pub struct Identity<'a> {
    pub principal: Option<&'a str>,
    pub session: Option<&'a str>,
}

/// Serialize `params` with all object keys recursively sorted, so `{a,b}` and
/// `{b,a}` collapse to one string. Arrays keep their order (order is semantic).
pub fn canonicalize(params: &Value) -> String {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                let mut out = Map::new();
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                for k in keys {
                    out.insert(k.clone(), sorted(&m[k]));
                }
                Value::Object(out)
            }
            Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    // Serialization of a canonicalized Value is infallible in practice; fall
    // back to the params' own string form rather than panicking.
    serde_json::to_string(&sorted(params)).unwrap_or_default()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Build the cache key. Returns `None` for `identity` scope when neither
/// principal nor session is available (caller must bypass — never cache under
/// a weak key).
pub fn cache_key(
    method: &str,
    params: &Value,
    scope: CacheScope,
    identity: &Identity,
) -> Option<String> {
    let canon = canonicalize(params);
    let mut preimage = Vec::with_capacity(method.len() + 1 + canon.len());
    preimage.extend_from_slice(method.as_bytes());
    preimage.push(RS);
    preimage.extend_from_slice(canon.as_bytes());
    let base = sha256_hex(&preimage);

    match scope {
        CacheScope::Shared => Some(format!("{method}:{base}")),
        CacheScope::Identity => {
            if identity.principal.is_none() && identity.session.is_none() {
                return None;
            }
            let p = sha256_hex(identity.principal.unwrap_or("").as_bytes());
            let s = sha256_hex(identity.session.unwrap_or("").as_bytes());
            Some(format!("{method}:{base}:{p}:{s}"))
        }
    }
}
```

Add to `implementation/src/lib.rs` alongside the existing `mod` lines:

```rust
mod key;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd implementation && cargo test --lib key`
Expected: PASS (6 key tests).

- [ ] **Step 5: Commit**

```bash
git add implementation/src/key.rs implementation/src/lib.rs implementation/src/tests.rs
git commit -m "feat(cache): add scope-aware SHA-256 key construction"
```

---

### Task 2: Cache backend abstraction (`store.rs`)

**Files:**
- Create: `implementation/src/store.rs`
- Modify: `implementation/src/lib.rs` (`mod store;`)
- Test: `implementation/src/tests.rs` (append `CachedEntry` + `MockCacheStore` tests)

**Interfaces:**
- Consumes: `pdk::cache::Cache`, `pdk::data_storage::{DataStorage, StoreMode, DataStorageError}` (PDK 1.9.2).
- Produces:
  - `pub struct CachedEntry { pub written_at: u64, pub valid_until: u64, pub body: Vec<u8> }` (serde `Serialize`/`Deserialize`), with `pub fn is_fresh(&self, now: u64) -> bool`.
  - `pub trait CacheStore { async fn get(&self, key: &str) -> Option<CachedEntry>; async fn put(&self, key: &str, entry: &CachedEntry); }`
  - `pub struct LocalStore<C: Cache> { cache: C }` implementing `CacheStore` (lazy expiry: on read, if stale, `delete` and return `None` — safe, single-replica).
  - `pub struct GossipStore<S: DataStorage> { storage: S }` implementing `CacheStore` (put via `StoreMode::Absent`, `CasMismatch` → treat as already-populated; read: if stale return `None` **without** deleting).
  - `pub fn now_secs() -> u64` — `SystemTime` seconds since epoch.

- [ ] **Step 1: Write the failing tests**

Append to `implementation/src/tests.rs`:

```rust
use crate::store::{now_secs, CacheStore, CachedEntry, LocalStore};
use pdk::cache::{Cache, CacheError};
use std::collections::HashMap;
use std::sync::Mutex;

struct MockCache {
    data: Mutex<HashMap<String, Vec<u8>>>,
}
impl MockCache {
    fn new() -> Self { Self { data: Mutex::new(HashMap::new()) } }
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
    fn purge(&self) { self.data.lock().unwrap().clear(); }
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
```

Add `tokio` as a dev-dependency for `#[tokio::test]` (async unit tests). In `implementation/Cargo.toml` under `[dev-dependencies]`:

```toml
tokio = { version = "1", features = ["macros", "rt"] }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd implementation && cargo test --lib store`
Expected: FAIL — `crate::store` unresolved.

- [ ] **Step 3: Implement `store.rs`**

Create `implementation/src/store.rs`:

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// CacheStore abstraction over the two PDK storage primitives, selected at
// configure() time by the `distributed` flag. All filter code is generic over
// this trait; the backend choice lives only in configure().
//
// LocalStore wraps the synchronous PDK `Cache` (single-replica, native LRU via
// max_entries) and does lazy expiry with eviction. GossipStore wraps the async
// `DataStorage` remote backend (cross-replica via gossip) and NEVER proactively
// deletes (tombstones can race a concurrent write); it relies on the namespace
// TTL and stores first-writer-wins via StoreMode::Absent.

use pdk::cache::Cache;
use pdk::data_storage::{DataStorage, DataStorageError, StoreMode};
use pdk::logger;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch. PDK exposes no time module; SystemTime works
/// under wasm32-wasip1.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A cached JSON-RPC result body plus embedded expiry. The body is the raw
/// upstream response bytes (single-shot JSON); the id is re-stamped at hit time
/// by the caller, not stored here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEntry {
    pub written_at: u64,
    pub valid_until: u64,
    pub body: Vec<u8>,
}

impl CachedEntry {
    pub fn is_fresh(&self, now: u64) -> bool {
        now < self.valid_until
    }
}

/// Backend-agnostic cache seam. Async so the gossip backend fits; the local
/// backend's methods complete synchronously inside the async signature.
pub trait CacheStore {
    async fn get(&self, key: &str) -> Option<CachedEntry>;
    async fn put(&self, key: &str, entry: &CachedEntry);
}

// --- LocalStore -------------------------------------------------------------

pub struct LocalStore<C: Cache> {
    cache: C,
}

impl<C: Cache> LocalStore<C> {
    pub fn new(cache: C) -> Self {
        Self { cache }
    }
}

impl<C: Cache> CacheStore for LocalStore<C> {
    async fn get(&self, key: &str) -> Option<CachedEntry> {
        let bytes = self.cache.get(key)?;
        let entry: CachedEntry = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(_) => {
                // Corrupt entry: evict and miss (safe, single-replica).
                self.cache.delete(key);
                return None;
            }
        };
        if !entry.is_fresh(now_secs()) {
            self.cache.delete(key);
            return None;
        }
        Some(entry)
    }

    async fn put(&self, key: &str, entry: &CachedEntry) {
        match serde_json::to_vec(entry) {
            Ok(bytes) => {
                if let Err(e) = self.cache.save(key, bytes) {
                    logger::warn!("cache save failed for '{}': {}", key, e);
                }
            }
            Err(e) => logger::warn!("cache serialize failed for '{}': {}", key, e),
        }
    }
}

// --- GossipStore ------------------------------------------------------------

pub struct GossipStore<S: DataStorage> {
    storage: S,
}

impl<S: DataStorage> GossipStore<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S: DataStorage> CacheStore for GossipStore<S> {
    async fn get(&self, key: &str) -> Option<CachedEntry> {
        let (entry, _cas): (CachedEntry, String) = match self.storage.get(key).await {
            Ok(Some(pair)) => pair,
            Ok(None) => return None,
            Err(e) => {
                logger::warn!("data storage get failed for '{}': {:?}", key, e);
                return None;
            }
        };
        if !entry.is_fresh(now_secs()) {
            // Do NOT delete under gossip: the namespace TTL evicts it; a delete
            // here creates a tombstone that can race a concurrent re-populate.
            return None;
        }
        Some(entry)
    }

    async fn put(&self, key: &str, entry: &CachedEntry) {
        // First-writer-wins: never clobber a concurrent populate.
        match self.storage.store(key, &StoreMode::Absent, entry).await {
            Ok(()) => {}
            Err(DataStorageError::CasMismatch) => {
                // Another replica populated it first — fine, it's the same data.
            }
            Err(e) => logger::warn!("data storage put failed for '{}': {:?}", key, e),
        }
    }
}
```

Add to `implementation/src/lib.rs`:

```rust
mod store;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd implementation && cargo test --lib store`
Expected: PASS (`cached_entry_freshness`, `local_store_roundtrip_and_expiry`).

- [ ] **Step 5: Verify wasm build still compiles**

Run: `cd implementation && cargo build --target wasm32-wasip1 --release`
Expected: builds (dev-only `tokio` must not leak into the cdylib — it's under `[dev-dependencies]`).

- [ ] **Step 6: Commit**

```bash
git add implementation/src/store.rs implementation/src/lib.rs implementation/src/tests.rs implementation/Cargo.toml
git commit -m "feat(cache): add CacheStore trait with Local and Gossip backends"
```

---

### Task 3: Response parsing + hit-response builder (`mcp.rs` extensions)

**Files:**
- Modify: `implementation/src/mcp.rs` (add response-side helpers below the existing request parser; keep `#![allow(dead_code)]`)
- Test: `implementation/src/mcp.rs` (append to its existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `serde_json::Value`, existing `RequestId`.
- Produces:
  - `pub fn is_cacheable_response(body: &[u8]) -> bool` — true only if body parses as JSON-RPC 2.0, has a `result`, no `error`, and `result.isError != true`.
  - `pub fn restamp_id(body: &[u8], id: &RequestId) -> Option<Vec<u8>>` — replace the response's `id` with the live request's id; returns re-serialized bytes (`None` if body isn't a JSON object).
  - `pub fn tool_safety_from_list(result_body: &[u8]) -> Vec<(String, bool)>` — from a `tools/list` result, return `(tool_name, is_safe)` where `is_safe = readOnlyHint == true && destructiveHint != true`.

- [ ] **Step 1: Write the failing tests**

Append inside the existing `mod tests` in `implementation/src/mcp.rs`:

```rust
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
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], serde_json::json!("live"));
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
}

#[test]
fn restamp_null_id() {
    let stored = br#"{"jsonrpc":"2.0","id":7,"result":{}}"#;
    let out = restamp_id(stored, &RequestId::Null).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], serde_json::Value::Null);
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd implementation && cargo test --lib mcp`
Expected: FAIL — new functions not defined.

- [ ] **Step 3: Implement the helpers**

Append to `implementation/src/mcp.rs` (after `parse_request`, before `#[cfg(test)]`):

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd implementation && cargo test --lib mcp`
Expected: PASS (existing 4 + new 5).

- [ ] **Step 5: Commit**

```bash
git add implementation/src/mcp.rs
git commit -m "feat(mcp): add response parse, id re-stamp, and tool-annotation extraction"
```

---

### Task 4: Observed-annotation defense-in-depth (`annotations.rs`)

**Files:**
- Create: `implementation/src/annotations.rs`
- Modify: `implementation/src/lib.rs` (`mod annotations;`)
- Test: `implementation/src/tests.rs` (append)

**Interfaces:**
- Consumes: `crate::store::CacheStore`, `crate::store::CachedEntry`, `crate::store::now_secs`, `crate::mcp::tool_safety_from_list`.
- Produces:
  - `pub const ANNOTATION_TTL_SECS: u64 = 300;`
  - `pub fn annotation_key(tool: &str) -> String` → `"ann:unsafe:{tool}"`.
  - `pub async fn record_from_list<S: CacheStore>(store: &S, result_body: &[u8])` — for each tool observed **unsafe**, persist a short-TTL marker entry (body = `b"1"`).
  - `pub async fn is_known_unsafe<S: CacheStore>(store: &S, tool: &str) -> bool` — true iff an unexpired unsafe-marker exists.

Rationale: annotations live on `tools/list` *responses*, not on `tools/call` *requests*. The primary safety gate is the operator allowlist (default `cacheable:false`); this layer only ever *removes* caching for a tool observed destructive. Absence of knowledge defers to the allowlist (never fail-closed on unknown tools).

- [ ] **Step 1: Write the failing tests**

Append to `implementation/src/tests.rs` (reuses `MockCache` from Task 2):

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd implementation && cargo test --lib annotations`
Expected: FAIL — `crate::annotations` unresolved.

- [ ] **Step 3: Implement `annotations.rs`**

Create `implementation/src/annotations.rs`:

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Defense-in-depth: MCP tool annotations (readOnlyHint / destructiveHint) live
// on the tools/list RESPONSE, not on the tools/call REQUEST the policy decides
// about. When the policy observes a tool marked non-read-only / destructive in
// a tools/list result it passes through, it records that tool as unsafe (short
// TTL). A later tools/call for that tool is refused caching even if the
// operator allowlisted it. Absence of knowledge defers to the allowlist.

use crate::mcp::tool_safety_from_list;
use crate::store::{now_secs, CacheStore, CachedEntry};

/// Marker TTL. Refreshed each time discovery passes through.
pub const ANNOTATION_TTL_SECS: u64 = 300;

pub fn annotation_key(tool: &str) -> String {
    format!("ann:unsafe:{tool}")
}

/// Persist an unsafe-marker for every tool the list reports as unsafe.
pub async fn record_from_list<S: CacheStore>(store: &S, result_body: &[u8]) {
    let now = now_secs();
    for (name, is_safe) in tool_safety_from_list(result_body) {
        if !is_safe {
            let entry = CachedEntry {
                written_at: now,
                valid_until: now + ANNOTATION_TTL_SECS,
                body: b"1".to_vec(),
            };
            store.put(&annotation_key(&name), &entry).await;
        }
    }
}

/// True iff an unexpired unsafe-marker exists for this tool.
pub async fn is_known_unsafe<S: CacheStore>(store: &S, tool: &str) -> bool {
    store.get(&annotation_key(tool)).await.is_some()
}
```

Add to `implementation/src/lib.rs`:

```rust
mod annotations;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd implementation && cargo test --lib annotations`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add implementation/src/annotations.rs implementation/src/lib.rs implementation/src/tests.rs
git commit -m "feat(cache): add observed-annotation defense-in-depth layer"
```

---

### Task 5: Wire the request/response lifecycle (`lib.rs`)

**Files:**
- Modify: `implementation/src/lib.rs` (replace the scaffold `request_filter`/`response_filter`/`configure` with the real lifecycle; keep the module declarations and header comment)
- Test: covered by unit tests already written (compile gate) + Task 6 integration.

**Interfaces:**
- Consumes: `key::{cache_key, Identity}`, `store::{CacheStore, CachedEntry, LocalStore, GossipStore, now_secs}`, `mcp::{parse_request, is_discovery_method, is_notification, is_cacheable_response, restamp_id, tool_safety_from_list, RequestId, TOOLS_CALL, TOOLS_LIST}`, `annotations::{record_from_list, is_known_unsafe, ANNOTATION_TTL_SECS}`, `generated::config::{Config, CacheScope}`, PDK `hl` + `cache::CacheBuilder` + `data_storage::DataStorageBuilder`.
- Produces: the launched policy. Threading type from request→response filter: `MissCtx { key: String, ttl: u64, method: String, tool_name: Option<String> }` carried in `Flow::Continue(MissCtx)` and read back as `RequestData<MissCtx>`.

Design notes for the implementer (all derived from the approved architecture §4–§5):
- **Recognition:** only `POST` + content-type containing `json`. Else `Flow::Continue(MissCtx::bypass())` and set `x-mcp-cache: bypass` header via the request handler's `add_header` on the *request* is not visible to the client; the header must be added on the **response**. So carry a `disposition` in `MissCtx` and stamp the header in the response filter. (Hits stamp their own header on the `Response`.)
- **Bypass** if `cache-control` request header contains `no-cache`.
- **Discovery** cached iff `config.discovery.cacheable && config.discovery.ttl > 0`, scope always `Shared`, ttl = `discovery.ttl`.
- **`tools/call`** cached iff tool present in `config.tools` with `cacheable: true` **and** not `is_known_unsafe`; ttl/scope from the tool entry.
- **Identity:** principal from `x-forwarded-user` (fallback `authorization` absent → None); session from `mcp-session-id` header. If scope is `Identity` and `cache_key` returns `None`, bypass.
- **Notifications / non-cacheable methods:** pass through as bypass.
- **Response filter:** only acts on `RequestData::Continue(ctx)` where `ctx.disposition == Miss`. Skips if response content-type is `text/event-stream`. If `is_cacheable_response`, store `CachedEntry`. Regardless, if the method was `tools/list`, call `record_from_list`. Always stamp `x-mcp-cache` from the disposition.

- [ ] **Step 1: Replace `lib.rs` with the full lifecycle**

Overwrite `implementation/src/lib.rs` (preserve the copyright/header comment block and the `mod` lines; the body below is the new implementation):

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// MCP Response Cache — MuleSoft Omni Gateway custom policy (PDK).
//
// Caches side-effect-free MCP responses (discovery methods and allowlisted
// read-only tools/call) at the gateway so repeated agent calls are served
// locally instead of re-hitting the upstream MCP server.

mod annotations;
mod generated;
mod key;
mod mcp;
mod store;

use crate::generated::config::{CacheScope, Config, ToolConfig};
use crate::key::{cache_key, Identity};
use crate::mcp::{
    is_cacheable_response, is_discovery_method, is_notification, parse_request, restamp_id,
    McpRequest, RequestId, TOOLS_CALL, TOOLS_LIST,
};
use crate::store::{now_secs, CacheStore, CachedEntry, GossipStore, LocalStore};
use anyhow::{anyhow, Result};
use pdk::cache::CacheBuilder;
use pdk::data_storage::DataStorageBuilder;
use pdk::hl::*;
use pdk::logger;
use std::collections::HashMap;

const POLICY_NAME: &str = "mcp-response-cache-policy";
const CACHE_ID: &str = "mcp-response-cache";
const HEADER: &str = "x-mcp-cache";

/// Disposition threaded to the response filter and used to stamp `x-mcp-cache`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Miss,
    Bypass,
}

impl Disposition {
    fn header_value(self) -> &'static str {
        match self {
            Disposition::Miss => "miss",
            Disposition::Bypass => "bypass",
        }
    }
}

/// Context carried from request filter to response filter on a non-hit.
#[derive(Clone)]
struct MissCtx {
    disposition: Disposition,
    /// Present only when disposition == Miss and the response should be stored.
    key: Option<String>,
    ttl: u64,
    /// Method string, so the response filter can observe tools/list annotations.
    method: String,
}

impl MissCtx {
    fn bypass() -> Self {
        Self { disposition: Disposition::Bypass, key: None, ttl: 0, method: String::new() }
    }
}

/// Immutable per-worker view of config, precomputed for O(1) tool lookup.
struct Policy {
    discovery_cacheable: bool,
    discovery_ttl: u64,
    tools: HashMap<String, ToolConfig>,
}

impl Policy {
    fn new(config: &Config) -> Self {
        let tools = config
            .tools
            .iter()
            .cloned()
            .map(|t| (t.name.clone(), t))
            .collect();
        Self {
            discovery_cacheable: config.discovery.cacheable && config.discovery.ttl > 0,
            discovery_ttl: config.discovery.ttl,
            tools,
        }
    }
}

/// Decision for a parsed request: cache under `key`/`ttl`, or bypass.
enum Decision {
    Cache { key: String, ttl: u64 },
    Bypass,
}

async fn decide<S: CacheStore>(
    policy: &Policy,
    store: &S,
    req: &McpRequest,
    params: &serde_json::Value,
    identity: &Identity<'_>,
) -> Decision {
    // Notifications and unknown methods never cache.
    if is_notification(&req.method) {
        return Decision::Bypass;
    }

    let (ttl, scope) = if is_discovery_method(&req.method) {
        if !policy.discovery_cacheable {
            return Decision::Bypass;
        }
        (policy.discovery_ttl, CacheScope::Shared)
    } else if req.method == TOOLS_CALL {
        let name = match &req.tool_name {
            Some(n) => n,
            None => return Decision::Bypass,
        };
        let tool = match policy.tools.get(name) {
            Some(t) if t.cacheable => t,
            _ => return Decision::Bypass,
        };
        // Defense-in-depth: refuse a tool observed destructive even if allowlisted.
        if crate::annotations::is_known_unsafe(store, name).await {
            return Decision::Bypass;
        }
        (tool.ttl, tool.scope.clone())
    } else {
        return Decision::Bypass;
    };

    match cache_key(&req.method, params, scope, identity) {
        Some(key) => Decision::Cache { key, ttl },
        None => Decision::Bypass, // identity scope with no principal/session
    }
}

async fn request_filter<S: CacheStore>(
    request_state: RequestState,
    policy: &Policy,
    store: &S,
) -> Flow<MissCtx> {
    let headers_state = request_state.into_headers_state().await;
    let handler = headers_state.handler();

    // Recognition: only POST + JSON is a candidate.
    if headers_state.method().as_str() != "POST" {
        return Flow::Continue(MissCtx::bypass());
    }
    match handler.header("content-type") {
        Some(ct) if ct.contains("json") => {}
        _ => return Flow::Continue(MissCtx::bypass()),
    }

    // Transport-level bypass hint.
    if handler
        .header("cache-control")
        .map(|v| v.contains("no-cache"))
        .unwrap_or(false)
    {
        return Flow::Continue(MissCtx::bypass());
    }

    // Capture identity headers before consuming the state for the body.
    let principal = handler.header("x-forwarded-user");
    let session = handler.header("mcp-session-id");

    let body_state = headers_state.into_body_state().await;
    let body = body_state.handler().body();

    let req = match parse_request(&body) {
        Some(r) => r,
        None => return Flow::Continue(MissCtx::bypass()), // not MCP JSON-RPC
    };

    // params for keying (default to null object when absent).
    let params: serde_json::Value = serde_json::from_slice(&body)
        .ok()
        .and_then(|v: serde_json::Value| v.get("params").cloned())
        .unwrap_or(serde_json::Value::Null);

    let identity = Identity {
        principal: principal.as_deref(),
        session: session.as_deref(),
    };

    match decide(policy, store, &req, &params, &identity).await {
        Decision::Bypass => Flow::Continue(MissCtx {
            disposition: Disposition::Bypass,
            key: None,
            ttl: 0,
            method: req.method,
        }),
        Decision::Cache { key, ttl } => {
            if let Some(entry) = store.get(&key).await {
                // HIT: re-stamp id and short-circuit.
                if let Some(body) = restamp_id(&entry.body, &req.id) {
                    let resp = Response::new(200)
                        .with_headers(vec![
                            ("content-type".to_string(), "application/json".to_string()),
                            (HEADER.to_string(), "hit".to_string()),
                        ])
                        .with_body(body);
                    return Flow::Break(resp);
                }
            }
            // MISS: forward, remember where to store.
            Flow::Continue(MissCtx {
                disposition: Disposition::Miss,
                key: Some(key),
                ttl,
                method: req.method,
            })
        }
    }
}

async fn response_filter<S: CacheStore>(
    response_state: ResponseState,
    request_data: RequestData<MissCtx>,
    store: &S,
) {
    let ctx = match request_data {
        RequestData::Continue(ctx) => ctx,
        _ => return,
    };

    let headers_state = response_state.into_headers_state().await;
    // Always surface the disposition to the client.
    headers_state
        .handler()
        .set_header(HEADER, ctx.disposition.header_value());

    // Only a genuine miss with a key does storage work.
    let key = match (ctx.disposition, ctx.key.as_ref()) {
        (Disposition::Miss, Some(k)) => k.clone(),
        _ => return,
    };

    // Never buffer SSE.
    if headers_state
        .handler()
        .header("content-type")
        .map(|ct| ct.contains("text/event-stream"))
        .unwrap_or(false)
    {
        return;
    }

    let body_state = headers_state.into_body_state().await;
    let body = body_state.handler().body();

    // Observe tools/list annotations regardless of whether we store the result.
    if ctx.method == TOOLS_LIST {
        crate::annotations::record_from_list(store, &body).await;
    }

    if is_cacheable_response(&body) {
        let now = now_secs();
        let entry = CachedEntry {
            written_at: now,
            valid_until: now + ctx.ttl,
            body,
        };
        store.put(&key, &entry).await;
    }
}

/// Launch the policy generically over the selected backend.
async fn launch_policy<S: CacheStore + 'static>(
    launcher: Launcher,
    policy: Policy,
    store: S,
) -> Result<()> {
    let policy = std::rc::Rc::new(policy);
    let store = std::rc::Rc::new(store);

    let req_policy = policy.clone();
    let req_store = store.clone();
    let resp_store = store.clone();

    let filter = on_request(move |rs| {
        let policy = req_policy.clone();
        let store = req_store.clone();
        async move { request_filter(rs, &policy, &store).await }
    })
    .on_response(move |rs, req_data| {
        let store = resp_store.clone();
        async move { response_filter(rs, req_data, &store).await }
    });

    launcher.launch(filter).await?;
    Ok(())
}

#[entrypoint]
async fn configure(
    launcher: Launcher,
    Configuration(bytes): Configuration,
    cache_builder: CacheBuilder,
    store_builder: DataStorageBuilder,
) -> Result<()> {
    let config: Config = serde_json::from_slice(&bytes).map_err(|err| {
        anyhow!(
            "Failed to parse configuration '{}'. Cause: {}",
            String::from_utf8_lossy(&bytes),
            err
        )
    })?;

    logger::info!(
        "[{}] configured (distributed={}, max_entries={}, tools={}, discovery.cacheable={}, discovery.ttl={}s)",
        POLICY_NAME,
        config.distributed,
        config.max_entries,
        config.tools.len(),
        config.discovery.cacheable,
        config.discovery.ttl,
    );

    let policy = Policy::new(&config);

    if config.distributed {
        // Namespace TTL bounds the whole cache; use the max configured ttl,
        // with a floor so discovery-only configs still get a sane window.
        let ttl_secs = config
            .tools
            .iter()
            .map(|t| t.ttl)
            .chain(std::iter::once(config.discovery.ttl))
            .max()
            .unwrap_or(60)
            .max(60);
        let storage = store_builder.remote(CACHE_ID, (ttl_secs as u32).saturating_mul(1000));
        launch_policy(launcher, policy, GossipStore::new(storage)).await
    } else {
        let cache = cache_builder
            .new(CACHE_ID.to_string())
            .max_entries(config.max_entries as usize)
            .build();
        launch_policy(launcher, policy, LocalStore::new(cache)).await
    }
}

#[cfg(test)]
mod tests;
```

- [ ] **Step 2: Verify unit tests still compile & pass**

Run: `cd implementation && cargo test --lib`
Expected: PASS — all unit tests (config, key, store, mcp, annotations) green.

- [ ] **Step 3: Verify the wasm build**

Run: `cd implementation && make build`
Expected: compiles to `wasm32-wasip1` and packages without error. (If `make build` needs Exchange/`build-asset-files`, run `cargo build --target wasm32-wasip1 --release` to gate the code compile, then `make build` when toolchain/auth is available.)

- [ ] **Step 4: Commit**

```bash
git add implementation/src/lib.rs
git commit -m "feat(cache): wire request/response cache lifecycle with backend selection"
```

---

### Task 6: Integration tests (`tests/requests.rs`)

**Files:**
- Create: `implementation/tests/common/mod.rs`
- Create: `implementation/tests/requests.rs`
- Modify: `implementation/Cargo.toml` (add `[dev-dependencies]`: `pdk-test`, `httpmock`, `reqwest`, `tokio` if not present, `anyhow`)

**Interfaces:**
- Consumes: the built policy WASM (via `pdk-test` harness), `POLICY_NAME`/`POLICY_DIR` constants.
- Produces: end-to-end assertions proving a cache hit does not reach the upstream.

- [ ] **Step 1: Add integration dev-dependencies**

In `implementation/Cargo.toml` `[dev-dependencies]` (keep the existing `pdk-unit` and the `tokio` from Task 2):

```toml
pdk-test = { version = "1.9.2" }
httpmock = "0.7"
reqwest = { version = "0.12", features = ["json"] }
anyhow = "1.0"
```

(If a version pin fails to resolve, match the pin used by a sibling policy's `Cargo.toml` in this repo rather than inventing one.)

- [ ] **Step 2: Create shared test constants**

Create `implementation/tests/common/mod.rs`:

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
// Shared constants for integration tests.

pub const POLICY_NAME: &str = "mcp-response-cache-policy";
// Points pdk-test at the built policy package under target/.
pub const POLICY_DIR: &str = "target/mcp-response-cache-policy";
```

- [ ] **Step 3: Write the failing integration test**

Create `implementation/tests/requests.rs`:

```rust
// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Integration tests: prove miss→store→hit round-trips and that a hit does not
// reach the upstream MCP server. Requires Docker (pdk-test spins up Flex).

mod common;

use common::POLICY_NAME;
use httpmock::prelude::*;
use pdk_test::pdk_test;
use pdk_test::services::flex::{ApiConfig, Flex, FlexConfig, PolicyConfig};
use pdk_test::services::httpbin::{HttpBinConfig, HttpMock};
use serde_json::json;

const DISCOVERY_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;

#[pdk_test]
async fn discovery_hit_skips_upstream() -> anyhow::Result<()> {
    let policy = PolicyConfig::builder()
        .name(POLICY_NAME)
        .configuration(json!({
            "discovery": { "cacheable": true, "ttl": 60 },
            "tools": [],
            "maxEntries": 100,
            "distributed": false
        }))
        .build();

    // Build Flex with the policy in front of a mock MCP upstream.
    let config = FlexConfig::builder()
        .version("1.9.3")
        .hostname("local-flex")
        .with_api(
            ApiConfig::builder()
                .name("mcp-api")
                .upstream(&HttpMock::service_name())
                .path("/")
                .port(80)
                .policy(policy)
                .build(),
        )
        .build();

    let mock_cfg = HttpBinConfig::builder().hostname("backend").build();

    let composite = pdk_test::TestComposite::builder()
        .with_flex(config)
        .with_httpmock(mock_cfg)
        .build()
        .await?;

    let flex: Flex = composite.service()?;
    let mock_server: HttpMock = composite.service()?;
    let server = MockServer::connect_async(mock_server.socket()).await;

    let upstream = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"search"}]}}"#);
        })
        .await;

    let flex_url = format!("http://{}", flex.external_url()?);
    let client = reqwest::Client::new();

    // First call: MISS → reaches upstream.
    let r1 = client
        .post(&flex_url)
        .header("content-type", "application/json")
        .body(DISCOVERY_BODY)
        .send()
        .await?;
    assert_eq!(r1.headers().get("x-mcp-cache").unwrap(), "miss");

    // Second identical call: HIT → served locally, upstream NOT hit again.
    let r2 = client
        .post(&flex_url)
        .header("content-type", "application/json")
        .body(DISCOVERY_BODY)
        .send()
        .await?;
    assert_eq!(r2.headers().get("x-mcp-cache").unwrap(), "hit");

    upstream.assert_hits_async(1).await; // exactly one upstream call
    Ok(())
}

#[pdk_test]
async fn no_cache_header_forces_bypass() -> anyhow::Result<()> {
    let policy = PolicyConfig::builder()
        .name(POLICY_NAME)
        .configuration(json!({ "discovery": { "cacheable": true, "ttl": 60 } }))
        .build();

    let config = FlexConfig::builder()
        .version("1.9.3")
        .hostname("local-flex")
        .with_api(
            ApiConfig::builder()
                .name("mcp-api")
                .upstream(&HttpMock::service_name())
                .path("/")
                .port(80)
                .policy(policy)
                .build(),
        )
        .build();

    let composite = pdk_test::TestComposite::builder()
        .with_flex(config)
        .with_httpmock(HttpBinConfig::builder().hostname("backend").build())
        .build()
        .await?;

    let flex: Flex = composite.service()?;
    let mock_server: HttpMock = composite.service()?;
    let server = MockServer::connect_async(mock_server.socket()).await;
    let upstream = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#);
        })
        .await;

    let flex_url = format!("http://{}", flex.external_url()?);
    let client = reqwest::Client::new();
    let resp = client
        .post(&flex_url)
        .header("content-type", "application/json")
        .header("cache-control", "no-cache")
        .body(DISCOVERY_BODY)
        .send()
        .await?;
    assert_eq!(resp.headers().get("x-mcp-cache").unwrap(), "bypass");
    upstream.assert_hits_async(1).await;
    Ok(())
}
```

> **Harness caveat:** the exact `pdk-test` builder API (`TestComposite`, `FlexConfig`, `ApiConfig`, `HttpMock`) may differ slightly across PDK versions. Before running, open a sibling policy's `tests/requests.rs` **in this repo** (e.g. `mcp-tool-rate-limit-policy`) and match its harness imports/builders exactly — the *shape* above (build policy config → stand up Flex + mock upstream → assert `x-mcp-cache` + `assert_hits`) is what matters; adjust the builder calls to the repo's proven idiom. Do **not** copy from the private reference repos.

- [ ] **Step 4: Run integration tests**

Run: `cd implementation && make test`
Expected: Docker starts Flex; both tests PASS. First call `miss`, second `hit` with exactly one upstream hit; `no-cache` yields `bypass`.

If Docker/Flex is unavailable in the environment, record that `make test` could not run here and leave the tests in place; they are the acceptance gate when run in a Docker-capable environment.

- [ ] **Step 5: Commit**

```bash
git add implementation/tests/ implementation/Cargo.toml
git commit -m "test(cache): add pdk-test integration round-trip and bypass tests"
```

---

### Task 7: Docs alignment + status flip

**Files:**
- Modify: `docs/architecture.md` (flip the "currently ships the scaffold" note to "implemented")
- Modify: `README.md` (flip the `Status: scaffold` note)
- Modify: `implementation/src/lib.rs` — remove the stale scaffold sentence from the header comment if still present.
- Modify: `implementation/playground/README.md` — remove the "current scaffold passes traffic through" caveat.

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update architecture.md status note**

Replace the scaffold note block:

```markdown
> This document is the approved design for the policy. The repository currently
> ships the scaffold (repo shape, config schema, MCP JSON-RPC parsing); the
> cache lookup/store lifecycle described in §4–§5 is implemented in the
> follow-up implementation phase.
```

with:

```markdown
> This document is the approved design, now implemented: the cache
> lookup/store lifecycle (§4–§5), the `CacheStore` backends (§2), key
> construction, and the annotation defense-in-depth (§5) all ship in
> `implementation/src/`. Publishing/release (§9 non-goals) remains via the P4A
> MCP server.
```

- [ ] **Step 2: Update README status**

Replace the `> **Status:** scaffold. ...` block with:

```markdown
> **Status:** implemented. The cache lifecycle (miss→store→hit), both backends
> (local + distributed), scope-aware keying, and annotation guardrails are in
> place with unit and integration tests. See
> [`docs/architecture.md`](docs/architecture.md) for the design.
```

- [ ] **Step 3: Trim scaffold sentences in code/playground docs**

- In `implementation/playground/README.md`, remove the final blockquote beginning "> The current scaffold passes traffic through".
- Confirm `implementation/src/lib.rs`'s header comment no longer claims pass-through (Task 5 already rewrote it; verify).

- [ ] **Step 4: Final full build + test gate**

Run: `cd implementation && cargo test --lib && cargo build --target wasm32-wasip1 --release`
Expected: all unit tests pass, wasm builds. (Run `make test` where Docker is available.)

- [ ] **Step 5: Commit**

```bash
git add docs/architecture.md README.md implementation/playground/README.md implementation/src/lib.rs
git commit -m "docs: flip status from scaffold to implemented"
```

---

## Self-Review

**1. Spec coverage** (design §1–§9 → tasks):
- §2 Backend abstraction (`CacheStore`, Local + Gossip, `maxEntries` semantics) → Task 2. ✓
- §3 Repo shape (module list) → covered across Tasks 1–4; `errors.rs` folded into `mcp.rs` (envelope builder) — architecture.md updated to match in the pre-plan doc edit. ✓
- §4 Recognition / request filter / response filter / key construction / never-cached / failure modes → Tasks 1 (key), 3 (response parse/guards), 5 (filters, recognition, SSE skip, fail-open). ✓
- §5 Annotation defense-in-depth → Task 4 + wired in Task 5 `decide`. ✓
- §6 Config surface → already in `gcl.yaml`/`config.rs` (unchanged); consumed in Task 5. ✓
- §7 Observability (`x-mcp-cache`) → Task 5 stamps hit/miss/bypass; Task 6 asserts. ✓
- §8 Testing strategy → unit (Tasks 1–4), integration (Task 6). ✓
- §9 Non-goals (SSE/batch/ETag/publish) → SSE skip in Task 5; batch/ETag intentionally unimplemented (single-object parse); publish stays on P4A MCP. ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Two explicit caveats (Task 5 Step 3 `make build` auth; Task 6 harness builder API) point the implementer at a proven in-repo sibling and a fallback command rather than leaving a gap — both name the exact action to take.

**3. Type consistency:**
- `CachedEntry { written_at, valid_until, body }` — identical in Tasks 2, 4, 5. ✓
- `cache_key(method, params, scope, identity) -> Option<String>` — defined Task 1, called Task 5. ✓
- `Identity { principal: Option<&str>, session: Option<&str> }` — Task 1, constructed Task 5. ✓
- `CacheStore::{get -> Option<CachedEntry>, put(key, &entry)}` — Task 2, consumed Tasks 4 & 5. ✓
- `is_cacheable_response`/`restamp_id`/`tool_safety_from_list` — Task 3, consumed Tasks 4 & 5. ✓
- `MissCtx`/`Disposition` — internal to Task 5, self-consistent. ✓
- `record_from_list`/`is_known_unsafe` — Task 4, consumed Task 5. ✓
- PDK APIs match the verified 1.9.2 signatures: `Cache` sync (`get`/`save`/`delete`), `DataStorage` async with `StoreMode::Absent` + `DataStorageError::CasMismatch`, `store_builder.remote(ns, u32_millis)`, `Response::with_headers(Vec<(String,String)>)` + `with_body(impl Into<Vec<u8>>)`, `Flow::{Continue,Break}`, `RequestData::{Continue,Break,Cancel}`, `CacheBuilder`/`DataStorageBuilder` injectable into `configure()`. ✓
