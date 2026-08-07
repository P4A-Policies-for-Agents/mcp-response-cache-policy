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
