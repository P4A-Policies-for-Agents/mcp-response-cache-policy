// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Cache-key construction: canonicalize JSON-RPC params (order-independent),
// SHA-256 the (method, params) tuple, and assemble a scope-aware key. Sensitive
// identity values participate only as hashes, never raw.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// Record separator between method and params inside the hash preimage.
const RS: u8 = 0x1e;

/// Cache partitioning for a tool. Defined here (not in `generated::config`)
/// because `cargo anypoint config-gen` emits the `scope` property as a plain
/// `Option<String>`, not an enum — the P4A build pipeline regenerates that file
/// at deploy time, so domain types must live outside it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum CacheScope {
    /// Key on tool + canonical args only; one entry shared across all callers.
    #[default]
    Shared,
    /// Also partition the key by the policy-level partition strategy
    /// (presets or a DataWeave expression), resolved per request.
    Partitioned,
}

impl CacheScope {
    /// Parse the gcl `scope` string. Anything other than `"partitioned"`
    /// (including absence) is the safe default, `Shared`.
    pub fn parse(scope: Option<&str>) -> Self {
        match scope {
            Some("partitioned") => CacheScope::Partitioned,
            _ => CacheScope::Shared,
        }
    }
}

/// One preset partition source (the `partitionBy` list, mode = presets). Parsed
/// once at configure() and resolved per request against headers in `lib.rs`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PartitionSource {
    /// The authenticated principal — the `x-forwarded-user` request header.
    Principal,
    /// The MCP session — the `mcp-session-id` request header.
    Session,
    /// An arbitrary request header, lower-cased. From `header:<Name>`.
    Header(String),
}

impl PartitionSource {
    /// Parse one `partitionBy` entry. Recognizes `principal`, `session`, and
    /// `header:<Name>` (case-insensitive keyword; header name lower-cased for
    /// case-insensitive lookup). Returns `None` for an empty/unrecognized entry.
    pub fn parse(entry: &str) -> Option<Self> {
        let e = entry.trim();
        if e.eq_ignore_ascii_case("principal") {
            Some(PartitionSource::Principal)
        } else if e.eq_ignore_ascii_case("session") {
            Some(PartitionSource::Session)
        } else if let Some(name) = e
            .strip_prefix("header:")
            .or_else(|| e.strip_prefix("Header:"))
        {
            let name = name.trim();
            if name.is_empty() {
                None
            } else {
                Some(PartitionSource::Header(name.to_ascii_lowercase()))
            }
        } else {
            None
        }
    }
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
    // back to the empty string rather than panicking.
    serde_json::to_string(&sorted(params)).unwrap_or_default()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Build the cache key from already-resolved partition values.
///
/// `parts` are the partition values resolved for THIS request (in `lib.rs`):
/// for presets, one entry per `partitionBy` source that produced a value; for
/// dataweave, the single evaluated key value. The caller passes only the values
/// that actually resolved (absent sources are dropped, not passed as empty).
///
/// - `Shared` scope ignores `parts` and keys on tool + canonical args only.
/// - `Partitioned` scope with a non-empty `parts` appends each value's SHA-256
///   to the key, in the order given (order is significant, so the resolver must
///   preserve `partitionBy` order).
/// - `Partitioned` scope with an EMPTY `parts` returns `None` — the strategy
///   resolved to nothing, so the caller must bypass rather than cache under a
///   weak (unpartitioned) key.
pub fn cache_key(
    method: &str,
    params: &Value,
    scope: CacheScope,
    parts: &[String],
) -> Option<String> {
    let canon = canonicalize(params);
    let mut preimage = Vec::with_capacity(method.len() + 1 + canon.len());
    preimage.extend_from_slice(method.as_bytes());
    preimage.push(RS);
    preimage.extend_from_slice(canon.as_bytes());
    let base = sha256_hex(&preimage);

    match scope {
        CacheScope::Shared => Some(format!("{method}:{base}")),
        CacheScope::Partitioned => {
            if parts.is_empty() {
                return None;
            }
            let mut key = format!("{method}:{base}");
            for value in parts {
                key.push(':');
                key.push_str(&sha256_hex(value.as_bytes()));
            }
            Some(key)
        }
    }
}
