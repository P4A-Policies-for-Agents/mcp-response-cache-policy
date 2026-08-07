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
    // back to the empty string rather than panicking.
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
