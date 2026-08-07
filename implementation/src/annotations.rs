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
