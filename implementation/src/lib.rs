// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// MCP Response Cache — MuleSoft Omni Gateway custom policy (PDK).
//
// Caches side-effect-free MCP responses (discovery methods and allowlisted
// read-only tools/call) at the gateway so repeated agent calls are served
// locally instead of re-hitting the upstream MCP server.
//
// Lifecycle: the request filter parses the JSON-RPC envelope, decides
// cacheability (config allowlist + discovery toggle + guardrails), builds a
// SHA-256 key, and on a hit short-circuits with the stored result re-stamped to
// the live request id; on a miss it threads a MissCtx to the response filter,
// which parses the response and stores it. All filter code is generic over the
// CacheStore trait; the backend (local vs gossip) is chosen once in configure().

mod annotations;
mod generated;
mod key;
mod mcp;
mod store;

use crate::generated::config::Config;
use crate::key::{cache_key, CacheScope, Identity};
use crate::mcp::{
    is_cacheable_response, is_discovery_method, is_notification, parse_request, restamp_id,
    McpRequest, TOOLS_CALL, TOOLS_LIST,
};
use crate::store::{now_secs, CacheStore, CachedEntry, GossipStore, LocalStore};
use anyhow::{anyhow, Result};
use pdk::cache::CacheBuilder;
use pdk::data_storage::DataStorageBuilder;
use pdk::hl::*;
use pdk::logger;
use std::collections::HashMap;
use std::rc::Rc;

const POLICY_NAME: &str = "mcp-response-cache-policy";
const CACHE_ID: &str = "mcp-response-cache";
const HEADER: &str = "x-mcp-cache";

// Defaults mirror definition/gcl.yaml. The generated Config wraps optional
// fields in Option (config-gen drops gcl `default:` clauses), so the policy
// re-applies them here at load time.
const DEFAULT_DISCOVERY_CACHEABLE: bool = true;
const DEFAULT_DISCOVERY_TTL: u64 = 60;
const DEFAULT_MAX_ENTRIES: u32 = 1000;
const DEFAULT_DISTRIBUTED: bool = false;
const DEFAULT_TOOL_CACHEABLE: bool = false;

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
    fn bypass(method: String) -> Self {
        Self {
            disposition: Disposition::Bypass,
            key: None,
            ttl: 0,
            method,
        }
    }
}

/// A tool's cache settings, resolved from the generated (Option-wrapped)
/// config into concrete domain values.
#[derive(Clone)]
struct ToolConfig {
    cacheable: bool,
    ttl: u64,
    scope: CacheScope,
}

/// Immutable per-worker view of config, precomputed for O(1) tool lookup.
struct Policy {
    discovery_cacheable: bool,
    discovery_ttl: u64,
    tools: HashMap<String, ToolConfig>,
}

impl Policy {
    fn new(config: &Config) -> Self {
        let discovery_cacheable = config
            .discovery
            .cacheable
            .unwrap_or(DEFAULT_DISCOVERY_CACHEABLE);
        let discovery_ttl = config
            .discovery
            .ttl
            .map(|t| t.max(0) as u64)
            .unwrap_or(DEFAULT_DISCOVERY_TTL);

        let tools = config
            .tools
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|t| {
                let resolved = ToolConfig {
                    cacheable: t.cacheable.unwrap_or(DEFAULT_TOOL_CACHEABLE),
                    // gcl requires ttl >= 1; clamp defensively.
                    ttl: t.ttl.max(1) as u64,
                    scope: CacheScope::parse(t.scope.as_deref()),
                };
                (t.name.clone(), resolved)
            })
            .collect();

        Self {
            discovery_cacheable: discovery_cacheable && discovery_ttl > 0,
            discovery_ttl,
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
        return Flow::Continue(MissCtx::bypass(String::new()));
    }
    match handler.header("content-type") {
        Some(ct) if ct.contains("json") => {}
        _ => return Flow::Continue(MissCtx::bypass(String::new())),
    }

    // Transport-level bypass hint.
    if handler
        .header("cache-control")
        .map(|v| v.contains("no-cache"))
        .unwrap_or(false)
    {
        return Flow::Continue(MissCtx::bypass(String::new()));
    }

    // Capture identity headers before consuming the state for the body.
    let principal = handler.header("x-forwarded-user");
    let session = handler.header("mcp-session-id");

    let body_state = headers_state.into_body_state().await;
    let body = body_state.handler().body();

    let req = match parse_request(&body) {
        Some(r) => r,
        None => return Flow::Continue(MissCtx::bypass(String::new())), // not MCP JSON-RPC
    };

    // params for keying (default to null when absent).
    let params: serde_json::Value = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("params").cloned())
        .unwrap_or(serde_json::Value::Null);

    let identity = Identity {
        principal: principal.as_deref(),
        session: session.as_deref(),
    };

    match decide(policy, store, &req, &params, &identity).await {
        Decision::Bypass => Flow::Continue(MissCtx::bypass(req.method)),
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

    let body_state = headers_state.into_body_state().await;
    let raw = body_state.handler().body();

    // Normalize the transport framing. Streamable-HTTP MCP servers frame the
    // JSON-RPC result as SSE; extract the terminal success response to the raw
    // JSON so it can be cached and replayed as application/json on a hit. This
    // handles a bare-JSON body, a single-event frame, and a multi-event
    // "progress notifications + one terminal result" stream (the notifications
    // are dropped). An unsafe stream (server→client request, error, ambiguous)
    // yields None and is never cached.
    let body = match crate::mcp::extract_cacheable_json(&raw) {
        Some(json) => json,
        None => return,
    };

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
    let policy = Rc::new(policy);
    let store = Rc::new(store);

    let req_policy = policy.clone();
    let req_store = store.clone();
    let resp_store = store.clone();

    let filter = on_request(move |rs| {
        let policy = req_policy.clone();
        let store = req_store.clone();
        async move { request_filter(rs, &policy, store.as_ref()).await }
    })
    .on_response(move |rs, req_data| {
        let store = resp_store.clone();
        async move { response_filter(rs, req_data, store.as_ref()).await }
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

    let policy = Policy::new(&config);
    let distributed = config.distributed.unwrap_or(DEFAULT_DISTRIBUTED);
    let max_entries = config
        .max_entries
        .map(|m| m.max(1) as u32)
        .unwrap_or(DEFAULT_MAX_ENTRIES);

    logger::info!(
        "[{}] configured (distributed={}, max_entries={}, tools={}, discovery.cacheable={}, discovery.ttl={}s)",
        POLICY_NAME,
        distributed,
        max_entries,
        policy.tools.len(),
        policy.discovery_cacheable,
        policy.discovery_ttl,
    );

    if distributed {
        // Namespace TTL bounds the whole cache; use the max configured ttl,
        // with a floor so discovery-only configs still get a sane window.
        let ttl_secs = policy
            .tools
            .values()
            .map(|t| t.ttl)
            .chain(std::iter::once(policy.discovery_ttl))
            .max()
            .unwrap_or(60)
            .max(60);
        let storage = store_builder.remote(CACHE_ID, (ttl_secs as u32).saturating_mul(1000));
        launch_policy(launcher, policy, GossipStore::new(storage)).await
    } else {
        let cache = cache_builder
            .new(CACHE_ID.to_string())
            .max_entries(max_entries as usize)
            .build();
        launch_policy(launcher, policy, LocalStore::new(cache)).await
    }
}

#[cfg(test)]
mod tests;
