// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// MCP Response Cache — MuleSoft Omni Gateway custom policy (PDK).
//
// Caches side-effect-free MCP responses (discovery methods and allowlisted
// read-only tools/call) at the gateway so repeated agent calls are served
// locally instead of re-hitting the upstream MCP server.
//
// This entrypoint is the scaffold: it parses configuration, wires the
// request/response filters, and currently passes traffic through (fail-open).
// The cache lookup / store lifecycle, key construction, guardrails, and the
// CacheStore backends are implemented in the follow-up implementation phase —
// see docs/architecture.md and the modules below.

mod generated;
mod mcp;

use crate::generated::config::Config;
use anyhow::{anyhow, Result};
use pdk::hl::*;
use pdk::logger;

const POLICY_NAME: &str = "mcp-response-cache-policy";

/// Request filter. Fail-open scaffold: recognise MCP JSON-RPC traffic and log,
/// but pass everything through until the cache lifecycle lands.
async fn request_filter(request_state: RequestState, _config: &Config) -> Flow<()> {
    let headers_state = request_state.into_headers_state().await;
    let handler = headers_state.handler();

    // Only POST + JSON is a caching candidate; anything else passes through.
    let method = handler.header(":method").unwrap_or_default();
    let content_type = handler.header("content-type").unwrap_or_default();
    if method != "POST" || !content_type.contains("json") {
        return Flow::Continue(());
    }

    logger::debug!("[{}] MCP JSON-RPC candidate observed", POLICY_NAME);
    Flow::Continue(())
}

/// Response filter. Fail-open scaffold: no-op until the store lifecycle lands.
async fn response_filter(_response_state: ResponseState, _request_data: RequestData<()>) {}

#[entrypoint]
async fn configure(launcher: Launcher, Configuration(bytes): Configuration) -> Result<()> {
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

    let filter = on_request(move |rs| {
        let config = config.clone();
        async move { request_filter(rs, &config).await }
    })
    .on_response(|rs, req_data| async move { response_filter(rs, req_data).await });

    launcher.launch(filter).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
