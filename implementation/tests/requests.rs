// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Integration tests for the MCP Response Cache policy.
//
// These exercise the full request→response lifecycle through a real Flex
// Gateway container fronting an httpmock "MCP server" upstream. The core
// assertions prove the cache actually short-circuits the upstream:
//   * a discovery miss forwards to upstream (x-mcp-cache: miss), the identical
//     follow-up is served locally (x-mcp-cache: hit) WITHOUT a second upstream
//     hit, and the cached body is re-stamped onto the live request id;
//   * `cache-control: no-cache` bypasses the cache entirely;
//   * an allowlisted read-only tools/call round-trips the same way.
//
// Run via `make test` (builds + installs the policy into ./policies_config and
// exports POLICY_REF_NAME, then `cargo test`). Requires Docker.

mod common;

use common::*;
use httpmock::MockServer;
use pdk_test::services::flex::{ApiConfig, Flex, FlexConfig, PolicyConfig};
use pdk_test::services::httpmock::{HttpMock, HttpMockConfig};
use pdk_test::{pdk_test, TestComposite};
use serde_json::json;
use std::sync::OnceLock;

struct TestSetup {
    api_url: String,
    mock_server: MockServer,
}

impl std::fmt::Debug for TestSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestSetup").field("api_url", &self.api_url).finish()
    }
}

static TEST_SETUP: OnceLock<TestSetup> = OnceLock::new();

async fn setup_test() -> anyhow::Result<&'static TestSetup> {
    if let Some(setup) = TEST_SETUP.get() {
        return Ok(setup);
    }

    let httpmock_config = HttpMockConfig::builder()
        .port(80)
        .version("latest")
        .hostname("backend")
        .build();

    let policy_config = PolicyConfig::builder()
        .name(POLICY_NAME)
        .configuration(json!({
            "discovery": { "cacheable": true, "ttl": 60 },
            "tools": [
                { "name": "read_only_search", "cacheable": true, "ttl": 60, "scope": "shared" }
            ],
            "maxEntries": 1000,
            "distributed": false
        }))
        .build();

    let api_config = ApiConfig::builder()
        .name("mcp-api")
        .port(8185)
        .path("/")
        .upstream(&httpmock_config)
        .policies([policy_config])
        .build();

    let flex_config = FlexConfig::builder()
        .version("1.11.0")
        .hostname("local-flex")
        .with_api(api_config)
        .config_mounts([(POLICY_DIR, "policy"), (COMMON_CONFIG_DIR, "common")])
        .build();

    let composite = TestComposite::builder()
        .with_service(flex_config)
        .with_service(httpmock_config)
        .build()
        .await?;

    let flex: Flex = composite.service()?;
    let api_url = flex.external_url(8185).unwrap();

    let backend: HttpMock = composite.service()?;
    let mock_server = MockServer::connect_async(backend.socket()).await;

    std::mem::forget(composite);

    let setup = TestSetup { api_url, mock_server };
    TEST_SETUP
        .set(setup)
        .expect("TEST_SETUP should only be initialized once");

    Ok(TEST_SETUP.get().unwrap())
}

/// A JSON-RPC request envelope as a request body string.
fn rpc_request(id: i64, method: &str, params: serde_json::Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string()
}

fn cache_header(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get("x-mcp-cache")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

#[pdk_test]
async fn discovery_miss_then_hit() -> anyhow::Result<()> {
    let setup = setup_test().await?;

    // Upstream MCP server answers tools/list once; a second identical call must
    // be served from cache and never reach it. Scope the mock by the method so
    // it doesn't shadow other tests sharing this composite.
    let upstream = setup
        .mock_server
        .mock_async(|when, then| {
            when.method("POST").path("/").body_contains("\"method\":\"tools/list\"");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": { "tools": [ { "name": "read_only_search" } ] }
                    })
                    .to_string(),
                );
        })
        .await;

    let client = reqwest::Client::new();

    // First request: MISS — forwarded upstream.
    let first = client
        .post(&setup.api_url)
        .header("Content-Type", "application/json")
        .body(rpc_request(1, "tools/list", json!({})))
        .send()
        .await?;
    assert_eq!(first.status(), 200);
    assert_eq!(cache_header(&first).as_deref(), Some("miss"));
    upstream.assert_hits_async(1).await;

    // Second identical request with a DIFFERENT id: HIT — served locally, and
    // the cached body must be re-stamped onto the live request id (2).
    let second = client
        .post(&setup.api_url)
        .header("Content-Type", "application/json")
        .body(rpc_request(2, "tools/list", json!({})))
        .send()
        .await?;
    assert_eq!(second.status(), 200);
    assert_eq!(cache_header(&second).as_deref(), Some("hit"));

    // Upstream still hit exactly once — the hit did not forward.
    upstream.assert_hits_async(1).await;

    let body: serde_json::Value = second.json().await?;
    assert_eq!(body["id"], json!(2), "cached body id re-stamped to live request id");
    assert_eq!(body["result"]["tools"][0]["name"], json!("read_only_search"));

    Ok(())
}

#[pdk_test]
async fn no_cache_header_bypasses() -> anyhow::Result<()> {
    let setup = setup_test().await?;

    let upstream = setup
        .mock_server
        .mock_async(|when, then| {
            when.method("POST").path("/").body_contains("\"method\":\"prompts/list\"");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(
                    json!({ "jsonrpc": "2.0", "id": 1, "result": { "prompts": [] } }).to_string(),
                );
        })
        .await;

    let client = reqwest::Client::new();

    // Two requests, both with cache-control: no-cache. Neither is cached, so
    // both forward and the disposition is always "bypass".
    for id in 1..=2 {
        let resp = client
            .post(&setup.api_url)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .body(rpc_request(id, "prompts/list", json!({})))
            .send()
            .await?;
        assert_eq!(resp.status(), 200);
        assert_eq!(cache_header(&resp).as_deref(), Some("bypass"));
    }

    upstream.assert_hits_async(2).await;
    Ok(())
}

#[pdk_test]
async fn allowlisted_tool_call_miss_then_hit() -> anyhow::Result<()> {
    let setup = setup_test().await?;

    let upstream = setup
        .mock_server
        .mock_async(|when, then| {
            when.method("POST")
                .path("/")
                .body_contains("\"method\":\"tools/call\"")
                .body_contains("\"name\":\"read_only_search\"");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": { "content": [ { "type": "text", "text": "result-body" } ] }
                    })
                    .to_string(),
                );
        })
        .await;

    let client = reqwest::Client::new();
    let params = json!({ "name": "read_only_search", "arguments": { "q": "hello" } });

    let first = client
        .post(&setup.api_url)
        .header("Content-Type", "application/json")
        .body(rpc_request(10, "tools/call", params.clone()))
        .send()
        .await?;
    assert_eq!(first.status(), 200);
    assert_eq!(cache_header(&first).as_deref(), Some("miss"));
    upstream.assert_hits_async(1).await;

    // Identical arguments, different id → cache hit, no second upstream call.
    let second = client
        .post(&setup.api_url)
        .header("Content-Type", "application/json")
        .body(rpc_request(11, "tools/call", params))
        .send()
        .await?;
    assert_eq!(second.status(), 200);
    assert_eq!(cache_header(&second).as_deref(), Some("hit"));
    upstream.assert_hits_async(1).await;

    let body: serde_json::Value = second.json().await?;
    assert_eq!(body["id"], json!(11));
    assert_eq!(body["result"]["content"][0]["text"], json!("result-body"));

    Ok(())
}
