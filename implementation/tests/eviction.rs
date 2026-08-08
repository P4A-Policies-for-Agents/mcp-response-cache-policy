// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// `maxEntries` LRU eviction — a property of the local PDK `Cache`.
//
// This test lives in its OWN integration-test file (not requests.rs) on
// purpose: `cargo test` compiles each `tests/*.rs` as a separate test binary
// and runs them in separate processes. requests.rs deliberately LEAKS its
// shared Flex composite (`std::mem::forget`) so hit-count assertions survive
// across its cases; standing up a SECOND composite in that same process races
// the leaked one for Docker networks/ports and can wedge the run. Isolating
// eviction in its own process sidesteps that entirely — it gets a clean Docker
// scope with nothing leaked alongside it.

mod common;

use common::*;
use httpmock::MockServer;
use pdk_test::services::flex::{ApiConfig, Flex, FlexConfig, PolicyConfig};
use pdk_test::services::httpmock::{HttpMock, HttpMockConfig};
use pdk_test::{pdk_test, TestComposite};
use serde_json::json;

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
async fn max_entries_evicts_lru() -> anyhow::Result<()> {
    // Dedicated composite with maxEntries=1 and its own port/hostname so it
    // never collides with the shared requests.rs setup.
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
                { "name": "a", "cacheable": true, "ttl": 60, "scope": "shared" },
                { "name": "b", "cacheable": true, "ttl": 60, "scope": "shared" }
            ],
            "maxEntries": 1,
            "distributed": false
        }))
        .build();

    let api_config = ApiConfig::builder()
        .name("mcp-api-evict")
        .port(8186)
        .path("/")
        .upstream(&httpmock_config)
        .policies([policy_config])
        .build();

    let flex_config = FlexConfig::builder()
        .version("1.11.0")
        .hostname("local-flex-evict")
        .with_api(api_config)
        .config_mounts([(POLICY_DIR, "policy"), (COMMON_CONFIG_DIR, "common")])
        .build();

    let composite = TestComposite::builder()
        .with_service(flex_config)
        .with_service(httpmock_config)
        .build()
        .await?;

    let flex: Flex = composite.service()?;
    let api_url = flex.external_url(8186).unwrap();
    let backend: HttpMock = composite.service()?;
    let mock_server = MockServer::connect_async(backend.socket()).await;

    // One mock per tool so we can count hits independently.
    let mock_a = mock_server
        .mock_async(|when, then| {
            when.method("POST").path("/").body_contains("\"name\":\"a\"");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(json!({"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"A"}]}}).to_string());
        })
        .await;
    let mock_b = mock_server
        .mock_async(|when, then| {
            when.method("POST").path("/").body_contains("\"name\":\"b\"");
            then.status(200)
                .header("Content-Type", "application/json")
                .body(json!({"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"B"}]}}).to_string());
        })
        .await;

    let client = reqwest::Client::new();
    let call_a = || rpc_request(1, "tools/call", json!({ "name": "a", "arguments": {} }));
    let call_b = || rpc_request(1, "tools/call", json!({ "name": "b", "arguments": {} }));

    // 1) a → miss (stored, cache now holds {a}).
    let r = client.post(&api_url).header("Content-Type", "application/json").body(call_a()).send().await?;
    assert_eq!(cache_header(&r).as_deref(), Some("miss"));
    mock_a.assert_hits_async(1).await;

    // 2) b → miss (maxEntries=1 evicts a; cache now holds {b}).
    let r = client.post(&api_url).header("Content-Type", "application/json").body(call_b()).send().await?;
    assert_eq!(cache_header(&r).as_deref(), Some("miss"));
    mock_b.assert_hits_async(1).await;

    // 3) a again → miss (was evicted) → forwards to upstream a SECOND time.
    let r = client.post(&api_url).header("Content-Type", "application/json").body(call_a()).send().await?;
    assert_eq!(cache_header(&r).as_deref(), Some("miss"));
    mock_a.assert_hits_async(2).await;

    std::mem::forget(composite);
    Ok(())
}
