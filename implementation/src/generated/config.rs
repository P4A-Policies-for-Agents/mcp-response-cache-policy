// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Config structs mirroring definition/gcl.yaml.
//
// NOTE: The P4A build pipeline regenerates this file via
// `cargo anypoint config-gen` from the definition at deploy time. It is
// hand-maintained here so the crate compiles locally (config-gen does not
// reliably emit nested objects / arrays-of-objects). Keep it in sync with
// gcl.yaml; the serde aliases map gcl camelCase keys to snake_case fields.

use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub tools: Vec<ToolConfig>,
    #[serde(alias = "maxEntries", default = "default_max_entries")]
    pub max_entries: u32,
    #[serde(default)]
    pub distributed: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DiscoveryConfig {
    #[serde(default = "default_true")]
    pub cacheable: bool,
    #[serde(default = "default_discovery_ttl")]
    pub ttl: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ToolConfig {
    pub name: String,
    #[serde(default)]
    pub cacheable: bool,
    pub ttl: u64,
    #[serde(default)]
    pub scope: CacheScope,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheScope {
    #[default]
    Shared,
    Identity,
}

fn default_true() -> bool {
    true
}

fn default_discovery_ttl() -> u64 {
    60
}

fn default_max_entries() -> u32 {
    1000
}
