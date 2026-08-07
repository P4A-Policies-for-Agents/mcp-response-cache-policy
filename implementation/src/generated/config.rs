use serde::Deserialize;
#[derive(Deserialize, Clone, Debug)]
pub struct DiscoveryConfig {
    #[serde(alias = "cacheable")]
    pub cacheable: Option<bool>,
    #[serde(alias = "ttl")]
    pub ttl: Option<i64>,
}
#[derive(Deserialize, Clone, Debug)]
pub struct Tools0Config {
    #[serde(alias = "cacheable")]
    pub cacheable: Option<bool>,
    #[serde(alias = "name")]
    pub name: String,
    #[serde(alias = "scope")]
    pub scope: Option<String>,
    #[serde(alias = "ttl")]
    pub ttl: i64,
}
#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    #[serde(alias = "discovery")]
    pub discovery: DiscoveryConfig,
    #[serde(alias = "distributed")]
    pub distributed: Option<bool>,
    #[serde(alias = "maxEntries")]
    pub max_entries: Option<i64>,
    #[serde(alias = "tools")]
    pub tools: Option<Vec<Tools0Config>>,
}
#[pdk::hl::entrypoint_flex]
fn init(abi: &dyn pdk::flex_abi::api::FlexAbi) -> Result<(), anyhow::Error> {
    abi.setup()?;
    Ok(())
}
