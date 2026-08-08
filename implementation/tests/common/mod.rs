// Copyright 2026 Salesforce, Inc. All rights reserved.
//
// Shared constants for the pdk_test integration harness.
//
// `make test` builds the policy, installs it into ./policies_config, and
// exports POLICY_REF_NAME (the implementation ref name derived by
// `cargo anypoint get-policy-implementation-name`). The FlexConfig mounts
// POLICY_DIR as its "policy" config so the gateway loads this policy under
// that ref name.
//
// POLICY_NAME falls back to a placeholder via `option_env!` so the crate still
// compiles under a plain `cargo test`/`cargo test --no-run` when the Makefile
// has not exported the ref name (e.g. a compile-only gate without Docker). The
// #[pdk_test] cases only actually run under `make test` with Docker present.

pub const POLICY_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/policies_config");

/// Local-test gateway registration + logging config, mounted alongside the
/// policy so the Flex container can register every extension. Contains a
/// throwaway local-test cert/key (gitignored — never committed).
pub const COMMON_CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/config");

pub const POLICY_NAME: &str = match option_env!("POLICY_REF_NAME") {
    Some(name) => name,
    None => "mcp-response-cache-policy-flex",
};
