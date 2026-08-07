# MCP Tool-Call Response Cache

A MuleSoft Omni Gateway custom policy (PDK) that **caches side-effect-free MCP
responses at the gateway** so repeated, read-only tool calls and discovery
requests are served locally instead of re-hitting the upstream MCP server. It
cuts latency, upstream load, and per-call cost for the naturally repetitive
traffic agents generate — using standard PDK primitives only: **no Redis, no
external dependency.**

> **Status:** scaffold. The repository shape, configuration schema, and MCP
> JSON-RPC parsing are in place and the crate builds; the cache lookup/store
> lifecycle is implemented in the follow-up implementation phase. See
> [`docs/architecture.md`](docs/architecture.md) for the full design.

## Why

Agents call MCP servers in tight, repetitive loops: the same `tools/list` on
every session bootstrap; the same read-only `tools/call` (a lookup, a search, a
"get current X") re-issued across steps and across agents. Each round-trips to
the upstream — adding latency, backend load, and cost when the tool is metered
or fans out to a paid downstream. This policy collapses those duplicates at the
gateway.

It's the HTTP shared-cache model (RFC 9111) applied to a JSON-RPC body: because
MCP tunnels everything through `POST`, "safe to cache?" is decided by the
JSON-RPC **method** and an operator **per-tool allowlist** (plus `readOnlyHint`/
`destructiveHint` guardrails), not by an HTTP verb.

It **complements** the MCP governance policies (Tool Drift Detection,
Tool-Poisoning Detection): those police *what* a server exposes and *whether* it
drifted; this optimizes *how often* the gateway forwards calls. They share the
JSON-RPC parsing surface and stack in the same policy chain.

## Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `discovery.cacheable` | boolean | `true` | Cache `tools/list` / `resources/list` / `prompts/list`. |
| `discovery.ttl` | integer (s) | `60` | TTL for discovery entries. `0` disables discovery caching. |
| `tools` | array | `[]` | Per-tool cache table. A tool not listed is passed through (never cached). |
| `tools[].name` | string | — (required) | Exact MCP tool name (`params.name`). |
| `tools[].cacheable` | boolean | `false` | Enable caching for this tool. |
| `tools[].ttl` | integer (s) | — (required) | Max entry lifetime. |
| `tools[].scope` | `shared` \| `identity` | `shared` | `shared` = tool + canonical args; `identity` also partitions by principal + MCP session id. |
| `maxEntries` | integer | `1000` | Cache size cap. Hard LRU in local mode; soft/TTL-bound in distributed mode. |
| `distributed` | boolean | `false` | Share the cache across replicas via gossip-replicated storage (no Redis). |

## Behavior

- **Cacheable request** (allowlisted tool or enabled discovery method):
  canonicalize args, compute the key, look up the cache.
  - **Hit** → return the stored JSON-RPC result on the original request `id` via
    an early response; upstream is skipped. `x-mcp-cache: hit`.
  - **Miss** → forward upstream; on a cacheable success, store with TTL.
    `x-mcp-cache: miss`.
- **Never cached:** JSON-RPC error envelopes, results flagged `isError`,
  streaming/SSE responses, non-allowlisted tools, and tools observed as
  destructive / lacking `readOnlyHint`.
- **Bypass:** a request carrying `Cache-Control: no-cache` forces upstream (and
  stores the fresh result). `x-mcp-cache: bypass`.
- **Fail-open:** any cache read/write error or unparseable body falls through to
  upstream — the policy never blocks traffic.

## Observability

Every handled response carries `x-mcp-cache: hit | miss | bypass`.

## Repository structure

```
mcp-response-cache-policy/
├── definition/        # policy definition (gcl.yaml schema, exchange.json, Makefile)
├── implementation/    # Rust/WASM implementation (Cargo.toml, src/, playground/, tests/)
├── docs/              # architecture and design docs
└── scripts/           # shared build helpers
```

## Build & test

```bash
cd implementation
make setup    # install cargo-anypoint
make build    # compile to wasm32-wasip1
make test     # run tests
make run      # local Docker playground (Omni Gateway)
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
