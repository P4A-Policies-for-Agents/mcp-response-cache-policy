# MCP Response Cache — Architecture

> Delivers the P4A idea "MCP Tool-Call Response Cache"
> (`8ce9e2c4-03e2-44cf-96b1-9cca946e0a60`); shipped under the display name
> **MCP Response Cache**.

- **Target PDK:** 1.9.2 (common floor on crates.io `cargo-anypoint` and PDK release notes)
- **Category:** MCP · `assetTypes: mcp` · `interfaceScope: api`

> This document is the approved design for the policy, now fully implemented:
> the cache lookup/store lifecycle described in §4–§5 is in place, covered by
> unit tests and by pdk-test integration tests that exercise a real Flex
> Gateway (discovery miss→hit, `cache-control: no-cache` bypass, and an
> allowlisted read-only `tools/call` round-trip).

## 1. Problem & Goal

Agents call MCP servers in tight, repetitive loops: the same `tools/list` on
every session bootstrap; the same read-only `tools/call` (a lookup, a search, a
"get current X") re-issued across steps and across agents. Each round-trips to
the upstream MCP server — adding latency to the agent loop, load on the backend,
and cost when the tool is metered or fans out to a paid downstream.

This policy caches **side-effect-free MCP responses at the gateway** so repeated
discovery requests and read-only tool calls are served locally instead of
re-hitting the upstream. It cuts latency, upstream load, and per-call cost for
the naturally repetitive traffic agents generate, using **standard PDK
primitives only — no external dependency, no Redis.**

It complements (does not overlap) the MCP governance ideas (Tool Drift
Detection, Tool-Poisoning Detection): those police *what* the server exposes and
*whether* it drifted; this optimizes *how often* the gateway forwards calls.
They share the JSON-RPC parsing surface and stack in the same chain.

### Mental model: HTTP caching over a JSON-RPC body

This is the RFC 9111 shared-cache model adapted to MCP. Two twists:

| HTTP cache | This policy |
|---|---|
| Cacheable = method is `GET` | Everything is `POST`+JSON-RPC → "safe" comes from the **JSON-RPC method** (`*/list`) and the **operator allowlist + `readOnlyHint`/`destructiveHint`** |
| Key = URL + query (+`Vary`) | Key = JSON-RPC `method` + canonicalized `params` (hashed); `Vary` ≈ our `identity` scope |
| TTL from response `Cache-Control` | Upstream sends no cache headers → TTL is **operator-configured** |
| Bypass via `Cache-Control: no-cache` | Same header, reused verbatim |
| Revalidation via ETag/`304` | No MCP equivalent — expire and re-fetch |
| Single-node default; shared tier (Redis) opt-in | Local backend default; gossip `distributed` opt-in |

The single biggest design consequence: HTTP decides "safe to cache?" from the
method **verb**; MCP tunnels everything through `POST`, so the safety gate moves
to the **JSON-RPC method + per-tool allowlist + annotation guardrails**.

## 2. Backend Abstraction (configurable, ship both)

A small internal `CacheStore` seam with two implementations selected at
`configure()` time from the `distributed` flag. All downstream filter code is
generic over the trait; the backend branch exists **only** in `configure()`.

```
trait CacheStore {
    async fn get(&self, key: &str) -> Option<CachedEntry>;
    async fn put(&self, key: &str, entry: &CachedEntry, ttl: Duration);
}

  ├── LocalStore  → PDK `Cache`
  │     • default (distributed:false)
  │     • native `maxEntries` LRU eviction
  │     • no native TTL → `written_at`/`valid_until` embedded in the value,
  │       checked on read (lazy expiry)
  │
  └── GossipStore → PDK `DataStorage::remote(namespace, ttl_millis)`
        • distributed:true — cross-replica hits via gossip, still no Redis
        • TTL is the namespace-level bound (construction-time)
        • NO proactive deletes on expiry/read (gossip tombstones can kill a
          concurrent write) — return None and let namespace TTL evict
        • put uses StoreMode::Absent (first-writer-wins; never clobber a
          concurrent populate)
```

**`maxEntries` semantics** (documented, backend-dependent by nature — the same
trade-off as Nginx-local vs Nginx+shared-tier):

- Local mode: hard LRU cap enforced by the `Cache` primitive.
- Distributed mode: soft — the namespace TTL is the operative bound; no
  count-based eviction (proactive deletes are unsafe under gossip).

Rationale: the `distributed` flag is the idea's headline differentiator and only
`DataStorage` delivers cross-replica hits; the PDK `Cache` primitive is
single-replica but gives free LRU eviction and the simplest path for the common
single-node deployment. Shipping both behind one flag mirrors exactly how real
HTTP caches scale (local by default, shared tier opt-in). This trait-abstraction
approach is house style in the sibling `ai-semantic-cache` policy.

## 3. Repository Shape (split model)

New top-level sibling `mcp-response-cache-policy/`, split layout matching
`mcp-tool-rate-limit-policy`:

```
mcp-response-cache-policy/
├── definition/
│   ├── gcl.yaml          # schema (§6)
│   ├── exchange.json
│   └── Makefile
├── implementation/
│   ├── Cargo.toml        # pdk 1.9.2
│   ├── Makefile
│   ├── playground/       # docker-compose + config for local testing
│   ├── tests/            # integration tests (pdk-test)
│   └── src/
│       ├── lib.rs        # entrypoint + request/response filters
│       ├── mcp.rs        # JSON-RPC parse (request + response) + method
│       │                 #   vocabulary + hit-response envelope builder
│       ├── key.rs        # canonicalize params + SHA-256 keying + scope
│       ├── store.rs      # CacheStore trait + LocalStore + GossipStore
│       ├── annotations.rs# observed tool annotations (defense-in-depth, §5)
│       └── generated/    # config.rs (from gcl.yaml — do not hand-edit)
└── docs/
```

## 4. Request / Response Lifecycle

### Recognition (fail-open)

A request is a caching candidate iff: `POST` + `application/json` content-type +
body parses as a `jsonrpc:"2.0"` envelope + method is in the cacheable set.
Anything else → `Flow::Continue`, `x-mcp-cache: bypass`, no state.

### Request filter

```
parse JSON-RPC envelope ── not parseable / not MCP ─→ Continue, bypass
branch on method:
  discovery (tools/list | resources/list | prompts/list):
      cache only if discovery.cacheable AND discovery.ttl > 0
  tools/call:
      look up per-tool table by params.name
      absent OR cacheable:false ─→ Continue (pass-through, bypass)
guardrails (any true ─→ Continue, x-mcp-cache: bypass):
  • Cache-Control: no-cache present on the request
  • tool observed as destructive / lacking readOnlyHint (§5)
  • identity scope required but neither principal nor session present
build key (§ key construction)
cache.get(key):
  HIT  → Flow::Break(stored JSON-RPC result, id RE-STAMPED to THIS request's id)
         + header x-mcp-cache: hit ; upstream skipped
  MISS → Flow::Continue(Ctx { key, ttl, method }) + x-mcp-cache: miss
```

### Response filter

```
only on Flow::Continue(Ctx):
  normalize transport framing (extract_cacheable_json):
    • bare application/json object            → use as-is
    • single-event SSE frame (text/event-stream, one `event`/`data:` block,
      data is one JSON object)                → unwrap to that JSON
    • multi-event SSE stream, reduced by classifying each event:
        notification (method, NO id)          → drop (fire-and-forget)
        terminal success (id + result)        → the payload to store
        server→client request (method + id)   → UNSAFE ⇒ skip whole stream
        terminal error / 2nd terminal / other → UNSAFE ⇒ skip whole stream
      cache iff the stream is (notification)* + exactly one terminal success
    • data payload not a JSON object          → skip
  parse JSON-RPC response:
    skip if resp.error is present            (never cache error envelopes)
    skip if result missing                   (nothing to store)
    skip if result.isError == true           (tool-level error)
  else cache.put(key, CachedEntry { written_at, valid_until, body }, ttl)
```

**Why unwrap SSE rather than skip it.** MCP's streamable-HTTP transport frames
*every* response as Server-Sent Events — including the single-shot results of
the cacheable methods (`*/list`, read-only `tools/call`), which come back as one
`event: message` + one `data:` JSON-RPC line with `Content-Type:
text/event-stream`. Skipping all `text/event-stream` would mean the cache never
populates against a real MCP server (only against a backend that happens to
answer bare `application/json`). So the response filter unwraps the JSON payload
of the terminal success response and stores that; on a hit the request filter
serves it back as `application/json` (a clean single JSON-RPC object every MCP
client accepts).

**Multi-event streams — the safe collapse.** A stream carrying progress
notifications followed by the terminal result is a "completed operation that
emitted progress": the notifications are fire-and-forget (`method`, no `id`) and
a client that re-requests the same call never needed them, so the cache stores
only the terminal result and drops the rest. The distinguishing safety
invariant is JSON-RPC shape, not heuristics: an event with a `method` **and** an
`id` is a *server→client request* (sampling / elicitation / roots) that requires
a client round-trip — a stream containing one is interactive, not a completed
op, and is never cached. Likewise a terminal error, a second terminal response,
or a notifications-only stream aborts the collapse (`None`). The gate is exactly
`(notification)* + one terminal success`.

### Key construction

```
canonical_params = recursively sort object keys of params (BTreeMap) → serialize
base = sha256( method || 0x1e || canonical_params )
scope == shared    → key = "{method}:{base}"
scope == identity  → key = "{method}:{base}:{sha256(principal)}:{sha256(session)}"
```

- Canonicalization collapses `{a,b}` and `{b,a}` to one key (stable ordering).
- Sensitive values participating in the key are **hashed, never stored raw**
  (SHA-256), per the caching skill's key rules.
- Keys are deterministic: same inputs → same key.

### Never cached (guardrails, all fail-open)

- JSON-RPC error envelopes and results flagged `isError`.
- Interactive SSE streams — any stream carrying a server→client request
  (`method` + `id`: sampling / elicitation / roots), a terminal error, or more
  than one terminal response. A progress-then-result stream *is* cached (only
  its terminal success is stored); a bare/single-event result is cached as
  before.
- Non-allowlisted tools (default `cacheable:false` ⇒ pass-through).
- Side-effecting tools — honors observed MCP annotations (`destructiveHint`,
  absence of `readOnlyHint`); never cached even if allowlisted (§5).

### Failure modes

| Condition | Behavior |
|---|---|
| Cache read/write error | Never blocks — log warning, fall through to upstream. |
| Corrupt/expired entry on read | Treat as miss. Local: delete lazily. Distributed: return None, rely on TTL (no proactive delete under gossip). |
| Response not parseable as JSON-RPC | Bypass caching; `x-mcp-cache: bypass`. |
| Notification (no `id`) | Never cached, never short-circuited (fire-and-forget). |
| No control plane at t=0 | Policy loads and serves; no `unwrap`/`panic` on context-derived values. |

## 5. Annotation Defense-in-Depth (IN SCOPE for MVP)

MCP tool annotations (`readOnlyHint`, `destructiveHint`) live on the
**`tools/list` response**, not on the `tools/call` request the policy decides
about. So safety cannot depend on having seen discovery.

- **Primary gate:** the operator's per-tool allowlist. Default `cacheable:false`
  ⇒ pass-through. The policy is safe with **zero** annotation knowledge.
- **Defense-in-depth:** when the policy observes a tool marked `destructiveHint:
  true` (or lacking `readOnlyHint`) in a `tools/list` result it passes through,
  it records that tool as non-cacheable. A later `tools/call` for that tool is
  **refused caching even if the operator allowlisted it.**
- Observed-annotation state is stored via the same `CacheStore` (short TTL,
  refreshed each time discovery passes through), so it works in both backends
  and never blocks. Absence of knowledge ⇒ defer to the allowlist (safe
  default), never fail-closed on unknown tools.

## 6. Configuration Surface (`gcl.yaml`)

| Key | Type | Purpose | Default |
|---|---|---|---|
| `discovery.cacheable` | boolean | Cache `tools/list` / `resources/list` / `prompts/list`. | `true` |
| `discovery.ttl` | integer (s) | TTL for discovery entries. `0` ⇒ don't cache discovery. | `60` |
| `tools` | array | Per-tool cache table. Absent tool ⇒ pass-through. | `[]` |
| `tools[].name` | string | Tool name this entry configures (`params.name`). | — (req) |
| `tools[].cacheable` | boolean | Enable caching for this tool. | `false` |
| `tools[].ttl` | integer (s) | Max entry lifetime (capped, safety-margined). | — (req) |
| `tools[].scope` | enum `shared`\|`identity` | `shared` = tool+canonical args; `identity` also keyed by principal + session. | `shared` |
| `maxEntries` | integer | Cache size cap. Hard LRU (local) / soft, TTL-bound (distributed). | e.g. `1000` |
| `distributed` | boolean | Use gossip-replicated backend for cross-replica hits. | `false` |

Top-level metadata labels: `title`, `description`, `category: MCP`,
`metadata/interfaceScope: api`, `metadata/capabilities/assetTypes: mcp`. The
`tools` array follows the per-tool table pattern (`type: array` → `items:
{type: object, ...}` → generated `Vec<Tools0Config>` → `HashMap<name, entry>`
at `configure()` for O(1) lookup).

### Resolved open questions

1. **Discovery default TTL → 60s** (configurable; `0`/absent = off). Discovery
   rarely changes within a minute; Drift/Poisoning policies stack alongside.
2. **Identity scope → principal + `Mcp-Session-Id`**, degrade to whichever is
   present; if **neither**, bypass (never cache under a weak key).
3. **Bypass hint → honor `Cache-Control: no-cache`** on the request
   (transport-level; agents set it without mutating the RPC body). On bypass:
   force upstream, still store the fresh result, emit `x-mcp-cache: bypass`.

## 7. Outputs / Observability

- Cached JSON-RPC result served transparently to the agent, `id` re-stamped to
  the live request.
- Response header **`x-mcp-cache: hit | miss | bypass`** on every handled
  response.

## 8. Testing Strategy

- **Unit (`pdk-unit`):** envelope parse (numeric/string/null `id`, malformed /
  non-JSON-RPC), key canonicalization idempotence + value-sensitivity + scope
  partitioning, the full `decide()` decision matrix (notification, unknown
  method, discovery on/off/ttl-0, `tools/call` allowlist ×
  cacheable × missing-name × observed-destructive, identity scope with/without
  principal), response guardrails (`error`/`isError`/no-`result`), tool-safety
  annotation extraction + observed-unsafe marker, `LocalStore` roundtrip + lazy
  expiry eviction, and `GossipStore` roundtrip + first-writer-wins +
  stale-without-tombstone + get-error-falls-open. Mock `Cache` and
  `DataStorage`.
- **Integration (`pdk-test`, Docker playground, run serially):**
  discovery miss→store→hit with `id` re-stamp and upstream hit-count assertion
  (hit does not reach backend); allowlisted read-only `tools/call` miss→hit;
  `Cache-Control: no-cache` bypass; non-MCP body pass-through (never cached);
  identity-scope partitioning by `x-forwarded-user` principal; `maxEntries`
  LRU eviction (local backend, dedicated composite in its own `tests/eviction.rs`
  binary so it runs in a separate process and never races the leaked shared
  composite).

## 9. Non-Goals (v1)

- ETag/`304` revalidation (no MCP validator concept).
- Caching **interactive** SSE streams — any stream whose events include a
  server→client request (`method` + `id`). Progress-then-result streams (the
  "completed op that emitted progress" case) *are* cached by storing only the
  terminal success and dropping the fire-and-forget notifications (§4); the
  interactive case is out because collapsing it would skip a required client
  round-trip.
- JSON-RPC batch (array) bodies.
- Publishing/release — handled separately via the P4A MCP server
  (`submit_policy`/`deploy_policy`), gated for human approval.

## 10. References (generalized; no proprietary code cited)

- Sibling structural template: `mcp-tool-rate-limit-policy` (split model,
  JSON-RPC `tools/call` parse, `Flow::Break` with re-stamped `id`, fail-open).
- Sibling backend-trait + canonicalize/SHA-256 pattern: `ai-semantic-cache`.
- PDK skills: `pdk-mcp`, `pdk-caching`, `pdk-data-storage`,
  `pdk-distributed-cache-gossip`, `pdk-schema-definition`, `pdk-stop-execution`,
  `pdk-unit-tests`, `pdk-integration-tests`.
- Public doc: <https://docs.mulesoft.com/pdk/latest/policies-pdk-policy-templates>.
