# MCP Response Cache — Playground

Local smoke-test environment for the `mcp-response-cache-policy`.

## Run

From the policy `implementation/` directory:

```bash
make run
```

This builds the policy WASM, patches `config/api.yaml` with the live
policy-ref name, and starts a local Omni Gateway plus an `httpbin` backend via
`docker-compose.yaml`. The gateway listens on `localhost:8081`.

> A local Omni Gateway `registration.yaml` (gitignored — it holds TLS secrets)
> must be present in `config/`. Register a local gateway to generate it, or copy
> one from another policy's playground.

## Sample config

`config/api.yaml` caches discovery for 60s and enables caching for two example
tools (`search` at `shared` scope, `get_my_profile` at `identity` scope).

## Smoke test

Send JSON-RPC bodies to `http://localhost:8081/post`:

```bash
# Discovery — repeated calls should serve from cache (x-mcp-cache: hit)
curl -sD- http://localhost:8081/post \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

# Force a bypass
curl -sD- http://localhost:8081/post \
  -H 'content-type: application/json' \
  -H 'cache-control: no-cache' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

Inspect the `x-mcp-cache: hit | miss | bypass` response header to confirm cache
behavior.

> The current scaffold passes traffic through (fail-open) and does not yet emit
> cache hits; full lifecycle lands in the implementation phase.
