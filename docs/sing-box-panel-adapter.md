# sing-box Panel Adapter

A standalone daemon that connects a sing-box node to a cloud panel using the
[sing-box Panel Adapter REST API v1](../.omo/plans/sing-box-panel-api-spec-v1.md).

The adapter is a separate binary (`sing-box-panel-adapter`) that manages the
sing-box lifecycle, polls for configuration and user updates, reports traffic,
and sends heartbeats — all through a clean, stateless REST interface.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                  sing-box-panel-adapter                   │
│                                                          │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌───────────┐  │
│  │ Config   │  │ Client  │  │ Runtime  │  │  State     │  │
│  │ Loader   │  │ (REST)  │  │ Manager  │  │  Store     │  │
│  └────┬─────┘  └────┬────┘  └────┬─────┘  └─────┬─────┘  │
│       │             │            │               │        │
│       │        ┌────┴────┐  ┌────┴─────┐   ┌────┴────┐   │
│       │        │  User   │  │  Traffic │   │  Report  │   │
│       │        │ Poller  │  │ Tracker  │   │ Journal  │   │
│       │        └────┬────┘  └────┬─────┘   └────┬────┘   │
│       │             │            │               │        │
│       └─────────────┴──────┬─────┴───────────────┘        │
│                            │                              │
│                       ┌────┴────┐                         │
│                       │  Box    │  ← sing-box core        │
│                       │ (in-proc)│                         │
│                       └────┬────┘                         │
└────────────────────────────┼──────────────────────────────┘
                             │
                    ┌────────┴────────┐
                    │   Panel API     │
                    │  (HTTPS REST)   │
                    └─────────────────┘
```

The adapter runs as an external wrapper. It creates and manages an in-process
`box.Box` instance — it does **not** modify the `sing-box` binary or its
existing command-line behavior.

## Quick start

```bash
# Build the adapter binary
go build -o sing-box-panel-adapter ./cmd/sing-box-panel-adapter

# Run with a configuration file
./sing-box-panel-adapter -c /etc/sing-box/adapter-config.json
```

## Configuration reference

The adapter reads a local JSON configuration file. All field names use
`snake_case` — never V2bX-style names like `ApiKey` or `NodeID` (camelCase).

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `panel_base_url` | string | **yes** | Panel API base URL (e.g. `https://panel.example.com`). Must use `https` unless `insecure` is set. Must not contain tokens or secrets in query parameters. |
| `node_id` | string | **yes** | Node identifier assigned by the panel. Used as a path parameter in API requests, **not** as an authentication mechanism. |
| `node_token` | string | **yes** | Bearer token for `Authorization` header authentication. Must never appear in URLs, logs, or generated config paths. |
| `state_path` | string | **yes** | File path for durable adapter state (revisions, ETags, pending traffic reports). Parent directory must exist and be writable. |
| `generated_config_path` | string | no | Optional file path where the adapter writes the generated sing-box config for debugging. The filename must not contain secret-like patterns (`token`, `secret`, `apikey`, `password`). |
| `http_timeout` | duration | no | HTTP client timeout as a Go duration string (e.g. `"30s"`, `"5m"`). Default: no timeout. |
| `poll_interval_bounds` | object | no | Adaptive polling bounds: `min_seconds` and `max_seconds`. The panel's `poll_intervals` take precedence; these are local safety bounds. |
| `log_level` | string | no | Log level: `trace`, `debug`, `info`, `warn`, `error`. Default: `info`. |
| `insecure` | bool | no | Allow `http://` panel base URL. **Development only.** Must not be used in production. |

### Example configuration

See [`docs/examples/adapter-config.json`](examples/adapter-config.json).

## Authentication

The adapter authenticates every request with a bearer token in the
`Authorization` header:

```http
Authorization: Bearer <node_token>
```

**Security rules:**

- The token authenticates exactly one node identity.
- `node_id` in the path is a resource identifier, **not** an auth mechanism.
- Tokens must **never** appear in URL paths, query strings, generated config
  paths, log messages, or error output.
- TLS is required in production. Set `insecure: true` only for local
  development.
- The adapter never logs full request/response bodies at `info` level. Debug
  logs redact passwords, bearer tokens, and credential bodies.

## Supported protocols (v1)

| Protocol | Credential type | User apply method |
| --- | --- | --- |
| `hysteria2` | `password` | Hot reload via `ReplaceInboundUsers` |
| `anytls` | `password` | Hot reload via `ReplaceInboundUsers` |
| `shadowsocks` | `password` | Hot reload via `ReplaceInboundUsers` |

v1 supports only the `password` credential type. Unknown credential types are
rejected. Unknown managed inbound protocols are retained as unsupported
metadata and reported as `unsupported_protocol` in heartbeats.

## Apply strategies

### Configuration change strategy

The panel specifies how the adapter should react when the configuration
changes via `apply_strategy.on_configuration_change`:

| Strategy | Behavior |
| --- | --- |
| `restart_process` | Full `box.Box` recreation: close the old instance, create and start a new one. **Not** OS-level self-exec — the adapter process itself stays alive. |
| `recreate_instance` | Same as `restart_process` in this in-process wrapper: full Box recreation. |
| `manual` | Record the pending revision in state without applying. Report it in heartbeats for operator action. |

**Important:** In this in-process adapter, `restart_process` is explicitly
mapped to Box recreation (closing the old `box.Box` and creating a new one).
It does **not** re-exec the OS process. This mapping is documented and tested
to avoid confusion with `sing-box run` SIGHUP behavior.

### User change strategy

| Strategy | Behavior |
| --- | --- |
| `hot_reload_users` | Apply user snapshots without restarting the Box. This is the **only** supported user apply strategy in v1. |
| `restart_process` | Not supported for user changes in v1. Reports `apply_failed` if encountered. |
| `recreate_instance` | Not supported for user changes in v1. Reports `apply_failed` if encountered. |

## Fail-closed behavior

When the initial user fetch for a managed inbound fails, the adapter applies
**fail-closed** semantics:

- No dynamic users are allowed for that inbound until a successful user-set
  fetch completes.
- The inbound's `user_load_status` is set to `empty_initial_load`.
- The sing-box config template has its managed inbound user arrays stripped to
  `[]` before Box creation, ensuring no stale or placeholder users are active.

For subsequent failures after a successful load, the adapter keeps the last
applied user set and reports `stale` in heartbeats.

## Traffic reporting

The adapter tracks per-user traffic deltas keyed by `(inbound_id, user_id)`
and reports them to the panel:

- **Delta semantics:** Each report covers a half-open time interval
  `[started_at, ended_at)`.
- **Idempotency keys:** Every report includes a stable `Idempotency-Key`
  header. Retries of the same report reuse the exact same key and body to
  prevent double-counting.
- **Retry behavior:** On transport errors, HTTP 429, or 503, the adapter
  retries with the same idempotency key and identical body. Live traffic
  counters are **not** cleared until the report is accepted or durably
  persisted.
- **Conflict handling:** A 409 `IDEMPOTENCY_CONFLICT` (same key, different
  body) is a hard error requiring operator attention.
- **Accounting identity:** Traffic is scoped by the tuple
  `(node_id, inbound_id, user_id)`. The same `user_id` may appear in
  multiple inbounds with different credentials.

## Heartbeat statuses

The adapter reports per-inbound user load status in heartbeats. All six
status values:

| Status | Description |
| --- | --- |
| `ok` | Latest known user snapshot applied successfully. |
| `stale` | User polling failed after a previous successful load; old snapshot retained. |
| `empty_initial_load` | No successful initial user snapshot; inbound is fail-closed (no dynamic users). |
| `revision_conflict` | User response was discarded because `configuration_revision` did not match the applied config. Triggers config refetch. |
| `unsupported_protocol` | Config declared a protocol not in the v1 allow-list. No user apply attempted. |
| `apply_failed` | User snapshot was fetched but could not be applied (e.g., unsupported user apply strategy, protocol apply error). |

## v1 limits and scope

v1 is intentionally limited:

- **Aggregate metrics only:** Runtime metrics in heartbeats include connection
  counts, uptime, and memory. Reserved limit/IP fields are observational
  aggregate counts only — the adapter **never** enforces rate limits, device
  limits, IP limits, quota, billing, expiry, or package permissions.
- **No raw IP lists:** The adapter does not persist, report, or log raw client
  IP addresses. Only aggregate counts (e.g., distinct source IP count) may
  appear in runtime metrics.
- **Password credentials only:** v1 supports `credential.type = "password"` for
  all three protocols. Future credential types will extend this enum.
- **Hot reload users only:** The only supported `on_user_change` strategy in v1
  is `hot_reload_users`. Other strategies report `apply_failed`.

## API endpoints

The adapter calls exactly four panel API endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/nodes/{node_id}/configuration` | Fetch sing-box config template and adapter metadata |
| `GET` | `/api/v1/nodes/{node_id}/inbounds/{inbound_id}/users` | Fetch authoritative user snapshot for one inbound |
| `POST` | `/api/v1/nodes/{node_id}/traffic-reports` | Submit incremental traffic deltas |
| `POST` | `/api/v1/nodes/{node_id}/heartbeats` | Report liveness, revisions, and statuses |

The adapter does **not** call V2bX, V2Board, XrayR, or SSPanel compatibility
endpoints. See the [anti-compat guardrails](#anti-compat-guardrails) section.

## Signals

| Signal | Behavior |
| --- | --- |
| `SIGINT` / `SIGTERM` | Graceful shutdown: stop polling, flush state, close Box. |
| `SIGHUP` | Reload local adapter config (log level, etc.) without losing pending traffic. |

## Anti-compat guardrails

The adapter explicitly avoids compatibility with legacy panel APIs. A test
(`TestForbiddenCompatibilityStrings`) scans all adapter source code for
forbidden patterns that would indicate accidental compatibility with V2bX,
V2Board, XrayR, or SSPanel:

| Forbidden pattern | Reason |
| --- | --- |
| `UniProxy` | V2bX compatibility endpoint |
| `/server/v1` | V2bX server API path |
| `/api/v1/server/` | V2bX server API path prefix |
| `uPSK` | V2bX Shadowsocks multi-user key format |
| `node_type` | V2bX node type field |
| `ApiKey` | V2bX configuration field name |
| `V2Board` | Legacy panel name |
| `XrayR` | Legacy panel agent name |
| `SSPanel` | Legacy panel name |

If any of these patterns appear in adapter source code (excluding this
documentation and the test itself), the anti-compat test will fail.

## Security notes

- **Bearer token header auth:** Tokens are sent only in the `Authorization`
  header, never in URLs, query strings, or request bodies.
- **TLS required:** Production deployments must use `https://` for
  `panel_base_url`. The `insecure` flag is for development only.
- **No secrets in generated config paths:** The `generated_config_path`
  filename is validated to not contain patterns like `token`, `secret`,
  `apikey`, or `password`.
- **No secrets in state:** The state file stores revisions, ETags, and pending
  traffic reports — never bearer tokens, user passwords, or TLS keys.
- **Log redaction:** Full request/response bodies are never logged at `info`
  level. Debug logs must redact `Authorization` headers, passwords, and
  credential bodies.
- **Cache-Control verification:** The client verifies that configuration and
  user responses include `Cache-Control: no-store`.

## Reference

- [sing-box Panel Adapter REST API v1 Specification](../.omo/plans/sing-box-panel-api-spec-v1.md)
- [Adapter configuration example](examples/adapter-config.json)
