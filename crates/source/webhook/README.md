# faucet-source-webhook

[![Crates.io](https://img.shields.io/crates/v/faucet-source-webhook.svg)](https://crates.io/crates/faucet-source-webhook)
[![Docs.rs](https://docs.rs/faucet-source-webhook/badge.svg)](https://docs.rs/faucet-source-webhook)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-webhook.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-webhook.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **webhook receiver** source that starts a temporary HTTP server, collects incoming `POST` payloads as JSON records, and then hands them to the pipeline. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Reach for it to capture push-style events — GitHub/GitLab hooks, payment events, SaaS callbacks, IoT pushes — without standing up a separate ingestion service. The server is short-lived: it runs for a bounded receive window (a timeout and/or a payload cap), drains everything it collected, and shuts down. Built on [`axum`](https://crates.io/crates/axum) + [`tokio`](https://crates.io/crates/tokio), with a constant-time shared-secret check and a hard request-body cap so a single huge POST can't exhaust memory.

## Feature highlights

- **Zero-infrastructure receiver** — one `axum` route, bound on demand; no broker or queue to operate.
- **Bounded receive window** — terminates on `timeout_secs`, on `max_payloads`, or both (whichever comes first), so a run always completes.
- **Lenient body parsing** — JSON bodies become native JSON values; non-JSON-but-UTF-8 bodies become JSON strings; non-UTF-8 bodies are rejected with `400`.
- **Request-size guard** — `max_body_bytes` (default 1 MiB) rejects oversized POSTs with `413` before buffering them.
- **Optional shared secret** — `auth_token` requires a token in the `Authorization` header (raw or `Bearer <token>`); the comparison is **constant-time** (no timing side-channel).
- **Exact payload cap** — under concurrent arrivals the in-memory buffer never exceeds `max_payloads`; a surplus POST, or one arriving as the window closes, is refused with `503` + `Retry-After` so the sender retries it, never acknowledged and dropped.
- **Fast preflight** — `faucet doctor` verifies the listen address is bindable without booting the receive loop.

## Installation

```bash
# As a library:
cargo add faucet-source-webhook
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-webhook
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-webhook
```

`source-webhook` is **not** in the default feature set — opt in explicitly.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: webhook
    config:
      listen_addr: "127.0.0.1:8080"
      path: /webhook
      max_payloads: 100
      timeout_secs: 60
  sink:
    type: file
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

The server listens on `127.0.0.1:8080/webhook`, collects up to 100 POSTed payloads (or until 60 s elapse), writes them to `events.jsonl`, and exits.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | string | `"127.0.0.1:8080"` | Address to bind the HTTP server to. Defaults to **loopback only**; bind `0.0.0.0` only behind a trusted gateway (and set `auth_token`). |
| `path` | string | `"/webhook"` | Endpoint path that accepts `POST` requests. Supports matrix-context substitution — e.g. `path: /hooks/${row.id}` resolves per row at runtime; `{key}` context values are percent-encoded into one segment. Must start with `/`; `?`, `#`, whitespace, `:`/`*`-prefixed segments and braces other than a whole `{name}` segment are rejected at config load. |

### Termination

The receive window ends as soon as **any** configured condition is met; the server then drains its buffer and returns.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout_secs` | int | `30` | How long to listen before returning, in seconds. Always applies. |
| `max_payloads` | int \| null | `null` | Stop once this many payloads have been received. `null` means collect until `timeout_secs` expires. The cap is **exact** under concurrent load. |

### Reliability & security

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_body_bytes` | int | `1048576` | Max accepted request body size (1 MiB). Larger POSTs are rejected with `413 Payload Too Large` before buffering, so one huge request can't exhaust memory. |
| `auth_token` | string \| null | `null` | Optional shared secret. When set, requests must carry it in the `Authorization` header (raw value or `Bearer <token>`); others get `401`. Comparison is constant-time. Strongly recommended whenever `listen_addr` isn't loopback. |
| `signature` | object \| null | `null` | HMAC verification of the raw body for senders that sign requests: `header` (signature header), `secret`, `algorithm` (`sha256` default / `sha512`), `encoding` (`hex` default, case-insensitive / `base64`), optional `prefix` (e.g. `sha256=`), optional `timestamp_header` (then the signed content is `<timestamp>.<body>` and a timestamp more than `tolerance_secs` — default 300 — from now is refused). Checked in constant time; failures get `401`. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage` when chunking the collected buffer downstream. `0` is the "no batching" sentinel — emit the whole flush window in one page. See [Streaming & batching](#streaming--batching). Validated against `MAX_BATCH_SIZE` (1,000,000) at load time. |

## Authentication

The webhook source is a **server**, not a client, so it does not use the shared `auth: { ref }` provider catalog or the `{ type, config }` auth wire shape. Instead it accepts a single inbound shared secret via the top-level `auth_token` field. Callers must send it in the `Authorization` header, either raw or `Bearer`-prefixed:

```yaml
source:
  type: webhook
  config:
    listen_addr: "0.0.0.0:9090"
    path: /github-webhooks
    auth_token: "${env:WEBHOOK_SECRET}"   # never hard-code secrets
```

```bash
# Both of these are accepted:
curl -X POST http://host:9090/github-webhooks -H "Authorization: s3cr3t"        -d '{"event":"push"}'
curl -X POST http://host:9090/github-webhooks -H "Authorization: Bearer s3cr3t" -d '{"event":"push"}'
```

Requests missing or mismatching the token receive `401 Unauthorized`. When `auth_token` is unset, the endpoint is **unauthenticated** — only acceptable on a loopback bind or behind a gateway that authenticates for you.

## Examples

### Collect a fixed number of events, then exit

```yaml
version: 1
pipeline:
  source:
    type: webhook
    config:
      listen_addr: "127.0.0.1:8080"
      path: /events
      max_payloads: 5
      timeout_secs: 300        # safety cap — return after 5 min even if < 5 arrive
  sink:
    type: stdout
    config:
      format: jsonl
```

### Authenticated public receiver behind a gateway

```yaml
version: 1
pipeline:
  source:
    type: webhook
    config:
      listen_addr: "0.0.0.0:9090"
      path: /hooks/incoming
      auth_token: "${env:WEBHOOK_SECRET}"
      max_body_bytes: 4194304   # accept up to 4 MiB bodies
      timeout_secs: 3600
  sink:
    type: postgres
    config:
      connection_url: "${env:DATABASE_URL}"
      table: webhook_events
```

### Time-limited capture into Parquet

```yaml
version: 1
pipeline:
  source:
    type: webhook
    config:
      listen_addr: "127.0.0.1:8080"
      path: /telemetry
      timeout_secs: 30          # collect everything received in 30 s
      batch_size: 0             # emit a single page (load-job friendly)
  sink:
    type: file
    config:
      path: ./telemetry.parquet
```

## Streaming & batching

The webhook source is **buffer-shaped**: it has no native streaming primitive, so it keeps the default chunk-the-buffer `Source::stream_pages` implementation. During the receive window the `axum` handler accumulates every accepted POST into an in-process `Vec`; once the window closes (timeout or `max_payloads`), the default trait impl chunks that buffer into pages of `batch_size` records.

- `batch_size` shapes only the **downstream** page size handed to the sink — it does not change server-side buffering, which always pushes into the in-memory `Vec` until the receive window ends.
- `batch_size = 0` emits the entire flush window in one page. For this source it is functionally equivalent to any positive value larger than the received payload count.
- To bound memory at **receive** time, tune `timeout_secs` and `max_payloads` (which cap how much is collected), not `batch_size`.

Every page carries `bookmark: None` — there is no incremental-replication or resume mode for a webhook receiver (see [Resume & state](#resume--state)).

## Payload handling

| Request body | Stored as | HTTP response |
|--------------|-----------|---------------|
| Valid JSON (object/array/scalar) | the parsed JSON value | `200 OK` |
| UTF-8 but not JSON | a JSON string of the raw text | `200 OK` |
| Non-UTF-8 bytes | *(not stored)* | `400 Bad Request` |
| Larger than `max_body_bytes` | *(not stored)* | `413 Payload Too Large` |
| Missing/wrong `auth_token` (when set) | *(not stored)* | `401 Unauthorized` |
| Missing/invalid `signature` (when set) | *(not stored)* | `401 Unauthorized` |
| Arriving after `max_payloads` is reached or the window closed | *(not stored)* | `503 Service Unavailable` + `Retry-After: 60` |

Only `POST` is routed; other methods on the path return `405 Method Not Allowed` from `axum`.

## Resume & state

**Not supported.** A webhook receiver has no replayable position to bookmark — events are pushed to it, not pulled from an offset — so the source does not override `state_key()` / `apply_start_bookmark()` and emits `bookmark: None`. For at-most-once delivery to the sink within a single run, rely on `max_payloads` + `timeout_secs` to bound the window. If you need durable, replayable ingestion, push the events into a broker (e.g. Kafka or Redis Streams) and consume from there with a resumable source.

## Config loading

```rust,no_run
use faucet_core::config::{load_json, load_env_file};
use faucet_source_webhook::WebhookSourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config: WebhookSourceConfig = load_json("config.json")?;
let config: WebhookSourceConfig = load_env_file(".env", "WEBHOOK")?;
# Ok(()) }
```

### Example JSON config

```json
{
  "listen_addr": "0.0.0.0:9090",
  "path": "/hooks/incoming",
  "max_payloads": 100,
  "timeout_secs": 120,
  "max_body_bytes": 1048576,
  "batch_size": 1000
}
```

### Example .env file

```env
WEBHOOK_LISTEN_ADDR=0.0.0.0:8080
WEBHOOK_PATH=/webhook
WEBHOOK_MAX_PAYLOADS=50
WEBHOOK_TIMEOUT_SECS=60
```

## Schema introspection

```bash
faucet schema source webhook
```

Prints the full JSON Schema for `WebhookSourceConfig` (field names, types, defaults).

## Library usage

```rust,no_run
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::Source;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = WebhookSourceConfig::new()
    .listen_addr("0.0.0.0:9090")
    .path("/github-webhooks")
    .auth_token("s3cr3t")
    .max_payloads(100)
    .timeout_secs(3600)
    .with_batch_size(500);

let source = WebhookSource::new(config);

// Blocks until the receive window closes (timeout or max_payloads),
// then returns every collected payload.
let records = source.fetch_all().await?;
println!("Received {} webhook payloads", records.len());
# Ok(()) }
```

Or drive it through a `Pipeline` to stream collected payloads straight into any sink:

```rust,no_run
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::{Pipeline, Source, Sink};

# async fn example(my_sink: Box<dyn Sink>) -> Result<(), Box<dyn std::error::Error>> {
let source = WebhookSource::new(
    WebhookSourceConfig::new()
        .listen_addr("0.0.0.0:9090")
        .path("/github-webhooks")
        .max_payloads(100)
        .timeout_secs(3600),
);

let pipeline = Pipeline::new(Box::new(source), my_sink);
let result = pipeline.run().await?;
println!("Processed {} webhook events", result.records_written);
# Ok(()) }
```

## How it works

`fetch_with_context` (and the convenience `fetch_all`) build a single-route `axum::Router` with a `DefaultBodyLimit` set to `max_body_bytes`, bind a `tokio::net::TcpListener` to `listen_addr`, and then `tokio::select!` three futures: the running server, a `timeout_secs` sleep, and a `Notify` that fires when `max_payloads` is reached. Whichever resolves first ends the window; the collected `Vec` is cloned out and returned.

- **Exact cap under concurrency** — each accepted POST decides under the shared records lock whether there's room and whether the cap is now satisfied, so concurrently in-flight requests can never push the buffer past `max_payloads` (surplus is dropped while still notifying `done`).
- **Constant-time auth** — the shared-secret check uses `subtle::ConstantTimeEq` with a non-short-circuiting compare across both the raw and `Bearer` forms, so neither which form matched nor where the first differing byte is can be inferred from response timing.
- **Matrix context** — when invoked with a non-empty context map, `path` is interpolated (`${row.id}` etc.) before the route is registered, so one config can fan out to per-row paths.
- **Preflight** — `check()` (used by `faucet doctor`) just binds and immediately drops a listener on `listen_addr` to confirm the port is free, rather than the default probe that would boot the receive loop and block for the whole window.

## Lineage dataset URI

`webhook://<listen_addr><path>` — e.g. `webhook://0.0.0.0:8080/hooks/incoming`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI / umbrella crate via the `source-webhook` feature (opt-in; not in the default set).

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: failed to bind to <addr>` | The port is already in use, the address is malformed, or you lack permission to bind it (e.g. a privileged `:80`). Pick a free, high port; check with `faucet doctor`. |
| The run finishes with **zero** records | No POSTs arrived before `timeout_secs` expired — or they hit the wrong `path`/port. Confirm the sender's URL matches `listen_addr` + `path`, and raise `timeout_secs`. |
| Senders get `401 Unauthorized` | `auth_token` is set but the request lacks a matching `Authorization` header. Send the token raw or as `Bearer <token>`; verify the value matches exactly. |
| Senders get `413 Payload Too Large` | The body exceeds `max_body_bytes` (default 1 MiB). Raise `max_body_bytes` if large payloads are expected. |
| Senders get `400 Bad Request` | The body is not valid UTF-8 (binary uploads aren't supported). Have the sender post UTF-8 JSON or text. |
| Senders get `405 Method Not Allowed` | Only `POST` is routed. Ensure the client uses `POST` on the configured `path`. |
| The run blocks indefinitely | `max_payloads` is unset and `timeout_secs` is very large, so the server waits the full window. The window always ends at `timeout_secs`; lower it or set `max_payloads`. |
| Remote senders can't reach the server | The default `listen_addr` is loopback (`127.0.0.1`). Bind `0.0.0.0` to accept external traffic — and set `auth_token` (and/or front it with a gateway) when you do. |
| Received more than `max_payloads`? | Won't happen — the cap is exact; surplus concurrent POSTs are accepted with `200` but dropped, never stored. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- Related crates: [faucet-source-websocket](https://crates.io/crates/faucet-source-websocket) · [faucet-source-kafka](https://crates.io/crates/faucet-source-kafka) · [faucet-sink-http](https://crates.io/crates/faucet-sink-http)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
