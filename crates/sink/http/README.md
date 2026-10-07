# faucet-sink-http

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-http.svg)](https://crates.io/crates/faucet-sink-http)
[![Docs.rs](https://docs.rs/faucet-sink-http/badge.svg)](https://docs.rs/faucet-sink-http)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-http.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-http.svg)](https://github.com/faucet-hq/faucet-stream#license)

HTTP POST sink connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Sends JSON records to any HTTP(S) endpoint — a webhook, an ingest API, a serverless function, another faucet pipeline's webhook source.

Reach for it when the destination speaks HTTP but isn't one of the first-class warehouse/database/queue sinks: a SaaS ingest endpoint, an internal microservice, a Lambda/Cloud Function URL, or a fan-out webhook. It reuses a single `reqwest::Client`, sends records concurrently, and retries transient failures so it stays fast and resilient under load. This connector reports metrics under the observability label `connector="http"`.

## Feature highlights

- **Two batch modes** — `Individual` (one POST per record, sent concurrently up to a `concurrency` cap) and `Array` (records packed into a JSON-array body, re-chunked by `batch_size`).
- **Concurrent sends** — `Individual` mode drives a `FuturesUnordered` with at most `concurrency` requests in flight, refilling as each completes (no up-front permit acquisition, no deadlock).
- **Four auth methods** — `none`, `bearer`, `basic`, and `custom` headers — all on the project-wide `{ type, config }` wire shape. Tokens/passwords are masked (`***`) in `Debug` output.
- **Shared auth providers** — point `auth` at a `{ ref: <name> }` in the CLI's top-level `auth:` catalog to share one OAuth2 token (with single-flight refresh) across many sinks.
- **Configurable retry** — retriable failures (network errors, 5xx, 429) are retried up to `max_retries`; 4xx client errors fail fast.
- **Per-row DLQ in Individual mode** — `write_batch_partial` attempts every record and dead-letters only the ones that actually failed, so already-delivered rows are never duplicated against a non-idempotent endpoint.
- **Client built once** — the `reqwest::Client` is created in `new()` and reused for every request, with connection pooling and keep-alive.

## Installation

```bash
# As a library:
cargo add faucet-sink-http

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-http
```

Or via the umbrella crate: `cargo add faucet-stream --features sink-http`.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: webhook
    config:
      listen_addr: 0.0.0.0:8080
      path: /webhook
  sink:
    type: http
    config:
      url: https://downstream.example.com/ingest
      method: POST
      auth:
        type: bearer
        config:
          token: ${env:INGEST_TOKEN}
      batch_mode:
        type: Individual
      max_retries: 3
      concurrency: 16
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | Target endpoint URL. |
| `method` | string | `POST` | HTTP method (`POST`, `PUT`, `PATCH`, …). |
| `headers` | header map | empty | Additional request headers. **Library-only** — `#[serde(skip)]`, set via the `headers()` builder; not settable from YAML/JSON. |
| `auth` | `HttpSinkAuth` | `{ type: none }` | Authentication — inline `{ type, config }` or `{ ref: <name> }`. See [Authentication](#authentication). |

### Batching & concurrency

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_mode` | `HttpBatchMode` | `Individual` | `Individual` = one POST per record; `Array` = records sent as one JSON-array body. See [Batch modes](#batch-modes). |
| `concurrency` | int | `10` | Max concurrent in-flight requests in `Individual` mode. No effect in `Array` mode (one POST per chunk, issued sequentially). Clamped to a floor of `1`. |
| `batch_size` | int | `1000` | Max records per outbound HTTP request in `Array` mode (re-chunks the upstream page). **`0` = "no batching" sentinel** — forwards the whole page as one JSON array. **No effect in `Individual` mode** (each record is already its own request); kept for config-shape parity and validated via `faucet_core::validate_batch_size`. |
| `timeout` | int / null | `30` | Per-request timeout in seconds — a stalled peer fails the request (so retries and resilience act) instead of hanging the run. `null` disables it. |
| `connect_timeout` | int / null | `10` | Timeout for establishing a connection, in seconds. `null` disables it. |

### Reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | int | `0` | Number of retries on retriable failures (network errors, 5xx, 429). Each retry is immediate (no backoff). 4xx client errors fail fast. After exhausting retries the last error is returned. |

## Authentication

`auth` accepts the project-wide `{ type, config }` shape — either inline, or `{ ref: <name> }` pointing at a shared provider in the CLI's top-level `auth:` catalog.

| `type` | `config` | Description |
|--------|----------|-------------|
| `none` | *(none)* | No authentication. |
| `bearer` | `{ token }` | Sent as `Authorization: Bearer <token>`. |
| `basic` | `{ username, password }` | HTTP Basic authentication. |
| `custom` | `{ headers: { <name>: <value>, … } }` | Arbitrary header map applied to every request (e.g. an API key header). |

```yaml
# Bearer token (typically via env / secret indirection)
auth:
  type: bearer
  config:
    token: ${env:INGEST_TOKEN}
```

```yaml
# HTTP Basic
auth:
  type: basic
  config:
    username: ingest-user
    password: ${secret:vault:secret/ingest#password}
```

```yaml
# Custom API-key header
auth:
  type: custom
  config:
    headers:
      X-Api-Key: ${env:API_KEY}
```

```yaml
# Shared provider from the top-level `auth:` catalog (OAuth2 with single-flight refresh)
auth: { ref: my_idp }
```

When a shared provider is attached it **takes precedence** over inline `auth`; a bare `{ ref: <name> }` with no provider wired up is a config error.

## Examples

### Individual mode — one POST per record, sent concurrently

```yaml
sink:
  type: http
  config:
    url: https://webhooks.example.com/event
    auth:
      type: bearer
      config: { token: ${env:WEBHOOK_TOKEN} }
    batch_mode:
      type: Individual
    concurrency: 20
    max_retries: 2
```

### Array mode — bulk POST as a JSON array

```yaml
sink:
  type: http
  config:
    url: https://api.example.com/bulk-ingest
    batch_mode:
      type: Array
    batch_size: 500     # one POST per 500-record chunk
    max_retries: 3
```

### Custom method + API-key header

```yaml
sink:
  type: http
  config:
    url: https://api.example.com/data
    method: PUT
    auth:
      type: custom
      config:
        headers:
          X-Api-Key: ${env:API_KEY}
    batch_mode:
      type: Array
    batch_size: 0       # forward the whole upstream page as one array
```

## Streaming & batching

The pipeline calls `Sink::write_batch` once per upstream `StreamPage`. The on-the-wire effect depends on `batch_mode`:

- **`Individual`** — one HTTP request per record, executed concurrently up to `concurrency` via a refilling `FuturesUnordered`. `batch_size` has **no effect** on framing here (each record is already its own request); tune throughput with `concurrency`. For per-record send semantics, prefer `batch_mode: Individual` over `batch_size: 1` — it's the direct expression of intent and the only one that drives concurrent in-flight requests.
- **`Array`** — the page is re-chunked into `batch_size`-row slices and one POST is issued per chunk, each body a JSON array. With the default `batch_size: 1000`, a 2 500-record page produces 3 POSTs (1000 + 1000 + 500). With **`batch_size: 0`** the whole page is forwarded as a single JSON array — useful when the upstream source already chunks the stream to a size the destination accepts (e.g. a Postgres source with its own `batch_size`).

Recommended `batch_size` for array-accepting endpoints: match the destination's documented batch limit (commonly 100–1000 records per request).

## Dead-letter queue

The sink overrides `write_batch_partial` so a configured [DLQ](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) gets accurate per-row outcomes:

- **`Individual` mode** — every record is an independent POST, so each success/failure is attributable. The sink attempts **all** records (failures don't short-circuit siblings) and returns one outcome per record; only the genuinely-failed rows are dead-lettered. This avoids duplicating already-delivered rows against a non-idempotent endpoint under `on_batch_error: dlq_all`.
- **`Array` mode** — a single array POST cannot attribute a failure to specific rows, so it stays all-or-nothing: the whole array surfaces as an error and the DLQ `on_batch_error` policy decides whether to abort or dead-letter the batch.

This sink does **not** support effectively-once delivery or upsert/delete write modes — HTTP endpoints are opaque to faucet, so it can only append (POST). For idempotent delivery, make the destination endpoint idempotent (e.g. key on a record field) and use `Individual` mode + a DLQ.

## Config loading & schema

Load config from YAML/JSON, environment variables, or a `.env` file. Inspect the full JSON Schema with:

```bash
faucet schema sink http
```

Note: `headers` is `#[serde(skip)]` (library-only, set via the `headers()` builder). The `custom` auth variant's header map round-trips through JSON/YAML.

## Library usage

```rust
use faucet_core::Pipeline;
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkAuth, HttpSinkConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = HttpSinkConfig::new("https://api.example.com/ingest")
    .auth(HttpSinkAuth::Bearer { token: "my-token".into() })
    .batch_mode(HttpBatchMode::Array)
    .with_batch_size(500)
    .max_retries(3)
    .concurrency(20);

let sink = HttpSink::new(cfg);

// let result = Pipeline::new(source, sink).run().await?;
# let _ = sink;
# Ok(())
# }
```

Attach a shared `AuthProvider` (so several sinks share one token with single-flight refresh) via `HttpSink::new(cfg).with_auth_provider(provider)`.

## How it works

- `new()` builds the `reqwest::Client` **once** and reuses it for every request (connection pooling + keep-alive).
- Auth is resolved **once per batch** — the shared provider (if attached) wins, otherwise inline `auth` is used.
- `Individual` mode drives a `FuturesUnordered`, seeding it with up to `concurrency` futures and refilling as each completes — so exactly `concurrency` requests are in flight without acquiring permits up-front.
- `Array` mode collects each `batch_size` chunk into a `Value::Array` and POSTs it as one request body.
- Every response runs through `faucet_core::util::check_http_response`, which maps non-2xx statuses to typed `FaucetError`s; retriable ones (network, 5xx, 429) are retried up to `max_retries`.

## Lineage dataset URI

`https://<host><path>` with credentials stripped — e.g. `https://api.example.com/ingest` (a `user:pass@` prefix in the URL is redacted).

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-http` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error: "references provider … but no provider was supplied" | `auth: { ref: <name> }` points at a provider not in the top-level `auth:` catalog (or no provider was injected via `with_auth_provider`). Add the named provider, or use inline `auth`. |
| `401` / `403` from the endpoint | Wrong or expired credentials. Verify the `bearer` token / `basic` credentials, or that the shared provider's scopes are correct. |
| `Auth` error: "invalid custom header name/value" | A `custom` header name or value isn't a valid HTTP header. Remove non-ASCII / control characters. |
| Requests not parallel / slow in `Individual` mode | `concurrency` defaults to `10`. Raise it for high-fan-out endpoints; it's clamped to a floor of `1`. |
| `batch_size` seems ignored | You're in `Individual` mode, where each record is its own request — `batch_size` only affects `Array` mode. Switch to `batch_mode: Array` to pack records, or use `concurrency` to tune `Individual`. |
| Endpoint rejects large array bodies (`413` / timeout) | The `Array` body exceeds the destination's limit. Lower `batch_size` to its documented per-request cap. |
| Duplicate rows after a partial failure | The destination isn't idempotent. Use `batch_mode: Individual` + a DLQ so only failed rows are retried/dead-lettered, and make the endpoint idempotent (key on a record field). |
| `4xx` errors not retried | Intentional — only retriable failures (network, 5xx, 429) are retried. Fix the request shape (`url`, `method`, body) for 4xx. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix across all connectors.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — shared `auth:` providers and the `{ type, config }` shape.
- [Dead-letter queue cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) — `on_batch_error` policies and per-row dead-lettering.
- [`faucet-source-webhook`](https://crates.io/crates/faucet-source-webhook) — the natural upstream for HTTP fan-out pipelines.
- [`faucet-source-rest`](https://crates.io/crates/faucet-source-rest) — pull from a REST API and forward it over HTTP.

## Batch atomicity

What a failed write leaves behind (#737): **per-row** — per-request outcomes; the outer error fires only when nothing was delivered. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
