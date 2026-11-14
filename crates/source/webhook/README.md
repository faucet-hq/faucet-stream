# faucet-source-webhook

[![Crates.io](https://img.shields.io/crates/v/faucet-source-webhook.svg)](https://crates.io/crates/faucet-source-webhook)
[![Docs.rs](https://docs.rs/faucet-source-webhook/badge.svg)](https://docs.rs/faucet-source-webhook)
[![MSRV](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Ffaucet-hq%2Ffaucet-stream%2Fmain%2FCargo.toml&query=%24.workspace.package%5B%27rust-version%27%5D&label=MSRV&color=blue)](../../../Cargo.toml)
[![License](https://img.shields.io/crates/l/faucet-source-webhook.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **webhook receiver** source that starts a temporary HTTP server, collects incoming `POST` payloads as JSON records, and then hands them to the pipeline. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Reach for it to capture push-style events — GitHub/GitLab hooks, payment events, SaaS callbacks, IoT pushes — without standing up a separate ingestion service. The server is short-lived: it runs for a bounded receive window (a timeout and/or a payload cap) and shuts down. By default it answers a sender's `POST` only after the pipeline has written its event, so a failed run never loses an event it acknowledged (see [Acknowledgement](#acknowledgement)). Built on [`axum`](https://crates.io/crates/axum) + [`tokio`](https://crates.io/crates/tokio), with a constant-time shared-secret check and a hard request-body cap so a single huge POST can't exhaust memory.

## Feature highlights

- **Zero-infrastructure receiver** — one `axum` route, bound on demand; no broker or queue to operate.
- **Acknowledged means written** — with `ack: on_commit` (the default) a sender gets `200` only once its event's page has been written and flushed; if the run fails first it gets `503` + `Retry-After` and retries.
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
| `listen_addr` | string | `"127.0.0.1:8080"` | Address to bind the HTTP server to. Defaults to **loopback only**; bind `0.0.0.0` only behind a trusted gateway (and set `auth_token`). Annotated `x-faucet-resource: listen`: in a config submitted to `faucet serve` it must be a literal `address:port` inside the server's `--allow-listen` ranges (default `127.0.0.1` / `[::1]`, ports 29100–29199), and off loopback also needs a non-empty `auth_token` or a `signature` with a non-empty `secret`. |
| `path` | string | `"/webhook"` | Endpoint path that accepts `POST` requests. Supports matrix-context substitution — e.g. `path: /hooks/${row.id}` resolves per row at runtime, percent-encoded into one segment (a value of `.` or `..` is refused); `{key}` context values are percent-encoded the same way. Must start with `/`; `?`, `#`, whitespace, `:`/`*`-prefixed segments and braces other than a whole `{name}` segment are rejected at config load (and by `faucet validate`). Through `fetch_with_context` a `{key}` may also sit inside a segment (`/hooks/user-{id}`); a placeholder the context leaves unfilled is refused rather than served as a route capture. |

### Termination

The receive window ends as soon as **any** configured condition is met; the server then drains its buffer and returns.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout_secs` | int | `30` | How long to listen before returning, in seconds. Always applies. In a config submitted to `faucet serve` it may be 1 to 3,600; `null` and `0` are refused. |
| `max_payloads` | int \| null | `null` | Stop once this many payloads have been received. `null` means collect until `timeout_secs` expires. The cap is **exact** under concurrent load. In a config submitted to `faucet serve` it may be 1 to 1,000,000; `null` and `0` are refused. |

### Reliability & security

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_body_bytes` | int | `1048576` | Max accepted request body size (1 MiB). Larger POSTs are rejected with `413 Payload Too Large` before buffering, so one huge request can't exhaust memory. |
| `max_buffered_bytes` | int | `268435456` | Most request-body bytes held in memory (256 MiB). With `ack: on_commit` it bounds the bodies held and not yet acknowledged: a POST that would cross it is refused with `503` + `Retry-After` and the window stays open. With `ack: on_receive` it bounds one window: a POST that would cross it closes the window — the stored payloads are emitted — and is refused. An empty inbox always takes one POST, however large (`max_body_bytes` bounds it). `0` removes the bound. In a config submitted to `faucet serve` it may be 1 to 1 GiB (1,073,741,824); `null` and `0` are refused. |
| `auth_token` | string \| null | `null` | Optional shared secret. When set, requests must carry it in the `Authorization` header (raw value or `Bearer <token>`); others get `401`. Comparison is constant-time. An empty or blank value is refused. Strongly recommended whenever `listen_addr` isn't loopback. |
| `signature` | object \| null | `null` | HMAC verification of the raw body for senders that sign requests: `header` (signature header), `secret`, `algorithm` (`sha256` default / `sha512`), `encoding` (`hex` default, case-insensitive / `base64`), optional `prefix` (e.g. `sha256=`), optional `timestamp_header` (then the signed content is `<timestamp>.<body>` and a timestamp more than `tolerance_secs` — default 300 — from now is refused, and a request repeating one already stored in the window gets `200` without being stored again; the timestamp must be plain unix seconds, as the content is rebuilt from its integer value). One signature per request in the `<timestamp>.<body>` layout is supported — schemes that sign another layout (`v0:<ts>:<body>`), pack `t=…,v1=…` into one header, send several signatures during key rotation, or use URL-safe / unpadded base64 cannot be verified. Checked in constant time; failures get `401`. |

### Acknowledgement

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ack` | `on_commit` \| `on_receive` | `on_commit` | When a sender's `POST` is answered. `on_commit`: after the pipeline has written and flushed its event. `on_receive`: as soon as it is held in memory (the behaviour before #1032), which loses acknowledged events if the run fails before the sink commits them. See [Acknowledgement](#acknowledgement). |
| `max_hold_secs` | int | `5` | `ack: on_commit`: the longest a held request waits before its page is emitted, 1 to 300. |
| `max_held` | int | `2000` | `ack: on_commit`: the most requests open at once, waiting for a page or in the page being written (the default leaves room for one full page being written and the next filling). Above it a `POST` is refused with `503` + `Retry-After` and not stored. `0` removes the cap. In a config submitted to `faucet serve` it may be at most 100,000. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. With `ack: on_commit` a page is emitted while the window is open, once this many requests are held (or the oldest has waited `max_hold_secs`); `0` drops the count trigger. With `ack: on_receive` the window's events are emitted when it closes, in pages of this size; `0` emits them as one page. Validated against `MAX_BATCH_SIZE` (1,000,000) at load time. |

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

## Acknowledgement

A webhook sender does not keep its events: once it gets a `2xx`, it considers the event delivered. So the source answers only when that is true.

**`ack: on_commit` (default).** Each accepted `POST` is held open. While the window is open the source emits a page as soon as `batch_size` requests are held or the oldest has waited `max_hold_secs`. Every page carries a bookmark (`{"received": n}`), so the pipeline writes the page, runs its quality / contract / DLQ passes, flushes the sink and saves state before it asks for the next page. When it asks, the source answers that page's requests `200`.

- **The run fails** (a sink error, a budget, a crash): every held request is answered `503` + `Retry-After: 60`, or its connection drops. The sender retries; nothing it was told was delivered is lost.
- **The run is cancelled** (`faucet serve` cancel, timeout, shutdown): the window closes, what is held is emitted and written, and those senders get `200`.
- **Duplicates are possible:** a page written just before a crash, or a sink write slower than the sender's timeout, leads the sender to retry an event that was already written. Deduplicate on the sender's delivery id with `write_mode: upsert` and a `key`.
- **`200` means "handled by your pipeline's rules"**, not always "stored": rows dropped by a `filter`, a contract in `warn` mode or a drift `ignore` policy, or sent to the DLQ, are acknowledged once their page is processed.
- **How long a sender waits:** up to `max_hold_secs` plus the sink's write time, and up to twice the write time when a write is already in progress. Keep that well under your senders' timeout (often 10–30 s). The source logs a warning when a sender waited more than 10 s.
- **Open connections and memory** are capped by `max_held` (default 2000 requests, counting the page being written) and `max_buffered_bytes` (bodies not yet acknowledged). Over either, a `POST` gets `503` + `Retry-After` and the window stays open.
- **Commands that read without writing refuse this source:** `--dry-run`, `--limit`, `faucet preview`, `faucet plan --live`, `faucet verify`, topology graphs and `write_mode: overwrite`; a post-run `verify:` pass skips it with a warning. A sink that replaces its output on every run (`file`, `s3`, `gcs`, `azure-blob` or `sftp` with `if_exists: replace`, `jsonl` / `csv` without `append: true`) is refused too, because the next run would delete events already acknowledged.

**`ack: on_receive`.** The behaviour before #1032: a `POST` is answered `200` as soon as it is held in memory, and the window's events are emitted when it closes. Events acknowledged this way are lost if the run fails before the sink commits them. Use it only when a sender cannot wait for the write.

The buffered library calls (`WebhookSource::fetch_all`, `Source::fetch_with_context`) cannot know when their caller writes what they return, so they always answer on receipt.

## Streaming & batching

- With `ack: on_commit` pages flow while the window is open: one page per `batch_size` held requests, or what is held once the oldest has waited `max_hold_secs`. `batch_size = 0` emits only on `max_hold_secs` and when the window closes.
- With `ack: on_receive` the window's events are emitted when it closes, `batch_size` per page (`0` = one page), with no bookmark.
- Memory at receive time is bounded by `max_buffered_bytes` (256 MiB by default) per window; `timeout_secs` and `max_payloads` cap how much is collected.

There is no resume position for a webhook receiver (see [Resume & state](#resume--state)); the `{"received": n}` bookmark on an `on_commit` page only makes the pipeline flush before the next page, and is never read back.

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
| `ack: on_commit`, `max_held` requests or `max_buffered_bytes` already open | *(not stored)* | `503 Service Unavailable` + `Retry-After: 60` |
| `ack: on_commit`, its page written and flushed | the parsed value | `200 OK`, sent then |
| `ack: on_commit`, the run fails before its page is written | *(written or not, never acknowledged)* | `503 Service Unavailable` + `Retry-After: 60` |
| A byte-identical resend of a signed request whose page is still being written | *(not stored again)* | `503 Service Unavailable` + `Retry-After: 60` |

Only `POST` is routed; other methods on the path return `405 Method Not Allowed` from `axum`.

## Resume & state

**Not supported.** A webhook receiver has no replayable position to bookmark — events are pushed to it, not pulled from an offset — so a stored bookmark is never applied. With `ack: on_commit` the sender holds each event until it is written, which is what makes a failed run safe; if you need replayable ingestion beyond that, push the events into a broker (e.g. Kafka or Redis Streams) and consume from there with a resumable source.

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

The source builds a single-route `axum::Router` with a `DefaultBodyLimit` set to `max_body_bytes` and binds a `tokio::net::TcpListener` to `listen_addr`. The window ends on the first of: `timeout_secs`, `max_payloads` / `max_buffered_bytes` reached, or (with `ack: on_commit`) the run's cancel.

- **`ack: on_commit`** — the handler stores the request with a one-shot channel and, after releasing the inbox lock, waits on that channel or a run-wide abort token. The page stream takes held requests into pages and answers a page's channels when it is polled again. When the stream ends or is dropped, a guard cancels the abort token (every request still waiting gets `503`), stops the server, and aborts it after a 10 s grace.
- **`ack: on_receive` and the buffered calls** — the handler pushes the payload into the window's buffer and answers at once; the buffer is returned when the window closes.

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
| Received more than `max_payloads`? | Won't happen — the cap is exact; surplus concurrent POSTs are refused with `503 Service Unavailable` and a `Retry-After` header, so the sender retries them later. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- Related crates: [faucet-source-websocket](https://crates.io/crates/faucet-source-websocket) · [faucet-source-kafka](https://crates.io/crates/faucet-source-kafka) · [faucet-sink-http](https://crates.io/crates/faucet-sink-http)

## Capabilities

The crate exports `DESCRIPTOR` (RFC 0013). It declares no optional source capabilities: no exactly-once replay, discovery, sharding, columnar or native path. `faucet list --json` prints the declaration.

## How it is built

The source runs on the family-neutral source driver (RFC 0014):
`WebhookDriver` implements `faucet_core::driver::SourceDriver`:

- `read` streams held pages under `ack: on_commit`, or serves until the window closes and pages what arrived by `batch_size` under `ack: on_receive`.
- `fetch` is the buffered read (answers on receipt).
- `consumes_destructively` is `true` and `set_cancel` takes the run's cancel (the source drains) under `ack: on_commit`.
- `check` probes the listen address without starting the receive loop.

`WebhookSource` wraps it in a `DriverSource`. Reads have side effects (a sender is answered), which the driver adds to the derived capabilities.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
