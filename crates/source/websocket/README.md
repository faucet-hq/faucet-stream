# faucet-source-websocket

[![Crates.io](https://img.shields.io/crates/v/faucet-source-websocket.svg)](https://crates.io/crates/faucet-source-websocket)
[![Docs.rs](https://docs.rs/faucet-source-websocket/badge.svg)](https://docs.rs/faucet-source-websocket)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-websocket.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-websocket.svg)](https://github.com/faucet-hq/faucet-stream#license)

**WebSocket** streaming source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Connects to a `ws://` / `wss://` endpoint, optionally sends subscription frames, and streams every incoming message into the pipeline as a record — continuously, page-by-page — until a configured stop condition (`max_messages`, `idle_timeout`) or Ctrl-C ends the run.

Reach for it when you want to tap a live feed — market-data tickers, chat/event streams, IoT telemetry, real-time APIs — and land it in any faucet-stream sink with one declarative config and no glue code. The connection is built once and the receive loop is fully async, so records flow to the sink the moment they arrive rather than waiting for the run to finish.

## Feature highlights

- **Native streaming** — `stream_pages` yields a `StreamPage` every `batch_size` records, so the sink receives data continuously throughout the run. Peak memory is `O(batch_size)` no matter how long the feed runs.
- **Subscription frames** — send any number of `subscribe_messages` (in order) immediately after every connect *and* every reconnect, so subscribe-on-open APIs work without manual steps.
- **Automatic reconnect** — opt-in `reconnect` with jittered exponential backoff from `reconnect_backoff` and an optional cap on *consecutive* failures (`max_reconnect_attempts`). The idle clock spans reconnect gaps, so a long outage still trips `idle_timeout`.
- **Three message formats** — `json` (parse each frame into a JSON value), `raw_string` (UTF-8 string), or `binary` (base64-encoded string) — covering text and binary frames alike.
- **Lenient or strict JSON parsing** — `on_parse_error: fail` aborts on a non-JSON frame; `skip` logs and drops it (handy for feeds that interleave protocol noise with data).
- **Optional envelope** — wrap each record as `{ data, received_at, url }` to keep provenance and an arrival timestamp.
- **Keepalive pings** — send a WebSocket Ping on `ping_interval` to keep the connection alive through proxies and load balancers.
- **Shared auth providers** — bearer / custom-header auth on the HTTP upgrade request, inline or via a `{ ref }` to the CLI's top-level `auth:` catalog; a shared provider's token is re-resolved on every (re)connect so a rotated token is picked up automatically.
- **Memory safety knobs** — `max_message_bytes` caps frame/message size to prevent a runaway server from exhausting memory.

## Installation

```bash
# As a library:
cargo add faucet-source-websocket
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-websocket
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-websocket
```

The connector is opt-in — it is not part of any default feature set.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: websocket
    config:
      url: wss://stream.binance.com:9443/ws/btcusdt@trade
      message_format: json
      max_messages: 100
      idle_timeout: 30
  sink:
    type: jsonl
    config:
      path: ./trades.jsonl
```

```bash
faucet run pipeline.yaml
```

This connects to the Binance trade stream, captures the first 100 trade messages (or stops after 30 s of silence, whichever comes first), and writes each as a JSON line.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | WebSocket endpoint. Must start with `ws://` or `wss://`. Supports `{placeholder}` parent-matrix context substitution. |
| `auth` | `WebsocketAuth` / `{ ref }` | `none` | Authentication on the HTTP upgrade request — see [Authentication](#authentication). |
| `subscribe_messages` | string[] | `[]` | Frames sent (in order) immediately after every connect and reconnect. Empty = send nothing. |
| `message_format` | enum | `json` | How each incoming frame is converted to a record — `json` / `raw_string` / `binary`. See [Message formats](#message-formats). |
| `on_parse_error` | enum | `fail` | In `json` mode, what to do with a non-JSON frame — `fail` (abort) or `skip` (log + drop). |
| `envelope` | bool | `false` | `false` emits the record as-is; `true` wraps it as `{ data, received_at, url }`. |

### Reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ping_interval` | int (seconds) | *(unset)* | If set, send a WebSocket Ping frame on this interval to keep the connection alive through proxies / load balancers. |
| `reconnect` | bool | `false` | Reconnect on transport error or a non-`1000` close. |
| `reconnect_backoff` | int (seconds) | `1` | Base wait before the first retry; subsequent retries grow exponentially with jitter, capped. |
| `max_reconnect_attempts` | int | *(unlimited)* | Cap on *consecutive* failed reconnects (resets on any received message). Unset = unlimited (then `idle_timeout` is the natural cap). |
| `max_message_bytes` | int | *(tungstenite default)* | Bound the max message/frame size (bytes) to prevent runaway memory. Unset = tungstenite default (64 MiB message / 16 MiB frame). |

### Termination

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_messages` | int | *(unset)* | Stop after this many messages. |
| `idle_timeout` | int (seconds) | *(unset)* | Stop after this long with no inbound frame. The idle clock keeps ticking across reconnect gaps, so it also caps a connection outage. Reset by **any** inbound frame — data, ping/pong, or a frame dropped by `on_parse_error: skip` — so a live server streaming skippable frames does not trip it. |

> **At least one of `max_messages` / `idle_timeout` must be set** — a source with neither would never stop on its own and is rejected at config-load time. Ctrl-C (SIGINT) always ends the run regardless.

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching**: the entire run window is drained into a single page (same sentinel as the Kafka source). |

## Authentication

`auth` accepts either the inline `{ type, config }` shape or a `{ ref: <name> }` pointer to a shared provider in the CLI's top-level `auth:` catalog. The credential is applied to the HTTP upgrade request:

| `type` | `config` | Effect |
|--------|----------|--------|
| `none` | *(none)* | No authentication (default). |
| `bearer` | `{ token: <string> }` | Adds `Authorization: Bearer <token>`. |
| `custom` | `{ headers: { <name>: <value>, … } }` | Adds arbitrary request headers. |

```yaml
# No auth (default) — can be omitted entirely
auth:
  type: none
```

```yaml
# Bearer token (e.g. injected from the environment)
auth:
  type: bearer
  config:
    token: ${env:WS_TOKEN}
```

```yaml
# Arbitrary custom headers
auth:
  type: custom
  config:
    headers:
      X-API-Key: ${env:API_KEY}
      X-Client-Id: faucet-stream
```

### Shared auth providers

Point `auth` at a named provider in the top-level `auth:` catalog with `{ ref }`. The provider is built once and shared across every connector that references it, and its credential is **re-resolved on every (re)connect** — so a rotated OAuth2 token is used automatically after a reconnect.

```yaml
version: 1
auth:
  feed-idp:
    type: oauth2
    config:
      token_url: https://idp.example.com/oauth/token
      client_id: ${env:CLIENT_ID}
      client_secret: ${env:CLIENT_SECRET}
pipeline:
  source:
    type: websocket
    config:
      url: wss://feed.example.com/stream
      auth: { ref: feed-idp }
      idle_timeout: 60
  sink:
    type: stdout
    config: {}
```

## Message formats

| `message_format` | Record produced |
|------------------|-----------------|
| `json` | Each frame payload is parsed as JSON. Invalid JSON is handled by `on_parse_error`. |
| `raw_string` | The frame payload as a UTF-8 string (lossy for invalid UTF-8). |
| `binary` | The frame payload base64-encoded as a string — use for binary protocols. |

With `message_format: json` and `on_parse_error: skip`, non-JSON frames are logged at `warn` and dropped (and, importantly, still reset the idle clock) — so heartbeat/control frames interleaved with data don't fail the run.

## Examples

### Subscribe-on-open feed with the envelope

Many APIs require a subscribe message after connecting and don't echo provenance. Send the subscription frame and wrap each record with its source URL and arrival time:

```yaml
source:
  type: websocket
  config:
    url: wss://ws-feed.exchange.example.com
    subscribe_messages:
      - '{"type":"subscribe","channels":["ticker"],"product_ids":["BTC-USD"]}'
    message_format: json
    envelope: true            # → { data, received_at, url }
    idle_timeout: 120
    ping_interval: 30         # keep the socket alive through the LB
```

### Resilient long-running tap with reconnect

Run indefinitely (until Ctrl-C or a long outage), reconnecting on drops and re-sending the subscription each time:

```yaml
source:
  type: websocket
  config:
    url: wss://feed.example.com/events
    subscribe_messages:
      - '{"action":"subscribe","topic":"events"}'
    reconnect: true
    reconnect_backoff: 5         # 5s before the first retry, then exponential
    max_reconnect_attempts: 10   # give up after 10 consecutive failures
    idle_timeout: 300            # also caps a total outage at 5 min
    batch_size: 500
```

### Binary protocol with size limits

Capture raw binary frames as base64, bounding memory against an oversized frame:

```yaml
source:
  type: websocket
  config:
    url: wss://telemetry.example.com/raw
    message_format: binary
    max_message_bytes: 1048576   # 1 MiB cap per frame/message
    max_messages: 10000
    idle_timeout: 60
```

### Lenient JSON feed

Drop interleaved non-JSON control frames instead of aborting:

```yaml
source:
  type: websocket
  config:
    url: wss://chat.example.com/ws
    message_format: json
    on_parse_error: skip       # log + drop non-JSON frames
    idle_timeout: 45
```

## Streaming & batching

The source overrides `Source::stream_pages`: it connects, sends the subscribe frames, then reads frames in a loop, decoding each into a record and buffering until the buffer reaches `batch_size` — at which point it yields a `StreamPage` to the pipeline. Memory is bounded at one page regardless of total volume or run length.

`batch_size: 0` is the **no-batching sentinel**: the entire run window is buffered and emitted as a single page. Because a live feed has no natural end, only use `0` together with `max_messages` (a bounded window) — otherwise the run never emits.

There is **no resumable bookmark / state**: a live feed has no replayable offset, so on reconnect the source simply re-subscribes to the live stream from the current moment. The run is a tap on "now," not a replay of history.

### Termination

A run ends on the first of:

- `max_messages` reached,
- `idle_timeout` elapsed with no inbound frame,
- Ctrl-C (SIGINT),
- a clean `1000` close from the server while `reconnect: false`.

## Config loading & schema

Load config from a YAML/JSON file (the CLI), from a struct (the library), or from environment variables. Inspect the full JSON Schema with:

```bash
faucet schema source websocket
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_websocket::{WebsocketSource, WebsocketSourceConfig, WsMessageFormat};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// Build the config from a JSON value, then override fields:
let mut config: WebsocketSourceConfig = serde_json::from_value(serde_json::json!({
    "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
}))?;
config.message_format = WsMessageFormat::Json;
config.max_messages = Some(100);
config.idle_timeout = Some(std::time::Duration::from_secs(30));

let source = WebsocketSource::new(config)?;
let records = source.fetch_all().await?;
println!("got {} trades", records.len());
# Ok(())
# }
```

To attach a shared auth provider (re-resolved on every reconnect), build the source and call `with_auth_provider(provider)` before running it.

## How it works

1. `new()` validates the config (URL scheme, termination condition, batch size) and stores it; the connection is *not* opened yet.
2. `stream_pages` calls `connect()`, which builds the upgrade request, resolves the effective auth (shared provider first, then inline), applies any `max_message_bytes` limit, opens the socket, and sends every `subscribe_messages` frame.
3. The receive loop decodes each data frame per `message_format`, optionally wraps it in the envelope, and buffers it; a page is flushed every `batch_size` records.
4. Auth is resolved **per connect**, not once at construction — so reconnects pick up a freshly rotated token from a shared provider.
5. On a transport error or non-`1000` close with `reconnect: true`, the loop waits a jittered exponential delay based on `reconnect_backoff`, reconnects, re-subscribes, and continues — counting consecutive failures against `max_reconnect_attempts`.

## Lineage dataset URI

`wss://<host><path>` or `ws://<host><path>`, with any credentials in the URL stripped — e.g. `wss://stream.example.com/feed`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI / umbrella via the `source-websocket` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: at least one of max_messages or idle_timeout must be set` | A live feed has no natural end. Set `max_messages`, `idle_timeout`, or both. |
| `Config: url must start with ws:// or wss://` | Use the WebSocket scheme, not `http(s)://`. Use `wss://` for TLS. |
| `Source: websocket connect …` error | The server is unreachable, rejected the upgrade, or TLS failed. Run `faucet doctor` — the preflight check verifies TCP reachability of the endpoint without opening the stream. |
| `Auth: auth references provider '…' but no provider was supplied` | The config uses `auth: { ref: <name> }` but no matching entry exists in the top-level `auth:` catalog (CLI) / no provider was passed to `with_auth_provider` (library). Define the provider or switch to inline auth. |
| Connect succeeds but no records arrive | The server expects a subscription before it sends data. Add the required frame(s) to `subscribe_messages`. |
| Run stops early with no data | `idle_timeout` fired before the first frame. Raise it, or confirm the subscription frame is correct. |
| `Source: websocket json parse: …` | A frame wasn't valid JSON in `json` mode. Set `on_parse_error: skip` to drop such frames, or use `message_format: raw_string`. |
| Connection drops periodically through a proxy / LB | Set `ping_interval` to send keepalive pings, and enable `reconnect` for resilience. |
| Run aborts on a huge frame / memory spikes | Set `max_message_bytes` to cap frame size. |
| Reconnect loops forever after the server goes away | Set `max_reconnect_attempts` (consecutive cap) and/or `idle_timeout` to bound the outage. |

## See also

- [Connectors reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — inline auth and shared `auth:` providers.
- [Configuration reference](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full config-file grammar.
- [`faucet-source-webhook`](https://crates.io/crates/faucet-source-webhook) — the inbound (server) counterpart for push delivery over HTTP.
- [`faucet-source-kafka`](https://crates.io/crates/faucet-source-kafka) — durable, resumable streaming for a different transport.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
