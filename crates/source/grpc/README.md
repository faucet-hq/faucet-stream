# faucet-source-grpc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-grpc.svg)](https://crates.io/crates/faucet-source-grpc)
[![Docs.rs](https://docs.rs/faucet-source-grpc/badge.svg)](https://docs.rs/faucet-source-grpc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-grpc.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-grpc.svg)](https://github.com/faucet-hq/faucet-stream#license)

Config-driven **gRPC** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. It calls any gRPC service **dynamically** via protobuf reflection (`prost-reflect`) — no generated client code, no per-service Rust — using a compiled `FileDescriptorSet` to encode the request and decode each response into a typed `serde_json::Value`.

Reach for it when you need to pull data out of an internal gRPC API — a list/get RPC for a one-shot snapshot, or a server-streaming RPC for a long-lived event/change/log feed — and land it in any faucet-stream sink with one declarative config.

## Feature highlights

- **Dynamic protobuf, zero codegen** — point the source at a `FileDescriptorSet` (`.bin`) produced by `protoc`; it resolves the service/method, maps your JSON `request` onto the protobuf message, and decodes responses back to JSON. No `.proto` compilation into your binary.
- **Two RPC kinds** — `unary` (one request → one response) and `server_streaming` (one request → a stream of responses). Server-streaming is consumed message-by-message and emitted as records arrive.
- **Native streaming for server-streaming RPCs** — `stream_pages` flushes a `StreamPage` each time `batch_size` messages accumulate, bounding **both** source-side and sink-side memory for unbounded feeds.
- **Resilient reconnect** — server-streaming reconnects on transient errors (dropped connections, stalls, `UNAVAILABLE`-class statuses) with exponential backoff (`reconnect_initial_backoff` → `reconnect_max_backoff`) and a finite attempt budget; permanent failures (auth, permissions, decode, bad `records_path`) fail the run at once. Delivery across a reconnect is at-least-once unless you opt into replay-prefix skipping.
- **Timeouts** — `connect_timeout`, a unary / stream-start `timeout`, and a server-streaming `idle_timeout` between messages, so a stalled peer fails or reconnects instead of hanging.
- **JSONPath record extraction** — `records_path` (e.g. `$.users[*]`) pulls a repeated field out of each response message; unset returns the whole response as a single record.
- **Three auth modes** — none, bearer token (`authorization` metadata), or arbitrary ordered metadata key/value pairs (duplicate keys allowed). Bearer/metadata auth also resolves from the CLI's shared `auth:` catalog via `auth: { ref: <name> }`.
- **TLS auto-detection** — inferred from an `https://` endpoint, or forced on/off with `tls`.
- **Tunable message-size limits** — raise `max_decoding_message_size` / `max_encoding_message_size` above tonic's 4 MiB default for large messages.
- **Connection reuse** — the tonic channel is built once and reused for the run.

## Installation

```bash
# As a library:
cargo add faucet-source-grpc

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-grpc
```

Via the umbrella crate:

```bash
cargo add faucet-stream --features source-grpc
```

## Prerequisites

This source needs a compiled `FileDescriptorSet` — the binary schema of your protobuf services. Generate it from your `.proto` files with `protoc`:

```bash
protoc --descriptor_set_out=descriptor.bin --include_imports \
    -I proto/ proto/my_service.proto
```

`--include_imports` is required so transitively-imported message types resolve. The descriptor file drives both request encoding and response decoding at runtime.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: grpc
    config:
      endpoint: http://localhost:50051
      service_name: users.UserService
      method_name: ListUsers
      descriptor_set_path: proto/descriptor.bin
      request:
        page_size: 100
      records_path: $.users[*]
  sink:
    type: file
    config:
      path: ./users.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | — *(required)* | gRPC endpoint URL (e.g. `http://localhost:50051`, `https://grpc.example.com:443`). |
| `service_name` | string | — *(required)* | Fully qualified service name (e.g. `users.UserService`). Must exist in the descriptor set. |
| `method_name` | string | — *(required)* | Method (RPC) name on that service (e.g. `ListUsers`). |
| `descriptor_set_path` | path | — *(required)* | Path to the compiled `FileDescriptorSet` `.bin` file. |
| `request` | object | `{}` | Request message as JSON; fields are mapped onto the protobuf request message via the descriptor. Unknown fields fail encoding. |
| `records_path` | string | *(unset)* | JSONPath extracting records from each response (e.g. `$.users[*]`). When unset, the whole response is returned as a single record. For server-streaming it is applied to **each** message individually. |
| `tls` | bool | *(auto)* | Force TLS on/off. When unset, auto-detected from an `https://` endpoint. |

### Auth

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth` | `GrpcAuth` / `{ ref }` | `none` | Inline `{ type, config }` (see [Authentication](#authentication)) or `{ ref: <name> }` pointing at a shared provider in the CLI's top-level `auth:` catalog. |

### RPC kind & server-streaming

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rpc_kind` | enum | `unary` | `unary` (one request → one response) or `server_streaming` (one request → stream of responses). |
| `max_messages` | int | *(unset)* | Server-streaming only. Cap on streamed messages to consume before terminating. Unset = consume until the server closes the stream. |
| `terminate_on_error` | bool | `false` | Server-streaming only. `true` propagates a transient stream error on first failure; `false` reconnects with backoff. |
| `reconnect_initial_backoff` | int (seconds) | `1` | Server-streaming only. Initial reconnect backoff; doubles each failure up to `reconnect_max_backoff`. Must be `> 0`. |
| `reconnect_max_backoff` | int (seconds) | `30` | Server-streaming only. Upper bound on reconnect backoff. |
| `reconnect_max_attempts` | int \| null | `10` | Server-streaming only. Max consecutive reconnect attempts before surfacing the error; the count resets whenever a message arrives. `null` = unlimited. |
| `reconnect_replay_from_start` | bool | `false` | Server-streaming only. `false` emits every message received after a reconnect (at-least-once). `true` skips as many messages as were already emitted — only for a server that re-streams the same messages in the same order from message 0; on a live feed it drops new messages. See [Reconnect](#reconnect-on-transient-errors). |
| `connect_timeout` | int (seconds) \| null | `10` | Time allowed to open the connection. `null` waits indefinitely. |
| `timeout` | int (seconds) \| null | `30` | Time allowed for a unary call, or for a server-streaming call to start. A timed-out stream start is retried. `null` waits indefinitely. |
| `idle_timeout` | int (seconds) \| null | `300` | Server-streaming only. Time allowed between messages before the stream is treated as stalled and reopened. `null` waits indefinitely (a half-open connection then hangs the run). |

### Batching & limits

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching** (whole result set in one page). For unary RPCs any value behaves identically (full response is buffered first); for server-streaming it bounds memory. Max `1_000_000`. |
| `max_decoding_message_size` | int (bytes) | *(tonic 4 MiB)* | Max size of a single inbound (decoded) message. Raise for large responses; a too-low limit surfaces as a decode error. |
| `max_encoding_message_size` | int (bytes) | *(tonic default)* | Max size of a single outbound (encoded) request message. Rarely needs tuning for a data source. |

## Authentication

`auth` uses the project-wide `{ type, config }` shape (`GrpcAuth`):

| `type` | `config` | Description |
|--------|----------|-------------|
| `none` | *(none)* | No authentication (default). |
| `bearer` | `{ token: <string> }` | Token sent as `authorization` request metadata. |
| `metadata` | `{ entries: [{ key, value }, …] }` | Arbitrary metadata pairs attached to every request. Order is preserved and duplicate keys are allowed (gRPC permits both). |

```yaml
# Bearer token (via env indirection)
auth:
  type: bearer
  config:
    token: ${env:GRPC_TOKEN}
```

```yaml
# Custom metadata (e.g. API key + tenant)
auth:
  type: metadata
  config:
    entries:
      - { key: x-api-key, value: ${env:API_KEY} }
      - { key: x-tenant-id, value: tenant-123 }
```

```yaml
# Shared provider from the top-level auth: catalog
auth:
  ref: my_idp
```

## Examples

### Authenticated unary list with TLS and record extraction

```yaml
source:
  type: grpc
  config:
    endpoint: https://grpc.production.example.com:443
    service_name: analytics.EventService
    method_name: QueryEvents
    descriptor_set_path: proto/descriptor.bin
    request:
      start_time: "2026-01-01T00:00:00Z"
      end_time: "2026-02-01T00:00:00Z"
      limit: 1000
    auth:
      type: bearer
      config: { token: ${env:GRPC_TOKEN} }
    tls: true
    records_path: $.events[*]
```

### gRPC → HTTP (matches `cli/examples/grpc_to_http.yaml`)

```yaml
version: 1
name: grpc_to_http
pipeline:
  source:
    type: grpc
    config:
      endpoint: https://grpc.example.com:443
      service_name: metrics.MetricsService
      method_name: ListMetrics
      descriptor_set_path: proto/metrics.bin
      request: { window: 1h }
      auth:
        type: bearer
        config: { token: ${env:GRPC_TOKEN} }
      tls: true
      records_path: $.metrics[*]
  sink:
    type: http
    config:
      url: https://ingest.example.com/v1/events?tenant=acme
      method: POST
      auth:
        type: bearer
        config: { token: ${env:INGEST_TOKEN} }
      batch_mode: { type: Array }
      max_retries: 3
      concurrency: 8
```

### Server-streaming event feed with reconnect

```yaml
source:
  type: grpc
  config:
    endpoint: https://grpc.example.com:443
    service_name: events.EventService
    method_name: Tail
    descriptor_set_path: proto/descriptor.bin
    request: { topic: audit-log }
    auth:
      type: bearer
      config: { token: ${env:GRPC_TOKEN} }
    tls: true
    rpc_kind: server_streaming
    max_messages: 100000
    batch_size: 500
    reconnect_initial_backoff: 1
    reconnect_max_backoff: 30
    idle_timeout: 600                     # the feed may be quiet for minutes
    max_decoding_message_size: 16777216   # 16 MiB
```

### Custom-metadata auth, large messages

```yaml
source:
  type: grpc
  config:
    endpoint: http://localhost:50051
    service_name: inventory.InventoryService
    method_name: ListProducts
    descriptor_set_path: proto/descriptor.bin
    request: { category: electronics, page_size: 100 }
    auth:
      type: metadata
      config:
        entries:
          - { key: x-api-key, value: ${env:API_KEY} }
    records_path: $.products[*]
    max_decoding_message_size: 33554432   # 32 MiB
```

## Streaming & batching

### Unary RPCs

A unary RPC returns one response containing all records. `stream_pages` falls back to the default trait impl, which buffers the full response and chunks it in memory into `batch_size` pages. This bounds **sink-side** memory only — source-side memory is `O(full response)`. `batch_size = 0` and any positive value are observably identical here, since there is no native wire paging primitive to honour. Treat `batch_size` as a sink-side chunk size for unary.

### Server-streaming RPCs

When `rpc_kind: server_streaming`, the source calls `tonic::client::Grpc::server_streaming` and consumes the response stream message-by-message. Each streamed `DynamicMessage` is decoded via `prost-reflect`, converted to JSON, and (if `records_path` is set) flattened per message. `stream_pages` flushes a page every `batch_size` records, bounding **both** source-side and sink-side memory — the right mode for unbounded feeds. `batch_size = 0` drains the whole stream into a single page (good for short streams). Pages carry `bookmark: None`.

#### Reconnect on transient errors

Transient stream errors trigger a reconnect with exponential backoff from `reconnect_initial_backoff`, doubling up to `reconnect_max_backoff`; after `reconnect_max_attempts` consecutive failures (10 by default) the error is surfaced. Transient means a failed connect, a stream start slower than `timeout`, no message within `idle_timeout`, or a status of `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `UNKNOWN` or `INTERNAL` (dropped connections surface as the last two). Every other status, an auth or metadata error, a protobuf decode error (`DATA_LOSS`), an oversized message (`OUT_OF_RANGE`) and a `records_path` error fail the run at once. Set `terminate_on_error: true` to fail on the first transient error too.

`UNAUTHENTICATED` with a shared `auth: { ref }` provider invalidates the rejected token and reopens the call once with a fresh one (unary calls too); a second rejection fails the run.

Reconnect re-sends the *same* request (resolved once per run). By default every message the new stream delivers is emitted (at-least-once): a server that replays from message 0 produces duplicates, a live feed loses nothing. Set `reconnect_replay_from_start: true` only for a server known to re-stream the same messages in the same order from message 0; the source then skips as many messages as it already emitted. Against a live feed that sends new events after a resubscribe this drops that many new messages, so it is opt-in. For an exact resume, carry a resume token in the request (e.g. an `after_event_id` field you maintain) and deduplicate downstream with an upsert sink.

> **Resume/state:** this source has no faucet-managed bookmark or `state:` resume. For a resumable feed, drive the cursor through the `request` (e.g. an `after_event_id` your config advances), not via a faucet state store.

## Config loading & schema introspection

Load config from YAML/JSON, environment, or a `.env` file:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_grpc::GrpcStreamConfig;

let config: GrpcStreamConfig = load_json("config.json")?;
let config: GrpcStreamConfig = load_env_file(".env", "GRPC")?;
```

```env
GRPC_ENDPOINT=http://localhost:50051
GRPC_SERVICE_NAME=users.UserService
GRPC_METHOD_NAME=ListUsers
GRPC_DESCRIPTOR_SET_PATH=proto/descriptor.bin
```

Inspect the full JSON Schema with:

```bash
faucet schema source grpc
```

## Library usage

```rust
use faucet_core::{Pipeline, Source};
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = GrpcStreamConfig::new(
    "http://localhost:50051",
    "users.UserService",
    "ListUsers",
    "proto/descriptor.bin",
)
.request(json!({ "page_size": 100 }))
.records_path("$.users[*]");

// One-shot collect:
let stream = GrpcStream::new(config)?;
let records = stream.fetch_all().await?;
println!("fetched {} records", records.len());
# Ok(())
# }
```

For a long-lived server-streaming feed, drive the pipeline so pages flush to the sink as they arrive instead of buffering everything:

```rust
use faucet_core::Pipeline;
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use serde_json::json;

# async fn run(my_sink: impl faucet_core::Sink) -> Result<(), Box<dyn std::error::Error>> {
let config = GrpcStreamConfig::new(
    "http://localhost:50051",
    "events.EventService",
    "Tail",
    "proto/descriptor.bin",
)
.request(json!({ "topic": "audit-log" }))
.rpc_kind(RpcKind::ServerStreaming)
.with_batch_size(500);

let stream = GrpcStream::new(config)?;
Pipeline::new(&stream, &my_sink).run().await?;
# Ok(())
# }
```

## How it works

1. `new()` loads and parses the `FileDescriptorSet`, resolves the service/method, and builds the tonic channel **once** (TLS auto-detected from the scheme unless `tls` overrides it).
2. The JSON `request` is mapped onto the protobuf request message via reflection; any configured `max_encoding_message_size` is applied.
3. **Unary:** a single response is decoded to JSON; `records_path` (if set) extracts records, otherwise the whole response is one record.
4. **Server-streaming:** the response stream is consumed message-by-message; each `DynamicMessage` is decoded, JSON-converted, and `records_path`-flattened, with reconnect/backoff wrapping the consume loop.
5. A page is emitted each time `batch_size` records accumulate, while the stream is still open; `max_decoding_message_size` bounds each inbound message.

## Lineage dataset URI

`<endpoint>/<service_name>/<method_name>` with credentials stripped — e.g. `http://grpc.example.com:50051/example.Service/ListItems`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI/umbrella via the `source-grpc` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| Service or method not found | `service_name` / `method_name` don't match the descriptor. Use the **fully qualified** service name (`package.Service`) exactly as in the `.proto`, and confirm the method exists. |
| Descriptor fails to load | Regenerate with `--include_imports` so transitively-imported types resolve; pass the correct `descriptor_set_path`. |
| Request field rejected during encoding | A `request` key isn't a field of the protobuf request message. Match field names (proto field names, not JSON aliases) against the descriptor. |
| TLS handshake fails / plaintext on a TLS endpoint | Set `tls` explicitly. An `http://` endpoint defaults to plaintext, `https://` to TLS; override when the scheme and the server disagree. |
| `Unauthenticated` / 401-equivalent | Wrong or missing credentials. For `bearer`, the token is sent as `authorization` metadata; for `metadata`, confirm the server expects those exact keys. |
| Decode error on a large response | The message exceeds tonic's 4 MiB inbound limit. Raise `max_decoding_message_size`. |
| `records_path` returns nothing | The JSONPath doesn't match the decoded response shape. Drop `records_path` to inspect the raw response, then target the actual array field (e.g. `$.users[*]`). |
| Server-streaming run never ends | Expected for an open-ended feed. Bound it with `max_messages`, or cancel the run (the page loop stops at the next boundary). |
| Reconnect busy-spins / errors immediately | `reconnect_initial_backoff` must be `> 0`. Permanent failures (auth, permissions, decode) are not retried. |
| A quiet feed reconnects every few minutes | It went longer than `idle_timeout` without a message; raise it or set `null`. |
| Duplicate messages after a reconnect | The server replays from message 0 and delivery is at-least-once. Set `reconnect_replay_from_start: true` only if the replay is the same messages in the same order. |

## See also

- [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- [Connector capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html)
- [`faucet-sink-http`](https://crates.io/crates/faucet-sink-http) · [`faucet-source-rest`](https://crates.io/crates/faucet-source-rest) · [`faucet-source-graphql`](https://crates.io/crates/faucet-source-graphql)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="grpc"`.
