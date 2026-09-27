# faucet-sink-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-redis.svg)](https://crates.io/crates/faucet-sink-redis)
[![Docs.rs](https://docs.rs/faucet-sink-redis/badge.svg)](https://docs.rs/faucet-sink-redis)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-redis.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-redis.svg)](https://github.com/faucet-hq/faucet-stream#license)

**Redis** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records into Redis lists (`RPUSH`), streams (`XADD`), or individual keys (`SET`), batching each page of records into a single pipelined round-trip.

Reach for it when you want to land pipeline output in Redis as a work queue, an event stream for consumers, or a cache/lookup table — with one declarative config and no glue code. Redis pipelining keeps the write path fast: every chunk of records ships as one network round-trip over a connection that's opened once and reused. With `delivery: exactly_once` it commits each page's records and a watermark in one atomic `MULTI`/`EXEC` transaction.

## Feature highlights

- **Three write targets** — `List` (append via `RPUSH`), `Stream` (append via `XADD` with auto-generated IDs), and `KeyValue` (one key per record via `SET`).
- **Pipelined batching** — every chunk of records is packed into a single Redis pipeline, so a batch of N writes costs one round-trip instead of N.
- **Stream field mapping** — for `Stream` mode, each record's top-level JSON object fields become native stream entry fields; non-object records land in a single `_data` field.
- **Connection reuse** — a multiplexed async connection is opened once in `new()` and shared (cheaply cloned) across every `write_batch` call.
- **Tunable batch window** — `batch_size` controls how many commands go in one pipeline, with a `0` sentinel that passes the upstream page straight through.
- **Effectively-once delivery** — with `delivery: exactly_once`, each page's records and a per-page commit token commit in one atomic `MULTI`/`EXEC` transaction, so a resumed pipeline skips already-committed pages with zero duplicates.
- **Preflight probe** — `faucet doctor` issues a non-mutating `PING` over the live connection.
- **Credential-safe logging** — the config's `Debug` impl masks the connection URL, and the lineage dataset URI strips credentials.

## Installation

```bash
# As a library:
cargo add faucet-sink-redis

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-redis
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features sink-redis
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      endpoint: /v1/events
  sink:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      sink_type:
        type: Stream
        key: events
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | Redis connection URL, e.g. `redis://127.0.0.1:6379` or `rediss://host:6380` (TLS). Masked as `***` in `Debug` / log output. |
| `sink_type` | `RedisSinkType` | — *(required)* | The Redis data structure to write to — see [Sink types](#sink-types). |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum commands packed into one Redis pipeline. When `write_batch` receives a slice larger than this, the sink re-chunks it and issues one pipeline per chunk. **`0` = no batching**: the entire upstream slice is packed into a single pipeline, preserving the source's `StreamPage` framing. Validated against `MAX_BATCH_SIZE` (1,000,000) at construction. |

### Sink types

`sink_type` is an adjacently-tagged enum keyed by `type`:

| `type` | Fields | Redis command | Behaviour |
|--------|--------|---------------|-----------|
| `List` | `key: string` | `RPUSH` | Append each record, serialized to a JSON string, to the list at `key`. |
| `Stream` | `key: string` | `XADD` | Append each record as a stream entry at `key` with an auto-generated ID (`*`). Top-level object fields become entry fields; a non-object record is stored as a single `_data` field. |
| `KeyValue` | `key_field: string` | `SET` | Store each record under a key read from its `key_field`. The full record (serialized JSON) is the value. A record missing `key_field` raises an error. |

#### Stream entry field mapping

For `Stream` mode, each record's top-level JSON object fields are flattened into Redis stream entry fields:

- String values are stored as-is.
- Numbers, booleans, and null are converted to their string representation.
- Nested objects and arrays are serialized as JSON strings.

If a record is not a JSON object (e.g. a bare string), it is stored as a single `_data` field containing the serialized record. A record that flattens to zero fields also falls back to `_data`, because `XADD` requires at least one field.

## Examples

### Work queue (List)

Fan records out to consumers reading from one end of a Redis list:

```yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./jobs.csv
  sink:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      sink_type:
        type: List
        key: job_queue
      batch_size: 500
```

### Event stream (Stream)

Each record becomes a stream entry with native fields, ready for `XREAD` / consumer groups:

```yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      endpoint: /v1/user-events
  sink:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      sink_type:
        type: Stream
        key: user_events
```

### Cache / lookup table (KeyValue)

Materialize records into individual keys for point lookups:

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT user_id, name, plan FROM users
  sink:
    type: redis
    config:
      url: ${env:REDIS_URL}
      sink_type:
        type: KeyValue
        key_field: user_id
```

This writes keys named after each row's `user_id`, with the full JSON record as the value.

### One pipeline per source page (`batch_size: 0`)

When the source already chooses a sensible page size, pass it straight through so each page maps to exactly one Redis pipeline:

```yaml
sink:
  type: redis
  config:
    url: redis://127.0.0.1:6379
    sink_type:
      type: List
      key: events
    batch_size: 0
```

## Streaming and batching

The sink follows the workspace streaming contract: `Pipeline::run` drives the source's `stream_pages` and writes each emitted `StreamPage` via `Sink::write_batch` as it arrives, so memory stays bounded at one page. `batch_size` controls how those records are packed into Redis pipelines on the way out:

| `batch_size` | Behaviour |
|--------------|-----------|
| `1`..`MAX_BATCH_SIZE` (default `1000`) | A slice larger than `batch_size` is re-chunked into `batch_size` slices; one Redis pipeline is issued per chunk. Recommended for high-throughput writes — pipelined commands are cheap, and a 1000-command window amortises the round-trip without starving other clients. |
| `0` | "No batching" sentinel — the entire records slice is packed into a single pipeline regardless of size, preserving the upstream `StreamPage` framing. Use it when the source has already chosen the page size and you want one pipeline per page. |

This sink writes **append/insert-only** (`RPUSH` / `XADD` / `SET`) and does not implement upsert/delete write modes — see [Limitations](#limitations). It **does** support effectively-once delivery — see the next section.

This connector reports observability metrics under the label `connector="redis"`.

## Effectively-once delivery

`RedisSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — packs every record's command for the configured `sink_type` **plus** a `SET _faucet_commit_token:<scope> <token>` into one atomic Redis transaction (`MULTI`/`EXEC`), so the page's data and its watermark either commit together or not at all.
- `last_committed_token(scope)` — a `GET` on the same `_faucet_commit_token:<scope>` key, so the pipeline skips already-committed pages on resume. The token is stored and read back as an opaque string.

**One page = one transaction.** `batch_size` re-chunking does **not** apply on the idempotent path — splitting a page across multiple `MULTI`/`EXEC` blocks would break atomicity (a crash between chunks could commit rows without the watermark). Size the source's page (`batch_size` on the source) rather than the sink window when running `delivery: exactly_once`.

The watermark key mirrors the SQL sinks' `_faucet_commit_token(scope, token)` table: one plain Redis string key per pipeline scope (the per-row state key, e.g. `myfeed::row1`), namespaced under the `_faucet_commit_token:` prefix. It lives in the same database as the data keys — don't evict or delete it while a pipeline is live, or resume falls back to replaying from the state-store bookmark.

To use effectively-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      sink_type:
        type: Stream
        key: change_events
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

Note the usual Redis caveat: `List` and `Stream` modes append, so a page that was *fully* committed is never re-applied — but downstream consumers should still treat entry IDs as the identity of a stream record. See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).

## Config loading & schema

Load from YAML/JSON files or environment variables via the helpers in `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_redis::RedisSinkConfig;

// From a JSON file
let config: RedisSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: RedisSinkConfig = load_env_file(".env", "REDIS_SINK")?;
```

Example `.env`:

```env
REDIS_SINK_URL=redis://127.0.0.1:6379
REDIS_SINK_SINK_TYPE='{"type":"List","key":"events"}'
REDIS_SINK_BATCH_SIZE=1000
```

Inspect the full JSON Schema with:

```bash
faucet schema sink redis
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = RedisSinkConfig::new(
    "redis://127.0.0.1:6379",
    RedisSinkType::Stream { key: "events".into() },
)
.with_batch_size(1000);

let sink = RedisSink::new(config).await?;

let records = vec![
    json!({"user_id": "u123", "event": "signup"}),
    json!({"user_id": "u456", "event": "login"}),
];

let written = sink.write_batch(&records).await?;
println!("wrote {written} records to Redis");
# Ok(())
# }
```

Drive it from a full pipeline by pairing it with any source:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events"),
);
let sink = RedisSink::new(RedisSinkConfig::new(
    "redis://127.0.0.1:6379",
    RedisSinkType::Stream { key: "api_events".into() },
))
.await?;

let result = Pipeline::new(source, sink).run().await?;
println!("transferred {} records", result.records_written);
# Ok(())
# }
```

## How it works

1. `new()` validates `batch_size`, opens a `redis::Client` from `url`, and establishes a **multiplexed async connection** — once. The connection is cheaply cloneable and shared across every `write_batch` call (it multiplexes commands over a single socket).
2. `write_batch` chunks the incoming slice by the effective window (`batch_size`, or the whole slice when `batch_size: 0`).
3. Each chunk is assembled into one `redis::pipe()` — `RPUSH` for `List`, `XADD` for `Stream`, `SET` for `KeyValue` — and executed in a single round-trip via `query_async`.
4. Under `delivery: exactly_once`, `write_batch_idempotent` builds the same per-record commands but ships the **whole page** as one atomic `MULTI`/`EXEC` pipeline with a final `SET _faucet_commit_token:<scope> <token>` — see [Effectively-once delivery](#effectively-once-delivery).
5. A failed pipeline surfaces as `FaucetError::Sink`; a record missing its `key_field` (KeyValue) or a JSON-serialization failure does the same.

## Lineage dataset URI

`redis://<host>:<port>?key=<key>` (List/Stream) or `redis://<host>:<port>?key_field=<field>` (KeyValue), with credentials stripped — e.g. `redis://localhost:6379?key=events`.

## Limitations

- **Append/insert-only.** The sink writes via `RPUSH` / `XADD` / `SET`; it does not implement `write_mode: upsert | delete`. (`SET` on an existing key in `KeyValue` mode overwrites by nature, but there is no keyed upsert/delete planner.)
- **No compression.** Not applicable to a Redis protocol sink.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-redis` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: invalid Redis URL` | `url` is malformed. Use the `redis://[:password@]host:port[/db]` form (or `rediss://` for TLS). |
| `Sink: Redis connection failed` | The server is unreachable, refused the connection, or rejected auth. Check the host/port, that Redis is running, and that any password in the URL is correct. `faucet doctor` runs a `PING` probe that surfaces the same error early. |
| `Config: batch_size ... exceeds maximum` | `batch_size` is above `MAX_BATCH_SIZE` (1,000,000). Lower it; `0` is valid (no batching). |
| `Sink: record missing key field '<field>'` (KeyValue) | A record has no `key_field`. Ensure every record carries the field, or add a transform to populate/rename it before the sink. |
| `Sink: Redis pipeline execution failed` | The server rejected a command mid-pipeline (e.g. `WRONGTYPE` — the key already holds a different data type, or `OOM` under `maxmemory`). Use a fresh key per `sink_type`, or free memory / raise `maxmemory`. |
| Stream entry has only a `_data` field | The record wasn't a JSON object, or flattened to zero fields. `XADD` requires at least one field, so the whole record is stored under `_data`. Emit object records to get native stream fields. |
| Numbers/booleans arrive as strings in a stream | Intentional — non-string scalars are stringified for `XADD`, and nested objects/arrays are serialized as JSON strings. Parse them on the consumer side. |
| TLS connection rejected | Use a `rediss://` URL (double `s`) to negotiate TLS against a server configured for it. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix across all connectors.
- [Configuration grammar](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full pipeline config shape.
- [State & resumability cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html).
- [`faucet-source-redis`](https://crates.io/crates/faucet-source-redis) — the Redis **source** (streams, lists, key patterns).
- [`faucet-core`](https://crates.io/crates/faucet-core) — traits, pipeline, and error types.

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — pipelines are not MULTI/EXEC transactions. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
