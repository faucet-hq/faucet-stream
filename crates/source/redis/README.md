# faucet-source-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-source-redis.svg)](https://crates.io/crates/faucet-source-redis)
[![Docs.rs](https://docs.rs/faucet-source-redis/badge.svg)](https://docs.rs/faucet-source-redis)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-redis.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-redis.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **Redis** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Reads records from a Redis **list**, **stream**, or a set of keys matched by a **glob pattern**, yielding each record as a `serde_json::Value`.

Built on a lazily-opened, reused `MultiplexedConnection` and each mode's native paging primitive (`LRANGE`, `XRANGE`, `SCAN` + `MGET`), so it streams page-by-page without buffering the whole dataset in memory. Reach for it to drain a work queue, replay a stream's history, or load a keyspace into any faucet-stream sink.

## Feature highlights

- **Three source modes** — `List` (FIFO list elements), `Stream` (entries with IDs + fields), and `Keys` (glob-matched keys with their values).
- **Native streaming in all three modes** — `stream_pages` walks the list, stream, or keyspace in pages so the sink writes as data arrives.
- **Server-side cursoring** — `Keys` drives the `SCAN` cursor manually and `MGET`s a page at a time, so a million-key namespace never materializes in memory at once.
- **Never touches consumer groups** — every read is `XRANGE`, so a preview or run never claims entries into a group's pending list.
- **Faithful values** — JSON objects/arrays are parsed (`parse_json`), scalars stay the strings Redis holds, and binary values are base64-encoded (`binary`) instead of failing the read.
- **Connection reuse** — one multiplexed connection is opened on first use and cheaply cloned across every call (no per-call TCP/AUTH handshake).
- **Secrets-safe** — the connection URL is masked in `Debug` output and credentials are stripped from the lineage dataset URI.

## Installation

```bash
# As a library:
cargo add faucet-source-redis
cargo add tokio --features full

# Or via the umbrella crate:
cargo add faucet-stream --features source-redis

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-redis
```

The Redis source is opt-in — it is not in the CLI's default feature set.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      source_type:
        type: List
        key: jobs:pending
  sink:
    type: file
    config:
      path: ./jobs.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### `RedisSourceConfig`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | Redis connection URL, e.g. `redis://127.0.0.1:6379` or `redis://user:pass@host:6379/0`. Masked in `Debug` output. |
| `source_type` | `RedisSourceType` | — *(required)* | The Redis data structure to read from — `List`, `Stream`, or `Keys` (see below). |
| `max_records` | int | *(unbounded)* | Optional cap on the total number of records returned. Honored by both `fetch_all` and streaming. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`, mapped onto each mode's native paging primitive. **`0` = no batching** (drain into one page). See [Streaming & batching](#streaming--batching). |
| `parse_json` | `containers` \| `all` \| `none` | `containers` | Which values are parsed as JSON. `containers` parses only objects and arrays, so `"1.10"`, `"true"` and long numeric ids stay strings; `all` also parses scalars; `none` keeps every value a string. |
| `binary` | `base64` \| `lossy` \| `error` | `base64` | A value that is not valid UTF-8 (msgpack, gzip, …): base64-encode it, replace invalid sequences with U+FFFD, or fail the run naming the key. |

The connection reconnects on its own after the server drops it (a failover), with a short bounded retry when the server is unreachable.

### `source_type` — `RedisSourceType`

A tagged enum (`type` discriminator). Pick exactly one variant.

#### `List`

Reads list elements via `LRANGE`, decoded per `parse_json` / `binary`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | Literal `List`. |
| `key` | string | — *(required)* | The list key. |

#### `Stream`

Reads stream entries via `XRANGE`, on every path (`stream_pages`, `fetch_all`, preview).

> **Consumer groups are ignored.** Reading through a group (`XREADGROUP`) moves entries into the group's pending list and withholds them from its other consumers, and nothing here acknowledges them — so the source never does. `group` / `consumer` / `count` are accepted for compatibility and ignored; a configured group logs a warning, and the whole stream is re-read every run.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | Literal `Stream`. |
| `key` | string | — *(required)* | The stream key. |
| `group` | string | *(none)* | Ignored (logs a warning). |
| `consumer` | string | *(none)* | Ignored. |
| `count` | int | *(none)* | Ignored; `batch_size` is the `XRANGE COUNT`. |

Each entry is returned as:

```json
{ "id": "1234567890-0", "fields": { "field1": "value1", "field2": "value2" } }
```

#### `Keys`

Scans keys matching a glob `pattern` via `SCAN`, then `MGET`s each batch. A key `SCAN` returns twice is emitted once. Only **string** keys have a value `MGET` can read: matched hashes, sets, lists, sorted sets and streams are skipped and counted in a `WARN` — narrow the pattern to string keys. Values substituted into the pattern from a parent record (`{parent.field}`) have their glob metacharacters escaped, so they match literally.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | Literal `Keys`. |
| `pattern` | string | — *(required)* | Glob pattern for `SCAN MATCH`, e.g. `user:*`. |

Each key-value pair is returned as:

```json
{ "key": "user:123", "value": { "name": "Alice", "email": "alice@example.com" } }
```

Stream field values and key values are decoded per `parse_json` / `binary`.

## Examples

### Drain a list into SQLite

```yaml
version: 1
name: redis_to_sqlite
pipeline:
  source:
    type: redis
    config:
      url: redis://localhost:6379
      source_type:
        type: List
        key: jobs:pending
      max_records: 10000
  sink:
    type: sqlite
    config:
      database_url: sqlite:./cache.db
      table_name: jobs
      column_mapping: auto_map
      batch_size: 500
```

### Replay a stream's history

```yaml
version: 1
pipeline:
  source:
    type: redis
    config:
      url: redis://localhost:6379
      source_type:
        type: Stream
        key: order-events
      batch_size: 2000
  sink:
    type: file
    config:
      path: ./order-events.jsonl
```

### Scan a keyspace by pattern

```yaml
version: 1
pipeline:
  source:
    type: redis
    config:
      url: redis://localhost:6379
      source_type:
        type: Keys
        pattern: "session:*"
      max_records: 500
  sink:
    type: file
    config:
      path: ./sessions.jsonl
```

## Streaming & batching

`RedisSource` overrides [`Source::stream_pages`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html#method.stream_pages) so the pipeline writes pages to the sink as they arrive instead of buffering the full result set. Each mode maps `batch_size` onto its native paging primitive:

| Mode | Native primitive | Per-page command |
|------|------------------|------------------|
| `List` | `LRANGE start stop`, sliding the window | `LRANGE <key> <start> <start + batch_size - 1>` |
| `Stream` | `XRANGE start + COUNT batch_size`, advancing the start ID | `XRANGE <key> <start> + COUNT <batch_size>` |
| `Keys` | `SCAN MATCH pattern COUNT batch_size` cursor → `MGET` per page | `SCAN <cursor> MATCH <pattern> COUNT <batch_size>` then `MGET key1 … keyN` |

**`batch_size = 0`** is the "no batching" sentinel — every mode drains its primitive into a single page: `LRANGE 0 -1` for `List`, `XRANGE - +` for `Stream`, and the full `SCAN` cursor followed by one `MGET` for `Keys`. Useful for small lookup tables or for sinks (SQL `COPY`, BigQuery load jobs) that prefer one large request.

The trait-level `batch_size` argument passed to `stream_pages` is **ignored** in favour of `RedisSourceConfig::batch_size` — the config is the authoritative, user-facing knob, so a pipeline-supplied hint can never silently override an explicit config value.

Every emitted page carries `bookmark: None` — the Redis source has no incremental-replication / resume mode today, so it is **not resumable** and does not support effectively-once delivery.

## Config loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_redis::RedisSourceConfig;

let config: RedisSourceConfig = load_json("config.json")?;
let config: RedisSourceConfig = load_env_file(".env", "REDIS_SOURCE")?;
```

```env
# .env
REDIS_SOURCE_URL=redis://127.0.0.1:6379
REDIS_SOURCE_MAX_RECORDS=1000
```

## Schema introspection

```bash
faucet schema source redis
```

```rust
use faucet_core::Source;
let source = faucet_source_redis::RedisSource::new(config)?;
println!("{}", serde_json::to_string_pretty(&source.config_schema())?);
```

## Library usage

```rust,no_run
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use faucet_core::Source;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// List mode, capped at 100 records.
let config = RedisSourceConfig::new(
    "redis://127.0.0.1:6379",
    RedisSourceType::List { key: "notifications".into() },
)
.max_records(100)
.with_batch_size(500);

let source = RedisSource::new(config)?;
let records = source.fetch_all().await?;
for record in &records {
    println!("{record}");
}

// Stream mode with a consumer group.
let config = RedisSourceConfig::new(
    "redis://127.0.0.1:6379",
    RedisSourceType::Stream {
        key: "order-events".into(),
        group: Some("analytics-group".into()),
        consumer: Some("worker-1".into()),
        count: Some(200),
    },
);
let source = RedisSource::new(config)?;
let events = source.fetch_all().await?;
for event in &events {
    println!("event {}: {:?}", event["id"], event["fields"]);
}
# Ok(()) }
```

`RedisSource::new` is synchronous and does no I/O — the connection is opened lazily on first use. It returns `Err(FaucetError::Config)` only on an out-of-range `batch_size`. To drive it through the streaming pipeline, hand the source to `faucet_core::run_stream` or `Pipeline`.

## How it works

The multiplexed connection is opened **once** on first use (`OnceCell`) and cloned per call — `MultiplexedConnection` shares a single underlying socket, so there is no repeated TCP/AUTH handshake. List and stream pages advance a server-side cursor (a sliding `LRANGE` window; the immediate-successor stream ID via `next_stream_id`) so no entry is re-emitted across page boundaries. `Keys` drives the `SCAN` cursor manually and `MGET`s a page as soon as `batch_size` keys accumulate, rather than materializing the whole matched keyset first; keys deleted between `SCAN` and `MGET` are dropped.

**List paging consistency caveat:** index-based `LRANGE` paging is only stable if the list is not mutated mid-scan. A concurrent `LPUSH`/`LPOP` shifts every element's index, so a writer pushing/popping while the source drains can skip or duplicate elements across page boundaries. For a queue being consumed concurrently, prefer a Redis Stream (`XRANGE` / consumer groups) over a list.

## Lineage dataset URI

`redis://<host>:<port>/<db>?key=<key>` (List / Keys) or `?stream=<stream>` (Stream), with credentials stripped — e.g. `redis://localhost:6379/0?key=jobs:pending`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI / umbrella crate via the `source-redis` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: invalid Redis URL` | The `url` is malformed. Use the `redis://[user:pass@]host:port[/db]` form (or `rediss://` for TLS). |
| `FaucetError::Config: batch_size` | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` for a single page. |
| `FaucetError::Source: Redis connection failed` | The server is unreachable or auth failed. Check host/port, credentials in the URL, and that Redis is running. |
| `XREADGROUP failed: NOGROUP` | The consumer group doesn't exist. Create it with `XGROUP CREATE <key> <group> $ MKSTREAM` before running. |
| Consumer-group read returns nothing | `XREADGROUP` with `>` only delivers **new** (never-delivered) messages. Already-pending entries for this consumer are not re-read here. |
| Streaming ignores `group` / `consumer` | Intentional — `stream_pages` always uses `XRANGE`; consumer-group semantics only apply to `fetch_all`. |
| List read skips/duplicates rows | The list is being mutated mid-scan (`LPUSH`/`LPOP`). Use a Redis Stream for a concurrently-consumed queue. |
| Fewer key records than `SCAN` matched | Keys were deleted between `SCAN` and `MGET`; missing values are dropped. Expected for a live keyspace. |
| Values arrive as strings, not objects | The stored value isn't valid JSON, so it's returned verbatim as a string. Store JSON to get parsed objects. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [faucet-sink-redis](https://crates.io/crates/faucet-sink-redis) · [faucet-source-kafka](https://crates.io/crates/faucet-source-kafka)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="redis"`.
