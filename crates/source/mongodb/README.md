# faucet-source-mongodb

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mongodb.svg)](https://crates.io/crates/faucet-source-mongodb)
[![Docs.rs](https://docs.rs/faucet-source-mongodb/badge.svg)](https://docs.rs/faucet-source-mongodb)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mongodb.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mongodb.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **MongoDB** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs a single `find()` query against a collection — with optional filter, projection, sort, and limit — and yields each matching document as a `serde_json::Value` object.

Built on the official `mongodb` driver, which maintains an internal connection pool created once at construction and reused for every query. The source streams documents straight off the driver cursor (no full-result buffering), so memory stays bounded by `batch_size` regardless of how large the collection or result set is. Reach for it to drain collections into any faucet-stream sink — SQL warehouses, object stores, search indexes, or another database.

## Feature highlights

- **`find()` with the full query surface** — JSON `filter`, `projection`, `sort`, and `limit`, all converted to BSON at query time.
- **Native cursor streaming** — `stream_pages` pulls documents off the driver cursor incrementally and yields a `StreamPage` once the buffer fills; the full result set is never materialized in memory.
- **Two independent batching knobs** — `batch_size` (records per emitted page) and `cursor_batch_size` (the driver's per-round-trip fetch size), tunable separately to trade network round-trips against buffering.
- **Pooled connections** — the driver's internal connection pool is created once in `new()` and reused across every query.
- **Relaxed extended JSON** — BSON types (`ObjectId`, `DateTime`, `Int64`, …) are rendered as MongoDB relaxed extended JSON, so round-trips stay lossless and self-describing.
- **Matrix-row context interpolation** — `{key}` placeholders inside `filter`/`projection`/`sort` are resolved per parent record (JSON-safe escaping) for parent/child fan-out pipelines.
- **Credential safety** — the connection URI is masked in `Debug` output and stripped from the lineage dataset URI.

## Installation

```bash
# As a library:
cargo add faucet-source-mongodb
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-mongodb
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-mongodb
```

`source-mongodb` is an opt-in feature — it is not part of the CLI or umbrella default build.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: mongodb
    config:
      connection_uri: mongodb://localhost:27017
      database: shop
      collection: orders
      filter:
        status: completed
      sort:
        created_at: 1
      limit: 100000
  sink:
    type: file
    config:
      path: ./orders.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

All fields live directly under the source `config:` block.

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | string | — *(required)* | MongoDB connection URI (e.g. `mongodb://localhost:27017` or `mongodb+srv://…`). Credentials, options, and replica-set config all go here. Masked in `Debug` output. |
| `database` | string | — *(required)* | Database name. |
| `collection` | string | — *(required)* | Collection name. |

### Query

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `filter` | object | *(none)* | Query filter as a JSON object, converted to a BSON `Document` at query time. Must be a JSON object — arrays/scalars error. Empty/unset matches all documents. |
| `projection` | object | *(none)* | Field projection as a JSON object, e.g. `{"_id": 0, "name": 1}`. |
| `sort` | object | *(none)* | Sort specification as a JSON object, e.g. `{"created_at": -1}`. |
| `limit` | int (i64) | *(none)* | Maximum number of documents to return. Unset = no limit. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int (usize) | `1000` | Records per emitted `StreamPage`. `0` is the **no-batching sentinel** — the cursor is fully drained and the entire result set is emitted in a single page. Values above `MAX_BATCH_SIZE` (1,000,000) are rejected at construction by `faucet_core::validate_batch_size`. |
| `cursor_batch_size` | int (u32) | *(driver default, 101)* | The MongoDB driver's per-round-trip batch size (`find().batch_size(N)`). Independent of `batch_size` — see [Streaming & batching](#streaming--batching). |

> **Note on the rename:** an earlier release used a single `batch_size: Option<u32>` that mapped to the driver cursor knob. That field is now `cursor_batch_size`, and the standardized `batch_size: usize` (records per `StreamPage`) lives alongside it. A config that previously used `batch_size: <n>` as the driver knob should rename it to `cursor_batch_size`.

## Authentication

MongoDB authentication is configured **inside the connection URI** — this connector does not take a separate `auth:` block or participate in the shared `auth:` catalog. Put credentials, auth source, and TLS options in the URI:

```yaml
# Username / password (SCRAM)
connection_uri: mongodb://appuser:s3cret@db1.example.com:27017/?authSource=admin

# Atlas / SRV with TLS
connection_uri: mongodb+srv://appuser:s3cret@cluster0.abcde.mongodb.net/?retryWrites=true&w=majority
```

To keep the password out of the config file, use a secrets directive (`${env:…}`, `${vault:…}`, `${aws-sm:…}`, …) on the URI:

```yaml
config:
  connection_uri: ${env:MONGO_URI}
```

## Examples

### Filtered query with projection and sort

```yaml
# MongoDB → PostgreSQL JSONB
version: 1
name: mongodb_to_postgres
pipeline:
  source:
    type: mongodb
    config:
      connection_uri: mongodb://localhost:27017
      database: shop
      collection: orders
      filter:
        status: completed
      projection:
        _id: 1
        customer_id: 1
        total: 1
        items: 1
      sort:
        created_at: 1
      limit: 500000
      cursor_batch_size: 1000
  sink:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/warehouse
      table_name: orders_mirror
      column_mapping:
        type: jsonb
        column: payload
      batch_size: 1000
      max_connections: 10
```

### Operator-rich filter, large pages

```yaml
version: 1
pipeline:
  source:
    type: mongodb
    config:
      connection_uri: ${env:MONGO_URI}
      database: analytics
      collection: events
      filter:
        event_type: purchase
        amount: { $gt: 100 }
      projection:
        _id: 0
        user_id: 1
        amount: 1
      sort:
        amount: -1
      batch_size: 5000          # 5k docs per StreamPage
      cursor_batch_size: 500    # 500 docs per server round-trip
  sink:
    type: elasticsearch
    config:
      url: http://localhost:9200
      index: events
```

### Small lookup table — one page, no batching

```yaml
version: 1
pipeline:
  source:
    type: mongodb
    config:
      connection_uri: mongodb://localhost:27017
      database: ref
      collection: countries
      batch_size: 0             # drain the whole cursor into a single page
  sink:
    type: stdout
    config:
      format: jsonl
```

### Per-parent context interpolation (matrix fan-out)

`{key}` placeholders inside `filter`/`projection`/`sort` are resolved per parent record at runtime (JSON-safe), so a child row can scope its query to each parent:

```yaml
filter:
  tenant_id: "{tenant_id}"      # resolved from the parent record's `tenant_id`
```

## Streaming & batching

`MongoSource::stream_pages` drives the driver cursor (`Collection::find`) without buffering the full result. Documents are accumulated into a `batch_size` buffer and yielded as a `StreamPage` once the buffer fills; the trailing partial page (if any) is yielded after the cursor drains. The pipeline writes each page to the sink as it arrives, so memory is bounded by `batch_size × document_width`, not by the size of the collection or result set.

- **`batch_size = 0`** is the no-batching sentinel — the cursor is drained completely and the entire result set is emitted in one `StreamPage`. Use it for small lookup tables, or for sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage uploads) that prefer one large request to many small ones.
- The trait-level `batch_size` argument passed to `stream_pages` is **ignored** in favour of the config field — the config is the authoritative knob, so a pipeline-supplied hint can never silently override an explicit config value.

`cursor_batch_size` is a **different, lower-level knob**: it sets the MongoDB driver's per-round-trip batch size (`find().batch_size(N)`). It is independent of the pipeline-level `batch_size` — you can have the driver fetch 100 docs per round-trip and still yield `StreamPage`s of 1000 docs (or vice versa), trading network round-trips against buffering. Leave it unset to use the driver default (101 documents).

Every emitted page carries `bookmark: None` — the MongoDB source has no incremental-replication mode (see [Resume & state](#resume--state)).

## Resume & state

This source is **not resumable** and does not support effectively-once delivery. It runs a single `find()` per invocation and emits `bookmark: None` on every page, so no durable position is written and a re-run replays the full query. For change-data-capture with resumable resume tokens and effectively-once semantics, use [faucet-source-mongodb-cdc](https://crates.io/crates/faucet-source-mongodb-cdc) instead.

To approximate incremental loads with this query source, add a high-watermark clause to `filter` and parameterize it (e.g. `created_at: { $gt: "${env:SINCE}" }`) at the orchestration layer.

## Dataset discovery

The source implements [`Source::discover`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html#method.discover) (#211): it enumerates every collection in the configured `database` (excluding `system.*`) and returns one `DatasetDescriptor` per collection with

- `name` — the collection name; `kind` — `"collection"`;
- `config_patch` — `{"collection": "<name>"}`, ready to deep-merge over the connection config (one matrix row per collection);
- `estimated_rows` — from `estimated_document_count()` (collection metadata, no scan);
- `schema` — inferred from a bounded 10-document sample per collection (relaxed extended JSON, same conversion as the fetch path). An empty collection reports no schema.

Discovery is read-only and cheap: one `listCollections`, plus one metadata count and one 10-document `find` per collection.

## BSON ↔ JSON conversion

- **Inputs** (`filter`, `projection`, `sort`) are converted from JSON to BSON `Document`s at query time. Each must be a JSON **object** — passing an array or scalar produces a `FaucetError::Config`.
- **Outputs** — documents returned from MongoDB are rendered as **relaxed extended JSON**:

| BSON type | JSON shape |
|---|---|
| `ObjectId` | `{"$oid": "<hex>"}` |
| `DateTime` (post-1970) | `{"$date": "<RFC3339>"}` |
| `Int32` / `Int64` | bare number |
| `Double` | bare number |
| `Boolean` | `true` / `false` |
| `String` | string |
| `Null` | JSON `null` |
| `Array` | array |
| embedded `Document` | nested object |

If a sink needs flat scalar columns instead of the `{"$oid": …}` / `{"$date": …}` wrappers, add a transform (e.g. `set` / `rename_field` / `cast`) between the source and sink.

## Config loading & schema

Load config from a JSON file or environment in library code:

```rust,no_run
use faucet_core::config::{load_json, load_env_file};
use faucet_source_mongodb::MongoSourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let from_file: MongoSourceConfig = load_json("config.json")?;
let from_env: MongoSourceConfig = load_env_file(".env", "MONGO_SOURCE")?;
# Ok(()) }
```

Example `.env`:

```env
MONGO_SOURCE_CONNECTION_URI=mongodb://localhost:27017
MONGO_SOURCE_DATABASE=mydb
MONGO_SOURCE_COLLECTION=users
MONGO_SOURCE_LIMIT=5000
MONGO_SOURCE_CURSOR_BATCH_SIZE=200
MONGO_SOURCE_BATCH_SIZE=1000
```

Inspect the full JSON Schema from the CLI:

```bash
faucet schema source mongodb
```

## Library usage

```rust,no_run
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::Source;
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = MongoSourceConfig::new(
    "mongodb://localhost:27017",
    "ecommerce",
    "orders",
)
.filter(json!({ "status": "completed", "total": { "$gt": 100 } }))
.projection(json!({ "_id": 0, "order_id": 1, "total": 1 }))
.sort(json!({ "total": -1 }))
.limit(500)
.cursor_batch_size(100)
.with_batch_size(2000);

let source = MongoSource::new(config).await?;

// One-shot collect:
let orders = source.fetch_all().await?;
println!("found {} high-value orders", orders.len());
# Ok(()) }
```

Drive it through a `Pipeline` to stream into a sink:

```rust,no_run
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::{Pipeline, Source};
use serde_json::json;

# async fn example(my_sink: Box<dyn faucet_core::Sink>) -> Result<(), Box<dyn std::error::Error>> {
let source = MongoSource::new(
    MongoSourceConfig::new("mongodb://localhost:27017", "logs", "app_events")
        .filter(json!({ "level": "error" }))
        .sort(json!({ "timestamp": -1 }))
        .limit(10_000),
).await?;

let pipeline = Pipeline::new(Box::new(source), my_sink);
let result = pipeline.run().await?;
# let _ = result;
# Ok(()) }
```

## How it works

- The MongoDB `Client` (and its internal connection pool) is created **once** in `MongoSource::new()` from the connection URI and reused for every `fetch_all()` / `stream_pages()` call — never recreated per query.
- `stream_pages` opens a single cursor with `FindOptions` (`projection` / `sort` / `limit` / `cursor_batch_size`) and pulls documents with `cursor.advance()`; each is deserialized to a BSON `Document` and converted to relaxed extended JSON, then pushed into the page buffer. A page is yielded when the buffer reaches `batch_size` (or at end-of-cursor for the trailing partial page).
- `batch_size` is validated against `MAX_BATCH_SIZE` at construction, so an out-of-range value fails fast with `FaucetError::Config` rather than mid-run.
- Context interpolation serializes the JSON `filter`/`projection`/`sort`, runs JSON-safe placeholder substitution, then re-parses — so a `{key}` value containing quotes can't break JSON validity.

## Lineage dataset URI

`mongodb://<host>:<port>/<database>/<collection>` with credentials stripped — e.g. `mongodb://host:27017/mydb/events`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI / umbrella crate via the `source-mongodb` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Source: MongoDB connection failed` | Bad URI, unreachable host, wrong port, or auth rejected. Verify `connection_uri`, network reachability, and `authSource`. For Atlas, ensure your IP is allowlisted. |
| `FaucetError::Source: MongoDB find failed` | Invalid filter/sort against the collection (e.g. sorting on an unindexed field past the in-memory sort limit), or insufficient read privileges. Check the query and the user's role. |
| `FaucetError::Config: expected a JSON object, got BSON type …` | `filter`, `projection`, or `sort` was a JSON array or scalar. Each must be a JSON **object** (`{}`). |
| `FaucetError::Config` mentioning `batch_size` | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` for a single page. |
| Results come back with `{"$oid": …}` / `{"$date": …}` wrappers | Intentional — relaxed extended JSON preserves BSON types. Add a transform to flatten them if the sink needs plain scalars. |
| Query is much slower than expected | High-cardinality `sort` without a supporting index forces an in-memory sort (capped server-side). Add an index, or lower `limit`. Tune `cursor_batch_size` to cut round-trips. |
| `mongodb+srv://` URI fails to resolve | SRV requires DNS SRV/TXT records (Atlas provides them). Confirm DNS resolution from the host, or use the standard `mongodb://` seed-list form. |
| Re-running reprocesses everything | Expected — this query source is not resumable. Use [faucet-source-mongodb-cdc](https://crates.io/crates/faucet-source-mongodb-cdc) for resumable change capture, or add a watermark to `filter`. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- [faucet-source-mongodb-cdc](https://crates.io/crates/faucet-source-mongodb-cdc) — resumable, effectively-once change capture from MongoDB.
- [faucet-sink-mongodb](https://crates.io/crates/faucet-sink-mongodb) — write into MongoDB (with upsert/delete write modes).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="mongodb"`.
