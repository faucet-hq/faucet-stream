# faucet-sink-mongodb

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mongodb.svg)](https://crates.io/crates/faucet-sink-mongodb)
[![Docs.rs](https://docs.rs/faucet-sink-mongodb/badge.svg)](https://docs.rs/faucet-sink-mongodb)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-mongodb.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-mongodb.svg)](https://github.com/faucet-hq/faucet-stream#license)

**MongoDB** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records into a MongoDB collection — appending with batched `insert_many`, or keeping a collection in sync with a changing source via `upsert` / `delete`. With `delivery: exactly_once` (replica set required) it commits each page and a watermark in one multi-document transaction.

Reach for it when you want to land any faucet-stream source — a REST API, a database, a CDC stream, a file — into MongoDB with one declarative config and no glue code. Each record is converted to a BSON document; the client connection is established once and reused across every write.

## Feature highlights

- **Batched inserts** — append mode issues `insert_many` per `batch_size`-sized chunk, the documented sweet spot for balancing round-trip overhead against MongoDB's ~48 MB per-request budget.
- **Unordered by default** — `ordered: false` so a single poison document (duplicate `_id`, validation error) can't drop the rest of the batch.
- **Write modes** — `append` (default), `upsert` (per-document `replace_one(upsert)`), and `delete` (`delete_one`); the `key` fields become the match filter (MongoDB is schemaless — no key columns).
- **CDC mirroring** — a `delete_marker` mixes upserts and deletes in one stream, so a CDC source carrying an op flag (`__op: "u" | "d"`) keeps a collection in lock-step with its origin.
- **Effectively-once delivery** — with `delivery: exactly_once` each page and its commit-token watermark commit atomically in one **multi-document transaction** (replica set required). See [Effectively-once delivery](#effectively-once-delivery).
- **Dead-letter queue aware** — overrides `write_batch_partial` so a document the server rejects (append) or a missing/null-key row (upsert/delete) is routed to a DLQ per-row while the good documents still commit.
- **Nested documents preserved** — arbitrary nested objects, arrays, and all JSON scalar types convert to their BSON equivalents losslessly.
- **Client built once** — the connection pool is created and validated in `new()` and reused for every write; the driver handles pooling and reconnection internally.
- **Credential-safe logging** — the `Debug` impl masks `connection_uri` with `***`, and the lineage URI strips embedded credentials.

## Installation

```bash
# As a library:
cargo add faucet-sink-mongodb

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-mongodb
```

Or via the umbrella crate: `cargo add faucet-stream --features sink-mongodb`.

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
    type: mongodb
    config:
      connection_uri: mongodb://localhost:27017
      database: analytics
      collection: events
      batch_size: 1000
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | string | — *(required)* | MongoDB connection URI (e.g. `mongodb://user:pass@host:27017`, or a replica-set URI). Masked as `***` in `Debug` output. |
| `database` | string | — *(required)* | Target database name. |
| `collection` | string | — *(required)* | Target collection name. |

### Batching & reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum documents per `insert_many` call (append mode). **`0` = no batching**: the entire records slice is sent in one call. Values above `MAX_BATCH_SIZE` (1,000,000) are rejected. See [Streaming & batching](#streaming--batching). |
| `ordered` | bool | `false` | Whether `insert_many` is ordered. Default `false` (unordered) so one bad document doesn't drop the rest of the batch. Set `true` only when you require strict insertion order and want the batch to abort at the first failure. |

### Write mode

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | string | `append` | `append`, `upsert`, or `delete`. See [Write modes (upsert / delete)](#write-modes-upsert--delete). |
| `key` | `[string]` | `[]` | Match-filter fields for `upsert` / `delete`. **Required and non-empty** for those modes; ignored for `append`. Typically `["_id"]`. |
| `delete_marker` | object | *(none)* | Upsert only. `{ field, values }` — rows whose `field` matches one of `values` are routed to deletes instead of upserts. The marker field is stripped from upsert rows before writing. |

## Examples

### Append events from a REST API

```yaml
sink:
  type: mongodb
  config:
    connection_uri: mongodb://localhost:27017
    database: analytics
    collection: events
    batch_size: 1000
```

### High-throughput load with large batches

```yaml
sink:
  type: mongodb
  config:
    connection_uri: mongodb://writer:s3cret@mongo-primary:27017
    database: warehouse
    collection: raw_events
    batch_size: 5000        # narrow docs where round-trip latency dominates
```

### Replica-set target, no client-side re-chunking

```yaml
sink:
  type: mongodb
  config:
    connection_uri: mongodb://writer:s3cret@mongo1:27017,mongo2:27017,mongo3:27017/analytics?replicaSet=rs0
    database: analytics
    collection: events
    batch_size: 0           # forward each upstream StreamPage as one insert_many
```

### CDC mirror — upsert with a delete marker

Pair a CDC source (e.g. `faucet-source-mongodb-cdc`) with a `delete_marker` to keep a collection in lock-step with its origin:

```yaml
sink:
  type: mongodb
  config:
    connection_uri: mongodb://localhost:27017
    database: analytics
    collection: users
    write_mode: upsert
    key: ["_id"]
    delete_marker:
      field: __op
      values: ["d", "delete"]
```

## Streaming & batching

`MongoSink::write_batch` re-chunks the incoming records slice into `batch_size` slices and issues one `insert_many` call per chunk. The default of `1000` matches MongoDB's documented sweet spot — roughly 1000 documents per call balances round-trip overhead against the per-request BSON size budget (the server caps a single request at ~48 MB). Tune **up** for narrow documents where round-trip latency dominates, and **down** for very wide documents that bump against the size cap.

`batch_size = 0` is the **"no batching" sentinel** — `write_batch` forwards the entire records slice in a single `insert_many` call, so upstream `StreamPage` framing flows through untouched. Use it when the source already emits pages sized for MongoDB's per-request limits. Values above `MAX_BATCH_SIZE` (1,000,000) are rejected by `faucet_core::validate_batch_size`.

This `batch_size` is a **write-side chunking knob** specific to the sink. It is unrelated to the driver's internal `cursor_batch_size` (the read-side cursor tuning knob used by `faucet-source-mongodb`) — `insert_many` and a query cursor are different operations.

## Write modes (upsert / delete)

By default the sink runs in `append` mode and inserts every record via `insert_many`. Set `write_mode: upsert` or `write_mode: delete` to keep a collection in sync with a changing source instead of only appending.

MongoDB is schemaless, so unlike the SQL sinks there are no key *columns* — the `key` fields become the **match filter** for each document. `key` is typically `["_id"]`, but any combination of top-level fields works (composite keys are matched on all of them). `key` must be non-empty for `upsert` / `delete`; an empty `key` is rejected at config-load time.

- **`upsert`** — each row is committed with a per-document `replace_one(filter, replacement).upsert(true)`. The filter is built from the row's `key` fields and the whole row is the replacement document, so an existing document is **replaced in place** (not field-merged) and a missing one is inserted.
- **`delete`** — each row is removed with `delete_one(filter)`, where the filter is built from the row's `key` fields.
- **`delete_marker`** (upsert only) — mix upserts and deletes in one stream: rows whose `delete_marker.field` matches one of `delete_marker.values` are routed to `delete_one`; all others are upserted. The marker field is stripped from the upsert replacement so it never lands in the collection — ideal for CDC streams that carry an operation flag like `__op: "u" | "d"`.

Within a single batch, repeated keys are deduped **last-write-wins** before any write is issued, so a page that touches the same `_id` twice results in a single `replace_one` / `delete_one` carrying the final value.

A document missing or null in a key field fails. When a `dlq:` block is configured the good documents are still written and only the missing/null-key documents are routed to the DLQ per-row; without a DLQ the whole batch fails.

Each `replace_one` / `delete_one` is a per-document primitive (not the namespaced `Client::bulk_write`) for compatibility with all supported MongoDB server versions; the sink recovers throughput by issuing the deduped ops concurrently.

## Effectively-once delivery

`MongoSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes the page's documents **and** upserts the watermark document `{ _id: <scope>, token: <token> }` into a `_faucet_commit_token` collection (in the same database) inside one **multi-document transaction**, so both either commit together or neither does.
- `last_committed_token(scope)` — reads the watermark back so the pipeline skips already-committed pages on resume. The token is opaque and round-trips verbatim.

> **A replica set (or sharded cluster) is required.** MongoDB multi-document transactions are unavailable on a standalone server — `write_batch_idempotent` surfaces that as a typed error naming the requirement (`… requires a replica set or sharded cluster …`). A **single-node** replica set is sufficient (e.g. `mongod --replSet rs0` + `rs.initiate()`); no second member is needed.

Semantics on the effectively-once path (vs. the at-least-once `write_batch`):

- **One page = one transaction.** In append mode the whole page goes in a single `insert_many` — the sink's `batch_size` re-chunking knob does **not** apply on this path (chunking would break page↔watermark atomicity). Size pages with the **source's** `batch_size` instead, keeping each page within MongoDB's per-transaction limits.
- **Upsert/delete ops run sequentially inside the transaction.** A MongoDB `ClientSession` cannot be used concurrently, so the planned `replace_one(upsert)` / `delete_one` ops are issued one at a time — a throughput tradeoff versus the at-least-once path's concurrent fan-out. Atomicity requires the single session.
- **The transaction writes with `w: majority`**, whatever the client's default, so a committed page and its watermark cannot roll back after a failover.
- **Commit is retried** while the driver reports `UnknownTransactionCommitResult` (the driver-recommended pattern, bounded); any other failure aborts the transaction (best-effort) and surfaces the original error — nothing from the page is committed.
- The data and `_faucet_commit_token` collections are pre-created (idempotently) before the first transaction, so the path also works on servers that can't create collections inside a transaction (MongoDB < 4.4).

To use effectively-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
version: 1
pipeline:
  source:
    type: mongodb-cdc
    config:
      connection_uri: mongodb://source:pass@src-mongo:27017/?replicaSet=rs0
      database: appdb
      collection: users
  sink:
    type: mongodb
    config:
      connection_uri: mongodb://writer:pass@mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0
      database: analytics
      collection: users_mirror
      write_mode: upsert
      key: ["_id"]
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

`delivery: exactly_once` and `write_mode: upsert` compose — the planned upserts/deletes and the watermark upsert commit in the same transaction, as in the example above. See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).

## Dead-letter queue

The sink overrides `write_batch_partial`, so a `dlq:` block in the pipeline config catches per-row failures (missing/null-key rows in `upsert` / `delete` mode) and routes them to the dead-letter sink while the rest of the page commits. Without a DLQ, a row-level failure aborts the batch. See the [DLQ cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html).

## Config loading & schema

Load config from YAML/JSON, environment variables, or a `.env` file:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_mongodb::MongoSinkConfig;

let from_file: MongoSinkConfig = load_json("config.json")?;
let from_env: MongoSinkConfig = load_env_file(".env", "MONGO_SINK")?;
```

```env
MONGO_SINK_CONNECTION_URI=mongodb://writer:s3cret@mongo.example.com:27017
MONGO_SINK_DATABASE=analytics
MONGO_SINK_COLLECTION=events
MONGO_SINK_BATCH_SIZE=1000
```

Inspect the full JSON Schema with:

```bash
faucet schema sink mongodb
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = MongoSinkConfig::new(
    "mongodb://localhost:27017",
    "analytics",
    "events",
)
.with_batch_size(1000);

let sink = MongoSink::new(config).await?;

let records = vec![
    json!({"user_id": "u123", "event": "signup", "source": "web"}),
    json!({"user_id": "u456", "event": "login", "source": "mobile"}),
];

let written = sink.write_batch(&records).await?;
println!("Inserted {written} documents");
# Ok(())
# }
```

Drive it from a `Pipeline` with any source:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events"),
);
let sink = MongoSink::new(
    MongoSinkConfig::new("mongodb://localhost:27017", "analytics", "events"),
).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
# Ok(())
# }
```

## How it works

1. `MongoSink::new()` builds the client via `Client::with_uri_str()` — the connection is established and validated **once** and the pool is reused for every write.
2. **Append:** `write_batch` splits records into `batch_size` chunks; each chunk is converted from `serde_json::Value` to `bson::Document` and inserted with `collection.insert_many()` (unordered unless `ordered: true`). With `batch_size = 0`, the whole slice goes in one call.
3. **Upsert / delete:** `faucet_core::plan_writes` dedups the page last-write-wins, strips the delete marker, and partitions into upserts / deletes / failed rows; the sink issues per-document `replace_one(upsert)` / `delete_one` ops **concurrently**.
4. **Exactly-once (`delivery: exactly_once`):** `write_batch_idempotent` opens a `ClientSession` transaction, applies the page (whole-page `insert_many`, or the planned ops sequentially) plus a `replace_one(upsert)` of the `{ _id: scope, token }` watermark into `_faucet_commit_token`, then commits with the driver-recommended `UnknownTransactionCommitResult` retry loop. Requires a replica set.
5. Every record must be a JSON object — non-object values produce an error during BSON conversion.
6. The driver handles connection pooling and automatic reconnection internally.

## Lineage dataset URI

`mongodb://<host>:<port>/<database>/<collection>` (credentials stripped) — e.g. `mongodb://host:27017/analytics/events`.

This connector reports observability metrics under the label `connector="mongodb"`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI / umbrella via the `sink-mongodb` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| Connection fails in `new()` / `check` ping fails | `connection_uri`, credentials, or network are wrong. Run `faucet doctor` — the sink's preflight runs a `ping` admin command. Verify the URI, that the server is reachable, and (for a replica set) that the `replicaSet=` name matches. |
| Duplicate `_id` errors drop part of an append batch | You set `ordered: true`. Leave the default `ordered: false` so only the genuinely-bad documents fail and the rest of the batch still commits. |
| `mongodb upsert: row N: ...` error | A row is missing or null in a `key` field. Ensure every record carries the `key` field(s), or configure a `dlq:` block to quarantine the bad rows per-row while the good ones commit. |
| Upsert replaces the whole document instead of merging fields | Expected — `upsert` uses `replace_one`, which replaces the matched document in place. Shape the record upstream (e.g. with transforms) to carry every field you want retained. |
| Delete-flagged CDC rows are inserted instead of removed | The `delete_marker.field` / `values` don't match the source's op flag. Confirm the field name and values (commonly `__op` with `["d", "delete"]`); pair with the `cdc_unwrap` transform to normalize the envelope to `__op`. |
| `mongodb exactly-once (write_batch_idempotent) requires a replica set or sharded cluster …` | You pointed `delivery: exactly_once` at a **standalone** server — MongoDB transactions need a replica set. Convert the target to a (single-node is fine) replica set: start `mongod` with `--replSet rs0` and run `rs.initiate()`, then reconnect (add `directConnection=true` if connecting to a single member by address). |
| "expected a JSON object" / BSON conversion error | A record is an array, string, number, or null. Every record must be a JSON object; reshape upstream with a transform. |
| Documents rejected at ~48 MB | A batch exceeds MongoDB's per-request limit. Lower `batch_size` for wide documents, or split very large nested documents upstream. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix across all connectors.
- [Write modes / upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) — the shared upsert layer.
- [Dead-letter queue cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) — routing failed rows.
- [`faucet-source-mongodb`](https://crates.io/crates/faucet-source-mongodb) — the MongoDB source (`find()` with filter / projection / sort).
- [`faucet-source-mongodb-cdc`](https://crates.io/crates/faucet-source-mongodb-cdc) — MongoDB Change Streams CDC source; the natural upstream for an upsert mirror.

## Batch atomicity

What a failed write leaves behind (#737): **per-row** for `append` / `overwrite` — `write_batch_partial` maps `insert_many`'s write errors to the rejected documents (a duplicate `_id`, a validation failure, a non-object record), inserts every other document (under `ordered: true` it resumes after the rejected one), and fails outright only while nothing has been inserted. `upsert` / `delete` are **best-effort** — per-document upserts are not transactional. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Overwrite (`write_mode: overwrite`)

Full-refresh: each run atomically **replaces** the whole collection. Documents
are staged into `{collection}__faucet_ovw` and published with an atomic
`renameCollection(dropTarget: true)` only after the run succeeds, so a mid-run
failure leaves the previous documents intact. No `key` is needed. Requires the
`renameCollection` privilege and is unsupported on sharded collections.

The staging collection is created at `begin` — empty when there is no destination yet, so a first run whose source returns no rows still commits an empty collection. The staging collection is created with the destination's options (validator,
collation, capped size, …) and its secondary indexes (unique, TTL, compound,
…), so they survive the swap. A unique index is enforced while the run loads,
so rows that would break it fail the run before anything is replaced. The
copy needs the `listCollections`, `listIndexes`, `createCollection` and
`createIndex` privileges.


**Leftover staging.** `overwrite_staging_exists()` probes for the `<collection>__faucet_ovw` collection read-only; `faucet status --probe` uses it to report staging a crashed or aborted overwrite left behind (`present` / `absent`). The next overwrite run replaces it.
