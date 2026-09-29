# faucet-source-mongodb-cdc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mongodb-cdc.svg)](https://crates.io/crates/faucet-source-mongodb-cdc)
[![Docs.rs](https://docs.rs/faucet-source-mongodb-cdc/badge.svg)](https://docs.rs/faucet-source-mongodb-cdc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mongodb-cdc.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mongodb-cdc.svg)](https://github.com/faucet-hq/faucet-stream#license)

MongoDB **Change Data Capture (CDC)** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Tails a MongoDB [Change Stream](https://www.mongodb.com/docs/manual/changeStreams/) — at collection, database, or whole-cluster scope — and emits each per-document change event (insert, update, replace, delete, DDL) as a flat JSON CDC envelope.

Reach for it when you want to stream live mutations out of MongoDB into any faucet-stream sink — a warehouse, a queue, another database — with no polling and no missed changes. The opaque `resumeToken` is persisted to any `faucet-core` `StateStore` after every page, so a restarted pipeline resumes from exactly where it stopped: no duplicates, no gap. Paired with a CDC-aware sink, it is also an **effectively-once** delivery source.

## Feature highlights

- **Native Change Streams** — tails MongoDB's oplog-backed change stream directly via the official `mongodb` driver. No polling loop, no last-modified timestamp column, no `_changed_at` bookkeeping on your documents.
- **Three watch scopes** — one collection, one whole database, or the entire deployment (`cluster`). Pick the narrowest scope that covers what you need.
- **Resumable by design** — the server-assigned `resumeToken` of the last event in each page becomes the pipeline bookmark; on restart the stream re-opens with `resumeAfter: <token>` for a duplicate-free, gap-free resume.
- **Effectively-once delivery** — `supports_exactly_once()` is `true`. Combined with an idempotent sink (sqlite / postgres / mysql / mssql / iceberg / bigquery) and a state store, the pipeline commits records and a monotonic commit token in one atomic unit.
- **Snapshot → CDC handoff** — implements `capture_resume_position()`, so `faucet replicate` can anchor the change stream *before* a bulk snapshot of the collection and stitch the two into a true mirror.
- **Server-side filtering** — push an `operationType` allowlist and arbitrary aggregation stages into the change stream so unwanted events never cross the wire.
- **Pre-image / post-image control** — request the full document `before` and/or `after` each change (MongoDB 6.0+ for pre-images), with explicit `off` / `when_available` / `required` / `update_lookup` modes.
- **Bounded memory** — events are emitted in pages of `batch_size`; peak memory is `O(batch_size)` no matter how busy the collection is. With `batch_size: 0` (single-page drain), `max_staged_records` is the OOM safety valve that caps the in-memory buffer and aborts cleanly rather than risking an OOM-kill.
- **Client built once** — the authenticated `mongodb::Client` is constructed in `new()` and reused for the lifetime of the source.

## Installation

```bash
# As a library:
cargo add faucet-source-mongodb-cdc

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-mongodb-cdc
```

This connector is **not** in the CLI default build — enable the `source-mongodb-cdc` feature (or the `source` / `full` aggregates).

## Prerequisites

Change Streams are only available on a **replica set** or a **sharded cluster** — they read from the oplog, which a standalone `mongod` does not maintain. The connector validates this at startup: if the connection URI points at a standalone instance, `new()` fails fast with a typed `FaucetError::Source` rather than silently producing no events.

For local development, run a single-node replica set:

```bash
mongod --replSet rs0 --bind_ip localhost
# then, in the shell, once:
mongosh --eval 'rs.initiate()'
```

Connect with `?replicaSet=rs0` in the URI. Some additional requirements depending on the features you use:

- **Pre-images** (`full_document_before_change`) require MongoDB **6.0+** and per-collection `changeStreamPreAndPostImages: { enabled: true }`.
- The connecting user needs `find` and `changeStream` privileges on the watched namespace (the built-in `read` role covers a single database; cluster scope needs read across all databases).

## Quick start

```yaml
# pipeline.yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://user:pass@localhost:27017/?replicaSet=rs0
    scope:
      type: collection
      database: appdb
      collection: orders
    full_document: update_lookup
    idle_timeout: 30
sink:
  type: file
  config:
    path: ./changes.jsonl
state:
  type: file
  config: { path: ./state }
```

```bash
faucet run pipeline.yaml
```

Every change to `appdb.orders` lands as one JSON line in `changes.jsonl`; the resumeToken is checkpointed to `./state` after each page so a re-run picks up exactly where it left off.

## Output record schema

Every change event is one JSON object — a flat CDC envelope:

```json
{
  "op": "c",
  "ts_ms": 1779019200000,
  "namespace": { "db": "appdb", "coll": "orders" },
  "document_key": { "_id": "6654a1b2c3d4e5f600000001" },
  "before": null,
  "after": {
    "_id": "6654a1b2c3d4e5f600000001",
    "status": "shipped",
    "total": 49.99
  },
  "update_description": null,
  "resume_token": { "_data": "826654A1B20000000..." }
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | Operation type: `c` (insert), `u` (update), `r` (replace), `d` (delete), `ddl` (drop / rename / dropDatabase / invalidate) |
| `ts_ms` | number | Wall-clock time of the change in Unix-epoch milliseconds (from `clusterTime`) |
| `namespace` | object \| null | `{ "db": "…", "coll": "…" }`. `null` for cluster-scope events that carry no namespace (e.g. a cluster-level invalidate) |
| `document_key` | object \| null | Document identity key (typically `{ "_id": … }`) |
| `before` | object \| null | Pre-image of the document. Populated only when `full_document_before_change` is enabled and the collection has `changeStreamPreAndPostImages` turned on (MongoDB 6.0+). |
| `after` | object \| null | Post-image of the document. Populated on inserts, replaces, and updates when `full_document` is `update_lookup`, `when_available`, or `required`. `null` on deletes. |
| `update_description` | object \| null | Present on `u` events: `{ "updated_fields": {…}, "removed_fields": ["…"], "truncated_arrays": [{…}] }`. `null` for all other op types. |
| `resume_token` | object | Opaque server-assigned token. The pipeline persists this as the page bookmark and passes it to `resumeAfter` on the next run. |

### Operation-type mapping

| MongoDB `operationType` | `op` value |
|-------------------------|-----------|
| `insert` | `c` |
| `update` | `u` |
| `replace` | `r` |
| `delete` | `d` |
| `drop`, `rename`, `dropDatabase`, `invalidate` | `ddl` |

## Configuration reference

### Connection & scope

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | string | — (required) | MongoDB connection URI. Must point at a replica set (`?replicaSet=rs0`) or sharded cluster. A standalone `mongod` URI causes a hard error at startup. Credentials in the URI are redacted from `Debug` output. |
| `scope` | enum | `{ type: cluster }` | Which change stream to open. See the scope variants below. |

`scope` variants:

```yaml
# Whole deployment (default)
scope: { type: cluster }

# One database (all collections)
scope: { type: database, database: appdb }

# One collection
scope: { type: collection, database: appdb, collection: orders }
```

### Filtering

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `operation_types` | string[] | `[]` (all) | Server-side `$match` allowlist on `operationType`. Empty = every operation. Example: `["insert", "update", "delete"]`. |
| `aggregation_pipeline` | object[] | `[]` | Extra aggregation stages appended **after** the internal `operation_types` `$match`. Use for server-side field projection, redaction, or further filtering of the change-event documents. |

### Document images

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `full_document` | enum | `off` | Post-image (`after`) delivery: `off` (no full doc), `when_available` (include when the server can supply it), `required` (error if unavailable), `update_lookup` (re-read the current document at event time — read-skew caveat, see [Caveats](#caveats)). |
| `full_document_before_change` | enum | `off` | Pre-image (`before`) delivery: `off`, `when_available`, or `required`. Requires MongoDB 6.0+ and `changeStreamPreAndPostImages` enabled on the collection. |

### Start position

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_from` | enum | `{ type: now }` | Where to start when **no persisted bookmark exists**. See the variants and precedence rules below. |

`start_from` variants:

```yaml
start_from: { type: now }                        # only events from now on (default)
start_from: { type: earliest }                   # from the oldest retained oplog entry
start_from: { type: resume_token, token: {...} } # an explicit opaque token
start_from: { type: timestamp, timestamp_secs: 1779019200 }  # a cluster time, in epoch seconds
```

### Batching & timing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `idle_timeout` | seconds | `30` | Terminate the current fetch cycle after this long with no new events. The page accumulated so far is flushed and the bookmark is saved. Must be `> 0`. |
| `max_await_time_ms` | u64 | `1000` | Server-side `maxAwaitTimeMS` on `getMore` requests — how long the server blocks waiting for new events before returning an empty batch. Must be **strictly less than** `idle_timeout` (in milliseconds), else config validation fails. |
| `batch_size` | usize | `1000` | Records per emitted `StreamPage`. `0` = no batching: drain until idle and emit one page. Max `1,000,000`. |
| `max_staged_records` | usize \| null | `null` | OOM safety valve. Abort with a typed `FaucetError::Source` once more than this many change events are buffered in memory before a page is emitted. `null` = unbounded. Matters most with `batch_size: 0` (or a library caller using `fetch_all`), where the buffer would otherwise grow without bound for the whole fetch cycle and risk an OOM-kill on a high-throughput stream. Mirrors the same field on the sibling `postgres-cdc` / `mysql-cdc` sources. |

## Examples

### Capture all changes to a collection, land in BigQuery

```yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://faucet:secret@mongo1:27017,mongo2:27017/?replicaSet=rs0&authSource=admin
    scope:
      type: collection
      database: myapp
      collection: events
    operation_types: ["insert", "update", "replace", "delete"]
    full_document: when_available
    full_document_before_change: off
    idle_timeout: 60
    max_await_time_ms: 500
    batch_size: 500
sink:
  type: bigquery
  config:
    project_id: my-gcp-project
    dataset_id: cdc
    table_id: events_stream
    credentials:
      type: service_account_file
      config: { path: /secrets/bq-sa.json }
state:
  type: redis
  config:
    url: redis://localhost:6379
    namespace: faucet-cdc
```

### Cluster-wide CDC with operation filtering, printed to stdout

```yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://faucet:secret@mongos:27017/?tls=true
    scope:
      type: cluster
    operation_types: ["insert", "delete"]
    idle_timeout: 30
sink:
  type: stdout
  config:
    format: pretty
state:
  type: file
  config: { path: ./state/mongodb-cdc }
```

### Server-side projection via an aggregation pipeline

Strip large fields and redact a secret before events ever leave the server:

```yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://localhost:27017/?replicaSet=rs0
    scope:
      type: collection
      database: appdb
      collection: users
    full_document: update_lookup
    aggregation_pipeline:
      - { $unset: ["fullDocument.password_hash", "fullDocument.blob"] }
state:
  type: file
  config: { path: ./state }
sink:
  type: file
  config:
    path: ./users-changes.jsonl
```

## Streaming & batching

The source overrides `stream_pages` to tail the change stream natively. It accumulates change events into a `StreamPage` and yields a page when **either** `batch_size` events have accumulated **or** `idle_timeout` elapses with no new event. The bookmark carried on each page is the `resume_token` of the last event in that page, so the pipeline persists durable progress every page.

- The `batch_size` config field is authoritative — a pipeline-supplied hint never overrides it.
- `batch_size: 0` is the "no batching" sentinel: drain events until idle and emit a single page. Use it for low-traffic collections where you'd rather get one consolidated page per quiet period.
- `max_await_time_ms` is the inner blocking-wait knob; `idle_timeout` is the outer cycle terminator. The validation `max_await_time_ms < idle_timeout` guarantees the server returns control to the connector before the idle timer fires.

## Resume & state

The connector is fully resumable. After each emitted page the pipeline writes the `resume_token` of the last event to the configured `StateStore`. On the next run, `apply_start_bookmark` restores the token and the change stream opens with `resumeAfter: <token>` — MongoDB delivers events from exactly that point onwards.

**`start_from` precedence (only consulted when no persisted bookmark exists, except as noted):**

- `resume_token` and `timestamp` variants **always** override a persisted bookmark — they force an explicit start position regardless of what the state store holds.
- `now` and `earliest` variants **yield** to a persisted bookmark: if one exists the connector resumes from it; if none exists, the variant chooses the initial position.

State keys (one per scope) so that two pipelines on different scopes never collide:

| Scope | State key |
|-------|-----------|
| Cluster | `mongodb-cdc:cluster` |
| Database `mydb` | `mongodb-cdc:db:mydb` |
| Collection `mydb.mycoll` | `mongodb-cdc:coll:mydb.mycoll` |

Durability is **per-page, not per-event**: the bookmark is saved once per emitted page (every `batch_size` events). A crash mid-page replays at most `batch_size` events on the next run — at-least-once unless you also enable effectively-once delivery.

## Effectively-once delivery

`supports_exactly_once()` returns `true`. Set `delivery: exactly_once` on the pipeline and the source becomes eligible for end-to-end effectively-once semantics. The pipeline assigns a monotonic, fixed-width commit token to each bookmark-carrying page and calls the sink's `write_batch_idempotent`, which commits the records **and** the token atomically; on resume the sink's `last_committed_token` lets the pipeline skip any page already committed.

The CLI enforces a hard gate at config-load time (caught by `faucet validate` before any run):

1. **Source** must support effectively-once — `mongodb-cdc` qualifies.
2. **Sink** must support idempotent writes — one of `sqlite`, `postgres`, `mysql`, `mssql`, `iceberg`, `bigquery`.
3. A `state:` block must be configured.
4. No `dlq:` block (DLQ and effectively-once are mutually exclusive in this version).

```yaml
version: 1
delivery: exactly_once
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://localhost:27017/?replicaSet=rs0
    scope: { type: collection, database: appdb, collection: orders }
    full_document: update_lookup
sink:
  type: postgres
  config:
    connection_url: postgres://user:pass@localhost/warehouse
    table: orders_cdc
state:
  type: postgres
  config:
    connection_url: postgres://user:pass@localhost/warehouse
```

> **Note:** `full_document: update_lookup` re-reads the document at lookup time, not change time (see [Caveats](#caveats)). For a strict mirror, prefer the change-event payload itself plus an idempotent sink, and pair with the `cdc_unwrap` transform + `write_mode: upsert` — see the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

## Snapshot → CDC handoff

This source implements `capture_resume_position()`, which opens a change stream against the configured scope and reads its `postBatchResumeToken` (present after the first server round-trip even with no events) as a resume bookmark — **without consuming any changes**. The `faucet replicate` command uses it to anchor the change stream at-or-before a bulk snapshot of the collection, so the combined snapshot + CDC result is a gap-free, duplicate-free mirror when paired with a `write_mode: upsert` sink.

There is no replication slot to pin — the **oplog window** is the retention risk. Keep it comfortably larger than the expected snapshot duration, or the captured token may roll off the oplog before CDC starts and the stream will error. See the [replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html) for the full handoff model.

## `invalidate` events

An `invalidate` event (emitted when a watched collection or database is dropped while the stream is open) is delivered as a `ddl` record and then **terminates the stream**. The connector closes the stream and returns; a fresh `faucet run` is needed to start a new change stream. Persisted bookmarks from before the invalidate are no longer resumable by MongoDB — use `start_from: { type: now }` or `{ type: earliest }` on the next run.

## Config loading

Configs load from YAML/JSON files, environment variables, or `.env` files. With the CLI, point `faucet run` at any YAML or JSON file. Programmatically, deserialize `MongoCdcSourceConfig` with `serde` from any supported format, or use the helpers in `faucet_core::config` (`load_json`, `load_env`, `load_env_file`).

## Schema introspection

Print the full JSON Schema for this source's config — every field, type, default, and enum variant — with:

```bash
faucet schema source mongodb-cdc
```

This is generated from the config struct via `schemars`, so it never drifts from the code.

## Library usage

```rust,no_run
use faucet_core::{Pipeline, state::FileStateStore};
use faucet_source_mongodb_cdc::{MongoCdcSource, MongoCdcSourceConfig, Scope};
use std::sync::Arc;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config: MongoCdcSourceConfig = serde_json::from_value(serde_json::json!({
    "connection_uri": "mongodb://localhost:27017/?replicaSet=rs0",
    "scope": { "type": "collection", "database": "appdb", "collection": "orders" },
    "full_document": "update_lookup",
    "idle_timeout": 30
}))?;

// `new()` validates config, builds the client, and verifies the deployment
// supports change streams (fails fast on a standalone).
let source = MongoCdcSource::new(config).await?;

// Build your sink (any faucet-stream sink) and a durable state store so the
// resumeToken survives restarts.
let state = Arc::new(FileStateStore::new("./state"));
# let sink = todo!();
let pipeline = Pipeline::new(source, sink).with_state_store(state);
pipeline.run().await?;
# Ok(())
# }
```

## How it works

- **One client, reused.** `MongoCdcSource::new()` builds the `mongodb::Client`, validates the config's fail-fast invariants, and confirms the deployment is a replica set / sharded cluster. The same client backs every fetch cycle.
- **Native change stream.** Each fetch cycle opens (or resumes) a change stream with the configured scope, `$match` on `operation_types`, any extra `aggregation_pipeline` stages, the chosen full-document / pre-image modes, and `resumeAfter` (or `startAtOperationTime`) derived from the bookmark or `start_from`.
- **Page assembly.** Events are decoded into the flat CDC envelope and buffered. A page flushes on `batch_size` events or `idle_timeout` quiet; `max_await_time_ms` controls the server's blocking `getMore` wait so the idle timer can fire promptly.
- **Bookmark tracking.** The `resume_token` of the last event in a page becomes the page bookmark, which the pipeline persists after the sink confirms the write — so a token is only durable once its events are.

## Lineage dataset URI

`dataset_uri()` emits `mongodb://<host>:<port>/<database>/<collection>`, `/<database>`, or just the host (depending on scope), with credentials stripped — e.g. `mongodb://host:27017/appdb/orders` for collection scope, `mongodb://host:27017/appdb` for database scope, and the bare host for cluster scope.

## Feature flags

This crate has no optional features of its own. From the umbrella / CLI it is gated behind:

| Feature | Enables |
|---------|---------|
| `source-mongodb-cdc` | this connector |
| `source` | all sources, including this one |
| `full` | everything |

## Troubleshooting / FAQ

| Symptom | Cause & fix |
|---------|-------------|
| `The $changeStream stage is only supported on replica sets` or a startup `FaucetError::Source` about standalone | The URI points at a standalone `mongod`. Change Streams need a replica set or sharded cluster. Run `mongod --replSet rs0` and `rs.initiate()`, then add `?replicaSet=rs0` to the URI. |
| Config error: `max_await_time_ms must be strictly less than idle_timeout` | `max_await_time_ms` is in **milliseconds** and must be `< idle_timeout` (which is in **seconds**). E.g. `idle_timeout: 30` (30 000 ms) with `max_await_time_ms: 1000` is valid. |
| `start_from: earliest` errors with an oplog/`resume of change stream was not possible` message | The oldest available oplog entry has rolled past. Use `{ type: now }` for fresh deployments, and size the oplog (and your tolerated downtime) so the window outlasts any gap between runs. |
| Resume fails after restart with a "resume point may no longer be in the oplog" error | The persisted `resumeToken` rolled off the oplog while the pipeline was down. Increase the oplog size, or restart with `start_from: { type: now }` (accepting the gap). |
| `before` is always `null` despite `full_document_before_change` set | Pre-images need MongoDB 6.0+ **and** `changeStreamPreAndPostImages: { enabled: true }` on the collection: `db.runCommand({ collMod: "mycoll", changeStreamPreAndPostImages: { enabled: true } })`. With `required` and pre-images off, the stream errors; with `when_available` you get `before: null`. |
| `after` reflects a newer state than the change | You're using `full_document: update_lookup`, which re-reads the document at lookup time. If the doc was further modified or deleted in between, `after` shows the later state (or is absent). Don't rely on it for a strict point-in-time image. |
| Pipeline exits cleanly but the source dropped a collection | An `invalidate` (collection/db dropped) arrives as a `ddl` event and **terminates the stream**. Re-run `faucet run`; the prior bookmark is no longer resumable, so set `start_from`. |
| Authorization errors opening the stream | The user needs `find` + `changeStream` on the namespace (cluster scope needs read across all databases). Check `authSource` in the URI matches where the user is defined. |
| No events appear even though documents are changing | Check `operation_types` isn't filtering them out, and that `scope` targets the right database/collection. A too-short `idle_timeout` ends the cycle before events arrive on a quiet collection — that's expected; the next run resumes. |
| `in-memory change buffer exceeded max_staged_records` | A high-throughput stream buffered more events than the configured `max_staged_records` cap before a page flushed — most likely with `batch_size: 0` (single-page drain). Raise `max_staged_records` to fit your available memory, or set a non-zero `batch_size` so pages flush more often. |

## Caveats

- **Replica set or sharded cluster required.** Standalone `mongod` instances do not support Change Streams; the connector validates this at startup and returns a typed `FaucetError::Source` immediately.
- **`full_document: update_lookup` has at-least-once / read-skew semantics.** The document is re-read from the primary at delivery time, not at change time. If the document was further modified or deleted in between, the `after` image reflects the later state (or is absent).
- **`start_from: earliest` may error** if the oplog has rolled past the earliest timestamp. Keep the oplog window large enough for your expected downtime.
- **DDL events are best-effort.** Collection drops/renames arrive as `ddl` records, but the change stream does not replicate index operations or `collMod` changes that don't appear in the oplog.
- **Per-batch durability, not per-event.** A crash mid-page replays at most `batch_size` events (unless effectively-once delivery is enabled).

## See also

- [State & resumability cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) — bookmarks, durable state stores, effectively-once.
- [Replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html) — the snapshot → CDC handoff with `faucet replicate`.
- [Upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) — `cdc_unwrap` + `write_mode: upsert` for a true CDC mirror.
- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full capability matrix.
- [`faucet-source-mongodb`](https://crates.io/crates/faucet-source-mongodb) — query-mode MongoDB source (`find()` snapshots).
- [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis) / [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres) — durable `StateStore` backends for resumeToken bookmarks.
- [`faucet-source-postgres-cdc`](https://crates.io/crates/faucet-source-postgres-cdc) / [`faucet-source-mysql-cdc`](https://crates.io/crates/faucet-source-mysql-cdc) — CDC sources for other databases.

## Source lag

`Source::lag` (#733) reports **seconds**: the cluster's `operationTime` minus the cluster time of the oldest change not yet delivered — near zero on an idle stream that has caught up. Before a run has read anything it opens one short-lived change stream after the stored resume token to find that change. The pipeline polls it on the first page, at most every
15 s after, and when the run ends, exporting `faucet_source_lag_*{pipeline,row,connector}`;
`faucet status --probe` and `faucet doctor` ask it from the stored bookmark,
and `sla.max_lag_*` thresholds turn it into an SLA. A failing lag query is
logged once and reported as no lag — it never fails a run.

## Bookmark schema

The stored bookmark is at **schema 1** (#736): `{ "resume_token": {…}, "invalidate": false }`,
with `invalidate` always written. Schema 0 (releases before versioned state)
omitted the flag when `false`; such a bookmark is migrated on the next run
(`MongoCdcSource::migrate_state`, pure) or ahead of it with
`faucet migrate --state`. A bookmark at a newer schema is refused rather than
misread. See [Upgrading faucet safely](https://faucet-hq.github.io/faucet-stream/operations/upgrading.html).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/faucet-hq/faucet-stream/blob/main/LICENSE-APACHE))
- MIT license ([LICENSE-MIT](https://github.com/faucet-hq/faucet-stream/blob/main/LICENSE-MIT))

at your option.
