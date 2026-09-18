# faucet-sink-iceberg

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-iceberg.svg)](https://crates.io/crates/faucet-sink-iceberg)
[![Docs.rs](https://docs.rs/faucet-sink-iceberg/badge.svg)](https://docs.rs/faucet-sink-iceberg)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-iceberg.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-iceberg.svg)](https://github.com/faucet-hq/faucet-stream#license)

Apache **Iceberg** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Buffers JSON records into Arrow batches, writes them as Parquet data files, and commits each batch as a new Iceberg snapshot via `Transaction::fast_append` — so any faucet-stream source can land directly in an Iceberg lakehouse.

Reach for it when you want a database query, a CDC stream, a CSV dump, or an API feed to become append-only Iceberg snapshots in your lake — with a pluggable catalog (REST, AWS Glue, SQL-backed, or Hive Metastore) and a local **or** cloud (S3/GCS) warehouse, all from one declarative config and no glue code.

## Feature highlights

- **Four catalog backends, one config shape** — REST (Polaris, Nessie, Tabular, …), AWS Glue, SQL-backed (Postgres/SQLite/…), and Hive Metastore, each selected by a Cargo feature and a single `catalog.type` discriminator.
- **Local & cloud warehouses** — REST resolves FileIO server-side; the SQL/Glue/HMS catalogs pick an OpenDAL-backed storage factory from the `warehouse` URI scheme (`file://`, `s3://`/`s3a://`, `gs://`).
- **Atomic snapshot commits** — each `flush()` writes the Parquet footer and registers the new data files in a single `fast_append` transaction. One `StreamPage` becomes exactly one snapshot.
- **Schema inference on create** — when `create_if_missing` is set and the table is new, the Iceberg schema is inferred from the first Arrow batch (every field becomes a nullable column typed by its first non-null value).
- **Partitioning on create** — `identity`, `year`, `month`, `day`, `hour`, `void`, plus parameterized `bucket[N]` / `truncate[N]`.
- **Parquet codec choice** — `snappy` (default), `zstd`, `gzip`, `lz4`, or `none`, with a soft `target_file_size_mb` rollover.
- **Effectively-once delivery** — pairs with the CDC sources; the commit token is durably recorded as Iceberg snapshot summary properties (`faucet.commit-scope` / `faucet.commit-token`) inside the same atomic commit.
- **`faucet doctor` preflight** — probes catalog connectivity and table existence without writing any data.
- **Secrets-safe `Debug`** — the catalog `credential` and `uri` are redacted, never logged.

## Installation

```bash
# As a library (REST catalog, the default):
cargo add faucet-sink-iceberg

# Add other catalogs by feature:
cargo add faucet-sink-iceberg --features catalog-glue
cargo add faucet-sink-iceberg --features catalog-glue,catalog-sql,catalog-hms

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-iceberg                    # REST only
cargo install faucet-cli --features sink-iceberg,sink-iceberg-glue  # REST + Glue
```

The umbrella crate mirrors the same shape — `sink-iceberg` enables the REST catalog; `sink-iceberg-glue` / `sink-iceberg-sql` / `sink-iceberg-hms` forward the catalog features onto the sink. Enabling any non-REST catalog automatically pulls in `storage-opendal` (S3/GCS/local warehouse support).

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: postgres-to-iceberg
pipeline:
  source:
    type: postgres
    config:
      connection_url: "postgres://faucet:faucet@localhost:5432/appdb"
      query: "SELECT * FROM users"
  sink:
    type: iceberg
    config:
      catalog:
        type: rest
        uri: "http://localhost:8181"
        warehouse: "s3://warehouse/"
      namespace: ["analytics"]
      table: "users"
      create_if_missing: true
      batch_size: 10000
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | `CatalogConfig` | — *(required)* | Catalog type + connection settings — see [Catalog config](#catalog-config). |
| `namespace` | `[string]` | — *(required)* | Multi-part namespace containing the target table, e.g. `["analytics", "events"]`. Must be non-empty; no segment may be empty. |
| `table` | string | — *(required)* | Table name within the namespace (no namespace prefix). |
| `create_if_missing` | bool | `true` | Create the table (inferring schema from the first batch) if it doesn't exist. When `false`, `new()` fails immediately if the table is absent. |
| `partition_spec` | `[PartitionField]` | `[]` | Partition fields used **only when creating** the table; ignored for existing tables — the table's own spec is used. See [Partitioning](#partitioning). |
| `write_mode` | `WriteMode` | `append` | Write semantics. **Only `append` is supported** — see [Write mode](#write-mode-append-only). |

### Format & files

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_file_size_mb` | int | `256` | Soft target for Parquet data-file size (MB). The rolling writer rolls a new file when the estimated in-memory size (uncompressed Arrow bytes × 0.4) exceeds this. Must be `> 0`. |
| `parquet.compression` | string | `"snappy"` | Parquet codec: `snappy`, `zstd`, `gzip`, `lz4`, or `none`. |
| `snapshot_properties` | map | `{}` | Key-value pairs written into the Iceberg snapshot summary on every commit. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `10000` | Records buffered in memory before each Arrow write pass. **`0` = no limit** (the entire upstream page is written in one batch). Validated against `MAX_BATCH_SIZE` (1,000,000). |
| `cleanup_orphans_on_failure` | bool | `false` | Delete the data files a flush already uploaded when the snapshot commit **definitively** fails, so they don't accumulate as orphans. Ambiguous failures are never cleaned up. See [Concurrent writers & orphaned files](#concurrent-writers--commit-conflict-retry). |

### Catalog config

The `catalog` block is an internally-tagged enum selected by `type`. Every variant carries the same inner fields; the relevant set differs per catalog:

| Field | Type | Description |
|-------|------|-------------|
| `type` | `rest` \| `glue` \| `sql` \| `hms` | Catalog backend (each gated by its own Cargo feature — see [Feature flags](#feature-flags)). |
| `uri` | string | Catalog endpoint. **Required** for `rest` (`https://catalog.example.com`), `sql` (the SQLx/JDBC connection string, e.g. `postgres://…`), and `hms` (`thrift://hms:9083`). For `glue` the endpoint is resolved from AWS config, so `uri` is **not** required. |
| `warehouse` | string | Object-storage warehouse root, e.g. `s3://lake/warehouse`. Required by the non-REST catalogs (its scheme selects the storage factory). |
| `credential` | string | REST bearer token (or other catalog-specific credential). **Redacted in `Debug`.** |
| `properties` | map | Arbitrary key-value pairs forwarded to the catalog/storage builder (S3/GCS options — see [Warehouse storage](#warehouse-storage)). |

#### `PartitionField`

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Source column name in the table schema. Must not be empty. |
| `transform` | string | One of: `identity`, `year`, `month`, `day`, `hour`, `void`, `bucket[N]`, `truncate[N]` (`N` a positive integer, e.g. `bucket[16]`). |

## Catalog support

| Catalog | `catalog.type` | Cargo feature | In default build? |
|---------|----------------|---------------|-------------------|
| REST (Polaris, Nessie, Tabular, …) | `rest` | `catalog-rest` | **yes** |
| AWS Glue | `glue` | `catalog-glue` | no |
| SQL-backed (Postgres, SQLite, …) | `sql` | `catalog-sql` | no |
| Hive Metastore | `hms` | `catalog-hms` | no |

Configuring a catalog type whose feature is not compiled in returns `FaucetError::Config` at startup.

### Warehouse storage

All catalogs support both cloud object stores and local filesystems. The **REST** catalog resolves FileIO server-side from the catalog config + `s3.*` properties. The **SQL / Glue / HMS** catalogs select an OpenDAL-backed storage factory from the `warehouse` URI scheme:

| Warehouse scheme | Storage |
|---|---|
| `file://…` / bare path | local filesystem |
| `s3://…` / `s3a://…` | Amazon S3 / S3-compatible (MinIO, …) |
| `gs://…` | Google Cloud Storage |

Cloud storage is enabled automatically when you enable a non-REST catalog feature (each of `catalog-glue` / `catalog-sql` / `catalog-hms` also enables `storage-opendal`). Pass storage credentials/options through `catalog.properties`:

- **S3:** `s3.region`, `s3.endpoint` (for S3-compatible stores), `s3.access-key-id`, `s3.secret-access-key`, `s3.path-style-access`, `s3.disable-config-load`.
- **GCS:** `gcs.credentials-json`, `gcs.no-auth`, `gcs.service.path`.

Schemes without a built-in factory (e.g. `oss://`, `abfss://`) are rejected at config-load time for the non-REST catalogs; use the REST catalog for those object stores.

## Examples

### SQL catalog (Postgres metadata) writing to an S3 warehouse

```yaml
sink:
  type: iceberg
  config:
    catalog:
      type: sql
      uri: "postgres://meta:meta@localhost:5432/iceberg"
      warehouse: "s3://lake/warehouse"
      properties:
        s3.region: "us-east-1"
        s3.access-key-id: "${env:AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${env:AWS_SECRET_ACCESS_KEY}"
    namespace: ["analytics"]
    table: "events"
    create_if_missing: true
```

### Partitioned table with zstd Parquet

```yaml
sink:
  type: iceberg
  config:
    catalog:
      type: rest
      uri: "http://localhost:8181"
      warehouse: "s3://warehouse/"
    namespace: ["analytics"]
    table: "events"
    create_if_missing: true
    partition_spec:
      - source: created_at
        transform: day
      - source: tenant_id
        transform: bucket[16]
    parquet:
      compression: zstd
    target_file_size_mb: 512
    batch_size: 50000
```

### AWS Glue catalog (endpoint resolved from AWS config)

```yaml
sink:
  type: iceberg
  config:
    catalog:
      type: glue
      warehouse: "s3://lake/warehouse"   # no uri — Glue uses the AWS default chain
      properties:
        s3.region: "eu-west-1"
    namespace: ["lake", "raw"]
    table: "page_views"
    create_if_missing: true
```

## Streaming and batching

The pipeline calls `Sink::write_batch` for each `StreamPage`, then `flush()` once the page carries a bookmark — so **each page becomes exactly one Iceberg snapshot**. `batch_size` controls the Arrow write granularity, not the snapshot granularity:

| `batch_size` | Meaning |
|--------------|---------|
| `10000` (default) | records buffered per Arrow write pass |
| `0` | "no batching" — write the entire upstream page in one Arrow batch |

For high-throughput pipelines, use a large **upstream** `batch_size` (e.g. `100000`) so the snapshot amortises catalog-commit overhead across many rows, and leave the sink's `batch_size` near its default so Arrow batches stay within memory limits.

`flush()` does two things in sequence: (1) closes the Parquet data file (writes the footer, uploads it), then (2) commits the snapshot via `Transaction::fast_append`.

### Concurrent writers & commit-conflict retry

Iceberg commits use optimistic concurrency. If a competing writer commits between this sink's table load and its commit, `Transaction::commit` (iceberg-rust 0.10.0) **transparently retries**: it reloads the table metadata and re-applies the `fast_append` against the latest snapshot — *without re-uploading the data files* — with exponential backoff. A benign concurrent write therefore does **not** abort the run. Tune the retry budget with the standard Iceberg `commit.retry.*` table properties, set via `snapshot_properties` at table creation:

```yaml
snapshot_properties:
  commit.retry.num-retries: "8"        # default 4
  commit.retry.min-wait-ms: "100"
  commit.retry.max-wait-ms: "60000"
  commit.retry.total-timeout-ms: "1800000"
```

### Orphaned data files on a definitive commit failure

If the commit *definitively* fails after those retries are exhausted (a competing writer won), the data files from step 1 are already in storage but referenced by no snapshot — they are **orphaned**. The error propagates, the run aborts, and the bookmark does not advance; the re-run writes fresh files and commits them.

By default the sink leaves orphans in place — reclaim them with Iceberg's standard `remove_orphan_files` maintenance. Set **`cleanup_orphans_on_failure: true`** to delete them automatically:

```yaml
cleanup_orphans_on_failure: true   # default false
```

Cleanup runs **only** on a *definitive* loss (an exhausted commit conflict, or a catalog-rejected commit). An **ambiguous** failure — e.g. a network error on the catalog update where the commit may have landed server-side — is **never** cleaned up regardless of this flag, because deleting then could remove files a successful-but-unacknowledged commit references. The data files this sink writes have unique (UUID-based) names, so cleanup can never remove a file a concurrent writer references.

## Write mode (append-only)

`write_mode` accepts the shared `faucet_core::WriteMode` enum, but **only `append` is supported at runtime**. `upsert` / `delete` deserialise successfully (so configs round-trip) but are rejected by `IcebergSink::new` with a typed `FaucetError::Config`; `overwrite` is not a recognised variant and fails to deserialise. `Sink::supported_write_modes()` therefore returns `[Append]`. Equality-delete upsert is tracked in [#179](https://github.com/faucet-hq/faucet-stream/issues/179), blocked on upstream `iceberg-rust` exposing a replace/overwrite transaction action.

## Effectively-once delivery

`IcebergSink` implements `Sink::supports_idempotent_writes()` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and stashes the `(scope, token)` so it lands in the snapshot summary as `faucet.commit-scope` / `faucet.commit-token` inside the same atomic `fast_append`. Records and token commit together or not at all.
- `last_committed_token(scope)` — scans snapshot history newest-first for a matching `faucet.commit-scope`, letting the pipeline skip already-committed pages on resume.

To enable it, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is **not** permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: iceberg
    config:
      catalog:
        type: rest
        uri: http://catalog.example.com
        warehouse: s3://lake/warehouse
      namespace: ["analytics"]
      table: change_events
      create_if_missing: true
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [Effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) for the full rationale and supported source/sink set.

## Schema drift

`IcebergSink` reports its live table schema via `current_schema()` (the table's current Iceberg schema converted through Arrow to the `infer_schema` JSON shape; a missing table → `None`), so the pipeline-level `schema:` policy can **detect** drift between an incoming page's top-level shape and the real table. The `warn` / `ignore` / `quarantine` / `fail` modes all work against this sink.

**`on_drift: evolve` supports additive columns** ([#255](https://github.com/faucet-hq/faucet-stream/issues/255)). `supports_schema_evolution()` returns `true`: on drift, each **new** column is added as an optional field via `iceberg-rust` 0.10.0's `Transaction::update_schema` action, in a single schema-update commit. Only additions are applied — iceberg-rust 0.10.0's `update_schema` exposes `add_column` (+ `delete_column`) but **no** in-place type promotion or nullability relaxation, so a `widenings` / `relax_nullability` evolution is rejected with a typed error rather than silently ignored. Row-level overwrite/upsert remain separately blocked upstream (#179 / #225). See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Schema inference

When `create_if_missing: true` and the table is new, the Iceberg schema is inferred from the first Arrow batch: every JSON field becomes a nullable column, typed by its first non-null value. Subsequent batches use the table's existing schema so the writer and table stay in sync. Iceberg assigns **field IDs** sequentially from `1`; IDs are stable once the table exists, so renaming or reordering JSON keys in later runs does not change them.

## Config loading & schema

Configs load from YAML/JSON (or env). Inspect the full JSON Schema with:

```bash
faucet schema sink iceberg
```

`faucet doctor` runs the sink's preflight `check()` — it builds the namespace/table ident and probes catalog connectivity + table existence (bounded by the probe timeout) **without writing any data**; a catalog connection failure or timeout surfaces as a red probe.

## Library usage

```rust
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// Configs are normally loaded from YAML/JSON; deserialize one here.
let config: IcebergSinkConfig = serde_json::from_value(json!({
    "catalog": {
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/"
    },
    "namespace": ["analytics"],
    "table": "events",
    "create_if_missing": true,
    "batch_size": 10000
}))?;

let sink = IcebergSink::new(config).await?;

sink.write_batch(&[
    json!({"user_id": "u123", "event": "page_view", "ts": "2026-01-02T10:00:00Z"}),
    json!({"user_id": "u456", "event": "click",     "ts": "2026-01-02T10:01:00Z"}),
]).await?;

// Required: writes the Parquet footer and commits the Iceberg snapshot.
sink.flush().await?;
# Ok(())
# }
```

## How it works

1. `new()` validates the config, builds the configured catalog client, and either creates the table (inferring schema from the first batch) or loads an existing one.
2. `write_batch` shovels JSON → Arrow, buffers into the iceberg-rust rolling writer, and rolls a new Parquet data file when the estimated size crosses `target_file_size_mb`.
3. `flush()` closes the open data file and commits all buffered files as one snapshot via `Transaction::fast_append`; the catalog client is reused across all calls.
4. In effectively-once mode the pending `(scope, token)` is merged into that snapshot's summary properties so it commits atomically with the data.

**Arrow / Parquet version note:** this crate links Arrow / Parquet **57** to match `iceberg-rust` 0.9.x, which pins the same major. This does not affect the workspace's other connectors — the Parquet source/sink use Arrow 58, and Cargo resolves both majors simultaneously.

## Lineage dataset URI

`iceberg://<catalog_type>/<namespace>.<table>` — e.g. `iceberg://rest/analytics.events` (catalog type is `rest`, `glue`, `sql`, or `hms`).

## Feature flags

| Feature | Default? | Enables |
|---------|----------|---------|
| `catalog-rest` | **yes** | REST catalog (`iceberg-catalog-rest`). |
| `catalog-glue` | no | AWS Glue catalog (also enables `storage-opendal`). |
| `catalog-sql` | no | SQL-backed catalog (also enables `storage-opendal`). |
| `catalog-hms` | no | Hive Metastore catalog (also enables `storage-opendal`). |
| `storage-opendal` | no (auto) | OpenDAL S3/GCS/local warehouse storage factory for the non-REST catalogs. Auto-enabled by each non-REST catalog feature. |

In the CLI/umbrella, the corresponding feature is `sink-iceberg` (REST), with `sink-iceberg-glue` / `sink-iceberg-sql` / `sink-iceberg-hms` forwarding the catalog features.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: catalog '…' requires a non-empty uri` | `rest` / `sql` / `hms` catalogs need a `uri`. Set it (Glue is the only catalog that resolves its endpoint from AWS config). |
| `FaucetError::Config: warehouse scheme '…://' is not supported` | A non-REST catalog got an unsupported warehouse scheme. Use `file://`, `s3://`, `s3a://`, or `gs://` — or switch to the REST catalog, which accepts any scheme (it resolves FileIO server-side). |
| `unknown variant 'overwrite'` / `write_mode … rejected` | Only `append` is supported. `upsert`/`delete` parse but are rejected at `new()`; `overwrite` is not a variant. See [Write mode](#write-mode-append-only). |
| Catalog type fails at startup despite a valid config | The catalog's Cargo feature isn't compiled in. Rebuild with `--features catalog-glue` (or `-sql` / `-hms`) — `catalog-rest` is the only one in the default build. |
| `faucet doctor` catalog probe times out / fails | Verify the catalog URI, credentials, and network reachability. The probe calls `table_exists` and is bounded by `--timeout-secs`. |
| Data files exist in object storage but no snapshot references them | A commit failed *definitively* after the Parquet file was uploaded (orphaned files). Re-run to write fresh files. Reclaim orphans with Iceberg's `remove_orphan_files` maintenance, or set `cleanup_orphans_on_failure: true` to delete them automatically on a definitive failure (ambiguous failures are never auto-deleted). |
| `partition_spec[…].transform … is not a recognised Iceberg transform` | Use one of `identity`/`year`/`month`/`day`/`hour`/`void` or `bucket[N]`/`truncate[N]` with a positive `N`. |
| `target_file_size_mb must be > 0` | `0` would roll a tiny file per batch. Use a positive MB target (default `256`). |
| Partition spec seems ignored | `partition_spec` only applies when **creating** a new table (`create_if_missing: true`). An existing table keeps its own spec. |
| Effectively-once config rejected by `faucet validate` | `delivery: exactly_once` requires a CDC source + a `state:` block + **no** DLQ. Fix whichever requirement the error names. |

## See also

- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery)
- [Configuration grammar](https://faucet-hq.github.io/faucet-stream/reference/config.html)
- Related crates: [`faucet-sink-parquet`](https://crates.io/crates/faucet-sink-parquet), [`faucet-sink-s3`](https://crates.io/crates/faucet-sink-s3), [`faucet-sink-bigquery`](https://crates.io/crates/faucet-sink-bigquery), [`faucet-source-postgres-cdc`](https://crates.io/crates/faucet-source-postgres-cdc)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
