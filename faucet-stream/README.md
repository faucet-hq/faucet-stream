# faucet-stream

[![Crates.io](https://img.shields.io/crates/v/faucet-stream.svg)](https://crates.io/crates/faucet-stream)
[![Docs.rs](https://docs.rs/faucet-stream/badge.svg)](https://docs.rs/faucet-stream)
[![MSRV](https://img.shields.io/crates/msrv/faucet-stream.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-stream.svg)](https://github.com/faucet-hq/faucet-stream#license)

The umbrella crate for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem — a modular, config-driven Rust data-pipeline toolkit. One dependency gives you feature-gated access to every source and sink connector, the state-store backends, shared auth providers, data-quality checks, OpenLineage emission, and the embedded SQL transform — all built on the lightweight `faucet-core` traits.

This crate is for **Rust library users** who want to build pipelines in code. If you'd rather drive pipelines declaratively from YAML/JSON with zero Rust, install the [`faucet-cli`](https://crates.io/crates/faucet-cli) binary instead — it wraps these same connectors behind a `faucet run pipeline.yaml` command.

## Why the umbrella vs. individual crates?

- **One dependency, one version line.** `faucet-stream` pins every connector to a compatible version so you never hand-align `faucet-source-rest`, `faucet-sink-postgres`, etc. Add the features you want and the right crates come along.
- **Pay only for what you enable.** Every connector is behind an opt-in Cargo feature. The default build pulls in just the REST source, so an unused connector never touches your compile time or binary size.
- **Uniform re-exports.** Connector types surface under one `faucet_stream::*` namespace, alongside the full `faucet-core` API (`Source`, `Sink`, `Pipeline`, `run_stream`, `FaucetError`, transforms).

Prefer depending on the individual crates (`faucet-core` + `faucet-source-rest` + `faucet-sink-postgres`, …) when you want the absolute minimum dependency closure or are publishing your own connector crate — see [Using individual crates](#using-individual-crates) below.

## Installation

```bash
# Default: REST source only (smallest build)
cargo add faucet-stream

# Pick exactly the connectors you need
cargo add faucet-stream --features source-rest,source-s3,sink-postgres,sink-file

# All sources / all sinks
cargo add faucet-stream --features source
cargo add faucet-stream --features sink

# Everything — every connector, state backend, auth, quality, lineage, SQL transform
cargo add faucet-stream --features full
```

> The default feature set is `["source-rest"]`. Every other connector and capability below is **opt-in**.

## Feature-flag matrix

### Source connectors

| Feature | Default | Description |
|---------|---------|-------------|
| `source-rest` | **yes** | REST API — auth, pagination, JSONPath extraction, transforms |
| `source-graphql` | no | GraphQL API — cursor pagination, variable injection |
| `source-xml` | no | XML/SOAP API — XML-to-JSON conversion, dot-path extraction |
| `source-grpc` | no | gRPC — dynamic protobuf via prost-reflect (unary + server-streaming) |
| `source-postgres` | no | PostgreSQL — run SQL, return rows as JSON |
| `source-postgres-cdc` | no | PostgreSQL CDC — logical replication (pgoutput), resumable |
| `source-mysql` | no | MySQL — SQL queries as JSON |
| `source-mysql-cdc` | no | MySQL CDC — binlog replication, resumable |
| `source-mssql` | no | Microsoft SQL Server — streaming, incremental replication |
| `source-sqlite` | no | SQLite — SQL queries as JSON |
| `source-s3` | no | AWS S3 — JSONL, JSON array, or raw text |
| `source-gcs` | no | Google Cloud Storage — JSONL, JSON array, or raw text |
| `source-mongodb` | no | MongoDB — `find()` with filter / projection / sort |
| `source-mongodb-cdc` | no | MongoDB CDC — Change Streams, resumable via resumeToken |
| `source-redis` | no | Redis — streams, lists, or key patterns |
| `source-webhook` | no | Webhook — temporary HTTP server collecting POSTs |
| `source-websocket` | no | WebSocket — live streaming, subscription frames, reconnect |
| `source-csv` | no | Deprecated (use `source-file`): the old `faucet-source-csv` crate, unchanged until the next major release |
| `source-elasticsearch` | no | Elasticsearch — search / scroll API |
| `source-parquet` | no | Deprecated (use `source-file`, or `source-s3` for S3): the old `faucet-source-parquet` crate, unchanged until the next major release |
| `source-kafka` | no | Kafka consumer — subscribe, drain with idle / max-message termination |
| `source-bigquery` | no | BigQuery query source — `jobs.query` + pagination |
| `source-snowflake` | no | Snowflake query source — SQL REST API, JWT / OAuth |

### Sink connectors

| Feature | Default | Description |
|---------|---------|-------------|
| `sink-bigquery` | no | Google BigQuery — streaming inserts; upsert/delete via MERGE |
| `sink-iceberg` | no | Apache Iceberg — append snapshots (REST / Glue / SQL / HMS catalogs) |
| `sink-postgres` | no | PostgreSQL — JSONB or auto-mapped columns |
| `sink-mysql` | no | MySQL — JSON column or auto-mapped columns |
| `sink-mssql` | no | Microsoft SQL Server — JSON or auto-mapped columns |
| `sink-sqlite` | no | SQLite — JSON or auto-mapped columns |
| `sink-snowflake` | no | Snowflake — SQL REST API |
| `sink-mongodb` | no | MongoDB — `insert_many` / upsert |
| `sink-redis` | no | Redis — streams, lists, key-value |
| `sink-elasticsearch` | no | Elasticsearch — bulk index API |
| `sink-s3` | no | AWS S3 — JSONL files |
| `sink-gcs` | no | Google Cloud Storage — JSONL files |
| `sink-parquet` | no | Deprecated (use `sink-file`, or `sink-s3` for S3): the old `faucet-sink-parquet` crate, unchanged until the next major release |
| `sink-kafka` | no | Kafka producer — batched sends, multi-topic routing |
| `sink-csv` | no | Deprecated (use `sink-file`): the old `faucet-sink-csv` crate, unchanged until the next major release |
| `sink-jsonl` | no | Deprecated (use `sink-file`): the old `faucet-sink-jsonl` crate, unchanged until the next major release |
| `sink-http` | no | HTTP — POST records to any endpoint |
| `sink-stdout` | no | Stdout/stderr — JSON Lines, pretty JSON, or TSV |

### Iceberg catalog add-ons

| Feature | Description |
|---------|-------------|
| `sink-iceberg-glue` | Enables the AWS Glue catalog on the Iceberg sink |
| `sink-iceberg-sql` | Enables the SQL catalog on the Iceberg sink |
| `sink-iceberg-hms` | Enables the Hive Metastore catalog on the Iceberg sink |

### State-store backends

The `memory` and `file` state stores are always available via `faucet-core`. These add durable backends for replication bookmarks / effectively-once commit tokens:

| Feature | Description |
|---------|-------------|
| `state-redis` | Redis-backed `StateStore` |
| `state-postgres` | PostgreSQL-backed `StateStore` |

### Capabilities

| Feature | Description |
|---------|-------------|
| `auth` | Shared single-flight auth providers (`faucet-auth`) — OAuth2 client-credentials / refresh, token-endpoint — reusable across connectors |
| `quality` | Data-quality checks (the `quality:` config block) — per-record and per-batch |
| `quality-jsonschema` | Adds the `json_schema` record check (implies `quality`) |
| `contract` | Data contracts (the `contract:` config block) — a versioned output schema/constraint promise enforced per page (fail / quarantine / warn) |
| `lineage` | OpenLineage emission (the `lineage:` block) — HTTP + file transports |
| `lineage-kafka` | Adds the Kafka lineage transport (implies `lineage`) |
| `transform-sql` | SQL-as-transform via embedded DuckDB — each page is a `batch` relation |
| `compression` | gzip/zstd on every opted-in file-shaped connector (see below) |
| `kafka-schema-registry` | Confluent Schema Registry support (Avro / Protobuf / JSON Schema) for the Kafka pair |

> `compression` uses optional-dependency forwarding (`faucet-source-csv?/compression`, …): it turns on gzip/zstd **only** for the object-store and deprecated file connectors you have already enabled (`source-s3`, `source-gcs`, `source-azure-blob`, `sink-s3`, `sink-gcs`, `sink-azure-blob`, `sink-sftp`, `source-csv`, `sink-jsonl`, `sink-csv`). `source-file` / `sink-file` always support compression. It does not pull connectors you haven't requested.

### Transforms

Transforms reshape records mid-pipeline. Enabling `source-rest` (the default) already turns on `transform-flatten`, `transform-rename-keys`, and `transform-keys-case`.

| Feature | Description |
|---------|-------------|
| `transform-flatten` | Flatten nested objects |
| `transform-rename-keys` | Regex key renaming |
| `transform-keys-case` | Re-case every key (snake / camel / pascal / kebab / screaming_snake / dot) |
| `transform-select` | Keep only listed top-level fields |
| `transform-drop` | Remove listed top-level fields |
| `transform-set` | Add/overwrite top-level fields with constants |
| `transform-rename-field` | Exact-name field rename (single or batch) |
| `transform-cast` | Per-field type coercion with configurable `on_error` |
| `transform-redact` | Replace listed field values with a mask |
| `transform-value-case` | Lowercase / uppercase / trim string field values |
| `transform-spell-symbols` | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) |
| `transform-filter` | Drop records that don't match a predicate (1→0\|1) |
| `transform-explode` | Fan one record into N from an array field (1→0..N) |
| `transform-cdc-unwrap` | Normalize a CDC envelope into a flat row + `__op` marker (the standard pairing for upsert sinks) |
| `transforms` | All built-in transforms (includes `transform-cdc-unwrap`) |

### Aggregate features

| Feature | Pulls in |
|---------|----------|
| `source` | Every source connector |
| `sink` | Every sink connector |
| `state` | Every state-store backend (`state-redis`, `state-postgres`) |
| `auth` | Shared auth providers |
| `full` | Everything — all sources, sinks, state backends, `auth`, `kafka-schema-registry`, `compression`, `quality`, `quality-jsonschema`, `lineage`, `lineage-kafka`, `transform-sql`, `transform-cdc-unwrap` |

## Pick your connectors

Common combinations — enable only what your pipeline touches:

```bash
# REST API → PostgreSQL ELT
cargo add faucet-stream --features source-rest,sink-postgres

# Data-lake landing: REST → S3 with gzip
cargo add faucet-stream --features source-rest,sink-s3,compression

# S3 → BigQuery warehouse load
cargo add faucet-stream --features source-s3,sink-bigquery

# Postgres CDC → Postgres mirror with effectively-once delivery + durable state
cargo add faucet-stream --features source-postgres-cdc,sink-postgres,state-postgres,transform-cdc-unwrap

# Kafka with Schema Registry → Parquet on S3
cargo add faucet-stream --features source-kafka,sink-s3,file-format-parquet,kafka-schema-registry

# Everything, for prototyping
cargo add faucet-stream --features full
```

## Library usage

The whole `faucet-core` API is re-exported unconditionally; connector types appear when their feature is enabled.

```rust
use faucet_stream::{Pipeline, run_stream};
use faucet_source_rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};
use faucet_sink_file::{FileSink, FileSinkConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build a source: paginated REST API with bearer auth
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .auth(Auth::Bearer { token: "my-token".into() })
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            }),
    )?;

    // Build a sink
    let sink = FileSink::new(FileSinkConfig::new("users.jsonl"))?;

    // Batch mode: fetch all, then write
    let result = Pipeline::new(&source, &sink).run().await?;
    println!("Wrote {} records", result.records_written);

    // Streaming mode: write each page as it arrives (memory bounded at O(batch_size))
    let _ = run_stream(source.stream_pages(), &sink).await?;
    Ok(())
}
```

### What's re-exported

Always available (from `faucet-core`):

- `Source`, `Sink` traits; `Pipeline`, `PipelineResult`, `run_stream`
- `FaucetError`; `RecordTransform`, `ReplicationMethod`
- `config::load_json`, `config::load_env`, `config::load_env_file`
- the connector-author re-exports — `async_trait`, `serde_json`, `Value`, `json!`, `JsonSchema`, `schema_for!`

Plus the config + connector types from each enabled feature (e.g. `RestStream` / `RestStreamConfig` / `Auth` / `PaginationStyle` under `source-rest`). For connector-specific config fields, auth variants, and capability details, see each connector crate's own README and the docs site.

## Examples

Runnable examples live in [`examples/`](examples/); each declares its required features at the top of the file. The `rest_to_jsonl` and `rest_streaming` examples run end-to-end against a public test API; the rest are compile-only source→sink pairings that need real infrastructure to run.

```bash
cargo run -p faucet-stream --example rest_to_jsonl --features "source-rest sink-file"
cargo run -p faucet-stream --example postgres_to_bigquery --features "source-postgres sink-bigquery"
```

## Using individual crates

You can depend on the connector crates directly instead of the umbrella for the tightest dependency closure or finer compile-time control:

```bash
cargo add faucet-core
cargo add faucet-source-rest
cargo add faucet-sink-postgres
```

If you are **building your own connector**, depend only on `faucet-core` — it is the single required dependency and re-exports everything a `faucet-source-*` / `faucet-sink-*` crate needs.

## Troubleshooting / FAQ

| Symptom | Cause & fix |
|---------|-------------|
| `cannot find type RestStream` (or any connector type) | The connector's feature isn't enabled. Add it, e.g. `cargo add faucet-stream --features source-rest`. The default build only enables `source-rest`. |
| `transform-select`/`transform-cast`/etc. has no effect | The built-in transforms are forwarded to the REST source. Enable the specific `transform-*` feature (or `transforms` for all). The default build includes only `transform-flatten`, `transform-rename-keys`, `transform-keys-case`. |
| Compression flag enabled but files aren't compressed | `compression` only activates for the object-store / deprecated file connectors you've **also** enabled (`source-s3`, `source-gcs`, `source-azure-blob`, `sink-s3`, `sink-gcs`, `sink-azure-blob`, `sink-sftp`, `source-csv`, `sink-jsonl`, `sink-csv`); `file` always has it. Confirm both features are on, and the path suffix is `.gz`/`.zst` (under `Auto`). |
| Kafka Avro/Protobuf decode fails | Schema-Registry formats need `kafka-schema-registry` in addition to `source-kafka`/`sink-kafka`. |
| `auth: { ref }` errors / shared OAuth provider not found | The shared-auth catalog needs the `auth` feature (`faucet-auth`). Library callers build providers directly and pass them via `Source::with_auth_provider`. |
| Iceberg catalog (Glue/SQL/HMS) not recognized | Enable the matching add-on: `sink-iceberg-glue`, `sink-iceberg-sql`, or `sink-iceberg-hms`. The bare `sink-iceberg` only ships the REST catalog. |
| Effectively-once or CDC bookmarks lost across restarts | Configure a durable state backend (`state-postgres` / `state-redis`); the in-memory store does not survive a restart. Effectively-once delivery also requires a CDC source + an idempotent sink (sqlite/postgres/mysql/mssql/iceberg/bigquery). |
| Long compile times / huge binary | Don't use `full`. Enable only the connectors your pipeline uses. |

## See also

- [Getting started](https://faucet-hq.github.io/faucet-stream/getting-started/concepts.html) and the [library tutorial](https://faucet-hq.github.io/faucet-stream/tutorials/library.html)
- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Choosing connectors](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- [`faucet-core`](https://crates.io/crates/faucet-core) — the traits + pipeline engine (the only dependency connector authors need)
- [`faucet-cli`](https://crates.io/crates/faucet-cli) — drive these connectors from YAML/JSON with no Rust code

## License

Licensed under either of [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE) at your option.
