# faucet-source-postgres

[![Crates.io](https://img.shields.io/crates/v/faucet-source-postgres.svg)](https://crates.io/crates/faucet-source-postgres)
[![Docs.rs](https://docs.rs/faucet-source-postgres/badge.svg)](https://docs.rs/faucet-source-postgres)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-postgres.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-postgres.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **PostgreSQL query source** for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs any SQL `SELECT` against a PostgreSQL database and streams the result set back row-by-row as `serde_json::Value` records.

Built on [`sqlx`](https://crates.io/crates/sqlx) with a reusable connection pool and a true row cursor (`Query::fetch`), so the source never buffers the whole result set in memory and the downstream sink starts writing as soon as the first batch is parsed off the wire. Reach for it to extract tables, joins, aggregates, or ad-hoc query results out of Postgres and land them in any faucet-stream sink — a file, an object store, a warehouse, a queue — with one declarative config and no glue code.

## Feature highlights

- **Streaming row cursor** — `stream_pages` drives a `sqlx` cursor and yields `batch_size`-sized pages; peak client memory is `O(batch_size)`, independent of total row count.
- **Connection pooling** — a `PgPoolOptions` pool sized by `max_connections` is built once in `new()` and reused for every fetch.
- **Type-aware row decoding** — integers, floats, booleans, `text`, `json`/`jsonb`, `timestamp(tz)`, `date`/`time`, `uuid`, `numeric`/`decimal`, and `bytea` are each converted to the right JSON shape (full numeric precision preserved as strings; binary base64-encoded).
- **Positional bind parameters** — `params` from config are bound as native scalar types (`$1`, `$2`, …), so a numeric or boolean bind compares correctly against a typed column instead of being coerced to `jsonb`.
- **Matrix-context binding** — `${parent.path}` tokens in the query are rewritten to additional positional bind markers and filled per parent record, so the same query template runs once per row produced by a parent in a [matrix](https://faucet-hq.github.io/faucet-stream/reference/config.html) pipeline.
- **Credential redaction** — the connection URL is masked in `Debug` output and stripped from the emitted lineage dataset URI.

## Installation

```bash
# As a library:
cargo add faucet-source-postgres

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-postgres
```

`source-postgres` is an opt-in feature — it is not in the CLI default build. Enable it (or the umbrella `source` aggregate) when you need this connector.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost:5432/app
      query: SELECT id, name, email FROM users WHERE active = true
  sink:
    type: jsonl
    config:
      path: ./users.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

All fields live under `pipeline.source.config`.

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | PostgreSQL connection URL (`postgres://user:pass@host:5432/db`). Masked in `Debug` output and stripped from the lineage URI. |
| `query` | string | — *(required)* | SQL query to execute. May contain positional placeholders `$1`, `$2`, … and `${parent.path}` matrix-context tokens. |
| `params` | array | `[]` | Bind parameters for the query, in positional order. Each value is bound as its native scalar type (string / integer / float / bool / null). Integer params bind exactly — any JSON integer up to `u64::MAX` round-trips without the precision loss an `f64` cast would cause, so large 64-bit ids compare correctly. |

### Reliability & batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_connections` | int | `10` | Maximum connections in the `sqlx` pool. |
| `batch_size` | int | `1000` | Rows per `StreamPage`. **`0` = no batching** — the cursor is fully drained and the entire result set is emitted in a single page (see [Streaming & batching](#streaming--batching)). Values above `MAX_BATCH_SIZE` (1,000,000) are rejected at construction. |
| `shard` | object | *(unset)* | Optional [Mode B sharding](#sharded-execution-cluster-mode-b): `{ key: <integer column> }`. Opts the source into primary-key range splitting under `faucet serve --cluster`; no effect on a plain `faucet run`. |

## Examples

### Parameterized query

Bind a literal value into the query. Parameters are positional (`$1` maps to `params[0]`):

```yaml
version: 1
name: shipped_orders
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost:5432/app
      query: SELECT * FROM orders WHERE status = $1 AND total > $2
      params:
        - shipped
        - 100.0
  sink:
    type: jsonl
    config:
      path: ./shipped_orders.jsonl
```

### Archive a large table to S3, tuned pool and page size

```yaml
version: 1
name: postgres_to_s3
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost:5432/app
      query: SELECT * FROM events WHERE created_at < $1
      params:
        - "2026-01-01T00:00:00Z"
      max_connections: 12
      batch_size: 5000
  sink:
    type: s3
    config:
      bucket: my-archive-bucket
      prefix: events/2025/
      region: us-east-1
      file_extension: .jsonl
      max_records_per_file: 50000
      concurrency: 16
```

### Small lookup table, single page (load-job friendly)

Set `batch_size: 0` to emit the whole result set as one page — ideal feeding a sink that prefers one large request (SQL `COPY`, a BigQuery load job, a Snowflake stage upload):

```yaml
version: 1
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost:5432/app
      query: SELECT code, label FROM country_codes
      batch_size: 0
  sink:
    type: bigquery
    config:
      project_id: my-project
      dataset_id: ref
      table_id: country_codes
```

### Secrets-manager interpolation

Keep the credentials out of the config file — the CLI resolves `${vault:…}` / `${aws-sm:…}` / `${gcp-sm:…}` / `${azure-kv:…}` at load time when built with the matching `secrets-*` feature:

```yaml
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${vault:secret/data/warehouse#dsn}
      query: SELECT * FROM customers
```

## Streaming & batching

`PostgresSource::stream_pages` drives a `sqlx` row cursor (`Query::fetch`) without buffering the full result. Rows are accumulated into a `batch_size` buffer and yielded as a `StreamPage` once the buffer fills; the trailing partial page (if any) is yielded after the cursor drains. The pipeline writes each page to the sink as it arrives, so memory is bounded by `O(batch_size)` on both ends.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained completely and the entire result set is emitted in a single `StreamPage`. Use it for small lookup tables, or for downstream sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage uploads) that prefer one large request to many small ones.

The trait-level `batch_size` argument to `stream_pages` is informational; the source always uses its own config field as the authoritative knob, so a pipeline-supplied hint cannot silently override an explicit config value.

This is a **query source with no incremental-replication mode** — it runs the configured query once and streams the result. Every emitted page therefore carries `bookmark: None`; there is no resume/state bookmark, no effectively-once delivery, and no upsert/write modes (those are sink concerns). For change data capture, see [`faucet-source-postgres-cdc`](https://crates.io/crates/faucet-source-postgres-cdc), which captures logical-replication changes and is resumable via a state store.

> **Note** — Postgres' wire protocol sends rows from a simple `SELECT` in a single response (no server-side cursor by default). The streaming implementation bounds *client-side* memory at `O(batch_size)` and lets the sink begin writing as soon as the first batch is parsed off the wire.

## Supported column types

Columns are converted to JSON values by probing the row's value with each candidate Rust type in turn:

| PostgreSQL type | JSON shape |
|-----------------|------------|
| `json`, `jsonb` | native JSON value |
| `text`, `varchar`, `char` | string |
| `int8` / `bigint` | number (i64) |
| `int4` / `integer` | number (i32) |
| `int2` / `smallint` | number (i16) |
| `float8` / `double precision` | number (f64) |
| `float4` / `real` | number (f32) |
| `bool` / `boolean` | boolean |
| `timestamptz` | string (RFC 3339) |
| `timestamp`, `date`, `time` | string (ISO-8601) |
| `uuid` | string (canonical hyphenated) |
| `numeric`, `decimal` | string (exact precision preserved) |
| `bytea` | string (base64) |
| other / `NULL` | `null` |

## Matrix-context binding

In a parent/child [matrix](https://faucet-hq.github.io/faucet-stream/reference/config.html) pipeline, the query may reference fields from each parent record with `${parent_id.dotted.path}` tokens. At runtime these are rewritten to additional positional bind markers (appended after the static `params`) and filled per parent record, so a single query template fans out into one parameterized execution per parent row — never string-interpolated, so it is SQL-injection safe.

```yaml
matrix:
  - id: parent
    source: { ref: tenants }              # produces rows with a `tenant_id`
  - id: child
    parent: parent
    source:
      type: postgres
      config:
        connection_url: postgres://user:pass@localhost:5432/app
        query: SELECT * FROM orders WHERE tenant_id = ${parent.tenant_id}
```

## Config loading

Load a config from JSON, env vars, or a `.env` file via the helpers in `faucet_core::config`:

```rust,no_run
use faucet_core::config::{load_json, load_env_file};
use faucet_source_postgres::PostgresSourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config: PostgresSourceConfig = load_json("config.json")?;
let config: PostgresSourceConfig = load_env_file(".env", "PG_SOURCE")?;
# Ok(()) }
```

```json
{
  "connection_url": "postgres://analytics:password@db.example.com:5432/warehouse",
  "query": "SELECT id, name, created_at, metadata FROM events WHERE created_at > $1 ORDER BY created_at",
  "params": ["2025-01-01T00:00:00Z"],
  "max_connections": 5,
  "batch_size": 5000
}
```

```env
PG_SOURCE_CONNECTION_URL=postgres://user:password@localhost:5432/mydb
PG_SOURCE_QUERY=SELECT * FROM users
PG_SOURCE_MAX_CONNECTIONS=10
```

## Schema introspection

Print the JSON Schema for this source's config (drives `faucet validate` and `faucet init`):

```bash
faucet schema source postgres
```

## Library usage

```rust,no_run
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use faucet_core::Source;
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresSourceConfig::new(
    "postgres://user:pass@localhost:5432/app",
    "SELECT * FROM orders WHERE status = $1 AND total > $2",
)
.params(vec![json!("shipped"), json!(100.0)])
.with_max_connections(20)
.with_batch_size(5000);

let source = PostgresSource::new(config).await?;

// Drain the whole result set:
let orders = source.fetch_all().await?;
for order in &orders {
    println!("{order}");
}
# Ok(()) }
```

To wire it into a streaming pipeline, hand the source to `faucet_core::Pipeline` (or `run_stream`) together with any sink — each `StreamPage` is written as it arrives.

## How it works

- **Pool reuse** — `PostgresSource::new` validates `batch_size`, then builds one `PgPool` (`PgPoolOptions::max_connections(...)`) and stores it on the struct. Every fetch borrows a connection from the pool; nothing is reconnected per call.
- **True cursor streaming** — `stream_pages` calls `Query::fetch` (a `sqlx` cursor) and pulls rows with `try_next()`, converting each `PgRow` to a JSON object keyed by column name. Rows are batched into a reusable buffer and the buffer is swapped (`mem::replace`) on each yield to avoid reallocation churn.
- **Typed binding** — config and context parameters are bound as native scalars (string / i64 / f64 / bool / null), never as raw `jsonb`. Binding a `serde_json::Value` directly would encode it as `jsonb` and break comparisons against typed columns (e.g. `WHERE id = $1` against an integer column).
- **Errors** — connection and query failures surface as `FaucetError::Config` with the underlying `sqlx` message; an out-of-range `batch_size` is rejected at construction.

## Lineage dataset URI

`postgres://<host>:<port>/<db>?query=<sql>` (credentials stripped) — e.g. `postgres://host:5432/app?query=SELECT id FROM orders`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI or umbrella crate via the `source-postgres` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: PostgreSQL connection failed: …` | Bad host/port/db, wrong credentials, or the server is unreachable / not accepting TCP. Verify the `connection_url` and that the DB is up and reachable from the runner. |
| `FaucetError::Config: PostgreSQL query failed: …` | Invalid SQL, a missing table/column, or insufficient privileges. Run the query directly with `psql` to confirm it works for that role. |
| `operator does not exist: integer = text` (or similar) | A `params` value's JSON type doesn't match the column type. Use the matching JSON scalar — e.g. `42` (number) for an integer column, not `"42"` (string). |
| `FaucetError::Config: batch_size must be …` | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` for a single un-chunked page. |
| Run uses too much memory on a huge table | Lower `batch_size` so each page is smaller, and ensure the sink flushes per page. Avoid `batch_size: 0` for very large result sets — it materializes everything in one page. |
| A `numeric`/`timestamp`/`uuid` arrives as a string | Intentional — these are encoded as strings to preserve exact precision / format. Cast downstream (e.g. a `cast` transform) if you need a JSON number. |
| A column comes back as `null` unexpectedly | The column's Postgres type isn't in the supported-type table above and falls through to `null`. Cast it in SQL (e.g. `SELECT my_col::text`) so it decodes as a string. |
| Connection-pool exhaustion under high matrix fan-out | Many matrix children share the pool. Raise `max_connections`, or lower the pipeline's `execution.max_concurrent`. |
| Credentials appear in logs | The DSN is masked in `Debug` and stripped from the lineage URI, but never run a connector config holding a resolved secret with `FAUCET_LOG=debug` — third-party `sqlx` logging is outside faucet's redaction boundary. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Choosing a connector](https://faucet-hq.github.io/faucet-stream/reference/choosing.html)
- [`faucet-source-postgres-cdc`](https://crates.io/crates/faucet-source-postgres-cdc) — resumable change data capture via logical replication
- [`faucet-sink-postgres`](https://crates.io/crates/faucet-sink-postgres) — the PostgreSQL sink (insert / upsert / delete)
- [`faucet-source-mysql`](https://crates.io/crates/faucet-source-mysql) · [`faucet-source-sqlite`](https://crates.io/crates/faucet-source-sqlite) · [`faucet-source-mssql`](https://crates.io/crates/faucet-source-mssql)

## Sharded execution (cluster Mode B)

Under [`faucet serve --cluster`](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html),
a top-level `shard: { count: N }` block splits this source into contiguous
primary-key ranges that different cluster workers process concurrently. Opt in
by naming an integer-typed key column:

```yaml
shard:
  count: 8
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      query: "SELECT * FROM events"
      shard: { key: id }   # integer column to range-partition on
```

The coordinator computes `MIN(key)` / `MAX(key)` once and splits that range
into half-open slices (`"key"` in the generated predicate, injection-safe).
The boundary shards stay open-ended so rows inserted outside the captured
range during the run are still read, and exactly one shard additionally
matches `key IS NULL` so nullable keys are never silently dropped. Each shard
keeps its own state key (`{run}::{shard}`), so a reassigned shard resumes
where its previous owner left off.

Outside the cluster coordinator the `shard` config has **no effect** — a plain
`faucet run` streams the whole query.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="postgres"`.

## Range digests (`faucet verify`, #701)

The source implements `Source::range_digest`: `faucet verify` asks it for a
**server-side content digest** of one integer-key range — `count(*)`, the
exact sum of a 60-bit prefix of each row's `MD5` over the compared columns
(NULL rendered distinctly from an empty string), and the key bounds —
computed by one aggregate query over the range-wrapped `query`. Algorithm id
`postgres:md5-60-sum:v1`; two digests compare only when both sides report the same id, so a
pair of these sources verifies without shipping any rows for the ranges that
match (other pairings stream and hash client-side). See the [verification
cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/verify.html).
