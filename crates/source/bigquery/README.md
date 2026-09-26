# faucet-source-bigquery

[![Crates.io](https://img.shields.io/crates/v/faucet-source-bigquery.svg)](https://crates.io/crates/faucet-source-bigquery)
[![Docs.rs](https://docs.rs/faucet-source-bigquery/badge.svg)](https://docs.rs/faucet-source-bigquery)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-bigquery.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-bigquery.svg)](https://github.com/faucet-hq/faucet-stream#license)

Google **BigQuery** query source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs a SQL statement against BigQuery's [REST API](https://cloud.google.com/bigquery/docs/reference/rest) (`jobs.query` + `jobs.getQueryResults`), decodes each row into a typed `serde_json::Value`, and streams the result set back page-by-page so memory stays bounded no matter how large the query result is.

Reach for it when you want to pull analytics tables, aggregates, or ad-hoc query results out of BigQuery and land them in any faucet-stream sink — a file, a database, a warehouse, a queue — with one declarative config and no glue code.

## Feature highlights

- **Three credential modes** — Application Default Credentials, a service-account key file, or inline service-account JSON. The shared `BigQueryCredentials` enum is re-exported from [`faucet-common-bigquery`](https://crates.io/crates/faucet-common-bigquery) so it matches the BigQuery **sink** byte-for-byte.
- **Server-side pagination** — pages through arbitrarily large result sets via `pageToken`; rows are re-framed into pages of `batch_size` for streaming, so peak memory is `O(batch_size)`.
- **Async-job aware** — when BigQuery returns `jobComplete=false` (the statement ran past `statement_timeout`), the source polls `jobs.getQueryResults` until the job finishes, bounded by `poll_timeout` so a job that never completes fails cleanly instead of hanging forever.
- **Type-aware row decoding** — `INTEGER`/`INT64` → JSON number, `FLOAT`/`FLOAT64` → number, `BOOLEAN`/`BOOL` → bool, `NUMERIC`/`BIGNUMERIC` → string (full precision preserved), `RECORD`/`STRUCT` → nested object, `REPEATED` → array, `JSON` → parsed value, everything else → string.
- **Positional bind parameters** — `params` from config plus any `${parent.path}` matrix-context values are sent as BigQuery `POSITIONAL` parameters, typed from the JSON value (`INT64` / `FLOAT64` / `BOOL` / `STRING`) so a numeric or boolean bind compares correctly against a typed column.
- **Standard or legacy SQL** — `use_legacy_sql` toggle; defaults to Standard SQL.
- **Client built once** — the authenticated BigQuery client is constructed in `new()` and reused for every request.

## Installation

```bash
# As a library:
cargo add faucet-source-bigquery

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-bigquery
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: bigquery
    config:
      project_id: my-project
      auth:
        type: service_account_key_path
        config:
          path: /etc/secrets/bigquery-sa.json
      query: |
        SELECT user_id, event_name, occurred_at
        FROM `my-project.analytics.events`
        WHERE occurred_at >= ?
      params:
        - "2026-01-01"
  sink:
    type: jsonl
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | string | — *(required)* | GCP project ID the query is billed to and run against. |
| `auth` | `BigQueryCredentials` | — *(required)* | Authentication — see [Authentication](#authentication). |
| `query` | string | — *(required)* | SQL to execute. May contain positional `?` markers (bound from `params` + matrix context) and `${field.path}` placeholders resolved against the parent-record context at runtime. |
| `use_legacy_sql` | bool | `false` | Use BigQuery's legacy SQL dialect. Leave `false` (Standard SQL) unless the query uses legacy `[project:dataset.table]` references. |
| `location` | string | *(unset)* | Location override for non-`US` jobs (`"EU"`, `"asia-east1"`, …). When unset, BigQuery uses the queried tables' default location. |
| `max_results_per_page` | int | `1000` | Rows per `jobs.getQueryResults` page. Smaller = more HTTP round-trips, less memory; larger = fewer requests, more memory. |
| `params` | array | `[]` | Positional bind parameters, sent in declaration order **before** any context-derived values. Typed from each JSON value. |
| `statement_timeout` | int (seconds) | `60` | Per-statement server-side timeout (`timeoutMs` on `jobs.query`). If the query isn't done within this window, BigQuery returns `jobComplete=false` and the source begins polling. |
| `poll_timeout` | int (seconds) | `300` | Max wall-clock time spent polling `jobs.getQueryResults` for a job still reporting `jobComplete=false`, before failing with `FaucetError::Source`. **`0` disables the cap** (poll forever). Only the completion wait is bounded; `pageToken` paging afterward is unaffected. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching**: the entire result set is emitted in a single page (good for small lookup tables or sinks that prefer one large request). |

### Authentication

`auth` uses the shared `BigQueryCredentials` enum (the project-wide `{ type, config }` shape):

| `type` | `config` | Use when |
|--------|----------|----------|
| `application_default` | *(none)* | Running on GCP (workload identity / metadata server) or after `gcloud auth application-default login`. |
| `service_account_key_path` | `{ path: <file> }` | You have a service-account key file on disk. |
| `service_account_key` | `{ json: <string> }` | You want to inject the key JSON inline, typically via `${env:VAR}` / `${secret:…}` indirection. |

```yaml
# Application Default Credentials (workload identity, gcloud)
auth:
  type: application_default
```

```yaml
# Service-account key file
auth:
  type: service_account_key_path
  config:
    path: /etc/secrets/bigquery-sa.json
```

```yaml
# Inline service-account JSON via env indirection
auth:
  type: service_account_key
  config:
    json: ${env:GCP_SERVICE_ACCOUNT_JSON}
```

## Examples

### Incremental pull with a typed bind parameter

```yaml
source:
  type: bigquery
  config:
    project_id: my-project
    auth: { type: application_default }
    query: |
      SELECT id, name, score
      FROM `my-project.app.users`
      WHERE score > ?
    params:
      - 0.75          # bound as FLOAT64, compares correctly against a numeric column
```

### Large export with no re-chunking

```yaml
source:
  type: bigquery
  config:
    project_id: my-project
    auth:
      type: service_account_key_path
      config: { path: /etc/secrets/bigquery-sa.json }
    query: SELECT * FROM `my-project.warehouse.daily_snapshot`
    location: EU
    max_results_per_page: 10000
    batch_size: 0      # emit the whole result set as one page
```

### Long-running aggregate that may exceed the statement timeout

```yaml
source:
  type: bigquery
  config:
    project_id: my-project
    auth: { type: application_default }
    query: |
      SELECT country, COUNT(*) AS n
      FROM `my-project.analytics.events`
      GROUP BY country
    statement_timeout: 30     # return fast; if not done, poll…
    poll_timeout: 600         # …for up to 10 minutes before failing
```

## Streaming & batching

The source overrides `Source::stream_pages`: it runs the job, then walks `jobs.getQueryResults` by `pageToken`, buffering decoded rows and yielding a `StreamPage` every time the buffer reaches `batch_size`. Memory is bounded at one page. With `batch_size: 0` the entire result set is buffered and emitted in a single page. `max_results_per_page` controls the *HTTP* page size independently of the *emitted* page size.

This is a one-shot query source — it has no incremental bookmark / resume support (each run re-executes the query). For incremental loads, encode the watermark in the query (e.g. `WHERE occurred_at >= ?`) and drive it from a matrix context or `${now.*}` token.

## Dataset discovery

The source supports live introspection via `Source::discover()`: it lists every dataset in the project (`datasets.list`), enumerates the physical tables in each (`tables.list` — views, materialized views, and external tables are skipped), and fetches each table's schema and row count via `tables.get`. One dataset descriptor is returned per table with:

- `name` — `dataset.table` (e.g. `sales.orders`), `kind: table`
- `schema` — the column shape as a JSON-Schema object (`INTEGER`/`INT64` → integer; `FLOAT`/`FLOAT64`/`NUMERIC`/`BIGNUMERIC` → number; `BOOLEAN`/`BOOL` → boolean; `RECORD`/`STRUCT`/`JSON` → object; `mode: REPEATED` → array; everything else → string; `NULLABLE` columns become `["T", "null"]`)
- `estimated_rows` — the table's `numRows` from `tables.get` (omitted when unavailable)
- `config_patch` — `{ "query": "SELECT * FROM `project.dataset.table`" }`, backtick-quoted, ready to deep-merge over the connection config as a matrix row

Discovery reads catalog metadata only — it never runs a billed query or scans table data. To keep it cheap on very large projects, enumeration is capped at 500 tables and per-table schema fetches at 100 (tables past the schema cap are still emitted, just without a `schema` / `estimated_rows`); a warning names the cap when it is hit.

## Config loading & schema

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source bigquery
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_bigquery::{BigQueryCredentials, BigQuerySource, BigQuerySourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = BigQuerySourceConfig::new(
    "my-project",
    BigQueryCredentials::ApplicationDefault,
    "SELECT COUNT(*) AS n FROM analytics.events",
)
.with_location("EU")
.with_batch_size(0);

let rows = BigQuerySource::new(cfg).await?.fetch_all().await?;
println!("rows: {rows:?}");
# Ok(())
# }
```

## How it works

1. `new()` resolves `BigQueryCredentials` and builds an authenticated client **once**.
2. `jobs.query` submits the SQL with `statement_timeout` as `timeoutMs` and any positional parameters.
3. If `jobComplete=false`, the source polls `jobs.getQueryResults` until the job finishes or `poll_timeout` elapses.
4. Result pages are walked via `pageToken`; each cell is decoded by its schema type into a typed JSON value.
5. Rows are re-framed into `batch_size` pages and streamed to the pipeline.

## Lineage dataset URI

`bigquery://<project_id>?query=<sql>` — e.g. `bigquery://my-project?query=SELECT id FROM events`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-bigquery` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 / 403 | Credentials invalid or missing scope. Confirm the service account has **BigQuery Data Viewer** + **BigQuery Job User** on the project, or that ADC is initialized (`gcloud auth application-default login`). |
| `403 ... billing` | The `project_id` has no billing enabled, or the SA lacks `bigquery.jobs.create`. Use a project with billing and the Job User role. |
| Job fails with a location error | The queried dataset isn't in the default (`US`) location. Set `location` to the dataset's region (e.g. `EU`). |
| `FaucetError::Source: poll timeout` | The job ran longer than `poll_timeout`. Raise `poll_timeout` (or set `0` to wait indefinitely), and/or raise `statement_timeout`. |
| A `NUMERIC` column arrives as a string | Intentional — `NUMERIC`/`BIGNUMERIC` are decoded as strings to preserve full precision. Cast in a downstream transform if you need a JSON number. |
| Bind parameter compared as text unexpectedly | Each `params` entry is typed from its JSON value. Pass `0.75` (number) not `"0.75"` (string) to bind a `FLOAT64`. |
| Legacy table reference fails to parse | Set `use_legacy_sql: true` for `[project:dataset.table]` syntax. |


## Storage Read throughput (`max_streams` / `stream_concurrency`)

`max_streams` asks BigQuery to shard the table into that many read streams;
`stream_concurrency` (default `4`) says how many of those shards to read at
once (#621). Consuming them one at a time made `max_streams` pointless — the
read stayed as slow as a single stream. Batches from different streams
**interleave**, which costs nothing: the Storage Read API gives no ordering
guarantee across streams, since that is what sharding means. Each in-flight
stream carries its own decode buffer, so this is also the memory knob.

`read_api` no longer needs `read_table`: with only a `query`, the source runs
it and reads the job's **destination table** through the same gRPC Arrow path
(#621). Before this, arbitrary SQL fell back to paginating `getQueryResults`
REST JSON — exactly where a large result most needs the fast path. Only a
config with *neither* a table nor SQL is rejected.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Arrow columnar read (opt-in, `arrow` feature)

With a binary built `--features arrow`, set `read_api: true` + `read_table` to
read a table directly through the BigQuery **Storage Read API** (gRPC) as Arrow
`RecordBatch`es — no `jobs.query`, no per-row JSON. Paired with a columnar sink
(e.g. `bigquery → parquet`) the whole pipeline runs on Arrow; with a row-based
sink the batches are decoded to JSON transparently.

```yaml
source:
  type: bigquery
  config:
    project_id: my-project
    auth: { type: application_default }
    query: ""                      # ignored in read_api mode
    read_api: true
    read_table: analytics.events   # dataset.table or project.dataset.table
    row_restriction: "state = 'CA'"  # optional server-side filter
    selected_fields: [id, state, amount]  # optional projection (empty = all)
    max_streams: 1                 # streams to request (read sequentially)
```

This mode is **full-extract only** (no incremental bookmark) and requires a
`read_table`; enabling it on an `arrow`-off binary is rejected at construction.
The Snowflake source has no equivalent (its REST API is jsonv2-only).

## Usage signals (#704)

The source reports its backend round trips (`query`, `poll`, `job`) and the
`bytes_processed` cost signal (`totalBytesProcessed` of each query job) to
faucet's usage meter, priced with `usage.pricing.warehouse.bigquery_per_tib_scanned`;
see the [usage cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/usage.html).
