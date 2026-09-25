# faucet-source-mysql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mysql.svg)](https://crates.io/crates/faucet-source-mysql)
[![Docs.rs](https://docs.rs/faucet-source-mysql/badge.svg)](https://docs.rs/faucet-source-mysql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mysql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mysql.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **MySQL query source** that runs a SQL query and streams the rows out as `serde_json::Value` records.

Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Built on `sqlx` with a pooled, async connection and a true row cursor (`Query::fetch`) — rows are decoded and handed to the sink as they arrive off the wire, so client-side memory stays bounded by `batch_size` no matter how large the result set is. Reach for it to pull tables, filtered extracts, or analytics queries out of MySQL / MariaDB into any faucet-stream sink, or to fan a query out per parent record in a matrix pipeline.

## Feature highlights

- **Streaming row cursor** — `Source::stream_pages` drives a sqlx cursor and yields one `StreamPage` per `batch_size` rows; the sink starts writing before the query finishes draining.
- **Connection pooling** — a single `MySqlPool` is built once in `new()` and reused for every fetch; size it with `max_connections` (default `10`).
- **Rich type decoding** — JSON, integers, floats, booleans, `DATETIME`/`TIMESTAMP`/`DATE`/`TIME`, `DECIMAL` (exact precision), and `BLOB`/`BINARY` (base64) all map to sensible JSON.
- **Parameterised per-record queries** — in a parent/child matrix run, `${parent.field}` tokens in the query are substituted as **safe bind parameters** (`?` placeholders), never string-interpolated.
- **TLS by default** — built with `tls-rustls`; encrypted connections need no extra dependency.
- **Credential-safe** — the connection URL is masked in `Debug` output and stripped from the lineage dataset URI.
- **`batch_size: 0` sentinel** — drain the whole result set into a single page for small lookup tables or load-job-style sinks.

## Installation

```bash
# As a library:
cargo add faucet-source-mysql
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-mysql
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-mysql
```

The MySQL source is **not** in the CLI/umbrella default build — enable the `source-mysql` feature explicitly.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: mysql
    config:
      connection_url: mysql://user:pass@localhost:3306/mydb
      query: SELECT id, name, email FROM users WHERE active = 1 ORDER BY id
  sink:
    type: jsonl
    config:
      path: ./users.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | MySQL connection URL, e.g. `mysql://user:pass@host:3306/db`. Masked in `Debug` output and stripped from the lineage URI. |
| `query` | string | — *(required)* | The SQL query to execute. May contain `${parent.field}` tokens that are bound as parameters in a matrix run (see [Per-record queries](#per-record-queries-matrix-pipelines)). |
| `max_connections` | int | `10` | Maximum connections in the sqlx pool. |
| `batch_size` | int | `1000` | Rows per emitted `StreamPage`. **`0` = no batching** (drain the whole result set into one page). Values above `MAX_BATCH_SIZE` (1,000,000) are rejected at construction by `faucet_core::validate_batch_size`. |
| `shard` | object | *(unset)* | Optional [Mode B sharding](#sharded-execution-cluster-mode-b): `{ key: <integer column> }`. Opts the source into primary-key range splitting under `faucet serve --cluster`; no effect on a plain `faucet run`. |

There is no separate `auth` block — credentials live in `connection_url` (and can be sourced from env or a secrets manager; see [Config loading](#config-loading)).

## Examples

### Filtered extract into Postgres

Reuses [`cli/examples/mysql_to_postgres.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/mysql_to_postgres.yaml):

```yaml
version: 1
name: mysql_to_postgres
pipeline:
  source:
    type: mysql
    config:
      connection_url: mysql://user:pass@localhost/legacy
      query: SELECT id, name, address, created_at FROM customers ORDER BY id
      max_connections: 16
  sink:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/modern
      table_name: customers_imported
      column_mapping:
        type: auto_map
      batch_size: 1000
      max_connections: 10
```

### Rolling time window into stdout

```yaml
version: 1
pipeline:
  source:
    type: mysql
    config:
      connection_url: ${env:MYSQL_URL}
      query: >
        SELECT id, type, payload, created_at
        FROM events
        WHERE created_at >= CURDATE() - INTERVAL 7 DAY
        ORDER BY created_at
      batch_size: 5000
  sink:
    type: stdout
    config:
      format: jsonl
```

### Small lookup table — one large page

```yaml
version: 1
pipeline:
  source:
    type: mysql
    config:
      connection_url: ${env:MYSQL_URL}
      query: SELECT code, label FROM country_codes
      batch_size: 0          # drain the whole result set into a single page
  sink:
    type: bigquery
    config:
      project_id: my-project
      dataset_id: ref
      table_id: country_codes
```

### Per-record queries (matrix pipelines)

When a row runs once per record emitted by a `parent`, `${parent.field}` tokens in the `query` are resolved per record as **bind parameters** — the value is never spliced into the SQL text:

```yaml
version: 1
name: orders_per_customer
pipeline:
  sources:
    customers:
      type: mysql
      config:
        connection_url: ${env:MYSQL_URL}
        query: SELECT id FROM customers WHERE region = 'EU'
    orders:
      type: mysql
      config:
        connection_url: ${env:MYSQL_URL}
        query: SELECT id, total FROM orders WHERE customer_id = ${customers.id}
  sink:
    type: jsonl
    config:
      path: ./orders.jsonl
matrix:
  - id: customers
    source: { ref: customers }
  - id: orders
    parent: customers
    source: { ref: orders }
```

## Streaming & batching

`MysqlSource::stream_pages` drives a sqlx row cursor (`Query::fetch`) without buffering the full result. Rows are accumulated into a `batch_size` buffer and yielded as a `StreamPage` once the buffer fills; the trailing partial page (if any) is yielded after the cursor drains. This bounds *client-side* memory at `O(batch_size)` and lets the sink begin writing as soon as the first batch is parsed off the wire.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained completely and the entire result set is emitted in a single `StreamPage`. Use it for small lookup tables, or for downstream sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage uploads) that prefer one large request to many small ones.

The trait-level `batch_size` argument to `stream_pages` is ignored in favour of the config field — the config is the authoritative, user-facing knob, so a pipeline-supplied hint can never silently override an explicit config value.

> **Note** — MySQL's wire protocol sends rows from a simple `SELECT` in a single response (no server-side cursor), so the streaming here bounds memory on the client side rather than asking the server to page. True server-side cursor streaming is tracked separately as a follow-up.

The MySQL query source has **no incremental-replication mode**, so every emitted page carries `bookmark: None` (there is no resume or effectively-once support — see [Capabilities](#capabilities)). For change-data-capture against MySQL binlogs, use [`faucet-source-mysql-cdc`](https://crates.io/crates/faucet-source-mysql-cdc) instead.

## Column types

Columns are converted to JSON in order of likelihood; an unsupported or `NULL` column becomes `null`:

| MySQL type | JSON shape |
|------------|------------|
| `json` | native JSON value |
| `varchar`, `text`, `char` | string |
| `bigint` | number (i64) |
| `int`, `mediumint` | number (i32) |
| `smallint`, `tinyint` | number (i16) |
| `double` | number (f64) |
| `float` | number (f32) |
| `tinyint(1)`, `boolean` | `true`/`false` |
| `datetime`, `timestamp` | string (RFC 3339 / ISO-8601) |
| `date`, `time` | string (ISO-8601) |
| `decimal`, `numeric` | string (exact precision preserved) |
| `blob`, `binary`, `varbinary` | string (base64) |
| other / `NULL` | `null` |

## Capabilities

| Capability | Supported | Notes |
|------------|-----------|-------|
| Native streaming | ✅ | sqlx row cursor; one page per `batch_size` rows. |
| Connection pooling | ✅ | `max_connections`, reused across fetches. |
| Resume / bookmark state | ❌ | Stateless query source; every page is `bookmark: None`. |
| Effectively-once delivery | ❌ | Source does not implement `supports_exactly_once`. |
| Write modes / upsert | — | Not applicable (this is a source). |
| Compression | ❌ | No `compression` feature. |
| Per-record (matrix) queries | ✅ | `${parent.field}` → SQL bind parameters. |

## Config loading

Load a config from a JSON file, environment variables, or a `.env` file via the helpers in `faucet_core::config`:

```rust,no_run
use faucet_core::config::{load_json, load_env_file};
use faucet_source_mysql::MysqlSourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config: MysqlSourceConfig = load_json("config.json")?;
let config: MysqlSourceConfig = load_env_file(".env", "MYSQL_SOURCE")?;
# Ok(()) }
```

```json
{
  "connection_url": "mysql://analytics:password@db.example.com:3306/warehouse",
  "query": "SELECT id, name, created_at, status FROM orders WHERE created_at > '2025-01-01' ORDER BY created_at",
  "max_connections": 5,
  "batch_size": 5000
}
```

```env
MYSQL_SOURCE_CONNECTION_URL=mysql://user:password@localhost:3306/mydb
MYSQL_SOURCE_QUERY=SELECT * FROM users
MYSQL_SOURCE_MAX_CONNECTIONS=10
```

In CLI configs the `connection_url` can also be drawn from env, files, or a secrets manager via `${env:VAR}` / `${file:PATH}` / `${vault:...}` directives.

## Schema introspection

Print the JSON Schema for this connector's config:

```bash
faucet schema source mysql
```

Or from Rust:

```rust,no_run
use faucet_core::Source;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let source = MysqlSource::new(MysqlSourceConfig::new("mysql://localhost/db", "SELECT 1")).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
# Ok(()) }
```

## Dataset discovery

The source supports live introspection via `Source::discover()` (#211): it enumerates every base table in the connection's **current database** (`information_schema.tables` / `information_schema.columns` where `table_schema = DATABASE()`) and returns one dataset descriptor per table with:

- `name` — the bare table name (a MySQL connection is scoped to a single database, so no qualifier is needed); `kind: table`
- `schema` — the column shape as a JSON-Schema object (`data_type` mapped per the Column types table; columns with `IS_NULLABLE = 'YES'` become `["T", "null"]`)
- `estimated_rows` — the storage engine's approximate count from `information_schema.tables.table_rows` (InnoDB statistics — refresh with `ANALYZE TABLE`; `NULL` means no estimate)
- `config_patch` — `` { "query": "SELECT * FROM `table`" } ``, backtick-quoted (interior backticks doubled), ready to deep-merge over the connection config as a matrix row

Discovery reads catalog metadata only — it never scans table data.

## Library usage

```rust,no_run
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use faucet_core::Source;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = MysqlSourceConfig::new(
    "mysql://user:password@localhost:3306/mydb",
    "SELECT id, name, email FROM users WHERE active = 1",
)
.with_max_connections(20)
.with_batch_size(5000);

let source = MysqlSource::new(config).await?;

// One-shot collect:
let records = source.fetch_all().await?;
for record in &records {
    println!("{record}");
}
# Ok(()) }
```

Wire it into a pipeline to stream straight to any sink:

```rust,ignore
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use faucet_core::Pipeline;

let source = MysqlSource::new(
    MysqlSourceConfig::new("mysql://localhost/production", "SELECT * FROM events"),
).await?;
let pipeline = Pipeline::new(Box::new(source), Box::new(my_sink));
let result = pipeline.run().await?;
```

## How it works

`MysqlSource::new` validates `batch_size`, then builds one `MySqlPool` (`MySqlPoolOptions::max_connections`) and stores it on the struct — every subsequent fetch reuses the pool rather than reconnecting. `stream_pages` opens a sqlx cursor with `Query::fetch` and pulls rows incrementally via `TryStreamExt::try_next`, flushing a `StreamPage` each time the buffer reaches `batch_size`. Each row is turned into a JSON object keyed by column name (`row_to_json`), and per-column decoding (`mysql_value_to_json`) probes the likely sqlx types in order, falling back to `Null`. Connection and query failures surface as `FaucetError::Config` with the underlying sqlx error attached. Throughput is dominated by row width and network round-trips; benchmark with your own schema and `batch_size`.

## Lineage dataset URI

`mysql://<host>:<port>/<db>?query=<sql>` (credentials stripped) — e.g. `mysql://host:3306/app?query=SELECT id FROM orders`.

## Feature flags

This crate has no optional Cargo features of its own. Enable it in the CLI / umbrella via the `source-mysql` feature. TLS (`tls-rustls`), `chrono`, `bigdecimal`, and `json` sqlx features are always compiled in.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: MySQL connection failed: ...` | Wrong host/port/credentials, the database is unreachable, or the URL scheme isn't `mysql://`. Verify the `connection_url` and that the server accepts the connection. |
| `FaucetError::Config: batch_size ...` at startup | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` to disable batching. |
| `FaucetError::Config: MySQL query failed: ...` | SQL syntax error, missing table/column, or insufficient privileges. Run the query directly against MySQL to confirm. |
| TLS handshake / certificate error | The server requires TLS the client can't negotiate. Adjust the server's TLS settings or supply the right host; `tls-rustls` is built in, so no extra dependency is needed. |
| A `DECIMAL` or `DATETIME` column arrives as a string | Intentional — `DECIMAL` is stringified to preserve exact precision and temporal types use ISO-8601. Cast downstream if you need a number. |
| A column comes back as `null` unexpectedly | The column type isn't in the decode list, or the value is genuinely `NULL`. Wrap the column in `CAST(... AS CHAR)` in the query to force a string. |
| Run holds many connections open | `max_connections` is high relative to the workload. Lower it; the default of `10` is plenty for a single streaming query. |
| `${parent.field}` shows up literally in the SQL | The row isn't a matrix child of that parent (no `parent:` set), so the token was never resolved. Check the matrix wiring. |
| Need change-data-capture, not a snapshot | This source only runs point-in-time queries. Use [`faucet-source-mysql-cdc`](https://crates.io/crates/faucet-source-mysql-cdc) for binlog replication. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Config grammar](https://faucet-hq.github.io/faucet-stream/reference/config.html) · [CLI reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html)
- Related crates: [faucet-source-mysql-cdc](https://crates.io/crates/faucet-source-mysql-cdc) · [faucet-sink-mysql](https://crates.io/crates/faucet-sink-mysql) · [faucet-source-postgres](https://crates.io/crates/faucet-source-postgres)

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
    type: mysql
    config:
      connection_url: ${env:MYSQL_URL}
      query: "SELECT * FROM events"
      shard: { key: id }   # integer column to range-partition on
```

The coordinator computes `MIN(key)` / `MAX(key)` once and splits that range
into half-open slices (`` `key` `` in the generated predicate, injection-safe).
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

Metrics emitted by this source are labelled `connector="mysql"`.

## Range digests (`faucet verify`, #701)

The source implements `Source::range_digest`: `faucet verify` asks it for a
**server-side content digest** of one integer-key range — `count(*)`, the
exact sum of a 60-bit prefix of each row's `MD5` over the compared columns
(NULL rendered distinctly from an empty string), and the key bounds —
computed by one aggregate query over the range-wrapped `query`. Algorithm id
`mysql:md5-60-sum:v1`; two digests compare only when both sides report the same id, so a
pair of these sources verifies without shipping any rows for the ranges that
match (other pairings stream and hash client-side). See the [verification
cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/verify.html).
