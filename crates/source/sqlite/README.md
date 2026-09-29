# faucet-source-sqlite

[![Crates.io](https://img.shields.io/crates/v/faucet-source-sqlite.svg)](https://crates.io/crates/faucet-source-sqlite)
[![Docs.rs](https://docs.rs/faucet-source-sqlite/badge.svg)](https://docs.rs/faucet-source-sqlite)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-sqlite.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-sqlite.svg)](https://github.com/faucet-hq/faucet-stream#license)

A **SQLite** query source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Executes a SQL statement against a SQLite database (a file or an in-memory database), decodes each row into a typed `serde_json::Value`, and streams the result set back page-by-page via a sqlx row cursor so memory stays bounded no matter how large the table is.

SQLite is an in-process, file-based engine — there is no server, no network wire, and no auth. That makes this connector the simplest, lowest-overhead way to pull rows out of a local database and land them in any faucet-stream sink (a file, another database, a warehouse, a queue) with one declarative config and no glue code. It's also ideal for CI and locked-down environments where you don't want to stand up a server.

## Feature highlights

- **Native cursor streaming** — overrides `Source::stream_pages` to drive a sqlx row cursor (`Query::fetch`) without buffering the whole result. Rows are accumulated into a `batch_size` buffer and yielded as a `StreamPage` as soon as it fills, so peak client-side memory is `O(batch_size)` and the sink can start writing while the rest of the table is still being read off disk.
- **`batch_size: 0` "no batching" sentinel** — drain the entire cursor into one page for small lookup tables or for sinks that prefer one large request to many small ones.
- **Dynamic-type aware decoding** — SQLite's storage classes (INTEGER, REAL, TEXT, BLOB, NULL) are probed in order of specificity; TEXT that parses as JSON is returned as a native JSON value, BLOBs are base64-encoded so binary survives the round-trip.
- **Connection pooling** — a `sqlx::SqlitePool` is built once in `new()` and reused for every query; pool size is configurable.
- **Safe parameter binding** — `{field}` placeholders in the query are bound from the matrix / parent-record context as positional `?` parameters, so values are never string-interpolated into SQL (no injection).
- **File or in-memory** — point at `sqlite:data.db`, an absolute path, or `sqlite::memory:`.

## Installation

```bash
# As a library:
cargo add faucet-source-sqlite
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-sqlite
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-sqlite
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: sqlite_to_jsonl
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite:./app.db
      query: SELECT * FROM events ORDER BY ts
  sink:
    type: file
    config:
      path: events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

All fields live under `pipeline.source.config`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | string | — *(required)* | SQLite database URL. A file path (`"sqlite:data.db"`, `"sqlite://./data/app.db"`, `"sqlite:/abs/path.db"`) or in-memory (`"sqlite::memory:"`). |
| `query` | string | — *(required)* | SQL to execute. May contain `{field}` placeholders that are bound from the matrix / parent-record context as positional `?` parameters at runtime. |
| `max_connections` | int (`u32`) | `10` | Maximum connections in the sqlx pool. SQLite is single-writer; a small pool (2–5) is usually plenty for a read-only source. |
| `batch_size` | int (`usize`) | `1000` | Rows per emitted `StreamPage`. **`0` = no batching**: the cursor is fully drained and the entire result set is emitted in a single page. Values above `MAX_BATCH_SIZE` (1,000,000) are rejected at construction. |
| `shard` | object | *(unset)* | Optional [Mode B sharding](#sharded-execution-cluster-mode-b): `{ key: <integer column> }`. Opts the source into primary-key range splitting under `faucet serve --cluster`; no effect on a plain `faucet run`. |

There is **no auth, TLS, or credential configuration** — SQLite is a local in-process engine.

## Examples

### Read a table into CSV (no network required)

```yaml
version: 1
name: sqlite_to_csv
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite://./data/app.db
      query: SELECT id, email, created_at FROM users ORDER BY id
  sink:
    type: file
    config:
      path: ./out/users.csv
      csv:
        delimiter: ","
        has_headers: true
```

### Project a JSON column out of a row

`json_extract` is a SQLite built-in; the extracted TEXT is returned as a native JSON value when it parses.

```yaml
source:
  type: sqlite
  config:
    database_url: sqlite:/var/data/app.db
    query: |
      SELECT id, name, created_at,
             json_extract(metadata, '$.tags') AS tags
      FROM items
      WHERE active = 1
    batch_size: 5000
```

### Whole-table export with no re-chunking

```yaml
source:
  type: sqlite
  config:
    database_url: sqlite:analytics.db
    query: SELECT * FROM daily_snapshot
    batch_size: 0          # emit the entire result set as one page
```

### Per-parent query in a matrix fan-out

A child matrix row can substitute values from each parent record into the query with `{field}` placeholders. Each value is bound as a positional parameter, so injection is impossible.

```yaml
matrix:
  - id: tenants
    source:
      ref: tenant_list
  - id: rows
    parent: tenants
    source:
      type: sqlite
      config:
        database_url: sqlite:tenants.db
        query: SELECT * FROM records WHERE tenant_id = {tenants.id}
```

## Streaming & batching

`SqliteSource::stream_pages` drives a sqlx row cursor (`Query::fetch`) without buffering the full result. Rows are accumulated into a `batch_size` buffer and yielded as a `StreamPage` once the buffer fills; the trailing partial page (if any) is yielded after the cursor drains. The pipeline writes each page to the sink as it arrives, so memory is bounded at `O(batch_size)` on both sides.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained completely and the entire result set is emitted in a single `StreamPage`. Use it for small lookup tables, or for downstream sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage uploads) that prefer one large request to many small ones. Values larger than `MAX_BATCH_SIZE` (1,000,000) are rejected by `faucet_core::validate_batch_size` at construction time.

> **Note** — SQLite is an in-process, file-based engine: there is no server-side cursor concept and no network wire to worry about. The streaming implementation bounds *client-side* memory and lets the sink begin writing as soon as the first batch is parsed off disk, rather than waiting for the whole result set to materialise in a `Vec`.

The `batch_size` argument passed to `stream_pages` by the pipeline is informational — the source always uses its own `config.batch_size` so a pipeline-supplied hint cannot silently override an explicit config value.

### No incremental / resume support

This is a one-shot query source. It does **not** implement bookmark-based resume, incremental replication, or effectively-once delivery: every emitted page carries `bookmark: None`, and each run re-executes the full query. For incremental loads, encode the watermark directly in the query (e.g. `WHERE updated_at > '2026-01-01'`) and drive it from a matrix context or a `${now.*}` token. If you need change-data-capture, use a CDC source (postgres-cdc / mysql-cdc / mongodb-cdc) instead.

## Supported column types

SQLite has dynamic typing — values are stored as INTEGER, REAL, TEXT, BLOB, or NULL. The source probes each column value in order of specificity:

| SQLite storage class | JSON type |
|----------------------|-----------|
| TEXT (valid JSON) | native JSON value |
| TEXT | `string` |
| INTEGER (`i64`) | `number` |
| INTEGER (`i32`) | `number` |
| REAL (`f64`) | `number` |
| BOOLEAN | `boolean` |
| BLOB | `string` (base64) |
| NULL / unsupported | `null` |

SQLite has no native datetime / UUID / decimal types — those are stored as TEXT/INTEGER/REAL and surface accordingly. A non-finite `f64` (NaN / infinity) cannot be represented in JSON and decodes to `null`.

## Config loading

Load from YAML/JSON, environment variables, or a `.env` file via the helpers in `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_sqlite::SqliteSourceConfig;

let config: SqliteSourceConfig = load_json("config.json")?;
let config: SqliteSourceConfig = load_env_file(".env", "SQLITE_SOURCE")?;
```

### Example JSON config

```json
{
  "database_url": "sqlite:/var/data/app.db",
  "query": "SELECT id, name, created_at FROM items WHERE active = 1",
  "max_connections": 5,
  "batch_size": 5000
}
```

### Example `.env` file

```env
SQLITE_SOURCE_DATABASE_URL=sqlite:data.db
SQLITE_SOURCE_QUERY=SELECT * FROM events
SQLITE_SOURCE_MAX_CONNECTIONS=10
SQLITE_SOURCE_BATCH_SIZE=1000
```

## Schema introspection

Inspect the full JSON Schema for this connector's config with:

```bash
faucet schema source sqlite
```

Programmatically, every `Source` exposes `config_schema()`:

```rust
use faucet_core::Source;

let source = SqliteSource::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Dataset discovery

The source supports live introspection via `Source::discover()` (#211): it enumerates every user table in `sqlite_master` (internal `sqlite_*` tables excluded, ordered by name) and returns one dataset descriptor per table with:

- `name` — the table name; `kind: table`
- `schema` — the column shape from `pragma_table_info` as a JSON-Schema object (declared type names mapped to JSON types; columns without `NOT NULL` become `["T", "null"]`; a typeless column maps to `string`)
- `estimated_rows` — always absent: SQLite keeps no cheap row-count statistic, and discovery never scans data to count
- `config_patch` — `` { "query": "SELECT * FROM `table`" } ``, backtick-quoted (interior backticks doubled), ready to deep-merge over the connection config as a matrix row

Discovery reads catalog metadata only — it never scans table data.

## Library usage

```rust
use faucet_core::Source;
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = SqliteSourceConfig::new(
    "sqlite:data.db",
    "SELECT id, name, score FROM students ORDER BY score DESC",
)
.with_max_connections(5)
.with_batch_size(2000);

let source = SqliteSource::new(config).await?;

// Convenience: buffer the whole result set.
let records = source.fetch_all().await?;
for record in &records {
    println!("{record}");
}
# Ok(())
# }
```

To wire it into a pipeline with a sink, build a `faucet_core::Pipeline` (or call `run_stream`) over this source and any `Sink` — the pipeline drives `stream_pages` and writes each page as it arrives.

## How it works

1. `new()` validates `batch_size` and builds a `sqlx::SqlitePool` (`SqlitePoolOptions::max_connections`) **once**, reusing it for every query. A connection failure surfaces as `FaucetError::Config`.
2. `stream_pages` resolves any `{field}` context placeholders into positional `?` bind markers, binds the values by JSON type (string / integer / float / bool / null — integers up to `u64::MAX` bind exactly, with no `f64` precision loss), and opens a streaming cursor with `Query::fetch`.
3. Rows are decoded one at a time (`row_to_json`), buffered to `batch_size`, and yielded as `StreamPage`s; the final partial buffer is flushed at the end.
4. Each column value is decoded by probing storage classes in specificity order (see [Supported column types](#supported-column-types)).

## Lineage dataset URI

`sqlite://<path>?query=<sql>` — e.g. `sqlite:///var/db/app.db?query=SELECT id FROM events`. The `sqlite://` / `sqlite:` scheme prefix is stripped from `database_url` before the path is rendered.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI / umbrella crate via the `source-sqlite` feature. It is **not** in the CLI default build — opt in with `cargo install faucet-cli --features source-sqlite`.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: SQLite connection failed` at startup | The database file doesn't exist or the path is wrong. sqlx does **not** create the file for a read-only URL — point at an existing `.db`, or use `sqlite:data.db?mode=rwc` to create it. Check the path is relative to the process working directory. |
| `unable to open database file` | The directory doesn't exist or the process lacks read permission on the file. Verify the path and file permissions. |
| `FaucetError::Config: SQLite query failed` | A SQL syntax error or a reference to a missing table/column. Run the query directly with the `sqlite3` CLI to confirm it's valid. |
| In-memory DB looks empty / each query sees a fresh database | `sqlite::memory:` databases are per-connection. With a pool of more than one connection, different queries may hit different empty databases — use `max_connections: 1` for `:memory:`, or a real file. |
| A `{placeholder}` in the query wasn't substituted | `{field}` substitution only happens when a matrix / parent-record context is present. For a non-matrix run there is no context, so the literal query is used as-is. Encode static filters as literals or bind via a matrix parent. |
| `database is locked` errors under concurrent writes | Another process holds a write lock. SQLite is single-writer; lower `max_connections`, or enable WAL mode on the database (`PRAGMA journal_mode=WAL;`) out of band. |
| A BLOB column arrives as a base64 string | Intentional — binary data is base64-encoded so it survives the JSON round-trip. Decode it in a downstream transform if you need the raw bytes. |
| `batch_size` rejected at startup | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or use `0` for the no-batching sentinel. |
| Need only changed rows, not the whole table | This source has no incremental/CDC mode. Add a `WHERE` watermark to the query (driven by `${now.*}` or a matrix context), or use a CDC source. |

## See also

- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [CLI & config-file grammar](https://faucet-hq.github.io/faucet-stream/reference/cli.html)
- [State & resume cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html)
- [`faucet-sink-sqlite`](https://crates.io/crates/faucet-sink-sqlite) — the matching SQLite sink
- [`faucet-source-postgres`](https://crates.io/crates/faucet-source-postgres) · [`faucet-source-mysql`](https://crates.io/crates/faucet-source-mysql) — server-based SQL sources

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
    type: sqlite
    config:
      database_url: sqlite:/shared/data.db
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

> Sharding a SQLite source across cluster workers requires every worker to
> reach the same database file (e.g. a shared volume).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="sqlite"`.
