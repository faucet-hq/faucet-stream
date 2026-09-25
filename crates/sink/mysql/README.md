# faucet-sink-mysql

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mysql.svg)](https://crates.io/crates/faucet-sink-mysql)
[![Docs.rs](https://docs.rs/faucet-sink-mysql/badge.svg)](https://docs.rs/faucet-sink-mysql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-mysql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-mysql.svg)](https://github.com/faucet-hq/faucet-stream#license)

**MySQL** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to a MySQL (or MariaDB) table using a pooled `sqlx` connection and efficient multi-row `INSERT` statements with backtick-quoted identifiers.

Reach for it when you want to land any faucet-stream source — a REST API, a CSV file, a CDC stream, a queue — into MySQL with one declarative config and no glue code. Store records as opaque JSON blobs, or map their keys straight onto real table columns; mirror a source table with `write_mode: upsert`; or run end-to-end effectively-once from a CDC source.

## Feature highlights

- **Two column-mapping modes** — `json` packs each record into a single JSON column; `auto_map` maps top-level JSON keys directly onto table columns discovered from `INFORMATION_SCHEMA`.
- **Multi-row `INSERT`** — every batch becomes a single `INSERT INTO t VALUES (...), (...), ...`, sub-chunked so `rows × columns` never exceeds MySQL's 65,535-placeholder limit.
- **Connection pooling** — a `sqlx::MySqlPool` built once in `new()` and reused for every write; pool size is configurable.
- **Write modes** — `append` (default), `upsert` (`INSERT … ON DUPLICATE KEY UPDATE`), and `delete` (delete by key), with last-write-wins de-duplication within a page and atomic per-page transactions.
- **Effectively-once delivery** — pairs with a CDC source for at-most-once-effective writes; records and the commit-token watermark commit in one transaction.
- **Dead-letter queue** — in upsert/delete mode, rows with a missing/null key are routed per-row to a configured DLQ instead of failing the whole batch.
- **Native-typed binding** — in `auto_map` mode, values bind as native MySQL types (text, integer/double, `TINYINT` booleans, JSON for nested values).
- **Credential-safe** — the `Debug` impl masks `connection_url` so credentials never leak into logs.

## Installation

```bash
# As a library:
cargo add faucet-sink-mysql
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-mysql

# Or via the umbrella crate:
cargo add faucet-stream --features sink-mysql
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: csv_to_mysql
pipeline:
  source:
    type: csv
    config:
      path: customers.csv
      has_headers: true
  sink:
    type: mysql
    config:
      connection_url: mysql://user:pass@localhost:3306/crm
      table_name: customers
      column_mapping: auto_map
      batch_size: 1000
      max_connections: 10
```

```bash
faucet run pipeline.yaml
```

> Note YAML configs use `type:` (not `kind:`) for the connector discriminator.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | MySQL connection URL, e.g. `mysql://user:pass@host:3306/db`. Masked as `***` in `Debug` output. |
| `table_name` | string | — *(required)* | Target table. Quoted with backticks (embedded backticks doubled). |
| `column_mapping` | `MysqlColumnMapping` | `{ json: { column: "data" } }` | How JSON records map to columns — see [Column mapping](#column-mapping). |
| `max_connections` | int | `5` | Maximum connections in the `sqlx` pool. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Rows per multi-row `INSERT`. **`0` = no re-chunking**: the whole upstream page is sent in one `INSERT` (bounded by `max_allowed_packet`). See [Streaming & batching](#streaming--batching). |

### Write mode (flattened — top-level fields)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | `"append"` \| `"upsert"` \| `"delete"` | `"append"` | Write semantics. `upsert`/`delete` require `column_mapping: auto_map` and a non-empty `key`. See [Write modes](#write-modes-upsert--delete). |
| `key` | array of string | `[]` | Key columns for `upsert`/`delete`. The table must have a PRIMARY or UNIQUE index on these columns. Ignored for `append`. |
| `delete_marker` | `{ field, values }` | *(none)* | Upsert-only: rows whose `field` matches one of `values` become deletes; all others are upserts. The marker field is stripped from upsert rows before writing. |

### Column mapping

`column_mapping` is a tagged enum (`snake_case` discriminators):

| Variant | YAML | Description |
|---------|------|-------------|
| `Json { column }` | `column_mapping: { json: { column: data } }` | Insert each record as a serialized JSON string in one column (defaults to `data`). Uses `INSERT INTO t (col) VALUES (?), (?), ...`. |
| `AutoMap` | `column_mapping: auto_map` | Map top-level JSON keys directly to table columns discovered from `INFORMATION_SCHEMA.COLUMNS`. The INSERT column set is the **union** of record keys across the batch (a field present only in a later record is still written; a row missing a column binds SQL `NULL`). Extra keys with no matching column are silently ignored; records with no matching keys are skipped with a warning. |

## Examples

### JSON column mode — store records as serialized JSON

```sql
CREATE TABLE raw_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```yaml
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com, path: /v1/events } }
  sink:
    type: mysql
    config:
      connection_url: ${env:MYSQL_URL}
      table_name: raw_events
      column_mapping:
        json:
          column: data
      batch_size: 1000
```

### AutoMap mode — map JSON keys onto columns

```sql
CREATE TABLE events (
    user_id VARCHAR(255),
    event   VARCHAR(255),
    amount  DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```yaml
pipeline:
  source: { type: csv, config: { path: events.csv, has_headers: true } }
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@db.internal:3306/analytics
      table_name: events
      column_mapping: auto_map
      batch_size: 1000
      max_connections: 10
```

### High-throughput pooling

```yaml
sink:
  type: mysql
  config:
    connection_url: mysql://writer:pass@db-primary.internal:3306/warehouse
    table_name: metrics
    column_mapping: auto_map
    max_connections: 20
    batch_size: 1000
```

## Streaming & batching

The sink re-chunks each incoming `StreamPage` to keep individual multi-row `INSERT` statements under MySQL's `max_allowed_packet` limit.

- **`batch_size > 0`** (default `1000`) — the incoming slice is sliced into `batch_size`-row chunks; one multi-row `INSERT INTO ... VALUES (...), (...), ...` per chunk. **`1000` is the sweet spot**: small enough to stay well under the default 64 MB `max_allowed_packet` even for wide rows, large enough to amortise per-statement overhead. Bump it higher for narrow rows; drop it for very wide rows.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream `StreamPage` is forwarded in a single multi-row `INSERT`. Use this when the source already emits page sizes tuned for MySQL (e.g. a Postgres source configured with `batch_size: 1000`). Larger pages risk a `Packet too large` error.

In `auto_map` mode the INSERT is sub-chunked further so `rows × columns` never exceeds MySQL's 65,535-placeholder limit. `batch_size` is purely a chunk-size knob — SQL semantics, identifier quoting, and column-mapping behaviour are unchanged.

## Write modes (upsert / delete)

`MysqlSink` advertises `[Append, Upsert, Delete]` via `Sink::supported_write_modes()`.

**Requirements for `upsert` / `delete`:**

- `column_mapping` must be `auto_map` — key columns must be real table columns, not packed inside a JSON blob.
- `key` must be a non-empty list of column names.
- The target table must have a **PRIMARY KEY or UNIQUE index whose columns exactly match `key`** (order-insensitive — a UNIQUE index on `(a, b)` matches `key: [b, a]`). MySQL's `ON DUPLICATE KEY UPDATE` does **not** name a conflict target; it resolves on *any* unique index on the table. Because the pipeline dedups and routes by exactly the configured `key`, a `key` that does not match a real unique index would make MySQL silently upsert on a *different* index — producing wrong results you cannot detect. To prevent this, the sink **validates `key` against `INFORMATION_SCHEMA.STATISTICS` at construction** and fails fast with a clear error if it does not match a PRIMARY/UNIQUE index exactly (a prefix, subset, or superset of an index does **not** match). If the table does not exist yet (no unique indexes found), the check is skipped with a warning and the first write surfaces the missing-table error.
- A row missing/null in a key column fails. With a `dlq:` block configured, good rows are still written and only the bad rows are routed to the DLQ per-row; without a DLQ the whole batch fails.

**`write_mode: upsert`** — each record is `INSERT … ON DUPLICATE KEY UPDATE` (last-write-wins). An optional `delete_marker` routes flagged records to deletes instead:

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: products
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker:
        field: __op
        values: [d, delete]
```

**`write_mode: delete`** — every record in the page is deleted by its key:

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: products
      column_mapping: auto_map
      write_mode: delete
      key: [id]
```

**Behaviour:**

- The planner (`plan_writes`) de-duplicates within each page (last-write-wins), so a page containing the same key twice produces exactly one statement touching that row.
- Upserts and deletes within a page are applied inside **one transaction** — they commit atomically or not at all.
- The `delete_marker` field is stripped from upsert records before writing.

Pair `write_mode: upsert` with the [`cdc_unwrap`](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) transform to mirror a CDC source into MySQL. See the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

## Scoped cleanup

`MysqlSink` implements `Sink::supports_cleanup` (`true` in `auto_map` mode) and `Sink::cleanup_scope`, so an incremental sync can remove rows that were **deleted at the source** — something `write_mode: upsert` alone can never do, because a deleted record simply stops appearing in the feed.

Opt in with `cleanup: delete_missing` alongside `write_mode: upsert`, and pair it with a source that declares a completeness claim (`complete_for`):

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: contact_associations
      column_mapping: auto_map
      write_mode: upsert
      key: [contact_id, association_id]
      cleanup: delete_missing
```

After a successful, uncancelled invocation the sink deletes every row matching the claimed scope whose key this run did not write:

- The written keys are loaded into a session-scoped `CREATE TEMPORARY TABLE faucet_cleanup_keys`, and the delete is a single `DELETE … WHERE <scope> AND NOT EXISTS (SELECT 1 FROM faucet_cleanup_keys …)`. Not `key NOT IN (…)`: the written-key set routinely exceeds MySQL's 65535-placeholder limit.
- The whole thing runs in **one transaction** (`CREATE`/`DROP TEMPORARY TABLE` are the DDL statements MySQL does not implicitly commit), so the delete is all-or-nothing — a partial delete would remove rows the run actually wrote.
- **An empty result set is not a no-op**: if the source reports the scope as empty, every row in that scope is deleted. That is the case the feature exists for.
- Scope and `key` columns are validated against `INFORMATION_SCHEMA.COLUMNS` first, so a name that is not a real column fails with a clear error naming the column and table instead of a mid-`DELETE` SQL failure. They are written in **destination** column terms.
- The connection user needs the `CREATE TEMPORARY TABLES` privilege.

`cleanup` requires `write_mode: upsert` with a non-empty `key`, and `column_mapping: auto_map` — a single JSON payload column has no real columns for the scope predicate to address.

## Effectively-once delivery

`MysqlSink` implements `Sink::supports_idempotent_writes` (`true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and UPSERTs the `token` into a `_faucet_commit_token(scope VARCHAR, token VARCHAR)` watermark table inside the **same transaction**, so both commit together or neither does.
- `last_committed_token(scope)` — reads the current watermark so the pipeline can skip already-committed pages on resume.

Set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is **not** permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://faucet:faucet@localhost:3306/appdb
      server_id: 1
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: change_events
      column_mapping: auto_map
  state:
    type: file
    config: { path: ./state }
delivery: exactly_once
```

See the [effectively-once cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) for the full rationale and the supported source/sink set.

## Schema evolution

`MysqlSink` reports its live destination schema via `current_schema()` (read from `INFORMATION_SCHEMA.COLUMNS`, including `IS_NULLABLE`), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real table. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `MysqlSink::evolve_schema()` applies additive DDL:

- **New columns** → `ADD COLUMN`. MySQL has no `ADD COLUMN IF NOT EXISTS`, so the current column set is read first and an `ADD COLUMN` is emitted only for names not already present (idempotent by pre-check).
- **Lossless widenings** (e.g. integer → number) → `MODIFY COLUMN` — gated on `allow_type_widening`; re-running the same `MODIFY` is a no-op.
- **Nullability relaxations** → the column is re-emitted at its current mapped type with an explicit `NULL` (`MODIFY COLUMN`).

Incompatible changes (narrowing / type swaps) are never auto-applied — they are routed by `on_incompatible` (`fail` or `quarantine`). See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Dead-letter queue

In `upsert`/`delete` mode the sink overrides `Sink::write_batch_partial`: good rows are applied and only the rows whose key could not be extracted (missing/null key) are reported as `Err`, so the pipeline routes them to the configured DLQ per-row rather than failing the whole page. In `append` mode it delegates to `write_batch`. Add a `dlq:` block to your pipeline to capture those rows; without one, a bad key fails the batch.

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: ${env:MYSQL_URL}
      table_name: products
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  dlq:
    type: jsonl
    config: { path: ./dlq/mysql.jsonl }
```

## Config loading & schema

Load from YAML/JSON or environment, and inspect the full JSON Schema:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_mysql::MysqlSinkConfig;

// From a JSON file
let config: MysqlSinkConfig = load_json("config.json")?;
// From an .env file with a prefix
let config: MysqlSinkConfig = load_env_file(".env", "MYSQL_SINK")?;
```

```env
MYSQL_SINK_CONNECTION_URL=mysql://writer:s3cret@db.example.com:3306/analytics
MYSQL_SINK_TABLE_NAME=raw_events
MYSQL_SINK_COLUMN_MAPPING='{"json":{"column":"data"}}'
MYSQL_SINK_BATCH_SIZE=1000
MYSQL_SINK_MAX_CONNECTIONS=5
```

```bash
faucet schema sink mysql
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = MysqlSinkConfig::new("mysql://writer:pass@localhost:3306/app", "orders")
    .column_mapping(MysqlColumnMapping::AutoMap)
    .with_batch_size(1000)
    .max_connections(10);

let sink = MysqlSink::new(config).await?;

let records = vec![
    json!({"id": "o1", "status": "paid", "amount": 29.99}),
    json!({"id": "o2", "status": "pending"}),
];
let rows = sink.write_batch(&records).await?;
println!("wrote {rows} rows");
# Ok(())
# }
```

Drive it end-to-end via `Pipeline::new(source, sink).run()`.

## How it works

1. `MysqlSink::new()` builds a `sqlx::MySqlPool` **once** with the configured `max_connections`.
2. `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`) and inserts each chunk with a single multi-row `INSERT`.
3. **JSON mode** — each record is serialized to a JSON string: `INSERT INTO t (col) VALUES (?), (?), ...`.
4. **AutoMap mode** — column names are read from `INFORMATION_SCHEMA.COLUMNS` for the current database; a multi-row INSERT is built dynamically with `?` placeholders, bound as native MySQL types. The column set is the union of record keys across the batch; the INSERT is sub-chunked so `rows × columns` stays under the 65,535-placeholder limit.
5. **Upsert/delete** — `plan_writes` partitions the page (last-write-wins) and the resulting upserts/deletes are applied in one transaction.
6. All identifiers are backtick-quoted with MySQL-safe escaping (embedded backticks doubled).

## Lineage dataset URI

`mysql://<host>:<port>/<db>?table=<table>` (credentials stripped) — e.g. `mysql://host:3306/app?table=orders`.

This connector reports observability metrics under the label `connector="mysql"`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-mysql` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `MySQL pool acquire failed` / connection refused | Wrong `connection_url`, DB unreachable, or credentials rejected. Run `faucet doctor` — the sink's `SELECT 1` probe pinpoints connectivity vs auth. |
| `Packet too large` / `max_allowed_packet` error | A single `INSERT` exceeded the server limit. Lower `batch_size` (or set a smaller upstream page size when using `batch_size: 0`), or raise the server's `max_allowed_packet`. |
| Records silently not written in AutoMap mode | The record's keys don't match any existing column (extra keys are ignored; a record with **no** matching keys is skipped with a warning). Confirm the table columns match the JSON keys, or switch to JSON column mode. |
| `upsert`/`delete` rejected at validate time | `write_mode: upsert`/`delete` requires `column_mapping: auto_map` and a non-empty `key`. Fix the config; `faucet validate` catches this before any run. |
| Upsert updates nothing / inserts duplicates | The table has no PRIMARY/UNIQUE index on the `key` columns, so `ON DUPLICATE KEY UPDATE` never detects a conflict. Add the index. |
| `mysql upsert: row N: ...` (missing/null key) | A record had a null/absent key column. Add a `dlq:` block to route those rows per-row, or fix upstream. |
| Effectively-once config rejected | `delivery: exactly_once` requires a CDC source, an idempotent sink (this one), a `state:` block, and **no** DLQ. All four are validated at load time. |
| Numbers stored as text in AutoMap mode | Values bind by JSON type. A numeric column receiving a JSON string stores text. Cast upstream with a transform, or send JSON numbers. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) — write modes & `cdc_unwrap`.
- [State & effectively-once cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) — resumable and effectively-once runs.
- [`faucet-source-mysql`](https://crates.io/crates/faucet-source-mysql) · [`faucet-source-mysql-cdc`](https://crates.io/crates/faucet-source-mysql-cdc) — the MySQL query and CDC sources.


## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field (narrowing later is the `schema:` drift policy's job).

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Overwrite (`write_mode: overwrite`)

Full-refresh: each run atomically **replaces** the whole table. Writes are
staged into a `CREATE TABLE … LIKE` clone (`{table}__faucet_ovw`) and published
with an atomic `RENAME TABLE` swap (MySQL auto-commits DDL, so a rename — not a
transaction — is what makes the swap atomic) only after the run succeeds, so a
mid-run failure leaves the previous rows intact. No `key` is needed; a missing target is created by the
first run (staged from the first page, then renamed into place at commit — a
failed first run leaves no table) when `create_table: true`.

## Rollback (`faucet rollback`, #706)

With a top-level `rollback:` block in the pipeline config, every run of this
sink (column mapping only) is **undoable**: the run-id column
(`_faucet_run_id`, via `metadata_columns`) is stamped, the before-image of
every key an upsert / delete touches is journaled into `_faucet_run_journal`
**in the same transaction** as the write, and an overwrite keeps the replaced
table as `<table>__faucet_prev` (the swap `RENAME`s the old table aside instead of dropping it; the journal keys on a stored SHA-256 of the key JSON to stay under InnoDB's index size limit). `faucet rollback --run <id>` then
deletes the run's appended rows, restores the journaled before-images, or
swaps the previous table back — refusing (unless `--force`) when a later run
changed the same keys — and rewinds the row's bookmark and exactly-once
watermark. Sink hooks: `supports_rollback`, `rollback_run`, `forget_run`,
`rewind_commit_token`, `readback_source` (the `mysql` source config
`faucet verify` reads the destination back with). See the [rollback
cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/rollback.html).
