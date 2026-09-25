# faucet-sink-postgres

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-postgres.svg)](https://crates.io/crates/faucet-sink-postgres)
[![Docs.rs](https://docs.rs/faucet-sink-postgres/badge.svg)](https://docs.rs/faucet-sink-postgres)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-postgres.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-postgres.svg)](https://github.com/faucet-hq/faucet-stream#license)

PostgreSQL **sink** connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to a Postgres table over a pooled `sqlx` connection, batching them into multi-row `INSERT` statements for high throughput.

Reach for it whenever you want to land a faucet-stream source — a REST API, a CDC stream, a file, a queue — into Postgres with one declarative config and no glue code. Store records verbatim as a single `jsonb` column for schemaless ingestion, or map JSON keys straight onto typed table columns. With `write_mode: upsert` and a CDC source it becomes a live mirror; with `delivery: exactly_once` it commits records and a watermark in one transaction.

## Feature highlights

- **Two column-mapping modes** — **JSONB** stores each record as a single `jsonb` value (schemaless ingestion, query later with JSON operators); **AutoMap** maps top-level JSON keys directly onto typed table columns discovered from the Postgres catalog.
- **Multi-row `INSERT` batching** — each page is written with one multi-row `INSERT` per chunk; JSONB mode uses `unnest($1::jsonb[])`, AutoMap binds per-column casts. Auto-splits to stay under Postgres' 65 535 bind-parameter ceiling.
- **Write modes: upsert & delete** — merge or remove rows by key via `INSERT … ON CONFLICT (key) DO UPDATE` (AutoMap + a `UNIQUE`/`PRIMARY KEY` on the key columns required). Last-write-wins de-dup within a batch.
- **Effectively-once delivery** — records and a monotonic commit token UPSERT into a `_faucet_commit_token` watermark table inside the **same transaction**; resume skips already-committed pages.
- **Dead-letter queue** — per-row partial writes route missing/null-key rows to a configured DLQ while good rows still commit.
- **Connection pooling** — one `sqlx::PgPool` built once in `new()` and reused for every batch; `max_connections` is configurable.
- **Schema-qualified targets** — optional `schema` scopes both column discovery and the `INSERT` target, so a same-named table in another schema can't pollute the AutoMap column set.
- **Credential-safe logging** — the `Debug` impl masks `connection_url` with `***`.

## Installation

```bash
# As a library:
cargo add faucet-sink-postgres
cargo add tokio --features full

# Or via the umbrella crate:
cargo add faucet-stream --features sink-postgres

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-postgres
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      endpoint: /v1/users
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/app
      table_name: users
      column_mapping: auto_map
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | PostgreSQL connection URL, e.g. `postgres://user:pass@host:5432/db`. Masked in logs. |
| `table_name` | string | — *(required)* | Target table name. |
| `schema` | string | *(unset)* | Schema (namespace) qualifying `table_name`. When set, both AutoMap column discovery and the `INSERT` target `schema.table_name` explicitly. When unset, the table resolves against the connection's `search_path`. |
| `column_mapping` | `PostgresColumnMapping` | `{ jsonb: { column: "data" } }` | How to map JSON records to columns — see [Column mapping](#column-mapping). |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum rows per multi-row `INSERT` (or per `COPY` under `write_method: copy`). **`0` = no batching** — the whole page is sent in one statement. See [Streaming & batching](#streaming--batching). |
| `max_connections` | int | `5` | Maximum connections in the `sqlx` pool. |
| `write_method` | `"insert" \| "copy"` | `"insert"` | How append-mode rows are shipped. `copy` uses the `COPY … FROM STDIN` bulk-load fast-path — see [Bulk load](#bulk-load-write_method-copy). |

### Write mode

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | `"append" \| "upsert" \| "delete"` | `"append"` | How records are applied. See [Write modes](#write-modes-upsert--delete). |
| `key` | `[string]` | `[]` | Key columns identifying a row. **Required and non-empty** for `upsert`/`delete`. Composite keys are supported. |
| `delete_marker` | `{ field, values: [string] }` | *(none)* | **Upsert only.** Records whose `field` equals one of `values` are deleted by key; all others are upserts. The marker field is stripped from upserted rows before writing. |

### Column mapping

`column_mapping` is the adjacently-tagged `PostgresColumnMapping` enum:

| Variant | YAML | Description |
|---------|------|-------------|
| `Jsonb { column }` | `{ jsonb: { column: data } }` | Insert each record as a single `jsonb` column (default name `"data"`). Uses `unnest($1::jsonb[])` for efficient batch inserts. |
| `AutoMap` | `auto_map` | Map top-level JSON keys directly to table columns. Column names + types are discovered from the catalog, scoped (via `to_regclass`) to exactly the relation the `INSERT` targets. Only keys matching existing columns are inserted; extra keys are silently ignored. Records with no matching keys are skipped with a warning. |

## Examples

### JSONB mode — store entire records in one column

Ideal for schemaless ingestion: store raw JSON and query it later with Postgres' JSONB operators.

```sql
CREATE TABLE raw_events (
    id         SERIAL PRIMARY KEY,
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

```yaml
sink:
  type: postgres
  config:
    connection_url: postgres://writer:s3cret@db.example.com:5432/analytics
    table_name: raw_events
    column_mapping:
      jsonb:
        column: data
    batch_size: 1000
    max_connections: 5
```

### AutoMap mode — map JSON keys to typed columns

Discovers column names and types from the table schema and maps matching JSON keys. A field present only in some records is still written; rows missing a column bind SQL `NULL`.

```sql
CREATE TABLE events (
    user_id   TEXT,
    event     TEXT,
    timestamp TIMESTAMPTZ,
    amount    NUMERIC
);
```

```yaml
sink:
  type: postgres
  config:
    connection_url: postgres://writer:s3cret@db.example.com:5432/analytics
    table_name: events
    column_mapping: auto_map
    batch_size: 1000
    max_connections: 10
```

### CDC mirror — upsert with a delete marker

Pair a CDC source (run through the `cdc_unwrap` transform) with `write_mode: upsert` to keep a destination table in lock-step with the source.

```yaml
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/warehouse
      table_name: users
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker:
        field: __op
        values: [d]
```

The destination must define the key as a constraint, e.g. `CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)`.

### High-throughput pool

```yaml
sink:
  type: postgres
  config:
    connection_url: postgres://writer:pass@db-primary.internal:5432/warehouse
    table_name: metrics
    column_mapping: auto_map
    max_connections: 20
    batch_size: 1000
```

## Streaming & batching

The sink re-chunks each incoming `StreamPage` so individual multi-row `INSERT` statements stay well under Postgres' per-statement bind-parameter limit.

- **`batch_size > 0`** (default `1000`) — slice the incoming page into `batch_size`-row chunks, one multi-row `INSERT` per chunk. **`1000` is the recommended value** — Postgres' multi-row `INSERT` sweet spot. AutoMap binds one parameter per column per row; the sink sub-splits each chunk further so `rows × columns` never exceeds Postgres' 65 535 bind-parameter ceiling, so a wide table never causes a rejected statement. JSONB mode binds a single `jsonb[]` array regardless of row count.
- **`batch_size = 0`** — the "no batching" sentinel: the entire upstream page is forwarded in a single logical write. Use it when the source already emits Postgres-tuned page sizes. AutoMap still sub-splits internally to respect the 65 535-parameter ceiling.

`batch_size` is purely a chunk-size knob — connection pooling, identifier quoting, and JSONB vs AutoMap behaviour are unchanged.

This connector reports observability metrics under the label `connector="postgres"`.

## Bulk load (`write_method: copy`)

For **append-mode bulk loads**, set `write_method: copy` to ship rows via
`COPY … FROM STDIN (FORMAT text)` instead of multi-row `INSERT` — typically
**5–10× faster** at the destination, because `COPY` skips per-statement
planning/binding overhead entirely (issue #308).

```yaml
sink:
  type: postgres
  config:
    connection_url: ${env:PG_URL}
    table_name: events
    column_mapping: auto_map     # jsonb mapping works too
    write_method: copy
```

Semantics are identical to the `INSERT` path — same rows, same target, same
durability. The server parses every field with the destination column's
*input function*, exactly like the `INSERT` path's `$N::<udt>` casts, so
timestamps, numerics, booleans, and JSONB all land the same way. Values are
escaped for COPY's text format (tabs, newlines, backslashes, control
characters), and the column set is the same union-of-present-fields the
`INSERT` path computes (columns absent from every record keep their
`DEFAULT`).

Restrictions and interactions:

- **Append-only.** `COPY` has no `ON CONFLICT`, so `write_method: copy` +
  `write_mode: upsert|delete` is rejected at config load with a typed error.
- **All-or-nothing per batch.** One bad row fails the whole `COPY` batch —
  the same behaviour as a failed multi-row `INSERT`. With a `dlq:` block the
  DLQ router's `on_batch_error` policy applies (`dlq_all` routes the failed
  page's rows to the DLQ; `propagate` aborts the run).
- **Exactly-once is unaffected.** Under `delivery: exactly_once` the
  watermark path (`write_batch_idempotent`) always uses the
  `INSERT`/transaction machinery regardless of `write_method`, because the
  page and its commit token must land in one atomic transaction.

## Destination tuning

Knobs that make bulk loads dramatically faster but change durability or
consistency guarantees are **left to you on the destination** — faucet never
flips them silently:

- **`UNLOGGED` tables** — skip WAL entirely for the target table. Fastest
  possible ingest; the table is truncated on crash recovery and is not
  replicated. Suitable for staging tables you can re-load.
- **`SET synchronous_commit = off`** (per session/role/database) — commits
  return before WAL reaches disk. A crash can lose the last few transactions
  (no corruption). Often a large win on spinning disks or busy WAL devices.
- **Drop/disable indexes and constraints before the load, rebuild after** —
  index maintenance frequently dominates bulk-insert cost; one bulk rebuild
  is much cheaper than a million incremental updates.

See the [throughput tuning guide](https://faucet-hq.github.io/faucet-stream/cookbook/tuning.html)
for the full decision table.

## Write modes (upsert / delete)

By default the sink **appends** every record. Set `write_mode` to `upsert` or `delete` to merge or remove rows by a key instead.

Requirements (validated in `PostgresSink::new`, before any connection is made):

- **`column_mapping: auto_map` is required.** Upsert/delete match on real columns, so the key columns must be table columns — not fields buried inside a JSONB blob.
- **The `key` columns must carry a `UNIQUE` or `PRIMARY KEY` constraint**, since upsert is implemented with `INSERT … ON CONFLICT (key) DO UPDATE SET …` (non-key columns set from `EXCLUDED`; if every column is a key column the clause degrades to `DO NOTHING`). Without the constraint Postgres rejects the `ON CONFLICT` target.

Semantics:

- **Last-write-wins within a batch.** When the same key appears multiple times in one `write_batch` call, the records are de-duplicated to a single effective action (the final one), so a single statement never hits the same `ON CONFLICT` target twice. A delete after an upsert (or vice-versa) for the same key resolves to whichever came last.
- **`write_mode: delete`** routes every record to a delete by key.
- A record missing a key column (or with a `null` key value) fails with a typed `Sink` error. When a `dlq:` block is configured the good rows are still written (upserts + deletes applied) and only the missing/null-key rows are routed to the DLQ per-row; without a DLQ the whole batch fails.

The `cdc_unwrap` transform pairs naturally with upsert — it normalizes a CDC envelope into a flat row plus a `__op` marker (`"u"`/`"d"`) that the `delete_marker` matches. See the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

## Effectively-once delivery

`PostgresSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and UPSERTs the `token` into a `_faucet_commit_token(scope TEXT, token TEXT)` watermark table inside the **same transaction**, so both either commit together or neither does.
- `last_committed_token(scope)` — reads the current watermark so the pipeline skips already-committed pages on resume.

To use effectively-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/warehouse
      table_name: change_events
      column_mapping: auto_map
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

`delivery: exactly_once` and `write_mode: upsert` compose — the upsert and the commit-token UPSERT commit in the same transaction. See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).

## Schema evolution

`PostgresSink` reports its live destination schema via `current_schema()` (read from `pg_catalog`, including `attnotnull` so nullability round-trips), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real table. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `PostgresSink::evolve_schema()` applies additive DDL in one connection:

- **New columns** → `ALTER TABLE … ADD COLUMN IF NOT EXISTS` (idempotent).
- **Lossless widenings** (e.g. integer → number) → `ALTER COLUMN … TYPE` — gated on `allow_type_widening`.
- **Nullability relaxations** (a previously `NOT NULL` column absent from the page) → `ALTER COLUMN … DROP NOT NULL`.

Incompatible changes (narrowing / type swaps) are never auto-applied — they are routed by `on_incompatible` (`fail` or `quarantine`). See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Dead-letter queue

The sink overrides `Sink::write_batch_partial`, so when a `dlq:` block is configured the router gets per-row outcomes: good rows commit and only the failing rows (e.g. missing/null key columns under `write_mode: upsert`/`delete`) are wrapped in a DLQ envelope and routed to the DLQ sink — the batch is not aborted. Without a DLQ, a row failure fails the whole batch. See the [DLQ cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html).

## Config loading & schema introspection

Load from YAML/JSON, environment variables, or a `.env` file via `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_postgres::PostgresSinkConfig;

let config: PostgresSinkConfig = load_json("config.json")?;
let config: PostgresSinkConfig = load_env_file(".env", "PG_SINK")?;
```

```env
# .env (prefix PG_SINK)
PG_SINK_CONNECTION_URL=postgres://writer:s3cret@db.example.com:5432/analytics
PG_SINK_TABLE_NAME=raw_events
PG_SINK_COLUMN_MAPPING='{"jsonb":{"column":"data"}}'
PG_SINK_BATCH_SIZE=1000
PG_SINK_MAX_CONNECTIONS=5
```

Inspect the full JSON Schema with:

```bash
faucet schema sink postgres
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = PostgresSinkConfig::new("postgres://writer:pass@localhost:5432/app", "events")
    .column_mapping(PostgresColumnMapping::AutoMap)
    .with_batch_size(1000)
    .max_connections(10);

let sink = PostgresSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "event": "purchase", "amount": 29.99}),
    json!({"user_id": "u2", "event": "signup"}), // missing "amount" → NULL
];
let rows = sink.write_batch(&records).await?;
println!("wrote {rows} rows");
# Ok(())
# }
```

Drive it from a full pipeline:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/users"));
let sink = PostgresSink::new(
    PostgresSinkConfig::new("postgres://writer:pass@localhost:5432/app", "users")
        .column_mapping(PostgresColumnMapping::AutoMap),
)
.await?;

let result = Pipeline::new(source, sink).run().await?;
println!("transferred {} records", result.records_written);
# Ok(())
# }
```

## How it works

- A `sqlx::PgPool` is created once in `PostgresSink::new()` with the configured `max_connections` and reused for every batch.
- `write_batch()` slices records into `batch_size` chunks (or forwards the whole slice when `batch_size = 0`) and inserts each chunk with a single multi-row `INSERT`.
- **JSONB mode** inserts via `INSERT INTO table (col) SELECT * FROM unnest($1::jsonb[])` — one bound array, no per-row parameters.
- **AutoMap mode** queries each column's name **and underlying type** (`udt_name`) from the catalog, scoped via `to_regclass` to exactly the relation the `INSERT` targets (the configured `schema`, else the `search_path`-resolved table). A multi-row `INSERT INTO ... VALUES ($1::int4, $2::timestamptz), ...` is built dynamically with a per-column cast; each value is bound as text so the destination column's input function parses it — numbers, booleans, timestamps, uuids, and `json`/`jsonb` columns all land in their native types. The column set is the **union** of record keys across the batch (in declared table order); a row missing a column binds SQL `NULL`.
- All identifiers (table + column names) are quoted with `quote_ident()` to prevent SQL injection.

## Lineage dataset URI

`postgres://<host>:<port>/<db>?table=<schema.table>` (credentials stripped) — e.g. `postgres://host:5432/app?table=public.orders`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-postgres` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| Connection refused / auth failed | Check the `connection_url` host/port/credentials and that the role can connect to the target database. The URL is masked in logs, so verify it in the config. |
| `Sink` error: upsert/delete requires `auto_map` | `write_mode: upsert`/`delete` only works in AutoMap mode. Set `column_mapping: auto_map`. |
| `ON CONFLICT` rejected / no unique constraint | The `key` columns need a `UNIQUE` or `PRIMARY KEY` constraint on the table. Add one, e.g. `ALTER TABLE t ADD PRIMARY KEY (id)`. |
| `key` empty for upsert/delete | `key` must be non-empty for `upsert`/`delete`. List the key column(s), e.g. `key: [id]`. |
| Rows silently dropped (AutoMap) | A record had no keys matching existing columns — logged as a warning. Verify the JSON keys match column names (case-sensitive). Extra keys are ignored by design. |
| Wrong table picked up | A same-named table exists in another schema on the `search_path`. Set `schema:` explicitly to disambiguate. |
| Statement rejected with too many parameters | Hit the 65 535 bind-parameter ceiling. AutoMap auto-splits to stay under it; if you still see this, lower `batch_size` (very wide tables). |
| Missing/null key rows fail the whole batch | Without a `dlq:` block, a row missing a key column aborts the batch. Configure a DLQ to route just the bad rows, or ensure keys are present and non-null. |
| Effectively-once config rejected at validate | Effectively-once requires a CDC source + idempotent sink + `state:` block + no `dlq:`. `faucet validate` names the missing requirement. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix across all connectors.
- [Upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) — write modes and CDC mirroring.
- [Effectively-once delivery](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) — state, watermarks, supported source/sink set.
- [Dead-letter queue cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html) — routing bad rows.
- [`faucet-source-postgres`](https://crates.io/crates/faucet-source-postgres) — the matching query source.
- [`faucet-source-postgres-cdc`](https://crates.io/crates/faucet-source-postgres-cdc) — the CDC source that pairs with upsert/effectively-once.


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
staged into a `LIKE` clone (`{table}__faucet_ovw`) and swapped in one
transaction (`TRUNCATE` + `INSERT … SELECT` + `DROP`) only after the run
succeeds, so a mid-run failure leaves the previous rows intact. No `key` is
needed; a missing target is created by the first run
(staged from the first page, then renamed into place at commit — a failed first
run leaves no table) when `create_table: true`. See the
[Overwrite cookbook](../../../docs/book/src/cookbook/upsert.md).

**Scoped / windowed overwrite (#518):** add a `scope: { window: { column, from, to } }`
to replace only the rows in a half-open `[from, to)` window — the swap becomes
`DELETE FROM target WHERE <window>; INSERT … SELECT` in one transaction, leaving
out-of-window rows intact.
