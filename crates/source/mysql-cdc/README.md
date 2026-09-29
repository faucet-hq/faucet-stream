# faucet-source-mysql-cdc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mysql-cdc.svg)](https://crates.io/crates/faucet-source-mysql-cdc)
[![Docs.rs](https://docs.rs/faucet-source-mysql-cdc/badge.svg)](https://docs.rs/faucet-source-mysql-cdc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mysql-cdc.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mysql-cdc.svg)](https://github.com/faucet-hq/faucet-stream#license)

MySQL **Change Data Capture (CDC)** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Tails the MySQL binary log via row-based replication and emits every `INSERT` / `UPDATE` / `DELETE` (and optionally DDL) as a structured JSON change event.

Reach for it when you want to mirror a MySQL database into a warehouse, lake, queue, or search index in near-real-time — without polling, without modifying the source schema, and without dropping a single committed row. Bookmarks are persisted as binlog `{ file, pos }` coordinates, so a restarted pipeline resumes from the exact position it left off: no gap, no duplicate.

## Feature highlights

- **Row-level change events** — each committed transaction is decoded from the binlog into a Debezium-style envelope (`op` / `before` / `after` / `lsn` / `txid`) and emitted as JSON.
- **Per-transaction durability** — every committed transaction is its own `StreamPage` with a `{ file, pos }` bookmark attached, so the pipeline persists progress per commit. Uncommitted partial transactions never leak: they're buffered in memory and discarded at idle timeout, then re-delivered from the last persisted bookmark on the next run.
- **Resumable** — overrides `state_key()` / `apply_start_bookmark()`; on resume the binlog stream reopens from the persisted file/position.
- **Effectively-once delivery** — `supports_exactly_once()` is `true`. Pair with an idempotent sink (postgres / mysql / mssql / sqlite / iceberg / bigquery) + a state store for end-to-end effectively-once semantics, gated and validated by the CLI.
- **Snapshot → CDC handoff** — implements `capture_resume_position()` (anchor the binlog *before* a bulk snapshot); `faucet replicate` uses it to build a gap-free, duplicate-free mirror. Works against MySQL **5.7 / 8.0 / 8.4+** — current-position capture tries `SHOW BINARY LOG STATUS` (8.4) and falls back to `SHOW MASTER STATUS` (5.7 / 8.0).
- **Client-side table filtering** — `include_tables` / `exclude_tables` allowlist/blocklist by fully-qualified `database.table`.
- **Flexible start positions** — `current`, `earliest`, explicit `file_pos`, or `gtid_set`.
- **TLS** — `disable` / `require` / `verify_ca` / `verify_full` for the replication connection.
- **OOM guard** — `max_staged_records` caps a single in-progress transaction's in-memory buffer and aborts cleanly rather than risking OOM on an unexpectedly huge transaction.

## Installation

```bash
# As a library:
cargo add faucet-source-mysql-cdc

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-mysql-cdc
```

## Server prerequisites

The MySQL server must have binary logging configured for **row-based** replication. All four settings are verified at startup; the connector fails fast with a clear error if any are missing.

### Required server variables

Set these in `my.cnf` (or pass as `--option` flags to `mysqld`):

```ini
[mysqld]
server-id           = 1          # any non-zero value, unique per server
log_bin             = mysql-bin  # enable binary logging
binlog_format       = ROW        # required: row-level events (not STATEMENT/MIXED)
binlog_row_image    = FULL       # required: full before/after images
binlog_row_metadata = FULL       # REQUIRED for column names in the envelope
binlog_row_value_options =       # REQUIRED empty: full JSON (NOT partial_json)
```

> **`binlog_row_metadata=FULL` is critical.** It is `MINIMAL` by default in MySQL 8.0. Without `FULL`, column names are absent from row events and the connector cannot decode them — startup fails.

> **`binlog_row_value_options` must be empty (full JSON).** With `binlog_row_value_options=PARTIAL_JSON`, an UPDATE that touches a JSON column writes only a *partial diff* of the document to the binlog. faucet-stream does not buffer prior row state, so the diff cannot be reconstructed — emitting it would silently corrupt the JSON column. Startup therefore fails fast when `PARTIAL_JSON` is set; clear it (`binlog_row_value_options=''`) and restart MySQL for CDC.

### Required user grants

The replication user needs `REPLICATION SLAVE` and `REPLICATION CLIENT` on `*.*`:

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
```

### GTID mode (only for `start_position: gtid_set`)

If you use `start_position: { type: gtid_set, value: "…" }`, the server must also have GTID mode enabled:

```ini
gtid_mode                = ON
enforce_gtid_consistency = ON
```

`current`, `earliest`, and `file_pos` start positions work with or without GTID mode.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: mysql-cdc
    config:
      # The binlog is global: mysql-cdc reads all databases and filters
      # client-side via include_tables / exclude_tables. No default DB needed.
      connection_url: mysql://repl:repl@localhost:3306
      server_id: 1001
      start_position:
        type: current
      include_columns: true
      idle_timeout: 30
  sink:
    type: file
    config:
      path: ./changes.jsonl
  state:
    type: file
    config:
      path: ./state/mysql_cdc.json
```

```bash
faucet run pipeline.yaml
```

A `state:` block is what makes the pipeline resumable — without it, each run restarts from `start_position`.

## Output record schema (CDC envelope)

Every change event is one JSON object:

```json
{
  "op": "c",
  "ts_ms": 1779019200000,
  "schema": "appdb",
  "table": "users",
  "before": null,
  "after": { "id": 1, "name": "alice", "email": "alice@example.com" },
  "lsn": { "file": "mysql-bin.000003", "pos": 4567 },
  "txid": 42
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | Operation: `c` (insert), `u` (update), `d` (delete), `ddl` (schema change, when `emit_schema_changes: true`). |
| `ts_ms` | number | Wall-clock time of the binlog event in Unix-epoch milliseconds (from the event header timestamp). |
| `schema` | string | Source database (schema) name. |
| `table` | string | Source table name. |
| `before` | object \| null | Pre-image of the row. Populated on updates and deletes when `include_columns: true`. `null` on inserts, or when `include_columns: false`. |
| `after` | object \| null | Post-image of the row. Populated on inserts and updates. `null` on deletes. |
| `lsn` | object | Binlog coordinates of the commit event: `{ "file": "mysql-bin.000003", "pos": 4567 }`. Used as the persisted bookmark. |
| `txid` | number | Monotonically increasing per-session transaction counter (resets to 0 on each `faucet run`). Useful for grouping rows from the same transaction. |

### `op` mapping

| Binlog event | `op` value |
|---|---|
| `WriteRowsEvent` (INSERT) | `c` |
| `UpdateRowsEvent` (UPDATE) | `u` |
| `DeleteRowsEvent` (DELETE) | `d` |
| DDL (`QueryEvent` other than BEGIN/COMMIT), when `emit_schema_changes: true` | `ddl` |

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | MySQL connection URL, e.g. `mysql://repl:pass@host:3306/db`. The path component is optional — the binlog is global. |
| `server_id` | u32 | — *(required)* | Replica server ID. Must be non-zero and **unique** across every replication client connected to this server. |
| `start_position` | enum | `{ type: current }` | Where to start on a fresh run (no persisted bookmark). See [Start positions](#start-positions). |

### Filtering

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include_tables` | string[] | `[]` (all) | Client-side allowlist of fully-qualified `database.table` names. Empty = all tables. Takes precedence over `exclude_tables`. |
| `exclude_tables` | string[] | `[]` | Client-side blocklist of fully-qualified `database.table` names. Ignored when `include_tables` is non-empty. |

Both lists must use fully-qualified names (e.g. `appdb.users`); an unqualified entry fails validation.

### Payload

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include_columns` | bool | `true` | Emit the pre-image (`before`) on updates and deletes. Set `false` to suppress before-images and shrink payloads. |
| `emit_schema_changes` | bool | `false` | Emit DDL statements (CREATE/ALTER/DROP TABLE etc.) as `{ op: "ddl" }` records. A DDL implicitly auto-commits any in-progress transaction in MySQL; any rows already buffered for that transaction are flushed (and the bookmark advanced) **before** the DDL is processed, so no rows are lost on resume regardless of this setting. |

### Reliability & batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `idle_timeout` | int (seconds) | `30` | End the fetch cycle after this long with no new events. The accumulated page is flushed and the bookmark saved. Must be `> 0`. |
| `max_staged_records` | int \| null | `null` | Abort if a single in-progress transaction buffers more than this many rows. `null` = unbounded. Set it to guard against OOM on bulk transactions. |
| `batch_size` | int | `1000` | Advisory page-size hint. `0` = accumulate all committed transactions into one trailing page (useful for tiny lookup tables). Per-transaction streaming still emits one page per commit when the pipeline drives `stream_pages`. |

### TLS

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tls` | enum | `{ mode: disable }` | TLS for the replication connection. See [TLS modes](#tls-modes). |

### Start positions

| Variant | Description |
|---------|-------------|
| `{ type: current }` | Start at the server's current binlog position — skip history. **Default.** |
| `{ type: earliest }` | Start from the oldest available binlog file. Errors if binlogs have been purged past the earliest available point. |
| `{ type: file_pos, file: "mysql-bin.000003", pos: 4567 }` | Resume from an explicit file/position. |
| `{ type: gtid_set, value: "uuid:1-1000" }` | Start after an executed GTID set. Requires `gtid_mode=ON` on the server. |

A persisted bookmark always wins over `start_position` — the latter only applies on a fresh run.

### TLS modes

| Mode | Description |
|------|-------------|
| `{ mode: disable }` | No TLS (default). Credentials and row data travel cleartext. |
| `{ mode: require }` | Require TLS but do not verify the server certificate. |
| `{ mode: verify_ca, ca_path: "/path/to/ca.pem" }` | Require TLS and verify the certificate chain. `ca_path` optional (uses system roots if omitted). |
| `{ mode: verify_full, ca_path: "/path/to/ca.pem" }` | Require TLS and verify both the chain and the hostname. `ca_path` optional. |

## Examples

### Capture two tables and land them in BigQuery

```yaml
version: 1
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://repl:${env:REPL_PASSWORD}@db.prod.internal:3306/myapp
      server_id: 2001
      start_position:
        type: current
      include_tables:
        - myapp.orders
        - myapp.order_items
      include_columns: true
      idle_timeout: 60
      max_staged_records: 100000
      batch_size: 500
  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: cdc
      table_id: mysql_changes
      credentials:
        type: service_account_file
        config:
          path: /secrets/bq-sa.json
  state:
    type: redis
    config:
      url: redis://localhost:6379
      namespace: faucet-cdc
```

### GTID-based start with full TLS verification

```yaml
version: 1
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://repl:secret@db.internal:3306/events
      server_id: 3001
      start_position:
        type: gtid_set
        value: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23"
      tls:
        mode: verify_full
        ca_path: /etc/ssl/certs/mysql-ca.pem
      idle_timeout: 30
  sink:
    type: stdout
    config:
      format: pretty
  state:
    type: file
    config:
      path: ./state/mysql_cdc.json
```

### Exclude noisy tables, suppress before-images

```yaml
version: 1
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://repl:repl@localhost:3306
      server_id: 4001
      exclude_tables:
        - appdb.audit_log
        - appdb.sessions
      include_columns: false   # smaller payloads — no before-image on update/delete
      emit_schema_changes: true
  sink:
    type: file
    config:
      path: ./changes.jsonl
  state:
    type: file
    config:
      path: ./state/mysql_cdc.json
```

## Streaming & batching

The source overrides `Source::stream_pages`. Binlog events are decoded and buffered per transaction; on a commit boundary (`XidEvent` for InnoDB, or an explicit `COMMIT` `QueryEvent`) the accumulated rows are emitted as a single `StreamPage` carrying the commit's `{ file, pos }` as its bookmark. The pipeline writes the page, flushes the sink, and persists the bookmark — giving you **per-transaction durability** out of the box.

`batch_size` is advisory: per-transaction streaming naturally emits one page per commit. With `batch_size: 0` all committed transactions seen before the idle timeout are accumulated into one trailing page (handy for tiny tables). `idle_timeout` bounds how long a fetch cycle waits for new events before flushing and returning.

## Resume & state

The connector is fully resumable. After each committed transaction the pipeline writes the binlog `{ file, pos }` of that commit's end event to the configured `StateStore`. On the next run, `apply_start_bookmark()` restores the coordinates and the binlog stream opens from that exact position — MySQL redelivers events from there with no duplicates and no gap.

**Bookmark shape** (the end-position of the commit event):

```json
{ "file": "mysql-bin.000003", "pos": 4567 }
```

All persisted bookmarks use `{ file, pos }` — even when `start_position` is `gtid_set`. This avoids accumulating executed-GTID intervals across sessions while still guaranteeing unambiguous resume: `{ file, pos }` is always available and honoured by the server regardless of whether GTID mode is on.

**State key** (one per `server_id`):

```
mysql-cdc:<server_id>
```

Example: `server_id: 1001` → state key `mysql-cdc:1001`.

Any [`StateStore`](https://crates.io/crates/faucet-core) works — `file` and `memory` ship in `faucet-core`; [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis) and [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres) provide durable shared backends.

## Effectively-once delivery

`MysqlCdcSource::supports_exactly_once()` returns `true`. Combined with an idempotent sink and a state store, a pipeline can run with `delivery: exactly_once` for end-to-end effectively-once semantics — the pipeline assigns a monotonic commit token per bookmark-carrying page, the sink commits records and token atomically, and on resume already-committed pages are skipped.

The CLI enforces all four requirements at config-load time (`faucet validate` catches them before any run starts):

1. **Source** must support effectively-once — `mysql-cdc` qualifies.
2. **Sink** must support idempotent writes — `postgres`, `mysql`, `mssql`, `sqlite`, `iceberg`, or `bigquery`.
3. A **`state:`** block must be configured.
4. **No `dlq:`** block (DLQ and effectively-once are mutually exclusive in this version).

```yaml
version: 1
delivery: exactly_once
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://repl:repl@localhost:3306
      server_id: 5001
  sink:
    type: postgres
    config:
      connection_url: postgres://app:app@localhost:5432/mirror
      table: mysql_changes
      auto_map: true
  state:
    type: postgres
    config:
      connection_url: postgres://app:app@localhost:5432/mirror
```

To build a true table mirror (rather than an append-only change log), pair the `cdc_unwrap` transform with a `write_mode: upsert` sink — see the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).

## Snapshot → CDC handoff

This source implements `capture_resume_position()`, which reads the server's **current binlog coordinates** (`{ file, pos }`) as a resume bookmark — without consuming any changes. The `faucet replicate` command uses it to anchor the binlog stream at-or-before a bulk snapshot of the table, so the combined snapshot + CDC result is a gap-free, duplicate-free mirror when paired with a `write_mode: upsert` sink.

There is no slot to pin; binlog retention is **time-based**, so keep your binlog retention window comfortably larger than the expected snapshot duration. If the captured position is purged before CDC starts, the stream errors that its start position is unavailable. See the [replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html) for the full handoff model.

## Config loading & schema

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source mysql-cdc
```

## Library usage

```rust,no_run
use faucet_core::Source;
use faucet_source_mysql_cdc::{MysqlCdcSource, MysqlCdcSourceConfig, StartPosition};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg: MysqlCdcSourceConfig = serde_json::from_value(serde_json::json!({
    "connection_url": "mysql://repl:repl@localhost:3306",
    "server_id": 1001,
    "start_position": { "type": "current" },
    "include_tables": ["appdb.users", "appdb.orders"],
    "idle_timeout": 30
}))?;

let source = MysqlCdcSource::new(cfg).await?;

// Drive it via a Pipeline / run_stream, attaching a StateStore for resumable bookmarks.
// (Direct fetch_all() is also available for a single fetch cycle.)
let _ = source;
# Ok(())
# }
```

For resumable / effectively-once runs, drive the source through `faucet_core::Pipeline` (or `run_stream`) with `Pipeline::with_state_store(...)` so bookmarks are read before each fetch and persisted only after the sink confirms each transaction.

## How it works

1. `new()` validates the config, opens a replication connection (honouring `tls`), and verifies the server variables (`log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`, and `binlog_row_value_options` empty — not `PARTIAL_JSON`).
2. The start position is resolved: a persisted bookmark wins; otherwise `start_position` (`current` capture tries `SHOW BINARY LOG STATUS` then falls back to `SHOW MASTER STATUS`).
3. Binlog events stream in; row events are decoded against the relation metadata (column names come from `binlog_row_metadata=FULL`) and buffered per transaction.
4. On each commit boundary, the buffered rows are emitted as one `StreamPage` with the commit's `{ file, pos }` bookmark; uncommitted buffers are bounded by `max_staged_records`.
5. The cycle ends after `idle_timeout` of quiet, flushing the final page and saving the bookmark.

## Lineage dataset URI

`mysql://<host>:<port>/<db>` or `mysql://<host>:<port>/<db>?tables=<t1>,<t2>` (credentials stripped) — e.g. `mysql://host:3306/app?tables=db.orders,db.users`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-mysql-cdc` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| Startup fails: `binlog_row_metadata` not `FULL` | The server has the MySQL-8.0 default of `MINIMAL`. Set `binlog_row_metadata=FULL` in `my.cnf` and restart — required for column names. |
| Startup fails: binary logging / `binlog_format` | `log_bin` is off or `binlog_format` is `STATEMENT`/`MIXED`. Enable `log_bin` and set `binlog_format=ROW` + `binlog_row_image=FULL`. |
| Startup fails: `binlog_row_value_options` is `PARTIAL_JSON` | The server logs partial JSON diffs, which can't be reconstructed for CDC. Set `binlog_row_value_options=''` (full JSON) and restart MySQL. |
| `Access denied` / replication error | The user lacks `REPLICATION SLAVE` / `REPLICATION CLIENT`. Grant both on `*.*` and `FLUSH PRIVILEGES`. |
| Another replica disconnects when this one connects | Two clients share a `server_id`. Pick a unique non-zero `server_id` per replication client. |
| `start_position: earliest` errors | Binlogs were purged (`expire_logs_days` / `PURGE BINARY LOGS`) past the earliest point. Use `current`, or widen binlog retention. |
| `gtid_set` start rejected by the server | `gtid_mode` is `OFF`. Set `gtid_mode=ON` + `enforce_gtid_consistency=ON`, or use a `file_pos` / `current` start instead. |
| Resume errors that the start position is unavailable | The persisted/captured `{ file, pos }` was purged before resume. Widen binlog retention so it exceeds expected downtime / snapshot duration. |
| A JSON column embeds a `DECIMAL`/`DATE`/temporal value | Such values are stored as JSONB *opaque* scalars. They are now preserved **losslessly**: `DECIMAL` → its exact decimal string, `DATE`/`TIME`/`DATETIME`/`TIMESTAMP` → a formatted temporal string, any other opaque type → a round-trippable `base64:type<N>:<…>` string. (Earlier versions dropped the whole column to `null`.) A JSON column only errors out if the binlog bytes are structurally corrupt, surfacing a typed `Source` error (DLQ-routable) rather than a silent `null`. |
| Pipeline never returns / hangs | It's waiting for events. Lower `idle_timeout` so the fetch cycle ends sooner on a quiet binlog. |
| OOM during a bulk load | A huge single transaction buffered entirely in memory before its commit. Set `max_staged_records` to a safe upper bound. |
| `Config` error on a table filter | An `include_tables` / `exclude_tables` entry isn't fully qualified. Use `database.table` (e.g. `appdb.users`). |
| TLS handshake fails | Wrong `tls` mode or CA. For self-signed servers use `require`; for verified chains set `verify_ca`/`verify_full` with a correct `ca_path`. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [CDC / state cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) · [Upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) · [Replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html)
- [faucet-source-mysql](https://crates.io/crates/faucet-source-mysql) — query-mode MySQL source (snapshots via SQL)
- [faucet-source-postgres-cdc](https://crates.io/crates/faucet-source-postgres-cdc) · [faucet-source-mongodb-cdc](https://crates.io/crates/faucet-source-mongodb-cdc) — sibling CDC sources
- [faucet-state-redis](https://crates.io/crates/faucet-state-redis) · [faucet-state-postgres](https://crates.io/crates/faucet-state-postgres) — durable bookmark stores

## Source lag

`Source::lag` (#733) reports **bytes**: the binlog distance from the consumed file/position to the server's head, across files (`SHOW BINARY LOGS` + `SHOW BINARY LOG STATUS`; needs `REPLICATION CLIENT`). Before any position is known (no bookmark, no page yet) it reports none. The pipeline polls it on the first page, at most every
15 s after, and when the run ends, exporting `faucet_source_lag_*{pipeline,row,connector}`;
`faucet status --probe` and `faucet doctor` ask it from the stored bookmark,
and `sla.max_lag_*` thresholds turn it into an SLA. A failing lag query is
logged once and reported as no lag — it never fails a run.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
