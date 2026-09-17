# faucet-source-postgres-cdc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-postgres-cdc.svg)](https://crates.io/crates/faucet-source-postgres-cdc)
[![Docs.rs](https://docs.rs/faucet-source-postgres-cdc/badge.svg)](https://docs.rs/faucet-source-postgres-cdc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-postgres-cdc.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-postgres-cdc.svg)](https://github.com/faucet-hq/faucet-stream#license)

PostgreSQL **change-data-capture (CDC)** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Subscribes to a Postgres logical-replication slot via the built-in `pgoutput` plugin and emits each row-level change — `INSERT` / `UPDATE` / `DELETE` / `TRUNCATE` — as a JSON change event, in transaction order.

Reach for it when you want to stream live mutations out of an operational Postgres database into any faucet-stream sink — a warehouse, a queue, a search index, a file — without polling, triggers, or touching the application. Replication position is persisted to any `faucet-core` `StateStore`, so pipelines resume exactly where they left off across restarts: no gap, no loss.

## Feature highlights

- **Real CDC, not polling** — reads the write-ahead log directly through logical replication; no `updated_at` columns, triggers, or query load on the source tables.
- **Transactionally consistent** — each transaction is buffered in memory and flushed to the sink only on `COMMIT`, so a sink never sees half a transaction.
- **Resumable across restarts** — the connector overrides `state_key()` / `apply_start_bookmark()`; the durable LSN bookmark survives process crashes and restarts.
- **Effectively-once delivery** — `supports_exactly_once()` is `true`; pair with an idempotent sink (`postgres`, `mysql`, `mssql`, `sqlite`, `iceberg`, `bigquery`) under `delivery: exactly_once`.
- **Snapshot → CDC handoff** — implements `capture_resume_position()` so `faucet replicate` can bulk-snapshot a table and hand off to CDC with no gap and no duplicate.
- **Crash-safe WAL feedback** — the advertised `confirmed_flush_lsn` advances *only* from a durably-persisted bookmark, so Postgres never recycles WAL for changes the consumer hasn't committed.
- **TLS-capable** — `require` / `verify_ca` / `verify_full` modes for the replication connection (plaintext `disable` is the default for back-compat).
- **Type-aware decoding** — booleans, integers, floats (incl. `NaN`/`Infinity`), `numeric` (exact precision), `bytea` (base64), `json`/`jsonb`, and 1-D scalar arrays decode to native JSON; everything else is preserved as raw Postgres text.
- **OOM safety valves** — `max_staged_records` bounds a single in-progress transaction; `idle_timeout` and `max_messages` bound a fetch cycle.

## Installation

```bash
# As a library:
cargo add faucet-source-postgres-cdc

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-postgres-cdc
```

## Postgres setup (one-time)

Logical replication is off by default and requires a server restart to enable. Run this once as a superuser before pointing faucet at the database:

```sql
-- 1. Enable logical decoding (requires a Postgres restart afterwards).
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;   -- ≥ number of concurrent slots
ALTER SYSTEM SET max_wal_senders     = 4;     -- ≥ number of concurrent streams
-- → restart Postgres now

-- 2. The connecting role must have the REPLICATION attribute.
ALTER ROLE faucet WITH REPLICATION;

-- 3. Create a publication selecting the tables you want to capture.
--    (faucet does NOT create publications — that is a DBA concern.)
CREATE PUBLICATION faucet_pub FOR TABLE public.users, public.orders;
-- or:  CREATE PUBLICATION faucet_pub FOR ALL TABLES;

-- 4. RECOMMENDED: capture a full row pre-image on UPDATE/DELETE.
--    Without this, `before` is null on UPDATE and a DELETE carries only
--    the primary-key columns.
ALTER TABLE public.users  REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

The **replication slot** is created automatically on first run when `create_slot_if_missing: true` (the default). The **publication** must already exist — faucet never creates one.

> `REPLICA IDENTITY FULL` writes the whole old row into the WAL on every update/delete, increasing WAL volume. The default (`DEFAULT` = primary key only) is fine if you don't need the `before` image of changed columns.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30
  sink:
    type: jsonl
    config:
      path: ./changes.jsonl
      append: true
  state:
    type: file
    config:
      path: ./state
```

```bash
faucet run pipeline.yaml
```

Each fetch cycle drains all pending changes, then stops once the stream has been idle for `idle_timeout` seconds. Re-running resumes from the persisted bookmark. For a continuously-running mirror, drive this under `faucet schedule` or `faucet replicate`.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — *(required)* | Postgres connection URL. The crate internally upgrades it to `replication=database` — you do **not** add that yourself. Redacted in logs/`Debug`. |
| `slot_name` | string | — *(required)* | Logical replication slot. Must match `[a-z0-9_]{1,63}` (lowercase letters, digits, underscores; ≤ 63 chars). |
| `publication_name` | string | — *(required)* | Existing publication that selects which tables are replicated. |
| `create_slot_if_missing` | bool | `true` | Create the slot as a logical/`pgoutput` slot on first connect if it doesn't exist. |
| `slot_type` | enum | `permanent` | `permanent` (survives disconnect, pins WAL until consumed or dropped) or `temporary` (auto-dropped when the replication connection closes). See [Slot lifecycle](#slot-lifecycle). |
| `start_lsn` | string? | `null` | One-time starting-LSN override (e.g. `"0/16A4F88"`). Ignored when a state-store bookmark exists — the bookmark wins. With neither set, replication starts from the slot's `confirmed_flush_lsn`. |
| `proto_version` | u32 | `1` | pgoutput protocol version. Only `1` is supported in this release (`validate` rejects anything else). |

### Reliability & flow control

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `idle_timeout` | seconds | `30` | Stop the current fetch cycle after this long with no new replication message. Must be `> 0`. |
| `max_messages` | usize? | `null` | Optional cap on change events drained per fetch call. Checked **after each COMMIT**, never mid-transaction — a transaction larger than the cap still emits atomically. `idle_timeout` is the primary terminator. |
| `max_staged_records` | usize? | `null` | Max change records buffered for a **single in-progress transaction** before the run aborts with a typed `FaucetError::Source`. `null` = unbounded. The OOM safety valve for huge bulk transactions — see [Transactional consistency](#transactional-consistency). |
| `status_update_interval` | seconds | `10` | Standby Status Update (keepalive) cadence. Must be **strictly less than** `idle_timeout` and well under the server's `wal_sender_timeout` (default 60 s). |
| `tcp_keepalive` | seconds | `60` | TCP keepalive on the replication connection. |
| `slot_acquire_retries` | u32 | `10` | Retries when the slot is still **active** (held by a not-yet-released prior connection) on a rapid restart. Both the pre-stream slot advance and `START_REPLICATION` retry with exponential backoff (250 ms, doubling, capped at 4 s). `0` = fail fast. |
| `batch_size` | usize | `1000` | Advisory page size. The source emits **one `StreamPage` per committed transaction** for per-transaction durability; transactions are never split, so a transaction larger than `batch_size` still emits as one page. **`0` = no batching**: accumulate every transaction in the run window into a single trailing page (negates per-transaction durability; for tests/snapshot-style runs only). |

### TLS (`tls`)

| `mode` | Extra config | Description |
|--------|--------------|-------------|
| `disable` | *(none)* | Plaintext — **default**, back-compatible. Credentials and WAL travel unencrypted. |
| `require` | *(none)* | Require TLS, but do not verify the server certificate. |
| `verify_ca` | `ca_path?` | Require TLS and verify the certificate chain against `ca_path` (or the system roots when omitted). |
| `verify_full` | `ca_path?` | Require TLS and verify both the certificate chain **and** the hostname. |

```yaml
# Verify the full chain + hostname against a custom CA bundle
tls:
  mode: verify_full
  ca_path: /etc/ssl/certs/rds-ca.pem
```

> Use `require` or a `verify_*` mode in any production / cross-network deployment — `disable` sends database credentials and all WAL data in the clear.

## Output record schema

Every change event is one JSON object:

```json
{
  "op":     "insert",
  "schema": "public",
  "table":  "users",
  "lsn":    "0/16A4F88",
  "ts_ms":  1779019200000,
  "before": null,
  "after":  { "id": 42, "name": "Ada", "email": "ada@example.com" }
}
```

| Field | Description |
|-------|-------------|
| `op` | One of `insert` / `update` / `delete` / `truncate`. |
| `schema` | Source schema name (e.g. `public`). |
| `table` | Source table name. |
| `lsn` | The `commit_lsn` of the enclosing transaction. |
| `ts_ms` | Unix-epoch milliseconds, derived from the COMMIT timestamp. |
| `before` | Row image **before** the change. Always present on `delete`; present on `update` only when the table is `REPLICA IDENTITY FULL`; otherwise `null`. |
| `after` | Row image **after** the change. Present on `insert` and `update`; `null` on `delete` and `truncate`. |

- A `truncate` emits one record per truncated relation with `before = after = null`.
- **Unchanged TOAST:** Postgres elides large out-of-line values whose stored copy wasn't rewritten. Such columns are dropped from `before`/`after` and their names are recorded in `before.__unchanged_toast__` / `after.__unchanged_toast__` (a JSON array of column names) — meaning "this row image is partial: these columns still hold their previous value at the source."

  `__unchanged_toast__` is a **reserved key that is part of the emitted record**, not out-of-band metadata: it is an ordinary field in the row image, so downstream consumers see it. After a `cdc_unwrap` it survives into the flat row, where an `auto_map` SQL sink treats it as a column (and a `schema:` drift policy as an added one). Drop it with a `drop` transform (`fields: ["__unchanged_toast__"]`) when the destination shouldn't carry it, or keep it to detect partial rows. The name is exported as `faucet_source_postgres_cdc::stream::UNCHANGED_TOAST_FIELD` for programmatic consumers.

### Column type mapping

Values arrive in Postgres text form and are decoded by column type OID:

| Postgres type | JSON |
|---|---|
| `bool` | `true` / `false` |
| `int2` / `int4` / `int8` | number |
| `float4` / `float8` (finite) | number |
| `float4` / `float8` `NaN` / `±Infinity` | `"NaN"` / `"Infinity"` / `"-Infinity"` (string, to stay distinct from a SQL `NULL` → `null`) |
| `numeric` | string (exact precision) |
| `bytea` | base64 string |
| `json` / `jsonb` | parsed JSON value |
| 1-D arrays of the above scalar types (`int4[]`, `text[]`, `uuid[]`, …) | JSON array, decoded element-by-element (`NULL` elements → `null`) |
| `uuid`, `date`, `time`, `timestamp`, `timestamptz`, and anything else | string (raw Postgres text — stable under the default `DateStyle ISO`) |

Multi-dimensional arrays, ranges, composites, and enums fall back to the raw Postgres array/text string.

## Examples

### CDC → Postgres mirror, effectively-once + upsert

Stream changes into a target table with idempotent upserts. Pair with the `cdc_unwrap` transform so the envelope's `op` becomes a normalized `__op` marker the upsert sink can act on.

```yaml
version: 1
delivery: exactly_once          # CDC source + idempotent sink + state, no DLQ
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: mirror_slot
      publication_name: mirror_pub
      tls: { mode: require }
  transforms:
    - type: cdc_unwrap          # flatten {op,before,after} → row + __op
  sink:
    type: postgres
    config:
      connection_url: ${env:TARGET_PG_URL}
      table: users
      auto_map: true
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: ["d"] }
  state:
    type: postgres
    config:
      connection_url: ${env:STATE_PG_URL}
```

### Bound a large bulk transaction

Protect the process from an OOM when a single `UPDATE`/`COPY` touches millions of rows.

```yaml
source:
  type: postgres-cdc
  config:
    connection_url: postgres://faucet:faucet@db:5432/appdb
    slot_name: faucet_slot
    publication_name: faucet_pub
    max_staged_records: 500000   # abort cleanly instead of OOM-killing
    idle_timeout: 60
```

### Ephemeral / test run with a self-cleaning slot

A temporary slot is dropped automatically when the connection closes, so it won't pin WAL after the run. (Note: temporary slots reset on reconnect — not for cross-run resume.)

```yaml
source:
  type: postgres-cdc
  config:
    connection_url: postgres://faucet:faucet@localhost:5432/appdb
    slot_name: ephemeral_slot
    publication_name: faucet_pub
    slot_type: temporary
    idle_timeout: 10
```

## Streaming & batching

The source overrides `Source::stream_pages` and emits **one `StreamPage` per committed transaction**, carrying `bookmark = commit_lsn`. The pipeline persists that bookmark to the state store after the sink flushes, giving per-transaction durability for free. Because transactions are atomic units, they are never split across pages — a transaction whose record count exceeds `batch_size` still emits as a single page.

`batch_size: 0` is the "no batching" sentinel: every committed transaction in the run window is accumulated into a single trailing page emitted at the end with `bookmark = max(commit_lsn)`. This negates per-transaction durability and is only useful for tests or snapshot-style runs.

## Resume & state

The connector overrides `state_key()` and `apply_start_bookmark()` for durable, resumable replication:

- **State key:** `postgres-cdc:<slot_name>` (e.g. `postgres-cdc:faucet_slot`). One bookmark per slot.
- **Bookmark:** the most-recently-committed `commit_lsn`, persisted by the pipeline only **after the sink confirms** the batch flushed.
- **On resume:** `apply_start_bookmark` receives that LSN and advances the slot's `confirmed_flush_lsn` to it before streaming continues.

Configure any `faucet-core` `StateStore` — `file`, `memory`, [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres), or [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis). **Always configure a durable (non-`memory`) state store in production** — without one the slot's `confirmed_flush_lsn` never advances and WAL is retained indefinitely.

## Effectively-once delivery

`supports_exactly_once()` returns `true`, so this source qualifies for `delivery: exactly_once`. With that mode the pipeline assigns a monotonic per-transaction commit token; the sink commits the records and the token in one atomic unit, and on resume skips any transaction whose token is already committed — so a crash between sink-flush and bookmark-write can never double-apply.

The CLI enforces the full effectively-once gate at config-load time (`faucet validate` catches all four):

1. **Source** supports effectively-once — `postgres-cdc` does. ✅
2. **Sink** supports idempotent writes — one of `postgres` / `mysql` / `mssql` / `sqlite` / `iceberg` / `bigquery`.
3. A **`state:`** block is configured.
4. **No `dlq:`** block (DLQ and effectively-once are mutually exclusive in this version).

Without `delivery: exactly_once` the source still delivers **at-least-once**: Postgres redelivers everything after the most recent durably-persisted `confirmed_flush_lsn` on the next `START_REPLICATION`, so a crash replays the most recent transaction rather than losing it. Make sinks idempotent or tolerant of duplicates at transaction boundaries.

## Transactional consistency

The connector buffers each transaction in full and flushes to the sink only on `COMMIT`. Partial transactions (a `BEGIN` with no `COMMIT` before `idle_timeout` / `max_messages`) are dropped and redelivered after the next `START_REPLICATION`. This keeps the output transactionally consistent — at the cost of needing each transaction to fit in one fetch cycle and in memory.

A single bulk `UPDATE`/`DELETE`/`COPY` of millions of rows therefore holds every decoded row as a `serde_json::Value` in RAM at once. Set `max_staged_records` to a value sized to your available memory: when an in-progress transaction exceeds it, the run aborts with a typed `FaucetError::Source` instead of being OOM-killed.

The decoder **fails fast** (`FaucetError::Source`, which the pipeline restarts from the durable bookmark) rather than silently dropping data on a protocol desync — a `COMMIT` without a `BEGIN`, a second `BEGIN` while a transaction is still staged, or an unrecognised replication event. A relation whose column set changes mid-stream (an `ALTER TABLE`) is logged at `warn`; subsequent rows decode against the new descriptor.

## Slot lifecycle

| `slot_type` | Survives disconnect? | WAL retention | Cross-run resume? | Use for |
|-------------|----------------------|---------------|-------------------|---------|
| `permanent` *(default)* | Yes | Pins WAL until consumed or dropped | Yes | Production pipelines, snapshot→CDC handoff. |
| `temporary` | No (auto-dropped) | Released on disconnect | No (resets on reconnect) | Ephemeral / test runs that should self-clean. |

A **permanent** slot keeps pinning WAL even when no consumer is connected — an abandoned slot fills `pg_wal` and can take the whole instance down. Decommission an unused pipeline by calling `PostgresCdcSource::drop_slot()` (or dropping it via `SELECT pg_drop_replication_slot('faucet_slot');` in `psql`). Permanent-slot creation logs a loud warning so the WAL-retention obligation is hard to miss.

## Snapshot → CDC handoff

This source implements `capture_resume_position()`: it ensures the slot exists, reads the server's **current WAL LSN** (`pg_current_wal_lsn`) as a resume bookmark, and consumes no changes. The `faucet replicate` command uses it to anchor the CDC stream at-or-before a bulk snapshot of the table, so the combined snapshot + CDC result is a gap-free, duplicate-free mirror when paired with a `write_mode: upsert` sink.

Capture **requires a permanent slot** (`slot_type: permanent`, the default): a temporary slot is dropped when the short-lived capture connection closes and so cannot retain WAL across the snapshot. Capture rejects a temporary slot with a typed error. See the [replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html) for the full handoff model.

## Crash-safe WAL feedback (durability)

The advertised `confirmed_flush_lsn` is advanced **only** from a durably-persisted bookmark (via `apply_start_bookmark`) — never from decoded WAL at commit-decode time, and never from a keepalive's `wal_end`. This guarantees Postgres is never told to recycle WAL for changes the consumer hasn't durably persisted, so a crash can never lose committed data (it replays instead).

The tradeoff: within a single long-running fetch cycle the flush LSN does not advance, so Postgres retains all WAL produced during the cycle until the next run resumes from the persisted bookmark. Run frequently enough (or keep fetch cycles short) that WAL retention stays within your disk budget.

## Config loading & schema

Load config from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source postgres-cdc
```

## Library usage

```rust,no_run
use std::sync::Arc;
use faucet_core::{Pipeline, state::FileStateStore};
use faucet_source_postgres_cdc::{PostgresCdcSource, PostgresCdcSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg: PostgresCdcSourceConfig = serde_json::from_value(serde_json::json!({
    "connection_url": "postgres://faucet:faucet@localhost:5432/appdb",
    "slot_name": "faucet_slot",
    "publication_name": "faucet_pub",
    "idle_timeout": 30,
}))?;

let source = PostgresCdcSource::new(cfg).await?;

// Pair with any Sink + a durable StateStore for resumable runs.
let state = Arc::new(FileStateStore::new("./state"));
// let pipeline = Pipeline::new(source, my_sink).with_state_store(state);
// pipeline.run().await?;
# let _ = (source, state);
# Ok(())
# }
```

## How it works

Built on the [`pgwire-replication`](https://crates.io/crates/pgwire-replication) crate for the logical-replication wire protocol; the `pgoutput` payload bytes are decoded by a hand-rolled decoder in this crate so the output record shape stays under our control. The `sqlx` Postgres driver handles slot lifecycle (create / advance / drop) and the `pg_current_wal_lsn` probe. Standby Status Updates are sent every `status_update_interval` to keep the connection alive and report the flush position. The replication connection is created once and held for the lifetime of the source.

## Lineage dataset URI

`postgres://<host>:<port>/<db>?publication=<publication_name>` (credentials stripped) — e.g. `postgres://host:5432/app?publication=faucet_pub`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-postgres-cdc` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: slot_name … must contain only [a-z0-9_]` | Slot names allow only lowercase letters, digits, and underscores, ≤ 63 chars. Rename the slot. |
| `wal_level` / "logical decoding requires wal_level >= logical" | Run `ALTER SYSTEM SET wal_level = 'logical';` and **restart Postgres**. |
| `must be superuser or replication role` | The connecting role lacks `REPLICATION`. Run `ALTER ROLE <user> WITH REPLICATION;`. |
| `publication "…" does not exist` | faucet doesn't create publications. `CREATE PUBLICATION … FOR TABLE …;` first. |
| `replication slot … is active for PID …` on restart | A prior connection hasn't released the slot yet. The source retries (`slot_acquire_retries`, default 10, exp. backoff). Raise it, or wait for the old backend to exit. |
| `before` is `null` on UPDATE / DELETE carries only the key | The table isn't `REPLICA IDENTITY FULL`. Run `ALTER TABLE … REPLICA IDENTITY FULL;` to capture the full pre-image. |
| Replication connection dropped after ~60 s of silence | `status_update_interval` ≥ the server's `wal_sender_timeout`. Keep it well below (default 10 s vs 60 s). |
| Process OOM-killed during a bulk load | A giant single transaction is buffered in full. Set `max_staged_records` sized to your RAM. |
| `pg_wal` keeps growing / disk fills | A permanent slot is pinning WAL (no consumer, or fetch cycles too long). Run the pipeline more often, shorten cycles, or drop the slot via `PostgresCdcSource::drop_slot()` / `pg_drop_replication_slot(...)`. |
| Pipeline doesn't resume / replays everything | No durable state store, or a `temporary` slot (resets on reconnect). Use `slot_type: permanent` + `file`/`postgres`/`redis` state. |
| `proto_version must be 1` | Only pgoutput protocol v1 is supported. Remove the `proto_version` override or set it to `1`. |
| Initial changes missing | The slot is created on first fetch — changes made **before** it exists aren't replicated. Create the slot (or do a warm-up fetch) before applying writes. |
| `exactly_once` rejected by `faucet validate` | The sink isn't idempotent, no `state:` block, or a `dlq:` block is present. See [Effectively-once delivery](#effectively-once-delivery). |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/replication.html) · [State & resume cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) · [Upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html)
- [faucet-source-postgres](https://crates.io/crates/faucet-source-postgres) (query-mode snapshots, not CDC) · [faucet-state-postgres](https://crates.io/crates/faucet-state-postgres) (pair as a `StateStore`) · `cli/examples/postgres_cdc_to_jsonl.yaml`

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
