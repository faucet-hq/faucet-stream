# faucet-state-postgres

[![Crates.io](https://img.shields.io/crates/v/faucet-state-postgres.svg)](https://crates.io/crates/faucet-state-postgres)
[![Docs.rs](https://docs.rs/faucet-state-postgres/badge.svg)](https://docs.rs/faucet-state-postgres)
[![MSRV](https://img.shields.io/crates/msrv/faucet-state-postgres.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-state-postgres.svg)](https://github.com/faucet-hq/faucet-stream#license)

PostgreSQL-backed [`StateStore`](https://docs.rs/faucet-core/latest/faucet_core/state/trait.StateStore.html) for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Persists incremental-replication bookmarks in a single JSONB-backed table so sources can resume exactly where they left off after a crash, restart, or scheduled invocation.

Reach for it when you run faucet pipelines across multiple machines (or in Kubernetes) and want durable, shared resume state in a database you already operate — instead of a local `file` store that lives on one node's disk.

## Why durable state?

faucet-stream sources that support incremental replication (CDC sources like `postgres-cdc` / `mysql-cdc` / `mongodb-cdc`, and query sources with a watermark column) emit a **bookmark** after each committed batch — the replication position they reached. A `StateStore` persists that bookmark; on the next run the pipeline reads it back and resumes from there, so a restart never re-emits already-delivered rows or skips data.

The built-in `memory` store forgets everything on exit, and the built-in `file` store ties state to one node's filesystem. A Postgres-backed store gives you:

- **Durability across restarts and crashes** — state survives the process and the host.
- **A shared source of truth** — every replica of a `faucet serve` cluster, every scheduled tick, and every operator running `faucet replicate` reads and writes the same row, keyed by pipeline + matrix row.
- **One less moving part** — if you already run Postgres for the pipeline's source or sink, state lives in the same database you back up and monitor.

## Who should use this crate

- **End users** reference it indirectly: add the CLI's `state-postgres` feature and point a pipeline's `state:` block at `type: postgres` (see below). You never import this crate directly.
- **Library authors** building their own pipeline runner construct [`PostgresStateStore`] and hand it to [`Pipeline::with_state_store`](https://docs.rs/faucet-core/latest/faucet_core/pipeline/struct.Pipeline.html) / `RunStreamOptions`.

It implements the [`faucet_core::state::StateStore`] trait — nothing more — so it slots in anywhere a `StateStore` is accepted.

## Installation

```bash
# As a library (you also need faucet-core for the StateStore trait + Pipeline):
cargo add faucet-core faucet-state-postgres

# In the CLI (opt-in state backend):
cargo install faucet-cli --features state-postgres
```

The `postgres` state backend is **not** in the CLI default build — enable the `state-postgres` feature explicitly (it is included in the `full` aggregate). The built-in `memory` and `file` backends ship in `faucet-core` and need no feature.

## CLI configuration

Point a pipeline's top-level `state:` block at the `postgres` backend:

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_slot
      publication: faucet_pub
      tables: [public.orders]
  sink:
    type: file
    config:
      path: ./orders.jsonl
  state:
    type: postgres
    config:
      url: ${env:STATE_PG_URL}      # e.g. postgres://user:pass@localhost:5432/faucet
      table: faucet_state
      ensure_table: true
```

```bash
faucet run pipeline.yaml
```

### `state.config` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | PostgreSQL connection URL (`postgres://user:pass@host:port/db`). Resolve secrets via `${env:VAR}` / `${secret:…}` indirection — never hard-code credentials. |
| `table` | string | `faucet_state` | Table that holds state rows. Validated as a Postgres identifier (ASCII letters/digits/underscore, ≤ 63 chars). |
| `ensure_table` | bool | `false` | When `true`, run `CREATE TABLE IF NOT EXISTS` on startup. Leave `false` if you provision the table via migrations or want to grant the runtime role no DDL. |
| `max_connections` | integer | `5` | Size of the connection pool backing the state store. Tune it up when many concurrent matrix rows share one state store, or down against a connection-limited managed Postgres. Rejected at config-load time when `0`. |

`max_connections` maps onto [`PostgresStateStore::connect_with`]; library callers can also size the pool directly.

> **Tip:** the state database can be the *same* Postgres instance as your source or sink — just give it its own `url`/`table`. It does not have to be a separate server.

## Library usage

### Build it and hand it to a `Pipeline`

```rust,no_run
use std::sync::Arc;
use faucet_core::{Pipeline, state::StateStore};
use faucet_state_postgres::PostgresStateStore;

# async fn run(source: impl faucet_core::Source, sink: impl faucet_core::Sink)
#     -> Result<(), faucet_core::FaucetError> {
// Default: 5 connections, `faucet_state` table.
let store = PostgresStateStore::connect("postgres://user:pass@localhost/faucet").await?;
store.ensure_table().await?;            // optional — skip if you manage schema yourself
let store: Arc<dyn StateStore> = Arc::new(store);

Pipeline::new(&source, &sink)
    .with_state_store(store)
    .run()
    .await?;
# Ok(())
# }
```

### Custom pool size, table name, or a shared pool

```rust,no_run
use faucet_state_postgres::PostgresStateStore;
use sqlx::PgPool;

# async fn build() -> Result<(), faucet_core::FaucetError> {
// Explicit pool size + table name.
let store = PostgresStateStore::connect_with(
    "postgres://user:pass@localhost/faucet",
    10,                 // max_connections
    "pipeline_state",   // table
).await?;

// Or reuse a PgPool you already manage.
# let existing_pool: PgPool = unimplemented!();
let store = PostgresStateStore::from_pool(existing_pool, "faucet_state")?;
# let _ = store;
# Ok(())
# }
```

### Use the store directly

`PostgresStateStore` implements `StateStore`, so you can read and write bookmarks yourself:

```rust,no_run
use faucet_core::state::StateStore;
use faucet_core::serde_json::json;
use faucet_state_postgres::PostgresStateStore;

# async fn demo() -> Result<(), faucet_core::FaucetError> {
let store = PostgresStateStore::connect("postgres://user:pass@localhost/faucet").await?;
store.put("postgres-cdc:faucet_slot", &json!({ "lsn": "0/16B3748" })).await?;
let bookmark = store.get("postgres-cdc:faucet_slot").await?;   // Some(Value) or None
store.delete("postgres-cdc:faucet_slot").await?;               // missing key is not an error
# let _ = bookmark;
# Ok(())
# }
```

## How it works

### Storage layout

Every entry is one row in a single table:

```sql
CREATE TABLE faucet_state (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`ensure_table()` runs the `CREATE TABLE IF NOT EXISTS` form (the identifier is double-quoted via `faucet_core::util::quote_ident`). If you manage schema with migrations, skip it.

JSONB cannot hold the character U+0000, so a value that contains it (an opaque API cursor, a text bookmark) is stored as its JSON text inside a one-key envelope `{"$faucet_nul_escaped_json": "…"}` and unwrapped on read; every other value is stored as-is.

### Operations

| Trait method | SQL | Notes |
|--------------|-----|-------|
| `get(key)` | `SELECT value FROM <table> WHERE key = $1` | Returns `None` when no row exists. |
| `put(key, value)` | `INSERT … VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()` | Atomic upsert — the bookmark and its `updated_at` advance together. |
| `delete(key)` | `DELETE FROM <table> WHERE key = $1` | A missing row is not an error. |
| `list(prefix)` | `SELECT key FROM <table> WHERE left(key, char_length($1)) = $1 ORDER BY key` | Literal prefix match (no `LIKE` wildcards) — how `faucet state show\|export` enumerate a pipeline's keys. |
| `put_batch(entries)` | the upsert above, once per entry, in one transaction | All-or-nothing (`supports_atomic_batch() == true`) — what `faucet state import` uses. |
| `compare_and_put(key, expected, value)` | `UPDATE <table> SET value = $2, updated_at = NOW() WHERE key = $1 AND value = $3`, or `INSERT … ON CONFLICT (key) DO NOTHING` when `expected` is absent | Writes only when the stored value still equals `expected` (JSONB equality, so key order does not matter); returns whether a row changed. Atomic across processes (`supports_compare_and_put() == true`) — the run lease takes with it, so two `faucet run` processes sharing this table cannot both start the same row. |

The pipeline reads the bookmark **before** fetching and writes it **only after the sink confirms** the batch, so a crash mid-write never advances state past delivered data.

### Performance & safety

- **Pooled connections** — the `sqlx::PgPool` is built once in `connect`/`connect_with` and reused for every operation. State writes are cheap and infrequent (one per committed batch), so the default pool of 5 is intentionally small.
- **Single-row upsert** — `put` is one round-trip with no read-modify-write race; concurrent writers to the same key resolve via `ON CONFLICT`.
- **Validated identifiers** — table names are checked against Postgres identifier rules (non-empty, ≤ 63 chars, `[A-Za-z0-9_]`) at construction and double-quoted in every statement, so a misconfigured table name fails fast instead of risking SQL injection.
- **Validated keys** — state keys are checked by [`faucet_core::state::validate_state_key`] (ASCII alphanumerics plus `_ - : .`, ≤ 256 chars).

### Health check

`check()` runs a real upsert → select → delete round-trip on a sentinel key (`__faucet_doctor__`), so `faucet doctor` verifies connectivity, credentials, table existence, and read/write permissions through the actual code path — and leaves no residue.

## Feature flags

This crate has **no optional features of its own**. Enable the backend in the CLI / umbrella via the `state-postgres` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `State: PostgreSQL state-store connection failed` | The `url` is wrong, the server is unreachable, or auth failed. Verify the URL, that Postgres is listening, and that the role can log in. |
| `State: … relation "faucet_state" does not exist` | The table was never created. Set `ensure_table: true` (CLI) or call `ensure_table()` (library), or create it via your migrations. |
| `Config: state-store table name '…' contains illegal character` | `table` must be a bare Postgres identifier — letters, digits, underscores only, ≤ 63 chars. No dots, dashes, spaces, or schema-qualified names. |
| `State: Postgres UPSERT … failed: permission denied` | The runtime role lacks `INSERT`/`UPDATE` (or `SELECT`/`DELETE`) on the table. Grant the needed privileges, or use a role that has them. |
| `ensure_table` fails with `permission denied for schema` | The role can't run DDL. Provision the table out-of-band (migrations / a privileged role) and set `ensure_table: false`. |
| Pipeline re-processes data after a restart | State isn't being persisted — confirm a `state:` block is present and points at this backend (not `memory`), and that the source actually emits bookmarks (incremental/CDC sources do; one-shot query sources without a watermark don't). |
| `faucet doctor` reports the sentinel probe failing | The round-trip couldn't complete — follow the hint it prints: check reachability, credentials, and that the table exists. |

## See also

- [State & resumability cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) — how bookmarks, resume, and effectively-once work end to end.
- [Configuration reference](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full `state:` block grammar.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `StateStore` trait, `Pipeline`, and the built-in `MemoryStateStore` / `FileStateStore`.
- [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis) — the Redis-backed alternative for the same role.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
