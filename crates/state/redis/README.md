# faucet-state-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-state-redis.svg)](https://crates.io/crates/faucet-state-redis)
[![Docs.rs](https://docs.rs/faucet-state-redis/badge.svg)](https://docs.rs/faucet-state-redis)
[![MSRV](https://img.shields.io/crates/msrv/faucet-state-redis.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-state-redis.svg)](https://github.com/faucet-hq/faucet-stream#license)

Redis-backed [`StateStore`](https://docs.rs/faucet-core/latest/faucet_core/state/trait.StateStore.html) for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Persists incremental-replication bookmarks in Redis so a pipeline can resume exactly where it left off after a crash, restart, or scheduled invocation.

## Why durable state

A faucet-stream pipeline records a **bookmark** — the source's resume position (a CDC LSN, a `WHERE id > ?` watermark, a Kafka offset) — every time the sink confirms a batch. Where that bookmark lives decides what happens after a restart:

- **`memory`** (built into `faucet-core`) — fast, but the bookmark dies with the process. Every run starts from scratch.
- **`file`** (built into `faucet-core`) — survives restarts on a single host, but isn't shared and doesn't fit ephemeral or multi-replica deployments.
- **`redis`** (this crate) — a network-accessible, shared store. Ideal when the runner is containerized, scheduled, or horizontally placed, and you want resume state to outlive any one process or host.

The bookmark is read **before** each fetch and persisted **only after** the sink confirms the batch — so a crash mid-write replays the last batch rather than losing it (at-least-once). This crate is the storage layer behind that guarantee; you wire it in once and the pipeline handles the rest.

## Who depends on this

You only depend on `faucet-state-redis` directly if you are **building a pipeline in Rust** and want Redis-backed resume state. If you drive faucet-stream through the **`faucet` CLI**, you don't add this crate by hand — it's compiled in via the `state-redis` feature and selected with a `state:` block in your YAML config (see below).

## Installation

```bash
# Library — add alongside faucet-core:
cargo add faucet-core
cargo add faucet-state-redis

# CLI — enable the Redis state backend (opt-in feature):
cargo install faucet-cli --features state-redis
```

The `state-redis` feature is **not** in the CLI's default build — enable it explicitly (or use `--features full`).

## Configuration (CLI `state:` block)

In a `faucet` YAML/JSON config, select this backend with `type: redis`:

```yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      # ... your CDC source ...
  sink:
    type: postgres
    config:
      # ... your sink ...
  state:
    type: redis
    config:
      url: redis://127.0.0.1:6379
      namespace: faucet
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | Standard Redis URL: `redis://[:password@]host:port[/db]`. Use `rediss://` for TLS (rustls, verified against the bundled Mozilla root set). |
| `namespace` | string | `faucet` | Prefix applied to every key (`{namespace}:{key}`). Use it to isolate pipelines that share one Redis instance — e.g. `prod`, `staging`, `team-a`. Must be non-empty and contain only ASCII alphanumerics plus `_ - .` (no `:`). |

Point `url` at a secret directive to keep credentials out of the file, e.g. `url: ${env:REDIS_URL}` or `url: ${secret:redis-url}`.

## Library usage

```rust,no_run
use std::sync::Arc;
use faucet_core::{Pipeline, state::StateStore};
use faucet_state_redis::RedisStateStore;

# async fn run(source: impl faucet_core::Source, sink: impl faucet_core::Sink)
#     -> Result<(), faucet_core::FaucetError> {
let store: Arc<dyn StateStore> = Arc::new(
    RedisStateStore::connect("redis://127.0.0.1:6379", "faucet").await?,
);

Pipeline::new(&source, &sink)
    .with_state_store(store)
    .run()
    .await?;
# Ok(())
# }
```

To share one Redis connection across several stores (e.g. one per pipeline, each with its own namespace), open the connection yourself and use `from_connection`:

```rust,no_run
use faucet_state_redis::RedisStateStore;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let client = redis::Client::open("redis://127.0.0.1:6379")
    .map_err(|e| faucet_core::FaucetError::Config(e.to_string()))?;
let conn = client.get_multiplexed_async_connection().await
    .map_err(|e| faucet_core::FaucetError::State(e.to_string()))?;

let prod = RedisStateStore::from_connection(conn.clone(), "prod")?;
let staging = RedisStateStore::from_connection(conn, "staging")?;
# let _ = (prod, staging);
# Ok(())
# }
```

## How it works

### Storage layout

Each bookmark is a single Redis string keyed by namespace plus state key:

| Redis key | Value |
|-----------|-------|
| `{namespace}:{state_key}` | UTF-8 JSON serialization of the bookmark `Value`. |

The namespace is fixed once in `connect()` / `from_connection()` and prefixed onto every key. The `state_key` is supplied by the pipeline (e.g. `postgres-cdc:<slot>` for a CDC source, or the CLI's `{name}::{row_id}` composite for matrix runs), so multiple sources sharing one namespace never collide.

### Operations

The three `StateStore` methods map directly onto Redis commands on a namespaced key:

- `get(key)` → `GET {namespace}:{key}`, then parse the JSON; a missing key returns `None`.
- `put(key, value)` → `SET {namespace}:{key} <serialized JSON>`.
- `delete(key)` → `DEL {namespace}:{key}`; deleting a missing key is not an error.
- `list(prefix)` → `SCAN MATCH {namespace}:{prefix}*`, namespace stripped, sorted — how `faucet state show|export` enumerates a pipeline's keys.
- `put_batch(entries)` → one `MSET`, so a `faucet state import` lands all-or-nothing (`supports_atomic_batch() == true`).
- `compare_and_put(key, expected, value)` → reads the key, compares it with `expected` as JSON (`None` = absent), then runs a Lua script that `SET`s only while the stored string is still the one compared (or the key is still absent). Atomic across processes (`supports_compare_and_put() == true`), so two `faucet run` processes sharing this store cannot both take a row's run lease.

`connect()` opens a `redis::aio::ConnectionManager`, which is cheaply cloneable and safe to share across concurrent tasks — the store clones it per call, so no locking or pooling is needed on the caller's side. A dropped connection (a Redis restart, a failover, a server or load-balancer idle cut) is re-established, and an operation that hit the drop is retried once on the new connection, so one blip no longer fails every later bookmark read and write. Connecting is bounded by 10 s and every reply by 30 s (`CONNECT_TIMEOUT` / `RESPONSE_TIMEOUT`), so a black-holed server fails the operation instead of stalling the run for the OS TCP timeout. `list()` walks every `SCAN` page and fails on any page's error rather than returning a partial key set. A connection passed to `from_connection()` is used as given (no reconnect).

### Validation & preflight

- **State keys** are validated by [`faucet_core::state::validate_state_key`] — ASCII alphanumerics plus `_ - : .`, max 256 characters, no leading dot.
- **Namespaces** are validated at construction: non-empty, and the same character set minus `:` (since the namespace itself becomes a key prefix). An invalid namespace or URL fails fast as `FaucetError::Config`.
- **`faucet doctor`** exercises a real put → get → delete round-trip on a sentinel key, so the preflight validates connectivity, auth, and read/write permissions through the actual code path and leaves no residue.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI or the `faucet-stream` umbrella crate via the `state-redis` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: invalid Redis URL` | The `url` isn't a valid Redis URL. Use the `redis://[:password@]host:port[/db]` form (`rediss://` for TLS). |
| `State: Redis connection failed` | The server is unreachable, the port is wrong, or auth was rejected. Check the host/port, that Redis is running, and that any password in the URL is correct. |
| `Config: ... namespace contains illegal character` / `must not be empty` | The `namespace` is empty or contains a disallowed character (only `[A-Za-z0-9_.-]` allowed — no `:`, `/`, or spaces). |
| Pipeline always restarts from scratch | The `state:` block is missing, set to `type: memory`, or a different `namespace`/source `state_key` is used each run. Resume needs a stable, durable backend and a stable key. |
| `State: stored value ... is not valid JSON` | The namespaced key holds a non-JSON string written by something other than faucet. Use a dedicated `namespace` (or db) for faucet state so it doesn't collide with other Redis users. |
| Multiple pipelines clobber each other's bookmarks | Two pipelines share both a namespace and a source `state_key`. Give each pipeline its own `namespace`. |

## See also

- [State & resume cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) — bookmarks, resume semantics, and choosing a backend.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `StateStore` trait this crate implements, plus the built-in `memory` and `file` backends.
- [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres) — the PostgreSQL-backed sibling backend.
- [faucet-stream on GitHub](https://github.com/faucet-hq/faucet-stream) — the full ecosystem.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
