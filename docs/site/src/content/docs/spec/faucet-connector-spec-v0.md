---
title: "Faucet Connector Protocol (FCP) — v0"
description: "Status: draft · Version: 0 · Audience: connector authors"
---

**Status:** draft · **Version:** 0 · **Audience:** connector authors

FCP is the contract every `faucet-source-*` / `faucet-sink-*` crate upholds. It
is deliberately small: two object-safe async traits, one error type, one config
convention. A connector that satisfies this contract composes with any other
connector, streams with bounded memory, resumes safely, and reports its
capabilities honestly.

:::note[The contract is executable]
Everything normative below is checked by the
reusable [`faucet-conformance`](https://github.com/faucet-hq/faucet-stream/tree/main/crates/conformance)
battery. A connector is **Tier-1 / conformant** exactly when it invokes and
passes that battery in CI — there is no separate certification. See
[Authoring a connector](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html).
:::

---

## 1. Scope & terminology

- **MUST / SHOULD / MAY** follow RFC 2119.
- A **record** is a `serde_json::Value` (conventionally a JSON object).
- A **page** is a `StreamPage { records: Vec<Value>, bookmark: Option<Value> }`.
- A **bookmark** is an opaque `Value` a source emits to mark replication
  progress; the pipeline persists it and hands it back on the next run.
- A **commit token** is a monotonic, fixed-width string a sink stores atomically
  alongside a page to support effectively-once delivery.

The only crate a connector MUST depend on is `faucet-core`. It re-exports the
common third-party types authors need (`async_trait`, `serde_json`, `schemars`).

---

## 2. Source contract

```rust
#[async_trait]
pub trait Source: Send + Sync {
    async fn fetch_with_context(&self, ctx: &HashMap<String, Value>)
        -> Result<Vec<Value>, FaucetError>;
    // + defaulted: fetch_all, *_incremental, stream_pages, state_key,
    //   apply_start_bookmark, capture_resume_position, supports_exactly_once,
    //   is_shardable/enumerate_shards/apply_shard, supports_discover/discover,
    //   config_schema, connector_name, dataset_uri, check
}
```

A source **MUST**:

1. **Fetch.** Implement `fetch_with_context`, returning records or a typed
   `FaucetError`. It **MUST NOT** panic on bad input, an unreachable endpoint, a
   malformed response, or an empty result — every failure path returns `Err`.
   *(conformance check 6)*
2. **Stream with bounded memory.** Either rely on the default `stream_pages`
   (which chunks `fetch_*` by `batch_size`) or override it to stream natively. A
   source that can page **MUST NOT** buffer the whole dataset into one page when
   a positive `batch_size` is given. `batch_size == 0` is the explicit "no
   batching" sentinel (emit one page). *(check 2)*
3. **Expose a valid config schema.** `config_schema()` **MUST** return a
   structurally valid JSON Schema (`schemars::schema_for!(MyConfig)`). *(check 1)*
4. **Report capabilities truthfully.** `supports_exactly_once()`,
   `supports_discover()`, `is_shardable()` **MUST** be `true` only if the
   corresponding methods genuinely work. *(check 5, and the CLI capability gates)*

A source **SHOULD**, when it has a natural cursor:

5. **Be resumable.** Return `Some(key)` from `state_key()`, attach a `bookmark`
   to the final (or per-transaction) page, and honour a bookmark handed back via
   `apply_start_bookmark()` so a resumed run does not replay committed records.
   *(check 3)*

A source **MAY** additionally implement discovery (`discover()`), sharding
(`enumerate_shards`/`apply_shard`), CDC position capture
(`capture_resume_position`), and a custom preflight `check()`.

---

## 3. Sink contract

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError>;
    // + defaulted: flush, write_batch_partial, supported_write_modes,
    //   supports_idempotent_writes/write_batch_idempotent/last_committed_token,
    //   dedups_by_key, current_schema/supports_schema_evolution/evolve_schema,
    //   config_schema, connector_name, dataset_uri, check
}
```

A sink **MUST**:

1. **Write & count.** Implement `write_batch`, returning the number of records
   written or a typed `FaucetError`. It **MUST NOT** panic on a partial failure.
2. **Expose a valid config schema.** As for sources. *(check 1)*
3. **Report capabilities truthfully.** *(check 5)* Specifically:
   - `supported_write_modes()` lists only modes it really applies (default
     `[Append]`); the CLI rejects a configured mode not in this set.
   - `supports_idempotent_writes()` is `true` only if
     `write_batch_idempotent()` commits the records **and** the commit token
     atomically, and `last_committed_token()` reads that token back durably.
   - `dedups_by_key()` reflects the *live config* (upsert/delete with a
     non-empty key).
   - `supports_schema_evolution()` is `true` only if `evolve_schema()` applies
     idempotent additive DDL (`ADD COLUMN IF NOT EXISTS` semantics).

A sink **SHOULD** override `write_batch_partial` when its API exposes per-row
results (so the DLQ router can quarantine only the failed rows), and **MAY**
implement upsert (`supported_write_modes`), the atomic-watermark idempotent path,
and schema evolution.

### 3.1 Delivery guarantee — say "effectively-once"

faucet delivers **effectively-once**, not distributed-consensus exactly-once. The
guarantee is: *no duplicate and no lost records at the destination across
retries/resumes*, achieved by one of two mechanisms —

- **Atomic watermark** — a CDC-style deterministic source + a sink that commits
  records and a monotonic commit token in one transaction; on resume the
  pipeline skips already-committed pages. Requires durable state and no DLQ.
- **Keyed upsert** — any source + an upsert-capable sink configured with a
  non-empty `key`; re-applying a record converges instead of duplicating.

Both are verified by conformance **check 4** (`assert_idempotent_replay`). It is
*not* the two-phase-commit "exactly-once" of a consensus system, and connector
docs MUST NOT claim otherwise.

---

## 4. Errors

Every fallible path returns `faucet_core::FaucetError`. Third-party error types
wrap into the `Custom(Box<dyn Error + Send + Sync>)` variant. Connectors **MUST
NOT** `unwrap()` / `expect()` on values that can fail at runtime (only on
invariants established at construction). Panics are contract violations —
conformance check 6 catches an unwinding source.

---

## 5. Config

Config structs derive `Serialize + Deserialize + JsonSchema`. Auth/credentials
serialize with the project-wide adjacently-tagged shape
`{ type: <method>, config: { … } }`. Non-serializable fields use
`#[serde(skip)]`; custom-serde fields carry `#[schemars(with = "…")]`.

---

## 6. Naming & packaging

- Crate name: `faucet-source-<name>` / `faucet-sink-<name>`.
- New crates start at `version = "1.0.0"`.
- `lib.rs` starts with `#![cfg_attr(docsrs, feature(doc_cfg))]`.
- `connector_name()` returns a short, non-empty, stable snake_case label.

---

## 7. Conformance (normative)

A connector claims **Tier-1 / conformant** by adding a `tests/conformance.rs`
that invokes the applicable [`faucet-conformance`](https://github.com/faucet-hq/faucet-stream/tree/main/crates/conformance)
checks against the real connector and passing them in CI:

| # | Check | Applies to |
|---|-------|------------|
| 1 | `assert_config_schema_valid` | every source & sink |
| 2 | `assert_bounded_memory` | every pageable source |
| 3 | `assert_bookmark_roundtrip` | resumable sources |
| 4 | `assert_idempotent_replay` | idempotent / keyed-upsert sinks |
| 5 | `assert_capabilities_truthful` | every sink |
| 6 | `assert_errors_not_panics` | every source |
| 7 | `assert_write_modes_truthful` | sinks advertising `Upsert` / `Delete` |
| 8 | `assert_schema_evolution_effective` | schema-evolving sinks |
| 9 | `assert_batch_size_zero_single_page` | sources honoring the `batch_size = 0` sentinel |
| 10 | `assert_connector_name_nonempty` | every source & sink |
| 11 | `assert_preflight_check_wellformed` | every source & sink with a `check()` probe |
| 12 | `assert_discover_roundtrips` *(integration)* | discoverable sources (`supports_discover`) |
| 13 | `assert_cancellation_flushes` *(integration)* | buffered sinks / the pipeline (#146 H16) |

Checks 12–13 are **integration-level**: they drive a live backend (a rebuilt
source reading a discovered dataset) or the real `run_stream` (a mid-run
cancellation that must flush), so they live in a connector's
testcontainers/tempfile conformance test rather than against the synthetic
doubles.

Where a connector legitimately cannot satisfy a check (e.g. an append-only sink
has no idempotency mechanism), it asserts the **honest branch** instead — the
capability returns `false` and the pipeline refuses `delivery: exactly_once`.

---

## 8. Versioning of this spec

v0 is pre-stability: it may change as the trait surface evolves (additively).
Breaking changes bump the spec version. The authoritative, always-current
contract is the `faucet-conformance` battery — if this prose and the battery ever
disagree, the battery wins.

