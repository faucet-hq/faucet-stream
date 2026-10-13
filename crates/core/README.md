# faucet-core

[![Crates.io](https://img.shields.io/crates/v/faucet-core.svg)](https://crates.io/crates/faucet-core)
[![Docs.rs](https://docs.rs/faucet-core/badge.svg)](https://docs.rs/faucet-core)
[![MSRV](https://img.shields.io/crates/msrv/faucet-core.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-core.svg)](https://github.com/faucet-hq/faucet-stream#license)

The foundation crate for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem — the shared traits, pipeline orchestration, error type, transforms, state stores, and reliability machinery that every source and sink connector builds on.

**If you are building a custom connector, this is the only dependency you need.** `faucet-core` re-exports `async-trait`, `serde_json`, `schemars`, and `CancellationToken`, so a third-party `faucet-source-*` / `faucet-sink-*` crate can implement the `Source` / `Sink` traits without pulling those in directly. The traits are object-safe (`Box<dyn Source>` / `Box<dyn Sink>` work), and `Pipeline` is generic over any source + sink combination — so anything you write here drops straight into the `faucet` CLI and the umbrella crate.

## Feature highlights

- **Two object-safe traits** — [`Source`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html) and [`Sink`](https://docs.rs/faucet-core/latest/faucet_core/trait.Sink.html), with rich defaults so a minimal connector needs only one method.
- **Batch and streaming orchestration** — `Pipeline::run` (fetch-all) and `run_stream` (page-by-page, O(batch_size) memory) connect any source to any sink.
- **Pluggable transforms** — `RecordTransform` (flatten, key/value casing, regex rename, cast, redact, …) plus stage-level `Filter` / `Explode` / `CdcUnwrap`, attachable to any source via `TransformingSource`.
- **Durable bookmarks** — the `StateStore` trait with in-process `MemoryStateStore` and crash-safe `FileStateStore` built in; Redis / Postgres backends live in their own crates.
- **Reliability primitives** — dead-letter queue routing, effectively-once delivery (`DeliveryMode`), key-based upsert/delete write modes, per-page / per-record data-quality checks, and versioned data contracts.
- **Governance** — PII detection + column-level masking (`masking` feature): classify sensitive fields (name pattern / value detector / explicit list) and `redact`/`hash`/`tokenize`/`partial`-mask them; the pass runs before every sink so PII never leaks to a sink, the DLQ, or a lineage sample.
- **Shared authentication** — the `AuthProvider` trait and `AuthSpec` config field give N connectors one token with single-flight refresh.
- **Typed errors** — one `FaucetError` enum covers every failure path, with a `Custom` variant for third-party connector errors.
- **Built-in observability** — pipelines emit `tracing` spans and `metrics` counters/histograms automatically; connectors only override `connector_name()` for a friendly label.

## Installation

```bash
# As a connector author (the only dependency you need):
cargo add faucet-core
cargo add tokio --features rt   # plus a runtime to drive the async traits
```

`faucet-core` is always linked by the [`faucet-stream`](https://crates.io/crates/faucet-stream) umbrella crate and the [`faucet-cli`](https://crates.io/crates/faucet-cli) binary — you only depend on it directly when writing a connector or driving a pipeline from Rust.

## Quick start — a minimal connector

A source needs just one method; everything else has a default.

```rust
use faucet_core::{async_trait, FaucetError, Source, Sink, Value, json};

struct MySource;

#[async_trait]
impl Source for MySource {
    async fn fetch_with_context(
        &self,
        _ctx: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![json!({ "id": 1 }), json!({ "id": 2 })])
    }

    // Optional overrides: fetch_with_context_incremental (bookmarks),
    // stream_pages (native streaming), config_schema, connector_name, …
}

struct MySink;

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        for r in records {
            println!("{r}");
        }
        Ok(records.len())
    }
    // Optional: flush(), config_schema(), supported_write_modes(), …
}
```

Then connect them with a `Pipeline`:

```rust
use faucet_core::Pipeline;

# async fn run(source: MySource, sink: MySink) -> Result<(), faucet_core::FaucetError> {
let result = Pipeline::new(&source, &sink).run().await?;
println!("Wrote {} records", result.records_written);
# Ok(())
# }
```

## The `Source` trait

`Source: Send + Sync`. Implement at least `fetch_with_context`; the rest have defaults.

| Method | Purpose | Default |
|--------|---------|---------|
| `fetch_with_context(ctx)` | **Required.** Fetch all records, resolving `{placeholder}` tokens from a parent-record `ctx`. | — |
| `fetch_all()` | Convenience: fetch with an empty context. | delegates to `fetch_with_context` |
| `fetch_with_context_incremental(ctx)` | Fetch records + an optional bookmark `Value` for incremental replication. | `(records, None)` |
| `fetch_all_incremental()` | Convenience: incremental fetch, empty context. | delegates above |
| `stream_pages(ctx, batch_size)` | Stream `StreamPage`s so the pipeline writes per page (bounded memory). | chunks `fetch_*_incremental` in memory |
| `config_schema()` | JSON Schema of the config struct (via `schema_for!`). | empty object |
| `state_key()` | `Some(key)` opts into resumable runs (read before fetch, persist after sink confirms). | `None` |
| `apply_start_bookmark(value)` | Apply a bookmark from the state store as the run's start point. | no-op |
| `capture_resume_position()` | Capture the current replication position without consuming changes (used by `faucet replicate`). | `None` |
| `supports_exactly_once()` | `true` only for sources that deterministically replay from a bookmark (CDC). | `false` |
| `connector_name()` | Stable `&'static str` label for metrics/spans. | stripped `type_name` |
| `dataset_uri()` | Credential-free OpenLineage dataset URI. | `"<connector_name>://unknown"` |
| `check(ctx)` | Non-mutating preflight probe for `faucet doctor`. | pulls one page via `stream_pages` |
| `lag()` | How far the source is behind its head — `SourceLag { bytes, events, seconds }` (#733). Polled by the pipeline on the first page, at most every 15 s after, and at the end of the run; must be cheap and read-only. | `Ok(None)` |
| `state_schema()` | Version of this source's bookmark shape, stored in the versioned state envelope (#736). Bump it when the shape changes. | `0` |
| `migrate_state(from, data)` | Pure migration of a bookmark stored at shape version `from` to `state_schema()`. | identity at the current version, error otherwise |

## The `Sink` trait

`Sink: Send + Sync`. Implement at least `write_batch`.

| Method | Purpose | Default |
|--------|---------|---------|
| `write_batch(records)` | **Required.** Write a batch; return count written. | — |
| `flush()` | Flush buffered data (Parquet footer, S3 multipart, …). | no-op |
| `write_batch_partial(records)` | Per-row `RowOutcome`s so failed rows can be routed to a DLQ. | maps a single success → all-`Ok` |
| `supports_idempotent_writes()` | `true` for sinks that commit rows + a commit token atomically. | `false` |
| `write_batch_idempotent(records, scope, token)` | Atomic effectively-once write + watermark. | delegates to `write_batch` (not idempotent) |
| `last_committed_token(scope)` | Highest committed token for resume-skip. | `None` |
| `supported_write_modes()` | The `WriteMode`s this sink accepts (`Append` / `Upsert` / `Delete`). | `[Append]` |
| `batch_atomicity()` | What a failed `write_batch` leaves behind — `Atomic` (nothing), `PerRow` (per-row outcomes), `BestEffort` (unknown). The pipeline refuses `on_batch_error: dlq_all` on a `BestEffort` sink that does not dedup by key (#737). | `BestEffort` |
| `config_schema()` | JSON Schema of the config struct. | empty object |
| `connector_name()` / `dataset_uri()` | Metrics label / lineage URI. | as for `Source` |
| `local_outputs()` | The concrete **local files** this sink opened, for faucet's local-output retention GC. Return one entry per file (a rolling parquet sink returns each part), each flagged `pre_existing` if the file already existed — such a file is never deleted. | `[]` (nothing local to collect) |
| `check(ctx)` | Non-mutating preflight probe. | `not_implemented` (sinks override with a connect/auth probe) |

## The `AuthProvider` trait

A live, shareable source of credentials. Multiple connectors hitting the **same** identity provider hold an `Arc<dyn AuthProvider>` ([`SharedAuthProvider`]) and ask for the current `Credential` per request — so N connectors share one token with **single-flight refresh** instead of racing.

```rust
use faucet_core::{async_trait, AuthProvider, Credential, FaucetError};

#[derive(Debug)]
struct StaticToken(String);

#[async_trait]
impl AuthProvider for StaticToken {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(self.0.clone()))
    }
    fn provider_name(&self) -> &'static str { "static" }
}
```

- `Credential` variants: `Bearer(token)`, `Header { name, value }`, `Basic { username, password }`, `Token(raw)`. Its `Debug` impl redacts secrets as `"***"` — part of the frozen 1.0 contract.
- `invalidate(stale)` is a compare-and-swap refresh: connectors that all hit a `401` with the same token collapse into one refresh.
- `AuthSpec<A>` is the connector config field: it accepts **either** inline `{ type, config }` auth **or** a `{ ref: <name> }` pointer to a provider in the top-level `auth:` catalog. `ref` and inline fields are mutually exclusive (enforced at deserialize time).

Concrete HTTP-based providers (OAuth2 client-credentials / refresh, token-endpoint) live in the separate [`faucet-auth`](https://crates.io/crates/faucet-auth) crate so `faucet-core` stays free of an HTTP-client dependency.

## Pipeline & streaming

```rust
use faucet_core::{Pipeline, run_stream};

// Batch mode: fetch all, then write.
let result = Pipeline::new(&source, &sink).run().await?;
println!("Wrote {} records", result.records_written);

// Streaming mode: write page-by-page (bounded memory).
let ctx = std::collections::HashMap::new();
let result = run_stream(source.stream_pages(&ctx, 1000), &sink).await?;
```

`Pipeline` builders compose the reliability features below:

```rust
use faucet_core::{Pipeline, CancellationToken};

let result = Pipeline::new(&source, &sink)
    .with_state_store(state_store)   // durable bookmarks
    .with_dlq(dlq_config)            // dead-letter routing
    .with_quality(compiled_quality)  // per-page data-quality checks
    .with_contract(compiled_contract) // versioned data contract (contract feature)
    .with_cancel(CancellationToken::new()) // flush-completing cooperative cancel
    .run()
    .await?;
```

### `stream_pages` and `batch_size`

`stream_pages(ctx, batch_size)` returns a `Stream<Item = Result<StreamPage, FaucetError>>`; each `StreamPage` is a chunk of records plus an optional bookmark. The default implementation chunks `fetch_*_incremental` in memory; sources that can stream natively (REST, CDC, query DBs with cursor pagination) override it to bound source-side memory at O(batch_size). `Pipeline::run` drives this internally — library users rarely call it directly.

- `DEFAULT_BATCH_SIZE` = `1000`, `MAX_BATCH_SIZE` = `1_000_000`.
- `validate_batch_size(n)` enforces the range with `FaucetError::Config` for config-load-time validation.
- **`batch_size = 0` is the "no batching" sentinel** — sources emit the entire result set in a single `StreamPage` (and sinks that expose their own `batch_size` accept whatever upstream hands them without re-chunking). Use it for small lookup tables or bulk-load sinks (SQL `COPY`, BigQuery load jobs).

### Cooperative cancellation

`Pipeline::with_cancel(CancellationToken)` (and `RunStreamOptions::with_cancel`) attach a cancel token. When cancelled mid-run, the page loop stops at the next page boundary, **flushes the sinks** (so a buffered Parquet/S3 sink writes its footer instead of orphaning the file), and returns `Ok` with the partial result. `CancellationToken` is re-exported from `faucet-core`, so callers need not add `tokio-util`.

## Transforms

Transform records as they flow through the pipeline. `RecordTransform` covers per-record (1→1) operations; stage-level variants add filter, explode, and CDC-unwrap.

```rust
use faucet_core::{RecordTransform, KeyCaseMode};

// Flatten nested objects: {"user": {"id": 1}} -> {"user__id": 1}
RecordTransform::Flatten { separator: "__".into() };

// Convert keys to snake_case (or camel / pascal / kebab / screaming_snake / dot)
RecordTransform::KeysCase { mode: KeyCaseMode::Snake };

// Regex key renaming
RecordTransform::RenameKeys { pattern: r"^_sdc_".into(), replacement: "".into() };

// Custom closure
RecordTransform::custom(|record| record);
```

Built-in `RecordTransform` variants (each feature-gated `transform-<name>`; the `transforms` aggregate enables all): `flatten`, `rename_keys` (regex), `keys_case`, `select`, `drop`, `set`, `rename_field`, `cast` (`CastType` + `CastOnError`), `redact`, `value_case` (`ValueCaseMode`), `spell_symbols`, plus `Custom` closures.

Reshape transforms compile to an object-safe `TransformStage::Custom` (1→0..N), so they need no dedicated stage-enum variant: `UnpivotSpec` (wide/map → long), `TreeFlattenSpec` (recursive report tree / matrix → one row per leaf — the nested accounting-report shape; feature `transform-tree-flatten`). Build one with `spec.into_stage()?`.

Stage-level transforms (`TransformStage`, in the `stage` module): `Map(RecordTransform)` (1→1), `Filter` (1→0|1), `Explode` (1→0..N), `CdcUnwrap` (normalize a CDC envelope into a flat row + `__op` marker — the standard pairing for an upsert sink), and `PageFn` (whole-page closure). Attach any set of stages to any source with `TransformingSource` — the canonical way library callers add transforms:

```rust
use faucet_core::{TransformingSource, TransformStage, RecordTransform};

let stages = vec![TransformStage::Map(
    RecordTransform::Flatten { separator: "__".into() }.compile()?,
)];
let wrapped = TransformingSource::new(source, stages);
```

## Backend calls: `CallContext`, `http::Client`, batching

Every call a connector makes to its backend runs through
`faucet_core::io::CallContext` (RFC 0013, #880). `ctx.call(op, || fut)` applies
the per-attempt timeout (`DEFAULT_CALL_TIMEOUT`, 300 s, unless set), the run's
deadline, classified retry under a `RetryPolicy` (a
`FaucetError::RateLimited(wait)` waits the time it states; an optional
separate rate-limit budget), cancellation, round-trip metering (`op` is the
`faucet_*_roundtrips_total` label) and redaction of the returned error.
`ctx.wait(d)` is the cancel- and deadline-aware pause for poll intervals.

A connector keeps a `CallSlot`: its own settings from config, plus the context
the pipeline installs through the defaulted `Source::set_call_context` /
`Sink::set_call_context` (every wrapper forwards it). The pipeline's context
carries the round-trip recorder, any deadline from
`Pipeline::with_call_context`, and — for a source only — the run's cancel
token; it never carries a retry policy.

```rust
use faucet_core::io::{CallContext, CallSlot};
use faucet_core::http::Client;

let client = Client::builder().timeout(std::time::Duration::from_secs(30)).build()?;
let calls = CallSlot::new(CallContext::new().with_retry(faucet_core::RetryPolicy::default()));
let resp = client.execute(&calls.get(), "page", || client.get("https://api.example.com/items")).await?;
let body: serde_json::Value = client.json(resp).await?;
```

`faucet_core::http::Client` wraps reqwest: redirects followed by faucet, which
drops credential headers (and never replays a body) on a hop to another
origin; bounded buffered reads (`max_response_bytes`, 1 GiB default); one
status-error shape. `faucet_core::batching` re-chunks under the `batch_size`
contract (`chunks`, `into_chunks`) and bounds concurrency (`map_concurrent`,
`for_each_concurrent`, `ConcurrencyLimit`).

The workspace `clippy.toml` disallows constructing reqwest clients directly and
`tokio::time::sleep` / `sleep_until` / `std::thread::sleep`; a sanctioned site
carries `#[allow(clippy::disallowed_methods)] // <reason>`.

## Resource annotations and reference policies

Mark a config field that names a local path, a URL or a program with
`#[schemars(extend("x-faucet-resource" = "path"))]` (`"url"`, `"command"`).
`faucet_core::resolve` reads the annotation back from a config schema
(`annotated_resources(schema, config)`, `declares(schema, kind)`) and checks
each value with a `ReferencePolicy`: an allow-list of path roots (compared
after symlinks resolve), URL schemes and hosts (link-local and cloud-metadata
hosts refused unless listed), environment variable names, secret-manager
schemes and whether a program may run. `ReferencePolicy::deny_all()` is the
default and `open()` allows everything but metadata hosts. `faucet serve` uses
it to confine configs a caller submits, so an annotated third-party connector
is confined without serve changes.

A connector whose own input can reach host files or the network — a SQL
engine that can `ATTACH` or `read_csv` — exposes a boolean switch that confines
it and marks it `#[schemars(extend("x-faucet-sandbox" = true))]`;
`resolve::sandbox_fields(schema)` reads the marked fields back, and a host
loading a config it does not trust forces each one on.

## Error types

`FaucetError` covers every failure mode:

| Variant | Use case |
|---------|----------|
| `Http(reqwest::Error)` | HTTP transport errors |
| `HttpStatus { status, url, body }` | Non-success HTTP responses (truncated body) |
| `Json(serde_json::Error)` | JSON parse/serialize errors |
| `JsonPath(String)` | JSONPath extraction failures |
| `Auth(String)` | Authentication errors |
| `RateLimited(Duration)` | 429 responses (wait duration from `Retry-After`) |
| `Url(String)` | URL construction/parse errors |
| `Transform(String)` | Record-transform compile/apply errors |
| `Config(String)` | Configuration / validation errors |
| `Source(String)` | Source-specific errors |
| `Sink(String)` | Sink-specific errors |
| `QualityFailure { check, message }` | A quality check failed under `abort` |
| `ContractViolation { version, message }` | A record breached the data contract under `on_breach: fail` |
| `State(String)` | State-store read/write/delete failures |
| `StateIncompatible { key, found, expected }` | A stored bookmark written by a newer release or another source (#736) — refused before the source is read |
| `Timeout { operation, after }` | A call did not finish within its bound (retriable) |
| `Cancelled { operation }` | A backend call was abandoned because the run was cancelled (#880) |
| `Custom(Box<dyn Error + Send + Sync>)` | Wrap any third-party connector error |

`FaucetError::is_retriable()` classifies transient failures (network errors, 5xx, 429); connectors retry through `CallContext::call` (see [Backend calls](#backend-calls-callcontext-httpclient-batching)). The `Custom` variant is intentionally permanent in the public API so connector authors can wrap their own error types without losing the chain.

## State stores (resume & bookmarks)

The `StateStore` trait is a minimal async key/value store over `Value` (`get` / `put` / `delete`). Sources that override `state_key()` opt into resumable runs: the pipeline reads the bookmark before fetching and persists the new one **only after the sink confirms** the batch.

| Backend | Crate | Use when |
|---------|-------|----------|
| `MemoryStateStore` | `faucet-core` | Tests, or runs that start fresh each time. |
| `FileStateStore` | `faucet-core` | One JSON file per key, written via atomic rename (crash-safe). |
| `RedisStateStore` | [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis) | Shared bookmarks across hosts. |
| `PostgresStateStore` | [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres) | Durable bookmarks in an existing Postgres. |

```rust
use faucet_core::{Pipeline, FileStateStore};
use std::sync::Arc;

let store = Arc::new(FileStateStore::new("./state"));
let result = Pipeline::new(&source, &sink)
    .with_state_store(store)
    .run()
    .await?;
```

State keys are validated via `state::validate_state_key`.

Runtimes that drive many pipelines build their keys with `StateKey` (#882): a
typed key from its components (tenant, pipeline, row, parent key, shard, a
`KeySuffix` marker) with one rendering,
`[{tenant}::]{pipeline}::{row}[::{sub}][::{shard}][::{suffix}]`. The rendering
is versioned by `KEY_SCHEME_VERSION`; `StateKey::legacy_renderings()` lists the
keys earlier schemes rendered, and `migrate_state_key(store, key, candidates)`
moves a value left under one of them to the current key (only while the current
key is empty), so a rendering change never turns a resumable pipeline into a
first run.

```rust
use faucet_core::{KeySuffix, StateKey};

let key = StateKey::invocation("orders", "lines").with_parent_key("42");
assert_eq!(key.render(), "orders::lines::42");
let sla = key.with_suffix(KeySuffix::Sla);
assert_eq!(sla.render(), "orders::lines::42::__sla__");
```

The pipeline stores each bookmark in a **versioned envelope** (#736,
`state_version`): `{ "faucet_state": 1, "owner": <connector_name>, "schema":
<state_schema>, "data": <bookmark> }`. On resume it refuses
(`FaucetError::StateIncompatible`) a value from a newer release or another
source, migrates an older shape through `Source::migrate_state`, and reads a
bare pre-envelope value as schema 0. `Pipeline::with_legacy_state_writes(true)`
writes the bare value instead — for a mixed-version cluster.

## Dead-letter queue (DLQ)

`Pipeline::with_dlq(DlqConfig)` attaches an optional DLQ sink. The streaming loop calls `Sink::write_batch_partial` per page; rows that come back `Err` are wrapped in a fixed-shape envelope (`build_envelope`) and routed to the DLQ before the page bookmark advances. `OnBatchError` controls the policy when a sink can't report per-row results: `propagate` aborts the run; `dlq_all` routes the whole failed page. Sinks override `write_batch_partial` to expose per-row results (BigQuery `insertAll`, Elasticsearch `_bulk`).

## Governance (RFC 0013)

`faucet_core::Governance` carries every guarantee a run is held to — masking,
quality, contract, schema drift, the data-flow policy, budget, profiling,
metadata columns, metering, resilience, cancellation and the DLQ — and
assembles them in one canonical order. Wrap the destination with
`sink_builder()` and attach the value with `Pipeline::with_governance`:

```rust,ignore
let governance = Governance::new()
    .with_masking(masking)
    .with_budget(BudgetSpec { max_records: Some(1_000_000), ..Default::default() })
    .with_dlq(DlqGovernance::new(dlq).with_facts(dlq_facts))
    .with_cancel(cancel)
    .without(Pass::Profiling, "a backfill would skew the baseline");
let sink = governance.sink_builder().build(Box::new(destination));
Pipeline::new(&source, &sink).with_governance(governance).run().await?;
```

Around the sink the order is `policy → profiling → [outer tap] → metadata →
[inner tap] → budget → destination` (`SINK_LAYERS`); per page it is masking →
quality → contract → drift. The DLQ is governed as a destination of its own:
masking rules naming it, the policy with its own attributes, the shared budget
and metering. A pass that does not apply is turned off with
`without(pass, reason)`, which is recorded (`opt_outs()`) and logged.

## Row accounting

Every row offered to a sink is accounted for: `offered = accepted + coalesced +
skipped + failed + quarantined + dropped`. The pipeline writes through
`Sink::write_batch_reported`, whose default wraps `write_batch`'s count
(crediting a keyed sink's merged rows as coalesced); a shortfall fails the page
with `FaucetError::RowsUnaccounted` before its bookmark advances. The
exactly-once path does the same through `Sink::write_batch_idempotent_reported`
(default: `write_batch_idempotent`'s count): a short report fails the page after
the sink committed its token and before the bookmark is persisted, so the next
run resumes from the sink's watermark and the run is never reported green.
`Pipeline::with_row_accounts(Arc<RowAccounts>)` exposes the counts, including
`non_finite` — values with no JSON rendering a source wrote as `null`.

## Value fidelity

`fidelity` is the single definition of how a typed backend value becomes JSON
(RFC 0013, #885): exact decimals and integers past `i64`/`u64` as strings,
floats with `NaN`/`±Infinity` as a counted `null`, binary as base64, instants
as RFC 3339 with an offset at the unit's precision. Every built-in connector
renders through it; a connector author should too:

```rust
use faucet_core::fidelity;

assert_eq!(fidelity::decimal_from_i128(150, 2), serde_json::json!("1.50"));
assert_eq!(fidelity::integer_i128(1 << 70), serde_json::json!("1180591620717411303424"));
assert_eq!(fidelity::binary(&[0, 255]), serde_json::json!("AP8="));
// A JSON payload, every number exact.
let v = fidelity::from_str(r#"{"id": 18446744073709551616}"#).unwrap();
assert_eq!(v["id"], serde_json::json!("18446744073709551616"));
```

`Pipeline::run` counts the non-finite values a run's source wrote as `null`
(`RowAccountsSnapshot::non_finite`, `faucet_fidelity_non_finite_total`). See
the [value fidelity reference](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html).

## Effectively-once delivery

`DeliveryMode` (`AtLeastOnce` default, or `ExactlyOnce`) controls run semantics. Under `ExactlyOnce` the pipeline assigns a monotonic fixed-width commit token (`format_token(seq)`) to each bookmark-carrying page and calls `write_batch_idempotent(records, scope, token)`; the sink commits records **and** token atomically. On resume, `last_committed_token(scope)` is consulted to skip already-committed pages.

Effectively-once requires a **deterministic-replay source** (`supports_exactly_once() == true` — CDC sources) and an **idempotent sink** (`supports_idempotent_writes() == true`). The bookmark + sequence persist together via `wrap_state(bookmark, seq)` / `unwrap_state(value)`.

## Write modes (upsert / delete)

`faucet_core::write_mode` is the unified upsert layer. `WriteSpec` (`write_mode` + `key` + `delete_marker`) flattens into a capable sink's config; `plan_writes(page, &spec) -> WritePlan { upserts, deletes, failed }` is the single partitioning routine (last-write-wins dedup by `key`; missing/null-key rows → `failed` for DLQ/abort). Sinks advertise support via `supported_write_modes()`. Helpers: `key_to_doc_id`, `key_to_filter`, `KeyTuple`.

## Data-quality checks

Add a `quality:` block (sibling of `transforms:` and `dlq:` under `pipeline:`) to assert invariants on every page before records reach the sink. The quality pass runs **after** transforms and **before** `write_batch`: per-record checks partition the page into survivors and quarantined rows, then per-batch checks run over the survivors. Quarantined rows are routed to the DLQ.

Enable with the `quality` Cargo feature (base) or `quality-jsonschema` (adds the `json_schema` record check).

### Check catalog

**Per-record checks** — each record is evaluated in declared order; first failure wins. `on_failure` may be `quarantine` (route the row to the DLQ) or `abort` (fail the run with `FaucetError::QualityFailure`).

| Check | Key config fields | Passes when | Missing field |
|---|---|---|---|
| `not_null` | `field`, `treat_missing_as_null` (default `true`) | value present and non-null | fail (pass iff `treat_missing_as_null: false`) |
| `not_empty` | `field` | value is a non-empty string after `trim()` | fail |
| `regex_match` | `field`, `pattern` | value is a string matching `pattern` | fail |
| `value_in_set` | `field`, `values: [...]` | value is in `values` (exact JSON equality) | fail |
| `not_in_set` | `field`, `values: [...]` | value is NOT in `values` | pass (trivially not in set) |
| `compare` | `field`, `op` (`gt`/`gte`/`lt`/`lte`/`eq`/`ne`), `value` | ordering or equality holds | fail |
| `type_is` | `field`, `expected` (`boolean`/`number`/`string`/`array`/`object`/`null`) | JSON type matches | fail |
| `string_length` | `field`, `min?`, `max?` (at least one required) | char count in `[min, max]` | fail |
| `json_schema` *(quality-jsonschema feature)* | `schema` (JSON Schema doc) | record validates against `schema` | (whole-record check; always evaluated) |

`json_schema` is the most expressive check; its cost scales with schema complexity — for very large or deeply nested schemas on hot paths, prefer the granular checks above and benchmark your case.

**Per-batch checks** — evaluated per page over the survivors. `on_failure` for aggregate checks (`row_count`, `null_rate`, `distinct_count`) may be `abort` or `quarantine_batch` (route all current survivors to the DLQ, write nothing this page). `unique` is row-attributable and accepts `quarantine` or `abort`.

| Check | Key config fields | Passes when |
|---|---|---|
| `row_count` | `min?`, `max?` (at least one required) | survivor count in `[min, max]` |
| `null_rate` | `field`, `max: f64` (0.0–1.0) | null-or-missing rate ≤ `max`; zero survivors → 0.0 → pass |
| `unique` | `fields: [...]` (composite key, ≥1) | every survivor's key is unique within the page |
| `distinct_count` | `field`, `min?`, `max?` | distinct values of `field` in `[min, max]` |

### `on_failure` policies

| Policy | Meaning | Allowed on |
|---|---|---|
| `quarantine` | Route the specific offending row(s) to the DLQ; keep the rest | per-record checks; `unique` |
| `quarantine_batch` | Route all survivors of the page to the DLQ; write nothing this page | aggregate batch checks |
| `abort` | Surface `FaucetError::QualityFailure` and fail the run | every check |

**`quarantine` and `quarantine_batch` require a DLQ sink.** Configuring either without a `dlq:` block is rejected at config-load time with `FaucetError::Config`.

### Example (YAML, via the CLI)

```yaml
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com/v1
      path: /users
      method: GET
      auth: { type: bearer, config: { token: "${env:API_TOKEN}" } }
      pagination: { type: Cursor, next_token_path: $.meta.next_cursor, param_name: cursor }

  transforms:
    - type: keys_case
      config: { mode: snake }

  quality:
    record:
      - type: not_null
        field: id
        on_failure: abort
      - type: regex_match
        field: email
        pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
        on_failure: quarantine
    batch:
      - type: row_count
        min: 1
        on_failure: abort
      - type: unique
        fields: [id]
        on_failure: quarantine

  dlq:
    sink:
      type: file
      config: { path: ./dlq/quality_failures.jsonl }
    max_failures_per_page: 50
    max_failures_total: 500

  sink:
    type: postgres
    config:
      connection_url: "${env:PG_URL}"
      table_name: users
      column_mapping: { jsonb: { column: data } }
      batch_size: 500
```

### Quality — Rust API

```rust
use faucet_core::{Pipeline, CompiledQuality, QualitySpec};

let quality_spec: QualitySpec = serde_json::from_value(/* ... */)?;
let compiled = CompiledQuality::compile(&quality_spec)?;
let result = Pipeline::new(&source, &sink)
    .with_dlq(dlq_config)   // required when any check uses quarantine
    .with_quality(compiled)
    .run()
    .await?;
```

## Data contracts

A `contract:` block (the `contract` Cargo feature) declares a **versioned
promise** about the pipeline's output — required fields, types, nullability,
enum sets, regex patterns, numeric/length bounds — enforced per page after the
quality pass and before the sink write. The contract-level `on_breach` policy
picks the enforcement: `fail` (default — abort on the first breach, writing
nothing from the page), `quarantine` (route breaching records to the DLQ,
write the rest), or `warn` (log + count, write everything). Compilation is
fail-fast: a malformed contract (bad regex, duplicate fields, constraints on
the wrong type) is a `Config` error at load time.

```yaml
contract:
  version: "1.0.0"
  on_breach: quarantine        # fail (default) | quarantine | warn
  allow_extra_fields: true
  fields:
    - { name: order_id, type: string, min_length: 1 }
    - { name: status, type: string, enum: [open, shipped, cancelled] }
    - { name: amount, type: number, min: 0, required: false, nullable: true }
```

### Contracts — Rust API

```rust
use faucet_core::{CompiledContract, ContractSpec, Pipeline};
use std::sync::Arc;

let spec: ContractSpec = serde_json::from_value(/* ... */)?;
let compiled = Arc::new(CompiledContract::compile(&spec)?);
let result = Pipeline::new(&source, &sink)
    .with_dlq(dlq_config)      // required when on_breach = quarantine
    .with_contract(compiled)
    .run()
    .await?;
```

Machine-readable exports for downstream consumers are plain functions:
`contract::to_json_schema(&spec)` (a standalone JSON Schema document) and
`contract::to_openlineage_facet(&spec, producer)` (an OpenLineage
`SchemaDatasetFacet`). The CLI surfaces them as `faucet contract --export`.

## PII masking

The `masking` feature adds a `MaskingSpec` policy that classifies sensitive
fields — by field-name pattern (regex over the dot-path), by value detector
(`email` / `credit_card` (Luhn) / `ssn` / `phone` / `phone_loose` / `ipv4`), or by explicit
field list — and rewrites them per action: `redact` (fixed mask), `hash`
(HMAC-SHA256 keyed / SHA-256 unkeyed; deterministic, so masked values stay
joinable), `tokenize` (short opaque token), or `partial` (reveal only the last N
chars). Detectors are conservative (anchored; cards require a valid Luhn
checksum) so silent over-masking is rare. `credit_card` and `phone_loose` can
match a JSON number, so a policy holding either writes every number as a string,
keeping each column one type on every page.

The masking pass runs **first** — before the quality, contract, and drift passes
and before every sink write — so PII never reaches a sink, the DLQ, or a lineage
sample unmasked. It never fails a run or quarantines (matching fields are
rewritten in place), so no DLQ is required.

```rust
use faucet_core::{CompiledMasking, MaskingSpec, Pipeline};
use std::sync::Arc;

let spec: MaskingSpec = serde_json::from_value(/* ... */)?;
let compiled = Arc::new(CompiledMasking::compile(&spec)?);
let result = Pipeline::new(&source, &sink)
    .with_masking(compiled)
    .run()
    .await?;
```

## Config loading & schema

Load any `Deserialize`-able config from JSON files or environment variables:

```rust
use faucet_core::config::{load_json, load_env, load_env_file};

let config: MyConfig = load_json("config.json")?;          // from a JSON file
let config: MyConfig = load_env("MYAPP")?;                 // from MYAPP_* env vars
let config: MyConfig = load_env_file(".env", "MYAPP")?;    // .env file + env vars
```

For `Duration` config fields, use the provided serde modules:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct MyConfig {
    #[serde(with = "faucet_core::config::duration_secs")]
    timeout: std::time::Duration,                         // u64 seconds
    #[serde(with = "faucet_core::config::duration_secs_option", default)]
    retry_delay: Option<std::time::Duration>,             // Option<u64>
}
```

All config structs derive `schemars::JsonSchema`; implement `config_schema()` with `schema_for!` so the CLI's `faucet schema source|sink <name>` and `faucet init` can introspect them:

```rust
use faucet_core::{schema_for, JsonSchema};

#[derive(serde::Serialize, serde::Deserialize, JsonSchema)]
struct MyConfig { url: String, batch_size: usize }

let schema = serde_json::to_value(schema_for!(MyConfig))?;
```

## Re-exports for connector authors

`faucet-core` re-exports the dependencies a connector needs, so a third-party crate depends only on `faucet-core`:

| Re-export | From | Why |
|-----------|------|-----|
| `async_trait` | `async-trait` | Implement the `Source` / `Sink` async traits |
| `serde_json`, `Value`, `json!` | `serde_json` | The record type and JSON literals |
| `schemars`, `JsonSchema`, `schema_for!` | `schemars` | Config-schema introspection |
| `Stream`, `async_stream`, `futures_core` | `futures-core` / `async-stream` | Implement `stream_pages` |
| `CancellationToken` | `tokio-util` | Name the token for `with_cancel` without adding `tokio-util` |

> **Keep `faucet-core` the only required dependency.** If your connector needs a new common dependency, propose re-exporting it from `faucet-core` rather than requiring every connector author to add it. Keep the `Source` / `Sink` traits object-safe (no connector-specific types or generics on methods) and `FaucetError::Custom` intact for third-party error wrapping.

## Wrapping a source or sink

A wrapper that holds another connector must forward every defaulted hook, or the
feature behind it turns off for everything it wraps. `forward_source!` /
`forward_sink!` generate the forwarding body of every hook inside the wrapper's
`impl` block; the wrapper writes only the hooks it changes and lists them (plus
any it deliberately leaves at the trait default) after `except`:

```rust,ignore
#[async_trait]
impl Sink for Stamped {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.inner.write_batch(&self.stamp(records)).await
    }
    forward_sink!(inner; except write_batch, write_batch_partial, write_batch_idempotent,
        supports_columnar, write_batch_columnar, native_load_capabilities, load_native);
}
```

An unknown name in `except` does not compile, so the list stays exact. Prove the
wrapper with `faucet_conformance::forwarding::assert_sink_forwards_every_hook` /
`assert_source_forwards_every_hook`.

## Modules

| Module | Contents |
|--------|----------|
| `traits` | `Source` and `Sink` async traits; `RowOutcome` |
| `local_outputs` | `LocalOutput`, `LocalOutputLog`, `probe_pre_existing` — the provenance a local-file sink records so faucet's retention GC deletes only files it created |
| `auth` | `AuthProvider`, `Credential`, `AuthSpec`, `SharedAuthProvider` |
| `pipeline` | `Pipeline`, `PipelineResult`, `StreamPage`, `run_stream`, batch-size constants |
| `error` | `FaucetError` enum + `is_retriable` |
| `config` | `load_json`, `load_env`, `load_env_file`, duration serde helpers |
| `transform` | `RecordTransform`, `CompiledTransform`, support enums (`CastType`, `CastOnError`, `KeyCaseMode`, `ValueCaseMode`) |
| `transforming_source` | `TransformingSource` — attach stages to any source |
| `forward` | `forward_source!` / `forward_sink!` — generate a wrapper's pass-through hooks (see below) |
| `stage` | `TransformStage`, `FilterSpec`, `ExplodeSpec`, `CdcUnwrapSpec`, `compile_stage` |
| `state` | `StateStore` trait, `MemoryStateStore`, `FileStateStore`, `validate_state_key` |
| `dlq` | `DlqConfig`, `OnBatchError`, `DlqReason`, `DlqStats`, `build_envelope` |
| `governance` | `Governance`, `GovernedSinkBuilder`, `GovernedSink`, `DlqGovernance`, `PolicyGovernance`, `Pass`, `OptOut`, `SINK_LAYERS` — one builder for every guarantee (#875, #870) |
| `accounting` | `WriteReport`, `RowAccounts`, `RowAccountsSnapshot` — per-page row accounting (#879) |
| `fidelity` | Canonical value renderings (#885) — `decimal_text`, `decimal_from_i128`, `decimal_from_pg_numeric`, `integer_*`, `number_text`, `float64` / `float32`, `binary`, `timestamp*`, `date`, `time`, `from_str` / `from_slice` / `parse_json`, `non_integer_text`, `is_exact_decimal_type`, `NonFiniteCounter` |
| `json_numbers` | `parse_json_exact` / `JsonBigNumbers` — exactness of numbers in JSON text a database stored |
| `idempotency` | `DeliveryMode`, `format_token`, `parse_token`, `wrap_state`, `unwrap_state` |
| `state_version` | Versioned state envelope (#736) — `StoredState`, `wrap_versioned`, `peel_versioned`, `check_compat`, `resolve_for_source`, `StateCodec`, `VersionedStateStore` |
| `write_mode` | `WriteMode`, `WriteSpec`, `DeleteMarker`, `plan_writes`, `WritePlan` |
| `quality` | Per-record / per-batch checks (`quality` / `quality-jsonschema` features) |
| `contract` | Versioned data contracts — `ContractSpec`, `CompiledContract`, `apply_contract`, JSON-Schema / OpenLineage exports (the `contract` feature) |
| `masking` | PII detection + column-level masking — `MaskingSpec`, `CompiledMasking`, `apply_masking`, `Detector`, `MaskAction` (the `masking` feature) |
| `replication` | `ReplicationMethod`, `filter_incremental`, `max_replication_value` |
| `retry` | `execute_with_retry` (exponential backoff + jitter) |
| `io` | `CallContext`, `CallSlot`, `DEFAULT_CALL_TIMEOUT` — the backend-call wrapper (#880) |
| `http` | `Client`, `ClientBuilder`, `status_error`, `is_cross_origin`, `strip_credentials`, `ResponseTooLarge` |
| `batching` | `chunks`, `into_chunks`, `map_concurrent`, `for_each_concurrent`, `ConcurrencyLimit` |
| `schema` | `infer_schema` from record samples |
| `check` | `CheckContext`, `Probe`, `CheckReport` for `faucet doctor` |
| `observability` | Pipeline-internal `tracing`/`metrics` decorators; `install_observability` |
| `compression` | `CompressionConfig`, `compress_buf` (the `compression` feature) |
| `file_format` | One format vocabulary for every file connector — `FileFormat` (incl. `from_path` extension detection), `decode` / `encode`, `ContainerDecoder` (Avro / ORC across many files, resolved against the first file's schema), `avro` (OCF read + write, explicit logical-type mapping, schema inference), `orc` (read-only, projection, stripe streaming) |
| `util` | `quote_ident`, `extract_records`, `check_http_response`, `redact_uri_credentials` |

## Feature flags

Defaults: `transform-flatten`, `transform-rename-keys`, `transform-keys-case`.

| Feature | Enables |
|---------|---------|
| `transform-<name>` | One built-in transform (`flatten`, `rename-keys`, `keys-case`, `select`, `drop`, `set`, `rename-field`, `cast`, `redact`, `value-case`, `spell-symbols`, `filter`, `explode`, `cdc-unwrap`) |
| `transforms` | All built-in transforms |
| `quality` | The 12 base per-record / per-batch quality checks |
| `quality-jsonschema` | Adds the `json_schema` record check (pulls `jsonschema`) |
| `contract` | Versioned data contracts (the `contract:` config block + enforcement pass) |
| `masking` | PII detection + column-level masking (the `masking:` config block; pulls `regex`+`sha2`+`hmac`) |
| `compression` | `CompressionConfig` + gzip/zstd helpers |
| `file-format-csv` / `-xml` / `-excel` | The CSV, XML and Excel codecs in `file_format` |
| `file-format-avro` | Avro Object Container Files (`apache-avro`) — read with an optional reader schema, write with a supplied or inferred schema and a `null` / `deflate` / `snappy` / `zstd` codec |
| `file-format-orc` | ORC reading (`orc-rust`, Arrow-native, so it turns on `arrow`); read-only by design |
| `file-formats` | All of the above |
| `observability-install` | `install_observability` (Prometheus exporter + tracing subscriber) |
| `otel` | OTLP export of traces and metrics (`OtelSignal::Logs` is accepted in `export` for the CLI's durable log shipping, #806; this crate's installer does not export logs itself). A host that installs its own global subscriber first registers a `tracing_subscriber::reload` slot with `observability::otel::register_trace_layer_slot`; `install_observability` puts the OTLP trace layer there. A host that renders `/metrics` from its own Prometheus recorder installs `observability::install_prometheus_with_otel_metrics` to fan its metrics out to OTLP too |

> Connector authors: enable in your **own** `Cargo.toml` every feature your crate uses — the feature-isolation CI matrix builds each connector alone, so relying on workspace feature unification compiles locally but fails CI.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `the trait Source is not object-safe` / can't `Box<dyn Source>` | You added a method with a generic or an associated type. Keep trait methods concrete; give new methods a default impl so existing connectors don't break. |
| Records aren't being transformed when driving a source from Rust | Transforms aren't part of the `Source` trait. Wrap the source with `TransformingSource::new(source, stages)` — there is no per-connector `add_transform`. |
| `FaucetError::Config: batch_size out of range` | `batch_size` exceeded `MAX_BATCH_SIZE` (1,000,000). Use `validate_batch_size` at load time; `0` is the valid "no batching" sentinel. |
| Bookmark never persists across runs | Your source returns `None` from `state_key()`, or no `with_state_store` was set. Override `state_key()` and read the bookmark via `apply_start_bookmark`. |
| `DeliveryMode::ExactlyOnce` rejected | The source isn't deterministic-replay (`supports_exactly_once()` is `false`) or the sink isn't idempotent (`supports_idempotent_writes()` is `false`). Effectively-once needs a CDC source + an idempotent sink + a state store + no DLQ. |
| Quality config rejected at load time | A `quarantine` / `quarantine_batch` policy was set without a `dlq:` block, or a regex / JSON Schema / bound is invalid — `CompiledQuality::compile` is fail-fast. Add a DLQ or fix the spec. |
| `Custom` errors lose their cause | Wrap with `FaucetError::Custom(Box::new(your_error))` — it implements `From<Box<dyn Error + Send + Sync>>` and preserves the chain. |
| Secret printed in logs | `Credential`'s `Debug` redacts secrets, but connector-specific debug logging is outside that boundary. Never run a secret-bearing pipeline at `FAUCET_LOG=debug`. |
| Buffered sink output lost on cancel | Use `Pipeline::with_cancel` (not a dropped future): cancellation flushes at the next page boundary so Parquet/S3 sinks finalize their output. |

## See also

- **Concepts & library guide:** <https://faucet-hq.github.io/faucet-stream/getting-started/concepts.html> · <https://faucet-hq.github.io/faucet-stream/tutorials/library.html>
- **Transforms cookbook:** <https://faucet-hq.github.io/faucet-stream/cookbook/transforms.html>
- **State & effectively-once:** <https://faucet-hq.github.io/faucet-stream/cookbook/state.html>
- **Upsert / write modes:** <https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html>
- **Quality checks & DLQ:** <https://faucet-hq.github.io/faucet-stream/reference/config.html>
- **Related crates:** [`faucet-auth`](https://crates.io/crates/faucet-auth) (shared auth providers) · [`faucet-state-redis`](https://crates.io/crates/faucet-state-redis) / [`faucet-state-postgres`](https://crates.io/crates/faucet-state-postgres) (state backends) · [`faucet-stream`](https://crates.io/crates/faucet-stream) (umbrella) · [`faucet-cli`](https://crates.io/crates/faucet-cli) (the `faucet` binary)

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
