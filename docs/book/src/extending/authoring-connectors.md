# Authoring a connector

faucet-stream is designed as an ecosystem: third parties can publish their own
`faucet-source-*` / `faucet-sink-*` crates with minimal friction. **`faucet-core`
is the only required dependency** — it re-exports everything a connector author
needs (`async_trait`, `serde_json`, `schemars`).

## Scaffold it in one command

Don't hand-assemble the crate — generate one that already follows every
convention below:

```bash
faucet new connector acme --kind source        # → faucet-source-acme/
faucet new connector acme --kind sink --common  # also emit faucet-common-acme/
```

The generated crate has the standard module layout (`config.rs`, `stream.rs` /
`sink.rs`), a `JsonSchema`-deriving config, the `config_schema()` /
`connector_name()` overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]`
crate-root line, the `[package.metadata.docs.rs]` block, system-name-first
crates.io keywords, a README, a passing unit test, and `tests/conformance.rs`
wired to the [`faucet-conformance`](../reference/conformance.md) battery (a
`faucet-conformance` dev-dependency) — so `cargo test` is green immediately with
a trivial passthrough, and `cargo test --test conformance` runs the SDK-contract
checks. Add checks to that file as the connector grows (bounded memory and
bookmark round-trips for a source; truthful capabilities, write modes and
idempotent replay for a sink). `faucet conformance` scores only connectors
compiled into a faucet binary, so it does not apply to a standalone crate. Replace the `TODO`s with your real
config fields and I/O, then publish. The rest of this page explains what the
scaffold sets up.

To make your published connector usable from a `faucet.yaml` config (not just
from Rust), see
[Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors).

## Start from a family driver

Write the connector's I/O as a **driver** and let the engine drive everything
else ([RFC 0014](https://github.com/faucet-hq/faucet-stream/blob/main/rfcs/0014-phase-3a-family-drivers.md)).
A driver is a small trait of primitives — write a page, publish a message,
send a request, open a transaction — with no lifecycle in it. An adapter turns
it into a `Sink` (or `Source`), derives the connector's capabilities from the
primitives it has, and drives overwrite staging, the exactly-once commit,
cleanup, rollback, per-row outcomes and row accounting. Your public type holds
the adapter and forwards to it, so its users see an ordinary connector.

Pick the family your backend belongs to:

| Your backend | Implement | Wrap it in | From |
|---|---|---|---|
| a SQL database | `SqlDriver` (transactions, DDL, the commit-token read, the staging clone and swap, catalog reads) | `DriverSink<SqlSinkDriver<_>>` | `faucet-common-sql` |
| object or file storage | `StorageBackend` — it is the `ObjectStoreDriver` | `FileSink<_>` (`delegate_sink!`) | `faucet-common-file` |
| a message broker (produce) | `MessageDriver` (`encode`, `publish` a group, `connect`, `flush`; `MessageTransactions` for exactly-once) | `DriverSink<MessageSinkDriver<_>>` | `faucet_core::driver` |
| a message broker (consume) | `MessageConsumer` (`receive`, `ack`, `decode`) | `DriverSource<MessageSourceDriver<_>>` | `faucet_core::driver` |
| an HTTP endpoint | `HttpDriver` (`session` per page, `send` one body, `batching`) | `DriverSink<HttpSinkDriver<_>>` | `faucet_core::driver` |
| anything else | `SinkDriver` (+ the lifecycle groups you support) / `SourceDriver` | `DriverSink<_>` / `DriverSource<_>` | `faucet_core::driver` |

Every async primitive receives the run's `CallSlot`; make each backend request
through `calls.call(..)` / `calls.call_once(..)` so the run's deadline, cancel
token and round-trip metering apply (see [Backend calls](#backend-calls-callcontext-httpclient-batching)).
Use the slot you are handed. The exception is a driver whose own per-request
policy sits over the run's context (the REST source's 429 budget, the MSSQL
sink's per-statement retry classification): it keeps its own `CallSlot` and
installs the run's context and recorder into it through `set_call_context` /
`set_roundtrip_recorder`, so the run's deadline, cancel token and metering
still reach every request.

### A sink driver

The family-neutral contract is one write primitive plus optional groups. A
group you return turns its capability on; one you leave at `None` turns it off,
and the hook refuses — a declaration cannot disagree with the code:

```rust,ignore
use faucet_core::driver::{DriverSink, PageWrite, RowPolicy, SinkDriver, StagingDriver};
use faucet_core::{async_trait, forward_sink, CallSlot, FaucetError, Sink, Value, WriteMode};

struct AcmeDriver { /* client, config */ }

#[async_trait]
impl SinkDriver for AcmeDriver {
    fn connector_name(&self) -> &'static str { "acme" }

    // `RowPolicy::WholePage` (plain and reported writes): a bad row fails the page.
    // `RowPolicy::PerRow` (the dead-letter path): write the good rows, report each bad one.
    async fn write(&self, calls: &CallSlot, rows: &[Value], policy: RowPolicy)
        -> Result<PageWrite, FaucetError>
    {
        let outcomes = calls.call_once("insert", self.insert(rows, policy)).await?;
        Ok(PageWrite::from_outcomes(outcomes))
    }

    fn write_modes(&self) -> &'static [WriteMode] { &[WriteMode::Append, WriteMode::Overwrite] }
    fn staging(&self) -> Option<&dyn StagingDriver> { Some(self) }  // overwrite: on
}

#[async_trait]
impl StagingDriver for AcmeDriver {
    fn active(&self) -> bool { self.config.write.is_overwrite() }
    async fn stage(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("begin_overwrite", self.create_staging()).await }
    async fn publish(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("commit_overwrite", self.swap()).await }
    async fn discard(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("abort_overwrite", self.drop_staging()).await }
    async fn staged(&self, c: &CallSlot) -> Result<bool, FaucetError> { c.call_once("overwrite_staging_exists", self.staging_exists()).await }
}

/// The public connector type: unchanged for its users.
pub struct AcmeSink { inner: DriverSink<AcmeDriver> }

#[async_trait]
impl Sink for AcmeSink { forward_sink!(inner); }
```

The groups are `StagingDriver` (overwrite), `CommitTokenDriver`
(exactly-once: `write_with_token` commits the rows and the token as one unit,
`last_token` reads it back), `CleanupDriver` (scoped cleanup), `SchemaDriver`
(the live schema and, if `evolvable`, additive evolution), `RollbackDriver`
(run rollback) and, with the `arrow` feature, `ColumnarDriver` (write Arrow
batches directly). `DriverSink` serves the plain, reported and per-row write
paths from `write`, checks every result against the rows offered (a write that
loses rows fails with `RowsUnaccounted`), and `DriverSink::check_declarations`
refuses write modes that disagree with the groups. An overwrite whose
destination has no staging object to look for (a table version a target
activates) returns `false` from `StagingDriver::reports_staged`, and the
staging probe answers "unknown"; a destination that cannot vouch for keyed
convergence lowers the derived guarantee with `SinkDriver::sink_guarantee`.

A source is the same shape: `SourceDriver::read` streams `StreamPage`s (each
with the bookmark to resume after it), and `DriverSource` provides the fetch,
stream, resume and probe hooks from it. A source with its own buffered path
returns it from `fetch` (the buffered fetches use it instead of draining
`read`); one with no native paging reads everything and pages it with
`faucet_core::driver::pages_of`. The optional primitives — `read_native`
(raw bytes), `read_columnar` (Arrow), `discover`, `set_run_clock` — reach
the matching `Source` hooks, and `set_call_context` /
`set_roundtrip_recorder` hand the run's context to a driver whose client
keeps its own call slot.

### A SQL sink

Implement `SqlDriver` and the whole sink comes from `SqlSinkDriver`: the
create-on-first-write rule, keyed and chunked writes in transactions, per-row
splits, rows and token in one transaction, the key-table cleanup, the
overwrite sequence (drop stale staging, clone, first-run publish or swap,
discard) and the evolution steps. The backend supplies only what is its own:

```rust,ignore
use faucet_common_sql::{SqlDriver, SqlLayout, SqlSinkDriver, SqlSinkEngine};
use faucet_core::driver::DriverSink;

#[faucet_core::async_trait]
impl SqlDriver for AcmeSqlDriver {
    type Dialect = AcmeDialect;
    type Conn<'a> = AcmeConn<'a>;            // a transaction or a pooled connection, as an SqlExecutor
    fn engine(&self) -> &SqlSinkEngine<AcmeDialect> { &self.engine }
    fn layout(&self) -> &SqlLayout { &self.layout } // table, JSON column, create_table, batch_size
    fn connector_name(&self) -> &'static str { "acme" }
    // begin / commit / connection, table_exists, ddl, read_token,
    // create_staging / publish_staging / swap_staging, current_schema,
    // existing_columns / apply_evolution, and optionally rollback()
    // …
}

pub struct AcmeSqlSink { inner: DriverSink<SqlSinkDriver<AcmeSqlDriver>> }
```

`faucet-sink-sqlite` is the reference implementation.

A backend whose rules differ overrides a defaulted hook instead of the
lifecycle: `insert` (a bulk path such as `COPY`, told whether it serves a
plain page or the exactly-once page), `write_token` and `prepare_keyed_write`
(token-scope rewriting, DDL that must run before a transaction because it
commits implicitly), `drop_staging` / `staging_exists` / `staging_table`
(extra swap leftovers, identifier length limits), `split_page` /
`splits_keyed_pages` (values refused per row on the dead-letter path), `guard`
(a per-operation timeout keyed by the call label), `table_display` and
`capabilities`. `faucet-sink-postgres` and `faucet-sink-mysql` use them. A
backend whose writes do not fit an async statement executor (a blocking
driver, array DML, per-chunk failure classification) implements the core
`SinkDriver` and its groups directly, as duckdb, oracle and mssql do.

### Prove it

Call the two driver checks from `tests/conformance.rs`; CI requires them for a
crate built on a driver:

```rust,ignore
faucet_conformance::assert_driver_declarations_truthful(&sink).await; // undeclared lifecycles refuse
faucet_conformance::assert_driver_row_paths_agree(&sink).await;       // plain, reported, per-row, governed agree
```

### Hooks the drivers replace

These `Sink` methods are deprecated since `faucet-core` 1.14.0 and are removed
in 2.0 (#888): `begin_overwrite`, `commit_overwrite`, `abort_overwrite`,
`overwrite_staging_exists` (→ `StagingDriver`), `write_batch_idempotent`,
`write_batch_idempotent_reported`, `last_committed_token`
(→ `CommitTokenDriver`), `cleanup_scope` (→ `CleanupDriver`). A connector that
still implements them keeps working on 1.x and compiles without a warning —
implementing a deprecated method does not warn, calling one does. RFC 0014 has
a before/after migration.

### Governed pages

The engine writes only pages that went through masking, quality, contract and
schema drift: a `GovernedPage`, which nothing outside
`faucet_core::governance` can construct. A sink receives one through the new
`Sink::write_governed` (its default calls `write_batch_reported`, so you need
not implement it). To write a page outside a pipeline — a test, a preview —
run the passes with `Governance::govern(records, &sink)`.

## The traits underneath

A driver adapter implements `Source` or `Sink` for you; a connector that
does not fit a family can implement them directly. Both are object-safe (`Box<dyn Source>` works) and
all newer methods have defaults, so a minimal connector is small.

```rust,ignore
use std::collections::HashMap;

use faucet_core::{async_trait, Source, Sink, FaucetError, Value};

struct MySource { /* reusable client/pool created in new() */ }

#[async_trait]
impl Source for MySource {
    // Primary entry point. (`fetch_all()` is a provided convenience.)
    // `context` carries parent-record values for `${parent.path}` placeholders.
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        todo!("fetch records from your system")
    }
}

struct MySink { /* reusable client/pool */ }

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        todo!("write records to your system")
    }
}
```

Your connector now works with the `Pipeline` and every other connector:
`Pipeline::new(&MySource { .. }, &MySink { .. }).run().await?`.

The trait signatures, every defaulted method and every re-export are on
[docs.rs/faucet-core](https://docs.rs/faucet-core) — open the version your
crate's `Cargo.lock` resolves (`cargo tree -p faucet-core --depth 0`). The
sections below are the contract those methods carry.

### Object safety

The pipeline and the CLI hold connectors as `Box<dyn Source>` /
`Box<dyn Sink>`. Implement only the trait's own methods with their exact
signatures: no generic trait methods, associated types or driver types in a
signature. Generics belong on private helpers; pools and SDK clients live
inside your struct. Keep a constructor that returns `Result<Self, FaucetError>`
and does **no network I/O** — a custom binary registers connectors through a
synchronous factory, and `faucet validate` builds them without contacting
anything. Connect lazily and probe connectivity in `check()`.

Always override `config_schema`, `connector_name` (a short, stable, non-empty
name — it is the `connector` metric label) and `dataset_uri` (the lineage
identity; credential-free — `faucet_core::redact_uri_credentials` strips
userinfo from a URL).

### Wrapping another connector

A **wrapper** holds an inner source or sink and changes part of its behaviour:
it stamps a column, caps the row count, samples records, overrides the state
key. Every hook the wrapper does not define falls back to its trait default,
which turns the feature behind it off for everything the wrapper holds:
exactly-once, upsert, the columnar fast path, multi-table routing. Don't write
the pass-throughs by hand. `forward_source!` / `forward_sink!` generate the
forwarding body of every hook, so you write only the hooks you change, and a
hook added to the trait in a later release reaches the inner connector without
any change to your wrapper:

```rust,ignore
use faucet_core::{async_trait, forward_sink, FaucetError, Sink, Value};

struct Counted {
    inner: Box<dyn Sink>,
    rows: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Sink for Counted {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.rows.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }
    // The other write paths must be counted too, so they take the trait
    // default (which calls `write_batch` above) instead of being forwarded.
    forward_sink!(inner; except write_batch, write_batch_partial,
        write_batch_idempotent, supports_idempotent_writes,
        supports_columnar, write_batch_columnar,
        native_load_capabilities, load_native);
}
```

The list after `except` is exact. A name that is not a hook of the trait does
not compile, and a hook you define without listing it is a duplicate
definition. A listed hook you don't define takes the trait default, which is
how you switch a path off.

Forwarding is verbatim. That includes the convenience hooks whose default calls
another hook (`fetch_all`, `stream_pages`, `check`, `write_batch_partial`,
`write_batch_idempotent`). If your wrapper changes a hook, list the other hooks
that must go through the change rather than straight to the inner connector.
One exception is automatic: a wrapper that lists `write_batch` does not forward
`write_batch_reported` either, so the pipeline's reported write goes through
your `write_batch` (#879). Define `write_batch_reported` yourself to pass the
inner sink's coalescing / skip report through. The same holds for the
exactly-once write: a wrapper that lists `write_batch_idempotent` does not
forward `write_batch_idempotent_reported`; define it to pass the inner report
through.

Then prove it with the forwarding harness from `faucet-conformance`.
`HookSource` / `HookSink` answer every hook differently from the default and
record which hooks were reached. The assertion calls every hook through your
wrapper and names any that never got to the inner connector, except the ones
you list as deliberately not forwarded:

```rust,ignore
use faucet_conformance::forwarding::{HookSink, assert_sink_forwards_every_hook};

#[tokio::test]
async fn counted_forwards_every_hook_it_does_not_count() {
    let probe = HookSink::default();
    let wrapped = Counted { inner: Box::new(probe.clone()), rows: Default::default() };
    assert_sink_forwards_every_hook(
        &wrapped,
        &probe,
        &["supports_idempotent_writes", "supports_columnar", "write_batch_columnar",
          "native_load_capabilities", "load_native"],
    )
    .await;
}
```

## Crate layout

Follow the same module layout as the built-in connectors:

- **`lib.rs`** — re-export the config + the `Source`/`Sink` type. First line:
  `#![cfg_attr(docsrs, feature(doc_cfg))]` (see below).
- **`config.rs`** — the config struct + sub-enums, deriving
  `Serialize + Deserialize + JsonSchema`. **No I/O here.**
- **`stream.rs`** (source) / **`sink.rs`** (sink) — the one place that performs
  I/O. Create reusable clients/pools in `new()` and store them; never reconnect
  per call.

## Streaming pages and bookmarks

`Pipeline::run` drives `Source::stream_pages` and writes each `StreamPage`
(`records` + optional `bookmark`) to the sink as it arrives, one page at a time,
in order. The default `stream_pages` loads the whole result and chunks it —
correct but unbounded — so a source with a paging primitive (cursor, keyset,
scroll, offset, a consumer) overrides it and yields as it reads, keeping memory
at O(page size). Then have `fetch_with_context` drain that stream, so every
entry point returns the same records.

- The `batch_size` argument is a hint; a page-size field in your config wins
  when set. `batch_size == 0` means "one page with everything" — handle it
  explicitly. Clamp to the backend's request limit; pages smaller than the hint
  are fine, larger are not.
- Decide end-of-data from the backend's own signal (short page, null
  next-cursor), and let an empty first page end cleanly.
- Never emit page N+1 before page N; bookmarks are persisted in emit order.
- Don't hold a `std::sync::MutexGuard` across an `.await` inside the stream (it
  makes the stream `!Send`).
- A record missing its cursor field is an error, never a silent skip — skipping
  would move the bookmark past data.

**Resumable sources** return a valid key from `state_key()` (the CLI may
substitute its own `{pipeline}::{row}` key, so never read it back), receive the
last persisted bookmark in `apply_start_bookmark` before streaming starts, and
attach bookmarks to pages: the final page only for a query whose high-water mark
is known at the end, or every page when replay from a bookmark is deterministic
(keyset on a unique increasing key, CDC) — each page then becomes a checkpoint.
Keep the bookmark a small JSON object, never a record. A stored bookmark of the
wrong shape is `FaucetError::State`, never a silent restart from zero. When the
shape changes in a later release, bump `state_schema()` and teach
`migrate_state()` the step.

## Write, flush, checkpoint

For every page the pipeline writes (`write_batch`, `write_batch_partial` or
`write_batch_idempotent`), then calls `flush()`, then persists the page's
bookmark. The bookmark never moves ahead of durable data, which asks of you:

- **Sources** never persist, ack, commit offsets or delete source data
  themselves. Emit the position on a page and let the pipeline persist it after
  the flush. A backend that needs an ack acks when the next page is polled (the
  pipeline polls only after the previous page is durable) and reports
  `consumes_destructively() -> true`, so previews and dry runs refuse it.
- **Sinks** make everything passed to `write_batch` so far durable and readable
  when `flush()` returns `Ok`. A write-through sink keeps the no-op default; a
  buffering sink (file writer, multipart upload, client-side batch) commits in
  `flush`. The pipeline also flushes on cancellation, so `flush` must be safe to
  call at any time, including with nothing buffered.
- The crash window between flush and checkpoint is deliberate: the page is
  re-read on the next run. Default delivery is at-least-once, so every sink must
  tolerate seeing a page twice. An empty slice is a cheap `Ok(0)`.

**Row accounting.** The pipeline checks that every row it offers a sink is
accounted for (#879): it calls `write_batch_reported` and fails the page with
`RowsUnaccounted` when the report covers fewer rows than were offered — so
`write_batch` must return the number of rows it wrote, not the number of
requests or a count the backend may lower. The default report wraps that
count, and for a sink configured to merge rows by key (`dedups_by_key()`)
credits the shortfall as coalesced. Override `write_batch_reported` when the
sink declines rows **by design of its configuration** (a key policy that
skips records, a preview cap) and report them as skipped:

```rust,ignore
async fn write_batch_reported(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
    let (written, skipped) = self.produce(records).await?;
    Ok(WriteReport::accepted(written).with_skipped(skipped))
}
```

A row the sink could not write is never "skipped": return it as a per-row
`Err` from `write_batch_partial` (it goes to the DLQ) or fail the batch.

The exactly-once path is held to the same rule. It calls
`write_batch_idempotent_reported`, whose default wraps
`write_batch_idempotent`'s count the same way; a page whose report is short
fails with `RowsUnaccounted` after the sink committed its token but before the
bookmark is persisted, so the run is red and the next run resumes from the
sink's watermark. An idempotent sink that declines rows by design inside the
atomic commit overrides `write_batch_idempotent_reported` and reports them as
skipped (the kafka sink does this for `on_key_error: skip`).

**Per-row outcomes.** Override `write_batch_partial` when the backend reports
per-row results or a row is invalid before sending (a missing key): one outcome
per input row, in order; an outer `Err` means the whole call failed. Declare
`batch_atomicity()` truthfully — `Atomic` only if a failed write commits
nothing — and if `write_batch` splits a page into several requests to respect
a backend limit, it is partially committed on failure, so it stays best-effort.

### Effectively-once

Two mechanisms remove duplicates across retries and resumes
([Exactly-once delivery](../cookbook/state.md#effectively-once-delivery) is the user's view):

| Mechanism | Sink | Source |
|---|---|---|
| Keyed upsert | flatten `faucet_core::WriteSpec` into the config, list `Upsert`/`Delete` in `supported_write_modes()`, return `self.config.write.dedups_by_key()` from `dedups_by_key()`, route rows through `faucet_core::plan_writes`, and genuinely converge on the key | nothing |
| Atomic watermark | `supports_idempotent_writes() -> true`; `write_batch_idempotent` commits the rows **and** the token under `scope` in one atomic unit (one transaction, one object commit, one producer transaction); `last_committed_token` reads it back | deterministic replay (`supports_exactly_once() -> true`) |

Store the token verbatim and never parse it — it may carry a bookmark only the
pipeline decodes. Two separate writes do not qualify. If you can't do all of
this, leave the capability `false`: an honest `false` makes the CLI refuse
`delivery: exactly_once` at load time, a false `true` corrupts data. The same
goes for every `supports_*` probe and for `supported_write_modes()` — the CLI
gates configs on them. A sink that lists `Upsert` but can't apply a
`delete_marker` rejects it as a `Config` error rather than ignoring it.

### Declaring capabilities

The CLI checks a config before it builds anything, so it cannot ask a built
connector what it supports. Each connector therefore ships a static
`faucet_core::ConnectorDescriptor` next to its factory: its `type:` name and a
pure `fn(&Value) -> Capabilities` that answers from the config alone (RFC 0013).
Every load-time gate — write mode, exactly-once, cleanup, rollback, schema drift,
`faucet verify` range mode, `faucet discover` — reads it, and `faucet list --json`
prints it.

```rust
use faucet_core::capabilities::config;
use faucet_core::{Capabilities, ConnectorDescriptor, Value, WriteMode};

pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("acme", capabilities);

fn capabilities(cfg: &Value) -> Capabilities {
    let modes = [WriteMode::Append, WriteMode::Upsert, WriteMode::Delete];
    Capabilities::builder()
        .write_modes(modes)
        .keyed_upsert(true)
        // A config fact: true only when *this* config upserts by a key.
        .keyed_dedup(!cfg.is_null() && config::dedups_by_key(cfg))
        .build()
}
```

- Called with a config object, the function must return exactly what
  `Sink::capabilities()` / `Source::capabilities()` of a connector built from
  that config returns. The trait method is defaulted and derives every field
  from the per-capability methods above, so most connectors only write the
  descriptor. Override `capabilities()` for what no method expresses —
  `range_read` / `range_digest` / `reports_lag` / `table_routing` (your
  source implements `record_table`) / `read_side_effects` (reading creates a
  slot, joins a consumer group or opens a listener) / `complete_reads` (a run
  can read the whole dataset and report it through `read_completeness`, after
  which destination rows it did not write are deleted) on a source,
  `scoped_overwrite` / `schema_report` on a sink — starting from
  `Capabilities::of_source(self)` / `of_sink(self)`.
- Called with `Value::Null`, it returns the kind-level view: every capability
  some valid config enables (`keyed_dedup` stays `false`).
- Read fields leniently (`faucet_core::capabilities::config`): a config may still
  hold `${…}` tokens, and the function must never fail.
- A sink that silently drops record fields its destination lacks declares
  `schema_report(SchemaReport::DropsUnknownFields)`: a drift policy cannot see
  those fields, so `schema.on_drift: fail` is refused at load time
  ([Feature compatibility](../reference/compatibility.md)).
- In `tests/conformance.rs`, build the connector from a few configs and call
  `faucet_conformance::assert_descriptor_matches_source` / `_sink` with each.
- A custom binary registers it with `register_source_described` /
  `register_sink_described`; a connector registered without one is treated as
  append-only with no capabilities.

### Multi-table mirror hooks

A CDC source joins a multi-table `faucet mirror` (`mirror.tables`) through four
defaulted `Source` methods. The mirror resumes one shared stream from the
earliest table's position, so a replayed page can reach tables that already
applied part of it:

| Method | Contract |
|---|---|
| `record_table(&record)` | The table a change belongs to, spelled exactly as the paired bulk source's `discover()` names it; `None` for DDL / control events. Required — without it the source cannot be mirrored per table. |
| `position_le(a, b)` | `Some(true)` when every change up to bookmark `a` is covered by `b`; `None` when the two cannot be related. |
| `position_min(positions)` | The earliest position every table can resume from. Override it when positions are only partially ordered (one cursor per capture instance). |
| `record_covered(&record, &position)` | Whether a run resumed from `position` would not deliver `record` again. **Implement it**: pages are cut differently on replay, so a page can straddle a table's committed position, and only this per-record test drops what that table already applied. Returning `None` (the default) re-delivers those changes — duplicates on an append destination; the mirror logs a warning when it happens. |

## Backend calls: `CallContext`, `http::Client`, batching

Every call a connector makes to its backend goes through
[`faucet_core::io::CallContext`](https://docs.rs/faucet-core/latest/faucet_core/io/struct.CallContext.html)
(RFC 0013, #880). One `ctx.call(op, || fut)` applies the per-call timeout, the
run's deadline, classified retry (a `FaucetError::RateLimited(wait)` waits the
time it states), prompt cancellation, round-trip metering and redaction of the
error it returns. Don't write a retry loop, a timeout or a backoff sleep.

Keep one in a `CallSlot`, built in `new()` from what your config documents,
and let the pipeline install the run's part:

```rust
use faucet_core::io::{CallContext, CallSlot};

pub struct AcmeSource {
    client: faucet_core::http::Client,
    calls: CallSlot,
}

impl AcmeSource {
    pub fn new(config: AcmeConfig) -> Result<Self, FaucetError> {
        let client = faucet_core::http::Client::builder()
            .timeout(config.timeout)
            .sensitive_header("x-api-key")
            .build()?;
        let calls = CallSlot::new(
            CallContext::new()
                .with_timeout(config.timeout)
                .with_retry(faucet_core::RetryPolicy {
                    max_attempts: config.max_retries + 1,
                    ..Default::default()
                }),
        );
        Ok(Self { client, calls })
    }

    async fn page(&self, url: &str) -> Result<Value, FaucetError> {
        let resp = self
            .client
            .execute(&self.calls.get(), "page", || self.client.get(url))
            .await?;
        self.client.json(resp).await
    }
}

#[async_trait]
impl Source for AcmeSource {
    fn set_call_context(&self, ctx: CallContext) {
        self.calls.install(ctx);
    }
    fn set_roundtrip_recorder(&self, r: Arc<RoundtripRecorder>) {
        self.calls.install_recorder(r);
    }
    // …
}
```

What you get, and who sets it:

| Setting | Set by | Default |
|---|---|---|
| attempt timeout (`with_timeout` / `without_timeout`) | the connector, from its config; else the caller | `DEFAULT_CALL_TIMEOUT` (300 s) |
| retry policy (`with_retry`) | the connector | one attempt |
| rate-limit budget (`with_rate_limit_retries`) | the connector | a rate limit is an ordinary attempt |
| deadline | the caller (`Pipeline::with_call_context`) | none |
| cancel token | the pipeline, **sources only** | none |
| round-trip recorder | the pipeline | none |
| redactor | the caller | the process-wide `faucet_core::redact` hook |

`ctx.call` counts every attempt as a round trip labelled `op` (a closed,
connector-defined set — `query`, `page`, `put`). Use `ctx.wait(d)` for a pause
that is not a retry (a poll interval, a pacing delay): it ends early on cancel
and at the deadline.

`ctx.call_once(op, fut)` runs a call that cannot be rebuilt — a statement on a
borrowed transaction or connection, a step of a cursor or row stream, a
non-idempotent write — in exactly one attempt, with the same timeout, deadline,
cancel, metering and redaction. When the backend's client is not cancel-safe
(a TDS connection dropped mid-response, a query running on a blocking thread),
check whether the call was cut short (`FaucetError::Timeout` / `Cancelled`) and
repair what it left behind: replace the connection, interrupt the query.

A sink's context never carries the run's cancel token — a cancelled run still
flushes its sinks — and neither side gets a retry policy from the pipeline.

When a call cannot be rebuilt for a retry (a write, a request that consumes a
body, an SDK future), use the one-shot forms. They apply the timeout, the
deadline and the cancel token and count the round trip, but never retry:

- `slot.call_once("put", fut)` / `ctx.call_once("put", fut)` for a future
  returning `Result<T, FaucetError>`;
- `fut.within(&slot, "put")` / `fut.within_ctx(&ctx, "put")`
  (`faucet_core::CallExt`) for any other future — an SDK call whose error you
  map afterwards;
- `ctx.reader(inner)` wraps an `AsyncRead` (a streamed object body, a child
  process pipe) so a read that stalls ends at the deadline (`TimedOut`) or on
  cancel (`Interrupted`) instead of hanging.

### Retries and writes

- **Reads and other idempotent calls** retry through your context's policy.
  Expose the retry count in config.
- **Sink writes** are retried by the pipeline (under a `resilience:` policy),
  and a plain `write_batch` only when `write_batch_is_replay_safe()` is true —
  by default that follows `dedups_by_key()`. `supports_idempotent_writes()` does
  not make a plain `write_batch` retryable. Override `write_batch_is_replay_safe`
  only when a replayed write converges by construction (every write a keyed
  `PUT`).
- Never retry a non-idempotent write inside your own `write_batch`: if the
  server committed and the response was lost, the retry duplicates every row.
  Give such a call `without_retry()` and let the error propagate; the page is
  replayed from the last checkpoint ([ADR 0007](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0007-retries.md)).

### HTTP connectors

Build clients with `faucet_core::http::Client`, never `reqwest::Client`
directly. It follows redirects itself so a hop to another origin drops
`Authorization`, cookies, every header marked sensitive and every header named
with `sensitive_header`, and never replays a request body to another origin. It
refuses a buffered body above `max_response_bytes` (1 GiB by default), turns a
non-2xx response into `FaucetError::HttpStatus` (a `429`/`503` with
`Retry-After` into `RateLimited`), and strips credentials from the URLs errors
carry. `client.execute(&ctx, op, || request)` runs a request under a context;
`client.send(request)` sends one attempt when you classify the response
yourself. reqwest options it does not name (compression, mutual TLS) go
through `ClientBuilder::configure`.

### Batching

`faucet_core::batching::chunks(records, batch_size)` re-chunks a page under the
`batch_size` contract (`0` = one request for the whole page);
`map_concurrent` / `for_each_concurrent` run work with at most `n` in flight
and stop at the first error, and `ConcurrencyLimit` bounds work spread over
separate calls.

### The lint

The workspace `clippy.toml` disallows `reqwest::Client::new`,
`reqwest::Client::builder`, `reqwest::ClientBuilder::new`,
`tokio::time::sleep`, `tokio::time::sleep_until` and `std::thread::sleep`, and
CI runs clippy with `-D warnings`. A site that genuinely needs one — a
test driving time, a server-side timer that is not a backend wait — carries
`#[allow(clippy::disallowed_methods)] // <reason>`. A third-party connector in
its own repository can copy the same `clippy.toml`.

Prove it: call
`faucet_conformance::calls::assert_source_call_deadline` (or
`assert_sink_call_deadline`) from your `tests/conformance.rs` against a backend
that never answers (a wiremock server with a long `set_delay`); it checks the
call ends at the deadline and on cancel. For a non-HTTP protocol,
`faucet_conformance::hung::SilentListener` accepts connections and never
writes a byte, and `HangingProxy` forwards to a real backend (an emulator)
until you call `hang()`, so a client that connects in `new()` can be built
first and then stalled. A connector with no backend round trip (a local file)
is listed with its reason in `crates/conformance/call-deadline-exempt.txt`.

## SQL sinks: the shared dialect and engine

A sink that writes rows into a SQL database does not plan its own SQL. The
`faucet-common-sql` crate holds the planning every SQL sink used to copy, and
the [`SqlDriver`](#a-sql-sink) family driver that runs it:

- **`SqlDialect`** answers "how is this spelled here": identifier quoting
  (`check_ident` refuses what cannot be quoted), literal escaping,
  placeholders (`$3::int4`, `?`, `@P3`, `:3`) and the parameter limit, created,
  key and widened type names, the keyed upsert (`ON CONFLICT`,
  `ON DUPLICATE KEY`, `MERGE`) and delete statements, the scoped-cleanup
  statements, the commit-token watermark table and the rollback-journal SQL.
  Every statement builder is a provided method, so a dialect overrides only
  where its SQL differs. Built-in dialects live in
  `faucet_common_sql::dialects`.
- **`SqlSinkEngine<D>`** owns the decisions: which record field lands in which
  column (`ColumnMatching`: exact, case-insensitive, fold-to-unique, upper-fold),
  a record matching no column (an error, never a silent skip), the union column
  set of an append (a missing column binds NULL), one upsert per set of carried
  columns (an absent column keeps its stored value), page-ordered keyed groups
  (`plan_writes_ordered`, the journal written first on the same connection),
  statements chunked under the parameter limit, create-table and evolution DDL,
  the cleanup sequence, the commit-token write and the row report
  (`WriteReport`: a keyed page accepts one row per planned upsert or delete and
  coalesces the rest).

A backend crate supplies a dialect and a `SqlExecutor` over its driver
connection or open transaction: it returns the target's live columns (with
types when its placeholders cast) and binds each planned `Param` as its column
needs. It keeps what is genuinely its own — transactions, bulk-load paths,
overwrite swaps, catalog queries — and runs the engine inside its own backend
call, so the deadline and metering of [`CallContext`](#backend-calls-callcontext-httpclient-batching)
still cover every statement:

```rust
use faucet_common_sql::dialects::Postgres;
use faucet_common_sql::{SqlExecutor, SqlSinkEngine, Statement, TableColumn, TableTarget};
use faucet_core::{async_trait, FaucetError};

struct Exec<'c> {
    conn: &'c mut sqlx::PgConnection,
}

#[async_trait]
impl SqlExecutor for Exec<'_> {
    async fn columns(&mut self, t: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        // read the catalog for `t.reference` on `self.conn`
        # Ok(vec![])
    }
    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        // bind each `stmt.params` value as its column needs, run `stmt.sql`
        # Ok(0)
    }
}

// In the sink: plan the page, then apply it inside one transaction.
// let plans = engine.plan_keyed(records)?;
// engine.apply_plans(&mut Exec { conn: &mut tx }, &target, &plans).await?;
```

The shared planning matrix (`crates/common/sql/tests/dialect_matrix.rs`) runs
the same cases — mapping, dedup, upsert/delete ordering, chunking, widening,
create-table, cleanup, journal and token SQL — against every dialect, so a rule
fixed for one backend is checked for all of them. A new SQL sink adds its
dialect there.

Which built-in sinks use how much of it:

| Sink | Uses |
|---|---|
| sqlite | the SQL family driver: `SqlDriver` + `SqlSinkDriver` drive every write and lifecycle (RFC 0014) |
| postgres, mysql | the SQL family driver, with backend hooks: `insert` (postgres `COPY` / JSONB `unnest`), `guard` (`write_timeout_secs`), `staging_table` (postgres's 63-byte names), `drop_staging` / `staging_exists` (mysql's `__faucet_ovw_old`), `write_token` / `prepare_keyed_write` (mysql's fitted scope key, journal before the transaction), `split_page` (binary-value refusals per row), `capabilities` (postgres scoped overwrite) |
| mssql | the dialect (MERGE, `CAST` placeholders, limits) and the shared mapping; its own retry-classified chunk execution, on the core `SinkDriver` with staging, commit-token, cleanup and schema groups |
| duckdb | the engine's row planning, executed on the blocking driver; on the core `SinkDriver` |
| oracle | the dialect; blocking array DML on the core `SinkDriver` with staging, commit-token and schema groups |
| redshift, clickhouse, snowflake, databricks, bigquery | the dialect for quoting, escaping, types and DDL; the family-neutral `SinkDriver` and its lifecycle groups (staging, commit tokens, cleanup, schema) over their own row handling (`COPY`/`JSONEachRow` bodies, JSON-payload statement APIs), because their commit unit — a cross-page commit group, a statement-API request, a load job — is not the per-page client transaction `SqlSinkDriver` opens and commits (RFC 0014) |

## Make it fast

Performance is the project's first principle, and none of this may trade away
the ordering rules above:

- Build HTTP clients (`faucet_core::http::Client`), pools and producers once
  in `new()` and store them; never per call, page or record. Bound database pools with a `max_connections`
  field.
- Use bulk APIs: multi-row `INSERT … VALUES (…), (…)` or `COPY` per batch inside
  a transaction, a bulk HTTP endpoint per chunk, pipelining for key-value
  stores. One statement per record is a defect. Make the per-request limit a
  config field with a safe default.
- Bound parallel I/O (`faucet_core::batching::map_concurrent`, or
  `ConcurrencyLimit` across calls) and expose `concurrency`; concurrency must
  never reorder emitted pages.
- Put blocking or CPU-heavy work (synchronous drivers, encoding, compression)
  on `tokio::task::spawn_blocking`; buffer file and socket writers.
- Don't log or meter per record. The pipeline already records metrics and spans
  for every call; never use record ids, URLs or cursors as labels.

## Values: the fidelity model

Render every typed value through `faucet_core::fidelity`, not by hand, so a
value reads the same whichever connector produced it
([value fidelity](../reference/value-fidelity.md)):

- an exact decimal is a string at its declared scale (`decimal_text`,
  `decimal_from_i128`) — never `to_f64()` or `parse::<f64>()`;
- an integer a `Value` cannot hold is a string (`integer_i128`,
  `integer_text`);
- a float goes through `float64` / `float32`, which turn `NaN` / `±Infinity`
  into a `null` the run counts;
- bytes are base64 (`binary`), instants RFC 3339 with an offset (`timestamp`);
- a JSON body or payload parses with `fidelity::from_slice`, a JSON column
  with `fidelity::parse_json`;
- a sink binds a non-integer number into a SQL column as its shortest text
  (`non_integer_text`), so the server parses the decimal exactly.

Then run the shared corpus. A source/sink pair does it in
`crates/interop-tests/tests/`; a connector alone can do it against its
backend in its own `tests/`, or through its value converter in a `#[cfg(test)]`
module:

```rust,ignore
use faucet_conformance::fidelity::{self, Tolerance};

// A sink: write the corpus, read the destination back.
fidelity::assert_corpus_written(&sink, &fidelity::corpus(), || read_back(), Tolerance::exact()).await;
// A source over a backend seeded with the corpus.
fidelity::assert_corpus_read(&source, &fidelity::corpus(), Tolerance::exact()).await;
```

`scripts/fidelity-coverage.py` fails CI for a built-in connector that runs
none of these, and `scripts/decimal-f64.py` for a decimal converted through a
float.

## Config and schema

Implement `config_schema()` so `faucet schema` and `faucet init` work:

```rust,ignore
fn config_schema(&self) -> Value {
    faucet_core::schema_for!(MyConfig).into()
}
```

- Derive `Serialize + Deserialize + JsonSchema` on the config and every nested
  type; doc comments become the schema's descriptions, so write them for users.
- Give optional fields `#[serde(default)]` with safe, bounded defaults, and add
  `#[schemars(with = "String")]` (or the matching type) to custom-serde fields.
- `#[serde(deny_unknown_fields)]` catches typos at `faucet validate`; serde
  can't combine it with `#[serde(flatten)]`, so drop it on a sink that flattens
  `WriteSpec`.
- Auth uses the adjacently tagged shape every built-in uses
  (`auth: { type: bearer, config: { … } }`).
- Validate in the constructor (empty names, zero sizes, bad URLs, unsupported
  modes, `WriteSpec::validate`) and return `FaucetError::Config`, so
  `faucet validate` fails before data moves.
- Depend on `faucet-core` at the major only (`faucet-core = "1"`); a minor floor
  forces a release of your crate whenever core releases. Use its re-exports
  (`async_trait`, `serde_json`, `schemars`, `async_stream`, `Stream`) rather
  than adding them yourself; `serde` and `schemars` are direct dependencies only
  because their derive macros need them in scope.

**Secrets.** Never hardcode a credential, host or URL. Users write `${env:…}`
or a secrets-manager reference and the CLI resolves it before your constructor
runs. Don't derive `Debug` on a struct holding a secret (print `<redacted>`),
and never log a config, auth header or connection string.

## Errors

Map every failure to a `FaucetError` variant. Third-party error types wrap into
`FaucetError::Custom(Box<dyn Error + Send + Sync>)` without losing the chain.
Never `.unwrap()` on anything that can fail at runtime.

The variant decides whether the pipeline retries: `is_retriable()` is true for
transport errors, 5xx and 429 statuses, and rate limits, and false for
everything else. So type a transient failure as one of those (a 503 wrapped in
a plain sink-error string is never retried) and a permanent one as a config,
auth, JSON, state or source/sink error. `FaucetError::sink_status` picks the
right one from an HTTP status. Truncate server error bodies — they can echo
request data — and keep secrets out of every message. `FaucetError` is
`#[non_exhaustive]`; match it with a `_` arm.

## Tests

Unit tests live in a `#[cfg(test)]` module at the bottom of each file and cover
the pure logic: config validation branches, request building, cursor and
bookmark handling (including a missing cursor and a malformed bookmark), error
mapping, capability probes per config, and a `Debug` that hides secrets.
Integration tests in `tests/` use `wiremock` for HTTP backends (a `Respond` impl
can model pagination or a keyed store) and `testcontainers` for databases and
queues (start them with `faucet_conformance::containers` from the `containers`
feature, which retries a flaky start and waits for readiness): a multi-page read, a resume, a retried 5xx, a non-retried 4xx, a
malformed response, per-row sink failures and every write mode you advertise.
Assert exact outcomes — records, bookmark values, error variants.

## Versioning

Start at `1.0.0`. Adding an optional config field, a defaulted method or a
variant of a `#[non_exhaustive]` enum is a minor release; construct config
types through serde, `new()`, `Default` or builders so struct-literal
construction is not part of your API. Renaming or removing a field, changing a
default's meaning, or changing the bookmark shape without
`state_schema`/`migrate_state` is breaking — keep compatibility
(`#[serde(alias = "…")]`, a bookmark migration) or release a new major. The
scaffold's `[package.metadata.cargo-semver-checks.lints]` block encodes this.

## Self-certify with the conformance battery

A connector becomes **Tier-1 / conformant** by adding a `tests/conformance.rs`
that invokes the reusable `faucet-conformance` battery against the *real*
connector and passing it in CI. That battery **is** the tiering mechanism —
there is no separate scheme. Anything not yet wired into it is Tier-2 (still
useful, usually with its own integration tests — Tier-2 does not mean low
quality).

Add the battery as a dev-dependency. A third-party crate takes it from
crates.io at the same major as `faucet-core`; a connector inside this workspace
uses the workspace entry instead (`faucet-conformance.workspace = true`):

```toml
[dev-dependencies]
faucet-conformance = "1"
```

For a **source**, drive the checks against a live connector:

```rust,ignore
// crates/source/foo/tests/conformance.rs
use faucet_source_foo::{FooSource, FooSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let source = FooSource::new(FooSourceConfig::new(/* … */));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    // drive a source that yields `total` records in pages of `batch`
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // a source configured to fail must return Err, not panic
    faucet_conformance::assert_errors_not_panics(&broken_source).await;
}
```

Resumable sources also add `assert_bookmark_roundtrip` (persist a bookmark,
re-run, confirm the stream resumes at exactly that position). For a **sink**,
use `assert_idempotent_replay` and `assert_capabilities_truthful` — both take a
`distinct_count` closure that returns the destination's current row count (for a
real sink, a `SELECT count(*)` against the target table).

A **discoverable source** (one that overrides `supports_discover`) adds
`assert_discover_roundtrips`, an *integration-level* check that proves every
dataset `discover()` reports is genuinely selectable — it deep-merges each
descriptor's `config_patch` onto the base config (the `merge_config_patch`
helper does this), rebuilds the source, and reads it. Run it against the same
live/seeded backend your other checks use:

```rust,ignore
faucet_conformance::assert_discover_roundtrips(&source, |patch| {
    let base = /* the connection config as a serde_json::Value */;
    let merged = faucet_conformance::merge_config_patch(base, &patch);
    let cfg = serde_json::from_value(merged).unwrap();
    async move { Box::new(FooSource::new(cfg).await.unwrap()) as Box<dyn faucet_core::Source> }
})
.await;
```

`assert_cancellation_flushes` covers the flush-completing cancellation contract
(a mid-run `CancellationToken` stops at a page boundary and still flushes) by
driving the real pipeline — most useful for a buffered sink (Parquet footer, S3
multipart) whose output only commits on `flush()`.

**Assert the honest branch.** Where a connector legitimately can't satisfy a
check — an append-only sink has no idempotency mechanism, for instance — don't
skip it: assert the honest behaviour instead. The capability method returns
`false` and the pipeline refuses `delivery: exactly_once`. A passing conformance
run that documents what a connector *cannot* do is exactly the point.

Which checks apply (the [crate docs](https://docs.rs/faucet-conformance) list
each one's exact signature for your version):

| Applies to | Checks |
|---|---|
| every source | config schema valid, connector name non-empty, errors not panics (pass one configured to fail), well-formed `check()` probe |
| a pageable source | bounded memory (`total > batch`), `batch_size = 0` gives a single page |
| a resumable source | bookmark round-trip |
| a discoverable source | discover round-trips (integration-level) |
| every sink | config schema valid, connector name non-empty, capabilities truthful, well-formed `check()` probe |
| a keyed or idempotent sink | idempotent replay, write modes truthful |
| a sink with schema evolution | schema evolution effective |
| a sink declaring atomic batches | batch atomicity truthful (pass a page holding a row the sink rejects) |
| a sink overriding `write_batch_partial` | partial outcomes truthful |
| a sink overriding `write_batch_reported` | reported rows truthful |
| a keyed (replay-safe) sink | replay safe |
| a sink with `write_mode: overwrite` | overwrite lifecycle (seed the destination first) |
| a buffering sink | cancellation flushes (integration-level) |
| a connector with backend calls | call deadline (see [Backend calls](#backend-calls-callcontext-httpclient-batching)) |

The sink checks write rows keyed on `"id"` with a `"v"` column, so configure
the sink under test `write_mode: upsert`, `key: ["id"]` for the keyed checks.
They measure row-count deltas through your `distinct_count` closure, so give
each check its own fresh destination (a new mock server, table or container).
`faucet_conformance::doubles` has a counting source and a test sink for testing
your own wrappers.

**A declared capability needs its check.** `faucet_conformance::required::REQUIREMENTS`
is the table behind the rows above: each capability, how a connector declares
it (a descriptor flag, a write mode, a trait-method override, a buffering
`flush`, a declared `BatchAtomicity`) and the assertions that prove it. For the
built-in connectors CI enforces it: `scripts/conformance-coverage.py` (the
`Doc counts` job) reads the committed snapshot
`crates/conformance/declared-capabilities.json` plus each crate's `src/` and
`tests/conformance.rs`, and fails when a declared capability has no matching
assertion call. A check that genuinely cannot run in CI (no emulator for the
backend) goes in `crates/conformance/conformance-coverage-exempt.txt` with the
reason; an exemption that no longer applies fails the run too. When a
connector's declared capabilities change, regenerate the snapshot with
`cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate`.

A check that needs a second connector — a sink counted back through its own
source — lives in `crates/interop-tests/tests/<kind>_conformance.rs` (or
`<kind>_<role>_conformance.rs`), which the script reads as well. A cloud client
whose futures overflow the default test stack in a debug build runs its test
body through `faucet_conformance::block_on_large_stack`.

The full contract is the
[Faucet Connector Protocol (FCP v0)](../spec/faucet-connector-spec-v0.md).

## docs.rs setup

So docs.rs renders your full API with per-feature badges, add to `Cargo.toml`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

and make the first line of `lib.rs` `#![cfg_attr(docsrs, feature(doc_cfg))]`.

## Naming & publishing

Name crates `faucet-source-<name>` / `faucet-sink-<name>`. If you ship both a
source and a sink for the same system, put shared types (auth, formats) in a
`faucet-common-<name>` crate that both depend on and re-export.

> See any built-in connector (e.g. `faucet-source-rest`) for a reference
> implementation.
