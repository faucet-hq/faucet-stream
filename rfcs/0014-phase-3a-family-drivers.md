# RFC 0014 — Phase 3a: family drivers and the governed-page typestate

*Give every connector family a small I/O-only driver trait whose lifecycle the engine drives, make an ungoverned write inside `faucet-core` a compile error, and deprecate the `Sink` hooks the drivers replace — all on 1.x, without a major bump. Phase 3b (#888) then deletes what this RFC deprecates.*

| | |
|---|---|
| **RFC** | 0014 |
| **Title** | Phase 3a: family drivers and the governed-page typestate |
| **Status** | Accepted — **implemented** (foundation, #895); family migrations in progress |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #895 (this phase) · #888 (Phase 3b, the removals) · RFC 0013 phases 1–2: #891, #894 · #884 (SQL engine) · #886 (file sinks) · #885 (fidelity) · epic #38 |
| **Related ADRs** | [0002 checkpoint ordering](../docs/adr/0002-checkpoint-ordering.md), [0010 pipeline runtime](../docs/adr/0010-pipeline-runtime.md) |
| **Builds on** | [RFC 0013](./0013-structural-core-guarantees.md) — Phase 3, split here into 3a (additive) and 3b (breaking) |

## Summary

RFC 0013's third phase makes an un-governed write impossible to express. Most
of it does not need a breaking change, so it is split:

- **3a (this RFC, minor releases).** A `GovernedPage` typestate that only
  `faucet_core::governance` can mint and that every write path inside the
  engine takes; a defaulted `Sink::write_governed`; family **driver traits**
  (`SinkDriver` + lifecycle groups, `SourceDriver`, `MessageDriver`,
  `HttpDriver` in core; `SqlDriver` in `faucet-common-sql`;
  `ObjectStoreDriver` in `faucet-common-file`) with adapters (`DriverSink`,
  `DriverSource` and one per family) that turn a driver into a `Sink` /
  `Source`; conformance checks for driver-built sinks; the lifecycle hooks the
  drivers replace marked `#[deprecated]`; a warning for plugins registered
  without a descriptor. One reference connector per family (sqlite, the local
  file sink — and with it every `FileSink`-based sink — nats, http) runs on
  its driver.
- **3b (#888, `faucet-core` 2.0).** Delete the deprecated hooks, seal
  `Source` / `Sink`, make `capabilities()` required, change the public write
  signatures to take `GovernedPage`, ship the 1.x shim crate.

## Motivation

RFC 0013 traced ~180 audit findings to a core guarantee being bypassed, and the
first mechanism on its list was **fail-open defaulted trait hooks**: a connector
must implement a contract correctly and nothing checks it. Phases 1–2 made the
right path easy (pipeline-owned governance, declared capabilities, forwarding
macros, shared SQL and file engines). They did not make the wrong path
impossible:

- `Sink` has 40 methods. Overwrite is four of them (`begin_overwrite`,
  `commit_overwrite`, `abort_overwrite`, `overwrite_staging_exists`) and each
  SQL sink re-implemented their sequencing — drop stale staging, the
  first-run publish (#676), the swap — in its own crate; exactly-once is three
  (`write_batch_idempotent`, `write_batch_idempotent_reported`,
  `last_committed_token`) and every idempotent sink re-implemented "rows and
  token in one transaction". A capability flag (`supports_idempotent_writes`)
  could be `true` while the write path quietly was not atomic.
- Inside core, the per-page passes ran in `pipeline.rs` and their output was a
  plain `Vec<Value>`; nothing stopped a new write path (a DLQ write, a
  columnar loop, a retry) from writing records that never went through
  masking. Phase 1 caught such paths by review; the type system did not.

## Guide-level explanation

### The engine writes governed pages only

```text
records ──► governance::apply (masking → quality → contract → drift)
                │
                ▼
          GovernedPage<'_> ──► governance::write::{reported, partial, idempotent, …} ──► Sink
          GovernedEnvelopes ─► governance::write::dlq ──► the governed DLQ stack
```

`GovernedPage` has a private field and no public constructor. The only code
that mints one is `governance::apply` (and, for a page outside a pipeline,
`Governance::govern`, which runs the same passes). Its derivations stay
governed: `slice` (adaptive sub-batches), `select` (the poison-row retry
subset), and `restamped` (crate-internal, for the metadata layer). The columnar
and native paths take `GovernedBatch` / `GovernedNative`, minted only from a
governed page or under a `Clearance` — the proof that no record pass is
configured. `pipeline.rs` and `topology.rs` never call a sink's write methods:
every write goes through `governance::write`, and a unit test scans both files
to keep it so. Writing a raw page through the governed entry point is a type
error:

```rust,compile_fail
let records = vec![json!({"id": 1})];
sink.write_governed(&records).await; // expected `&GovernedPage<'_>`
```

### A connector is a driver

A connector author implements I/O primitives and nothing else:

```rust
#[async_trait]
impl SinkDriver for AcmeDriver {
    fn connector_name(&self) -> &'static str { "acme" }

    async fn write(&self, calls: &CallSlot, rows: &[Value], policy: RowPolicy)
        -> Result<PageWrite, FaucetError>
    {
        let outcomes = calls.call_once("insert", self.client.insert(rows)).await?;
        Ok(PageWrite::from_outcomes(outcomes))
    }

    // Overwrite? Return the staging group; the capability follows.
    fn staging(&self) -> Option<&dyn StagingDriver> { Some(self) }
}

pub struct AcmeSink { inner: DriverSink<AcmeDriver> }

#[async_trait]
impl Sink for AcmeSink { forward_sink!(inner); }
```

`DriverSink` derives every capability from the groups the driver returns —
overwrite exactly when `staging()` is `Some`, exactly-once when
`commit_tokens()` is, cleanup, schema evolution, rollback, columnar — so a
declaration cannot disagree with the code. It serves the plain, reported and
per-row write paths from one `write` primitive (`RowPolicy::WholePage` fails
the page on its first failed row; `RowPolicy::PerRow` reports each), checks
each result against the rows offered (`FaucetError::RowsUnaccounted`), and
owns the run's call context, which it hands to every primitive.

Families go one step further: the family adapter drives the lifecycle itself,
and the connector only supplies the backend's genuinely specific primitives.

| Family | Driver (I/O) | Adapter drives | Where |
|---|---|---|---|
| any sink | `SinkDriver` + `StagingDriver` / `CommitTokenDriver` / `CleanupDriver` / `SchemaDriver` / `RollbackDriver` / `ColumnarDriver` | capability derivation, the three row paths, row accounting, call context | `faucet_core::driver` |
| any source | `SourceDriver` (`read` → pages) | fetch / incremental fetch / stream, resume, probe | `faucet_core::driver` |
| SQL | `SqlDriver`: `begin`/`commit`/`connection`, `table_exists`, `ddl`, `read_token`, `create_staging`/`publish_staging`/`swap_staging`, `current_schema`, `existing_columns`/`apply_evolution` | create-on-first-write, keyed and chunked writes, per-row splits, rows + token in one transaction, scoped cleanup, stage → (publish \| swap) → discard, evolution steps | `faucet_common_sql::SqlSinkDriver` |
| object store | `ObjectStoreDriver` (= every `StorageBackend`) | encoding, rollover, the swap area, the columnar path | `faucet_common_file::FileDriver` / `FileSink` |
| messaging, produce | `MessageDriver`: `encode`, `publish(group)`, `connect`, `flush` (+ `MessageTransactions`) | groups, the per-group deadline, which rows a timeout or cancel leaves unconfirmed, the transactional exactly-once commit | `faucet_core::driver::MessageSinkDriver` |
| messaging, consume | `MessageConsumer`: `receive`, `ack`, `decode` | acknowledge a page only after the pipeline wrote and flushed it | `faucet_core::driver::MessageSourceDriver` |
| HTTP | `HttpDriver`: `session` (per page), `send(body)`, `batching` | per-record concurrency, array chunks, never reporting a delivered row failed | `faucet_core::driver::HttpSinkDriver` |

## Reference-level explanation

### `GovernedPage` and the write gateway

```rust
// faucet_core::governance (re-exported)
pub struct GovernedPage<'a> { /* private: Cow<'a, [Value]> */ }
impl<'a> GovernedPage<'a> {
    pub fn records(&self) -> &[Value];
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn slice(&self, range: Range<usize>) -> GovernedPage<'_>;
    pub fn select(&self, indices: &[usize]) -> GovernedPage<'static>;
    pub fn into_records(self) -> Vec<Value>;
}
pub struct GovernedEnvelopes { /* private */ }   // the DLQ writer's only input
#[non_exhaustive] pub struct GovernedOutput { pub page: GovernedPage<'static>, pub envelopes: GovernedEnvelopes }
impl Governance { pub async fn govern<Si: Sink + ?Sized>(&self, records: Vec<Value>, sink: &Si) -> Result<GovernedOutput, FaucetError>; }

// faucet_core::Sink — new, defaulted, forwarded by forward_sink!
async fn write_governed(&self, page: &GovernedPage<'_>) -> Result<WriteReport, FaucetError> {
    self.write_batch_reported(page.records()).await
}
```

- The per-page passes moved from `pipeline.rs` to `governance/apply.rs`
  unchanged (same order, same page-index bookkeeping, same deferred drift
  abort).
- `governance/write.rs` is the gateway: `reported`, `partial`, `admit`,
  `idempotent`, `columnar`, `native`, `dlq` — each taking a governed value.
  The row path's plain write now reaches `Sink::write_governed`; its default
  is the old `write_batch_reported`, so behaviour is unchanged.
- `forward_sink!` forwards `write_governed`, except that a wrapper changing
  `write_batch` or `write_batch_reported` keeps the trait default (which goes
  through its change). Core's own decorators — instrumentation, budget,
  cleanup tracker, metadata, profiling, policy — override `write_governed` to
  apply their layer and pass the governed page on.
- `write_governed` joins `SINK_HOOKS` and the `HookSink` probe.

Invariants: I1–I4 (write → flush → checkpoint) are untouched — only the type
of what is written changed. I5–I6 (masking first, fixed order) become a type
property inside core.

### Driver traits

All traits are object-safe `async_trait` traits except the family adapters'
generic parameters. Every async primitive receives `&CallSlot`; drivers make
each backend request through `CallSlot::call` / `call_once`, so the run's
deadline, cancellation and metering apply. New public types are
`#[non_exhaustive]` (`RowPolicy`, `PageWrite`, `SqlLayout`, `OutboundMessage`,
`InboundMessage`, `AckToken`, `HttpBatching`, `GovernedOutput`).

```rust
pub trait SinkDriver: Send + Sync {
    fn connector_name(&self) -> &'static str;
    async fn write(&self, calls: &CallSlot, rows: &[Value], policy: RowPolicy) -> Result<PageWrite, FaucetError>;
    // defaulted: config_schema, dataset_uri, call_context, check, flush, write_modes,
    // dedups_by_key, write_is_replay_safe, batch_atomicity, staged_load, capabilities(derived),
    // readback_source, complete_run, local_outputs, set_call_context, set_roundtrip_recorder,
    // and the groups: staging, commit_tokens, cleanup, schema, rollback, columnar (arrow)
}
pub trait StagingDriver { fn active(&self) -> bool; async fn stage/publish/discard(&self, &CallSlot); async fn staged(&self, &CallSlot) -> bool; }
pub trait CommitTokenDriver { async fn last_token(..); async fn write_with_token(.., rows, scope, token) -> WriteReport; }
pub trait CleanupDriver { async fn delete_absent(.., scope, seen) -> u64; }
pub trait SchemaDriver { async fn current_schema(..); fn evolvable(&self) -> bool; async fn evolve(.., &SchemaEvolution); }
pub trait RollbackDriver { fn enabled(&self) -> bool; async fn rollback_run/forget_run/rewind_commit_token(..); }
```

`DriverSink::check_declarations` refuses a driver whose `write_modes` include
overwrite without a staging group (or the reverse), or that dedups by key
without a keyed mode. `RollbackDriver::enabled` lets a configuration that
cannot roll back (a SQL sink in JSON-column mode) still answer with its own
explanation.

**Why the SQL and object-store traits live in their family crates.** Core
depends on no connector-family crate (RFC 0013: "the SQL engine lives in a
common crate, not core"). `SqlDriver` is defined over `SqlSinkEngine`,
`SqlDialect` and `SqlExecutor`, and `ObjectStoreDriver` is `StorageBackend`;
both families' lifecycles are written against those engines. So the
family-neutral contract (`SinkDriver` and its groups) is in core, and each
family crate provides the trait plus the adapter that implements
`SinkDriver` over it. Messaging and HTTP need nothing beyond core
(`faucet_core::http` is in core), so their traits and adapters are in core.

**Typed values.** Drivers exchange JSON `Value` under the #885 fidelity rules;
`ColumnarDriver` takes an Arrow `RecordBatch` unconverted. Native byte
passthrough stays on `Sink` in 3a (no driver needs it yet).

### Deprecations (since `faucet-core` 1.14.0)

| Hook | Replaced by | Why core can drive it now |
|---|---|---|
| `begin_overwrite` | `StagingDriver::stage` | `DriverSink` advertises overwrite only with a staging group and calls `stage` before the first write; the SQL family adapter owns the drop-stale-staging and first-run rules (#676) |
| `commit_overwrite` | `StagingDriver::publish` | called once, after a successful uncancelled run; the SQL adapter chooses first-run publish vs. swap from the database, not instance memory |
| `abort_overwrite` | `StagingDriver::discard` | best-effort discard on every other outcome |
| `overwrite_staging_exists` | `StagingDriver::staged` | a read-only probe of the same staging target |
| `write_batch_idempotent` | `CommitTokenDriver::write_with_token` | the SQL adapter writes rows and the token in one transaction from `begin`/`commit`; the message adapter does it inside a producer transaction |
| `write_batch_idempotent_reported` | `CommitTokenDriver::write_with_token` (returns the report) | same; the report is the accounting #879 checks |
| `last_committed_token` | `CommitTokenDriver::last_token` | the adapter ensures the token table, the driver reads it |
| `cleanup_scope` | `CleanupDriver::delete_absent` | the SQL adapter validates the key and runs the key-table delete in one transaction |

Not deprecated, because no driver replaces them yet: the capability booleans
(`is_overwrite`, `supports_idempotent_writes`, `supports_cleanup`, …, read by
`Capabilities::of_sink`), the row write methods (`write_batch`,
`write_batch_reported`, `write_batch_partial` — every sink still implements
them, and the engine's accounting uses them), the rollback hooks (CLI-driven;
`RollbackDriver` routes them but adds no lifecycle), schema evolution, native
loading, and every `Source` hook.

Deprecation warnings inside the workspace are silenced with a scoped
`#[allow(deprecated)]` and a one-line reason at each call site — the engine,
the CLI, the conformance probes and the forwarding macro still drive legacy
sinks through these hooks until 3b. Implementing a deprecated trait method
does not warn, so legacy connectors compile clean.

### Plugins without a descriptor

`PluginRegistry::register_source` / `register_sink` (no descriptor) log a
warning naming the plugin, and `faucet doctor` lists each such plugin as a
`plugins` probe: its capabilities cannot be checked at load time. Built-ins
already register descriptors (#876).

### Conformance

Two sink checks, required (trigger `driver`: the crate holds a `DriverSink<`
or implements `SinkDriver` / `SqlDriver` / `MessageDriver` / `HttpDriver`):

- `assert_driver_declarations_truthful` — every undeclared lifecycle is
  refused by its hook, the declared ones' probes answer, keyed dedup has a
  keyed mode, the overwrite capability matches `supported_write_modes`.
- `assert_driver_row_paths_agree` — plain, reported, per-row and governed
  writes of a valid page each account for every row.

Both run on any sink, so a third-party connector can call them too.

### Reference migrations

| Connector | Family | Shape after |
|---|---|---|
| `faucet-sink-sqlite` | SQL | `SqliteSink { inner: DriverSink<SqlSinkDriver<SqliteDriver>> }`; `SqliteDriver: SqlDriver + RollbackDriver` |
| `faucet-sink-file` (and every `FileSink`: s3, gcs, azure-blob, sftp, the deprecated parquet sink's S3 path) | object store | `FileSink<B> { inner: DriverSink<FileDriver<B>> }`, `B: ObjectStoreDriver` |
| `faucet-sink-nats` | messaging | `NatsSink { inner: DriverSink<MessageSinkDriver<NatsDriver>> }` |
| `faucet-sink-http` | HTTP | `HttpSink { inner: DriverSink<HttpSinkDriver<Endpoint>> }` |

Public types and configs are unchanged; each crate's existing tests pass
unmodified (nats's three unit tests of its private timeout bookkeeping moved
to core with the code).

## Migration guide for third-party connectors

Nothing breaks in 1.x: a connector on the old `Sink` keeps identical
behaviour, and implementing a deprecated method does not warn. Moving to a
driver is how a connector stays compatible with 2.0.

**Before** (1.x, hand-written lifecycle):

```rust
#[async_trait]
impl Sink for AcmeSink {
    async fn write_batch(&self, rows: &[Value]) -> Result<usize, FaucetError> { self.insert(rows).await }
    fn supported_write_modes(&self) -> &'static [WriteMode] { &[WriteMode::Append, WriteMode::Overwrite] }
    fn is_overwrite(&self) -> bool { self.config.write.is_overwrite() }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> { self.create_staging().await }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> { self.swap().await }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> { self.drop_staging().await }
    fn supports_idempotent_writes(&self) -> bool { true }
    async fn write_batch_idempotent(&self, rows: &[Value], scope: &str, token: &str) -> Result<usize, FaucetError> {
        let tx = self.begin().await?; /* rows + token */ tx.commit().await?; Ok(rows.len())
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> { self.read_token(scope).await }
}
```

**After** (driver):

```rust
#[async_trait]
impl SinkDriver for AcmeDriver {
    fn connector_name(&self) -> &'static str { "acme" }
    async fn write(&self, calls: &CallSlot, rows: &[Value], _: RowPolicy) -> Result<PageWrite, FaucetError> {
        Ok(PageWrite::accepted(calls.call_once("insert", self.insert(rows)).await?))
    }
    fn write_modes(&self) -> &'static [WriteMode] { &[WriteMode::Append, WriteMode::Overwrite] }
    fn staging(&self) -> Option<&dyn StagingDriver> { Some(self) }
    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> { Some(self) }
}

#[async_trait]
impl StagingDriver for AcmeDriver {
    fn active(&self) -> bool { self.config.write.is_overwrite() }
    async fn stage(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("begin_overwrite", self.create_staging()).await }
    async fn publish(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("commit_overwrite", self.swap()).await }
    async fn discard(&self, c: &CallSlot) -> Result<(), FaucetError> { c.call_once("abort_overwrite", self.drop_staging()).await }
    async fn staged(&self, c: &CallSlot) -> Result<bool, FaucetError> { c.call_once("overwrite_staging_exists", self.staging_exists()).await }
}

#[async_trait]
impl CommitTokenDriver for AcmeDriver {
    async fn last_token(&self, c: &CallSlot, scope: &str) -> Result<Option<String>, FaucetError> { c.call_once("last_committed_token", self.read_token(scope)).await }
    async fn write_with_token(&self, c: &CallSlot, rows: &[Value], scope: &str, token: &str) -> Result<WriteReport, FaucetError> {
        c.call_once("write_batch_idempotent", async { /* rows + token in one tx */ Ok(WriteReport::accepted(rows.len())) }).await
    }
}

pub struct AcmeSink { inner: DriverSink<AcmeDriver> }
#[async_trait] impl Sink for AcmeSink { forward_sink!(inner); }
```

A SQL backend implements `faucet_common_sql::SqlDriver` instead and gets the
whole lifecycle from `SqlSinkDriver`; a store implements `StorageBackend` and
builds a `FileSink`; a broker implements `MessageDriver`; an HTTP endpoint
implements `HttpDriver`. Add `assert_driver_declarations_truthful` and
`assert_driver_row_paths_agree` to `tests/conformance.rs`.

## Phase 3b removal list (#888, `faucet-core` 2.0)

1. Delete from `Sink`: `begin_overwrite`, `commit_overwrite`,
   `abort_overwrite`, `overwrite_staging_exists`, `write_batch_idempotent`,
   `write_batch_idempotent_reported`, `last_committed_token`, `cleanup_scope`
   (and their `forward_sink!` arms, `SINK_HOOKS` entries, `HookSink` methods).
2. Move the capability booleans those hooks paired with
   (`is_overwrite`, `supports_idempotent_writes`, `supports_cleanup`,
   `supports_schema_evolution`, `supports_rollback`, `supports_columnar`,
   `supports_staged_load`) behind `capabilities()`, which loses its default.
3. Split `Source` / `Sink` into the connector-facing driver traits and a
   sealed `SourceExt` / `SinkExt` implemented only by `DriverSource` /
   `DriverSink` (and core's decorators).
4. Change `write_batch` / `write_batch_reported` / `write_batch_partial` /
   `write_batch_columnar` and the DLQ writer to take `GovernedPage` /
   `GovernedEnvelopes`; `write_governed` becomes the one write method.
5. `PluginRegistry::register_source` / `register_sink` without a descriptor
   are removed (the warning becomes a compile error).
6. Ship `faucet-compat-1x`: a `LegacySink<S: 1.x Sink>` / `LegacySource`
   adapter implementing the 2.0 drivers for one major version.

## Drawbacks

- **Two shapes during 3a.** A connector can be a hand-written `Sink` or a
  driver; reviewers must know both until 3b.
- **Indirection.** A driver-built sink is three layers (`Connector` →
  `DriverSink` → family adapter → driver). The forwarding macro keeps the
  outer layer free; the cost is in reading, not running.
- **Deprecation noise.** Every in-workspace call of a deprecated hook carries
  an `#[allow(deprecated)]` with a reason until 3b.
- **`write_governed` is reachable only through forwarding decorators.** A
  third-party wrapper that changes `write_batch` without overriding
  `write_governed` keeps the old path for everything behind it — correct, but
  the inner sink's `write_governed` is not reached.

## Rationale and alternatives

- **Do everything in 3b.** Rejected: it would ship the typestate, the drivers
  and the migrations in one breaking release, with no period where a
  connector author can move at their own pace.
- **Put `SqlDriver` and `ObjectStoreDriver` in core.** Rejected: core would
  depend on the SQL and file engines, which RFC 0013 keeps out of core.
- **Enforce the gateway with a lint instead of types.** Partly adopted (the
  source-scan test), but a type cannot be forgotten by a new path the scan
  does not list.
- **Make `write_governed` the only write path now.** Rejected: changing
  `write_batch`'s signature is the 3b break.

## Prior art

Kafka Connect's task/converter split (the framework owns offsets and
transactions; a task only produces records), DataFusion's `TableProvider`
(capabilities by method, not flags), and the typestate builders common in Rust
I/O crates (`hyper`'s connection states) — an invalid sequence is unrepresentable
rather than checked.

## Unresolved questions

Can resolve during the family migrations:
- Whether the SQL adapter's first-run publish rule fits every backend (mysql
  swaps by `RENAME TABLE`; bigquery by a transaction) or needs a
  backend-chosen variant.
- The Kafka transactional producer maps onto `MessageTransactions` with the
  commit-token side topic; Redis and MongoDB fit `CommitTokenDriver` directly.
- An HTTP *source* driver: REST / GraphQL / XML keep their pagination in their
  crates and implement `SourceDriver` over `faucet_core::http::Client`.

## Future possibilities

With every connector on a driver, deterministic simulation (fault injection at
the driver boundary) and a generated connector skeleton become small.

## Related

- [RFC 0013 — Structural core guarantees](./0013-structural-core-guarantees.md)
- [Authoring connectors](../docs/book/src/extending/authoring-connectors.md)
- [Conformance reference](../docs/book/src/reference/conformance.md)
- [RFC process](./README.md)
