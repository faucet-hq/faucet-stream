# Glossary

*The canonical vocabulary of faucet-stream. These terms are used with these exact meanings throughout the engineering documentation — link here rather than redefining a term in place.*

Terminology precision matters most around delivery semantics, where a loose word
("exactly-once") hides a real distinction. When in doubt, this page is
authoritative; if code and this page disagree, the code wins and this page is a
bug.

---

**Adaptive batching** — an opt-in controller (`crates/core/src/adaptive.rs`,
AIMD) that reslices a source [page](#page--streampage) into sub-batches whose
size it tunes from observed sink latency and error rate. Off by default. See
[batching](./architecture/batching.md).

**At-least-once** — the default [delivery guarantee](#delivery-guarantee). Every
record reaches the sink one *or more* times; a crash in the
[checkpoint](#checkpoint) window replays a [page](#page--streampage), which may
duplicate rows but never loses them. See [recovery](./architecture/recovery.md).

**Atomic-watermark** — one of the two [effectively-once](#effectively-once)
mechanisms: the sink commits the page's records *and* a monotonic
[commit token](#commit-token--watermark) atomically. Requires an idempotent sink,
a deterministic-replay source, durable [state](#state-store), and no
[DLQ](#dlq-dead-letter-queue).

**Backfill** — a bounded historical replay over a `--from/--to` window, chunked
into units, each re-running a root with `${backfill.*}` tokens. CLI-layer
orchestration; see [execution](./architecture/execution.md).

**Bookmark** — the durable value a [source](#source) uses to resume — an
incrementing column's max value, a CDC log position, a Kafka offset set. Opaque
to everything but the owning source; the [state store](#state-store) round-trips
it as a `serde_json::Value` without interpreting it. Persisting a bookmark is a
[checkpoint](#checkpoint). See [state management](./architecture/state-management.md).

**Checkpoint** — the act of persisting a [bookmark](#bookmark) to the
[state store](#state-store). The central invariant: a checkpoint happens **only
after** the sink has durably written and flushed the page
([invariants I1](./architecture/invariants.md)).

**CDC (Change Data Capture)** — sources that stream a database's change log
(postgres-cdc, mysql-cdc, mongodb-cdc). They emit a [bookmark](#bookmark) per
committed transaction, getting per-transaction [checkpoint](#checkpoint)
durability automatically.

**Commit token / watermark** — the monotonic, fixed-width per-page token issued
under the [atomic-watermark](#atomic-watermark) mechanism
(`format_token` / `format_token_with_bookmark`, `crates/core/src/idempotency.rs`).
Sinks store it opaquely; it may embed the page's resume [bookmark](#bookmark) so
recovery can re-anchor the source exactly. "Watermark" and "commit token" are
used interchangeably.

**Connector** — a [source](#source) or a [sink](#sink). The unit of the
marketplace; each lives in a `faucet-source-*` / `faucet-sink-*` crate depending
only on `faucet-core`. See [connector SDK](./architecture/connector-sdk.md).

**Contract** — a versioned promise about a pipeline's *output* schema/constraints
(`pipeline.contract:`). `on_breach: fail` aborts the page (writes nothing);
`quarantine` routes breaches to the [DLQ](#dlq-dead-letter-queue); `warn` logs.
See [contracts](./architecture/contracts.md).

**Delivery guarantee** — the promise about how many times a record reaches the
sink under failure: [at-least-once](#at-least-once) (default) or
[effectively-once](#effectively-once). Derived per topology in
`derive_delivery_guarantee` and surfaced by `faucet validate`/`doctor`.

**DLQ (dead-letter queue)** — an optional sink that receives records that failed
to write (or were quarantined by quality/contract/drift), wrapped in a
fixed-shape envelope. Note the envelope holds the *raw* failed record — see the
[security model](./architecture/security.md). See
[`../site/src/content/docs/cookbook/dlq.md`](./site/src/content/docs/cookbook/dlq.md).

**Effectively-once** — the guarantee `delivery: exactly_once` actually provides:
each record's *effect* on the sink is applied once, via one of two mechanisms —
[atomic-watermark](#atomic-watermark) or [keyed-upsert](#keyed-upsert). faucet
avoids the bare term "exactly-once" because true end-to-end exactly-once is not
achievable across arbitrary systems; the effect being idempotent is what is
delivered. See [recovery](./architecture/recovery.md).

**Keyed-upsert** — the second [effectively-once](#effectively-once) mechanism:
any source into an upsert-capable sink configured with `write_mode: upsert|delete`
+ a `key`. Idempotence comes from the sink's own keyed writes (replaying is a
no-op); needs no watermark and allows a [DLQ](#dlq-dead-letter-queue).

**Matrix** — the CLI's fan-out mechanism: a `matrix:` list where each row is
deep-merged into the pipeline, with parent/child (`parent:`) and completion
(`depends_on:`) edges forming a DAG. See [execution](./architecture/execution.md).

**Page / `StreamPage`** — the unit of work: `StreamPage { records, bookmark }`, a
chunk of records plus an optional [bookmark](#bookmark). Sources emit a stream of
pages; the pipeline writes one at a time, bounding memory at `O(batch_size)`. See
[stream-pages](./architecture/stream-pages.md).

**Pipeline** — the engine (`faucet_core::Pipeline` / `run_stream`) that connects
one [source](#source) to one [sink](#sink) and drives the per-page loop. Owns the
[checkpoint](#checkpoint) ordering. See [pipeline](./architecture/pipeline.md).

**Quality check** — per-record and per-batch validations
(`pipeline.quality:`) that run after transforms and before the sink; failures
`abort` or `quarantine` (to the [DLQ](#dlq-dead-letter-queue)). See
[quality](./architecture/quality.md).

**Resilience policy** — the opt-in `resilience:` block unifying retry, circuit
breaker, and poison-pill handling (`crates/core/src/resilience/`). See
[resilience](./architecture/resilience.md).

**Run** — one execution of a [pipeline](#pipeline) (one invocation of a matrix
row, one `faucet run`, one scheduled tick, one serve submission).

**Schema drift** — divergence between an incoming page's top-level shape and the
sink's live schema (`schema:` block). Policies: `warn` / `evolve` / `ignore` /
`quarantine` / `fail`; `fail` *defers* its abort until survivors are durable
(unlike a [contract](#contract) `fail`). See [schema](./architecture/schema.md).

**Shard** — a slice of a single source distributed across cluster workers
(`shard: { count }`), via PK-range, hash-of-key, or Kafka consumer-group
membership (`crates/core/src/shard.rs`). Distinct from the [matrix](#matrix).

**Sink** — a [connector](#connector) that writes records to a destination.
Required method: `write_batch`; capabilities (idempotent writes, upsert, schema
evolution) are opt-in defaulted trait methods. See
[connector SDK](./architecture/connector-sdk.md).

**Source** — a [connector](#connector) that reads records from an origin.
Required method: `fetch_with_context`; streaming, resumability, exactly-once,
sharding, and discovery are opt-in defaulted trait methods.

**State key** — the string identifying a resumable position, validated by
`validate_state_key`. Scheme: `{name}::{row_id}` for roots,
`{name}::{row_id}::{parent_record_key}` for matrix children. See
[state management](./architecture/state-management.md).

**State store** — the `StateStore` trait (`get`/`put`/`delete` over `Value`)
that persists [bookmarks](#bookmark). Built-in `memory` and `file` in core;
`redis` and `postgres` in their own crates.

**Transform** — a record-shaping stage (`flatten`, `select`, `cast`, `sql`,
`cdc_unwrap`, …) applied per page between source and sink. Layered
pipeline/source/row and resolved additively. See
[`../site/src/content/docs/cookbook/transforms.md`](./site/src/content/docs/cookbook/transforms.md).

**Write mode** — how a [sink](#sink) applies records: `append` (default),
`upsert`, or `delete` (`faucet_core::write_mode`). Upsert/delete require a `key`
and an upsert-capable sink; they enable the [keyed-upsert](#keyed-upsert)
guarantee.

## Related

- [Design invariants](./architecture/invariants.md) — where the load-bearing terms are enforced.
- [Architecture overview](./architecture/README.md)
- [Documentation home](./README.md)
