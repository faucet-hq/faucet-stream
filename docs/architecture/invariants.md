# Design invariants

*The load-bearing guarantees the whole system depends on. If you change the write path, you are changing this file — update it deliberately.*

An invariant here is a rule the codebase relies on being true *everywhere*,
always. They are not aspirations; each is enforced in code and cited below.
Violating one silently corrupts data or breaks recovery — the worst class of
bug this project can ship. Treat this page as a contract with future
maintainers.

Every invariant lists **what it guarantees**, **where it is enforced**, and
**what breaks if you violate it**.

---

## I1 — A bookmark is persisted only after a durable, flushed write

**Guarantee.** For any page, the state store is updated with that page's
bookmark only *after* both `write_batch*` and `flush()` have returned `Ok`. The
ordering is always: **write → flush → checkpoint**. The state store is never
ahead of the sink.

**Where.** `run_stream` in `crates/core/src/pipeline.rs`, in all three write
paths (DLQ, exactly-once, default). Each ends with
`… sink.flush()? … store.put(key, bookmark)?`.

**If violated.** A crash after the checkpoint but before the write would skip
records permanently — silent data loss. This is the single most important rule
in the codebase. See [ADR 0002](../adr/0002-checkpoint-ordering.md),
[recovery](./recovery.md), [state](./state-management.md).

## I2 — Retries never advance state, and never retry a non-idempotent write

**Guarantee.** A retry re-attempts the *same* operation against the *same*
position; it never moves the bookmark forward. And a plain, non-idempotent
`write_batch` is retried **only** when replaying it converges
(`Sink::write_batch_is_replay_safe()`, which defaults to `dedups_by_key()` —
a sink configured with `write_mode: upsert|delete` and a `key`); otherwise a
lost-response retry could duplicate every row.

**Where.** The `with_retry_write!` macro in `run_stream` gates retrying plain
writes on `sink.write_batch_is_replay_safe()`. It deliberately does **not**
gate on `supports_idempotent_writes()`: that only promises the rows and a
commit token commit together on the `write_batch_idempotent` path, and most
sinks that advertise it still do a plain multi-row `INSERT` in `write_batch`.
The idempotent path (`write_batch_idempotent`) is retried through
`eo_write_once`, which re-reads the committed token before every attempt, so a
page that committed before its response was lost is not written again.

**If violated.** Duplicated rows on a lost sink response — the repo's
#1-worst-bug class. See [retries](./retries.md), [ADR 0007](../adr/0007-retries.md).

## I3 — State reflects only committed work

**Guarantee.** A bookmark read on the next run points at a position for which
every prior record is durably in the sink. Under exactly-once the stored
`(bookmark, seq)` envelope is reconciled against the sink's committed watermark,
so the stored state is never treated as further ahead than the sink.

**Where.** `wrap_state`/`unwrap_state` + the sink-anchored resume block in
`Pipeline::run` (`crates/core/src/idempotency.rs`, `pipeline.rs`).

**If violated.** Recovery would resume past uncommitted data (loss) or
re-process committed data as if new (duplication under exactly-once).

## I4 — `StreamPage` ordering is preserved and checkpoints advance monotonically

**Guarantee.** Pages are consumed strictly in the order the source emits them;
checkpoints only ever move forward. Page N is fully durable before page N+1's
bookmark can be persisted.

**Where.** The single-threaded page loop in `run_stream`; there is no
concurrent consumption of the page stream.

**If violated.** Out-of-order checkpointing could persist a later bookmark while
an earlier page's write is still pending — reintroducing the I1 loss window.
See [stream-pages](./stream-pages.md).

## I5 — Masking runs first, before anything observes the records

**Guarantee.** When a masking policy is attached, the masking pass runs on each
page **before** the quality, contract, and drift passes, before any sink write,
before the DLQ, and before any lineage sample. No downstream component ever sees
unmasked PII.

**Where.** `run_stream` applies masking as the first per-page transformation
(`crates/core/src/pipeline.rs`, guarded by `#[cfg(feature = "masking")]`).

**If violated.** PII would leak to a sink, a DLQ envelope, or a lineage sample —
a security/compliance defect. See [masking](./masking.md).

## I6 — The per-page pass order is fixed: mask → quality → contract → drift

**Guarantee.** The four protection/validation passes always run in this order.
`contract: on_breach=fail` aborts the page (writes nothing) because breaching
data must never be committed; `schema: on_drift=fail` *defers* its abort until
the page's individually-valid survivors are durable.

**Where.** `run_stream` pass sequence in `crates/core/src/pipeline.rs`.

**If violated.** Reordering could let a quality check pass on unmasked data, or
commit contract-breaching rows. The fixed order removes a footgun config knob.
See [schema](./schema.md), [contracts](./contracts.md), [quality](./quality.md).

## I7 — Every early exit flushes the sinks

**Guarantee.** A source error, a propagated write/flush/state error, a DLQ-budget
abort, a circuit-breaker trip, or a cancellation all funnel through a single
inner future that performs a best-effort final `flush()` before returning.

**Where.** The `loop_result` inner future + trailing flush in `run_stream`.

**If violated.** A buffered sink that only commits on flush (Parquet writes its
footer there; without it the whole file is unreadable; S3 multipart aborts)
would orphan everything written so far. See [pipeline](./pipeline.md).

## I8 — Cancellation is cooperative and flush-completing

**Guarantee.** A `CancellationToken` cancel stops the loop at the **next page
boundary**, flushes the sinks, and returns the *partial* `PipelineResult` as
`Ok`. It never tears down a run mid-`write_batch` from the core's side (a sink
stuck inside a write past the caller's grace is hard-dropped by the caller, not
the core).

**Where.** The `tokio::select! { biased; token.cancelled() … }` page-poll race
in `run_stream`; the executor's `STOP_FLUSH_GRACE` backstop
(`cli/src/executor.rs`).

**If violated.** A cancel that dropped the future immediately would flush
nothing — the difference between a clean stop and a corrupted Parquet file.
See [execution](./execution.md) and [ADR 0011](../adr/0011-cooperative-cancellation.md).

## I9 — Config gates are enforced before any run starts

**Guarantee.** An impossible topology — an invalid exactly-once combination, an
unsupported `write_mode × sink`, `schema: evolve` on a schemaless sink,
quarantine without a DLQ — is rejected at config-load in `expand`, so
`faucet validate` fails before a single record moves.

**Where.** `cli/src/expand.rs`; the atomic-watermark gate is re-checked at the
core boundary in `run_stream`.

**If violated.** A config that can't work would fail *mid-stream*, potentially
after partial, unrecoverable side effects.

## I10 — Bookmarks and commit tokens are opaque to everyone but their owner

**Guarantee.** The `StateStore` round-trips a bookmark `Value` without
interpreting it; only the owning source reads it (`apply_start_bookmark`). A
sink stores a commit token as an opaque string and never parses it — the token
may embed a bookmark suffix, which only the pipeline decodes.

**Where.** `StateStore` (`state.rs`), the sink `write_batch_idempotent`
contracts (`traits.rs`), `format_token`/`parse_token` (`idempotency.rs`).

**If violated.** A backend or sink that interpreted the value would couple to
connector internals and break the marketplace's connector-independence. See
[connector SDK](./connector-sdk.md), [extensibility](./extensibility.md).

## I11 — Observability can never fail a run

**Guarantee.** Metrics, spans, lineage emission, and OTLP export are
best-effort. An export error is logged and counted, never propagated; a panic in
an instrumented section is isolated (`catch_unwind`) and surfaced as a `Panic`
error kind, not an aborted process.

**Where.** The decorators in `crates/core/src/observability/`; the lineage
emitter's swallow-and-count path.

**If violated.** A metrics backend outage could take down data movement — an
unacceptable coupling. See [observability](./observability.md),
[standards: logging](../standards/logging.md).

---

## How these compose into delivery guarantees

| Guarantee | Requires | Behaviour on the crash window (I1) |
|-----------|----------|-------------------------------------|
| **At-least-once** (default) | nothing | replays the page — may duplicate |
| **Effectively-once / atomic-watermark** | idempotent sink + deterministic-replay source + durable state + no DLQ | skips or re-anchors the page — no duplication |
| **Effectively-once / keyed-upsert** | upsert-capable sink + `write_mode: upsert\|delete` + `key` | re-upsert is a no-op — no duplication |

## When you change the write path

If your change touches `run_stream`, ask for each invariant above: *does my
change preserve it?* If it relaxes one, that is a design decision requiring an
[ADR](../adr/) or [RFC](../../rfcs/README.md), not a quiet edit. Add a test that
would fail if the invariant regressed.

## Related

- [ADR 0002 — Checkpoint ordering](../adr/0002-checkpoint-ordering.md) · [ADR 0005 — Runtime recovery](../adr/0005-runtime-recovery.md) · [ADR 0007 — Retries](../adr/0007-retries.md)
- [Pipeline engine](./pipeline.md) · [Recovery](./recovery.md) · [State management](./state-management.md) · [Retries](./retries.md)
- [Standards: state & durability](../standards/state.md)
- [Engineering principles](../engineering-principles.md)
