# RFC 0005 — Shared async connector-runtime primitives

*Expose reusable bounded-concurrency, cancellation-aware retry, and task-supervision helpers from `faucet-core` so connectors stop re-implementing them.*

| | |
|---|---|
| **RFC** | 0005 |
| **Title** | Shared async connector-runtime primitives |
| **Status** | Superseded by [RFC 0013](./0013-structural-core-guarantees.md) (#880) |
| **Authors** | faucet-stream maintainers |
| **Related issues** | epic #38 |
| **Related ADRs** | [0007 retries](../docs/adr/0007-retries.md), [0005 runtime recovery](../docs/adr/0005-runtime-recovery.md) |

## Summary

Each connector currently manages its own async concurrency, retry, and
task-supervision logic. Some of this is already centralized (`faucet-core`'s
retry/resilience modules; `tokio_util::sync::CancellationToken` re-exported for
cooperative cancellation), but bounded-concurrency patterns and per-connector
retry wrappers are re-implemented repeatedly. This RFC proposes a small set of
shared runtime primitives in `faucet-core` that connectors reuse, reducing
duplication and inconsistency without changing any connector's external
behaviour.

## Motivation

The codebase already shows both the pattern and the duplication:

- **Retry is partly shared, partly bespoke.** `crates/core/src/retry.rs` provides
  `execute_with_retry` (exponential backoff + jitter, gated on
  `FaucetError::is_retriable`), used by the XML and GraphQL sources; the
  `resilience/` module provides `execute_with_policy[_metered]`, a circuit
  breaker, and classification. But the REST source keeps its *own* retry module
  for 429/`Retry-After` handling, and the sink-side retry lives inline in
  `run_stream`'s `with_retry!` macro. Three retry implementations, three
  behaviours.
- **Bounded concurrency is copy-pasted.** The performance discipline (see
  [performance architecture](../docs/architecture/performance.md)) mandates
  `buffer_unordered`/semaphore-bounded parallel I/O — implemented independently in
  the S3, GCS, parquet, Kinesis, HTTP, and other connectors, each with its own
  concurrency knob and glue.
- **Cancellation is shared but wrapping is not.** `CancellationToken` is
  re-exported and the pipeline is cancel-aware, but connectors that spawn their
  own tasks (per-shard workers, per-partition consumers) each wire
  cancellation/cleanup by hand.

Nothing here is broken; the cost is consistency and connector-author effort. A
new connector author must re-derive "how do I bound concurrency and retry safely"
from other connectors rather than reaching for a blessed helper.

## Guide-level explanation

`faucet-core` exposes a small, opt-in runtime toolkit that connector authors
reach for instead of hand-rolling:

- **`bounded_map(items, concurrency, f)`** — run an async closure over items with
  a shared semaphore/`buffer_unordered`, the one-true implementation of the
  parallel-I/O pattern, cancellation-aware.
- **A unified retry entry point** — one function that all HTTP-style sources and
  all sink-side writes call, honouring the [resilience policy](../docs/architecture/resilience.md)
  and the [safety rule](../docs/adr/0007-retries.md) that a non-idempotent write
  is only retried when the sink commits idempotently.
- **A task-supervision helper** — spawn-and-supervise for the per-shard /
  per-partition worker pattern, with cancellation propagation and error
  aggregation, so connectors like Kinesis and the Kafka member-mode source share
  one supervisor.

These are helpers, not a framework: a connector may still drop to raw `tokio`
where it needs to. They live in `faucet-core` so connector authors, who depend
only on `faucet-core`, get them for free (see the
[extensibility model](../docs/architecture/extensibility.md)).

## Reference-level explanation

- **Concurrency helper.** A generic `bounded_map`/`for_each_concurrent`-style
  function taking a concurrency limit and an optional `CancellationToken`,
  returning results in completion order with per-item error capture (so a
  connector can route per-item failures to partial-batch handling). No new deps —
  built on `futures`/`tokio` already in the tree.
- **Retry consolidation.** Converge the three retry implementations onto the
  `resilience::execute_with_policy` family. The REST source's 429/`Retry-After`
  handling becomes a `RetryClass`/policy input rather than a separate module;
  `retry.rs`'s `execute_with_retry` becomes a thin adapter over the policy runner
  (it already shares the `BackoffKind::Exponential` + jitter machinery). This is
  a **behaviour-preserving** refactor gated by tests, not a semantics change.
- **Supervision.** A `spawn_supervised` helper that owns a `JoinSet`, propagates
  a parent `CancellationToken` to children, and returns aggregated results — the
  pattern the CLI executor already implements for matrix fan-out
  (`cli/src/executor.rs`), lifted to a reusable core primitive for connector-
  internal worker pools.
- **Object-safety / additivity.** These are free functions and helper structs,
  not trait methods, so they add zero surface to `Source`/`Sink` and cannot break
  object-safety (see [ADR 0003](../docs/adr/0003-builder-pattern.md)).

## Drawbacks

- **Refactor risk.** Consolidating three retry implementations touches the REST
  source's carefully-tuned 429 handling; the migration must be test-gated to
  prove behaviour is preserved (the [testing standard](../docs/standards/testing.md)
  applies — don't blindly update tests to match a regression).
- **Abstraction leakage.** A too-opinionated helper that doesn't fit a
  connector's real needs is worse than raw `tokio`; the helpers must stay small
  and escapable.
- **Core surface growth.** Every helper added to `faucet-core` is API third
  parties may depend on and we must keep stable ([stability policy](../docs/stability.md)).

## Rationale and alternatives

- **Status quo (per-connector).** Rejected as the long-term state — it scales
  duplication linearly with the connector count and lets behaviours drift.
- **A separate `faucet-runtime` crate.** Rejected for v1 — connectors depend only
  on `faucet-core` by design; putting the primitives elsewhere would add a
  required dependency, contrary to the [extensibility](../docs/architecture/extensibility.md)
  principle. Could be revisited if the toolkit grows large.
- **Adopt a third-party structured-concurrency crate wholesale.** Rejected —
  unnecessary dependency weight for the handful of patterns we actually reuse.

## Prior art

`tokio`/`futures` (`buffer_unordered`, `JoinSet`), Tokio's own utilities crate,
and structured-concurrency libraries (e.g. `async-nursery`) inform the
supervision helper. Kafka Connect's worker/task model is the domain analog for
the per-partition supervision pattern.

## Unresolved questions

- Must resolve before Accepted: the exact retry consolidation plan for the REST
  source's 429/`Retry-After` semantics (fold into `RetryClass` vs keep a
  specialized adapter).
- During implementation: whether the concurrency helper should surface partial
  results for DLQ routing directly.

## Future possibilities

- A shared per-partition/per-shard consumer runtime underpinning the push-source
  adapter from [RFC 0004](./0004-streaming-improvements.md).
- Runtime-level metrics for connector-internal worker pools via the existing
  observability layer.

## Related

- [RFC process](./README.md) · [RFC 0004 streaming](./0004-streaming-improvements.md)
- [Resilience](../docs/architecture/resilience.md) · [Retries](../docs/architecture/retries.md) · [ADR 0007 retries](../docs/adr/0007-retries.md) · [Recovery](../docs/architecture/recovery.md)
