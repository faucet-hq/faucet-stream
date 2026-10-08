# ADR 0007 — Two-layer retries with a duplication-safety rule

*Retry transient failures on both the source and sink side — but never retry a non-idempotent write.*

- **Status:** Accepted (implemented) — `crates/core/src/retry.rs`, `resilience/`; `with_retry!` / `with_retry_write!` in `pipeline.rs`; `Source::with_retry_policy`.

## Context

Networked data movement fails transiently all the time — 5xx, rate limits, dropped
connections, timeouts. Retrying turns a flaky dependency into a reliable pipeline.
But retrying carelessly is dangerous: a write that committed server-side but lost
its response, retried, duplicates data — the project's stated worst bug class.

## Problem

Design a retry mechanism that (a) recovers from transient failures on both the
request (source) and write (sink) side, (b) never introduces duplicates, and (c) is
inert by default so the simplest pipelines are unchanged.

## Decision

**Two layers, one hard safety rule.**

- **Source side:** HTTP sources retry their own requests. `rest` keeps its bespoke
  `max_retries` / `retry_backoff` + `429`/`Retry-After` handling; `xml`/`graphql` use
  the shared `execute_with_retry`. A policy is injected via
  `Source::with_retry_policy`.
- **Sink side:** `run_stream` wraps `write_batch*`, `flush`, and `state_put` in
  `with_retry!`, gated on an attached `ResiliencePolicy`. With no policy the macro is
  a bare `.await` — the write path is byte-for-byte identical to un-retried code.

**The duplication-safety rule** (`with_retry_write!`): a plain `write_batch` /
`write_batch_partial` is retried **only** when `sink.write_batch_is_replay_safe()`
(default: `dedups_by_key()`, a keyed `write_mode: upsert|delete` config).
Otherwise it falls through to a bare `.await` with no retry. The gate is not
`supports_idempotent_writes()`, which covers only the token path; most sinks
advertising it still do a plain multi-row `INSERT` in `write_batch`.
`write_batch_idempotent` is retried through `eo_write_once`, which re-reads the
committed token before each attempt so a page that already committed is skipped.

Backoff is `base * 2^attempt` capped at `MAX_BACKOFF`, with decorrelated `[0.5,1.5)`
jitter seeded per call so concurrent retries don't realign. Only transient
`RetryClass` errors (`Http5xx`, `RateLimited`, `Connection`, `Timeout`) are retried;
`Config`/`Auth`/`Json` fail fast.

## Alternatives considered

- **Retry every write unconditionally.** Simplest, but duplicates data on a lost
  response to a non-idempotent sink. Rejected outright — it is the exact bug the
  project most wants to avoid.
- **Retry nothing in the core; leave it to connectors.** Rejected: every connector
  would reinvent backoff/jitter inconsistently, and sink writes (the risky part)
  would go unprotected.
- **Fixed backoff, no jitter.** Rejected: synchronises concurrent retries into a
  thundering herd.

## Trade-offs

- Declining to retry non-idempotent writes means a transient sink blip aborts the run
  (to be replayed from the last bookmark) rather than silently retrying — a restart
  cost paid for correctness.
- REST keeps its own runner for back-compat, so only `max_attempts` + `base` of the
  injected policy apply to REST (`retry_on`/`jitter` inert there). An asymmetry.

## Consequences

- **Positive:** transient failures self-heal on both sides; duplicates are never
  introduced by a retry; the no-policy path is unchanged.
- **Negative:** a non-idempotent sink under transient failure restarts rather than
  retries; REST's field precedence is a documented wart.

## Future work

- Fold the bespoke REST runner into the unified policy.
- Richer per-sink idempotency hints to safely retry more write shapes
  ([RFC 0001](../../rfcs/0001-capability-traits.md)).

## Related

- [Retries](../architecture/retries.md) · [resilience](../architecture/resilience.md) · [recovery](../architecture/recovery.md)
- [Design invariants (I2)](../architecture/invariants.md) · [ADR 0002 — Checkpoint ordering](./0002-checkpoint-ordering.md)
