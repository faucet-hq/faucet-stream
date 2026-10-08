# State & Durability Standard

*The write→flush→checkpoint ordering is the project's central data-integrity invariant and must never be reordered.*

State is what makes a run resumable and what stands between a crash and a corrupted destination. The rules here are stricter than the rest of the standards because a violation is silent: it does not fail CI, it loses or duplicates data in production. Read alongside [ADR 0002 — Checkpoint Ordering](../adr/0002-checkpoint-ordering.md) and [Design Invariants](../architecture/invariants.md).

## The ordering invariant

- **A page's bookmark MUST be persisted only AFTER the sink has durably written and flushed that page.** In `run_stream` (`crates/core/src/pipeline.rs`) every one of the three write paths (default, DLQ, exactly-once) follows the same order:

  1. `write_batch` / `write_batch_partial` / `write_batch_idempotent`
  2. `sink.flush()`
  3. `StateStore::put(key, bookmark)`

- **This ordering MUST NOT be reordered, "optimized", or short-circuited.** Persisting the bookmark before the flush would advance the checkpoint past records the sink has not durably accepted — a crash there loses data with no way to detect it. The flush matters concretely: a buffered sink (Parquet writes its footer on flush; S3 completes its multipart) is only readable after the flush returns.
- **The crash window between flush and put is intentional and safe.** If the process dies after the flush but before the put, the run resumes and re-reads the page:
  - **at-least-once** (default): the page is re-written. Sinks must tolerate this — it is the contract.
  - **effectively-once**: the sink's committed watermark seq ≥ the page's, so the page is skipped (`faucet_pipeline_pages_skipped_total`), or the sink-anchored resume re-anchors the source to the token-embedded bookmark. See [Recovery](../architecture/recovery.md).

## State keys

- **Every state key MUST pass `validate_state_key`** (`crates/core/src/state.rs`) before a get/put/delete. Keys are colon-namespaced and reject empty, leading-dot, and path-traversal forms.
- **The CLI owns the key scheme**, not the connector: `{name}::{row_id}` for roots, `{name}::{row_id}::{parent_record_key}` for matrix children, with namespaced variants for backfill (`{name}::backfill::{unit}`) and replication (`{name}::__replication__`). A connector exposes its *natural* key via `state_key()`; the executor overrides it via `StateKeyOverride`.

## Bookmarks are opaque

- **A bookmark is an opaque `serde_json::Value` owned by the source that produced it.** Only that source interprets it (`apply_start_bookmark`). No other component parses a bookmark's internal shape.
- **A source MUST advance its bookmark monotonically** with respect to committed work, so a resume never moves backward past durably-written records.

## Commit tokens are opaque to sinks

- **A sink MUST store the commit token verbatim and never parse it.** The token may carry a `#<bookmark-json>` suffix (`format_token_with_bookmark`); the sink treats the whole string as an opaque watermark it writes atomically alongside the page and reads back in `last_committed_token`. Only the pipeline (`crates/core/src/idempotency.rs`) parses tokens.
- **`write_batch_idempotent` MUST commit the page and the token in one atomic unit** — an in-transaction UPSERT into `_faucet_commit_token`, a snapshot summary property, a Kafka transaction, etc. Committing them separately breaks the guarantee.

## Delivery-guarantee gate

- **`delivery: exactly_once` MUST satisfy one of two topologies** (enforced at config-load in `cli/src/expand.rs`, re-checked in `run_stream`):
  - **atomic-watermark:** an idempotent sink (`supports_idempotent_writes`) + a deterministic-replay source (`replay_guarantee`) + a durable (non-`memory`) state store + **no DLQ** (incompatible in this version); or
  - **keyed-upsert:** an upsert-capable sink configured with `write_mode: upsert|delete` + non-empty `key`, with any source and no state/DLQ requirement.
- **Retries on a plain `write_batch` are forbidden** unless the sink reports `write_batch_is_replay_safe()` (default: `dedups_by_key()`, i.e. a keyed upsert/delete config) — a retried lost-response write silently duplicates rows. `supports_idempotent_writes()` is not the gate: it covers only the token path. `run_stream`'s `with_retry_write!` enforces this. See [ADR 0007 — Retries](../adr/0007-retries.md).

## Related

- [ADR 0002 — Checkpoint Ordering](../adr/0002-checkpoint-ordering.md)
- [ADR 0006 — State Management](../adr/0006-state-management.md)
- [Design Invariants](../architecture/invariants.md)
- [State Management](../architecture/state-management.md) · [Recovery](../architecture/recovery.md)
- [Error Handling Standard](./error-handling.md)
