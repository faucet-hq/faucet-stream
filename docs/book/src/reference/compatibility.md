# Feature compatibility

<!-- Generated from `faucet_core::compat::RULES`. Do not edit by hand: run `cargo test -p faucet-cli --test compat_doc -- --ignored regenerate`. -->

Which write path a run takes, and whether a combination is allowed at all, comes from one table in `faucet_core::compat`. `Pipeline::run` picks the fastest path every active feature supports, and `faucet validate` refuses a combination no path honours before anything runs.

- **✓** — supported on that path.
- **falls back** — that path would skip the feature, so the run takes a slower path that honours it (native → columnar → row).
- **refused** — the combination is rejected when the config is loaded.

A pair the table does not list is supported.

## Write paths

| Feature | native | columnar | row | exactly-once watermark | overwrite staging |
|---|---|---|---|---|---|
| `transforms:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `masking:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `masking:` rule that retypes a value | falls back | falls back | ✓ | ✓ | ✓ |
| `quality:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `contract:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `schema:` (any `on_drift`) | falls back | ✓ | ✓ | ✓ | ✓ |
| `schema: { on_drift: fail }` | falls back | ✓ | ✓ | ✓ | ✓ |
| `schema: { on_drift: evolve }` | falls back | ✓ | ✓ | ✓ | refused |
| `dlq:` (`on_batch_error: propagate`) | falls back | ✓ | ✓ | refused | ✓ |
| `dlq:` (`on_batch_error: dlq_all`) | falls back | falls back | ✓ | refused | ✓ |
| `dlq:` with a sink reporting per-row outcomes | ✓ | falls back | ✓ | ✓ | ✓ |
| `delivery: exactly_once` (atomic watermark) | falls back | falls back | ✓ | ✓ | ✓ |
| `write_mode: upsert` / `delete` | falls back | ✓ | ✓ | ✓ | ✓ |
| `write_mode: overwrite` | ✓ | falls back | ✓ | refused | ✓ |
| `write_mode: overwrite` shared by fan-out rows | falls back | falls back | ✓ | refused | ✓ |
| adaptive batch sizing | falls back | falls back | ✓ | ✓ | ✓ |
| `resilience:` | falls back | falls back | ✓ | ✓ | ✓ |
| `complete_for: { on_missing: delete }` | falls back | falls back | ✓ | ✓ | refused |
| `policy:` rule with `on_runtime: quarantine` | falls back | falls back | ✓ | refused | ✓ |
| `policy:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `budget:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `profiling:` | falls back | ✓ | ✓ | ✓ | ✓ |
| `metadata_columns:` | falls back | falls back | ✓ | ✓ | ✓ |

The exactly-once watermark and overwrite staging columns apply only when the run uses them (`delivery: exactly_once` against an idempotent sink, `write_mode: overwrite`).

## Sink capabilities

A sink declares its capabilities (`Sink::capabilities`, `faucet list --json`). These combinations depend on them:

| Feature | Sink | Verdict |
|---|---|---|
| `schema: { on_drift: fail }` | sink drops unknown fields | refused |
| `schema: { on_drift: evolve }` | sink without schema evolution | refused |
| `complete_for: { on_missing: delete }` | sink without cleanup | refused |

## Refusal reasons

- `dlq:` (`on_batch_error: propagate`) × exactly-once watermark: delivery: exactly_once (atomic watermark) is not compatible with a DLQ in this version.
- `write_mode: overwrite` × exactly-once watermark: delivery: exactly_once (atomic watermark) is not compatible with write_mode: overwrite.
- `policy:` rule with `on_runtime: quarantine` × exactly-once watermark: a policy rule with on_runtime: quarantine is not compatible with delivery: exactly_once (atomic watermark): a quarantined row would fail the whole page's atomic commit instead of reaching the DLQ (only a rule whose `when` can match the row's sink kind and attributes counts).
- `complete_for: { on_missing: delete }` × overwrite staging: write_mode: overwrite replaces the whole destination, so scoped cleanup has nothing to do.
- `schema: { on_drift: evolve }` × overwrite staging: write_mode: overwrite stages into a copy of the destination taken before the run, so schema.on_drift: evolve cannot apply to it.
- `schema: { on_drift: fail }` × sink drops unknown fields: schema.on_drift: fail cannot be enforced against this sink: it reports no destination schema and silently drops fields its schema lacks, so a drift would never be detected and the fields would be lost.
- `schema: { on_drift: evolve }` × sink without schema evolution: schema.on_drift: evolve requires a sink that can evolve its destination schema.
- `complete_for: { on_missing: delete }` × sink without cleanup: scoped cleanup requires a sink that can delete a scope.
