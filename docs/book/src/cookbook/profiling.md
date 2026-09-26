# Column profiling: learned baselines & drift detection

Quality checks catch what someone anticipated and wrote down (`null_rate <=
5%`), and the SLA monitor learns only row *volume*. The failures that actually
reach dashboards are the unanticipated ones: an upstream change makes a column
40 % null, a currency field starts arriving in cents, an enum gains a value.
The top-level `profiling:` block closes that gap **with no thresholds to
write**: every run profiles the records it wrote, the profile is compared with
a rolling baseline of earlier runs, and a statistically significant change on
any column is reported per column and per metric.

```yaml
version: 1
name: orders
pipeline:
  source: { type: postgres, config: { connection_url: "${env:PG_URL}", query: "SELECT * FROM orders" } }
  sink: { type: jsonl, config: { path: ./orders.jsonl } }
  state: { type: file, config: { path: ./state } }
profiling:
  min_history: 5        # runs of baseline before detection starts (default 5)
  window: 20            # rolling baseline size (default 20)
  on_drift: warn        # warn | notify | fail
```

A runnable example lives at `cli/examples/csv_to_jsonl_with_profiling.yaml`.

## What a profile holds

Per top-level column of the records the run **wrote** (after transforms and
masking, without the `_faucet_*` metadata columns):

| Statistic | Detail |
|-----------|--------|
| `null_rate` | absent-or-null records ÷ records written |
| `types` | how many values of each JSON type (`integer`, `number`, `string`, `boolean`, `object`, `array`, `null`) |
| `distinct` | estimated distinct non-null values (HyperLogLog, ≈1.6 % error, 4 KiB per column) |
| `numeric` | `min` / `max` / `mean` / `stddev` and approximate `p50` / `p95` (a 1 024-value reservoir) |
| `string` | character length `len_min` / `len_max` / `len_mean` |
| `top_values` | the most frequent values with count and share (a Space-Saving sketch; absent for high-cardinality columns) |

Nested objects and arrays count as one column whose value is their canonical
JSON. Memory is bounded per column regardless of row count, so a 10 M-row run
profiles in the same memory as a 10-row one.

A column whose estimated distinct count exceeds `categorical_max_distinct`
(default 100) is **high-cardinality**: it keeps null / type / length / numeric
statistics but publishes no values (an id or free-text column should never
land in a catalog as a list of examples). Because the pass runs *after*
[masking](./masking.md), a masked column's profile holds only its masked
values.

## Drift detection

Detection starts once the baseline holds `min_history` runs; earlier runs
just learn. Every finding names a column and one of these metrics:

| Metric | Test |
|--------|------|
| `null_rate`, `distinct`, `mean`, `min`, `max`, `string_length` | the run's value against the baseline series of that statistic — z-score (default, `sensitivity` 3.0) or Tukey IQR fences (`method: iqr`, `sensitivity` 1.5), the same detector the [SLA volume check](./sla.md) uses. A change below a small floor (one percentage point of null rate, 5 % of a distinct count, 1 % of anything else) is never reported, so sketch noise stays quiet. |
| `type_mix` | a JSON type appears that the baseline never held (strings in a numeric column) at ≥ `new_value_min_share` |
| `new_value` | a frequent value (≥ `new_value_min_share`, default 5 %) the baseline never listed |
| `vanished_value` | a value frequent in every baseline run is gone |
| `psi` | the value distribution shifted: population stability index above `psi_threshold` (default 0.2), computed only when the listed top values cover most of the column |

A column absent from the baseline (new this run) is learned, not flagged —
[schema drift](./schema-drift.md) owns added and removed columns.

## `on_drift`

| Value | Effect |
|-------|--------|
| `warn` (default) | a `WARN` log per finding and the `faucet_profile_drift_total` counter; the run succeeds |
| `notify` | as `warn`, plus one `profile_drift` [notification](./notifications.md) per finding |
| `fail` | as `notify`, and the run is reported **failed** — the data is already written (profiling runs after the write), so the failure marks the run for attention rather than undoing it |

Whatever the policy, the run's profile joins the baseline, so a repeated
drift fades once the new shape is the norm (after `min_history` runs). For a
*planned* change, re-baseline explicitly:

```bash
faucet profiling reset pipeline.yaml                    # forget every column's history
faucet profiling reset pipeline.yaml --column amount    # just one column
faucet profiling reset pipeline.yaml --row invoices     # one matrix row
```

## Inspecting profiles

```bash
faucet profiling show pipeline.yaml            # latest profile + drift per root row
faucet profiling show pipeline.yaml --full     # every column's full statistics
faucet profiling show pipeline.yaml --json
```

```text
row row-0 — 6 run(s) in the baseline (state orders::row-0)
  latest run 0199a3… at 2026-09-25T02:00:04Z: 12,480 row(s), 4 column(s)
  drift: 2 finding(s)
    ! amount.null_rate: null_rate 0.4000 vs baseline mean 0.0012 — |z| 41.30 exceeds 3 (…)
    ! region.new_value: value "latam" is 12.5% of the run; never among the top values in 6 baseline runs
  columns:
  amount                       null  40.0%  distinct     6 812  [number:7488]  min 10.5 max 99.5 mean 54.9 p50 55.5 p95 95.5
  region                       null   0.0%  distinct         4  [string:12480]  top "eu" 41.0%, "us" 33.2%, "latam" 12.5%, "apac" 13.3%
```

`faucet doctor` reports the baseline depth (`warming up: 2 of 5 run(s)`) and
whether the last run raised drift. With a [`catalog:`](./catalog.md) block (or
under `faucet serve`), every run's profile is also recorded on the **sink
dataset**: `faucet catalog show <id>` prints a `profile:` section, the
dataset detail endpoint carries `profile.latest` + `profile.history`, and the
web console's dataset page renders per-column null-rate sparklines with drift
markers.

## Where the baseline lives

The rolling history is stored in the pipeline's `state:` store under
`{name}::{row}::__profiling__` (topology mode: `{name}::{sink node}`), next to
the bookmarks — so it needs a `state:` block (enforced at config load) and a
`memory` store only baselines within one `faucet schedule` / `serve` process.
The catalog copy is for browsing; it is never the detector's input.

Profiling runs for real **root** invocations only: `--dry-run`, `--limit`,
shard executions, and cancelled runs neither profile nor touch the baseline.
A per-row `profiling:` on a matrix row replaces the top-level block for that
row, and a [deployment overlay](./template-hub.md#deployment-overlays) may set
it.

## Fields

| Field | Default | Description |
|-------|---------|-------------|
| `columns` | all | Only these top-level columns |
| `exclude` | `["_faucet_*"]` | Exact names or `prefix*` globs to skip |
| `max_columns` | 200 | Cap on profiled columns per run (the profile records how many were skipped) |
| `top_values` | 10 | Frequent values kept per categorical column; `0` disables |
| `categorical_max_distinct` | 100 | Distinct estimate above which a column publishes no values |
| `window` | 20 | Rolling baseline size (≥ `min_history`) |
| `min_history` | 5 | Baseline runs before detection starts (≥ 2) |
| `method` | `zscore` | `zscore` \| `iqr` for the numeric metrics |
| `sensitivity` | 3.0 / 1.5 | z-score threshold, or the IQR fence multiplier |
| `new_value_min_share` | 0.05 | Share a new value or type must reach to be reported |
| `psi_threshold` | 0.2 | PSI above which the value distribution counts as drifted |
| `on_drift` | `warn` | `warn` \| `notify` \| `fail` |

`faucet schema profiling` prints the JSON Schema.

## Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `faucet_profile_drift_total` | `pipeline`, `row`, `column`, `metric` | one increment per finding |
| `faucet_profile_runs_total` | `pipeline`, `row`, `outcome` | `stable` \| `drifted` \| `warming` |
| `faucet_profile_columns` | `pipeline`, `row` | columns profiled in the latest run |
| `faucet_profile_baseline_runs` | `pipeline`, `row` | runs currently in the baseline |

The shipped Prometheus rules include a `FaucetProfileDrift` alert on the
counter.

## Relationship to the other governance blocks

- [Quality checks](./quality.md) assert what you declared, per page, and can
  quarantine or abort **before** the write. Profiling learns what is normal
  and reports **after** the write.
- [Schema drift](./schema-drift.md) watches the *shape* (columns and types
  against the destination schema). Profiling watches the *content* of the
  columns that exist.
- [SLA monitoring](./sla.md) baselines one number per run (volume) with the
  same z-score / IQR test profiling applies to every column statistic.
