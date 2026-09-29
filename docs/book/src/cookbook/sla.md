# SLA monitoring: freshness & volume

The most damaging pipeline failures are *silent*: a source quietly starts
returning nothing, or a pipeline stops advancing, and nobody notices until a
dashboard is empty. The top-level `sla:` block turns faucet's raw run telemetry
into a declared contract — evaluated automatically after every root invocation
by `faucet run`, `schedule`, `serve`, and `replicate`.

It is **fully opt-in** and it **never fails a run**: a violation emits a
Prometheus counter and a structured warning, and shows up in `faucet doctor` —
the run itself completes exactly as it would have without the block.

```yaml
version: 1
name: orders
pipeline:
  source: { type: postgres, config: { connection_url: "${env:PG_URL}", query: "SELECT * FROM orders" } }
  sink: { type: file, config: { path: ./orders.jsonl } }
  state: { type: file, config: { path: ./state } }
sla:
  max_staleness_secs: 7200     # alert when no successful run within 2 hours
  min_rows_per_run: 1          # a successful run writing 0 rows is a violation
  volume_anomaly:
    method: zscore             # zscore | iqr
    min_history: 5             # don't alert until 5 successful runs of history
```

A runnable example lives at `cli/examples/csv_to_jsonl_with_sla.yaml`.

## The checks

| Check | Fires when… | Needs `state:`? |
|-------|-------------|-----------------|
| `max_staleness_secs` | a run **fails** and the last successful run is older than the threshold (also probed read-only by `faucet doctor`) | yes |
| `min_rows_per_run` | a run **succeeds** but writes fewer records than the floor | no |
| `volume_anomaly` | a run **succeeds** but its volume is anomalous against the rolling baseline of recent successful runs | yes |
| `max_lag_bytes` / `max_lag_events` / `max_lag_seconds` | the source reports it is further behind its head than the threshold when a run ends (#733) | no |

They compose freely — declare any subset. An `sla:` block that declares
none of them is rejected at config load, as is a stateful check without a
`state:` block (`faucet validate` catches both).

## How the baseline works

After every successful root invocation the executor folds the run's record
count and timestamp into a small history object stored **next to the
pipeline's bookmarks** in the configured state store, under
`{name}::{row}::__sla__`. The history keeps the last `window` (default 20)
successful-run volumes; failed, cancelled, `--dry-run`, and `--limit` runs
never touch it, so synthetic or partial volumes cannot poison the baseline.

`volume_anomaly` compares each new successful run against that baseline
*before* folding it in:

- **`zscore`** (default) — anomalous when |volume − mean| / std exceeds
  `sensitivity` (default `3.0`). A constant baseline (std = 0) flags any
  deviation.
- **`iqr`** — anomalous when the volume falls outside the Tukey fences
  `[Q1 − k·IQR, Q3 + k·IQR]` with `k = sensitivity` (default `1.5`). More
  robust than z-score when the baseline itself contains outliers.

Both are two-sided: a silent drop to zero and a 10× spike both fire. Detection
stays quiet until `min_history` (default 5) successful runs have accumulated,
and the anomalous volume still joins the rolling window afterwards — a genuine
regime change (e.g. a backfill doubling daily volume) stops alerting once the
window adapts, rather than firing forever.

Staleness is measured against the last *successful* run: when a run fails, the
executor checks how long ago the pipeline last succeeded and fires the
`staleness` violation once that exceeds `max_staleness_secs`. Under
`faucet schedule` this means every failing tick past the threshold re-alerts,
which is exactly what you want a pager rule keyed on.

## Source lag

"Fresh by bookmark" is not "caught up": a CDC pipeline can commit a bookmark
every minute while WAL piles up faster than it drains. Sources with a notion of
a head report how far behind it they are, and the `max_lag_*` thresholds turn
that into an SLA:

```yaml
sla:
  max_lag_bytes: 1073741824   # Postgres slot / MySQL binlog: at most 1 GiB unread
  max_lag_events: 100000      # Kafka: at most 100k unconsumed messages
  max_lag_seconds: 900        # MongoDB / SQL Server / Oracle / Kinesis / DynamoDB: oldest unread change ≤ 15 min
```

| Source | Reports | Measured as |
|---|---|---|
| `postgres-cdc` | bytes | `pg_current_wal_lsn()` minus the furthest of the slot's `confirmed_flush_lsn` and the position this run reached |
| `mysql-cdc` | bytes | binlog bytes from the consumed file/position to the head, across files (`SHOW BINARY LOGS`) |
| `mssql-cdc` | events, seconds | change transactions after the slowest capture instance's LSN (`cdc.lsn_time_mapping`) and the age of the oldest |
| `mongodb-cdc` | seconds | the cluster's `operationTime` minus the time of the oldest undelivered change |
| `kafka` | events | each partition's high watermark minus the next offset to read, summed |
| `kinesis` | seconds | `MillisBehindLatest` from `GetRecords`, worst shard |
| `oracle-cdc` | seconds | commit time of the current SCN minus that of the captured `commit_scn` (`SCN_TO_TIMESTAMP`); nothing when the position is older than the SCN-to-time mapping (ORA-08181) |
| `dynamodb` (`mode: streams`) | seconds | now minus the `ApproximateCreationDateTime` of the newest record read from each open shard, worst shard — or, before any, of the oldest unconsumed record at the bookmark (one `Limit: 1` read per open shard); a shard with nothing to read counts as caught up |

The pipeline asks on the first page, at most every 15 s while pages flow, and
once when the run ends; the gauges `faucet_source_lag_bytes`,
`faucet_source_lag_events` and `faucet_source_lag_seconds{pipeline,row,connector}`
carry the latest reading, and the end-of-run value is kept on the row's status
marker. Between scheduled runs a gauge goes stale, so `faucet status --probe`
(and `faucet doctor`) ask the source again from the stored bookmark. A lag
query that fails is logged once and reported as no lag — it never fails a run.
Some need extra grants: `pg_replication_slots` is readable by the replication
role; MySQL needs `REPLICATION CLIENT` for `SHOW BINARY LOGS`; SQL Server needs
`SELECT` on `cdc.lsn_time_mapping`.

## Metrics & alerting

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `faucet_pipeline_sla_violations_total` | counter | `pipeline`, `row`, `kind` | One increment per detected violation; `kind` ∈ `staleness` \| `min_rows` \| `volume` \| `lag`. |
| `faucet_source_lag_bytes` / `_events` / `_seconds` | gauge | `pipeline`, `row`, `connector` | The source's latest lag reading (#733), in the units it reports. |
| `faucet_pipeline_sla_baseline_runs` | gauge | `pipeline`, `row` | Successful runs currently in the rolling volume baseline (cold-start visibility). |

A minimal Prometheus alert:

```yaml
- alert: FaucetSlaViolation
  expr: increase(faucet_pipeline_sla_violations_total[15m]) > 0
  labels: { severity: page }
  annotations:
    summary: "faucet pipeline {{ $labels.pipeline }}/{{ $labels.row }} violated its {{ $labels.kind }} SLA"
```

Every violation is also logged as a `WARN` with `pipeline`, `row`, and `kind`
fields, so log-based alerting works without Prometheus.

## `faucet doctor`

When an `sla:` block is present, `doctor` adds read-only probes per root
invocation:

```text
▸ Invocation row-0  (source=postgres, sink=jsonl)
  ✓ source [postgres] read                          42 ms
  ✓ sink   [jsonl] io                                1 ms
  ✓ state  [file] sentinel                           0 ms
  ✗ sla    [sla] staleness (last success 9341s ago exceeds max_staleness_secs 7200)
        hint: check the pipeline's schedule and recent run failures
  • sla    [sla] baseline (skip: volume baseline warming up: 2/5 successful runs)
```

A source that reports lag also gets a `lag` probe, measured from the stored
bookmark (`✓ source [postgres-cdc] lag` with `hint: behind by 412 MiB`); it
fails when a `max_lag_*` threshold is exceeded.

A stale pipeline makes `doctor` exit non-zero — usable as a standalone
freshness check in CI or a cron health probe, independent of any run.

## Scoping & interactions

- **Root invocations only.** Matrix children fan out per parent record, so
  their volumes are not a stable series to baseline (same scoping as
  `faucet doctor` probes). Each matrix **row** gets its own independent
  history and baseline.
- **`state:` required for staleness/volume** — the history rides whatever
  durability your bookmarks have. `memory` works within a long-running
  `schedule`/`serve` process but resets on restart (faucet warns at load
  time); use `file`/`redis`/`postgres` for one-shot runs.
- **`serve` cluster shard runs are exempt** — a shard's volume is a fraction
  of the row's and shard counts change between runs. Whole-run `serve`
  executions evaluate normally.
- **`--dry-run` / `--limit` skip evaluation** entirely.
- The block is pipeline-level in v1 (no per-matrix-row override, like
  `resilience:`).

Schema: `faucet schema sla`.
