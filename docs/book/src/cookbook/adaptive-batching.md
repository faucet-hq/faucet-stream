# Adaptive batch sizing

Adaptive batch sizing lets faucet automatically tune how many records it sends to
the sink in each write, instead of using a fixed `batch_size`. The built-in
**AIMD controller** (Additive Increase / Multiplicative Decrease) starts at the
source page size, grows the batch additively when writes are clean and fast, and
shrinks it multiplicatively when errors appear or write latency rises above a
target.

## When to use it

Useful when the optimal write batch size changes over time or varies by data
shape:

- **Spiky data volumes** — smaller batches during large-row bursts; bigger ones
  for narrow rows.
- **Sink rate limits / quotas** — back off automatically when the API starts
  returning errors or timing out.
- **Latency-sensitive pipelines** — keep each write inside a target window
  (e.g. `target_latency_ms: 1000`) rather than guessing a fixed size.

Adaptive batch sizing is pure write-side tuning: the source page size is
unchanged, and the controller simply reslices each page into sub-batches of the
current effective size.

## Configuration

Add an `execution.adaptive_batch_size:` block to your config file:

```yaml
execution:
  adaptive_batch_size:
    enabled: true
    min: 500
    max: 10000
    increase_step: 500
    decrease_factor: 0.5
    cooldown_batches: 5
    target_latency_ms: 1000
    latency_window: 10
    error_threshold: 0.01
    respect_source_max: true
    log_every: 50
```

### Full example

The [`postgres_to_bigquery_adaptive.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_to_bigquery_adaptive.yaml)
example pairs a PostgreSQL source with a BigQuery sink and a JSONL DLQ:

```yaml
# postgres_to_bigquery_adaptive.yaml  (abbreviated)
version: 1
name: postgres_to_bigquery_adaptive

pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      query: SELECT id, created_at, payload FROM orders WHERE created_at > $1
      params: ["2026-01-01T00:00:00Z"]
      batch_size: 5000   # source page size — also the effective upper ceiling
      max_connections: 8

  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: warehouse
      table_id: orders
      auth:
        type: service_account_key
        config:
          json: ${env:GCP_KEY_JSON}
      batch_size: 0      # one insertAll per write — the controller picks the size,
                         # and a failed write lands nothing (dlq_all needs that)

  dlq:
    sink:
      type: jsonl
      config:
        path: ./dlq/orders_failed.jsonl
    on_batch_error: dlq_all

execution:
  adaptive_batch_size:
    enabled: true
    min: 500
    max: 10000
    increase_step: 500
    decrease_factor: 0.5
    cooldown_batches: 5
    target_latency_ms: 1000
    error_threshold: 0.01
```

## Config field reference

All fields are optional except `enabled`. Unset fields take the defaults shown
below.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch. Set to `true` to activate the controller. |
| `controller` | string | `"aimd"` | Algorithm. Only `"aimd"` is supported in v1. |
| `min` | integer | `100` | Lower bound on effective batch size. Must be ≥ 1. |
| `max` | integer | `50000` | Upper bound. Must be ≤ 1,000,000. Values above the source page size are inert (see Caveats). |
| `increase_step` | integer | `250` | Rows added per clean, fast batch (additive growth). Must be ≥ 1 and ≤ 1,000,000. |
| `decrease_factor` | float | `0.5` | Multiplicative shrink factor on error or high latency. Must be in (0, 1). |
| `cooldown_batches` | integer | `5` | Batches to skip after a shrink before allowing growth again. |
| `target_latency_ms` | integer \| null | `null` | Optional target write latency (ms). `null` means react to errors only. |
| `latency_window` | integer | `10` | Rolling window size (batches) for the p50 latency estimate. Must be ≥ 1. |
| `error_threshold` | float | `0.01` | Per-batch error rate (0.0–1.0) above which the controller shrinks. |
| `respect_source_max` | bool | `true` | Cap effective batch size at the source page size. Must be `true`; `false` is rejected (cross-page buffering would break the O(batch_size) memory guarantee). |
| `log_every` | integer | `50` | Emit a `tracing::info` summary every N adjustments (0 = never). |

## AIMD behavior

The controller follows a strict priority order for each sub-batch observation:

1. **Error shrink (always fires, even during cooldown)** — if the per-batch error
   rate exceeds `error_threshold`, the current size is multiplied by
   `decrease_factor` (floor-rounded, clamped to `min`), and `cooldown_batches`
   is armed.
2. **Cooldown gate** — if cooldown is active, decrement the counter and skip
   growth. A new error during cooldown fires rule 1 again and re-arms the counter.
3. **Latency target (when `target_latency_ms` is set)** — evaluate the rolling
   p50 latency:
   - p50 > 1.2 × `target_latency_ms` → shrink.
   - p50 < 0.5 × `target_latency_ms` → grow.
   - Otherwise, stay (dead-band prevents oscillation).
4. **Success growth** — add `increase_step` to the current size (clamped to
   `max`).

### Cold start

The controller initialises to the first source page length, clamped into
`[min, max]`. If the first page is smaller than `min`, the effective size starts
at `min`.

### Example trajectory

With `min=500`, `max=5000`, `increase_step=500`, `decrease_factor=0.5`,
`cooldown_batches=2`:

```
batch 1: size=1000, ok, fast  → grow  → 1500
batch 2: size=1500, ok, fast  → grow  → 2000
batch 3: size=2000, 3% errors → shrink→ 1000, cooldown armed (2)
batch 4: size=1000, cooldown  → skip  → 1000
batch 5: size=1000, cooldown  → skip  → 1000
batch 6: size=1000, ok, fast  → grow  → 1500
```

## Metrics

Four per-pipeline-row gauges / counters are emitted automatically:

| Metric | Type | Description |
|--------|------|-------------|
| `faucet_pipeline_adaptive_batch_size` | gauge | Current effective batch size. |
| `faucet_pipeline_adaptive_batch_adjustments_total` | counter | Total adjustments, labeled `direction=up\|down` and `reason=success\|error\|latency`. |
| `faucet_pipeline_adaptive_batch_cooldown_active` | gauge | `1` while cooldown is active, `0` otherwise. |
| `faucet_pipeline_adaptive_batch_p50_latency_ms` | gauge | Rolling p50 write latency (ms); absent until the window fills. |

All four carry the standard `pipeline` and `row` labels.

Example PromQL to alert when the controller is stuck shrinking:

```promql
# Shrink rate over the last 5 minutes
rate(faucet_pipeline_adaptive_batch_adjustments_total{direction="down"}[5m])
  > 0.5
```

## Caveats

### Error-driven shrink requires a DLQ

The error signal comes from per-row outcomes reported via the DLQ path
(`Sink::write_batch_partial`). If no `dlq:` block is present, the controller
sees zero errors regardless of the sink response — only `target_latency_ms`
can drive shrinks. Add a `dlq:` block with `on_batch_error: dlq_all` if you
want the controller to react to sink-side write errors — on a sink whose failed
write is all-or-nothing (see [batch atomicity](dlq.md#batch-atomicity-and-dlq_all)),
which for BigQuery means `batch_size: 0`.

### Within-page ceiling: `max` is capped at the source page size

In v1 the controller reslices pages it already received from the source — it
cannot buffer records across pages. The effective upper bound is therefore
`min(max, source_page_size)`. If you set `max: 50000` but the source emits
pages of 1 000 records, the controller will never write more than 1 000 rows
per call.

**To allow bigger write batches, raise the source's `batch_size`** (e.g.
`batch_size: 20000` on the postgres source config). Setting `max` higher than
the source page size is harmless but inert.

`respect_source_max: false` to cross page boundaries is **rejected** at config
load: cross-page buffering would have to hold records across source pages, which
breaks the pipeline's O(batch_size) memory guarantee. Raise the source
`batch_size` instead.

### No-op for per-record sinks

`jsonl`, `csv`, and `stdout` write one record at a time regardless of
`batch_size`. Adaptive sizing is active but harmless for these sinks — the
controller adjusts its internal state normally, but the actual write granularity
is unchanged. A one-time `tracing::info` message notes this when the pipeline
starts.
