# Cost & usage accounting

Every invocation faucet runs is metered: records read and written, the
estimated serialized bytes on each side, the round trips each connector made
to its backend, and the cost signals a connector learned along the way
(BigQuery's bytes billed, an S3 request count). The record is priced against
a table of rates you control and reported per run, aggregated over time, and
compared to what a per-row-priced hosted ELT service would have charged for
the same rows.

Nothing here is a bill. Every figure faucet prints is an **estimate** whose
inputs are shown next to it, and a connector that reports no cost signal is
listed as *compute not reported* — never as zero.

## What is metered

| Figure | Where it comes from |
|---|---|
| `records_read` / `records_written` | The pipeline's own decorators around every source and sink (post-transform, what the sink accepted). |
| `bytes_read` / `bytes_written` | An estimate of the serialized JSON size of the same records — the same estimate for every connector, so pipelines compare; **not** wire bytes. |
| `source_roundtrips` / `sink_roundtrips` | Calls a connector made to its backend, by op (`page`, `get`, `list`, `put`, `insert`, …) — the same counts as the `faucet_*_roundtrips_total` metrics. |
| `signals` | Backend-reported figures: BigQuery `bytes_processed` / `bytes_billed` / `bytes_streamed` / `bytes_loaded`; S3 and GCS request counts. Each names the connector it came from. |

Accounting is always on; there is nothing to enable. Recording into a store
(so `faucet usage` and `GET /v1/usage` can aggregate across runs) needs a
[`catalog:`](./catalog.md) block — the usage rows ride the same store as the
Data Movement Catalog — or a `faucet serve` instance, which records every run
into its `--history` backend.

## Reading it

`faucet run` prints one line per invocation:

```text
csv_to_jsonl: 1 invocation, 1 ok, 0 failed, wrote 5 records
  default                        usage: 5 in / 5 out, 312 B read / 312 B written, est. USD 0.0000; hosted per-row equivalent USD 0.00
```

`faucet run --output json` carries the full record under each row's `usage`,
and a `faucet serve` run record does the same under each invocation.

`faucet usage` aggregates what the catalog store holds:

```bash
faucet usage --config pipeline.yaml                  # by pipeline, all time
faucet usage --config pipeline.yaml --by day --since 2026-09-01
faucet usage --config pipeline.yaml --by dataset --pipeline orders --json
```

```text
usage by pipeline (since 2026-09-01T00:00:00Z; 42 invocation(s); estimates in USD)
  pipeline    runs      rows out     bytes out    duration    requests     est. cost    hosted eq.
  orders        42     1,204,000     512.3 MiB      118.4s         168        3.1200         18.06
  total         42     1,204,000     512.3 MiB      118.4s         168        3.1200         18.06
```

`--by` groups by `pipeline` (default), `row`, `dataset` (the sink dataset's
catalog id), `sink` (connector kind), `day` or `tenant` (the
[tenant](./embedded-integrations.md) a server run was started for; `--tenant`
keeps one). The same report is
`GET /v1/usage` on a server (`by`, `since`, `until`, `pipeline`, `tenant`, `limit`,
`include_records`; `UsageRead`, viewer and up) and the **Usage** page of the
web console.

## Pricing

Estimates use the `usage:` block's pricing table. Every rate has a shipped
default (public list prices, USD, at the time of writing); set the ones that
apply to your deployment:

```yaml
usage:
  pricing_file: ./pricing.yaml         # optional, merged under the inline table
  pricing:
    currency: EUR
    egress_per_gb: 0.09                # bytes read, when source and sink are not both local files
    object_storage:
      read_per_1k_requests: 0.0004     # S3 / GCS list + get + head
      write_per_1k_requests: 0.005     # put
    warehouse:
      bigquery_per_tib_scanned: 6.25   # bytes billed / processed
      bigquery_streaming_per_gib: 0.05 # bytes streamed
      snowflake_per_credit: 3.0
    hosted_elt_per_million_rows: 15    # the comparison rate
```

`egress_per_gb` defaults to `0` — faucet cannot know which cloud boundary a
pipeline crosses. Set it and the estimate charges every byte read when the
source and sink are not both local files. The `hosted_elt_per_million_rows`
rate is what the `hosted_equivalent` column multiplies rows written by; change
it to the service you compare against. `faucet schema usage` prints the block's
schema. A config submitted to `faucet serve` may set `pricing:` inline but not
`pricing_file` (a submitted config has no filesystem).

## Metrics

| Metric | Meaning |
|---|---|
| `faucet_source_bytes_total{pipeline,row,connector}` / `faucet_sink_bytes_total{…}` | Estimated bytes read / written, per page, live. |
| `faucet_cost_signals_total{pipeline,row,connector,kind,unit}` | Connector-reported cost signals as they arrive. |
| `faucet_usage_estimated_cost_total{pipeline,row,currency}` | Estimated cost of finished invocations, in thousandths of a currency unit. |
| `faucet_usage_hosted_equivalent_total{pipeline,row,currency}` | The hosted per-row equivalent, same unit. |
| `faucet_usage_bytes_total{pipeline,row,direction}` | Estimated bytes of finished invocations (`read` / `written`). |

## Run budgets

A `budget:` block puts hard ceilings on what one invocation may move, so a
run can never exceed what was agreed — the enforcement half of a
[change approval](./approvals.md), and useful on its own:

```yaml
budget:
  max_records: 1000000       # the page that would cross it is refused whole
  max_bytes: 5368709120      # estimated bytes written, same rule
  max_duration_secs: 1800    # cancels cooperatively at the next page boundary
  allowed_sinks: [warehouse, postgres]   # sink template names or connector kinds
```

- **Records and bytes** are checked *before* a page is written. A page that
  would cross the ceiling is refused whole: nothing of it lands and the
  bookmark never advances past it, so a resumed run picks the page up again.
  Refusing rather than truncating is what keeps the bookmark honest.
- **Duration** cancels the run's cooperative token when the deadline passes:
  the pipeline stops at its next page boundary and flushes (an overwrite
  aborts cleanly), and the run fails with `budget_exceeded`.
- **`allowed_sinks`** is checked before anything runs: a row writing to a sink
  the list does not name refuses the whole run.

`faucet run --max-records N --max-bytes B --max-duration-secs S --allowed-sink
X` merges with the config's block — the stricter of each ceiling and the
intersection of the sink lists. A budget applies to every runtime (`run`,
`schedule`, `serve`); a backfill is bounded by its window instead. The
verdict is `FaucetError::BudgetExceeded` (error kind `budget_exceeded`,
`InvocationErrorKind::BudgetExceeded` on the outcome). `faucet schema budget`
prints the block's schema.

Example: [`cli/examples/csv_to_jsonl_with_usage_budget.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/csv_to_jsonl_with_usage_budget.yaml).
