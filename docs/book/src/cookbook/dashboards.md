# Dashboards & alerts

faucet ships **ready-made Grafana dashboards** and **Prometheus alert rules**
built on the metrics every pipeline emits automatically — production
observability without hand-building panels. The artifacts live in the repo
under [`observability/`](https://github.com/faucet-hq/faucet-stream/tree/main/observability)
and are kept honest by a CI lint that fails whenever they reference a metric
name that no longer exists in the code.

## What ships

| Dashboard (uid) | Focus |
|-----------------|-------|
| `faucet-pipeline-overview` | Run outcomes + duration percentiles, source/sink throughput and errors by connector, transform in/out, bookmark staleness, effectively-once page skips, state-store traffic. Further rows: pages and writes per second, page / flush / invocation / transform / state-op latency p95, in-flight work, transform errors, state deletes and key migrations, records missing their replication key, data lag when the last bookmark was written, uptime of running pipelines; adaptive batching (batch size, p50 write latency, adjustments, cooldown); topology nodes (tee / merge records, join matches / misses, join build p95 and build / probe counts); peak RSS; WASM transforms (invocations, invocation and compile p95, fuel, linear memory). `faucet_build_info` annotates version rollouts. |
| `faucet-reliability` | Retries, retry sleep p95, give-ups, circuit-breaker state, DLQ traffic (records, pages by reason, write / flush p95, failure-limit hits), poison rows, SLA violations and time since the last successful run, backfill progress, source lag and whether its gauges are stale, sink batch outcomes, source throttling (rate-limit responses, share of time waiting, retries by class). Log shipping: oldest undelivered line, buffered lines and bytes, shipped and dropped lines, export errors, local write failures, OTLP export failures. Lineage events, emit p95 and drops. Notifications sent, dropped and dispatch p95. |
| `faucet-governance` | Quality checks by check and outcome, quarantines and aborts, check duration p95; contract violations and aborts; schema drift; PII fields masked; data-flow policy violations by rule, phase and action; column profiling (drift by column and metric, runs by outcome, baseline depth, columns profiled); rows unaccounted; non-finite values written as null; verify runs, differences, ranges and duration; scoped-cleanup deletes and runs. |
| `faucet-cost` | Estimated cost, the hosted-ELT equivalent and the difference over the selected range; cost against the hosted equivalent per pipeline; budget refusals by budget; usage bytes by direction; source and sink bytes per second; connector cost signals by kind and unit; local-output retention (files recorded, deleted and skipped, bytes reclaimed); source and sink round trips and their p95 by op; rate-limit responses, time waiting and retries on both sides. The `faucet_usage_*` cost counters count thousandths of a currency unit; the panels divide by 1000 and name the currency. |
| `faucet-schedule` | Scheduled-run outcomes, heartbeat staleness, next-tick countdown, lateness p95, overlaps, consecutive-failure streak, runs in flight, last run duration, time since the last run started and completed, config reloads. |
| `faucet-serve` | Control-plane request rate/latency, run queue, terminal statuses, history degradation, idempotency replays, cluster claims/reclaims, trigger health. Further rows: cluster mode and history reachability, pending runs by pool; triggers (runs enqueued and coalesced, missed schedule ticks, tenant fan-out failures, time since the last fire); tenants (runs by outcome, limit rejections), connections by status, connect flows; change requests by kind and outcome, pending changes; template sync pulls and mutations, time since the last sync, template tests, revalidations and label refusals; run-log lines persisted, purged and dropped. |

Each dashboard carries a **Data source** picker and a **Pipeline** template
variable, so they import cleanly into any Grafana ≥ 10.

Alert rules (`observability/prometheus/alerts.yml`):

| Alert | Fires when | Severity |
|-------|-----------|----------|
| `FaucetPipelineErrorRateSpike` | >50% of a pipeline's runs fail over 15 m | critical |
| `FaucetNoBookmarkProgress` (+`Critical`) | a running pipeline's newest bookmark is over 1 h / 6 h old, for 10 m | warning / critical |
| `FaucetSlaViolations` | any freshness/volume/lag SLA violation in 1 h | warning |
| `FaucetSourceLagHigh` | a source reports > 1 GiB unread or an oldest unread change > 1 h, for 15 m | warning |
| `FaucetSourceThrottled` | a source spent > 25 % of the last 15 m sleeping on rate limits, for 15 m | warning |
| `FaucetBatchesRoutedWhole` | a sink write was routed whole to the DLQ (`dlq_all`) or failed in the last hour | warning |
| `FaucetProfileDrift` | a profiled column drifted from its baseline in the last hour | warning |
| `FaucetCircuitBreakerOpen` | the resilience breaker stays open 5 m | critical |
| `FaucetRowsUnaccounted` | a sink lost rows without reporting why in the last hour | critical |
| `FaucetPolicyRuntimeViolations` | the runtime policy backstop refused or quarantined rows in the last hour | warning |
| `FaucetVerifyDifferences` | `faucet verify` found differing keys in the last hour | warning |
| `FaucetBudgetExceeded` | a run hit a `budget:` ceiling in the last hour | warning |
| `FaucetLogsUndelivered` | the oldest buffered run-log line has waited > 15 m, for 5 m | warning |
| `FaucetLogsDropped` | the log buffer dropped lines in the last hour | warning |
| `FaucetStuckScheduler` | schedule heartbeat silent for 90 s | critical |
| `FaucetScheduleRunLateness` | tick lateness p95 > 60 s for 15 m | warning |
| `FaucetConsecutiveScheduleFailures` | ≥3 consecutive failed scheduled runs | critical |
| `FaucetServeHistoryDegraded` | serve's history backend degraded 5 m | critical |
| `FaucetServeHistoryUnreachable` | a clustered server cannot reach its history database, for 2 m | critical |
| `FaucetServePoolStarved` | runs in an execution pool stay unclaimed for 15 m | warning |
| `FaucetServePoolRunsFailed` | runs failed because no replica served their pool, in the last hour | warning |
| `FaucetServeScheduleTicksMissed` | a schedule trigger skipped ticks in the last hour | warning |
| `FaucetServeConnectionsNeedReauth` | a tenant connection needs re-authorization, for 15 m | warning |
| `FaucetServeTemplateSyncFailing` | a template sync origin failed in the last hour | warning |
| `FaucetNotificationsFailing` | notification deliveries failed in the last hour | warning |
| `FaucetOtelExportFailures` / `FaucetLineageEventsDropped` | telemetry/lineage export failing | info |

## Quick start with the example stack

The [examples Docker stack](https://github.com/faucet-hq/faucet-stream/tree/main/examples)
provisions both automatically:

```bash
docker compose -f examples/docker-compose.yml up -d prometheus grafana
```

- Grafana: <http://localhost:3000> (admin / admin) — the six dashboards are
  pre-loaded in the **faucet** folder.
- Prometheus: <http://localhost:9095> — scrapes a faucet process on the host
  and evaluates the alert rules.

Point faucet's exporter at it by enabling Prometheus exposition in your
config (the compose stack scrapes host port `9464`, the default):

```yaml
observability:
  prometheus:
    listen: 0.0.0.0:9464
```

## Importing into your own Grafana / Prometheus

- **Grafana UI:** Dashboards → New → Import → upload a JSON from
  `observability/grafana/`. Pick your Prometheus data source when prompted.
- **Grafana provisioning:** mount `observability/grafana/` and add a file
  dashboard provider (see `examples/infra/grafana/provisioning/`).
- **Prometheus:** copy `observability/prometheus/alerts.yml` next to your
  `prometheus.yml` and list it under `rule_files:`.

## Staying in sync

`cli/tests/observability_artifacts.rs` checks the artifacts against the code
in both directions:

- Every `faucet_*` name the dashboards and alerts reference (histogram
  `_bucket`/`_sum`/`_count` suffixes normalized) must exist in the source
  tree, so renaming a metric without updating the artifacts fails.
- Every metric the code emits must appear on a dashboard, or be listed in
  `observability/metrics-not-charted.txt` as `<metric>: <reason>`. A listed
  metric that a dashboard shows, or that the code no longer emits, also fails.

Both run in the required `Test` job. Panels group only by bounded labels
(`pipeline`, `row`, `connector`, and closed sets such as `op`, `kind` or
`outcome`). The profiling panels group by `column`, which
`profiling.max_columns` bounds. Never add record keys or run ids.

## See also

- [Observability](../operations/observability.md) — the metric and label
  reference these dashboards are built on.
- [SLA monitoring (freshness & volume)](./sla.md) and
  [Notifications (Slack / PagerDuty / webhook)](./notifications.md) — turn
  dashboard signals into alerts.
