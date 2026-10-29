# faucet-stream observability artifacts

Ready-made **Grafana dashboards** and **Prometheus alert rules** for the
metrics faucet emits out of the box (issue #200). A CI lint
(`cli/tests/observability_artifacts.rs`) keeps them in sync with the code in
both directions. It fails when an artifact references a metric name that no
longer exists. It also fails when the code emits a metric that no dashboard
shows and that [`metrics-not-charted.txt`](metrics-not-charted.txt) does not
list with a reason (#1004).

## Dashboards (`grafana/`)

| Dashboard | uid | Shows |
|-----------|-----|-------|
| `faucet-pipeline-overview.json` | `faucet-pipeline-overview` | Runs by status, run-duration p50/p95, source/sink throughput + errors by connector, transform in/out, bookmark staleness, effectively-once page skips, state-store ops; pages and writes per second, page / flush / invocation / transform / state-op p95, in-flight work, replication-key misses, newest-data age, uptime; adaptive batching; topology tee / merge / join nodes; peak RSS; WASM transforms. A `faucet_build_info` annotation marks version rollouts. |
| `faucet-reliability.json` | `faucet-reliability` | Resilience retries / sleep / give-ups / circuit state, DLQ traffic (pages by reason, write / flush p95, failure-limit hits), poison rows, SLA violations + time since last success, backfill progress, source lag + staleness, sink batch outcomes, source throttling; log shipping (backlog age, buffer, shipped / dropped, export and local write failures, OTLP failures); lineage events / emit p95 / drops; notifications sent / dropped / dispatch p95. |
| `faucet-governance.json` | `faucet-governance` | Quality checks, quarantines and aborts, contract violations, schema drift, PII masking, policy violations, column-profile drift / runs / baseline, rows unaccounted, non-finite values, verify runs / differences / ranges / duration, scoped-cleanup deletes and runs. |
| `faucet-cost.json` | `faucet-cost` | Estimated cost vs the hosted-ELT equivalent (counters are thousandths of a currency unit, divided by 1000), budget refusals, usage bytes, source / sink bytes, cost signals, local-output retention, backend round trips and p95 by op, rate limits and retries on both sides. |
| `faucet-schedule.json` | `faucet-schedule` | Scheduled runs by outcome, heartbeat staleness (alert at 90 s), next-tick countdown, lateness p95, overlap policy hits, consecutive-failure streak, time since the last run started / completed, config reloads. |
| `faucet-serve.json` | `faucet-serve` | HTTP request rate/latency by path, run queue depth, terminal run statuses, history-degraded flag, idempotency replays, cluster membership + claim/reclaim rates, trigger health/fires/drops; pending runs by pool, trigger enqueues / coalescing / missed ticks / tenant failures, tenant runs and limit rejections, connections and connect flows, change requests, template sync / tests / revalidations, run-log persistence. |

Every dashboard has a **Data source** picker and a **Pipeline** template
variable — import them anywhere.

**Import (UI):** Grafana → Dashboards → New → Import → upload the JSON.

**Import (provisioning):** mount this directory and point a dashboard
provider at it — the [example Docker stack](../examples/README.md) does
exactly that (`examples/infra/grafana/`).

## Alert rules (`prometheus/alerts.yml`)

| Alert | Condition | Severity |
|-------|-----------|----------|
| `FaucetPipelineErrorRateSpike` | >50% of runs failing over 15 m | critical |
| `FaucetNoBookmarkProgress` / `…Critical` | bookmark stale >1 h / >6 h | warning / critical |
| `FaucetSlaViolations` | any `sla:` violation in the last hour | warning |
| `FaucetSourceLagHigh` | source >1 GiB or >1 h behind, for 15 m | warning |
| `FaucetSourceThrottled` | source rate-limited >25% of 15 m, for 15 m | warning |
| `FaucetBatchesRoutedWhole` | a sink write routed whole to the DLQ or failed in 1 h | warning |
| `FaucetProfileDrift` | a profiled column drifted in the last hour | warning |
| `FaucetCircuitBreakerOpen` | resilience breaker open ≥5 m | critical |
| `FaucetRowsUnaccounted` | a sink lost rows without reporting why in 1 h | critical |
| `FaucetPolicyRuntimeViolations` | runtime policy backstop refused/quarantined rows in 1 h | warning |
| `FaucetVerifyDifferences` | `faucet verify` found differing keys in 1 h | warning |
| `FaucetBudgetExceeded` | a run hit a `budget:` ceiling in 1 h | warning |
| `FaucetLogsUndelivered` | oldest buffered log line >15 m, for 5 m | warning |
| `FaucetLogsDropped` | log buffer dropped lines in 1 h | warning |
| `FaucetStuckScheduler` | schedule heartbeat silent >90 s | critical |
| `FaucetScheduleRunLateness` | tick lateness p95 >60 s for 15 m | warning |
| `FaucetConsecutiveScheduleFailures` | ≥3 consecutive failed runs | critical |
| `FaucetServeHistoryDegraded` | run-history backend degraded ≥5 m | critical |
| `FaucetServeHistoryUnreachable` | clustered server cannot reach its history DB ≥2 m | critical |
| `FaucetServePoolStarved` | a pool's runs unclaimed for 15 m | warning |
| `FaucetServePoolRunsFailed` | runs failed with `pool_unavailable` in 1 h | warning |
| `FaucetServeScheduleTicksMissed` | a schedule trigger skipped ticks in 1 h | warning |
| `FaucetServeConnectionsNeedReauth` | a tenant connection needs re-authorization ≥15 m | warning |
| `FaucetServeTemplateSyncFailing` | a template sync origin failed in 1 h | warning |
| `FaucetNotificationsFailing` | notification deliveries failed in 1 h | warning |
| `FaucetOtelExportFailures` | OTLP exports failing | info |
| `FaucetLineageEventsDropped` | lineage events dropped | info |

Load them from `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/faucet-alerts.yml
```

## Wiring faucet to Prometheus

Enable the exporter in your pipeline config (default listen `127.0.0.1:9464`):

```yaml
observability:
  prometheus:
    listen: 0.0.0.0:9464
```

Labels are low-cardinality by design (`pipeline`, `row`, `connector`, and
closed sets such as `op` or `outcome`). Never extend panels with record keys or
run ids. The profiling panels group by `column`, which `profiling.max_columns`
bounds.
