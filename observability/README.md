# faucet-stream observability artifacts

Ready-made **Grafana dashboards** and **Prometheus alert rules** for the
metrics faucet emits out of the box (issue #200). A CI lint
(`cli/tests/observability_artifacts.rs`) fails whenever these artifacts
reference a metric name that no longer exists in the codebase, so they stay
in sync with the code.

## Dashboards (`grafana/`)

| Dashboard | uid | Shows |
|-----------|-----|-------|
| `faucet-pipeline-overview.json` | `faucet-pipeline-overview` | Runs by status, run-duration p50/p95, source/sink throughput + errors by connector, transform in/out, bookmark staleness, effectively-once page skips, state-store ops. A `faucet_build_info` annotation marks version rollouts. |
| `faucet-reliability.json` | `faucet-reliability` | Resilience retries / give-ups / circuit state, DLQ traffic, poison rows, quality quarantines + aborts, contract violations, schema drift, PII masking activity, SLA violations, backfill progress, source throttling (rate-limit responses, share of time waiting, retries by class). |
| `faucet-schedule.json` | `faucet-schedule` | Scheduled runs by outcome, heartbeat staleness (alert at 90 s), next-tick countdown, lateness p95, overlap policy hits, consecutive-failure streak. |
| `faucet-serve.json` | `faucet-serve` | HTTP request rate/latency by path, run queue depth, terminal run statuses, history-degraded flag, idempotency replays, cluster membership + claim/reclaim rates, trigger health/fires/drops. |

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
| `FaucetSourceThrottled` | source rate-limited >25% of 15 m, for 15 m | warning |
| `FaucetCircuitBreakerOpen` | resilience breaker open ≥5 m | critical |
| `FaucetStuckScheduler` | schedule heartbeat silent >90 s | critical |
| `FaucetScheduleRunLateness` | tick lateness p95 >60 s for 15 m | warning |
| `FaucetConsecutiveScheduleFailures` | ≥3 consecutive failed runs | critical |
| `FaucetServeHistoryDegraded` | run-history backend degraded ≥5 m | critical |
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

Labels are low-cardinality by design (`pipeline`, `row`, `connector`) — never
extend panels with record keys or run ids.
