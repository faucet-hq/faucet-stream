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
| `faucet-pipeline-overview` | Run outcomes + duration percentiles, source/sink throughput and errors by connector, transform in/out, bookmark staleness, effectively-once page skips, state-store traffic. `faucet_build_info` annotates version rollouts. |
| `faucet-reliability` | Retries / give-ups / circuit-breaker state, DLQ traffic, poison rows, quality quarantines, contract violations, schema drift, PII masking activity, SLA violations, backfill progress, source lag, sink batch outcomes, source throttling (rate-limit responses, share of time waiting, retries by class). |
| `faucet-schedule` | Scheduled-run outcomes, heartbeat staleness, next-tick countdown, lateness p95, overlaps, consecutive-failure streak. |
| `faucet-serve` | Control-plane request rate/latency, run queue, terminal statuses, history degradation, idempotency replays, cluster claims/reclaims, trigger health. |

Each dashboard carries a **Data source** picker and a **Pipeline** template
variable, so they import cleanly into any Grafana ≥ 10.

Alert rules (`observability/prometheus/alerts.yml`):

| Alert | Fires when | Severity |
|-------|-----------|----------|
| `FaucetPipelineErrorRateSpike` | >50% of a pipeline's runs fail over 15 m | critical |
| `FaucetNoBookmarkProgress` (+`Critical`) | no durable bookmark progress for 1 h / 6 h | warning / critical |
| `FaucetSlaViolations` | any freshness/volume/lag SLA violation in 1 h | warning |
| `FaucetSourceLagHigh` | a source reports > 1 GiB unread or an oldest unread change > 1 h, for 15 m | warning |
| `FaucetSourceThrottled` | a source spent > 25 % of the last 15 m sleeping on rate limits, for 15 m | warning |
| `FaucetBatchesRoutedWhole` | a sink write was routed whole to the DLQ (`dlq_all`) or failed in the last hour | warning |
| `FaucetCircuitBreakerOpen` | the resilience breaker stays open 5 m | critical |
| `FaucetStuckScheduler` | schedule heartbeat silent for 90 s | critical |
| `FaucetScheduleRunLateness` | tick lateness p95 > 60 s for 15 m | warning |
| `FaucetConsecutiveScheduleFailures` | ≥3 consecutive failed scheduled runs | critical |
| `FaucetServeHistoryDegraded` | serve's history backend degraded 5 m | critical |
| `FaucetOtelExportFailures` / `FaucetLineageEventsDropped` | telemetry/lineage export failing | info |

## Quick start with the example stack

The [examples Docker stack](https://github.com/faucet-hq/faucet-stream/tree/main/examples)
provisions both automatically:

```bash
docker compose -f examples/docker-compose.yml up -d prometheus grafana
```

- Grafana: <http://localhost:3000> (admin / admin) — the four dashboards are
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

`cli/tests/observability_artifacts.rs` extracts every `faucet_*` name the
dashboards and alerts reference (histogram `_bucket`/`_sum`/`_count` suffixes
normalized) and asserts each exists in the source tree. Renaming a metric
without updating the artifacts fails the required `Test` job. Panels group
only by the low-cardinality labels (`pipeline`, `row`, `connector`) — never
add record keys or run ids.

## See also

- [Observability](../operations/observability.md) — the metric and label
  reference these dashboards are built on.
- [SLA monitoring (freshness & volume)](./sla.md) and
  [Notifications (Slack / PagerDuty / webhook)](./notifications.md) — turn
  dashboard signals into alerts.
