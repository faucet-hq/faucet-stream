# Shipping run logs over OTLP

Recipes for the receiving end of faucet's log shipping (#806). faucet buffers
every run-log line locally first and ships it to an OTLP collector, advancing a
per-run watermark only when the collector acknowledges the batch — see
[Shipping logs](../../docs/book/src/operations/observability.md#shipping-logs).

| File | What it is |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | `faucet serve` → Grafana Alloy → Loki, with Grafana on :3000 and the console's **View logs** link pointed at Explore. |
| [`faucet-otel.yaml`](faucet-otel.yaml) | The `faucet serve --otel-config` file the compose example mounts (`export: [traces, logs]`). |
| [`alloy-loki.alloy`](alloy-loki.alloy) | Grafana Alloy: OTLP in (gRPC 4317 / HTTP 4318) → Loki. |
| [`collector-s3.yaml`](collector-s3.yaml) | OpenTelemetry Collector (contrib 0.130): OTLP → S3, `year=/month=/day=/hour=/minute=` partitions, gzip. |
| [`collector-gcs.yaml`](collector-gcs.yaml) | The same to Google Cloud Storage through its S3-compatible API (HMAC key). |
| [`collector-azure.yaml`](collector-azure.yaml) | OTLP → Azure Blob Storage. |

Every collector file passes `otelcol-contrib validate`, the Alloy file
`alloy fmt`, and the compose file `docker compose config`.

## faucet side

`faucet run` / `faucet schedule` (pipeline config):

```yaml
observability:
  otel:
    endpoint: http://collector:4317
    export: [traces, metrics, logs]
  logs:
    spool_dir: /var/lib/faucet/logs
```

`faucet serve`: put `export: [logs]` in the `--otel-config` file; the
run-history backend (`--history sqlite:…` / `postgres://…`) is the buffer.
`--log-link-template` sets the console's **View logs** link, e.g. a Grafana
Explore query on `serve_run_id`.

Kubernetes: the Helm chart's `otel.logs` values render the serve OTLP config,
the buffer bounds and the link, and mount a spool volume (optionally a PVC)
for `job` / `cronjob` pods.

## Attributes on every record

`service.name` (resource), severity, `target`, the message body, the capture
timestamp, `run_id` (`faucet run` / `schedule`) or `serve_run_id` (`faucet
serve`), `pipeline`, `row`, `connector`, `invocation_id`, `tenant` and `shard`
when set, `faucet.seq` (the line's per-run sequence), and the trace context
(`trace_id` / `span_id`) when traces are exported too. The scope carries
`faucet.batch.first_seq` / `faucet.batch.last_seq`, so a batch re-sent after a
lost acknowledgement is recognisable.
