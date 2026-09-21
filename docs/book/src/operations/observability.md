# Observability

Every source, sink, transform, and state-store operation is automatically wrapped
to emit `tracing` spans and `metrics` counters/histograms. Connector authors
write no observability code — they only override `connector_name()` for a
friendly label.

## Enabling the Prometheus endpoint

The CLI's `observability` feature (on by default in the full build) installs a
Prometheus exporter. Configure it from the pipeline config or environment; once
running, scrape the listen address with Prometheus.

## Common labels

`pipeline`, `row` (matrix row id; empty for non-matrix runs), and `connector`
(from `connector_name()`). `run_id` is a span attribute only — it's high
cardinality and never a Prometheus label.

## Key metrics

- **Source:** `faucet_source_records_total`, `faucet_source_errors_total{kind}`,
  `faucet_source_page_duration_seconds`, `faucet_source_in_flight`.
- **Sink:** `faucet_sink_records_total`, `faucet_sink_writes_total`,
  `faucet_sink_errors_total`, `faucet_sink_write_duration_seconds`,
  `faucet_sink_flush_duration_seconds`, `faucet_sink_in_flight`.
- **Upstream round trips** (#638): `faucet_source_roundtrips_total{op}` and
  `faucet_sink_roundtrips_total{op}` — how many calls a connector actually made
  to its backend, with companion `*_roundtrip_duration_seconds{op}` histograms.
  This is the number that maps onto an API quota, S3 GET cost, or database
  load; `faucet_source_pages_total` only proxies the data fetches for paged
  HTTP sources and misses job submits, poll loops, and every non-HTTP
  connector. `op` is a **closed, connector-defined** set — the REST source
  emits `submit` / `poll` / `fetch` / `page` / `discover`. Retries count: a
  retried call is a real round trip. A connector that has not been instrumented
  emits nothing.
- **Transform:** `faucet_transform_records_in_total`,
  `faucet_transform_records_out_total` (use the `out/in` ratio for
  filter drop rate or explode fan-out), `faucet_transform_errors_total{kind}`,
  `faucet_transform_duration_seconds`.
- **State:** `faucet_state_{get,put,delete}_total` (get carries
  `outcome=hit|miss`), `faucet_state_errors_total{op,kind}`, plus duration
  histograms.
- **Pipeline:** `faucet_pipeline_runs_total{status=ok|err,kind}`,
  `faucet_pipeline_run_duration_seconds`, `faucet_pipeline_in_flight`,
  `faucet_pipeline_seconds_since_last_bookmark`,
  `faucet_pipeline_last_bookmark_unix_seconds`.
- **Local outputs** (`catalog` feature): the retention GC for local sink output
  files — `faucet_local_outputs_recorded_total{kind}`,
  `faucet_local_outputs_sweeps_total{scope}`,
  `faucet_local_outputs_deleted_total{scope}`,
  `faucet_local_outputs_bytes_deleted_total{scope}`, and
  `faucet_local_outputs_skipped_total{scope,reason}`. Deleted/bytes are emitted
  even at zero — a sweep that found nothing is the healthy steady state, and its
  *absence* is how you notice the sweeper stopped. A rising
  `skipped{reason="delete_failed"}` means the footprint is **not** being bounded
  and is worth alerting on; `reason="pre_existing"` is benign (files faucet did
  not create are never deleted). Distinct from `faucet_cleanup_*`, which counts
  destination *rows* removed by [scoped cleanup](../cookbook/upsert.md).
- **Build:** `faucet_build_info{version}` is set to `1` — `group_left` it onto
  other metrics to annotate dashboards with the running version.

## Reliability properties

- **Drop-guard timers** sample durations even when a task is cancelled.
- **Panic isolation** — a panicking connector surfaces as a `Panic` error kind
  rather than crashing the process.
- **Idempotent install** — installing the recorder/subscriber twice warns rather
  than panics.

## Cardinality rules

Never use high-cardinality values (record ids, URLs, query strings) as metric
labels. `parent_record_key` in a DAG is a span attribute only. Connector authors
must return a non-empty `&'static str` from `connector_name()`.

## Structured (JSON) logs

`--log-format json` (or `FAUCET_LOG_FORMAT=json`) renders every log record as
one JSON object per line on stderr, so a k8s / ECS / Nomad log pipeline can
ingest it without grok or regex:

```console
$ faucet run --log-format json pipeline.yaml
{"timestamp":"2026-09-19T10:02:11.481Z","level":"INFO","target":"faucet_cli::executor","pipeline":"orders","row":"contact","records_written":4821,"message":"row completed"}
```

The span fields faucet already records — `pipeline`, `row`, `run_id`,
`connector`, error `kind` — arrive as fields rather than being rendered into
the message, so they are filterable at the collector.

Two things follow from "every line is one object":

- Under `json` the end-of-run human status block, per-row timing table, and
  peak-RSS line are not printed; the same numbers leave as structured events
  (`pipeline completed`, `row completed`, `process peak rss`).
- `faucet mcp` keeps logs on stderr under either format, because stdout carries
  the JSON-RPC stream and two JSON streams on one pipe would corrupt it.

Secret redaction is unaffected: it operates on the serialized bytes, so a
resolved `${vault:…}` value appearing in a field is still scrubbed.

`text` remains the default.

## Tracing

Spans carry `run_id`, `pipeline`, `row`, and per-operation timing. Point a
`tracing` subscriber at your logging/trace backend; control verbosity with
`--log-level` or `FAUCET_LOG`.

> Full design: `docs/superpowers/specs/2026-05-23-observability-otel-prometheus-design.md`.

## OTLP / OpenTelemetry export

The `otel` feature pushes traces **and** metrics to any OTLP-compatible
collector (Jaeger, Grafana Tempo, Honeycomb, Datadog, the OpenTelemetry
Collector, etc.) alongside — not instead of — the Prometheus endpoint. Build
the CLI with `cargo install faucet-cli --features otel`; the feature is
included in the `full` aggregate. Enable it in your pipeline config with an
`otel:` sub-block under the existing `observability:` key:

```yaml
observability:
  prometheus:
    listen: "0.0.0.0:9090"
  otel:
    endpoint: "https://api.honeycomb.io"
    protocol: grpc                        # grpc (default) | http
    headers:
      x-honeycomb-team: "${env:HONEYCOMB_KEY}"
    sample_ratio: 0.1                     # head-based; 1.0 = keep all traces
    export: [traces, metrics]             # which signals to push
    service_name: faucet                  # OTel resource service.name
    timeout_secs: 10
    metric_interval_secs: 60
```

The `observability.prometheus:` and `observability.otel:` blocks coexist
independently — both can be active in the same run and metrics are fanned out
to both exporters.

**Protocol notes:**

- `grpc` uses `tonic` (the default). The `faucet` CLI always runs inside a
  tokio runtime, so gRPC works without any extra setup.
- `http` uses HTTP/Protobuf. When `endpoint` does not already end in a
  per-signal path (`/v1/traces`, `/v1/metrics`), faucet appends it
  automatically — point `endpoint` at the base URL of the collector (e.g.
  `http://localhost:4318`) and the right path is added per signal.

**Reliability:** export is best-effort. An unreachable or slow collector
**never** fails or delays a pipeline run. Export failures increment
`faucet_otel_export_failures_total{signal}` so you can alert on a broken
pipeline to your observability backend.

See `examples/infra/otel-collector.yaml` for a minimal local collector config
you can run with `otelcol --config examples/infra/otel-collector.yaml`.

### OTLP metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `faucet_otel_export_failures_total` | `signal` (`traces`/`metrics`/`export`) | OTLP export attempts that failed. Failures are non-fatal; the pipeline continues. |
