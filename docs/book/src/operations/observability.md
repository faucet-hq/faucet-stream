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
- **Incremental key coverage** (#747): `faucet_source_replication_key_missing_total{pipeline,row,connector}`
  — records an incremental source received without its `replication_key` (kept,
  dropped or failed per `on_missing_key`). A non-zero rate usually means a
  misspelled or wrongly nested key.
- **Source lag** (#733): `faucet_source_lag_bytes`, `faucet_source_lag_events`,
  `faucet_source_lag_seconds{pipeline,row,connector}` — how far a CDC or
  streaming source is behind its head (unread WAL / binlog, unconsumed
  messages, age of the oldest unread change), polled at page boundaries and at
  the end of a run. Sources without a head (query sources) emit nothing. See
  [Source lag](../cookbook/sla.md#source-lag).
- **Sink:** `faucet_sink_records_total`, `faucet_sink_writes_total`,
  `faucet_sink_errors_total`, `faucet_sink_write_duration_seconds`,
  `faucet_sink_flush_duration_seconds`, `faucet_sink_in_flight`.
- **Batch outcomes** (#737): `faucet_batch_outcomes_total{pipeline,row,sink,outcome}`
  — one increment per sink write, `outcome` ∈ `committed` \| `dlq_partial` \|
  `dlq_all` \| `failed`; their sum is the writes attempted. See
  [batch atomicity](../cookbook/dlq.md#batch-atomicity-and-dlq_all).
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
  other metrics to annotate dashboards with the running version. `version` is
  the binary's own version (what `faucet --version` prints); a library
  application sets its own with `faucet_core::set_build_version`, else the
  label is `faucet-core`'s version.

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
  tokio runtime, so gRPC works without any extra setup. `headers` are sent as
  gRPC metadata (names are lower-cased); an invalid header name or value
  disables export with a warning.
- `http` uses HTTP/Protobuf. When `endpoint` does not already end in a
  per-signal path (`/v1/traces`, `/v1/metrics`), faucet appends it
  automatically — point `endpoint` at the base URL of the collector (e.g.
  `http://localhost:4318`) and the right path is added per signal.

Traces are exported whenever `export` lists `traces`, with or without an
`observability.tracing` level, by `run`, `schedule`, `replicate`/`mirror` and
the other commands that load a config. `faucet serve` takes no config, so it
reads the same block from its own file: `faucet serve --otel-config otel.yaml`
(or `FAUCET_SERVE_OTEL_CONFIG`), a YAML/JSON document shaped like
`observability.otel` (`${env:…}` / `${file:…}` resolved, header values kept out
of logs). Traces cover every run the server executes; with `export: [metrics]`
the metrics `/metrics` serves are also pushed over OTLP.

```yaml
# otel.yaml
endpoint: http://otel-collector:4318
protocol: http
export: [traces, metrics]
service_name: faucet-serve
headers: { authorization: "Bearer ${env:OTLP_TOKEN}" }
```

**Reliability:** export is best-effort. An unreachable or slow collector
**never** fails or delays a pipeline run. Export failures increment
`faucet_otel_export_failures_total{signal}` so you can alert on a broken
pipeline to your observability backend.

See `examples/infra/otel-collector.yaml` for a minimal local collector config
you can run with `otelcol --config examples/infra/otel-collector.yaml`.

### OTLP metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `faucet_otel_export_failures_total` | `signal` (`traces`/`metrics`/`logs`/`export`) | OTLP export attempts that failed. Failures are non-fatal; the pipeline continues. |

## Shipping logs

Add `logs` to `export` and faucet ships every run's log lines to the OTLP
collector — and never loses one because the collector, the network or the faucet
process went down. Every line is **written locally first**; a background
shipper delivers it and advances a per-run delivery watermark only after the
collector acknowledges the batch. Local copies are kept until delivered
(bounded), then for a short window.

```yaml
observability:
  otel:
    endpoint: http://collector:4317
    export: [traces, metrics, logs]     # `logs` turns shipping on
  logs:                                 # optional — the defaults are shown
    spool_dir: ~/.local/state/faucet/logs   # faucet run / schedule only
    retention_secs: 86400               # keep delivered lines 24 h
    buffer_max_age_secs: 604800         # drop undelivered lines after 7 days
    buffer_max_bytes: 1073741824        # …or once the buffer passes 1 GiB
    max_lines_per_run: 100000
    flush_timeout_secs: 10              # `faucet run` waits this long at exit
    notify_after_secs: 300              # `log_export_failed` after 5 min failing
    link_template: "https://grafana.example/explore?...{run_id}..."
```

### Where the buffer lives

| Runtime | Buffer | Shipper |
|---|---|---|
| `faucet serve` | The run-history log store (`--history sqlite:…` / `postgres://…`). With `--history memory` it is an in-memory, non-durable buffer — the server warns at startup. | A background task. In `--cluster`, each run's delivery is leased (`faucet_serve_log_ship`), so one instance ships it; a peer resumes it once the lease lapses. |
| `faucet run` | A spool directory: `<run_id>.jsonl` (append-only, fsync'd every 1024 lines and at run end), `<run_id>.cursor.json` (the watermark, written temp + rename), `<run_id>.meta.json`, and an advisory `<run_id>.lock`. | Ships in the background while the run goes, then for at most `flush_timeout_secs` at exit. Anything left is shipped by the next faucet process using the spool, or by `faucet logs ship`. |
| `faucet schedule` | The same spool; every tick is a run. | A background task for the process lifetime, draining between ticks. |

`faucet serve` reads its settings from flags (each also an env var):
`--log-retention-secs` (default 86400), `--log-buffer-max-age-secs`,
`--log-buffer-max-bytes`, `--log-link-template`,
`--log-export-notify-after-secs`, with `export: [logs]` in `--otel-config`.

### Delivery guarantees

- **At least once per batch.** A batch whose acknowledgement is lost (or whose
  watermark write fails) is sent again; every record carries `faucet.seq` and
  the batch scope carries `faucet.batch.first_seq` / `faucet.batch.last_seq`, so
  a backend can drop the duplicate. An acknowledged line is never re-sent.
- **The pipeline is never slowed.** Capture hands each line to a bounded queue;
  a full queue drops the line (counted as `queue_full`) instead of blocking, and
  a local-write failure is reported on stderr and counted — never a run failure.
- **Redacted at capture.** Every resolved secret is scrubbed before the line is
  written locally, so neither the buffer nor the export ever holds one. Lines
  longer than 16 KiB are cut and marked.
- **Two processes, one spool.** A run's `.lock` makes sure only one process
  ships it; a run whose writer died is picked up by the next shipper.
- **Bounds drop the oldest, visibly.** Past `buffer_max_age_secs` /
  `buffer_max_bytes` (or the per-run line cap) the oldest undelivered lines are
  dropped, counted in `faucet_logs_dropped_lines_total{reason}`, and the run is
  reported `partially_dropped`.

### Was a run's log shipped?

`GET /v1/runs/{id}` carries `log_export`:

```json
{ "status": "failed", "delivered_seq": 7338097199674630147, "pending_lines": 4,
  "dropped_lines": 0, "last_error": "OTLP HTTP export … failed",
  "last_attempt_at": "2026-10-09T06:34:13Z",
  "link": "https://grafana.example/explore?q=01a11f5e-…" }
```

`status` is one of `not_configured`, `pending`, `exported`, `failed`,
`partially_dropped`. The console's run page shows it as a **Log export** panel,
with **View logs ↗** built from the link template; once a run's local copy has
aged out, the log endpoint returns that link (and the SSE stream opens with a
`link` event) instead of nothing. `faucet run` prints `logs: exported` or
`logs: 1,204 lines pending — run "faucet logs ship"` and adds `log_export` to
`--output json`.

`faucet logs ship [CONFIG] [--spool DIR] [--endpoint URL] [--protocol grpc|http]`
drains a spool once (a cron job or sidecar); its exit code is the number of runs
still undelivered.

A `log_export_failed` notification goes through the run's `notifications:`
when its export has been failing for `notify_after_secs`, and when lines were
dropped.

### Log-shipping metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `faucet_logs_buffered_lines` | — | Lines buffered and not yet acknowledged. |
| `faucet_logs_buffered_bytes` | — | Bytes of those lines. |
| `faucet_logs_oldest_undelivered_seconds` | — | Age of the oldest undelivered line. |
| `faucet_logs_shipped_lines_total` | — | Lines the collector acknowledged. |
| `faucet_logs_dropped_lines_total` | `reason` (`max_age`/`max_bytes`/`max_lines`/`queue_full`) | Lines dropped before delivery. |
| `faucet_logs_export_errors_total` | — | Failed export attempts (also `faucet_otel_export_failures_total{signal="logs"}`). |
| `faucet_logs_local_write_failures_total` | — | Lines that could not be written to the local buffer. |

`observability/prometheus/alerts.yml` has `FaucetLogsUndelivered` (oldest
undelivered line > 15 min) and `FaucetLogsDropped`. Receiving-end recipes —
Alloy → Loki, the OpenTelemetry Collector → S3 / GCS / Azure, a Docker Compose
example — are in [`deploy/otel/`](https://github.com/faucet-hq/faucet-stream/tree/main/deploy/otel).
`--log-format json` stays the option for shippers that read stdout.
