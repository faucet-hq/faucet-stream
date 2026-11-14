# Logging & Metrics Standard

*Spans and metrics are emitted automatically by the pipeline; this standard keeps them low-cardinality, consistently labelled, and safe to expose.*

Connectors do **not** write their own metrics code. The pipeline-internal decorators in `crates/core/src/observability/` wrap every source, sink, transform, and state-store call, so instrumentation is uniform whether the connector is built-in or third-party. This standard is the set of rules that keep that surface healthy. See [Observability](../architecture/observability.md) for the mechanism.

## Metric naming & labels

- **Metric names MUST use the `faucet_` prefix** and standard suffix conventions (`_total` counters, `_seconds` histograms, `_unix_seconds` gauges).
- **The common label set is `pipeline`, `row`, `connector`.** `row` is the matrix-row id (`""` for non-matrix runs); `connector` comes from `connector_name()`.
- **`run_id` MUST be a span attribute only, never a metric label.** It is unbounded per run and would blow up cardinality.

  ```rust
  // GOOD — run_id on the span, bounded labels on the metric.
  let span = info_span!("faucet.pipeline.run", pipeline=%name, row=%row, run_id=%run_id);
  counter!("faucet_source_records_total", labels).increment(n);

  // BAD — unbounded label; one series per run forever.
  counter!("faucet_source_records_total", "run_id" => run_id).increment(n);
  ```

## Cardinality rules

- **MUST NOT use a high-cardinality value as a metric label** — record IDs, URLs, query strings, offsets, cursor tokens, primary keys. These belong in span attributes or structured log fields, never in the Prometheus label set.
- **`parent_record_key` (in a parent/child matrix) MUST be a span attribute only**, never a metric label.
- **`connector_name()` MUST return a non-empty `&'static str`.** An empty string falls back to `"unknown"` in release builds and trips a `debug_assert!` in debug. Built-in connectors override the default with a friendly label (`"rest"`, `"jsonl"`, …).

## Spans

- **SHOULD attach identifying context to the span, not the message.** The pipeline opens `faucet.pipeline.run` with `pipeline`, `row`, `run_id`, `source`, `sink`; subordinate spans inherit it. Emit structured fields (`records = n`) rather than interpolating them into a free-text message.
- **Long-running (streaming/CDC) pipelines** rely on gauge heartbeats (`faucet_pipeline_start_time_unix_seconds`, `faucet_pipeline_seconds_since_last_bookmark`) rather than the run-duration histogram, which never fires until the run ends. Preserve those gauges when touching the run loop.

## Secret redaction boundary

- **The redaction boundary covers faucet's own tracing/log/error output only** — resolved `${vault:…}` / `${aws-sm:…}` values are scrubbed at the I/O boundary by the CLI `RedactingWriter` (`cli/src/secrets/registry.rs`).
- **MUST NOT assume redaction covers third-party output.** A connector's driver debug logging, Prometheus label values, and span attributes are **outside** the boundary. Never place a resolved secret where a driver might log it, and never enable `FAUCET_LOG=debug` on a pipeline whose connector configs hold resolved secrets.
- **MUST register any new operator-supplied token/secret for redaction** when adding one (e.g. serve auth tokens are registered so they never appear in run logs).

## Failure isolation

- **Instrumentation MUST NOT be able to fail a run.** Lineage/OTLP export errors are logged and counted (`faucet_lineage_dropped_total`, `faucet_otel_export_failures_total`), never propagated. Metric recorder / subscriber double-install warns rather than panics (`install_observability` is idempotent). Preserve this property.

## Related

- [Observability architecture](../architecture/observability.md)
- [ADR 0008 — Observability](../adr/0008-observability.md)
- [Error Handling Standard](./error-handling.md)
- [Operations: Observability](../../docs/site/src/content/docs/operations/observability.md)
