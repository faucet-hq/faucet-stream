# ADR 0008 — Automatic, decorator-based observability

*Instrument sources, sinks, transforms, and state stores from inside the pipeline so connectors write zero metrics code.*

- **Status:** Accepted (implemented) — `crates/core/src/observability/`; `otel` feature for OTLP.

## Context

An operator needs uniform, complete visibility into every pipeline — record counts,
error rates, latencies, in-flight gauges, bookmark lag — regardless of which
connector is running. Connectors are written by many hands, including third parties.

## Problem

If each connector emitted its own metrics, coverage would be inconsistent (some
connectors instrumented, some not), label conventions would drift, and the metric
surface could explode with high-cardinality labels. But requiring every connector
author to learn and correctly apply a metrics API is friction that violates the
[extensibility goal](../architecture/extensibility.md).

## Decision

Instrument **automatically from inside the pipeline** using decorator wrappers:
`InstrumentedSource`, `InstrumentedSink`, `InstrumentedStateStore`, plus
transform/pipeline/resilience decorators (`crates/core/src/observability/`).
`Pipeline::run` wraps the source, sink, and store in these before driving the loop,
so **every** connector is fully instrumented with **zero** per-connector code. A
connector's only obligation is to return a non-empty `connector_name()` for the
label.

Metrics use the `metrics` facade (fanning out to Prometheus and, under the `otel`
feature, OTLP); traces use `tracing` spans. Common labels are `{pipeline, row,
connector}`. High-cardinality values (`run_id`, `parent_record_key`, URLs, query
strings) are **span attributes only, never metric labels**.

## Alternatives considered

- **Per-connector manual instrumentation.** Rejected: inconsistent coverage, drifting
  labels, author friction, and a real risk of high-cardinality label mistakes in
  third-party code.
- **No built-in observability; leave it to the embedding application.** Rejected: the
  CLI is the primary consumer and needs uniform metrics out of the box; operators
  should not have to instrument the tool themselves.
- **A pull-based introspection API instead of push metrics.** Rejected: does not fit
  Prometheus/OTLP ecosystems operators already run.

## Trade-offs

- Decorators add a thin per-call wrapper (drop-guard timers, counters) — negligible
  next to network I/O, and the decorators use RAII guards so gauges stay correct even
  on cancellation.
- The label set is fixed by the core, so a connector cannot add a bespoke dimension
  without a core change — intentional, to protect cardinality.

## Consequences

- **Positive:** complete, uniform coverage for every connector including third-party
  ones; disciplined cardinality; one place to evolve the metric surface; free OTLP.
- **Negative:** connectors cannot easily add custom metrics; observability is coupled
  to the pipeline driving the connector (a connector used outside `Pipeline` is
  uninstrumented).

## Future work

- A sanctioned, cardinality-safe hook for connector-specific metrics if a real need
  arises.

## Related

- [Observability](../architecture/observability.md) · [resilience](../architecture/resilience.md)
- [Standards: logging & metrics](../standards/logging.md) · [Design invariants (I11)](../architecture/invariants.md)
- User guide: [Observability](../site/src/content/docs/operations/observability.md)
