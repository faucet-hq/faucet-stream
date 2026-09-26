# faucet-stream vs. Vector

*Both are Rust, single-binary, config-driven — but they solve different problems. Here's the honest line between them.*

> Reflects each tool as of **2026-07**. Vector is actively developed by Datadog; check [vector.dev](https://vector.dev/) for its current state.

## The short version

**Vector** is an excellent **observability pipeline** — a Rust, single-binary, config-driven agent/aggregator for collecting, transforming, and routing **logs, metrics, and traces** to observability backends, with its own remap language (VRL). It's MPL-2.0 and fully open source.

**faucet-stream** shares Vector's DNA — Rust, one static binary, declarative config — but a different **domain**: moving **business data** between APIs, databases, object stores, and warehouses, with change data capture, incremental/resumable replication, and governance built into the movement path.

They're cousins, not competitors: reach for Vector for **telemetry**, faucet-stream for **data movement**. Many stacks run both.

## Where faucet-stream is different

- **Domain: databases, SaaS, and warehouses — not telemetry.** faucet connects Postgres, MySQL, MongoDB, Kafka, S3/GCS, BigQuery, Snowflake, Iceberg, Delta, and more, as source→sink pipelines. Vector's sources/sinks are observability-oriented (log shippers, metrics stores, trace backends).
- **Change data capture.** faucet does engine-level CDC (Postgres / MySQL / Mongo) with resumable state. Vector has no database CDC — it isn't an ELT tool.
- **Governance in the movement path.** Data-quality checks, versioned data contracts, PII masking (before any sink sees a row), schema-drift policy, column-level lineage (OpenLineage) + a data-movement catalog, and freshness/volume SLAs — native and zero-config.
- **Effectively-once delivery.** Per-page commit tokens commit atomically with the data, so a resumed run drops duplicates — across 12 sinks (SQL, Kafka, Iceberg, BigQuery, Snowflake, Spanner, Databricks, MongoDB, Redis).
- **Embeddable.** Compile the same engine into your own Rust service via the typed `Source` / `Sink` traits.

## Where Vector is the better choice

Straight with you — it's a different job, and Vector is superb at it:

- **Observability telemetry.** If you're collecting and routing logs, metrics, and traces, Vector is purpose-built and battle-tested at scale. faucet doesn't play in that space.
- **Agent + aggregator topologies.** Vector is designed for fleet-wide telemetry collection with local agents feeding aggregators.
- **VRL** — a rich, expressive remap language for reshaping telemetry events in flight.

## Side-by-side

| | **faucet-stream** | Vector |
|---|---|---|
| Language / runtime | Rust, single binary | Rust, single binary |
| Domain | ETL / CDC / warehouse data movement | observability (logs / metrics / traces) |
| Connectors | 58 DB / API / object-store / warehouse | dozens, observability-oriented |
| Change data capture | ✓ Postgres / MySQL / Mongo | ✗ |
| Warehouse / ELT sinks | ✓ BigQuery, Snowflake, Iceberg, Delta, … | ✗ |
| In-flight transforms | 11 record transforms + embedded-DuckDB `sql` | VRL (Vector Remap Language) |
| Governance in-path (quality / contracts / masking / lineage / SLA) | ✓ native | ✗ |
| Embeddable as a library | ✓ (Rust) | ✗ (standalone agent/aggregator) |
| License | MIT / Apache-2.0 | MPL-2.0 |

## Using them together

They coexist cleanly in one stack: **Vector** ships your logs/metrics/traces to your observability backend, while **faucet-stream** moves your business data between databases, object stores, and warehouses. If anything, faucet's own [Prometheus metrics + `tracing`](../operations/observability.md) can flow *through* a Vector pipeline to your telemetry backend.

## See for yourself

- **[Choosing a connector](../reference/choosing.md)** — confirm your sources and sinks are covered.
- **[Try it in 60 seconds](../getting-started/try-it-locally.md)** — no infrastructure needed.
- **[Benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)** — full methodology and honest caveats.
