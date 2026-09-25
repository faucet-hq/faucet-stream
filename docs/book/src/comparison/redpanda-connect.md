# faucet-stream vs. Redpanda Connect (Benthos)

*The other declarative-YAML single binary. Here's where each one wins — including an honest note on the license history.*

> Reflects each tool as of **2026-07**. Verify licensing against the current [project](https://github.com/redpanda-data/connect) `LICENSE` files, which are the source of truth per component.

## The short version

**Redpanda Connect** is the tool formerly known as **Benthos** (acquired by Redpanda in 2024) — a Go stream processor configured with declarative YAML (`input → processors → output`). It's streaming-first, has a large component library, and is the closest architectural analogue to faucet-stream's config-driven model. Ships as a single binary or as managed pipelines on Redpanda Cloud.

**faucet-stream** is built for **batch/ELT data movement** rather than continuous stream processing: incremental + resumable replication, snapshot→CDC, first-class warehouse sinks, and governance in the movement path — under uniform permissive licensing.

Reach for Redpanda Connect for **continuous, record-by-record streaming**; reach for faucet-stream to **move data between databases, object stores, and warehouses** as discrete, resumable, governed runs.

## A note on licensing

Worth stating precisely, because it's a real adoption consideration: Benthos was originally **MIT**. After the Redpanda acquisition the maintained repo moved to a **mix of Apache-2.0 and a source-available Redpanda Enterprise/Community license**, with some components (certain CDC inputs and others) gated behind the enterprise license. The community forked the pre-relicensing project as **[Bento](https://github.com/warpstreamlabs/bento)**, which continues under permissive terms. faucet-stream is uniformly **MIT / Apache-2.0** with no enterprise-gated connectors.

## Where faucet-stream is different

- **Batch/ELT is the home turf.** Incremental + resumable replication, snapshot→CDC handoff, and first-class warehouse sinks (BigQuery, Snowflake, Iceberg, Delta) — the job faucet is built for.
- **Governance in the movement path.** Data-quality checks, versioned data contracts, PII masking (before any sink sees a row), schema-drift policy, column-level lineage (OpenLineage) + a catalog, and freshness/volume SLAs — native and zero-config.
- **Effectively-once delivery.** Per-page commit tokens commit atomically with the data, so a resumed run drops duplicates — across 11 sinks (SQL, Kafka, Iceberg, BigQuery, Snowflake, Spanner, MongoDB, Redis).
- **Uniform permissive licensing.** MIT / Apache-2.0 throughout — no per-component enterprise gate to audit.

## Where Redpanda Connect is the better choice

Straight with you — for its core job it's excellent:

- **Continuous, record-by-record streaming.** It's purpose-built for never-ending stream processing with a rich processor/transform library. faucet runs discrete pipelines to completion — even its long-running modes (`faucet schedule`, `faucet serve`) orchestrate complete runs, not an endless stream.
- **Deep Redpanda/Kafka ecosystem integration** and a large, mature component catalog.
- **Battle-tested** across years of production stream-processing use, with a Go library you can embed.

## Side-by-side

| | **faucet-stream** | Redpanda Connect (Benthos) |
|---|---|---|
| Language / runtime | Rust, single binary | Go, single binary |
| Orientation | discrete runs to completion | continuous stream processing |
| Connectors | <!--COUNT:connectors-->68<!--/COUNT--> source/sink, ETL/CDC/warehouse | hundreds of components/processors |
| Change data capture | ✓ engine-level | some CDC inputs (several enterprise-gated) |
| Warehouse / ELT sinks | ✓ BigQuery, Snowflake, Iceberg, Delta, … | more messaging/stream-oriented |
| Governance in-path (quality / contracts / masking / lineage / SLA) | ✓ native | ✗ |
| Effectively-once delivery | ✓ (11 sinks incl. Kafka, Iceberg, BigQuery) | ✗ |
| Embeddable as a library | ✓ (Rust) | ✓ (Go) |
| License | MIT / Apache-2.0 | Apache-2.0 + source-available enterprise |

## Rule of thumb

If the workload is a **continuous stream** you transform in flight, Redpanda Connect is purpose-built for it. If the workload is **moving data between APIs, databases, object stores, and warehouses** as discrete, resumable, governed runs — see [replication (snapshot → CDC)](../cookbook/replication.md) — that's faucet-stream.

## See for yourself

- **[Choosing a connector](../reference/choosing.md)** — confirm your sources and sinks are covered.
- **[Try it in 60 seconds](../getting-started/try-it-locally.md)** — no infrastructure needed.
- **[Benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)** — full methodology and honest caveats.
