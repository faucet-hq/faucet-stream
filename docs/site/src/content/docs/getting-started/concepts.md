---
title: "Core concepts"
description: "faucet-stream is built from a handful of small pieces. Understanding them makes both the YAML config and the Rust API obvious."
---

faucet-stream is built from a handful of small pieces. Understanding them makes
both the YAML config and the Rust API obvious.

## Source

A **source** fetches records from an external system (a REST API, a database, a
Kafka topic, an object store, …) and yields them as JSON values. Sources stream
in batches via `stream_pages`, so memory stays bounded no matter how much data
flows through.

## Sink

A **sink** writes records to an external system. Sinks accept batches and most
expose a `batch_size` knob that controls the natural unit of work (a multi-row
`INSERT`, a `_bulk` body, an `insertAll` request, and so on).

## Transform

An optional **transform** reshapes each record between source and sink. The
config-exposed transforms include `flatten`, `rename_keys`, `keys_case`,
`select`, `cast`, and `redact` (see the
[record transforms recipe](/faucet-stream/cookbook/transforms/) for the full list);
additional custom transforms are available from Rust.

## Pipeline

The **pipeline** connects a source to a sink. It drives the source's
`stream_pages`, applies transforms, and writes each page to the sink as it
arrives — then flushes and records progress. Memory is bounded at one
`batch_size` page on both sides regardless of total volume.

```rust
let result = Pipeline::new(&source, &sink).run().await?;
```

## State store & bookmarks

For incremental and resumable runs, a **state store** persists a *bookmark* after
each page the sink confirms. On the next run the source resumes from that
bookmark. Built-in backends are `memory` and `file` (in `faucet-core`); `redis`
and `postgres` backends live in their own crates.

This is what makes change-data-capture safe: the PostgreSQL CDC source only tells
Postgres it can recycle write-ahead log up to a bookmark that has actually been
persisted.

## Dead-letter queue (DLQ)

A pipeline can attach a **DLQ** sink. When a sink reports per-row failures, the
failing rows are wrapped in a fixed-shape envelope and routed to the DLQ before
the page's bookmark advances — so a few bad records don't abort the whole run.
The `on_batch_error` policy (`propagate` vs `dlq_all`) decides what happens when
a sink can't report per-row results.

## Matrix & DAGs

A single config can fan out into many invocations with a `matrix:` block — either
independent rows or a parent/child **DAG** where a child runs once per record
produced by its parent. See the [matrix DAG tutorial](/faucet-stream/tutorials/matrix-dag/).

## Observability

Every source, sink, transform, and state operation is automatically wrapped to
emit `tracing` spans and `metrics` counters/histograms — no per-connector code.
See [Observability](/faucet-stream/operations/observability/).
