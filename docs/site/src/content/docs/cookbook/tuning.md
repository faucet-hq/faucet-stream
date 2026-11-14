---
title: "Throughput tuning"
description: "faucet's defaults are already tuned for sustained bulk movement (pooled clients, multi-row writes, bounded-memory streaming). When you need more, work through these levers in…"
---

faucet's defaults are already tuned for sustained bulk movement (pooled
clients, multi-row writes, bounded-memory streaming). When you need more,
work through these levers in order — the first two are faucet config, the
rest are destination-side decisions faucet deliberately never makes for you.

Benchmarked context for what these levers buy is in
[BENCHMARKS.md](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)
(Scenario C is the sink-bound case this page mostly talks about).

## 1. `batch_size` — the universal knob

Every source and sink exposes `batch_size` (default `1000`, `0` = "no
batching": the whole result set / upstream page as one unit).

- **Sink-bound moves rarely improve past ~1000–5000 rows per write.** For
  the Postgres sink, throughput is flat from 500→5000 rows per `INSERT` and
  degrades once `rows × columns` approaches the 65 535 bind-parameter cap
  (the sink auto-splits to stay under it, but the sweet spot is ~1000).
- **Match source and sink sizes** so pages aren't re-chunked twice; setting
  only the source's `batch_size` and leaving the sink at `0` forwards each
  page verbatim.

## 2. Postgres bulk load — `write_method: copy`

For append-only loads into PostgreSQL, switch the sink to the `COPY` wire
protocol (issue #308):

```yaml
sink:
  type: postgres
  config:
    connection_url: ${env:PG_URL}
    table_name: events
    column_mapping: auto_map
    write_method: copy       # COPY … FROM STDIN instead of multi-row INSERT
```

`COPY` skips per-statement parse/bind/plan overhead and is typically **5–10×
faster** than multi-row `INSERT` at the destination. Semantics are unchanged
(same rows, same types, same durability); restrictions:

- append-only — rejected with `write_mode: upsert|delete` at config load;
- all-or-nothing per batch (one bad row fails the whole `COPY`; the DLQ
  `on_batch_error` policy applies);
- `delivery: exactly_once` always stays on the `INSERT` transaction path so
  the watermark commits atomically with the page.

See the [postgres sink README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/sink/postgres#bulk-load-write_method-copy)
for details.

## 3. Destination-side knobs (your call, not faucet's)

These make bulk loads dramatically faster but **change durability or
consistency guarantees**, so faucet never flips them silently. Set them on
the destination yourself when the trade-off fits:

| Knob | Win | Cost |
|------|-----|------|
| `CREATE UNLOGGED TABLE …` (Postgres) | Skips WAL entirely — the fastest ingest path | Table is **truncated on crash recovery** and not replicated. Use for staging tables you can re-load. |
| `SET synchronous_commit = off` (session/role/database) | Commits return before WAL reaches disk | A crash can lose the last few transactions (never corrupts). Good default for re-runnable batch loads. |
| Drop/disable indexes + constraints before the load, rebuild after | Index maintenance often dominates bulk-insert cost | A window where constraints aren't enforced; rebuild time at the end. |
| Load into a staging table, then `INSERT … SELECT` / partition-swap | Keeps the hot table available and indexes warm | Extra disk + a copy step. |

## 4. Parallelism

- **Source sharding (Mode B)** — shardable sources (`postgres`, `mysql`,
  `mssql`, `sqlite` via `shard: { key }`; `s3`/`gcs`/`parquet` by hash;
  `kafka` by consumer group) split one dataset across workers under
  `faucet serve --cluster`. See [Running a cluster](/faucet-stream/cookbook/cluster/).
- **Matrix fan-out** — independent tables/endpoints parallelize with matrix
  rows and `execution.max_concurrent`.
- Database sinks bound their pools (`max_connections`, default 5) on
  purpose; raise it explicitly if the destination has headroom.

## Measure, don't guess

Run the shipped harness before and after a change:

```bash
make bench-smoke      # 100k rows, fast signal
make bench-postgres   # adds the Docker Postgres scenarios (B & C)
```

The harness methodology and current numbers live in
[BENCHMARKS.md](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md).

## See also

- [Performance tuning](/faucet-stream/operations/tuning/) — the destination-side and
  runtime knobs (connection pools, concurrency, retries) that complement the
  config knobs here.
- [Adaptive batching](/faucet-stream/cookbook/adaptive-batching/) — let faucet size batches for you.
- [Connector capability matrix](/faucet-stream/reference/capability-matrix/) — per-connector
  streaming and batch support.
