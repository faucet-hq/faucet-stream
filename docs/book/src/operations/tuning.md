# Performance tuning

faucet is built to be fast by default, but a few knobs let you trade memory for
throughput on a given pipeline.

## `batch_size`

The single most important knob. It bounds how many records are buffered and sets
each sink's natural write unit (multi-row `INSERT`, `_bulk` body, `insertAll`
request, Redis pipeline, …).

- **Default:** 1000. **Max:** 1,000,000.
- **Larger** = fewer, bigger requests = more throughput, more memory per batch.
- **`batch_size: 0`** = "no batching": the source emits the whole result set in
  one page and the sink writes it in one request. Use it for small lookup tables,
  or for sinks that prefer one large request (load-job-style ingestion).

Set it on the sink (authoritative) and/or source. Streaming keeps memory at
O(batch_size) on both sides regardless of total volume.

## Connection pooling

Database connectors use configurable pools — `max_connections` defaults to 10 for
sources and 5 for sinks. Raise it for highly concurrent workloads; keep it under
your database's connection limit.

## Concurrency

- The REST source can fetch its `requests:` entries concurrently (`request_concurrency`).
- S3/GCS sources and sinks read/write objects in parallel
  (`buffer_unordered`-style concurrency).
- The HTTP sink sends per-record requests concurrently under a semaphore.
- Kafka sink uses `FuturesUnordered` batched sends with QueueFull retry.
- At the pipeline level, `execution.max_concurrent` bounds how many matrix rows
  run at once.

## Retries

HTTP-based sources retry with exponential backoff + jitter on retriable failures.
The backoff is capped at 60s and its jitter is decorrelated across concurrent
retries (so a fleet of matrix rows doesn't re-align into a thundering herd). The
REST source additionally honors `429` / `Retry-After` (delta-seconds **or** an
RFC 7231 HTTP-date). Tune `max_retries` and `retry_backoff` per connector. A
permanently throttled endpoint surfaces a `RateLimited` error rather than hanging.

## Choosing values

- **Many small rows, row-oriented sink (Postgres/MySQL):** larger `batch_size`
  (5k–50k) for fat multi-row INSERTs.
- **Large objects (Parquet/S3):** moderate `batch_size`; lean on parallel I/O.
- **Tiny lookup tables / COPY-style loads:** `batch_size: 0`.
- **Memory-constrained host:** smaller `batch_size` to cap per-batch footprint.

Measure with the [metrics](./observability.md) — `faucet_sink_write_duration_seconds`
and `faucet_source_page_duration_seconds` tell you where time goes.

## Benchmarks

`faucet-core` ships two benchmarks of the pipeline's own overhead: the work it
does around a source and a sink, measured with a source and sink that do no I/O.

- **`pipeline_instructions`** counts CPU instructions with Valgrind's callgrind
  (through [gungraun](https://github.com/gungraun/gungraun)). Instruction counts do not move between runs, so
  CI compares every PR against its base commit and fails if the pipeline's
  instruction count grows by more than 5%. It needs Linux, `valgrind` and
  `gungraun-runner` at the library's version:

  ```bash
  cargo install --version 0.20.0 --locked gungraun-runner
  cargo bench -p faucet-core --bench pipeline_instructions -- --save-baseline=main
  # …make your change…
  cargo bench -p faucet-core --bench pipeline_instructions -- --baseline=main --callgrind-limits='ir=5%'
  ```

- **`observability`** measures wall-clock time with
  [criterion](https://github.com/bheisler/criterion.rs). Use it to profile on
  your own machine. It is not a CI gate, because wall-clock differences of a
  few percent are within the noise of a shared runner:

  ```bash
  cargo bench -p faucet-core --bench observability
  ```

Wall-clock numbers are hardware-dependent, so run that benchmark on your target
machine rather than relying on published figures.

For the end-to-end throughput comparison against Meltano (Singer) — charted per
scenario, with the full methodology and caveats — see
[Benchmarks (vs Meltano)](../comparison/benchmarks.md).

## See also

- [Throughput tuning](../cookbook/tuning.md) — the config-side knobs
  (`batch_size`, `COPY` fast-paths, sharding) that pair with the destination-side
  wins on this page.
