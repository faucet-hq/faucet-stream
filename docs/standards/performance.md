# Performance Standard

*Throughput and bounded memory are review criteria, not afterthoughts — every connector should be the fastest correct way to move data between its endpoints in Rust.*

The Primary Goal of this project is that all sources and sinks are as fast, efficient, and reliable as possible. That makes performance a **correctness-adjacent review gate**: a connector that recreates a client per call or buffers the whole dataset is not "slow but fine", it is below standard.

## Mandatory disciplines

Apply these when adding or modifying any connector. Each is a MUST unless the connector's protocol makes it impossible (say so in the PR if so).

- **Reuse clients and connections.** Create HTTP clients, S3/GCS clients, DB pools, Redis connections, and Kafka producers once in `new()` and store them on the struct. **MUST NOT** construct a client, pool, or connection inside a per-record or per-page hot path.

  ```rust
  // GOOD — pool built once, reused per page.
  struct PostgresSink { pool: PgPool }
  impl PostgresSink { fn new(cfg) -> Self { Self { pool: build_pool(cfg) } } }

  // BAD — reconnects on every batch; destroys throughput and leaks handles.
  async fn write_batch(&self, rows) { let pool = build_pool(&self.cfg); … }
  ```

- **Pool connections with a configurable bound.** Database connectors expose `max_connections` (default 10 sources / 5 sinks).
- **Use multi-row / bulk APIs.** DB sinks issue multi-row `INSERT … VALUES (…), (…)`; prefer native bulk endpoints (Elasticsearch `_bulk`, BigQuery `insertAll`, Mongo `insert_many`, Redis pipelines). One request per record is a defect.
- **Wrap batches in a transaction** where the store supports it (the SQLite sink brackets each batch in `BEGIN`/`COMMIT`).
- **Parallelize I/O with a bound.** Use `buffer_unordered(concurrency)` for concurrent object reads/writes and semaphore-gated concurrent HTTP sends. Expose the bound (`concurrency` / `max_connections`) with a sane default — never unbounded.
- **Buffer file I/O** and move blocking work (`csv` (de)serialization) onto `spawn_blocking` so the async runtime is never blocked.

## Memory bound

- **The pipeline holds at most O(`batch_size`) records in flight per side**, regardless of total volume, because `run_stream` writes each `StreamPage` as it arrives (`crates/core/src/pipeline.rs`). A connector **MUST NOT** defeat this by materializing the full result set when a streaming primitive exists — override `stream_pages` to stream natively from the underlying cursor/scroll/consumer. See [Batching](../architecture/batching.md) and [Stream Pages](../architecture/stream-pages.md).
- **`batch_size = 0`** is the explicit opt-out (emit/accept the entire result set in one page) — use it only where a single large request is genuinely better (SQL `COPY`, a load job), not as a shortcut around streaming.

## Optimizing responsibly

- **MUST have evidence before optimizing.** A performance change to working code requires a benchmark or a profile showing the hot path — not intuition. The benchmark harness and scale findings are the reference (`BENCHMARKS.md`, `scripts/`).
- **MUST NOT trade correctness for speed.** A retry that can duplicate rows, a skipped flush, or a reordered checkpoint is never an acceptable optimization. See [State & Durability Standard](./state.md).
- **SHOULD prefer the highest-leverage change**: reducing round-trips (batching, pipelining) beats micro-optimizing allocation in almost every connector. The known systemic allocation cost — `serde_json::Value` per record — is tracked for a columnar record model in [RFC 0002](../../rfcs/0002-arrow-support.md), not worked around ad hoc.

## Related

- [Performance architecture](../architecture/performance.md)
- [Batching](../architecture/batching.md) · [Stream Pages](../architecture/stream-pages.md)
- [State & Durability Standard](./state.md)
- [Book: throughput tuning](../../docs/site/src/content/docs/cookbook/tuning.md)
