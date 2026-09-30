# faucet-source-parquet

> **Deprecated.** Use [`faucet-source-file`](https://crates.io/crates/faucet-source-file) (or [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) for an S3 location) instead.
> This crate gets security fixes only until the next major release. In a
> pipeline config, `type: parquet` keeps working on this crate, unchanged — it is
> not an alias of `type: file` — and the CLI prints a one-line notice naming the
> replacement (`type: file` with `format: parquet`, or `type: s3` for an S3 location). The migration table is in
> the [`faucet-source-file` README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/source/file/README.md).

[![Crates.io](https://img.shields.io/crates/v/faucet-source-parquet.svg)](https://crates.io/crates/faucet-source-parquet)
[![Docs.rs](https://docs.rs/faucet-source-parquet/badge.svg)](https://docs.rs/faucet-source-parquet)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-parquet.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-parquet.svg)](https://github.com/faucet-hq/faucet-stream#license)

Apache **Parquet** file source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Reads one or more Parquet files from a **local path**, a **local glob pattern**, or **Amazon S3** (single object or prefix) and yields each row as a `serde_json::Value` object.

Built on the `parquet` + `arrow` crates wired through `object_store`, so local and S3 share one vectorized, streaming code path — `RecordBatch`es are decoded and converted to JSON incrementally and the source never buffers a whole file in memory. Reach for it to load columnar data lakes, S3 exports, or partitioned `year=/month=/` trees into any faucet-stream sink.

## Feature highlights

- **Three location modes** — single local file, local glob (sorted for determinism), or S3 (single key **or** prefix listing).
- **Column projection** — `columns: [a, b]` prunes at the Parquet level via `ProjectionMask`, so unread columns are skipped at the I/O layer, not just in JSON.
- **Parallel multi-file reads** — Glob / S3-prefix modes read up to `concurrency` files at once via `buffer_unordered`.
- **Vectorized streaming** — one Arrow `RecordBatch` per row-group; memory is bounded by `batch_size`, not file size.
- **Fail-fast schema validation** — multi-file scans validate every file's Arrow schema up front (cheap footer probe) and surface a mismatch naming both files and the first diverging field, *before* any rows are committed downstream.
- **S3-compatible** — custom `endpoint_url` for MinIO / LocalStack; credentials from the standard AWS chain.
- **Arrow columnar fast path** (`arrow` feature, RFC 0002) — emits Arrow `RecordBatch`es natively (`stream_batches`), so a `parquet → parquet` pipeline moves data end-to-end without materializing `serde_json::Value` rows. Opt-in; off by default and inert unless the sink is also columnar.

## Installation

```bash
# As a library:
cargo add faucet-source-parquet

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-parquet
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: parquet
    config:
      source:
        type: local_path
        path: /data/events.parquet
  sink:
    type: jsonl
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | `ParquetLocation` | — *(required)* | Where to read from — `local_path`, `glob`, or `s3` (see below). |
| `batch_size` | int | `1000` | Per-page row-count hint forwarded to `ParquetRecordBatchStreamBuilder::with_batch_size`. Arrow treats it as a max — a page may hold fewer rows at a row-group boundary. **`0` = no batching**: the file's native row-group size drives the page cadence (one page per row-group). |
| `columns` | array | *(all)* | Top-level columns to decode. Unknown names error out. Prunes I/O at the Parquet layer. |
| `concurrency` | int | `4` | Files read in parallel for Glob / S3-prefix modes. Ignored for single-file modes. Must be `> 0`. |

### `source` location (`ParquetLocation`)

```yaml
# Single local file
source: { type: local_path, path: /data/events.parquet }

# Local glob — all matched files must share one Arrow schema
source: { type: glob, pattern: "/data/year=2024/*.parquet" }

# S3 — set EXACTLY ONE of key / prefix
source:
  type: s3
  bucket: my-bucket
  prefix: "events/2024/"      # or:  key: events/2024/data.parquet
  region: us-east-1           # optional; defaults to the AWS resolution chain
  endpoint_url: http://localhost:9000   # optional; MinIO / LocalStack
```

| `ParquetS3Config` field | Type | Description |
|-------------------------|------|-------------|
| `bucket` | string | S3 bucket (required). |
| `key` | string | Single object key. Mutually exclusive with `prefix`. |
| `prefix` | string | Object-key prefix to list and read. Mutually exclusive with `key`. |
| `region` | string | AWS region. Defaults to the standard resolution chain. |
| `endpoint_url` | string | Custom endpoint for S3-compatible services. HTTP endpoints automatically allow plain HTTP. |

S3 credentials come from the standard `object_store` AWS chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, IMDS, profile, …).

## Examples

### Projected columns from a partitioned tree, 8 files in parallel

```yaml
source:
  type: parquet
  config:
    source: { type: glob, pattern: "/data/year=2024/*.parquet" }
    columns: [id, amount]
    concurrency: 8
    batch_size: 4096
```

### S3 prefix, one page per row-group (load-job friendly)

```yaml
source:
  type: parquet
  config:
    source: { type: s3, bucket: my-bucket, prefix: "events/2024/", region: us-east-1 }
    batch_size: 0          # emit one page per native row-group
```

## JSON representation

Each row becomes a `serde_json::Value::Object`. Field encoding is delegated to `arrow_json::ArrayWriter`, following Arrow's standard rules:

| Arrow type | JSON shape |
|---|---|
| `Int32`/`Int64`/`Float32`/`Float64` | number |
| `Utf8`/`LargeUtf8` | string |
| `Boolean` | `true`/`false` |
| `Date32`/`Date64` | ISO-8601 date string |
| `Time32`/`Time64` | ISO-8601 time string |
| `Timestamp(unit, tz)` | ISO-8601 timestamp string |
| `Decimal128`/`Decimal256` | string (precision/scale preserved) |
| `Binary`/`LargeBinary`/`FixedSizeBinary` | base64 string |
| `Struct(...)` | nested object |
| `List`/`LargeList`/`FixedSizeList` | array |
| `Map(K, V)` | object keyed by `K` |
| Null values | field omitted |

## Streaming & batching

`ParquetSource` implements `Source::stream_pages`, writing each Arrow `RecordBatch` to the sink as it's decoded — memory is bounded by `batch_size × row_width`, not file size. `batch_size` is a hint (Arrow may emit smaller batches at row-group boundaries); `batch_size: 0` skips the override so the reader emits one batch per row-group. Multi-file scans flatten in sorted-path order, and **all files' schemas are validated up front** so a later mismatch fails before earlier rows are committed. Every page carries `bookmark: None` — there is no incremental-replication mode for Parquet.

## Config loading & schema

```bash
faucet schema source parquet
```

## Library usage

```rust,no_run
use faucet_source_parquet::{ParquetSource, ParquetSourceConfig, ParquetS3Config};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Local glob, two columns, eight files in parallel
let source = ParquetSource::new(
    ParquetSourceConfig::glob("/data/year=2024/*.parquet")
        .columns(["id", "amount"])
        .concurrency(8)
        .batch_size(4096),
).await?;

// S3 prefix
let source = ParquetSource::new(
    ParquetSourceConfig::s3(
        ParquetS3Config::prefix("my-bucket", "events/2024/").region("us-east-1"),
    ),
).await?;
# Ok(()) }
```

## How it works

Each row group is read as an Arrow `RecordBatch` and converted to JSON on the fly. The S3 client is built once in `new()` and reused across every per-file read. Glob / S3-prefix mode reads up to `concurrency` files in parallel via `futures::buffer_unordered`. Column projection is applied with `ProjectionMask::columns` so unread columns are skipped at the Parquet I/O layer. A 100k-row, all-primitive file reads in well under 500 ms on a recent laptop in release mode; throughput depends on row width, projection, and storage medium — benchmark with your own data.

## Lineage dataset URI

`file://<path>` (local/glob), `s3://<bucket>/<key>` or `s3://<bucket>/<prefix>` — e.g. `file:///tmp/data.parquet` or `s3://my-bucket/events/2024/`.

## Feature flags

No optional features of its own; enable it in the CLI/umbrella via the `source-parquet` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `FaucetError::Config: concurrency` | `concurrency` is `0`. Set it to `≥ 1`. |
| `FaucetError::Config` on S3 | Both `key` **and** `prefix` set, or neither, or an empty `bucket`. Set exactly one of `key`/`prefix`. |
| Local read fails with `FaucetError::Source` | File missing/unreadable, or not valid Parquet. Check the path and permissions. |
| `Source` error naming a column | A name in `columns` doesn't exist in the file schema. Match the Parquet column names exactly. |
| `Source` error naming two files + a field | Globbed/prefixed files have divergent Arrow schemas. Ensure all files share one schema, or narrow the glob. |
| S3 `403`/credential errors | Credentials missing from the AWS chain, or wrong region. Set `region` and the AWS env vars/profile. |
| Connecting to MinIO/LocalStack fails | Set `endpoint_url` to the service URL (plain HTTP is auto-allowed for HTTP endpoints). |
| A `Decimal`/`Timestamp` arrives as a string | Intentional — Arrow encodes these as strings to preserve precision. Cast downstream if you need a number. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [faucet-sink-parquet](https://crates.io/crates/faucet-sink-parquet) · [faucet-source-s3](https://crates.io/crates/faucet-source-s3)

## Sharded execution (cluster Mode B)

Under [`faucet serve --cluster`](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html),
a top-level `shard: { count: N }` block splits this source across cluster
workers **automatically — no connector config needed**. Each worker reads the
files whose path hashes to its shard index (stable FNV-1a modulo `count`),
so the partition is disjoint and complete: every file is read by exactly one
worker, and the partition stays stable as new files appear. Outside the
cluster coordinator a run reads every file, unchanged.

> Cross-file schema validation still covers the **full** resolved file set on
> every worker, so a schema mismatch fails the run even when the mismatching
> files hash into different shards.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="parquet"`.
