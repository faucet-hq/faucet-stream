# faucet-sink-parquet

> **Deprecated.** Use [`faucet-sink-file`](https://crates.io/crates/faucet-sink-file) (or [`faucet-sink-s3`](https://crates.io/crates/faucet-sink-s3) for an S3 location) instead.
> This crate gets security fixes only until the next major release. In a
> pipeline config, `type: parquet` keeps working on this crate, unchanged — it is
> not an alias of `type: file` — and the CLI prints a one-line notice naming the
> replacement (`type: file` with `format: parquet`, or `type: s3` for an S3 location). The migration table is in
> the [`faucet-sink-file` README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/sink/file/README.md).

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-parquet.svg)](https://crates.io/crates/faucet-sink-parquet)
[![Docs.rs](https://docs.rs/faucet-sink-parquet/badge.svg)](https://docs.rs/faucet-sink-parquet)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-parquet.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-parquet.svg)](https://github.com/faucet-hq/faucet-stream#license)

Apache **Parquet** file sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records as columnar Parquet files to a **local filesystem path** or an **Amazon S3** bucket (or any S3-compatible service).

Built on the `parquet` + `arrow` crates wired through `object_store`, so local and S3 share one streaming code path — records are decoded into Arrow `RecordBatch`es and paged through an `AsyncArrowWriter` with bounded buffering, never staging a whole file in memory. Reach for it to land pipeline output as a columnar data lake, partition large exports across many files, or feed downstream analytics engines (DuckDB, Spark, BigQuery external tables).

## Feature highlights

- **Local or S3** — write to a local file/directory or to S3 (and S3-compatible services like MinIO / LocalStack via `endpoint_url`).
- **Schema inference per file** — the Arrow schema is learned from each file's opening batch; every field is forced nullable, so missing keys round-trip as `NULL`, and a field null in every sampled record becomes a nullable string column instead of failing the first non-null value. On rollover the schema is re-inferred, so a column added mid-run (e.g. CDC after `ALTER TABLE ADD COLUMN`) is picked up by the next file rather than dropped for the whole run.
- **Columnar compression** — `snappy` (default), `gzip`, `zstd`, `lz4`, or `uncompressed` — applied internally by the Parquet writer.
- **Row & byte rollover** — split large outputs across multiple `<uuid>.parquet` files by row count (`max_rows_per_file`) or byte budget (`max_bytes_per_file`).
- **Streaming writer** — one reused `object_store` client, bounded buffering, configurable `row_group_size` for read-back performance.
- **Drops unknown fields (within a file)** — a field absent from the *current file's* locked schema (one that appeared after the file's first batch) is dropped, with a `tracing::warn!` once per field per file. A Parquet file cannot change its schema mid-stream, so this loss is unavoidable within a file — but the schema is re-inferred per file, so the field is captured in the next file on rollover.
- **Flush-safe** — files become valid only when the footer is written. In **rollover / directory / S3 mode** each `flush()` (called automatically by the pipeline on success, error-unwind, and cooperative cancellation) closes the current file and writes its footer. In **single-file mode** each `flush()` publishes the whole file (footer, fsync, atomic rename) and the next page continues it, so the file on disk always matches the stored bookmark; nothing is finalised when the sink is dropped.
- **Arrow columnar fast path** (`arrow` feature, RFC 0002) — accepts Arrow `RecordBatch`es natively (`write_batch_columnar`), so a `parquet → parquet` pipeline writes them straight through the same writer/rollover path with no `serde_json::Value` round-trip. Opt-in; off by default and used only when the source is also columnar.

## Installation

```bash
# As a library:
cargo add faucet-sink-parquet

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-parquet
```

The `sink-parquet` feature is opt-in (not part of the CLI/umbrella defaults).

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./events.csv
  sink:
    type: parquet
    config:
      destination:
        type: local_path
        path: /var/lib/exports/events
      compression: snappy
```

```bash
faucet run pipeline.yaml
```

Pointing `path` at a directory writes UUID-named files into it; pointing it at a `*.parquet` file writes a single file (only valid when no rollover thresholds are set — see [Rollover](#rollover)).

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `destination` | `ParquetDestination` | — *(required)* | Where to write — `local_path` or `s3` (see [Destinations](#destinations)). |
| `schema` | `SchemaSource` | infer | How the Arrow schema is determined. Omit to infer from the first batch. `explicit` is reserved and currently errors. |
| `compression` | enum | `snappy` | Column-data codec: `uncompressed` · `snappy` · `gzip` · `zstd` · `lz4`. See [Compression](#compression). |
| `row_group_size` | int | `1048576` | Max rows per Parquet **row group**. Must be `> 0`. Larger groups favour read scan throughput. |

### Rollover

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_rows_per_file` | int | *(none)* | Roll to a new file once the current writer has accepted this many rows. Must be `> 0` when set. |
| `max_bytes_per_file` | int | *(none)* | Roll to a new file once the writer's estimated in-memory size exceeds this. Checked per batch (approximate — see below). Must be `> 0` when set. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` (`DEFAULT_BATCH_SIZE`) | Re-chunk incoming pages into this many records per internal write. **`0` = no batching** — pages pass through as-is. For Parquet, leaving this at the default (or `0`) is recommended. Capped at `MAX_BATCH_SIZE`. |

### `schema` (`SchemaSource`)

```yaml
# Infer from the first batch, sampling up to N records (default 100):
schema: { type: inferred, sample_size: 200 }
```

`type: explicit` is reserved for a future revision and currently returns a `Config` error.

## Destinations

### Local filesystem — directory mode (UUID-suffixed files)

```yaml
destination:
  type: local_path
  path: /var/lib/exports/events
```

### Local filesystem — single-file mode (no rollover thresholds)

```yaml
destination:
  type: local_path
  path: /var/lib/exports/events.parquet
```

In single-file mode every `flush()` publishes a complete file at the path:
the footer is written, the file is fsynced and renamed into place, so the
bookmark the pipeline stores after a flush never names rows that are not on
disk, and a reader sees either the previous complete file or the new one. The
next page reopens the file and copies its rows into a new writer before
appending (a Parquet file cannot be extended in place), so every page of the
run ends up in the one file; the cost is a rewrite per bookmark-carrying page,
so a high-frequency CDC source is better served by a directory destination.
Rows written after the last `flush()` are discarded when the sink is dropped —
nothing is finalised on drop, so a failed or killed run never publishes rows
its bookmark does not cover.

### S3 (or any S3-compatible service)

```yaml
destination:
  type: s3
  bucket: my-bucket
  prefix: events/                       # each object is <prefix><uuid>.parquet
  region: us-east-1                     # optional; defaults to the AWS chain
  endpoint_url: http://localhost:4566   # optional; MinIO / LocalStack
  allow_http: true                      # required for http:// endpoints
```

| `ParquetS3Destination` field | Type | Default | Description |
|------------------------------|------|---------|-------------|
| `bucket` | string | — *(required)* | S3 bucket name. |
| `prefix` | string | `""` | Key prefix for written objects. Empty writes to the bucket root. |
| `region` | string | *(SDK default)* | AWS region. |
| `endpoint_url` | string | *(none)* | Custom endpoint for S3-compatible services. |
| `allow_http` | bool | `false` | Allow non-HTTPS endpoints. Required when `endpoint_url` is `http://`. |

S3 credentials follow the standard `aws_config` / `object_store` AWS chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, profile, instance metadata).

## Examples

### Roll over every million rows, zstd-compressed

```yaml
sink:
  type: parquet
  config:
    destination: { type: local_path, path: /data/exports }
    compression: zstd
    max_rows_per_file: 1000000
    row_group_size: 262144
```

### S3 export with a byte budget per file

```yaml
sink:
  type: parquet
  config:
    destination:
      type: s3
      bucket: analytics-lake
      prefix: events/dt=2026-06-16/
      region: us-east-1
    compression: snappy
    max_bytes_per_file: 134217728   # ~128 MiB (approximate)
```

### Dated subdirectory via `${now.*}` interpolation

```yaml
sink:
  type: parquet
  config:
    destination:
      type: local_path
      path: ./data/dt=${now.date}/events    # parent dirs auto-created
    compression: gzip
```

The `${now.*}` tokens resolve to the run's clock (`faucet run` uses process-start UTC or `--clock`; `faucet schedule` uses the tick time). Missing parent directories are created automatically, so dated partition trees like `dt=2026-06-16/` work without pre-creating the path.

### S3-compatible service (MinIO / LocalStack), one large file

```yaml
sink:
  type: parquet
  config:
    destination:
      type: s3
      bucket: dev-bucket
      prefix: out/
      endpoint_url: http://localhost:9000
      allow_http: true
    batch_size: 0          # write upstream pages through as-is
```

## Schema handling

- **Inferred (default)** — each file's first batch is used to learn an Arrow schema via `arrow_json::reader::infer_json_schema_from_iterator`, sampling up to `sample_size` records (default `DEFAULT_SAMPLE_SIZE` = 100). All inferred fields are forced nullable.
- **Per-file inference / re-inference on rollover** — a Parquet file's schema is **immutable once its first batch is written**, so the schema is locked for the lifetime of *that file*. On rollover (a `max_rows_per_file` / `max_bytes_per_file` threshold firing, or an explicit `flush()` in directory / S3 mode) the locked schema is cleared and the *next* file re-infers from its own first batch. The practical effect: when the record shape **widens mid-run** (a new column appears — e.g. CDC following an `ALTER TABLE ADD COLUMN`, or a non-homogeneous first page), the new column is written in the next file once a rollover boundary is crossed, instead of being silently dropped for the entire run. **In single-file mode (no rollover) there is no file boundary**, so the schema stays locked to the first batch for the whole run — see the within-file limitation below.
- **Missing fields** — absent keys are written as Arrow `NULL` columns.
- **Unknown fields (within-file widening limitation)** — a field present in a record but **absent from the current file's locked schema** is silently dropped by the Arrow JSON decoder. This only happens for a field that appears *after* the file's schema was locked and *before* the next file boundary — a Parquet file cannot change its schema mid-stream, so this drop is genuinely unavoidable within a single file. Each such field is logged with a `tracing::warn!` **once per field per file** (the warn de-dupe resets on rollover, so a fresh file re-warns if it still drops the field) — making the unavoidable loss visible rather than silent. To capture the field: cross a rollover boundary (it lands in the next file), set rollover thresholds so files roll more often, or re-run; in single-file mode, ensure the widest record shape appears in the opening batch (or widen `sample_size`).
- **Type drift** — if a later batch sends a value whose JSON type disagrees with the locked-in schema (e.g. int → string), `write_batch` returns `FaucetError::Sink` naming the field, the schema's declared type, and the drifting record's type.
- **Explicit schema** — reserved; currently returns a `Config` error.

> For a destination-managed, declarative response to a widening source schema (evolve / quarantine / fail policies), see the workspace **schema-drift** feature. The Parquet sink is schemaless from that feature's perspective (it reports no live destination schema), so a `schema:` policy is inert against it — the per-file re-inference described here is the Parquet-native mechanism.

## Rollover

| Threshold | Trigger |
|-----------|---------|
| `max_rows_per_file` | row count of the current writer ≥ limit |
| `max_bytes_per_file` | estimated in-memory size ≥ limit |

When either limit fires, the current Parquet writer is closed (writing the footer) and the next `write_batch` opens a fresh `<uuid>.parquet`. Both thresholds are checked after each batch, so the actual file may exceed the limit by up to one batch. **The schema is re-inferred for each new file**, so if the record shape has widened since the previous file, the new column appears in the new file (see [Schema handling](#schema-handling)).

> **`max_bytes_per_file` is approximate.** The threshold compares against an estimate of the *in-memory Arrow* size, not the on-disk Parquet size. Column encoding + compression usually make the actual file substantially smaller. Treat it as a soft target, not a hard byte cap.

> **A fixed `*.parquet` local path + a rollover threshold can't coexist.** Single-file mode (a `foo.parquet` destination) requires *no* rollover thresholds. If you set `max_rows_per_file` / `max_bytes_per_file` alongside a fixed `.parquet` path, the sink logs a warning and falls back to writing UUID-named files into the parent directory. Use a directory destination when you want rollover.

## Compression

| Codec | Notes |
|-------|-------|
| `uncompressed` | Largest output, fastest writes. |
| `snappy` | **Default.** Best balance of speed and size. |
| `gzip` | Smaller than snappy, ~3–5× slower. |
| `zstd` | Strong compression; default level. |
| `lz4` | Maps to `LZ4_RAW` (the Parquet-spec variant). |

Conservative (default) compression levels are used — every percent of CPU spent compressing is a percent the pipeline loses on throughput; post-process if you need maximum compression. This is **internal Parquet column compression** and is unrelated to the workspace-wide `compression` feature (file-level gzip/zstd wrappers), which this sink does not use.

## Streaming and batching

The sink accepts whatever the upstream pipeline hands it — the streaming runtime in `faucet-core` already caps per-call memory at the upstream source's `batch_size`. On top of that, the sink's own `batch_size` knob re-chunks every incoming page before it reaches the Arrow writer:

- `batch_size` (default `1000`) — slice pages into this many records per internal write.
- `batch_size: 0` — the "no batching" sentinel; pages pass through as-is.

For Parquet (local or S3) the source-defined page size is usually optimal because the writer streams into the destination as one multipart upload and benefits from larger row groups. **Recommended: leave `batch_size` at its default (or `0`) and let the upstream `batch_size` drive sizing.** The row/byte rollover thresholds are **independent of `batch_size`** and continue to work unchanged.

## Flush semantics

Parquet files are only valid once the trailing footer is written. The sink streams data into the destination as an `object_store` multipart upload. **How `flush()` interacts with the footer depends on the mode:**

- **Rollover / directory / S3 mode** (a `max_rows_per_file` / `max_bytes_per_file` threshold, a directory destination, or any S3 destination): each `flush()` closes the in-flight writer and writes its footer; the next page opens a fresh `<uuid>.parquet`. A row/byte threshold also triggers an automatic mid-run close + rollover. **If you drop the sink without a final `flush()`, the last file's footer is never written** — `object_store` aborts the multipart upload, so you get no half-written object rather than a corrupt one. Pipelines must therefore always call `flush()` at the end of a run.
- **Single-file mode** (a fixed `*.parquet` local path with no rollover thresholds): one writer stays open for the whole run. The pipeline flushes after every bookmark-carrying page, but those intermediate flushes only push buffered row groups to the open file — they do **not** write the footer, and the file is never reopened/truncated mid-stream. The footer is written exactly once when the sink is dropped at end of run. This is essential for multi-bookmark sources (e.g. CDC), where a footer-on-every-flush would truncate the file on the next page and silently lose all but the last page.

`faucet-core`'s streaming pipeline flushes on the success path **and** the error-unwind path, and on **cooperative cancellation**: when a run is cancelled via a `CancellationToken` (the `faucet serve` run-timeout / `POST /cancel` / shutdown, or the CLI's `on_error: stop`), the pipeline stops at the next page boundary and flushes, so the rows committed so far survive — rather than the whole file being orphaned by a dropped future (#146 H16). A sink stuck *mid-write* past the flush-grace window is still hard-dropped (and its file lost), so size pages so a single `write_batch` stays well within the grace.

## Config loading & schema introspection

Config can be loaded from a YAML/JSON pipeline file (the 80% path), env vars, or a `.env` file via the helpers in `faucet_core::config`. Inspect the full JSON Schema with:

```bash
faucet schema sink parquet
```

## Library usage

```rust,no_run
use faucet_core::Sink;
use faucet_sink_parquet::{ParquetCompression, ParquetSink, ParquetSinkConfig};
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let cfg = ParquetSinkConfig::local("/tmp/events")
    .compression(ParquetCompression::Snappy)
    .max_rows_per_file(100_000);

let sink = ParquetSink::new(cfg).await?;
sink.write_batch(&[
    json!({"id": 1, "name": "alice"}),
    json!({"id": 2, "name": "bob"}),
])
.await?;
// `flush()` writes the Parquet footer. Skipping it leaves no visible file.
sink.flush().await?;
# Ok(()) }
```

The builder methods (`compression`, `row_group_size`, `max_rows_per_file`, `max_bytes_per_file`, `schema`, `with_batch_size`) and the `ParquetSinkConfig::local(path)` / `ParquetSinkConfig::new(destination)` constructors all chain. Call `cfg.validate()` to fail fast on bad values before building the sink.

## How it works

The S3 (or local) `object_store` client is built once in `new()` and reused for the lifetime of the sink. Incoming JSON records are decoded into Arrow `RecordBatch`es via `arrow_json::Decoder` and paged through an `AsyncArrowWriter` with bounded buffering — the writer is opened lazily on the first batch so the schema is inferred from real data. Column data is written with the configured codec and `row_group_size`. Writing 1,000,000 rows of `{"id": i64, "name": utf8}` to a single Snappy file completes in well under 5 seconds on a recent laptop; throughput depends on row width, codec, and storage medium — benchmark with your own data.

## Reading the output back

Files produced by this sink are standard Apache Parquet and read back with any Parquet engine — DuckDB, Spark, pandas/pyarrow, BigQuery/Athena external tables — or with the companion [`faucet-source-parquet`](https://crates.io/crates/faucet-source-parquet) crate, which decodes them back to JSON records using the same Arrow schema. Because every inferred field is nullable, columns absent from some records read back as `NULL`.

## Lineage dataset URI

`file://<path>` (local) or `s3://<bucket>/<prefix>` (S3) — e.g. `file:///tmp/output/` or `s3://my-bucket/data/`.

This connector reports observability metrics under the label `connector="parquet"`.

## Local output retention

The sink records every local file it opens, so faucet's local-output retention GC
(`faucet cleanup`, the `faucet serve` sweeper, and the console's Datasets page)
can reclaim them. Provenance is captured at the **first** open, before the file is
created:

- a path that did not exist is faucet's own output and is collectable;
- a path that **already existed** is flagged `pre_existing` and is **never**
  deleted — faucet wrote to a file it did not create, and no retention window
  makes deleting that acceptable.

In rollover mode each UUID-named part is recorded **individually**, so cleanup is
always a list of files — the directory holding them is never an output and is
never removed. An S3 destination records nothing (there is no local file to
collect); remote-object cleanup is out of scope.

The GC only ever deletes recorded paths — never a glob, never a directory. Set
`local_outputs.retention_days` in the config to change the window for a
pipeline's outputs (`0` keeps them forever), or `local_outputs.track: false` to
keep them out of the ledger entirely.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-parquet` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| No file appears after a run (rollover / directory / S3 mode) | `flush()` was never called (the multipart upload was aborted on drop). Run via the faucet pipeline, which flushes on success/error/cancel; in library code call `sink.flush().await?`. (Single-file mode writes its footer on drop, so it does not need a final `flush()` to produce a file — though running via the pipeline still flushes.) |
| `FaucetError::Config: row_group_size` | `row_group_size` is `0`. Set it to `≥ 1` (or omit to use the default). |
| `FaucetError::Config` on `max_rows_per_file` / `max_bytes_per_file` | The threshold is `0`. Use `> 0`, or omit for single-file mode. |
| `FaucetError::Config` on `destination` | Empty `path` (local) or empty `bucket` (S3). Provide a non-empty value. |
| `FaucetError::Config` on `schema` | `type: explicit` is reserved/unsupported, or `inferred` `sample_size` is `0`. Use `inferred` with `sample_size ≥ 1`, or omit `schema`. |
| `FaucetError::Sink` naming a field + two types | Type drift — a later batch's value type disagrees with the inferred schema. Cast the field upstream (e.g. a `cast` transform) so every record agrees. |
| A field is missing from the output | It wasn't in the current file's first batch (so not in that file's inferred schema) and was dropped — check the one-shot-per-file warn log naming the field. The schema is re-inferred per file, so a widened field is captured in the next file once a rollover boundary is crossed; in single-file mode (no rollover) ensure the widest record shape appears in the opening batch, or widen `sample_size`. Set `max_rows_per_file` / `max_bytes_per_file` if you need files to roll more often so a mid-run column addition is picked up sooner. |
| Single-file `.parquet` path ignored; UUID files appear | You set a rollover threshold on a fixed `.parquet` path. Drop the threshold for single-file mode, or use a directory destination. |
| S3 `403`/credential errors | Credentials missing from the AWS chain, or wrong region. Set `region` and the AWS env vars/profile. |
| Connecting to MinIO/LocalStack fails | Set `endpoint_url` and `allow_http: true` (required for `http://` endpoints). |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [faucet-source-parquet](https://crates.io/crates/faucet-source-parquet) · [faucet-sink-s3](https://crates.io/crates/faucet-sink-s3)

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — rows stream into an open file that may roll over mid-batch. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
