# faucet-sink-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-s3.svg)](https://crates.io/crates/faucet-sink-s3)
[![Docs.rs](https://docs.rs/faucet-sink-s3/badge.svg)](https://docs.rs/faucet-sink-s3)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-s3.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-s3.svg)](https://github.com/faucet-hq/faucet-stream#license)

AWS **S3** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to S3 (or any S3-compatible store) as JSON Lines (NDJSON) objects, one UUID-keyed object per chunk, uploaded concurrently via `buffer_unordered`.

Reach for it to land any faucet-stream source — a REST API, a database, a Kafka topic, a CDC stream — into an S3 data lake as newline-delimited JSON with one declarative config and no glue code. It's tuned to write a small number of large objects rather than a flood of tiny ones, which keeps downstream scans and PUT/LIST costs low.

## Feature highlights

- **JSON Lines (NDJSON) output** — each object is newline-delimited JSON, uploaded with `Content-Type: application/x-ndjson`. Reads back cleanly in Spark, Athena, DuckDB, `jq`, and the [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) JSONL reader.
- **Concurrent uploads** — chunks are serialized up front and uploaded in parallel via `futures::stream::buffer_unordered`, bounded by `concurrency` (default 10).
- **File splitting** — `max_records_per_file` caps records per object; `batch_size` re-chunks each `write_batch` call. The effective per-object cap is the smaller of the two.
- **S3-compatible endpoints** — point `endpoint_url` at MinIO, LocalStack, Cloudflare R2, Backblaze B2, or any S3 API.
- **AWS credential chain** — credentials resolve through the standard AWS SDK chain (env vars, shared credentials file, IAM instance/task roles, SSO) — no secrets in the config.
- **Optional compression** — gzip / zstd / auto behind the crate-local `compression` feature; the codec auto-resolves from the file extension.
- **Apache Parquet (Arrow columnar)** — behind the `arrow` feature, `format: parquet` writes each object as a complete, self-contained ZSTD-compressed Parquet file and enables the columnar fast path, so a Parquet/Delta source can stream Arrow `RecordBatch`es straight through with no `serde_json::Value` in between. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).
- **Client built once** — the S3 client is constructed eagerly in `new()` and reused for every upload.
- **Preflight `check()`** — `faucet doctor` issues a non-mutating `HeadBucket` to confirm the bucket is reachable and credentials work, uploading nothing.

## Installation

```bash
# As a library:
cargo add faucet-sink-s3
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-s3
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features sink-s3
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
      records_path: $.events[*]
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/raw/
      region: us-east-1
      max_records_per_file: 10000
```

```bash
faucet run pipeline.yaml
```

This writes `s3://my-data-lake/events/raw/<uuid>.jsonl` objects, each holding up to 10,000 records.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | — *(required)* | S3 bucket name. |
| `prefix` | string | `""` | Key prefix for written objects (e.g. `"data/events/"`). Combined as `{prefix}{uuid}{file_extension}`. |
| `region` | string | *(SDK default)* | AWS region. When unset, the AWS SDK resolves it from the environment / config. |
| `endpoint_url` | string | *(unset)* | Custom endpoint for S3-compatible services (MinIO, LocalStack, R2, …). |
| `format` | `json_lines` \| `parquet` | `json_lines` | Object format. `parquet` (requires the `arrow` feature) writes self-contained ZSTD-compressed Parquet files and enables the columnar fast path — see [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |
| `file_extension` | string | `".jsonl"` | Extension appended to each object key. Append `.gz` / `.zst` here when using compression so consumers can detect the codec. |

### Batching & file splitting

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_records_per_file` | int | *(unset)* | Maximum records per object. When unset, all records in a `write_batch` call go to one object. |
| `concurrency` | int | `10` | Maximum number of concurrent object uploads. Bounds peak memory together with object size. |
| `batch_size` | int | `1000` | Records per object written by a single `write_batch` call (write-side re-chunking). `0` = no re-chunking — see [Streaming & batching](#streaming--batching). **`0` is the recommended value for S3.** |

### Format (compression feature)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | `none` \| `gzip` \| `zstd` \| `auto` | `auto` | Object-body codec. `auto` resolves from `file_extension`. Requires the crate-local `compression` feature. See [Compression](#compression). |

## Examples

### Sharded JSONL from a REST API

```yaml
# Adapted from cli/examples/rest_to_s3.yaml
version: 1
name: rest_to_s3
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
      records_path: $.events[*]
      pagination:
        type: Offset
        offset_param: offset
        limit_param: limit
        limit: 500
        total_path: $.meta.total
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/raw/
      region: us-east-1
      file_extension: .jsonl
      max_records_per_file: 10000
      concurrency: 8
```

### Dated prefix driven by `${now.*}`

```yaml
pipeline:
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/dt=${now.date}/    # e.g. events/dt=2026-06-17/
      region: us-east-1
      batch_size: 0                      # let the source size each object
```

### Compressed objects (gzip)

```yaml
pipeline:
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/raw/
      file_extension: .jsonl.gz   # auto-resolves to gzip
      region: us-east-1
```

### MinIO / LocalStack for local development

```yaml
pipeline:
  sink:
    type: s3
    config:
      bucket: test-bucket
      prefix: dev/
      endpoint_url: http://localhost:9000
      region: us-east-1
```

## Streaming & batching

The pipeline calls `Sink::write_batch` once per upstream page. Inside a call, `batch_size` and `max_records_per_file` together decide how many objects that page becomes:

- The effective per-object cap is `min(batch_size, max_records_per_file)` when both are set, whichever single one is set, or **unbounded** when both are `0` / unset (the whole page becomes one object).
- With a cap of `M`, a page of `N` records is written as `ceil(N / M)` objects, each holding at most `M` records (the last holds the remainder).
- **`batch_size = 0` is the "no batching" sentinel:** the sink writes whatever upstream hands it without re-chunking (still honouring `max_records_per_file` if set).

**Recommended: `batch_size: 0`.** S3 is the canonical case where one large object beats many small ones — per-request overhead, slower downstream scans, and LIST/PUT cost all compound with tiny objects. Most sources already size each page via their own `batch_size` (REST page, sqlx cursor chunk, Kafka poll, …), so let that drive object sizing.

This connector reports observability metrics under the label `connector="s3"`.

> **Memory ceiling.** Each object's body is buffered fully in memory before a single-shot `PutObject` (and, with compression on, briefly held as both raw and compressed). Up to `concurrency` objects upload at once, so peak memory is roughly **`concurrency` × object-size × ~2**. Pair `batch_size: 0` with a *streaming* source that sizes its own pages, or cap memory via `max_records_per_file` / lower `concurrency`. Streaming multipart upload for very large objects is a future enhancement.

## Arrow columnar (Parquet) mode

Behind the crate-local `arrow` Cargo feature, `format: parquet` writes each object as a complete, self-contained Apache Parquet file (ZSTD-compressed) instead of JSON Lines. The sink implements the columnar `write_batch_columnar` fast path (RFC 0002 / #375): when the **source** is also Arrow-native — the [Parquet](https://crates.io/crates/faucet-source-parquet) or [Delta Lake](https://crates.io/crates/faucet-source-delta) source, or the [S3](https://crates.io/crates/faucet-source-s3) / [GCS](https://crates.io/crates/faucet-source-gcs) source in `file_format: parquet` mode — and no `Value`-shaped transform is configured, records move end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` materialization.

If either end of the pipeline isn't Arrow-native — or a `Value`-shaped transform sits in between — the run transparently falls back to the JSON row path.

```yaml
# s3(parquet) → s3(parquet) — runs Arrow end-to-end
pipeline:
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/parquet/
      region: us-east-1
      format: parquet   # requires the `arrow` feature
```

Enable it with `cargo add faucet-sink-s3 --features arrow` (library) or `cargo install faucet-cli --features "sink-s3,arrow"` (CLI).

## Compression

Behind the crate-local `compression` Cargo feature (`cargo add faucet-sink-s3 --features compression`, or `cargo install faucet-cli --features compression`). Adds the `compression` config field with values `none` / `gzip` / `zstd` / `auto`.

- `auto` (the default) resolves the codec from `file_extension`: `.gz` → gzip, `.zst` → zstd, anything else → none.
- Append `.gz` / `.zst` to `file_extension` so consumers can detect the codec from the object key.
- The S3 **`Content-Encoding` header is deliberately unset** — consumers must decompress explicitly (the codec lives in the key suffix, not the HTTP metadata).
- Resolution runs per-object, so a `${now.*}`- or matrix-driven extension can vary across a run.

```yaml
pipeline:
  sink:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/raw/
      file_extension: .jsonl.zst
      compression: auto    # or 'gzip' | 'zstd' | 'none'
```

## Config loading & schema

Load from YAML/JSON files or environment variables, and inspect the full JSON Schema:

```bash
faucet schema sink s3
```

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_s3::S3SinkConfig;

// From a JSON file
let config: S3SinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: S3SinkConfig = load_env_file(".env", "S3_SINK")?;
```

Example `.env`:

```env
S3_SINK_BUCKET=my-data-lake
S3_SINK_PREFIX=raw/events/
S3_SINK_REGION=us-east-1
S3_SINK_FILE_EXTENSION=.jsonl
S3_SINK_MAX_RECORDS_PER_FILE=50000
S3_SINK_CONCURRENCY=10
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_s3::{S3Sink, S3SinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = S3SinkConfig::new("my-data-bucket")
    .prefix("events/2026/06/")
    .region("us-east-1")
    .max_records_per_file(10_000)
    .concurrency(20);

let sink = S3Sink::new(config).await?;

let records = vec![
    json!({"id": 1, "event": "page_view", "user": "alice"}),
    json!({"id": 2, "event": "click", "user": "bob"}),
];
let written = sink.write_batch(&records).await?;
println!("Wrote {written} records to S3");
# Ok(())
# }
```

Driven by a `Pipeline`:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_s3::{S3Sink, S3SinkConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/events"));
let sink = S3Sink::new(
    S3SinkConfig::new("my-data-lake")
        .prefix("ingest/events/")
        .region("us-east-1")
        .max_records_per_file(100_000),
)
.await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records to S3", result.records_written);
# Ok(())
# }
```

## How it works

1. `new()` validates `batch_size` and builds an S3 client **once** via the AWS SDK default credential chain, applying `region` / `endpoint_url` overrides if set.
2. `write_batch()` computes the effective per-object cap (`min(batch_size, max_records_per_file)`) and splits the page into chunks.
3. Each chunk is serialized to a JSON Lines body and assigned a UUID-based key (`{prefix}{uuid}{file_extension}`) *before* any upload begins.
4. Chunks upload concurrently via `buffer_unordered(concurrency)` as single-shot `PutObject` calls with `Content-Type: application/x-ndjson`.
5. With the `compression` feature, each body is compressed in memory just before upload using the codec resolved from `file_extension`.

## Object key format

Each object is keyed `{prefix}{uuid}{file_extension}`. With `prefix = "events/"` and `file_extension = ".jsonl"`:

```
events/a1b2c3d4-e5f6-7890-abcd-ef1234567890.jsonl
```

UUID keys make writes idempotent-safe against collisions but mean re-runs append new objects rather than overwriting — downstream consumers should treat the prefix as append-only.

## Lineage dataset URI

`s3://<bucket>/<prefix>` — e.g. `s3://my-data-lake/events/raw/`.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field (gzip / zstd / auto) and compresses each object body before upload. Pulls in `faucet-core/compression`. |
| `arrow` | off | Adds the `format: parquet` value and the columnar fast path (`Sink::write_batch_columnar`); pulls in `faucet-core/arrow`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |

This is a write-only file sink: it does **not** support effectively-once delivery, upsert/delete write modes, or resumable bookmarks (UUID keys make every run append new objects).

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `S3 put object error … AccessDenied` | The resolved credentials lack `s3:PutObject` on the bucket/prefix. Grant the IAM principal `s3:PutObject` (and `s3:ListBucket` if `faucet doctor` is used) on `arn:aws:s3:::<bucket>/<prefix>*`. |
| `dispatch failure` / no credentials | The AWS SDK chain found no credentials. Set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, configure `~/.aws/credentials`, or run on a host with an instance/task role. |
| `NoSuchBucket` / `PermanentRedirect` | Bucket doesn't exist, or `region` doesn't match the bucket's region. Set `region` to the bucket's actual region. |
| Works against AWS but not MinIO/LocalStack | Set `endpoint_url` to the service URL and a non-empty `region` (e.g. `us-east-1`); S3-compatible stores still require a region string. |
| `Config: batch_size …` at startup | `batch_size` exceeds `MAX_BATCH_SIZE` (1,000,000). Lower it, or set `0` for no re-chunking. |
| Flood of tiny objects, slow downstream scans | `batch_size`/`max_records_per_file` are too small. Set `batch_size: 0` and let the source size each page, or raise `max_records_per_file`. |
| OOM / high memory under load | Large pages × `concurrency` are buffered in memory. Lower `concurrency`, set `max_records_per_file`, or feed from a streaming source that sizes its pages. |
| Compressed objects won't auto-decompress in a consumer | The `Content-Encoding` header is intentionally unset. Decompress by the key suffix (`.gz` / `.zst`), or use [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) with its `compression` feature. |

## See also

- [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html) — codecs, auto-detection, and the `Content-Encoding` note.
- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [CLI & config-file reference](https://faucet-hq.github.io/faucet-stream/reference/cli.html)
- [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) — read JSONL / JSON-array / raw-text objects back out of S3.
- [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) — the equivalent sink for Google Cloud Storage.
- [`faucet-sink-parquet`](https://crates.io/crates/faucet-sink-parquet) — columnar output to local or S3 with internal compression.


## Object rollover (`max_records_per_file` / `max_bytes_per_file`)

Records **accumulate across `write_batch` calls** and roll to a new object when
either cap is reached (#618). Before this, every upstream page became its own
object, so a small `batch_size` produced a swarm of tiny objects — the
small-files problem that dominates read time on a data lake, where per-object
overhead outweighs the bytes.

- `max_records_per_file` — record cap. When unset, `batch_size` still sizes
  objects, so an existing config keeps the object size it asked for; what
  changed is that a page *smaller* than the cap now joins the open object.
- `max_bytes_per_file` — byte cap, counted on the **uncompressed** body. Rows
  are a poor proxy for size (10k wide rows and 10k `{"id":1}` rows differ by
  orders of magnitude), and this is the axis that bounds buffered memory. A
  single record larger than the cap still gets its own object rather than
  being split or dropped.

With neither cap set the whole run lands in one object, closed at `flush` —
which the pipeline calls at every bookmark-carrying page and at the end, so
the remainder is always written before a bookmark advances.

Large objects stream through **multipart** upload: a part is sent as soon as it
fills and its buffer is dropped, so peak memory is O(part size) rather than
O(object size). The upload is started lazily on the first full part, so an
object that fits in one part stays a single request and leaves nothing
abandoned if the run dies early. With a `compression` codec configured each
part is compressed independently — gzip and zstd both concatenate, so the
object decodes transparently, and compressing the whole body instead would
mean buffering it, which is the bound multipart exists to remove.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
