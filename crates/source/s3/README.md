# faucet-source-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-source-s3.svg)](https://crates.io/crates/faucet-source-s3)
[![Docs.rs](https://docs.rs/faucet-source-s3/badge.svg)](https://docs.rs/faucet-source-s3)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-s3.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-s3.svg)](https://github.com/faucet-hq/faucet-stream#license)

An **AWS S3** source that lists objects under a prefix and parses them as JSON Lines, a JSON array, or raw text. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

Built on the official `aws-sdk-s3` client (built once, reused across every read) with concurrent object fetches via `buffer_unordered`, it pulls a whole bucket prefix into any faucet-stream sink as fast as the storage and your parallelism allow. Point it at MinIO / LocalStack / Cloudflare R2 with a custom `endpoint_url`, and let the standard AWS credential chain handle auth.

## Feature highlights

- **Three file formats** — `json_lines` (one record per line), `json_array` (one record per array element), and `raw_text` (one `{key, content}` record per object).
- **Apache Parquet (Arrow columnar)** — behind the `arrow` feature, a fourth format `file_format: parquet` decodes each object via the Arrow Parquet reader and, when the sink is also Arrow-native (Parquet / Delta), moves records end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` in between. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).
- **Parallel object reads** — up to `concurrency` objects fetched at once via `futures::buffer_unordered` (default 10).
- **True line-level streaming** — for `json_lines` / `raw_text`, object bodies are decoded line-by-line via `tokio::io::AsyncBufReadExt`, so client memory is bounded at `O(batch_size)` regardless of file or scan size.
- **Prefix listing with pagination** — handles truncated `ListObjectsV2` responses transparently and honours an optional `max_objects` cap.
- **S3-compatible** — custom `endpoint_url` for MinIO, LocalStack, R2, and other S3-API services.
- **Optional compression** — behind the `compression` feature: `gzip` / `zstd` / `auto` (suffix-detected per object key), so a single run can mix compressed and uncompressed objects.
- **Read-integrity verification** — every object's byte length is checked against the `Content-Length` S3 advertises (`verify_length`, default on), so a cleanly-truncated transfer is rejected instead of silently parsed as a complete object. Opt into full checksum verification (`verify_checksum`) for SHA-256 / CRC-32 / CRC-32C / CRC-64-NVME / ETag-MD5.
- **Matrix-friendly** — `${parent.field}` tokens in `prefix` are substituted per parent record in parent/child pipelines.

## Installation

```bash
# As a library:
cargo add faucet-source-s3
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-s3

# With compression support:
cargo install faucet-cli --features "source-s3,compression"
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features source-s3
```

`source-s3` is opt-in — it is not part of the CLI/umbrella default feature set.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: s3_to_postgres
pipeline:
  source:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/2026/
      region: us-east-1
      file_format: json_lines
      concurrency: 16
  sink:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/warehouse
      table_name: events_raw
      column_mapping:
        type: jsonb
        column: payload
```

```bash
export AWS_ACCESS_KEY_ID=...    # standard AWS credential chain
export AWS_SECRET_ACCESS_KEY=...
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | *(required)* | S3 bucket name. |
| `prefix` | string | *(none)* | Object-key prefix filter. Only objects whose key starts with this prefix are read. |
| `region` | string | *(SDK chain)* | AWS region. `None` defers to the SDK default (env vars, profile, or instance metadata). |
| `endpoint_url` | string | *(none)* | Custom endpoint URL for S3-compatible services (MinIO, LocalStack, R2). |
| `file_format` | enum | `json_lines` | How object bodies are parsed — `json_lines`, `json_array`, or `raw_text`. See below. |
| `max_objects` | int | *(all)* | Cap on the number of objects read. `None` reads every matching object. |
| `concurrency` | int | `10` | Maximum number of objects fetched concurrently. Clamped to `≥ 1` at runtime. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. See [Streaming & batching](#streaming--batching). Rejected above `MAX_BATCH_SIZE` (1,000,000) by `faucet_core::validate_batch_size`. |
| `compression` | enum | `auto` | *(requires the `compression` feature)* Per-object decompression codec — `none` / `gzip` / `zstd` / `auto`. |
| `verify_length` | bool | `true` | Verify each object's byte count against the advertised `Content-Length`; a short (truncated) or over-long transfer fails with `FaucetError::Source`. Skipped (debug-logged) when the store reports no length. Disable only for a store with unreliable `Content-Length`. See [Read-integrity verification](#read-integrity-verification). |
| `verify_checksum` | bool | `false` | Also verify the body against the checksum S3 stores for the object. Sets `ChecksumMode::Enabled` on each `GetObject` and checks the strongest available of SHA-256 / CRC-64-NVME / CRC-32C / CRC-32 / non-multipart ETag-MD5. Costs a hash over the full body. |

### File formats (`S3FileFormat`)

| Variant | Wire value | Record output |
|---------|-----------|---------------|
| JSON Lines (default) | `json_lines` | One record per non-empty line. Blank lines are skipped. |
| JSON array | `json_array` | One record per array element. The object must be a top-level JSON array. |
| Raw text | `raw_text` | One record per object: `{"key": "<object-key>", "content": "<file-text>"}`. |
| Apache Parquet | `parquet` | One record per Parquet row (Arrow-decoded). *(Requires the `arrow` feature — see [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).)* |

## Read-integrity verification

Object bodies are read through a verifying reader that validates the transfer at
EOF, so a stream that ends early but *cleanly* (a truncated transfer that still
yields a clean EOF) is rejected instead of being parsed and emitted as a
complete object — silent data loss otherwise.

- **Length** (`verify_length`, default `true`) — counts the bytes read and
  compares them against the `Content-Length` S3 advertised. A mismatch (short or
  long) fails the page with `FaucetError::Source`. Cheap; runs over the bytes
  that are read anyway. If the store reports no `Content-Length`, the check is
  skipped with a debug log rather than failing.
- **Checksum** (`verify_checksum`, default `false`) — sets
  `ChecksumMode::Enabled` on each `GetObject` and verifies the body against the
  strongest checksum S3 has stored for the object, in order: SHA-256,
  CRC-64-NVME, CRC-32C, CRC-32, then the ETag as MD5 for a non-multipart upload.
  Costs a hash over the full body. If the object advertises no checksum this
  source can verify, a one-shot warning is logged and the length check still
  applies.

Both checks operate on the **stored** bytes (below decompression), so they work
unchanged for `compression`-enabled `.gz` / `.zst` objects.

Both keys are named and behave identically across the S3, GCS, and Azure Blob
sources (Azure exposes no body checksum and rejects `verify_checksum`).

```yaml
pipeline:
  source:
    type: s3
    config:
      bucket: my-bucket
      prefix: events/
      verify_checksum: true   # length check is already on by default
```

## Authentication

This source uses the **standard AWS SDK credential chain** — there are no credential fields in the config. Credentials resolve automatically, in order:

1. Environment variables — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.
2. Shared AWS config files — `~/.aws/credentials`, `~/.aws/config` (honours `AWS_PROFILE`).
3. IAM roles — EC2 instance metadata, ECS task roles, or Lambda execution roles.

For MinIO / LocalStack, set `endpoint_url` and supply the service's access/secret keys via the same environment variables.

## Examples

### JSON array files from MinIO (S3-compatible)

```yaml
version: 1
pipeline:
  source:
    type: s3
    config:
      bucket: local-bucket
      prefix: exports/
      region: us-east-1
      endpoint_url: http://localhost:9000
      file_format: json_array
  sink:
    type: jsonl
    config:
      path: ./exports.jsonl
```

### Raw text files into a document store

```yaml
version: 1
pipeline:
  source:
    type: s3
    config:
      bucket: documents-bucket
      prefix: reports/
      file_format: raw_text
      max_objects: 50
  sink:
    type: mongodb
    config:
      connection_url: mongodb://localhost:27017
      database: lake
      collection: reports
```

Each record is `{"key": "reports/q1.txt", "content": "…"}`.

### Compressed JSONL with auto codec detection

```yaml
version: 1
pipeline:
  source:
    type: s3
    config:
      bucket: my-data-lake
      prefix: logs/2026/
      file_format: json_lines
      concurrency: 32
      compression: auto    # .gz → gzip, .zst → zstd, otherwise none
  sink:
    type: stdout
    config:
      format: jsonl
```

`auto` resolves the codec per object key, so a prefix holding a mix of `.jsonl`, `.jsonl.gz`, and `.jsonl.zst` objects all read correctly in one run. Requires building with the `compression` feature.

### One page per object for a load-job sink

```yaml
version: 1
pipeline:
  source:
    type: s3
    config:
      bucket: my-data-lake
      prefix: staging/
      file_format: json_lines
      batch_size: 0      # one StreamPage per S3 object
  sink:
    type: bigquery
    config:
      project_id: my-project
      dataset_id: analytics
      table_id: events
```

## Streaming & batching

`S3Source` implements `Source::stream_pages`: it lists matching objects, then streams their records into `StreamPage`s without buffering the whole scan. The per-page record count comes from the config `batch_size` field (the trait-level `batch_size` argument is ignored so a pipeline hint can't silently override an explicit config value).

Behaviour by format:

- **`json_lines`** — the object body is decoded line-by-line via `tokio::io::AsyncBufReadExt::lines`, so client-side memory stays bounded at `O(batch_size)` lines regardless of file or scan size. Records flow across object boundaries — a single page may carry lines drawn from multiple objects.
- **`raw_text`** — each object contributes exactly one `{key, content}` record. The whole body is held in one `String` (the record *is* the file), then accumulated into the same `batch_size` buffer.
- **`json_array`** — the array can only be validated once the closing `]` is observed, so each object is buffered fully, then its elements are chunked into pages of `batch_size`. This bounds *page* size but not *peak per-object* memory.

`batch_size = 0` is the **"no batching" sentinel**: every emitted `StreamPage` corresponds to exactly one S3 object — no within-object chunking and no cross-object accumulation. Use it for small lookup files, or for downstream sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage uploads) that prefer one large request per file to many small ones.

> **Memory ceiling — `raw_text` / `json_array`.** Both formats hold one whole decoded object in memory at a time (inherent: `raw_text`'s record *is* the file, and a JSON array isn't valid until its closing `]`). Because objects are fetched concurrently, peak memory is roughly **`concurrency` × (largest object's decoded size)**, not `batch_size`. For large `raw_text` / `json_array` objects, lower `concurrency` to cap peak memory, or re-emit the data as `json_lines` upstream so it streams line-by-line.

The S3 source has **no incremental-replication mode** today, so every emitted page carries `bookmark: None`. It does not implement resume/state, effectively-once, write modes, or a dead-letter queue (those are sink- or CDC-source-specific capabilities).

## Arrow columnar (Parquet) mode

Behind the crate-local `arrow` Cargo feature, `file_format: parquet` reads each object as an Apache Parquet file through the in-memory Arrow Parquet reader. The same decode serves two paths:

- the ordinary **row path** — each Parquet `RecordBatch` is converted to JSON records, exactly like the other formats; and
- the opt-in **columnar fast path** (RFC 0002 / #375) — when the sink is also Arrow-native (the [Parquet](https://crates.io/crates/faucet-sink-parquet) or [Delta Lake](https://crates.io/crates/faucet-sink-delta) sink) and no `Value`-shaped transform is configured, records move end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` materialization.

`supports_columnar()` is `true` only when `file_format: parquet`. If either end of the pipeline isn't Arrow-native — or a `Value`-shaped transform sits in between — the run transparently falls back to the row path.

```yaml
# s3(parquet) → parquet — runs Arrow end-to-end
pipeline:
  source:
    type: s3
    config:
      bucket: my-data-lake
      prefix: events/2026/
      region: us-east-1
      file_format: parquet   # requires the `arrow` feature
  sink:
    type: parquet
    config:
      path: ./out/
```

Enable it with `cargo add faucet-source-s3 --features arrow` (library) or `cargo install faucet-cli --features "source-s3,arrow"` (CLI).

## Compression

Behind the crate-local `compression` Cargo feature. Adds the `compression` config field with values `none`, `gzip`, `zstd`, or `auto` (the default — detects `.gz` / `.zst` from the object key).

```yaml
type: s3
config:
  bucket: my-data-lake
  prefix: logs/
  compression: auto    # or 'gzip' | 'zstd' | 'none'
```

The codec resolves per object key at read time, so a single source can read a mix of compressed and uncompressed objects in one run. When an explicit codec disagrees with the key suffix, a one-time warning is logged. Decompression is streamed straight into the line reader — there is no separate raw + decompressed copy held at once.

## Config loading

```rust,no_run
use faucet_core::config::{load_json, load_env_file};
use faucet_source_s3::S3SourceConfig;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config: S3SourceConfig = load_json("config.json")?;
let config: S3SourceConfig = load_env_file(".env", "S3_SOURCE")?;
# Ok(()) }
```

Example `.env`:

```env
S3_SOURCE_BUCKET=my-data-lake
S3_SOURCE_PREFIX=raw/events/
S3_SOURCE_REGION=us-east-1
S3_SOURCE_CONCURRENCY=10
```

## Schema introspection

```bash
faucet schema source s3
```

Programmatically:

```rust,no_run
use faucet_core::Source;
# async fn example(source: faucet_source_s3::S3Source) -> Result<(), Box<dyn std::error::Error>> {
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
# Ok(()) }
```

## Library usage

```rust,no_run
use faucet_source_s3::{S3Source, S3SourceConfig, S3FileFormat};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = S3SourceConfig::new("my-data-bucket")
        .prefix("exports/2026/")
        .region("us-west-2")
        .file_format(S3FileFormat::JsonLines)
        .concurrency(20)
        .with_batch_size(5000);

    let source = S3Source::new(config).await?;
    let records = source.fetch_all().await?;
    println!("Read {} records from S3", records.len());
    Ok(())
}
```

To run a full pipeline, wrap the source with any sink in `faucet_core::Pipeline` or drive `faucet_core::run_stream` for bounded-memory streaming.

## How it works

- **Client reuse** — the `aws-sdk-s3` `Client` is built once in `S3Source::new()` (resolving region and `endpoint_url`) and reused for every list and get. No per-object client construction.
- **Paginated listing** — `list_objects_v2` follows `next_continuation_token` until the response is no longer truncated, stopping early once `max_objects` is reached.
- **Parallel reads** — listed keys are read through `futures::stream::buffer_unordered(concurrency)`, so up to `concurrency` `GetObject` calls are in flight at once.
- **Streaming decode** — `json_lines` / `raw_text` open each body as an `AsyncBufRead` and decode line-by-line; only `json_array` buffers a full object before parsing.

Throughput scales with `concurrency` and is ultimately bounded by S3 / network bandwidth — benchmark with your own object sizes and parallelism.

## Dataset discovery

The source supports live introspection via `Source::discover()`: one `ListObjectsV2` delimiter (`/`) listing under the configured `prefix` (bucket root when unset) enumerates the "directories" directly below it, returning one dataset descriptor per common prefix with:

- `name` — the full key prefix (e.g. `raw/orders/`), `kind: prefix`
- `config_patch` — `{ "prefix": "raw/orders/" }`, ready to deep-merge over the connection config as a matrix row

When the listing returns no common prefixes but does return objects directly under the prefix (a "leaf directory"), each object becomes a descriptor instead (`kind: object`, `config_patch: { "prefix": "<full key>" }`), capped at the single listing page of 1000. `schema` and `estimated_rows` are never set — either would require reading or paging the whole listing. Discovery issues exactly one listing call and never recurses.

## Lineage dataset URI

`s3://<bucket>` or `s3://<bucket>/<prefix>` — e.g. `s3://my-bucket/data/2026/`.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field (`none`/`gzip`/`zstd`/`auto`); pulls in `faucet-core/compression`. |
| `arrow` | off | Adds the `file_format: parquet` value and the Arrow columnar fast path (`Source::stream_batches`); pulls in `faucet-core/arrow`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |

Enable the connector itself in the CLI/umbrella via the `source-s3` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `S3 list objects error … 403` / credential errors | Credentials missing from the AWS chain, or wrong region. Set `region` and the AWS env vars / profile (or an IAM role). |
| `S3 get object error … NoSuchBucket` | `bucket` is wrong, or the region doesn't match the bucket's region. Verify both. |
| `S3 JSON parse error in '<key>' at line N` | A line in a `json_lines` object isn't valid JSON. Fix the source data, or switch to the format that matches the file. |
| `S3 expected JSON array in '<key>', got object` | `file_format: json_array` but the object isn't a top-level array. Use `json_lines` or `raw_text` instead. |
| `S3 read/decode error … not valid UTF-8?` | The object isn't UTF-8 text. This source reads text formats only; binary blobs aren't supported. |
| No records and no error | The `prefix` matched no objects (note S3 prefixes are case-sensitive and don't imply a trailing `/`), or `max_objects` is `0`. Check the prefix. |
| `compression` field rejected as unknown | The crate was built without the `compression` feature. Rebuild with `--features compression`. |
| Connecting to MinIO / LocalStack fails | Set `endpoint_url` to the service URL (e.g. `http://localhost:9000`) and supply the service's keys via the AWS env vars. |
| Out-of-memory on large `raw_text` / `json_array` objects | These formats buffer a whole object; peak memory ≈ `concurrency` × largest object. Lower `concurrency`, or re-emit as `json_lines`. |
| `batch_size` rejected at load | Value exceeds `MAX_BATCH_SIZE` (1,000,000). Use a smaller value, or `0` for the no-batching sentinel. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) · [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html)
- Related crates: [faucet-sink-s3](https://crates.io/crates/faucet-sink-s3) · [faucet-source-gcs](https://crates.io/crates/faucet-source-gcs) · [faucet-source-parquet](https://crates.io/crates/faucet-source-parquet)

## Sharded execution (cluster Mode B)

Under [`faucet serve --cluster`](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html),
a top-level `shard: { count: N }` block splits this source across cluster
workers **automatically — no connector config needed**. Each worker reads the
objects whose key hashes to its shard index (stable FNV-1a modulo `count`),
so the partition is disjoint and complete: every object is read by exactly one
worker, and the partition stays stable as new objects appear. Outside the
cluster coordinator a run reads every object, unchanged.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="s3"`.
