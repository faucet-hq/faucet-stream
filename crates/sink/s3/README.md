# faucet-sink-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-s3.svg)](https://crates.io/crates/faucet-sink-s3)
[![Docs.rs](https://docs.rs/faucet-sink-s3/badge.svg)](https://docs.rs/faucet-sink-s3)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-s3.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-s3.svg)](https://github.com/faucet-hq/faucet-stream#license)

AWS **S3** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to S3 (or any S3-compatible store) as JSON Lines (NDJSON) objects, one UUID-keyed object per chunk, uploaded concurrently via `buffer_unordered`.

Reach for it to land any faucet-stream source — a REST API, a database, a Kafka topic, a CDC stream — into an S3 data lake as newline-delimited JSON with one declarative config and no glue code. It's tuned to write a small number of large objects rather than a flood of tiny ones, which keeps downstream scans and PUT/LIST costs low.

## Feature highlights

- **JSON Lines (NDJSON) output** — each object is newline-delimited JSON, uploaded with `Content-Type: application/x-ndjson`. Reads back cleanly in Spark, Athena, DuckDB, `jq`, and the [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) JSONL reader.
- **Pipelined uploads** — a closed object uploads in the background while the next one is encoded, up to `concurrency` (default 10) in flight; a large object's multipart parts also go up `concurrency` at a time.
- **File splitting** — records accumulate across pages; `max_records_per_file` (else `batch_size`) caps records per object, `max_bytes_per_file` bytes.
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
| `format` | `json_lines` \| `json_array` \| `csv` \| `xml` \| `xlsx` \| `avro` \| `parquet` | `json_lines` | Object format. `parquet` (requires the `arrow` feature) writes self-contained Parquet files (ZSTD by default, `parquet.compression`) and enables the columnar fast path — see [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode); the rest are [file formats](#file-formats-604). |
| `file_extension` | string | `".jsonl"` | Extension appended to each object key. Append `.gz` / `.zst` here when using compression so consumers can detect the codec. |

### Batching & file splitting

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_records_per_file` | int | *(unset)* | Maximum records per object. When unset, all records in a `write_batch` call go to one object. |
| `concurrency` | int | `10` | Maximum uploads in flight: up to `concurrency` objects, each sending up to `concurrency` multipart parts, so at most `concurrency²` part requests (100 at the default). See [Streaming & batching](#streaming--batching). |
| `batch_size` | int | `1000` | Records per object when `max_records_per_file` is unset (without `path`). `0` = no record cap: one object per `flush` (Parquet: one per `write_batch`) — see [Streaming & batching](#streaming--batching). |

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


> **Shared destinations are refused at load.** With `path` set, the CLI refuses two matrix rows writing the same destination and a fan-out row without a per-invocation token (`${parent.id}`) in it: concurrent writers would overwrite each other's parts and prune the rest.

## Streaming & batching

Records accumulate across `write_batch` calls into one open object, built in a local scratch file. An object closes at the per-object cap — `max_records_per_file`, else `batch_size` (the smaller when both are set) — at `max_bytes_per_file`, or at `flush`, which the pipeline calls at every bookmark-carrying page and at the end of the run.

- **Uploads are pipelined.** A closed object is handed to a background upload and the sink keeps encoding the next one; up to `concurrency` uploads are in flight, and a close past that waits for a slot. Every `write_batch` waits for the uploads it started before returning, so a failed upload fails the page that wrote it (and a DLQ gets the right rows); `flush` and the overwrite commit likewise return only after every upload has landed, so a bookmark never passes an object that is not in the store. An aborted overwrite waits for in-flight uploads, then removes the staged objects.
- **`batch_size = 0` is the "no re-chunking" sentinel** (without `path`): no record cap. JSON Lines and the whole-object formats write one object per `flush`; Parquet writes one object per `write_batch` call (as the sink always has), so a page stays one self-contained file. With `path` set, `batch_size` is ignored and objects follow the path's `{part}` rollover.
- Scratch files are local and never fsynced — they are uploaded, then deleted.

Many tiny objects are a well-known anti-pattern (per-request overhead, slower downstream scans, LIST/PUT cost), so size objects with `max_records_per_file` / `max_bytes_per_file` rather than a small `batch_size`.

This connector reports observability metrics under the label `connector="s3"`.


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

1. `new()` validates the config and builds an S3 client **once** via the AWS SDK default credential chain, applying `region` / `endpoint_url` overrides if set.
2. `write_batch()` hands the page to the shared file writer, which encodes it into the open object's local scratch file.
3. The object closes at the row / byte cap or at `flush` and is uploaded once — a single `PutObject` up to 8 MiB, a multipart upload beyond it — with a `Content-Type` from its extension.
4. `compression` (the `compression` feature) is applied to the whole object before upload, resolved from `path` or `file_extension`.

## Object key format

Without `path`, each object is keyed `{prefix}{run id}-{part}{file_extension}`, where the run id is a time-ordered UUID fresh for each sink instance and `{part}` counts objects from `00001`. With `prefix = "events/"` and `file_extension = ".jsonl"`:

```
events/01a0ecba-c433-70b7-9245-73029acfe6c2-00001.jsonl
```

Keys are never reused, so re-runs add new objects rather than overwriting — treat the prefix as append-only, or set `path` for deterministic names.

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

## File formats (#604)

Beyond JSON Lines this sink writes **JSON array**, **CSV**, **XML** and
**Excel** through `faucet_core::file_format`, so what it writes is exactly what
the file *sources* can read back.

```yaml
sink:
  type: s3
  config:
    bucket: reports
    prefix: monthly/
    format: xlsx
    file_extension: .xlsx
    excel: { sheet: Orders }
```

| Option block | Applies to | Fields |
|---|---|---|
| `csv` | `csv` | `delimiter` (one byte; `"\t"` for tabs), `has_headers` (default `true`) |
| `xml` | `xml` | `record_element`, `root_element` |
| `excel` | `xlsx` | `sheet` (worksheet name) |

Enable with `--features file-formats` (or one of `file-format-csv` /
`file-format-xml` / `file-format-excel`). Format composes with `compression`.

**Only `json_lines` can be built a record at a time.** Every other format has a
header, a document element, a container index or a pair of brackets, so its
records are buffered and encoded together at the rollover — bounded by the same
`max_records_per_file` / `max_bytes_per_file` caps, so object sizing means the
same thing whatever the format. Columns are the union of every record's keys in
the group, so a record that gains a field mid-page widens the file rather than
losing it. See the
[file-formats cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/file-formats.html).

## Avro (#719)

`format: avro` writes one Avro Object Container File per object, encoded
against `avro.schema` or against a schema inferred from that object's records.
Nullable and absent fields become `["null", T]`, mixed-type fields become
`string`, and invalid names are sanitized (the original is kept in
`faucet.name`). The block codec comes from `avro.codec`: `null` (default),
`deflate`, `snappy` or `zstd`. With an explicit schema, logical types
(`decimal`, `date`, `timestamp-*`, `uuid`, …) accept their string forms or
epoch integers.

```yaml
format: avro
file_extension: .avro
avro:
  codec: zstd
```

ORC is read-only, so there is no `orc` format here. Enable with
`file-format-avro` (or `file-formats`).

## Shared file writer (#777)

This sink writes through the same file-writing layer as the local
[`file` sink](https://crates.io/crates/faucet-sink-file), so it takes every
format and option the file sink does, with the same field names:

**Experimental** (PRINCIPLES.md §3): this block's shape may change in a minor release; any change is called out in the changelog.

| Field | Values | Notes |
|---|---|---|
| `format` | `json_lines` (default), `json_array`, `csv`, `xml`, `xlsx`, `avro`, `parquet`, `raw_text`, `auto` | `auto` takes the format from `path`'s extension (else `file_extension`), looking through `.gz` / `.zst`. `parquet` needs the `arrow` feature; the other shared formats their `file-format-*` feature. |
| `path` | a name template | object name template: `{part}` numbers the objects, `${now.*}` tokens work, a trailing `/` is a directory of `part-{part}<extension>` objects. |
| `if_exists` | `replace` (default), `append`, `error` | What happens when an object of the same name exists. `append` works for JSON Lines, CSV and raw text, or with `{part}` for every format. `mode` is accepted as another name for this key, and `overwrite` / `error_if_exists` for its values. |
| `write_mode` | `append` (default), `overwrite` | `overwrite` writes the run's objects under a hidden `.faucet-overwrite-…/` prefix and moves them into place only after a successful run; a failed run leaves the old output untouched. The move is one object at a time: a reader listing the prefix while it runs can see new objects beside old ones, and a move that stops half-way is finished by the next run. |
| `parquet` | `compression` (`none`/`snappy`/`gzip`/`zstd`/`lz4`, default **`zstd`** — the local `file` sink defaults to `snappy`), `row_group_size`, `schema` (explicit fields) | The schema is inferred from each object's first page and widened by later pages. |
| `json_lines` | `pretty` | |
| `encryption` | `{ key: … }` | Encrypt at rest (the `encryption` feature); read back by the `file` source. |
| `scratch_dir` | a local directory | Where objects are built before upload (default: the system temporary directory; a private subdirectory is created in it). JSON Lines and raw text go up as a multipart upload while they are written and need no scratch space; other formats need room for each object being built (up to `concurrency` of them). Scratch files are not encrypted while the run is in progress. |

`if_exists: append` / `error` and `write_mode: overwrite` need `path`: without it every run
writes new, uniquely named objects (`<run id>-<part><file_extension>`), so
there is nothing to replace or append to.

**Publishing.** JSON Lines and raw text go up as a multipart upload while they are written — memory holds at most `concurrency` parts, and no scratch file is written — and the object becomes visible when it closes. Every other object is built in a local scratch file and published
with one upload (multipart past 8 MiB, parts in parallel up to `concurrency`, completed only after every part landed and aborted on failure) when it closes — at `max_records_per_file` /
`max_bytes_per_file` (encoded bytes) or at `flush` — so a reader never sees a
partial object, and a bookmark never advances past records that are not
there.

Object keys are `prefix + path`.

```yaml
sink:
  type: s3
  config:
    # … connection fields …
    path: "dt=${now.date}/part-{part}.parquet"
    format: auto
    max_records_per_file: 1000000
    parquet: { compression: zstd, row_group_size: 131072 }
```

## Batch atomicity

What a failed write leaves behind (#737): **atomic** without a rollover cap —
a page is encoded locally and published only at `flush` — otherwise
**best-effort**: objects closed at an earlier cap stay. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Usage signals (#704)

Every request is reported to faucet's usage meter as a sink round trip, by op:

| Op | Requests | Priced as |
|---|---|---|
| `put` | `PutObject`, and each multipart create / part / complete | write (`usage.pricing.object_storage.write_per_1k_requests`) |
| `copy` | `CopyObject`, and each multipart-copy create / `UploadPartCopy` (an overwrite commit's promote) | write |
| `list` | each `ListObjectsV2` page (append, overwrite pruning and commit) | read (`read_per_1k_requests`) |
| `head` | `HeadObject` (existence checks, and the size probe before a promote) | read |
| `delete` | `DeleteObject` | free (S3 does not bill deletes) |

See the [usage cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/usage.html).

## Server-side copies and retries (#783)

An overwrite commit promotes each staged object with a server-side copy and a
delete. `CopyObject` copies at most 5 GiB, so a larger object is copied with a
multipart `UploadPartCopy` in 512 MiB ranges (grown so no copy exceeds 10 000
parts), `concurrency` at a time, completed only after every part landed and
aborted on any failure.

A request that fails with HTTP 429 or a 5xx (`SlowDown`,
`ServiceUnavailable`, …) is reported as a typed `HttpStatus` error, so a
pipeline [`resilience:`](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html)
policy retries it; any other failure is a plain sink error.
