# faucet-sink-gcs

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-gcs.svg)](https://crates.io/crates/faucet-sink-gcs)
[![Docs.rs](https://docs.rs/faucet-sink-gcs/badge.svg)](https://docs.rs/faucet-sink-gcs)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-gcs.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-gcs.svg)](https://github.com/faucet-hq/faucet-stream#license)

Google **Cloud Storage** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Serializes batches of `serde_json::Value` records as [JSON Lines](https://jsonlines.org/) (NDJSON) and uploads them concurrently to a GCS bucket with time-sortable UUIDv7 object keys.

Reach for it when you want to land data from any faucet-stream source — a database, an API, a queue, a CDC stream — into GCS as newline-delimited JSON, ready for BigQuery external tables, Dataflow, or any downstream reader. It is built on the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage) SDK and uses `buffer_unordered` to fan uploads out across the wire.

## Feature highlights

- **Concurrent uploads** — each batch is chunked and the chunks upload in parallel via `buffer_unordered(concurrency)` (default 10), so throughput scales with your network rather than serializing one PUT at a time.
- **Time-sortable keys** — every object is named `{prefix}{uuidv7}{file_extension}`. UUIDv7 embeds a timestamp, so a bucket listing returns objects in write order without any extra index.
- **Explicit NDJSON content type** — uploads are tagged `application/x-ndjson` so consumers and tooling recognize the format.
- **Flexible object sizing** — combine `batch_size` (pipeline-level chunking) and `max_records_per_file` (a hard per-object cap); the sink uploads at whichever limit is smaller.
- **Optional compression** — gzip or zstd per object behind the `compression` feature, with auto-detection from the file extension.
- **Apache Parquet (Arrow columnar)** — behind the `arrow` feature, `format: parquet` writes each object as a complete, self-contained ZSTD-compressed Parquet file and enables the columnar fast path, so a Parquet/Delta source can stream Arrow `RecordBatch`es straight through with no `serde_json::Value` in between. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).
- **Four credential modes** — Application Default Credentials, a service-account key file, inline service-account JSON, or anonymous (emulators). The shared `GcsCredentials` enum is re-exported from [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs), so it matches the GCS **source** byte-for-byte.
- **Client built once** — the authenticated `Storage` client is constructed in `new()` and reused for every upload.

## Installation

```bash
# As a library:
cargo add faucet-sink-gcs

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-gcs

# With compression support:
cargo install faucet-cli --features "sink-gcs,compression"
```

`sink-gcs` is opt-in — it is not part of the CLI's default feature set.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/events
  sink:
    type: gcs
    config:
      bucket: my-bucket
      prefix: events/
      auth:
        type: application_default
```

```bash
faucet run pipeline.yaml
```

This uploads each batch of records as one or more `events/{uuidv7}.jsonl` objects in `gs://my-bucket`.

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | — *(required)* | GCS bucket name (without the `gs://` scheme). |
| `prefix` | string | — *(required)* | Object-name prefix; concatenated with the UUIDv7 key and `file_extension` to form each object name. Use a trailing `/` for a folder-like layout (e.g. `events/2026/`). |
| `auth` | `GcsCredentials` | `application_default` | Authentication — see [Authentication](#authentication). |
| `format` | `json_lines` \| `json_array` \| `csv` \| `xml` \| `xlsx` \| `parquet` | `json_lines` | Object format. `parquet` (requires the `arrow` feature) writes self-contained ZSTD-compressed Parquet files and enables the columnar fast path — see [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode); the rest are [file formats](#file-formats-604). |
| `file_extension` | string | `.jsonl` | Suffix appended to every object name. Also drives compression auto-detection (`.jsonl.gz` → gzip). |

### Batching & throughput

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per uploaded object from a single `write_batch` call. **`0` = no batching** — the sink writes whatever upstream hands it as one object. Recommended value for GCS (see [Streaming & batching](#streaming--batching)). Validated at construction (`0` ≤ value ≤ `1_000_000`). |
| `max_records_per_file` | int | *(unset)* | Hard cap on records per uploaded object. `None` means no file-rollover cap (a single object per `write_batch` call, still subject to `batch_size`). When both are set, the **smaller** of `batch_size` and `max_records_per_file` wins. |
| `concurrency` | int | `10` | Maximum number of object uploads in flight at once. Higher = more throughput but more peak memory (see the memory note below). Clamped to a minimum of 1. |

### Compression *(feature `compression`)*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | `none` \| `gzip` \| `zstd` \| `auto` | `auto` | Codec applied to each object body. `auto` resolves from `file_extension` (`.gz` → gzip, `.zst` → zstd, otherwise none). This sink does **not** set the GCS `Content-Encoding` metadata — consumers must decompress explicitly. |

### Advanced

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_host` | string | *(unset)* | Endpoint override for the storage host. Integration-test escape hatch (e.g. `http://localhost:4443` for `fake-gcs-server`; a plaintext host uses the JSON API throughout) — leave unset in production. |

## Authentication

`auth` uses the shared `GcsCredentials` enum (the project-wide `{ type, config }` shape, snake_case discriminators). It defaults to `application_default` when omitted.

| `type` | `config` | Use when |
|--------|----------|----------|
| `application_default` *(default)* | *(none)* | Running on GCP (workload identity / GCE/GKE metadata server), with `GOOGLE_APPLICATION_CREDENTIALS` set, or after `gcloud auth application-default login`. |
| `service_account_json_file` | `{ path: <file> }` | You have a service-account key file on disk. |
| `service_account_json_inline` | `{ json: <string> }` | You want to inject the key JSON inline, typically via `${env:VAR}` / `${secret:…}` indirection. |
| `anonymous` | *(none)* | Talking to an emulator (e.g. `fake-gcs-server`) that does not validate bearer tokens. **Never use in production.** |

```yaml
# Application Default Credentials (workload identity, gcloud)
auth:
  type: application_default
```

```yaml
# Service-account key file
auth:
  type: service_account_json_file
  config:
    path: /run/secrets/sa.json
```

```yaml
# Inline service-account JSON via env indirection
auth:
  type: service_account_json_inline
  config:
    json: ${env:GCP_SA_JSON}
```

## Examples

### Large export with no re-chunking (recommended)

Let the source size its own pages and write one object per page — the standard, cost-efficient layout for GCS.

```yaml
sink:
  type: gcs
  config:
    bucket: my-bucket
    prefix: warehouse/daily/
    auth: { type: application_default }
    batch_size: 0   # accept upstream pages as-is, one object per page
```

### Hard cap per object with high concurrency

Bound each object at 50k records and fan 16 uploads out at once.

```yaml
sink:
  type: gcs
  config:
    bucket: my-bucket
    prefix: events/2026/
    auth:
      type: service_account_json_file
      config: { path: /run/secrets/sa.json }
    max_records_per_file: 50000
    concurrency: 16
```

### Date-partitioned, gzip-compressed output

Uses a `${now.*}` token for a dated prefix and gzip via the file extension (requires the `compression` feature).

```yaml
sink:
  type: gcs
  config:
    bucket: my-bucket
    prefix: events/dt=${now.date}/
    auth: { type: application_default }
    file_extension: .jsonl.gz
    compression: auto   # resolves to gzip from the .gz suffix
    batch_size: 0
```

## Streaming & batching

The sink implements `Sink::write_batch`: the pipeline streams pages from the source, and each page is handed to the sink as it arrives, so peak memory is bounded by the page size rather than the total record volume.

Within a `write_batch` call the records are chunked by the **effective chunk size** — `min(batch_size, max_records_per_file)`, where `batch_size = 0` and `max_records_per_file = None` each mean "no limit" (`usize::MAX`). Each chunk becomes a single GCS object with a fresh UUIDv7 key. Chunks upload concurrently via `buffer_unordered(concurrency)` and `try_collect`, so the first upload error aborts the batch.

**Recommended: `batch_size = 0`.** Writing many small objects is a well-known cloud-storage anti-pattern — per-request overhead, slower downstream reads, and inflated LIST/GET costs. Let the source's `batch_size` drive object sizing, or set an explicit `max_records_per_file` if you need a hard cap.

> **Memory ceiling.** Each chunk is buffered fully in memory (and, with compression enabled, briefly held as both the raw and compressed body) before a single-shot upload. Because up to `concurrency` chunks upload at once, peak memory is roughly **`concurrency` × (chunk size) × ~2**. A `batch_size = 0` (or `max_records_per_file = None`) fed by a `fetch_all`-style source produces one very large chunk per `write_batch` call — pair `batch_size = 0` with a streaming source that already sizes its pages, or set `max_records_per_file` / lower `concurrency` to cap peak memory.

**Partial-failure caveat:** because uploads abort on the first error, a batch that fails mid-flight may leave already-uploaded chunks in the bucket. The pipeline bookmark only advances after the whole batch confirms, so a resumed run re-uploads the batch (yielding new UUIDv7 keys for the chunks that already landed). This sink does not use resumable uploads.

## Arrow columnar (Parquet) mode

Behind the crate-local `arrow` Cargo feature, `format: parquet` writes each object as a complete, self-contained Apache Parquet file (ZSTD-compressed) instead of JSON Lines. The sink implements the columnar `write_batch_columnar` fast path (RFC 0002 / #375): when the **source** is also Arrow-native — the [Parquet](https://crates.io/crates/faucet-source-parquet) or [Delta Lake](https://crates.io/crates/faucet-source-delta) source, or the [S3](https://crates.io/crates/faucet-source-s3) / [GCS](https://crates.io/crates/faucet-source-gcs) source in `file_format: parquet` mode — and no `Value`-shaped transform is configured, records move end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` materialization.

If either end of the pipeline isn't Arrow-native — or a `Value`-shaped transform sits in between — the run transparently falls back to the JSON row path.

```yaml
# gcs(parquet) → gcs(parquet) — runs Arrow end-to-end
pipeline:
  sink:
    type: gcs
    config:
      bucket: my-bucket
      prefix: events/parquet/
      auth: { type: application_default }
      format: parquet   # requires the `arrow` feature
```

Enable it with `cargo add faucet-sink-gcs --features arrow` (library) or `cargo install faucet-cli --features "sink-gcs,arrow"` (CLI).

## Compression

Gated behind the crate-local `compression` Cargo feature. It adds the `compression` config field (`none` / `gzip` / `zstd` / `auto`, default `auto`). With `auto`, the codec is resolved from `file_extension` per upload, so `.jsonl.gz` triggers gzip and `.jsonl.zst` triggers zstd; an explicit codec that disagrees with the suffix logs a one-time warning.

```yaml
sink:
  type: gcs
  config:
    bucket: my-bucket
    prefix: events/
    auth: { type: application_default }
    file_extension: .jsonl.zst
    compression: zstd
```

> **Content-Encoding is NOT set.** Unlike a gzip object served with `Content-Encoding: gzip`, this sink uploads the compressed bytes with the `application/x-ndjson` content type and no encoding metadata. Downstream consumers must decompress the object explicitly (the `.gz` / `.zst` suffix is the signal).

Enable it with `cargo add faucet-sink-gcs --features compression` (library) or `cargo install faucet-cli --features "sink-gcs,compression"` (CLI).

## Config loading & schema

Load config from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema sink gcs
```

## Library usage

```rust
use faucet_core::Sink;
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = GcsSinkConfig::new("my-bucket")
    .prefix("events/")
    .auth(GcsCredentials::ApplicationDefault)
    .with_batch_size(0)
    .concurrency(16);

let sink = GcsSink::new(cfg).await?;
let written = sink
    .write_batch(&[json!({ "id": 1 }), json!({ "id": 2 })])
    .await?;
println!("uploaded {written} records");
# Ok(())
# }
```

Most users drive the sink through a `Pipeline` rather than calling `write_batch` directly — see the [library tutorial](https://faucet-hq.github.io/faucet-stream/tutorials/library.html).

## How it works

1. `new()` validates `batch_size`, resolves `GcsCredentials`, and builds an authenticated data-plane `Storage` client **once**.
2. `write_batch` chunks the page by the effective chunk size and serializes each chunk to a JSON Lines byte buffer (one `serde_json` line + `\n` per record).
3. With the `compression` feature, each buffer is compressed in-memory before upload.
4. Each chunk gets a fresh UUIDv7 key (`{prefix}{uuidv7}{file_extension}`) and is uploaded single-shot via `write_object(...).set_content_type("application/x-ndjson").send_unbuffered()`.
5. Chunk uploads run concurrently through `buffer_unordered(concurrency)`; the first error aborts the batch.

The `faucet doctor` preflight probe (`Sink::check`) builds a control-plane `StorageControl` client and issues a non-mutating `list_objects` call capped at one result, so it verifies bucket reachability and credentials without writing anything.

## Lineage dataset URI

`gs://<bucket>/<prefix>` — e.g. `gs://my-bucket/events/`.

## Feature flags

| Feature | Default | Enables |
|---------|---------|---------|
| `compression` | off | The `compression` config field (gzip / zstd / auto); pulls in `faucet-core/compression`. |
| `arrow` | off | The `format: parquet` value and the columnar fast path (`Sink::write_batch_columnar`); pulls in `faucet-core/arrow`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |

Enable the connector itself in the CLI/umbrella via the `sink-gcs` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / `GCS auth: ...` | Credentials invalid, missing, or unreadable. Confirm ADC is initialized (`gcloud auth application-default login`) or that the service-account `path` exists and is valid JSON. On GCP, confirm the workload identity / metadata server is reachable. |
| `403` on upload | The principal lacks write permission. Grant the service account **Storage Object Creator** (or **Storage Object Admin**) on the bucket. |
| `404` / bucket-not-found | The `bucket` name is wrong or the project's credentials can't see it. Verify the bucket exists and `faucet doctor` passes. |
| `Sink: GCS put object error ...` | A transient API/network failure or a quota limit. Retry; if persistent, lower `concurrency` to reduce request burst, and check GCS quotas. |
| Many tiny objects in the bucket | `batch_size` is too small for the source's page size. Set `batch_size: 0` and let the source size pages, or raise `max_records_per_file`. |
| High memory / OOM during writes | Large chunks × high `concurrency`. Lower `concurrency`, set `max_records_per_file`, or feed the sink from a streaming source rather than a `fetch_all`-style one (see the memory note above). |
| Downstream reader sees garbled bytes | The object is compressed but the reader didn't decompress. This sink sets no `Content-Encoding`; consumers must decompress explicitly based on the `.gz` / `.zst` suffix. |
| Duplicate-looking data after a failed run resumed | A partial batch left some chunks behind, then the retry re-uploaded with new UUIDv7 keys. De-duplicate downstream, or size batches so a single object is the unit of retry. |
| `h2 protocol error / GoAway` against an emulator | The emulator was given an `https://` host, so the preflight listing went over gRPC. Use the plaintext port (`http://…`); the integration tests run `fake-gcs-server` with `-scheme=http` and need only Docker. |

## See also

- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html)
- [`faucet-source-gcs`](https://crates.io/crates/faucet-source-gcs) — the matching GCS source.
- [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) — shared `GcsCredentials` enum and client builders.
- [`faucet-sink-s3`](https://crates.io/crates/faucet-sink-s3) — the structurally equivalent AWS S3 sink.


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

## File formats (#604)

Beyond JSON Lines this sink writes **JSON array**, **CSV**, **XML** and
**Excel** through `faucet_core::file_format`, so what it writes is exactly what
the file *sources* can read back.

```yaml
sink:
  type: gcs
  config:
    bucket: reports
    format: csv
    file_extension: .csv.gz
    compression: gzip
    csv: { delimiter: ";" }
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

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Usage signals (#704)

Every object upload (`put`) is reported to faucet's usage meter as a sink
round trip and priced as a GCS class-A request
(`usage.pricing.object_storage.write_per_1k_requests`); see the
[usage cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/usage.html).
