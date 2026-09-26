# faucet-source-gcs

[![Crates.io](https://img.shields.io/crates/v/faucet-source-gcs.svg)](https://crates.io/crates/faucet-source-gcs)
[![Docs.rs](https://docs.rs/faucet-source-gcs/badge.svg)](https://docs.rs/faucet-source-gcs)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-gcs.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-gcs.svg)](https://github.com/faucet-hq/faucet-stream#license)

Google **Cloud Storage** source connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Lists objects in a bucket (with an optional prefix) or reads an explicit list of object keys, fetches each one over the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage) SDK, and parses it as **JSON Lines**, **JSON Array**, or **raw text** — yielding records as `serde_json::Value`.

Reach for it when your data already lives in GCS — event exports, log dumps, analytics extracts, daily snapshots — and you want to move it into any faucet-stream sink (a database, a warehouse, a queue, a file) with one declarative config and no glue code. Objects are read concurrently and JSONL/raw-text bodies stream line-by-line, so a multi-gigabyte export lands without buffering the whole bucket in memory.

## Feature highlights

- **Seven file formats** — `json_lines`, `json_array`, `raw_text`, `parquet`, plus `csv`, `xml` and `xlsx` via [file formats](#file-formats-604).
- **Apache Parquet (Arrow columnar)** — behind the `arrow` feature, a fourth format `file_format: parquet` decodes each object via the Arrow Parquet reader and, when the sink is also Arrow-native (Parquet / Delta), moves records end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` in between. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).
- **List or explicit keys** — scan a bucket by `prefix`, or skip listing entirely by passing an exact `object_keys` list.
- **Concurrent reads** — objects are fetched in parallel (default 10) on the streaming path as well as the batch one, so wall-clock time is bounded by your slowest objects, not their sum. The streaming prefetch is *ordered*, so records still arrive in listing order and a failing object is still blamed at its own position. For `json_lines` the look-ahead holds only open body readers and for `parquet` only footer metadata, so peak memory stays `O(batch_size)`; `json_array` / `raw_text` hold up to `concurrency` whole bodies.
- **Bounded-memory streaming** — `json_lines` and `raw_text` decode straight off the GCS body reader, so peak memory is `O(batch_size)` regardless of total file size.
- **Compression auto-detect** — behind the `compression` feature, `.gz` / `.zst` objects are transparently decompressed; the codec resolves *per object key*, so one run can mix compressed and uncompressed objects.
- **Read-integrity verification** — every object's byte length is checked against the `size` GCS reports (`verify_length`, default on), so a cleanly-truncated transfer is rejected instead of silently parsed as a complete object. Opt into CRC-32C / MD5 checksum verification with `verify_checksum`. Both checks auto-skip GCS-transcoded (`Content-Encoding: gzip`) objects.
- **Four credential modes** — Application Default Credentials, a service-account key file, inline service-account JSON, or anonymous (for emulators). The shared `GcsCredentials` enum is re-exported from [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) so it matches the GCS **sink** byte-for-byte.
- **Clients built once** — the data-plane and control-plane clients are constructed in `new()` and reused for every list and read.
- **Matrix-aware prefixes** — `${parent.path}` placeholders in `prefix` are resolved per-record at runtime, so a parent matrix row can fan out into many per-record bucket scans.

## Installation

```bash
# As a library:
cargo add faucet-source-gcs

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-gcs

# With transparent gzip/zstd decompression:
cargo install faucet-cli --features "source-gcs,compression"
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: gcs
    config:
      bucket: my-bucket
      prefix: events/2026/
      auth:
        type: service_account_json_file
        config:
          path: /run/secrets/gcp-sa.json
      file_format: json_lines
  sink:
    type: jsonl
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | — *(required)* | GCS bucket name. No `gs://` prefix, no path. |
| `prefix` | string | *(unset)* | Object-name prefix filter for listing. Ignored when `object_keys` is set. Supports `${field.path}` placeholders resolved against the parent-record context at runtime. |
| `object_keys` | array of string | *(unset)* | Explicit object names to read. When set, listing is skipped and `prefix` is ignored. |
| `auth` | `GcsCredentials` | `application_default` | Authentication — see [Authentication](#authentication). |
| `file_format` | enum | `json_lines` | `json_lines`, `json_array`, `raw_text`, `parquet`, `csv`, `xml`, `xlsx` — see [File formats](#file-formats-604). |
| `max_objects` | int | *(unset)* | Hard cap on the number of objects read (applied after listing, and to an explicit `object_keys` list). |

### Performance

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | int | `10` | Maximum concurrent object reads. Higher = faster on many small objects; lower caps peak memory for large `raw_text` / `json_array` objects. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching** (one page per object). See [Streaming & batching](#streaming--batching). Validated at config load: an empty `bucket`, or a `batch_size` above `MAX_BATCH_SIZE` (1,000,000), is rejected with `FaucetError::Config`. |
| `verify_length` | bool | `true` | Verify each object's byte count against the `size` GCS reports; a short (truncated) or over-long transfer fails with `FaucetError::Source`. Auto-skipped for a transcoded object (non-empty `Content-Encoding`) or when no size is reported. See [Read-integrity verification](#read-integrity-verification). |
| `verify_checksum` | bool | `false` | Also verify the body against the CRC-32C (preferred) or MD5 checksum GCS reports. Costs a hash over the full body; skipped for transcoded objects. |

### Format & testing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | enum | `auto` | *(requires the `compression` feature)* Decompression codec — `none`, `gzip`, `zstd`, or `auto`. `auto` detects `.gz` / `.zst` from the object key. |
| `storage_host` | string | *(unset)* | Endpoint override (integration tests / emulators only, e.g. `http://localhost:4443`). A plaintext `http://` host lists and stats objects over the JSON API, so `fake-gcs-server` works end to end. Production users leave this unset. |

## Authentication

`auth` uses the shared `GcsCredentials` enum from [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) (the project-wide `{ type, config }` shape):

| `type` | `config` | Use when |
|--------|----------|----------|
| `application_default` | *(none)* | Running on GCE/GKE (metadata server / workload identity) or after `gcloud auth application-default login`. Also honours `GOOGLE_APPLICATION_CREDENTIALS`. **Default.** |
| `service_account_json_file` | `{ path: <file> }` | You have a service-account key file on disk. |
| `service_account_json_inline` | `{ json: <string> }` | You want to inject the key JSON inline, typically via `${env:VAR}` / `${secret:…}` indirection. |
| `anonymous` | *(none)* | Talking to an emulator (e.g. `fake-gcs-server`) that does not validate bearer tokens. |

```yaml
# Application Default Credentials (workload identity, gcloud, GOOGLE_APPLICATION_CREDENTIALS)
auth:
  type: application_default
```

```yaml
# Service-account key file
auth:
  type: service_account_json_file
  config:
    path: /run/secrets/gcp-sa.json
```

```yaml
# Inline service-account JSON via env indirection
auth:
  type: service_account_json_inline
  config:
    json: ${env:GCP_SA_JSON}
```

HMAC-key auth, signed-URL generation, and KMS/CMEK encryption configuration are out of scope.

## File formats (#604)

| `file_format` | Behaviour | Streaming |
|---------------|-----------|-----------|
| `json_lines` *(default)* | One JSON record per line; blank lines are skipped. | Streams line-by-line — `O(batch_size)` memory. |
| `json_array` | The entire object is a single JSON array of records. | Buffered fully per object (the closing `]` is required to parse), then chunked. |
| `raw_text` | The whole object becomes one record `{"key": <name>, "content": <utf-8>}`. | Streamed into one `String` per object. |
| `csv` / `xml` / `xlsx` | Decoded through `faucet_core::file_format` (below). | Buffered fully per object, then chunked. |
| `parquet` | One record per Parquet row (Arrow-decoded). *(Requires the `arrow` feature — see [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).)* | Batches decoded via the Arrow Parquet reader. |

Parse errors are precise: a JSONL failure carries the object key **and** the 1-based line number; a JSON-array failure carries the key. A `json_array` object whose top-level value isn't an array fails with an `"expected JSON array"` message. Non-UTF-8 bodies surface as `FaucetError::Source` with a `"not valid UTF-8"` hint.

Beyond JSON Lines, JSON array and raw text, this source reads **CSV**, **XML**
and **Excel** through `faucet_core::file_format`, so the records it produces
match what every other file connector produces for the same bytes.

```yaml
source:
  type: gcs
  config:
    bucket: feeds
    file_format: xml
    xml: { record_element: order }
```

| Option block | Applies to | Fields |
|---|---|---|
| `csv` | `csv` | `delimiter` (one byte; `"\t"` for tabs), `has_headers` (default `true`; `false` names fields `column_0`, `column_1`, …) |
| `xml` | `xml` | `record_element` (the repeated element that delimits a record) |
| `excel` | `xlsx` | `sheet` (name, or an index as a string; default first), `header_row` (0-based) |

Enable with `--features file-formats` (or one of `file-format-csv` /
`file-format-xml` / `file-format-excel`), so a build that reads CSV does not
link an Excel reader. Format composes with `compression`.

**Memory:** these three are read **whole** and decoded before their records are
chunked into pages — a workbook is a zip container whose directory sits at the
end, and an XML document is a tree. `csv` and `xml` are text formats: every
value comes back a string. See the
[file-formats cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/file-formats.html).

## Examples

### Scan a prefix as JSON Lines (the 80% path)

```yaml
source:
  type: gcs
  config:
    bucket: analytics-exports
    prefix: events/dt=2026-06-16/
    auth: { type: application_default }
    file_format: json_lines
    concurrency: 20
    batch_size: 5000
```

### Read an explicit set of objects, no listing

```yaml
source:
  type: gcs
  config:
    bucket: my-bucket
    object_keys:
      - lookups/countries.json
      - lookups/currencies.json
    auth:
      type: service_account_json_file
      config: { path: /run/secrets/gcp-sa.json }
    file_format: json_array
    batch_size: 0      # emit one page per object — ideal for small lookup tables
```

### Raw text files, compressed, with a cap

```yaml
source:
  type: gcs
  config:
    bucket: log-archive
    prefix: app/2026/06/
    auth: { type: application_default }
    file_format: raw_text
    compression: auto    # requires the `compression` feature; decompresses .gz / .zst objects
    max_objects: 100     # read at most the first 100 objects
    concurrency: 4       # raw_text holds a whole object in memory — keep concurrency low
```

### Date-templated prefix driven by the run clock

```yaml
source:
  type: gcs
  config:
    bucket: analytics-exports
    prefix: events/dt=${now.date}/   # resolves to e.g. events/dt=2026-06-16/ at run time
    auth: { type: application_default }
    file_format: json_lines
```

## Streaming & batching

The source overrides `Source::stream_pages`. It lists object keys once, then walks them in order:

- **`json_lines`** decodes the (optionally decompressed) body line-by-line off an async buffered reader, emitting a `StreamPage` every `batch_size` records. Memory stays at `O(batch_size)` no matter how large the file is.
- **`raw_text`** emits one `{key, content}` record per object, streamed straight into a single `String` (no separate raw + decompressed copies for compressed objects).
- **`json_array`** buffers each object fully (a JSON array isn't parseable until its closing `]`), then chunks its records into pages.

For a non-zero `batch_size`, records from multiple objects can share a page (cross-object flattening) — the page boundary follows the record count, not the object boundary. This matches the `faucet-source-s3` source and is intentional.

**`batch_size: 0`** is the no-batching sentinel: every page contains exactly one complete object's records, with no within-object chunking and no cross-object accumulation. Useful for small lookup tables or when a downstream sink prefers one large request per object.

> **Memory ceiling — `raw_text` / `json_array`.** Both hold one whole decoded object in memory at a time (inherent: a raw-text record *is* the whole file, and a JSON array isn't valid until its closing `]`). Because objects are fetched concurrently, peak memory is bounded by roughly **`concurrency` × (largest object's decoded size)**, not by `batch_size`. For large `raw_text` / `json_array` objects, lower `concurrency` to cap peak memory, or re-emit the data as `json_lines` upstream so it streams at `O(batch_size)`.

> **Parquet streams row groups.** A `file_format: parquet` object is read over byte ranges — its footer locates every row group, so peak memory is one Arrow batch (capped at `batch_size`), not the object. Two settings fall back to reading the whole object, because each is a guarantee worth more than the memory saving: `verify_checksum: true` (the checksum covers the whole object, so verifying it means streaming all of it) and a resolved `compression` codec (a compressed member is not randomly addressable). Records are identical either way.


This is a one-shot scan source — it has no incremental bookmark / resume support, so each run re-lists and re-reads the matching objects. For incremental loads, advance the `prefix` between runs (e.g. a dated `events/dt=${now.date}/` prefix) so each run reads only fresh objects.

## Arrow columnar (Parquet) mode

Behind the crate-local `arrow` Cargo feature, `file_format: parquet` reads each object as an Apache Parquet file through the Arrow Parquet reader. The same decode serves two paths:

- the ordinary **row path** — each Parquet `RecordBatch` is converted to JSON records, exactly like the other formats; and
- the opt-in **columnar fast path** (RFC 0002 / #375) — when the sink is also Arrow-native (the [Parquet](https://crates.io/crates/faucet-sink-parquet) or [Delta Lake](https://crates.io/crates/faucet-sink-delta) sink) and no `Value`-shaped transform is configured, records move end-to-end as Arrow `RecordBatch`es with no `serde_json::Value` materialization.

`supports_columnar()` is `true` only when `file_format: parquet`. If either end of the pipeline isn't Arrow-native — or a `Value`-shaped transform sits in between — the run transparently falls back to the row path.

```yaml
# gcs(parquet) → delta — runs Arrow end-to-end
pipeline:
  source:
    type: gcs
    config:
      bucket: analytics-exports
      prefix: events/2026/
      auth: { type: application_default }
      file_format: parquet   # requires the `arrow` feature
  sink:
    type: delta
    config:
      table_uri: ./out/events
```

Enable it with `cargo add faucet-source-gcs --features arrow` (library) or `cargo install faucet-cli --features "source-gcs,arrow"` (CLI).

## Compression

Behind the crate-local `compression` Cargo feature. Adds the `compression` config field with values `none`, `gzip`, `zstd`, or `auto` (the default). `auto` detects `.gz` / `.zst` from the object key; explicit codecs apply to every object.

```yaml
source:
  type: gcs
  config:
    bucket: log-archive
    prefix: app/
    auth: { type: application_default }
    compression: auto     # or 'gzip' | 'zstd' | 'none'
```

The codec resolves per object key, so a single source can read a mix of compressed and uncompressed objects in one run. A one-shot warning fires when an explicit codec disagrees with the object's filename suffix.

## Read-integrity verification

Object bodies are read through a verifying reader that validates the transfer at
EOF, so a stream that ends early but *cleanly* is rejected instead of being
parsed and emitted as a complete object — silent data loss otherwise.

- **Length** (`verify_length`, default `true`) — counts the bytes read and
  compares them against the `size` GCS reports for the object. A mismatch fails
  the page with `FaucetError::Source`.
- **Checksum** (`verify_checksum`, default `false`) — verifies the body against
  the CRC-32C (preferred) or MD5 digest GCS reports. Costs a hash over the full
  body. A one-shot warning is logged if the object advertises neither.

Both checks are **automatically skipped for a transcoded object** — one stored
with a non-empty `Content-Encoding` (e.g. `gzip`), which GCS may decompress on
read so the received bytes match neither the stored `size` nor the stored
checksum. They operate on the bytes GCS delivers (below the client-side
`compression` feature's decompression), so `.gz` / `.zst` objects served
without a `Content-Encoding` header verify correctly.

Both keys are named and behave identically across the S3, GCS, and Azure Blob
sources (Azure exposes no body checksum and rejects `verify_checksum`).

```yaml
pipeline:
  source:
    type: gcs
    config:
      bucket: my-bucket
      prefix: events/
      verify_checksum: true   # length check is already on by default
```

## Config loading & schema

Config loads from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source gcs
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_gcs::{GcsCredentials, GcsFileFormat, GcsSource, GcsSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = GcsSourceConfig::new("analytics-exports")
    .prefix("events/dt=2026-06-16/")
    .auth(GcsCredentials::ApplicationDefault)
    .file_format(GcsFileFormat::JsonLines)
    .concurrency(20)
    .with_batch_size(5000);

let records = GcsSource::new(cfg).await?.fetch_all().await?;
println!("records: {}", records.len());
# Ok(())
# }
```

## How it works

1. `new()` resolves `GcsCredentials` and builds both a data-plane `Storage` client and a control-plane `StorageControl` client **once**, reusing them across calls.
2. Listing uses the control-plane `list_objects` paginator (page size 1000), filtered by `prefix` and capped at `max_objects`. An explicit `object_keys` list skips listing entirely.
3. Each object's body is opened as an async buffered reader; with the `compression` feature it is wrapped in a per-key decompressor.
4. Objects are read concurrently — `stream_pages` uses an ordered `buffered(concurrency)` look-ahead (so up to `concurrency` reads overlap the current object's decode), the eager batch path uses `buffer_unordered(concurrency)` — and parsed per `file_format`. `concurrency: 0` is clamped to 1. For `json_lines` the look-ahead holds only open body readers, keeping peak memory `O(batch_size)`; for `json_array` / `raw_text` / `parquet` it holds up to `concurrency` whole bodies.
5. `stream_pages` re-frames the decoded records into `batch_size` pages and yields them to the pipeline, keeping peak memory bounded for the streaming formats.

## Dataset discovery

The source supports live introspection via `Source::discover()`: one control-plane delimiter (`/`) listing under the configured `prefix` (bucket root when unset) enumerates the "directories" directly below it, returning one dataset descriptor per common prefix with:

- `name` — the full object-name prefix (e.g. `raw/orders/`), `kind: prefix`
- `config_patch` — `{ "prefix": "raw/orders/" }`, ready to deep-merge over the connection config as a matrix row

When the listing returns no common prefixes but does return objects directly under the prefix (a "leaf directory"), each object becomes a descriptor instead (`kind: object`, `config_patch: { "object_keys": ["<full name>"] }` — the exact-match field, which makes `prefix` inert), capped at the single listing page of 1000. `schema` and `estimated_rows` are never set — either would require reading or paging the whole listing. Discovery issues exactly one listing call and never recurses.

## Lineage dataset URI

`gs://<bucket>` or `gs://<bucket>/<prefix>` — e.g. `gs://my-bucket/events/2026/`.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field and transparent gzip/zstd decompression (pulls in `faucet-core/compression`). |
| `arrow` | off | Adds the `file_format: parquet` value and the Arrow columnar fast path (`Source::stream_batches`); pulls in `faucet-core/arrow`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |

Enable the connector itself in the CLI/umbrella via the `source-gcs` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 / 403 | Credentials invalid or missing scope. Confirm the service account has **Storage Object Viewer** (`roles/storage.objectViewer`) on the bucket, or that ADC is initialized (`gcloud auth application-default login`). |
| `GCS list error for bucket '…'` | The bucket name is wrong, doesn't exist, or the principal lacks `storage.objects.list`. Pass the bare bucket name (no `gs://`, no path) and grant the Viewer role. |
| No objects read / empty output | The `prefix` matched nothing. Prefixes are literal (not globs) and case-sensitive; verify the exact object-name prefix, and remember `prefix` is ignored when `object_keys` is set. |
| `GCS JSON parse error in '…' at line N` | A line in a `json_lines` object isn't valid JSON. The message pins the object key and 1-based line; fix the source data or switch `file_format`. |
| `GCS expected JSON array in '…'` | `file_format: json_array` but the object's top-level value isn't an array. Use `json_lines` for newline-delimited data or `raw_text` for opaque blobs. |
| `not valid UTF-8` | A `raw_text` / `json_array` object has a non-UTF-8 body. These formats require UTF-8; binary objects aren't supported. |
| Compressed objects come through as garbled text | The `compression` feature isn't enabled, or `compression: none` is set. Build with `--features compression` and leave `compression: auto` (the default) so `.gz` / `.zst` keys are decompressed. |
| Out-of-memory on large `raw_text` / `json_array` objects | These formats hold a whole object in memory and peak at ~`concurrency × largest-object size`. Lower `concurrency`, cap with `max_objects`, or re-emit the data as `json_lines`. |
| Each run re-reads everything | This source has no resume bookmark. Advance the `prefix` between runs (e.g. a dated `events/dt=${now.date}/`) so each run reads only new objects. |
| `h2 protocol error / GoAway` against an emulator | The emulator was given an `https://` host, so listing went over gRPC, which `fake-gcs-server` serves only with its self-signed TLS certificate. Point `storage_host` at the plaintext port (`-scheme=http`, `http://…`) with `auth: { type: anonymous }`; plaintext hosts use the JSON API. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full source/sink capability matrix.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — shared auth providers and the `{type, config}` shape.
- [Compression cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/compression.html) — gzip/zstd across file connectors.
- [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) — the matching GCS sink.
- [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) — the AWS S3 equivalent with the same format semantics.
- [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) — the shared credentials enum and client builders.

## Sharded execution (cluster Mode B)

Under [`faucet serve --cluster`](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html),
a top-level `shard: { count: N }` block splits this source across cluster
workers **automatically — no connector config needed**. Each worker reads the
objects whose key hashes to its shard index (stable FNV-1a modulo `count`),
so the partition is disjoint and complete: every object is read by exactly one
worker, and the partition stays stable as new objects appear. Outside the
cluster coordinator a run reads every object, unchanged.

> `max_objects` is applied before the shard filter, so it caps the run's
> *total* object set (matching single-worker semantics) rather than
> multiplying by the shard count.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Usage signals (#704)

Every object listing page (`list`), object read (`get`, one per object or
per ranged read) and metadata read (`head`) is reported to faucet's usage
meter as a source round trip and priced as a GCS class-B request
(`usage.pricing.object_storage.read_per_1k_requests`); see the
[usage cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/usage.html).
