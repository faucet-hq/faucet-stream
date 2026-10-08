# faucet-sink-elasticsearch

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-elasticsearch.svg)](https://crates.io/crates/faucet-sink-elasticsearch)
[![Docs.rs](https://docs.rs/faucet-sink-elasticsearch/badge.svg)](https://docs.rs/faucet-sink-elasticsearch)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-elasticsearch.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-elasticsearch.svg)](https://github.com/faucet-hq/faucet-stream#license)

**Elasticsearch** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Indexes JSON records into an Elasticsearch index via the bulk API (`POST /_bulk`, NDJSON body), re-chunking each page into payloads that land in Elasticsearch's per-request sweet spot.

Reach for it when you want to land any faucet-stream source — a database, a queue, a file, a REST API — into Elasticsearch for search and analytics, with one declarative config and no glue code. The `_bulk` `index` action is an idempotent overwrite by `_id`, so the same sink does append, upsert, and delete just by switching `write_mode`.

## Feature highlights

- **Bulk indexing** — every chunk is a single `POST /_bulk` NDJSON request; the HTTP client is built once in `new()` and reused for every call.
- **Write modes** — `append` (default), `upsert`, `delete`, and `overwrite` (full-refresh via an atomic alias swap). Upsert/delete derive a stable `_id` from `key` columns, so they're idempotent overwrites and keyed removals with no staging.
- **Four auth methods** — none, HTTP Basic, Bearer token, or API key. The shared `ElasticsearchAuth` enum is re-exported from [`faucet-common-elasticsearch`](https://crates.io/crates/faucet-common-elasticsearch) so it matches the Elasticsearch **source** byte-for-byte, and credentials are masked in `Debug` output.
- **Shared auth providers** — `auth: { ref: <name> }` points at a provider in the CLI's top-level `auth:` catalog, so many sinks can share one token.
- **Per-row DLQ** — overrides `write_batch_partial` to read per-item errors from the `_bulk` response, so only the documents Elasticsearch actually rejected go to the dead-letter queue (no duplicates of already-indexed rows).
- **Tunable batching** — `batch_size` controls documents per `_bulk` call, with a `0` sentinel that forwards each page untouched.

## Installation

```bash
# As a library:
cargo add faucet-sink-elasticsearch

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-elasticsearch
```

Or via the umbrella crate: `cargo add faucet-stream --features sink-elasticsearch`.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      endpoint: /v1/logs
  sink:
    type: elasticsearch
    config:
      base_url: http://localhost:9200
      index: events
      auth:
        type: none
      batch_size: 1000
      id_field: event_id
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | string | — *(required)* | Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`). Trailing slashes are stripped automatically. |
| `index` | string | — *(required)* | Target index name. |
| `auth` | `AuthSpec<ElasticsearchAuth>` | `{ type: none }` | Authentication — inline `{ type, config }` or `{ ref: <name> }`. See [Authentication](#authentication). |
| `id_field` | string | *(unset)* | JSON field name used as the document `_id` in `append` mode. If unset, Elasticsearch auto-generates IDs. **Superseded by `key` in `upsert`/`delete` modes.** See [Document ID extraction](#document-id-extraction). |
| `op_type` | `index` \| `create` | `index` | The `_bulk` action for `append` writes. Data streams accept only `create`; with `create`, a document whose `_id` already exists is a per-row error. Refused with any other `write_mode`. |
| `connect_timeout_secs` | int | `10` | Seconds to wait for a connection to the cluster. |
| `request_timeout_secs` | int | `300` | Seconds one HTTP request may take end to end; a half-open connection or wedged node fails the request (retriable) instead of hanging the run. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum actions per `_bulk` request, in every write mode. The sink slices larger pages into `batch_size`-action chunks. **`0` = no batching**: the whole upstream page is sent in one `_bulk` call. See [Streaming & batching](#streaming--batching). |

Bulk items Elasticsearch rejects as overloaded (`429`, `503`, `es_rejected_execution_exception`) are re-sent on their own with exponential backoff (200 ms doubling, up to 5 retries) before they count as failed, so a briefly saturated write pool neither sends valid rows to the DLQ nor makes a retry re-index the items that already succeeded.

### Write mode

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | `append \| upsert \| delete \| overwrite` | `append` | Write semantics. See [Write modes](#write-modes-upsert--delete). `overwrite` is a full-refresh via an atomic alias swap — see below. |
| `key` | array of string | `[]` | Key columns whose values form the document `_id`. A **single** key column is used as the `_id` verbatim (its plain string / JSON form). A **composite** (multi-column) key is encoded as a canonical JSON array of its values — *not* a separator-join — so distinct key tuples always map to distinct `_id`s (e.g. `["a_", "b"]` and `["a", "_b"]` no longer collide). Required and non-empty for `upsert`/`delete`; ignored for `append`. |
| `delete_marker` | `{ field, values }` | *(none)* | Upsert only: rows whose `field` matches one of `values` become deletes; the marker field is stripped from upserted docs. |

### Overwrite (full refresh)

`write_mode: overwrite` replaces the **entire** destination each run via the
idiomatic Elasticsearch **alias swap**, so a reader never sees a half-replaced
dataset and a failed/cancelled run leaves the previous data intact:

1. `begin` creates a fresh physical index `{index}-faucet-ovw-…` (copying the
   current target's mappings **and** its settings — analysis, shard/replica
   counts, `refresh_interval`, index sorting, `mapping.*`, ingest pipelines,
   lifecycle policy, minus identity keys such as `uuid`) behind a marker alias
   `{index}-faucet-ovw-staging`,
   and the run's documents are indexed through that marker (`_bulk` with
   `require_alias=true`, so a write without a staging index fails instead of
   creating one);
2. `commit` atomically repoints the read alias with one `POST /_aliases` call
   (and detaches the marker), then drops the old physical index;
3. a failed run instead drops the staging index (`abort`) and never touches the
   alias.

The staging index is found through the marker alias, not kept in the sink, so
begin, the writes and the commit may run on different sink instances (the CLI
does exactly that). A staging index left behind by a crashed run is dropped by
the next `begin`. Two overwrite runs against the same alias must not overlap.

**The configured `index` must be an alias** (or a not-yet-existing name — the
first run creates the alias). A *concrete* index of that name is rejected, because
there is no atomic replace of a concrete index. No `key` is required.

## Authentication

`auth` uses the shared `ElasticsearchAuth` enum (the project-wide `{ type, config }` shape). It also accepts `auth: { ref: <name> }` to use a shared provider from the CLI's top-level `auth:` catalog (`bearer` / `basic` credentials map onto the matching variant).

| `type` | `config` | Use when |
|--------|----------|----------|
| `none` | *(none)* | No authentication (local / unsecured cluster). |
| `basic` | `{ username, password }` | HTTP Basic — the classic `elastic` / password setup. |
| `bearer` | `{ token }` | Bearer token in the `Authorization` header. |
| `api_key` | `{ key }` | API key sent as `ApiKey <key>` in the `Authorization` header (Elastic Cloud). |

```yaml
# No authentication
auth:
  type: none
```

```yaml
# HTTP Basic
auth:
  type: basic
  config:
    username: elastic
    password: ${env:ES_PASSWORD}
```

```yaml
# API key (Elastic Cloud)
auth:
  type: api_key
  config:
    key: ${env:ES_API_KEY}
```

The `Debug` impl masks passwords, tokens, and keys with `***` to prevent credential leakage in logs.

## Examples

### Postgres → Elasticsearch with API key and a stable `_id`

```yaml
version: 1
name: postgres_to_elasticsearch
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/app
      query: SELECT id, title, body, tags FROM articles WHERE published = $1
      params: [true]
      max_connections: 8
  sink:
    type: elasticsearch
    config:
      base_url: https://es.example.com:9200
      index: articles
      auth:
        type: api_key
        config:
          key: ${env:ES_API_KEY}
      batch_size: 500
      id_field: id
```

### CDC → upsert mirror with tombstones

```yaml
pipeline:
  sink:
    type: elasticsearch
    config:
      base_url: http://localhost:9200
      index: products
      write_mode: upsert
      key: [product_id]
      delete_marker:
        field: __op
        values: [d, delete]   # rows with __op in (d, delete) are removed by _id
```

### One `_bulk` call per upstream page

```yaml
pipeline:
  sink:
    type: elasticsearch
    config:
      base_url: http://localhost:9200
      index: events
      auth: { type: none }
      batch_size: 0          # forward each upstream page as a single bulk request
```

## Streaming & batching

The sink re-chunks each incoming `StreamPage` to keep individual `POST /_bulk` calls within Elasticsearch's recommended payload size.

- **`batch_size > 0`** (default `1000`) — the page is sliced into `batch_size`-document chunks; one `_bulk` HTTP call per chunk. Elasticsearch's documented sweet spot is **5–15 MB of NDJSON per request**, so the right document count depends on average document size:

  | Avg doc size | Recommended `batch_size` |
  |--------------|--------------------------|
  | ~1 KB (log lines, simple events) | 5000 |
  | ~5 KB (typical app events, denormalised rows) | 1000–2000 |
  | ~25 KB (analytics aggregates, large nested objects) | 200–500 |
  | ~100 KB+ (huge nested docs) | 50–100 |

  Start with the default `1000`, watch the [`_bulk` response size in ES logs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk), and adjust until each call lands in the 5–15 MB band. Larger calls risk HTTP 413, slow GC, or rejected-execution exceptions; smaller calls amortise less per-request overhead.

- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream `StreamPage` is forwarded in a single `_bulk` call. Use this when the source already emits page sizes tuned for Elasticsearch (e.g. a Postgres source with `batch_size: 2000`). Larger pages risk HTTP 413 from `http.max_content_length` (default 100 MB but typically lowered to 10–20 MB in production).

`batch_size` is purely a chunk-size knob — per-item error inspection of the `_bulk` response is unchanged.

## Write modes (upsert / delete)

Elasticsearch is schemaless and `_id`-addressable, so the sink supports all three write modes (`Sink::supported_write_modes()` returns `[Append, Upsert, Delete]`). The `_bulk` `index` action is an idempotent overwrite by `_id`, so an **upsert is just an `index` keyed on a stable `_id`**, and a **delete is a `delete` action by `_id`**.

- **`append`** (default) — every record is indexed; `_id` comes from `id_field` if set, otherwise Elasticsearch auto-generates one.
- **`upsert`** — each record's `_id` is derived from its `key` columns and the document is re-indexed (idempotent overwrite). This **supersedes `id_field`** — `key` is authoritative for `_id` here.
- **`delete`** — each record's `key` columns derive an `_id` and a `delete` action removes that document (no doc body is sent).

A **single** key column is used as the `_id` verbatim (e.g. `key: [id]` over `{id: "abc-123"}` → `_id` `"abc-123"`). A **composite** key is encoded as a **canonical JSON array** of its values — *not* a separator-join — so the mapping is injective: `["a_", "b"]` and `["a", "_b"]` (which a naive `:`/`_` join would both collapse to one `_id`) stay distinct and never silently overwrite each other. Within a single batch, **duplicate keys collapse last-write-wins** before the bulk request is built.

In `upsert` mode, `delete_marker` lets a single stream carry both writes and tombstones: rows whose `field` value matches one of `values` become `delete` actions; all other rows are upserted with the marker field stripped from the indexed document. This is the standard pairing with the `cdc_unwrap` transform for a CDC → mirror pipeline.

Missing or null `key` values are per-row failures: `write_batch` aborts the page, while the DLQ-aware `write_batch_partial` path routes exactly those rows to the dead-letter queue and writes the rest.

## Document ID extraction

In `append` mode, when `id_field` is set the sink extracts the document `_id` from each record:

- A string field value is used directly.
- A number or boolean is converted to its string representation.
- A record missing `id_field` gets an Elasticsearch auto-generated ID.
- A `null`, object or array value is an error for **that row** (DLQ-routable; without a DLQ the batch fails before anything is sent). Rendering it would put every such record on one `_id` such as `"null"` and silently keep only the last.

In `upsert` / `delete` modes rows are deduplicated by the `_id` they address, last write wins, so keys `7` and `"7"` (both `_id "7"`) are one document.

Setting `id_field` to a stable business key makes resumed/retried runs **idempotent overwrites** rather than duplicates — the recommended setting for resumable append pipelines (or configure a DLQ, whose per-row path avoids the whole-page re-send).

## Schema evolution

`ElasticsearchSink` reports its live index mappings via `current_schema()` (`GET /<index>/_mapping`; every field is reported as `[type, "array", "null"]` because ES has no NOT NULL concept and every field is multi-valued, so array data is not drift; a missing index → `None`), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real index. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `ElasticsearchSink::evolve_schema()` is **add-fields only**:

- **New fields** → `PUT /<index>/_mapping` adding the field mappings. An array column maps to its item type (not `object`), and strings map to `keyword` with `ignore_above: 8191`, so an over-long value is stored but not indexed instead of rejecting its document.
- **Type widenings and nullability relaxations are no-ops** — Elasticsearch cannot change an existing field's mapping type or nullability in place (a one-shot `debug` log notes this).

Because ES cannot retype an existing field, any change to an existing field's type is classified as **incompatible** and routed by `on_incompatible` (`fail` or `quarantine`) rather than applied. See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Scoped cleanup

With a source `complete_for` claim and `write_mode: upsert`, the sink deletes the documents in the claimed scope it did not write this run, in one `_delete_by_query` that excludes every written `_id`. Each scope field is matched with a `term` query on its exact indexed value: a field mapped as analyzed `text` (Elasticsearch's dynamic mapping for strings) is matched through its `keyword` sub-field (`account.keyword`), and a `text` field with no `keyword` sub-field is refused, because an exact match on it would delete nothing. Elasticsearch has no transactions, so concurrent searches can observe a partially cleaned scope; a partial outcome (failures, version conflicts, a timeout) fails the run, and the next run finishes the job.

## Dead-letter queue

This sink overrides `Sink::write_batch_partial` to surface per-row failures from Elasticsearch's `_bulk` response items. Configure a DLQ at the pipeline level (see [cli/README.md — `dlq:`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md)) and only the documents Elasticsearch actually rejected are routed there — already-indexed items stay in the main sink with no duplicates. This is why the bulk API's best-effort, partial-success behaviour doesn't double-write rows into the DLQ.

> **Not effectively-once.** Elasticsearch does not commit a faucet commit token transactionally, so this sink does not support `delivery: exactly_once`. For idempotent re-sends use `write_mode: upsert` (or a stable `id_field` in append mode).

## Config loading & schema

Load from YAML/JSON or environment via the helpers in `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_elasticsearch::ElasticsearchSinkConfig;

// From a JSON file
let config: ElasticsearchSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: ElasticsearchSinkConfig = load_env_file(".env", "ES_SINK")?;
```

Example `.env`:

```env
ES_SINK_BASE_URL=http://localhost:9200
ES_SINK_INDEX=events
ES_SINK_AUTH='{"type":"basic","config":{"username":"elastic","password":"changeme"}}'
ES_SINK_BATCH_SIZE=1000
ES_SINK_ID_FIELD=event_id
```

Inspect the full JSON Schema with:

```bash
faucet schema sink elasticsearch
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_elasticsearch::{ElasticsearchAuth, ElasticsearchSink, ElasticsearchSinkConfig};
use faucet_source_rest::{RestStream, RestStreamConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/logs"));

let config = ElasticsearchSinkConfig::new("http://localhost:9200", "api_logs")
    .auth(ElasticsearchAuth::ApiKey { key: std::env::var("ES_API_KEY")? })
    .with_batch_size(1000)
    .id_field("log_id");
let sink = ElasticsearchSink::new(config)?;

let result = Pipeline::new(source, sink).run().await?;
println!("Indexed {} documents", result.records_written);
# Ok(())
# }
```

## How it works

1. `new()` validates `batch_size` (≤ `MAX_BATCH_SIZE`) and the write spec, then builds the HTTP client **once**.
2. `write_batch()` slices the input into `batch_size`-document chunks (or forwards the whole slice when `batch_size = 0`). For each chunk it builds an NDJSON body of alternating action/data lines:
   ```
   {"index":{"_index":"events","_id":"optional-id"}}
   {"user_id":"u123","event":"page_view"}
   {"index":{"_index":"events"}}
   {"user_id":"u456","event":"click"}
   ```
3. The NDJSON body is `POST`ed to `{base_url}/_bulk` with `Content-Type: application/x-ndjson`; auth headers are applied per request.
4. The bulk response is inspected for per-item errors — `write_batch` fails with a `FaucetError::Sink` naming the first failed item; `write_batch_partial` returns per-row outcomes for the DLQ router.

## Lineage dataset URI

`http://<host>:<port>/<index>` (credentials stripped) — e.g. `http://localhost:9200/my-index`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-elasticsearch` feature.

## Shared types

`ElasticsearchAuth` lives in [`faucet-common-elasticsearch`](https://crates.io/crates/faucet-common-elasticsearch) and is shared with [`faucet-source-elasticsearch`](https://crates.io/crates/faucet-source-elasticsearch). The sink re-exports it as `faucet_sink_elasticsearch::ElasticsearchAuth`.

> **Deprecation:** the previous name `ElasticsearchSinkAuth` is retained as a deprecated type alias and removed in `0.4.0`. Migrate imports to `ElasticsearchAuth` at your convenience.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `401 Unauthorized` / `403 Forbidden` | Missing or wrong credentials. Set `auth` (`basic` / `bearer` / `api_key`) and confirm the role can write to the index. |
| `FaucetError::Auth` on a shared provider | The `auth: { ref }` provider yielded a non-bearer/basic credential. Elasticsearch only accepts `bearer` or `basic` from a shared provider. |
| HTTP 413 / request-too-large | A `_bulk` body exceeded `http.max_content_length`. Lower `batch_size` (or set a non-zero value if you were using `0`) until each call lands in the 5–15 MB band. |
| `rejected_execution_exception` in ES logs | Bulk queue saturated — calls too large or too frequent. Lower `batch_size`; back-pressure with a smaller upstream page. |
| Resumed run creates duplicate documents | Append mode with auto-generated IDs re-sends a partially-committed page. Set `id_field` to a stable business key (idempotent overwrite) or configure a DLQ. |
| `write_mode: upsert/delete` rejected at validate | `key` is empty. Provide a non-empty `key: [...]` — it's required for upsert/delete. |
| Rows land in the DLQ before any HTTP call | Those rows are missing/null `key` values (upsert/delete) — they're per-row failures routed to the DLQ. Fix the upstream data or the `key` columns. |
| `index_not_found_exception` | The target `index` doesn't exist and the cluster has auto-create disabled. Create the index/template first, or enable `action.auto_create_index`. |
| Mapping / `mapper_parsing_exception` | A field's type conflicts with the existing index mapping. Reshape with a transform, or use a fresh index/template with the right mapping. |

## See also

- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Upsert & write modes cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).
- [Dead-letter queue cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html).
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html).
- [`faucet-source-elasticsearch`](https://crates.io/crates/faucet-source-elasticsearch) — the Elasticsearch source (search/scroll API).

## Batch atomicity

What a failed write leaves behind (#737): **per-row** with `batch_size: 0` (one `_bulk` request per write), otherwise **best-effort** — per-item outcomes come from one _bulk request per chunk; an outer error after the first chunk leaves earlier chunks indexed. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
