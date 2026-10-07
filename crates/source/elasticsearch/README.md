# faucet-source-elasticsearch

[![Crates.io](https://img.shields.io/crates/v/faucet-source-elasticsearch.svg)](https://crates.io/crates/faucet-source-elasticsearch)
[![Docs.rs](https://docs.rs/faucet-source-elasticsearch/badge.svg)](https://docs.rs/faucet-source-elasticsearch)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-elasticsearch.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-elasticsearch.svg)](https://github.com/faucet-hq/faucet-stream#license)

An **Elasticsearch** search source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs a query-DSL search against an index, walks the result set page-by-page via the [scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html), emits each hit's `_source` as a `serde_json::Value`, and streams pages straight into any faucet-stream sink so memory stays bounded no matter how many documents match.

Reach for it when you need to export logs, metrics, orders, or any indexed documents out of Elasticsearch — for backup, reindexing, analytics offload, or feeding a database/warehouse/queue — with one declarative config and no glue code.

## Feature highlights

- **Scroll-based streaming** — the source uses the scroll API so an index of any size is drained at `O(batch_size)` client memory; each scroll response becomes exactly one `StreamPage` written to the sink as it lands.
- **Full query-DSL support** — pass any Elasticsearch query object verbatim (`bool`, `range`, `term`, `match`, …); defaults to `match_all`.
- **Four auth modes** — none, HTTP Basic, Bearer token, or API key. The shared `ElasticsearchAuth` enum is re-exported from [`faucet-common-elasticsearch`](https://crates.io/crates/faucet-common-elasticsearch) so it matches the Elasticsearch **sink** byte-for-byte; credentials are masked in debug output.
- **Shared auth providers** — `auth: { ref: <name> }` points at a provider in the CLI's top-level `auth:` catalog, so many sources share one token with single-flight refresh.
- **Unconditional scroll cleanup** — the open `_scroll_id` is always sent to `DELETE _search/scroll` on every exit path (clean drain, `max_pages` truncation, mid-stream error, or the consumer dropping the stream) via an RAII guard, so the cluster never leaks server-side scroll state.
- **Matrix-context interpolation** — `${field.path}` placeholders in `index` and `query` resolve against the parent-record context at runtime, so one config can fan out across indices or query values.
- **`max_pages` cap** — bound the number of scroll responses for previews or partial exports.
- **No-batching sentinel** — `batch_size: 0` issues a single non-scroll `_search?size=10000` for small lookup indices or sinks that prefer one large request.
- **Client built once** — the `reqwest` client is constructed in `new()` and reused for every request.

## Installation

```bash
# As a library:
cargo add faucet-source-elasticsearch

# Or via the umbrella crate:
cargo add faucet-stream --features source-elasticsearch

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-elasticsearch
```

`source-elasticsearch` is an opt-in feature — it is not part of the CLI/umbrella default build.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: elasticsearch
    config:
      base_url: http://localhost:9200
      index: my_index
      # query defaults to { match_all: {} }
  sink:
    type: file
    config:
      path: ./docs.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | string | — *(required)* | Base URL of the Elasticsearch cluster (e.g. `http://localhost:9200`). A trailing slash is trimmed. |
| `index` | string | — *(required)* | Index (or pattern, e.g. `metrics-*`) to search. Supports `${field.path}` matrix-context placeholders. |
| `query` | object | `{ "match_all": {} }` | Elasticsearch query DSL, sent verbatim as the `query` of the search body. Supports `${field.path}` placeholders resolved against the parent-record context. |
| `scroll_timeout` | string | `"5m"` | Scroll-context keep-alive sent on the initial and every follow-up scroll request (e.g. `"1m"`, `"5m"`). Must exceed the time taken to process one page downstream. |
| `connect_timeout_secs` | int | `10` | Seconds to wait for a connection to the cluster. |
| `request_timeout_secs` | int | `300` | Seconds one search or scroll request may take; a half-open connection or wedged node fails the request instead of hanging the run. |
| `auth` | `ElasticsearchAuth` | `{ type: none }` | Authentication — inline `{ type, config }` or `{ ref: <name> }`. See [Authentication](#authentication). |
| `max_pages` | int | *(unset = no limit)* | Maximum number of scroll responses to emit. The cap applies *after* a page is yielded. |
| `batch_size` | int | `1000` | Docs per emitted `StreamPage`, also the scroll API `size` parameter. **`0` = no batching**: the whole result set is drained through the scroll API (10 000 docs per request) and emitted as one page. Validated against `MAX_BATCH_SIZE` (1,000,000) at construction (an empty `base_url` / `index` is likewise rejected with `FaucetError::Config`). |

### Authentication

`auth` uses the shared `ElasticsearchAuth` enum (the project-wide `{ type, config }` shape):

| `type` | `config` | Sent as |
|--------|----------|---------|
| `none` | *(none)* | No `Authorization` header. |
| `basic` | `{ username, password }` | HTTP Basic `Authorization: Basic <base64>`. |
| `bearer` | `{ token }` | `Authorization: Bearer <token>`. |
| `api_key` | `{ key }` | `Authorization: ApiKey <key>` (the base64 `id:api_key` value). |

```yaml
# No auth (local dev)
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
# Bearer token
auth:
  type: bearer
  config:
    token: ${env:ES_BEARER_TOKEN}
```

```yaml
# API key (recommended for Elastic Cloud)
auth:
  type: api_key
  config:
    key: ${env:ES_API_KEY}
```

Shared provider via the top-level `auth:` catalog (single token shared across sources):

```yaml
auth:
  es_token:
    type: static
    config: { token: ${env:ES_BEARER_TOKEN} }

pipeline:
  source:
    type: elasticsearch
    config:
      base_url: https://es.example.com:9200
      index: orders
      auth: { ref: es_token }
```

A shared provider may only yield a `bearer` or `basic` credential — header- and token-style credentials have no Elasticsearch equivalent and surface as an auth error.

## Examples

### Filtered error-log export to S3

```yaml
# Elasticsearch (API-key auth) → S3 with sharded parallel uploads.
version: 1
name: elasticsearch_to_s3
pipeline:
  source:
    type: elasticsearch
    config:
      base_url: https://es.example.com:9200
      index: logs-2026-05
      query:
        match:
          level: error
      scroll_timeout: 2m
      batch_size: 2000
      auth:
        type: api_key
        config:
          key: ${env:ES_API_KEY}
  sink:
    type: s3
    config:
      bucket: my-es-backups
      prefix: logs/2026-05/
      region: us-east-1
      file_extension: .jsonl
      max_records_per_file: 50000
      concurrency: 16
```

### Bool query with a time range and Basic auth

```yaml
source:
  type: elasticsearch
  config:
    base_url: https://elasticsearch.example.com:9200
    index: application-logs-2026.03
    query:
      bool:
        must:
          - match: { level: error }
          - range:
              "@timestamp": { gte: "2026-03-01", lt: "2026-04-01" }
    scroll_timeout: 5m
    batch_size: 5000
    max_pages: 100
    auth:
      type: basic
      config:
        username: elastic
        password: ${env:ES_PASSWORD}
```

### Small lookup index — one request, no scroll

```yaml
source:
  type: elasticsearch
  config:
    base_url: http://localhost:9200
    index: country_codes
    batch_size: 0      # single _search?size=10000, no scroll context
```

### Daily metrics window driven by a matrix context

```yaml
source:
  type: elasticsearch
  config:
    base_url: https://es.example.com:9200
    index: "metrics-${region.name}"
    query:
      range:
        "@timestamp": { gte: now-1h, lt: now }
    auth: { ref: es_token }
```

## Streaming & batching

The source overrides [`Source::stream_pages`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html#tymethod.stream_pages): the pipeline writes documents to the sink as each scroll page lands, so client-side memory stays `O(batch_size)` regardless of the index's total document count.

- `batch_size` (default `1000`) is passed straight to the scroll API as the `size` parameter on the initial `POST /{index}/_search?scroll={timeout}&size={batch_size}`. Each scroll response — initial and follow-up — becomes exactly one `StreamPage`. The trailing empty scroll page is the end-of-scroll sentinel and is consumed but not emitted.
- **`batch_size: 0` is the "no batching" sentinel**: the source skips scroll entirely and issues a single `POST /{index}/_search?size=10000`, emitting one page. The `10000` cap mirrors Elasticsearch's default `index.max_result_window`; indices with a larger window still receive only `10000` hits — switch back to scroll if you need more.
- `batch_size: 0` is also honoured on the buffered `fetch_all` / `fetch_with_context` path — it maps to the same `size=10000` initial search (not the literal `size=0`, which would return zero hits).
- `max_pages`, when set, caps the total number of scroll responses emitted; the cap applies *after* the page is yielded.
- The trait-level `batch_size` argument to `stream_pages` is **ignored** in favour of the config field, so a pipeline-supplied hint never silently overrides an explicit config value.
- This source has no incremental-replication / resume mode today, so every emitted page carries `bookmark: None` (see [Resume & state](#resume--state)). For incremental loads, encode the watermark in the query (e.g. a `range` on `@timestamp`) and drive it from a matrix context or `${now.*}` token.

## Resume & state

The Elasticsearch search source is **stateless** — it does not implement `state_key()` / `apply_start_bookmark()`, so each run re-executes the full query and there is no durable bookmark. Make runs incremental at the query level: filter on a monotonic field (`@timestamp`, a sequence id) and supply the lower bound via a `${now.*}` token, a matrix-context value, or relative date math (`now-1h`).

## Dataset discovery

The source implements [`Source::discover`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html#method.discover) (#211): it lists the cluster's indices via `GET _cat/indices?format=json` (skipping system indices whose name starts with `.`) and its data streams via `GET _data_stream` (kind `data_stream`; their `.ds-*` backing indices are hidden), and returns one `DatasetDescriptor` per index or stream with

- `name` — the index name; `kind` — `"index"`;
- `config_patch` — `{"index": "<name>"}`, ready to deep-merge over the connection config (one matrix row per index);
- `estimated_rows` — parsed from the `_cat` `docs.count` column (`None` when unavailable);
- `schema` — top-level columns from `GET /<index>/_mapping`: `long`/`integer`/`short`/`byte` → `integer`; `double`/`float`/`half_float`/`scaled_float` → `number`; `boolean` → `boolean`; `object`/`nested` (or a type-less field with nested `properties`) → `object`; everything else (`text`, `keyword`, `date`, `ip`, …) → `string`. Mappings carry no nullability, so types stay scalar.

Discovery is catalog metadata only — no document scan. Every request uses the configured `auth`. An index whose mapping cannot be read (closed, no permission) is skipped with a `WARN` instead of failing discovery for the whole cluster.

A parent value substituted into `index` (`logs-{parent.tenant}`) is percent-encoded, so it stays one URL path segment. Auth from a shared provider is resolved per request, so a token that expires during a long scroll is refreshed.

## Config loading & schema

Load from YAML/JSON files, environment variables, or a `.env` file via the helpers in `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_elasticsearch::ElasticsearchSourceConfig;

let config: ElasticsearchSourceConfig = load_json("config.json")?;
let config: ElasticsearchSourceConfig = load_env_file(".env", "ES_SOURCE")?;
```

```env
# .env
ES_SOURCE_BASE_URL=http://localhost:9200
ES_SOURCE_INDEX=my_index
ES_SOURCE_SCROLL_TIMEOUT=1m
ES_SOURCE_BATCH_SIZE=1000
ES_SOURCE_MAX_PAGES=50
```

Inspect the full JSON Schema with:

```bash
faucet schema source elasticsearch
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_elasticsearch::{
    ElasticsearchSource, ElasticsearchSourceConfig, ElasticsearchAuth,
};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = ElasticsearchSourceConfig::new("https://es.example.com:9200", "orders-2026")
    .query(json!({
        "bool": { "filter": [ { "term": { "status": "completed" } } ] }
    }))
    .auth(ElasticsearchAuth::Bearer { token: "your-token".into() })
    .scroll_timeout("5m")
    .with_batch_size(2000)
    .max_pages(50);

// `new` validates batch_size and does no I/O.
let source = ElasticsearchSource::new(config)?;
let orders = source.fetch_all().await?;
println!("Found {} matching orders", orders.len());
# Ok(())
# }
```

For streaming into a sink, build a `Pipeline` (or call `faucet_core::run_stream`) with this source — it drives `stream_pages` automatically so memory stays bounded.

## How it works

1. `new()` validates `batch_size` and builds a `reqwest::Client` **once** (no I/O on construction).
2. The effective `index` and `query` are resolved against the matrix context (placeholder substitution), and auth is resolved once per run and reused across every scroll request.
3. An initial `POST /{index}/_search?scroll={timeout}&size={batch_size}` returns the first page of hits plus a `_scroll_id`.
4. Each scroll response's `hits.hits[*]._source` is extracted into a `StreamPage` and handed to the sink; the source then `POST /_search/scroll`s the `_scroll_id` for the next page.
5. The loop stops on an empty hits array (the end-of-scroll sentinel) or when `max_pages` is reached.
6. An RAII `ScrollGuard` owns the live `_scroll_id` and `DELETE`s the scroll context on **every** exit — clean drain, truncation, error, or drop — by spawning the cleanup so it survives the stream future being cancelled mid-await.

## Lineage dataset URI

`http://<host>:<port>/<index>` with credentials stripped — e.g. `http://localhost:9200/my_index`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-elasticsearch` feature. `ElasticsearchAuth` is re-exported from [`faucet-common-elasticsearch`](https://crates.io/crates/faucet-common-elasticsearch), shared with `faucet-sink-elasticsearch` — `faucet_source_elasticsearch::ElasticsearchAuth` continues to work unchanged.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `401 Unauthorized` / `403` | Missing or wrong credentials. Set `auth` to `basic` / `bearer` / `api_key` with valid values; confirm the user/key has `read` privilege on the index. |
| `auth references provider '<name>' but no provider was supplied` | The config uses `auth: { ref: <name> }` but no matching entry exists in the top-level `auth:` catalog. Add the provider, or switch to inline `{ type, config }`. |
| Auth provider error: *"must yield a bearer or basic credential"* | A shared provider returned a `Header`/`Token` credential, which Elasticsearch can't use. Use a provider type that yields a bearer or basic credential, or inline `api_key` auth. |
| `Search context not found` / scroll expired mid-run | `scroll_timeout` is shorter than the time to process one page downstream. Raise it (e.g. `5m`) so the context outlives each page's sink write. |
| `index_not_found_exception` | The `index` doesn't exist (after any `${...}` substitution). Verify the index/pattern name; patterns like `metrics-*` need `allow_no_indices` semantics on the cluster. |
| `parsing_exception` / `400` on the query | The `query` object isn't a valid query-DSL clause. It is sent verbatim as the body's `query` — wrap a single clause directly (e.g. `{ "match": { … } }`), not inside another `query:` key. |
| Connection refused / TLS error | Wrong `base_url` scheme/port, or a self-signed cert. Use the correct `https://host:9200` and ensure the cert chain is trusted by the host. |
| Resumes re-read everything | This source is stateless by design — encode an incremental watermark in the `query` (see [Resume & state](#resume--state)). |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — the full capability matrix.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — inline vs shared providers.
- [Configuration grammar](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the YAML/JSON config shape.
- [`faucet-sink-elasticsearch`](https://crates.io/crates/faucet-sink-elasticsearch) — the matching bulk-index sink.
- [`faucet-common-elasticsearch`](https://crates.io/crates/faucet-common-elasticsearch) — the shared `ElasticsearchAuth` enum.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="elasticsearch"`.
