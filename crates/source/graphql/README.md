# faucet-source-graphql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-graphql.svg)](https://crates.io/crates/faucet-source-graphql)
[![Docs.rs](https://docs.rs/faucet-source-graphql/badge.svg)](https://docs.rs/faucet-source-graphql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-graphql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-graphql.svg)](https://github.com/faucet-hq/faucet-stream#license)

Config-driven **GraphQL API** source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Executes a GraphQL query against any endpoint, follows Relay-style cursor pagination, extracts records with a JSONPath expression, and streams them page-by-page into any faucet-stream sink — a file, a database, a warehouse, a queue — with one declarative config and no glue code.

Reach for it when you want to pull a paginated GraphQL collection (users, repositories, orders, issues…) out of a SaaS API or your own service. Pages are emitted as they arrive, so peak memory stays bounded no matter how many pages the query walks, and transient HTTP failures are retried automatically.

## Feature highlights

- **Native streaming** — overrides `Source::stream_pages`: every upstream GraphQL response is emitted as one `StreamPage` and written to the sink immediately, so a million-row collection never buffers client-side.
- **Relay cursor pagination** — follows `pageInfo { hasNextPage, endCursor }`, injecting the `endCursor` back into the query's `after:` variable on each request. Stops cleanly when `hasNextPage` is false, the cursor is absent, the same cursor repeats (loop guard), or `max_pages` is reached. If `has_next_page_path` can't be resolved to a boolean on a page, the signal is treated as "unknown" and pagination **defers to cursor presence** (and warns once) rather than silently stopping — so a missing has-next field never drops the remaining pages.
- **Offset pagination** — for query languages that page with `LIMIT … OFFSET …` (ShopifyQL and similar). Injects an integer offset into a GraphQL variable (starting at `0`, advancing by `page_size` after each page) and terminates on a **short page** — fewer than `page_size` records — rather than a `pageInfo` boolean. Set `stop_when_short: false` to keep paginating until a fully empty page instead. See [Offset pagination](#offset-pagination).
- **Incremental replication** — `replication_method: incremental` + `replication_key` filters every page against the stored bookmark and persists a new one on the final page only; `replication_bind` renders the bookmark into a GraphQL variable (a name or a JSON Pointer into `variables`) so the server returns only newer rows. See [Incremental replication](#incremental-replication).
- **Variable injection** — static `variables` from config, plus per-request cursor / page-size variables, plus parent-record context values (`${parent.path}` matrix fan-out) merged into the GraphQL variables map at runtime.
- **JSONPath record extraction** — `records_path` plucks the record array out of any response shape (`$.data.users.edges[*].node`). When unset, the whole `data` object is returned as a single record.
- **Pluggable authentication** — inline Bearer or custom-header auth, or a `{ ref: <name> }` pointer to a shared `auth:` provider so many sources share one token with single-flight refresh.
- **Automatic retries** — transient HTTP failures (5xx, connection resets) are retried up to 3 times with exponential backoff + jitter (500 ms base).
- **Client built once** — the `reqwest` client is constructed in `new()` and reused for every request.

## Installation

```bash
# As a library:
cargo add faucet-source-graphql

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-graphql
```

`source-graphql` is **not** in the CLI default build — enable it explicitly (or use the `full` / `source` aggregate features). Via the umbrella crate: `cargo add faucet-stream --features source-graphql`.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: graphql
    config:
      endpoint: https://api.example.com/graphql
      query: |
        query Users($after: String, $first: Int!) {
          users(first: $first, after: $after) {
            edges { node { id name email createdAt } }
            pageInfo { endCursor hasNextPage }
          }
        }
      auth:
        type: bearer
        config:
          token: ${env:API_TOKEN}
      records_path: $.data.users.edges[*].node
      pagination:
        has_next_page_path: $.data.users.pageInfo.hasNextPage
        cursor_path: $.data.users.pageInfo.endCursor
        cursor_variable: after
        page_size_variable: first
      batch_size: 100
  sink:
    type: jsonl
    config:
      path: ./users.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | — *(required)* | GraphQL endpoint URL. |
| `query` | string | — *(required)* | The GraphQL query string. Declare cursor/page-size variables (`$after`, `$first`) to enable pagination. |
| `variables` | object | `{}` | Static variables merged into every request. Cursor, page-size, and parent-context values are layered on top per request. |
| `records_path` | string | *(unset)* | JSONPath plucking the record array out of the response (e.g. `$.data.users.edges[*].node`). When unset, the whole `data` object is emitted as one record. |
| `auth` | `GraphqlAuth` \| `{ ref }` | `none` | Authentication — inline `{ type, config }` or a shared-provider reference. See [Authentication](#authentication). |

### Pagination

`pagination` accepts one of two shapes, distinguished by the presence of a `type:` field. Omit the block entirely for a single-page query. `max_pages` caps either style.

#### Cursor pagination (Relay, the default shape — no `type:`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination.has_next_page_path` | string | `$.data.*.pageInfo.hasNextPage` | JSONPath to the `hasNextPage` boolean. |
| `pagination.cursor_path` | string | `$.data.*.pageInfo.endCursor` | JSONPath to the `endCursor` string. |
| `pagination.cursor_variable` | string | `after` | GraphQL variable the `endCursor` is injected into on the next request. |
| `pagination.page_size_variable` | string | `first` | GraphQL variable the `batch_size` value is injected into. |

#### Offset pagination (`type: Offset`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination.type` | `"Offset"` | — *(required)* | Selects the offset style. |
| `pagination.offset_variable` | string | — *(required)* | GraphQL variable that receives the current offset (a JSON number, starting at `0`, advancing by `page_size` after each page). Reference it in the query (e.g. `${q_offset}`). |
| `pagination.page_size` | int | — *(required, > 0)* | Records per page. Used to advance the offset and (with `stop_when_short`) to detect the final page. **Not injected** into the request — bake the limit into your query (`LIMIT 250 OFFSET ${q_offset}`). |
| `pagination.stop_when_short` | bool | `true` | Terminate when a page yields fewer than `page_size` records. When `false`, pagination runs until a fully empty page (or `max_pages`). |

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_pages` | int | *(unbounded)* | Hard cap on pages fetched per run (both styles). |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage` **and** the value injected as the page-size variable (`first:`). Max `1_000_000`. **`0` = no batching**: the page-size variable is omitted so the upstream uses its own default page size, and the whole response is emitted as a single page. See [Streaming & batching](#streaming--batching). Validated at config load: an empty `endpoint` / `query`, or a `batch_size` above `MAX_BATCH_SIZE` (1,000,000), is rejected with `FaucetError::Config`. |

### Incremental replication

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replication_method` | `full_table` \| `incremental` | `full_table` | `incremental` filters on `replication_key` and emits a bookmark so the next run resumes. |
| `replication_key` | string | — | Field compared against the bookmark: a top-level name (`updatedAt`), a dot path (`node.updatedAt`), or a JSON Pointer (`/node/updatedAt`). Required for `incremental`. |
| `on_missing_key` | `keep` \| `drop` \| `fail` | `keep` | A record whose key is missing or `null` once a bookmark exists. |
| `start_replication_value` | any | — | Bookmark used when none is stored yet. |
| `state_key` | string | *(derived)* | Key the bookmark is stored under; derived from endpoint + query + variables when unset. The CLI sets a per-row key itself. |
| `replication_bind.variable` | string | — | The GraphQL variable to set: a name (`since`) or a JSON Pointer into `variables` (`/filter/updatedAt/gte`). |
| `replication_bind.template` | string | `${bookmark}` | Rendered with `${bookmark}` replaced, e.g. `updated_at:>${bookmark}`. |
| `replication_bind.format` | `raw` \| `iso8601` \| `epoch_s` \| `epoch_ms` \| `date` | `raw` | How the bookmark is formatted first. |
| `replication_bind.value_type` | `string` \| `number` | `string` | JSON type of the variable. |

```yaml
source:
  type: graphql
  config:
    endpoint: https://shop.example.com/admin/api/2025-07/graphql.json
    query: |
      query($after: String, $first: Int, $query: String) {
        orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
          edges { node { id updatedAt totalPriceSet { shopMoney { amount } } } }
          pageInfo { hasNextPage endCursor }
        }
      }
    variables: { query: "updated_at:>2020-01-01T00:00:00Z" }
    records_path: "$.data.orders.edges[*].node"
    pagination:
      has_next_page_path: "$.data.orders.pageInfo.hasNextPage"
      cursor_path: "$.data.orders.pageInfo.endCursor"
    replication_method: incremental
    replication_key: updatedAt
    replication_bind: { variable: query, template: "updated_at:>${bookmark}", format: iso8601 }
```

- Add a durable `state:` block to the pipeline. The first run sends `variables` as configured; later runs replace the bound
  variable with the rendered bookmark. The client-side `replication_key`
  filter always runs too, so a server that ignores the filter never
  re-delivers old rows.
- The bookmark is attached to the **final** page only (and to a trailing empty
  page when `max_pages` truncates), so a crash between cursor pages re-reads
  the run instead of skipping rows. A GraphQL `errors[]` response fails the
  run before any bookmark is written.
- When the bookmark sits inside a query-language string (Shopify's `query:
  "updated_at:>…"`), escaping it is the template author's job.
- Delivery stays at-least-once: pair it with an upsert sink (`write_mode:
  upsert` + `key`) to make re-reads harmless.

## Authentication

`auth` accepts either an inline `{ type, config }` block or a `{ ref: <name> }` pointer to a provider in the CLI's top-level `auth:` catalog.

| `type` | `config` | Behaviour |
|--------|----------|-----------|
| `none` | *(none)* | No authentication (the default). |
| `bearer` | `{ token: <string> }` | Adds `Authorization: Bearer <token>`. |
| `custom` | `{ headers: { <name>: <value>, … } }` | Attaches arbitrary headers (API keys, cookies, tenant headers) to every request. |

### Mutual TLS (client certificates)

For endpoints requiring a client certificate, add a `tls:` block (requires the
crate's `mtls` feature): a PEM pair (`client_cert` + `client_key`) or a PKCS#12
file (`client_identity_pkcs12` + `pkcs12_password`), optional `min_version`. The
certificate is presented on every request. See the
[`faucet-source-rest` README](../rest/README.md#mutual-tls-client-certificates)
for the full field reference.

```yaml
# Bearer token (resolved from the environment at load time)
auth:
  type: bearer
  config:
    token: ${env:API_TOKEN}
```

```yaml
# Custom headers — API key + tenant
auth:
  type: custom
  config:
    headers:
      X-Api-Key: ${env:API_KEY}
      X-Tenant: acme
```

```yaml
# Shared provider from the top-level auth: catalog
auth: { ref: my_idp }
```

When an `auth: { ref }` is resolved (or a shared provider is attached via `with_auth_provider`), the provider's credential takes precedence over any inline auth and is shared across every source referencing it, with single-flight token refresh.

## Examples

### GraphQL → PostgreSQL with cursor pagination

```yaml
version: 1
name: graphql_to_postgres
pipeline:
  source:
    type: graphql
    config:
      endpoint: https://api.example.com/graphql
      query: |
        query Users($after: String, $first: Int!) {
          users(first: $first, after: $after) {
            edges { node { id name email createdAt } }
            pageInfo { endCursor hasNextPage }
          }
        }
      variables:
        first: 100
      auth:
        type: bearer
        config:
          token: ${env:API_TOKEN}
      records_path: $.data.users.edges[*].node
      pagination:
        has_next_page_path: $.data.users.pageInfo.hasNextPage
        cursor_path: $.data.users.pageInfo.endCursor
        cursor_variable: after
        page_size_variable: first
      batch_size: 100
  sink:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/app
      table_name: users_imported
      column_mapping:
        type: auto_map
      batch_size: 1000
      max_connections: 8
```

### GraphQL → BigQuery with custom-header auth and a page cap

```yaml
version: 1
name: graphql_to_bigquery
pipeline:
  source:
    type: graphql
    config:
      endpoint: https://api.example.com/graphql
      query: |
        query Orders($after: String, $first: Int!) {
          orders(first: $first, after: $after) {
            edges { node { id total status createdAt } }
            pageInfo { endCursor hasNextPage }
          }
        }
      variables:
        first: 200
      auth:
        type: custom
        config:
          headers:
            X-Api-Key: ${env:API_KEY}
            X-Tenant: acme
      records_path: $.data.orders.edges[*].node
      pagination:
        has_next_page_path: $.data.orders.pageInfo.hasNextPage
        cursor_path: $.data.orders.pageInfo.endCursor
        cursor_variable: after
        page_size_variable: first
      max_pages: 500
      batch_size: 200
  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: raw
      table_id: orders
      auth:
        type: application_default
      batch_size: 1000
```

### Single-page query (no pagination)

```yaml
source:
  type: graphql
  config:
    endpoint: https://api.example.com/graphql
    query: "query($id: ID!) { user(id: $id) { name email } }"
    variables:
      id: user-123
    records_path: $.data.user
    # no `pagination:` block → one request, one page
```

### Offset pagination (ShopifyQL)

For APIs whose query language pages with `LIMIT … OFFSET …`, bake the limit into
the query and parameterize only the offset. Pagination stops on the first page
shorter than `page_size`.

```yaml
source:
  type: graphql
  config:
    endpoint: https://shop.example.com/admin/api/2024-10/graphql.json
    query: |
      query($q_offset: Int!) {
        shopifyqlQuery(query: "FROM orders SHOW total_sales LIMIT 250 OFFSET $q_offset") {
          ... on TableResponse { tableData { rowData } }
        }
      }
    auth:
      type: custom
      config:
        headers:
          X-Shopify-Access-Token: ${env:SHOPIFY_TOKEN}
    records_path: $.data.shopifyqlQuery.tableData.rowData[*]
    pagination:
      type: Offset
      offset_variable: q_offset
      page_size: 250
      stop_when_short: true
```

## Streaming & batching

`GraphqlStream` overrides `Source::stream_pages`: each upstream GraphQL response becomes one `StreamPage`, written to the sink as it arrives rather than buffering the entire result set.

- `batch_size` (default `1000`, max `1_000_000`) maps directly to the page-size variable named by `pagination.page_size_variable` (`first:` by default). The upstream ultimately decides how many records to return per request — a server that caps `first:` produces smaller pages than requested.
- **`batch_size = 0` is the "no batching" sentinel**: the page-size variable is omitted from the request so the upstream uses its own default page size, and the whole response is emitted as a single page. Use it for tiny lookup queries or when the upstream default already matches what the sink wants. **If the upstream schema declares the page-size variable as non-null (`first: Int!`), the server returns a GraphQL validation error and the stream surfaces a `FaucetError::Config` — pick a non-zero `batch_size` in that case.**

This is a one-shot query source — page bookmarks are always `None`, so it has **no incremental-replication / resume support** (each run re-walks the query from page one). For incremental loads, encode a watermark in the query and drive it from a matrix context value or a `${now.*}` token.

## Config loading & schema

Load from YAML/JSON or environment:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_graphql::GraphqlStreamConfig;

let config: GraphqlStreamConfig = load_json("config.json")?;
let config: GraphqlStreamConfig = load_env_file(".env", "GRAPHQL")?;
```

Inspect the full JSON Schema with:

```bash
faucet schema source graphql
```

## Library usage

```rust
use faucet_core::{Pipeline, Source};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use faucet_source_graphql::config::{GraphqlAuth, GraphqlPagination};

# async fn run(my_sink: Box<dyn faucet_core::Sink>) -> Result<(), Box<dyn std::error::Error>> {
let config = GraphqlStreamConfig::new(
    "https://api.example.com/graphql",
    r#"query($first: Int!, $after: String) {
        users(first: $first, after: $after) {
            edges { node { id name email } }
            pageInfo { hasNextPage endCursor }
        }
    }"#,
)
.auth(GraphqlAuth::Bearer { token: "your-token".into() })
.records_path("$.data.users.edges[*].node")
.pagination(GraphqlPagination::default())
.with_batch_size(100)
.max_pages(20);

let source = GraphqlStream::new(config);
let pipeline = Pipeline::new(Box::new(source), my_sink);
let result = pipeline.run().await?;
println!("Transferred {} records", result.records_written);
# Ok(())
# }
```

To share one token across many sources, build a provider and inject it with `GraphqlStream::new(config).with_auth_provider(provider)`.

## How it works

1. `new()` builds the `reqwest::Client` **once** and reuses it for every request.
2. Each request merges static `variables`, the current cursor (into `cursor_variable`), the page-size value (into `page_size_variable`, unless `batch_size = 0`), and any parent-record context values into the GraphQL variables map.
3. The POST is retried up to 3 times with exponential backoff + jitter (500 ms base) on retriable HTTP failures. When driven by the CLI, a pipeline-level [`resilience:`](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html) block replaces these built-in retry defaults with one shared policy — GraphQL honors the policy's `max_attempts`, `base`, `max`, `jitter`, and `retry_on` in full.
4. `records_path` extracts the record array via JSONPath; the page is yielded immediately.
5. **Cursor** pagination advances by reading `hasNextPage` / `endCursor`, stopping on a false flag, an absent or repeated cursor (loop guard), or `max_pages`. **Offset** pagination advances an integer offset variable by `page_size` each page, stopping on a short page (`stop_when_short`, default), a fully empty page, or `max_pages`.

## Lineage dataset URI

`https://<endpoint>` with credentials stripped — e.g. `https://api.example.com/graphql`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-graphql` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `401` / `403` from the endpoint | Missing or invalid auth. Set `auth: { type: bearer, config: { token: … } }` (or `custom` headers), and confirm the token has scope for the queried fields. |
| Response has `errors` but no `data` | A GraphQL-level error (bad field, unauthorized field, malformed query). The body's `errors[]` carries the reason — fix the query or permissions. |
| `FaucetError::Config` mentioning a non-null `first:` | You used `batch_size: 0` against a schema that declares `first: Int!`. Set a concrete `batch_size`. |
| Zero records returned despite a valid response | `records_path` doesn't match the response shape. Verify the JSONPath against the actual `data` envelope (e.g. `$.data.<field>.edges[*].node`). |
| Pagination fetches only one page | No `pagination:` block, or `has_next_page_path` / `cursor_path` don't resolve. Point them at the real `pageInfo` location in your response. |
| Pagination never stops / loops | The server returns the same `endCursor` repeatedly — the loop guard stops this automatically; also set `max_pages` as a hard cap. |
| Run re-fetches everything each time | Expected — this source has no resume/bookmark. Encode a watermark in `query` + `variables` and drive it from a matrix context or `${now.*}` token. |
| Transient 5xx still fails the run | Retries are capped at 3 attempts. Persistent 5xx indicates a server-side problem; check the upstream's status. |

## See also

- [Pagination guide](https://faucet-hq.github.io/faucet-stream/cookbook/pagination.html) — cursor pagination across connectors.
- [Authentication guide](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — inline auth and the shared `auth:` catalog.
- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [`faucet-source-rest`](https://crates.io/crates/faucet-source-rest) — the REST sibling source with broader pagination styles.
- [`faucet-auth`](https://crates.io/crates/faucet-auth) — shared OAuth2 / token-endpoint providers.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="graphql"`.

- `faucet_source_roundtrips_total{op="request"}` — one per HTTP request, retries included (#638).
- `faucet_source_throttled_total` — `429` responses received; `faucet_source_throttle_wait_seconds` — the backoff actually slept after each; `faucet_source_retries_total{class}` — every retry by class (#734). The totals also appear on the run's usage record (`throttled 2× · waited 1.2 s` in `faucet run` / `faucet usage`). The sleep after a `429` is the retry policy's backoff (this source does not read `Retry-After`).
