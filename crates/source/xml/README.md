# faucet-source-xml

[![Crates.io](https://img.shields.io/crates/v/faucet-source-xml.svg)](https://crates.io/crates/faucet-source-xml)
[![Docs.rs](https://docs.rs/faucet-source-xml/badge.svg)](https://docs.rs/faucet-source-xml)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-xml.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-xml.svg)](https://github.com/faucet-hq/faucet-stream#license)

A config-driven **XML / SOAP API source** for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. It fetches an XML (or SOAP) HTTP endpoint, converts the response to JSON, pulls the repeating record element out by a dot-separated element path, and streams the records page-by-page into any faucet-stream sink — a file, a database, a warehouse, a queue — with one declarative config and no glue code.

Reach for it when the upstream system only speaks XML or SOAP: legacy enterprise web services, RSS/Atom-style feeds, government data portals, or any HTTP endpoint that returns `application/xml`. The parser is event-driven (streaming `quick-xml`), so peak memory stays bounded by `batch_size` even for large documents.

## Feature highlights

- **Automatic XML → JSON conversion** — every response is parsed into a `serde_json::Value` tree; attributes, text nodes, and repeated elements all map to predictable JSON shapes.
- **Element-path record extraction** — `records_element_path` walks a dot-separated path (e.g. `Envelope.Body.GetUsersResponse.Users.User`) to the repeating element and emits one record per match. A single element collapses to one record; a repeated element fans out to many.
- **First-class SOAP** — a `soap:` block assembles the envelope, injects the version-correct headers (SOAPAction for 1.1, `Content-Type` action param for 1.2), resolves `records_element_path` relative to `Envelope.Body`, and surfaces SOAP `<Fault>` responses as errors. Or drop down to the raw-envelope path (`method: POST` + `body`) any time.
- **Two pagination styles** — page-number and offset/limit, each with a built-in loop guard so a misbehaving endpoint can't spin forever; `max_pages` is a hard cap across both.
- **Pluggable authentication** — `none`, `bearer`, `basic`, or fully custom headers — inline or via a shared provider from the CLI `auth:` catalog (OAuth2 token reuse across matrix rows).
- **Retry with backoff** — transient HTTP failures (5xx / connection resets) are retried up to **3 attempts** with exponential backoff (base 500 ms) via the shared `faucet_core::execute_with_retry`.
- **Bounded-memory streaming** — `Source::stream_pages` accumulates matched subtrees and yields a `StreamPage` every `batch_size` records; `batch_size: 0` drains the whole document into one page.
- **Client built once** — the `reqwest` client is constructed in `new()` and reused for every request and every page.

## Installation

```bash
# As a library:
cargo add faucet-source-xml

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-xml
```

Or pull it in through the umbrella crate:

```bash
cargo add faucet-stream --features source-xml
```

`source-xml` is **not** in the CLI default build — enable it explicitly via the feature flag above.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: xml
    config:
      base_url: https://api.example.com
      path: /users.xml
      method: GET
      records_element_path: Response.Users.User
  sink:
    type: file
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
| `base_url` | string | — *(required)* | Base URL of the API. |
| `path` | string | — *(required)* | Request path appended to `base_url`. |
| `method` | string | `GET` | HTTP method. Use `POST` for SOAP. |
| `body` | string | *(unset)* | Optional **raw** request body — a hand-written SOAP envelope for `POST`. Mutually exclusive with `soap.body_inner`. |
| `soap` | `SoapConfig` | *(unset)* | First-class SOAP ergonomics — see [SOAP](#soap). Sugar over the raw `body` path. |
| `records_element_path` | string | *(unset)* | Dot-separated path to the repeating element in the converted JSON (e.g. `Envelope.Body.GetUsersResponse.Users.User`). When unset, the whole converted document is emitted as one record. With a `soap:` block and `path_relative_to_body` (the default), it is resolved relative to `Envelope.Body` — you write `GetUsersResponse.Users.User`. |
| `query_params` | map<string,string> | `{}` | Query parameters added to every request (in addition to pagination params). |

### Authentication

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth` | `XmlAuth` *(inline `{type,config}`)* or `{ ref: <name> }` | `{ type: none }` | Authentication — see [Authentication](#authentication). |

### Pagination

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination` | `XmlPagination` | *(unset)* | Pagination strategy — see [Pagination](#pagination). When unset, exactly one request is made. |
| `max_pages` | int | *(unset)* | Hard cap on pages fetched, across either pagination style. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. The event-driven parser buffers matched subtrees and yields whenever the buffer reaches this size. **`0` = no batching**: the document is drained end-to-end and the entire result set is emitted in a single page. Validated against `MAX_BATCH_SIZE` (1,000,000). |
| `timeout` | int / null | `30` | Per-request timeout in seconds — a stalled peer fails the request (so retries and resilience act) instead of hanging the run. `null` disables it. |
| `connect_timeout` | int / null | `10` | Timeout for establishing a connection, in seconds. `null` disables it. |

> `headers` exists on the Rust config struct for programmatic use but is `#[serde(skip)]` — it is **not** settable from YAML/JSON. Use `Custom` auth (or `query_params`) to attach request headers from config.

## Authentication

`auth` accepts either an **inline** `{ type, config }` block or a `{ ref: <name> }` pointer to a shared provider declared in the CLI's top-level `auth:` catalog. Inline variants:

| `type` | `config` | Description |
|--------|----------|-------------|
| `none` | *(none)* | No authentication (default). |
| `bearer` | `{ token: <string> }` | `Authorization: Bearer <token>`. |
| `basic` | `{ username, password }` | HTTP Basic authentication. |
| `custom` | `{ headers: { <name>: <value>, … } }` | Arbitrary headers attached to every request — SOAPAction, API keys, content-type overrides, etc. |

```yaml
# Bearer token (read from the environment)
auth:
  type: bearer
  config:
    token: ${env:FEED_TOKEN}
```

### Mutual TLS (client certificates)

For endpoints that require a client certificate, add a `tls:` block (requires the
crate's `mtls` feature). Supply a PEM pair (`client_cert` + `client_key`) or a
PKCS#12 file (`client_identity_pkcs12` + `pkcs12_password`), optionally with
`min_version: "1.2" | "1.3"`. The certificate is presented on every request. See
the [`faucet-source-rest` README](../rest/README.md#mutual-tls-client-certificates)
for the full field reference.

```yaml
# HTTP Basic
auth:
  type: basic
  config:
    username: admin
    password: ${env:XML_PASSWORD}
```

```yaml
# Custom headers — e.g. SOAPAction + an API key
auth:
  type: custom
  config:
    headers:
      SOAPAction: "http://example.com/orders/GetOrders"
      X-API-Key: ${env:API_KEY}
```

```yaml
# Shared provider from the top-level auth: catalog (OAuth2 token reused across rows)
auth: { ref: my_idp }
```

## Examples

### REST XML API with page-number pagination

```yaml
version: 1
pipeline:
  source:
    type: xml
    config:
      base_url: https://api.example.com
      path: /api/products.xml
      records_element_path: Products.Product
      pagination:
        type: PageNumber
        param_name: page
        start_page: 1
        page_size: 50
        page_size_param: per_page
      max_pages: 20
  sink:
    type: file
    config:
      path: ./products.jsonl
```

### SOAP service with Basic auth (first-class `soap:` block)

```yaml
version: 1
pipeline:
  source:
    type: xml
    config:
      base_url: https://soap.example.com
      path: /ws
      method: POST
      auth:
        type: basic
        config:
          username: admin
          password: ${env:SOAP_PASSWORD}
      soap:
        version: "1.1"                # "1.1" (default) or "1.2"
        action: http://example.com/orders/GetOrders
        body_inner: '<GetOrders xmlns="http://example.com/orders"/>'
      # Resolved relative to Envelope.Body by default:
      records_element_path: GetOrdersResponse.Orders.Order
  sink:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      table: orders
```

### Offset-paginated feed → MongoDB (Bearer auth)

```yaml
version: 1
name: xml_to_mongodb
pipeline:
  source:
    type: xml
    config:
      base_url: https://feeds.example.com
      path: /catalog
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:FEED_TOKEN}
      query_params:
        format: xml
      records_element_path: catalog.products.product
      pagination:
        type: Offset
        offset_param: offset
        limit_param: limit
        limit: 250
  sink:
    type: mongodb
    config:
      connection_uri: mongodb://localhost:27017
      database: warehouse
      collection: catalog_items
      batch_size: 1000
```

Runnable copies of the last two shapes live in [`cli/examples/xml_to_mongodb.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/xml_to_mongodb.yaml) and [`cli/examples/xml_to_s3.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/xml_to_s3.yaml).

## SOAP

The optional `soap:` block is **sugar over the XML-over-HTTP request/response path** — it is *not* a WSDL client. When present, the source:

1. **assembles the request envelope** from `version` (the namespace), any declared `namespaces` (prefix → URI on the envelope element), and `body_inner` (placed inside `<soap:Body>`);
2. **injects the version-correct headers**, regardless of the `auth` variant (so real bearer / basic auth stays free):
   - **1.1** → `SOAPAction: "<action>"` header **and** `Content-Type: text/xml; charset=utf-8`;
   - **1.2** → `Content-Type: application/soap+xml; charset=utf-8; action="<action>"` and **no** `SOAPAction` header;
3. **resolves `records_element_path` relative to `Envelope.Body`** when `path_relative_to_body` (default) — you write `GetUsersResponse.Users.User`;
4. **surfaces a SOAP `<Fault>`** as `FaucetError::Source` when `fault_as_error` (default); set it `false` to emit zero records (logged once) instead.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `version` | `"1.1"` \| `"1.2"` | `"1.1"` | SOAP protocol version — selects the envelope namespace and the header shape. |
| `action` | string | *(unset)* | SOAP action. For 1.1 → `SOAPAction` header; for 1.2 → `action` param on `Content-Type`. Omit for actionless operations. |
| `body_inner` | string | *(unset)* | XML fragment placed inside `<soap:Body>` (typically the operation element). Mutually exclusive with the top-level `body`. |
| `namespaces` | map<string,string> | `{}` | Extra `prefix → URI` declarations added to the envelope element. The `soap` prefix is reserved. |
| `path_relative_to_body` | bool | `true` | Auto-prepend `Envelope.Body.` to `records_element_path`. Set `false` to pass a fully-qualified path from the document root. |
| `fault_as_error` | bool | `true` | Raise an error on a SOAP `<Fault>`; when `false`, emit zero records instead. |

> **Load-time validation.** Setting both the top-level `body` and `soap.body_inner` is rejected as ambiguous, and a `soap:` block with `method: GET` is rejected (SOAP is a POST protocol). Both surface as `FaucetError::Config` before any request is made.

### Manual alternative — a raw SOAP envelope

The `soap:` block is optional sugar. You can always hand-write the whole envelope as a raw `body` and target the fully-qualified path (namespace prefixes preserved). This is the pre-`soap:` behavior and is unchanged:

```yaml
version: 1
pipeline:
  source:
    type: xml
    config:
      base_url: https://soap.example.com
      path: /ws
      method: POST
      auth:
        type: custom
        config:
          headers:
            SOAPAction: '"http://example.com/orders/GetOrders"'
      body: |
        <?xml version="1.0"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
          <soapenv:Body>
            <GetOrders xmlns="http://example.com/orders"/>
          </soapenv:Body>
        </soapenv:Envelope>
      records_element_path: soapenv:Envelope.soapenv:Body.GetOrdersResponse.Orders.Order
  sink:
    type: file
    config:
      path: ./orders.jsonl
```

## Pagination

| Style (`type`) | Fields | Stops when |
|----------------|--------|------------|
| `PageNumber` | `param_name`, `start_page`, `page_size` *(optional)*, `page_size_param` *(optional)* | A page returns zero records, or fewer records than `page_size`. |
| `Offset` | `offset_param`, `limit_param`, `limit` | A page returns fewer records than `limit`, or a loop is detected. |
| `BodyCursor` | `next_token_path`, `next_body` | The continuation token is absent/empty, or repeats (loop guard). |

`max_pages` caps the total number of pages for all styles. With no `pagination` block, exactly one request is made.

### Body-cursor pagination (`BodyCursor`, #544)

For stateful XML/SOAP APIs that page with a `readMore`/`resultId` handle carried in the request **body** (an XML gateway's `readByQuery` / `readMore` pair). Each response's continuation token is read via a dot-path (`next_token_path`), and the next request's body is replaced with `next_body` (with `${next_token}` substituted):

```yaml
source:
  type: xml
  config:
    method: POST
    base_url: https://xml-gateway.example.com
    path: /ia/xml/xmlgw.phtml
    body: "<readByQuery><object>GLDETAIL</object><pagesize>1000</pagesize></readByQuery>"
    records_element_path: response.operation.result.data.gldetail
    pagination:
      type: BodyCursor
      next_token_path: response.operation.result.resultId
      next_body: "<readMore><resultId>${next_token}</resultId></readMore>"
```

## Response decode pipeline (`decode:`, #540)

A declarative chain applied to the raw response body **before** record extraction, so the source can consume payloads that aren't plain XML records — including a file embedded in a SOAP envelope. Steps compose left-to-right; the terminal `parse` step turns the bytes into records (when `decode:` is set, records come from it instead of `records_element_path`).

| Step | Effect |
|---|---|
| `extract: "<dot.path>"` | Element text at the dot-path (namespace-insensitive, trailing-match) becomes the buffer. |
| `base64` / `gunzip` | Decode base64 text / gzip-decompress the buffer. |
| `unzip: { member: "*.csv" }` | Select a member from a zip archive. |
| `parse: { format: csv\|xlsx\|xml\|json, header_row, delimiter, has_headers, sheet, records_path }` | Parse the bytes into records. |

Example — a SOAP `runReport` report service returns a base64-encoded XLSX inside `<reportBytes>`:

```yaml
source:
  type: xml
  config:
    method: POST
    base_url: https://fusion.example.com
    path: /xmlpserver/services/v2/ReportService
    soap: { version: "1.1", action: runReport, body_inner: "<v2:runReport>…</v2:runReport>" }
    decode:
      - extract: "runReportResponse.runReportReturn.reportBytes"
      - base64
      - parse: { format: xlsx, header_row: 4 }
```

XLSX parsing requires the crate's **`excel`** feature (CLI: `--features source-xml-excel`); CSV/gzip/zip/base64 are always available.

## Streaming & batching

The source overrides `Source::stream_pages`. The XML response is parsed with an event-driven `quick-xml` reader: only the subtree matching `records_element_path` is materialized, accumulated into a buffer, and yielded as a `StreamPage` whenever the buffer reaches `batch_size`. Memory is bounded at one page regardless of document size. With `batch_size: 0`, the whole document is drained and emitted in a single page — handy for small lookup payloads or for sinks (SQL `COPY`, BigQuery load jobs) that prefer one large request to many small ones.

This is a one-shot fetch source: each run re-requests the endpoint and has no incremental bookmark / resume support. For incremental loads, encode a watermark in `query_params` or the request `body` (e.g. a `since=${now.date}` param) and drive it from a matrix context or `${now.*}` token.

## Config loading & schema introspection

Load from YAML/JSON files, environment variables, or a `.env` file via the helpers in `faucet_core::config`:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_xml::XmlStreamConfig;

let config: XmlStreamConfig = load_json("config.json")?;
let config: XmlStreamConfig = load_env_file(".env", "XML")?;
```

Inspect the full JSON Schema with:

```bash
faucet schema source xml
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_xml::{XmlStream, XmlStreamConfig, XmlAuth, XmlPagination};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = XmlStreamConfig::new("https://feeds.example.com", "/catalog")
    .auth(XmlAuth::Bearer { token: std::env::var("FEED_TOKEN")? })
    .query_param("format", "xml")
    .records_element_path("catalog.products.product")
    .pagination(XmlPagination::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 250,
    })
    .with_batch_size(500);

let records = XmlStream::new(config).fetch_all().await?;
println!("fetched {} records", records.len());
# Ok(())
# }
```

To wire it into a pipeline, hand the `XmlStream` to `faucet_core::Pipeline` (batch) or `faucet_core::run_stream` (streaming) alongside any `Sink`. For shared-token reuse across many sources, build a `faucet_core::AuthProvider` once and inject it with `XmlStream::with_auth_provider(provider)`.

## How it works

1. `new()` builds the `reqwest` client **once** and stores it on the struct.
2. Each page issues one request (`base_url + path` with `query_params` + the pagination param), retried up to **3 times** with exponential backoff (base 500 ms) on transient failures via `faucet_core::execute_with_retry`. Non-cloneable request bodies are not retried (surfaced as `FaucetError::Source`). When driven by the CLI, a pipeline-level [`resilience:`](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html) block replaces these built-in retry defaults with one shared policy — XML honors the policy's `max_attempts`, `base`, `max`, `jitter`, and `retry_on` in full.
3. The response is converted XML → JSON and `records_element_path` is walked to find the repeating element.
4. Records are buffered and yielded as `StreamPage`s of `batch_size`; pagination advances until the stop condition or `max_pages`.

## Lineage dataset URI

`<base_url><path>` with any embedded credentials stripped — e.g. `https://soap.example.com/svc`.

## Feature flags

This crate has no optional features of its own. Enable it in the CLI / umbrella crate via the `source-xml` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| Zero records returned, no error | `records_element_path` doesn't match the converted JSON. Inspect the response (the converter maps attributes and text nodes); fix the dot-path to the *repeating* element, or drop it to emit the whole document as one record. |
| Only one record when you expected many | The path points at a single element rather than the repeated parent's child. Point at the repeating element itself (e.g. `…Users.User`, not `…Users`). |
| `401` / `403` | Auth missing or wrong. Set the right `auth` variant; for SOAP endpoints that gate on `SOAPAction`, add it via `custom` headers. |
| `FaucetError::Source: request is not cloneable for retry` | A streaming/non-cloneable request body can't be retried. Pass the SOAP envelope as a plain `body` string (the default), which is cloneable. |
| Persistent `5xx` after retries | The source retries transient failures **3 times** with backoff before failing. A persistent 5xx is upstream — check the service; raising your own request timeout won't help. |
| Pagination never stops / fetches too much | Set `max_pages` as a hard cap. Confirm `page_size` (PageNumber) or `limit` (Offset) matches what the API actually returns per page so the "fewer than expected" stop condition fires. |
| Headers set in code aren't sent from YAML | `headers` is `#[serde(skip)]` and not configurable from YAML/JSON. Use `custom` auth (or `query_params`) to attach headers from config. |
| Malformed XML / parse error | The body isn't well-formed XML (often an HTML error page returned with a 200). Verify the endpoint and that auth/headers select the XML representation (e.g. `Accept: application/xml`). |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — inline vs shared `auth:` providers.
- [Pagination cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/pagination.html).
- [`faucet-source-rest`](https://crates.io/crates/faucet-source-rest) — the JSON/REST sibling source.
- [`faucet-source-graphql`](https://crates.io/crates/faucet-source-graphql) — GraphQL source.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Observability

Metrics emitted by this source are labelled `connector="xml"`.

- `faucet_source_roundtrips_total{op="request"}` — one per HTTP request, retries included (#638).
- `faucet_source_throttled_total` — `429` responses received; `faucet_source_throttle_wait_seconds` — the backoff actually slept after each; `faucet_source_retries_total{class}` — every retry by class (#734). The totals also appear on the run's usage record (`throttled 2× · waited 1.2 s` in `faucet run` / `faucet usage`). The sleep after a `429` is the retry policy's backoff (this source does not read `Retry-After`).
