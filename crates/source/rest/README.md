# faucet-source-rest

[![Crates.io](https://img.shields.io/crates/v/faucet-source-rest.svg)](https://crates.io/crates/faucet-source-rest)
[![Docs.rs](https://docs.rs/faucet-source-rest/badge.svg)](https://docs.rs/faucet-source-rest)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-rest.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-rest.svg)](https://github.com/faucet-hq/faucet-stream#license)

A declarative, config-driven **REST API source** with pluggable authentication, nine pagination styles, schema inference, and incremental replication. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This is the flagship faucet-stream source: point it at any JSON-over-HTTP API, describe how to authenticate and paginate, and it streams every record page-by-page into any faucet-stream sink — with retries, `Retry-After` honouring, loop detection, and resumable bookmarks — all from one YAML config and no glue code.

## Feature highlights

- **Nine pagination styles** — `Cursor`, `CursorInBody` (POST-search cursor in the request body), `OffsetInBody` (offset/limit in the request body), `RecordFieldCursor` (keyset — page by the running max/min of a record field), `LinkHeader`, `NextLinkInBody`, `PageNumber`, `Offset`, and `None`, each with its own termination/loop guard so a misbehaving API can't loop forever.
- **Eight auth methods** — `bearer`, `basic`, `api_key` (header), `api_key_query`, `oauth2` (client credentials with token caching), `token_endpoint` (fetch a token from an arbitrary endpoint), `custom` headers, and `none` — plus shared `auth: { ref }` providers via the CLI's top-level `auth:` catalog.
- **Mutual TLS** — present a client certificate (PEM pair or PKCS#12) on every request via a `tls:` block (feature `mtls`), for APIs that require client-certificate auth.
- **Memory-bounded streaming** — overrides `Source::stream_pages`, so `Pipeline::run` writes each page to the sink as it arrives; peak memory stays `O(page)` regardless of total record count.
- **Resilient by default** — exponential backoff with jitter (capped at 60 s), `429` `Retry-After` (delta-seconds or HTTP-date) honouring, and a `tolerated_http_errors` allowlist for legitimately-absent resources.
- **Incremental replication** — bookmark by any record field, persist it across runs with a state store, and resume from the last value.
- **Concurrent partitions** — fan a single config across many path substitutions (`/orgs/{org_id}/users`) and fetch them concurrently.
- **Schema inference** — sample records to produce a JSON Schema, or supply your own; Singer/Meltano `primary_keys` / `name` metadata is carried through.
- **Client built once** — the `reqwest` client is constructed in `new()` and reused for every request and partition.

## Installation

```bash
# As a library:
cargo add faucet-source-rest
cargo add tokio --features full

# In the CLI (source-rest is a DEFAULT feature — already enabled):
cargo install faucet-cli
```

The umbrella crate enables it by default too:

```bash
cargo add faucet-stream --features source-rest
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: github_issues_to_jsonl

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.github.com
      path: /repos/faucet-hq/faucet-stream/issues
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:GITHUB_TOKEN}
      query_params:
        state: open
        per_page: "100"
      pagination:
        type: LinkHeader
  sink:
    type: jsonl
    config:
      path: ./out/issues.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core request

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | string | `""` | Base URL of the API (trailing slash trimmed). |
| `path` | string | `""` | URL path relative to `base_url`. Supports `{key}` placeholders for partition substitution (e.g. `/orgs/{org_id}/users`). |
| `method` | string | `GET` | HTTP method for the request. |
| `auth` | `Auth` / `{ ref }` | `none` | Inline `{ type, config }` auth, or a `{ ref: <name> }` pointer to a shared provider. See [Authentication](#authentication). |
| `headers` | map<string,string> | empty | Static HTTP headers sent on **every** request (data pages, async-job requests, and OData `$metadata` probes). Applied *before* auth, so an auth header of the same name wins on a clash. Values honor `${env:}` / `${param.*}` interpolation and pass through the secrets/redaction boundary. An invalid header name/value is rejected at config load. See [Custom request headers](#custom-request-headers). |
| `query_params` | map<string,string> | empty | Query parameters added to every request. |
| `query_params_multi` | map<string,list<string>> | empty | Repeated / array-valued query params, rendered as repeated keys — e.g. `{ "group_by[]": ["api_key_id", "model"] }` → `?group_by[]=api_key_id&group_by[]=model`. Applied alongside `query_params`. |
| `body` | JSON / null | `null` | JSON request body (sent with `Content-Type: application/json`). |

#### Custom request headers

Set arbitrary static headers on every request via the `headers:` map — useful for
APIs that require a fixed non-auth header (e.g. NetSuite SuiteQL's `Prefer:
transient`, Stripe's `Stripe-Version`, Plaid's `Plaid-Version`, or a custom
`Accept`/tenant/feature-flag header):

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /v1/records
    headers:
      Prefer: transient
      Accept: application/json
      X-Tenant: ${env:TENANT_ID}      # ${env:} / ${param.*} interpolation
```

Precedence: config headers are applied **first**, then the auth provider's header
placements — so an auth header (e.g. `Authorization`) always wins over a
same-named config header. Header names/values are validated at config load
(invalid → a typed config error, never a mid-run panic), and values pass through
the secrets/redaction boundary like any other config string.

### Pagination

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination` | `PaginationStyle` | `None` | Pagination strategy. See [Pagination](#pagination). |
| `records_path` | string / null | `null` | JSONPath expression to extract the record array from each response body (e.g. `$.data[*]`). When unset, the whole body is treated as the record set. |
| `drop_key_prefixes` | list | `[]` | Drop per-record keys starting with any of these prefixes — protocol control fields (OData's `@odata.etag`, JSON:API's `links`, HAL's `_links`) are metadata, not data, and are often invalid column names downstream. An `odata:` block implies `@odata.`, so existing OData configs need no change (#654). |
| `max_pages` | int / null | `100` | Hard cap on pages fetched, across **all** pagination styles. `null` removes the cap (rely on the style's own termination). |
| `request_delay` | int (seconds) / null | `null` | Delay between consecutive page requests. |

**Body & keyset pagination.** Two styles page without a query param or a response token:

```yaml
# OffsetInBody — POST-query APIs that carry offset/limit in the JSON body.
pagination: { type: OffsetInBody, offset_field: offset, limit_field: limit, limit: 500, stop_when_short: true }

# RecordFieldCursor — keyset: page by the running max (or min) of a record field.
pagination: { type: RecordFieldCursor, field: JournalNumber, into: query, param: offset, agg: max, page_size: 100, stop_when_short: true }
```

Both stop on a short page (fewer than `limit`/`page_size` records) and guard against a non-advancing cursor.

**Resumable cursor (`persist_cursor`).** With `persist_cursor: true`, a `Cursor` / `CursorInBody` stream emits its terminal cursor as the run's `StreamPage` bookmark (persisted via a `state:` store) and, on the next run, seeds that saved cursor into the first request — so an envelope-cursor feed (e.g. Plaid `/transactions/sync`) resumes incrementally instead of re-pulling from the start.

### Multi-array fan-out (`records_multi`) & envelope carry (`record_ancestors`)

```yaml
# records_multi — emit several arrays from ONE response (one pagination advance),
# each stamped with an op marker for a downstream upsert sink's delete_marker.
records_multi:
  - { path: "$.added[*]",    op: upsert }
  - { path: "$.modified[*]", op: upsert }
  - { path: "$.removed[*]",  op: delete }
op_field: _op          # each record gets { _op: "<op>" }; default "_op"

# record_ancestors — lift fields from the enclosing array-element ancestor onto
# each record when records_path selects a NESTED array (e.g. Stripe events).
records_path: "$.data[*].data.object"
record_ancestors: { event_id: id, event_created: created }
```

`records_multi` is mutually exclusive with `records_path` / `record_ancestors` and requires a JSON response. Pair it with a sink `write_mode: upsert` + `delete_marker: { field: _op, values: [delete] }` to route inserts/updates and deletes from a single sync response.

### Reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | int (seconds) / null | `30` | Per-request HTTP timeout. |
| `max_retries` | int | `3` | Max retries on transient failures. |
| `retry_backoff` | int (seconds) | `1` | Base for exponential backoff. Per-attempt sleep is `retry_backoff × 2^attempt`, **capped at 60 s** and scaled by random jitter in `[0.5, 1.5)` (decorrelated across concurrent retries). On `429`, the server's `Retry-After` (delta-seconds **or** an RFC 7231 HTTP-date) is honoured instead. |
| `tolerated_http_errors` | array<int> | `[]` | HTTP status codes treated as an empty page **on the first request only**. Mid-pagination, a tolerated status surfaces as an error instead of silently ending the stream (otherwise a transient failure on page _N_ would drop every later page as a "successful" run). Only safe for genuinely-empty resources. |

A **`204 No Content`** response — or any `2xx` with an empty/whitespace-only body — is treated as an empty page ("no data"), not a parse error. A non-empty body that isn't valid JSON still fails loudly with `FaucetError::Json`.

### Response format — authenticated CSV / Excel files

By default the REST source parses a **JSON** body and extracts records via `records_path`. Set `response_format` to consume an authenticated **file** endpoint instead — a Microsoft Graph / OneDrive / SharePoint `…/content` download, a signed export URL, or any authed host serving a CSV/Excel file — reusing all of this source's auth (inline **or** a shared `auth: { ref }` provider), retry, and `${...}` substitution.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `response_format` | `json` \| `csv` \| `excel` | `json` | How to parse the body. `csv`/`excel` parse a whole tabular file into records. `excel` requires the crate's `excel` feature. |
| `csv_delimiter` | int (byte) / char | `,` | CSV field delimiter. `response_format: csv` only. |
| `csv_has_headers` | bool | `true` | Whether the first CSV row supplies field names (else `column_0`, `column_1`, …). `csv` only. |
| `excel_sheet` | string / null | first sheet | Worksheet name, or a 0-based index as a string. `excel` only. |
| `excel_header_row` | int | `0` | 0-based index of the Excel header row. `excel` only. |

In file mode a **single** response is fetched: `pagination` must be `none` and `records_path` does not apply (both are rejected at load time). CSV values are strings; Excel numbers/booleans/dates are decoded to their JSON types. Enable Excel with `cargo add faucet-source-rest --features excel` (or `cargo install faucet-cli --features source-rest-excel`).

```yaml
source:
  type: rest
  config:
    base_url: https://graph.microsoft.com
    path: /v1.0/me/drive/items/ITEM_ID/content
    pagination: none
    response_format: excel
    excel_sheet: "Sheet1"
    auth: { ref: graph }   # a shared oauth2_refresh provider
```

#### Unified `resilience:` policy

When driven by the CLI, a pipeline-level [`resilience:`](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html) block can inject one shared retry policy into this source. **Legacy fields win when set explicitly:** if you set `max_retries` or `retry_backoff` to anything other than their defaults (`3` / `1`), the per-connector value is used and the injected policy is ignored for that field — an explicit setting is never silently overridden. Otherwise the injected policy applies.

Because the REST source keeps its own `429`/`Retry-After`-aware retry runner, it honors **only** the injected policy's `max_attempts` (→ `max_retries`) and `base` (→ `retry_backoff`). The policy's `retry_on`, `max` (per-sleep cap), and `jitter` fields are **inert on REST** — they are honored on the `xml`/`graphql` sources and on every sink-side write.

### Replication & state

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replication_method` | `{ type: FullTable \| Incremental }` | `FullTable` | `FullTable` fetches all records; `Incremental` filters by bookmark. |
| `replication_key` | string / null | `null` | Record **field name** (not a JSONPath) used for incremental bookmarking. |
| `start_replication_value` | JSON / null | `null` | Bookmark value; records where `record[replication_key] <= start_replication_value` are filtered out in `Incremental` mode. |
| `state_key` | string / null | `null` | Stable key used by `Pipeline::with_state_store` to persist this stream's bookmark across runs. See [Resume & state](#resume--state). |

#### Server-side incremental push-down (`replication_bind`)

By default `Incremental` mode filters **client-side** (after download). A
`replication_bind` block instead pushes the stored bookmark **into the request**
so the server returns only new rows (the client-side filter stays on as a safety
net). Requires `replication_method: incremental` + `replication_key`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `into` | `query \| header \| body \| path` | `query` | Where to place the rendered bookmark. |
| `name` | string | — | Query param / header / body-field / path-placeholder name. |
| `template` | string | `${bookmark}` | Rendered with `${bookmark}` → the formatted value, e.g. `"gte\|${bookmark}"`, `"[${bookmark} TO *]"`. |
| `format` | `raw \| iso8601 \| epoch_s \| epoch_ms \| date` | `raw` | Value formatting (string↔epoch conversion via a parsed instant). |
| `advance_from` | string / null | `null` | JSONPath into the **response** to advance the bookmark from, instead of `max(record[replication_key])`. |

```yaml
replication_method: { type: incremental }
replication_key: updated_at
start_replication_value: "2024-01-01T00:00:00Z"
replication_bind:
  into: query
  name: updated_after
  template: "gte|${bookmark}"
  format: iso8601
```

#### Datetime window slicing (`window`)

`replication_bind` pushes a single **lower** bound. Some APIs require **both** a
lower and an upper bound and **cap the span** (analytics / ads / reporting feeds
that reject a range over 30 or 90 days) — against those an unbounded incremental
either errors or silently truncates. A `window` block bounds each request to a
rolling `[start, end)` window between the stored bookmark and `now`, iterating the
windows within one run (each `step` wide) and persisting the window's end as the
bookmark — so a mid-sweep crash resumes from the last completed window. This is
parity with Airbyte's `DatetimeBasedCursor`. Requires `replication_method:
incremental` + `replication_key`, and a start bookmark (from a `state:` store or
`start_replication_value`).

Each boundary is rendered through a `WindowBind` (same placement/formatting as
`replication_bind`, with the placeholder `${window}`): the `lower` bind renders
the window start, the `upper` bind the window end.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `step` | string | — | Window size: `45s` / `30m` / `6h` / `30d` (absolute UTC; `d` = 24h), or a bare integer (= seconds). |
| `lower` | `WindowBind` | — | Bind rendered with the window **start**. |
| `upper` | `WindowBind` | — | Bind rendered with the window **end**. |
| `granularity` | string / null | `null` | Subtract from each *rendered* upper bound so `[start, end]` is non-overlapping for inclusive-inclusive APIs (Airbyte `cursor_granularity`). The persisted bookmark stays the true half-open boundary. |
| `lookback` | string / null | `null` | Re-scan this much *before* the bookmark on the first window, for late-arriving rows. |
| `max_windows` | integer | `10000` | Safety cap; on overflow the sweep is truncated (logged) and the next run resumes. |

A `WindowBind` has `into` (`query \| header \| body \| path`, default `query`),
`name`, `template` (default `${window}`, e.g. `"[${window} TO *]"`), and `format`
(`raw \| iso8601 \| epoch_s \| epoch_ms \| date`).

```yaml
replication_method: { type: incremental }
replication_key: date
start_replication_value: "2024-01-01"
window:
  step: 30d
  lookback: 1d
  lower: { into: query, name: start_date, template: "${window}", format: date }
  upper: { into: query, name: end_date,   template: "${window}", format: date }
```

### OData (`odata`)

Speak the OData protocol natively — a single block derives `@odata.nextLink`
paging, the `$.value` envelope, the `$select`/`$filter`/`$expand`/`$orderby`
query options, and the `Prefer: odata.maxpagesize` header. It stays a `rest`
source (no new crate). `faucet discover` reads the service's `$metadata` (EDMX)
and emits one dataset per entity set, with a typed schema.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `version` | `v2 \| v4` | `v4` | Selects the next-link key (`odata.nextLink` vs `@odata.nextLink`). |
| `entity` | string / null | `null` | Entity set → the request path (when `path` is empty). |
| `select` | array<string> | `[]` | `$select` columns. |
| `expand` | array<string> | `[]` | `$expand` related entities (one level). |
| `filter` | string / null | `null` | `$filter` expression (verbatim). |
| `orderby` | string / null | `null` | `$orderby` (verbatim). |
| `page_size` | int / null | `null` | `Prefer: odata.maxpagesize=<n>`. |

```yaml
source:
  type: rest
  config:
    base_url: "https://host/odata/v4"
    odata: { version: v4, entity: Orders, select: [DocEntry, DocDate], page_size: 500 }
```

### Config-driven discovery (`discovery`)

A generic, vendor-neutral discovery recipe: enumerate datasets from a listing
endpoint (or an explicit `objects` list), optionally *describe* each one to
build a typed schema + field list, and *emit* one dataset descriptor per object
— all with plain, templated HTTP calls, so any provider (a CRM's describe API,
a catalog endpoint, a REST admin API) is pure YAML, never a code path.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `list` | object / null | — | Step 1: listing request — `get` (path), `items` (JSONPath to the array), `name` (JSONPath per item), optional `keep_if` predicate and `exclude_name_suffixes`. Omit when `objects` is supplied directly. |
| `objects` | array<string> **or** string | `[]` | Datasets supplied directly — a list or a comma-separated string, so one run-param can drive it (`objects: "${param.objects}"`). |
| `describe` | object / null | — | Step 2: per-dataset field discovery — `get` (templated path), `fields`/`field_name`/`field_type`/`field_nullable` JSONPaths, a `type_map`, and `skip_types`. Omit when the source returns every column by default. |
| `emit.config` | object | — | Step 3: JSON deep-merged into the dataset's source config (e.g. a query built from `${field_names}`). Every string leaf is templated. |
| `emit.table_id` | string / null | — | Sink `table_id` template (e.g. `"raw_${name_snake}"`). |
| `emit.sink_ref` | string / null | `default` | With `fan_out`, the sink template (under `pipeline.sinks`) each dataset routes to. |
| `fan_out` | bool | `false` | **Run-time** fan-out: `faucet run` / `faucet serve` discover the datasets and generate the matrix at trigger time — one generic template syncs any object set passed as a param. `false` ⇒ the block only powers `faucet discover`. |

Template variables available in every emitted string: `${name}` (verbatim),
`${name_snake}`, `${name_lower}`, and — when a `describe` step ran —
`${field_names}` (comma-joined field list).

**Two ways to use it.** *Generate-time* (`faucet discover`, `fan_out: false`)
writes a static matrix you review/commit. *Run-time* (`fan_out: true`) skips
the generate step — register one generic template with `objects` as a param
and it discovers + fans out on every trigger, picking up new fields
automatically:

```yaml
source:
  type: rest
  config:
    base_url: "https://api.example.com"
    auth: { type: token_endpoint, config: { … } }
    async_job:
      submit: { method: POST, url: "/jobs/query", json: { query: "placeholder" } }
      # … poll/fetch …
    discovery:
      fan_out: true
      objects: "${param.objects}"
      describe:
        get: "/objects/${name}/describe"
        fields: "$.fields[*]"
        field_name: "$.name"
        field_type: "$.type"
        field_nullable: "$.nullable"
        type_map: { int: integer, double: number, "*": string }
      emit:
        config:
          async_job: { submit: { json: { query: "SELECT ${field_names} FROM ${name}" } } }
        table_id: "${name_lower}"
        sink_ref: warehouse
# no matrix: — generated live from `objects` at run time
```
```console
$ faucet run pipeline.yaml --param objects="Account,Contact,Lead"   # or "all"
```

The OData block composes with the same fan-out vocabulary (`objects`,
`fan_out`, `emit`) plus a `partition` block for key-range parallel extraction
of large entities:

```yaml
source:
  type: rest
  config:
    base_url: "https://host/data"
    odata:
      version: v4
      fan_out: true
      objects: "${param.objects}"
      emit: { table_id: "raw_${name_snake}", sink_ref: warehouse }
      partition:               # split huge entities into concurrent key ranges
        objects: [LedgerEntries, Transactions]
        workers: 16            # concurrent range readers (default 4, max 64)
        count: 64              # ranges to tile into (default workers × 4)
```

Each `partition.objects` entity whose `$metadata` declares a single **integer**
key is tiled with `faucet_core::shard::plan_pk_shards` (the same primitive the
SQL sources shard with) and fetched as concurrent `$filter` ranges; entities
without such a key fall back to sequential paging. Ranges are planned per run —
never bookmarked.

### Response-decode pipeline (`decode`)

Consume payloads that aren't plain JSON — including files embedded in a JSON/SOAP
envelope. A `decode:` chain runs over the raw response **body** before record
extraction (replaces `response_format` parsing; requires `pagination: none`):

| Step | Form | Effect |
|------|------|--------|
| `extract` | `{ extract: "$.d.reportBytes" }` | Pull a string field out of a JSON envelope. |
| `base64` | `base64` | Base64-decode the buffer. |
| `gunzip` | `gunzip` | Gzip-decompress. |
| `unzip` | `{ unzip: { member: "*.csv" } }` | Extract a zip member (glob; first file if omitted). |
| `parse` | `{ parse: { format: json\|csv\|xlsx\|xml, … } }` | Terminal: parse bytes → records. |

```yaml
decode:
  - extract: "$.d.reportBytes"   # base64 XLSX inside a SOAP/JSON envelope
  - base64
  - parse: { format: xlsx, sheet: "Sheet1", header_row: 4 }
```

### Async-job pattern (`async_job`)

For bulk/export/report-run APIs (Salesforce Bulk, Stripe Reporting, …): submit a
job → poll a status endpoint until terminal → fetch the result → hand it to the
`decode:` pipeline. Requires `pagination: none`.

| Field | Description |
|-------|-------------|
| `submit` | `{ method, url, headers, query, json }` — job-creation request. |
| `job_id` | JSONPath to the job id in the submit response. |
| `poll` | `{ url, method, interval_secs (5), timeout_secs (1800) }` — `${job_id}` substituted. `interval_secs` is the **ceiling** on the poll cadence, not a fixed wait: polling starts at 1s and doubles up to the cap, so a fast job is noticed in ~1s while a long one isn't hammered. |
| `lookback` | Incremental only: re-read margin subtracted from the persisted bookmark (`45s` / `30m` / `6h`, default `5m`) — see below. |
| `status` | `{ path, success: [...], failure: [...] }` — classify the poll response. |
| `fetch` | `{ method, url \| url_from, headers, query, json }` — result download; body flows through `decode:`. Set **exactly one** of `url` (a `${job_id}`-templated path) or `url_from` (a JSONPath into the last poll body — see below). |

```yaml
async_job:
  submit: { method: POST, url: /jobs, json: { query: "SELECT ..." } }
  job_id: "$.id"
  poll:   { url: "/jobs/${job_id}", interval_secs: 5, timeout_secs: 1800 }
  status: { path: "$.state", success: [JobComplete], failure: [Failed, Aborted] }
  fetch:  { url: "/jobs/${job_id}/result" }
decode:
  - parse: { format: csv }
```

#### Resolving the download URL from the poll body (`fetch.url_from`)

Some APIs return the download URL **in the poll response body** rather than at a
deterministic `/{job_id}` path. Set `fetch.url_from` to a JSONPath into the last
(successful) poll response instead of `fetch.url`. Exactly one of `url` /
`url_from` must be set (enforced at config load). The matched value must be a
string; an absolute URL is used verbatim, a relative one is resolved against
`base_url`. Because such links are often one-time/expiring, the URL is fetched
immediately after resolution.

Stripe report runs are the canonical case — poll until `status: succeeded`, then
download the signed CSV link at `result.url`:

```yaml
async_job:
  submit: { method: POST, url: "/v1/reporting/report_runs", json: { report_type: "..." } }
  job_id: "$.id"
  poll:   { url: "/v1/reporting/report_runs/${job_id}", interval_secs: 5, timeout_secs: 1800 }
  status: { path: "$.status", success: [succeeded], failure: [failed] }
  fetch:  { url_from: "$.result.url" }   # download URL comes from the poll body
decode:
  - parse: { format: csv }
```

#### Result-set continuation (`fetch.locator_*`)

When a job's results span several pages behind a continuation locator (e.g. the Salesforce Bulk API's `Sforce-Locator` response header), loop the fetch until the locator is absent:

```yaml
async_job:
  # …submit / poll / status…
  fetch:
    url: "/jobs/${job_id}/result"
    locator_header: "Sforce-Locator"   # or locator_body: "$.nextLocator"
    locator_param: "locator"           # sent as ?locator=<value> on each continuation
    records_path: "$.records[*]"
```

Records are appended across pages; the loop stops when the locator header/body is missing, empty, or matches one of `locator_terminal_values` (default `["null"]` — the Salesforce Bulk sentinel). An API that signals completion differently sets its own list, without a code change:

```yaml
    locator_terminal_values: ["EOF", "-1"]   # replaces the default, does not extend it
```

#### Incremental replication with `async_job`

Set `replication_method: { type: Incremental }` + `replication_key` and the
source pushes the bookmark down into the job itself: the resumed bookmark is
injected as `WHERE <replication_key> > <bookmark>` into the **statement inside
`submit.json`** (wrapping any existing `WHERE`, before trailing clauses;
subqueries and quoted literals are left alone). `query_path` says where that
statement lives — an RFC 6901 JSON Pointer, default `/query`, so an API whose
body reads `{"request": {"sql": "…"}}` sets `query_path: /request/sql` instead
of losing push-down silently. A statement at the configured path is
**required** for this mode — without one the predicate could never apply, so
the config is rejected at validate time rather than silently exporting
full-table on every run. `replication_bind` is mutually exclusive with
`async_job` (the submit query *is* the bind).

The persisted bookmark is the run's **start time minus `lookback`** (default
5m): using the start time keeps the path streaming-native (no row parsing),
and the margin makes it safe against a client clock running ahead of the
server — the cost is a bounded re-read overlap each run, which an upsert sink
dedups. First run (no bookmark) = full export, as expected. The source
auto-opts into resumability in this mode, so add a `state:` block to persist
the bookmark across runs.

```yaml
replication_method: { type: Incremental }
replication_key: SystemModstamp
async_job:
  submit: { method: POST, url: /jobs, json: { operation: query, query: "SELECT Id FROM Lead" } }
  # …job_id / poll / status / fetch…
  lookback: 15m        # optional; default 5m
  query_path: /query   # optional; where the statement sits in submit.json
```

The same pointer names the dataset for catalog and lineage, so pointing it at
the real statement is what keeps one object per dataset rather than every
object collapsing onto one.

#### Native byte passthrough (#633)

When the job's result is CSV, the `decode:` pipeline is exactly one
`parse: { format: csv }` step, and the sink can bulk-load NDJSON natively
(e.g. BigQuery with `media_load: true`), the pipeline streams the fetched
bytes straight into the sink as NDJSON — no `Vec<serde_json::Value>` is ever
built, so peak memory stays flat regardless of row count. Negotiated
automatically; any transform, quality/contract/masking pass, DLQ, or
exactly-once delivery falls back to the ordinary record path.

### Singer / Meltano metadata

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string / null | `null` | Human-readable stream name (logging, Singer SCHEMA messages). |
| `primary_keys` | array<string> | `[]` | Fields that uniquely identify a record (Singer `key_properties`). |
| `schema` | JSON / null | `null` | JSON Schema describing each record. When set, it's returned by `infer_schema()` instead of sampling. |
| `schema_sample_size` | int | `100` | Max records sampled when inferring the schema. `0` = sample all available records. |

### Partitions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `partitions` | array<map> | `[]` | Each entry is a context map substituted into `path` placeholders. The stream runs once per partition and concatenates results. Empty = run once with no substitution. |
| `partition_concurrency` | int / null | `null` | Max partitions fetched concurrently. `null` = sequential. |

## Authentication

The `auth` field accepts the project-wide adjacently-tagged `{ type, config }` shape (snake_case discriminators), or a `{ ref: <name> }` pointer into the CLI's top-level shared `auth:` catalog.

| `type` | `config` fields | Description |
|--------|-----------------|-------------|
| `none` | *(none)* | No authentication. |
| `bearer` | `token` | Bearer token in the `Authorization` header. |
| `basic` | `username`, `password` | HTTP Basic authentication. |
| `api_key` | `header`, `value` | API key sent in a custom request header. |
| `api_key_query` | `param`, `value` | API key sent as a query parameter (e.g. `?api_key=secret`). |
| `oauth2` | `token_url`, `client_id`, `client_secret`, `scopes`, `expiry_ratio` | OAuth2 client-credentials flow with token caching. |
| `token_endpoint` | `url`, `method`, `body`, `encoding` (`json` default / `form`), `token_path`, `expiry_path`, `expiry_ratio` | Fetch a token from an arbitrary HTTP endpoint (JSONPath-extracted). `encoding: form` sends the body `application/x-www-form-urlencoded` — what RFC 6749 token endpoints require. |
| `custom` | `headers` (map<string,string>) | Arbitrary headers attached to every request. |

**`oauth2` / `token_endpoint` notes:** `expiry_ratio` is the fraction of the token lifetime after which the cached token is proactively refreshed — must be in `(0.0, 1.0]`, defaults to `0.9`. For `token_endpoint`, `token_path` is the JSONPath to the token string and `expiry_path` (optional) is the JSONPath to the expiry in seconds; when absent the token is cached indefinitely. A cached token that the API later rejects with **401 Unauthorized** (a server-side expiry the time-based cache can't see, including the cached-indefinitely case) is invalidated and the request is retried once with a freshly-fetched token, so a long run doesn't abort mid-way.

### Mutual TLS (client certificates)

For APIs that require the client to present a certificate (mutual TLS, e.g. ADP),
add a `tls:` block. The identity is attached to the source's HTTP client, so it's
presented on **every** request — data pages *and* any inline token-endpoint call.
Requires the crate's `mtls` feature (`cargo add faucet-source-rest --features mtls`,
or `cargo install faucet-cli --features mtls`); a `tls:` block on a build without
it is a load-time error.

```yaml
config:
  base_url: https://api.eu.adp.com
  tls:
    client_cert: ${file:./cert.pem}   # PEM cert chain (inline / ${file:} / ${secret:})
    client_key:  ${file:./key.pem}    # PEM PKCS#8 private key
    # min_version: "1.2"              # optional: "1.2" | "1.3"
```

| Field | Description |
|-------|-------------|
| `client_cert` + `client_key` | PEM certificate chain + PKCS#8 key (supply together). |
| `client_identity_pkcs12` + `pkcs12_password` | Path to a `.p12`/`.pfx` bundle — an alternative to the PEM pair. |
| `min_version` | Minimum TLS version, `"1.2"` or `"1.3"` (optional). |

Supply **either** the PEM pair **or** the PKCS#12 file. Key material never appears
in logs or errors. A token minted by a shared `auth: { ref }` provider uses that
provider's own client and does **not** present this certificate — use inline auth
for mTLS endpoints.

```yaml
# Bearer
auth:
  type: bearer
  config:
    token: ${env:GITHUB_TOKEN}
```

```yaml
# API key header
auth:
  type: api_key
  config:
    header: X-API-Key
    value: ${env:API_KEY}
```

```yaml
# OAuth2 client credentials
auth:
  type: oauth2
  config:
    token_url: https://auth.example.com/oauth/token
    client_id: ${env:CLIENT_ID}
    client_secret: ${env:CLIENT_SECRET}
    scopes: ["read:events"]
    expiry_ratio: 0.9
```

```yaml
# Shared provider from the top-level auth: catalog
auth: { ref: my_idp }
```

## Examples

### Cursor-paginated API with bearer auth

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /v2/contacts
    auth:
      type: bearer
      config:
        token: ${env:API_TOKEN}
    pagination:
      type: Cursor
      next_token_path: $.meta.next_cursor
      param_name: cursor
    records_path: $.data[*]
    max_pages: 50
```

### OAuth2 + incremental replication with a persisted bookmark

```yaml
version: 1
name: events_incremental
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
      auth:
        type: oauth2
        config:
          token_url: https://auth.example.com/oauth/token
          client_id: ${env:CLIENT_ID}
          client_secret: ${env:CLIENT_SECRET}
          scopes: ["read:events"]
          expiry_ratio: 0.9
      pagination:
        type: Offset
        offset_param: offset
        limit_param: limit
        limit: 100
        total_path: $.total
      records_path: $.events[*]
      replication_method:
        type: Incremental
      replication_key: updated_at
      start_replication_value: "2026-01-01T00:00:00Z"
      state_key: events_stream
  sink:
    type: jsonl
    config:
      path: ./out/events.jsonl
  state:
    type: file
    config:
      path: ./state.json
```

### Multi-partition concurrent fetch

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /orgs/{org_id}/members
    auth:
      type: bearer
      config:
        token: ${env:API_TOKEN}
    records_path: $.members[*]
    partitions:
      - { org_id: acme }
      - { org_id: globex }
      - { org_id: initech }
    partition_concurrency: 3
```

### Tolerating a `429` and capping retries

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /v1/comments
    auth:
      type: api_key
      config:
        header: X-API-Key
        value: ${env:API_KEY}
    pagination:
      type: LinkHeader
    records_path: $.items[*]
    timeout: 60
    max_retries: 5
    retry_backoff: 2
    tolerated_http_errors: [429]
```

## Pagination

The `pagination` field selects a `PaginationStyle` (tagged by `type`). `max_pages` is a hard cap across all styles.

| Style (`type`) | Fields | Stops when |
|----------------|--------|------------|
| `None` | — | After the first page. |
| `Cursor` | `next_token_path`, `param_name` | Next-token JSONPath is null/absent, or the same cursor repeats (loop detection). |
| `CursorInBody` | `next_token_path`, `body_cursor_field` | POST-search endpoints: the next-page cursor is read from the response body and written **into the request JSON body** at `body_cursor_field` (rather than a query param). Stops when the cursor is null/absent or repeats. E.g. HubSpot CRM `POST …/search` — `$.paging.next.after` → `after`. |
| `LinkHeader` | — | No `rel="next"` in the `Link` response header, or the same link repeats. |
| `NextLinkInBody` | `next_link_path` | Next-page URL is absent, null, empty, or repeats. |
| `PageNumber` | `param_name`, `start_page`, `page_size`, `page_size_param` | A zero-record page, or the same body returned twice in a row (content-stagnation detection for APIs that clamp out-of-range pages). |
| `Offset` | `offset_param`, `limit_param`, `limit`, `total_path` | A zero-record page, offset reaches `total` (via `total_path`), or a page returns fewer records than `limit`. |

An HTTP **`204 No Content`** (or any 2xx with an empty body) is treated as an empty page, so a feed that ends with a `204` after its last data page (e.g. ADP's `$top`/`$skip` paging) terminates cleanly rather than erroring.

## Streaming & batching

`RestStream` overrides `Source::stream_pages`, fetching the next HTTP page on demand and yielding it as a `StreamPage`. `Pipeline::run` writes each page to the sink as it arrives, so peak memory is bounded at one page no matter how large the feed. The bookmark is carried on the final page (incremental mode) so the state store advances only after the sink confirms the full run.

The inherent `stream_pages()` method (yielding `Vec<Value>` pages, no per-page bookmark) remains for direct callers, alongside the eager `fetch_all()` / `fetch_all_incremental()` helpers.

## Resume & state

This source supports resumable runs. Set `state_key` and configure a `state:` block (or call `Pipeline::with_state_store` from Rust). On each run the pipeline:

1. loads the previously persisted bookmark and applies it via `apply_start_bookmark` (overriding `start_replication_value`);
2. fetches only records newer than the bookmark (`replication_method: Incremental` + `replication_key`);
3. persists the new bookmark **only after the sink confirms** the batch — so a crash mid-run re-fetches rather than skips.

`state_key` must satisfy `faucet_core::state::validate_state_key`. See the second [example](#oauth2--incremental-replication-with-a-persisted-bookmark) above.

## Config loading & schema introspection

Configs load from YAML/JSON files or environment variables:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_rest::RestStreamConfig;

// From a JSON file:
let config: RestStreamConfig = load_json("config.json")?;

// From a .env file + environment (REST_ prefix):
let config: RestStreamConfig = load_env_file(".env", "REST")?;
```

Example `.env`:

```env
REST_BASE_URL=https://api.github.com
REST_PATH=/repos/faucet-hq/faucet-stream/issues
REST_METHOD=GET
REST_MAX_PAGES=10
REST_TIMEOUT=30
REST_MAX_RETRIES=3
REST_RETRY_BACKOFF=1
REST_SCHEMA_SAMPLE_SIZE=100
```

Inspect the full JSON Schema with:

```bash
faucet schema source rest
```

## Library usage

```rust
use faucet_source_rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RestStreamConfig::new("https://api.example.com", "/v2/contacts")
        .auth(Auth::Bearer { token: "your-api-token".into() })
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.meta.next_cursor".into(),
            param_name: "cursor".into(),
        })
        .records_path("$.data[*]")
        .max_pages(50);

    let stream = RestStream::new(config)?;          // validates auth at construction
    let contacts = stream.fetch_all().await?;
    println!("fetched {} records", contacts.len());
    Ok(())
}
```

Inherent helper methods on `RestStream`:

| Method | Returns | Description |
|--------|---------|-------------|
| `RestStream::new(config)` | `Result<Self, FaucetError>` | Build the stream; validates auth at construction time. |
| `fetch_all()` | `Result<Vec<Value>, _>` | Fetch all records across all pages and partitions. |
| `fetch_all_as::<T>()` | `Result<Vec<T>, _>` | Fetch and deserialize into typed structs. |
| `fetch_all_incremental()` | `Result<(Vec<Value>, Option<Value>), _>` | Fetch with incremental replication; returns records + new bookmark. |
| `infer_schema()` | `Result<Value, _>` | Infer a JSON Schema from sampled records (or return the configured `schema`). |

Attach transforms by wrapping the source with [`faucet_core::TransformingSource`](https://docs.rs/faucet-core/latest/faucet_core/struct.TransformingSource.html).

## How it works

1. `new()` resolves the auth method and builds the `reqwest` client **once**, reusing it for every request and partition.
2. For each partition, `{key}` placeholders in `path` are substituted from the context map; with `partition_concurrency` set, partitions run concurrently.
3. Each page request is wrapped in the retry layer: transient failures back off exponentially with jitter (capped at 60 s); `429` honours `Retry-After`.
4. Records are extracted from the response body via `records_path` (JSONPath); the pagination style decides the next request and when to stop.
5. In `Incremental` mode, records at or before the bookmark are filtered out and the max replication-key value becomes the new bookmark, carried on the final page.

## Lineage dataset URI

`https://<base_url><path>` (credentials stripped) — e.g. `https://api.example.com/v1/users`.

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `transform-flatten` | yes | `Flatten` record transform. |
| `transform-rename-keys` | yes | `RenameKeys` regex-based transform. |
| `transform-keys-case` | yes | `KeysCase` transform (snake / camel / pascal / kebab / screaming_snake). |
| `transform-select` | no | `Select` transform (keep listed top-level fields). |
| `transform-drop` | no | `Drop` transform (remove listed top-level fields). |
| `transform-set` | no | `Set` transform (insert/overwrite constants). |
| `transform-rename-field` | no | `RenameField` transform (exact-name rename). |
| `transform-cast` | no | `Cast` transform (per-field type coercion with `on_error` policy). |
| `transform-redact` | no | `Redact` transform (mask listed field values). |
| `transform-value-case` | no | `ValueCase` transform (lower / upper / trim string values). |
| `transform-spell-symbols` | no | `SpellSymbols` transform (spell out `%`, `#`, `$`, … in keys). |
| `transforms` | no | Enable every transform feature. |

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `401` / `403` on every page | Wrong or missing credentials. Verify the `auth` block; for `bearer`/`api_key`, confirm the token/header value is set (e.g. `${env:GITHUB_TOKEN}` is exported). |
| Auth validation fails at `RestStream::new` | An auth field is malformed — e.g. `expiry_ratio` outside `(0.0, 1.0]`, or an empty required field. Fix the value; auth is validated at construction, not first request. |
| Pagination stops after one page | `pagination` left as the default `None`. Set the style your API uses (`LinkHeader`, `Cursor`, `Offset`, …). |
| Pagination stalls / never advances | The cursor/link token repeated, or the response body was identical twice — loop detection halted it. Check `next_token_path` / `next_link_path` points at the *next* token, not the current one. |
| Records come back empty but the API has data | `records_path` doesn't match the response shape. Test your JSONPath against a real body (e.g. `$.data[*]` vs `$.items[*]`); when unset, the whole body is treated as the record set. |
| Run ends early after a transient `5xx`/`429` mid-pagination | A `tolerated_http_errors` code only short-circuits the **first** request. Mid-stream it errors instead of silently truncating — raise `max_retries` / `retry_backoff` rather than tolerating the code. |
| `FaucetError::Json` on a non-empty body | The response wasn't valid JSON (HTML error page, gateway response). Empty/`204` bodies are fine; a non-empty non-JSON body fails loudly by design. |
| Incremental run re-fetches everything | No `state_key` + `state:` block, so the bookmark isn't persisted; or `replication_method` is still `FullTable`. Set both, plus `replication_key`. |
| Rate-limited despite retries | The API returns `429` without `Retry-After`, or limits are stricter than backoff. Add `request_delay` to space requests, and/or lower `partition_concurrency`. |

## See also

- [Connector catalog & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html)
- [Pagination cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/pagination.html)
- [Resumable state & bookmarks](https://faucet-hq.github.io/faucet-stream/cookbook/state.html)
- [Config-file grammar](https://faucet-hq.github.io/faucet-stream/reference/config.html)
- Related crates: [`faucet-source-graphql`](https://crates.io/crates/faucet-source-graphql), [`faucet-source-xml`](https://crates.io/crates/faucet-source-xml), [`faucet-sink-http`](https://crates.io/crates/faucet-sink-http), [`faucet-auth`](https://crates.io/crates/faucet-auth).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
