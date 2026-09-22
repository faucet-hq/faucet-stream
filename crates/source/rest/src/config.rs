//! Stream configuration and builder.

use crate::auth::Auth;
use crate::pagination::PaginationStyle;
use faucet_core::AuthSpec;
use faucet_core::{ReplicationBind, ReplicationMethod};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// How to parse the response body into records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormat {
    /// JSON — extract records via `records_path` (JSONPath). The default.
    #[default]
    Json,
    /// CSV — parse a tabular file body (each row → a JSON object). For
    /// authenticated file endpoints (e.g. an export URL). Always available.
    Csv,
    /// Excel (`.xlsx`/`.xls`) — parse a workbook body. Requires the crate's
    /// `excel` feature.
    Excel,
}

fn default_method() -> Method {
    Method::GET
}
fn default_auth() -> AuthSpec<Auth> {
    AuthSpec::Inline(Auth::None)
}
fn default_pagination() -> PaginationStyle {
    PaginationStyle::None
}
fn default_max_pages() -> Option<usize> {
    Some(100)
}
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(30))
}
fn default_max_retries() -> u32 {
    3
}
fn default_retry_backoff() -> Duration {
    Duration::from_secs(1)
}
fn default_replication_method() -> ReplicationMethod {
    ReplicationMethod::FullTable
}
fn default_schema_sample_size() -> usize {
    100
}
fn default_csv_delimiter() -> u8 {
    b','
}
fn default_csv_has_headers() -> bool {
    true
}

/// Configuration for a RestStream.
///
/// `#[serde(default)]` at the container level: every field falls back to its
/// value from the [`Default`] impl below when omitted. Without it, **22 fields
/// were required** — a config had to spell out `max_retries`, `retry_backoff`,
/// `tolerated_http_errors`, `primary_keys`, `partitions`,
/// `schema_sample_size` and more before it would deserialize at all. That is
/// why the shipped examples are so verbose, and several of them still omitted
/// one field and could not run; nothing caught it because `faucet validate`
/// did not deserialize connector configs until #609.
///
/// `base_url` stays required — in serde *and* in the generated JSON Schema, so
/// `faucet init` still marks it `# REQUIRED`. Everything else defaults to the
/// value in the [`Default`] impl below.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RestStreamConfig {
    // ── Core request ──────────────────────────────────────────────────────────
    /// Base URL of the API, e.g. `https://api.example.com/v2`. Required. Joined
    /// with [`path`](Self::path); any trailing slash is handled either way.
    pub base_url: String,
    /// URL path, relative to `base_url`. May contain `{key}` placeholders that
    /// are substituted per-partition (e.g. `"/orgs/{org_id}/users"`).
    #[serde(default)]
    pub path: String,
    /// HTTP method for the data request. Defaults to `GET`; use `POST` for
    /// search-style APIs that take a query `body`.
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    #[serde(default = "default_method")]
    pub method: Method,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    #[serde(default = "default_auth")]
    pub auth: AuthSpec<Auth>,
    /// Static request headers sent on **every** request (data pages, async-job
    /// submit/poll/fetch requests, and OData `$metadata` discovery probes).
    /// Applied *before* the auth provider's header placements, so an auth
    /// header of the same name always wins on a clash. Values honor
    /// `${env:}` / `${param.*}` load-time interpolation and pass through the
    /// secrets/redaction boundary like other config strings. Invalid header
    /// names/values are rejected at config load
    /// ([`FaucetError::Config`](faucet_core::FaucetError::Config)), never a
    /// mid-run panic.
    ///
    /// ```yaml
    /// headers:
    ///   Prefer: transient
    ///   Accept: application/json
    /// ```
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub headers: HashMap<String, String>,
    /// Static query-string parameters, rendered as `?k=v`. Values honor
    /// `{placeholder}` context substitution for child sources. Empty by
    /// default.
    ///
    /// The `#[serde(default)]` here is load-bearing: without it this field was
    /// **required**, so any config omitting it failed to deserialize — while
    /// its siblings `headers` and `query_params_multi` both defaulted. Several
    /// shipped examples omitted it and could not run at all, which nothing
    /// caught because `faucet validate` did not deserialize connector configs
    /// until #609.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub query_params: HashMap<String, String>,
    /// Repeated / array-valued query params (#536), rendered as repeated keys —
    /// e.g. `{ "group_by[]": ["api_key_id", "model"] }` → `?group_by[]=api_key_id&group_by[]=model`.
    /// Applied alongside (in addition to) [`query_params`](Self::query_params);
    /// use this for APIs that need a key to appear more than once (`group_by[]`,
    /// repeated `expand`/`fields`). Values honor `{placeholder}` context
    /// substitution for child sources, like `query_params`. Empty by default.
    /// Query parameters that may repeat, e.g. `{ "fields": ["id", "name"] }`
    /// renders `?fields=id&fields=name`. Use this rather than `query_params`
    /// when the API expects a key more than once.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query_params_multi: HashMap<String, Vec<String>>,
    /// JSON request body, for `POST`/`PUT`-style reads. Ignored for `GET`.
    #[serde(default)]
    pub body: Option<Value>,

    // ── Pagination ────────────────────────────────────────────────────────────
    /// How to walk pages. Defaults to `none` (a single request). Each style
    /// carries its own termination guard — see the crate README's pagination
    /// table.
    #[serde(default = "default_pagination")]
    pub pagination: PaginationStyle,
    /// JSONPath to the array of records inside the response body, e.g.
    /// `$.data.items`. When unset the body is expected to *be* the array (or a
    /// single object, which is emitted as one record).
    #[serde(default)]
    pub records_path: Option<String>,
    /// Hard cap on pages fetched per run, across every pagination style — the
    /// backstop against a feed that never signals completion. `None` removes
    /// the cap.
    #[serde(default = "default_max_pages")]
    pub max_pages: Option<usize>,
    /// Fixed delay between page requests, in seconds — a politeness knob for
    /// APIs that rate-limit by request frequency. Applied *in addition* to any
    /// `Retry-After` honoured by the retry path.
    #[serde(with = "faucet_core::config::duration_secs_option", default)]
    #[schemars(with = "Option<u64>")]
    pub request_delay: Option<Duration>,

    // ── Reliability ───────────────────────────────────────────────────────────
    #[serde(
        with = "faucet_core::config::duration_secs_option",
        default = "default_timeout"
    )]
    /// Per-request timeout in seconds. Covers one HTTP request, not the whole
    /// run, so a paginated extract is bounded per page.
    #[schemars(with = "Option<u64>")]
    pub timeout: Option<Duration>,
    /// Number of retries (after the first attempt) for transient request
    /// failures. Default `3`.
    ///
    /// **Precedence note:** the REST source predates the unified pipeline
    /// `resilience:` policy. When this field (or [`retry_backoff`](Self::retry_backoff))
    /// is left at its default, an injected `RetryPolicy` (e.g. from a
    /// pipeline-level `resilience:` block, via
    /// [`RestStream::with_retry_policy`](crate::RestStream::with_retry_policy))
    /// governs the retry budget. Setting this field away from its default makes
    /// it win — an explicit per-connector value is never silently overridden by
    /// a pipeline-wide default.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Base exponential-backoff delay between retries. Default `1s`. Shares the
    /// legacy-field precedence rule documented on [`max_retries`](Self::max_retries).
    #[serde(with = "faucet_core::config::duration_secs")]
    #[schemars(with = "u64")]
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff: Duration,
    /// HTTP status codes that should **not** cause an error. Responses with
    /// these codes are treated as empty pages (no records, no further pages).
    #[serde(default)]
    pub tolerated_http_errors: Vec<u16>,

    // ── Replication ───────────────────────────────────────────────────────────
    /// `full_table` (default) re-reads everything each run; `incremental`
    /// filters on [`replication_key`](Self::replication_key) and emits a
    /// bookmark so the next run resumes.
    #[serde(default = "default_replication_method")]
    pub replication_method: ReplicationMethod,
    /// Field name (not a JSONPath) used for incremental replication bookmarking.
    #[serde(default)]
    pub replication_key: Option<String>,
    /// Bookmark value: records where `record[replication_key] <= start_replication_value`
    /// are filtered out when `replication_method` is `Incremental`.
    #[serde(default)]
    pub start_replication_value: Option<Value>,
    /// Opt-in identifier used by [`Pipeline::with_state_store`](faucet_core::Pipeline::with_state_store)
    /// to persist this stream's bookmark across runs. When set, the pipeline
    /// will load any previously-stored bookmark before fetching and write the
    /// new bookmark only after the sink confirms the batch.
    ///
    /// Keys must satisfy [`faucet_core::state::validate_state_key`].
    #[serde(default)]
    pub state_key: Option<String>,

    // ── Singer / Meltano metadata ─────────────────────────────────────────────
    /// Human-readable stream name (used in logging and Singer SCHEMA messages).
    #[serde(default)]
    pub name: Option<String>,
    /// Field names that uniquely identify a record (Singer `key_properties`).
    #[serde(default)]
    pub primary_keys: Vec<String>,
    /// JSON Schema describing the structure of each record.
    #[serde(default)]
    pub schema: Option<Value>,
    /// Maximum number of records to sample when inferring the schema via
    /// [`crate::stream::RestStream::infer_schema`].  `0` means sample all
    /// available records (up to `max_pages`).  Defaults to `100`.
    #[serde(default = "default_schema_sample_size")]
    pub schema_sample_size: usize,

    // ── Partitions ────────────────────────────────────────────────────────────
    /// Each entry is a context map whose values are substituted into `path`
    /// placeholders. The stream is executed once per partition and results are
    /// concatenated.  Empty means run once with no substitution.
    #[serde(default)]
    pub partitions: Vec<HashMap<String, Value>>,
    /// Maximum number of partitions to fetch concurrently.
    /// `None` (and `0`/`1`) means sequential processing — the default.
    ///
    /// Honoured on **both** read paths since #624: the buffering `fetch_all`
    /// and the `stream_pages` path the pipeline actually drives, where it used
    /// to be silently ignored. Above 1, partition pages **interleave**: the
    /// streams are polled together, so a page from partition 3 can arrive
    /// before partition 1 has finished. Partitions are disjoint and the
    /// persisted bookmark is a max across all of them, so this changes
    /// throughput rather than the resume position — but a downstream that
    /// assumed partition-at-a-time page order no longer gets it.
    #[serde(default)]
    pub partition_concurrency: Option<usize>,

    // ── Mutual TLS ─────────────────────────────────────────────────────────────
    /// Optional client-certificate (mutual TLS) config. When set, the source
    /// presents a client certificate on **every** request — data requests and
    /// any inline auth token request (both go through the same HTTP client).
    /// Requires the crate's `mtls` feature; a `tls` block on a build without it
    /// is a load-time error rather than being silently ignored.
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,

    // ── Response format (#497) ─────────────────────────────────────────────────
    /// How to parse the response body. `json` (default) uses JSONPath
    /// extraction (`records_path`); `csv` / `excel` parse a tabular **file**
    /// body into records — for authenticated file endpoints such as a Microsoft
    /// Graph / OneDrive / SharePoint `…/content` download or any signed export
    /// URL. In file mode a single response is fetched (pagination must be
    /// `none`) and `records_path` does not apply. `excel` requires the crate's
    /// `excel` feature.
    #[serde(default)]
    pub response_format: ResponseFormat,
    /// CSV field delimiter byte (default `,`). Used only when
    /// `response_format: csv`.
    #[serde(default = "default_csv_delimiter")]
    pub csv_delimiter: u8,
    /// Whether the first CSV row is a header row supplying field names
    /// (default `true`). When `false`, fields are named `column_0`, `column_1`, …
    #[serde(default = "default_csv_has_headers")]
    pub csv_has_headers: bool,
    /// Excel worksheet to read: a sheet name, or a 0-based index as a string.
    /// When omitted, the first worksheet is used. `response_format: excel` only.
    #[serde(default)]
    pub excel_sheet: Option<String>,
    /// 0-based index of the Excel header row (default `0`). Rows above it are
    /// skipped; the header row supplies field names. `response_format: excel` only.
    #[serde(default)]
    pub excel_header_row: usize,

    // ── Server-side incremental push-down (#513) ────────────────────────────────
    /// Bind the stored bookmark into the outgoing request (query param / header /
    /// body field / path) so the server returns only new rows. Composes with the
    /// existing `replication_key` client-side filter, which stays active as a
    /// safety net. Requires `replication_method: incremental` + `replication_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_bind: Option<ReplicationBind>,

    // ── OData (#512) ────────────────────────────────────────────────────────────
    /// Speak the OData protocol: `@odata.nextLink` paging, the `$.value`
    /// envelope, `$select`/`$filter`/`$expand`/`$orderby` sugar, and
    /// `$metadata` (EDMX) → schema discovery. When set, it derives the
    /// pagination, `records_path`, query params, and `Prefer` header at load
    /// time (explicit values still win). See [`ODataConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub odata: Option<ODataConfig>,
    /// Drop per-record keys starting with any of these prefixes (#654 M24).
    ///
    /// Protocol control fields — OData's `@odata.etag` / `@odata.editLink`,
    /// JSON:API's `links`, HAL's `_links` — are metadata, not data, and are
    /// often invalid column names downstream. An `odata:` block implies
    /// `@odata.` (the prefix that used to be hardcoded), so existing configs
    /// need no change; list prefixes here for any other protocol envelope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drop_key_prefixes: Vec<String>,

    // ── Response-decode pipeline (#515) ─────────────────────────────────────────
    /// Decode the response body before record extraction: a chain of
    /// `extract` (JSONPath) / `base64` / `gunzip` / `unzip` / `parse`
    /// (json|csv|xlsx|xml) steps. Lets a source consume base64/compressed/file
    /// payloads (e.g. a base64 XLSX inside a SOAP body, or a gzipped-CSV export).
    /// When set, it replaces the `response_format` body parsing, and pagination
    /// must be `none`. See [`DecodeStep`](crate::decode::DecodeStep).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decode: Vec<crate::decode::DecodeStep>,

    // ── Async-job pattern (#514) ────────────────────────────────────────────────
    /// Run a submit→poll→fetch job lifecycle instead of a single GET, for
    /// bulk/export/report-run APIs (Salesforce Bulk, Stripe Reporting, …). The
    /// fetched result flows through `decode:` / `response_format`. When set,
    /// pagination must be `none`. See [`AsyncJobConfig`](crate::async_job::AsyncJobConfig).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_job: Option<crate::async_job::AsyncJobConfig>,

    // ── In-run datetime window slicing (#527) ───────────────────────────────────
    /// Bound each request to a rolling `[start, end)` window between the stored
    /// bookmark and `now`, iterating the windows within one run (each `step`
    /// wide) with per-window bookmark durability. For APIs that require — or cap —
    /// a bounded date range (analytics/ads/reporting feeds). Parity with Airbyte's
    /// `DatetimeBasedCursor`. Requires `replication_method: incremental` +
    /// `replication_key`, and a start bookmark (from state, or
    /// `start_replication_value`). See [`WindowSpec`](faucet_core::WindowSpec).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<faucet_core::WindowSpec>,

    // ── Envelope-ancestor lifting (#549) ────────────────────────────────────────
    /// When `records_path` selects a **nested** array element (e.g.
    /// `$.data[*].data.object`), copy fields from the enclosing `[*]`
    /// array-element ancestor onto each emitted record. The map is
    /// `dest_field: ancestor_relative_path` — for each matched leaf, the source
    /// walks up to the array-element ancestor and copies the named path onto the
    /// record under `dest_field`. Absent ⇒ records are emitted unchanged.
    ///
    /// ```yaml
    /// records_path: "$.data[*].data.object"
    /// record_ancestors: { event_id: "id", event_created: "created" }
    /// ```
    ///
    /// Requires `records_path` to contain an array wildcard `[*]`; mutually
    /// exclusive with [`records_multi`](Self::records_multi).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_ancestors: Option<HashMap<String, String>>,

    // ── Multi-array fan-out (#548) ──────────────────────────────────────────────
    /// Emit several record arrays from one response in a single page (sharing one
    /// pagination advance), each stamped with a user-defined op marker under
    /// [`op_field`](Self::op_field). Composes with a downstream
    /// `write_mode: upsert` + `delete_marker` so added/modified/removed feeds
    /// route correctly. Mutually exclusive with `records_path` /
    /// `record_ancestors`, and requires `response_format: json` with no
    /// `decode:` pipeline.
    ///
    /// ```yaml
    /// records_multi:
    ///   - { path: "$.added[*]",    op: upsert }
    ///   - { path: "$.modified[*]", op: upsert }
    ///   - { path: "$.removed[*]",  op: delete }
    /// op_field: _op
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records_multi: Vec<RecordsMultiSpec>,
    /// Field name each [`records_multi`](Self::records_multi) record is stamped
    /// with its spec's `op` value. Defaults to `_op` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_field: Option<String>,

    // ── Resumable cursor (#547) ─────────────────────────────────────────────────
    /// Persist the terminal pagination cursor as this run's bookmark (riding the
    /// existing `StreamPage.bookmark` / `StateStore` path — no core trait change)
    /// and, on resume, seed the stored bookmark back into the first request
    /// (query param for `cursor`, request body field for `cursor_in_body`) before
    /// paging. Only meaningful with `pagination: cursor` / `cursor_in_body`;
    /// mutually exclusive with `window` slicing. Default `false`.
    #[serde(default)]
    pub persist_cursor: bool,

    // ── Config-driven discovery (#647) ──────────────────────────────────────────
    /// Generic, vendor-neutral discovery recipe: declare which API calls to make
    /// and how to extract datasets from their JSON responses (reusing this
    /// source's `auth` / `base_url` / `headers`). Powers `faucet discover` and
    /// run-time fan-out for any REST API with a listing / describe shape, with no
    /// connector-specific code. See [`DiscoverySpec`](crate::discovery::DiscoverySpec).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<crate::discovery::DiscoverySpec>,
}

/// One entry in a [`RestStreamConfig::records_multi`] fan-out (#548): a JSONPath
/// selecting an array of records, plus the op marker each is stamped with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordsMultiSpec {
    /// JSONPath selecting an array of records (e.g. `"$.added[*]"`).
    #[serde(default)]
    pub path: String,
    /// Op marker stamped onto each record from `path` under
    /// [`RestStreamConfig::op_field`] (e.g. `upsert` / `delete`, or `u` / `d`).
    pub op: String,
}

/// OData protocol version, which selects the paging-link key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum ODataVersion {
    /// OData v2 — JSON-light next link `odata.nextLink`.
    V2,
    /// OData v4 (default) — next link `@odata.nextLink`.
    #[default]
    V4,
}

impl ODataVersion {
    /// JSONPath to the next-page link for this version.
    pub fn next_link_path(self) -> &'static str {
        match self {
            // Bracketed single-quoted keys — `@`/`.` aren't bare-identifier
            // chars, and jsonpath-rust wants `$['key']`, not `$."key"`.
            ODataVersion::V2 => "$['odata.nextLink']",
            ODataVersion::V4 => "$['@odata.nextLink']",
        }
    }
}

/// OData protocol options for the REST source (#512).
///
/// A minimal block — `{ entity: Orders }` — is enough; it derives paging,
/// the `$.value` envelope, and (for `faucet discover`) `$metadata` parsing.
/// The query-option fields render into the standard `$select`/`$filter`/
/// `$expand`/`$orderby` params.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct ODataConfig {
    /// Protocol version (default `v4`).
    #[serde(default)]
    pub version: ODataVersion,
    /// Entity set to read (appended to `base_url` as the path, e.g. `Orders`).
    /// Optional when the path already names the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// `$select` — columns to return.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub select: Vec<String>,
    /// `$expand` — related entities to inline (one level).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expand: Vec<String>,
    /// `$filter` — server-side filter expression (verbatim).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// `$orderby` — server-side ordering (verbatim).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orderby: Option<String>,
    /// Server page size, sent as `Prefer: odata.maxpagesize=<n>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,

    // ── Run-time fan-out over entity sets ─────────────────────────────────────────
    /// **Experimental** (PRINCIPLES.md §3): the fan-out key group may change
    /// shape in a minor release; changes are called out in the changelog.
    ///
    /// Fan out **at run time**: when `true`, `faucet run` / `faucet serve` turn the
    /// [`objects`](Self::objects) list into one matrix row per entity set (each
    /// with its `odata.entity` selected and its sink `table_id` rendered from
    /// [`emit`](Self::emit)), so one generic template with
    /// `objects: "${param.objects}"` syncs any entity set passed at trigger — no
    /// pre-generated matrix, no field discovery needed (OData returns every column
    /// by default). Default `false` (the block then only affects a single-entity
    /// run / `faucet discover`). Same key and semantics as `discovery.fan_out`.
    #[serde(default)]
    pub fan_out: bool,
    /// Entity sets to select — a YAML list **or** a comma-separated string (so a
    /// single string run-param can drive it: `objects: "${param.objects}"`).
    /// Empty means fall back to `$metadata` discovery of every declared entity
    /// set (`faucet discover`). Same key and semantics as `discovery.objects`.
    #[serde(
        default,
        deserialize_with = "de_objects",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub objects: Vec<String>,
    /// What each discovered/fanned-out entity emits (sink `table_id` template,
    /// sink routing, extra per-entity config). Same shape as `discovery.emit`.
    /// Unset ⇒ defaults (`table_id: "${name_snake}"`, the default sink).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emit: Option<EmitSpec>,
    /// Key-range partitioned extraction: split large entities into contiguous
    /// primary-key ranges fetched concurrently, instead of one sequential
    /// `@odata.nextLink` page walk. See [`PartitionSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionSpec>,
}

/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
///
/// What each discovered dataset **emits** — shared by every discovery mechanism
/// (`discovery.emit` and `odata.emit`), so the vocabulary is identical wherever
/// datasets fan out. All string leaves are templates over the dataset:
/// `${name}` (verbatim), `${name_snake}`, `${name_lower}`, and (where a
/// `describe` step ran) `${field_names}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct EmitSpec {
    /// A JSON object deep-merged into the dataset's source config (e.g. an
    /// `async_job` query, or extra per-entity overrides). Every string leaf is
    /// templated. Optional for mechanisms whose selection patch is implicit
    /// (OData sets `odata.entity` itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    /// Sink `table_id` template (e.g. `"raw_${name_snake}"`). Rendered per
    /// dataset into the descriptor's `sink_patch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    /// Sink template (an entry under `pipeline.sinks`) each fanned-out dataset
    /// routes to. Unset ⇒ the default (singular `pipeline.sink`) template, with
    /// the rendered `table_id` merged in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_ref: Option<String>,
}

/// Default concurrent range readers per partitioned dataset — conservative
/// because all readers share the source's request quota.
pub const DEFAULT_PARTITION_WORKERS: usize = 4;
/// Upper clamp on range readers: concurrency gains are sublinear and a typo
/// (`workers: 6400`) must not spawn thousands of tasks against one API.
pub const MAX_PARTITION_WORKERS: usize = 64;
/// Default ranges per worker: over-tiling (×4) gives early-finishing workers
/// more ranges to pick up, smoothing key-space skew.
pub const RANGES_PER_WORKER: usize = 4;
/// Upper clamp on the number of ranges — beyond this the per-range bound
/// requests outweigh the skew benefit.
pub const MAX_PARTITION_COUNT: usize = 256;

/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
///
/// Key-range partitioned extraction of a paged dataset (`odata.partition`):
/// tile the dataset's integer primary-key space into contiguous ranges and
/// fetch them concurrently. Range planning is
/// [`faucet_core::shard::plan_pk_shards`] — the same primitive the SQL sources
/// shard with, including its unbounded first/last ranges (rows inserted below
/// MIN / above MAX during the run are still read).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct PartitionSpec {
    /// With `fan_out`: which datasets to partition (a YAML list or a
    /// comma-separated string). Each listed dataset whose `$metadata` declares a
    /// **single integer** key partitions on it; others fall back to sequential
    /// paging. Empty on a single-entity run (where [`key`](Self::key) applies
    /// directly).
    #[serde(
        default,
        deserialize_with = "de_objects",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub objects: Vec<String>,
    /// Integer key column to range-split on. On the fan-out path this is derived
    /// from `$metadata` per dataset; set it explicitly for a single-entity run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Concurrent range readers (default [`DEFAULT_PARTITION_WORKERS`], clamped
    /// to [`MAX_PARTITION_WORKERS`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<usize>,
    /// Number of ranges to tile into (default `workers ×`
    /// [`RANGES_PER_WORKER`], clamped to [`MAX_PARTITION_COUNT`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
}

impl PartitionSpec {
    /// Concurrent range readers, clamped to `1..=`[`MAX_PARTITION_WORKERS`].
    pub fn resolved_workers(&self) -> usize {
        self.workers
            .unwrap_or(DEFAULT_PARTITION_WORKERS)
            .clamp(1, MAX_PARTITION_WORKERS)
    }

    /// Ranges to tile into (default `workers × `[`RANGES_PER_WORKER`], clamped
    /// to [`MAX_PARTITION_COUNT`]).
    pub fn resolved_count(&self) -> usize {
        self.count
            .filter(|&n| n > 0)
            .unwrap_or(self.resolved_workers() * RANGES_PER_WORKER)
            .min(MAX_PARTITION_COUNT)
    }
}

/// Deserialize `objects` from either a YAML sequence or a comma-separated string
/// (so a single string run-param can drive it). `"all"` (any case) → empty
/// (= discover every queryable object). Blanks are trimmed and dropped.
pub(crate) fn de_objects<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrSeq {
        Str(String),
        Seq(Vec<String>),
    }
    let split = |s: &str| -> Vec<String> {
        if s.trim().eq_ignore_ascii_case("all") {
            return Vec::new();
        }
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    };
    Ok(match StrOrSeq::deserialize(d)? {
        StrOrSeq::Str(s) => split(&s),
        StrOrSeq::Seq(v) => v.into_iter().flat_map(|s| split(&s)).collect(),
    })
}

pub use faucet_core::TlsClientConfig;

/// Build a validated [`HeaderMap`] from the static `headers` string map.
///
/// Invalid header names/values become a typed
/// [`FaucetError::Config`](faucet_core::FaucetError::Config) so a malformed
/// header fails at config load rather than panicking mid-run. Used both by
/// [`RestStreamConfig::validate`] (to fail loudly at load) and by the request
/// path (which reuses the already-validated map).
pub(crate) fn build_header_map(
    headers: &HashMap<String, String>,
) -> Result<HeaderMap, faucet_core::FaucetError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let hn = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
            faucet_core::FaucetError::Config(format!("rest: invalid header name '{name}': {e}"))
        })?;
        let hv = HeaderValue::from_str(value).map_err(|e| {
            faucet_core::FaucetError::Config(format!(
                "rest: invalid value for header '{name}': {e}"
            ))
        })?;
        map.insert(hn, hv);
    }
    Ok(map)
}

impl Default for RestStreamConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            path: String::new(),
            method: Method::GET,
            auth: AuthSpec::Inline(Auth::None),
            drop_key_prefixes: Vec::new(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            query_params_multi: HashMap::new(),
            body: None,
            pagination: PaginationStyle::None,
            records_path: None,
            max_pages: Some(100),
            request_delay: None,
            timeout: Some(Duration::from_secs(30)),
            max_retries: 3,
            retry_backoff: Duration::from_secs(1),
            tolerated_http_errors: Vec::new(),
            replication_method: ReplicationMethod::FullTable,
            replication_key: None,
            start_replication_value: None,
            state_key: None,
            name: None,
            primary_keys: Vec::new(),
            schema: None,
            schema_sample_size: 100,
            partitions: Vec::new(),
            partition_concurrency: None,
            tls: None,
            response_format: ResponseFormat::Json,
            csv_delimiter: b',',
            csv_has_headers: true,
            excel_sheet: None,
            excel_header_row: 0,
            replication_bind: None,
            odata: None,
            decode: Vec::new(),
            async_job: None,
            window: None,
            record_ancestors: None,
            records_multi: Vec::new(),
            op_field: None,
            persist_cursor: false,
            discovery: None,
        }
    }
}

impl RestStreamConfig {
    /// Validate cross-field invariants that serde alone can't express.
    ///
    /// File response formats (`csv` / `excel`) fetch a single response and
    /// parse the whole body, so paginated / JSONPath-extracted requests are
    /// rejected rather than silently ignored.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        // The one field with no sensible default. Checked here rather than left
        // to serde so the message names the knob and the connector.
        if self.base_url.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "rest: `base_url` is required (e.g. `base_url: https://api.example.com`)".into(),
            ));
        }
        // Static custom headers: reject an invalid header name/value at load
        // time rather than panicking on the first request (#539).
        build_header_map(&self.headers)?;
        if !matches!(self.response_format, ResponseFormat::Json) {
            if !matches!(self.pagination, PaginationStyle::None) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `response_format: csv|excel` fetches a single file body and does not \
                     paginate — set `pagination: none`"
                        .into(),
                ));
            }
            if self.records_path.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_path` (JSONPath) does not apply to `response_format: csv|excel` \
                     — the whole file body becomes the record set"
                        .into(),
                ));
            }
        }
        if let Some(bind) = &self.replication_bind {
            bind.validate()?;
            if !matches!(self.replication_method, ReplicationMethod::Incremental) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `replication_bind` requires `replication_method: incremental`".into(),
                ));
            }
            if self.replication_key.is_none() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `replication_bind` requires `replication_key` (the field whose \
                     bookmark is pushed down)"
                        .into(),
                ));
            }
        }
        if self.odata.is_some() && !matches!(self.response_format, ResponseFormat::Json) {
            return Err(faucet_core::FaucetError::Config(
                "rest: `odata` speaks JSON — remove `response_format: csv|excel`".into(),
            ));
        }
        if !self.decode.is_empty() {
            if !matches!(self.pagination, PaginationStyle::None) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: a `decode:` pipeline consumes a single response body — set \
                     `pagination: none`"
                        .into(),
                ));
            }
            if !matches!(self.response_format, ResponseFormat::Json) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `decode:` replaces `response_format` body parsing — remove \
                     `response_format: csv|excel`"
                        .into(),
                ));
            }
        }
        if let Some(job) = &self.async_job {
            job.validate()?;
            // Size-based routing (#629) is the one shape that legitimately
            // carries both: the job for large objects, and the ordinary
            // paginated read — which needs its own pagination style — for
            // small ones.
            if !matches!(self.pagination, PaginationStyle::None) && job.sync_routing().is_none() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: an `async_job:` lifecycle fetches a single result — set \
                     `pagination: none` (or add `sync_below_rows` + `count:` to route \
                     small objects to the paginated path, #629)"
                        .into(),
                ));
            }
            if matches!(self.replication_method, ReplicationMethod::Incremental) {
                // The async-job incremental predicate (#630) is injected into
                // the submit body's top-level string `query`. Without one the
                // predicate can never apply: every run would silently stay a
                // full export while the bookmark advances — a lying high-water
                // mark. Fail before the first byte moves instead.
                if self.replication_key.is_none() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `replication_method: incremental` with `async_job` requires \
                         `replication_key` (the field the submit query is filtered on)"
                            .into(),
                    ));
                }
                if !job.supports_incremental_query() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `replication_method: incremental` with `async_job` requires a \
                         top-level string `query` in `async_job.submit.json` — that is where the \
                         `WHERE <replication_key> > <bookmark>` predicate is injected. Without \
                         one every run is a full export, so use `replication_method: full_table`"
                            .into(),
                    ));
                }
            }
            if self.replication_bind.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `replication_bind` and `async_job` are mutually exclusive — the \
                     async-job path pushes the bookmark down by injecting a predicate into \
                     `async_job.submit.json.query`, not via a request bind"
                        .into(),
                ));
            }
        }
        if let Some(window) = &self.window {
            window.validate()?;
            if !matches!(self.replication_method, ReplicationMethod::Incremental) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing requires `replication_method: incremental`".into(),
                ));
            }
            if self.replication_key.is_none() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing requires `replication_key` (the datetime cursor field)"
                        .into(),
                ));
            }
            if self.async_job.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing and `async_job` are mutually exclusive — the async-job \
                     lifecycle fetches a single result and does not slice by window"
                        .into(),
                ));
            }
        }
        // #548: multi-array fan-out is its own extraction mode.
        if !self.records_multi.is_empty() {
            if self.records_path.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_multi` and `records_path` are mutually exclusive — \
                     `records_multi` names the arrays itself"
                        .into(),
                ));
            }
            if self.record_ancestors.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_multi` and `record_ancestors` are mutually exclusive".into(),
                ));
            }
            if !matches!(self.response_format, ResponseFormat::Json) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_multi` extracts JSON arrays — remove `response_format: csv|excel`"
                        .into(),
                ));
            }
            if !self.decode.is_empty() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_multi` and a `decode:` pipeline are mutually exclusive".into(),
                ));
            }
            for spec in &self.records_multi {
                if spec.path.trim().is_empty() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: each `records_multi[].path` must not be empty".into(),
                    ));
                }
                if spec.op.trim().is_empty() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: each `records_multi[].op` must not be empty".into(),
                    ));
                }
            }
            if let Some(f) = &self.op_field
                && f.trim().is_empty()
            {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `op_field` must not be empty".into(),
                ));
            }
        } else if self.op_field.is_some() {
            return Err(faucet_core::FaucetError::Config(
                "rest: `op_field` only applies to `records_multi`".into(),
            ));
        }
        // #549: envelope-ancestor lifting requires a nested array records_path.
        if let Some(anc) = &self.record_ancestors
            && !anc.is_empty()
        {
            match &self.records_path {
                Some(rp) if rp.contains("[*]") => {}
                Some(_) => {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `record_ancestors` requires `records_path` to select a nested array \
                         element (a path containing `[*]`, e.g. `$.data[*].data.object`)"
                            .into(),
                    ));
                }
                None => {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `record_ancestors` requires `records_path`".into(),
                    ));
                }
            }
            for (dest, rel) in anc {
                if dest.trim().is_empty() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `record_ancestors` destination field names must not be empty".into(),
                    ));
                }
                if rel.trim().is_empty() {
                    return Err(faucet_core::FaucetError::Config(
                        "rest: `record_ancestors` ancestor paths must not be empty".into(),
                    ));
                }
            }
        }
        // #547: resumable cursor is only meaningful for cursor pagination.
        if self.persist_cursor {
            if !matches!(
                self.pagination,
                PaginationStyle::Cursor { .. } | PaginationStyle::CursorInBody { .. }
            ) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `persist_cursor` requires `pagination: cursor` or `cursor_in_body`"
                        .into(),
                ));
            }
            if self.window.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `persist_cursor` and `window` slicing are mutually exclusive".into(),
                ));
            }
        }
        if let Some(discovery) = &self.discovery {
            discovery.validate()?;
        }
        Ok(())
    }

    /// Derive request defaults from the `odata:` block (paging, `$.value`
    /// envelope, `$select`/`$filter`/`$expand`/`$orderby` params, and the
    /// `Prefer` page-size header). Explicit config always wins — a field the
    /// user already set is never overwritten. Idempotent.
    pub fn apply_odata_defaults(&mut self) {
        let Some(odata) = self.odata.clone() else {
            return;
        };
        // Entity → path when the path doesn't already name one.
        if self.path.trim_matches('/').is_empty()
            && let Some(entity) = &odata.entity
        {
            self.path = entity.clone();
        }
        // OData records live under `$.value`.
        if self.records_path.is_none() {
            self.records_path = Some("$.value[*]".to_owned());
        }
        // Follow `@odata.nextLink` (v4) / `odata.nextLink` (v2).
        if matches!(self.pagination, PaginationStyle::None) {
            self.pagination = PaginationStyle::NextLinkInBody {
                next_link_path: odata.version.next_link_path().to_owned(),
            };
        }
        // Query-option sugar → standard params (don't clobber explicit ones).
        let mut set_param = |k: &str, v: String| {
            self.query_params.entry(k.to_owned()).or_insert(v);
        };
        if !odata.select.is_empty() {
            set_param("$select", odata.select.join(","));
        }
        if !odata.expand.is_empty() {
            set_param("$expand", odata.expand.join(","));
        }
        if let Some(filter) = &odata.filter {
            set_param("$filter", filter.clone());
        }
        if let Some(orderby) = &odata.orderby {
            set_param("$orderby", orderby.clone());
        }
        // Server page size via the `Prefer` header (case-insensitive check so a
        // user-set `Prefer:` is not double-inserted).
        if let Some(n) = odata.page_size
            && !self
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("prefer"))
        {
            self.headers
                .insert("prefer".to_owned(), format!("odata.maxpagesize={n}"));
        }
    }

    pub fn new(base_url: &str, path: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            path: path.to_string(),
            ..Default::default()
        }
    }

    // ── Core request ──────────────────────────────────────────────────────────

    pub fn method(mut self, m: Method) -> Self {
        self.method = m;
        self
    }

    pub fn auth(mut self, a: Auth) -> Self {
        self.auth = AuthSpec::Inline(a);
        self
    }

    /// Add a static request header. Validation is deferred to
    /// [`RestStream::new`](crate::RestStream::new) (via [`validate`](Self::validate)),
    /// so an invalid name/value surfaces as a typed
    /// [`FaucetError::Config`](faucet_core::FaucetError::Config) rather than
    /// panicking here.
    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.insert(k.to_string(), v.to_string());
        self
    }

    pub fn query(mut self, k: &str, v: &str) -> Self {
        self.query_params.insert(k.into(), v.into());
        self
    }

    pub fn body(mut self, b: Value) -> Self {
        self.body = Some(b);
        self
    }

    /// Attach a mutual-TLS client identity (requires the `mtls` feature at build
    /// time; otherwise [`RestStream::new`](crate::RestStream::new) errors).
    pub fn tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    // ── Pagination ────────────────────────────────────────────────────────────

    pub fn pagination(mut self, p: PaginationStyle) -> Self {
        self.pagination = p;
        self
    }

    pub fn records_path(mut self, p: &str) -> Self {
        self.records_path = Some(p.into());
        self
    }

    pub fn max_pages(mut self, n: usize) -> Self {
        self.max_pages = Some(n);
        self
    }

    pub fn request_delay(mut self, d: Duration) -> Self {
        self.request_delay = Some(d);
        self
    }

    // ── Reliability ───────────────────────────────────────────────────────────

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn retry_backoff(mut self, d: Duration) -> Self {
        self.retry_backoff = d;
        self
    }

    /// HTTP status codes that should be silently ignored (treated as empty pages).
    pub fn tolerate_http_error(mut self, status: u16) -> Self {
        self.tolerated_http_errors.push(status);
        self
    }

    // ── Replication ───────────────────────────────────────────────────────────

    pub fn replication_method(mut self, m: ReplicationMethod) -> Self {
        self.replication_method = m;
        self
    }

    /// Field name (not JSONPath) used as the incremental replication bookmark.
    pub fn replication_key(mut self, key: &str) -> Self {
        self.replication_key = Some(key.into());
        self
    }

    /// Bookmark start value: records at or before this value are filtered out
    /// when using `ReplicationMethod::Incremental`.
    pub fn start_replication_value(mut self, v: Value) -> Self {
        self.start_replication_value = Some(v);
        self
    }

    /// Opt the stream into resumable runs by giving it a stable state key.
    /// When this is set and the [`Pipeline`](faucet_core::Pipeline) is
    /// configured with a state store, the previously persisted bookmark is
    /// applied to the stream before fetching.
    pub fn state_key(mut self, key: &str) -> Self {
        self.state_key = Some(key.into());
        self
    }

    /// Bind the stored bookmark into the outgoing request (#513).
    pub fn replication_bind(mut self, bind: ReplicationBind) -> Self {
        self.replication_bind = Some(bind);
        self
    }

    /// Slice the run into rolling `[start, end)` datetime windows (#527).
    pub fn window(mut self, window: faucet_core::WindowSpec) -> Self {
        self.window = Some(window);
        self
    }

    /// Speak OData: derive paging, the `$.value` envelope, the query-option
    /// sugar, and `$metadata` discovery from the block (#512).
    pub fn odata(mut self, odata: ODataConfig) -> Self {
        self.odata = Some(odata);
        self
    }

    /// Set the response-decode pipeline (#515).
    pub fn decode(mut self, steps: Vec<crate::decode::DecodeStep>) -> Self {
        self.decode = steps;
        self
    }

    // ── Singer / Meltano metadata ─────────────────────────────────────────────

    /// Human-readable stream name.
    pub fn name(mut self, n: &str) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Field names that uniquely identify a record (Singer `key_properties`).
    pub fn primary_keys(mut self, keys: Vec<String>) -> Self {
        self.primary_keys = keys;
        self
    }

    /// JSON Schema for the stream's records.
    pub fn schema(mut self, s: Value) -> Self {
        self.schema = Some(s);
        self
    }

    /// Maximum records to sample for schema inference (`0` = unlimited).
    pub fn schema_sample_size(mut self, n: usize) -> Self {
        self.schema_sample_size = n;
        self
    }

    // ── Partitions ────────────────────────────────────────────────────────────

    /// Add a partition context. The stream will execute once for each partition,
    /// substituting `{key}` placeholders in `path` with values from the context.
    pub fn add_partition(mut self, ctx: HashMap<String, Value>) -> Self {
        self.partitions.push(ctx);
        self
    }

    /// Add a repeated / array-valued query parameter (#536): `key` is emitted
    /// once per value (`?key=v0&key=v1`). Chainable.
    pub fn add_query_param_multi(mut self, key: &str, values: Vec<String>) -> Self {
        self.query_params_multi.insert(key.to_string(), values);
        self
    }

    /// Set the maximum number of partitions to fetch concurrently.
    /// `None` (default) means sequential processing.
    pub fn partition_concurrency(mut self, concurrency: Option<usize>) -> Self {
        self.partition_concurrency = concurrency;
        self
    }

    // ── Extraction / cursor extras ──────────────────────────────────────────────

    /// Copy enclosing `[*]` ancestor fields onto each nested record (#549).
    pub fn record_ancestors(mut self, map: HashMap<String, String>) -> Self {
        self.record_ancestors = Some(map);
        self
    }

    /// Emit several op-stamped record arrays from one response in one page (#548).
    pub fn records_multi(mut self, specs: Vec<RecordsMultiSpec>) -> Self {
        self.records_multi = specs;
        self
    }

    /// Field name each [`records_multi`](Self::records_multi) record is stamped
    /// with its op value (default `_op`).
    pub fn op_field(mut self, field: &str) -> Self {
        self.op_field = Some(field.into());
        self
    }

    /// Persist the terminal pagination cursor as the run's bookmark and seed it
    /// on resume (#547).
    pub fn persist_cursor(mut self, enabled: bool) -> Self {
        self.persist_cursor = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{BindFormat, BindTarget, ReplicationBind};

    fn bind() -> ReplicationBind {
        ReplicationBind {
            into: BindTarget::Query,
            name: "since".to_owned(),
            template: "${bookmark}".to_owned(),
            format: BindFormat::Raw,
            advance_from: None,
        }
    }

    #[test]
    fn replication_bind_requires_incremental_and_key() {
        // Bind without incremental method → error.
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.replication_bind = Some(bind());
        assert!(c.validate().is_err());

        // Incremental but no replication_key → error.
        c.replication_method = ReplicationMethod::Incremental;
        assert!(c.validate().is_err());

        // Incremental + key → ok.
        c.replication_key = Some("updated_at".to_owned());
        assert!(c.validate().is_ok());

        // An invalid bind (empty name) is rejected too.
        let mut bad = c.clone();
        bad.replication_bind = Some(ReplicationBind {
            name: String::new(),
            ..bind()
        });
        assert!(bad.validate().is_err());
    }

    #[test]
    fn odata_rejects_non_json_response_format() {
        let mut c = RestStreamConfig::new("https://x", "");
        c.odata = Some(ODataConfig {
            entity: Some("Orders".to_owned()),
            ..Default::default()
        });
        c.response_format = ResponseFormat::Csv;
        assert!(c.validate().is_err());
    }

    #[test]
    fn apply_odata_defaults_renders_all_options_and_v2_link() {
        let mut c = RestStreamConfig::new("https://host/odata", "");
        c.odata = Some(ODataConfig {
            version: ODataVersion::V2,
            entity: Some("Orders".to_owned()),
            select: vec!["A".to_owned(), "B".to_owned()],
            expand: vec!["Lines".to_owned()],
            filter: Some("A gt 1".to_owned()),
            orderby: Some("A desc".to_owned()),
            page_size: Some(250),
            ..Default::default()
        });
        c.apply_odata_defaults();

        assert_eq!(c.path, "Orders");
        assert_eq!(c.records_path.as_deref(), Some("$.value[*]"));
        assert_eq!(c.query_params.get("$select").unwrap(), "A,B");
        assert_eq!(c.query_params.get("$expand").unwrap(), "Lines");
        assert_eq!(c.query_params.get("$filter").unwrap(), "A gt 1");
        assert_eq!(c.query_params.get("$orderby").unwrap(), "A desc");
        assert_eq!(
            c.headers.get("prefer").map(String::as_str),
            Some("odata.maxpagesize=250")
        );
        // v2 uses the un-prefixed next-link key.
        assert!(matches!(
            c.pagination,
            crate::pagination::PaginationStyle::NextLinkInBody { ref next_link_path }
                if next_link_path == "$['odata.nextLink']"
        ));
        // Idempotent: a second application doesn't clobber explicit values.
        c.apply_odata_defaults();
        assert_eq!(c.query_params.get("$select").unwrap(), "A,B");
    }

    #[test]
    fn headers_serde_round_trip_as_string_map() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.headers
            .insert("Prefer".to_owned(), "transient".to_owned());
        c.headers
            .insert("Accept".to_owned(), "application/json".to_owned());
        // Serializes as a plain JSON string map (schema-visible field).
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["headers"]["Prefer"], "transient");
        assert_eq!(v["headers"]["Accept"], "application/json");
        // And round-trips back into the string map.
        let back: RestStreamConfig = serde_json::from_value(v).unwrap();
        assert_eq!(
            back.headers.get("Prefer").map(String::as_str),
            Some("transient")
        );
        assert!(back.validate().is_ok());
    }

    #[test]
    fn validate_rejects_invalid_header_name() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.headers
            .insert("Invalid Header".to_owned(), "v".to_owned());
        let err = c.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "expected Config error, got {err:?}"
        );
        assert!(err.to_string().contains("invalid header name"), "{err}");
    }

    #[test]
    fn validate_rejects_invalid_header_value() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        // A newline is not a legal header value byte.
        c.headers
            .insert("X-Bad".to_owned(), "line\nbreak".to_owned());
        let err = c.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "{err:?}"
        );
    }

    #[test]
    fn odata_version_next_link_paths() {
        assert_eq!(ODataVersion::V4.next_link_path(), "$['@odata.nextLink']");
        assert_eq!(ODataVersion::V2.next_link_path(), "$['odata.nextLink']");
    }

    // ── #548 records_multi validation ───────────────────────────────────────────

    fn multi() -> Vec<RecordsMultiSpec> {
        vec![
            RecordsMultiSpec {
                path: "$.added[*]".into(),
                op: "upsert".into(),
            },
            RecordsMultiSpec {
                path: "$.removed[*]".into(),
                op: "delete".into(),
            },
        ]
    }

    #[test]
    fn records_multi_ok_and_rejects_conflicts() {
        // Bare records_multi validates.
        let c = RestStreamConfig::new("https://x", "/y").records_multi(multi());
        assert!(c.validate().is_ok());

        // records_multi + records_path → error.
        let mut both = c.clone();
        both.records_path = Some("$.data[*]".into());
        assert!(both.validate().is_err());

        // records_multi + record_ancestors → error.
        let mut anc = c.clone();
        anc.record_ancestors = Some(HashMap::from([("x".into(), "y".into())]));
        assert!(anc.validate().is_err());

        // records_multi with a non-JSON response format → error.
        let mut csv = c.clone();
        csv.response_format = ResponseFormat::Csv;
        assert!(csv.validate().is_err());

        // Empty path / op → error.
        let mut empty = c.clone();
        empty.records_multi[0].path = "  ".into();
        assert!(empty.validate().is_err());
        let mut empty_op = c.clone();
        empty_op.records_multi[0].op = String::new();
        assert!(empty_op.validate().is_err());
    }

    #[test]
    fn op_field_requires_records_multi() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.op_field = Some("_op".into());
        assert!(c.validate().is_err());

        c.records_multi = multi();
        assert!(c.validate().is_ok());

        c.op_field = Some(" ".into());
        assert!(c.validate().is_err());
    }

    // ── #549 record_ancestors validation ────────────────────────────────────────

    #[test]
    fn record_ancestors_requires_nested_array_path() {
        let anc = HashMap::from([("event_id".to_owned(), "id".to_owned())]);

        // No records_path → error.
        let mut c = RestStreamConfig::new("https://x", "/y").record_ancestors(anc.clone());
        assert!(c.validate().is_err());

        // records_path without `[*]` → error.
        c.records_path = Some("$.data".into());
        assert!(c.validate().is_err());

        // Nested array path → ok.
        c.records_path = Some("$.data[*].data.object".into());
        assert!(c.validate().is_ok());

        // Empty dest/rel → error.
        let mut bad = c.clone();
        bad.record_ancestors = Some(HashMap::from([(String::new(), "id".to_owned())]));
        assert!(bad.validate().is_err());
    }

    // ── #547 persist_cursor validation ──────────────────────────────────────────

    #[test]
    fn persist_cursor_requires_cursor_pagination() {
        // Default pagination (None) → error.
        let mut c = RestStreamConfig::new("https://x", "/y").persist_cursor(true);
        assert!(c.validate().is_err());

        // Cursor → ok.
        c.pagination = PaginationStyle::Cursor {
            next_token_path: "$.next".into(),
            param_name: "cursor".into(),
        };
        assert!(c.validate().is_ok());

        // CursorInBody → ok.
        c.pagination = PaginationStyle::CursorInBody {
            next_token_path: "$.paging.next".into(),
            body_cursor_field: "after".into(),
        };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn odata_fan_out_and_partition_blocks_deserialize_grouped() {
        // The config surface is grouped by concept: `emit` (what each dataset
        // produces) and `partition` (how a large dataset is range-split) are
        // blocks, not flat sibling keys.
        let raw = serde_json::json!({
            "version": "v4",
            "fan_out": true,
            "objects": "Alpha,BetaV2",
            "emit": { "table_id": "raw_${name_snake}", "sink_ref": "warehouse" },
            "partition": { "objects": ["Alpha"], "workers": 24, "count": 96 }
        });
        let c: ODataConfig = serde_json::from_value(raw).unwrap();
        assert!(c.fan_out);
        assert_eq!(c.objects, vec!["Alpha".to_string(), "BetaV2".to_string()]);
        let emit = c.emit.unwrap();
        assert_eq!(emit.table_id.as_deref(), Some("raw_${name_snake}"));
        assert_eq!(emit.sink_ref.as_deref(), Some("warehouse"));
        let p = c.partition.unwrap();
        assert_eq!(p.objects, vec!["Alpha".to_string()]);
        assert_eq!(p.resolved_workers(), 24);
        assert_eq!(p.resolved_count(), 96);
    }

    #[test]
    fn partition_spec_defaults_and_clamps() {
        let p = PartitionSpec::default();
        assert_eq!(p.resolved_workers(), DEFAULT_PARTITION_WORKERS);
        assert_eq!(
            p.resolved_count(),
            DEFAULT_PARTITION_WORKERS * RANGES_PER_WORKER
        );
        // A typo cannot spawn thousands of tasks: workers clamp to the cap and
        // the range count clamps too.
        let p = PartitionSpec {
            workers: Some(6400),
            count: Some(100_000),
            ..Default::default()
        };
        assert_eq!(p.resolved_workers(), MAX_PARTITION_WORKERS);
        assert_eq!(p.resolved_count(), MAX_PARTITION_COUNT);
        // `count: 0` falls back to the derived default rather than zero ranges.
        let p = PartitionSpec {
            workers: Some(2),
            count: Some(0),
            ..Default::default()
        };
        assert_eq!(p.resolved_count(), 2 * RANGES_PER_WORKER);
    }

    #[test]
    fn odata_rejects_ungrouped_flat_partition_keys() {
        // The old flat spelling must not silently deserialize.
        let raw = serde_json::json!({ "partition_key": "SourceKey" });
        assert!(serde_json::from_value::<ODataConfig>(raw).is_err());
    }

    #[test]
    fn objects_all_sentinel_means_every_object() {
        // `all` (any case) is the documented "discover everything" sentinel and
        // deserializes to an empty list, distinct from a named single object.
        #[derive(serde::Deserialize)]
        struct W(#[serde(deserialize_with = "de_objects")] Vec<String>);
        assert!(
            serde_json::from_value::<W>(serde_json::json!("all"))
                .unwrap()
                .0
                .is_empty()
        );
        assert!(
            serde_json::from_value::<W>(serde_json::json!(" ALL "))
                .unwrap()
                .0
                .is_empty()
        );
        // Comma strings split + trim + drop blanks; lists pass through.
        assert_eq!(
            serde_json::from_value::<W>(serde_json::json!(" A , ,B "))
                .unwrap()
                .0,
            vec!["A".to_string(), "B".to_string()]
        );
        assert_eq!(
            serde_json::from_value::<W>(serde_json::json!(["X", "Y"]))
                .unwrap()
                .0,
            vec!["X".to_string(), "Y".to_string()]
        );
    }

    #[test]
    fn validate_delegates_to_the_discovery_recipe() {
        // A malformed recipe must fail at config load, not on the first request.
        let mut cfg = RestStreamConfig::new("https://api.example.com", "/");
        cfg.discovery = Some(
            serde_json::from_value(serde_json::json!({
                "list": { "get": "", "items": "$.x[*]", "name": "$.n" },
                "emit": { "config": { "path": "/x" } }
            }))
            .unwrap(),
        );
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("`list.get` must not be empty"), "{err}");
    }

    /// Only `base_url` is genuinely required (#609).
    ///
    /// Before the per-field defaults, this config demanded **22** fields, so a
    /// hand-written `rest` entry could not deserialize at all — which is why
    /// `faucet validate` never tried. Deserializing the minimum is therefore
    /// the regression test, and asserting each defaulted value pins what a
    /// user gets when they omit it.
    #[test]
    fn a_minimal_config_deserializes_and_every_omitted_field_takes_its_default() {
        let cfg: RestStreamConfig =
            serde_json::from_value(serde_json::json!({ "base_url": "https://api.example.com" }))
                .expect("base_url alone must be enough");

        assert_eq!(cfg.base_url, "https://api.example.com");
        assert_eq!(cfg.method, reqwest::Method::GET);
        assert_eq!(cfg.max_pages, Some(100));
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.schema_sample_size, 100);
        assert!(matches!(cfg.pagination, PaginationStyle::None));
        assert!(matches!(
            cfg.replication_method,
            ReplicationMethod::FullTable
        ));
        assert!(cfg.timeout.is_some(), "a request must not hang forever");
        assert!(cfg.retry_backoff > std::time::Duration::ZERO);
        assert!(
            cfg.query_params.is_empty(),
            "query_params was accidentally required (#609); it must default to empty"
        );
        assert_eq!(cfg.csv_delimiter, b',');
        assert!(cfg.csv_has_headers);
        // The defaulted auth must be the inert one — a default that
        // accidentally carried credentials-shaped state would be a security
        // problem, not a convenience.
        assert!(
            serde_json::to_value(&cfg.auth)
                .expect("auth serializes")
                .to_string()
                .contains("none"),
            "the default auth must be `none`"
        );

        // And it must survive the validate() that `faucet validate` now runs.
        cfg.validate().expect("a minimal config is valid");
    }

    #[test]
    fn an_empty_base_url_is_refused() {
        let cfg: RestStreamConfig =
            serde_json::from_value(serde_json::json!({ "base_url": "" })).expect("deserializes");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.to_lowercase().contains("base_url"), "{err}");
    }
}
