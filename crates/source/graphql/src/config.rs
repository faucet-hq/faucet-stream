//! GraphQL source configuration.

use faucet_core::replication::BIND_PLACEHOLDER;
use faucet_core::{
    AuthSpec, BindFormat, BindValueType, DEFAULT_BATCH_SIZE, FaucetError, OnMissingKey,
    ReplicationKey, ReplicationMethod, RetryMatcher, TlsClientConfig, validate_batch_size,
};
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Authentication for GraphQL endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum GraphqlAuth {
    /// No authentication.
    None,
    /// Bearer token in the Authorization header.
    Bearer { token: String },
    /// Custom headers (e.g. API keys, cookies).
    Custom { headers: HashMap<String, String> },
}

/// Cursor-based pagination configuration for GraphQL.
///
/// Most GraphQL APIs use the Relay cursor specification with
/// `pageInfo { hasNextPage, endCursor }`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GraphqlPagination {
    /// JSONPath to the `hasNextPage` boolean in the response.
    pub has_next_page_path: String,
    /// JSONPath to the `endCursor` string in the response.
    pub cursor_path: String,
    /// Name of the cursor variable in the GraphQL query (default: `"after"`).
    pub cursor_variable: String,
    /// Name of the page size variable (default: `"first"`).
    ///
    /// The per-page record count itself comes from
    /// [`GraphqlStreamConfig::batch_size`] — the variable named here is the
    /// GraphQL variable that the `batch_size` value is injected into on each
    /// request. The plain `batch_size = 0` sentinel omits the variable so the
    /// upstream uses its own default page size.
    pub page_size_variable: String,
}

impl Default for GraphqlPagination {
    fn default() -> Self {
        Self {
            has_next_page_path: "$.data.*.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.*.pageInfo.endCursor".into(),
            cursor_variable: "after".into(),
            page_size_variable: "first".into(),
        }
    }
}

/// Discriminator for [`GraphqlOffsetPagination`]; serializes as `"Offset"`.
///
/// A dedicated single-variant enum (rather than a bare `String`) so the
/// `type: Offset` marker is validated at config-load time and gives the
/// untagged [`GraphqlPaginationSpec`] a reliable way to route an offset block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum OffsetPaginationKind {
    /// The offset pagination style.
    Offset,
}

/// Offset-into-query-variable pagination (query languages that embed `LIMIT … OFFSET …`).
///
/// Increments an integer offset injected into a GraphQL variable and
/// terminates on a **short page** (fewer than `page_size` records) — unlike
/// cursor pagination, which follows a `pageInfo` boolean. Suited to APIs whose
/// query language embeds `LIMIT … OFFSET …`: bake the limit
/// into the query string and parameterize only the offset with `${…}`.
///
/// The offset starts at `0`, is sent as a JSON number in the `variables` map on
/// every request, and advances by `page_size` after each page.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphqlOffsetPagination {
    /// Discriminator — must be `Offset`.
    pub r#type: OffsetPaginationKind,
    /// Name of the GraphQL variable that receives the current offset. It is
    /// injected as a JSON number (starting at `0`, incremented by `page_size`
    /// after each page). Reference it from the query string or as a variable
    /// (e.g. `${q_offset}` inside the query string).
    pub offset_variable: String,
    /// Records requested per page. Used both to advance the offset
    /// (`offset += page_size`) and, with `stop_when_short`, to detect the final
    /// page. Must be greater than `0`. This value is **not** injected into the
    /// request — bake the limit into your query (`LIMIT 250 OFFSET ${q_offset}`).
    pub page_size: usize,
    /// Terminate when a page yields fewer than `page_size` records (default
    /// `true`). When `false`, pagination continues until a fully empty page (or
    /// `max_pages`) is reached.
    #[serde(default = "default_true")]
    pub stop_when_short: bool,
    /// Substitute `${offset_variable}` occurrences in the **query string** with
    /// the current offset before each request, instead of sending it as a
    /// GraphQL variable. Required for query languages that embed the offset in a
    /// string-literal argument — e.g.
    /// `reportQuery(query: "… LIMIT 250 OFFSET ${q_offset}")`, where a GraphQL
    /// variable cannot interpolate into a string literal (#569). Default
    /// `false` (variable injection, the #550 behavior).
    #[serde(default)]
    pub substitute_in_query: bool,
}

fn default_true() -> bool {
    true
}

/// Pagination style for the GraphQL source.
///
/// Deserialized **untagged** so the legacy cursor block (which carries no
/// `type:` discriminator) keeps working unchanged, while the offset block is
/// selected by its required `type: Offset` field. The two field sets are
/// disjoint (cursor requires `cursor_path` / `has_next_page_path`; offset
/// requires `type` / `offset_variable` / `page_size`), so serde routes each
/// config to exactly one variant. The cursor variant is tried first.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum GraphqlPaginationSpec {
    /// Relay-style cursor pagination (the original, `type`-less shape).
    Cursor(GraphqlPagination),
    /// Offset-into-variable pagination — selected by `type: Offset`.
    Offset(GraphqlOffsetPagination),
}

/// Binds the stored bookmark into a GraphQL **variable** (#751), so the server
/// returns only newer records. The client-side `replication_key` filter stays
/// active as a safety net.
///
/// ```yaml
/// replication_bind:
///   variable: since                    # or a JSON Pointer: /filter/updatedAt/gte
///   template: "updated_at:>${bookmark}"
///   format: iso8601
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphqlReplicationBind {
    /// The variable to set: a top-level variable name (`since`), or an RFC 6901
    /// JSON Pointer into `variables` (`/filter/updatedAt/gte`) that resolves to
    /// an existing value or to a new key of an existing object.
    pub variable: String,
    /// Template rendered with `${bookmark}` replaced by the formatted bookmark.
    /// Defaults to the bare `${bookmark}`. When the bookmark sits inside a
    /// query-language string filter (`query: "updated_at:>…"`), escaping is
    /// the template author's job.
    #[serde(default = "default_bind_template")]
    pub template: String,
    /// How to format the bookmark before substitution.
    #[serde(default)]
    pub format: BindFormat,
    /// JSON type of the variable: `string` (default) or `number`.
    #[serde(default)]
    pub value_type: BindValueType,
}

fn default_bind_template() -> String {
    BIND_PLACEHOLDER.to_string()
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn default_replication_method() -> ReplicationMethod {
    ReplicationMethod::FullTable
}

/// Configuration for the GraphQL source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GraphqlStreamConfig {
    /// GraphQL endpoint URL.
    pub endpoint: String,
    /// The GraphQL query string.
    pub query: String,
    /// Variables to pass with the query. Defaults to an empty object.
    #[serde(default = "empty_object")]
    pub variables: Value,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<GraphqlAuth>,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// JSONPath expression to extract records from the response.
    pub records_path: Option<String>,
    /// Pagination configuration. `None` for single-page queries. Accepts either
    /// the Relay cursor block (no `type:`) or an offset block (`type: Offset`) —
    /// see [`GraphqlPaginationSpec`].
    pub pagination: Option<GraphqlPaginationSpec>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<usize>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage), and the
    /// value injected as the GraphQL `first:` cursor argument (or whatever
    /// variable name [`GraphqlPagination::page_size_variable`] specifies).
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the page-size variable
    /// is omitted from the request so the upstream uses its own default page
    /// size, and the entire result set is emitted as a single page. If the
    /// upstream schema requires a non-null `first:` argument this will
    /// surface as `FaucetError::Config` at stream-time.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional client-certificate (mutual TLS) config. When set, the source
    /// presents a client certificate on every request (data + inline auth token
    /// request). Requires the crate's `mtls` feature.
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,

    /// `full_table` (default) re-reads everything each run; `incremental`
    /// filters on [`replication_key`](Self::replication_key) and emits a
    /// bookmark on the final page so the next run resumes (#751).
    #[serde(default = "default_replication_method")]
    pub replication_method: ReplicationMethod,
    /// Field used for incremental replication bookmarking: a top-level field
    /// (`updatedAt`), a dot path into nested objects (`node.updatedAt`), or a
    /// JSON Pointer (`/node/updatedAt`). Must resolve to one value per record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_key: Option<String>,
    /// What to do with a record whose `replication_key` is missing or `null`
    /// once a start bookmark exists: `keep` (default), `drop`, or `fail`.
    #[serde(default)]
    pub on_missing_key: OnMissingKey,
    /// Initial bookmark used when no stored bookmark exists: records whose
    /// `replication_key` is not greater than it are filtered out, and a
    /// `replication_bind` renders it into the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_replication_value: Option<Value>,
    /// Identifier the pipeline persists this stream's bookmark under. Derived
    /// from the endpoint when unset and replicating incrementally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Bind the bookmark into a GraphQL variable (see [`GraphqlReplicationBind`]).
    /// Requires `replication_method: incremental` + `replication_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_bind: Option<GraphqlReplicationBind>,
    /// Responses to treat as throttling and retry — including a `200` whose
    /// body reports it (`match_success: true` with
    /// `body_path: "$.errors[*].extensions.code"`, `values: [THROTTLED]`). A
    /// rule's `backoff_from` reads the wait from the response (`cost_bucket`
    /// for a leaky-bucket cost report, or a reset header / body value).
    /// Checked before the response's `errors[]` are classified; a match
    /// retries the whole request, so partial `data` is never emitted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retry_on_response: Vec<RetryMatcher>,
    /// Per-request timeout in seconds. Covers one HTTP request (a stalled peer
    /// fails the request, so retries and resilience can act), not the whole
    /// run. `null` disables it. Defaults to `30`.
    #[serde(
        with = "faucet_core::config::duration_secs_option",
        default = "default_request_timeout"
    )]
    #[schemars(with = "Option<u64>")]
    pub timeout: Option<std::time::Duration>,
    /// Timeout for establishing a connection, in seconds. `null` disables it.
    /// Defaults to `10`.
    #[serde(
        with = "faucet_core::config::duration_secs_option",
        default = "default_connect_timeout"
    )]
    #[schemars(with = "Option<u64>")]
    pub connect_timeout: Option<std::time::Duration>,
}

fn default_request_timeout() -> Option<std::time::Duration> {
    Some(std::time::Duration::from_secs(30))
}

fn default_connect_timeout() -> Option<std::time::Duration> {
    Some(std::time::Duration::from_secs(10))
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl GraphqlStreamConfig {
    /// Create a new config with an endpoint and query.
    pub fn new(endpoint: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            query: query.into(),
            variables: Value::Object(Default::default()),
            auth: AuthSpec::Inline(GraphqlAuth::None),
            headers: HeaderMap::new(),
            records_path: None,
            pagination: None,
            max_pages: None,
            batch_size: DEFAULT_BATCH_SIZE,
            tls: None,
            replication_method: ReplicationMethod::FullTable,
            replication_key: None,
            on_missing_key: OnMissingKey::default(),
            start_replication_value: None,
            state_key: None,
            replication_bind: None,
            retry_on_response: Vec::new(),
            timeout: default_request_timeout(),
            connect_timeout: default_connect_timeout(),
        }
    }

    /// Attach a mutual-TLS client identity (requires the `mtls` feature at build
    /// time; otherwise [`GraphqlStream::try_new`](crate::GraphqlStream::try_new)
    /// errors).
    pub fn tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the per-request timeout (`None` disables it).
    pub fn timeout(mut self, timeout: Option<std::time::Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the connect timeout (`None` disables it).
    pub fn connect_timeout(mut self, timeout: Option<std::time::Duration>) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the GraphQL variables.
    pub fn variables(mut self, vars: Value) -> Self {
        self.variables = vars;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: GraphqlAuth) -> Self {
        self.auth = AuthSpec::Inline(auth);
        self
    }

    /// Set additional headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set the JSONPath expression for record extraction.
    pub fn records_path(mut self, path: impl Into<String>) -> Self {
        self.records_path = Some(path.into());
        self
    }

    /// Enable cursor-based (Relay) pagination with the given configuration.
    pub fn pagination(mut self, pagination: GraphqlPagination) -> Self {
        self.pagination = Some(GraphqlPaginationSpec::Cursor(pagination));
        self
    }

    /// Enable offset-into-variable pagination (query languages that embed `LIMIT … OFFSET …`).
    pub fn offset_pagination(mut self, pagination: GraphqlOffsetPagination) -> Self {
        self.pagination = Some(GraphqlPaginationSpec::Offset(pagination));
        self
    }

    /// Set the maximum number of pages to fetch.
    pub fn max_pages(mut self, max: usize) -> Self {
        self.max_pages = Some(max);
        self
    }

    /// Set the per-page record count for [`Source::stream_pages`](faucet_core::Source::stream_pages)
    /// and the GraphQL `first:` cursor argument.
    ///
    /// Pass `0` to opt out of batching — the page-size variable is omitted
    /// from the request so the upstream uses its own default page size, and
    /// the response is emitted as a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate the config at load time so a bad config fails fast with a typed
    /// [`FaucetError::Config`] instead of surfacing deep in a run: rejects an
    /// out-of-range `batch_size` (`> MAX_BATCH_SIZE`) and an empty `endpoint` or
    /// `query`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.endpoint.trim().is_empty() {
            return Err(FaucetError::Config(
                "GraphQL source requires a non-empty `endpoint`".into(),
            ));
        }
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "GraphQL source requires a non-empty `query`".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        if let Some(GraphqlPaginationSpec::Offset(off)) = &self.pagination
            && off.page_size == 0
        {
            return Err(FaucetError::Config(
                "GraphQL offset pagination requires `page_size` > 0 \
                 (a zero page size never advances the offset)"
                    .into(),
            ));
        }
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        for (i, m) in self.retry_on_response.iter().enumerate() {
            m.validate(i).map_err(|e| match e {
                FaucetError::Config(msg) => FaucetError::Config(format!("graphql: {msg}")),
                other => other,
            })?;
        }
        self.validate_replication()
    }

    /// Replication settings are consistent: incremental needs a parseable
    /// `replication_key`, and a bind needs incremental mode, a variable, a
    /// template carrying `${bookmark}`, and `variables` that are an object.
    fn validate_replication(&self) -> Result<(), FaucetError> {
        let incremental = self.replication_method == ReplicationMethod::Incremental;
        match (&self.replication_key, incremental) {
            (Some(k), _) => {
                ReplicationKey::parse(k)?;
            }
            (None, true) => {
                return Err(FaucetError::Config(
                    "GraphQL source: `replication_method: incremental` requires a \
                     `replication_key`"
                        .into(),
                ));
            }
            (None, false) => {}
        }
        if let Some(state_key) = &self.state_key {
            faucet_core::state::validate_state_key(state_key)?;
        }
        let Some(bind) = &self.replication_bind else {
            return Ok(());
        };
        if !incremental {
            return Err(FaucetError::Config(
                "GraphQL source: `replication_bind` requires `replication_method: incremental`"
                    .into(),
            ));
        }
        let var = bind.variable.trim();
        if var.is_empty() || var == "/" {
            return Err(FaucetError::Config(
                "GraphQL source: `replication_bind.variable` must name a variable or be a JSON \
                 Pointer into `variables`"
                    .into(),
            ));
        }
        if !bind.template.contains(BIND_PLACEHOLDER) {
            return Err(FaucetError::Config(format!(
                "GraphQL source: `replication_bind.template` must contain {BIND_PLACEHOLDER}"
            )));
        }
        if !self.variables.is_object() && !self.variables.is_null() {
            return Err(FaucetError::Config(
                "GraphQL source: `replication_bind` needs `variables` to be an object".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { users { id name } }",
        );
        assert_eq!(config.endpoint, "https://api.example.com/graphql");
        assert!(config.records_path.is_none());
        assert!(config.pagination.is_none());
        assert!(config.max_pages.is_none());
    }

    #[test]
    fn builder_methods() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { users { id } }")
                .variables(json!({"org": "acme"}))
                .records_path("$.data.users.edges[*].node")
                .max_pages(10)
                .auth(GraphqlAuth::Bearer {
                    token: "token".into(),
                });
        assert_eq!(config.variables["org"], "acme");
        assert_eq!(config.records_path.unwrap(), "$.data.users.edges[*].node");
        assert_eq!(config.max_pages, Some(10));
    }

    #[test]
    fn default_pagination() {
        let pag = GraphqlPagination::default();
        assert_eq!(pag.cursor_variable, "after");
        assert_eq!(pag.page_size_variable, "first");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "endpoint": "https://api.example.com/graphql",
            "query": "query { x }",
            "variables": {},
            "auth": {"type": "none"},
            "records_path": null,
            "pagination": null,
            "max_pages": null,
            "batch_size": 500
        }"#;
        let config: GraphqlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(config.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        assert!(matches!(
            GraphqlStreamConfig::new("  ", "query { x }").validate(),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_query() {
        assert!(matches!(
            GraphqlStreamConfig::new("https://api.example.com/graphql", "").validate(),
            Err(FaucetError::Config(_))
        ));
    }

    // ── Offset pagination ───────────────────────────────────────────────────

    #[test]
    fn offset_pagination_deserializes_with_type_tag() {
        let json = r#"{
            "type": "Offset",
            "offset_variable": "q_offset",
            "page_size": 250,
            "stop_when_short": true
        }"#;
        let spec: GraphqlPaginationSpec = serde_json::from_str(json).unwrap();
        match spec {
            GraphqlPaginationSpec::Offset(off) => {
                assert_eq!(off.r#type, OffsetPaginationKind::Offset);
                assert_eq!(off.offset_variable, "q_offset");
                assert_eq!(off.page_size, 250);
                assert!(off.stop_when_short);
            }
            other => panic!("expected Offset variant, got {other:?}"),
        }
    }

    #[test]
    fn offset_pagination_stop_when_short_defaults_true() {
        let json = r#"{ "type": "Offset", "offset_variable": "q_offset", "page_size": 100 }"#;
        let spec: GraphqlPaginationSpec = serde_json::from_str(json).unwrap();
        match spec {
            GraphqlPaginationSpec::Offset(off) => assert!(
                off.stop_when_short,
                "stop_when_short must default to true when omitted"
            ),
            other => panic!("expected Offset variant, got {other:?}"),
        }
    }

    #[test]
    fn cursor_pagination_still_deserializes_without_type_tag() {
        // Backward compatibility: the legacy cursor block has no `type:` field
        // and must route to the Cursor variant untouched.
        let json = r#"{
            "has_next_page_path": "$.data.users.pageInfo.hasNextPage",
            "cursor_path": "$.data.users.pageInfo.endCursor",
            "cursor_variable": "after",
            "page_size_variable": "first"
        }"#;
        let spec: GraphqlPaginationSpec = serde_json::from_str(json).unwrap();
        match spec {
            GraphqlPaginationSpec::Cursor(pag) => {
                assert_eq!(pag.cursor_variable, "after");
                assert_eq!(pag.page_size_variable, "first");
            }
            other => panic!("expected Cursor variant, got {other:?}"),
        }
    }

    #[test]
    fn full_config_with_offset_pagination_deserializes() {
        let json = r#"{
            "endpoint": "https://api.example.com/graphql",
            "query": "{ orders(first: 250, offset: $q_offset) { id } }",
            "variables": {},
            "auth": {"type": "none"},
            "records_path": "$.data.orders[*]",
            "pagination": { "type": "Offset", "offset_variable": "q_offset", "page_size": 250 },
            "max_pages": null,
            "batch_size": 250
        }"#;
        let config: GraphqlStreamConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(
            config.pagination,
            Some(GraphqlPaginationSpec::Offset(_))
        ));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn offset_pagination_rejects_unknown_field() {
        let json = r#"{
            "type": "Offset",
            "offset_variable": "q_offset",
            "page_size": 250,
            "bogus": true
        }"#;
        // `deny_unknown_fields` on the offset struct means an unknown field can't
        // silently be ignored; the untagged enum then matches no variant.
        assert!(serde_json::from_str::<GraphqlPaginationSpec>(json).is_err());
    }

    #[test]
    fn offset_pagination_builder_wraps_offset_variant() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .offset_pagination(GraphqlOffsetPagination {
                r#type: OffsetPaginationKind::Offset,
                offset_variable: "q_offset".into(),
                page_size: 250,
                stop_when_short: true,
                substitute_in_query: false,
            });
        assert!(matches!(
            config.pagination,
            Some(GraphqlPaginationSpec::Offset(_))
        ));
    }

    #[test]
    fn validate_rejects_zero_page_size_offset() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .offset_pagination(GraphqlOffsetPagination {
                r#type: OffsetPaginationKind::Offset,
                offset_variable: "q_offset".into(),
                page_size: 0,
                stop_when_short: true,
                substitute_in_query: false,
            });
        assert!(matches!(config.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn validate_accepts_nonzero_page_size_offset() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .offset_pagination(GraphqlOffsetPagination {
                r#type: OffsetPaginationKind::Offset,
                offset_variable: "q_offset".into(),
                page_size: 1,
                stop_when_short: false,
                substitute_in_query: false,
            });
        assert!(config.validate().is_ok());
    }

    fn incremental() -> GraphqlStreamConfig {
        let mut c = GraphqlStreamConfig::new("https://x/graphql", "{ a }")
            .variables(json!({ "filter": {} }));
        c.replication_method = ReplicationMethod::Incremental;
        c.replication_key = Some("node.updatedAt".into());
        c
    }

    fn bind(variable: &str, template: &str) -> GraphqlReplicationBind {
        serde_json::from_value(json!({ "variable": variable, "template": template })).unwrap()
    }

    #[test]
    fn replication_validation() {
        assert!(incremental().validate().is_ok());
        let err = |c: GraphqlStreamConfig| c.validate().unwrap_err().to_string();

        let mut c = incremental();
        c.replication_key = None;
        assert!(err(c).contains("requires a `replication_key`"));

        let mut c = incremental();
        c.replication_key = Some("  ".into());
        assert!(c.validate().is_err());

        let mut c = incremental();
        c.state_key = Some("bad key with spaces\n".into());
        assert!(c.validate().is_err());

        let mut c = incremental();
        c.replication_bind = Some(bind("/filter/since", "gt:${bookmark}"));
        assert!(c.validate().is_ok());

        let mut c = incremental();
        c.replication_bind = Some(bind(" ", "${bookmark}"));
        assert!(err(c).contains("replication_bind.variable"));
        let mut c = incremental();
        c.replication_bind = Some(bind("/", "${bookmark}"));
        assert!(err(c).contains("replication_bind.variable"));

        let mut c = incremental();
        c.replication_bind = Some(bind("since", "no placeholder"));
        assert!(err(c).contains("must contain"));

        let mut c = incremental();
        c.variables = json!([1]);
        c.replication_bind = Some(bind("since", "${bookmark}"));
        assert!(err(c).contains("to be an object"));

        let mut c = incremental();
        c.replication_method = ReplicationMethod::FullTable;
        c.replication_bind = Some(bind("since", "${bookmark}"));
        assert!(err(c).contains("requires `replication_method: incremental`"));
    }

    #[test]
    fn replication_fields_default_and_round_trip() {
        let c: GraphqlStreamConfig = serde_json::from_value(json!({
            "endpoint": "https://x/graphql", "query": "{ a }", "variables": {},
            "auth": { "type": "none" }, "records_path": null, "pagination": null,
            "max_pages": null
        }))
        .unwrap();
        assert_eq!(c.replication_method, ReplicationMethod::FullTable);
        assert!(c.replication_bind.is_none());
        let b: GraphqlReplicationBind =
            serde_json::from_value(json!({ "variable": "since" })).unwrap();
        assert_eq!(b.template, "${bookmark}");
        assert!(
            serde_json::from_value::<GraphqlReplicationBind>(json!({ "variable": "a", "x": 1 }))
                .is_err()
        );
    }

    #[test]
    fn variables_default_to_empty_object() {
        let c: GraphqlStreamConfig = serde_json::from_value(json!({
            "endpoint": "https://x/graphql", "query": "{ a }", "auth": {"type": "none"}
        }))
        .unwrap();
        assert_eq!(c.variables, json!({}));
        assert!(c.validate().is_ok());
        let schema = serde_json::to_value(schemars::schema_for!(GraphqlStreamConfig)).unwrap();
        let required = schema["required"].as_array().cloned().unwrap_or_default();
        assert!(!required.contains(&json!("variables")));
        let null: GraphqlStreamConfig = serde_json::from_value(json!({
            "endpoint": "https://x/graphql", "query": "{ a }", "auth": {"type": "none"},
            "variables": null
        }))
        .unwrap();
        assert!(null.variables.is_null());
    }
}
