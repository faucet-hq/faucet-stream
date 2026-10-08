//! Elasticsearch scroll-based search source.

use crate::config::{ElasticsearchAuth, ElasticsearchSourceConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Stream, StreamPage};
use reqwest::Client;
use serde_json::{Value, json};
use std::pin::Pin;

/// Scroll `size` when `batch_size = 0` (drain into one page). Mirrors
/// Elasticsearch's default `index.max_result_window`.
pub(crate) const NO_BATCHING_SEARCH_SIZE: usize = 10_000;

/// A source that reads documents from an Elasticsearch index using the scroll API.
pub struct ElasticsearchSource {
    config: ElasticsearchSourceConfig,
    client: Client,
    /// Optional shared auth provider. When set it takes precedence over inline
    /// auth. Injected by the CLI (to resolve `auth: { ref }`) or directly by
    /// library callers who want to share one token across multiple sources.
    auth_provider: Option<SharedAuthProvider>,
}

impl ElasticsearchSource {
    /// Create a new Elasticsearch source from the given configuration.
    /// Construction does no I/O; it fails only on an invalid config (an empty
    /// `base_url` / `index` or an out-of-range `batch_size`).
    pub fn new(config: ElasticsearchSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        // Bounded timeouts: a half-open connection or a wedged node must fail
        // the request, not hang the run (#789 MSG-58).
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| FaucetError::Config(format!("elasticsearch HTTP client: {e}")))?;
        Ok(Self {
            config,
            client,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and
    /// by library callers who inject one provider into many sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective [`ElasticsearchAuth`] for the current request.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise use the inline auth from config.
    /// 3. If the config is a `Reference` with no provider, return an error.
    async fn resolve_auth(&self) -> Result<ElasticsearchAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return faucet_common_elasticsearch::credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Apply an [`ElasticsearchAuth`] to a request builder.
    fn apply_auth_value(
        req: reqwest::RequestBuilder,
        auth: &ElasticsearchAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
            ElasticsearchAuth::ApiKey { key } => {
                req.header("Authorization", format!("ApiKey {key}"))
            }
        }
    }

    /// Extract `hits.hits[*]._source` from an Elasticsearch search response.
    fn extract_hits(body: &Value) -> Vec<Value> {
        body.get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
            .map(|hits| {
                hits.iter()
                    .filter_map(|hit| hit.get("_source").cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Extract the `_scroll_id` from an Elasticsearch response.
    fn extract_scroll_id(body: &Value) -> Option<String> {
        body.get("_scroll_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Resolve the index and query under the supplied parent context. Returns
    /// `(index, query)`.
    fn resolve_index_and_query(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<(String, Value), FaucetError> {
        // The index is a URL path segment: a parent value is percent-encoded
        // so `/`, `?` or `#` cannot reach another endpoint (#789 MSG-94).
        let index = if context.is_empty() {
            self.config.index.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.index, &encoded_context(context))
        };
        let query = if context.is_empty() {
            self.config.query.clone()
        } else {
            let s = serde_json::to_string(&self.config.query)
                .map_err(|e| FaucetError::Config(format!("failed to serialize query: {e}")))?;
            let s = faucet_core::util::substitute_context_json(&s, context);
            serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted query: {e}"))
            })?
        };
        Ok((index, query))
    }
}

#[async_trait]
impl faucet_core::Source for ElasticsearchSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all = Vec::new();
        let pages = self.stream_pages(context, self.config.batch_size);
        futures::pin_mut!(pages);
        while let Some(page) = futures::StreamExt::next(&mut pages).await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    /// Stream documents from Elasticsearch as scroll pages, one
    /// [`StreamPage`] per scroll response. Bounds client-side memory at
    /// O(batch_size) regardless of the index's total document count.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of
    /// [`ElasticsearchSourceConfig::batch_size`] — the config is the
    /// user-facing knob the README documents, and routing the
    /// pipeline-supplied hint through it would silently override an explicit
    /// config value.
    ///
    /// When `batch_size = 0` the source issues a single non-scroll
    /// `_search?size=10_000` and emits exactly one page. The scroll API is
    /// not used and no scroll context needs to be cleared.
    ///
    /// The Elasticsearch search source has no incremental-replication mode
    /// today, so every emitted page carries `bookmark: None`.
    ///
    /// **Scroll-context cleanup is mandatory.** On every exit path — clean
    /// drain, `max_pages` truncation, mid-stream HTTP error, or consumer
    /// dropping the stream — the open `_scroll_id` is sent to
    /// `DELETE _search/scroll` so the cluster does not leak server-side
    /// state. Cleanup runs inside a guard whose `Drop` impl spawns the
    /// delete request, so even cancellation at any `.await` point still
    /// releases the context.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        // `batch_size = 0` drains the whole scroll into one page. A single
        // capped `_search` silently truncated any result above 10 000 hits
        // (#789 MSG-90).
        let (size, drain) = if batch_size == 0 {
            (NO_BATCHING_SEARCH_SIZE, true)
        } else {
            (batch_size, false)
        };

        Box::pin(async_stream::try_stream! {
            let (index, query) = self.resolve_index_and_query(context)?;
            // Auth is resolved per request, so a token that expires during a
            // long scroll is refreshed by its provider (#789 MSG-90).
            let auth = self.resolve_auth().await?;

            // Scroll path. Wire up a guard so the scroll context is always
            // cleared, even on early-return / error / drop.
            let mut guard = ScrollGuard::new(
                self.config.base_url.clone(),
                self.client.clone(),
                auth.clone(),
            );

            let url = format!(
                "{}/{}/_search?scroll={}&size={}",
                self.config.base_url, index, self.config.scroll_timeout, size
            );
            let req = self.client.post(&url).json(&json!({"query": query}));
            let req = Self::apply_auth_value(req, &auth);
            let resp = req.send().await?;
            let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
            let body: Value = resp.json().await?;

            let records = Self::extract_hits(&body);
            guard.update(Self::extract_scroll_id(&body));
            let mut pages_fetched: usize = 1;
            let mut total = records.len();
            let mut buffer: Vec<Value> = Vec::new();

            let first_is_final = records.is_empty()
                || guard.scroll_id().is_none()
                || matches!(self.config.max_pages, Some(max) if pages_fetched >= max);
            if drain {
                buffer.extend(records);
            } else {
                // The initial search always counts as page 1, even when it
                // returns zero hits — emit it and move on.
                yield StreamPage { records, bookmark: None };
            }

            if !first_is_final {
                while let Some(sid) = guard.scroll_id().map(|s| s.to_string()) {
                    let auth = self.resolve_auth().await?;
                    guard.set_auth(auth.clone());
                    let scroll_url = format!("{}/_search/scroll", self.config.base_url);
                    let req = self.client.post(&scroll_url).json(&json!({
                        "scroll": self.config.scroll_timeout,
                        "scroll_id": sid,
                    }));
                    let req = Self::apply_auth_value(req, &auth);
                    let resp = req.send().await?;
                    let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                    let body: Value = resp.json().await?;

                    let records = Self::extract_hits(&body);
                    guard.update(Self::extract_scroll_id(&body));
                    pages_fetched += 1;
                    total += records.len();

                    // An empty hits array is ES's end-of-scroll sentinel.
                    if records.is_empty() {
                        break;
                    }
                    let hit_cap = matches!(self.config.max_pages, Some(max) if pages_fetched >= max);
                    if drain {
                        buffer.extend(records);
                    } else {
                        yield StreamPage { records, bookmark: None };
                    }
                    if hit_cap {
                        tracing::debug!(
                            max_pages = self.config.max_pages.unwrap_or(0),
                            "max_pages reached, stopping scroll"
                        );
                        break;
                    }
                }
            }

            if drain {
                yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
            }

            tracing::info!(
                docs = total,
                pages = pages_fetched,
                batch_size,
                "Elasticsearch source stream complete",
            );

            // Successful drain — let the guard clean up the scroll id (if any).
            guard.disarm_if_done();
        })
    }

    fn connector_name(&self) -> &'static str {
        "elasticsearch"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.index
        )
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate the cluster's non-system indices via
    /// `GET _cat/indices?format=json` (names + doc counts) and
    /// `GET /<index>/_mapping` (field types). Catalog metadata only — no
    /// document scan.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        // Resolve auth once; reuse across the _cat and _mapping requests.
        let auth = self.resolve_auth().await?;

        let url = format!(
            "{}/_cat/indices?format=json&h=index,docs.count",
            self.config.base_url
        );
        let req = Self::apply_auth_value(self.client.get(&url), &auth);
        let resp = req.send().await.map_err(|e| {
            FaucetError::Source(format!("elasticsearch: catalog discovery failed: {e}"))
        })?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let cat: Value = resp.json().await.map_err(|e| {
            FaucetError::Source(format!("elasticsearch: catalog discovery failed: {e}"))
        })?;

        let mut entries: Vec<(String, Option<u64>, &'static str)> = parse_cat_indices(&cat)
            .into_iter()
            .map(|(i, n)| (i, n, "index"))
            .collect();
        // Data streams live in hidden `.ds-*` backing indices, which `_cat`
        // filtering drops; list them by name (#789 MSG-94).
        let url = format!("{}/_data_stream", self.config.base_url);
        let req = Self::apply_auth_value(self.client.get(&url), &auth);
        if let Ok(resp) = req.send().await
            && resp.status().is_success()
            && let Ok(body) = resp.json::<Value>().await
        {
            entries.extend(
                parse_data_streams(&body)
                    .into_iter()
                    .map(|n| (n, None, "data_stream")),
            );
        }

        let mut datasets = Vec::with_capacity(entries.len());
        for (index, doc_count, kind) in entries {
            // One index whose mapping cannot be read (closed, permission) is
            // skipped with a warning instead of failing discovery for the
            // whole cluster (#789 MSG-83).
            match self.read_mapping(&index, &auth).await {
                Ok(body) => {
                    let mut d = descriptor_for_index(&index, doc_count, &body);
                    d.kind = kind.to_string();
                    datasets.push(d);
                }
                Err(e) => {
                    tracing::warn!(index = %index, error = %e, "elasticsearch discover: skipping an index whose mapping could not be read")
                }
            }
        }
        Ok(datasets)
    }
}

impl ElasticsearchSource {
    async fn read_mapping(
        &self,
        index: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
        let url = format!(
            "{}/{}/_mapping",
            self.config.base_url,
            urlencoding::encode(index)
        );
        let req = Self::apply_auth_value(self.client.get(&url), auth);
        let resp = req.send().await.map_err(|e| {
            FaucetError::Source(format!(
                "elasticsearch: catalog discovery failed (mapping for {index:?}): {e}"
            ))
        })?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        resp.json().await.map_err(|e| {
            FaucetError::Source(format!(
                "elasticsearch: catalog discovery failed (mapping for {index:?}): {e}"
            ))
        })
    }
}

/// `context` with every string value percent-encoded for a URL path segment.
fn encoded_context(
    context: &std::collections::HashMap<String, Value>,
) -> std::collections::HashMap<String, Value> {
    context
        .iter()
        .map(|(k, v)| {
            let text = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (
                k.clone(),
                Value::String(urlencoding::encode(&text).into_owned()),
            )
        })
        .collect()
}

/// Names of the non-system data streams in a `GET /_data_stream` response.
fn parse_data_streams(body: &Value) -> Vec<String> {
    let mut names: Vec<String> = body
        .get("data_streams")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .filter_map(|s| s.get("name").and_then(Value::as_str))
                .filter(|n| !n.starts_with('.'))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Map an Elasticsearch mapping field type to a JSON-Schema type. ES mappings
/// carry no nullability, so types stay scalar (never nullable-wrapped).
/// Everything not listed (text, keyword, date, ip, …) serializes as a JSON
/// string in `_source`, so `string` is the safe over-approximation.
fn es_type_to_json_type(es_type: &str) -> &'static str {
    match es_type {
        "long" | "integer" | "short" | "byte" => "integer",
        "double" | "float" | "half_float" | "scaled_float" => "number",
        "boolean" => "boolean",
        "object" | "nested" => "object",
        _ => "string",
    }
}

/// Convert one index's `mappings` object (`{"properties": {field: spec, …}}`)
/// into an [`infer_schema`](faucet_core::schema::infer_schema)-shaped object
/// schema at top-level-column granularity. A field spec without a scalar
/// `type` is an object field (Elasticsearch's default for mapping entries
/// that carry only nested `properties`). Pure.
fn mapping_to_schema(mappings: &Value) -> Value {
    let mut properties = serde_json::Map::new();
    if let Some(fields) = mappings.get("properties").and_then(Value::as_object) {
        for (name, spec) in fields {
            let ty = match spec.get("type").and_then(Value::as_str) {
                Some(t) => es_type_to_json_type(t),
                None => "object",
            };
            properties.insert(name.clone(), json!({ "type": ty }));
        }
    }
    json!({ "type": "object", "properties": Value::Object(properties) })
}

/// Parse a `GET _cat/indices?format=json` response into `(index, doc_count)`
/// pairs, skipping system indices (leading `.`). Doc counts arrive as strings
/// in `_cat` JSON — an unparsable or missing count yields `None`. Sorted by
/// index name for deterministic output. Pure.
fn parse_cat_indices(cat: &Value) -> Vec<(String, Option<u64>)> {
    let mut entries: Vec<(String, Option<u64>)> = cat
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let index = row.get("index")?.as_str()?;
                    if index.starts_with('.') {
                        return None;
                    }
                    let doc_count = row.get("docs.count").and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse::<u64>().ok())
                            .or_else(|| v.as_u64())
                    });
                    Some((index.to_string(), doc_count))
                })
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// Build the [`DatasetDescriptor`](faucet_core::DatasetDescriptor) for one
/// index from its `_cat` row and its `GET /<index>/_mapping` response body
/// (`{"<index>": {"mappings": {…}}}`). The body's top-level key is the
/// concrete index name, which can differ from the requested one when the
/// request resolved through an alias — fall back to the first entry. Pure.
fn descriptor_for_index(
    index: &str,
    doc_count: Option<u64>,
    mapping_body: &Value,
) -> faucet_core::DatasetDescriptor {
    let empty = json!({});
    let mappings = mapping_body
        .get(index)
        .or_else(|| mapping_body.as_object().and_then(|o| o.values().next()))
        .and_then(|entry| entry.get("mappings"))
        .unwrap_or(&empty);
    let mut descriptor =
        faucet_core::DatasetDescriptor::new(index, "index", json!({ "index": index }))
            .with_schema(mapping_to_schema(mappings));
    if let Some(rows) = doc_count {
        descriptor = descriptor.with_estimated_rows(rows);
    }
    descriptor
}

/// RAII guard that owns the active scroll id and clears it on drop.
///
/// Holds a pre-resolved [`ElasticsearchAuth`] (not `AuthSpec`) so the drop-path
/// spawned cleanup tasks never need to perform async auth resolution.
struct ScrollGuard {
    base_url: String,
    client: Client,
    auth: ElasticsearchAuth,
    scroll_id: Option<String>,
}

impl ScrollGuard {
    fn new(base_url: String, client: Client, auth: ElasticsearchAuth) -> Self {
        Self {
            base_url,
            client,
            auth,
            scroll_id: None,
        }
    }

    fn scroll_id(&self) -> Option<&str> {
        self.scroll_id.as_deref()
    }

    fn set_auth(&mut self, auth: ElasticsearchAuth) {
        self.auth = auth;
    }

    fn update(&mut self, new_id: Option<String>) {
        if let Some(id) = new_id {
            self.scroll_id = Some(id);
        }
    }

    /// Called when the stream drained cleanly. Spawns cleanup as a detached
    /// task and disarms the drop fallback.
    fn disarm_if_done(&mut self) {
        if let Some(sid) = self.scroll_id.take() {
            let base_url = self.base_url.clone();
            let auth = self.auth.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let url = format!("{base_url}/_search/scroll");
                let req = client.delete(&url).json(&json!({"scroll_id": sid}));
                let req = apply_auth_to(req, &auth);
                if let Err(e) = req.send().await {
                    tracing::warn!(error = %e, "failed to clear Elasticsearch scroll context");
                }
            });
        }
    }
}

impl Drop for ScrollGuard {
    fn drop(&mut self) {
        if let Some(sid) = self.scroll_id.take() {
            // Error / cancellation path. Spawn so cleanup survives the
            // stream future being dropped mid-await.
            let base_url = self.base_url.clone();
            let auth = self.auth.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let url = format!("{base_url}/_search/scroll");
                let req = client.delete(&url).json(&json!({"scroll_id": sid}));
                let req = apply_auth_to(req, &auth);
                if let Err(e) = req.send().await {
                    tracing::warn!(
                        error = %e,
                        "failed to clear Elasticsearch scroll context (drop path)",
                    );
                }
            });
        }
    }
}

/// Apply an [`ElasticsearchAuth`] to a request builder. Standalone so
/// spawned cleanup tasks can use it without holding a source reference.
fn apply_auth_to(
    req: reqwest::RequestBuilder,
    auth: &ElasticsearchAuth,
) -> reqwest::RequestBuilder {
    match auth {
        ElasticsearchAuth::None => req,
        ElasticsearchAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
        ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
        ElasticsearchAuth::ApiKey { key } => req.header("Authorization", format!("ApiKey {key}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_values_are_path_encoded_and_non_strings_rendered() {
        let ctx: std::collections::HashMap<String, Value> = [
            ("a".to_string(), serde_json::json!("x/y")),
            ("n".to_string(), serde_json::json!(7)),
        ]
        .into_iter()
        .collect();
        let enc = encoded_context(&ctx);
        assert_eq!(enc["a"], serde_json::json!("x%2Fy"));
        assert_eq!(enc["n"], serde_json::json!("7"));
    }
    use faucet_core::Source;

    #[test]
    fn new_rejects_out_of_range_batch_size() {
        let mut config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match ElasticsearchSource::new(config) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            _ => panic!("expected a batch_size Config error"),
        }
    }

    #[test]
    fn dataset_uri_returns_base_url_slash_index() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "my_index");
        let source = ElasticsearchSource::new(config).unwrap();
        assert_eq!(source.dataset_uri(), "http://localhost:9200/my_index");
    }

    #[test]
    fn dataset_uri_strips_credentials() {
        let config =
            ElasticsearchSourceConfig::new("http://user:secret@es.example.com:9200", "logs");
        let source = ElasticsearchSource::new(config).unwrap();
        assert_eq!(source.dataset_uri(), "http://es.example.com:9200/logs");
    }

    // ── discover: pure mapping/_cat parsing (#211) ──────────────────────────

    #[test]
    fn es_types_map_to_json_types() {
        for (es, want) in [
            ("long", "integer"),
            ("integer", "integer"),
            ("short", "integer"),
            ("byte", "integer"),
            ("double", "number"),
            ("float", "number"),
            ("half_float", "number"),
            ("scaled_float", "number"),
            ("boolean", "boolean"),
            ("object", "object"),
            ("nested", "object"),
            ("text", "string"),
            ("keyword", "string"),
            ("date", "string"),
            ("ip", "string"),
            ("geo_point", "string"),
        ] {
            assert_eq!(es_type_to_json_type(es), want, "for ES type {es:?}");
        }
    }

    #[test]
    fn mapping_to_schema_covers_scalar_object_and_nested_fields() {
        // Real-shaped mapping: scalar types, a text field with a keyword
        // sub-field, and an object field declared only via nested properties.
        let mappings = json!({
            "properties": {
                "id": {"type": "long"},
                "total": {"type": "scaled_float", "scaling_factor": 100},
                "note": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "active": {"type": "boolean"},
                "customer": {"properties": {"name": {"type": "text"}}},
                "meta": {"type": "nested", "properties": {"k": {"type": "keyword"}}},
            }
        });
        let schema = mapping_to_schema(&mappings);
        assert_eq!(schema["type"], "object");
        let props = &schema["properties"];
        assert_eq!(props["id"]["type"], "integer");
        assert_eq!(props["total"]["type"], "number");
        assert_eq!(props["note"]["type"], "string");
        assert_eq!(props["active"]["type"], "boolean");
        assert_eq!(
            props["customer"]["type"], "object",
            "type-less field with nested properties is an object"
        );
        assert_eq!(props["meta"]["type"], "object");
    }

    #[test]
    fn mapping_to_schema_empty_mappings_yield_empty_properties() {
        assert_eq!(
            mapping_to_schema(&json!({})),
            json!({"type": "object", "properties": {}})
        );
        assert_eq!(
            mapping_to_schema(&Value::Null),
            json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn parse_cat_indices_skips_system_and_parses_counts() {
        let cat = json!([
            {"index": "orders", "docs.count": "1200"},
            {"index": ".kibana_1", "docs.count": "3"},
            {"index": "logs", "docs.count": "n/a"},
            {"index": "metrics"},
            {"index": "numeric", "docs.count": 7},
        ]);
        let entries = parse_cat_indices(&cat);
        assert_eq!(
            entries,
            vec![
                ("logs".to_string(), None),
                ("metrics".to_string(), None),
                ("numeric".to_string(), Some(7)),
                ("orders".to_string(), Some(1200)),
            ],
            "system index skipped, unparsable/missing counts → None, sorted"
        );
    }

    #[test]
    fn parse_cat_indices_non_array_is_empty() {
        assert!(parse_cat_indices(&json!({"error": "nope"})).is_empty());
        assert!(parse_cat_indices(&Value::Null).is_empty());
    }

    #[test]
    fn descriptor_for_index_builds_full_descriptor() {
        let body = json!({
            "orders": {"mappings": {"properties": {"id": {"type": "long"}}}}
        });
        let d = descriptor_for_index("orders", Some(1200), &body);
        assert_eq!(d.name, "orders");
        assert_eq!(d.kind, "index");
        assert_eq!(d.config_patch, json!({"index": "orders"}));
        assert_eq!(d.estimated_rows, Some(1200));
        let schema = d.schema.as_ref().expect("schema");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
    }

    #[test]
    fn descriptor_for_index_falls_back_to_first_mapping_entry() {
        // The _mapping response is keyed by the *concrete* index name, which
        // differs from the requested name when it resolved via an alias.
        let body = json!({
            "orders-000001": {"mappings": {"properties": {"id": {"type": "long"}}}}
        });
        let d = descriptor_for_index("orders", None, &body);
        assert_eq!(d.estimated_rows, None);
        let schema = d.schema.as_ref().expect("schema");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
    }

    #[test]
    fn descriptor_for_index_missing_mappings_yields_empty_schema() {
        let d = descriptor_for_index("orders", Some(0), &json!({}));
        assert_eq!(
            d.schema,
            Some(json!({"type": "object", "properties": {}})),
            "no mappings → empty object schema, never a panic"
        );
    }

    #[test]
    fn source_advertises_discover() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx");
        let source = ElasticsearchSource::new(config).unwrap();
        assert!(source.supports_discover());
    }
}
