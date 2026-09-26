//! Databricks SQL query source over the Statement Execution API.
//!
//! Submits `sql` to `POST /api/2.0/sql/statements`, polls
//! `GET /api/2.0/sql/statements/{id}` until the statement is terminal, then
//! streams the result chunks (INLINE + JSON_ARRAY) as typed JSON rows,
//! following `result.next_chunk_internal_link` across chunks. No SDK — plain
//! `reqwest` over the shared-auth bearer token.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_common_databricks::{
    ErrorSide, StatementClient, StatementOptions, resolve_authorization, value_to_param_string,
};
use faucet_core::replication::{filter_incremental, max_value};
use faucet_core::{FaucetError, SharedAuthProvider, Source, Stream, StreamPage};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::{DatabricksReplication, DatabricksSourceConfig, shared_auth};
use crate::convert::{ColumnInfo, row_to_json};

/// Databricks SQL query source.
pub struct DatabricksSource {
    config: DatabricksSourceConfig,
    client: Client,
    /// Base URL override (up to but not including `/api/2.0/...`). `None` uses
    /// `config.workspace_url`. Set by tests to point at a mock server.
    endpoint_base: Option<String>,
    /// Shared auth provider; when set, supplies the bearer token (takes
    /// precedence over inline auth).
    auth_provider: Option<SharedAuthProvider>,
    /// Bookmark applied via [`Source::apply_start_bookmark`].
    start_bookmark: Mutex<Option<Value>>,
}

#[derive(Debug, Deserialize)]
struct ResultChunk {
    #[serde(default)]
    data_array: Option<Vec<Vec<Value>>>,
    #[serde(default)]
    next_chunk_internal_link: Option<String>,
    /// Present under `EXTERNAL_LINKS` disposition (ARROW_STREAM): each entry
    /// carries a presigned URL to an Arrow IPC chunk plus its own
    /// next-chunk link. Only consumed by the `arrow` columnar path.
    #[cfg(feature = "arrow")]
    #[serde(default)]
    external_links: Option<Vec<ExternalLink>>,
}

/// One `result.external_links[]` entry (EXTERNAL_LINKS disposition).
#[cfg(feature = "arrow")]
#[derive(Debug, Deserialize)]
struct ExternalLink {
    #[serde(default)]
    external_link: Option<String>,
    #[serde(default)]
    next_chunk_internal_link: Option<String>,
}

/// The internal link to the next result chunk, checked at both the result
/// level and (for EXTERNAL_LINKS) the first external-link level.
#[cfg(feature = "arrow")]
fn next_chunk_link(chunk: &ResultChunk) -> Option<String> {
    chunk.next_chunk_internal_link.clone().or_else(|| {
        chunk
            .external_links
            .as_ref()
            .and_then(|l| l.first())
            .and_then(|e| e.next_chunk_internal_link.clone())
    })
}

/// A terminal statement: its result columns and first chunk.
struct Executed {
    columns: Vec<ColumnInfo>,
    result: Option<ResultChunk>,
}

/// Client-side incremental filter context.
struct IncrementalCtx {
    column: String,
    start: Value,
}

impl DatabricksSource {
    /// Create a new source. Validates config; does no I/O.
    pub fn new(config: DatabricksSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            config,
            client: Client::new(),
            endpoint_base: None,
            auth_provider: None,
            start_bookmark: Mutex::new(None),
        })
    }

    /// Attach a shared auth provider (yields the bearer token). Takes
    /// precedence over inline auth — lets several connectors share one token.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Override the base URL (e.g. a wiremock server). Requests go to
    /// `{base}/api/2.0/sql/statements`.
    pub fn with_endpoint_base(mut self, base: impl Into<String>) -> Self {
        self.endpoint_base = Some(base.into());
        self
    }

    fn base_url(&self) -> String {
        match &self.endpoint_base {
            Some(b) => b.trim_end_matches('/').to_owned(),
            None => self.config.workspace_url.trim_end_matches('/').to_owned(),
        }
    }

    fn statements_url(&self) -> String {
        format!("{}/api/2.0/sql/statements", self.base_url())
    }

    /// Resolve the `Authorization` header value: shared provider first, else
    /// inline auth.
    async fn auth_header(&self) -> Result<String, FaucetError> {
        resolve_authorization(&shared_auth(&self.config.auth), self.auth_provider.as_ref()).await
    }

    /// A Statement Execution API client for this source's warehouse.
    fn statement_client(&self) -> StatementClient {
        StatementClient::new(
            self.client.clone(),
            self.base_url(),
            self.config.warehouse_id.clone(),
            shared_auth(&self.config.auth),
            self.auth_provider.clone(),
            StatementOptions {
                wait_timeout_secs: self.config.wait_timeout_secs,
                poll_interval: Duration::from_secs(self.config.poll_interval_secs.max(1)),
                ..StatementOptions::default()
            },
            ErrorSide::Source,
        )
    }

    /// The effective incremental start bookmark (persisted bookmark, else the
    /// configured `initial_value`).
    fn incremental_ctx(&self) -> Option<IncrementalCtx> {
        match &self.config.replication {
            DatabricksReplication::Full => None,
            DatabricksReplication::Incremental {
                column,
                initial_value,
            } => {
                let start = self
                    .start_bookmark
                    .lock()
                    .expect("start_bookmark mutex poisoned")
                    .clone()
                    .unwrap_or_else(|| initial_value.clone());
                Some(IncrementalCtx {
                    column: column.clone(),
                    start,
                })
            }
        }
    }

    /// Build the request body: the SQL (with `${bookmark}` and `{ctx}` tokens
    /// rewritten to named params) plus the `parameters` array.
    fn build_body(&self, context: &HashMap<String, Value>, incr: Option<&IncrementalCtx>) -> Value {
        let mut sql = self.config.sql.clone();
        // Named parameters: start with the user's static ones.
        let mut params: Vec<Value> = self
            .config
            .parameters
            .iter()
            .map(|p| {
                json!({
                    "name": p.name,
                    "value": value_to_param_string(&p.value),
                    "type": p.param_type.clone().unwrap_or_else(|| "STRING".into()),
                })
            })
            .collect();

        // Parent-context `{key}` tokens → `:_faucet_ctN` named params.
        if !context.is_empty() {
            let (rewritten, ctx_values) =
                faucet_core::util::substitute_context_bind_params(&sql, context, 0, |i| {
                    format!(":_faucet_ct{i}")
                });
            sql = rewritten;
            for (i, v) in ctx_values.into_iter().enumerate() {
                params.push(json!({
                    "name": format!("_faucet_ct{i}"),
                    "value": value_to_param_string(&v),
                }));
            }
        }

        // Incremental `${bookmark}` token → `:_faucet_bookmark` named param.
        if let Some(ctx) = incr
            && sql.contains("${bookmark}")
        {
            sql = sql.replace("${bookmark}", ":_faucet_bookmark");
            params.push(json!({
                "name": "_faucet_bookmark",
                "value": value_to_param_string(&ctx.start),
            }));
        }

        // ARROW_STREAM is only valid with EXTERNAL_LINKS disposition; JSON_ARRAY
        // stays INLINE. `arrow_native` is validated to require the `arrow`
        // feature at config load, so requesting Arrow here is always decodable.
        let (disposition, format) = if self.config.arrow_native {
            ("EXTERNAL_LINKS", "ARROW_STREAM")
        } else {
            ("INLINE", "JSON_ARRAY")
        };
        let mut body = json!({
            "statement": sql,
            "warehouse_id": self.config.warehouse_id,
            "wait_timeout": format!("{}s", self.config.wait_timeout_secs),
            "on_wait_timeout": "CONTINUE",
            "disposition": disposition,
            "format": format,
        });
        if let Some(c) = &self.config.catalog {
            body["catalog"] = json!(c);
        }
        if let Some(s) = &self.config.schema {
            body["schema"] = json!(s);
        }
        if !params.is_empty() {
            body["parameters"] = Value::Array(params);
        }
        body
    }

    /// Submit the statement and poll until it reaches a terminal state.
    async fn run_statement(
        &self,
        context: &HashMap<String, Value>,
        incr: Option<&IncrementalCtx>,
    ) -> Result<Executed, FaucetError> {
        let body = self.build_body(context, incr);
        let resp = self.statement_client().execute_body(&body).await?;
        let columns = resp.columns().to_vec();
        let result = resp.result.map(decode_chunk).transpose()?;
        Ok(Executed { columns, result })
    }

    /// Fetch a follow-up chunk by its `next_chunk_internal_link` (a full API path).
    async fn fetch_chunk(&self, link: &str) -> Result<ResultChunk, FaucetError> {
        decode_chunk(self.statement_client().fetch_chunk(link).await?)
    }

    /// Fetch a presigned external link and decode its body as an Arrow IPC
    /// stream. The link is a pre-signed cloud-storage URL, so it is fetched
    /// **without** an `Authorization` header (adding one breaks the signature).
    #[cfg(feature = "arrow")]
    async fn fetch_arrow_link(
        &self,
        url: &str,
    ) -> Result<Vec<arrow::array::RecordBatch>, FaucetError> {
        let resp = self.client.get(url).send().await.map_err(|e| {
            FaucetError::Source(format!("databricks: external-link request failed: {e}"))
        })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Source(format!(
                "databricks: external link HTTP {status}: {body}"
            )));
        }
        let data = resp.bytes().await.map_err(|e| {
            FaucetError::Source(format!(
                "databricks: reading external-link body failed: {e}"
            ))
        })?;
        tokio::task::spawn_blocking(move || decode_arrow_ipc(data))
            .await
            .map_err(|e| {
                FaucetError::Source(format!("databricks: arrow decode task panicked: {e}"))
            })?
    }
}

/// Decode an Arrow IPC **stream** (the ARROW_STREAM chunk body) into its
/// `RecordBatch`es. Synchronous (runs inside `spawn_blocking`).
#[cfg(feature = "arrow")]
fn decode_arrow_ipc(data: bytes::Bytes) -> Result<Vec<arrow::array::RecordBatch>, FaucetError> {
    use arrow::ipc::reader::StreamReader;

    let reader = StreamReader::try_new(std::io::Cursor::new(data), None).map_err(|e| {
        FaucetError::Source(format!("databricks: arrow IPC reader init failed: {e}"))
    })?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| {
            FaucetError::Source(format!("databricks: arrow IPC decode failed: {e}"))
        })?);
    }
    Ok(batches)
}

/// Derive a stable state key from the workspace, warehouse, and query.
fn default_state_key(config: &DatabricksSourceConfig) -> String {
    let mut h = DefaultHasher::new();
    config.workspace_url.hash(&mut h);
    config.warehouse_id.hash(&mut h);
    config.sql.hash(&mut h);
    format!("databricks:{:016x}", h.finish())
}

/// Decode a raw result chunk.
fn decode_chunk(v: Value) -> Result<ResultChunk, FaucetError> {
    serde_json::from_value(v)
        .map_err(|e| FaucetError::Source(format!("databricks: could not parse response: {e}")))
}

#[async_trait]
impl Source for DatabricksSource {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DatabricksSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "databricks"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "databricks://{}/warehouses/{}",
            self.config
                .workspace_url
                .trim_start_matches("https://")
                .trim_end_matches('/'),
            self.config.warehouse_id
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            DatabricksReplication::Full => None,
            DatabricksReplication::Incremental { .. } => Some(
                self.config
                    .state_key
                    .clone()
                    .unwrap_or_else(|| default_state_key(&self.config)),
            ),
        }
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self
            .start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned") = Some(bookmark);
        Ok(())
    }

    /// The Databricks source advertises the columnar fast path only when
    /// [`arrow_native`](DatabricksSourceConfig::arrow_native) is set — i.e. the
    /// statement is fetched as `ARROW_STREAM` (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config.arrow_native
    }

    /// Stream the statement's `ARROW_STREAM` result chunks as Arrow
    /// `RecordBatch`es — one [`ColumnarPage`](faucet_core::columnar::ColumnarPage)
    /// per batch — so a `databricks → parquet`/`delta`/`sql` chain never
    /// materializes `serde_json::Value`. `arrow_native` is Full-replication
    /// only, so every page carries `bookmark: None`. Empty batches are skipped.
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a,
        >,
    > {
        Box::pin(async_stream::try_stream! {
            if !self.config.arrow_native {
                Err(FaucetError::Source(
                    "databricks: stream_batches requires `arrow_native: true`".into(),
                ))?;
            }
            let resp = self.run_statement(context, None).await?;
            let mut chunk = resp.result;
            let mut total_records = 0usize;
            let mut total_pages = 0usize;
            while let Some(c) = chunk {
                if let Some(links) = c.external_links.as_ref() {
                    for link in links {
                        if let Some(url) = link.external_link.as_deref() {
                            let batches = self.fetch_arrow_link(url).await?;
                            for batch in batches {
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                total_records += batch.num_rows();
                                total_pages += 1;
                                yield faucet_core::columnar::ColumnarPage { batch, bookmark: None };
                            }
                        }
                    }
                }
                chunk = match next_chunk_link(&c) {
                    Some(link) => Some(self.fetch_chunk(&link).await?),
                    None => None,
                };
            }
            tracing::info!(
                pages = total_pages,
                total_records,
                "databricks columnar stream complete",
            );
        })
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            // Arrow-native row path: fetch ARROW_STREAM chunks and decode each
            // RecordBatch to JSON rows (for a non-columnar sink). `arrow_native`
            // is Full-replication only (enforced by config validation), so no
            // incremental filter runs here.
            #[cfg(feature = "arrow")]
            if self.config.arrow_native {
                let resp = self.run_statement(context, None).await?;
                let cap = if self.config.batch_size == 0 { 1024 } else { self.config.batch_size };
                let mut buffer: Vec<Value> = Vec::with_capacity(cap);
                let mut chunk = resp.result;
                while let Some(c) = chunk {
                    if let Some(links) = c.external_links.as_ref() {
                        for link in links {
                            if let Some(url) = link.external_link.as_deref() {
                                let batches = self.fetch_arrow_link(url).await?;
                                for batch in &batches {
                                    for row in faucet_core::columnar::record_batch_to_values(batch)? {
                                        buffer.push(row);
                                        if self.config.batch_size != 0
                                            && buffer.len() >= self.config.batch_size
                                        {
                                            let page = std::mem::replace(
                                                &mut buffer,
                                                Vec::with_capacity(cap),
                                            );
                                            yield StreamPage { records: page, bookmark: None };
                                        }
                                    }
                                }
                            }
                        }
                    }
                    chunk = match next_chunk_link(&c) {
                        Some(link) => Some(self.fetch_chunk(&link).await?),
                        None => None,
                    };
                }
                if !buffer.is_empty() {
                    yield StreamPage { records: buffer, bookmark: None };
                }
                return;
            }

            let incr = self.incremental_ctx();
            let resp = self.run_statement(context, incr.as_ref()).await?;

            let columns: Vec<ColumnInfo> = resp.columns;

            let batch = self.config.batch_size;
            let cap = if batch == 0 { 1024 } else { batch };
            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            let mut running_max: Option<Value> = None;

            // Walk chunks: the initial `result`, then follow next_chunk_internal_link.
            let mut chunk = resp.result;
            while let Some(c) = chunk {
                if let Some(data) = c.data_array {
                    for row in &data {
                        let obj = row_to_json(row, &columns);
                        // Track the running max BEFORE the client-side filter so
                        // the persisted bookmark reflects the full scan.
                        if let Some(ic) = &incr
                            && let Some(v) = obj.get(&ic.column)
                        {
                            running_max = Some(match running_max.take() {
                                Some(m) => max_value(m, v.clone()),
                                None => v.clone(),
                            });
                        }
                        buffer.push(obj);
                        if batch != 0 && buffer.len() >= batch {
                            let page = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                            let kept = apply_incr_filter(page, incr.as_ref());
                            if !kept.is_empty() {
                                yield StreamPage { records: kept, bookmark: None };
                            }
                        }
                    }
                }
                chunk = match c.next_chunk_internal_link {
                    Some(link) => Some(self.fetch_chunk(&link).await?),
                    None => None,
                };
            }

            // Final page carries the new bookmark (incremental only).
            let kept = apply_incr_filter(buffer, incr.as_ref());
            let bookmark = if incr.is_some() { running_max } else { None };
            if !kept.is_empty() || bookmark.is_some() {
                yield StreamPage { records: kept, bookmark };
            }
        })
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut s = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = s.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        // Non-scanning probe: run `SELECT 1` on the warehouse.
        let auth = match self.auth_header().await {
            Ok(a) => a,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                )));
            }
        };
        let body = json!({
            "statement": "SELECT 1",
            "warehouse_id": self.config.warehouse_id,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        });
        let fut = self
            .client
            .post(self.statements_url())
            .header("Authorization", &auth)
            .header("Content-Type", "application/json")
            .json(&body)
            .send();
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Ok(Ok(r)) if r.status().is_success() => Probe::pass("warehouse", started.elapsed()),
            Ok(Ok(r)) => Probe::fail_hint(
                "warehouse",
                started.elapsed(),
                format!("databricks probe returned HTTP {}", r.status()),
                "Verify workspace_url, warehouse_id, and token permissions (CAN USE).",
            ),
            Ok(Err(e)) => Probe::fail_hint(
                "warehouse",
                started.elapsed(),
                format!("databricks probe request failed: {e}"),
                "Verify workspace_url and network reachability.",
            ),
            Err(_) => Probe::fail_hint(
                "warehouse",
                started.elapsed(),
                format!("databricks probe timed out after {:?}", ctx.timeout),
                "Check warehouse availability and network reachability.",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

/// Apply the client-side incremental filter to a page (no-op for full runs).
fn apply_incr_filter(page: Vec<Value>, incr: Option<&IncrementalCtx>) -> Vec<Value> {
    match incr {
        Some(ic) => filter_incremental(page, &ic.column, &ic.start),
        None => page,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabricksAuth, DatabricksParam};
    use faucet_core::AuthSpec;

    fn cfg() -> DatabricksSourceConfig {
        DatabricksSourceConfig {
            workspace_url: "https://x.cloud.databricks.com".into(),
            warehouse_id: "wh1".into(),
            sql: "SELECT * FROM t WHERE ts > ${bookmark}".into(),
            auth: AuthSpec::Inline(DatabricksAuth::Pat {
                token: "tok".into(),
            }),
            catalog: Some("main".into()),
            schema: Some("s".into()),
            parameters: vec![DatabricksParam {
                name: "min".into(),
                value: json!(10),
                param_type: Some("INT".into()),
            }],
            wait_timeout_secs: 50,
            poll_interval_secs: 1,
            batch_size: 1000,
            arrow_native: false,
            replication: DatabricksReplication::Incremental {
                column: "ts".into(),
                initial_value: json!("2026-01-01"),
            },
            state_key: None,
        }
    }

    fn source(c: DatabricksSourceConfig) -> DatabricksSource {
        DatabricksSource::new(c).unwrap()
    }

    #[test]
    fn body_has_required_fields_and_params() {
        let s = source(cfg());
        let incr = s.incremental_ctx();
        let body = s.build_body(&HashMap::new(), incr.as_ref());
        assert_eq!(body["warehouse_id"], json!("wh1"));
        assert_eq!(body["catalog"], json!("main"));
        assert_eq!(body["disposition"], json!("INLINE"));
        assert_eq!(body["format"], json!("JSON_ARRAY"));
        assert_eq!(body["wait_timeout"], json!("50s"));
        // ${bookmark} rewritten to the named param marker.
        assert!(
            body["statement"]
                .as_str()
                .unwrap()
                .contains(":_faucet_bookmark")
        );
        assert!(!body["statement"].as_str().unwrap().contains("${bookmark}"));
        let params = body["parameters"].as_array().unwrap();
        // static `min` (typed) + the bookmark param.
        assert!(
            params
                .iter()
                .any(|p| p["name"] == json!("min") && p["type"] == json!("INT"))
        );
        let bm = params
            .iter()
            .find(|p| p["name"] == json!("_faucet_bookmark"))
            .unwrap();
        assert_eq!(bm["value"], json!("2026-01-01"));
    }

    #[test]
    fn full_mode_has_no_state_key_or_bookmark_param() {
        let mut c = cfg();
        c.replication = DatabricksReplication::Full;
        c.sql = "SELECT 1".into();
        let s = source(c);
        assert!(s.state_key().is_none());
        let body = s.build_body(&HashMap::new(), None);
        assert!(
            body.get("parameters").is_none()
                || body["parameters"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|p| p["name"] != json!("_faucet_bookmark"))
        );
    }

    #[test]
    fn incremental_state_key_derived_and_stable() {
        let s = source(cfg());
        let k1 = s.state_key().unwrap();
        let k2 = source(cfg()).state_key().unwrap();
        assert_eq!(k1, k2);
        assert!(k1.starts_with("databricks:"));
    }

    #[tokio::test]
    async fn explicit_state_key_wins() {
        let mut c = cfg();
        c.state_key = Some("my-key".into());
        let s = source(c);
        assert_eq!(s.state_key().as_deref(), Some("my-key"));
        // apply_start_bookmark overrides initial_value in the incr ctx.
        s.apply_start_bookmark(json!("2026-06-01")).await.unwrap();
        assert_eq!(s.incremental_ctx().unwrap().start, json!("2026-06-01"));
    }

    #[test]
    fn value_to_param_string_stringifies() {
        assert_eq!(value_to_param_string(&json!(5)), json!("5"));
        assert_eq!(value_to_param_string(&json!(true)), json!("true"));
        assert_eq!(value_to_param_string(&json!("x")), json!("x"));
        assert_eq!(value_to_param_string(&Value::Null), Value::Null);
    }

    #[test]
    fn decode_chunk_rejects_non_objects() {
        assert!(decode_chunk(json!({"data_array": [["1"]]})).is_ok());
        let err = decode_chunk(json!(5)).unwrap_err().to_string();
        assert!(err.contains("could not parse"), "{err}");
    }

    #[test]
    fn dataset_uri_redacts_scheme() {
        let s = source(cfg());
        assert_eq!(
            s.dataset_uri(),
            "databricks://x.cloud.databricks.com/warehouses/wh1"
        );
    }
}
