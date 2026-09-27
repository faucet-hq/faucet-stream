//! A thin client over the Databricks SQL Statement Execution API.
//!
//! `POST /api/2.0/sql/statements` submits; `GET /api/2.0/sql/statements/{id}`
//! polls until the statement is terminal; `POST …/{id}/cancel` stops one that
//! outlived the client deadline. No SDK — plain `reqwest`.
//!
//! Retries are deliberately narrow, because a retried submit that the server
//! already accepted would run a non-idempotent statement twice:
//!
//! - a submit (`POST`) is retried only on `429` / `503` — the warehouse (or its
//!   gateway) refused it, so nothing ran — and once on `401` when a shared auth
//!   provider can mint a fresh token;
//! - a poll / chunk fetch (`GET`) is retried on `429`, any `5xx` and transport
//!   errors, since reading a statement's status has no side effects.

use std::time::{Duration, Instant};

use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::DatabricksAuth;

/// Which side of the pipeline a client serves — selects the [`FaucetError`]
/// variant errors are reported as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSide {
    /// Errors surface as [`FaucetError::Source`].
    Source,
    /// Errors surface as [`FaucetError::Sink`].
    Sink,
}

impl ErrorSide {
    /// Wrap `msg` in this side's error variant.
    pub fn error(self, msg: impl Into<String>) -> FaucetError {
        match self {
            ErrorSide::Source => FaucetError::Source(msg.into()),
            ErrorSide::Sink => FaucetError::Sink(msg.into()),
        }
    }
}

/// Lifecycle state of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementState {
    /// Queued (e.g. the warehouse is starting).
    Pending,
    /// Executing.
    Running,
    /// Finished successfully.
    Succeeded,
    /// Finished with an error.
    Failed,
    /// Cancelled.
    Canceled,
    /// Closed (results expired).
    Closed,
    /// A state this client does not know.
    Unknown,
}

impl StatementState {
    /// Parse the API's state string.
    pub fn parse(s: &str) -> Self {
        match s {
            "PENDING" => StatementState::Pending,
            "RUNNING" => StatementState::Running,
            "SUCCEEDED" => StatementState::Succeeded,
            "FAILED" => StatementState::Failed,
            "CANCELED" => StatementState::Canceled,
            "CLOSED" => StatementState::Closed,
            _ => StatementState::Unknown,
        }
    }
}

/// Error detail attached to a failed statement.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatementError {
    /// Databricks error code (e.g. `BAD_REQUEST`).
    #[serde(default)]
    pub error_code: Option<String>,
    /// Human-readable message.
    #[serde(default)]
    pub message: Option<String>,
}

/// `status` block of a statement response.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatementStatus {
    /// Raw state string (`PENDING`, `RUNNING`, `SUCCEEDED`, …).
    #[serde(default)]
    pub state: String,
    /// Error detail when the state is `FAILED`.
    #[serde(default)]
    pub error: Option<StatementError>,
}

/// One entry of `manifest.schema.columns`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResultColumn {
    /// Column name (casing preserved).
    pub name: String,
    /// High-level Databricks type — `BOOLEAN`, `LONG`, `DECIMAL`, `STRING`, …
    #[serde(default)]
    pub type_name: String,
}

/// `manifest.schema`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResultSchema {
    /// Result columns, in order.
    #[serde(default)]
    pub columns: Vec<ResultColumn>,
}

/// `manifest` block of a statement response.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Manifest {
    /// Result schema.
    #[serde(default)]
    pub schema: Option<ResultSchema>,
}

/// A statement response (only the fields the connectors consume). `result`
/// is kept raw so each connector decodes the chunk shape it asked for.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatementResponse {
    /// Statement id (present once accepted).
    #[serde(default)]
    pub statement_id: Option<String>,
    /// Lifecycle status.
    #[serde(default)]
    pub status: Option<StatementStatus>,
    /// Result manifest (schema).
    #[serde(default)]
    pub manifest: Option<Manifest>,
    /// First result chunk, undecoded.
    #[serde(default)]
    pub result: Option<Value>,
}

impl StatementResponse {
    /// The parsed lifecycle state ([`StatementState::Unknown`] when absent).
    pub fn state(&self) -> StatementState {
        StatementState::parse(self.status.as_ref().map(|s| s.state.as_str()).unwrap_or(""))
    }

    /// The manifest's result columns (empty when absent).
    pub fn columns(&self) -> &[ResultColumn] {
        self.manifest
            .as_ref()
            .and_then(|m| m.schema.as_ref())
            .map(|s| s.columns.as_slice())
            .unwrap_or(&[])
    }

    /// The first chunk's `JSON_ARRAY` rows as string cells (`None` = SQL NULL).
    /// Non-string cells are rendered as their JSON text.
    pub fn string_rows(&self) -> Vec<Vec<Option<String>>> {
        let Some(rows) = self
            .result
            .as_ref()
            .and_then(|r| r.get("data_array"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };
        rows.iter()
            .map(|row| {
                row.as_array()
                    .map(|cells| {
                        cells
                            .iter()
                            .map(|c| match c {
                                Value::Null => None,
                                Value::String(s) => Some(s.clone()),
                                other => Some(other.to_string()),
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }
}

/// A named statement parameter (`:name` marker in the SQL).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatementParam {
    /// Marker name without the colon.
    pub name: String,
    /// String value, or `null` for a typed NULL.
    pub value: Value,
    /// Optional SQL type (defaults to `STRING` server-side).
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub param_type: Option<String>,
}

impl StatementParam {
    /// A `STRING` parameter.
    pub fn string(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Value::String(value.into()),
            param_type: None,
        }
    }
}

/// A statement to run with the `INLINE` + `JSON_ARRAY` result format.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StatementRequest {
    /// SQL text.
    pub statement: String,
    /// Named parameters.
    pub parameters: Vec<StatementParam>,
    /// Default catalog.
    pub catalog: Option<String>,
    /// Default schema.
    pub schema: Option<String>,
}

impl StatementRequest {
    /// A statement with no parameters or defaults.
    pub fn new(statement: impl Into<String>) -> Self {
        Self {
            statement: statement.into(),
            ..Self::default()
        }
    }

    /// Add a named parameter.
    pub fn param(mut self, p: StatementParam) -> Self {
        self.parameters.push(p);
        self
    }

    /// The request body for `POST /api/2.0/sql/statements`.
    pub fn to_body(&self, warehouse_id: &str, wait_timeout_secs: u64) -> Value {
        let mut body = json!({
            "statement": self.statement,
            "warehouse_id": warehouse_id,
            "wait_timeout": format!("{wait_timeout_secs}s"),
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        });
        if let Some(c) = &self.catalog {
            body["catalog"] = json!(c);
        }
        if let Some(s) = &self.schema {
            body["schema"] = json!(s);
        }
        if !self.parameters.is_empty() {
            body["parameters"] = json!(self.parameters);
        }
        body
    }
}

/// Timing and retry knobs for a [`StatementClient`].
#[derive(Debug, Clone, PartialEq)]
pub struct StatementOptions {
    /// Server-side wait before a statement goes async (`0` or `5`–`50` s).
    pub wait_timeout_secs: u64,
    /// Client poll cadence while `PENDING` / `RUNNING`.
    pub poll_interval: Duration,
    /// Client deadline for one statement; on expiry it is cancelled and the
    /// call fails. `None` polls until terminal.
    pub statement_timeout: Option<Duration>,
    /// Retries for `429` / `503` (and, on reads, `5xx` / transport errors).
    pub max_retries: u32,
    /// Base of the exponential backoff between retries.
    pub retry_backoff: Duration,
}

impl Default for StatementOptions {
    fn default() -> Self {
        Self {
            wait_timeout_secs: 50,
            poll_interval: Duration::from_secs(1),
            statement_timeout: None,
            max_retries: 3,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

/// Upper bound on a single backoff sleep.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Exponential backoff: `base * 2^attempt`, capped at 30 s.
pub fn backoff_delay(base: Duration, attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(MAX_BACKOFF)
}

/// A `Retry-After` header in seconds (HTTP-date form is ignored).
fn retry_after(resp: &reqwest::Response) -> Option<Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|s| Duration::from_secs(s).min(MAX_BACKOFF))
}

/// Stringify a JSON value for a named parameter (`value` is a string or null).
pub fn value_to_param_string(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(s) => Value::String(s.clone()),
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Number(n) => Value::String(n.to_string()),
        other => Value::String(other.to_string()),
    }
}

fn statement_error(side: ErrorSide, state: &str, status: Option<&StatementStatus>) -> FaucetError {
    let detail = status.and_then(|s| s.error.as_ref()).map(|e| {
        format!(
            " [{}] {}",
            e.error_code.as_deref().unwrap_or("UNKNOWN"),
            e.message.as_deref().unwrap_or("")
        )
    });
    side.error(format!(
        "databricks: statement {state}{}",
        detail.unwrap_or_default()
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verb {
    Get,
    Post,
}

/// Client for the Statement Execution API. Cheap to construct per run: the
/// `reqwest::Client` is reference-counted.
#[derive(Clone)]
pub struct StatementClient {
    http: reqwest::Client,
    base_url: String,
    warehouse_id: String,
    auth: AuthSpec<DatabricksAuth>,
    provider: Option<SharedAuthProvider>,
    options: StatementOptions,
    side: ErrorSide,
}

impl std::fmt::Debug for StatementClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementClient")
            .field("base_url", &self.base_url)
            .field("warehouse_id", &self.warehouse_id)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl StatementClient {
    /// Build a client. `base_url` is the workspace URL (or a test server),
    /// up to but excluding `/api/2.0/…`.
    pub fn new(
        http: reqwest::Client,
        base_url: impl Into<String>,
        warehouse_id: impl Into<String>,
        auth: AuthSpec<DatabricksAuth>,
        provider: Option<SharedAuthProvider>,
        options: StatementOptions,
        side: ErrorSide,
    ) -> Self {
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            warehouse_id: warehouse_id.into(),
            auth,
            provider,
            options,
            side,
        }
    }

    /// The base URL (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The underlying HTTP client (for non-statement calls, e.g. the Files API).
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// The configured options.
    pub fn options(&self) -> &StatementOptions {
        &self.options
    }

    /// `{base}/api/2.0/sql/statements`.
    pub fn statements_url(&self) -> String {
        format!("{}/api/2.0/sql/statements", self.base_url)
    }

    /// The current `Authorization` header value.
    pub async fn authorization(&self) -> Result<String, FaucetError> {
        crate::auth::resolve_authorization(&self.auth, self.provider.as_ref()).await
    }

    /// Submit a request and wait until it is terminal.
    pub async fn execute(&self, req: &StatementRequest) -> Result<StatementResponse, FaucetError> {
        let body = req.to_body(&self.warehouse_id, self.options.wait_timeout_secs);
        self.execute_body(&body).await
    }

    /// Submit a pre-built request body and wait until it is terminal.
    pub async fn execute_body(&self, body: &Value) -> Result<StatementResponse, FaucetError> {
        let mut auth = self.authorization().await?;
        let started = Instant::now();
        let url = self.statements_url();
        let resp = self.send(Verb::Post, &url, Some(body), &mut auth).await?;
        let mut current: StatementResponse = self.parse(resp).await?;
        loop {
            let raw_state = current
                .status
                .as_ref()
                .map(|s| s.state.clone())
                .unwrap_or_default();
            match StatementState::parse(&raw_state) {
                StatementState::Succeeded => return Ok(current),
                StatementState::Failed | StatementState::Canceled | StatementState::Closed => {
                    return Err(statement_error(
                        self.side,
                        &raw_state,
                        current.status.as_ref(),
                    ));
                }
                StatementState::Pending | StatementState::Running => {
                    let id = current.statement_id.clone().ok_or_else(|| {
                        self.side
                            .error("databricks: pending statement without a statement_id to poll")
                    })?;
                    if let Some(limit) = self.options.statement_timeout
                        && started.elapsed() >= limit
                    {
                        self.cancel(&id, &auth).await;
                        return Err(self.side.error(format!(
                            "databricks: statement {id} did not finish within {}s (cancelled)",
                            limit.as_secs()
                        )));
                    }
                    tokio::time::sleep(self.options.poll_interval).await;
                    let poll_url = format!("{url}/{id}");
                    let resp = self.send(Verb::Get, &poll_url, None, &mut auth).await?;
                    current = self.parse(resp).await?;
                }
                StatementState::Unknown => {
                    return Err(self.side.error(format!(
                        "databricks: unexpected statement state '{raw_state}'"
                    )));
                }
            }
        }
    }

    /// Fetch a follow-up result chunk by its `next_chunk_internal_link`
    /// (a full API path), returned undecoded.
    pub async fn fetch_chunk(&self, link: &str) -> Result<Value, FaucetError> {
        let mut auth = self.authorization().await?;
        let url = format!("{}{}", self.base_url, link);
        let resp = self.send(Verb::Get, &url, None, &mut auth).await?;
        self.parse(resp).await
    }

    /// Best-effort cancel; a failure is logged, never raised.
    async fn cancel(&self, id: &str, auth: &str) {
        let url = format!("{}/{id}/cancel", self.statements_url());
        match self
            .http
            .post(&url)
            .header("Authorization", auth)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => {
                tracing::warn!(statement_id = id, status = %r.status(), "databricks: cancel failed")
            }
            Err(e) => tracing::warn!(statement_id = id, error = %e, "databricks: cancel failed"),
        }
    }

    async fn parse<T: for<'de> Deserialize<'de>>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, FaucetError> {
        resp.json::<T>().await.map_err(|e| {
            self.side
                .error(format!("databricks: could not parse response: {e}"))
        })
    }

    async fn send(
        &self,
        verb: Verb,
        url: &str,
        body: Option<&Value>,
        auth: &mut String,
    ) -> Result<reqwest::Response, FaucetError> {
        let mut attempt: u32 = 0;
        let mut refreshed = false;
        loop {
            let mut req = match verb {
                Verb::Get => self.http.get(url),
                Verb::Post => self.http.post(url),
            }
            .header("Authorization", auth.as_str());
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if verb == Verb::Get && attempt < self.options.max_retries {
                        tokio::time::sleep(backoff_delay(self.options.retry_backoff, attempt))
                            .await;
                        attempt += 1;
                        continue;
                    }
                    let what = if verb == Verb::Get { "poll" } else { "submit" };
                    return Err(self
                        .side
                        .error(format!("databricks: {what} request failed: {e}")));
                }
            };
            let status = resp.status();
            if status.is_success() {
                return Ok(resp);
            }
            if status.as_u16() == 401
                && !refreshed
                && let Some(p) = &self.provider
            {
                refreshed = true;
                let stale: Credential = p.credential().await?;
                let fresh = p.invalidate(&stale).await?;
                if let Some(v) = fresh.authorization_value() {
                    *auth = v;
                }
                continue;
            }
            let code = status.as_u16();
            let retryable = code == 429 || code == 503 || (verb == Verb::Get && code >= 500);
            if retryable && attempt < self.options.max_retries {
                let wait = retry_after(&resp)
                    .unwrap_or_else(|| backoff_delay(self.options.retry_backoff, attempt));
                tracing::debug!(status = code, attempt, "databricks: retrying request");
                tokio::time::sleep(wait).await;
                attempt += 1;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(self
                .side
                .error(format!("databricks: HTTP {status}: {text}")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_parse_covers_every_variant() {
        for (s, v) in [
            ("PENDING", StatementState::Pending),
            ("RUNNING", StatementState::Running),
            ("SUCCEEDED", StatementState::Succeeded),
            ("FAILED", StatementState::Failed),
            ("CANCELED", StatementState::Canceled),
            ("CLOSED", StatementState::Closed),
            ("WAT", StatementState::Unknown),
        ] {
            assert_eq!(StatementState::parse(s), v);
        }
    }

    #[test]
    fn side_selects_variant() {
        assert!(matches!(
            ErrorSide::Source.error("x"),
            FaucetError::Source(_)
        ));
        assert!(matches!(ErrorSide::Sink.error("x"), FaucetError::Sink(_)));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let b = Duration::from_millis(100);
        assert_eq!(backoff_delay(b, 0), Duration::from_millis(100));
        assert_eq!(backoff_delay(b, 3), Duration::from_millis(800));
        assert_eq!(backoff_delay(b, 40), MAX_BACKOFF);
    }

    #[test]
    fn request_body_shape() {
        let req = StatementRequest {
            statement: "SELECT :a".into(),
            parameters: vec![StatementParam::string("a", "1")],
            catalog: Some("main".into()),
            schema: Some("s".into()),
        };
        let b = req.to_body("wh", 30);
        assert_eq!(b["warehouse_id"], json!("wh"));
        assert_eq!(b["wait_timeout"], json!("30s"));
        assert_eq!(b["on_wait_timeout"], json!("CONTINUE"));
        assert_eq!(b["catalog"], json!("main"));
        assert_eq!(b["schema"], json!("s"));
        assert_eq!(b["parameters"][0], json!({"name": "a", "value": "1"}));
        let bare = StatementRequest::new("SELECT 1").to_body("wh", 0);
        assert!(bare.get("parameters").is_none() && bare.get("catalog").is_none());
    }

    #[test]
    fn typed_param_serializes_type() {
        let p = StatementParam {
            name: "n".into(),
            value: Value::Null,
            param_type: Some("INT".into()),
        };
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            json!({"name": "n", "value": null, "type": "INT"})
        );
        let req = StatementRequest::new("x").param(p.clone());
        assert_eq!(req.parameters, vec![p]);
    }

    #[test]
    fn value_to_param_string_stringifies() {
        assert_eq!(value_to_param_string(&json!(5)), json!("5"));
        assert_eq!(value_to_param_string(&json!(true)), json!("true"));
        assert_eq!(value_to_param_string(&json!("x")), json!("x"));
        assert_eq!(value_to_param_string(&json!({"a": 1})), json!("{\"a\":1}"));
        assert_eq!(value_to_param_string(&Value::Null), Value::Null);
    }

    #[test]
    fn response_accessors() {
        let r: StatementResponse = serde_json::from_value(json!({
            "statement_id": "s",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "a", "type_name": "STRING"}]}},
            "result": {"data_array": [["x", null, 3], "bad"]}
        }))
        .unwrap();
        assert_eq!(r.state(), StatementState::Succeeded);
        assert_eq!(r.columns()[0].name, "a");
        assert_eq!(
            r.string_rows(),
            vec![
                vec![Some("x".into()), None, Some("3".into())],
                Vec::<Option<String>>::new()
            ]
        );
        let empty = StatementResponse::default();
        assert_eq!(empty.state(), StatementState::Unknown);
        assert!(empty.columns().is_empty());
        assert!(empty.string_rows().is_empty());
    }

    #[test]
    fn statement_error_formats_detail() {
        let st = StatementStatus {
            state: "FAILED".into(),
            error: Some(StatementError {
                error_code: None,
                message: Some("boom".into()),
            }),
        };
        let e = statement_error(ErrorSide::Sink, "FAILED", Some(&st));
        assert_eq!(
            e.to_string(),
            "Sink error: databricks: statement FAILED [UNKNOWN] boom"
        );
        let e = statement_error(ErrorSide::Source, "CLOSED", None);
        assert!(e.to_string().ends_with("databricks: statement CLOSED"));
    }

    #[test]
    fn debug_hides_auth() {
        let c = StatementClient::new(
            reqwest::Client::new(),
            "https://x/",
            "wh",
            AuthSpec::Inline(DatabricksAuth::Pat {
                token: "secret".into(),
            }),
            None,
            StatementOptions::default(),
            ErrorSide::Sink,
        );
        let d = format!("{c:?}");
        assert!(!d.contains("secret"));
        assert_eq!(c.base_url(), "https://x");
        assert_eq!(c.options().max_retries, 3);
        let _ = c.http();
    }
}
