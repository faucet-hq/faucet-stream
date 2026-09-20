//! Snowflake SQL REST API source.
//!
//! Executes the configured SQL statement against
//! [`POST /api/v2/statements`](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests)
//! and streams the rows back as JSON. Large result sets are split server-side
//! into partitions; the source fetches each subsequent partition via
//! `GET /api/v2/statements/{handle}?partition={n}` and re-frames the rows
//! into pages of [`SnowflakeSourceConfig::batch_size`].

use crate::config::SnowflakeSourceConfig;
use crate::convert::{ColumnMeta, row_to_json};
use async_trait::async_trait;
use faucet_common_snowflake::{
    SnowflakeAuth, authorization_header, credential_to_auth, snowflake_token_type,
};
use faucet_core::util::substitute_context_bind_params;
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Stream, StreamPage};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// Snowflake API response envelope. Only the fields we need are pulled out;
/// everything else (`createdOn`, `numRows`, `databaseProvider`, ...) is
/// ignored.
#[derive(Debug, Deserialize)]
struct StatementResponse {
    /// Snowflake SQL state code. `"090001"` means "statement executed
    /// successfully"; anything else is an error.
    #[serde(default)]
    code: Option<String>,
    /// Human-readable message paired with `code`.
    #[serde(default)]
    message: Option<String>,
    /// Opaque handle for fetching additional partitions and re-polling
    /// asynchronous results.
    #[serde(rename = "statementHandle", default)]
    statement_handle: Option<String>,
    /// Result schema and partition manifest. Only present on the first
    /// (synchronous) response or on the resync GET after async submission;
    /// follow-up partition fetches omit it.
    #[serde(rename = "resultSetMetaData", default)]
    result_set_metadata: Option<ResultSetMetadata>,
    /// One element per row, each a JSON array of stringified cells.
    #[serde(default)]
    data: Option<Vec<Vec<Value>>>,
}

#[derive(Debug, Deserialize)]
struct ResultSetMetadata {
    /// Per-column metadata. Required to map cell arrays back to JSON keys.
    #[serde(rename = "rowType", default)]
    row_type: Vec<ColumnMeta>,
    /// One entry per partition. The first entry covers the rows in the
    /// initial response; subsequent entries must be fetched individually.
    #[serde(rename = "partitionInfo", default)]
    partition_info: Vec<PartitionInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // `rowCount` is informational; we drain partitions until empty.
struct PartitionInfo {
    #[serde(rename = "rowCount", default)]
    row_count: u64,
}

/// A source that streams the rows of a Snowflake SQL statement via the SQL
/// REST API.
pub struct SnowflakeSource {
    config: SnowflakeSourceConfig,
    client: Client,
    /// Optional explicit endpoint override (full base URL up to but not
    /// including `/api/v2/statements`). When `None`, derived from
    /// `config.account`. Intended for wiremock tests and private-link
    /// deployments.
    endpoint_base: Option<String>,
    /// Optional shared auth provider. When set, takes precedence over inline
    /// auth; the provider yields a `Bearer` or `Token` credential mapped onto
    /// [`SnowflakeAuth::OAuth`]. Set via [`Self::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
}

impl SnowflakeSource {
    /// Create a new Snowflake source. Initialises the underlying HTTP client and
    /// does no I/O; it fails only on an invalid config (an out-of-range
    /// `batch_size`).
    pub fn new(config: SnowflakeSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            client: Client::new(),
            endpoint_base: None,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sources can share one OAuth
    /// token with single-flight refresh. Used by the CLI to resolve
    /// `auth: { ref }`, and by library callers who inject a provider directly.
    ///
    /// The provider must yield a `Bearer` or `Token` credential, which maps
    /// onto [`SnowflakeAuth::OAuth`]. Key-pair JWT cannot be supplied via a
    /// provider (JWT is minted locally from the RSA key).
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Override the base URL used to reach the SQL REST API.
    ///
    /// Pass `http://127.0.0.1:1234` to point the source at a mock server —
    /// the source will issue requests to `{base}/api/v2/statements` and
    /// `{base}/api/v2/statements/{handle}?partition=N`. Useful for tests
    /// and proxy/private-link setups.
    pub fn with_endpoint_base(mut self, base: impl Into<String>) -> Self {
        self.endpoint_base = Some(base.into());
        self
    }

    fn base_url(&self) -> String {
        match &self.endpoint_base {
            Some(b) => b.trim_end_matches('/').to_owned(),
            None => format!("https://{}.snowflakecomputing.com", self.config.account),
        }
    }

    fn statements_url(&self) -> String {
        format!("{}/api/v2/statements", self.base_url())
    }

    fn partition_url(&self, handle: &str, partition: usize) -> String {
        format!(
            "{}/api/v2/statements/{}?partition={}",
            self.base_url(),
            handle,
            partition
        )
    }

    /// Resolve the effective [`SnowflakeAuth`] for this request.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise, use the inline auth from the config.
    /// 3. If the config holds an unresolved `Reference` with no provider,
    ///    return [`FaucetError::Auth`].
    async fn resolve_auth(&self) -> Result<SnowflakeAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Build the JSON body for `POST /api/v2/statements`.
    ///
    /// Bindings are sent as 1-based positional values under the documented
    /// `bindings: {"1": {"type": "<TYPE>", "value": "..."}}` shape. The value
    /// is always a string; the `type` is inferred from the JSON value
    /// (`FIXED` for integers, `REAL` for floats, `BOOLEAN` for bools, `TEXT`
    /// for strings/arrays/objects) so a numeric/boolean bind compares against
    /// a typed column rather than being forced to TEXT (#78/#34). A JSON
    /// `null` is bound as an explicit `{"type":"TEXT","value":null}` so it
    /// keeps its positional slot rather than shifting later parameters.
    fn build_request_body(&self, bindings: &[Value]) -> Value {
        let mut body = json!({
            "statement": self.config.query,
            "timeout": self.config.statement_timeout.as_secs(),
            "database": self.config.database,
            "schema": self.config.schema,
            "warehouse": self.config.warehouse,
        });

        if let Some(role) = &self.config.role {
            body["role"] = json!(role);
        }

        if !bindings.is_empty() {
            let mut map = Map::with_capacity(bindings.len());
            for (i, v) in bindings.iter().enumerate() {
                // Bindings are 1-based positional (`?` markers). A NULL must be
                // bound as an explicit `{"type":"TEXT","value":null}` rather
                // than skipped — skipping leaves a gap that shifts every later
                // parameter onto the wrong marker and corrupts the query
                // (#78/#18).
                //
                // The Snowflake type is inferred from the JSON value so a
                // numeric or boolean bind compares correctly against a
                // NUMBER/BOOLEAN column instead of being forced to TEXT
                // (#78/#34). The value is always sent as a string (or null).
                let (ty, value) = match v {
                    Value::Null => ("TEXT", Value::Null),
                    Value::Bool(b) => ("BOOLEAN", Value::String(b.to_string())),
                    Value::Number(n) => {
                        let ty = if n.is_i64() || n.is_u64() {
                            "FIXED"
                        } else {
                            "REAL"
                        };
                        (ty, Value::String(n.to_string()))
                    }
                    Value::String(s) => ("TEXT", Value::String(s.clone())),
                    other => ("TEXT", Value::String(other.to_string())),
                };
                map.insert((i + 1).to_string(), json!({"type": ty, "value": value}));
            }
            body["bindings"] = Value::Object(map);
        }

        body
    }

    /// Resolve the final SQL statement and ordered bind values for a given
    /// parent-record context.
    ///
    /// `{key}` tokens whose key matches `context` are replaced with `?`
    /// markers (Snowflake's positional binding form) and their values are
    /// appended to the returned `Vec` after any [`config.params`](SnowflakeSourceConfig::params).
    fn resolve_query(&self, context: &HashMap<String, Value>) -> (String, Vec<Value>) {
        let mut bindings = self.config.params.clone();
        let (rewritten, context_values) = if context.is_empty() {
            (self.config.query.clone(), Vec::new())
        } else {
            substitute_context_bind_params(&self.config.query, context, bindings.len() + 1, |_| {
                "?".to_string()
            })
        };
        bindings.extend(context_values);
        (rewritten, bindings)
    }

    /// Issue the initial `POST /api/v2/statements` request for the configured
    /// query, with `{key}` context tokens resolved to positional bindings.
    async fn submit_statement(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<StatementResponse, FaucetError> {
        let (query, bindings) = self.resolve_query(context);
        self.submit_sql(query, &bindings).await
    }

    /// Issue `POST /api/v2/statements` for an arbitrary SQL statement (the
    /// configured query, or an internal catalog query for
    /// [`Source::discover`](faucet_core::Source::discover)), following the
    /// async-202 poll until the statement completes.
    async fn submit_sql(
        &self,
        statement: String,
        bindings: &[Value],
    ) -> Result<StatementResponse, FaucetError> {
        let mut body = self.build_request_body(bindings);
        body["statement"] = Value::String(statement);

        let url = self.statements_url();
        let effective = self.resolve_auth().await?;
        let auth = authorization_header(&effective, &self.config.account)?;
        let token_type = snowflake_token_type(&effective);

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", token_type)
            .json(&body)
            .send()
            .await
            .map_err(|e| FaucetError::Source(format!("Snowflake request failed: {e}")))?;

        let status = resp.status();
        let async_pending = status.as_u16() == 202;
        if !status.is_success() && !async_pending {
            let text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Source(format!(
                "Snowflake SQL API returned HTTP {status}: {text}"
            )));
        }

        let parsed: StatementResponse = resp
            .json()
            .await
            .map_err(|e| FaucetError::Source(format!("failed to parse Snowflake response: {e}")))?;

        if async_pending {
            // Statement still running on the server. Poll until it completes.
            let handle = parsed.statement_handle.clone().ok_or_else(|| {
                FaucetError::Source(
                    "Snowflake returned 202 without a statementHandle to poll".into(),
                )
            })?;
            self.poll_until_ready(&handle, &auth, token_type).await
        } else {
            check_code(&parsed)?;
            Ok(parsed)
        }
    }

    /// Poll the same handle as a partition-less GET until the response is
    /// 200 + `code: "090001"`. Used after a 202 from the initial POST.
    async fn poll_until_ready(
        &self,
        handle: &str,
        auth: &str,
        token_type: &'static str,
    ) -> Result<StatementResponse, FaucetError> {
        let url = format!("{}/api/v2/statements/{}", self.base_url(), handle);
        let poll_timeout = self.config.poll_timeout;
        let started = std::time::Instant::now();
        loop {
            let resp = self
                .client
                .get(&url)
                .header("Authorization", auth)
                .header("Accept", "application/json")
                .header("X-Snowflake-Authorization-Token-Type", token_type)
                .send()
                .await
                .map_err(|e| FaucetError::Source(format!("Snowflake poll request failed: {e}")))?;

            let status = resp.status();
            if status.as_u16() == 202 {
                // `poll_timeout == 0` disables the cap (poll forever).
                if !poll_timeout.is_zero() && started.elapsed() >= poll_timeout {
                    return Err(FaucetError::Source(format!(
                        "Snowflake statement '{handle}' did not finish within poll_timeout ({}s); still HTTP 202",
                        poll_timeout.as_secs()
                    )));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(FaucetError::Source(format!(
                    "Snowflake poll returned HTTP {status}: {text}"
                )));
            }
            let parsed: StatementResponse = resp.json().await.map_err(|e| {
                FaucetError::Source(format!("failed to parse Snowflake poll response: {e}"))
            })?;
            check_code(&parsed)?;
            return Ok(parsed);
        }
    }

    /// Fetch one additional partition's worth of rows.
    async fn fetch_partition(
        &self,
        handle: &str,
        partition: usize,
        auth: &str,
        token_type: &'static str,
    ) -> Result<Vec<Vec<Value>>, FaucetError> {
        let url = self.partition_url(handle, partition);
        let resp = self
            .client
            .get(&url)
            .header("Authorization", auth)
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", token_type)
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "Snowflake partition fetch failed (partition {partition}): {e}"
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Source(format!(
                "Snowflake partition fetch returned HTTP {status} (partition {partition}): {text}"
            )));
        }

        let parsed: StatementResponse = resp.json().await.map_err(|e| {
            FaucetError::Source(format!(
                "failed to parse Snowflake partition response (partition {partition}): {e}"
            ))
        })?;
        check_code(&parsed)?;
        Ok(parsed.data.unwrap_or_default())
    }
}

/// Catalog query behind [`Source::discover`](faucet_core::Source::discover):
/// one row per column of every base table in the configured database
/// (`INFORMATION_SCHEMA` itself excluded), with the table's `row_count`
/// estimate joined in from `information_schema.tables`. Catalog metadata
/// only — never a data scan.
const CATALOG_SQL: &str = "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, \
            c.is_nullable, t.row_count \
       FROM information_schema.columns c \
       JOIN information_schema.tables t \
         ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
      WHERE t.table_type = 'BASE TABLE' \
        AND c.table_schema <> 'INFORMATION_SCHEMA' \
      ORDER BY c.table_schema, c.table_name, c.ordinal_position";

/// One flattened `information_schema.columns` row used by `discover`:
/// (schema, table, column, data_type, is_nullable, estimated_rows).
type CatalogRow = (String, String, String, String, bool, Option<i64>);

/// In-progress per-table accumulator used while grouping catalog rows:
/// (schema, table, estimated_rows, columns).
type TableAcc = (String, String, Option<i64>, Vec<(String, Value)>);

/// Decode the catalog result's raw cell arrays into [`CatalogRow`]s using the
/// statement's own `rowType` metadata (so cells are decoded positionally, with
/// the existing type-aware [`row_to_json`] machinery — no assumption about the
/// server's identifier casing). Pure — unit-testable without a live server.
fn catalog_rows(
    data: &[Vec<Value>],
    columns: &[ColumnMeta],
) -> Result<Vec<CatalogRow>, FaucetError> {
    if columns.len() < 6 {
        return Err(FaucetError::Source(format!(
            "snowflake: catalog discovery failed: expected 6 result columns, got {}",
            columns.len()
        )));
    }
    data.iter()
        .map(|raw| {
            let rec = row_to_json(raw, columns);
            let text = |i: usize| -> Result<String, FaucetError> {
                rec[columns[i].name.as_str()]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        FaucetError::Source(format!(
                            "snowflake: catalog decode failed ({}): expected a string",
                            columns[i].name
                        ))
                    })
            };
            let is_nullable = rec[columns[4].name.as_str()]
                .as_str()
                .map(|v| v.eq_ignore_ascii_case("yes"))
                .unwrap_or(true);
            // `row_count` is a scale-0 FIXED column → decoded as a JSON
            // number; the string branch is defensive.
            let estimated_rows = match &rec[columns[5].name.as_str()] {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.trim().parse::<i64>().ok(),
                _ => None,
            };
            Ok((
                text(0)?,
                text(1)?,
                text(2)?,
                text(3)?,
                is_nullable,
                estimated_rows,
            ))
        })
        .collect()
}

/// Group flattened catalog rows (ordered by schema, table, ordinal position)
/// into one [`faucet_core::DatasetDescriptor`] per table. Pure. `quote` is the
/// dialect's identifier quoter — Snowflake uses ANSI double quotes with
/// interior quotes doubled ([`faucet_core::util::quote_ident`]).
fn descriptors_from_catalog(
    rows: Vec<CatalogRow>,
    quote: fn(&str) -> String,
) -> Vec<faucet_core::DatasetDescriptor> {
    let mut out: Vec<faucet_core::DatasetDescriptor> = Vec::new();
    let mut current: Option<TableAcc> = None;

    let flush = |cur: Option<TableAcc>, out: &mut Vec<faucet_core::DatasetDescriptor>| {
        if let Some((schema, table, est, cols)) = cur {
            let query = format!("SELECT * FROM {}.{}", quote(&schema), quote(&table));
            let mut d = faucet_core::DatasetDescriptor::new(
                format!("{schema}.{table}"),
                "table",
                json!({ "query": query }),
            )
            .with_schema(faucet_core::columns_to_schema(cols));
            if let Some(n) = est
                && n >= 0
            {
                d = d.with_estimated_rows(n as u64);
            }
            out.push(d);
        }
    };

    for (schema, table, column, data_type, is_nullable, est) in rows {
        let same = current
            .as_ref()
            .is_some_and(|(s, t, _, _)| *s == schema && *t == table);
        if !same {
            flush(current.take(), &mut out);
            current = Some((schema, table, est, Vec::new()));
        }
        let mut fragment = faucet_core::sql_type_to_json_schema(&data_type);
        if is_nullable {
            fragment = faucet_core::nullable_type(fragment);
        }
        if let Some((_, _, _, cols)) = current.as_mut() {
            cols.push((column, fragment));
        }
    }
    flush(current, &mut out);
    out
}

/// Validate `code: "090001"` (statement executed successfully). Any other
/// code surfaces as `FaucetError::Source` carrying Snowflake's message.
fn check_code(resp: &StatementResponse) -> Result<(), FaucetError> {
    if let Some(code) = &resp.code
        && code != "090001"
    {
        return Err(FaucetError::Source(format!(
            "Snowflake error {}: {}",
            code,
            resp.message.clone().unwrap_or_default()
        )));
    }
    Ok(())
}

#[async_trait]
impl faucet_core::Source for SnowflakeSource {
    fn connector_name(&self) -> &'static str {
        "snowflake"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SnowflakeSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "snowflake://{}/{}/{}?query={}",
            self.config.account, self.config.database, self.config.schema, self.config.query
        )
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate every base table in the configured database (from
    /// `information_schema.columns` / `information_schema.tables`, excluding
    /// `INFORMATION_SCHEMA` itself), with column types mapped to JSON-Schema
    /// fragments and a row estimate from `information_schema.tables.row_count`.
    /// Runs one catalog statement through the source's ordinary SQL REST API
    /// path (async-202 poll and partition paging included) — catalog metadata
    /// only, no data scan.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let wrap = |e: FaucetError| -> FaucetError {
            FaucetError::Source(format!("snowflake: catalog discovery failed: {e}"))
        };
        let initial = self
            .submit_sql(CATALOG_SQL.to_string(), &[])
            .await
            .map_err(wrap)?;
        let columns = initial
            .result_set_metadata
            .as_ref()
            .map(|m| m.row_type.clone())
            .unwrap_or_default();
        if columns.is_empty() {
            // No metadata at all — an empty catalog (no base tables visible).
            return Ok(Vec::new());
        }

        let partition_count = initial
            .result_set_metadata
            .as_ref()
            .map(|m| m.partition_info.len())
            .unwrap_or(0);
        let mut raw = initial.data.unwrap_or_default();
        if partition_count > 1 {
            let handle = initial.statement_handle.ok_or_else(|| {
                FaucetError::Source(
                    "snowflake: catalog discovery failed: >1 partition without a statementHandle"
                        .into(),
                )
            })?;
            let effective = self.resolve_auth().await.map_err(wrap)?;
            let auth = authorization_header(&effective, &self.config.account).map_err(wrap)?;
            let token_type = snowflake_token_type(&effective);
            for i in 1..partition_count {
                raw.extend(
                    self.fetch_partition(&handle, i, &auth, token_type)
                        .await
                        .map_err(wrap)?,
                );
            }
        }

        let rows = catalog_rows(&raw, &columns)?;
        Ok(descriptors_from_catalog(
            rows,
            faucet_core::util::quote_ident,
        ))
    }

    /// Preflight probe for `faucet doctor`. Overrides the default (which pulls a
    /// page via `stream_pages` and would **execute the configured query**).
    /// Instead submits a cheap `SELECT 1` so doctor validates auth, warehouse,
    /// and connectivity without running the user's real statement.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let probe = async {
            let mut body = self.build_request_body(&[]);
            body["statement"] = Value::String("SELECT 1".to_string());
            let url = self.statements_url();
            let effective = self.resolve_auth().await.map_err(|e| {
                Probe::fail_hint(
                    "auth",
                    start.elapsed(),
                    e.to_string(),
                    "verify the Snowflake credentials / shared auth provider",
                )
            })?;
            let auth = authorization_header(&effective, &self.config.account)
                .map_err(|e| Probe::fail("auth", start.elapsed(), e.to_string()))?;
            let token_type = snowflake_token_type(&effective);
            let resp = self
                .client
                .post(&url)
                .header("Authorization", &auth)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("X-Snowflake-Authorization-Token-Type", token_type)
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    Probe::fail_hint(
                        "query",
                        start.elapsed(),
                        format!("Snowflake request failed: {e}"),
                        "verify the account endpoint is reachable",
                    )
                })?;
            let status = resp.status();
            if status.is_success() || status.as_u16() == 202 {
                Ok::<Probe, Probe>(Probe::pass("query", start.elapsed()))
            } else {
                let text = resp.text().await.unwrap_or_default();
                Err(Probe::fail_hint(
                    "query",
                    start.elapsed(),
                    format!("Snowflake SQL API returned HTTP {status}: {text}"),
                    "verify credentials, warehouse, database/schema, and role",
                ))
            }
        };
        let probe = match tokio::time::timeout(ctx.timeout, probe).await {
            Ok(Ok(p)) | Ok(Err(p)) => p,
            Err(_elapsed) => Probe::fail_hint(
                "query",
                start.elapsed(),
                "Snowflake probe timed out",
                "Snowflake did not respond within the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let initial = self.submit_statement(context).await?;
        let columns = initial
            .result_set_metadata
            .as_ref()
            .map(|m| m.row_type.clone())
            .unwrap_or_default();

        if columns.is_empty() {
            tracing::info!(
                rows = 0,
                query = %self.config.query,
                "Snowflake source fetch returned no schema (likely no rows)",
            );
            return Ok(Vec::new());
        }

        let mut rows: Vec<Value> = initial
            .data
            .unwrap_or_default()
            .iter()
            .map(|r| row_to_json(r, &columns))
            .collect();

        let partition_count = initial
            .result_set_metadata
            .as_ref()
            .map(|m| m.partition_info.len())
            .unwrap_or(0);

        if partition_count > 1 {
            let handle = initial.statement_handle.ok_or_else(|| {
                FaucetError::Source(
                    "Snowflake reported >1 partition without a statementHandle to fetch them"
                        .into(),
                )
            })?;
            let effective = self.resolve_auth().await?;
            let auth = authorization_header(&effective, &self.config.account)?;
            let token_type = snowflake_token_type(&effective);

            for i in 1..partition_count {
                let raw = self.fetch_partition(&handle, i, &auth, token_type).await?;
                for r in raw {
                    rows.push(row_to_json(&r, &columns));
                }
            }
        }

        tracing::info!(
            rows = rows.len(),
            query = %self.config.query,
            "Snowflake source fetch complete",
        );
        Ok(rows)
    }

    /// Stream rows partition-by-partition without buffering the full result
    /// set. Rows are accumulated into a per-page buffer of
    /// [`SnowflakeSourceConfig::batch_size`] entries and yielded as soon as
    /// the buffer is full.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains every partition and emits the full result
    /// set as a single page. The source has no incremental-replication mode
    /// today, so every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let initial = self.submit_statement(context).await?;
            let columns = initial
                .result_set_metadata
                .as_ref()
                .map(|m| m.row_type.clone())
                .unwrap_or_default();

            if columns.is_empty() {
                // Either the query returned no schema or an empty result set
                // with no metadata. Either way nothing to emit.
                return;
            }

            let partition_count = initial
                .result_set_metadata
                .as_ref()
                .map(|m| m.partition_info.len())
                .unwrap_or(1);

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 {
                // Sum the row counts across partitions for a tight pre-allocation.
                initial
                    .result_set_metadata
                    .as_ref()
                    .map(|m| m.partition_info.iter().map(|p| p.row_count as usize).sum())
                    .unwrap_or(1024)
            } else {
                batch_size
            };

            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            // First partition: rows come back in the POST response itself.
            for raw in initial.data.unwrap_or_default() {
                buffer.push(row_to_json(&raw, &columns));
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }

            // Remaining partitions: one GET each.
            if partition_count > 1 {
                let handle = initial.statement_handle.ok_or_else(|| {
                    FaucetError::Source(
                        "Snowflake reported >1 partition without a statementHandle".into(),
                    )
                })?;
                let effective = self.resolve_auth().await?;
                let auth = authorization_header(&effective, &self.config.account)?;
                let token_type = snowflake_token_type(&effective);

                // Ordered look-ahead (#621): partitions are fetched up to
                // `partition_concurrency` at a time but consumed **in order**,
                // so the emitted row order is unchanged and a failure is still
                // attributed to the partition that caused it. Unordered would
                // be marginally faster and would silently reorder a result set
                // the user wrote an ORDER BY for.
                let concurrency = self.config.partition_concurrency.max(1);
                use futures::StreamExt as _;
                let mut fetches = futures::stream::iter(1..partition_count)
                    .map(|i| {
                        let handle = handle.clone();
                        let auth = auth.clone();
                        async move { self.fetch_partition(&handle, i, &auth, token_type).await }
                    })
                    .buffered(concurrency);
                while let Some(raw) = fetches.next().await {
                    let raw = raw?;
                    for r in &raw {
                        buffer.push(row_to_json(r, &columns));
                        if buffer.len() >= chunk {
                            let page = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(initial_capacity),
                            );
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                query = %self.config.query,
                "Snowflake source stream complete",
            );
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SnowflakeAuth;

    fn cfg() -> SnowflakeSourceConfig {
        SnowflakeSourceConfig::new(
            "xy12345.us-east-1",
            "WH",
            "DB",
            "PUBLIC",
            SnowflakeAuth::OAuth { token: "t".into() },
            "SELECT 1",
        )
    }

    #[test]
    fn new_rejects_out_of_range_batch_size() {
        let mut config = cfg();
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match SnowflakeSource::new(config) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            _ => panic!("expected a batch_size Config error"),
        }
    }

    #[test]
    fn statements_url_uses_account_when_no_override() {
        let src = SnowflakeSource::new(cfg()).unwrap();
        assert_eq!(
            src.statements_url(),
            "https://xy12345.us-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[test]
    fn statements_url_uses_endpoint_override() {
        let src = SnowflakeSource::new(cfg())
            .unwrap()
            .with_endpoint_base("http://127.0.0.1:9999");
        assert_eq!(
            src.statements_url(),
            "http://127.0.0.1:9999/api/v2/statements"
        );
    }

    #[test]
    fn partition_url_includes_handle_and_index() {
        let src = SnowflakeSource::new(cfg())
            .unwrap()
            .with_endpoint_base("http://srv");
        assert_eq!(
            src.partition_url("abc-123", 2),
            "http://srv/api/v2/statements/abc-123?partition=2"
        );
    }

    #[test]
    fn build_request_body_minimal() {
        let src = SnowflakeSource::new(cfg()).unwrap();
        let body = src.build_request_body(&[]);
        assert_eq!(body["statement"], "SELECT 1");
        assert_eq!(body["timeout"], 60);
        assert_eq!(body["database"], "DB");
        assert_eq!(body["schema"], "PUBLIC");
        assert_eq!(body["warehouse"], "WH");
        assert!(body.get("bindings").is_none());
        assert!(body.get("role").is_none());
    }

    #[test]
    fn build_request_body_includes_role_when_set() {
        let mut c = cfg();
        c.role = Some("ANALYST".into());
        let src = SnowflakeSource::new(c).unwrap();
        let body = src.build_request_body(&[]);
        assert_eq!(body["role"], "ANALYST");
    }

    #[test]
    fn build_request_body_infers_binding_types() {
        // #78/#34: types are inferred from the JSON value (value still a
        // string), so numeric/boolean binds compare against typed columns.
        let src = SnowflakeSource::new(cfg()).unwrap();
        let body = src.build_request_body(&[
            Value::String("alice".into()),
            json!(42),
            json!(true),
            json!(3.5),
        ]);
        let b = &body["bindings"];
        assert_eq!(b["1"]["type"], "TEXT");
        assert_eq!(b["1"]["value"], "alice");
        assert_eq!(b["2"]["type"], "FIXED");
        assert_eq!(b["2"]["value"], "42");
        assert_eq!(b["3"]["type"], "BOOLEAN");
        assert_eq!(b["3"]["value"], "true");
        assert_eq!(b["4"]["type"], "REAL");
        assert_eq!(b["4"]["value"], "3.5");
    }

    #[test]
    fn build_request_body_array_and_object_bindings_fall_back_to_text() {
        // The catch-all arm (`other => ("TEXT", ...)`) stringifies array/object
        // JSON values into a TEXT bind so they keep their positional slot.
        let src = SnowflakeSource::new(cfg()).unwrap();
        let body = src.build_request_body(&[json!([1, 2, 3]), json!({"k": "v"})]);
        let b = &body["bindings"];
        assert_eq!(b["1"]["type"], "TEXT");
        assert_eq!(b["1"]["value"], "[1,2,3]");
        assert_eq!(b["2"]["type"], "TEXT");
        assert_eq!(b["2"]["value"], r#"{"k":"v"}"#);
    }

    #[test]
    fn connector_name_is_snowflake() {
        use faucet_core::Source;
        let src = SnowflakeSource::new(cfg()).unwrap();
        assert_eq!(src.connector_name(), "snowflake");
    }

    #[test]
    fn config_schema_reports_required_fields() {
        use faucet_core::Source;
        let src = SnowflakeSource::new(cfg()).unwrap();
        let schema = src.config_schema();
        // The generated JSON Schema should describe the config's properties.
        assert!(schema["properties"]["account"].is_object());
        assert!(schema["properties"]["query"].is_object());
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "account"));
        assert!(required.iter().any(|v| v == "query"));
    }

    #[test]
    fn build_request_body_null_binding_preserves_positional_alignment() {
        // Regression for #78/#18: a NULL must occupy its positional slot as an
        // explicit null-valued binding, not be skipped (which would shift "42"
        // onto marker 1 and leave marker 2 unbound).
        let src = SnowflakeSource::new(cfg()).unwrap();
        let body = src.build_request_body(&[Value::Null, json!(42)]);
        let b = &body["bindings"];
        assert_eq!(b["1"]["type"], "TEXT");
        assert_eq!(
            b["1"]["value"],
            Value::Null,
            "position 1 must be a NULL binding"
        );
        assert_eq!(b["2"]["value"], "42", "position 2 must still be 42");
    }

    #[test]
    fn resolve_query_with_no_context_returns_input() {
        let src = SnowflakeSource::new(cfg().with_params(vec![json!(7)])).unwrap();
        let (q, binds) = src.resolve_query(&HashMap::new());
        assert_eq!(q, "SELECT 1");
        assert_eq!(binds, vec![json!(7)]);
    }

    #[test]
    fn resolve_query_substitutes_context_with_question_mark_markers() {
        let mut c = cfg();
        c.query = "SELECT * FROM t WHERE id = {parent.id}".into();
        let src = SnowflakeSource::new(c).unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("parent.id".to_string(), json!(7));
        let (q, binds) = src.resolve_query(&ctx);
        assert_eq!(q, "SELECT * FROM t WHERE id = ?");
        assert_eq!(binds, vec![json!(7)]);
    }

    // ── discover: pure catalog decoding + grouping ───────────────────────────

    use faucet_core::util::quote_ident;
    use serde_json::json;

    /// The six-column `rowType` metadata the catalog query produces.
    fn catalog_columns() -> Vec<ColumnMeta> {
        [
            ("TABLE_SCHEMA", "text"),
            ("TABLE_NAME", "text"),
            ("COLUMN_NAME", "text"),
            ("DATA_TYPE", "text"),
            ("IS_NULLABLE", "text"),
            ("ROW_COUNT", "fixed"),
        ]
        .into_iter()
        .map(|(name, ty)| ColumnMeta {
            name: name.into(),
            ty: ty.into(),
            scale: 0,
        })
        .collect()
    }

    fn catalog_cell(
        schema: &str,
        table: &str,
        column: &str,
        ty: &str,
        nullable: &str,
        rows: Value,
    ) -> Vec<Value> {
        vec![
            json!(schema),
            json!(table),
            json!(column),
            json!(ty),
            json!(nullable),
            rows,
        ]
    }

    #[test]
    fn catalog_rows_decodes_cells_positionally() {
        let data = vec![
            catalog_cell("PUBLIC", "ORDERS", "ID", "NUMBER", "NO", json!("120")),
            catalog_cell("PUBLIC", "ORDERS", "NOTE", "TEXT", "YES", json!("120")),
        ];
        let rows = catalog_rows(&data, &catalog_columns()).unwrap();
        assert_eq!(
            rows[0],
            (
                "PUBLIC".into(),
                "ORDERS".into(),
                "ID".into(),
                "NUMBER".into(),
                false,
                Some(120)
            )
        );
        assert!(rows[1].4, "IS_NULLABLE = YES");
    }

    #[test]
    fn catalog_rows_null_row_count_means_no_estimate() {
        let data = vec![catalog_cell(
            "PUBLIC",
            "T",
            "ID",
            "NUMBER",
            "NO",
            Value::Null,
        )];
        let rows = catalog_rows(&data, &catalog_columns()).unwrap();
        assert_eq!(rows[0].5, None);
    }

    #[test]
    fn catalog_rows_rejects_short_metadata() {
        let cols = &catalog_columns()[..3];
        match catalog_rows(&[], cols) {
            Err(FaucetError::Source(m)) => {
                assert!(m.contains("catalog discovery failed"), "got: {m}")
            }
            other => panic!("expected Source error, got: {other:?}"),
        }
    }

    #[test]
    fn catalog_rows_rejects_non_string_identifier() {
        // A null in an identifier cell is a decode error, not a silent skip.
        let data = vec![catalog_cell("PUBLIC", "T", "ID", "NUMBER", "NO", json!(1))];
        let mut bad = data.clone();
        bad[0][1] = Value::Null; // TABLE_NAME
        match catalog_rows(&bad, &catalog_columns()) {
            Err(FaucetError::Source(m)) => {
                assert!(m.contains("catalog decode failed"), "got: {m}")
            }
            other => panic!("expected Source error, got: {other:?}"),
        }
    }

    #[test]
    fn descriptors_group_catalog_rows_per_table() {
        let rows = vec![
            (
                "PUBLIC".to_string(),
                "ORDERS".to_string(),
                "ID".to_string(),
                "NUMBER".to_string(),
                false,
                Some(120i64),
            ),
            (
                "PUBLIC".to_string(),
                "ORDERS".to_string(),
                "NOTE".to_string(),
                "TEXT".to_string(),
                true,
                Some(120i64),
            ),
            (
                "SALES".to_string(),
                "ORDERS".to_string(),
                "DATA".to_string(),
                "VARIANT".to_string(),
                false,
                None,
            ),
        ];
        let ds = descriptors_from_catalog(rows, quote_ident);
        assert_eq!(ds.len(), 2, "same table name in two schemas = two datasets");

        assert_eq!(ds[0].name, "PUBLIC.ORDERS");
        assert_eq!(ds[0].kind, "table");
        assert_eq!(ds[0].estimated_rows, Some(120));
        assert_eq!(
            ds[0].config_patch["query"],
            r#"SELECT * FROM "PUBLIC"."ORDERS""#
        );
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        // NUMBER maps to "number" — Snowflake's NUMBER(38,0) is integer-ish,
        // but "number" is the acceptable conservative mapping.
        assert_eq!(schema["properties"]["ID"]["type"], "number");
        assert_eq!(
            schema["properties"]["NOTE"]["type"],
            json!(["string", "null"])
        );

        assert_eq!(ds[1].name, "SALES.ORDERS");
        assert_eq!(ds[1].estimated_rows, None);
        assert_eq!(
            ds[1].schema.as_ref().unwrap()["properties"]["DATA"]["type"],
            "object",
            "VARIANT maps to object"
        );
    }

    #[test]
    fn snowflake_catalog_types_map_to_json_types() {
        for (sf, want) in [
            ("TEXT", "string"),
            ("NUMBER", "number"),
            ("FLOAT", "number"),
            ("BOOLEAN", "boolean"),
            ("VARIANT", "object"),
            ("OBJECT", "object"),
            ("ARRAY", "array"),
            ("TIMESTAMP_NTZ", "string"),
            ("DATE", "string"),
            ("BINARY", "string"),
        ] {
            assert_eq!(
                faucet_core::sql_type_to_json_schema(sf),
                json!({ "type": want }),
                "for Snowflake type {sf:?}"
            );
        }
    }

    #[test]
    fn descriptors_quote_hostile_identifiers() {
        let rows = vec![(
            "PUBLIC".to_string(),
            "weird\"; DROP".to_string(),
            "ID".to_string(),
            "NUMBER".to_string(),
            false,
            None,
        )];
        let ds = descriptors_from_catalog(rows, quote_ident);
        let q = ds[0].config_patch["query"].as_str().unwrap();
        assert!(q.contains(r#""weird""; DROP""#), "quoted identifier: {q}");
    }

    #[test]
    fn descriptors_empty_catalog_is_empty() {
        assert!(descriptors_from_catalog(Vec::new(), quote_ident).is_empty());
    }

    #[test]
    fn source_advertises_discover() {
        use faucet_core::Source;
        let src = SnowflakeSource::new(cfg()).unwrap();
        assert!(src.supports_discover());
    }

    #[test]
    fn dataset_uri_includes_account_db_schema_and_query() {
        use faucet_core::Source;
        let src = SnowflakeSource::new(cfg()).unwrap();
        assert_eq!(
            src.dataset_uri(),
            "snowflake://xy12345.us-east-1/DB/PUBLIC?query=SELECT 1"
        );
    }
}
