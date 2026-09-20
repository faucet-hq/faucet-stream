//! Snowflake SQL REST API sink.

use crate::config::SnowflakeSinkConfig;
use crate::idempotent;
use async_trait::async_trait;
use faucet_common_snowflake::{
    SnowflakeAuth, authorization_header, credential_to_auth, snowflake_token_type,
};
use faucet_core::util::quote_ident;
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

/// A sink that writes JSON records to a Snowflake table using the
/// SQL REST API.
pub struct SnowflakeSink {
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    config: SnowflakeSinkConfig,
    client: Client,
    /// Optional explicit endpoint override. When `None`, the URL is derived
    /// from `config.account`. Used by tests to point the sink at a mock
    /// server, and useful for proxies / private-link deployments.
    endpoint: Option<String>,
    /// Optional shared auth provider. When set, takes precedence over inline
    /// auth; the provider yields a `Bearer` or `Token` credential mapped onto
    /// [`SnowflakeAuth::OAuth`]. Set via [`Self::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
    /// One-shot guard so the exactly-once watermark table's
    /// `CREATE TABLE IF NOT EXISTS` DDL runs at most once per sink instance
    /// (Snowflake DDL auto-commits, so it must be its own request, outside
    /// the data transaction). A failed attempt leaves the cell empty and is
    /// retried on the next call.
    commit_table_ready: OnceCell<()>,
    /// Lazily-resolved external-stage upload store for the Arrow columnar
    /// bulk-load path (#381). Built once from `config.bulk_load` on the first
    /// columnar write and reused for every subsequent staged file.
    #[cfg(feature = "arrow")]
    bulk_store: OnceCell<crate::bulk::BulkStore>,
}

#[derive(Deserialize)]
struct SnowflakeResponse {
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
    /// Present on an HTTP 202 (asynchronous execution) response — the
    /// opaque handle used to poll the statement to completion.
    #[serde(rename = "statementHandle", default)]
    statement_handle: Option<String>,
    /// Result rows for a completed query (`[["cell", …], …]`); the SQL REST
    /// API renders every cell as a JSON string (or `null`). Only consumed by
    /// [`SnowflakeSink::last_committed_token`].
    #[serde(default)]
    data: Option<Vec<Vec<Value>>>,
}

/// Map a parsed statement response onto a success/error result. Code
/// `090001` is "Statement executed successfully"; any other non-null code
/// is a Snowflake-side error.
fn check_statement_code(sf_resp: &SnowflakeResponse) -> Result<(), FaucetError> {
    if let Some(code) = &sf_resp.code
        && code != "090001"
    {
        return Err(FaucetError::Sink(format!(
            "Snowflake error {}: {}",
            code,
            sf_resp.message.clone().unwrap_or_default()
        )));
    }
    Ok(())
}

impl SnowflakeSink {
    /// Make sure the target table exists before the first write (#580).
    ///
    /// Snowflake DDL auto-commits, so the `CREATE TABLE IF NOT EXISTS` is its
    /// own request — the same rule the watermark table already follows.
    async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) || !self.config.create_table {
            // With `create_table: false` there is nothing to do here: the
            // INSERT itself reports a missing table, and what this branch
            // guarantees is that faucet does not create one unasked.
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        // A page with nothing inferable leaves the table uncreated so the next
        // page can try, rather than emitting a zero-column CREATE.
        let Some(columns) = faucet_core::plan_columns(records) else {
            return Ok(());
        };
        let sql = build_create_table_sql(
            &self.config.database,
            &self.config.schema,
            &self.config.table,
            &columns,
        );
        self.execute_sql(&sql, None).await?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Create a new Snowflake sink.
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: SnowflakeSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        // `bulk_load` (Arrow columnar COPY) only functions with the `arrow`
        // feature compiled in — reject a config that requests it otherwise so
        // the failure is loud at load time, not a silent row-path fallback.
        #[cfg(not(feature = "arrow"))]
        if config.bulk_load.is_some() {
            return Err(FaucetError::Config(
                "snowflake `bulk_load` requires a binary built with the `arrow` feature \
                 (e.g. `cargo install faucet-cli --features arrow`)"
                    .into(),
            ));
        }
        Ok(Self {
            table_ready: std::sync::atomic::AtomicBool::new(false),
            config,
            client: Client::new(),
            endpoint: None,
            auth_provider: None,
            commit_table_ready: OnceCell::new(),
            #[cfg(feature = "arrow")]
            bulk_store: OnceCell::new(),
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sinks can share one OAuth
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

    /// Override the API endpoint URL (full URL including
    /// `/api/v2/statements`). When set, this URL is used verbatim instead
    /// of the account-derived `https://{account}.snowflakecomputing.com/...`
    /// URL. Intended for tests (wiremock) and proxy / private-link setups.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Build the SQL REST API endpoint URL.
    fn api_url(&self) -> String {
        if let Some(endpoint) = &self.endpoint {
            return endpoint.clone();
        }
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.config.account
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

    /// Get the authorization header value.
    async fn auth_header(&self) -> Result<(String, &'static str), FaucetError> {
        let effective = self.resolve_auth().await?;
        let header = authorization_header(&effective, &self.config.account)?;
        let token_type = snowflake_token_type(&effective);
        Ok((header, token_type))
    }

    /// Execute a SQL statement via the REST API, optionally with positional
    /// bindings (`{"1": {"type": "TEXT", "value": ...}}`). Convenience
    /// wrapper over [`Self::execute_statement`] for callers that don't need
    /// the parsed response.
    async fn execute_sql(&self, sql: &str, bindings: Option<Value>) -> Result<(), FaucetError> {
        self.execute_statement(sql, bindings, None)
            .await
            .map(|_| ())
    }

    /// Execute a SQL statement via the REST API and return the parsed final
    /// response (after polling to completion if Snowflake answered 202).
    ///
    /// `bindings` are positional (`{"1": {"type": "TEXT", "value": ...}}`);
    /// `parameters` is the optional session-parameters object merged into the
    /// request body (used by the exactly-once path to set
    /// `MULTI_STATEMENT_COUNT` for a multi-statement transaction).
    async fn execute_statement(
        &self,
        sql: &str,
        bindings: Option<Value>,
        parameters: Option<Value>,
    ) -> Result<SnowflakeResponse, FaucetError> {
        let url = self.api_url();
        let (auth, token_type) = self.auth_header().await?;

        let mut body = json!({
            "statement": sql,
            "timeout": 60,
            "database": self.config.database,
            "schema": self.config.schema,
            "warehouse": self.config.warehouse,
        });
        if let Some(bindings) = bindings {
            body["bindings"] = bindings;
        }
        if let Some(parameters) = parameters {
            body["parameters"] = parameters;
        }

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
            .map_err(|e| FaucetError::Sink(format!("Snowflake request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "Snowflake SQL API returned HTTP {status}: {body_text}"
            )));
        }

        // HTTP 202 means Snowflake *accepted* the statement but has not yet
        // executed it. Treating that as success would report rows as written
        // before they are actually committed. Poll the returned handle until
        // the statement completes (#78/#17).
        let is_async = status.as_u16() == 202;

        let sf_resp: SnowflakeResponse = resp
            .json()
            .await
            .map_err(|e| FaucetError::Sink(format!("failed to parse Snowflake response: {e}")))?;

        if is_async {
            let handle = sf_resp.statement_handle.ok_or_else(|| {
                FaucetError::Sink(
                    "Snowflake returned HTTP 202 without a statementHandle to poll".into(),
                )
            })?;
            return self.poll_until_complete(&handle).await;
        }

        check_statement_code(&sf_resp)?;
        Ok(sf_resp)
    }

    /// Poll `GET /api/v2/statements/{handle}` until the statement finishes
    /// executing (HTTP 200 + code `090001`), bounded by `poll_timeout`.
    /// Returns the final parsed response (which carries the result `data`
    /// for a completed query).
    async fn poll_until_complete(&self, handle: &str) -> Result<SnowflakeResponse, FaucetError> {
        let url = format!("{}/{}", self.api_url(), handle);
        let poll_timeout = self.config.poll_timeout;
        let started = std::time::Instant::now();
        loop {
            // Re-resolve auth every iteration: a long-running async statement can
            // outlive a short-lived OAuth token, so we re-ask the (single-flight,
            // cached) provider for a current token rather than reusing the one
            // minted at submit time — otherwise the poll 401s mid-run after a
            // rotation (#146).
            let (auth, token_type) = self.auth_header().await?;
            let resp = self
                .client
                .get(&url)
                .header("Authorization", &auth)
                .header("Accept", "application/json")
                .header("X-Snowflake-Authorization-Token-Type", token_type)
                .send()
                .await
                .map_err(|e| FaucetError::Sink(format!("Snowflake poll request failed: {e}")))?;

            let status = resp.status();
            if status.as_u16() == 202 {
                // `poll_timeout == 0` disables the cap (poll forever).
                if !poll_timeout.is_zero() && started.elapsed() >= poll_timeout {
                    return Err(FaucetError::Sink(format!(
                        "Snowflake statement '{handle}' did not finish within poll_timeout ({}s); still HTTP 202",
                        poll_timeout.as_secs()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(FaucetError::Sink(format!(
                    "Snowflake poll returned HTTP {status}: {body_text}"
                )));
            }
            let sf_resp: SnowflakeResponse = resp.json().await.map_err(|e| {
                FaucetError::Sink(format!("failed to parse Snowflake poll response: {e}"))
            })?;
            check_statement_code(&sf_resp)?;
            return Ok(sf_resp);
        }
    }

    /// Create the exactly-once commit-token watermark table if it does not
    /// exist — at most once per sink instance.
    ///
    /// Snowflake DDL auto-commits, so the `CREATE TABLE IF NOT EXISTS` must
    /// be its own request, submitted before (never inside) the data
    /// transaction. On failure the guard cell stays empty, so the next call
    /// retries the DDL instead of proceeding against a possibly-missing
    /// table.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        self.commit_table_ready
            .get_or_try_init(|| async {
                let sql = idempotent::build_create_commit_table(
                    &self.config.database,
                    &self.config.schema,
                );
                self.execute_sql(&sql, None).await
            })
            .await
            .map(|_| ())
    }

    /// Compute the column set for an INSERT chunk as the **union of keys across
    /// all records**, in first-seen order (stable, deterministic).
    ///
    /// All rows in one INSERT share a single column list, so a key that appears
    /// only in a later record must still become a column — otherwise that
    /// record's value for it is silently dropped (data-loss bug F16, audit
    /// #264). Records missing a union column project to SQL `NULL` for that
    /// column. Every record must be a JSON object; a chunk whose records carry
    /// no fields at all is rejected.
    fn column_union(records: &[Value]) -> Result<Vec<String>, FaucetError> {
        let mut columns: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for record in records {
            let obj = record.as_object().ok_or_else(|| {
                FaucetError::Sink("Snowflake sink requires JSON object records".into())
            })?;
            for key in obj.keys() {
                if seen.insert(key.clone()) {
                    columns.push(key.clone());
                }
            }
        }
        if columns.is_empty() {
            return Err(FaucetError::Sink(
                "Snowflake sink: records have no fields to insert".into(),
            ));
        }
        Ok(columns)
    }

    /// Build an INSERT statement plus the JSON payload to bind to its single
    /// `PARSE_JSON(?)` parameter.
    ///
    /// The record array travels as one bound `TEXT` parameter to
    /// `PARSE_JSON(?)`, never interpolated into a SQL string literal:
    /// interpolation was a SQL-injection vector and corrupted any value
    /// containing an apostrophe (#78/#5). `FLATTEN` then yields one row per
    /// array element, and each record field is projected into its matching
    /// column.
    ///
    /// The projection is **per-column** — `value:"col"::string` for each key —
    /// not `SELECT *`. `SELECT *` over `FLATTEN` returns FLATTEN's fixed
    /// `SEQ, KEY, PATH, INDEX, VALUE, THIS` metadata columns, so the previous
    /// statement inserted that metadata instead of the record's fields and was
    /// non-functional for any normal table (audit #146 C2). The `::string` cast
    /// strips the VARIANT's JSON quotes and lets Snowflake coerce the scalar
    /// into the destination column's type on `INSERT` (text → number / boolean
    /// / timestamp, etc.). The column set is the **union of keys across every
    /// record in the chunk** (first-seen order), so a key present only in a
    /// later record is never silently dropped (data-loss bug F16, audit #264);
    /// a key missing from a given record projects to SQL `NULL` for that row
    /// (the FLATTEN `value:"k"` path yields `NULL` when `k` is absent).
    ///
    /// Both the column identifiers and the JSON path keys are escaped via
    /// [`quote_ident`] (double-quote doubling), so record keys cannot inject
    /// SQL. Returns `(sql, json_payload)`.
    ///
    /// Note: a record key whose target column is semi-structured (`VARIANT` /
    /// `OBJECT` / `ARRAY`) is stringified by the `::string` cast rather than
    /// stored as structured JSON; this sink maps records to scalar columns.
    fn build_insert(&self, records: &[Value]) -> Result<(String, String), FaucetError> {
        let columns = Self::column_union(records)?;

        // `quote_ident` produces a `"`-escaped quoted identifier, which is also
        // the correct (injection-safe) form for a FLATTEN path key: `value:"k"`.
        let col_list = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let projection = columns
            .iter()
            .map(|c| format!("value:{}::string", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let payload = Value::Array(records.to_vec()).to_string();
        let sql = format!(
            "INSERT INTO {}.{}.{} ({}) SELECT {} FROM TABLE(FLATTEN(input => PARSE_JSON(?)))",
            quote_ident(&self.config.database),
            quote_ident(&self.config.schema),
            quote_ident(&self.config.table),
            col_list,
            projection,
        );
        Ok((sql, payload))
    }
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
///
/// Every column is a nullable `STRING`, matching what `build_insert` actually
/// writes — it projects each value with `::string`, so a typed column would
/// reject its own writer's cast. An operator who wants typed columns defines
/// the table and sets `create_table: false`.
fn build_create_table_sql(
    database: &str,
    schema: &str,
    table: &str,
    columns: &[faucet_core::PlannedColumn],
) -> String {
    let cols = faucet_core::render_columns(columns, quote_ident, |_| "STRING");
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{}.{} ({cols})",
        quote_ident(database),
        quote_ident(schema),
        quote_ident(table)
    )
}

#[async_trait]
impl faucet_core::Sink for SnowflakeSink {
    fn connector_name(&self) -> &'static str {
        "snowflake"
    }

    /// Snowflake bulk-loads via `COPY INTO … FROM @stage` under `bulk_load`
    /// (#528). Advertise the `staging` capability.
    fn supports_staged_load(&self) -> bool {
        true
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SnowflakeSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "snowflake://{}/{}/{}?table={}",
            self.config.account, self.config.database, self.config.schema, self.config.table
        )
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Runs a single read-only `SELECT 1` through the existing SQL REST API
    /// request path (`execute_sql`), reusing the sink's
    /// configured account/warehouse/auth. This resolves the effective
    /// credential (inline or shared provider), builds the authorization
    /// header, and confirms Snowflake accepts the session — without writing
    /// any rows. Auth-resolution, network, and SQL-API errors surface as a
    /// `Fail` probe with a hint. Tokens are never placed in the reason/hint.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();

        let result = tokio::time::timeout(ctx.timeout, self.execute_sql("SELECT 1", None)).await;

        let probe = match result {
            Ok(Ok(())) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("Snowflake SELECT 1 failed: {e}"),
                "Verify the account identifier, warehouse, and credentials \
                 (OAuth token or key-pair JWT) and that the role can use the \
                 configured warehouse.",
            ),
            Err(_elapsed) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("Snowflake SELECT 1 timed out after {:?}", ctx.timeout),
                "Check network reachability to the Snowflake SQL REST API \
                 endpoint and that the warehouse can resume within the timeout.",
            ),
        };

        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records).await?;

        // `batch_size = 0` is the "no batching" sentinel: forward whatever
        // upstream handed us as a single INSERT, preserving `StreamPage`
        // framing. Otherwise re-chunk into `batch_size` slices so each
        // outbound REST request stays near Snowflake's documented sweet
        // spot (~1000 rows).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
            let (sql, payload) = self.build_insert(chunk)?;
            let bindings = json!({ "1": { "type": "TEXT", "value": payload } });
            self.execute_sql(&sql, Some(bindings)).await?;
            total += chunk.len();
        }

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, self.config.table
            ),
            rows = total,
            "Snowflake write complete"
        );
        Ok(total)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Atomically write `records` and record `token` for `scope` in one
    /// Snowflake multi-statement transaction: the sink's regular
    /// parameterized page INSERT plus a watermark `MERGE` into
    /// `_faucet_commit_token`, wrapped in `BEGIN`/`COMMIT`. Either both the
    /// rows and the token commit, or neither does — so a crash/resume skips
    /// the already-committed page (zero duplicates) and a failed page
    /// replays cleanly.
    ///
    /// The entire page is one atomic unit — **no `batch_size` re-chunking on
    /// this path** (core issues exactly one token per page; splitting the
    /// page across transactions would break the atomicity of rows + token).
    /// An empty page still advances the watermark via a commit-only
    /// `BEGIN; MERGE; COMMIT;` transaction.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;

        let (sql, bindings, count) = if records.is_empty() {
            let sql =
                idempotent::build_commit_only_statement(&self.config.database, &self.config.schema);
            let bindings = json!({
                "1": { "type": "TEXT", "value": scope },
                "2": { "type": "TEXT", "value": token },
            });
            (sql, bindings, idempotent::COMMIT_ONLY_STATEMENT_COUNT)
        } else {
            let (insert_sql, payload) = self.build_insert(records)?;
            let sql = idempotent::build_transaction_statement(
                &insert_sql,
                &self.config.database,
                &self.config.schema,
            );
            let bindings = json!({
                "1": { "type": "TEXT", "value": payload },
                "2": { "type": "TEXT", "value": scope },
                "3": { "type": "TEXT", "value": token },
            });
            (sql, bindings, idempotent::TRANSACTION_STATEMENT_COUNT)
        };

        let parameters = json!({ "MULTI_STATEMENT_COUNT": count.to_string() });
        self.execute_statement(&sql, Some(bindings), Some(parameters))
            .await?;

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, self.config.table
            ),
            rows = records.len(),
            token = %token,
            "Snowflake exactly-once page committed"
        );
        Ok(records.len())
    }

    /// Read the last durably-committed token for `scope` from the watermark
    /// table, so the pipeline can skip already-committed pages on resume.
    ///
    /// The token string is treated as **opaque** — it may carry a `#` + JSON
    /// bookmark suffix appended by core; this sink never parses or validates
    /// its format.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;

        let sql = idempotent::build_select_token(&self.config.database, &self.config.schema);
        let bindings = json!({ "1": { "type": "TEXT", "value": scope } });
        let resp = self.execute_statement(&sql, Some(bindings), None).await?;

        // A completed SELECT always carries a `data` array (empty when the
        // scope has no watermark row yet). If it is somehow absent we cannot
        // tell "no committed token" from "token present but unreadable" — and
        // a wrong `None` would replay an already-committed page, producing
        // duplicates. Fail safe instead.
        let rows = resp.data.ok_or_else(|| {
            FaucetError::Sink(
                "Snowflake watermark read returned no result data; cannot trust the token result"
                    .into(),
            )
        })?;
        match rows.first() {
            None => Ok(None),
            Some(row) => match row.first() {
                Some(Value::String(token)) => Ok(Some(token.clone())),
                other => Err(FaucetError::Sink(format!(
                    "Snowflake watermark row has an unexpected token cell: {other:?}"
                ))),
            },
        }
    }

    /// Columnar bulk-load is available only when a `bulk_load` external stage
    /// is configured (#381). Otherwise the sink participates on the row path
    /// via the default `write_batch_columnar` fallback.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config.bulk_load.is_some()
    }

    /// Write one Arrow [`RecordBatch`](arrow::array::RecordBatch) by encoding
    /// it to a self-contained Parquet file, uploading it to the external
    /// stage's backing storage, and issuing `COPY INTO … FILE_FORMAT=(TYPE=
    /// PARQUET)` over the SQL REST API. Append-only — the exactly-once
    /// watermark path stays on the `Value` route (the pipeline never selects
    /// the columnar loop when exactly-once is configured).
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let stage = self.config.bulk_load.as_ref().ok_or_else(|| {
            FaucetError::Sink(
                "Snowflake columnar write requested with no `bulk_load` stage configured".into(),
            )
        })?;

        // Parquet encode is CPU-bound — keep it off the async runtime.
        let batch_owned = batch.clone();
        let bytes = tokio::task::spawn_blocking(move || crate::bulk::encode_parquet(&batch_owned))
            .await
            .map_err(|e| FaucetError::Sink(format!("parquet encode task panicked: {e}")))??;

        // Resolve (once) the stage's object store, then upload the file.
        let store = self
            .bulk_store
            .get_or_try_init(|| async { crate::bulk::resolve_store(stage) })
            .await?;
        let file = format!("faucet-{}.parquet", uuid::Uuid::new_v4());
        crate::bulk::upload(store, &file, bytes).await?;

        // Load the staged Parquet file into the target table.
        let sql = crate::bulk::build_copy_into(
            &self.config.database,
            &self.config.schema,
            &self.config.table,
            stage,
            &file,
        );
        self.execute_sql(&sql, None).await?;

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, self.config.table
            ),
            rows = batch.num_rows(),
            file = %file,
            "Snowflake columnar bulk-load COPY complete"
        );
        Ok(batch.num_rows())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SnowflakeAuth;
    use faucet_core::Sink as _;

    #[test]
    fn dataset_uri_includes_account_db_schema_table() {
        let config = SnowflakeSinkConfig::new(
            "myacct.us-east-1",
            "wh",
            "mydb",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        assert_eq!(
            sink.dataset_uri(),
            "snowflake://myacct.us-east-1/mydb/PUBLIC?table=events"
        );
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn supports_columnar_only_with_bulk_load() {
        use crate::config::SnowflakeStageConfig;
        let base = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "PUBLIC",
            "t",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        // No bulk_load → row path only.
        let plain = SnowflakeSink::new(base.clone()).unwrap();
        assert!(!plain.supports_columnar());

        // bulk_load configured → columnar fast path advertised.
        let staged = SnowflakeSink::new(base.with_bulk_load(SnowflakeStageConfig {
            stage: "MY_STAGE".into(),
            url: "s3://bucket/prefix/".into(),
            storage_options: Default::default(),
            match_by_column_name: "CASE_INSENSITIVE".into(),
            purge: false,
        }))
        .unwrap();
        assert!(staged.supports_columnar());
    }

    #[test]
    fn new_rejects_oversized_batch_size() {
        // Regression for #78/#44.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        )
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(SnowflakeSink::new(config).is_err());
    }

    #[test]
    fn api_url_format() {
        let config = SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "tok".into(),
            },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        assert_eq!(
            sink.api_url(),
            "https://xy12345.us-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[tokio::test]
    async fn oauth_auth_header() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "my-token".into(),
            },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let (header, token_type) = sink.auth_header().await.unwrap();
        assert_eq!(header, "Snowflake Token=\"my-token\"");
        assert_eq!(token_type, "OAUTH");
    }

    #[test]
    fn api_url_honours_endpoint_override() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config)
            .unwrap()
            .with_endpoint("http://127.0.0.1:1234/api/v2/statements");
        assert_eq!(sink.api_url(), "http://127.0.0.1:1234/api/v2/statements");
    }

    #[test]
    fn build_insert_uses_quoted_identifiers() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "MY_DB",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({"id": 1})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();
        assert!(sql.contains("\"MY_DB\".\"PUBLIC\".\"events\""));
    }

    #[test]
    fn build_insert_binds_payload_instead_of_interpolating() {
        // Regression for #78/#5. The record JSON must travel as a bound TEXT
        // parameter to PARSE_JSON(?), never interpolated into a SQL string
        // literal — interpolation is a SQL-injection vector and breaks on any
        // value containing an apostrophe.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![
            serde_json::json!({"name": "O'Brien"}),
            serde_json::json!({"note": "'); DROP TABLE events;--"}),
        ];
        let (sql, payload) = sink.build_insert(&records).unwrap();

        // SQL is a parameterised placeholder — no record data, no literal.
        assert!(sql.contains("PARSE_JSON(?)"), "sql: {sql}");
        assert!(
            !sql.contains('\''),
            "sql must not embed a quoted literal: {sql}"
        );
        assert!(!sql.contains("O'Brien"));
        assert!(!sql.contains("DROP TABLE"));

        // The payload is the JSON array, carrying the apostrophe data intact.
        let parsed: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["name"], "O'Brien");
        assert_eq!(parsed[1]["note"], "'); DROP TABLE events;--");
    }

    #[test]
    fn build_insert_maps_record_fields_to_columns_not_flatten_metadata() {
        // C2 regression (audit #146): the INSERT must project each record field
        // into its named column, NOT `SELECT *` over FLATTEN — `SELECT *` over
        // FLATTEN returns the fixed SEQ/KEY/PATH/INDEX/VALUE/THIS metadata
        // columns, so the old statement inserted metadata instead of the
        // record's own fields.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({"user_id": 1, "event": "click"})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();

        // Named column list + per-column projection from the FLATTEN `value`.
        assert!(sql.contains("\"user_id\""), "sql: {sql}");
        assert!(sql.contains("\"event\""), "sql: {sql}");
        assert!(sql.contains("value:\"user_id\"::string"), "sql: {sql}");
        assert!(sql.contains("value:\"event\"::string"), "sql: {sql}");
        // Crucially, NOT a metadata-projecting `SELECT *`.
        assert!(
            !sql.contains("SELECT *"),
            "must not SELECT * over FLATTEN: {sql}"
        );
        assert!(
            sql.contains("FLATTEN(input => PARSE_JSON(?))"),
            "sql: {sql}"
        );
    }

    #[test]
    fn build_insert_escapes_record_keys_in_columns_and_paths() {
        // Record keys are user-controlled; a key containing a double quote must
        // be `"`-doubled in both the column list and the FLATTEN path so it
        // cannot break out of the identifier / path.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({"a\"b": 1})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();
        // Column identifier and path key are both escaped as "a""b".
        assert!(sql.contains("\"a\"\"b\""), "sql: {sql}");
        assert!(sql.contains("value:\"a\"\"b\"::string"), "sql: {sql}");
    }

    #[test]
    fn check_statement_code_maps_non_success_code_to_sink_error() {
        // The error branch of `check_statement_code`: any code other than
        // 090001 surfaces as a `FaucetError::Sink` carrying the code + message.
        let resp = SnowflakeResponse {
            message: Some("Object does not exist".into()),
            code: Some("002003".into()),
            statement_handle: None,
            data: None,
        };
        match check_statement_code(&resp) {
            Err(FaucetError::Sink(msg)) => {
                assert!(msg.contains("002003"), "msg: {msg}");
                assert!(msg.contains("Object does not exist"), "msg: {msg}");
            }
            other => panic!("expected a Sink error, got {other:?}"),
        }
    }

    #[test]
    fn check_statement_code_accepts_success_and_missing_code() {
        let ok = SnowflakeResponse {
            message: None,
            code: Some("090001".into()),
            statement_handle: None,
            data: None,
        };
        assert!(check_statement_code(&ok).is_ok());
        let no_code = SnowflakeResponse {
            message: None,
            code: None,
            statement_handle: None,
            data: None,
        };
        assert!(check_statement_code(&no_code).is_ok());
    }

    #[test]
    fn build_insert_rejects_non_object_record() {
        // A non-object record (here a JSON array) must surface a typed Sink
        // error rather than panicking.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!([1, 2, 3])];
        match sink.build_insert(&records) {
            Err(FaucetError::Sink(msg)) => {
                assert!(msg.contains("requires JSON object records"), "msg: {msg}")
            }
            other => panic!("expected a Sink error, got {other:?}"),
        }
    }

    #[test]
    fn config_schema_reports_required_fields() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let schema = sink.config_schema();
        assert!(schema["properties"]["account"].is_object());
        assert!(schema["properties"]["table"].is_object());
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "account"));
        assert!(required.iter().any(|v| v == "table"));
    }

    #[test]
    fn build_insert_uses_union_of_all_record_keys_not_just_first() {
        // Data-loss regression F16 (audit #264): the column set must be the
        // UNION of keys across every record in the chunk, not just the first
        // record's keys. With differing key sets the union is {a, b, c} and
        // every column must appear in both the column list and the FLATTEN
        // projection so no record's value is silently dropped.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![
            serde_json::json!({"a": 1}),
            serde_json::json!({"b": 2}),
            serde_json::json!({"a": 3, "b": 4, "c": 5}),
        ];

        // The union helper itself: first-seen order, all three columns.
        let union = SnowflakeSink::column_union(&records).unwrap();
        assert_eq!(union, vec!["a", "b", "c"]);

        let (sql, _payload) = sink.build_insert(&records).unwrap();

        // Every union column appears in the column list and the projection.
        for col in ["a", "b", "c"] {
            let quoted = format!("\"{col}\"");
            assert!(
                sql.contains(&quoted),
                "column {col} missing from column list: {sql}"
            );
            let proj = format!("value:\"{col}\"::string");
            assert!(sql.contains(&proj), "projection for {col} missing: {sql}");
        }

        // Records missing a column rely on the FLATTEN `value:"col"` path
        // returning NULL — the projection covers all three columns, so the
        // first record (only `a`) yields NULL for `b` and `c`, etc. No column
        // is dropped.
        assert_eq!(
            sql.matches("value:").count(),
            3,
            "exactly 3 projections: {sql}"
        );
    }

    #[test]
    fn column_union_collects_all_keys_without_duplicates() {
        // The union must contain every key across the chunk exactly once. The
        // absolute column ORDER is intentionally NOT asserted: it depends on
        // `serde_json::Map`'s key ordering (sorted BTreeMap vs insertion-order
        // IndexMap under the `preserve_order` feature, which feature unification
        // can toggle), and order does not affect correctness — the column list
        // and the value projection are built from the same `Vec`, so they stay
        // internally consistent regardless.
        let records = vec![
            serde_json::json!({"z": 1, "a": 2}),
            serde_json::json!({"m": 3, "z": 4}),
            serde_json::json!({"a": 5, "b": 6}),
        ];
        let mut union = SnowflakeSink::column_union(&records).unwrap();
        let len_before = union.len();
        union.sort();
        union.dedup();
        assert_eq!(union.len(), len_before, "no duplicate columns");
        assert_eq!(
            union,
            vec!["a", "b", "m", "z"],
            "every key present exactly once"
        );
    }

    #[test]
    fn build_insert_rejects_all_empty_records() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({})];
        assert!(sink.build_insert(&records).is_err());
    }
}
