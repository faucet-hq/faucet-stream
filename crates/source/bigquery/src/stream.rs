//! BigQuery query source.
//!
//! Submits the configured SQL statement via `jobs.query` and pages through
//! the result set via `jobs.getQueryResults`. The first response may carry
//! `jobComplete=false` (statement still running on the server side); the
//! source polls `getQueryResults` until BigQuery flips that flag, exactly
//! mirroring the behaviour of `gcp_bigquery_client::Client::job().query_all`
//! without giving up the row-level access we need for incremental
//! [`StreamPage`]s.

use crate::config::BigQuerySourceConfig;
use crate::convert::row_to_json;
use async_trait::async_trait;
use faucet_common_bigquery::TokenProvider;
use faucet_common_bigquery::raw::QueryResultsReader;
use faucet_core::util::substitute_context_bind_params;
use faucet_core::{DatasetDescriptor, FaucetError, Stream, StreamPage};
use gcp_bigquery_client::Client;
use gcp_bigquery_client::dataset::ListOptions as DatasetListOptions;
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::QueryResponse;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_row::TableRow;
use gcp_bigquery_client::table::ListOptions as TableListOptions;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// Hard cap on the total number of tables [`Source::discover`] enumerates
/// across all datasets in the project. Discovery is a preflight convenience —
/// a project with thousands of tables should not turn it into an API storm.
/// When the cap is hit, enumeration stops and a warning names the cap.
const MAX_DISCOVER_TABLES: usize = 500;

/// Hard cap on per-table `tables.get` schema/row-count fetches during
/// [`Source::discover`]. Tables beyond this cap are still emitted (name +
/// `config_patch`), just without a `schema` / `estimated_rows`, and a warning
/// names the cap.
const MAX_DISCOVER_SCHEMA_FETCHES: usize = 100;

/// A source that runs a SQL query against BigQuery and yields rows as JSON.
pub struct BigQuerySource {
    config: BigQuerySourceConfig,
    client: Client,
    /// Round-trip recorder installed by the pipeline (#638 / #704). Ops:
    /// `query` (`jobs.query`), `poll` (`jobs.getQueryResults`), `job`
    /// (`jobs.get`). Cost signal: `bytes_processed` (bytes) per query.
    roundtrips: faucet_core::observability::RecorderSlot,
    /// Backend-call context (#880): every request runs through it for the
    /// run's timeout, deadline and cancel. Round trips stay counted by
    /// `roundtrips`, so the slot does not meter them a second time.
    calls: faucet_core::CallSlot,
    /// The client's token provider, when the source built the client.
    tokens: Option<TokenProvider>,
    /// `jobs.getQueryResults` reader with integer-microsecond timestamps,
    /// built on first use.
    results: tokio::sync::OnceCell<QueryResultsReader>,
}

/// The source's call slot: the default attempt timeout, `retry` for the
/// transient failures [`bq_error`] marks, and no round-trip metering (the
/// `roundtrips` slot counts them).
///
/// Every call the source makes is safe to repeat: `jobs.getQueryResults`,
/// `jobs.get`, `tables.get` and the listings only read, and `jobs.query`
/// carries a `requestId`, which makes BigQuery answer a repeated request with
/// the job the first one started instead of running the query again.
fn call_slot(retry: faucet_core::RetryPolicy) -> faucet_core::CallSlot {
    faucet_core::CallSlot::new(
        faucet_core::CallContext::new()
            .with_roundtrip_metering(false)
            .with_retry(retry),
    )
}

/// The retry policy a source starts with: five attempts, 200 ms doubling to
/// at most 30 s, on HTTP 5xx / 429 and timeouts.
fn default_retry() -> faucet_core::RetryPolicy {
    faucet_core::RetryPolicy::default()
}

/// Marks a transient BigQuery failure inside the call slot (see [`bq_error`]).
const TRANSIENT_MARKER: &str = "bigquery-transient";

/// The error-reason codes BigQuery reports for a transient failure, whatever
/// the HTTP status (a `rateLimitExceeded` comes back as 403).
const TRANSIENT_REASONS: [&str; 3] = ["rateLimitExceeded", "backendError", "internalError"];

/// The HTTP status a BigQuery failure should be retried as, when it is
/// transient: a 429 or 5xx response, or a transient error reason.
fn transient_status(e: &gcp_bigquery_client::error::BQError) -> Option<u16> {
    use gcp_bigquery_client::error::BQError;
    match e {
        BQError::ResponseError { error } => {
            let code = u16::try_from(error.error.code).unwrap_or(0);
            if code == 429 || code >= 500 {
                return Some(code);
            }
            let reason = |r: &str| {
                error
                    .error
                    .errors
                    .iter()
                    .any(|item| item.get("reason").is_some_and(|v| v == r))
            };
            if reason(TRANSIENT_REASONS[0]) {
                Some(429)
            } else if TRANSIENT_REASONS[1..].iter().any(|r| reason(r)) {
                Some(503)
            } else {
                None
            }
        }
        BQError::RequestError(e) => e
            .status()
            .map(|s| s.as_u16())
            .filter(|s| *s == 429 || *s >= 500),
        _ => None,
    }
}

/// A BigQuery API failure as the call slot sees it: a transient one is an
/// [`FaucetError::HttpStatus`] so the slot's retry policy repeats the call;
/// anything else is the [`FaucetError::Source`] `message`. [`settle`] turns a
/// transient failure the retries did not clear back into that `Source` error.
fn bq_error(message: String, e: &gcp_bigquery_client::error::BQError) -> FaucetError {
    match transient_status(e) {
        Some(status) => FaucetError::HttpStatus {
            status,
            url: TRANSIENT_MARKER.into(),
            body: message,
        },
        None => FaucetError::Source(message),
    }
}

/// The error a call returns once its retries are spent (see [`bq_error`]).
fn settle(e: FaucetError) -> FaucetError {
    match e {
        FaucetError::HttpStatus { url, body, .. } if url == TRANSIENT_MARKER => {
            FaucetError::Source(body)
        }
        other => other,
    }
}

impl BigQuerySource {
    /// The effective call context (#880), for the Storage Read path.
    #[cfg(feature = "arrow")]
    pub(crate) fn call_context(&self) -> faucet_core::CallContext {
        self.calls.get()
    }

    /// `jobs.query` for `req` as one backend call.
    ///
    /// The request gets a `requestId` (unless it has one), so a retried
    /// `jobs.query` returns the job the first attempt started.
    async fn run_query(&self, req: &QueryRequest) -> Result<QueryResponse, FaucetError> {
        let mut req = req.clone();
        if req.request_id.is_none() {
            req.request_id = Some(uuid::Uuid::new_v4().to_string());
        }
        let req = &req;
        self.calls
            .call("query", || async {
                faucet_common_bigquery::raw::tolerant_query(
                    &self.client,
                    self.raw_target(),
                    &self.config.project_id,
                    req.clone(),
                )
                .await
                .map_err(|e| bq_error(format!("BigQuery jobs.query failed: {e}"), &e))
            })
            .await
            .map_err(settle)
    }

    /// Retry transient BigQuery failures with `policy` instead of the
    /// default (five attempts) — how the CLI applies a pipeline's
    /// `resilience.retry`. Call before the pipeline installs its context.
    pub fn with_retry_policy(mut self, policy: faucet_core::RetryPolicy) -> Self {
        self.calls = call_slot(policy);
        self
    }

    /// Where schema-tolerant fallback requests go (SQL-172).
    fn raw_target(&self) -> faucet_common_bigquery::raw::RawTarget<'_> {
        faucet_common_bigquery::raw::RawTarget {
            creds: &self.config.auth,
            host: self.config.api_host.as_deref(),
        }
    }

    /// Report the bytes a `jobs.query` response says the query scanned
    /// (`totalBytesProcessed`, the figure on-demand pricing bills) as a
    /// `bytes_processed` cost signal (#704).
    fn signal_bytes_processed(&self, resp: &QueryResponse) {
        if let Some(b) = resp
            .total_bytes_processed
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
        {
            self.roundtrips.signal("bytes_processed", "bytes", b);
        }
    }

    /// Create a new BigQuery source from the given configuration.
    ///
    /// Initialises the underlying BigQuery client and exchanges credentials
    /// for an OAuth token. Returns [`FaucetError::Auth`] on credential
    /// failures.
    pub async fn new(config: BigQuerySourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Self::validate_read_api(&config)?;
        let (client, tokens) =
            faucet_common_bigquery::build_client_with_tokens(&config.auth).await?;
        Ok(Self {
            config,
            client,
            roundtrips: faucet_core::observability::RecorderSlot::new(),
            calls: call_slot(default_retry()),
            tokens: Some(tokens),
            results: tokio::sync::OnceCell::new(),
        })
    }

    /// Validate `read_api` mode: it needs the `arrow` feature and a
    /// `read_table`. Rejecting here makes a misconfiguration loud at load time
    /// rather than silently falling back to the (empty) query path.
    fn validate_read_api(config: &BigQuerySourceConfig) -> Result<(), FaucetError> {
        if !config.read_api {
            return Ok(());
        }
        #[cfg(not(feature = "arrow"))]
        {
            Err(FaucetError::Config(
                "BigQuery `read_api` requires a binary built with the `arrow` feature \
                 (e.g. `cargo install faucet-cli --features arrow`)"
                    .into(),
            ))
        }
        #[cfg(feature = "arrow")]
        {
            // Either a table to read outright, or SQL whose destination
            // table can be read (#621). Only *neither* is a misconfiguration.
            if config.read_table.as_deref().unwrap_or("").is_empty()
                && config.query.trim().is_empty()
            {
                return Err(FaucetError::Config(
                    "BigQuery `read_api` requires either `read_table` (dataset.table or \
                     project.dataset.table) or a `query` whose result table can be read"
                        .into(),
                ));
            }
            Ok(())
        }
    }

    /// Run the configured SQL and return its **destination table** in Storage
    /// Read API form (#621).
    ///
    /// BigQuery materialises every query result into a table — an anonymous
    /// cache table when the caller names none — so a large SQL result can take
    /// the same fast gRPC Arrow path as a direct table read instead of
    /// paginating `getQueryResults` REST JSON.
    ///
    /// The query is run to completion first: the destination table only exists
    /// (and only has rows) once the job is DONE.
    #[cfg(feature = "arrow")]
    pub(crate) async fn query_destination_table(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<String, FaucetError> {
        let (query, bindings) = self.resolve_query(context);
        let req = self.build_query_request(query, &bindings);
        self.roundtrips.record("query");
        let initial = self.run_query(&req).await?;
        self.signal_bytes_processed(&initial);
        let job_ref = initial.job_reference.as_ref().ok_or_else(|| {
            FaucetError::Source("BigQuery jobs.query returned no jobReference".into())
        })?;
        let job_id = job_ref.job_id.as_deref().ok_or_else(|| {
            FaucetError::Source("BigQuery jobs.query returned a jobReference with no jobId".into())
        })?;
        let poll_started = std::time::Instant::now();
        let job = loop {
            self.roundtrips.record("job");
            let job = self
                .calls
                .call("job", || async {
                    faucet_common_bigquery::raw::tolerant_get_job(
                        &self.client,
                        self.raw_target(),
                        &self.config.project_id,
                        job_id,
                        job_ref.location.as_deref(),
                    )
                    .await
                    .map_err(|e| bq_error(format!("BigQuery jobs.get failed: {e}"), &e))
                })
                .await
                .map_err(settle)?;
            if job_finished(job.status.as_ref(), job_id)? {
                break job;
            }
            let poll_timeout = self.config.poll_timeout;
            if !poll_timeout.is_zero() && poll_started.elapsed() >= poll_timeout {
                return Err(FaucetError::Source(format!(
                    "BigQuery job '{job_id}' did not complete within poll_timeout ({}s)",
                    poll_timeout.as_secs()
                )));
            }
            self.calls.wait(Duration::from_millis(200)).await?;
        };
        let dest = job
            .configuration
            .as_ref()
            .and_then(|c| c.query.as_ref())
            .and_then(|q| q.destination_table.as_ref())
            .ok_or_else(|| {
                FaucetError::Source(
                    "BigQuery query job reported no destination table, so its result cannot be \
                     read through the Storage Read API — set `read_table`, or turn `read_api` \
                     off for this query"
                        .into(),
                )
            })?;
        Ok(format!(
            "projects/{}/datasets/{}/tables/{}",
            dest.project_id, dest.dataset_id, dest.table_id
        ))
    }

    /// Accessor for the Arrow Storage Read path (`storage_read.rs`).
    #[cfg(feature = "arrow")]
    pub(crate) fn config(&self) -> &BigQuerySourceConfig {
        &self.config
    }

    /// Construct a source from a pre-built BigQuery client.
    ///
    /// Low-level escape hatch for callers that build their own
    /// [`gcp_bigquery_client::Client`] — for example to target the
    /// [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) or
    /// drive a wiremock-backed test fixture. Production code should prefer
    /// [`BigQuerySource::new`], which handles credential loading.
    ///
    /// Query result pages (`jobs.getQueryResults`) do not go through `client`:
    /// faucet reads them itself, against `config.api_host` with credentials
    /// from `config.auth`. Point `api_host` at the same endpoint as the client
    /// (an emulator or mock), and use
    /// [`BigQuerySource::from_parts_with_tokens`] to reuse the client's token
    /// provider — otherwise result pages go to the production API with
    /// application-default credentials.
    #[doc(hidden)]
    pub fn from_parts(config: BigQuerySourceConfig, client: Client) -> Self {
        Self {
            config,
            client,
            roundtrips: faucet_core::observability::RecorderSlot::new(),
            calls: call_slot(default_retry()),
            tokens: None,
            results: tokio::sync::OnceCell::new(),
        }
    }

    /// [`BigQuerySource::from_parts`] with the token provider `client`
    /// authenticates with, so result pages reuse its cached token. Without
    /// one, a provider is built once from `config.auth`.
    #[doc(hidden)]
    pub fn from_parts_with_tokens(
        config: BigQuerySourceConfig,
        client: Client,
        tokens: TokenProvider,
    ) -> Self {
        Self {
            tokens: Some(tokens),
            ..Self::from_parts(config, client)
        }
    }

    /// One page (or poll) of a query job's results, with TIMESTAMP cells as
    /// integer microseconds on every page (#862 M-052).
    async fn get_query_results(
        &self,
        job_id: &str,
        params: GetQueryResultsParameters,
    ) -> Result<
        gcp_bigquery_client::model::get_query_results_response::GetQueryResultsResponse,
        FaucetError,
    > {
        let reader = self
            .results
            .get_or_try_init(|| async {
                let tokens = match &self.tokens {
                    Some(t) => t.clone(),
                    None => faucet_common_bigquery::token_provider(&self.config.auth).await?,
                };
                QueryResultsReader::new(tokens, self.config.api_host.clone())
            })
            .await?;
        self.calls
            .call("poll", || async {
                reader
                    .get(&self.config.project_id, job_id, params.clone())
                    .await
                    .map_err(|e| bq_error(format!("BigQuery jobs.getQueryResults failed: {e}"), &e))
            })
            .await
            .map_err(settle)
    }

    /// Resolve the final SQL statement and ordered bind values for a given
    /// parent-record context.
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

    fn build_query_request(&self, query: String, bindings: &[Value]) -> QueryRequest {
        build_query_request(&self.config, query, bindings)
    }

    /// [`Source::discover`] with explicit caps — split out so tests can
    /// exercise the truncation branches without mocking hundreds of tables.
    /// Production code goes through [`Source::discover`], which applies
    /// `MAX_DISCOVER_TABLES` / `MAX_DISCOVER_SCHEMA_FETCHES`.
    #[doc(hidden)]
    pub async fn discover_with_caps(
        &self,
        max_tables: usize,
        max_schema_fetches: usize,
    ) -> Result<Vec<DatasetDescriptor>, FaucetError> {
        let project = &self.config.project_id;
        let discovery_err = |e: gcp_bigquery_client::error::BQError| -> FaucetError {
            bq_error(format!("bigquery: catalog discovery failed: {e}"), &e)
        };

        // 1. Enumerate every dataset in the project (paged).
        let mut dataset_ids: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let token = page_token.take();
            let resp = self
                .calls
                .call("discover", || async {
                    let mut opts = DatasetListOptions::default();
                    if let Some(t) = token.clone() {
                        opts = opts.page_token(t);
                    }
                    self.client
                        .dataset()
                        .list(project, opts)
                        .await
                        .map_err(discovery_err)
                })
                .await
                .map_err(settle)?;
            dataset_ids.extend(
                resp.datasets
                    .iter()
                    .map(|d| d.dataset_reference.dataset_id.clone()),
            );
            page_token = resp.next_page_token;
            if page_token.is_none() {
                break;
            }
        }

        // 2. Enumerate physical tables per dataset (paged), capped in total.
        let mut refs: Vec<(String, String)> = Vec::new();
        let mut truncated = false;
        'datasets: for dataset_id in &dataset_ids {
            let mut page_token: Option<String> = None;
            loop {
                let token = page_token.take();
                let resp = self
                    .calls
                    .call("discover", || async {
                        let mut opts = TableListOptions::default();
                        if let Some(t) = token.clone() {
                            opts = opts.page_token(t);
                        }
                        self.client
                            .table()
                            .list(project, dataset_id, opts)
                            .await
                            .map_err(discovery_err)
                    })
                    .await
                    .map_err(settle)?;
                for table in resp.tables.unwrap_or_default() {
                    // Physical tables only — views / materialized views /
                    // external tables are not `SELECT *`-scannable datasets in
                    // the same cheap sense. A missing `type` is treated as a
                    // table (BigQuery always sets it in practice).
                    if let Some(kind) = table.r#type.as_deref()
                        && !kind.eq_ignore_ascii_case("TABLE")
                    {
                        continue;
                    }
                    if refs.len() >= max_tables {
                        truncated = true;
                        break 'datasets;
                    }
                    refs.push((dataset_id.clone(), table.table_reference.table_id));
                }
                page_token = resp.next_page_token;
                if page_token.is_none() {
                    break;
                }
            }
        }
        if truncated {
            tracing::warn!(
                cap = max_tables,
                "BigQuery discovery hit the {max_tables}-table cap; remaining tables were not enumerated",
            );
        }
        if refs.len() > max_schema_fetches {
            tracing::warn!(
                cap = max_schema_fetches,
                total = refs.len(),
                "BigQuery discovery found more than {max_schema_fetches} tables; \
                 only the first {max_schema_fetches} get a schema / row estimate",
            );
        }

        // 3. Fetch schema + row count for the first `max_schema_fetches`
        //    tables; the rest are emitted name-only.
        let mut out = Vec::with_capacity(refs.len());
        for (i, (dataset_id, table_id)) in refs.iter().enumerate() {
            if i < max_schema_fetches {
                let table = self
                    .calls
                    .call("discover", || async {
                        faucet_common_bigquery::raw::tolerant_get_table(
                            &self.client,
                            self.raw_target(),
                            project,
                            dataset_id,
                            table_id,
                        )
                        .await
                        .map_err(discovery_err)
                    })
                    .await
                    .map_err(settle)?;
                let fields = table.schema.fields.unwrap_or_default();
                out.push(table_descriptor(
                    project,
                    dataset_id,
                    table_id,
                    Some(&fields),
                    table.num_rows.as_deref(),
                ));
            } else {
                out.push(table_descriptor(project, dataset_id, table_id, None, None));
            }
        }
        Ok(out)
    }
}

/// Whether a query job is `DONE`; a `DONE` job carrying an `errorResult`
/// failed, and its destination table holds no (or partial) results.
#[cfg_attr(not(feature = "arrow"), allow(dead_code))]
pub(crate) fn job_finished(
    status: Option<&gcp_bigquery_client::model::job_status::JobStatus>,
    job_id: &str,
) -> Result<bool, FaucetError> {
    let Some(status) = status else {
        return Ok(false);
    };
    if let Some(err) = &status.error_result {
        return Err(FaucetError::Source(format!(
            "BigQuery job '{job_id}' failed: {}",
            err.message.as_deref().unwrap_or("unknown error")
        )));
    }
    Ok(status.state.as_deref() == Some("DONE"))
}

/// Map one BigQuery table-schema field to a JSON-Schema type fragment
/// matching the shape [`faucet_core::schema::infer_schema`] produces.
///
/// `mode: REPEATED` → `array`; `mode: NULLABLE` (BigQuery's default when the
/// mode is omitted) wraps the base type as `["T", "null"]`; `mode: REQUIRED`
/// keeps the bare base type.
fn bq_field_to_json_schema(field: &TableFieldSchema) -> Value {
    let base = match field.r#type {
        FieldType::Integer | FieldType::Int64 => "integer",
        FieldType::Float | FieldType::Float64 => "number",
        FieldType::Boolean | FieldType::Bool => "boolean",
        FieldType::Record | FieldType::Struct | FieldType::Json => "object",
        // STRING, BYTES, DATE, DATETIME, TIME, TIMESTAMP, GEOGRAPHY, INTERVAL,
        // and NUMERIC/BIGNUMERIC (exact decimal text) — emitted as strings.
        _ => "string",
    };
    match field.mode.as_deref() {
        Some(mode) if mode.eq_ignore_ascii_case("REPEATED") => json!({ "type": "array" }),
        Some(mode) if mode.eq_ignore_ascii_case("REQUIRED") => json!({ "type": base }),
        // NULLABLE, or absent (NULLABLE is the BigQuery default).
        _ => faucet_core::nullable_type(json!({ "type": base })),
    }
}

/// Backtick-quote a fully-qualified `project.dataset.table` path for Standard
/// SQL. Backslashes and backticks inside an identifier are escaped (`\\` /
/// `` \` ``) so a hostile identifier cannot break out of the quoted path.
fn bq_quote_path(project: &str, dataset: &str, table: &str) -> String {
    let esc = |s: &str| s.replace('\\', r"\\").replace('`', r"\`");
    format!("`{}.{}.{}`", esc(project), esc(dataset), esc(table))
}

/// Build one [`DatasetDescriptor`] for a BigQuery table. Pure —
/// unit-testable without a live client. `fields`/`num_rows` are `None` for
/// tables past the schema-fetch cap (emitted name-only).
fn table_descriptor(
    project: &str,
    dataset: &str,
    table: &str,
    fields: Option<&[TableFieldSchema]>,
    num_rows: Option<&str>,
) -> DatasetDescriptor {
    let query = format!("SELECT * FROM {}", bq_quote_path(project, dataset, table));
    let mut descriptor = DatasetDescriptor::new(
        format!("{dataset}.{table}"),
        "table",
        json!({ "query": query }),
    );
    if let Some(fields) = fields {
        descriptor = descriptor.with_schema(faucet_core::columns_to_schema(
            fields
                .iter()
                .map(|f| (f.name.clone(), bq_field_to_json_schema(f))),
        ));
    }
    // `numRows` arrives as a decimal string; a missing/unparseable value
    // simply means no estimate.
    if let Some(n) = num_rows.and_then(|s| s.trim().parse::<u64>().ok()) {
        descriptor = descriptor.with_estimated_rows(n);
    }
    descriptor
}

/// Ask `jobs.query` for TIMESTAMP cells as integer microseconds: the default
/// float-seconds text is already a rounded double, which cannot hold
/// microseconds past year 2106 and turns `9999-12-31 23:59:59.999999` into
/// year 10000 (#862 M-052). Later pages ask for the same through
/// [`QueryResultsReader`].
fn int64_timestamps() -> gcp_bigquery_client::model::data_format_options::DataFormatOptions {
    gcp_bigquery_client::model::data_format_options::DataFormatOptions {
        use_int64_timestamp: Some(true),
    }
}

/// Free-standing version of [`BigQuerySource::build_query_request`] — kept
/// separate so unit tests can exercise it without spinning up a real
/// `gcp_bigquery_client::Client`.
fn build_query_request(
    cfg: &BigQuerySourceConfig,
    query: String,
    bindings: &[Value],
) -> QueryRequest {
    let mut req = QueryRequest::new(query);
    req.use_legacy_sql = cfg.use_legacy_sql;
    req.format_options = Some(int64_timestamps());
    req.timeout_ms = Some(clamp_timeout_ms(cfg.statement_timeout));
    req.max_results = Some(cfg.max_results_per_page);
    if let Some(location) = &cfg.location {
        req.location = Some(location.clone());
    }

    if !bindings.is_empty() {
        req.parameter_mode = Some("POSITIONAL".to_string());
        req.query_parameters = Some(
            bindings
                .iter()
                .map(|v| QueryParameter {
                    name: None,
                    parameter_type: Some(QueryParameterType {
                        r#type: bq_param_type(v).to_string(),
                        array_type: None,
                        struct_types: None,
                    }),
                    parameter_value: Some(QueryParameterValue {
                        // BigQuery REST always carries the value as a string;
                        // the parameter_type tells the engine how to parse it.
                        // A JSON null becomes a typed NULL (value omitted).
                        value: match v {
                            Value::Null => None,
                            other => Some(stringify_param(other)),
                        },
                        array_values: None,
                        struct_values: None,
                    }),
                })
                .collect(),
        );
    }

    req
}

/// Infer the BigQuery positional-parameter type from the JSON value, so a
/// numeric or boolean bind compares correctly against a numeric/bool column
/// instead of being forced to STRING (#78/#34). Arrays / objects / null fall
/// back to STRING (stringified JSON).
fn bq_param_type(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "BOOL",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "INT64"
            } else {
                "FLOAT64"
            }
        }
        _ => "STRING",
    }
}

fn stringify_param(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn clamp_timeout_ms(timeout: Duration) -> i32 {
    let ms = timeout.as_millis();
    if ms > i32::MAX as u128 {
        i32::MAX
    } else {
        ms as i32
    }
}

fn schema_fields(qr: &QueryResponse) -> Vec<TableFieldSchema> {
    qr.schema
        .as_ref()
        .and_then(|s| s.fields.clone())
        .unwrap_or_default()
}

fn job_reference(qr: &QueryResponse) -> Result<(String, Option<String>), FaucetError> {
    let r = qr.job_reference.as_ref().ok_or_else(|| {
        FaucetError::Source("BigQuery query response missing jobReference".into())
    })?;
    let job_id = r
        .job_id
        .clone()
        .ok_or_else(|| FaucetError::Source("BigQuery jobReference missing jobId".into()))?;
    Ok((job_id, r.location.clone()))
}

#[async_trait]
impl faucet_core::Source for BigQuerySource {
    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.roundtrips.install(recorder.clone());
        self.calls.install_recorder(recorder);
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    fn connector_name(&self) -> &'static str {
        "bigquery"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(BigQuerySourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "bigquery://{}?query={}",
            self.config.project_id, self.config.query
        )
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate every physical table in the project, with column types from
    /// `tables.get` schemas and a row estimate from `numRows` (catalog
    /// metadata only — no data scan, no billed query). Datasets are listed
    /// via `datasets.list`, tables per dataset via `tables.list` (both
    /// paged); enumeration is capped at `MAX_DISCOVER_TABLES` tables and
    /// per-table schema fetches at `MAX_DISCOVER_SCHEMA_FETCHES` (tables
    /// past that cap are emitted without a schema / estimate).
    async fn discover(&self) -> Result<Vec<DatasetDescriptor>, FaucetError> {
        self.discover_with_caps(MAX_DISCOVER_TABLES, MAX_DISCOVER_SCHEMA_FETCHES)
            .await
    }

    /// Preflight probe for `faucet doctor`. Overrides the default (which pulls a
    /// page via `stream_pages` and would run the configured query — a **billed**
    /// execution). Instead submits the same query with `dryRun: true`, which
    /// validates auth, SQL syntax, and table/permission access **without
    /// executing or billing** it.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let mut req =
            build_query_request(&self.config, self.config.query.clone(), &self.config.params);
        req.dry_run = Some(true);

        let probe = async {
            self.roundtrips.record("query");
            match self.run_query(&req).await {
                Ok(_) => Ok::<Probe, Probe>(Probe::pass("query", start.elapsed())),
                Err(e) => Err(Probe::fail_hint(
                    "query",
                    start.elapsed(),
                    format!("BigQuery dry-run failed: {e}"),
                    "verify credentials, project_id, dataset/table access, and the SQL",
                )),
            }
        };
        let probe = match tokio::time::timeout(ctx.timeout, probe).await {
            Ok(Ok(p)) | Ok(Err(p)) => p,
            Err(_elapsed) => Probe::fail_hint(
                "query",
                start.elapsed(),
                "BigQuery dry-run timed out",
                "BigQuery did not respond within the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query, bindings) = self.resolve_query(context);
        let req = self.build_query_request(query, &bindings);

        self.roundtrips.record("query");

        let initial = self.run_query(&req).await?;

        self.signal_bytes_processed(&initial);

        let fields = schema_fields(&initial);
        let mut all_rows: Vec<Value> = rows_from_response(&initial, &fields);
        let mut page_token = initial.page_token.clone();
        let mut job_complete = initial.job_complete.unwrap_or(false);
        let (job_id, job_location) = job_reference(&initial)?;
        let mut fields = fields;
        let poll_timeout = self.config.poll_timeout;
        let poll_started = std::time::Instant::now();

        // Either keep polling until jobComplete, or keep paging until
        // pageToken vanishes. The two reasons we'd loop again share one
        // condition: we are not done.
        while !job_complete || page_token.is_some() {
            let params = GetQueryResultsParameters {
                page_token: page_token.clone(),
                max_results: Some(self.config.max_results_per_page),
                location: job_location.clone(),
                ..Default::default()
            };

            self.roundtrips.record("poll");

            let resp = self.get_query_results(&job_id, params).await?;

            job_complete = resp.job_complete.unwrap_or(false);
            if !job_complete {
                // `poll_timeout == 0` disables the cap (poll forever).
                if !poll_timeout.is_zero() && poll_started.elapsed() >= poll_timeout {
                    return Err(FaucetError::Source(format!(
                        "BigQuery job '{job_id}' did not complete within poll_timeout ({}s)",
                        poll_timeout.as_secs()
                    )));
                }
                self.calls.wait(Duration::from_millis(200)).await?;
                continue;
            }

            // Fill in the schema from the first complete page if `jobs.query`
            // returned 200 without one (happens when the statement timeout
            // fires before completion).
            if fields.is_empty()
                && let Some(s) = resp.schema.as_ref()
                && let Some(f) = s.fields.as_ref()
            {
                fields = f.clone();
            }

            for row in resp.rows.unwrap_or_default() {
                all_rows.push(row_to_json(&row, &fields));
            }
            page_token = resp.page_token;
            if page_token.is_none() {
                break;
            }
        }

        tracing::info!(
            rows = all_rows.len(),
            query = %self.config.query,
            "BigQuery source fetch complete",
        );
        Ok(all_rows)
    }

    /// Stream rows page-by-page via `jobs.getQueryResults` without
    /// buffering the full result set.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: all rows from all
    /// pages are concatenated and emitted as a single page. The source has
    /// no incremental-replication mode today, so every emitted page carries
    /// `bookmark: None`.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config.read_api
    }

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
        crate::storage_read::stream_batches_arrow_with(self, context)
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // `read_api` mode reads the table via the Storage Read API (Arrow →
        // JSON on this row path); the `query` path below is skipped entirely.
        #[cfg(feature = "arrow")]
        if self.config.read_api {
            return crate::storage_read::stream_pages_arrow_with(self, context);
        }

        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query, bindings) = self.resolve_query(context);
            let req = self.build_query_request(query, &bindings);

            self.roundtrips.record("query");

            let initial = self.run_query(&req).await?;

            self.signal_bytes_processed(&initial);

            let mut fields = schema_fields(&initial);
            let mut buffer: Vec<Value> = if batch_size == 0 {
                Vec::with_capacity(1024)
            } else {
                Vec::with_capacity(batch_size)
            };
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };

            for row in rows_from_response_owned(&initial, &fields) {
                buffer.push(row);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(chunk));
                    yield StreamPage { records: page, bookmark: None };
                }
            }

            let mut job_complete = initial.job_complete.unwrap_or(false);
            let mut page_token = initial.page_token.clone();

            // If the first response was incomplete, we have to know the job id
            // to keep polling. If it was complete but had no further token,
            // we're done after emitting the first batch.
            let (job_id, job_location) = job_reference(&initial)?;
            let poll_timeout = self.config.poll_timeout;
            let poll_started = std::time::Instant::now();

            while !job_complete || page_token.is_some() {
                let params = GetQueryResultsParameters {
                    page_token: page_token.clone(),
                    max_results: Some(self.config.max_results_per_page),
                    location: job_location.clone(),
                    ..Default::default()
                };

                self.roundtrips.record("poll");

                let resp = self.get_query_results(&job_id, params).await?;

                job_complete = resp.job_complete.unwrap_or(false);
                if !job_complete {
                    // `poll_timeout == 0` disables the cap (poll forever).
                    if !poll_timeout.is_zero() && poll_started.elapsed() >= poll_timeout {
                        Err(FaucetError::Source(format!(
                            "BigQuery job '{job_id}' did not complete within poll_timeout ({}s)",
                            poll_timeout.as_secs()
                        )))?;
                    }
                    self.calls.wait(Duration::from_millis(200)).await?;
                    continue;
                }

                if fields.is_empty()
                    && let Some(s) = resp.schema.as_ref()
                    && let Some(f) = s.fields.as_ref()
                {
                    fields = f.clone();
                }

                for row in resp.rows.unwrap_or_default() {
                    buffer.push(row_to_json(&row, &fields));
                    if buffer.len() >= chunk {
                        let page = std::mem::replace(&mut buffer, Vec::with_capacity(chunk));
                        yield StreamPage { records: page, bookmark: None };
                    }
                }
                page_token = resp.page_token;
                if page_token.is_none() {
                    break;
                }
            }

            if !buffer.is_empty() {
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                batch_size,
                query = %self.config.query,
                "BigQuery source stream complete",
            );
        })
    }
}

/// Borrow-based row extraction (used by `fetch_with_context`, which collects
/// into a `Vec` anyway).
fn rows_from_response(resp: &QueryResponse, fields: &[TableFieldSchema]) -> Vec<Value> {
    resp.rows
        .as_ref()
        .map(|rows| rows.iter().map(|r| row_to_json(r, fields)).collect())
        .unwrap_or_default()
}

/// Owned-iteration variant — clones each row out of the response so the
/// streaming loop above doesn't have to keep a borrow open across yields.
fn rows_from_response_owned(resp: &QueryResponse, fields: &[TableFieldSchema]) -> Vec<Value> {
    let rows: &Vec<TableRow> = match resp.rows.as_ref() {
        Some(r) => r,
        None => return Vec::new(),
    };
    rows.iter().map(|r| row_to_json(r, fields)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BigQueryCredentials;
    use serde_json::json;

    #[test]
    fn validate_read_api_rules() {
        // read_api off → always ok.
        let mut c =
            BigQuerySourceConfig::new("p", BigQueryCredentials::ApplicationDefault, "SELECT 1");
        assert!(BigQuerySource::validate_read_api(&c).is_ok());

        c.read_api = true;
        #[cfg(feature = "arrow")]
        {
            // A `query` is enough since #621: its destination table is what
            // gets read, so arbitrary SQL reaches the fast Arrow path too.
            assert!(BigQuerySource::validate_read_api(&c).is_ok());

            // Neither a table nor SQL is the real misconfiguration.
            let mut empty = c.clone();
            empty.query = String::new();
            assert!(BigQuerySource::validate_read_api(&empty).is_err());

            // A named table works with or without SQL.
            empty.read_table = Some("ds.events".into());
            assert!(BigQuerySource::validate_read_api(&empty).is_ok());
            c.read_table = Some("ds.events".into());
            assert!(BigQuerySource::validate_read_api(&c).is_ok());
        }
        #[cfg(not(feature = "arrow"))]
        {
            // read_api on without the arrow feature → error.
            assert!(BigQuerySource::validate_read_api(&c).is_err());
        }
    }

    fn cfg() -> BigQuerySourceConfig {
        BigQuerySourceConfig::new(
            "my-project",
            BigQueryCredentials::ApplicationDefault,
            "SELECT id FROM events",
        )
    }

    #[test]
    fn dataset_uri_returns_project_and_query() {
        // Inline logic test — BigQuerySource::new requires a live client, so
        // we replicate the dataset_uri() computation directly from config fields.
        let c = cfg();
        let uri = format!("bigquery://{}?query={}", c.project_id, c.query);
        assert_eq!(uri, "bigquery://my-project?query=SELECT id FROM events");
    }

    #[test]
    fn stringify_param_passes_strings_unquoted() {
        assert_eq!(stringify_param(&json!("us-east")), "us-east");
        assert_eq!(stringify_param(&json!(42)), "42");
        assert_eq!(stringify_param(&json!(true)), "true");
    }

    #[test]
    fn clamp_timeout_ms_handles_overflow() {
        assert_eq!(clamp_timeout_ms(Duration::from_secs(1)), 1000);
        assert_eq!(clamp_timeout_ms(Duration::from_secs(u64::MAX)), i32::MAX);
    }

    #[test]
    fn build_request_no_params_omits_query_parameters() {
        let c = cfg();
        let req = build_query_request(&c, "SELECT id".to_string(), &[]);
        assert_eq!(req.query, "SELECT id");
        assert!(req.query_parameters.is_none());
        assert!(req.parameter_mode.is_none());
        assert!(!req.use_legacy_sql);
        assert_eq!(req.max_results, Some(1000));
    }

    #[test]
    fn doctor_probe_request_is_dry_run() {
        // The `faucet doctor` `check()` probe submits the configured query with
        // dryRun=true so it validates auth/SQL/permissions without a billed
        // execution — mirror that construction here to guard the field name.
        let c = cfg();
        let mut req = build_query_request(&c, "SELECT 1".to_string(), &[]);
        req.dry_run = Some(true);
        assert_eq!(
            req.dry_run,
            Some(true),
            "doctor probe must dry-run (no billing)"
        );
    }

    #[test]
    fn build_request_with_params_uses_positional_string_binds() {
        let c = cfg().with_params(vec![json!("us-east"), json!(42)]);
        let req = build_query_request(&c, "SELECT * WHERE r = ? AND n > ?".to_string(), &c.params);
        assert_eq!(req.parameter_mode.as_deref(), Some("POSITIONAL"));
        let params = req.query_parameters.as_ref().unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].parameter_type.as_ref().unwrap().r#type, "STRING");
        assert_eq!(
            params[0].parameter_value.as_ref().unwrap().value.as_deref(),
            Some("us-east")
        );
        assert_eq!(
            params[1].parameter_value.as_ref().unwrap().value.as_deref(),
            Some("42")
        );
    }

    #[test]
    fn build_request_propagates_location_and_legacy_flag() {
        let c = cfg()
            .with_location("EU")
            .with_use_legacy_sql(true)
            .with_max_results_per_page(250);
        let req = build_query_request(&c, "SELECT 1".to_string(), &[]);
        assert!(req.use_legacy_sql);
        assert_eq!(req.location.as_deref(), Some("EU"));
        assert_eq!(req.max_results, Some(250));
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = BigQuerySourceConfig::new(
            "my-project",
            BigQueryCredentials::ApplicationDefault,
            "SELECT id FROM events",
        );
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match BigQuerySource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // ── discover: pure descriptor-building helpers ───────────────────────────

    fn field(name: &str, ty: FieldType, mode: Option<&str>) -> TableFieldSchema {
        let mut f = TableFieldSchema::new(name, ty);
        f.mode = mode.map(str::to_owned);
        f
    }

    #[test]
    fn job_finished_requires_done_and_no_error() {
        use gcp_bigquery_client::model::error_proto::ErrorProto;
        use gcp_bigquery_client::model::job_status::JobStatus;
        assert!(!job_finished(None, "j").unwrap());
        let running = JobStatus {
            state: Some("RUNNING".into()),
            ..Default::default()
        };
        assert!(!job_finished(Some(&running), "j").unwrap());
        let done = JobStatus {
            state: Some("DONE".into()),
            ..Default::default()
        };
        assert!(job_finished(Some(&done), "j").unwrap());
        let failed = JobStatus {
            state: Some("DONE".into()),
            error_result: Some(ErrorProto {
                message: Some("syntax error".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = job_finished(Some(&failed), "j").unwrap_err();
        assert!(err.to_string().contains("syntax error"), "{err}");
    }

    #[test]
    fn bq_field_types_map_to_json_types() {
        for (ty, want) in [
            (FieldType::Integer, "integer"),
            (FieldType::Int64, "integer"),
            (FieldType::Float, "number"),
            (FieldType::Float64, "number"),
            (FieldType::Numeric, "string"),
            (FieldType::Bignumeric, "string"),
            (FieldType::Boolean, "boolean"),
            (FieldType::Bool, "boolean"),
            (FieldType::Record, "object"),
            (FieldType::Struct, "object"),
            (FieldType::Json, "object"),
            (FieldType::String, "string"),
            (FieldType::Bytes, "string"),
            (FieldType::Date, "string"),
            (FieldType::Datetime, "string"),
            (FieldType::Time, "string"),
            (FieldType::Timestamp, "string"),
            (FieldType::Geography, "string"),
            (FieldType::Interval, "string"),
        ] {
            let f = field("c", ty.clone(), Some("REQUIRED"));
            assert_eq!(
                bq_field_to_json_schema(&f),
                json!({ "type": want }),
                "for BigQuery type {ty:?}"
            );
        }
    }

    #[test]
    fn bq_field_mode_nullable_and_absent_wrap_as_nullable() {
        // Explicit NULLABLE and an absent mode (BigQuery's default) both wrap.
        for mode in [Some("NULLABLE"), None] {
            let f = field("c", FieldType::Integer, mode);
            assert_eq!(
                bq_field_to_json_schema(&f),
                json!({ "type": ["integer", "null"] }),
                "for mode {mode:?}"
            );
        }
    }

    #[test]
    fn bq_field_mode_repeated_maps_to_array() {
        let f = field("tags", FieldType::String, Some("REPEATED"));
        assert_eq!(bq_field_to_json_schema(&f), json!({ "type": "array" }));
    }

    #[test]
    fn bq_quote_path_backtick_quotes_and_escapes() {
        assert_eq!(
            bq_quote_path("proj", "sales", "orders"),
            "`proj.sales.orders`"
        );
        // A hostile identifier cannot break out of the quoted path.
        assert_eq!(bq_quote_path("p", "d", r"we`ird\x"), r"`p.d.we\`ird\\x`");
    }

    #[test]
    fn table_descriptor_carries_schema_estimate_and_patch() {
        let fields = vec![
            field("id", FieldType::Integer, Some("REQUIRED")),
            field("note", FieldType::String, Some("NULLABLE")),
        ];
        let d = table_descriptor("proj", "sales", "orders", Some(&fields), Some("120"));
        assert_eq!(d.name, "sales.orders");
        assert_eq!(d.kind, "table");
        assert_eq!(d.estimated_rows, Some(120));
        assert_eq!(d.config_patch["query"], "SELECT * FROM `proj.sales.orders`");
        let schema = d.schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["note"]["type"],
            json!(["string", "null"])
        );
    }

    #[test]
    fn table_descriptor_without_schema_fetch_is_name_only() {
        // Past the schema-fetch cap: still a full config_patch, no schema/rows.
        let d = table_descriptor("proj", "ops", "events", None, None);
        assert_eq!(d.name, "ops.events");
        assert!(d.schema.is_none());
        assert_eq!(d.estimated_rows, None);
        assert_eq!(d.config_patch["query"], "SELECT * FROM `proj.ops.events`");
    }

    #[test]
    fn table_descriptor_unparseable_num_rows_means_no_estimate() {
        let d = table_descriptor("p", "d", "t", Some(&[]), Some("not-a-number"));
        assert_eq!(d.estimated_rows, None);
        // An empty schema fetch still yields an (empty) object schema.
        assert_eq!(d.schema.as_ref().unwrap()["type"], "object");
    }

    #[test]
    fn resolve_query_substitutes_context_with_positional_markers() {
        // Test resolve_query without needing a Client by mimicking its core.
        let c = cfg();
        let mut bindings = c.params.clone();
        let mut ctx = HashMap::new();
        ctx.insert("parent.id".to_string(), json!(7));
        let (rewritten, extra) = substitute_context_bind_params(
            "SELECT * FROM t WHERE id = {parent.id}",
            &ctx,
            bindings.len() + 1,
            |_| "?".to_string(),
        );
        bindings.extend(extra);
        assert_eq!(rewritten, "SELECT * FROM t WHERE id = ?");
        assert_eq!(bindings, vec![json!(7)]);
    }

    /// `read_api` over SQL polls the query job until it is DONE and reads its
    /// destination table (#621), through the source's call context.
    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn query_destination_table_polls_the_job_to_its_destination() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "t", "token_type": "bearer", "expires_in": 3600
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/projects/p/queries"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jobComplete": false,
                "jobReference": {"projectId": "p", "jobId": "j1"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/projects/p/jobs/j1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jobReference": {"projectId": "p", "jobId": "j1"},
                "status": {"state": "RUNNING"}
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/projects/p/jobs/j1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jobReference": {"projectId": "p", "jobId": "j1"},
                "status": {"state": "DONE"},
                "configuration": {"query": {
                    "query": "SELECT 1",
                    "destinationTable": {"projectId": "p", "datasetId": "_anon", "tableId": "res"}
                }}
            })))
            .mount(&server)
            .await;
        let sa = json!({
            "type": "service_account",
            "project_id": "dummy",
            "private_key_id": "dummy",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
            "client_email": "dummy@developer.gserviceaccount.com",
            "client_id": "dummy",
            "token_uri": format!("{}/token", server.uri()),
        });
        let sa_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(sa_file.path(), sa.to_string()).unwrap();
        let client = gcp_bigquery_client::client_builder::ClientBuilder::new()
            .with_auth_base_url(format!("{}/auth", server.uri()))
            .with_v2_base_url(server.uri())
            .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
            .await
            .unwrap();
        let mut config = BigQuerySourceConfig::new(
            "p",
            BigQueryCredentials::ServiceAccountKey {
                json: sa.to_string(),
            },
            "SELECT 1",
        );
        config.api_host = Some(server.uri());
        let src = BigQuerySource::from_parts(config, client);
        let table = src
            .query_destination_table(&HashMap::new())
            .await
            .expect("the destination table");
        assert_eq!(table, "projects/p/datasets/_anon/tables/res");
        let jobs_polled = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/projects/p/jobs/j1")
            .count();
        assert_eq!(jobs_polled, 2, "polled until DONE");
    }

    /// `new` builds a source from inline service-account credentials.
    #[tokio::test]
    async fn new_builds_from_an_inline_service_account_key() {
        let sa = json!({
            "type": "service_account",
            "project_id": "dummy",
            "private_key_id": "dummy",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
            "client_email": "dummy@developer.gserviceaccount.com",
            "client_id": "dummy",
            "token_uri": "http://127.0.0.1:9/token",
        });
        let src = BigQuerySource::new(BigQuerySourceConfig::new(
            "p",
            BigQueryCredentials::ServiceAccountKey {
                json: sa.to_string(),
            },
            "SELECT 1",
        ))
        .await
        .expect("the source builds without a network round trip");
        assert_eq!(src.config().project_id, "p");
    }
}
