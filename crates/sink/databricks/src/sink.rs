//! The Databricks SQL warehouse sink — the only module that performs I/O.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use faucet_common_databricks::{
    ErrorSide, StatementClient, StatementOptions, StatementParam, StatementRequest,
    StatementResponse, backoff_delay,
};
use faucet_core::drift::SchemaEvolution;
use faucet_core::write_mode::{KeyTuple, WritePlan, plan_writes};
use faucet_core::{FaucetError, RowOutcome, SharedAuthProvider, Sink, WriteMode};
use serde_json::{Map, Value, json};

use crate::config::{DatabricksLoadMethod, DatabricksSinkConfig, OVW_SUFFIX};
use crate::sql::{
    self, Matrix, OP_COL, Relation, TableColumn, TableRef, build_matrix, chunk_ranges,
    missing_eo_columns,
};
use crate::stage::{self, FilesApiStager, StageLocation, StagedName, Stager};

/// Fixed statement text budgeted per chunk on top of the row literals.
const STATEMENT_OVERHEAD: usize = 4096;

/// Whether an already-accepted statement may be submitted again when its
/// submit failed ambiguously (transport error): only when re-running it
/// converges on the same result.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Replay {
    Safe,
    Unsafe,
}

/// Databricks SQL warehouse sink.
pub struct DatabricksSink {
    config: DatabricksSinkConfig,
    http: reqwest::Client,
    endpoint_base: Option<String>,
    auth_provider: Option<SharedAuthProvider>,
    stage: Option<StageLocation>,
    stager_override: Option<Arc<dyn Stager>>,
    run_id: String,
    stage_seq: AtomicU64,
    columns: tokio::sync::Mutex<Option<Vec<TableColumn>>>,
    token_table_ready: AtomicBool,
}

impl DatabricksSink {
    /// Create the sink. Validates config; does no I/O.
    pub fn new(config: DatabricksSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let stage = config
            .staging
            .as_ref()
            .map(|s| StageLocation::parse(&s.location))
            .transpose()?;
        if let Some(loc) = &stage
            && !loc.is_volume()
            && !cfg!(feature = "staging")
        {
            return Err(FaucetError::Config(
                "databricks sink: cloud staging locations need the `staging` feature; \
                 use a /Volumes/… location or rebuild with it"
                    .into(),
            ));
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        Ok(Self {
            run_id: format!("{nanos:x}-{:x}", std::process::id()),
            config,
            http: reqwest::Client::new(),
            endpoint_base: None,
            auth_provider: None,
            stage,
            stager_override: None,
            stage_seq: AtomicU64::new(0),
            columns: tokio::sync::Mutex::new(None),
            token_table_ready: AtomicBool::new(false),
        })
    }

    /// Attach a shared auth provider (takes precedence over inline auth).
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Override the base URL (e.g. a mock server).
    pub fn with_endpoint_base(mut self, base: impl Into<String>) -> Self {
        self.endpoint_base = Some(base.into());
        self
    }

    /// Upload cloud-location staged files through this store instead of one
    /// built from ambient credentials.
    #[cfg(feature = "staging")]
    pub fn with_object_store(mut self, store: Arc<dyn object_store::ObjectStore>) -> Self {
        self.stager_override = Some(Arc::new(stage::ObjectStoreStager::new(store)));
        self
    }

    fn client(&self) -> StatementClient {
        let c = &self.config;
        StatementClient::new(
            self.http.clone(),
            self.endpoint_base
                .clone()
                .unwrap_or_else(|| c.workspace_url.clone()),
            c.warehouse_id.clone(),
            c.auth.clone(),
            self.auth_provider.clone(),
            StatementOptions {
                wait_timeout_secs: c.wait_timeout_secs,
                poll_interval: Duration::from_millis(c.poll_interval_ms),
                statement_timeout: (c.statement_timeout_secs > 0)
                    .then(|| Duration::from_secs(c.statement_timeout_secs)),
                max_retries: c.max_retries,
                retry_backoff: Duration::from_millis(c.retry_backoff_ms),
            },
            ErrorSide::Sink,
        )
    }

    fn final_target(&self) -> TableRef {
        TableRef::new(
            self.config.catalog.as_deref(),
            &self.config.schema,
            &self.config.table,
        )
    }

    /// Where this instance's writes land: the overwrite staging table during
    /// an overwrite run, else the target.
    fn target(&self) -> TableRef {
        let t = self.final_target();
        if self.config.write.is_overwrite() {
            let name = format!("{}{OVW_SUFFIX}", self.config.table);
            t.sibling(&name)
        } else {
            t
        }
    }

    async fn run(
        &self,
        req: StatementRequest,
        replay: Replay,
    ) -> Result<StatementResponse, FaucetError> {
        let client = self.client();
        let mut req = req;
        req.catalog = self.config.catalog.clone();
        let mut attempt = 0u32;
        loop {
            match client.execute(&req).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    let msg = e.to_string();
                    let conflict = msg.to_ascii_uppercase().contains("CONCURRENT");
                    let ambiguous = msg.contains("submit request failed");
                    let retry = conflict || (ambiguous && replay == Replay::Safe);
                    if retry && attempt < self.config.max_retries {
                        tracing::debug!(attempt, error = %msg, "databricks sink: retrying statement");
                        tokio::time::sleep(backoff_delay(
                            Duration::from_millis(self.config.retry_backoff_ms),
                            attempt,
                        ))
                        .await;
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn exec(&self, sql: String, replay: Replay) -> Result<StatementResponse, FaucetError> {
        self.run(StatementRequest::new(sql), replay).await
    }

    async fn describe(&self, table: &TableRef) -> Result<Vec<TableColumn>, FaucetError> {
        let req = StatementRequest::new(sql::describe_sql(table.catalog.as_deref()))
            .param(StatementParam::string("faucet_schema", &table.schema))
            .param(StatementParam::string("faucet_table", &table.table));
        let resp = self.run(req, Replay::Safe).await?;
        Ok(sql::columns_from_rows(resp.string_rows()))
    }

    async fn invalidate_columns(&self) {
        *self.columns.lock().await = None;
    }

    /// The destination's columns, creating the table from `records` when it
    /// is missing (and allowed). `None`: no table and nothing to infer from.
    async fn ensure_table(
        &self,
        records: &[Value],
        eo: bool,
    ) -> Result<Option<Vec<TableColumn>>, FaucetError> {
        let mut cache = self.columns.lock().await;
        if let Some(c) = cache.as_ref()
            && (!eo || missing_eo_columns(c).is_empty())
        {
            return Ok(Some(c.clone()));
        }
        let target = self.target();
        let mut cols = match cache.take() {
            Some(c) => c,
            None => self.describe(&target).await?,
        };
        if cols.is_empty() {
            if !self.config.create_table {
                return Err(faucet_core::missing_target_error(
                    "databricks sink",
                    &target.display(),
                ));
            }
            let planned = if self.config.write.dedups_by_key() {
                faucet_core::plan_keyed_columns(records, &self.config.write.key)
            } else {
                faucet_core::plan_columns(records)
            };
            let Some(planned) = planned else {
                return Ok(None);
            };
            self.exec(sql::create_table_sql(&target, &planned, eo), Replay::Safe)
                .await?;
            cols = sql::planned_table_columns(&planned, eo);
        } else if eo {
            let missing = missing_eo_columns(&cols);
            if !missing.is_empty() {
                match self
                    .exec(sql::add_columns_sql(&target, &missing), Replay::Unsafe)
                    .await
                {
                    Ok(_) => cols.extend(missing),
                    Err(e) => {
                        let now = self.describe(&target).await?;
                        if !missing_eo_columns(&now).is_empty() {
                            return Err(e);
                        }
                        cols = now;
                    }
                }
            }
        }
        *cache = Some(cols.clone());
        Ok(Some(cols))
    }

    fn use_staging(&self, bytes: usize) -> bool {
        match self.config.load_method {
            DatabricksLoadMethod::Insert => false,
            DatabricksLoadMethod::CopyInto => true,
            DatabricksLoadMethod::Auto => {
                self.stage.is_some() && bytes >= self.config.copy_threshold_bytes
            }
        }
    }

    fn stager(&self, loc: &StageLocation) -> Result<Arc<dyn Stager>, FaucetError> {
        if let Some(s) = &self.stager_override {
            return Ok(Arc::clone(s));
        }
        if loc.is_volume() {
            return Ok(Arc::new(FilesApiStager::new(self.client())));
        }
        #[cfg(feature = "staging")]
        {
            Ok(Arc::new(stage::ObjectStoreStager::new(
                stage::build_object_store(loc)?,
            )))
        }
        #[cfg(not(feature = "staging"))]
        {
            Err(FaucetError::Config(
                "databricks sink: cloud staging locations need the `staging` feature".into(),
            ))
        }
    }

    /// Upload the matrix as Parquet, run `make_sql(uri, name)`, then clean up
    /// per policy (best-effort, logged).
    async fn staged<F>(
        &self,
        m: &Matrix,
        eo: Option<(&str, u64)>,
        make_sql: F,
    ) -> Result<(), FaucetError>
    where
        F: FnOnce(&StageLocation, &StagedName) -> (String, Replay),
    {
        let loc = self
            .stage
            .as_ref()
            .expect("staging presence is validated at construction");
        let names: Vec<String> = m.columns.iter().map(|c| c.name.clone()).collect();
        let body = stage::encode_parquet(&names, &m.rows)?;
        let name = match eo {
            Some((scope, seq)) => stage::eo_name(&self.config.table, scope, seq),
            None => stage::run_name(
                &self.config.table,
                &self.run_id,
                self.stage_seq.fetch_add(1, Ordering::Relaxed),
                &body,
            ),
        };
        let stager = self.stager(loc)?;
        let path = loc.object_path(&name.rel());
        stager.put(&path, body).await?;
        let (sql, replay) = make_sql(loc, &name);
        let result = self.exec(sql, replay).await;
        let cleanup = self
            .config
            .staging
            .as_ref()
            .map(|s| s.cleanup)
            .unwrap_or_default();
        if cleanup.should_delete(result.is_ok())
            && let Err(e) = stager.delete(&path).await
        {
            tracing::warn!(path = %path, error = %e, "databricks sink: staged file cleanup failed");
        }
        result.map(|_| ())
    }

    /// Append (or overwrite-stage) a page. `eo` switches to the exactly-once
    /// form keyed on `(scope, seq)`.
    async fn write_rows(
        &self,
        records: &[Value],
        cols: &[TableColumn],
        eo: Option<(&str, u64)>,
    ) -> Result<(), FaucetError> {
        let m = build_matrix(records, cols)?;
        let target = self.target();
        if self.use_staging(m.estimated_bytes()) {
            let copy_options = self
                .config
                .staging
                .as_ref()
                .and_then(|s| s.copy_options.clone());
            return self
                .staged(&m, eo, |loc, name| match eo {
                    Some((scope, seq)) => (
                        sql::replace_where_sql(
                            &target,
                            cols,
                            &m.columns,
                            Relation::File(&loc.uri(&name.rel())),
                            scope,
                            seq,
                        ),
                        Replay::Safe,
                    ),
                    None => (
                        sql::copy_into_sql(
                            &target,
                            &m.columns,
                            &loc.uri(&name.dir),
                            &name.file,
                            copy_options.as_deref(),
                        ),
                        Replay::Safe,
                    ),
                })
                .await;
        }
        let ranges = chunk_ranges(
            &m.row_sizes(),
            self.config.batch_size,
            self.config.max_statement_bytes,
            STATEMENT_OVERHEAD,
        );
        for (i, r) in ranges.into_iter().enumerate() {
            let rel = Relation::Values(&m.rows[r]);
            let (sql, replay) = match eo {
                Some((scope, seq)) if i == 0 => (
                    sql::replace_where_sql(&target, cols, &m.columns, rel, scope, seq),
                    Replay::Safe,
                ),
                _ => (
                    sql::insert_sql(&target, &m.columns, rel, eo),
                    Replay::Unsafe,
                ),
            };
            self.exec(sql, replay).await?;
        }
        Ok(())
    }

    /// Apply an upsert/delete plan with `MERGE`.
    async fn write_keyed(
        &self,
        plan: &WritePlan,
        cols: &[TableColumn],
        eo: Option<(&str, u64)>,
    ) -> Result<(), FaucetError> {
        if plan.upserts.is_empty() && plan.deletes.is_empty() {
            return Ok(());
        }
        let mut rows: Vec<Value> = plan.upserts.clone();
        rows.extend(plan.deletes.iter().map(key_object));
        let mut m = build_matrix(&rows, cols)?;
        let mut ops = vec![Some("u".to_owned()); plan.upserts.len()];
        ops.extend(vec![Some("d".to_owned()); plan.deletes.len()]);
        let merge_cols = m.columns.clone();
        m.push_column(TableColumn::new(OP_COL, "string"), ops);
        let target = self.target();
        let key = &self.config.write.key;
        if self.use_staging(m.estimated_bytes()) {
            return self
                .staged(&m, eo, |loc, name| {
                    (
                        sql::merge_sql(
                            &target,
                            &merge_cols,
                            key,
                            Relation::File(&loc.uri(&name.rel())),
                        ),
                        Replay::Safe,
                    )
                })
                .await;
        }
        let ranges = chunk_ranges(
            &m.row_sizes(),
            self.config.batch_size,
            self.config.max_statement_bytes,
            STATEMENT_OVERHEAD,
        );
        for r in ranges {
            let sql = sql::merge_sql(&target, &merge_cols, key, Relation::Values(&m.rows[r]));
            self.exec(sql, Replay::Safe).await?;
        }
        Ok(())
    }

    async fn ensure_token_table(&self) -> Result<(), FaucetError> {
        if self.token_table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.exec(sql::token_table_ddl(&self.final_target()), Replay::Safe)
            .await?;
        self.token_table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn put_token(&self, scope: &str, token: &str) -> Result<(), FaucetError> {
        let req = StatementRequest::new(sql::token_merge_sql(&self.final_target()))
            .param(StatementParam::string("scope", scope))
            .param(StatementParam::string("token", token));
        self.run(req, Replay::Safe).await.map(|_| ())
    }

    fn first_failure(plan: &WritePlan) -> Option<FaucetError> {
        plan.failed
            .first()
            .map(|(i, msg)| FaucetError::Sink(format!("databricks sink: record {i}: {msg}")))
    }
}

fn key_object(k: &KeyTuple) -> Value {
    let mut m = Map::new();
    for (name, v) in &k.0 {
        m.insert(name.clone(), v.clone());
    }
    Value::Object(m)
}

#[async_trait]
impl Sink for DatabricksSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let Some(cols) = self.ensure_table(records, false).await? else {
            return Ok(0);
        };
        if self.config.write.dedups_by_key() {
            let plan = plan_writes(records, &self.config.write);
            if let Some(e) = Self::first_failure(&plan) {
                return Err(e);
            }
            self.write_keyed(&plan, &cols, None).await?;
        } else {
            self.write_rows(records, &cols, None).await?;
        }
        Ok(records.len())
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if !self.config.write.dedups_by_key() {
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let plan = plan_writes(records, &self.config.write);
        if plan.failed.len() < records.len()
            && let Some(cols) = self.ensure_table(records, false).await?
        {
            self.write_keyed(&plan, &cols, None).await?;
        }
        let mut out: Vec<RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (i, msg) in &plan.failed {
            out[*i] = Err(FaucetError::Sink(format!("databricks sink: {msg}")));
        }
        Ok(out)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let seq = faucet_core::parse_token(token).ok_or_else(|| {
            FaucetError::Sink(format!(
                "databricks sink: unparseable commit token `{token}`"
            ))
        })?;
        match self.config.write.write_mode {
            WriteMode::Upsert | WriteMode::Delete => {
                if !records.is_empty() {
                    let plan = plan_writes(records, &self.config.write);
                    if let Some(e) = Self::first_failure(&plan) {
                        return Err(e);
                    }
                    if let Some(cols) = self.ensure_table(records, false).await? {
                        self.write_keyed(&plan, &cols, Some((scope, seq))).await?;
                    }
                }
            }
            WriteMode::Append => {
                if records.is_empty() {
                    let cols = self.describe(&self.target()).await?;
                    if !cols.is_empty() && missing_eo_columns(&cols).is_empty() {
                        self.exec(sql::delete_eo_sql(&self.target(), scope, seq), Replay::Safe)
                            .await?;
                    }
                } else if let Some(cols) = self.ensure_table(records, true).await? {
                    self.write_rows(records, &cols, Some((scope, seq))).await?;
                }
            }
            _ => {
                return Err(FaucetError::Config(
                    "databricks sink: write_mode: overwrite cannot be combined with \
                     delivery: exactly_once"
                        .into(),
                ));
            }
        }
        self.ensure_token_table().await?;
        self.put_token(scope, token).await?;
        Ok(records.len())
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_token_table().await?;
        let req = StatementRequest::new(sql::token_select_sql(&self.final_target()))
            .param(StatementParam::string("scope", scope));
        let resp = self.run(req, Replay::Safe).await?;
        Ok(resp
            .string_rows()
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten()))
    }

    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.ensure_token_table().await?;
        match token {
            Some(t) => self.put_token(scope, t).await,
            None => {
                let req = StatementRequest::new(sql::token_delete_sql(&self.final_target()))
                    .param(StatementParam::string("scope", scope));
                self.run(req, Replay::Safe).await.map(|_| ())
            }
        }
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ]
    }

    fn supports_staged_load(&self) -> bool {
        true
    }

    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        let cols = self.describe(&self.target()).await?;
        Ok((!cols.is_empty()).then(|| sql::schema_from_columns(&cols)))
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        let target = self.target();
        let current = self.describe(&target).await?;
        for stmt in sql::plan_evolution(&target, evolution, &current)? {
            self.exec(stmt, Replay::Unsafe).await?;
        }
        self.invalidate_columns().await;
        Ok(())
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let target = self.final_target();
        let staging = self.target();
        self.exec(sql::drop_table_sql(&staging), Replay::Safe)
            .await?;
        if self.describe(&target).await?.is_empty() {
            if !self.config.create_table {
                return Err(faucet_core::missing_target_error(
                    "databricks sink",
                    &target.display(),
                ));
            }
        } else {
            self.exec(sql::create_like_sql(&staging, &target), Replay::Unsafe)
                .await?;
        }
        self.invalidate_columns().await;
        Ok(())
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let target = self.final_target();
        let staging = self.target();
        let target_exists = !self.describe(&target).await?.is_empty();
        let staging_exists = !self.describe(&staging).await?.is_empty();
        match (target_exists, staging_exists) {
            (true, true) => {
                self.exec(sql::insert_overwrite_sql(&target, &staging), Replay::Safe)
                    .await?;
                self.exec(sql::drop_table_sql(&staging), Replay::Safe)
                    .await?;
            }
            (true, false) => {
                return Err(FaucetError::Sink(format!(
                    "databricks sink: overwrite staging table {} is missing; refusing to \
                     replace {}",
                    staging.display(),
                    target.display()
                )));
            }
            (false, true) => {
                self.exec(sql::rename_sql(&staging, &target), Replay::Unsafe)
                    .await?;
            }
            (false, false) => {}
        }
        Ok(())
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.exec(sql::drop_table_sql(&self.target()), Replay::Safe)
            .await
            .map(|_| ())
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        let auth = serde_json::to_value(&self.config.auth).ok()?;
        Some((
            "databricks".into(),
            json!({
                "workspace_url": self.config.workspace_url,
                "warehouse_id": self.config.warehouse_id,
                "auth": auth,
                "sql": format!("SELECT * FROM {}", self.final_target().sql()),
            }),
        ))
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DatabricksSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "databricks"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "databricks://{}/{}",
            self.config
                .workspace_url
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/'),
            self.final_target().display()
        )
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let warehouse = match tokio::time::timeout(
            ctx.timeout,
            self.run(StatementRequest::new("SELECT 1"), Replay::Safe),
        )
        .await
        {
            Ok(Ok(_)) => Probe::pass("warehouse", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "warehouse",
                started.elapsed(),
                e.to_string(),
                "Verify workspace_url, warehouse_id, and token permissions (CAN USE).",
            ),
            Err(_) => Probe::fail_hint(
                "warehouse",
                started.elapsed(),
                format!("timed out after {:?}", ctx.timeout),
                "A serverless warehouse may still be starting; retry or raise the timeout.",
            ),
        };
        let target = self.final_target();
        let started = std::time::Instant::now();
        let table = match tokio::time::timeout(ctx.timeout, self.describe(&target)).await {
            Ok(Ok(cols)) if !cols.is_empty() => Probe::pass("table", started.elapsed()),
            Ok(Ok(_)) if self.config.create_table => Probe::skip(
                "table",
                format!(
                    "{} does not exist yet; it will be created",
                    target.display()
                ),
            ),
            Ok(Ok(_)) => Probe::fail_hint(
                "table",
                started.elapsed(),
                format!("{} does not exist", target.display()),
                "Create the table or set `create_table: true`.",
            ),
            Ok(Err(e)) => Probe::fail("table", started.elapsed(), e.to_string()),
            Err(_) => Probe::fail(
                "table",
                started.elapsed(),
                format!("timed out after {:?}", ctx.timeout),
            ),
        };
        Ok(CheckReport {
            probes: vec![warehouse, table],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabricksAuth, DatabricksStagingConfig};

    fn cfg() -> DatabricksSinkConfig {
        DatabricksSinkConfig::new(
            "https://x.cloud.databricks.com/",
            "wh",
            "s",
            "t",
            DatabricksAuth::Pat {
                token: "tok".into(),
            },
        )
    }

    #[test]
    fn targets_and_identity() {
        let s = DatabricksSink::new(cfg()).unwrap();
        assert_eq!(s.target().sql(), "`s`.`t`");
        assert_eq!(s.dataset_uri(), "databricks://x.cloud.databricks.com/s.t");
        assert_eq!(s.connector_name(), "databricks");
        assert!(s.supports_idempotent_writes());
        assert!(s.supports_schema_evolution());
        assert!(s.supports_staged_load());
        assert!(!s.dedups_by_key());
        assert!(!s.is_overwrite());
        assert_eq!(s.supported_write_modes().len(), 4);
        assert!(
            s.config_schema()["properties"]
                .get("warehouse_id")
                .is_some()
        );
        let (kind, rb) = s.readback_source().unwrap();
        assert_eq!(kind, "databricks");
        assert_eq!(rb["sql"], json!("SELECT * FROM `s`.`t`"));

        let mut c = cfg();
        c.catalog = Some("main".into());
        c.write.write_mode = WriteMode::Overwrite;
        let s = DatabricksSink::new(c).unwrap();
        assert!(s.is_overwrite());
        assert_eq!(s.target().sql(), "`main`.`s`.`t__faucet_ovw`");
        assert_eq!(s.final_target().sql(), "`main`.`s`.`t`");
    }

    #[test]
    fn load_method_selection() {
        let mut c = cfg();
        c.copy_threshold_bytes = 100;
        let no_stage = DatabricksSink::new(c.clone()).unwrap();
        assert!(!no_stage.use_staging(1_000_000));
        c.staging = Some(DatabricksStagingConfig {
            location: "/Volumes/m/s/v".into(),
            cleanup: Default::default(),
            copy_options: None,
        });
        let auto = DatabricksSink::new(c.clone()).unwrap();
        assert!(!auto.use_staging(99));
        assert!(auto.use_staging(100));
        c.load_method = DatabricksLoadMethod::Insert;
        assert!(
            !DatabricksSink::new(c.clone())
                .unwrap()
                .use_staging(1_000_000)
        );
        c.load_method = DatabricksLoadMethod::CopyInto;
        assert!(DatabricksSink::new(c).unwrap().use_staging(0));
    }

    #[cfg(not(feature = "staging"))]
    #[test]
    fn cloud_staging_needs_feature() {
        let mut c = cfg();
        c.staging = Some(DatabricksStagingConfig {
            location: "s3://b/p".into(),
            cleanup: Default::default(),
            copy_options: None,
        });
        assert!(DatabricksSink::new(c).is_err());
    }

    #[test]
    fn key_object_and_first_failure() {
        let k = KeyTuple(vec![("id".into(), json!(1)), ("r".into(), json!("x"))]);
        assert_eq!(key_object(&k), json!({"id": 1, "r": "x"}));
        let plan = WritePlan {
            failed: vec![(2, "missing key".into())],
            ..WritePlan::default()
        };
        let e = DatabricksSink::first_failure(&plan).unwrap().to_string();
        assert!(e.contains("record 2") && e.contains("missing key"));
        assert!(DatabricksSink::first_failure(&WritePlan::default()).is_none());
    }

    #[tokio::test]
    async fn empty_plan_writes_nothing() {
        let s = DatabricksSink::new(cfg())
            .unwrap()
            .with_endpoint_base("http://127.0.0.1:1");
        s.write_keyed(&WritePlan::default(), &[], None)
            .await
            .unwrap();
    }

    #[test]
    fn rejects_invalid_config() {
        let mut c = cfg();
        c.table = String::new();
        assert!(DatabricksSink::new(c).is_err());
    }
}
