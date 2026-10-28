//! Amazon Redshift sink implementation.
//!
//! Two load paths, selected by `write_strategy`:
//! - `copy` (default) — stage each page to S3 (JSONL or CSV) and bulk-load it
//!   with `COPY … FROM 's3://…' IAM_ROLE '…'`, then best-effort delete the
//!   staged object. This is Redshift's recommended, fastest load path.
//! - `insert` — multi-row `INSERT INTO … VALUES (…), (…)`. Portable, no S3, but
//!   slower for bulk data.
//!
//! Append-only (`supported_write_modes` = `[Append]`): Redshift has no
//! `ON CONFLICT`, and `COPY` cannot upsert.

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use faucet_common_redshift::data_api::{Effect, Statement};
use faucet_common_redshift::{DataApiClient, RedshiftPool};
use faucet_core::driver::{DriverSink, PageWrite, RowPolicy, SinkDriver};
use faucet_core::{CallSlot, FaucetError};
use serde_json::Value;
use sqlx::Row;

use crate::config::{RedshiftCopyFormat, RedshiftSinkConfig, RedshiftWriteStrategy};
use crate::copy::{
    build_create_table_sql, columns_present, copy_statement, insert_statement, manifest_body,
    qualified_table_ref, s3_uri, serialize_csv, serialize_jsonl,
};

/// How long the clean-up of staged objects may take once the load is done;
/// it runs past the run's deadline so a cut load leaves nothing behind.
const STAGE_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Staged objects not yet deleted. Dropped before [`StagedObjects::delete`]
/// ran (the load future was abandoned), it deletes them on a spawned task.
struct StagedObjects {
    s3: S3Client,
    bucket: String,
    keys: Vec<String>,
}

impl StagedObjects {
    async fn delete(mut self) {
        let keys = std::mem::take(&mut self.keys);
        delete_staged(&self.s3, &self.bucket, keys).await;
    }
}

impl Drop for StagedObjects {
    fn drop(&mut self) {
        if self.keys.is_empty() {
            return;
        }
        let (s3, bucket, keys) = (
            self.s3.clone(),
            self.bucket.clone(),
            std::mem::take(&mut self.keys),
        );
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { delete_staged(&s3, &bucket, keys).await });
        }
    }
}

/// Best-effort delete of staged objects, bounded by its own timeout rather
/// than the run's call context.
async fn delete_staged(s3: &S3Client, bucket: &str, keys: Vec<String>) {
    let ctx = faucet_core::CallContext::new().with_timeout(STAGE_CLEANUP_TIMEOUT);
    for key in keys {
        let deleted = faucet_common_redshift::calls::once(&ctx, "stage", async {
            s3.delete_object()
                .bucket(bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| FaucetError::Sink(e.to_string()))
        })
        .await;
        if let Err(e) = deleted {
            tracing::warn!(key = %key, error = %e, "redshift: failed to delete staged S3 object (best-effort)");
        }
    }
}

/// A sink that loads JSON records into an Amazon Redshift table.
///
/// Built on the family-neutral sink driver (RFC 0014): `RedshiftDriver`
/// holds the load paths and [`DriverSink`] serves the plain, reported and
/// per-row writes from them.
pub struct RedshiftSink {
    inner: DriverSink<RedshiftDriver>,
}

/// The Redshift I/O primitives the sink runs on.
pub(crate) struct RedshiftDriver {
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    /// The source's declared columns (#955).
    declared: faucet_core::DeclaredSlot,
    /// The target's column names, lower-cased, loaded once.
    known_columns: tokio::sync::Mutex<Option<std::collections::HashSet<String>>>,
    /// The target's `(column, declared type)` pairs, loaded once and dropped
    /// when the sink adds a column.
    typed_columns: tokio::sync::Mutex<Option<Vec<(String, String)>>>,
    /// Records accumulated across `write_batch` calls (#617).
    ///
    /// `COPY` wants millions of rows per load; one `COPY` per 1000-row page
    /// produced many small commits, small unsorted blocks, and VACUUM
    /// pressure.
    pending: tokio::sync::Mutex<faucet_core::PageAccumulator>,
    config: RedshiftSinkConfig,
    backend: Backend,
    /// S3 client, built only for the `copy` strategy.
    s3: Option<S3Client>,
}

/// How statements reach Redshift.
enum Backend {
    /// The PostgreSQL wire protocol (`password`, `iam`).
    Wire(RedshiftPool),
    /// The Redshift Data API over HTTPS (`redshift_data_api`).
    DataApi(DataApiClient),
}

/// How many bytes of SQL and parameter text one Data API `INSERT` carries
/// at most; a longer page is split into several statements.
const DATA_API_STATEMENT_BUDGET: usize = 60_000;

impl RedshiftDriver {
    /// Run `sql` (DDL, `COPY`) as one write; `what` prefixes the error.
    async fn execute_sql(
        &self,
        calls: &CallSlot,
        op: &'static str,
        sql: &str,
        what: &str,
    ) -> Result<(), FaucetError> {
        match &self.backend {
            Backend::Wire(pool) => {
                let pool = pool.get(&calls.get()).await?;
                self.call(calls, op, async {
                    sqlx::query(sql)
                        .execute(&pool)
                        .await
                        .map(|_| ())
                        .map_err(|e| FaucetError::Sink(format!("{what}: {e}")))
                })
                .await
            }
            Backend::DataApi(api) => {
                Box::pin(api.execute(&calls.get(), Effect::Write, &Statement::new(sql)))
                    .await
                    .map(|_| ())
                    .map_err(|e| prefixed(e, what))
            }
        }
    }

    /// Run a catalog query through the Data API.
    async fn data_api_rows(
        &self,
        calls: &CallSlot,
        api: &DataApiClient,
        sql: &str,
        binds: &[Value],
    ) -> Result<Vec<serde_json::Map<String, Value>>, FaucetError> {
        let st = faucet_common_redshift::data_api::named_parameters(sql, binds);
        Ok(Box::pin(api.query(&calls.get(), &st)).await?.1)
    }

    /// Run `fut` as the single backend call `op` (#880).
    async fn call<T>(
        &self,
        calls: &CallSlot,
        op: &'static str,
        fut: impl std::future::Future<Output = Result<T, FaucetError>>,
    ) -> Result<T, FaucetError> {
        faucet_common_redshift::calls::once(&calls.get(), op, fut).await
    }

    /// Load one accumulated group, still re-chunked to `batch_size` so a
    /// single staged object / `INSERT` statement stays a reasonable size
    /// (#617).
    /// On failure, also returns how many leading rows did land.
    async fn commit_group(
        &self,
        calls: &CallSlot,
        rows: &[Value],
    ) -> Result<(), (usize, FaucetError)> {
        let chunk_len = if self.config.batch_size == 0 {
            rows.len().max(1)
        } else {
            self.config.batch_size
        };
        let mut total = 0;
        let mut done = 0;
        if self.config.write_strategy == RedshiftWriteStrategy::Copy {
            // One `COPY` per group: each `batch_size` chunk is its own staged
            // object, loaded together through a manifest.
            let chunks: Vec<&[Value]> = rows.chunks(chunk_len).collect();
            total = self.copy_chunks(calls, &chunks).await.map_err(|e| (0, e))?;
        } else {
            for chunk in rows.chunks(chunk_len) {
                total += self
                    .insert_chunk(calls, chunk)
                    .await
                    .map_err(|e| (done, e))?;
                done += chunk.len();
            }
        }
        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            strategy = self.config.write_strategy.as_str(),
            "Redshift write complete"
        );
        Ok(())
    }

    /// Commit a group the accumulator handed out; on failure put back the
    /// uncommitted rows of earlier pages (`rows[..keep_end]`), so a retried
    /// write or `flush` commits them instead of dropping them (SQL-04).
    async fn commit_or_restore(
        &self,
        calls: &CallSlot,
        rows: Vec<Value>,
        keep_end: usize,
    ) -> Result<(), FaucetError> {
        if let Err((done, e)) = self.commit_group(calls, &rows).await {
            if done < keep_end {
                let mut rows = rows;
                rows.truncate(keep_end);
                rows.drain(..done);
                self.pending.lock().await.restore(rows);
            }
            return Err(e);
        }
        Ok(())
    }

    /// Make sure the target table exists before the first write (#580).
    ///
    /// Runs once per sink instance. With `create_table: true` (the default) a
    /// missing table — and its schema, when `schema:` is set — is created from
    /// the first page's inferred columns; with `false` a missing table is a
    /// typed failure naming both ways out.
    async fn ensure_table_ready(
        &self,
        calls: &CallSlot,
        records: &[Value],
    ) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.config.create_table {
            let probe = format!(
                "SELECT 1 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE {TARGET_RELATION} LIMIT 1"
            );
            let exists = match &self.backend {
                Backend::Wire(pool) => {
                    let pool = pool.get(&calls.get()).await?;
                    let found: Option<i32> = self
                        .call(calls, "query", async {
                            sqlx::query_scalar(&probe)
                                .bind(&self.config.table_name)
                                .bind(self.config.schema.as_deref())
                                .fetch_optional(&pool)
                                .await
                                .map_err(|e| {
                                    FaucetError::Sink(format!("redshift table probe failed: {e}"))
                                })
                        })
                        .await?;
                    found.is_some()
                }
                Backend::DataApi(api) => !self
                    .data_api_rows(calls, api, &probe, &self.catalog_binds())
                    .await
                    .map_err(|e| prefixed(e, "redshift table probe failed"))?
                    .is_empty(),
            };
            if !exists {
                return Err(faucet_core::missing_target_error(
                    "redshift sink",
                    &self.table_ref(),
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        // A page with nothing inferable leaves the table uncreated so the next
        // page can try, rather than emitting a zero-column CREATE.
        let declared = self.declared.get();
        let fits = |t: &faucet_core::LogicalType, v: &Value| {
            faucet_common_sql::SqlDialect::declared_fits(
                &faucet_common_sql::dialects::Redshift,
                t,
                v,
            )
        };
        let Some(columns) =
            faucet_core::plan_columns_declared_with(records, declared.as_ref(), &fits)
        else {
            return Ok(());
        };
        if let Some(schema) = self.config.schema.as_deref() {
            let sql = format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                faucet_core::util::quote_ident(schema)
            );
            self.execute_sql(calls, "execute", &sql, "redshift CREATE SCHEMA failed")
                .await?;
        }
        let sql = build_create_table_sql(&self.table_ref(), &columns);
        self.execute_sql(calls, "execute", &sql, "redshift CREATE TABLE failed")
            .await?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Make every field of `records` a column before the page is buffered:
    /// with `create_table` a missing one is added (typed from the page),
    /// without it the write fails naming the fields. Loads drop fields with
    /// no column, so a field first seen after the table was created would
    /// otherwise be lost on every page.
    async fn ensure_columns(&self, calls: &CallSlot, records: &[Value]) -> Result<(), FaucetError> {
        let mut known = self.known_columns.lock().await;
        if known.is_none() {
            *known = Some(self.lowercased_columns(calls).await?);
        }
        let Some(cols) = known.as_mut() else {
            return Ok(());
        };
        let missing = crate::copy::missing_fields(records, cols);
        if missing.is_empty() {
            return Ok(());
        }
        if !self.config.create_table {
            return Err(FaucetError::Sink(format!(
                "redshift: {} has no column for field(s) {}; add them, drop the fields with a \
                 transform, or set `create_table: true` to let the sink add them",
                self.table_ref(),
                missing.join(", ")
            )));
        }
        let planned = faucet_core::plan_columns(records).unwrap_or_default();
        for name in &missing {
            let Some(col) = planned.iter().find(|c| &c.name == name) else {
                continue;
            };
            let sql = crate::copy::add_column_sql(&self.table_ref(), col);
            let added = self
                .execute_sql(
                    calls,
                    "execute",
                    &sql,
                    "redshift: ALTER TABLE ADD COLUMN failed",
                )
                .await;
            if let Err(e) = added {
                if matches!(
                    e,
                    FaucetError::Timeout { .. } | FaucetError::Cancelled { .. }
                ) {
                    return Err(e);
                }
                // A concurrent writer may have added it first.
                if !self
                    .lowercased_columns(calls)
                    .await?
                    .contains(&name.to_lowercase())
                {
                    return Err(e);
                }
            }
            cols.insert(name.to_lowercase());
        }
        *self.typed_columns.lock().await = None;
        Ok(())
    }

    async fn lowercased_columns(
        &self,
        calls: &CallSlot,
    ) -> Result<std::collections::HashSet<String>, FaucetError> {
        Ok(self
            .discover_typed_columns(calls)
            .await?
            .into_iter()
            .map(|(c, _)| c.to_lowercase())
            .collect())
    }

    /// Validate the config, build a lazily-connected pool (no DB I/O) and —
    /// for the `copy` strategy — an S3 client.
    async fn new(config: RedshiftSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let backend = match DataApiClient::from_connection(&config.connection, FaucetError::Sink)? {
            Some(api) => Backend::DataApi(api),
            None => Backend::Wire(faucet_common_redshift::connect_lazy(
                &config.connection,
                config.max_connections,
            )?),
        };
        let s3 = if config.write_strategy == RedshiftWriteStrategy::Copy {
            Some(Self::build_s3_client(&config).await)
        } else {
            None
        };
        Ok(Self {
            table_ready: std::sync::atomic::AtomicBool::new(false),
            declared: faucet_core::DeclaredSlot::new(),
            known_columns: tokio::sync::Mutex::new(None),
            typed_columns: tokio::sync::Mutex::new(None),
            pending: tokio::sync::Mutex::new(faucet_core::PageAccumulator::bounded(
                config.commit_rows,
                config.commit_bytes,
            )),
            config,
            backend,
            s3,
        })
    }

    /// Build an S3 client honouring the optional region / endpoint overrides.
    async fn build_s3_client(config: &RedshiftSinkConfig) -> S3Client {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        let copy_cfg = config.copy_spec();
        if let Some(region) = &copy_cfg.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &copy_cfg.endpoint_url {
            loader = loader.endpoint_url(endpoint);
        }
        let sdk_config = loader.load().await;
        // S3-compatible stores (MinIO) do not serve virtual-hosted bucket names.
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(copy_cfg.endpoint_url.is_some())
            .build();
        S3Client::from_conf(s3_config)
    }

    fn table_ref(&self) -> String {
        qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name)
    }

    /// The `$1` table / `$2` schema binds of [`TARGET_RELATION`].
    fn catalog_binds(&self) -> [Value; 2] {
        [
            Value::String(self.config.table_name.clone()),
            self.config
                .schema
                .clone()
                .map_or(Value::Null, Value::String),
        ]
    }

    /// Generate a unique staging object key for one page.
    fn staging_key(&self, ext: &str) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}.{}", self.config.copy_spec().staging_prefix, id, ext)
    }

    /// Discover the destination table's column names in ordinal order. Used
    /// by the `insert` path and the CSV `copy` path (both need the column
    /// set/order).
    async fn discover_columns(&self, calls: &CallSlot) -> Result<Vec<String>, FaucetError> {
        Ok(self
            .table_columns(calls)
            .await?
            .into_iter()
            .map(|(name, _)| name)
            .collect())
    }

    /// [`discover_typed_columns`](Self::discover_typed_columns), cached.
    async fn table_columns(&self, calls: &CallSlot) -> Result<Vec<(String, String)>, FaucetError> {
        let mut cached = self.typed_columns.lock().await;
        if let Some(cols) = cached.as_ref() {
            return Ok(cols.clone());
        }
        let cols = self.discover_typed_columns(calls).await?;
        *cached = Some(cols.clone());
        Ok(cols)
    }

    /// The table's `(column, declared type)` pairs in declared order.
    async fn discover_typed_columns(
        &self,
        calls: &CallSlot,
    ) -> Result<Vec<(String, String)>, FaucetError> {
        let sql = format!(
            "SELECT a.attname::text AS column_name, \
             format_type(a.atttypid, a.atttypmod)::text AS data_type \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE {TARGET_RELATION} AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum"
        );
        let cols: Vec<(String, String)> = match &self.backend {
            Backend::Wire(pool) => {
                let pool = pool.get(&calls.get()).await?;
                let rows = self
                    .call(calls, "query", async {
                        sqlx::query(&sql)
                            .bind(&self.config.table_name)
                            .bind(self.config.schema.as_deref())
                            .fetch_all(&pool)
                            .await
                            .map_err(|e| {
                                FaucetError::Sink(format!("redshift: column discovery failed: {e}"))
                            })
                    })
                    .await?;
                rows.iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("column_name"),
                            r.get::<String, _>("data_type"),
                        )
                    })
                    .collect()
            }
            Backend::DataApi(api) => self
                .data_api_rows(calls, api, &sql, &self.catalog_binds())
                .await
                .map_err(|e| prefixed(e, "redshift: column discovery failed"))?
                .iter()
                .map(|r| {
                    let text = |k: &str| {
                        r.get(k)
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    };
                    (text("column_name"), text("data_type"))
                })
                .collect(),
        };
        if cols.is_empty() {
            return Err(FaucetError::Sink(format!(
                "redshift: table {} has no columns or does not exist",
                self.config.table_name
            )));
        }
        Ok(cols)
    }

    /// Load `chunks` via the `COPY`-from-S3 fast path: each chunk is staged
    /// as one object and a single `COPY` loads them all (through a manifest
    /// when there are several), so the group lands or fails as a unit.
    async fn copy_chunks(
        &self,
        calls: &CallSlot,
        chunks: &[&[Value]],
    ) -> Result<usize, FaucetError> {
        if chunks.iter().all(|c| c.is_empty()) {
            return Ok(0);
        }
        let s3 = self.s3.as_ref().ok_or_else(|| {
            FaucetError::Sink("redshift: S3 client not initialized for copy strategy".into())
        })?;
        let copy_cfg = self.config.copy_spec();
        let bucket = copy_cfg.staging_bucket.as_deref().ok_or_else(|| {
            FaucetError::Sink("redshift: staging_bucket is required for copy strategy".into())
        })?;
        let iam_role = copy_cfg.iam_role.as_deref().ok_or_else(|| {
            FaucetError::Sink("redshift: iam_role is required for copy strategy".into())
        })?;
        let table_columns = self.discover_columns(calls).await?;
        let ext = match copy_cfg.format {
            RedshiftCopyFormat::Jsonl => "jsonl",
            RedshiftCopyFormat::Csv => "csv",
        };
        let mut staged = StagedObjects {
            s3: s3.clone(),
            bucket: bucket.to_string(),
            keys: Vec::new(),
        };
        let mut loaded = 0;
        let mut parts = Vec::new();
        for chunk in chunks.iter().filter(|c| !c.is_empty()) {
            // A record sharing no column with the table would load as an
            // all-NULL row, so it is skipped, as on the INSERT path (#862 L-118).
            let (records, skipped) = loadable_records(chunk, &table_columns)?;
            if skipped > 0 {
                tracing::warn!(
                    table = %self.config.table_name,
                    skipped,
                    "redshift: skipped record(s) with no column matching the table \
                     (would have loaded an all-NULL row)"
                );
            }
            let body = match copy_cfg.format {
                RedshiftCopyFormat::Jsonl => serialize_jsonl(&records)?,
                RedshiftCopyFormat::Csv => serialize_csv(&records, &table_columns)?,
            };
            let key = self.staging_key(ext);
            self.upload(calls, s3, bucket, &key, body).await?;
            staged.keys.push(key.clone());
            parts.push(key);
            loaded += records.len();
        }
        let (source, manifest) = if parts.len() == 1 {
            (s3_uri(bucket, &parts[0]), false)
        } else {
            let key = self.staging_key("manifest");
            self.upload(calls, s3, bucket, &key, manifest_body(bucket, &parts))
                .await?;
            staged.keys.push(key.clone());
            (s3_uri(bucket, &key), true)
        };
        let columns = match copy_cfg.format {
            RedshiftCopyFormat::Jsonl => None,
            RedshiftCopyFormat::Csv => Some(table_columns.as_slice()),
        };
        let mut sql = copy_statement(
            &self.table_ref(),
            columns,
            &source,
            iam_role,
            copy_cfg.region.as_deref(),
            copy_cfg.format,
        );
        if manifest {
            sql.push_str(" MANIFEST");
        }
        let copy_result = self
            .execute_sql(calls, "execute", &sql, "redshift: COPY failed")
            .await;
        // A failed COPY still leaves the objects behind.
        staged.delete().await;
        copy_result?;
        Ok(loaded)
    }

    /// Upload one staged object.
    async fn upload(
        &self,
        calls: &CallSlot,
        s3: &S3Client,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<(), FaucetError> {
        self.call(calls, "stage", async {
            s3.put_object()
                .bucket(bucket)
                .key(key)
                .body(body.into())
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    FaucetError::Sink(format!("redshift: S3 upload failed for key '{key}': {e}"))
                })
        })
        .await
    }

    /// Load one chunk via multi-row `INSERT`, sub-chunked to respect the bind
    /// parameter cap.
    async fn insert_chunk(
        &self,
        calls: &CallSlot,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let typed = self.table_columns(calls).await?;
        let table_columns: Vec<String> = typed.iter().map(|(n, _)| n.clone()).collect();
        let present: Vec<String> = columns_present(records, &table_columns)
            .into_iter()
            .cloned()
            .collect();
        // A column whose values arrive as text binds text cast to its type, so
        // the server parses the exact digits or the instant the record holds.
        let cast_types: Vec<Option<String>> = present
            .iter()
            .map(|c| {
                typed
                    .iter()
                    .find(|(n, _)| n == c)
                    .map(|(_, t)| t.clone())
                    .filter(|t| binds_as_cast_text(t))
            })
            .collect();
        let casts: Vec<Option<&str>> = cast_types.iter().map(|t| t.as_deref()).collect();
        if present.is_empty() {
            // Reporting the chunk as written would advance the bookmark past
            // rows that never landed (SQL-19).
            return Err(FaucetError::Sink(format!(
                "redshift: no field of the {} record(s) matches a column of {} (columns: {})",
                records.len(),
                self.table_ref(),
                table_columns.join(", ")
            )));
        }

        // Drop records that share *no* column with the table. `present` is the
        // union of table columns across the page, so binding such a record would
        // emit an all-NULL row rather than the data the caller sent — the DuckDB
        // and SQLite sinks skip it, and so do we (#466 L1). A record with at
        // least one matching column is still inserted (missing columns → NULL).
        let insertable: Vec<&Value> = records
            .iter()
            .filter(|r| crate::copy::shares_a_column(r, &present))
            .collect();
        let skipped = records.len() - insertable.len();
        if skipped > 0 {
            tracing::warn!(
                table = %self.config.table_name,
                skipped,
                "redshift: skipped record(s) with no column matching the table \
                 (would have inserted an all-NULL row)"
            );
        }
        if insertable.is_empty() {
            return Ok(0);
        }

        let num_cols = present.len();
        // Redshift caps bind parameters per statement; the dialect sizes each
        // multi-row INSERT under it.
        let max_rows = faucet_common_sql::SqlDialect::rows_per_statement(
            &faucet_common_sql::dialects::Redshift,
            num_cols,
        );
        let mut total = 0usize;

        if let Backend::DataApi(api) = &self.backend {
            for sub in insertable.chunks(max_rows) {
                for (statement, rows) in data_api_inserts(&self.table_ref(), &present, &casts, sub)?
                {
                    Box::pin(api.execute(&calls.get(), Effect::Write, &statement))
                        .await
                        .map_err(|e| prefixed(e, "redshift: INSERT failed"))?;
                    total += rows;
                }
            }
            return Ok(total);
        }
        let Backend::Wire(pool) = &self.backend else {
            return Ok(total);
        };
        let pool = pool.get(&calls.get()).await?;
        for sub in insertable.chunks(max_rows) {
            let sql = insert_statement(&self.table_ref(), &present, &casts, sub.len());
            let mut q = sqlx::query(&sql);
            for record in sub {
                let obj = record.as_object().ok_or_else(|| {
                    FaucetError::Sink("redshift: insert requires JSON object records".into())
                })?;
                for (col, cast) in present.iter().zip(&casts) {
                    let value = crate::copy::field(obj, col);
                    q = match (cast, value) {
                        (Some(_), Some(Value::Number(n))) => q.bind(n.to_string()),
                        _ => bind_json(q, value, col)?,
                    };
                }
            }
            self.call(calls, "execute", async {
                q.execute(&pool)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("redshift: INSERT failed: {e}")))
            })
            .await?;
            total += sub.len();
        }
        Ok(total)
    }
}

/// `err` with `what` in front of its message (Data API errors carry no
/// operation name of their own).
fn prefixed(err: FaucetError, what: &str) -> FaucetError {
    match err {
        FaucetError::Sink(m) => FaucetError::Sink(format!("{what}: {m}")),
        other => other,
    }
}

/// The multi-row `INSERT`s for `rows` through the Data API, each with how
/// many rows it carries. Values are `:pN` parameters (cast like the wire
/// path's binds); a NULL or empty string is written into the SQL, since a
/// parameter can be neither. A statement holds at most
/// [`DATA_API_STATEMENT_BUDGET`] bytes of SQL and parameters, or one row.
fn data_api_inserts(
    table_ref: &str,
    columns: &[String],
    casts: &[Option<&str>],
    rows: &[&Value],
) -> Result<Vec<(Statement, usize)>, FaucetError> {
    let head = format!(
        "INSERT INTO {table_ref} ({}) VALUES ",
        columns
            .iter()
            .map(|c| faucet_core::util::quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut out = Vec::new();
    let mut current = Statement::new(head.clone());
    let mut size = head.len();
    let mut count = 0usize;
    for record in rows {
        let obj = record.as_object().ok_or_else(|| {
            FaucetError::Sink("redshift: insert requires JSON object records".into())
        })?;
        let mut row = insert_tuple(obj, columns, casts, current.parameters.len())?;
        if count > 0 && size + row.2 > DATA_API_STATEMENT_BUDGET {
            out.push((
                std::mem::replace(&mut current, Statement::new(head.clone())),
                count,
            ));
            size = head.len();
            count = 0;
            row = insert_tuple(obj, columns, casts, 0)?;
        }
        if count > 0 {
            current.sql.push_str(", ");
        }
        current.sql.push_str(&row.0);
        current.parameters.extend(row.1);
        size += row.2;
        count += 1;
    }
    if count > 0 {
        out.push((current, count));
    }
    Ok(out)
}

/// One `VALUES` tuple whose parameters are numbered after `first`, its
/// parameters, and its size in bytes.
fn insert_tuple(
    obj: &serde_json::Map<String, Value>,
    columns: &[String],
    casts: &[Option<&str>],
    first: usize,
) -> Result<(String, Vec<(String, String)>, usize), FaucetError> {
    let mut tuple = String::from("(");
    let mut params: Vec<(String, String)> = Vec::new();
    for (i, (col, cast)) in columns.iter().zip(casts).enumerate() {
        if i > 0 {
            tuple.push_str(", ");
        }
        let text = match crate::copy::field(obj, col) {
            None | Some(Value::Null) => {
                tuple.push_str("NULL");
                continue;
            }
            Some(Value::String(s)) if s.is_empty() => {
                tuple.push_str("''");
                continue;
            }
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) if cast.is_none() && n.is_u64() && !n.is_i64() => {
                faucet_core::util::u64_to_signed(
                    n.as_u64().unwrap_or_default(),
                    &format!("column {}", faucet_core::util::quote_ident(col)),
                )?;
                n.to_string()
            }
            Some(other) => other.to_string(),
        };
        let name = format!("p{}", first + params.len() + 1);
        match cast {
            Some(t) => tuple.push_str(&format!("CAST(:{name} AS {t})")),
            None => tuple.push_str(&format!(":{name}")),
        }
        params.push((name, text));
    }
    tuple.push(')');
    let size = tuple.len() + 2 + params.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>();
    Ok((tuple, params, size))
}

/// Whether a column of `sql_type` takes its value as text cast to the type:
/// exact decimals (#885) and the types the fidelity model renders as strings.
fn binds_as_cast_text(sql_type: &str) -> bool {
    if faucet_core::fidelity::is_exact_decimal_type(sql_type) {
        return true;
    }
    let base = sql_type.trim().to_ascii_lowercase();
    ["date", "time", "timetz", "timestamp", "timestamptz", "uuid"]
        .iter()
        .any(|p| {
            base == *p || base.starts_with(&format!("{p} ")) || base.starts_with(&format!("{p}("))
        })
}

/// The records of a COPY chunk that share a column with the table, and how
/// many were left out. A chunk where none does is an error: reporting it
/// written would advance the bookmark past rows that never landed.
fn loadable_records<'a>(
    records: &'a [Value],
    table_columns: &[String],
) -> Result<(std::borrow::Cow<'a, [Value]>, usize), FaucetError> {
    if records
        .iter()
        .all(|r| crate::copy::shares_a_column(r, table_columns))
    {
        return Ok((std::borrow::Cow::Borrowed(records), 0));
    }
    let kept: Vec<Value> = records
        .iter()
        .filter(|r| crate::copy::shares_a_column(r, table_columns))
        .cloned()
        .collect();
    if kept.is_empty() {
        return Err(FaucetError::Sink(format!(
            "redshift: no field of the {} record(s) matches a column of the table \
             (columns: {})",
            records.len(),
            table_columns.join(", ")
        )));
    }
    let skipped = records.len() - kept.len();
    Ok((std::borrow::Cow::Owned(kept), skipped))
}

/// The catalog predicate (over `pg_class c` / `pg_namespace n`, binds `$1`
/// table, `$2` schema or NULL) selecting the relation an unqualified `INSERT`
/// or `COPY` resolves to: the first match on the `search_path`, which
/// `pg_table_is_visible` reports, not only `current_schema()`.
const TARGET_RELATION: &str = "lower(c.relname) = lower($1) \
    AND c.relkind IN ('r', 'p', 'v', 'f') \
    AND CASE WHEN $2::text IS NULL THEN pg_catalog.pg_table_is_visible(c.oid) \
        ELSE lower(n.nspname) = lower($2::text) END";

/// Bind one JSON value onto a `sqlx` query as a native scalar type (so it lands
/// in a typed Redshift column instead of being coerced from `jsonb`). Missing /
/// null binds SQL NULL.
fn bind_json<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: Option<&Value>,
    column: &str,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, FaucetError> {
    Ok(match v {
        None | Some(Value::Null) => query.bind(None::<String>),
        Some(Value::String(s)) => query.bind(s.clone()),
        Some(Value::Bool(b)) => query.bind(*b),
        Some(Value::Number(n)) => {
            if n.is_i64() {
                query.bind(n.as_i64().unwrap())
            } else if n.is_u64() {
                // Above `i64::MAX`. Redshift's BIGINT is signed and `as i64`
                // would *wrap*, writing the id as a large negative number with
                // nothing raised. Refuse instead (#462).
                query.bind(faucet_core::util::u64_to_signed(
                    n.as_u64().unwrap(),
                    &format!("column {}", faucet_core::util::quote_ident(column)),
                )?)
            } else {
                query.bind(n.as_f64().unwrap_or(0.0))
            }
        }
        Some(other) => query.bind(other.to_string()),
    })
}

impl RedshiftSink {
    /// Create a new sink. Validates config, builds a lazily-connected pool (no
    /// DB I/O), and — for the `copy` strategy — an S3 client.
    pub async fn new(config: RedshiftSinkConfig) -> Result<Self, FaucetError> {
        Ok(Self {
            inner: DriverSink::new(RedshiftDriver::new(config).await?),
        })
    }
}

#[async_trait]
impl faucet_core::Sink for RedshiftSink {
    faucet_core::forward_sink!(inner);
}

#[cfg(test)]
impl RedshiftSink {
    fn s3(&self) -> &Option<S3Client> {
        &self.inner.driver().s3
    }
    fn table_ref(&self) -> String {
        self.inner.driver().table_ref()
    }
    fn staging_key(&self, ext: &str) -> String {
        self.inner.driver().staging_key(ext)
    }
}

impl RedshiftDriver {
    /// Commit whatever the cross-page accumulator holds (#617).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so a buffered group is always loaded before the bookmark
    /// advances — records left in the accumulator after a "successful" run
    /// would be data loss with a green exit code.
    async fn flush_pending(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let group = {
            let mut open = self.pending.lock().await;
            open.finish()
        };
        if let Some(rows) = group {
            let len = rows.len();
            self.commit_or_restore(calls, rows, len).await?;
        }
        Ok(())
    }

    /// The DLQ path commits per page (SQL-21): earlier buffered rows are
    /// committed first, then this page alone, so a failed commit names exactly
    /// the rows of *this* page that did not land.
    async fn write_partial(
        &self,
        calls: &CallSlot,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        self.flush_pending(calls).await?;
        self.ensure_table_ready(calls, records).await?;
        self.ensure_columns(calls, records).await?;
        // A record sharing no column with the table cannot land: it fails on
        // its own (and reaches the DLQ) instead of counting as written.
        let unloadable = self.unloadable(records).await;
        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());
        let mut loadable = Vec::with_capacity(records.len());
        let mut at = Vec::with_capacity(records.len());
        for (i, (r, skip)) in records.iter().zip(&unloadable).enumerate() {
            if *skip {
                outcomes.push(Err(FaucetError::Sink(
                    "redshift: no field of the record matches a column of the table".into(),
                )));
            } else {
                outcomes.push(Ok(()));
                loadable.push(r.clone());
                at.push(i);
            }
        }
        if loadable.is_empty() {
            return Ok(outcomes);
        }
        match self.commit_group(calls, &loadable).await {
            Ok(()) => Ok(outcomes),
            Err((0, e)) if loadable.len() == records.len() => Err(e),
            Err((done, e)) => {
                let message = e.to_string();
                for &i in &at[done..] {
                    outcomes[i] = Err(FaucetError::Sink(format!(
                        "redshift: write failed: {message}"
                    )));
                }
                Ok(outcomes)
            }
        }
    }

    /// For each record, whether it shares no column with the table (by the
    /// columns known after [`ensure_columns`](Self::ensure_columns)): it would
    /// load as an all-NULL row, so it is never written.
    async fn unloadable(&self, records: &[Value]) -> Vec<bool> {
        let known = self.known_columns.lock().await;
        records
            .iter()
            .map(|r| match (known.as_ref(), r.as_object()) {
                (_, None) => true,
                (None, Some(_)) => false,
                (Some(cols), Some(o)) => !o.keys().any(|k| cols.contains(&k.to_lowercase())),
            })
            .collect()
    }

    async fn write_page(
        &self,
        calls: &CallSlot,
        records: &[Value],
    ) -> Result<faucet_core::WriteReport, FaucetError> {
        if records.is_empty() {
            return Ok(faucet_core::WriteReport::accepted(0));
        }
        self.ensure_table_ready(calls, records).await?;
        self.ensure_columns(calls, records).await?;
        // A record sharing no column with the table would load as an all-NULL
        // row, so it is skipped — and reported skipped, never written.
        let unloadable = self.unloadable(records).await;
        let skipped = unloadable.iter().filter(|s| **s).count();
        let loadable: std::borrow::Cow<'_, [Value]> = if skipped == 0 {
            std::borrow::Cow::Borrowed(records)
        } else {
            tracing::warn!(
                table = %self.config.table_name,
                skipped,
                "redshift: skipped record(s) with no column matching the table \
                 (would have loaded an all-NULL row)"
            );
            records
                .iter()
                .zip(&unloadable)
                .filter(|(_, skip)| !**skip)
                .map(|(r, _)| r.clone())
                .collect::<Vec<_>>()
                .into()
        };
        let report = faucet_core::WriteReport::accepted(loadable.len()).with_skipped(skipped);
        if loadable.is_empty() {
            return Ok(report);
        }
        // Accumulate across calls and load once per threshold (#617): `COPY`
        // wants millions of rows per load, and `batch_size` could only ever
        // split a page — never merge two small ones.
        let group = {
            let mut pending = self.pending.lock().await;
            pending.push_page(&loadable)
        };
        if let Some(rows) = group {
            let keep_end = rows.len() - loadable.len();
            self.commit_or_restore(calls, rows, keep_end).await?;
        }
        Ok(report)
    }
}

#[async_trait]
impl SinkDriver for RedshiftDriver {
    fn capabilities(&self, derived: faucet_core::Capabilities) -> faucet_core::Capabilities {
        derived
            .to_builder()
            .declared_types(self.config.create_table)
            .build()
    }

    fn set_declared_columns(&self, columns: faucet_core::DeclaredColumns) {
        self.declared.set(columns);
    }

    /// A run that wrote nothing creates the declared table (#955).
    async fn complete_run(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        if self.config.create_table
            && !self.table_ready.load(std::sync::atomic::Ordering::Relaxed)
            && self.declared.for_empty_run().is_some()
        {
            self.ensure_table_ready(calls, &[]).await?;
        }
        Ok(())
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn connector_name(&self) -> &'static str {
        "redshift"
    }

    fn call_context(&self) -> faucet_core::CallContext {
        faucet_core::CallContext::new()
            .without_timeout()
            .without_retry()
    }

    /// Redshift bulk-loads via `COPY … FROM 's3://…'` under `write_strategy: copy`
    /// (#528). The `staging` capability is advertised so the CLI + tooling can
    /// surface it.
    fn staged_load(&self) -> bool {
        true
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RedshiftSinkConfig))
            .expect("schema serialization")
    }

    fn write_modes(&self) -> &'static [faucet_core::WriteMode] {
        // Append-only: Redshift has no ON CONFLICT and COPY cannot upsert.
        &[faucet_core::WriteMode::Append]
    }

    fn dataset_uri(&self) -> String {
        let table = match &self.config.schema {
            Some(s) => format!("{}.{}", s, self.config.table_name),
            None => self.config.table_name.clone(),
        };
        let conn = &self.config.connection;
        if conn.uses_data_api() {
            return format!(
                "redshift://{}/{}?table={table}",
                conn.endpoint_label(),
                conn.database
            );
        }
        format!(
            "redshift://{}:{}/{}?table={}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.database,
            table
        )
    }

    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.flush_pending(calls).await
    }

    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        match policy {
            RowPolicy::PerRow => Ok(PageWrite::from_outcomes(
                Box::pin(self.write_partial(calls, rows)).await?,
            )),
            _ => Ok(PageWrite::reported(
                Box::pin(self.write_page(calls, rows)).await?,
            )),
        }
    }

    /// Preflight connectivity probe (`faucet doctor`): acquire a connection and
    /// run `SELECT 1`. Non-mutating and idempotent.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let ping = async {
            match &self.backend {
                Backend::Wire(pool) => {
                    let pool = pool.get(&calls.get()).await?;
                    self.call(calls, "query", async {
                        sqlx::query("SELECT 1")
                            .execute(&pool)
                            .await
                            .map(|_| ())
                            .map_err(|e| FaucetError::Sink(e.to_string()))
                    })
                    .await
                }
                Backend::DataApi(api) => {
                    Box::pin(api.query(&calls.get(), &Statement::new("SELECT 1")))
                        .await
                        .map(|_| ())
                }
            }
        };
        let probe = match tokio::time::timeout(ctx.timeout, ping).await {
            Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check host/port/database/user/credentials and that the cluster is reachable",
            ),
            Err(_) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                "timed out",
                "check host/port/database/user/credentials and that the cluster is reachable",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn text_rendered_types_bind_as_cast_text() {
        for t in [
            "numeric(12,2)",
            "date",
            "timestamp without time zone",
            "timestamp(3) with time zone",
            "time with time zone",
            "uuid",
        ] {
            assert!(super::binds_as_cast_text(t), "{t}");
        }
        for t in [
            "bigint",
            "character varying(256)",
            "boolean",
            "double precision",
            "super",
        ] {
            assert!(!super::binds_as_cast_text(t), "{t}");
        }
    }

    #[test]
    fn a_copy_chunk_drops_records_that_share_no_column() {
        use serde_json::json;
        let cols = vec!["id".to_string(), "name".to_string()];
        let all = [json!({"id": 1}), json!({"NAME": "x"})];
        let (kept, skipped) = super::loadable_records(&all, &cols).unwrap();
        assert!(matches!(kept, std::borrow::Cow::Borrowed(_)));
        assert_eq!(skipped, 0);
        let mixed = [json!({"id": 1}), json!({}), json!({"other": 2})];
        let (kept, skipped) = super::loadable_records(&mixed, &cols).unwrap();
        assert_eq!(kept.as_ref(), &[json!({"id": 1})]);
        assert_eq!(skipped, 2);
        let err = super::loadable_records(&[json!({}), json!(3)], &cols)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no field of the 2 record(s)"), "{err}");
    }
    use super::*;
    use crate::config::{RedshiftCopyFormat, RedshiftWriteStrategy};
    use faucet_common_redshift::RedshiftConnection;
    use faucet_core::Sink as _;
    use serde_json::json;

    fn insert_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection: RedshiftConnection::new("host", "db", "user", "pw"),
            table_name: "events".into(),
            create_table: true,
            commit_rows: None,
            commit_bytes: None,
            schema: Some("public".into()),
            write_strategy: RedshiftWriteStrategy::Insert,
            copy: None,
            copy_format: RedshiftCopyFormat::Jsonl,
            staging_bucket: None,
            staging_prefix: String::new(),
            iam_role: None,
            region: None,
            endpoint_url: None,
            batch_size: 1000,
            max_connections: 5,
        }
    }

    fn copy_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            write_strategy: RedshiftWriteStrategy::Copy,
            staging_bucket: Some("stage".into()),
            staging_prefix: "rs/".into(),
            iam_role: Some("arn:aws:iam::1:role/r".into()),
            // Explicit region so building the S3 client resolves without the
            // default region provider probing IMDS (hermetic, fast tests).
            region: Some("us-east-1".into()),
            ..insert_config()
        }
    }

    async fn sink(c: RedshiftSinkConfig) -> RedshiftSink {
        RedshiftSink::new(c).await.unwrap()
    }

    #[tokio::test]
    async fn new_insert_has_no_s3_client() {
        let s = sink(insert_config()).await;
        assert!(s.s3().is_none());
    }

    #[tokio::test]
    async fn new_copy_builds_s3_client() {
        let s = sink(copy_config()).await;
        assert!(s.s3().is_some());
    }

    #[tokio::test]
    async fn new_rejects_copy_without_bucket() {
        let mut c = copy_config();
        c.staging_bucket = None;
        assert!(matches!(
            RedshiftSink::new(c).await,
            Err(FaucetError::Config(_))
        ));
    }

    #[tokio::test]
    async fn new_builds_iam_and_data_api_sinks_and_validates_them() {
        let with = |creds: Value| {
            let mut c = insert_config();
            c.connection.credentials = serde_json::from_value(creds).unwrap();
            c
        };
        let s = sink(with(
            json!({"type": "iam", "config": {"workgroup_name": "wg"}}),
        ))
        .await;
        assert!(matches!(&s.inner.driver().backend, Backend::Wire(p) if p.is_iam()));
        let s = sink(with(json!({"type": "redshift_data_api", "config": {"cluster_identifier": "c", "db_user": "u"}}))).await;
        assert!(matches!(s.inner.driver().backend, Backend::DataApi(_)));
        assert_eq!(
            s.dataset_uri(),
            "redshift://cluster:c/db?table=public.events"
        );
        for bad in [
            json!({"type": "iam", "config": {"cluster_identifier": "c", "workgroup_name": "w"}}),
            json!({"type": "redshift_data_api", "config": {}}),
        ] {
            assert!(matches!(
                RedshiftSink::new(with(bad)).await,
                Err(FaucetError::Config(_))
            ));
        }
    }

    #[test]
    fn data_api_inserts_bind_parameters_and_split_by_size() {
        let cols = vec!["id".to_string(), "amount".to_string(), "note".to_string()];
        let casts = [None, Some("numeric(12,2)"), None];
        let rows = [
            json!({"id": 1, "amount": 12.5, "note": "it's"}),
            json!({"ID": 2, "amount": null, "note": ""}),
            json!({"id": 3, "note": {"k": [true]}}),
        ];
        let refs: Vec<&Value> = rows.iter().collect();
        let out = data_api_inserts("\"t\"", &cols, &casts, &refs).unwrap();
        assert_eq!(out.len(), 1);
        let (st, n) = &out[0];
        assert_eq!(*n, 3);
        assert_eq!(
            st.sql,
            "INSERT INTO \"t\" (\"id\", \"amount\", \"note\") VALUES \
             (:p1, CAST(:p2 AS numeric(12,2)), :p3), (:p4, NULL, ''), (:p5, NULL, :p6)"
        );
        let values: Vec<&str> = st.parameters.iter().map(|(_, v)| v.as_str()).collect();
        assert_eq!(values, ["1", "12.5", "it's", "2", "3", r#"{"k":[true]}"#]);

        let wide = "x".repeat(DATA_API_STATEMENT_BUDGET / 2);
        let big = [
            json!({"id": 1, "note": wide}),
            json!({"id": 2, "note": wide}),
            json!({"id": 3}),
        ];
        let refs: Vec<&Value> = big.iter().collect();
        let out = data_api_inserts("t", &cols, &casts, &refs).unwrap();
        assert_eq!(out.iter().map(|(_, n)| *n).collect::<Vec<_>>(), [1, 2]);
        assert!(
            out[1]
                .0
                .sql
                .ends_with("VALUES (:p1, NULL, :p2), (:p3, NULL, NULL)"),
            "{}",
            out[1].0.sql
        );

        let err = data_api_inserts("t", &cols, &casts, &[&json!({"id": u64::MAX})])
            .unwrap_err()
            .to_string();
        assert!(err.contains("\"id\""), "{err}");
        assert!(data_api_inserts("t", &cols, &casts, &[&json!(1)]).is_err());
        assert!(matches!(
            prefixed(FaucetError::Sink("x".into()), "w"),
            FaucetError::Sink(m) if m == "w: x"
        ));
        assert!(matches!(
            prefixed(FaucetError::Config("x".into()), "w"),
            FaucetError::Config(_)
        ));
    }

    #[tokio::test]
    async fn connector_name_is_redshift() {
        assert_eq!(sink(insert_config()).await.connector_name(), "redshift");
    }

    #[tokio::test]
    async fn supported_write_modes_is_append_only() {
        let s = sink(insert_config()).await;
        assert_eq!(
            s.supported_write_modes(),
            [faucet_core::WriteMode::Append].as_slice()
        );
    }

    #[tokio::test]
    async fn dataset_uri_schema_qualified() {
        let s = sink(insert_config()).await;
        assert_eq!(
            s.dataset_uri(),
            "redshift://host:5439/db?table=public.events"
        );
    }

    #[tokio::test]
    async fn config_schema_reports_required_fields() {
        let s = sink(insert_config()).await;
        let schema = s.config_schema();
        assert!(schema["properties"]["table_name"].is_object());
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "table_name"));
    }

    #[tokio::test]
    async fn write_batch_empty_is_zero() {
        let s = sink(insert_config()).await;
        assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn table_ref_and_staging_key() {
        let s = sink(copy_config()).await;
        assert_eq!(s.table_ref(), "\"public\".\"events\"");
        let key = s.staging_key("jsonl");
        assert!(key.starts_with("rs/"));
        assert!(key.ends_with(".jsonl"));
    }

    #[test]
    fn bind_json_covers_all_scalar_kinds() {
        // Smoke: binding must not panic for every JSON scalar kind. The actual
        // wire encoding is exercised by the live integration test.
        let q = sqlx::query("SELECT $1, $2, $3, $4, $5, $6");
        let q = bind_json(q, Some(&json!("s")), "c").unwrap();
        let q = bind_json(q, Some(&json!(7)), "c").unwrap();
        let q = bind_json(q, Some(&json!(7.5)), "c").unwrap();
        let q = bind_json(q, Some(&json!(true)), "c").unwrap();
        let q = bind_json(q, Some(&Value::Null), "c").unwrap();
        let _q = bind_json(q, None, "c").unwrap();

        // #462: a u64 above i64::MAX must be refused, not wrapped negative.
        let q = sqlx::query("SELECT 1");
        let err = match bind_json(q, Some(&json!(u64::MAX)), "big_id") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("u64::MAX must not bind to a signed BIGINT"),
        };
        assert!(err.contains("big_id"), "{err}");
        assert!(err.contains(&u64::MAX.to_string()), "{err}");
    }
}
