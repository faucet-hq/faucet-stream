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
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::config::{RedshiftCopyFormat, RedshiftSinkConfig, RedshiftWriteStrategy};
use crate::copy::{
    build_create_table_sql, columns_present, copy_statement, insert_statement, qualified_table_ref,
    s3_uri, serialize_csv, serialize_jsonl,
};

/// Redshift caps bind parameters per statement; keep multi-row `INSERT`s under
/// this by sub-chunking.
const MAX_REDSHIFT_PARAMS: usize = 32_767;

/// A sink that loads JSON records into an Amazon Redshift table.
pub struct RedshiftSink {
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    /// Records accumulated across `write_batch` calls (#617).
    ///
    /// `COPY` wants millions of rows per load; one `COPY` per 1000-row page
    /// produced many small commits, small unsorted blocks, and VACUUM
    /// pressure.
    pending: tokio::sync::Mutex<faucet_core::PageAccumulator>,
    config: RedshiftSinkConfig,
    pool: PgPool,
    /// S3 client, built only for the `copy` strategy.
    s3: Option<S3Client>,
}

impl RedshiftSink {
    /// Load one accumulated group, still re-chunked to `batch_size` so a
    /// single staged object / `INSERT` statement stays a reasonable size
    /// (#617).
    async fn commit_group(&self, rows: &[Value]) -> Result<(), FaucetError> {
        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![rows]
        } else {
            rows.chunks(self.config.batch_size).collect()
        };
        let mut total = 0;
        for chunk in chunks {
            total += match self.config.write_strategy {
                RedshiftWriteStrategy::Copy => self.copy_chunk(chunk).await?,
                RedshiftWriteStrategy::Insert => self.insert_chunk(chunk).await?,
            };
        }
        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            strategy = self.config.write_strategy.as_str(),
            "Redshift write complete"
        );
        Ok(())
    }

    /// Make sure the target table exists before the first write (#580).
    ///
    /// Runs once per sink instance. With `create_table: true` (the default) a
    /// missing table — and its schema, when `schema:` is set — is created from
    /// the first page's inferred columns; with `false` a missing table is a
    /// typed failure naming both ways out.
    async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.config.create_table {
            let exists: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_name = $1 AND ($2::text IS NULL OR table_schema = $2)",
            )
            .bind(&self.config.table_name)
            .bind(self.config.schema.as_deref())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("redshift table probe failed: {e}")))?;
            if exists.is_none() {
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
        let Some(columns) = faucet_core::plan_columns(records) else {
            return Ok(());
        };
        if let Some(schema) = self.config.schema.as_deref() {
            sqlx::query(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                faucet_core::util::quote_ident(schema)
            ))
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("redshift CREATE SCHEMA failed: {e}")))?;
        }
        let sql = build_create_table_sql(&self.table_ref(), &columns);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("redshift CREATE TABLE failed: {e}")))?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Create a new sink. Validates config, builds a lazily-connected pool (no
    /// DB I/O), and — for the `copy` strategy — an S3 client.
    pub async fn new(config: RedshiftSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool =
            faucet_common_redshift::build_pool_lazy(&config.connection, config.max_connections)?;
        let s3 = if config.write_strategy == RedshiftWriteStrategy::Copy {
            Some(Self::build_s3_client(&config).await)
        } else {
            None
        };
        Ok(Self {
            table_ready: std::sync::atomic::AtomicBool::new(false),
            pending: tokio::sync::Mutex::new(faucet_core::PageAccumulator::new(
                config.commit_rows,
                config.commit_bytes,
            )),
            config,
            pool,
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
        S3Client::new(&sdk_config)
    }

    fn table_ref(&self) -> String {
        qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name)
    }

    /// Generate a unique staging object key for one page.
    fn staging_key(&self, ext: &str) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}.{}", self.config.copy_spec().staging_prefix, id, ext)
    }

    /// Discover the destination table's column names in ordinal order via
    /// `information_schema.columns`. Used by the `insert` path and the CSV
    /// `copy` path (both need the column set/order).
    async fn discover_columns(&self) -> Result<Vec<String>, FaucetError> {
        let rows = sqlx::query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = $1 AND ($2::text IS NULL OR table_schema = $2) \
             ORDER BY ordinal_position",
        )
        .bind(&self.config.table_name)
        .bind(self.config.schema.as_deref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("redshift: column discovery failed: {e}")))?;

        let cols: Vec<String> = rows
            .iter()
            .map(|r| r.get::<String, _>("column_name"))
            .collect();
        if cols.is_empty() {
            return Err(FaucetError::Sink(format!(
                "redshift: table {} has no columns or does not exist",
                self.config.table_name
            )));
        }
        Ok(cols)
    }

    /// Load one chunk via the `COPY`-from-S3 fast path.
    async fn copy_chunk(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
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

        // Serialize the page and, for CSV, learn the destination column order.
        let (body, columns): (Vec<u8>, Option<Vec<String>>) = match copy_cfg.format {
            RedshiftCopyFormat::Jsonl => (serialize_jsonl(records)?, None),
            RedshiftCopyFormat::Csv => {
                let cols = self.discover_columns().await?;
                (serialize_csv(records, &cols)?, Some(cols))
            }
        };
        let ext = match copy_cfg.format {
            RedshiftCopyFormat::Jsonl => "jsonl",
            RedshiftCopyFormat::Csv => "csv",
        };
        let key = self.staging_key(ext);

        // 1. Upload the staged object.
        s3.put_object()
            .bucket(bucket)
            .key(&key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("redshift: S3 upload failed for key '{key}': {e}"))
            })?;

        // 2. COPY it into the table.
        let sql = copy_statement(
            &self.table_ref(),
            columns.as_deref(),
            &s3_uri(bucket, &key),
            iam_role,
            copy_cfg.region.as_deref(),
            copy_cfg.format,
        );
        let copy_result = sqlx::query(&sql).execute(&self.pool).await;

        // 3. Best-effort cleanup of the staged object (regardless of COPY
        //    outcome — a failed COPY still leaves the object behind).
        if let Err(e) = s3.delete_object().bucket(bucket).key(&key).send().await {
            tracing::warn!(key = %key, error = %e, "redshift: failed to delete staged S3 object (best-effort)");
        }

        copy_result.map_err(|e| FaucetError::Sink(format!("redshift: COPY failed: {e}")))?;
        Ok(records.len())
    }

    /// Load one chunk via multi-row `INSERT`, sub-chunked to respect the bind
    /// parameter cap.
    async fn insert_chunk(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let table_columns = self.discover_columns().await?;
        let present: Vec<String> = columns_present(records, &table_columns)
            .into_iter()
            .cloned()
            .collect();
        if present.is_empty() {
            tracing::warn!(
                table = %self.config.table_name,
                "redshift: no record keys match table columns; skipping insert"
            );
            return Ok(0);
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
        let max_rows = (MAX_REDSHIFT_PARAMS / num_cols).max(1);
        let mut total = 0usize;

        for sub in insertable.chunks(max_rows) {
            let sql = insert_statement(&self.table_ref(), &present, sub.len());
            let mut q = sqlx::query(&sql);
            for record in sub {
                let obj = record.as_object().ok_or_else(|| {
                    FaucetError::Sink("redshift: insert requires JSON object records".into())
                })?;
                for col in &present {
                    q = bind_json(q, obj.get(col), col)?;
                }
            }
            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("redshift: INSERT failed: {e}")))?;
            total += sub.len();
        }
        Ok(total)
    }
}

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

#[async_trait]
impl faucet_core::Sink for RedshiftSink {
    fn connector_name(&self) -> &'static str {
        "redshift"
    }

    /// Redshift bulk-loads via `COPY … FROM 's3://…'` under `write_strategy: copy`
    /// (#528). The `staging` capability is advertised so the CLI + tooling can
    /// surface it.
    fn supports_staged_load(&self) -> bool {
        true
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RedshiftSinkConfig))
            .expect("schema serialization")
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        // Append-only: Redshift has no ON CONFLICT and COPY cannot upsert.
        &[faucet_core::WriteMode::Append]
    }

    fn dataset_uri(&self) -> String {
        let table = match &self.config.schema {
            Some(s) => format!("{}.{}", s, self.config.table_name),
            None => self.config.table_name.clone(),
        };
        format!(
            "redshift://{}:{}/{}?table={}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.database,
            table
        )
    }

    /// Commit whatever the cross-page accumulator holds (#617).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so a buffered group is always loaded before the bookmark
    /// advances — records left in the accumulator after a "successful" run
    /// would be data loss with a green exit code.
    async fn flush(&self) -> Result<(), FaucetError> {
        let group = {
            let mut open = self.pending.lock().await;
            open.finish()
        };
        if let Some(rows) = group {
            self.commit_group(&rows).await?;
        }
        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records).await?;
        // Accumulate across calls and load once per threshold (#617): `COPY`
        // wants millions of rows per load, and `batch_size` could only ever
        // split a page — never merge two small ones.
        let group = {
            let mut pending = self.pending.lock().await;
            pending.push_page(records)
        };
        if let Some(rows) = group {
            self.commit_group(&rows).await?;
        }
        Ok(records.len())
    }

    /// Preflight connectivity probe (`faucet doctor`): acquire a connection and
    /// run `SELECT 1`. Non-mutating and idempotent.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
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
        assert!(s.s3.is_none());
    }

    #[tokio::test]
    async fn new_copy_builds_s3_client() {
        let s = sink(copy_config()).await;
        assert!(s.s3.is_some());
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
    async fn new_surfaces_unsupported_credentials() {
        let mut c = insert_config();
        c.connection.credentials = faucet_common_redshift::RedshiftCredentials::RedshiftDataApi {
            region: None,
            cluster_identifier: None,
            workgroup_name: None,
            secret_arn: None,
            db_user: None,
        };
        assert!(matches!(
            RedshiftSink::new(c).await,
            Err(FaucetError::Config(_))
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
