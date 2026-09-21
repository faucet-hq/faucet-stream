//! The ClickHouse [`Sink`] implementation — HTTP client and batched
//! `INSERT … FORMAT JSONEachRow` writes.

use async_trait::async_trait;
use faucet_common_clickhouse::{apply_auth, build_client, build_json_each_row, query_params};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{FaucetError, Sink};
use serde_json::Value;

use crate::config::ClickHouseSinkConfig;

/// ClickHouse sink (HTTP interface, `INSERT … FORMAT JSONEachRow`).
pub struct ClickHouseSink {
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    pub(crate) table_ready: std::sync::atomic::AtomicBool,
    /// Records accumulated across `write_batch` calls (#617).
    ///
    /// ClickHouse creates one MergeTree part per insert, so an insert per
    /// small page is not merely slow — it trips "too many parts", a hard
    /// failure. Merging pages is the fix the engine actually wants.
    pub(crate) pending: tokio::sync::Mutex<faucet_core::PageAccumulator>,
    pub(crate) config: ClickHouseSinkConfig,
    pub(crate) client: reqwest::Client,
    /// Resolved once in [`ClickHouseSink::new`] so the hot path never re-parses.
    pub(crate) base_url: String,
    /// Per-sink run id for staged-object keys (avoids cross-run collisions).
    #[cfg(feature = "staging")]
    stage_run_id: String,
    /// Monotonic part counter for staged objects within this sink.
    #[cfg(feature = "staging")]
    stage_seq: std::sync::atomic::AtomicUsize,
}

/// Quote a (possibly schema-qualified) table name. Each `.`-separated segment
/// is quoted with [`faucet_core::util::quote_ident`] so `db.table` becomes
/// `"db"."table"` — safe against identifier injection while preserving the
/// database/table split.
fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(faucet_core::util::quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Map a [`faucet_core::SqlBaseType`] to the ClickHouse column type used when
/// auto-creating a table (#580).
///
/// Every column is `Nullable(…)`: ClickHouse rejects a null into a
/// non-nullable column, so a type inferred from page 1 that happened to have
/// no nulls would fail page 2 the first time a field is absent. `String` is
/// the landing type for nested values, matching the `JSONEachRow` body the
/// writer sends.
fn clickhouse_type(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "Nullable(Int64)",
        Double => "Nullable(Float64)",
        Boolean => "Nullable(Bool)",
        Text | Json => "Nullable(String)",
    }
}

/// `CREATE TABLE IF NOT EXISTS … ENGINE = MergeTree ORDER BY tuple()` (#580).
///
/// `ORDER BY tuple()` is the neutral sort key — faucet has no basis to pick
/// one, and guessing wrong bakes a bad primary index into the table. An
/// operator who cares about the sort key defines the table and sets
/// `create_table: false`.
fn build_create_table_sql(table: &str, columns: &[faucet_core::PlannedColumn]) -> String {
    let cols =
        faucet_core::render_columns(columns, faucet_core::util::quote_ident, clickhouse_type);
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({cols}) ENGINE = MergeTree ORDER BY tuple()",
        quote_table(table)
    )
}

/// Build the `INSERT … FORMAT JSONEachRow` statement (carried in the `query`
/// URL parameter; the row data travels in the request body).
fn insert_statement(table: &str) -> String {
    format!("INSERT INTO {} FORMAT JSONEachRow", quote_table(table))
}

/// Build the ordered query parameters for an insert request: the `database`,
/// any async-insert settings, and the `query` statement. Pure and
/// unit-testable.
fn insert_params(
    database: &str,
    statement: &str,
    async_insert: bool,
    wait_for_async_insert: bool,
) -> Vec<(String, String)> {
    let mut settings: Vec<(&str, &str)> = Vec::new();
    if async_insert {
        settings.push(("async_insert", "1"));
        settings.push((
            "wait_for_async_insert",
            if wait_for_async_insert { "1" } else { "0" },
        ));
    }
    settings.push(("query", statement));
    query_params(database, &settings)
}

impl ClickHouseSink {
    /// Insert one accumulated group, still re-chunked to `batch_size` so a
    /// single HTTP request stays a reasonable size (#617).
    async fn commit_group(&self, rows: &[Value]) -> Result<(), FaucetError> {
        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![rows]
        } else {
            rows.chunks(self.config.batch_size).collect()
        };
        for chunk in chunks {
            self.send_insert(chunk).await?;
            tracing::debug!(records = chunk.len(), "ClickHouse insert chunk written");
        }
        Ok(())
    }

    /// Make sure the target table exists before the first write (#580).
    async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.config.create_table {
            // Nothing to probe against cheaply that `EXISTS TABLE` doesn't
            // already answer; a missing table then surfaces from the INSERT
            // itself. What this branch guarantees is that faucet does not
            // create one behind the operator's back.
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        // A page with nothing inferable leaves the table uncreated so the next
        // page can try, rather than emitting a zero-column CREATE.
        let Some(columns) = faucet_core::plan_columns(records) else {
            return Ok(());
        };
        let sql = build_create_table_sql(&self.config.table, &columns);
        self.execute_statement(&sql).await?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Run one DDL/DML statement through the HTTP interface.
    async fn execute_statement(&self, sql: &str) -> Result<(), FaucetError> {
        // The statement rides the **body**, not the `query` param: a POST with
        // no body has neither `Content-Length` nor chunked encoding, which
        // ClickHouse rejects with HTTP 411. Sending the SQL as the body is
        // also the shape that avoids URL-length limits on a long statement.
        let params: Vec<(String, String)> =
            vec![("database".into(), self.config.connection.database.clone())];
        let req = self
            .client
            .post(&self.base_url)
            .query(&params)
            .body(sql.to_string());
        let req = apply_auth(req, &self.config.connection);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// Validate the config and build the reusable HTTP client.
    pub fn new(config: ClickHouseSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let base_url = config.connection.base_url()?;
        let client = build_client(&config.connection)?;
        Ok(Self {
            table_ready: std::sync::atomic::AtomicBool::new(false),
            pending: tokio::sync::Mutex::new(faucet_core::PageAccumulator::new(
                config.commit_rows,
                config.commit_bytes,
            )),
            config,
            client,
            base_url,
            #[cfg(feature = "staging")]
            stage_run_id: crate::staged::new_stage_run_id(),
            #[cfg(feature = "staging")]
            stage_seq: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Stage the page to `uploader`'s store and build the `INSERT … SELECT FROM
    /// s3()/gcs()` statement. Split out from [`Self::write_batch_staged`] so it
    /// can be tested against an in-memory object store (the network execution is
    /// the only untested part).
    #[cfg(feature = "staging")]
    pub(crate) async fn stage_and_build_sql(
        &self,
        uploader: &faucet_core::staging::StageUploader,
        records: &[Value],
        staging: &crate::config::ClickHouseStagingConfig,
    ) -> Result<(faucet_core::staging::StagedFile, String), FaucetError> {
        use crate::staged::{clickhouse_stage_insert_sql, staged_https_url};
        use std::sync::atomic::Ordering;

        let loc = uploader.location().clone();
        let seq = self.stage_seq.fetch_add(1, Ordering::Relaxed);
        let staged = uploader
            .stage_page(
                &staging.spec,
                &self.config.table,
                &self.stage_run_id,
                seq,
                records,
                None,
            )
            .await?;

        let url = staged_https_url(
            loc.scheme,
            &loc.bucket,
            &staged.key,
            staging.region.as_deref(),
            staging.endpoint.as_deref(),
        )?;
        let creds = staging
            .access_key
            .as_deref()
            .zip(staging.secret_key.as_deref());
        let sql = clickhouse_stage_insert_sql(
            &quote_table(&self.config.table),
            loc.scheme,
            &url,
            creds,
            staging.spec.format,
        )?;
        Ok((staged, sql))
    }

    /// Send one `INSERT … FORMAT JSONEachRow` request for a slice of records.
    async fn send_insert(&self, records: &[Value]) -> Result<(), FaucetError> {
        let body = build_json_each_row(records)?;
        let statement = insert_statement(&self.config.table);
        let params = insert_params(
            &self.config.connection.database,
            &statement,
            self.config.async_insert,
            self.config.wait_for_async_insert,
        );
        let req = self.client.post(&self.base_url).query(&params).body(body);
        let req = apply_auth(req, &self.config.connection);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    /// Insert records via `INSERT … FORMAT JSONEachRow`.
    ///
    /// When `batch_size > 0` and the page is larger, it is split into
    /// `batch_size`-row chunks, each sent as its own request. `batch_size = 0`
    /// forwards the whole page as a single request.
    /// Commit whatever the cross-page accumulator holds (#617).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so a buffered group is always committed before the bookmark
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

        // Staged bulk load (#528): stage the whole page and let the server pull
        // it — no row body, no `batch_size` re-chunking.
        #[cfg(feature = "staging")]
        if let Some(staging) = &self.config.staging {
            return self.write_batch_staged(records, staging).await;
        }
        #[cfg(not(feature = "staging"))]
        if self.config.staging.is_some() {
            return Err(FaucetError::Config(
                "clickhouse: `staging:` is configured but this build lacks the `staging` \
                 feature — rebuild with `--features staging` (CLI: `sink-clickhouse-staging`)"
                    .into(),
            ));
        }

        // Accumulate across calls and insert once per threshold (#617): one
        // insert per small page creates one MergeTree part per page, which
        // ClickHouse rejects outright once they pile up.
        let group = {
            let mut pending = self.pending.lock().await;
            pending.push_page(records)
        };
        if let Some(rows) = group {
            self.commit_group(&rows).await?;
        }
        Ok(records.len())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ClickHouseSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "clickhouse"
    }

    /// Staged bulk load is active only when a `staging:` block is configured
    /// and the `staging` feature is compiled in.
    fn supports_staged_load(&self) -> bool {
        cfg!(feature = "staging") && self.config.staging.is_some()
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}",
            faucet_core::redact_uri_credentials(&self.base_url),
            self.config.table
        )
    }

    /// Non-mutating preflight probe (`connect`): runs `SELECT 1` over the HTTP
    /// interface. Deliberately does **not** touch the target table (no inserts,
    /// no residual rows).
    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let hint = "check url / host / database / credentials / that the server is reachable";
        let params = query_params(&self.config.connection.database, &[]);
        let req = self
            .client
            .post(&self.base_url)
            .query(&params)
            .body("SELECT 1");
        let req = apply_auth(req, &self.config.connection);
        let probe = match tokio::time::timeout(ctx.timeout, req.send()).await {
            Ok(Ok(resp)) => match check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                Ok(_) => Probe::pass("connect", started.elapsed()),
                Err(e) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            },
            Ok(Err(e)) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("connect", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(all(test, feature = "staging"))]
mod staging_tests {
    use super::*;
    use crate::config::ClickHouseStagingConfig;
    use faucet_core::staging::{StageUploader, StagingLocation};
    use serde_json::json;
    use std::sync::Arc;

    fn staging(location: &str) -> ClickHouseStagingConfig {
        ClickHouseStagingConfig {
            spec: serde_json::from_value(json!({
                "location": location,
                "format": "jsonl",
            }))
            .unwrap(),
            region: Some("us-east-1".into()),
            endpoint: None,
            access_key: Some("AKIA".into()),
            secret_key: Some("secret".into()),
        }
    }

    // Covers the staged upload + URL derivation + INSERT…SELECT FROM s3() build
    // against an in-memory object store (only the network send stays untested).
    #[tokio::test]
    async fn stage_and_build_sql_uploads_and_builds_s3_insert() {
        let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(
            "http://db.example.com:8123",
            "db.events",
        ))
        .unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let loc = StagingLocation::parse("s3://bucket/stage").unwrap();
        let uploader = StageUploader::new(store, loc);
        let cfg = staging("s3://bucket/stage");

        let records = vec![json!({"id": 1}), json!({"id": 2})];
        let (staged, sql) = sink
            .stage_and_build_sql(&uploader, &records, &cfg)
            .await
            .unwrap();

        assert_eq!(staged.rows, 2);
        assert!(sql.starts_with("INSERT INTO \"db\".\"events\" SELECT * FROM s3("));
        assert!(sql.contains("s3.us-east-1.amazonaws.com"));
        assert!(sql.contains("'AKIA', 'secret'"));
        assert!(sql.contains("'JSONEachRow'"));
    }

    #[test]
    fn supports_staged_load_reflects_config() {
        let mut c = ClickHouseSinkConfig::new("http://h:8123", "t");
        assert!(
            !ClickHouseSink::new(c.clone())
                .unwrap()
                .supports_staged_load()
        );
        c.staging = Some(staging("s3://b/p"));
        assert!(ClickHouseSink::new(c).unwrap().supports_staged_load());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink() -> ClickHouseSink {
        ClickHouseSink::new(ClickHouseSinkConfig::new(
            "http://db.example.com:8123",
            "events",
        ))
        .unwrap()
    }

    #[test]
    fn quote_table_quotes_each_segment() {
        assert_eq!(quote_table("events"), "\"events\"");
        assert_eq!(quote_table("analytics.events"), "\"analytics\".\"events\"");
    }

    #[test]
    fn quote_table_escapes_hostile_identifier() {
        // A double quote in the identifier is doubled by quote_ident, so it
        // cannot break out of the quoting.
        let q = quote_table("we\"ird");
        assert_eq!(q, "\"we\"\"ird\"");
    }

    #[test]
    fn insert_statement_uses_json_each_row() {
        assert_eq!(
            insert_statement("events"),
            "INSERT INTO \"events\" FORMAT JSONEachRow"
        );
    }

    #[test]
    fn insert_params_without_async_insert() {
        let params = insert_params("analytics", "INSERT INTO x FORMAT JSONEachRow", false, true);
        assert_eq!(params[0], ("database".to_string(), "analytics".to_string()));
        // Only database + query when async insert is off.
        assert_eq!(params.len(), 2);
        assert_eq!(
            params[1],
            (
                "query".to_string(),
                "INSERT INTO x FORMAT JSONEachRow".to_string()
            )
        );
        assert!(!params.iter().any(|(k, _)| k == "async_insert"));
    }

    #[test]
    fn insert_params_with_async_insert_and_wait() {
        let params = insert_params("db", "INSERT INTO x FORMAT JSONEachRow", true, true);
        assert!(params.contains(&("async_insert".to_string(), "1".to_string())));
        assert!(params.contains(&("wait_for_async_insert".to_string(), "1".to_string())));
    }

    #[test]
    fn insert_params_async_insert_no_wait() {
        let params = insert_params("db", "INSERT INTO x FORMAT JSONEachRow", true, false);
        assert!(params.contains(&("wait_for_async_insert".to_string(), "0".to_string())));
    }

    #[test]
    fn build_body_produces_ndjson() {
        // The body builder is shared with the source's decoder; assert the
        // exact wire bytes the sink would POST.
        let page = vec![json!({"id": 1}), json!({"id": 2})];
        assert_eq!(
            build_json_each_row(&page).unwrap(),
            "{\"id\":1}\n{\"id\":2}\n"
        );
    }

    #[test]
    fn dataset_uri_combines_base_url_and_table() {
        assert_eq!(sink().dataset_uri(), "http://db.example.com:8123/events");
    }

    #[test]
    fn connector_name_is_clickhouse() {
        assert_eq!(sink().connector_name(), "clickhouse");
    }

    #[test]
    fn config_schema_is_object() {
        assert_eq!(sink().config_schema()["type"], "object");
    }

    #[test]
    fn append_is_the_only_write_mode() {
        // ClickHouse upsert is engine-dependent (ReplacingMergeTree) and is not
        // emulated by the sink — see the crate README.
        assert_eq!(
            sink().supported_write_modes(),
            &[faucet_core::WriteMode::Append]
        );
    }

    #[test]
    fn new_rejects_invalid_config() {
        assert!(ClickHouseSink::new(ClickHouseSinkConfig::new("http://h:8123", "")).is_err());
    }

    #[tokio::test]
    async fn write_batch_empty_is_noop() {
        assert_eq!(sink().write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn check_fails_against_unreachable_server() {
        let sink =
            ClickHouseSink::new(ClickHouseSinkConfig::new("http://127.0.0.1:1", "t")).unwrap();
        let ctx = CheckContext {
            timeout: std::time::Duration::from_secs(2),
        };
        let report = sink.check(&ctx).await.unwrap();
        assert!(matches!(
            report.probes[0].status,
            faucet_core::check::ProbeStatus::Fail { .. }
        ));
    }

    #[test]
    fn create_table_sql_makes_every_column_nullable() {
        // ClickHouse rejects a null into a non-nullable column, so a type
        // inferred from a page that happened to have no nulls would fail the
        // first page that omits a field (#580).
        let cols = faucet_core::plan_columns(&[serde_json::json!({
            "id": 1, "name": "a", "amount": 1.5, "ok": true, "meta": {"k": 1}
        })])
        .expect("a plan");
        let sql = build_create_table_sql("analytics.events", &cols);
        assert!(sql.contains(r#""id" Nullable(Int64)"#), "{sql}");
        assert!(sql.contains(r#""name" Nullable(String)"#), "{sql}");
        assert!(sql.contains(r#""amount" Nullable(Float64)"#), "{sql}");
        assert!(sql.contains(r#""ok" Nullable(Bool)"#), "{sql}");
        assert!(sql.contains(r#""meta" Nullable(String)"#), "{sql}");
        assert!(sql.contains("IF NOT EXISTS"), "{sql}");
        // Each dotted segment is quoted separately, like every other
        // statement this sink builds.
        assert!(sql.contains(r#""analytics"."events""#), "{sql}");
    }

    #[test]
    fn create_table_sql_uses_a_neutral_sort_key() {
        // faucet has no basis to pick a sort key, and guessing wrong bakes a
        // bad primary index into the table an operator then has to migrate.
        let cols = faucet_core::plan_columns(&[serde_json::json!({ "id": 1 })]).expect("plan");
        let sql = build_create_table_sql("t", &cols);
        assert!(sql.contains("ENGINE = MergeTree ORDER BY tuple()"), "{sql}");
    }

    #[test]
    fn create_table_defaults_on() {
        let cfg = ClickHouseSinkConfig::new("http://localhost:8123", "t");
        assert!(cfg.create_table);
        assert!(!cfg.with_create_table(false).create_table);
    }
}
