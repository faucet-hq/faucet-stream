//! The Microsoft SQL Server CDC [`Source`] implementation.
//!
//! Polls native SQL Server change data capture: for each configured capture
//! instance it reads `sys.fn_cdc_get_max_lsn()` / `sys.fn_cdc_get_min_lsn()` for
//! the retained range, then streams `cdc.fn_cdc_get_all_changes_<ci>(from, to,
//! 'all')` in commit order, buffering by commit LSN (`__$start_lsn`) so a single
//! transaction is never split across a bookmark boundary — mirroring the
//! per-transaction durability contract of postgres-cdc / mysql-cdc.
//!
//! **Resumability.** The durable bookmark is a map of capture-instance → last
//! committed LSN (hex). On resume the next poll starts at `increment(bookmark)`,
//! so an already-committed change is never re-read. Each emitted page carries
//! the whole updated map, so the pipeline's single state key stays intact.
//!
//! **Exactly-once.** LSNs are a durable, monotonic, deterministic replay
//! coordinate and each committed transaction is its own page with its own
//! bookmark, so [`Source::supports_exactly_once`] is `true`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use tiberius::{QueryItem, ToSql};

use faucet_common_mssql::{MssqlPool, MssqlPooledConnection, build_pool, with_statement_timeout};

use crate::change::{
    LSN_ALIAS, OP_COLUMN, OpAction, PollPlan, SEQVAL_ALIAS, build_change_envelope,
    business_columns, op_action, plan_poll,
};
use crate::config::MssqlCdcSourceConfig;
use crate::decode::row_to_json;
use crate::lsn::Lsn;
use crate::state::Bookmarks;

/// A configured Microsoft SQL Server CDC source.
pub struct MssqlCdcSource {
    config: MssqlCdcSourceConfig,
    pool: MssqlPool,
    state_key_value: String,
    /// capture_instance -> (source schema, source table), resolved at build time
    /// from `cdc.change_tables`. Used to stamp `schema`/`table` on envelopes.
    tables: HashMap<String, (String, String)>,
    /// Bookmark provided by [`Source::apply_start_bookmark`], consumed at the
    /// start of the next fetch cycle.
    pending_bookmark: Mutex<Option<Bookmarks>>,
    /// The bookmark map last handed to the pipeline this run — where
    /// [`Source::lag`] measures from (#733).
    emitted: Mutex<Option<Bookmarks>>,
}

impl MssqlCdcSource {
    /// Connect, validate the config, build the pool, and run the CDC preflight
    /// (verify CDC is enabled on the database and every configured capture
    /// instance exists).
    pub async fn new(config: MssqlCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool = build_pool(&config.connection, config.max_connections).await?;
        let state_key_value = config.resolved_state_key();

        let mut conn = pool
            .get()
            .await
            .map_err(|e| FaucetError::Source(format!("mssql-cdc: pool checkout failed: {e}")))?;

        // Preflight: CDC must be enabled on the database.
        let (db_name, cdc_enabled) = fetch_db_cdc_status(&mut conn).await?;
        if !cdc_enabled {
            return Err(FaucetError::Source(format!(
                "mssql-cdc: change data capture is not enabled on database {db_name:?}; \
                 run `EXEC sys.sp_cdc_enable_db;` (requires sysadmin)"
            )));
        }

        // Preflight: every configured capture instance must exist.
        let tables = fetch_change_tables(&mut conn).await?;
        let missing: Vec<&str> = config
            .capture_instances
            .iter()
            .filter(|ci| !tables.contains_key(ci.as_str()))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(FaucetError::Source(format!(
                "mssql-cdc: capture instance(s) {missing:?} not found in cdc.change_tables on \
                 database {db_name:?}; enable them with \
                 `EXEC sys.sp_cdc_enable_table @source_schema=..., @source_name=..., \
                 @role_name=NULL, @capture_instance=...;`"
            )));
        }
        drop(conn);

        Ok(Self {
            config,
            pool,
            state_key_value,
            tables,
            pending_bookmark: Mutex::new(None),
            emitted: Mutex::new(None),
        })
    }

    fn timeout(&self) -> Option<Duration> {
        match self.config.statement_timeout_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        }
    }
}

#[async_trait]
impl Source for MssqlCdcSource {
    /// Drain a single fetch cycle into a flat `Vec` using the `batch_size = 0`
    /// aggregate sentinel (matches the convenience-API contract).
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages_impl(ctx, 0);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    /// Per-transaction streaming. Each committed transaction is emitted as its
    /// own [`StreamPage`] with `bookmark = Some(map)`. The trait-level
    /// `batch_size` argument is ignored in favour of the config field.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(MssqlCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let marks = Bookmarks::from_value(bookmark)?;
        *self
            .pending_bookmark
            .lock()
            .expect("pending_bookmark mutex poisoned") = Some(marks);
        Ok(())
    }

    /// Unread change transactions and the age of the oldest (#733), measured
    /// from the capture instance furthest behind.
    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let marks = {
            let emitted = self.emitted.lock().expect("emitted mutex poisoned").clone();
            emitted.or_else(|| {
                self.pending_bookmark
                    .lock()
                    .expect("pending_bookmark mutex poisoned")
                    .clone()
            })
        };
        let Some(from) = marks.and_then(|m| slowest_position(&m, &self.config.capture_instances))
        else {
            return Ok(None);
        };
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| FaucetError::Source(format!("mssql-cdc: lag checkout failed: {e}")))?;
        const SQL: &str = "DECLARE @from BINARY(10) = CONVERT(BINARY(10), @P1, 2); \
             DECLARE @max BINARY(10) = sys.fn_cdc_get_max_lsn(); \
             SELECT COUNT_BIG(*) AS pending, \
                    DATEDIFF_BIG(millisecond, MIN(tran_begin_time), SYSDATETIME()) AS age_ms \
             FROM cdc.lsn_time_mapping \
             WHERE start_lsn > @from AND start_lsn <= @max AND tran_id <> 0x00";
        let hex = from.to_hex();
        let p: &dyn ToSql = &hex;
        let rows = self.run_collect(&mut conn, SQL, &[p]).await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let pending: i64 = row
            .try_get::<i64, _>("pending")
            .map_err(|e| FaucetError::Source(format!("mssql-cdc: lag decode failed: {e}")))?
            .unwrap_or(0);
        let age_ms: Option<i64> = row
            .try_get::<i64, _>("age_ms")
            .map_err(|e| FaucetError::Source(format!("mssql-cdc: lag decode failed: {e}")))?;
        Ok(Some(faucet_core::SourceLag {
            bytes: None,
            events: Some(pending.max(0) as u64),
            seconds: Some(age_ms.map(|ms| ms.max(0) as f64 / 1000.0).unwrap_or(0.0)),
        }))
    }

    /// Capture the database's current max LSN as a bookmark for every configured
    /// capture instance, without consuming any changes. Used by
    /// `faucet replicate` to anchor CDC before a bulk snapshot (#189).
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            FaucetError::Source(format!("mssql-cdc: capture_position checkout failed: {e}"))
        })?;
        let max_lsn = match self.query_max_lsn(&mut conn).await? {
            Some(lsn) => lsn,
            // No CDC activity yet: no position to anchor. A fresh CDC run will
            // start from `current` at first poll.
            None => return Ok(None),
        };
        let mut marks = Bookmarks::new();
        for ci in &self.config.capture_instances {
            marks.set(ci.clone(), max_lsn);
        }
        Ok(Some(marks.to_value()?))
    }

    fn supports_exactly_once(&self) -> bool {
        true
    }

    fn record_table(&self, record: &Value) -> Option<String> {
        schema_table(record)
    }

    fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
        crate::state::bookmarks_le(a, b)
    }

    fn position_min(&self, positions: &[Value]) -> Option<Value> {
        crate::state::bookmarks_min(positions)
    }

    fn connector_name(&self) -> &'static str {
        "mssql-cdc"
    }

    fn dataset_uri(&self) -> String {
        let conn = self
            .config
            .connection
            .connection_url
            .as_deref()
            .or(self.config.connection.connection_string.as_deref())
            .unwrap_or("");
        format!(
            "{}?capture_instances={}",
            faucet_core::redact_uri_credentials(conn),
            self.config.capture_instances.join(",")
        )
    }

    /// Preflight probe for `faucet doctor`: connection, CDC-enabled, and
    /// capture-instances-exist, without opening any change stream.
    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let start = Instant::now();

        let probe_result = tokio::time::timeout(ctx.timeout, async {
            let mut conn = self.pool.get().await.map_err(|e| {
                Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    format!("could not check out a connection: {e}"),
                    "verify connection_url / credentials / TLS and that the server is reachable",
                )
            })?;
            let connection = Probe::pass("connection", start.elapsed());

            let cdc = match fetch_db_cdc_status(&mut conn).await {
                Ok((_db, true)) => Probe::pass("cdc-enabled", start.elapsed()),
                Ok((db, false)) => Probe::fail_hint(
                    "cdc-enabled",
                    start.elapsed(),
                    format!("CDC is not enabled on database {db:?}"),
                    "run `EXEC sys.sp_cdc_enable_db;` (requires sysadmin)",
                ),
                Err(e) => Probe::fail_hint(
                    "cdc-enabled",
                    start.elapsed(),
                    e.to_string(),
                    "the CDC status query failed — check permissions on sys.databases",
                ),
            };

            let instances = match fetch_change_tables(&mut conn).await {
                Ok(tables) => {
                    let missing: Vec<&str> = self
                        .config
                        .capture_instances
                        .iter()
                        .filter(|ci| !tables.contains_key(ci.as_str()))
                        .map(String::as_str)
                        .collect();
                    if missing.is_empty() {
                        Probe::pass("capture-instances", start.elapsed())
                    } else {
                        Probe::fail_hint(
                            "capture-instances",
                            start.elapsed(),
                            format!("capture instance(s) not found: {missing:?}"),
                            "enable them with `EXEC sys.sp_cdc_enable_table ...`",
                        )
                    }
                }
                Err(e) => Probe::fail_hint(
                    "capture-instances",
                    start.elapsed(),
                    e.to_string(),
                    "reading cdc.change_tables failed — check CDC is enabled and permissions",
                ),
            };

            Ok::<Vec<Probe>, Probe>(vec![connection, cdc, instances])
        })
        .await;

        match probe_result {
            Ok(Ok(probes)) => Ok(CheckReport { probes }),
            Ok(Err(probe)) => Ok(CheckReport::single(probe)),
            Err(_elapsed) => Ok(CheckReport::single(Probe::fail_hint(
                "connection",
                start.elapsed(),
                "connection timed out",
                "the database did not respond within the check timeout",
            ))),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Metadata queries (I/O)
// ──────────────────────────────────────────────────────────────────────────────

impl MssqlCdcSource {
    fn note_emitted(&self, marks: &Bookmarks) -> Result<Value, FaucetError> {
        *self.emitted.lock().expect("emitted mutex poisoned") = Some(marks.clone());
        marks.to_value()
    }

    /// Read the database's current maximum LSN (`None` when CDC has produced no
    /// changes yet).
    async fn query_max_lsn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
    ) -> Result<Option<Lsn>, FaucetError> {
        const SQL: &str = "SELECT CONVERT(VARCHAR(20), sys.fn_cdc_get_max_lsn(), 2) AS max_lsn";
        let rows = self.run_collect(conn, SQL, &[]).await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        opt_lsn(row, "max_lsn")
    }

    /// Read a capture instance's retained `(min, max)` LSN range. Either may be
    /// `None` (no changes retained / no changes at all).
    async fn query_lsn_bounds(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        capture_instance: &str,
    ) -> Result<(Option<Lsn>, Option<Lsn>), FaucetError> {
        const SQL: &str = "SELECT CONVERT(VARCHAR(20), sys.fn_cdc_get_min_lsn(@P1), 2) AS min_lsn, \
                                  CONVERT(VARCHAR(20), sys.fn_cdc_get_max_lsn(), 2) AS max_lsn";
        // Bind an owned String (guaranteed `ToSql`) for the capture-instance
        // name — never interpolate it into the SQL text.
        let ci_owned = capture_instance.to_string();
        let ci: &dyn ToSql = &ci_owned;
        let rows = self.run_collect(conn, SQL, &[ci]).await?;
        let Some(row) = rows.first() else {
            return Ok((None, None));
        };
        Ok((opt_lsn(row, "min_lsn")?, opt_lsn(row, "max_lsn")?))
    }

    /// Run a query and collect its first result set, honouring the statement
    /// timeout.
    async fn run_collect(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Vec<tiberius::Row>, FaucetError> {
        let run = async {
            conn.query(sql, params)
                .await
                .map_err(|e| FaucetError::Source(format!("mssql-cdc: query failed: {e}")))?
                .into_first_result()
                .await
                .map_err(|e| FaucetError::Source(format!("mssql-cdc: result read failed: {e}")))
        };
        match self.timeout() {
            Some(t) => {
                with_statement_timeout(t, run, || {
                    FaucetError::Source("mssql-cdc: query timed out".into())
                })
                .await
            }
            None => run.await,
        }
    }
}

/// Read `(DB_NAME(), is_cdc_enabled)` for the connected database.
async fn fetch_db_cdc_status(
    conn: &mut MssqlPooledConnection<'_>,
) -> Result<(String, bool), FaucetError> {
    const SQL: &str = "SELECT DB_NAME() AS db, \
        CONVERT(INT, is_cdc_enabled) AS enabled FROM sys.databases WHERE database_id = DB_ID()";
    let rows = conn
        .query(SQL, &[])
        .await
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: CDC-status query failed: {e}")))?
        .into_first_result()
        .await
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: CDC-status read failed: {e}")))?;
    let Some(row) = rows.first() else {
        return Err(FaucetError::Source(
            "mssql-cdc: could not resolve the current database (sys.databases returned no row)"
                .into(),
        ));
    };
    let db = row
        .try_get::<&str, _>("db")
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: DB_NAME decode failed: {e}")))?
        .unwrap_or("")
        .to_string();
    let enabled = row
        .try_get::<i32, _>("enabled")
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: is_cdc_enabled decode failed: {e}")))?
        .unwrap_or(0)
        != 0;
    Ok((db, enabled))
}

/// Read every capture instance visible on the database, mapping it to its source
/// `(schema, table)`.
async fn fetch_change_tables(
    conn: &mut MssqlPooledConnection<'_>,
) -> Result<HashMap<String, (String, String)>, FaucetError> {
    const SQL: &str = "SELECT ct.capture_instance AS ci, s.name AS src_schema, o.name AS src_table \
        FROM cdc.change_tables ct \
        JOIN sys.objects o ON o.object_id = ct.source_object_id \
        JOIN sys.schemas s ON s.schema_id = o.schema_id";
    let rows = conn
        .query(SQL, &[])
        .await
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: change_tables query failed: {e}")))?
        .into_first_result()
        .await
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: change_tables read failed: {e}")))?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in &rows {
        let get = |col: &str| -> Result<String, FaucetError> {
            row.try_get::<&str, _>(col)
                .map_err(|e| {
                    FaucetError::Source(format!("mssql-cdc: change_tables decode ({col}): {e}"))
                })?
                .map(str::to_string)
                .ok_or_else(|| FaucetError::Source(format!("mssql-cdc: change_tables null {col}")))
        };
        map.insert(get("ci")?, (get("src_schema")?, get("src_table")?));
    }
    Ok(map)
}

/// Read an optional LSN column (a hex string or SQL NULL) from a row.
/// The lowest committed LSN across the configured capture instances — the
/// one furthest behind. `None` until every configured instance has a position.
fn slowest_position(marks: &Bookmarks, capture_instances: &[String]) -> Option<Lsn> {
    capture_instances
        .iter()
        .map(|ci| marks.get(ci))
        .collect::<Option<Vec<Lsn>>>()?
        .into_iter()
        .min()
}

fn opt_lsn(row: &tiberius::Row, col: &str) -> Result<Option<Lsn>, FaucetError> {
    match row
        .try_get::<&str, _>(col)
        .map_err(|e| FaucetError::Source(format!("mssql-cdc: {col} decode failed: {e}")))?
    {
        Some(hex) => Ok(Some(Lsn::from_hex(hex)?)),
        None => Ok(None),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Stream loop
// ──────────────────────────────────────────────────────────────────────────────

impl MssqlCdcSource {
    fn stream_pages_impl<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let per_transaction = batch_size != 0;
        let poll_interval = self.config.poll_interval;
        let idle_timeout = self.config.idle_timeout;
        let start_position = self.config.start_position;
        let max_staged = self.config.max_staged_records;

        Box::pin(async_stream::try_stream! {
            // Resolve the starting bookmark map for this cycle.
            let mut marks = self
                .pending_bookmark
                .lock()
                .expect("pending_bookmark mutex poisoned")
                .take()
                .unwrap_or_default();

            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| FaucetError::Source(format!("mssql-cdc: pool checkout failed: {e}")))?;

            // Aggregate-mode accumulator (batch_size == 0).
            let mut agg: Vec<Value> = Vec::new();
            let mut agg_dirty = false;

            let mut last_activity = Instant::now();

            loop {
                let mut any_rows = false;

                for ci in &self.config.capture_instances {
                    let (schema, table) = self
                        .tables
                        .get(ci)
                        .cloned()
                        .unwrap_or_else(|| ("".to_string(), ci.clone()));

                    let (min_lsn, max_lsn) = self.query_lsn_bounds(&mut conn, ci).await?;
                    let plan = plan_poll(marks.get(ci), min_lsn, max_lsn, start_position);

                    match plan {
                        PollPlan::NoChanges { set_bookmark } => {
                            // Fresh `current` start: anchor the bookmark at the
                            // live max and persist it so history is skipped.
                            if let Some(anchor) = set_bookmark
                                && marks.get(ci).is_none()
                            {
                                marks.set(ci.clone(), anchor);
                                if per_transaction {
                                    yield StreamPage {
                                        records: Vec::new(),
                                        bookmark: Some(self.note_emitted(&marks)?),
                                    };
                                } else {
                                    agg_dirty = true;
                                }
                            }
                        }
                        PollPlan::Query { from, to, gap } => {
                            if gap {
                                crate::change::check_gap(self.config.on_gap, ci, &from.to_hex())?;
                            }

                            let sql = changes_sql(ci);
                            let from_hex = from.to_hex();
                            let to_hex = to.to_hex();

                            // Open the change stream (timeout only wraps opening
                            // it; the QueryStream borrows `conn`, not the params).
                            let mut stream = {
                                // `params` is a named local so it outlives the
                                // `.await` below (tiberius borrows the params
                                // only until the query future resolves, not for
                                // the returned QueryStream's lifetime).
                                let params: [&dyn ToSql; 2] = [&from_hex, &to_hex];
                                let query_fut = conn.query(&sql, &params);
                                match self.timeout() {
                                    Some(t) => {
                                        with_statement_timeout(t, async {
                                            query_fut.await.map_err(|e| {
                                                FaucetError::Source(format!(
                                                    "mssql-cdc: get_all_changes failed for {ci}: {e}"
                                                ))
                                            })
                                        }, || FaucetError::Source(
                                            "mssql-cdc: get_all_changes timed out".into()
                                        ))
                                        .await?
                                    }
                                    None => query_fut.await.map_err(|e| {
                                        FaucetError::Source(format!(
                                            "mssql-cdc: get_all_changes failed for {ci}: {e}"
                                        ))
                                    })?,
                                }
                            };

                            let mut buffer: Vec<Value> = Vec::new();
                            let mut cur_lsn: Option<Lsn> = None;

                            while let Some(item) = stream.try_next().await.map_err(|e| {
                                FaucetError::Source(format!(
                                    "mssql-cdc: change row stream failed for {ci}: {e}"
                                ))
                            })? {
                                let QueryItem::Row(row) = item else { continue };
                                let decoded = row_to_json(&row)?;

                                let lsn_hex = decoded
                                    .get(LSN_ALIAS)
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| FaucetError::Source(
                                        "mssql-cdc: change row missing __$start_lsn".into()
                                    ))?;
                                let row_lsn = Lsn::from_hex(lsn_hex)?;
                                let seqval_hex = decoded
                                    .get(SEQVAL_ALIAS)
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let op_code = decoded
                                    .get(OP_COLUMN)
                                    .and_then(Value::as_i64)
                                    .ok_or_else(|| FaucetError::Source(
                                        "mssql-cdc: change row missing __$operation".into()
                                    ))?;

                                // Commit boundary: a new __$start_lsn closes the
                                // previous transaction. Emit it bookmarked at the
                                // completed commit LSN (a safe resume point).
                                if let Some(prev) = cur_lsn
                                    && prev != row_lsn
                                {
                                    marks.set(ci.clone(), prev);
                                    let recs = std::mem::take(&mut buffer);
                                    if per_transaction {
                                        yield StreamPage {
                                            records: recs,
                                            bookmark: Some(self.note_emitted(&marks)?),
                                        };
                                    } else {
                                        agg.extend(recs);
                                        agg_dirty = true;
                                    }
                                }
                                cur_lsn = Some(row_lsn);

                                match op_action(op_code)? {
                                    OpAction::Skip => continue,
                                    OpAction::Emit(op) => {
                                        if let Some(max) = max_staged
                                            && buffer.len() >= max
                                        {
                                            Err(FaucetError::Source(format!(
                                                "mssql-cdc: in-progress transaction for {ci} exceeded \
                                                 max_staged_records ({max}); aborting to avoid \
                                                 unbounded memory growth. Raise max_staged_records \
                                                 or reduce the source transaction size."
                                            )))?;
                                        }
                                        let cols = business_columns(&decoded);
                                        let env = build_change_envelope(
                                            op,
                                            &schema,
                                            &table,
                                            lsn_hex,
                                            seqval_hex.as_deref(),
                                            cols,
                                        );
                                        buffer.push(env);
                                        any_rows = true;
                                    }
                                }
                            }
                            // Drop the change stream (release the conn borrow) by
                            // ending the while-loop scope, then flush the tail.
                            drop(stream);

                            // Final flush: advance to `to` (everything <= to is
                            // consumed) so we never re-scan this range.
                            marks.set(ci.clone(), to);
                            let recs = std::mem::take(&mut buffer);
                            if per_transaction {
                                yield StreamPage {
                                    records: recs,
                                    bookmark: Some(self.note_emitted(&marks)?),
                                };
                            } else {
                                agg.extend(recs);
                                agg_dirty = true;
                            }
                        }
                    }
                }

                if any_rows {
                    last_activity = Instant::now();
                }

                // In aggregate mode we still poll the whole idle window, then
                // emit a single trailing page below.
                if last_activity.elapsed() >= idle_timeout {
                    break;
                }
                tokio::time::sleep(poll_interval).await;
            }

            // Aggregate mode: one trailing page with everything and the final map.
            if !per_transaction && (agg_dirty || !agg.is_empty()) {
                yield StreamPage {
                    records: std::mem::take(&mut agg),
                    bookmark: Some(self.note_emitted(&marks)?),
                };
            }

            tracing::info!(
                connector = "mssql-cdc",
                state_key = %self.state_key_value,
                "mssql-cdc fetch cycle complete",
            );
        })
    }
}

/// Build the `fn_cdc_get_all_changes` query for one (already validated) capture
/// instance. The commit LSN and sequence value are surfaced as hex-string
/// aliases; the bind markers `@P1`/`@P2` carry the `from`/`to` LSN hex.
fn changes_sql(capture_instance: &str) -> String {
    format!(
        "SELECT CONVERT(VARCHAR(20), __$start_lsn, 2) AS {lsn}, \
                CONVERT(VARCHAR(20), __$seqval, 2) AS {seq}, * \
         FROM cdc.fn_cdc_get_all_changes_{ci}(\
                CONVERT(BINARY(10), @P1, 2), CONVERT(BINARY(10), @P2, 2), N'all') \
         ORDER BY __$start_lsn, __$seqval, __$operation",
        lsn = LSN_ALIAS,
        seq = SEQVAL_ALIAS,
        ci = capture_instance,
    )
}

/// `schema.table` of a change envelope, the name the `mssql` source's
/// discovery reports for the same table.
fn schema_table(record: &Value) -> Option<String> {
    let schema = record.get("schema")?.as_str()?;
    let table = record.get("table")?.as_str()?;
    Some(format!("{schema}.{table}"))
}

#[cfg(test)]
mod tests {

    #[test]
    fn routes_by_schema_table() {
        assert_eq!(
            schema_table(&serde_json::json!({"schema": "dbo", "table": "Orders"})),
            Some("dbo.Orders".into())
        );
        assert_eq!(schema_table(&serde_json::json!({"schema": "dbo"})), None);
    }

    #[test]
    fn slowest_position_waits_for_every_instance() {
        let a = Lsn::from_hex("0000002a000000550003").unwrap();
        let b = Lsn::from_hex("0000002a000000560001").unwrap();
        let mut m = Bookmarks::new();
        m.set("dbo_a", b);
        let cis = vec!["dbo_a".to_string(), "dbo_b".to_string()];
        assert_eq!(slowest_position(&m, &cis), None);
        m.set("dbo_b", a);
        assert_eq!(slowest_position(&m, &cis), Some(a));
        assert_eq!(slowest_position(&m, &[]), None);
    }

    use super::*;

    #[test]
    fn changes_sql_embeds_validated_instance_and_aliases() {
        let sql = changes_sql("dbo_Orders");
        assert!(
            sql.contains("cdc.fn_cdc_get_all_changes_dbo_Orders("),
            "{sql}"
        );
        assert!(sql.contains("AS __faucet_lsn"), "{sql}");
        assert!(sql.contains("AS __faucet_seqval"), "{sql}");
        assert!(sql.contains("N'all'"), "{sql}");
        assert!(
            sql.contains("ORDER BY __$start_lsn, __$seqval, __$operation"),
            "{sql}"
        );
        // Bounds are bound, never interpolated.
        assert!(sql.contains("@P1") && sql.contains("@P2"), "{sql}");
    }
}
