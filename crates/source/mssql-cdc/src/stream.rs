//! The Microsoft SQL Server CDC [`Source`] implementation.
//!
//! Polls native SQL Server change data capture: for each configured capture
//! instance it reads `sys.fn_cdc_get_max_lsn()` / `sys.fn_cdc_get_min_lsn()` for
//! the retained range, then opens `cdc.fn_cdc_get_all_changes_<ci>(from, to,
//! 'all')` for every instance at once and merges the streams by
//! (`__$start_lsn`, `__$seqval`), buffering by commit LSN so a transaction —
//! including one that touches several captured tables — is emitted whole, in
//! its own order, and never split across a bookmark boundary.
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
    LSN_ALIAS, LsnBounds, OP_COLUMN, OpAction, PollPlan, SEQVAL_ALIAS, build_change_envelope,
    business_columns, op_action, plan_poll,
};
use crate::config::{MssqlCdcSourceConfig, OnGap, StartPosition};
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
        let pool = build_pool(&config.connection, config.pool_size()).await?;
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
    ) -> Result<LsnBounds, FaucetError> {
        const SQL: &str = "SELECT CONVERT(VARCHAR(20), sys.fn_cdc_get_min_lsn(@P1), 2) AS min_lsn, \
                                  CONVERT(VARCHAR(20), sys.fn_cdc_get_max_lsn(), 2) AS max_lsn, \
                                  (SELECT CONVERT(VARCHAR(20), start_lsn, 2) FROM cdc.change_tables \
                                   WHERE capture_instance = @P1) AS start_lsn";
        // Bind an owned String (guaranteed `ToSql`) for the capture-instance
        // name — never interpolate it into the SQL text.
        let ci_owned = capture_instance.to_string();
        let ci: &dyn ToSql = &ci_owned;
        let rows = self.run_collect(conn, SQL, &[ci]).await?;
        let Some(row) = rows.first() else {
            return Ok(LsnBounds::default());
        };
        Ok(LsnBounds {
            min: opt_lsn(row, "min_lsn")?,
            max: opt_lsn(row, "max_lsn")?,
            start: opt_lsn(row, "start_lsn")?,
        })
    }

    /// Open `fn_cdc_get_all_changes` for one capture instance (the statement
    /// timeout bounds opening it).
    async fn open_changes<'c>(
        &self,
        conn: &'c mut MssqlPooledConnection<'_>,
        q: &ChangeQuery,
    ) -> Result<tiberius::QueryStream<'c>, FaucetError> {
        let sql = changes_sql(&q.ci);
        let (from_hex, to_hex) = (q.from.to_hex(), q.to.to_hex());
        let params: [&dyn ToSql; 2] = [&from_hex, &to_hex];
        let failed = |e: tiberius::error::Error| {
            FaucetError::Source(format!(
                "mssql-cdc: get_all_changes failed for {}: {e}",
                q.ci
            ))
        };
        let query_fut = conn.query(&sql, &params);
        match self.timeout() {
            Some(t) => {
                with_statement_timeout(t, async { query_fut.await.map_err(failed) }, || {
                    FaucetError::Source("mssql-cdc: get_all_changes timed out".into())
                })
                .await
            }
            None => query_fut.await.map_err(failed),
        }
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

            // Aggregate-mode accumulator (batch_size == 0).
            let mut agg: Vec<Value> = Vec::new();
            let mut agg_dirty = false;

            let mut last_activity = Instant::now();

            loop {
                let mut any_rows = false;

                // Plan every capture instance on one connection, released
                // before the change streams open.
                let mut queries: Vec<ChangeQuery> = Vec::new();
                {
                    let mut conn = self.pool.get().await.map_err(|e| {
                        FaucetError::Source(format!("mssql-cdc: pool checkout failed: {e}"))
                    })?;
                    for ci in &self.config.capture_instances {
                        let bounds = self.query_lsn_bounds(&mut conn, ci).await?;
                        match plan_instance(ci, &self.tables, bounds, &mut marks, start_position, self.config.on_gap)? {
                            InstanceStep::Idle => {}
                            InstanceStep::Query(q) => queries.push(q),
                            InstanceStep::Anchored => {
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
                    }
                }

                if !queries.is_empty() {
                    // One connection per capture instance, so their change
                    // streams can be merged in commit order: a transaction that
                    // touches several tables is emitted whole, in its own order.
                    let mut conns = Vec::with_capacity(queries.len());
                    for _ in &queries {
                        conns.push(self.pool.get().await.map_err(|e| {
                            FaucetError::Source(format!("mssql-cdc: pool checkout failed: {e}"))
                        })?);
                    }
                    let mut streams = Vec::with_capacity(queries.len());
                    for (conn, q) in conns.iter_mut().zip(&queries) {
                        streams.push(self.open_changes(conn, q).await?);
                    }
                    let mut heads = Vec::with_capacity(streams.len());
                    for (stream, q) in streams.iter_mut().zip(&queries) {
                        heads.push(next_change(stream, &q.ci).await?);
                    }

                    let mut tx = TxAssembler::new(max_staged);
                    while let Some(i) = next_head(&heads) {
                        let change = heads[i].take().expect("next_head picks a present head");
                        heads[i] = next_change(&mut streams[i], &queries[i].ci).await?;
                        if let Some((prev, recs)) = tx.push(change, &queries[i])? {
                            advance_marks(&mut marks, &queries, prev);
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
                    any_rows |= tx.emitted;
                    drop(streams);
                    drop(conns);

                    consume_all(&mut marks, &queries);
                    let recs = tx.finish();
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

/// One capture instance's change query for a poll.
struct ChangeQuery {
    ci: String,
    schema: String,
    table: String,
    from: Lsn,
    to: Lsn,
}

/// One decoded change row.
struct Change {
    lsn: Lsn,
    lsn_hex: String,
    seqval_hex: Option<String>,
    op_code: i64,
    decoded: Value,
}

/// The next change row of one capture instance's stream.
async fn next_change(
    stream: &mut tiberius::QueryStream<'_>,
    ci: &str,
) -> Result<Option<Change>, FaucetError> {
    while let Some(item) = stream.try_next().await.map_err(|e| {
        FaucetError::Source(format!("mssql-cdc: change row stream failed for {ci}: {e}"))
    })? {
        let QueryItem::Row(row) = item else { continue };
        return decode_change(row_to_json(&row)?).map(Some);
    }
    Ok(None)
}

/// Pull the commit LSN, sequence value and operation out of a decoded row.
fn decode_change(decoded: Value) -> Result<Change, FaucetError> {
    let lsn_hex = decoded
        .get(LSN_ALIAS)
        .and_then(Value::as_str)
        .ok_or_else(|| FaucetError::Source("mssql-cdc: change row missing __$start_lsn".into()))?
        .to_string();
    let seqval_hex = decoded
        .get(SEQVAL_ALIAS)
        .and_then(Value::as_str)
        .map(str::to_string);
    let op_code = decoded
        .get(OP_COLUMN)
        .and_then(Value::as_i64)
        .ok_or_else(|| FaucetError::Source("mssql-cdc: change row missing __$operation".into()))?;
    Ok(Change {
        lsn: Lsn::from_hex(&lsn_hex)?,
        lsn_hex,
        seqval_hex,
        op_code,
        decoded,
    })
}

/// What one capture instance contributes to a poll.
enum InstanceStep {
    /// Nothing to read.
    Idle,
    /// A fresh `current` start anchored the instance's bookmark; persist it.
    Anchored,
    /// Read this range of changes.
    Query(ChangeQuery),
}

/// Plan one capture instance's poll from its LSN bounds, anchoring a fresh
/// `current` start in `marks` and applying the `on_gap` policy.
fn plan_instance(
    ci: &str,
    tables: &HashMap<String, (String, String)>,
    bounds: LsnBounds,
    marks: &mut Bookmarks,
    start: StartPosition,
    on_gap: OnGap,
) -> Result<InstanceStep, FaucetError> {
    match plan_poll(marks.get(ci), bounds, start) {
        PollPlan::NoChanges {
            set_bookmark: Some(anchor),
        } if marks.get(ci).is_none() => {
            marks.set(ci, anchor);
            Ok(InstanceStep::Anchored)
        }
        PollPlan::NoChanges { .. } => Ok(InstanceStep::Idle),
        PollPlan::Query { from, to, gap } => {
            if gap {
                crate::change::check_gap(on_gap, ci, &from.to_hex())?;
            }
            let (schema, table) = tables
                .get(ci)
                .cloned()
                .unwrap_or_else(|| (String::new(), ci.to_string()));
            Ok(InstanceStep::Query(ChangeQuery {
                ci: ci.to_string(),
                schema,
                table,
                from,
                to,
            }))
        }
    }
}

/// Groups merged changes into whole transactions by commit LSN.
struct TxAssembler {
    buffer: Vec<Value>,
    cur: Option<Lsn>,
    max_staged: Option<usize>,
    /// Whether any change was turned into a record.
    emitted: bool,
}

impl TxAssembler {
    fn new(max_staged: Option<usize>) -> Self {
        Self {
            buffer: Vec::new(),
            cur: None,
            max_staged,
            emitted: false,
        }
    }

    /// Feed the next change in commit order. When it starts a new commit LSN,
    /// the previous transaction is complete and is returned with its LSN.
    fn push(
        &mut self,
        change: Change,
        q: &ChangeQuery,
    ) -> Result<Option<(Lsn, Vec<Value>)>, FaucetError> {
        let closed = match self.cur {
            Some(prev) if prev != change.lsn => Some((prev, std::mem::take(&mut self.buffer))),
            _ => None,
        };
        self.cur = Some(change.lsn);
        if let OpAction::Emit(op) = op_action(change.op_code)? {
            if let Some(max) = self.max_staged
                && self.buffer.len() >= max
            {
                return Err(FaucetError::Source(format!(
                    "mssql-cdc: in-progress transaction exceeded \
                     max_staged_records ({max}); aborting to avoid \
                     unbounded memory growth. Raise max_staged_records \
                     or reduce the source transaction size."
                )));
            }
            self.buffer.push(build_change_envelope(
                op,
                &q.schema,
                &q.table,
                &change.lsn_hex,
                change.seqval_hex.as_deref(),
                business_columns(&change.decoded),
            ));
            self.emitted = true;
        }
        Ok(closed)
    }

    /// The records of the last, still-open transaction.
    fn finish(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.buffer)
    }
}

/// Everything up to each queried instance's `to` has been consumed.
fn consume_all(marks: &mut Bookmarks, queries: &[ChangeQuery]) {
    for q in queries {
        marks.set(q.ci.clone(), q.to);
    }
}

/// The stream whose head comes first in commit order: by commit LSN, then by
/// `__$seqval` (the change's position within its transaction, comparable
/// across tables because both are fixed-width hex).
fn next_head(heads: &[Option<Change>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|c| (i, c)))
        .min_by(|(_, a), (_, b)| {
            a.lsn
                .cmp(&b.lsn)
                .then_with(|| a.seqval_hex.cmp(&b.seqval_hex))
        })
        .map(|(i, _)| i)
}

/// Mark every queried instance as consumed through commit `lsn`, never moving
/// one backwards.
fn advance_marks(marks: &mut Bookmarks, queries: &[ChangeQuery], lsn: Lsn) {
    for q in queries {
        if marks.get(&q.ci).is_none_or(|m| m < lsn) {
            marks.set(q.ci.clone(), lsn);
        }
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

    fn change(lsn: &str, seq: &str) -> Change {
        Change {
            lsn: Lsn::from_hex(lsn).unwrap(),
            lsn_hex: lsn.into(),
            seqval_hex: Some(seq.into()),
            op_code: 2,
            decoded: serde_json::json!({}),
        }
    }

    fn lsn(hex: &str) -> Lsn {
        Lsn::from_hex(hex).unwrap()
    }

    fn query(ci: &str) -> ChangeQuery {
        ChangeQuery {
            ci: ci.into(),
            schema: "dbo".into(),
            table: ci.into(),
            from: lsn("00000000000000000001"),
            to: lsn("00000000000000000100"),
        }
    }

    fn row(lsn_hex: &str, seq: &str, op: i64, id: i64) -> Change {
        decode_change(serde_json::json!({
            LSN_ALIAS: lsn_hex,
            SEQVAL_ALIAS: seq,
            OP_COLUMN: op,
            "__$update_mask": "AQ==",
            "id": id,
        }))
        .unwrap()
    }

    #[test]
    fn decode_change_reads_metadata_and_keeps_the_row() {
        let c = row("0000002a000000550003", "0000002a000000550002", 4, 7);
        assert_eq!(c.lsn, lsn("0000002a000000550003"));
        assert_eq!(c.lsn_hex, "0000002a000000550003");
        assert_eq!(c.seqval_hex.as_deref(), Some("0000002a000000550002"));
        assert_eq!(c.op_code, 4);
        assert_eq!(c.decoded["id"], 7);
    }

    #[test]
    fn decode_change_refuses_rows_without_lsn_or_operation() {
        let no_lsn = decode_change(serde_json::json!({ OP_COLUMN: 2 }));
        assert!(no_lsn.err().unwrap().to_string().contains("__$start_lsn"));
        let no_op = decode_change(serde_json::json!({ LSN_ALIAS: "0000002a000000550003" }));
        assert!(no_op.err().unwrap().to_string().contains("__$operation"));
        let bad_lsn = decode_change(serde_json::json!({ LSN_ALIAS: "zz", OP_COLUMN: 2 }));
        assert!(bad_lsn.is_err());
        let no_seq = decode_change(serde_json::json!({
            LSN_ALIAS: "0000002a000000550003", OP_COLUMN: 2
        }))
        .unwrap();
        assert!(no_seq.seqval_hex.is_none());
    }

    #[test]
    fn plan_instance_anchors_a_fresh_current_start_once() {
        let tables = HashMap::new();
        let bounds = LsnBounds {
            min: Some(lsn("00000000000000000010")),
            max: Some(lsn("00000000000000000020")),
            start: None,
        };
        let mut marks = Bookmarks::new();
        let step = plan_instance(
            "dbo_t",
            &tables,
            bounds,
            &mut marks,
            StartPosition::Current,
            OnGap::Fail,
        )
        .unwrap();
        assert!(matches!(step, InstanceStep::Anchored));
        assert_eq!(marks.get("dbo_t"), Some(lsn("00000000000000000020")));
        let again = plan_instance(
            "dbo_t",
            &tables,
            bounds,
            &mut marks,
            StartPosition::Current,
            OnGap::Fail,
        )
        .unwrap();
        assert!(matches!(again, InstanceStep::Idle));
        assert_eq!(marks.get("dbo_t"), Some(lsn("00000000000000000020")));
    }

    #[test]
    fn plan_instance_is_idle_without_change_activity() {
        let mut marks = Bookmarks::new();
        let step = plan_instance(
            "dbo_t",
            &HashMap::new(),
            LsnBounds::default(),
            &mut marks,
            StartPosition::Earliest,
            OnGap::Fail,
        )
        .unwrap();
        assert!(matches!(step, InstanceStep::Idle));
        assert_eq!(marks.get("dbo_t"), None);
    }

    #[test]
    fn plan_instance_queries_with_the_resolved_table() {
        let mut tables = HashMap::new();
        tables.insert(
            "dbo_Orders".to_string(),
            ("sales".to_string(), "Orders".to_string()),
        );
        let bounds = LsnBounds {
            min: Some(lsn("00000000000000000010")),
            max: Some(lsn("00000000000000000020")),
            start: None,
        };
        let mut marks = Bookmarks::new();
        let InstanceStep::Query(q) = plan_instance(
            "dbo_Orders",
            &tables,
            bounds,
            &mut marks,
            StartPosition::Earliest,
            OnGap::Fail,
        )
        .unwrap() else {
            panic!("expected a query");
        };
        assert_eq!((q.schema.as_str(), q.table.as_str()), ("sales", "Orders"));
        assert_eq!(
            (q.from, q.to),
            (lsn("00000000000000000010"), lsn("00000000000000000020"))
        );

        let InstanceStep::Query(unknown) = plan_instance(
            "dbo_Other",
            &tables,
            bounds,
            &mut Bookmarks::new(),
            StartPosition::Earliest,
            OnGap::Fail,
        )
        .unwrap() else {
            panic!("expected a query");
        };
        assert_eq!(
            (unknown.schema.as_str(), unknown.table.as_str()),
            ("", "dbo_Other")
        );
    }

    #[test]
    fn plan_instance_applies_the_gap_policy() {
        let bounds = LsnBounds {
            min: Some(lsn("00000000000000000050")),
            max: Some(lsn("00000000000000000090")),
            start: None,
        };
        let behind = || {
            let mut m = Bookmarks::new();
            m.set("dbo_t", lsn("00000000000000000010"));
            m
        };
        let err = plan_instance(
            "dbo_t",
            &HashMap::new(),
            bounds,
            &mut behind(),
            StartPosition::Current,
            OnGap::Fail,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("purged"), "{err}");
        let InstanceStep::Query(q) = plan_instance(
            "dbo_t",
            &HashMap::new(),
            bounds,
            &mut behind(),
            StartPosition::Current,
            OnGap::Skip,
        )
        .unwrap() else {
            panic!("expected a query");
        };
        assert_eq!(q.from, lsn("00000000000000000050"));
    }

    #[test]
    fn transactions_close_on_a_new_commit_lsn() {
        let (a, b) = (query("dbo_a"), query("dbo_b"));
        let mut tx = TxAssembler::new(None);
        assert!(
            tx.push(row("00000000000000000005", "01", 2, 1), &a)
                .unwrap()
                .is_none()
        );
        assert!(
            tx.push(row("00000000000000000005", "02", 3, 2), &b)
                .unwrap()
                .is_none()
        );
        assert!(
            tx.push(row("00000000000000000005", "03", 4, 2), &b)
                .unwrap()
                .is_none()
        );
        let (lsn5, recs) = tx
            .push(row("00000000000000000009", "01", 1, 3), &a)
            .unwrap()
            .expect("the first transaction closes");
        assert_eq!(lsn5, lsn("00000000000000000005"));
        assert_eq!(recs.len(), 2, "the update pre-image row is skipped");
        assert_eq!(recs[0]["op"], "i");
        assert_eq!(recs[0]["table"], "dbo_a");
        assert_eq!(recs[1]["op"], "u");
        assert_eq!(recs[1]["table"], "dbo_b");
        assert_eq!(recs[1]["after"]["id"], 2);
        assert!(tx.emitted);
        let tail = tx.finish();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0]["op"], "d");
        assert_eq!(tail[0]["before"]["id"], 3);
        assert!(tx.finish().is_empty());
    }

    #[test]
    fn a_transaction_of_skipped_rows_still_closes_empty() {
        let a = query("dbo_a");
        let mut tx = TxAssembler::new(None);
        assert!(
            tx.push(row("00000000000000000005", "01", 3, 1), &a)
                .unwrap()
                .is_none()
        );
        let (lsn5, recs) = tx
            .push(row("00000000000000000006", "01", 3, 1), &a)
            .unwrap()
            .unwrap();
        assert_eq!(lsn5, lsn("00000000000000000005"));
        assert!(recs.is_empty());
        assert!(!tx.emitted);
    }

    #[test]
    fn transactions_refuse_unknown_operations_and_oversized_buffers() {
        let a = query("dbo_a");
        let mut tx = TxAssembler::new(None);
        assert!(
            tx.push(row("00000000000000000005", "01", 9, 1), &a)
                .is_err()
        );

        let mut capped = TxAssembler::new(Some(2));
        capped
            .push(row("00000000000000000005", "01", 2, 1), &a)
            .unwrap();
        capped
            .push(row("00000000000000000005", "02", 2, 2), &a)
            .unwrap();
        let err = capped
            .push(row("00000000000000000005", "03", 2, 3), &a)
            .err()
            .unwrap();
        assert!(err.to_string().contains("max_staged_records (2)"), "{err}");
    }

    #[test]
    fn consume_all_moves_every_instance_to_its_upper_bound() {
        let mut marks = Bookmarks::new();
        marks.set("dbo_a", lsn("00000000000000000003"));
        consume_all(&mut marks, &[query("dbo_a"), query("dbo_b")]);
        assert_eq!(marks.get("dbo_a"), Some(lsn("00000000000000000100")));
        assert_eq!(marks.get("dbo_b"), Some(lsn("00000000000000000100")));
    }

    #[tokio::test]
    async fn new_fails_when_the_server_is_unreachable() {
        let cfg: MssqlCdcSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_url": "mssql://sa:pw@127.0.0.1:1/sales",
            "capture_instances": ["dbo_Orders"],
            "max_connections": 2
        }))
        .unwrap();
        let res = tokio::time::timeout(Duration::from_secs(60), MssqlCdcSource::new(cfg))
            .await
            .expect("an unreachable server fails promptly");
        assert!(res.is_err());
    }

    #[test]
    fn merged_streams_follow_commit_order() {
        let heads = vec![
            Some(change("0000002a000000560001", "0000002a000000560003")),
            None,
            Some(change("0000002a000000550003", "0000002a000000550009")),
            Some(change("0000002a000000550003", "0000002a000000550002")),
        ];
        assert_eq!(next_head(&heads), Some(3));
        assert_eq!(next_head(&[None, None]), None);
    }

    #[test]
    fn marks_advance_for_every_instance_but_never_back() {
        let q = |ci: &str| ChangeQuery {
            ci: ci.into(),
            schema: "dbo".into(),
            table: ci.into(),
            from: Lsn::from_hex("00000000000000000001").unwrap(),
            to: Lsn::from_hex("00000000000000000100").unwrap(),
        };
        let mut marks = Bookmarks::new();
        let ahead = Lsn::from_hex("00000000000000000090").unwrap();
        marks.set("b", ahead);
        let lsn = Lsn::from_hex("00000000000000000050").unwrap();
        advance_marks(&mut marks, &[q("a"), q("b")], lsn);
        assert_eq!(marks.get("a"), Some(lsn));
        assert_eq!(marks.get("b"), Some(ahead));
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
