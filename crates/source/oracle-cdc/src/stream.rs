//! The LogMiner [`Source`]. A fetch cycle runs on one blocking thread that
//! owns the mining session and hands each committed transaction to the async
//! stream as its own page (bookmarked), so a transaction is never split and
//! the pipeline persists progress only after the sink confirms it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use faucet_common_oracle::oracle::Connection;
use faucet_common_oracle::{
    NLS_SESSION_SQL, OraclePool, Side, TypeFamily, blocking, call_timeout, checkout, connect_pool,
    ora_err,
};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use serde_json::Value;
use tokio::sync::mpsc::Sender;

use crate::config::{OracleCdcSourceConfig, StartPosition};
use crate::logs::{LogFile, oldest_available_scn, plan_log_files};
use crate::miner::{Assembler, LogRow, Miner, TableMeta, to_envelope};
use crate::sql::{
    ADD_LOGFILE_SQL, ARCHIVED_LOGS_SQL, CONTAINER_SQL, DatabaseLogging, END_LOGMNR_SQL,
    LOGFILE_ADD, LOGFILE_NEW, LoggingReport, ONLINE_LOGS_SQL, POSITION_SQL, START_LOGMNR_SQL,
    SUPPLEMENTAL_SQL, columns_sql, contents_sql, flush_sql, flush_table_ddl, log_groups_sql,
    logging_report, needs_logfiles,
};
use crate::state::Position;

/// Oracle LogMiner change-data-capture source.
pub struct OracleCdcSource {
    shared: Arc<Shared>,
    pending: Mutex<Option<Position>>,
}

struct Shared {
    config: OracleCdcSourceConfig,
    pool: OraclePool,
    tables: Vec<(String, String)>,
    state_key: String,
    add_logfiles: bool,
}

fn src_err(context: &str, e: &faucet_common_oracle::oracle::Error) -> FaucetError {
    ora_err(Side::Source, context, e)
}

/// A LogMiner start failure; a missing log (ORA-01291) is lost redo.
fn start_error(e: &faucet_common_oracle::oracle::Error, from: u64, to: u64) -> FaucetError {
    classify_start_error(faucet_common_oracle::ora_code(e), &e.to_string(), from, to)
}

fn classify_start_error(code: Option<i32>, message: &str, from: u64, to: u64) -> FaucetError {
    match code {
        Some(1291) => FaucetError::Source(missing_redo_message(from, to)),
        _ => FaucetError::Source(format!("oracle start LogMiner: {message}")),
    }
}

fn missing_redo_message(from: u64, to: u64) -> String {
    format!(
        "oracle-cdc: the redo for SCN {from}..={to} is not available to LogMiner (ORA-01291 \
         missing log file). Run the database in ARCHIVELOG mode and keep archived logs until \
         they are mined; changes in a recycled range cannot be recovered — re-snapshot, then \
         restart capture"
    )
}

fn i64_scn(scn: u64) -> i64 {
    i64::try_from(scn).unwrap_or(i64::MAX)
}

fn table_binds(tables: &[(String, String)]) -> Vec<String> {
    tables
        .iter()
        .flat_map(|(o, t)| [o.clone(), t.clone()])
        .collect()
}

impl Shared {
    fn conn(&self) -> Result<Connection, FaucetError> {
        checkout(
            &self.pool,
            Side::Source,
            call_timeout(self.config.statement_timeout_secs),
        )
    }

    fn load_meta(&self, conn: &Connection) -> Result<TableMeta, FaucetError> {
        let binds = table_binds(&self.tables);
        let refs: Vec<&dyn faucet_common_oracle::oracle::sql_type::ToSql> =
            binds.iter().map(|b| b as _).collect();
        let rows = conn
            .query_as::<(String, String, String, String, Option<i64>)>(
                &columns_sql(self.tables.len()),
                &refs,
            )
            .map_err(|e| src_err("column metadata", &e))?;
        let mut meta = TableMeta::new();
        for r in rows {
            let (o, t, c, ty, scale) = r.map_err(|e| src_err("column metadata", &e))?;
            meta.entry((o, t))
                .or_default()
                .insert(c, TypeFamily::from_data_type(&ty, scale));
        }
        Ok(meta)
    }

    fn position_now(&self, conn: &Connection) -> Result<Position, FaucetError> {
        let (current, oldest): (u64, Option<u64>) = conn
            .query_row_as(POSITION_SQL, &[])
            .map_err(|e| src_err("current SCN", &e))?;
        Ok(Position::resume_from(current, oldest))
    }

    fn list_logs(&self, conn: &Connection, lo: u64, hi: u64) -> Result<Vec<LogFile>, FaucetError> {
        let mut out = Vec::new();
        for (sql, archived) in [(ARCHIVED_LOGS_SQL, true), (ONLINE_LOGS_SQL, false)] {
            let rows = conn
                .query_as::<(String, i64, i64, u64, u64)>(sql, &[&i64_scn(lo), &i64_scn(hi)])
                .map_err(|e| src_err("redo log listing", &e))?;
            for r in rows {
                let (name, thread, sequence, first_change, next_change) =
                    r.map_err(|e| src_err("redo log listing", &e))?;
                out.push(LogFile {
                    name,
                    thread,
                    sequence,
                    first_change,
                    next_change,
                    archived,
                });
            }
        }
        Ok(out)
    }

    fn initial_position(&self, conn: &Connection) -> Result<Position, FaucetError> {
        match self.config.start_position {
            StartPosition::Current => self.position_now(conn),
            StartPosition::Earliest => {
                let logs = self.list_logs(conn, 0, u64::MAX)?;
                let oldest = oldest_available_scn(&logs).ok_or_else(|| {
                    FaucetError::Source("oracle-cdc: no redo logs are visible".into())
                })?;
                Ok(Position::at(oldest.saturating_sub(1), oldest))
            }
        }
    }

    fn flush(&self, conn: &Connection, scn: u64) -> Result<(), FaucetError> {
        conn.execute(
            &flush_sql(&self.config.flush_table),
            &[&i64_scn(scn), &i64_scn(scn)],
        )
        .map_err(|e| src_err("redo flush", &e))?;
        conn.commit().map_err(|e| src_err("redo flush", &e))
    }

    /// Mine `(from, to]`, feeding every row to `on_row`. The session is always
    /// ended, even when a row handler fails.
    fn mine(
        &self,
        conn: &Connection,
        from: u64,
        to: u64,
        on_row: &mut dyn FnMut(LogRow) -> Result<bool, FaucetError>,
    ) -> Result<(), FaucetError> {
        let logs = plan_log_files(&self.list_logs(conn, from, to)?, from + 1, to)
            .map_err(|m| FaucetError::Source(format!("oracle-cdc: {m}")))?;
        if self.add_logfiles {
            for (i, log) in logs.iter().enumerate() {
                let opt = if i == 0 { LOGFILE_NEW } else { LOGFILE_ADD };
                conn.execute(ADD_LOGFILE_SQL, &[&log.name, &opt])
                    .map_err(|e| src_err("add log file", &e))?;
            }
        }
        conn.execute(START_LOGMNR_SQL, &[&i64_scn(from + 1), &i64_scn(to)])
            .map_err(|e| start_error(&e, from + 1, to))?;
        let result = self.read_contents(conn, from, to, on_row);
        let _ = conn.execute(END_LOGMNR_SQL, &[]);
        result
    }

    fn read_contents(
        &self,
        conn: &Connection,
        from: u64,
        to: u64,
        on_row: &mut dyn FnMut(LogRow) -> Result<bool, FaucetError>,
    ) -> Result<(), FaucetError> {
        let mut binds: Vec<Box<dyn faucet_common_oracle::oracle::sql_type::ToSql>> =
            vec![Box::new(i64_scn(from)), Box::new(i64_scn(to))];
        for b in table_binds(&self.tables) {
            binds.push(Box::new(b));
        }
        let refs: Vec<&dyn faucet_common_oracle::oracle::sql_type::ToSql> =
            binds.iter().map(|b| b.as_ref()).collect();
        let mut stmt = conn
            .statement(&contents_sql(self.tables.len()))
            .fetch_array_size(1000)
            .build()
            .map_err(|e| src_err("mine", &e))?;
        type Raw = (
            u64,
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            Option<String>,
            i64,
            Option<String>,
            Option<String>,
        );
        let rows = stmt
            .query_as::<Raw>(&refs)
            .map_err(|e| src_err("mine", &e))?;
        let mut assembler = Assembler::default();
        for r in rows {
            let (scn, op, xid, owner, table, redo, csf, row_id, rollback, ts, info) =
                r.map_err(|e| src_err("mine", &e))?;
            let row = LogRow {
                scn,
                op,
                xid,
                owner,
                table,
                redo: redo.unwrap_or_default(),
                row_id,
                rollback: rollback == 1,
                timestamp: ts,
                info,
            };
            if let Some(row) = assembler.push(row, csf == 1)
                && !on_row(row)?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    /// One fetch cycle: mine windows until idle, emitting a page per committed
    /// transaction. Returns early when the consumer goes away.
    fn capture(
        &self,
        start: Option<Position>,
        per_txn: bool,
        tx: Sender<StreamPage>,
    ) -> Result<(), FaucetError> {
        let conn = self.conn()?;
        conn.execute(NLS_SESSION_SQL, &[])
            .map_err(|e| src_err("session setup", &e))?;
        let mut meta = self.load_meta(&conn)?;
        let send = |page: StreamPage| tx.blocking_send(page).is_ok();
        let mut position = match start {
            Some(p) => p,
            None => {
                let p = self.initial_position(&conn)?;
                if per_txn
                    && !send(StreamPage {
                        records: Vec::new(),
                        bookmark: Some(p.to_value()),
                    })
                {
                    return Ok(());
                }
                p
            }
        };
        let mut miner = Miner::new(self.config.on_unsupported, self.config.max_staged_records);
        let mut from = position.restart_scn.saturating_sub(1);
        let mut agg: Vec<Value> = Vec::new();
        let mut last_activity = Instant::now();
        loop {
            let current = self.position_now(&conn)?.commit_scn;
            self.flush(&conn, current)?;
            let to = current.min(from.saturating_add(self.config.max_scn_window));
            let mut captured = false;
            if to > from {
                let before = position.clone();
                let mut open = true;
                self.mine(&conn, from, to, &mut |row| {
                    let Some(txn) = miner.apply(&row)? else {
                        return Ok(true);
                    };
                    if txn.events.is_empty() || position.already_emitted(txn.commit_scn, &txn.xid) {
                        return Ok(true);
                    }
                    let records: Vec<Value> = txn
                        .events
                        .iter()
                        .map(|e| to_envelope(e, &txn, &meta))
                        .collect();
                    position.record_commit(txn.commit_scn, &txn.xid, miner.oldest_open());
                    captured = true;
                    if per_txn {
                        open = send(StreamPage {
                            records,
                            bookmark: Some(position.to_value()),
                        });
                    } else {
                        agg.extend(records);
                    }
                    Ok(open)
                })?;
                if !open {
                    return Ok(());
                }
                position.advance_to(to, miner.oldest_open());
                from = to;
                if per_txn
                    && position != before
                    && !send(StreamPage {
                        records: Vec::new(),
                        bookmark: Some(position.to_value()),
                    })
                {
                    return Ok(());
                }
                if !miner.take_ddl_tables().is_empty() {
                    meta = self.load_meta(&conn)?;
                }
            }
            if captured {
                last_activity = Instant::now();
            }
            if last_activity.elapsed() >= self.config.idle_timeout {
                break;
            }
            if to >= current {
                std::thread::sleep(self.config.poll_interval);
            }
        }
        if !per_txn {
            send(StreamPage {
                records: agg,
                bookmark: Some(position.to_value()),
            });
        }
        tracing::info!(state_key = %self.state_key, "oracle-cdc fetch cycle complete");
        Ok(())
    }

    fn check_supplemental(&self, conn: &Connection) -> Result<LoggingReport, FaucetError> {
        let (min, pk, all): (String, String, String) = conn
            .query_row_as(SUPPLEMENTAL_SQL, &[])
            .map_err(|e| src_err("supplemental logging check", &e))?;
        let binds = table_binds(&self.tables);
        let refs: Vec<&dyn faucet_common_oracle::oracle::sql_type::ToSql> =
            binds.iter().map(|b| b as _).collect();
        let groups = conn
            .query_as::<(String, String, String)>(&log_groups_sql(self.tables.len()), &refs)
            .map_err(|e| src_err("supplemental logging check", &e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| src_err("supplemental logging check", &e))?;
        Ok(logging_report(
            &DatabaseLogging { min, pk, all },
            &self.tables,
            &groups,
        ))
    }
}

impl OracleCdcSource {
    /// Validate, connect, create the flush table and run the preflight: the
    /// captured tables must exist and minimal supplemental logging must be on.
    pub async fn new(config: OracleCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool = connect_pool(&config.connection, config.max_connections).await?;
        let tables = config.table_pairs();
        let state_key = config.resolved_state_key();
        let probe = Shared {
            config: config.clone(),
            pool: pool.clone(),
            tables: tables.clone(),
            state_key: state_key.clone(),
            add_logfiles: true,
        };
        let add_logfiles = blocking(move || {
            let conn = probe.conn()?;
            let (cdb, container): (String, String) = conn
                .query_row_as(CONTAINER_SQL, &[])
                .map_err(|e| src_err("container detection", &e))?;
            let meta = probe.load_meta(&conn)?;
            let missing: Vec<String> = probe
                .tables
                .iter()
                .filter(|t| !meta.contains_key(*t))
                .map(|(o, t)| format!("{o}.{t}"))
                .collect();
            if !missing.is_empty() {
                return Err(FaucetError::Config(format!(
                    "oracle-cdc: tables {missing:?} are not visible to this user (check the \
                     names' case and SELECT grants)"
                )));
            }
            let report = probe.check_supplemental(&conn)?;
            if !report.fatal.is_empty() {
                return Err(FaucetError::Config(format!(
                    "oracle-cdc: {}",
                    report.fatal.join("; ")
                )));
            }
            if !report.partial.is_empty() {
                tracing::warn!(
                    fix = ?report.partial,
                    "oracle-cdc: all-column supplemental logging is off, so update events carry \
                     only key and changed columns"
                );
            }
            conn.execute(&flush_table_ddl(&probe.config.flush_table), &[])
                .map_err(|e| src_err("create flush table", &e))?;
            Ok(needs_logfiles(&cdb, &container))
        })
        .await?;
        Ok(Self {
            shared: Arc::new(Shared {
                config,
                pool,
                tables,
                state_key,
                add_logfiles,
            }),
            pending: Mutex::new(None),
        })
    }

    fn pages(
        &self,
        per_txn: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + '_>> {
        let start = self.pending.lock().expect("pending mutex").take();
        let shared = self.shared.clone();
        Box::pin(async_stream::try_stream! {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamPage>(4);
            let handle = tokio::task::spawn_blocking(move || shared.capture(start, per_txn, tx));
            while let Some(page) = rx.recv().await {
                yield page;
            }
            handle
                .await
                .map_err(|e| FaucetError::Source(format!("oracle-cdc capture task failed: {e}")))??;
        })
    }
}

#[async_trait]
impl Source for OracleCdcSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.pages(false);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.pages(self.shared.config.batch_size != 0)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(OracleCdcSourceConfig))
            .expect("schema serialization")
    }

    fn state_key(&self) -> Option<String> {
        Some(self.shared.state_key.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.pending.lock().expect("pending mutex") = Some(Position::from_value(&bookmark)?);
        Ok(())
    }

    /// The current SCN (reaching back to open transactions), for anchoring CDC
    /// before a snapshot.
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        let shared = self.shared.clone();
        blocking(move || {
            let conn = shared.conn()?;
            Ok(Some(shared.position_now(&conn)?.to_value()))
        })
        .await
    }

    fn supports_exactly_once(&self) -> bool {
        true
    }

    fn connector_name(&self) -> &'static str {
        "oracle-cdc"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?tables={}",
            self.shared.config.connection.display_uri(),
            self.shared.config.tables.join(",")
        )
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let start = Instant::now();
        let shared = self.shared.clone();
        let run = blocking(move || {
            let conn = shared.conn()?;
            let report = shared.check_supplemental(&conn)?;
            let logs = shared.list_logs(&conn, 0, u64::MAX)?;
            Ok((report, logs.len()))
        });
        let probes = match tokio::time::timeout(ctx.timeout, run).await {
            Err(_) => vec![Probe::fail_hint(
                "connection",
                start.elapsed(),
                "timed out",
                "the database did not respond within the check timeout",
            )],
            Ok(Err(e)) => vec![Probe::fail_hint(
                "connection",
                start.elapsed(),
                e.to_string(),
                "check the connection settings and that the user can read V$DATABASE, V$LOG, \
                 V$ARCHIVED_LOG and ALL_LOG_GROUPS (grant SELECT_CATALOG_ROLE, LOGMINING)",
            )],
            Ok(Ok((report, logs))) => {
                let supplemental = if report.fatal.is_empty() {
                    Probe::pass("supplemental-logging", start.elapsed())
                } else {
                    Probe::fail_hint(
                        "supplemental-logging",
                        start.elapsed(),
                        "supplemental logging does not cover the captured tables",
                        report.fatal.join("; "),
                    )
                };
                let images = if report.partial.is_empty() {
                    Probe::pass("full-row-images", start.elapsed())
                } else {
                    Probe::skip(
                        "full-row-images",
                        format!(
                            "updates will carry only key and changed columns; to capture full \
                             rows run: {}",
                            report.partial.join(" ")
                        ),
                    )
                };
                let redo = if logs > 0 {
                    Probe::pass("redo-logs", start.elapsed())
                } else {
                    Probe::fail_hint(
                        "redo-logs",
                        start.elapsed(),
                        "no redo logs are visible",
                        "grant SELECT on V_$LOG, V_$LOGFILE and V_$ARCHIVED_LOG",
                    )
                };
                vec![
                    Probe::pass("connection", start.elapsed()),
                    supplemental,
                    images,
                    redo,
                ]
            }
        };
        Ok(CheckReport { probes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(i64_scn(5), 5);
        assert_eq!(i64_scn(u64::MAX), i64::MAX);
        assert_eq!(
            table_binds(&[("A".into(), "T".into())]),
            vec!["A".to_string(), "T".to_string()]
        );
        assert!(missing_redo_message(1, 2).contains("SCN 1..=2"));
        assert!(
            classify_start_error(Some(1291), "x", 3, 4)
                .to_string()
                .contains("SCN 3..=4")
        );
        let other = classify_start_error(Some(1017), "ORA-01017", 3, 4).to_string();
        assert!(
            other.contains("ORA-01017") && !other.contains("ARCHIVELOG"),
            "{other}"
        );
    }
}
