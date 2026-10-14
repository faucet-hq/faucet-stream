//! The MSSQL [`Sink`] implementation — connection pool, transaction-wrapped
//! multi-row `INSERT` with 2100-parameter auto-splitting, and row-isolation
//! partial-failure handling for DLQ routing.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::driver::{
    CleanupDriver, CommitTokenDriver, DriverSink, PageWrite, RowPolicy, SchemaDriver, SinkDriver,
    StagingDriver,
};
use faucet_core::{CallSlot, FaucetError, RowOutcome, Sink, WriteReport};
use serde_json::Value;
use tiberius::ToSql;

use faucet_common_mssql::{MssqlPool, MssqlPooledConnection, build_pool, quote_ident_mssql};
use faucet_common_sql::dialects::Mssql;
use faucet_common_sql::{SqlDialect, TableTarget};

/// Width of the watermark table's `scope` PRIMARY KEY column. SQL Server's index
/// key budget is 900 bytes = 450 UTF-16 chars, so the column cannot be
/// `NVARCHAR(MAX)`; a longer scope is shortened to a digest form rather than
/// erroring or truncating onto another row's watermark (#456 L1).
const SCOPE_COL_WIDTH: usize = 450;

/// Fit a pipeline scope into [`SCOPE_COL_WIDTH`].
fn scope_key(scope: &str) -> String {
    faucet_core::idempotency::scope_key(scope, SCOPE_COL_WIDTH)
}

/// The pre-1.13 shortened form of a long scope, read once and retired (#862 M-017).
fn legacy_scope_key(scope: &str) -> Option<String> {
    faucet_core::idempotency::legacy_scope_key(scope, SCOPE_COL_WIDTH)
}

use crate::coltype::{COLUMN_INFO_SQL, ColumnInfo};
use crate::config::{MssqlColumnMapping, MssqlSinkConfig};
use crate::encode::{
    BoundParam, build_cleanup_delete_sql, build_cleanup_temp_create_sql,
    build_cleanup_temp_drop_sql, max_rows_per_insert,
};

pub(crate) use crate::statements::Prepared;
use crate::statements::{
    cleanup_key_typing, delete_statements, insert_statements, key_load_statements,
    merge_statements, overwrite_swap_sql, poison_check, prepare_rows, staging_create_sql,
    staging_relax_sql, with_call, writable,
};

/// Microsoft SQL Server sink.
///
/// Built on the core sink driver (RFC 0014): `MssqlDriver` holds the TDS I/O
/// with its staging, commit-token, cleanup and schema groups, and a
/// [`DriverSink`] derives the capabilities and serves the write paths.
pub struct MssqlSink {
    inner: DriverSink<MssqlDriver>,
}

impl MssqlSink {
    /// Connect, validate, build the pool, and (in `json_column` + `create_table`
    /// mode) create the table if it doesn't exist.
    pub async fn new(config: MssqlSinkConfig) -> Result<Self, FaucetError> {
        Ok(Self {
            inner: DriverSink::new(MssqlDriver::new(config).await?),
        })
    }
}

#[async_trait]
impl Sink for MssqlSink {
    faucet_core::forward_sink!(inner);
}

/// The SQL Server I/O the sink driver runs on.
pub(crate) struct MssqlDriver {
    pub(crate) config: MssqlSinkConfig,
    pool: MssqlPool,
    pub(crate) table_quoted: String,
    /// Pre-quoted staging table (`[schema].[table__faucet_ovw]`) used while a
    /// `write_mode: overwrite` run is in flight (#492).
    staging_table_quoted: String,
    /// Cached columns of the relation writes target, as `sys.columns` declares them.
    columns_cache: Mutex<Option<Vec<ColumnInfo>>>,
    /// Set when a timed-out statement's connection could not be replaced
    /// (SQL-37): that connection may still hold an open transaction, so no
    /// later write may run as if its commit counted.
    poisoned: std::sync::atomic::AtomicBool,
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    /// Per-sink run id for staged-object keys (#528).
    #[cfg(feature = "staging")]
    pub(crate) stage_run_id: String,
    /// Monotonic part counter for staged objects.
    #[cfg(feature = "staging")]
    pub(crate) stage_seq: std::sync::atomic::AtomicUsize,
    /// Backend-call context (#880): the run's deadline, cancel token and
    /// round-trip recorder. Every statement runs on its own connection call
    /// (`on_conn`), so the driver keeps the run's context here, installed by
    /// the adapter's `set_call_context`.
    calls: faucet_core::CallSlot,
}

impl MssqlDriver {
    /// Connect, validate, build the pool, and (in `json_column` + `create_table`
    /// mode) create the table if it doesn't exist.
    pub(crate) async fn new(config: MssqlSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        config.write.validate()?;
        if matches!(
            config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) && !matches!(
            config.column_mapping,
            MssqlColumnMapping::AutoColumns { .. }
        ) {
            return Err(FaucetError::Config(
                "mssql sink: write_mode upsert/delete requires column_mapping: auto_columns \
                 (key columns must be real columns, not inside a JSON column)"
                    .into(),
            ));
        }
        let table_quoted = quote_table(&config.table)?;
        let staging_table_quoted = quote_table(&format!(
            "{}{}",
            config.table,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        ))?;
        let pool = build_pool(&config.connection, config.max_connections).await?;
        let statement_timeout_secs = config.statement_timeout_secs;

        let sink = Self {
            config,
            pool,
            table_quoted,
            staging_table_quoted,
            columns_cache: Mutex::new(None),
            poisoned: std::sync::atomic::AtomicBool::new(false),
            calls: faucet_core::CallSlot::new(faucet_common_mssql::statement_call_context(
                statement_timeout_secs,
            )),
            table_ready: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "staging")]
            stage_run_id: crate::staged::new_stage_run_id(),
            #[cfg(feature = "staging")]
            stage_seq: std::sync::atomic::AtomicUsize::new(0),
        };
        sink.maybe_create_table().await?;
        Ok(sink)
    }

    /// Run one statement on `conn` as a backend call named `op`. A statement
    /// the call context cut short was dropped mid-TDS, so the connection is
    /// replaced before the error returns. `bounded: false` lifts the statement
    /// timeout (a bulk copy or swap may legitimately run longer); the run's
    /// deadline still applies.
    pub(crate) async fn on_conn<T>(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        op: &'static str,
        bounded: bool,
        f: impl AsyncFnOnce(&mut MssqlPooledConnection<'_>) -> Result<T, FaucetError>,
    ) -> Result<T, FaucetError> {
        let ctx = self.calls.get();
        let ctx = if bounded { ctx } else { ctx.without_timeout() };
        let (result, cut_short) = with_call(&ctx, op, f(&mut *conn), |e| e).await;
        if cut_short {
            self.recover_desynced(conn).await;
        }
        result
    }

    /// Run a transaction-control or DDL statement and drain its (empty) result.
    pub(crate) async fn control(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        stmt: &str,
    ) -> Result<(), FaucetError> {
        let op = control_op(stmt);
        self.on_conn(conn, op, op != "statement", async |c| {
            c.simple_query(stmt)
                .await
                .map_err(|e| FaucetError::Sink(format!("MSSQL {stmt} failed: {e}")))?
                .into_results()
                .await
                .map_err(|e| FaucetError::Sink(format!("MSSQL {stmt} failed: {e}")))?;
            Ok(())
        })
        .await
    }

    /// Create the target at construction time when its shape does not depend
    /// on the data (#580).
    ///
    /// `json_column` mode has a fixed shape, so it can be created before the
    /// first page — which also means `faucet doctor` and a zero-record run
    /// leave a usable table behind. `auto_columns` mode has to wait for a page
    /// to infer from; that is [`ensure_table_ready`](Self::ensure_table_ready).
    async fn maybe_create_table(&self) -> Result<(), FaucetError> {
        if !self.config.create_table {
            return Ok(());
        }
        let MssqlColumnMapping::JsonColumn { column } = &self.config.column_mapping else {
            return Ok(());
        };
        let sql = tolerate_concurrent_create(&Mssql.create_json_table_sql(
            &TableTarget::new(&self.config.table, &self.table_quoted),
            column,
        )?);
        self.run_ddl(&sql, "create_table").await
    }

    /// Create the target from the first page's inferred columns, in
    /// `auto_columns` mode (#580).
    ///
    /// Runs once per sink instance. With `create_table: false` a missing table
    /// is a typed failure naming both ways out, rather than a bare
    /// "Invalid object name" from the first INSERT.
    pub(crate) async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !matches!(
            self.config.column_mapping,
            MssqlColumnMapping::AutoColumns { .. }
        ) {
            // The fixed json_column shape is already handled in `new`.
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        if !self.config.create_table {
            if !self.table_exists(&self.config.table).await? {
                return Err(faucet_core::missing_target_error(
                    "mssql sink",
                    &self.config.table,
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        // A page with nothing inferable leaves the table uncreated so the next
        // page can try, rather than emitting a zero-column CREATE.
        let key: &[String] = if self.config.write.dedups_by_key() {
            &self.config.write.key
        } else {
            &[]
        };
        let planned = if key.is_empty() {
            faucet_core::plan_columns(records)
        } else {
            faucet_core::plan_keyed_columns(records, key)
        };
        let Some(columns) = planned else {
            return Ok(());
        };
        // An overwrite writes to staging. On a first run (no target) nothing
        // else creates staging, so it is created here from the page (#676).
        let sql = if self.config.write.is_overwrite() {
            build_create_table_sql(
                &self.staging_literal(),
                &self.staging_table_quoted,
                &columns,
                key,
            )?
        } else {
            build_create_table_sql(&self.config.table, &self.table_quoted, &columns, key)?
        };
        self.run_ddl(&tolerate_concurrent_create(&sql), "create_table")
            .await?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn table_exists(&self, table_literal: &str) -> Result<bool, FaucetError> {
        let mut conn = self.checkout().await?;
        let probe = format!(
            "SELECT OBJECT_ID(N'{}', N'U')",
            table_literal.replace('\'', "''")
        );
        let rows = self
            .on_conn(&mut conn, "query", true, async |c| {
                c.simple_query(probe.as_str())
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL table probe failed: {e}")))?
                    .into_first_result()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL table probe failed: {e}")))
            })
            .await?;
        Ok(rows
            .first()
            .and_then(|r| r.try_get::<i32, _>(0).ok().flatten())
            .is_some())
    }

    /// Run one DDL statement, mapping both the send and the result-drain
    /// failure to the same typed error.
    async fn run_ddl(&self, sql: &str, what: &str) -> Result<(), FaucetError> {
        let mut conn = self.checkout().await?;
        self.on_conn(&mut conn, "statement", false, async |c| {
            c.simple_query(sql)
                .await
                .map_err(|e| FaucetError::Sink(format!("MSSQL {what} failed: {e}")))?
                .into_results()
                .await
                .map_err(|e| FaucetError::Sink(format!("MSSQL {what} failed: {e}")))?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn checkout(&self) -> Result<MssqlPooledConnection<'_>, FaucetError> {
        self.calls
            .get()
            .with_roundtrip_metering(false)
            .call_once("checkout", async {
                self.pool
                    .get()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL pool checkout failed: {e}")))
            })
            .await
    }

    /// Bare (un-quoted) staging table literal, for `OBJECT_ID(N'…')` lookups.
    fn staging_literal(&self) -> String {
        format!(
            "{}{}",
            self.config.table,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }

    /// The bracket-quoted relation the append/insert path targets. For
    /// `write_mode: overwrite` every write in this sink's lifetime lands in the
    /// staging table (created by [`begin_overwrite`], swapped by
    /// [`commit_overwrite`]); otherwise the configured table.
    fn effective_table_quoted(&self) -> &str {
        if self.config.write.is_overwrite() {
            &self.staging_table_quoted
        } else {
            &self.table_quoted
        }
    }

    /// Columns of the relation writes target, discovered once and cached.
    async fn column_infos(&self) -> Result<Vec<ColumnInfo>, FaucetError> {
        if let Some(cols) = self.columns_cache.lock().expect("columns mutex").clone() {
            return Ok(cols);
        }
        // The relation the writes target: staging during an overwrite (a clone
        // of the target, or — on a first run — the only table there is, #676).
        let effective = if self.config.write.is_overwrite() {
            self.staging_literal()
        } else {
            self.config.table.clone()
        };
        let infos = writable(
            self.fetch_column_infos(&effective).await?,
            &self.config.table,
        )?;
        *self.columns_cache.lock().expect("columns mutex") = Some(infos.clone());
        Ok(infos)
    }

    /// Every column of `table` as `sys.columns` declares it, in column order;
    /// empty when the table does not exist.
    async fn fetch_column_infos(&self, table: &str) -> Result<Vec<ColumnInfo>, FaucetError> {
        let mut conn = self.checkout().await?;
        let rows = self
            .on_conn(&mut conn, "query", true, async |c| {
                c.query(COLUMN_INFO_SQL, &[&table])
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL column discovery failed: {e}")))?
                    .into_first_result()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL column discovery failed: {e}")))
            })
            .await?;
        let mut cols = Vec::with_capacity(rows.len());
        for row in &rows {
            let (Some(name), Some(type_name)) =
                (row.get::<&str, _>("name"), row.get::<&str, _>("type_name"))
            else {
                continue;
            };
            cols.push(ColumnInfo {
                name: name.to_string(),
                type_name: type_name.to_string(),
                max_length: row.get::<i16, _>("max_length").unwrap_or(0),
                precision: row.get::<u8, _>("precision").unwrap_or(0),
                scale: row.get::<u8, _>("scale").unwrap_or(0),
                collation: row.get::<&str, _>("collation").map(str::to_string),
                // `bit` columns decode as bools.
                is_nullable: row.get::<bool, _>("is_nullable").unwrap_or(true),
                is_identity: row.get::<bool, _>("is_identity").unwrap_or(false),
                is_computed: row.get::<bool, _>("is_computed").unwrap_or(false),
            });
        }
        Ok(cols)
    }

    /// The target table's columns (not staging), for the schema and cleanup
    /// paths; empty when it does not exist.
    async fn discover_column_types(&self) -> Result<Vec<ColumnInfo>, FaucetError> {
        self.fetch_column_infos(&self.config.table).await
    }

    /// Fail every write once a desynced connection could not be replaced.
    fn check_poisoned(&self) -> Result<(), FaucetError> {
        poison_check(self.poisoned.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// A statement timed out: the connection may still be running it inside an
    /// open transaction, and returning it to the pool would let a later page's
    /// `BEGIN TRAN … COMMIT TRAN` merely nest inside it — then roll back when
    /// the connection closes (SQL-37). Swap in a fresh connection so the stale
    /// one closes now and the server rolls its transaction back.
    async fn recover_desynced(&self, conn: &mut MssqlPooledConnection<'_>) {
        if let Err(e) = faucet_common_mssql::replace_connection(&self.config.connection, conn).await
        {
            tracing::error!(error = %e, "MSSQL: could not replace a timed-out connection");
            self.poisoned
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Roll back after a failed statement, or replace the connection when it
    /// timed out (a desynced stream cannot run `ROLLBACK`).
    async fn abort_txn(&self, conn: &mut MssqlPooledConnection<'_>, timed_out: bool) {
        if timed_out {
            self.recover_desynced(conn).await;
        } else {
            let _ = self.control(conn, "ROLLBACK TRAN").await;
        }
    }

    /// Resolve the column list + per-row owned params for one chunk.
    /// Returns `None` when there is nothing to insert (e.g. auto_columns with no
    /// matching keys).
    async fn prepare_chunk(&self, chunk: &[Value]) -> Result<Option<Prepared>, FaucetError> {
        let infos = match &self.config.column_mapping {
            MssqlColumnMapping::AutoColumns { .. } => self.column_infos().await?,
            MssqlColumnMapping::JsonColumn { .. } => Vec::new(),
        };
        prepare_rows(&self.config.column_mapping, &infos, chunk)
    }

    /// Insert rows within an **already-open** transaction — caller owns
    /// `BEGIN TRAN`/`COMMIT TRAN`/`ROLLBACK TRAN`.  Splits into ≤2100-param
    /// sub-INSERTs but does NOT issue any transaction-control statements.
    ///
    /// Used by `write_batch_idempotent` so the data INSERTs and the commit-token
    /// MERGE share one externally-managed transaction.
    ///
    /// Returns `Err((error, timed_out))`. When `timed_out` is `true` the `exec`
    /// future was dropped mid-TDS, leaving the connection desynced — the caller
    /// must NOT issue ROLLBACK on it (mirrors `insert_chunk`).
    async fn insert_rows_no_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        cols: &[String],
        casts: &[Option<String>],
        rows: &[Vec<BoundParam>],
    ) -> Result<usize, (FaucetError, bool)> {
        if rows.is_empty() {
            return Ok(0);
        }
        let cols_quoted: Vec<String> = cols
            .iter()
            .map(|c| quote_ident_mssql(c))
            .collect::<Result<_, _>>()
            .map_err(|e| (e, false))?;
        for (sql, range) in insert_statements(
            self.effective_table_quoted(),
            &cols_quoted,
            casts,
            rows.len(),
        ) {
            let owned: Vec<&BoundParam> = rows[range].iter().flatten().collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_tosql()).collect();
            let exec = async {
                conn.execute(sql.as_str(), &refs)
                    .await
                    .map(|_| ())
                    .map_err(|e| FaucetError::Sink(format!("MSSQL insert failed: {e}")))
            };
            // On timeout the connection is desynced; the caller must NOT issue
            // ROLLBACK on it (mirrors `insert_chunk`).
            let (result, timed_out) = with_call(&self.calls.get(), "insert", exec, |e| {
                FaucetError::Sink(format!("MSSQL insert: {e}"))
            })
            .await;
            result.map_err(|e| (e, timed_out))?;
        }
        Ok(rows.len())
    }

    /// Ensure the per-sink commit-token watermark table exists.
    /// Uses `IF OBJECT_ID … IS NULL CREATE TABLE` so it is idempotent.
    async fn ensure_commit_table(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
    ) -> Result<(), FaucetError> {
        // `scope` is NVARCHAR(SCOPE_COL_WIDTH), the 900-byte index key budget;
        // `token` is NVARCHAR(MAX) because a `#291` commit token embeds the
        // page's resume bookmark (audit #321 C4).
        let sql = tolerate_concurrent_create(&Mssql.commit_token_table_sql());
        self.control(conn, &sql).await
    }

    /// Insert one chunk, splitting into ≤2100-parameter sub-INSERTs wrapped in a
    /// single transaction (when `transaction_per_batch`). Returns rows inserted.
    async fn insert_chunk(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        cols: &[String],
        casts: &[Option<String>],
        rows: &[Vec<BoundParam>],
    ) -> Result<usize, ChunkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let cols_quoted = quote_columns(cols)?;
        let per_insert = max_rows_per_insert(cols_quoted.len());

        // Wrap the chunk in a transaction when configured, OR whenever it spans
        // more than one ≤2100-param sub-INSERT. Under autocommit a multi-sub
        // chunk commits each sub-INSERT independently, so a later failure leaves
        // earlier sub-INSERTs committed — and both the batch-level transient
        // retry (`write_batch`) and the per-row isolation (`write_batch_partial`)
        // re-run the whole chunk, duplicating those committed rows (audit #146
        // H6). Forcing a transaction makes the chunk atomic so re-running is safe.
        let txn = self.config.transaction_per_batch || rows.len() > per_insert;
        if txn {
            self.control(conn, "BEGIN TRAN").await?;
        }

        for (sql, range) in insert_statements(
            self.effective_table_quoted(),
            &cols_quoted,
            casts,
            rows.len(),
        ) {
            let owned: Vec<&BoundParam> = rows[range].iter().flatten().collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_tosql()).collect();

            let exec = async {
                conn.execute(sql.as_str(), &refs)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        // Classify here, while the tiberius error is still typed —
                        // downstream only sees the rendered string.
                        ChunkError {
                            class: classify_chunk_failure(&e),
                            err: FaucetError::Sink(format!("MSSQL insert failed: {e}")),
                        }
                    })
            };
            // Track whether the failure was a *timeout* specifically. On timeout
            // the `exec` future is dropped mid-TDS, leaving an unread response on
            // the wire — the connection is desynced and must NOT be reused (the
            // pool helper's contract is "drop it"). Issuing ROLLBACK on it would
            // run on a corrupt stream. A *normal* error leaves the connection in
            // sync, so ROLLBACK is safe and releases the transaction promptly.
            // Outcome unknown on timeout: the server may have committed.
            let (result, timed_out) = with_call(&self.calls.get(), "insert", exec, |e| {
                FaucetError::Sink(format!("MSSQL insert: {e}")).into()
            })
            .await;
            if let Err(e) = result {
                if timed_out {
                    self.recover_desynced(conn).await;
                } else if txn {
                    let _ = self.control(conn, "ROLLBACK TRAN").await;
                }
                return Err(e);
            }
        }

        if txn {
            self.control(conn, "COMMIT TRAN").await?;
        }
        Ok(rows.len())
    }

    /// Run a single parameterized statement on an already-open connection,
    /// honouring the per-statement timeout, and return the rows it affected.
    ///
    /// `what` names the operation in the error message (`"merge"`, `"cleanup"`).
    /// Returns `Err((error, timed_out))`; on timeout the connection is desynced
    /// and the caller must NOT issue ROLLBACK on it (mirrors
    /// `insert_rows_no_txn`).
    async fn exec_params(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        sql: &str,
        refs: &[&dyn ToSql],
        what: &'static str,
    ) -> Result<u64, (FaucetError, bool)> {
        let exec = async {
            conn.execute(sql, refs)
                .await
                .map(|r| r.total())
                .map_err(|e| FaucetError::Sink(format!("MSSQL {what} failed: {e}")))
        };
        let (result, timed_out) = with_call(&self.calls.get(), what, exec, |e| {
            FaucetError::Sink(format!("MSSQL {what}: {e}"))
        })
        .await;
        result.map_err(|e| (e, timed_out))
    }

    /// Run a single `MERGE`-upsert / `MERGE`-delete statement (see
    /// [`exec_params`](Self::exec_params)).
    async fn exec_merge(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        sql: &str,
        refs: &[&dyn ToSql],
    ) -> Result<(), (FaucetError, bool)> {
        self.exec_params(conn, sql, refs, "merge").await.map(|_| ())
    }

    /// Upsert `upserts` into the table via `MERGE`, on an already-open
    /// connection (the caller owns the transaction). Resolves the column set
    /// via `resolve_insert_columns`, chunks by `max_rows_per_insert`, and binds
    /// each row's params row-major exactly as `build_merge` numbers them.
    async fn upsert_rows_no_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        upserts: &[Value],
    ) -> Result<usize, (FaucetError, bool)> {
        if upserts.is_empty() {
            return Ok(0);
        }
        let MssqlColumnMapping::AutoColumns { on_unknown_field } = &self.config.column_mapping
        else {
            // Validated in `new()` — upsert requires auto_columns.
            return Err((
                FaucetError::Sink("MSSQL upsert requires column_mapping: auto_columns".into()),
                false,
            ));
        };
        let infos = self.column_infos().await.map_err(|e| (e, false))?;
        let statements = merge_statements(
            &self.table_quoted,
            &self.config.write.key,
            &infos,
            *on_unknown_field,
            upserts,
        )
        .map_err(|e| (e, false))?;
        for st in &statements {
            let refs: Vec<&dyn ToSql> = st.params.iter().map(|p| p.as_tosql()).collect();
            self.exec_merge(conn, &st.sql, &refs).await?;
        }
        Ok(upserts.len())
    }

    /// Delete the `deletes` key tuples via `MERGE … WHEN MATCHED THEN DELETE`,
    /// on an already-open connection. Chunks by `max_rows_per_insert(key.len())`
    /// and binds each key tuple's values in `key` order, row-major.
    async fn delete_keys_no_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, (FaucetError, bool)> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let infos = self.column_infos().await.map_err(|e| (e, false))?;
        let statements =
            delete_statements(&self.table_quoted, &self.config.write.key, &infos, deletes)
                .map_err(|e| (e, false))?;
        for st in &statements {
            let refs: Vec<&dyn ToSql> = st.params.iter().map(|p| p.as_tosql()).collect();
            self.exec_merge(conn, &st.sql, &refs).await?;
        }
        Ok(deletes.len())
    }

    /// Apply planned upsert/delete groups atomically, in page order, in a
    /// single `BEGIN TRAN` / `COMMIT TRAN`. Groups come from [`page_plans`],
    /// so a delete that precedes an upsert of a key equal under the column
    /// collation (`"Bob"` / `"bob"`) cannot run after it and remove the row.
    async fn apply_plans(&self, plans: &[faucet_core::WritePlan]) -> Result<usize, FaucetError> {
        // Fill the column cache first: discovery takes its own pooled
        // connection, so doing it under the write connection deadlocks a
        // `max_connections: 1` pool.
        self.column_infos().await?;
        let mut conn = self.checkout().await?;
        self.control(&mut conn, "BEGIN TRAN").await?;

        match self.apply_plans_no_txn(&mut conn, plans).await {
            Ok(affected) => {
                self.control(&mut conn, "COMMIT TRAN").await?;
                Ok(affected)
            }
            Err((e, timed_out)) => {
                self.abort_txn(&mut conn, timed_out).await;
                Err(e)
            }
        }
    }

    /// Each group's upserts then deletes, inside the caller's transaction.
    async fn apply_plans_no_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        plans: &[faucet_core::WritePlan],
    ) -> Result<usize, (FaucetError, bool)> {
        let mut affected = 0usize;
        for plan in plans {
            affected += self.upsert_rows_no_txn(conn, &plan.upserts).await?;
            affected += self.delete_keys_no_txn(conn, &plan.deletes).await?;
        }
        Ok(affected)
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// Uses a `#temp` table + `NOT EXISTS` rather than `key NOT IN (…)` because
    /// the written-key set routinely dwarfs MSSQL's 2100-parameter ceiling (the
    /// cleanup ceiling defaults to 100k rows). The key loads and the `DELETE`
    /// share one transaction, so the delete is all-or-nothing: a partial delete
    /// would remove rows the run actually wrote.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every row in it is stale and must go. That
    /// is the case this feature exists for.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup requires a non-empty `key`".to_string(),
            ));
        }

        // Validate the columns *before* checking out the cleanup connection:
        // `discover_column_types` takes its own connection from the pool, and
        // nesting the two would deadlock a `max_connections: 1` pool.
        let live_infos = self.discover_column_types().await?;
        let (key_casts, key_binary) =
            cleanup_key_typing(&live_infos, scope.keys(), key, &self.config.table)?;

        // Build every statement up front so an identifier-quoting failure can
        // never leave a transaction open on a pooled connection.
        let scope_cols: Vec<&str> = scope.keys().map(String::as_str).collect();
        let create_sql = build_cleanup_temp_create_sql(&self.table_quoted, key)?;
        let delete_sql = build_cleanup_delete_sql(&self.table_quoted, &scope_cols, key)?;
        let drop_sql = build_cleanup_temp_drop_sql();

        let mut conn = self.checkout().await?;
        // A `#temp` table lives for the whole session and a pooled connection
        // outlives one cleanup, so clear any leftover from a previous run on this
        // connection. Outside the transaction: it is a cleanup of *our* scratch
        // state, not part of the atomic unit below.
        self.control(&mut conn, &drop_sql).await?;

        self.control(&mut conn, "BEGIN TRAN").await?;
        match self
            .cleanup_in_txn(
                &mut conn,
                CleanupStatements {
                    create: &create_sql,
                    delete: &delete_sql,
                    key_casts: &key_casts,
                    key_binary: &key_binary,
                },
                scope,
                seen,
            )
            .await
        {
            Ok(deleted) => {
                self.control(&mut conn, "COMMIT TRAN").await?;
                // Best-effort: free the scratch table so the connection returns to
                // the pool clean. The pre-drop above is the real guarantee.
                let _ = self.control(&mut conn, &drop_sql).await;
                Ok(deleted)
            }
            Err((e, timed_out)) => {
                // On timeout the exec future was dropped mid-TDS, leaving the
                // connection desynced — ROLLBACK would run on a corrupt stream, so
                // it is deliberately skipped and the connection dropped instead
                // (mirrors `insert_chunk` / `apply_plan`). Otherwise ROLLBACK both
                // undoes any loaded keys and drops the temp table, since T-SQL DDL
                // is transactional.
                self.abort_txn(&mut conn, timed_out).await;
                Err(e)
            }
        }
    }

    /// The transactional body of [`cleanup_scope_impl`]: materialize the written
    /// keys, then delete everything in scope that isn't among them. The caller
    /// owns `BEGIN TRAN` / `COMMIT TRAN` / `ROLLBACK TRAN`.
    ///
    /// Returns `Err((error, timed_out))` with the same desync contract as
    /// [`exec_params`](Self::exec_params).
    async fn cleanup_in_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        stmts: CleanupStatements<'_>,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, (FaucetError, bool)> {
        let key = &self.config.write.key;

        self.control(conn, stmts.create).await.map_err(|e| {
            (
                FaucetError::Sink(format!("cleanup: temp table creation failed: {e}")),
                false,
            )
        })?;

        // Load the written keys, chunked to stay inside MSSQL's 2100-parameter
        // and 1000-row-values ceilings. An empty set loads nothing and leaves the
        // `DELETE` below to remove the whole scope — the motivating case.
        let loads = key_load_statements(key, stmts.key_casts, stmts.key_binary, seen.keys())
            .map_err(|e| (e, false))?;
        for st in &loads {
            let refs: Vec<&dyn ToSql> = st.params.iter().map(|p| p.as_tosql()).collect();
            self.exec_params(conn, &st.sql, &refs, "cleanup key load")
                .await?;
        }

        // Delete everything in scope that isn't in the written-key set. Scope
        // values bind in `scope_cols` order, which is the same `BTreeMap`
        // iteration order the predicate was generated from.
        let owned: Vec<BoundParam> = scope.values().map(BoundParam::from_value).collect();
        let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_tosql()).collect();
        self.exec_params(conn, stmts.delete, &refs, "cleanup delete")
            .await
    }
}

/// The upsert/delete groups for one page. A single plan (one MERGE per kind)
/// unless a deleted key may equal an upserted one under the column's collation
/// (case, trailing spaces, or a non-ASCII key the check cannot fold); only then
/// does page order matter, so the page is split into op runs
/// ([`faucet_core::plan_writes_ordered`]).
fn page_plans(records: &[Value], spec: &faucet_core::WriteSpec) -> Vec<faucet_core::WritePlan> {
    let single = faucet_core::plan_writes(records, spec);
    if collation_collision(&single) {
        faucet_core::plan_writes_ordered(records, spec)
    } else {
        vec![single]
    }
}

/// A key folded the way a case-insensitive collation compares it; `None` when
/// it holds a non-ASCII string, which accent-insensitive collations may fold
/// further than this check can.
fn folded_key(values: impl Iterator<Item = Value>) -> Option<Vec<String>> {
    values
        .map(|v| match v {
            Value::String(s) if s.is_ascii() => Some(s.trim_end_matches(' ').to_ascii_lowercase()),
            Value::String(_) => None,
            other => Some(other.to_string()),
        })
        .collect()
}

/// Whether some deleted key may equal some upserted key under the collation.
fn collation_collision(plan: &faucet_core::WritePlan) -> bool {
    if plan.deletes.is_empty() || plan.upserts.is_empty() {
        return false;
    }
    let key: Vec<&String> = plan
        .deletes
        .first()
        .map(|k| k.0.iter().map(|(c, _)| c).collect())
        .unwrap_or_default();
    let mut upserted = std::collections::HashSet::new();
    for rec in &plan.upserts {
        let values = key
            .iter()
            .map(|c| rec.get(c.as_str()).cloned().unwrap_or(Value::Null));
        match folded_key(values) {
            Some(k) => {
                upserted.insert(k);
            }
            None => return true,
        }
    }
    plan.deletes.iter().any(|d| {
        folded_key(d.0.iter().map(|(_, v)| v.clone())).is_none_or(|k| upserted.contains(&k))
    })
}

/// The prepared statements and key typing a scoped cleanup runs with.
struct CleanupStatements<'a> {
    create: &'a str,
    delete: &'a str,
    key_casts: &'a [Option<String>],
    key_binary: &'a [bool],
}

/// The round-trip label of a control statement.
fn control_op(stmt: &str) -> &'static str {
    match stmt
        .split_whitespace()
        .next()
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("BEGIN") => "begin",
        Some("COMMIT") => "commit",
        Some("ROLLBACK") => "rollback",
        _ => "statement",
    }
}

/// `IF OBJECT_ID(...) IS NULL CREATE TABLE` for an `auto_columns` target
/// (#580). NVARCHAR(MAX) cannot be indexed, so key columns get a bounded type,
/// and a keyed write gets a PRIMARY KEY so MERGE has a target on a first run
/// (#676).
fn build_create_table_sql(
    table_literal: &str,
    table_quoted: &str,
    columns: &[faucet_core::PlannedColumn],
    key: &[String],
) -> Result<String, FaucetError> {
    Mssql.create_table_sql(&TableTarget::new(table_literal, table_quoted), columns, key)
}

/// Wrap an `IF OBJECT_ID(...) IS NULL CREATE TABLE` so a concurrent creator
/// winning between the probe and the `CREATE` is success, not error 2714
/// ("There is already an object named …") — two backfill windows or matrix
/// rows bootstrapping one table at once (#828). Every other error is rethrown.
fn tolerate_concurrent_create(ddl: &str) -> String {
    format!("BEGIN TRY {ddl} END TRY BEGIN CATCH IF ERROR_NUMBER() <> 2714 THROW; END CATCH")
}

/// `sp_rename` of a staging table onto the target's name. `sp_rename` takes the
/// object's (possibly schema-qualified) current name but only the bare new
/// name — a qualified one would become part of the name itself.
fn sp_rename_sql(from_literal: &str, to_table: &str) -> String {
    let bare = to_table.rsplit('.').next().unwrap_or(to_table);
    format!(
        "EXEC sp_rename N'{}', N'{}'",
        from_literal.replace('\'', "''"),
        bare.replace('\'', "''")
    )
}

/// Build an idempotent `ADD COLUMN` (guarded by `sys.columns`; T-SQL has no
/// `ADD COLUMN IF NOT EXISTS`). `table_quoted` is the bracket-quoted relation,
/// `table_literal` the bare name its `OBJECT_ID(N'…')` lookup resolves.
fn build_add_column_sql(
    table_quoted: &str,
    table_literal: &str,
    col: &str,
    t: faucet_core::SqlBaseType,
) -> Result<String, FaucetError> {
    Mssql.add_column_sql(&TableTarget::new(table_literal, table_quoted), col, t)
}

/// The type an evolving column widens to. An integer column that starts
/// receiving fractions becomes `DECIMAL(38,10)` — exact for every `BIGINT` —
/// rather than `FLOAT`, which would round stored values above 2^53 in place
/// (#862 M-045).
fn widened_keyword(
    from: Option<faucet_core::SqlBaseType>,
    to: faucet_core::SqlBaseType,
) -> &'static str {
    Mssql.widened_type_name(from, to)
}

/// The bounded type of a created primary-key column (`NVARCHAR(MAX)` cannot
/// be indexed).
#[cfg(test)]
fn mssql_key_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    Mssql.key_type_name(t)
}

#[cfg(test)]
fn mssql_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    Mssql.type_name(t)
}

/// The nullability a widened column keeps: a widening that gains `null`
/// relaxes it, any other re-states the column's current nullability.
fn widened_nullability(
    from: Option<&serde_json::Value>,
    to: &serde_json::Value,
    current: Option<bool>,
) -> Option<bool> {
    if from.is_some_and(|f| faucet_core::adds_null(f, to)) {
        Some(true)
    } else {
        current
    }
}

/// `ALTER TABLE <ref> ALTER COLUMN <col> <kw> [NULL|NOT NULL]` — widen an
/// existing column's type. `ALTER COLUMN` without a nullability clause follows
/// the session's `ANSI_NULL_DFLT` setting, so the column's current one is
/// re-stated when known. Naturally idempotent.
fn build_alter_type_sql(
    table_quoted: &str,
    col: &str,
    kw: &str,
    nullable: Option<bool>,
) -> Result<String, FaucetError> {
    let qcol = quote_ident_mssql(col)?;
    let null = match nullable {
        Some(true) => " NULL",
        Some(false) => " NOT NULL",
        None => "",
    };
    Ok(format!(
        "ALTER TABLE {table_quoted} ALTER COLUMN {qcol} {kw}{null}"
    ))
}

/// `ALTER TABLE <ref> ALTER COLUMN <col> <kw> NULL` — relax a NOT NULL
/// constraint. MSSQL requires re-stating the column's current type when toggling
/// nullability, so `kw` must be the column's existing type keyword. Naturally
/// idempotent.
fn build_alter_null_sql(table_quoted: &str, col: &str, kw: &str) -> Result<String, FaucetError> {
    let qcol = quote_ident_mssql(col)?;
    Ok(format!(
        "ALTER TABLE {table_quoted} ALTER COLUMN {qcol} {kw} NULL",
    ))
}

/// Map an MSSQL system type name (`sys.types.name`, e.g. `bigint`, `float`,
/// `bit`, `nvarchar`) back to a JSON-Schema type fragment so
/// [`MssqlSink::current_schema`] round-trips with [`faucet_core::diff_schema`].
/// `nullable` reflects `sys.columns.is_nullable`.
///
/// A `(max)` character or `text`/`ntext` column is where the sink stores
/// nested objects and arrays as JSON text, so it also accepts those types
/// (`x-faucet-also-accepts`) instead of reporting them as drift on every run
/// after the first (#862 M-046).
fn mssql_type_to_json_schema(type_name: &str, max_length: i16, nullable: bool) -> Value {
    let lower = type_name.to_ascii_lowercase();
    let base = match lower.as_str() {
        "bigint" | "int" | "smallint" | "tinyint" => "integer",
        "float" | "real" | "decimal" | "numeric" | "money" | "smallmoney" => "number",
        "bit" => "boolean",
        _ => "string",
    };
    let mut fragment = if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    };
    let holds_json = matches!(lower.as_str(), "text" | "ntext")
        || (matches!(lower.as_str(), "nvarchar" | "varchar") && max_length == -1);
    if holds_json {
        fragment[faucet_core::DRIFT_ALSO_ACCEPTS] = serde_json::json!(["object", "array"]);
    }
    fragment
}

/// Quote a (possibly schema-qualified) table name: `dbo.events` → `[dbo].[events]`.
fn quote_table(table: &str) -> Result<String, FaucetError> {
    let parts: Vec<String> = table
        .split('.')
        .map(quote_ident_mssql)
        .collect::<Result<_, _>>()?;
    Ok(parts.join("."))
}

/// How a failed chunk insert must be handled.
///
/// Decided from tiberius' **typed** error (server error number / transport
/// variant), never from message text. SQL Server error strings embed user data
/// — table, column, and constraint names — so a substring rule silently
/// misclassifies: a permanent `Violation of UNIQUE KEY constraint
/// 'UQ_connection_id'` matched a `"connection"` needle, was called transient,
/// and the poison row was propagated forever instead of being quarantined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkFailure {
    /// The server guarantees the statement did **not** commit — deadlock
    /// victim (1205) or lock-request timeout (1222); both are rolled back
    /// server-side. This is the only class that is safe to re-run, because
    /// re-running anything whose outcome is unknown duplicates rows.
    RolledBack,
    /// Infrastructure-level: connection/TLS/IO failure, a client-side timeout,
    /// or an Azure throttle/failover. Not row-specific, so it must propagate
    /// to the pipeline's `on_batch_error` policy rather than be blamed on a
    /// row — and must **not** be re-run here, since the write may have
    /// committed before the response was lost.
    Infrastructure,
    /// A row-specific server rejection (constraint violation, conversion
    /// error, …) — the candidate for per-row DLQ isolation.
    RowRejected,
}

/// Azure SQL transient error numbers (connection-level throttling/failover).
const AZURE_TRANSIENT: &[u32] = &[4060, 40197, 40501, 40613, 49918, 49919, 49920];

/// The pure decision table for a SQL Server error **number**.
///
/// Split out from [`classify_chunk_failure`] because `tiberius::error::TokenError`
/// has no public constructor, so the table is otherwise only reachable with a
/// live server. Keeping it pure makes the classification itself — the part
/// that decides whether rows get duplicated or quarantined — unit-testable.
pub(crate) fn classify_server_code(code: u32) -> ChunkFailure {
    match code {
        // Rolled back by the server, so re-running cannot duplicate rows:
        // 1205 = deadlock victim, 1222 = lock request timeout (never executed).
        1205 | 1222 => ChunkFailure::RolledBack,
        // Azure throttle/failover: connection-level, outcome unknown.
        c if AZURE_TRANSIENT.contains(&c) => ChunkFailure::Infrastructure,
        // Errors about the values being written: these blame rows.
        c if ROW_DATA_ERRORS.contains(&c) => ChunkFailure::RowRejected,
        // Anything else (log or filegroup full, missing object, permission,
        // memory, …) is environmental: it would fail every row, so quarantining
        // the stream while the bookmark advances would hide an outage.
        _ => ChunkFailure::Infrastructure,
    }
}

/// SQL Server error numbers that are about the data of the rows being
/// written: conversion and overflow (241, 242, 245, 220, 232, 517, 8114,
/// 8115, 8169, 8134), truncation (8152, 2628), `NULL` into `NOT NULL` (515),
/// constraint and key violations (547, 2601, 2627), invalid JSON (13609).
const ROW_DATA_ERRORS: &[u32] = &[
    220, 232, 241, 242, 245, 515, 517, 547, 2601, 2627, 2628, 8114, 8115, 8134, 8152, 8169, 13609,
];

/// Classify a tiberius failure while the error is still typed.
pub(crate) fn classify_chunk_failure(e: &tiberius::error::Error) -> ChunkFailure {
    use tiberius::error::Error;
    match e {
        // Server-reported error numbers are the API here.
        Error::Server(token) => classify_server_code(token.code()),
        // Transport/protocol: the session is gone or desynced.
        Error::Io { .. } | Error::Tls(_) | Error::Protocol(_) | Error::Routing { .. } => {
            ChunkFailure::Infrastructure
        }
        // Encoding/conversion problems are about the data being sent.
        Error::Encoding(_)
        | Error::Conversion(_)
        | Error::Utf8
        | Error::Utf16
        | Error::ParseInt(_) => ChunkFailure::RowRejected,
        _ => ChunkFailure::Infrastructure,
    }
}

/// Quote a chunk's column identifiers. Pure, so the failure path is unit-testable
/// without a server: a rejected identifier is **infrastructure**, not a row
/// rejection — it is a config/schema problem that every row in the chunk shares,
/// so blaming one row (and DLQ-ing it) would be wrong.
pub(crate) fn quote_columns(cols: &[String]) -> Result<Vec<String>, ChunkError> {
    cols.iter()
        .map(|c| quote_ident_mssql(c))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ChunkError {
            class: ChunkFailure::Infrastructure,
            err,
        })
}

/// A chunk-insert failure carrying its typed classification alongside the
/// user-facing error, so callers never re-parse the message.
#[derive(Debug)]
pub(crate) struct ChunkError {
    pub(crate) class: ChunkFailure,
    pub(crate) err: FaucetError,
}

impl From<ChunkError> for FaucetError {
    fn from(c: ChunkError) -> Self {
        c.err
    }
}

impl From<FaucetError> for ChunkError {
    /// Anything that is not a typed *server* rejection is infrastructure:
    /// propagate it, never blame a row for it, never blindly re-run it. Having
    /// this conversion means the I/O shim keeps using a bare `?` instead of a
    /// hand-written `map_err` at every call site.
    fn from(err: FaucetError) -> Self {
        Self {
            class: ChunkFailure::Infrastructure,
            err,
        }
    }
}

const TRANSIENT_RETRIES: usize = 3;
/// Base delay for a rolled-back-chunk re-run; jitter/cap come from
/// [`faucet_core::retry::backoff_with_jitter`].
const TRANSIENT_RETRY_BASE: Duration = Duration::from_millis(50);

impl MssqlDriver {
    /// Write a page as one unit; returns the rows written.
    async fn write_whole(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.check_poisoned()?;
        self.ensure_table_ready(records).await?;

        // Staged bulk load (#528): stage the page to Azure and `COPY INTO`.
        #[cfg(feature = "staging")]
        if let Some(staging) = &self.config.staging {
            return self.write_batch_staged(records, staging).await;
        }
        #[cfg(not(feature = "staging"))]
        if self.config.staging.is_some() {
            return Err(FaucetError::Config(
                "mssql: `staging:` is configured but this build lacks the `staging` feature — \
                 rebuild with `--features staging` (CLI: `sink-mssql-staging`)"
                    .into(),
            ));
        }

        // Upsert/delete modes: plan the writes and apply upserts + deletes
        // atomically. Append and overwrite are insert-shaped (overwrite lands in
        // the staging table via `effective_table_quoted`). (NOTE:
        // write_batch_partial upsert routing is handled in the DLQ task;
        // write_batch_idempotent in the exactly-once task.)
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mssql {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            let total = self
                .apply_plans(&page_plans(records, &self.config.write))
                .await?;
            tracing::info!(
                table = %self.config.table,
                mode = self.config.write.write_mode.as_str(),
                rows = total,
                "MSSQL write complete"
            );
            return Ok(total);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0usize;
        for chunk in chunks {
            let Some(prepared) = self.prepare_chunk(chunk).await? else {
                continue;
            };
            // Bounded re-run of chunks the SERVER rolled back (deadlock victim
            // / lock-request timeout). Nothing else is re-run here: for any
            // outcome-unknown failure a re-run duplicates rows. Backoff comes
            // from core so the jitter/cap behaviour cannot drift from the rest
            // of the repo.
            let mut attempt = 0;
            loop {
                let mut conn = self.checkout().await?;
                match self
                    .insert_chunk(&mut conn, &prepared.cols, &prepared.casts, &prepared.rows)
                    .await
                {
                    Ok(n) => {
                        total += n;
                        break;
                    }
                    Err(e)
                        if e.class == ChunkFailure::RolledBack && attempt < TRANSIENT_RETRIES =>
                    {
                        let wait = faucet_core::retry::backoff_with_jitter(
                            TRANSIENT_RETRY_BASE,
                            attempt as u32,
                        );
                        attempt += 1;
                        tracing::warn!(
                            attempt,
                            error = %e.err,
                            "MSSQL chunk was rolled back server-side; re-running"
                        );
                        self.calls.wait(wait).await?;
                    }
                    Err(e) => return Err(e.err),
                }
            }
        }
        tracing::info!(table = %self.config.table, rows = total, "MSSQL write complete");
        Ok(total)
    }

    /// Write the good rows and report each bad one (the dead-letter path).
    async fn write_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        self.check_poisoned()?;
        // The DLQ and exactly-once paths must create a missing target too (#676).
        self.ensure_table_ready(records).await?;

        // Upsert/delete: apply the good rows (upserts + deletes) and route only
        // the rows whose key could not be extracted (missing / null key) to the
        // DLQ per-row. The append/overwrite path below keeps its row-isolation
        // behaviour.
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            self.apply_plans(&page_plans(records, &self.config.write))
                .await?;

            let mut outcomes: Vec<RowOutcome> = records.iter().map(|_| Ok(())).collect();
            for (idx, msg) in &plan.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "mssql {}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return Ok(outcomes);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        // A record matching no writable column is that row's failure, not a
        // silent skip or an all-NULL row (#862 M-044).
        if let MssqlColumnMapping::AutoColumns { .. } = &self.config.column_mapping {
            let writable_cols = crate::statements::insertable(&self.column_infos().await?);
            let unmatched = crate::encode::unmatched_rows(
                &writable_cols,
                &crate::encode::fold_to_columns(&writable_cols, records),
            );
            if !unmatched.is_empty() {
                let writable: Vec<Value> = records
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !unmatched.contains(i))
                    .map(|(_, r)| r.clone())
                    .collect();
                let mut written = if writable.is_empty() {
                    Vec::new().into_iter()
                } else {
                    Box::pin(self.write_partial(&writable)).await?.into_iter()
                };
                return Ok((0..records.len())
                    .map(|i| {
                        if unmatched.contains(&i) {
                            Err(crate::encode::unmatched_row_error(i))
                        } else {
                            written.next().unwrap_or_else(|| {
                                Err(FaucetError::Sink("mssql: missing row outcome".into()))
                            })
                        }
                    })
                    .collect());
            }
        }

        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(records.len());
        for chunk in chunks {
            let Some(Prepared { cols, casts, rows }) = self.prepare_chunk(chunk).await? else {
                // Nothing to insert for this chunk (no matching columns): the
                // rows were effectively dropped per on_unknown_field; report Ok.
                outcomes.extend(chunk.iter().map(|_| Ok(())));
                continue;
            };

            let mut conn = self.checkout().await?;
            match self.insert_chunk(&mut conn, &cols, &casts, &rows).await {
                Ok(_) => outcomes.extend(chunk.iter().map(|_| Ok(()))),
                Err(e) if e.class != ChunkFailure::RowRejected => {
                    // Infra / rolled-back — not row-specific. Propagate so the
                    // pipeline's on_batch_error policy decides. (Previously a
                    // message grep sent permanent constraint violations down
                    // this path, so the poison row never reached the DLQ.)
                    return Err(e.err);
                }
                Err(_) if !self.config.isolate_row_failures => {
                    // One bad row fails the whole batch (caller's choice).
                    return Err(FaucetError::Sink(
                        "MSSQL batch insert failed and isolate_row_failures is disabled".into(),
                    ));
                }
                Err(_) => {
                    // Row-isolate: retry each row alone to find the offender.
                    for (i, row) in rows.iter().enumerate() {
                        let single = std::slice::from_ref(row);
                        let single_cols = cols.clone();
                        match self
                            .insert_chunk(&mut conn, &single_cols, &casts, single)
                            .await
                        {
                            Ok(_) => outcomes.push(Ok(())),
                            Err(e) if e.class != ChunkFailure::RowRejected => {
                                return Err(e.err);
                            }
                            Err(e) => {
                                tracing::warn!(row = i, error = %e.err, "MSSQL row rejected; routing to DLQ");
                                outcomes.push(Err(e.err));
                            }
                        }
                    }
                }
            }
        }
        Ok(outcomes)
    }

    /// Create the staging table as an empty structural clone of the target
    /// (`SELECT * INTO staging FROM target WHERE 1=0`), dropping any leftover
    /// staging from a crashed run first. With `create_table: false` a missing
    /// target makes the `SELECT INTO` error clearly.
    ///
    /// A missing target with `create_table: true` (a first run) has no shape to
    /// clone: the first write creates staging from the page, and the commit
    /// renames it into place (#676). Every step reads the database rather than
    /// sink-instance memory, because the CLI runs begin, the writes and the
    /// commit on different sink instances.
    async fn stage_impl(&self) -> Result<(), FaucetError> {
        let first_run = self.config.create_table && !self.table_exists(&self.config.table).await?;
        // Read before checking out the DDL connection (a one-connection pool).
        let target_infos = self.discover_column_types().await?;
        let staging = &self.staging_table_quoted;
        let target = &self.table_quoted;
        let staging_lit = self.staging_literal().replace('\'', "''");
        let mut conn = self.checkout().await?;
        self.control(
            &mut conn,
            &format!("IF OBJECT_ID(N'{staging_lit}', N'U') IS NOT NULL DROP TABLE {staging}"),
        )
        .await?;
        if first_run {
            return Ok(());
        }
        self.control(
            &mut conn,
            &staging_create_sql(staging, target, &target_infos)?,
        )
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "mssql overwrite: create staging from '{}' (does the table exist?): {e}",
                self.config.table
            ))
        })?;
        if let Some(sql) = staging_relax_sql(staging, &target_infos)? {
            self.control(&mut conn, &sql).await?;
        }
        Ok(())
    }

    /// Atomically replace the destination in one transaction: `DELETE FROM
    /// target`, then `INSERT INTO target (<cols>) SELECT <cols> FROM staging`
    /// over the explicit non-IDENTITY column list (so an IDENTITY column does
    /// not break the copy), then `DROP TABLE staging`. T-SQL DDL/DML is
    /// transactional, so a failure rolls the whole swap back and the prior rows
    /// survive.
    async fn publish_impl(&self) -> Result<(), FaucetError> {
        if !self.table_exists(&self.config.table).await? {
            // First run: staging holds everything; publish it as the target.
            // A run that wrote nothing has no staging either, and leaves no table.
            if self.table_exists(&self.staging_literal()).await? {
                let mut conn = self.checkout().await?;
                self.control(
                    &mut conn,
                    &sp_rename_sql(&self.staging_literal(), &self.config.table),
                )
                .await
                .map_err(|e| FaucetError::Sink(format!("mssql overwrite: publish staging: {e}")))?;
            }
            return Ok(());
        }
        // Column lists from the real target (not the staging clone, where
        // computed and rowversion columns turned into plain ones).
        let target_infos = self.discover_column_types().await?;
        let staging = &self.staging_table_quoted;
        let target = &self.table_quoted;
        let mut swap = overwrite_swap_sql(target, staging, &target_infos)?;
        swap.push(format!("DROP TABLE {staging}"));

        let mut conn = self.checkout().await?;
        self.control(&mut conn, "BEGIN TRAN").await?;
        for stmt in swap {
            if let Err(e) = self.control(&mut conn, &stmt).await {
                let _ = self.control(&mut conn, "ROLLBACK TRAN").await;
                return Err(FaucetError::Sink(format!(
                    "mssql overwrite swap failed: {e}"
                )));
            }
        }
        self.control(&mut conn, "COMMIT TRAN").await?;
        Ok(())
    }

    /// Drop the staging table so a failed/cancelled overwrite leaves nothing
    /// behind. Best-effort — the destination was never touched (on a first run
    /// it was never created).
    async fn discard_impl(&self) -> Result<(), FaucetError> {
        let staging = &self.staging_table_quoted;
        let staging_lit = self.staging_literal().replace('\'', "''");
        let mut conn = self.checkout().await?;
        self.control(
            &mut conn,
            &format!("IF OBJECT_ID(N'{staging_lit}', N'U') IS NOT NULL DROP TABLE {staging}"),
        )
        .await?;
        Ok(())
    }

    /// Read the live destination schema from `sys.columns` as an
    /// `infer_schema`-shaped object (`{"type":"object","properties":{…}}`), or
    /// `None` when the target table does not exist yet (issue #194).
    async fn current_schema_impl(&self) -> Result<Option<Value>, FaucetError> {
        // A JSON column stores each record whole; its physical columns are not
        // the record's fields, so there is nothing to drift against (SQL-20).
        if matches!(
            self.config.column_mapping,
            MssqlColumnMapping::JsonColumn { .. }
        ) {
            return Ok(None);
        }
        let cols = self.discover_column_types().await?;
        if cols.is_empty() {
            return Ok(None); // table does not exist yet
        }
        let mut props = serde_json::Map::new();
        for c in cols {
            props.insert(
                c.name,
                mssql_type_to_json_schema(&c.type_name, c.max_length, c.is_nullable),
            );
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    /// Apply an additive schema evolution (new columns, lossless widenings,
    /// nullability relaxations) to the destination table. Idempotent — the
    /// `ADD` is guarded with `IF NOT EXISTS (SELECT 1 FROM sys.columns …)`, and
    /// re-running the same `ALTER COLUMN` type / `… NULL` is a no-op (issue #194).
    async fn evolve_impl(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        // Read before checking out the DDL connection (a one-connection pool).
        let current = if evolution.widenings.is_empty() && evolution.relax_nullability.is_empty() {
            Vec::new()
        } else {
            self.discover_column_types().await?
        };
        let mut conn = self.checkout().await?;

        for c in &evolution.additions {
            let t =
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text);
            let sql = build_add_column_sql(&self.table_quoted, &self.config.table, &c.name, t)?;
            self.control(&mut conn, &sql).await.map_err(|e| {
                FaucetError::Sink(format!("MSSQL ADD COLUMN {} failed: {e}", c.name))
            })?;
        }
        for c in &evolution.widenings {
            let t =
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text);
            let from = c.from.as_ref().and_then(faucet_core::json_schema_base_type);
            let nullable = widened_nullability(
                c.from.as_ref(),
                &c.to,
                current
                    .iter()
                    .find(|i| i.name == c.name)
                    .map(|i| i.is_nullable),
            );
            let sql = build_alter_type_sql(
                &self.table_quoted,
                &c.name,
                widened_keyword(from, t),
                nullable,
            )?;
            self.control(&mut conn, &sql).await.map_err(|e| {
                FaucetError::Sink(format!("MSSQL ALTER COLUMN {} failed: {e}", c.name))
            })?;
        }
        if !evolution.relax_nullability.is_empty() {
            // `ALTER COLUMN … NULL` must re-state the column's exact declared type
            // (length, precision, scale, collation); anything coarser rewrites the
            // column (SQL-34).
            for col in &evolution.relax_nullability {
                let Some(info) = current.iter().find(|c| &c.name == col) else {
                    // Column not found in the live schema — nothing to relax.
                    continue;
                };
                let kw = info.declared_with_collation().ok_or_else(|| {
                    FaucetError::Sink(format!(
                        "MSSQL relax NULL {col}: cannot re-state type '{}' without changing it",
                        info.type_name
                    ))
                })?;
                let sql = build_alter_null_sql(&self.table_quoted, col, &kw)?;
                self.control(&mut conn, &sql).await.map_err(|e| {
                    FaucetError::Sink(format!("MSSQL relax NULL {col} failed: {e}"))
                })?;
            }
        }

        // Columns changed — drop the cached AutoColumns set so the next write
        // re-discovers them (a newly-added column must be picked up).
        *self.columns_cache.lock().expect("columns mutex") = None;
        Ok(())
    }

    async fn last_token_impl(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let mut conn = self.checkout().await?;
        self.ensure_commit_table(&mut conn).await?;
        for scope_owned in std::iter::once(scope_key(scope)).chain(legacy_scope_key(scope)) {
            let sql = Mssql.commit_token_select_sql();
            let rows = self
                .on_conn(&mut conn, "query", true, async |c| {
                    c.query(&sql, &[&scope_owned])
                        .await
                        .map_err(|e| FaucetError::Sink(format!("MSSQL token read failed: {e}")))?
                        .into_first_result()
                        .await
                        .map_err(|e| FaucetError::Sink(format!("MSSQL token read failed: {e}")))
                })
                .await?;
            if let Some(token) = rows.first().and_then(|r| r.get::<&str, _>("token")) {
                return Ok(Some(token.to_string()));
            }
        }
        Ok(None)
    }

    async fn write_with_token_impl(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.check_poisoned()?;
        // The DLQ and exactly-once paths must create a missing target too (#676).
        self.ensure_table_ready(records).await?;
        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mssql {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        } else {
            None
        };

        if plan.is_some()
            || matches!(
                self.config.column_mapping,
                MssqlColumnMapping::AutoColumns { .. }
            )
        {
            // Warm the column cache before holding a connection (see apply_plan).
            self.column_infos().await?;
        }
        let mut conn = self.checkout().await?;
        self.ensure_commit_table(&mut conn).await?;
        self.control(&mut conn, "BEGIN TRAN").await?;

        // Data write and the commit-token MERGE share ONE transaction so the
        // page is committed atomically with its watermark. For upsert/delete the
        // planned upserts/deletes run via the same no-txn MERGE helpers used by
        // the append path's INSERTs, inside this same BEGIN TRAN.
        let written = match &plan {
            Some(_) => {
                let plans = page_plans(records, &self.config.write);
                match self.apply_plans_no_txn(&mut conn, &plans).await {
                    Ok(n) => n,
                    Err((e, timed_out)) => {
                        self.abort_txn(&mut conn, timed_out).await;
                        return Err(e);
                    }
                }
            }
            None => match self.prepare_chunk(records).await {
                Ok(Some(p)) => {
                    match self
                        .insert_rows_no_txn(&mut conn, &p.cols, &p.casts, &p.rows)
                        .await
                    {
                        Ok(n) => n,
                        Err((e, timed_out)) => {
                            self.abort_txn(&mut conn, timed_out).await;
                            return Err(e);
                        }
                    }
                }
                Ok(None) => 0,
                Err(e) => {
                    let _ = self.control(&mut conn, "ROLLBACK TRAN").await;
                    return Err(e);
                }
            },
        };

        // UPSERT the commit token atomically with the data rows.
        let merge = Mssql.commit_token_upsert_sql();
        let (scope_owned, token_owned) = (scope_key(scope), token.to_string());
        let refs: Vec<&dyn ToSql> = vec![&scope_owned, &token_owned];
        let merged = self
            .on_conn(&mut conn, "merge", true, async |c| {
                c.execute(merge.as_str(), &refs)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL token merge failed: {e}")))
            })
            .await;
        if let Err(e) = merged {
            let _ = self.control(&mut conn, "ROLLBACK TRAN").await;
            return Err(e);
        }
        if let Some(legacy) = legacy_scope_key(scope) {
            let delete = format!(
                "DELETE FROM [{}] WHERE [scope] = @P1",
                faucet_core::idempotency::COMMIT_TOKEN_TABLE
            );
            let deleted = self
                .on_conn(&mut conn, "delete", true, async |c| {
                    c.execute(delete.as_str(), &[&legacy]).await.map_err(|e| {
                        FaucetError::Sink(format!("MSSQL legacy token delete failed: {e}"))
                    })
                })
                .await;
            if let Err(e) = deleted {
                let _ = self.control(&mut conn, "ROLLBACK TRAN").await;
                return Err(e);
            }
        }

        self.control(&mut conn, "COMMIT TRAN").await?;
        Ok(written)
    }

    /// A keyed page accepts one row per planned write and coalesces the rest.
    fn report(&self, offered: usize, written: usize) -> WriteReport {
        if self.config.write.dedups_by_key() {
            WriteReport::keyed(offered, written)
        } else {
            WriteReport::accepted(written)
        }
    }
}

/// Every statement goes through the driver's own call slot (`on_conn`), which
/// the adapter keeps in step with the run's context; the slot each primitive
/// receives carries the same context.
#[async_trait]
impl SinkDriver for MssqlDriver {
    fn call_context(&self) -> faucet_core::CallContext {
        faucet_common_mssql::statement_call_context(self.config.statement_timeout_secs)
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    async fn write(
        &self,
        _calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        match policy {
            RowPolicy::PerRow => Ok(PageWrite::from_outcomes(self.write_partial(rows).await?)),
            _ => {
                let n = self.write_whole(rows).await?;
                Ok(PageWrite::reported(self.report(rows.len(), n)))
            }
        }
    }

    fn write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn staging(&self) -> Option<&dyn StagingDriver> {
        Some(self)
    }

    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        Some(self)
    }

    /// Column-mapping mode only: the scope + key predicates address real
    /// columns, which a single NVARCHAR(MAX) JSON payload column does not have.
    fn cleanup(&self) -> Option<&dyn CleanupDriver> {
        matches!(
            self.config.column_mapping,
            MssqlColumnMapping::AutoColumns { .. }
        )
        .then_some(self as &dyn CleanupDriver)
    }

    fn schema(&self) -> Option<&dyn SchemaDriver> {
        Some(self)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(MssqlSinkConfig))
            .expect("schema serialization")
    }

    /// Staged bulk load is active only with a `staging:` block and the
    /// `staging` feature compiled in.
    fn staged_load(&self) -> bool {
        cfg!(feature = "staging") && self.config.staging.is_some()
    }

    fn connector_name(&self) -> &'static str {
        "mssql"
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
            "{}?table={}",
            faucet_core::redact_uri_credentials(conn),
            self.config.table
        )
    }

    async fn check(
        &self,
        _calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.pool.get()).await {
            Ok(Ok(_conn)) => Probe::pass("connect", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                e.to_string(),
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
            Err(_) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                "timed out",
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

#[async_trait]
impl StagingDriver for MssqlDriver {
    fn active(&self) -> bool {
        self.config.write.is_overwrite()
    }

    async fn stage(&self, _calls: &CallSlot) -> Result<(), FaucetError> {
        self.stage_impl().await
    }

    async fn publish(&self, _calls: &CallSlot) -> Result<(), FaucetError> {
        self.publish_impl().await
    }

    async fn discard(&self, _calls: &CallSlot) -> Result<(), FaucetError> {
        self.discard_impl().await
    }

    /// Probe for the `<table>__faucet_ovw` staging table (read-only).
    async fn staged(&self, _calls: &CallSlot) -> Result<bool, FaucetError> {
        self.table_exists(&self.staging_literal()).await
    }
}

#[async_trait]
impl CommitTokenDriver for MssqlDriver {
    async fn last_token(
        &self,
        _calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError> {
        self.last_token_impl(scope).await
    }

    async fn write_with_token(
        &self,
        _calls: &CallSlot,
        rows: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError> {
        let n = self.write_with_token_impl(rows, scope, token).await?;
        Ok(self.report(rows.len(), n))
    }
}

#[async_trait]
impl CleanupDriver for MssqlDriver {
    async fn delete_absent(
        &self,
        _calls: &CallSlot,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }
}

#[async_trait]
impl SchemaDriver for MssqlDriver {
    async fn current_schema(&self, _calls: &CallSlot) -> Result<Option<Value>, FaucetError> {
        self.current_schema_impl().await
    }

    fn evolvable(&self) -> bool {
        true
    }

    async fn evolve(
        &self,
        _calls: &CallSlot,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.evolve_impl(evolution).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_splits_into_op_runs_only_on_a_collation_collision() {
        use serde_json::json;
        let spec: faucet_core::WriteSpec = serde_json::from_value(json!({
            "write_mode": "upsert",
            "key": ["id"],
            "delete_marker": { "field": "op", "values": ["d"] }
        }))
        .unwrap();
        let interleaved = [
            json!({"id": "a", "op": "d"}),
            json!({"id": "b", "v": 1}),
            json!({"id": "c", "op": "d"}),
            json!({"id": "e", "v": 2}),
        ];
        assert_eq!(page_plans(&interleaved, &spec).len(), 1);
        let colliding = [
            json!({"id": "Bob", "op": "d"}),
            json!({"id": "bob ", "v": 1}),
        ];
        assert_eq!(page_plans(&colliding, &spec).len(), 2);
        let accented = [json!({"id": "José", "op": "d"}), json!({"id": "x", "v": 1})];
        assert_eq!(page_plans(&accented, &spec).len(), 2);
        let numbers = [json!({"id": 1, "op": "d"}), json!({"id": 2, "v": 1})];
        assert_eq!(page_plans(&numbers, &spec).len(), 1);
        let only_upserts = [json!({"id": "a"}), json!({"id": "A"})];
        assert_eq!(page_plans(&only_upserts, &spec).len(), 1);
    }

    // dataset_uri test is skipped: MssqlSink::new() requires a live pool
    // (connects to SQL Server in new()), and no offline constructor exists.

    #[test]
    fn create_table_sql_for_a_keyed_write_bounds_key_text_and_adds_a_primary_key() {
        let key = vec!["sku".to_string()];
        let cols = faucet_core::plan_keyed_columns(
            &[serde_json::json!({ "sku": "a", "note": "n", "qty": 1 })],
            &key,
        )
        .expect("a plan");
        let sql = build_create_table_sql("o'rders", "[orders]", &cols, &key).unwrap();
        assert!(
            sql.starts_with("IF OBJECT_ID(N'o''rders', N'U') IS NULL CREATE TABLE [orders] ("),
            "{sql}"
        );
        assert!(sql.contains("[sku] NVARCHAR(450)"), "{sql}");
        assert!(sql.contains("[note] NVARCHAR(MAX)"), "{sql}");
        assert!(sql.ends_with(", PRIMARY KEY ([sku]))"), "{sql}");
        assert_eq!(
            mssql_key_keyword(faucet_core::SqlBaseType::Integer),
            mssql_keyword(faucet_core::SqlBaseType::Integer)
        );
    }

    #[test]
    fn concurrent_create_swallows_only_object_exists() {
        let sql =
            tolerate_concurrent_create("IF OBJECT_ID(N't', N'U') IS NULL CREATE TABLE [t] (a INT)");
        assert_eq!(
            sql,
            "BEGIN TRY IF OBJECT_ID(N't', N'U') IS NULL CREATE TABLE [t] (a INT) END TRY \
             BEGIN CATCH IF ERROR_NUMBER() <> 2714 THROW; END CATCH"
        );
    }

    #[test]
    fn sp_rename_takes_the_bare_new_name() {
        assert_eq!(
            sp_rename_sql("dbo.t__faucet_ovw", "dbo.t"),
            "EXEC sp_rename N'dbo.t__faucet_ovw', N't'"
        );
        assert_eq!(
            sp_rename_sql("o'k__x", "o'k"),
            "EXEC sp_rename N'o''k__x', N'o''k'"
        );
    }

    #[test]
    fn create_table_sql_without_a_key_has_no_primary_key() {
        let cols = faucet_core::plan_columns(&[serde_json::json!({ "a": 1 })]).unwrap();
        let sql = build_create_table_sql("t", "[t]", &cols, &[]).unwrap();
        assert!(!sql.contains("PRIMARY KEY"), "{sql}");
    }

    #[test]
    fn quote_table_handles_schema_qualified() {
        assert_eq!(quote_table("dbo.events").unwrap(), "[dbo].[events]");
        assert_eq!(quote_table("events").unwrap(), "[events]");
        assert_eq!(
            quote_table("my.sales.events").unwrap(),
            "[my].[sales].[events]"
        );
    }

    #[test]
    fn idempotency_constant_names() {
        // The commit table and column constants used in ensure_commit_table,
        // last_committed_token, and write_batch_idempotent must match the
        // canonical values from faucet_core::idempotency.
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            "_faucet_commit_token",
            "COMMIT_TOKEN_TABLE name changed — update DDL and queries"
        );
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL,
            "scope",
            "COMMIT_TOKEN_SCOPE_COL name changed — update DDL and queries"
        );
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
            "token",
            "COMMIT_TOKEN_TOKEN_COL name changed — update DDL and queries"
        );
    }

    #[test]
    fn mssql_add_column_ddl() {
        let sql = build_add_column_sql(
            "[dbo].[events]",
            "dbo.events",
            "email",
            faucet_core::SqlBaseType::Text,
        )
        .unwrap();
        assert!(sql.starts_with("IF NOT EXISTS"), "{sql}");
        assert!(
            sql.contains("ALTER TABLE [dbo].[events] ADD [email] NVARCHAR(MAX)"),
            "{sql}"
        );
        // The OBJECT_ID guard targets the bare table literal.
        assert!(sql.contains("OBJECT_ID(N'dbo.events')"), "{sql}");
        assert!(sql.contains("name = N'email'"), "{sql}");
    }

    #[test]
    fn mssql_add_column_keyword_per_base_type() {
        use faucet_core::SqlBaseType::*;
        for (t, kw) in [
            (Integer, "BIGINT"),
            (Double, "FLOAT"),
            (Boolean, "BIT"),
            (Text, "NVARCHAR(MAX)"),
            (Json, "NVARCHAR(MAX)"),
        ] {
            let sql = build_add_column_sql("[t]", "t", "c", t).unwrap();
            assert!(sql.ends_with(&format!("ADD [c] {kw}")), "{t:?}: {sql}");
        }
    }

    #[test]
    fn mssql_add_column_escapes_literals() {
        // Single quotes in the table/column literal are doubled for N'…';
        // brackets in the identifier are doubled by quote_ident_mssql.
        let sql = build_add_column_sql("[d].[t]", "d.o'x", "c'l", faucet_core::SqlBaseType::Text)
            .unwrap();
        assert!(sql.contains("OBJECT_ID(N'd.o''x')"), "{sql}");
        assert!(sql.contains("name = N'c''l'"), "{sql}");
        assert!(sql.contains("ADD [c'l] NVARCHAR(MAX)"), "{sql}");
    }

    #[test]
    fn mssql_widen_column_ddl() {
        use faucet_core::SqlBaseType::{Double, Integer};
        let sql = build_alter_type_sql("[dbo].[t]", "score", widened_keyword(None, Double), None)
            .unwrap();
        assert_eq!(sql, "ALTER TABLE [dbo].[t] ALTER COLUMN [score] FLOAT");
        let sql = build_alter_type_sql(
            "[t]",
            "v",
            widened_keyword(Some(Integer), Double),
            Some(false),
        )
        .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE [t] ALTER COLUMN [v] DECIMAL(38,10) NOT NULL"
        );
        let sql = build_alter_type_sql("[t]", "v", "BIGINT", Some(true)).unwrap();
        assert_eq!(sql, "ALTER TABLE [t] ALTER COLUMN [v] BIGINT NULL");
    }

    #[test]
    fn a_widening_that_gains_null_relaxes_not_null() {
        let int = serde_json::json!({"type": "integer"});
        let nullable = serde_json::json!({"type": ["number", "null"]});
        let number = serde_json::json!({"type": "number"});
        assert_eq!(
            widened_nullability(Some(&int), &nullable, Some(false)),
            Some(true)
        );
        assert_eq!(
            widened_nullability(Some(&int), &number, Some(false)),
            Some(false)
        );
        assert_eq!(widened_nullability(None, &nullable, None), None);
        assert_eq!(
            widened_nullability(Some(&nullable), &nullable, Some(true)),
            Some(true)
        );
    }

    #[test]
    fn mssql_relax_null_ddl_re_emits_current_type() {
        let sql = build_alter_null_sql("[t]", "created_at", "DATETIME2").unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE [t] ALTER COLUMN [created_at] DATETIME2 NULL"
        );
    }

    #[test]
    fn mssql_type_round_trips_to_json_schema() {
        use serde_json::json;
        assert_eq!(
            mssql_type_to_json_schema("bigint", 100, false),
            json!({"type":"integer"})
        );
        assert_eq!(
            mssql_type_to_json_schema("int", 100, false),
            json!({"type":"integer"})
        );
        assert_eq!(
            mssql_type_to_json_schema("float", 100, false),
            json!({"type":"number"})
        );
        assert_eq!(
            mssql_type_to_json_schema("decimal", 100, false),
            json!({"type":"number"})
        );
        assert_eq!(
            mssql_type_to_json_schema("bit", 100, false),
            json!({"type":"boolean"})
        );
        assert_eq!(
            mssql_type_to_json_schema("nvarchar", 100, false),
            json!({"type":"string"})
        );
        // Case-insensitive; nullable widens to a type array.
        assert_eq!(
            mssql_type_to_json_schema("NVARCHAR", 100, true),
            json!({"type":["string","null"]})
        );
        for (ty, len) in [
            ("nvarchar", -1),
            ("varchar", -1),
            ("ntext", 16),
            ("text", 16),
        ] {
            assert_eq!(
                mssql_type_to_json_schema(ty, len, false),
                json!({"type": "string", "x-faucet-also-accepts": ["object", "array"]}),
                "{ty}"
            );
        }
    }

    #[test]
    fn chunk_failures_are_classified_from_typed_server_codes() {
        use super::{ChunkFailure, classify_chunk_failure};
        use tiberius::error::Error;

        // Transport-level → infrastructure: propagate, never blame a row,
        // never re-run (the write may have committed).
        assert_eq!(
            classify_chunk_failure(&Error::Tls("handshake".into())),
            ChunkFailure::Infrastructure
        );
        assert_eq!(
            classify_chunk_failure(&Error::Protocol("desync".into())),
            ChunkFailure::Infrastructure
        );
        // Data/encoding problems are about the rows being sent → DLQ candidate.
        assert_eq!(
            classify_chunk_failure(&Error::Conversion("bad datetime".into())),
            ChunkFailure::RowRejected
        );
        assert_eq!(
            classify_chunk_failure(&Error::Utf8),
            ChunkFailure::RowRejected
        );
    }

    #[test]
    fn a_constraint_violation_naming_a_connection_column_is_still_row_rejected() {
        // The exact regression: SQL Server error text embeds user identifiers,
        // so the old substring rule read
        // `Violation of UNIQUE KEY constraint 'UQ_connection_id'` as
        // "connection" → transient → propagated forever, and the poison row
        // never reached the DLQ. Classification no longer reads the message,
        // so a permanent server error stays row-scoped whatever it says.
        use super::{ChunkFailure, classify_chunk_failure};
        use tiberius::error::Error;
        // 2627 = unique-constraint violation; the message is deliberately
        // packed with every needle the old rule matched on.
        let e = Error::Conversion(
            "Violation of UNIQUE KEY constraint 'UQ_connection_timeout_deadlock_1205'".into(),
        );
        assert_eq!(
            classify_chunk_failure(&e),
            ChunkFailure::RowRejected,
            "message text must not influence the verdict"
        );
    }

    #[test]
    fn quote_columns_rejects_a_bad_identifier_as_infrastructure() {
        use super::{ChunkFailure, quote_columns};
        // Normal columns quote through.
        let ok = quote_columns(&["id".to_string(), "order date".to_string()]).unwrap();
        assert_eq!(ok, vec!["[id]".to_string(), "[order date]".to_string()]);
        // A rejected identifier is a schema/config problem shared by every row
        // in the chunk, so it must NOT be classified as a row rejection (which
        // would DLQ one arbitrary row and keep going).
        let err = quote_columns(&["ok".to_string(), "bad\0name".to_string()])
            .expect_err("NUL is not a legal identifier");
        assert_eq!(err.class, ChunkFailure::Infrastructure);
    }

    #[test]
    fn chunk_error_conversions_default_to_infrastructure() {
        use super::{ChunkError, ChunkFailure};
        // A plain FaucetError reaching a `?` in the chunk path is, by
        // definition, not a typed *server* rejection — so it must classify as
        // infrastructure (propagate) and never as a row rejection (which would
        // DLQ one arbitrary row and carry on).
        let ce: ChunkError = FaucetError::Sink("pool checkout failed".into()).into();
        assert_eq!(ce.class, ChunkFailure::Infrastructure);
        // Converting back preserves the user-facing error verbatim.
        let back: FaucetError = ce.into();
        assert!(back.to_string().contains("pool checkout failed"), "{back}");
    }

    #[test]
    fn server_error_numbers_map_to_the_right_handling() {
        use super::{AZURE_TRANSIENT, ChunkFailure, classify_server_code};
        // Rolled back server-side → the ONLY class safe to re-run.
        assert_eq!(classify_server_code(1205), ChunkFailure::RolledBack);
        assert_eq!(classify_server_code(1222), ChunkFailure::RolledBack);
        // Azure throttle/failover → propagate, never re-run (outcome unknown),
        // never blame a row.
        for code in AZURE_TRANSIENT {
            assert_eq!(
                classify_server_code(*code),
                ChunkFailure::Infrastructure,
                "code {code}"
            );
        }
        // Anything else the server reports is row-specific → DLQ candidate.
        // 2627 = unique-constraint violation, the poison row that the old
        // substring rule misread as transient and propagated forever.
        assert_eq!(classify_server_code(2627), ChunkFailure::RowRejected);
        assert_eq!(classify_server_code(8152), ChunkFailure::RowRejected);
        assert_eq!(classify_server_code(515), ChunkFailure::RowRejected);
        // Environmental faults fail the batch instead of quarantining every
        // row: log full, filegroup full, invalid object, permission, memory.
        for code in [9002, 1105, 208, 229, 701, 50000] {
            assert_eq!(
                classify_server_code(code),
                ChunkFailure::Infrastructure,
                "code {code}"
            );
        }
    }
}
