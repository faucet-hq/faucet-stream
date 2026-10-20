//! The SQL family driver (RFC 0014).
//!
//! A [`SqlDriver`] is a backend's I/O over its database driver: open and
//! commit a write transaction, probe and create tables, read the commit
//! token, and the few statements whose shape is genuinely the backend's own
//! (cloning a table into staging, the overwrite swap, the catalog reads).
//! [`SqlSinkDriver`] drives everything else from those primitives and the
//! shared [`SqlSinkEngine`]: the create-on-first-write rule, keyed and
//! chunked writes, per-row outcomes, the exactly-once commit (rows and token
//! in one transaction), scoped cleanup, the overwrite lifecycle (stage,
//! first-run publish or swap, discard) and schema evolution. Wrap it in a
//! [`DriverSink`](faucet_core::driver::DriverSink) for a `Sink`.
//!
//! ```text
//! SqliteSink ─ DriverSink<SqlSinkDriver<SqliteDriver>>
//!                         │               └ SqlDriver: begin / commit / ddl / table_exists / …
//!                         └ SinkDriver + StagingDriver + CommitTokenDriver + CleanupDriver + SchemaDriver
//! ```

use crate::dialect::{SqlDialect, TableTarget};
use crate::engine::{EvolutionStep, SplitPage, SqlExecutor, SqlSinkEngine};
use crate::mapping::TableColumn;
use faucet_core::check::{CheckContext, CheckReport};
use faucet_core::cleanup::SeenKeys;
use faucet_core::driver::{
    CleanupDriver, CommitTokenDriver, PageWrite, RollbackDriver, RowPolicy, SchemaDriver,
    SinkDriver, StagingDriver,
};
use faucet_core::{
    BatchAtomicity, CallContext, CallSlot, Capabilities, FaucetError, SchemaEvolution, WriteMode,
    WritePlan, WriteReport,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};

/// A boxed, sendable backend operation, as [`SqlDriver::guard`] wraps it.
pub type SqlFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, FaucetError>> + Send + 'a>>;

/// A backend's schema evolution (RFC 0014), returned by
/// [`SqlDriver::evolution`]: implementing it and declaring it are one step.
#[faucet_core::async_trait]
pub trait SqlEvolution: Send + Sync {
    /// The destination's column names, for a dialect without `ADD COLUMN IF
    /// NOT EXISTS`; `None` when every step is idempotent as written.
    async fn existing_columns(&self) -> Result<Option<HashSet<String>>, FaucetError> {
        Ok(None)
    }

    /// Apply the evolution steps, in order.
    async fn apply_evolution(&self, steps: Vec<EvolutionStep>) -> Result<(), FaucetError>;
}

/// Which write an [`SqlDriver::insert`] call serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InsertPath {
    /// A plain append or overwrite page.
    Page,
    /// The rows of an exactly-once page, inside the transaction that also
    /// writes its commit token.
    Token,
}

/// The `what` labels [`SqlSinkDriver`] passes to [`SqlDriver::ddl`] and
/// [`SqlDriver::guard`], for a backend that treats one of them specially.
pub mod label {
    /// Creating the destination table.
    pub const CREATE_TABLE: &str = "CREATE TABLE";
    /// Creating the commit-token table.
    pub const CREATE_TOKEN_TABLE: &str = "commit-table create";
    /// The overwrite swap.
    pub const COMMIT_OVERWRITE: &str = "commit_overwrite";
    /// The scoped cleanup delete.
    pub const CLEANUP_SCOPE: &str = "cleanup_scope";
}

/// Where a SQL sink writes and how its rows are shaped.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SqlLayout {
    /// The destination table.
    pub table: String,
    /// `Some(column)`: each record is stored whole, as JSON, in `column`;
    /// `None`: record fields map onto the table's columns.
    pub json_column: Option<String>,
    /// Create a missing table from the first page.
    pub create_table: bool,
    /// Rows per append transaction (`0`: the whole page).
    pub batch_size: usize,
}

impl SqlLayout {
    /// A column-mapped layout over `table` that creates it when missing and
    /// writes each page in one transaction.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            json_column: None,
            create_table: true,
            batch_size: 0,
        }
    }

    /// Store each record whole in `column`.
    pub fn json_column(mut self, column: impl Into<String>) -> Self {
        self.json_column = Some(column.into());
        self
    }

    /// Whether a missing table is created.
    pub fn create_table(mut self, on: bool) -> Self {
        self.create_table = on;
        self
    }

    /// Rows per append transaction.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// The staging table an overwrite run writes into.
    pub fn staging_table(&self) -> String {
        format!(
            "{}{}",
            self.table,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }
}

/// A SQL backend's I/O primitives (RFC 0014). See the [module docs](self).
///
/// `Conn` is one connection the engine runs statements on: an open write
/// transaction ([`begin`](Self::begin)) or a plain connection for catalog
/// reads ([`connection`](Self::connection)). Errors are worded by the driver
/// (`"SQLite transaction begin failed: …"`).
#[faucet_core::async_trait]
pub trait SqlDriver: Send + Sync {
    /// The backend's SQL dialect.
    type Dialect: SqlDialect;
    /// A connection the engine runs statements on.
    type Conn<'a>: SqlExecutor + Send
    where
        Self: 'a;

    /// The engine planning this sink's writes.
    fn engine(&self) -> &SqlSinkEngine<Self::Dialect>;

    /// Where the sink writes.
    fn layout(&self) -> &SqlLayout;

    /// The connector's name.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value;

    /// The destination, for lineage (credential-free).
    fn dataset_uri(&self) -> String;

    /// The connector's own call defaults.
    fn call_context(&self) -> CallContext {
        CallContext::new().without_timeout()
    }

    /// A non-mutating connectivity probe.
    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let _ = ctx;
        Ok(CheckReport::not_implemented())
    }

    /// What a failed write leaves behind.
    fn batch_atomicity(&self) -> BatchAtomicity {
        BatchAtomicity::Atomic
    }

    /// The write modes the backend applies.
    fn write_modes(&self) -> &'static [WriteMode] {
        &[
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ]
    }

    /// The quoted target of `table`. Default: the dialect's quoting.
    fn target(&self, table: &str) -> TableTarget {
        TableTarget::new(table, self.engine().dialect().quote_ident(table))
    }

    /// Open a write transaction.
    async fn begin(&self) -> Result<Self::Conn<'_>, FaucetError>;

    /// Commit a transaction [`begin`](Self::begin) opened.
    async fn commit(&self, conn: Self::Conn<'_>) -> Result<(), FaucetError>;

    /// A connection for catalog reads (no transaction).
    async fn connection(&self) -> Result<Self::Conn<'_>, FaucetError>;

    /// Whether `table` exists.
    async fn table_exists(&self, table: &str) -> Result<bool, FaucetError>;

    /// Run one DDL statement outside a transaction; `what` names it in the
    /// error (`"CREATE TABLE"`).
    async fn ddl(&self, sql: &str, what: &'static str) -> Result<(), FaucetError>;

    /// The token committed under `scope`, from the commit-token table (which
    /// exists).
    async fn read_token(&self, scope: &str) -> Result<Option<String>, FaucetError>;

    /// Create the staging table as a copy of the destination's definition,
    /// refusing a destination that cannot be replaced safely.
    async fn create_staging(&self) -> Result<(), FaucetError>;

    /// First run: make the staging table the destination (which does not
    /// exist).
    async fn publish_staging(&self) -> Result<(), FaucetError>;

    /// Replace the destination's rows with the staging table's, atomically,
    /// and drop staging.
    async fn swap_staging(&self) -> Result<(), FaucetError>;

    /// The live destination schema, or `None` (missing table, JSON layout).
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError>;

    /// The backend's schema evolution, or `None` when it cannot evolve:
    /// returning the group is what declares the capability.
    fn evolution(&self) -> Option<&dyn SqlEvolution> {
        None
    }

    /// Bound one sink operation (`what` is its call label: `"write_batch"`,
    /// `"begin_overwrite"`, …), for a backend with its own per-operation
    /// timeout. Default: unbounded.
    fn guard<'a, T: Send + 'a>(
        &'a self,
        what: &'static str,
        fut: SqlFuture<'a, T>,
    ) -> SqlFuture<'a, T> {
        let _ = what;
        fut
    }

    /// Adjust the capabilities the adapter derived (scoped overwrite, …).
    fn capabilities(&self, derived: Capabilities) -> Capabilities {
        derived
    }

    /// The staging table an overwrite run writes into. Default:
    /// [`SqlLayout::staging_table`]; a backend with an identifier length
    /// limit shortens it.
    fn staging_table(&self) -> String {
        self.layout().staging_table()
    }

    /// How an error names `table` (the missing-target refusal). Default: as
    /// configured.
    fn table_display(&self, table: &str) -> String {
        table.to_string()
    }

    /// Insert `records` into `target` on `conn` (append, overwrite and the
    /// exactly-once page). Default: the engine's mapped insert, or the JSON
    /// column insert.
    async fn insert(
        &self,
        conn: &mut Self::Conn<'_>,
        target: &TableTarget,
        records: &[Value],
        path: InsertPath,
    ) -> Result<usize, FaucetError> {
        let _ = path;
        match &self.layout().json_column {
            Some(column) => {
                self.engine()
                    .insert_json(conn, target, column, records)
                    .await
            }
            None => self.engine().insert(conn, target, records, false).await,
        }
    }

    /// Record `token` under `scope` on the page's transaction. Default: the
    /// engine's token upsert.
    async fn write_token(
        &self,
        conn: &mut Self::Conn<'_>,
        scope: &str,
        token: &str,
    ) -> Result<(), FaucetError> {
        self.engine().write_token(conn, scope, token).await
    }

    /// Run before a keyed write opens its transaction (a backend whose
    /// journal DDL commits implicitly creates the journal here). Default:
    /// nothing.
    async fn prepare_keyed_write(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Drop the overwrite staging table (and anything else a swap leaves
    /// behind); `stale` marks the drop before a run stages. Default: `DROP
    /// TABLE IF EXISTS` of the staging table.
    async fn drop_staging(&self, stale: bool) -> Result<(), FaucetError> {
        let staging = self.target(&self.staging_table());
        let what = if stale {
            "overwrite: drop stale staging"
        } else {
            "overwrite: drop staging"
        };
        self.ddl(&format!("DROP TABLE IF EXISTS {}", staging.reference), what)
            .await
    }

    /// Whether an overwrite left staging behind (a read-only probe).
    /// Default: the staging table exists.
    async fn staging_exists(&self) -> Result<bool, FaucetError> {
        self.table_exists(&self.staging_table()).await
    }

    /// Split a page on the dead-letter path into the rows to write and
    /// per-row failures, over the destination's `columns`. Default: a record
    /// matching no column (when `require_match`) fails its own row.
    fn split_page(
        &self,
        target: &TableTarget,
        records: &[Value],
        columns: &[TableColumn],
        require_match: bool,
    ) -> SplitPage {
        self.engine()
            .split_writable(target, records, columns, require_match, |_, _, _| None)
    }

    /// Whether a keyed page on the dead-letter path is split by
    /// [`split_page`](Self::split_page) before it is planned (a backend that
    /// refuses some values per row). Default: no.
    fn splits_keyed_pages(&self) -> bool {
        false
    }

    /// The rollback primitives, when this configuration supports rollback.
    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        None
    }

    /// A `(kind, config)` source that reads the destination back.
    fn readback_source(&self) -> Option<(String, Value)> {
        None
    }
}

/// A [`SqlDriver`] as a [`SinkDriver`] (RFC 0014). See the [module docs](self).
pub struct SqlSinkDriver<D> {
    driver: D,
    table_ready: AtomicBool,
    token_table_ready: AtomicBool,
}

impl<D: SqlDriver> SqlSinkDriver<D> {
    /// The sink driver over `driver`.
    pub fn new(driver: D) -> Self {
        Self {
            driver,
            table_ready: AtomicBool::new(false),
            token_table_ready: AtomicBool::new(false),
        }
    }

    /// The SQL driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    fn engine(&self) -> &SqlSinkEngine<D::Dialect> {
        self.driver.engine()
    }

    fn layout(&self) -> &SqlLayout {
        self.driver.layout()
    }

    /// One sink operation as a backend call: the run's deadline, then the
    /// driver's own bound.
    async fn op<'a, T: Send + 'a>(
        &'a self,
        calls: &CallSlot,
        what: &'static str,
        fut: impl Future<Output = Result<T, FaucetError>> + Send + 'a,
    ) -> Result<T, FaucetError> {
        calls
            .call_once(what, self.driver.guard(what, Box::pin(fut)))
            .await
    }

    /// The table data writes land in: staging while overwriting.
    fn effective_table(&self) -> String {
        if self.engine().write_spec().is_overwrite() {
            self.driver.staging_table()
        } else {
            self.layout().table.clone()
        }
    }

    /// Make sure the table data writes land in exists before the first write
    /// (#580, #676): checked once per sink; a missing one is created from the
    /// page when `create_table` is on, an error otherwise. A column-mapped
    /// page with nothing to infer from leaves it for the next page.
    pub async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        let layout = self.layout();
        if !layout.create_table {
            if !self.driver.table_exists(&layout.table).await? {
                return Err(faucet_core::missing_target_error(
                    &format!("{} sink", self.driver.connector_name()),
                    &self.driver.table_display(&layout.table),
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        let target = self.driver.target(&self.effective_table());
        let Some(sql) =
            self.engine()
                .create_table_sql(&target, records, layout.json_column.as_deref())?
        else {
            return Ok(());
        };
        self.driver.ddl(&sql, label::CREATE_TABLE).await?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn ensure_token_table(&self) -> Result<(), FaucetError> {
        if self.token_table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.driver
            .ddl(
                &self.engine().dialect().commit_token_table_sql(),
                label::CREATE_TOKEN_TABLE,
            )
            .await?;
        self.token_table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Apply keyed write groups in one transaction.
    async fn apply_plans(&self, plans: &[WritePlan]) -> Result<usize, FaucetError> {
        self.driver.prepare_keyed_write().await?;
        let mut conn = self.driver.begin().await?;
        let target = self.driver.target(&self.layout().table);
        let n = self.engine().apply_plans(&mut conn, &target, plans).await?;
        self.driver.commit(conn).await?;
        Ok(n)
    }

    /// Write a page and report every row: a keyed page accepts one row per
    /// planned upsert or delete and coalesces the rest; an append writes each
    /// `batch_size` chunk in its own transaction.
    async fn write_rows(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
        if records.is_empty() {
            return Ok(WriteReport::accepted(0));
        }
        self.ensure_table_ready(records).await?;
        if self.engine().is_keyed() {
            let plans = self.engine().plan_keyed(records)?;
            let written = self.apply_plans(&plans).await?;
            return Ok(self.engine().report(records.len(), written, &plans));
        }
        let chunk = match self.layout().batch_size {
            0 => records.len(),
            n => n,
        };
        let target = self.driver.target(&self.effective_table());
        let mut total = 0;
        for part in records.chunks(chunk) {
            let mut conn = self.driver.begin().await?;
            total += self
                .driver
                .insert(&mut conn, &target, part, InsertPath::Page)
                .await?;
            self.driver.commit(conn).await?;
        }
        tracing::info!(
            connector = self.driver.connector_name(),
            table = %self.layout().table,
            rows = total,
            "SQL write complete"
        );
        Ok(self.engine().report(records.len(), total, &[]))
    }

    /// The destination's live columns, for the dead-letter split.
    async fn live_columns(&self, table: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        let mut conn = self.driver.connection().await?;
        conn.columns(table).await
    }

    /// The dead-letter path: write the good rows and fail each bad one alone
    /// (a record matching no column, a keyed row without its key, a value
    /// the backend refuses).
    async fn write_rows_partial(&self, records: &[Value]) -> Result<PageWrite, FaucetError> {
        self.ensure_table_ready(records).await?;
        let table = self.driver.target(&self.effective_table());
        if !self.engine().is_keyed() {
            if self.layout().json_column.is_some() {
                self.write_rows(records).await?;
                return Ok(PageWrite::accepted(records.len()));
            }
            let columns = self.live_columns(&table).await?;
            let split = self.driver.split_page(&table, records, &columns, true);
            self.write_rows(&split.writable).await?;
            return Ok(PageWrite::from_outcomes(split.outcomes));
        }
        if self.driver.splits_keyed_pages() {
            let columns = self.live_columns(&table).await?;
            let split = self.driver.split_page(&table, records, &columns, false);
            let mut outcomes = split.outcomes;
            let (plans, failed) = self.engine().plan_keyed_partial(&split.writable);
            self.apply_plans(&plans).await?;
            for (idx, e) in failed {
                outcomes[split.positions[idx]] = Err(e);
            }
            return Ok(PageWrite::from_outcomes(outcomes));
        }
        let (plans, failed) = self.engine().plan_keyed_partial(records);
        self.apply_plans(&plans).await?;
        let ok = records.len() - failed.len();
        Ok(PageWrite::accepted(ok).with_failures(failed))
    }
}

#[faucet_core::async_trait]
impl<D: SqlDriver> SinkDriver for SqlSinkDriver<D> {
    fn connector_name(&self) -> &'static str {
        self.driver.connector_name()
    }

    fn config_schema(&self) -> Value {
        self.driver.config_schema()
    }

    fn dataset_uri(&self) -> String {
        self.driver.dataset_uri()
    }

    fn call_context(&self) -> CallContext {
        self.driver.call_context()
    }

    async fn check(
        &self,
        _calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        self.driver.check(ctx).await
    }

    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        match policy {
            RowPolicy::PerRow => {
                self.op(calls, "write_batch_partial", self.write_rows_partial(rows))
                    .await
            }
            _ => {
                self.op(calls, "write_batch", async {
                    self.write_rows(rows).await.map(PageWrite::reported)
                })
                .await
            }
        }
    }

    fn write_modes(&self) -> &'static [WriteMode] {
        self.driver.write_modes()
    }

    fn dedups_by_key(&self) -> bool {
        self.engine().write_spec().dedups_by_key()
    }

    fn batch_atomicity(&self) -> BatchAtomicity {
        self.driver.batch_atomicity()
    }

    fn capabilities(&self, derived: Capabilities) -> Capabilities {
        self.driver.capabilities(derived)
    }

    fn staging(&self) -> Option<&dyn StagingDriver> {
        self.driver
            .write_modes()
            .contains(&WriteMode::Overwrite)
            .then_some(self as &dyn StagingDriver)
    }

    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        Some(self)
    }

    fn cleanup(&self) -> Option<&dyn CleanupDriver> {
        self.layout()
            .json_column
            .is_none()
            .then_some(self as &dyn CleanupDriver)
    }

    fn schema(&self) -> Option<&dyn SchemaDriver> {
        Some(self)
    }

    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        self.driver.rollback()
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.driver.readback_source()
    }
}

#[faucet_core::async_trait]
impl<D: SqlDriver> StagingDriver for SqlSinkDriver<D> {
    fn active(&self) -> bool {
        self.engine().write_spec().is_overwrite()
    }

    /// Drop a stale staging table, then clone the destination into a fresh
    /// one. A missing destination with `create_table` on (a first run) has
    /// no shape to clone: the first write creates staging from the page.
    /// Every step reads the database, never this instance's memory, because
    /// the CLI runs begin, the writes and the commit on different instances.
    async fn stage(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.op(calls, "begin_overwrite", async {
            let layout = self.layout();
            self.driver.drop_staging(true).await?;
            if layout.create_table && !self.driver.table_exists(&layout.table).await? {
                return Ok(());
            }
            self.driver.create_staging().await
        })
        .await
    }

    /// Swap staging into place; on a first run (no destination) publish the
    /// staging table as the destination — and a run that wrote nothing
    /// leaves no table at all.
    async fn publish(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.op(calls, label::COMMIT_OVERWRITE, async {
            let layout = self.layout();
            if !self.driver.table_exists(&layout.table).await? {
                if self
                    .driver
                    .table_exists(&self.driver.staging_table())
                    .await?
                {
                    self.driver.publish_staging().await?;
                }
                return Ok(());
            }
            self.driver.swap_staging().await
        })
        .await
    }

    async fn discard(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.op(calls, "abort_overwrite", self.driver.drop_staging(false))
            .await
    }

    async fn staged(&self, calls: &CallSlot) -> Result<bool, FaucetError> {
        self.op(
            calls,
            "overwrite_staging_exists",
            self.driver.staging_exists(),
        )
        .await
    }
}

#[faucet_core::async_trait]
impl<D: SqlDriver> CommitTokenDriver for SqlSinkDriver<D> {
    async fn last_token(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError> {
        self.op(calls, "last_committed_token", async {
            self.ensure_token_table().await?;
            self.driver.read_token(scope).await
        })
        .await
    }

    /// The page and its token in one transaction: on a crash both land or
    /// neither does, which is what lets a replay skip a committed page. A
    /// keyed page is planned before the transaction opens, so a row without
    /// its key fails the page without leaving one open.
    async fn write_with_token(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError> {
        self.op(calls, "write_batch_idempotent", async {
            self.ensure_table_ready(rows).await?;
            self.ensure_token_table().await?;
            let plans = if self.engine().is_keyed() {
                Some(self.engine().plan_keyed(rows)?)
            } else {
                None
            };
            if plans.is_some() {
                self.driver.prepare_keyed_write().await?;
            }
            let mut conn = self.driver.begin().await?;
            let written = match &plans {
                Some(plans) => {
                    let target = self.driver.target(&self.layout().table);
                    self.engine().apply_plans(&mut conn, &target, plans).await?
                }
                None => {
                    let target = self.driver.target(&self.effective_table());
                    self.driver
                        .insert(&mut conn, &target, rows, InsertPath::Token)
                        .await?
                }
            };
            self.driver.write_token(&mut conn, scope, token).await?;
            self.driver.commit(conn).await?;
            Ok(self
                .engine()
                .report(rows.len(), written, plans.as_deref().unwrap_or(&[])))
        })
        .await
    }
}

#[faucet_core::async_trait]
impl<D: SqlDriver> CleanupDriver for SqlSinkDriver<D> {
    /// The written keys go into a key table and a `NOT EXISTS` delete removes
    /// the rest of the scope, in one transaction.
    async fn delete_absent(
        &self,
        calls: &CallSlot,
        scope: &BTreeMap<String, Value>,
        seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.op(calls, label::CLEANUP_SCOPE, async {
            self.engine().check_cleanup_key()?;
            // A journaling cleanup records before-images; journal DDL runs
            // before the transaction, as for a keyed write.
            if self.engine().write_spec().journals() {
                self.driver.prepare_keyed_write().await?;
            }
            let mut conn = self.driver.begin().await?;
            let target = self.driver.target(&self.layout().table);
            let deleted = self
                .engine()
                .cleanup(&mut conn, &target, scope, seen)
                .await?;
            self.driver.commit(conn).await?;
            Ok(deleted)
        })
        .await
    }
}

#[faucet_core::async_trait]
impl<D: SqlDriver> SchemaDriver for SqlSinkDriver<D> {
    async fn current_schema(&self, calls: &CallSlot) -> Result<Option<Value>, FaucetError> {
        self.op(calls, "current_schema", self.driver.current_schema())
            .await
    }

    fn evolvable(&self) -> bool {
        self.driver.evolution().is_some()
    }

    async fn evolve(
        &self,
        calls: &CallSlot,
        evolution: &SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.op(calls, "evolve_schema", async {
            let evolve = self.driver.evolution().ok_or_else(|| {
                FaucetError::Sink(format!(
                    "sink '{}' does not support schema evolution",
                    self.driver.connector_name()
                ))
            })?;
            let existing = evolve.existing_columns().await?;
            let steps = self.engine().evolution_steps(evolution, existing.as_ref());
            evolve.apply_evolution(steps).await
        })
        .await
    }
}

#[cfg(test)]
#[allow(deprecated)] // the tests drive the 1.x lifecycle hooks DriverSink implements
mod tests {
    use super::*;
    use crate::dialects::Sqlite;
    use crate::mapping::TableColumn;
    use crate::statement::Statement;
    use faucet_core::driver::DriverSink;
    use faucet_core::{Sink, WriteSpec};
    use serde_json::json;
    use std::sync::Mutex;

    /// A backend that records the SQL it is asked to run.
    struct Mem {
        engine: SqlSinkEngine<Sqlite>,
        layout: SqlLayout,
        exists: bool,
        split_keyed: bool,
        log: Mutex<Vec<String>>,
    }

    impl Mem {
        fn new(layout: SqlLayout, exists: bool) -> Self {
            Self {
                engine: SqlSinkEngine::new(Sqlite, WriteSpec::default()),
                layout,
                exists,
                split_keyed: false,
                log: Mutex::new(Vec::new()),
            }
        }
        fn keyed(mut self, split: bool) -> Self {
            self.engine = SqlSinkEngine::new(
                Sqlite,
                WriteSpec {
                    write_mode: WriteMode::Upsert,
                    key: vec!["id".into()],
                    ..WriteSpec::default()
                },
            );
            self.split_keyed = split;
            self
        }
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    struct Conn<'a>(&'a Mem);

    #[faucet_core::async_trait]
    impl SqlExecutor for Conn<'_> {
        async fn columns(&mut self, _: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
            Ok(vec![TableColumn::untyped("id")])
        }
        async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
            self.0.log.lock().unwrap().push(stmt.sql.clone());
            Ok(1)
        }
    }

    #[faucet_core::async_trait]
    impl SqlDriver for Mem {
        type Dialect = Sqlite;
        type Conn<'a> = Conn<'a>;
        fn engine(&self) -> &SqlSinkEngine<Sqlite> {
            &self.engine
        }
        fn layout(&self) -> &SqlLayout {
            &self.layout
        }
        fn connector_name(&self) -> &'static str {
            "mem"
        }
        fn config_schema(&self) -> Value {
            json!({"type": "object"})
        }
        fn dataset_uri(&self) -> String {
            "mem://t".into()
        }
        async fn begin(&self) -> Result<Conn<'_>, FaucetError> {
            Ok(Conn(self))
        }
        async fn commit(&self, _: Conn<'_>) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn connection(&self) -> Result<Conn<'_>, FaucetError> {
            Ok(Conn(self))
        }
        async fn table_exists(&self, _: &str) -> Result<bool, FaucetError> {
            Ok(self.exists)
        }
        async fn ddl(&self, sql: &str, _: &'static str) -> Result<(), FaucetError> {
            self.log.lock().unwrap().push(sql.to_string());
            Ok(())
        }
        async fn read_token(&self, _: &str) -> Result<Option<String>, FaucetError> {
            Ok(None)
        }
        async fn create_staging(&self) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn publish_staging(&self) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn swap_staging(&self) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
            Ok(None)
        }
        fn splits_keyed_pages(&self) -> bool {
            self.split_keyed
        }
    }

    #[tokio::test]
    async fn the_backend_hooks_default_to_the_generic_lifecycle() {
        let mem = Mem::new(SqlLayout::new("t"), true);
        assert_eq!(mem.staging_table(), "t__faucet_ovw");
        assert_eq!(mem.table_display("t"), "t");
        assert!(!mem.splits_keyed_pages());
        mem.prepare_keyed_write().await.unwrap();
        let guarded = mem.guard("write_batch", Box::pin(async { Ok(7) })).await;
        assert_eq!(guarded.unwrap(), 7);
        let sink = DriverSink::new(SqlSinkDriver::new(mem));
        assert!(sink.capabilities().overwrite());
        assert_eq!(sink.overwrite_staging_exists().await.unwrap(), Some(true));
        sink.abort_overwrite().await.unwrap();
        assert_eq!(
            sink.driver().driver().log(),
            ["DROP TABLE IF EXISTS `t__faucet_ovw`"]
        );
        assert_eq!(
            sink.write_batch_idempotent(&[json!({"id": 1})], "s", "00001")
                .await
                .unwrap(),
            1
        );
        assert!(
            sink.driver()
                .driver()
                .log()
                .iter()
                .any(|s| s.contains(faucet_core::idempotency::COMMIT_TOKEN_TABLE)),
            "the default token write runs on the page's transaction"
        );
    }

    #[tokio::test]
    async fn a_splitting_backend_maps_keyed_failures_back_to_their_rows() {
        for split in [true, false] {
            let sink = DriverSink::new(SqlSinkDriver::new(
                Mem::new(SqlLayout::new("t"), true).keyed(split),
            ));
            let outcomes = sink
                .write_batch_partial(&[json!({"id": 1}), json!({"x": 2}), json!({"id": 3})])
                .await
                .unwrap();
            assert!(
                outcomes[0].is_ok() && outcomes[1].is_err() && outcomes[2].is_ok(),
                "split = {split}: {outcomes:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_defaults_hold_and_a_missing_table_is_refused_without_create_table() {
        let sink = DriverSink::new(SqlSinkDriver::new(Mem::new(
            SqlLayout::new("t").create_table(false),
            false,
        )));
        assert_eq!(sink.batch_atomicity(), BatchAtomicity::Atomic);
        assert_eq!(sink.supported_write_modes().len(), 4);
        assert!(sink.readback_source().is_none() && !sink.supports_rollback());
        assert!(
            !sink.supports_schema_evolution(),
            "a driver without apply_evolution declares no evolution"
        );
        assert_eq!(sink.connector_name(), "mem");
        assert_eq!(sink.dataset_uri(), "mem://t");
        assert!(sink.config_schema().is_object());
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.probes[0].name, "check");
        let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
        let err = sink
            .evolve_schema(&faucet_core::SchemaEvolution::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("does not support schema evolution")
        );
        assert_eq!(
            sink.driver().driver().target("a`b").reference,
            "`a``b`",
            "the default target quotes with the dialect"
        );
    }

    #[tokio::test]
    async fn an_existing_table_is_checked_once_and_a_shapeless_page_creates_nothing() {
        let sink = DriverSink::new(SqlSinkDriver::new(Mem::new(
            SqlLayout::new("t").create_table(false),
            true,
        )));
        assert_eq!(sink.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
        assert_eq!(sink.write_batch(&[json!({"id": 2})]).await.unwrap(), 1);
        let created = DriverSink::new(SqlSinkDriver::new(Mem::new(SqlLayout::new("t"), false)));
        created
            .driver()
            .ensure_table_ready(&[json!({})])
            .await
            .unwrap();
        assert!(
            created.driver().driver().log().is_empty(),
            "nothing to infer from, so nothing is created yet"
        );
        created
            .driver()
            .ensure_table_ready(&[json!({"id": 1})])
            .await
            .unwrap();
        assert!(created.driver().driver().log()[0].starts_with("CREATE TABLE"));
    }

    #[tokio::test]
    #[allow(deprecated)] // the exactly-once hook the engine still drives (#888)
    async fn an_overwrite_page_with_a_token_is_inserted_not_planned_by_key() {
        let mut mem = Mem::new(SqlLayout::new("t").create_table(false), true);
        mem.engine = SqlSinkEngine::new(
            Sqlite,
            WriteSpec {
                write_mode: WriteMode::Overwrite,
                ..WriteSpec::default()
            },
        );
        let sink = DriverSink::new(SqlSinkDriver::new(mem));
        let n = sink
            .write_batch_idempotent(&[json!({"id": 1}), json!({"id": 2})], "scope", "0000001")
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert!(
            sink.driver()
                .driver()
                .log()
                .iter()
                .any(|q| q.starts_with("INSERT")),
            "{:?}",
            sink.driver().driver().log()
        );
    }

    #[tokio::test]
    #[allow(deprecated)] // the exactly-once hook the engine still drives (#888)
    async fn an_exactly_once_keyed_page_reports_planned_rows_not_affected_rows() {
        let sink = DriverSink::new(SqlSinkDriver::new(
            Mem::new(SqlLayout::new("t").create_table(false), true).keyed(false),
        ));
        let report = sink
            .write_batch_idempotent_reported(
                &[json!({"id": 1}), json!({"id": 1}), json!({"id": 2})],
                "scope",
                "0000001",
            )
            .await
            .unwrap();
        assert_eq!((report.accepted, report.coalesced), (2, 1));
    }

    #[tokio::test]
    #[allow(deprecated)] // the exactly-once hooks the engine still drives (#888)
    async fn the_token_table_is_created_once_per_sink() {
        let sink = DriverSink::new(SqlSinkDriver::new(Mem::new(
            SqlLayout::new("t").create_table(false),
            true,
        )));
        for token in ["0000001", "0000002"] {
            sink.last_committed_token("scope").await.unwrap();
            sink.write_batch_idempotent(&[json!({"id": 1})], "scope", token)
                .await
                .unwrap();
        }
        let ddl = sink.driver().driver().log();
        let creates = ddl
            .iter()
            .filter(|q| q.contains("_faucet_commit_token") && q.starts_with("CREATE"))
            .count();
        assert_eq!(creates, 1, "{ddl:?}");
    }
}
