//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_common_sql::dialects::{Sqlite, sqlite};
use faucet_common_sql::{
    EngineOptions, EvolutionStep, SqlDialect, SqlDriver, SqlExecutor, SqlLayout, SqlSinkDriver,
    SqlSinkEngine, Statement, StatementKind, TableColumn, TableTarget,
};
#[cfg(test)]
use faucet_core::SchemaEvolution;
use faucet_core::driver::{DriverSink, RollbackDriver};
use faucet_core::{FaucetError, SqlBaseType};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

/// Quote a SQLite identifier with backticks.
///
/// Deliberately NOT ANSI double quotes for the scoped-cleanup path: SQLite's
/// double-quoted-string misfeature silently reinterprets a double-quoted
/// identifier that does not resolve to a column as a **string literal**. In a
/// cleanup that is unacceptable — a typo'd scope column would make
/// `t."typo" = ?` a constant comparison instead of erroring, and the DELETE
/// would then match the wrong rows (or every row in the table). Backtick-quoted
/// identifiers are always identifiers, so an unknown column surfaces as a
/// proper "no such column" error. Embedded backticks are doubled, preventing
/// identifier injection. Mirrors `quote_ident_sqlite` in `faucet-source-sqlite`.
pub(crate) fn quote_ident_sqlite(name: &str) -> String {
    sqlite::quote_ident(name)
}

/// The quoted target of `table`, as the engine addresses it.
pub(crate) fn target(table: &str) -> TableTarget {
    TableTarget::new(table, quote_ident_sqlite(table))
}

#[cfg(test)]
use faucet_common_sql::dialects::sqlite::safe_type_spec;

#[cfg(test)]
fn build_cleanup_temp_table_sql(key_types: &[(String, String)]) -> String {
    let key: Vec<String> = key_types.iter().map(|(k, _)| k.clone()).collect();
    let types: Vec<Option<&str>> = key_types.iter().map(|(_, t)| Some(t.as_str())).collect();
    Sqlite
        .cleanup_create_sql(&target(""), &key, &types)
        .expect("sqlite cleanup DDL")
}

#[cfg(test)]
fn build_cleanup_insert_sql(key: &[String], rows: usize) -> String {
    Sqlite
        .cleanup_insert_sql(key, &[], rows)
        .expect("sqlite cleanup insert")
}

#[cfg(test)]
fn build_cleanup_delete_sql(table: &str, scope_cols: &[String], key: &[String]) -> String {
    Sqlite
        .cleanup_delete_sql(&target(table), scope_cols, &[], key)
        .expect("sqlite cleanup delete")
}

#[cfg(test)]
fn validate_cleanup_columns(
    existing: &std::collections::HashSet<String>,
    scope_cols: &[String],
    key: &[String],
    table: &str,
) -> Result<(), FaucetError> {
    faucet_common_sql::check_cleanup_columns(
        |c| existing.contains(c),
        scope_cols,
        key,
        &Sqlite.cleanup_table_display(&target(table)),
    )
}

/// Bind one JSON value to a SQLite query as its native type.
///
/// Shared by the delete-by-key and scoped-cleanup paths so the two never drift:
/// a key bound as a JSON string (`"7"` instead of `7`) would silently match
/// nothing and turn a delete into a no-op.
pub(crate) fn bind_value<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    v: &Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::Bool(b) => q.bind(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if n.is_u64() {
                // u64 above i64::MAX — preserve exact text rather than round through f64.
                q.bind(n.to_string())
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(n.to_string())
            }
        }
        Value::String(s) => q.bind(s.clone()),
        // Arrays/objects have no scalar SQL representation — bind their JSON
        // text (suitable for TEXT columns).
        other => q.bind(other.to_string()),
    }
}

/// A table's column names in declared order, read on `conn` (so it works inside
/// an open transaction on a single-connection pool).
pub(crate) async fn table_columns(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
) -> Result<Vec<String>, FaucetError> {
    let columns: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
            .bind(table)
            .fetch_all(conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?;
    if columns.is_empty() {
        return Err(FaucetError::Sink(format!(
            "table '{table}' has no columns or does not exist"
        )));
    }
    Ok(columns)
}

/// The SQLite affinity for a [`SqlBaseType`] (advisory: `Boolean` is
/// `INTEGER`, `Json` is `TEXT`).
#[cfg(test)]
fn sqlite_keyword(t: SqlBaseType) -> &'static str {
    Sqlite.type_name(t)
}

/// `ALTER TABLE <table> ADD COLUMN "<col>" <kw>` — SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so [`SqliteSink::evolve_schema`] only emits this
/// for columns it has already verified are absent (idempotency by pre-check).
/// `table` is the unquoted table name; it is quoted here via [`quote_ident_sqlite`].
fn build_add_column_sql(table: &str, col: &str, t: SqlBaseType) -> String {
    Sqlite
        .add_column_sql(&target(table), col, t)
        .expect("sqlite ADD COLUMN")
}

/// Rewrite the target's `CREATE TABLE` text (from `sqlite_master.sql`) so it
/// creates `staging` instead: the overwrite staging table then carries the
/// target's defaults, generated columns and constraints, which a
/// `CREATE TABLE … AS SELECT` clone drops (#789 SQL-43). `None` when the text
/// is not a recognisable `CREATE TABLE`.
fn staging_definition(create_sql: &str, staging: &str, table: &str) -> Result<String, FaucetError> {
    staging_ddl(create_sql, staging).ok_or_else(|| {
        FaucetError::Sink(format!(
            "sqlite overwrite: cannot derive a staging table from the definition of '{table}'"
        ))
    })
}

fn staging_ddl(create_sql: &str, staging: &str) -> Option<String> {
    let lower = create_sql.to_ascii_lowercase();
    let mut pos = lower.find("create")?;
    pos += "create".len();
    let rest = &lower[pos..];
    let skip = rest.len() - rest.trim_start().len();
    pos += skip;
    if lower[pos..].starts_with("temp") {
        return None;
    }
    if !lower[pos..].starts_with("table") {
        return None;
    }
    pos += "table".len();
    let rest = &lower[pos..];
    pos += rest.len() - rest.trim_start().len();
    if lower[pos..].starts_with("if not exists") {
        pos += "if not exists".len();
        let rest = &lower[pos..];
        pos += rest.len() - rest.trim_start().len();
    }
    let bytes = create_sql.as_bytes();
    let end = match *bytes.get(pos)? {
        open @ (b'"' | b'`' | b'[') => {
            let close = if open == b'[' { b']' } else { open };
            let mut i = pos + 1;
            loop {
                match bytes.get(i)? {
                    c if *c == close && close != b']' && bytes.get(i + 1) == Some(&close) => i += 2,
                    c if *c == close => break i + 1,
                    _ => i += 1,
                }
            }
        }
        _ => pos + create_sql[pos..].find(|c: char| c.is_whitespace() || c == '(')?,
    };
    Some(format!(
        "{}{}{}",
        &create_sql[..pos],
        quote_ident_sqlite(staging),
        &create_sql[end..]
    ))
}

/// A table's non-generated (insertable) columns in order, read on the
/// caller's connection so it works inside an open transaction.
pub(crate) async fn insertable_columns(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
) -> Result<Vec<String>, FaucetError> {
    sqlx::query_scalar("SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0 ORDER BY cid")
        .bind(table)
        .fetch_all(conn)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite: read columns of '{table}': {e}")))
}

/// Quote and comma-join column names.
pub(crate) fn column_list(names: &[String]) -> String {
    names
        .iter()
        .map(|n| quote_ident_sqlite(n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Map a SQLite column affinity string (`PRAGMA table_info.type`, e.g. `INTEGER`,
/// `REAL`, `VARCHAR(255)`, `TEXT`) to a JSON-Schema type fragment so
/// [`SqliteSink::current_schema`] round-trips with [`faucet_core::diff_schema`].
///
/// SQLite determines affinity by a tolerant, case-insensitive substring match on
/// the declared type (the rules in <https://www.sqlite.org/datatype3.html>), so
/// this mirrors that: contains `INT` → integer; `CHAR`/`CLOB`/`TEXT` → string;
/// `REAL`/`FLOA`/`DOUB` (and the loose `NUMERIC`/`DECIMAL`) → number; everything
/// else falls back to string. `nullable` reflects `PRAGMA table_info.notnull == 0`.
fn sqlite_affinity_to_json_schema(declared: &str, nullable: bool) -> serde_json::Value {
    let up = declared.to_ascii_uppercase();
    let contains = |needle: &str| up.contains(needle);
    let base = if contains("INT") {
        "integer"
    } else if contains("CHAR") || contains("CLOB") || contains("TEXT") {
        "string"
    } else if contains("REAL")
        || contains("FLOA")
        || contains("DOUB")
        || contains("NUMERIC")
        || contains("DECIMAL")
    {
        "number"
    } else {
        "string"
    };
    let mut fragment = if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    };
    // The sink stores booleans as INTEGER and objects/arrays as JSON TEXT, so
    // its own auto-created columns hold them faithfully (#789 SQL-83).
    let also: &[&str] = match base {
        "integer" => &["boolean"],
        "string" => &["object", "array"],
        _ => &[],
    };
    if !also.is_empty() {
        fragment[faucet_core::DRIFT_ALSO_ACCEPTS] = serde_json::json!(also);
    }
    fragment
}

/// Build the `ON CONFLICT(key) DO UPDATE …` tail for an upsert INSERT.
/// Non-key columns are SET from `excluded`. If every column is a key column,
/// emit `DO NOTHING`.
pub(crate) fn on_conflict_clause(key: &[String], all_cols: &[String]) -> String {
    Sqlite
        .upsert_tail(key, all_cols)
        .expect("sqlite upserts with ON CONFLICT")
}

/// How every write transaction starts. A deferred `BEGIN` takes a read lock and
/// upgrades on its first write; in WAL mode that upgrade fails at once with
/// `SQLITE_BUSY` when another connection committed in between, and the busy
/// timeout is not consulted — so two streams writing one database file failed
/// at random. `IMMEDIATE` takes the write lock up front, where the busy timeout
/// does apply.
pub(crate) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";

/// A sink that writes JSON records to a SQLite table.
///
/// Built on the SQL family driver (RFC 0014): `SqliteDriver` holds the
/// SQLite I/O, and [`SqlSinkDriver`] drives writes, per-row outcomes, the
/// exactly-once commit, cleanup, the overwrite lifecycle and schema evolution
/// from it.
pub struct SqliteSink {
    /// The connection pool (shared with the driver).
    #[cfg_attr(not(test), allow(dead_code))] // read by the unit tests
    pub(crate) pool: SqlitePool,
    inner: DriverSink<SqlSinkDriver<SqliteDriver>>,
}

/// The SQLite I/O primitives the SQL family driver runs on.
pub(crate) struct SqliteDriver {
    pub(crate) config: SqliteSinkConfig,
    pub(crate) pool: SqlitePool,
    /// The shared SQL planning engine (#884).
    pub(crate) engine: SqlSinkEngine<Sqlite>,
    layout: SqlLayout,
}

/// The engine's view of one SQLite connection (or open transaction): every
/// statement of an engine call runs on it, inside the sink's backend call.
pub(crate) struct SqliteExec<'c> {
    pub(crate) conn: &'c mut sqlx::SqliteConnection,
    pub(crate) sink: &'c SqliteDriver,
}

#[async_trait]
impl SqlExecutor for SqliteExec<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        Ok(table_columns(self.conn, &target.name)
            .await?
            .into_iter()
            .map(TableColumn::untyped)
            .collect())
    }

    /// Declared types from `PRAGMA table_info`, so the cleanup key table
    /// compares with the destination's type affinities. Empty when the table
    /// is missing.
    async fn cleanup_columns(
        &mut self,
        target: &TableTarget,
    ) -> Result<Vec<TableColumn>, FaucetError> {
        Ok(
            sqlx::query(&format!("PRAGMA table_info({})", target.reference))
                .fetch_all(&mut *self.conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: table_info query failed: {e}")))?
                .iter()
                .map(|row| {
                    TableColumn::new(row.get::<String, _>("name"), row.get::<String, _>("type"))
                })
                .collect(),
        )
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        let mut q = sqlx::query(&stmt.sql);
        for p in &stmt.params {
            // Native SQLite types so affinity and typed reads round-trip (#78/#4).
            q = bind_value(q, &p.value);
        }
        let context = match stmt.kind {
            StatementKind::Insert | StatementKind::InsertJson | StatementKind::Upsert => {
                "SQLite insert failed"
            }
            StatementKind::Delete => "SQLite delete failed",
            StatementKind::CleanupDrop => "cleanup: temp table drop failed",
            StatementKind::CleanupCreate => "cleanup: temp table creation failed",
            StatementKind::CleanupLoad => "cleanup: loading keys failed",
            StatementKind::CleanupDelete => "cleanup: delete failed",
            StatementKind::TokenTable => "SQLite commit-table create failed",
            _ => "SQLite token upsert failed",
        };
        q.execute(&mut *self.conn)
            .await
            .map(|r| r.rows_affected())
            .map_err(|e| FaucetError::Sink(format!("{context}: {e}")))
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.sink.journal_plan(self.conn, plan, run_id).await
    }
}

/// A connection the SQL family driver runs statements on: an open write
/// transaction, or a pooled connection for catalog reads.
pub(crate) enum SqliteConn<'c> {
    /// A `BEGIN IMMEDIATE` transaction.
    Tx(sqlx::Transaction<'c, sqlx::Sqlite>, &'c SqliteDriver),
    /// A plain pooled connection.
    Pooled(sqlx::pool::PoolConnection<sqlx::Sqlite>, &'c SqliteDriver),
}

impl SqliteConn<'_> {
    fn exec(&mut self) -> SqliteExec<'_> {
        match self {
            SqliteConn::Tx(tx, sink) => SqliteExec { conn: tx, sink },
            SqliteConn::Pooled(conn, sink) => SqliteExec { conn, sink },
        }
    }
}

#[async_trait]
impl SqlExecutor for SqliteConn<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        self.exec().columns(target).await
    }

    async fn cleanup_columns(
        &mut self,
        target: &TableTarget,
    ) -> Result<Vec<TableColumn>, FaucetError> {
        self.exec().cleanup_columns(target).await
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        self.exec().execute(stmt).await
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.exec().journal_plan(plan, run_id).await
    }
}

/// Create the directory a database file lives in, so `./out/db.sqlite` works
/// before `./out` exists — as every other file-writing sink does (#822).
/// `create_if_missing` creates only the file. In-memory databases (and a bare
/// file name in the working directory) have no directory to create.
/// A `sqlx` error as a [`FaucetError::Sink`] prefixed with `context`.
fn sink_err(context: &'static str) -> impl FnOnce(sqlx::Error) -> FaucetError {
    move |e| FaucetError::Sink(format!("{context}: {e}"))
}

fn ensure_database_dir(filename: &std::path::Path) -> Result<(), FaucetError> {
    if filename.as_os_str() == ":memory:" {
        return Ok(());
    }
    match filename.parent() {
        Some(dir) if !dir.as_os_str().is_empty() && !dir.exists() => std::fs::create_dir_all(dir)
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "sqlite sink: cannot create the database directory {}: {e}",
                    dir.display()
                ))
            }),
        _ => Ok(()),
    }
}

impl SqliteSink {
    /// Create a new SQLite sink. Establishes a connection pool.
    ///
    /// The pool opens each connection with `journal_mode = WAL` and the
    /// configured `busy_timeout_secs` (default 60). WAL lets a writer and readers proceed concurrently
    /// instead of locking each other out, and the busy timeout makes a
    /// connection wait-and-retry for the write lock rather than failing
    /// immediately with `SQLITE_BUSY` under contention. `create_if_missing`
    /// preserves the previous behaviour of creating the database file on first
    /// open. WAL on a `sqlite::memory:` database is a harmless no-op.
    pub async fn new(config: SqliteSinkConfig) -> Result<Self, FaucetError> {
        config.write.validate()?;
        if matches!(
            config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) && !matches!(config.column_mapping, SqliteColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "sqlite sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSON blob)"
                    .into(),
            ));
        }

        let options = SqliteConnectOptions::from_str(&config.database_url)
            .map_err(|e| FaucetError::Sink(format!("invalid SQLite database_url: {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(config.busy_timeout_secs));
        ensure_database_dir(options.get_filename())?;

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        let engine = SqlSinkEngine::new(Sqlite, config.write.clone())
            .with_options(EngineOptions::new().warn_unmatched_fields(true));
        let mut layout = SqlLayout::new(config.table_name.clone())
            .create_table(config.create_table)
            .batch_size(config.batch_size);
        if let SqliteColumnMapping::Json { column } = &config.column_mapping {
            layout = layout.json_column(column.clone());
        }
        let driver = SqliteDriver {
            config,
            pool: pool.clone(),
            engine,
            layout,
        };
        Ok(Self {
            pool,
            inner: DriverSink::new(SqlSinkDriver::new(driver)),
        })
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    faucet_core::forward_sink!(inner);
}

impl SqliteDriver {
    pub(crate) async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        let exists: Option<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite table probe failed: {e}")))?;
        Ok(exists.is_some())
    }

    /// Tables (other than the target and its own staging/previous copies) whose
    /// foreign keys reference the target.
    async fn referencing_tables(&self) -> Result<Vec<String>, FaucetError> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT m.name FROM sqlite_master m, pragma_foreign_key_list(m.name) f \
             WHERE m.type = 'table' AND f.\"table\" = ? COLLATE NOCASE AND m.name <> ? \
             ORDER BY m.name",
        )
        .bind(&self.config.table_name)
        .bind(&self.config.table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: read foreign keys: {e}")))?;
        let staging = self.staging_table();
        Ok(rows.into_iter().filter(|t| *t != staging).collect())
    }

    /// The staging table used while an overwrite run is in flight.
    fn staging_table(&self) -> String {
        self.layout.staging_table()
    }

    /// Delete rows whose key columns match any of `deletes`
    /// (`DELETE … WHERE (k1, …) IN ((?, …), …)`, chunked at the bind cap), on
    /// the caller's transaction.
    pub(crate) async fn delete_by_keys(
        &self,
        conn: &mut sqlx::SqliteConnection,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        let mut exec = SqliteExec { conn, sink: self };
        self.engine
            .delete(&mut exec, &target(&self.config.table_name), deletes)
            .await
    }

    /// Ensure the commit-token watermark table exists.
    pub(crate) async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        sqlx::query(&Sqlite.commit_token_table_sql())
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite commit-table create failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl SqlDriver for SqliteDriver {
    type Dialect = Sqlite;
    type Conn<'a> = SqliteConn<'a>;

    fn engine(&self) -> &SqlSinkEngine<Sqlite> {
        &self.engine
    }

    fn layout(&self) -> &SqlLayout {
        &self.layout
    }

    fn connector_name(&self) -> &'static str {
        "sqlite"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let path = self
            .config
            .database_url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        format!("sqlite://{}?table={}", path, self.config.table_name)
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database file /
    /// connection opens without writing anything.
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
                    "check database_url / that the database file is reachable and openable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check database_url / that the database file is reachable and openable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn target(&self, table: &str) -> TableTarget {
        target(table)
    }

    /// Open a write transaction (`BEGIN IMMEDIATE`, see [`BEGIN_WRITE`]).
    async fn begin(&self) -> Result<SqliteConn<'_>, FaucetError> {
        let tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;
        Ok(SqliteConn::Tx(tx, self))
    }

    async fn commit(&self, conn: SqliteConn<'_>) -> Result<(), FaucetError> {
        match conn {
            SqliteConn::Tx(tx, _) => tx
                .commit()
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}"))),
            SqliteConn::Pooled(..) => Ok(()),
        }
    }

    async fn connection(&self) -> Result<SqliteConn<'_>, FaucetError> {
        let conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection acquire failed: {e}")))?;
        Ok(SqliteConn::Pooled(conn, self))
    }

    async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        SqliteDriver::table_exists(self, table).await
    }

    async fn ddl(&self, sql: &str, what: &'static str) -> Result<(), FaucetError> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite {what} failed: {e}")))?;
        Ok(())
    }

    async fn read_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let row = sqlx::query(&Sqlite.commit_token_select_sql())
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    /// Create the staging table from the target's own definition (so defaults,
    /// generated columns and constraints carry over). A target that another
    /// table references by foreign key is refused: the swap deletes and
    /// re-inserts its rows, which would cascade into or break that table.
    async fn create_staging(&self) -> Result<(), FaucetError> {
        let referencing = self.referencing_tables().await?;
        if !referencing.is_empty() {
            return Err(FaucetError::Config(format!(
                "sqlite overwrite: table '{}' is referenced by a foreign key from {}; replacing \
                 its rows would cascade into or break those tables. Use write_mode: upsert, or \
                 drop the foreign key",
                self.config.table_name,
                referencing.join(", ")
            )));
        }
        let create_sql: Option<String> =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(&self.config.table_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(sink_err("sqlite overwrite: read target definition"))?;
        let create_sql = create_sql.ok_or_else(|| {
            FaucetError::Sink(format!(
                "sqlite overwrite: create staging from '{}': the table does not exist",
                self.config.table_name
            ))
        })?;
        let ddl = staging_definition(&create_sql, &self.staging_table(), &self.config.table_name)?;
        sqlx::query(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: create staging: {e}")))?;
        Ok(())
    }

    /// First run: staging holds everything; rename it into place. With
    /// `rollback.keep_previous`, an empty previous copy makes the first run
    /// undoable: its rollback empties the table it created (#789 SQL-151).
    async fn publish_staging(&self) -> Result<(), FaucetError> {
        let staging = quote_ident_sqlite(&self.staging_table());
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(sink_err("sqlite overwrite: begin publish"))?;
        if self.config.write.keeps_previous() {
            let prev = quote_ident_sqlite(&self.previous_table());
            for stmt in [
                format!("DROP TABLE IF EXISTS {prev}"),
                format!("CREATE TABLE {prev} AS SELECT * FROM {staging} WHERE 0"),
            ] {
                sqlx::query(&stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(sink_err("sqlite overwrite: keep previous copy"))?;
            }
        }
        sqlx::query(&format!(
            "ALTER TABLE {staging} RENAME TO {}",
            quote_ident_sqlite(&self.config.table_name)
        ))
        .execute(&mut *tx)
        .await
        .map_err(sink_err("sqlite overwrite: publish staging"))?;
        tx.commit()
            .await
            .map_err(sink_err("sqlite overwrite: commit publish"))
    }

    /// Atomically replace the destination with the staged rows in one
    /// transaction: `DELETE FROM target; INSERT INTO target (cols) SELECT cols
    /// FROM staging; DROP TABLE staging`, over the target's non-generated
    /// columns. SQLite DDL is transactional, so a failure anywhere rolls the
    /// whole swap back and the prior rows survive.
    async fn swap_staging(&self) -> Result<(), FaucetError> {
        let staging = quote_ident_sqlite(&self.staging_table());
        let target = quote_ident_sqlite(&self.config.table_name);
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(sink_err("sqlite overwrite: begin swap"))?;
        // `rollback.keep_previous`: snapshot the rows about to be replaced, in
        // the same transaction, so a rollback can swap them back (#706).
        if self.config.write.keeps_previous() {
            self.keep_previous_copy(&mut tx).await?;
        }
        let cols = column_list(&insertable_columns(&mut tx, &self.config.table_name).await?);
        for stmt in [
            format!("DELETE FROM {target}"),
            format!("INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}"),
            format!("DROP TABLE {staging}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut *tx)
                .await
                .map_err(sink_err("sqlite overwrite swap failed"))?;
        }
        tx.commit()
            .await
            .map_err(sink_err("sqlite overwrite: commit swap"))
    }

    /// Read the live destination schema via `PRAGMA table_info`, shaped as an
    /// `infer_schema`-compatible object (`{"type":"object","properties":{…}}`),
    /// or `None` when the target table does not exist yet (issue #194).
    ///
    /// `PRAGMA table_info` returns one row per column with `name`, `type` (the
    /// declared affinity string), and `notnull`. The affinity string is mapped
    /// to a JSON-Schema base type via `sqlite_affinity_to_json_schema`, and
    /// `notnull == 0` surfaces the column as nullable. The PRAGMA runs on a
    /// connection acquired from the pool (a standalone read — not inside an open
    /// transaction).
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
        // JSON-column mode stores each record whole; the physical columns are
        // not the record's fields, so there is nothing to drift against
        // (#789 SQL-20).
        if !matches!(self.config.column_mapping, SqliteColumnMapping::AutoMap) {
            return Ok(None);
        }
        let rows = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident_sqlite(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(sink_err("sqlite current_schema query failed"))?;

        if rows.is_empty() {
            return Ok(None); // table does not exist yet (or has no columns)
        }

        let mut props = serde_json::Map::new();
        for row in &rows {
            let name: String = row.get("name");
            let declared: String = row.get("type");
            let notnull: i64 = row.get("notnull");
            props.insert(
                name,
                sqlite_affinity_to_json_schema(&declared, notnull == 0),
            );
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    /// SQLite has no `ADD COLUMN IF NOT EXISTS`, so the current columns are
    /// read first and an addition already present is skipped.
    async fn existing_columns(
        &self,
    ) -> Result<Option<std::collections::HashSet<String>>, FaucetError> {
        Ok(Some(
            sqlx::query(&format!(
                "PRAGMA table_info({})",
                quote_ident_sqlite(&self.config.table_name)
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite evolve table_info failed: {e}")))?
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect(),
        ))
    }

    /// Apply an additive schema evolution to the destination table (issue #194).
    ///
    /// - **Additions** — `ALTER TABLE … ADD COLUMN`.
    /// - **Widenings** — a no-op under SQLite's dynamic typing: a column already
    ///   accepts a value of any type, so there is nothing to ALTER. Logged once
    ///   at `debug`.
    /// - **Nullability relaxations** — a no-op: SQLite cannot drop a `NOT NULL`
    ///   constraint in place (it requires a full table rebuild, which is out of
    ///   scope here). Logged at `debug`; the column is left as-is.
    fn evolvable(&self) -> bool {
        true
    }

    async fn apply_evolution(&self, steps: Vec<EvolutionStep>) -> Result<(), FaucetError> {
        let mut widened = false;
        for step in steps {
            match step {
                EvolutionStep::Add { column, base } => {
                    sqlx::query(&build_add_column_sql(
                        &self.config.table_name,
                        &column,
                        base,
                    ))
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("sqlite ADD COLUMN {column} failed: {e}"))
                    })?;
                }
                EvolutionStep::Widen { .. } if !widened => {
                    widened = true;
                    tracing::debug!("sqlite: type widening is a no-op under dynamic typing");
                }
                EvolutionStep::Relax { column } => {
                    tracing::debug!(
                        "sqlite cannot relax NOT NULL in place; column {column} left as-is"
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Column-mapping mode only: the run-id column, the journaled keys and the
    /// kept previous table all address real columns (#706).
    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        Some(self)
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.readback_source_impl()
    }
}

#[async_trait]
impl RollbackDriver for SqliteDriver {
    fn enabled(&self) -> bool {
        self.rollback_supported()
    }

    async fn rollback_run(
        &self,
        calls: &faucet_core::CallSlot,
        run_id: &str,
        opts: &faucet_core::rollback::RollbackOptions,
    ) -> Result<faucet_core::rollback::RollbackOutcome, FaucetError> {
        calls
            .call_once("rollback_run", self.rollback_run_impl(run_id, opts))
            .await
    }

    async fn forget_run(
        &self,
        calls: &faucet_core::CallSlot,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        calls
            .call_once("forget_run", self.forget_run_impl(run_id))
            .await
    }

    async fn rewind_commit_token(
        &self,
        calls: &faucet_core::CallSlot,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        calls
            .call_once(
                "rewind_commit_token",
                self.rewind_commit_token_impl(scope, token),
            )
            .await
    }
}

#[allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_dir_is_left_alone_for_memory_bare_and_existing_paths() {
        use std::path::Path;
        ensure_database_dir(Path::new(":memory:")).unwrap();
        ensure_database_dir(Path::new("db.sqlite")).unwrap();
        ensure_database_dir(Path::new("file:sqlx-in-memory-1")).unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        ensure_database_dir(&dir.path().join("db.sqlite")).unwrap();
        let nested = dir.path().join("a").join("b").join("db.sqlite");
        ensure_database_dir(&nested).unwrap();
        assert!(nested.parent().unwrap().is_dir());
    }

    #[test]
    fn staging_ddl_renames_only_the_table_of_any_quoting_style() {
        let cases = [
            (
                "CREATE TABLE users (id INTEGER, note TEXT DEFAULT 'users')",
                "CREATE TABLE `users__faucet_ovw` (id INTEGER, note TEXT DEFAULT 'users')",
            ),
            (
                "CREATE TABLE \"my \"\"t\"\"\"(a)",
                "CREATE TABLE `users__faucet_ovw`(a)",
            ),
            (
                "create table `t` (a)",
                "create table `users__faucet_ovw` (a)",
            ),
            ("CREATE TABLE [t](a)", "CREATE TABLE `users__faucet_ovw`(a)"),
            (
                "CREATE TABLE IF NOT EXISTS t (a)",
                "CREATE TABLE IF NOT EXISTS `users__faucet_ovw` (a)",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                staging_ddl(input, "users__faucet_ovw").as_deref(),
                Some(expected),
                "{input}"
            );
        }
        assert_eq!(staging_ddl("CREATE TEMP TABLE t (a)", "s"), None);
        assert_eq!(staging_ddl("CREATE VIEW v AS SELECT 1", "s"), None);
        assert_eq!(staging_ddl("SELECT 1", "s"), None);
        assert_eq!(staging_ddl("CREATE TABLE \"unterminated", "s"), None);
        assert_eq!(staging_ddl("CREATE TABLE", "s"), None);
        assert!(
            staging_definition("CREATE VIEW v AS SELECT 1", "s", "v")
                .unwrap_err()
                .to_string()
                .contains("'v'")
        );
    }
    use crate::config::SqliteSinkConfig;
    use faucet_core::Sink as _;

    #[tokio::test]
    async fn dataset_uri_strips_sqlite_prefix_and_includes_table() {
        let config = SqliteSinkConfig::new("sqlite:///tmp/test.db", "events");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite:///tmp/test.db?table=events");
    }

    #[tokio::test]
    async fn dataset_uri_with_memory_db() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "logs");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite://:memory:?table=logs");
    }

    #[test]
    fn sqlite_on_conflict_clause() {
        let clause =
            on_conflict_clause(&["id".to_string()], &["id".to_string(), "name".to_string()]);
        assert_eq!(
            clause,
            "ON CONFLICT(`id`) DO UPDATE SET `name` = excluded.`name`"
        );
    }

    #[test]
    fn sqlite_on_conflict_all_keys_does_nothing() {
        let clause = on_conflict_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, "ON CONFLICT(`id`) DO NOTHING");
    }

    #[test]
    fn sqlite_on_conflict_composite_key() {
        let clause = on_conflict_clause(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
        );
        assert_eq!(
            clause,
            "ON CONFLICT(`a`, `b`) DO UPDATE SET `v` = excluded.`v`"
        );
    }

    #[test]
    fn sqlite_add_column_ddl() {
        assert_eq!(
            build_add_column_sql("t", "email", SqlBaseType::Text),
            "ALTER TABLE `t` ADD COLUMN `email` TEXT"
        );
        assert_eq!(
            build_add_column_sql("t", "age", SqlBaseType::Integer),
            "ALTER TABLE `t` ADD COLUMN `age` INTEGER"
        );
        assert_eq!(
            build_add_column_sql("t", "score", SqlBaseType::Double),
            "ALTER TABLE `t` ADD COLUMN `score` REAL"
        );
        // Boolean has no native SQLite type → INTEGER affinity; JSON → TEXT.
        assert_eq!(
            build_add_column_sql("t", "ok", SqlBaseType::Boolean),
            "ALTER TABLE `t` ADD COLUMN `ok` INTEGER"
        );
        assert_eq!(
            build_add_column_sql("t", "meta", SqlBaseType::Json),
            "ALTER TABLE `t` ADD COLUMN `meta` TEXT"
        );
    }

    #[test]
    fn sqlite_keyword_mapping() {
        assert_eq!(sqlite_keyword(SqlBaseType::Integer), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Double), "REAL");
        assert_eq!(sqlite_keyword(SqlBaseType::Boolean), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Text), "TEXT");
        assert_eq!(sqlite_keyword(SqlBaseType::Json), "TEXT");
    }

    // ---------------------------------------------------------------------
    // Scoped cleanup (#478) — SQL generation and column validation
    // ---------------------------------------------------------------------

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn cleanup_quotes_identifiers_with_backticks() {
        // Not double quotes: SQLite's double-quoted-string misfeature would turn
        // a typo'd column into a string literal instead of an error.
        assert_eq!(quote_ident_sqlite("id"), "`id`");
        assert_eq!(quote_ident_sqlite("ev`il"), "`ev``il`");
    }

    #[test]
    fn cleanup_temp_table_mirrors_declared_types() {
        let sql = build_cleanup_temp_table_sql(&[
            ("id".to_string(), "INTEGER".to_string()),
            ("slug".to_string(), "VARCHAR(255)".to_string()),
        ]);
        assert_eq!(
            sql,
            "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id` INTEGER, `slug` VARCHAR(255))"
        );
    }

    #[test]
    fn cleanup_temp_table_omits_an_unusable_type() {
        // A typeless column is legal in SQLite; it only loses type affinity.
        let sql = build_cleanup_temp_table_sql(&[("id".to_string(), String::new())]);
        assert_eq!(sql, "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id`)");
        // A declared type that isn't type-spec-shaped is dropped rather than
        // pasted into DDL.
        let sql = build_cleanup_temp_table_sql(&[("id".to_string(), "INT); DROP".to_string())]);
        assert_eq!(sql, "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id`)");
    }

    #[test]
    fn safe_type_spec_accepts_real_types_and_rejects_the_rest() {
        assert_eq!(safe_type_spec("DOUBLE PRECISION"), Some("DOUBLE PRECISION"));
        assert_eq!(safe_type_spec("DECIMAL(10, 2)"), Some("DECIMAL(10, 2)"));
        assert_eq!(safe_type_spec("  TEXT  "), Some("TEXT"));
        assert_eq!(safe_type_spec(""), None);
        assert_eq!(safe_type_spec("   "), None);
        assert_eq!(safe_type_spec("TEXT`"), None);
        assert_eq!(safe_type_spec("TEXT'"), None);
    }

    #[test]
    fn cleanup_insert_emits_one_tuple_per_row() {
        let sql = build_cleanup_insert_sql(&cols(&["a", "b"]), 3);
        assert_eq!(
            sql,
            "INSERT INTO temp.`faucet_cleanup_keys` (`a`, `b`) VALUES (?, ?), (?, ?), (?, ?)"
        );
    }

    #[test]
    fn cleanup_delete_ands_the_scope_and_excludes_written_keys() {
        let sql = build_cleanup_delete_sql("assoc", &cols(&["contact_id"]), &cols(&["id"]));
        assert_eq!(
            sql,
            "DELETE FROM `assoc` WHERE `assoc`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM temp.`faucet_cleanup_keys` c \
             WHERE `assoc`.`id` = c.`id`)"
        );
    }

    #[test]
    fn cleanup_delete_composite_scope_and_key() {
        let sql =
            build_cleanup_delete_sql("t", &cols(&["tenant", "contact_id"]), &cols(&["a", "b"]));
        assert_eq!(
            sql,
            "DELETE FROM `t` WHERE `t`.`tenant` = ? AND `t`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM temp.`faucet_cleanup_keys` c \
             WHERE `t`.`a` = c.`a` AND `t`.`b` = c.`b`)"
        );
    }

    #[test]
    fn cleanup_validation_names_a_missing_scope_column() {
        let existing: std::collections::HashSet<String> =
            cols(&["id", "name"]).into_iter().collect();
        let err = validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
            .expect_err("unknown scope column must be refused");
        let msg = err.to_string();
        assert!(msg.contains("contact_id"), "{msg}");
        assert!(msg.contains("'t'"), "{msg}");
    }

    #[test]
    fn cleanup_validation_names_a_missing_key_column() {
        let existing: std::collections::HashSet<String> =
            cols(&["contact_id"]).into_iter().collect();
        let err = validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
            .expect_err("unknown key column must be refused");
        assert!(err.to_string().contains("id"), "{err}");
    }

    #[test]
    fn cleanup_validation_passes_when_every_column_exists() {
        let existing: std::collections::HashSet<String> =
            cols(&["id", "contact_id"]).into_iter().collect();
        assert!(
            validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn supports_cleanup_only_in_auto_map_mode() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "t")
            .column_mapping(SqliteColumnMapping::AutoMap);
        let sink = SqliteSink::new(config).await.unwrap();
        assert!(sink.supports_cleanup());

        // The default mapping is a single JSON payload column — no real columns
        // for the scope/key predicates to address.
        let config = SqliteSinkConfig::new("sqlite::memory:", "t");
        let sink = SqliteSink::new(config).await.unwrap();
        assert!(!sink.supports_cleanup());
    }

    #[test]
    fn sqlite_affinity_round_trips_to_json_schema() {
        use serde_json::json;
        // Tolerant case-insensitive substring matching, SQLite affinity rules.
        assert_eq!(
            sqlite_affinity_to_json_schema("INTEGER", false),
            json!({"type":"integer", "x-faucet-also-accepts":["boolean"]})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("BIGINT", false),
            json!({"type":"integer", "x-faucet-also-accepts":["boolean"]})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("REAL", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DOUBLE PRECISION", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DECIMAL(10,2)", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("TEXT", false),
            json!({"type":"string", "x-faucet-also-accepts":["object","array"]})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("VARCHAR(255)", false),
            json!({"type":"string", "x-faucet-also-accepts":["object","array"]})
        );
        // Unknown / empty affinity falls back to string.
        assert_eq!(
            sqlite_affinity_to_json_schema("BLOB", false),
            json!({"type":"string", "x-faucet-also-accepts":["object","array"]})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("", false),
            json!({"type":"string", "x-faucet-also-accepts":["object","array"]})
        );
        // Nullable columns widen the type array.
        assert_eq!(
            sqlite_affinity_to_json_schema("integer", true),
            json!({"type":["integer","null"], "x-faucet-also-accepts":["boolean"]})
        );
    }

    fn auto_map_cfg(url: &str, mode: faucet_core::WriteMode, key: &[&str]) -> SqliteSinkConfig {
        let mut config =
            SqliteSinkConfig::new(url, "t").column_mapping(SqliteColumnMapping::AutoMap);
        config.write.write_mode = mode;
        config.write.key = key.iter().map(|k| k.to_string()).collect();
        config
    }

    #[tokio::test]
    async fn sqlx_errors_name_the_failed_step() {
        let sink = SqliteSink::new(auto_map_cfg(
            "sqlite::memory:",
            faucet_core::WriteMode::Append,
            &[],
        ))
        .await
        .unwrap();
        sink.pool.close().await;
        let err = sink.current_schema().await.unwrap_err().to_string();
        assert!(
            err.contains("sqlite current_schema query failed: "),
            "{err}"
        );
    }

    /// Every SQLite primitive names its step when the pool is gone.
    #[tokio::test]
    #[allow(deprecated)] // probes the commit-token read the driver serves
    async fn a_closed_pool_fails_each_primitive_with_its_step() {
        let sink = SqliteSink::new(auto_map_cfg(
            "sqlite::memory:",
            faucet_core::WriteMode::Append,
            &[],
        ))
        .await
        .unwrap();
        sink.pool.close().await;
        let msg = |r: Result<(), FaucetError>| r.unwrap_err().to_string();
        assert!(
            msg(sink
                .write_batch(&[serde_json::json!({"id": 1})])
                .await
                .map(|_| ()))
            .contains("SQLite")
        );
        assert!(msg(sink.last_committed_token("s").await.map(|_| ())).contains("SQLite"));
        assert!(
            msg(sink
                .write_batch_partial(&[serde_json::json!({"id": 1})])
                .await
                .map(|_| ()))
            .contains("SQLite")
        );
        let evolution = SchemaEvolution::default();
        assert!(msg(sink.evolve_schema(&evolution).await).contains("sqlite"));
        let driver = sink.inner.driver().driver();
        assert!(msg(SqlDriver::begin(driver).await.map(|_| ())).contains("transaction begin"));
        assert!(
            msg(SqlDriver::connection(driver).await.map(|_| ())).contains("connection acquire")
        );
        assert!(
            msg(SqlDriver::ddl(driver, "SELECT 1", "probe").await).contains("SQLite probe failed")
        );
        assert!(msg(SqlDriver::read_token(driver, "s").await.map(|_| ())).contains("token read"));
    }

    #[tokio::test]
    async fn adding_a_column_to_a_view_reports_the_column() {
        let dir = tempfile::TempDir::new().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("v.db").display());
        let sink = SqliteSink::new(auto_map_cfg(&url, faucet_core::WriteMode::Append, &[]))
            .await
            .unwrap();
        sqlx::query("CREATE VIEW t AS SELECT 1 AS id")
            .execute(&sink.pool)
            .await
            .unwrap();
        let evolution = SchemaEvolution {
            additions: vec![faucet_core::ColumnChange {
                name: "extra".into(),
                from: None,
                to: serde_json::json!({"type": "string"}),
            }],
            widenings: vec![],
            relax_nullability: vec![],
        };
        let err = sink
            .evolve_schema(&evolution)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("sqlite ADD COLUMN extra failed"), "{err}");
    }

    #[tokio::test]
    async fn idempotent_upsert_refuses_a_row_without_its_key_before_writing() {
        let dir = tempfile::TempDir::new().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("k.db").display());
        let sink = SqliteSink::new(auto_map_cfg(&url, faucet_core::WriteMode::Upsert, &["id"]))
            .await
            .unwrap();
        let err = sink
            .write_batch_idempotent(&[serde_json::json!({"name": "x"})], "scope", "0001")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("sqlite upsert: row 0"), "{err}");
        assert_eq!(sink.last_committed_token("scope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn idempotent_append_in_auto_map_mode_commits_rows_and_token() {
        let dir = tempfile::TempDir::new().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("a.db").display());
        let sink = SqliteSink::new(auto_map_cfg(&url, faucet_core::WriteMode::Append, &[]))
            .await
            .unwrap();
        let rows = [serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        assert_eq!(
            sink.write_batch_idempotent(&rows, "scope", "0001")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sink.last_committed_token("scope").await.unwrap().as_deref(),
            Some("0001")
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&sink.pool)
            .await
            .unwrap();
        assert_eq!(n, 2);
    }
}
