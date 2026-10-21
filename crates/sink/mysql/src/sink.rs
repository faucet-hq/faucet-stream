//! MySQL sink implementation.

use crate::config::{BinaryEncoding, BinaryValue, MysqlColumnMapping, MysqlSinkConfig};
use async_trait::async_trait;
use faucet_common_sql::dialects::{Mysql, mysql};
use faucet_common_sql::driver::label;
use faucet_common_sql::{
    EvolutionStep, SplitPage, SqlDialect, SqlDriver, SqlEvolution, SqlExecutor, SqlFuture,
    SqlLayout, SqlSinkDriver, SqlSinkEngine, Statement, StatementKind, TableColumn, TableTarget,
};
use faucet_core::driver::{DriverSink, RollbackDriver};
use faucet_core::{FaucetError, SqlBaseType};
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlConnection, MySqlPool, Row};

/// Width of the watermark table's `scope` PRIMARY KEY column. MySQL cannot index
/// an unbounded column, so a scope longer than this is shortened to a digest
/// form rather than risking silent truncation onto another row's watermark
/// (#456 L1).
const SCOPE_COL_WIDTH: usize = 255;

/// Fit a pipeline scope into [`SCOPE_COL_WIDTH`].
pub(crate) fn scope_key(scope: &str) -> String {
    faucet_core::idempotency::scope_key(scope, SCOPE_COL_WIDTH)
}

/// The pre-1.13 shortened form of a long scope, read once and retired (#862 M-017).
pub(crate) fn legacy_scope_key(scope: &str) -> Option<String> {
    faucet_core::idempotency::legacy_scope_key(scope, SCOPE_COL_WIDTH)
}

/// A sink that writes JSON records to a MySQL table.
///
/// Built on the SQL family driver (RFC 0014): `MysqlDriver` holds the MySQL
/// I/O (the `RENAME TABLE` swap, the catalog reads, the journal created
/// before a keyed write), and [`SqlSinkDriver`] drives writes, per-row
/// outcomes, the exactly-once commit, cleanup, the overwrite lifecycle and
/// schema evolution from it.
pub struct MysqlSink {
    inner: DriverSink<SqlSinkDriver<MysqlDriver>>,
}

/// The MySQL I/O primitives the SQL family driver runs on.
pub(crate) struct MysqlDriver {
    pub(crate) config: MysqlSinkConfig,
    pub(crate) pool: MySqlPool,
    /// Whether the rollback journal table is known to exist.
    pub(crate) journal_ready: std::sync::atomic::AtomicBool,
    /// The shared SQL planning engine (#884).
    pub(crate) engine: SqlSinkEngine<Mysql>,
    layout: SqlLayout,
}

/// The engine target for a bare table name.
pub(crate) fn target(table: &str) -> TableTarget {
    TableTarget::new(table, quote_ident_mysql(table))
}

/// The engine's view of one MySQL connection (or open transaction). Values
/// bind as native MySQL types; a string for a binary column is decoded per
/// `binary` (#789 SQL-02).
pub(crate) struct MysqlExec<'c> {
    pub(crate) conn: &'c mut MySqlConnection,
    pub(crate) sink: &'c MysqlDriver,
    pub(crate) binary: BinaryEncoding,
}

#[async_trait]
impl SqlExecutor for MysqlExec<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        Ok(table_columns(&mut *self.conn, &target.name)
            .await?
            .into_iter()
            .map(|(name, data_type)| TableColumn::new(name, data_type))
            .collect())
    }

    /// Full type specs plus character set and collation, so the cleanup key
    /// table compares with the destination's key columns without a collation
    /// mismatch. Empty when the table is missing.
    /// Each key column's `DATA_TYPE`, so a keyed delete decodes a binary key
    /// the way the upsert stored it.
    async fn key_types(
        &mut self,
        target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        let columns = table_columns(&mut *self.conn, &target.name).await?;
        Ok(key
            .iter()
            .map(|k| {
                columns
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(k))
                    .map(|(_, t)| t.clone())
            })
            .collect())
    }

    async fn cleanup_columns(
        &mut self,
        _target: &TableTarget,
    ) -> Result<Vec<TableColumn>, FaucetError> {
        Ok(self
            .sink
            .read_cleanup_columns(&mut *self.conn)
            .await?
            .into_iter()
            .map(|(name, column_type, charset, collation)| {
                let def = cleanup_temp_type(&column_type, charset.as_deref(), collation.as_deref());
                TableColumn::new(name, def)
            })
            .collect())
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        let mut q = sqlx::query(&stmt.sql);
        for p in &stmt.params {
            // Every statement kind that binds a binary column — inserts,
            // upserts, keyed deletes, the cleanup key load and scope — decodes
            // the value the same way, or a delete would compare the encoded
            // text against the stored bytes and match nothing.
            q = match p.value.as_ref() {
                Value::String(s) if p.sql_type.is_some_and(is_binary_type) => {
                    bind_binary(q, s, self.binary).map_err(|e| {
                        FaucetError::Sink(format!(
                            "{}: column {}: {e}",
                            statement_context(stmt.kind),
                            p.column
                        ))
                    })?
                }
                _ if p.non_integer_text().is_some() => {
                    q.bind(p.non_integer_text().expect("checked"))
                }
                v => bind_value(q, v),
            };
        }
        let context = statement_context(stmt.kind);
        q.execute(&mut *self.conn)
            .await
            .map(|r| r.rows_affected())
            .map_err(|e| match stmt.kind {
                StatementKind::CleanupCreate => FaucetError::Sink(format!(
                    "{context}: {e} — the connection user needs the CREATE TEMPORARY TABLES \
                     privilege"
                )),
                _ => FaucetError::Sink(format!("{context}: {e}")),
            })
    }

    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        let mut q = sqlx::query(&stmt.sql);
        for p in &stmt.params {
            q = bind_value(q, p.value.as_ref());
        }
        let rows = q
            .fetch_all(&mut *self.conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: reading keys to journal: {e}")))?;
        rows.iter()
            .map(|r| {
                r.try_get::<String, _>(0).map_err(|e| {
                    FaucetError::Sink(format!("cleanup: reading keys to journal: {e}"))
                })
            })
            .collect()
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.sink.journal_plan(&mut *self.conn, plan, run_id).await
    }
}

/// Quote a MySQL identifier using backticks.
///
/// Wraps the name in backticks and escapes any embedded backticks by doubling
/// them, per MySQL convention.
pub(crate) fn quote_ident_mysql(name: &str) -> String {
    mysql::quote_ident(name)
}

/// A type spec read back from `INFORMATION_SCHEMA` that is safe to re-emit
/// verbatim in the cleanup temp table's DDL, or `None`.
///
/// `COLUMN_TYPE` is generated by MySQL itself (`varchar(255)`,
/// `bigint unsigned`, `enum('a','b')`) so it is well-formed by construction;
/// this filters to the shape those take rather than pasting catalog text into
/// DDL blind.
fn safe_type_spec(declared: &str) -> Option<&str> {
    let t = declared.trim();
    if t.is_empty() {
        return None;
    }
    t.chars()
        .all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '(' | ')' | ',' | '\'' | '-' | '.')
        })
        .then_some(t)
}

/// Whether a charset / collation name from `INFORMATION_SCHEMA` is a bare
/// identifier and can be emitted into DDL.
fn safe_charset_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The type of one cleanup temp-table column, mirroring the destination
/// column's type — and, for string columns, its character set and collation.
///
/// The collation matters: comparing two string columns with different
/// collations raises *"Illegal mix of collations"*, which would fail the
/// cleanup on any table whose key column overrides the schema default. Falls
/// back to `LONGTEXT` for an unrecognisable type spec — MySQL, unlike SQLite,
/// has no typeless column, and a text column still compares correctly against a
/// numeric one (MySQL coerces).
fn cleanup_temp_type(column_type: &str, charset: Option<&str>, collation: Option<&str>) -> String {
    let mut def = safe_type_spec(column_type)
        .unwrap_or("LONGTEXT")
        .to_string();
    if let Some(cs) = charset.filter(|c| safe_charset_name(c)) {
        def.push_str(&format!(" CHARACTER SET {cs}"));
    }
    if let Some(coll) = collation.filter(|c| safe_charset_name(c)) {
        def.push_str(&format!(" COLLATE {coll}"));
    }
    def
}

/// One column definition for the cleanup temp table (see
/// [`cleanup_temp_type`]).
#[cfg(test)]
fn cleanup_temp_column_def(
    col: &str,
    column_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> String {
    format!(
        "{} {}",
        quote_ident_mysql(col),
        cleanup_temp_type(column_type, charset, collation)
    )
}

/// `CREATE TEMPORARY TABLE `faucet_cleanup_keys` (…)` over rendered column
/// definitions.
#[cfg(test)]
fn build_cleanup_temp_table_sql(col_defs: &[String]) -> String {
    format!(
        "CREATE TEMPORARY TABLE {} ({})",
        Mysql.cleanup_keys_table(),
        col_defs.join(", ")
    )
}

/// `INSERT INTO `faucet_cleanup_keys` (…) VALUES (?, …), …` for `rows` key
/// tuples.
#[cfg(test)]
fn build_cleanup_insert_sql(key: &[String], rows: usize) -> String {
    Mysql
        .cleanup_insert_sql(key, &[], rows)
        .expect("mysql cleanup insert")
}

/// The cleanup DELETE, referencing the target by name (a single-table
/// `DELETE … AS alias` only became legal in MySQL 8.0.16).
#[cfg(test)]
fn build_cleanup_delete_sql(table: &str, scope_cols: &[String], key: &[String]) -> String {
    Mysql
        .cleanup_delete_sql(&target(table), scope_cols, &[], key)
        .expect("mysql cleanup delete")
}

/// Check that every scope and key column exists on the destination table.
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
        &Mysql.cleanup_table_display(&target(table)),
    )
}

/// Bind one JSON value to a MySQL query as its native type.
///
/// Shared by the delete-by-key and scoped-cleanup paths so the two never drift:
/// a key bound as a JSON string (`"7"` instead of `7`) would silently match
/// nothing and turn a delete into a no-op.
pub(crate) fn bind_value<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    v: &Value,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::Bool(b) => q.bind(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(u) = n.as_u64() {
                // Above i64::MAX: exact, never through f64 (#789 SQL-73).
                q.bind(u)
            } else {
                // Its shortest text, which MySQL parses exactly into a DECIMAL
                // where a bound double would be converted from binary (#885).
                q.bind(n.to_string())
            }
        }
        Value::String(s) => q.bind(s.clone()),
        // Arrays/objects have no scalar SQL representation — bind their JSON
        // text (suitable for TEXT / JSON columns).
        other => q.bind(other.to_string()),
    }
}

/// A table's `(column, lowercase DATA_TYPE)` pairs in declared order.
pub(crate) async fn table_columns(
    conn: &mut MySqlConnection,
    table: &str,
) -> Result<Vec<(String, String)>, FaucetError> {
    let columns: Vec<(String, String)> = sqlx::query(
        "SELECT COLUMN_NAME, CAST(DATA_TYPE AS CHAR) AS DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
    .iter()
    .map(|row| {
        (
            row.get::<String, _>("COLUMN_NAME"),
            row.get::<String, _>("DATA_TYPE").to_ascii_lowercase(),
        )
    })
    .collect();
    if columns.is_empty() {
        return Err(FaucetError::Sink(format!(
            "table '{table}' has no columns or does not exist"
        )));
    }
    Ok(columns)
}

/// Run a write under `secs` (0 = unbounded): a server that stops answering
/// fails the write instead of hanging the run forever (#789 SQL-109).
pub(crate) async fn bounded<T>(
    secs: u64,
    what: &str,
    fut: impl std::future::Future<Output = Result<T, FaucetError>>,
) -> Result<T, FaucetError> {
    if secs == 0 {
        return fut.await;
    }
    match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
        Ok(result) => result,
        Err(_) => Err(FaucetError::Sink(format!(
            "mysql {what} timed out after {secs}s waiting for the server (write_timeout_secs)"
        ))),
    }
}

/// What else hangs off a table that a `RENAME TABLE` swap would lose.
#[derive(Debug, Default)]
pub(crate) struct Dependents {
    /// Other tables whose foreign keys reference this one.
    pub(crate) referenced_by: Vec<String>,
    /// This table's own foreign keys plus its triggers: neither moves to a
    /// renamed-in replacement (#789 SQL-108).
    pub(crate) pinned: Vec<String>,
}

pub(crate) async fn table_dependents(
    conn: &mut MySqlConnection,
    table: &str,
) -> Result<Dependents, FaucetError> {
    let err = |e: sqlx::Error| FaucetError::Sink(format!("mysql: read dependents of {table}: {e}"));
    let referenced_by: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT CAST(TABLE_NAME AS CHAR) FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS \
         WHERE CONSTRAINT_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = ? AND TABLE_NAME <> ? \
         ORDER BY 1",
    )
    .bind(table)
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(err)?;
    let mut pinned: Vec<String> = sqlx::query_scalar(
        "SELECT CONCAT('foreign key ', CAST(CONSTRAINT_NAME AS CHAR)) \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS \
         WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY 1",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(err)?;
    let triggers: Vec<String> = sqlx::query_scalar(
        "SELECT CONCAT('trigger ', CAST(TRIGGER_NAME AS CHAR)) FROM INFORMATION_SCHEMA.TRIGGERS \
         WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ? ORDER BY 1",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(err)?;
    pinned.extend(triggers);
    Ok(Dependents {
        referenced_by,
        pinned,
    })
}

/// A table's non-generated columns, in order.
pub(crate) async fn insertable_columns(
    conn: &mut MySqlConnection,
    table: &str,
) -> Result<Vec<String>, FaucetError> {
    let rows = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, CAST(EXTRA AS CHAR) AS EXTRA \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() \
         ORDER BY ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| FaucetError::Sink(format!("mysql: read columns of {table}: {e}")))?;
    Ok(rows
        .iter()
        .filter(|r| {
            let extra = r
                .get::<Option<String>, _>("EXTRA")
                .unwrap_or_default()
                .to_ascii_lowercase();
            !extra.contains("generated") || extra.contains("default_generated")
        })
        .map(|r| r.get("COLUMN_NAME"))
        .collect())
}

/// One of a table's foreign keys: its name, referenced table, and
/// `(column, referenced column)` pairs in key order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ForeignKey {
    pub(crate) name: String,
    pub(crate) referenced_schema: String,
    pub(crate) referenced_table: String,
    /// The referenced table lives in the current database.
    pub(crate) local: bool,
    pub(crate) columns: Vec<(String, String)>,
}

impl ForeignKey {
    /// Whether the key references `table` itself, not a same-named table in
    /// another database.
    pub(crate) fn references(&self, table: &str) -> bool {
        self.local && self.referenced_table == table
    }
}

pub(crate) async fn table_foreign_keys(
    conn: &mut MySqlConnection,
    table: &str,
) -> Result<Vec<ForeignKey>, FaucetError> {
    let rows = sqlx::query(
        "SELECT CAST(CONSTRAINT_NAME AS CHAR) AS N, CAST(COLUMN_NAME AS CHAR) AS C, \
                CAST(REFERENCED_TABLE_SCHEMA AS CHAR) AS RS, \
                CAST(REFERENCED_TABLE_NAME AS CHAR) AS RT, \
                CAST(REFERENCED_COLUMN_NAME AS CHAR) AS RC, \
                CAST(REFERENCED_TABLE_SCHEMA = DATABASE() AS SIGNED) AS L \
         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
           AND REFERENCED_TABLE_NAME IS NOT NULL \
         ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| FaucetError::Sink(format!("mysql: read foreign keys of {table}: {e}")))?;
    let mut fks: Vec<ForeignKey> = Vec::new();
    for r in &rows {
        let name: String = r.get("N");
        let pair = (r.get::<String, _>("C"), r.get::<String, _>("RC"));
        match fks.last_mut() {
            Some(fk) if fk.name == name => fk.columns.push(pair),
            _ => fks.push(ForeignKey {
                name,
                referenced_schema: r.get("RS"),
                referenced_table: r.get("RT"),
                local: r.get::<Option<i64>, _>("L") == Some(1),
                columns: vec![pair],
            }),
        }
    }
    Ok(fks)
}

/// `SELECT COUNT(*)` of `table`'s rows that break `fk` — every key column set
/// (MATCH SIMPLE) and no referenced row carrying those values.
fn fk_violation_sql(table: &str, fk: &ForeignKey) -> String {
    let set = fk
        .columns
        .iter()
        .map(|(c, _)| format!("c.{} IS NOT NULL", quote_ident_mysql(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let join = fk
        .columns
        .iter()
        .map(|(c, r)| format!("p.{} = c.{}", quote_ident_mysql(r), quote_ident_mysql(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT COUNT(*) FROM {} c WHERE {set} AND NOT EXISTS (SELECT 1 FROM {}.{} p WHERE {join})",
        quote_ident_mysql(table),
        quote_ident_mysql(&fk.referenced_schema),
        quote_ident_mysql(&fk.referenced_table)
    )
}

/// Replace every row of `target` with the rows of `source` in one transaction
/// (`DELETE` + `INSERT … SELECT` over the shared non-generated columns), so the
/// target keeps its foreign keys and triggers — which fire for these rows.
///
/// InnoDB checks foreign keys row by row, so a self-referencing key fails the
/// `DELETE` at a parent whose children are still there and the unordered
/// `INSERT … SELECT` at a child that arrives before its parent (#862 M-042).
/// For such a table the replace runs with `FOREIGN_KEY_CHECKS = 0` and every
/// one of the target's foreign keys is re-validated before the commit.
pub(crate) async fn replace_rows_in_place(
    conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
    target: &str,
    source: &str,
) -> Result<(), FaucetError> {
    let into = insertable_columns(conn, target).await?;
    let from = insertable_columns(conn, source).await?;
    let cols = into
        .iter()
        .filter(|c| from.contains(c))
        .map(|c| quote_ident_mysql(c))
        .collect::<Vec<_>>()
        .join(", ");
    let fks = table_foreign_keys(conn, target).await?;
    let self_referencing = fks.iter().any(|fk| fk.references(target));
    let (t, s) = (quote_ident_mysql(target), quote_ident_mysql(source));
    let err = |e: sqlx::Error| FaucetError::Sink(format!("mysql overwrite: replace rows: {e}"));
    if self_referencing {
        // A dropped future or a failed restore must not return a connection
        // with foreign-key checks off to the pool.
        conn.close_on_drop();
        sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
            .execute(&mut **conn)
            .await
            .map_err(err)?;
    }
    let result = async {
        let mut tx = sqlx::Connection::begin(&mut **conn).await.map_err(err)?;
        for stmt in [
            format!("DELETE FROM {t}"),
            format!("INSERT INTO {t} ({cols}) SELECT {cols} FROM {s}"),
        ] {
            sqlx::query(&stmt).execute(&mut *tx).await.map_err(err)?;
        }
        if self_referencing {
            for fk in &fks {
                let broken: i64 = sqlx::query_scalar(&fk_violation_sql(target, fk))
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(err)?;
                if broken > 0 {
                    return Err(FaucetError::Sink(format!(
                        "mysql overwrite: {broken} row(s) of '{target}' break foreign key {} \
                         (no matching row in '{}'); nothing was replaced",
                        fk.name, fk.referenced_table
                    )));
                }
            }
        }
        tx.commit().await.map_err(err)
    }
    .await;
    if self_referencing {
        let restored = sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
            .execute(&mut **conn)
            .await;
        result?;
        if let Err(e) = restored {
            tracing::warn!(
                table = target,
                error = %e,
                "mysql overwrite: rows replaced, but restoring FOREIGN_KEY_CHECKS failed; the \
                 connection is closed instead of returned to the pool"
            );
        }
        return Ok(());
    }
    result
}

/// The refusal for an overwrite of a table other tables reference: InnoDB
/// re-points their foreign keys at the renamed-away table (#789 SQL-129), and a
/// `DELETE` would cascade into or be blocked by them.
pub(crate) fn referenced_overwrite_error(table: &str, referenced_by: &[String]) -> FaucetError {
    FaucetError::Config(format!(
        "mysql overwrite: table '{table}' is referenced by a foreign key from {}; replacing its \
         rows would cascade into, break or be blocked by those tables. Use write_mode: upsert, \
         or drop the foreign key",
        referenced_by.join(", ")
    ))
}

/// `(name, DATA_TYPE)` pairs as engine columns.
#[cfg(test)]
fn engine_columns(columns: &[(String, String)]) -> Vec<TableColumn> {
    columns
        .iter()
        .map(|(name, data_type)| TableColumn::new(name.clone(), data_type.clone()))
        .collect()
}

/// The `(column, type, value)` triples a record carries, in table order.
/// MySQL column names are case-insensitive, so a field matches its column
/// exactly first and otherwise ignoring ASCII case (#789 SQL-50).
#[cfg(test)]
pub(crate) fn match_columns<'a>(
    columns: &'a [(String, String)],
    obj: &'a serde_json::Map<String, Value>,
) -> Vec<(&'a String, &'a String, &'a Value)> {
    faucet_common_sql::mapping::match_record(&engine_columns(columns), obj, Mysql.column_matching())
        .into_iter()
        .map(|(i, v)| (&columns[i].0, &columns[i].1, v))
        .collect()
}

/// Split a page into per-row outcomes, the records to write, and each written
/// record's position in `records`. A record whose binary-column value
/// `binary_encoding` refuses — or, with `require_match`, that matches no
/// column — fails on its own row instead of failing the page.
fn split_columns(
    records: &[Value],
    columns: &[TableColumn],
    binary: BinaryEncoding,
    require_match: bool,
) -> SplitPage {
    faucet_common_sql::split_page(
        &Mysql,
        "",
        records,
        columns,
        require_match,
        |idx, col, v| match v {
            Value::String(s) if is_binary_type(&col.sql_type) => binary
                .resolve(s)
                .err()
                .map(|e| format!("mysql: row {idx}: column {}: {e}", col.name)),
            _ => None,
        },
    )
}

/// [`split_columns`] over `(name, data_type)` pairs.
#[cfg(test)]
fn split_writable(
    records: &[Value],
    columns: &[(String, String)],
    binary: BinaryEncoding,
    require_match: bool,
) -> (Vec<faucet_core::RowOutcome>, Vec<Value>, Vec<usize>) {
    let split = split_columns(records, &engine_columns(columns), binary, require_match);
    (split.outcomes, split.writable, split.positions)
}

/// Whether a MySQL `DATA_TYPE` (or a full `COLUMN_TYPE` such as
/// `varbinary(32)`) stores raw bytes.
pub(crate) fn is_binary_type(data_type: &str) -> bool {
    let base = data_type
        .trim()
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        base.as_str(),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
    )
}

/// Bind a string bound for a binary column, decoded per `binary_encoding`.
pub(crate) fn bind_binary<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    s: &str,
    binary: BinaryEncoding,
) -> Result<sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>, String> {
    match binary.resolve(s) {
        Ok(BinaryValue::Bytes(bytes)) => Ok(q.bind(bytes)),
        Ok(BinaryValue::Text(t)) => Ok(q.bind(t.to_string())),
        Err(e) => Err(e.to_string()),
    }
}

/// Bind a key value: decoded when its column is binary, else natively.
pub(crate) fn bind_key<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    v: &Value,
    binary_column: bool,
    binary: BinaryEncoding,
) -> Result<sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>, String> {
    match v {
        Value::String(s) if binary_column => bind_binary(q, s, binary),
        v => Ok(bind_value(q, v)),
    }
}

fn statement_context(kind: StatementKind) -> &'static str {
    match kind {
        StatementKind::Insert | StatementKind::InsertJson | StatementKind::Upsert => {
            "MySQL insert failed"
        }
        StatementKind::Delete => "MySQL delete failed",
        StatementKind::CleanupDrop => "cleanup: temp table drop failed",
        StatementKind::CleanupCreate => "cleanup: temp table creation failed",
        StatementKind::CleanupLoad => "cleanup: loading keys failed",
        StatementKind::CleanupDelete => "cleanup: delete failed",
        StatementKind::TokenTable => "MySQL commit-table create failed",
        _ => "MySQL token upsert failed",
    }
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580): key text
/// columns get a bounded type (MySQL cannot index `LONGTEXT`) and a keyed write
/// a PRIMARY KEY for `ON DUPLICATE KEY UPDATE` (#676).
#[cfg(test)]
fn build_create_table_sql(
    table: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
    key: &[String],
) -> String {
    match json_column {
        Some(col) => Mysql.create_json_table_sql(&target(table), col),
        None => Mysql.create_table_sql(&target(table), columns, key),
    }
    .expect("mysql CREATE TABLE")
}
/// The type for a primary-key column of a created table (#676). 191 characters
/// keeps even a four-column utf8mb4 key under InnoDB's 3072-byte index limit.
#[cfg(test)]
fn mysql_key_keyword(t: SqlBaseType) -> &'static str {
    Mysql.key_type_name(t)
}

/// Map a [`SqlBaseType`] to the MySQL type keyword used when adding/widening a
/// column during schema evolution (issue #194). Integers always widen to
/// `BIGINT` and floats to `DOUBLE` so a later, wider value never overflows a
/// narrower column. `Text` maps to `LONGTEXT` (the widest text type, so a long
/// value never truncates) and `Json` to MySQL's native `JSON`.
#[cfg(test)]
fn mysql_keyword(t: SqlBaseType) -> &'static str {
    Mysql.type_name(t)
}

/// A column's definition as `INFORMATION_SCHEMA.COLUMNS` reports it.
#[derive(Debug, Clone, Default)]
struct MysqlColumnDef {
    column_type: String,
    charset: Option<String>,
    collation: Option<String>,
    default: Option<String>,
    extra: String,
    comment: String,
    /// MariaDB reports `COLUMN_DEFAULT` as an SQL expression (`'abc'`,
    /// `current_timestamp()`), MySQL as the bare literal.
    mariadb: bool,
}

/// The `DEFAULT` clause body for `def`'s reported default (#862 M-065).
/// MySQL ≥ 8.0.13 marks expression defaults `DEFAULT_GENERATED`; MySQL 5.7
/// reports `CURRENT_TIMESTAMP` with no marker, and MariaDB quotes literals
/// itself — re-quoting either would change the default or be rejected.
fn render_default(d: &str, def: &MysqlColumnDef) -> String {
    if def.mariadb {
        return d.to_string();
    }
    let upper = d.to_ascii_uppercase();
    if upper.starts_with("CURRENT_TIMESTAMP")
        || upper.starts_with("NOW(")
        || upper.starts_with("LOCALTIMESTAMP")
    {
        return d.to_string();
    }
    if def.extra.to_ascii_lowercase().contains("default_generated") {
        return format!("({d})");
    }
    let is_bit_literal = def.column_type.to_ascii_lowercase().starts_with("bit")
        && d.len() > 3
        && d.starts_with("b'")
        && d.ends_with('\'')
        && d[2..d.len() - 1].chars().all(|c| c == '0' || c == '1');
    if is_bit_literal {
        return d.to_string();
    }
    format!("'{}'", d.replace('\\', "\\\\").replace('\'', "''"))
}

/// `ALTER TABLE … MODIFY COLUMN` that keeps `def` exactly and only drops NOT
/// NULL. `None` for a generated column, whose definition is not reproduced.
fn relax_column_sql(table: &str, col: &str, def: &MysqlColumnDef) -> Option<String> {
    redefine_column_sql(table, col, def, None, true)
}

/// `ALTER TABLE … MODIFY COLUMN` that re-emits `def` — charset, collation,
/// default, AUTO_INCREMENT, ON UPDATE, comment — with `new_type` in place of
/// its type when given (a numeric type drops the text-only charset/collation)
/// and `NULL`/`NOT NULL` per `nullable`. `None` for a generated column.
fn redefine_column_sql(
    table: &str,
    col: &str,
    def: &MysqlColumnDef,
    new_type: Option<&str>,
    nullable: bool,
) -> Option<String> {
    let extra = def.extra.to_ascii_lowercase();
    if extra.contains("generated") && !extra.contains("default_generated") {
        return None;
    }
    let quote = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
    let mut sql = format!(
        "ALTER TABLE {table} MODIFY COLUMN {} {}",
        quote_ident_mysql(col),
        new_type.unwrap_or(&def.column_type)
    );
    if new_type.is_none() {
        if let Some(cs) = &def.charset {
            sql.push_str(&format!(" CHARACTER SET {cs}"));
        }
        if let Some(co) = &def.collation {
            sql.push_str(&format!(" COLLATE {co}"));
        }
    }
    sql.push_str(if nullable { " NULL" } else { " NOT NULL" });
    if let Some(d) = &def.default {
        sql.push_str(&format!(" DEFAULT {}", render_default(d, def)));
    }
    if extra.contains("auto_increment") {
        sql.push_str(" AUTO_INCREMENT");
    }
    if let Some(i) = extra.find("on update ") {
        sql.push_str(&format!(
            " ON UPDATE {}",
            def.extra[i + "on update ".len()..].trim()
        ));
    }
    if !def.comment.is_empty() {
        sql.push_str(&format!(" COMMENT {}", quote(&def.comment)));
    }
    Some(sql)
}

/// `ALTER TABLE <table> ADD COLUMN `col` <kw>` — column addition.
///
/// MySQL (pre-8.0.x) has no `ADD COLUMN IF NOT EXISTS`, so idempotency is
/// achieved by the caller pre-checking the existing column set and only
/// emitting this for columns not already present. `table` is the already-quoted
/// table reference.
fn build_add_column_sql(table: &str, col: &str, t: SqlBaseType) -> String {
    Mysql
        .add_column_sql(&TableTarget::new(table, table), col, t)
        .expect("mysql ADD COLUMN")
}

/// The type a widening rewrites a column to: exact `DECIMAL(65,30)` for an
/// integer column that starts receiving fractions, otherwise the base type's
/// keyword. A value outside DECIMAL's range then fails loudly instead of being
/// rounded.
#[cfg(test)]
fn widened_type(from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
    Mysql.widened_type_name(from, to)
}

/// Map a MySQL `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` value (lowercase, no
/// precision — e.g. `bigint`, `double`, `json`, `varchar`) back to a JSON-Schema
/// type fragment so [`MysqlSink::current_schema`] round-trips with
/// [`faucet_core::diff_schema`]. `nullable` reflects `IS_NULLABLE = 'YES'`.
///
/// Note: `INFORMATION_SCHEMA.DATA_TYPE` returns bare `tinyint` without the
/// `(1)` precision, so a `TINYINT(1)` (conventionally boolean) is
/// indistinguishable from a real `TINYINT` — both map to `integer` for safety.
fn mysql_data_type_to_json_schema(data_type: &str, nullable: bool) -> Value {
    let base = match data_type {
        "bigint" | "int" | "integer" | "smallint" | "mediumint" | "tinyint" => "integer",
        "double" | "float" | "decimal" | "numeric" => "number",
        "json" => "object",
        _ => "string",
    };
    if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    }
}

/// Build the `ON DUPLICATE KEY UPDATE …` tail for an upsert INSERT.
///
/// MySQL's `ON DUPLICATE KEY UPDATE` does not name a conflict target — it
/// relies on the table's existing PRIMARY or UNIQUE key. Non-key columns are
/// set from `VALUES(col)`. If every column is a key column there is nothing to
/// update, so a self-assignment no-op on the first key column is emitted to
/// keep the statement syntactically valid.
/// Decide whether the configured upsert/delete `key` exactly corresponds to one
/// of the target table's PRIMARY/UNIQUE indexes.
///
/// MySQL's `INSERT … ON DUPLICATE KEY UPDATE` does not name a conflict target —
/// it resolves on *any* unique index present on the table. The unified
/// write-mode contract, however, treats the configured `key` as the
/// authoritative conflict target (`plan_writes` dedups and routes by exactly
/// that key). If the configured `key` does not match a real unique index, MySQL
/// would silently resolve the conflict on a *different* index, producing wrong
/// upsert results that the user cannot detect (finding F33). This check lets the
/// sink fail fast at construction instead.
///
/// `unique_indexes` is the set of the table's PRIMARY/UNIQUE indexes, each given
/// as the full set of its column names. `key` is the configured key columns.
/// The comparison is **order-insensitive** (a UNIQUE index on `(a, b)` matches a
/// key of `[b, a]`) and requires the **full** column set of some index to match
/// the key exactly — a prefix, subset, or superset does **not** match, because
/// `ON DUPLICATE KEY UPDATE` would then trigger on a broader or narrower index
/// than the one the pipeline deduped on.
fn key_matches_unique_index(
    unique_indexes: &[std::collections::BTreeSet<String>],
    key: &[String],
) -> bool {
    if key.is_empty() {
        return false;
    }
    let key_set: std::collections::BTreeSet<String> = key.iter().cloned().collect();
    unique_indexes.contains(&key_set)
}

/// The unique indexes other than the one `key` names, each rendered
/// `(col, …)`.
fn other_unique_indexes(
    unique_indexes: &[std::collections::BTreeSet<String>],
    key: &[String],
) -> Vec<String> {
    let key_set: std::collections::BTreeSet<String> = key.iter().cloned().collect();
    unique_indexes
        .iter()
        .filter(|idx| **idx != key_set)
        .map(|idx| {
            format!(
                "({})",
                idx.iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect()
}

#[cfg(test)]
fn on_duplicate_clause(key: &[String], all_cols: &[String]) -> String {
    Mysql
        .upsert_tail(key, all_cols)
        .expect("mysql upserts with ON DUPLICATE KEY")
}

/// A connection the SQL family driver runs statements on: an open write
/// transaction, or a pooled connection for catalog reads.
pub(crate) enum MysqlConn<'c> {
    /// A `BEGIN` transaction.
    Tx(sqlx::Transaction<'c, sqlx::MySql>, &'c MysqlDriver),
    /// A plain pooled connection.
    Pooled(sqlx::pool::PoolConnection<sqlx::MySql>, &'c MysqlDriver),
}

impl MysqlConn<'_> {
    fn exec(&mut self) -> MysqlExec<'_> {
        match self {
            MysqlConn::Tx(tx, sink) => sink.exec(tx),
            MysqlConn::Pooled(conn, sink) => sink.exec(conn),
        }
    }
}

#[async_trait]
impl SqlExecutor for MysqlConn<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        self.exec().columns(target).await
    }

    async fn key_types(
        &mut self,
        target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        self.exec().key_types(target, key).await
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

    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        self.exec().fetch_text(stmt).await
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.exec().journal_plan(plan, run_id).await
    }
}

/// Add `STRICT_ALL_TABLES` to a connection's `sql_mode`. A server whose mode
/// lacks it turns an over-long string, an out-of-range number or non-numeric
/// text for a number column into a warning and writes a truncated, clamped or
/// zero value; strict mode makes each a statement error, so the page fails
/// (or the row goes to the DLQ) instead of landing changed. The server's other
/// modes are kept.
pub(crate) const STRICT_SQL_MODE: &str =
    "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES')";

async fn strict_session(conn: &mut sqlx::MySqlConnection) -> Result<(), sqlx::Error> {
    sqlx::Executor::execute(conn, STRICT_SQL_MODE)
        .await
        .map(|_| ())
}

impl MysqlSink {
    /// Create a new MySQL sink. Establishes a connection pool.
    pub async fn new(config: MysqlSinkConfig) -> Result<Self, FaucetError> {
        config.write.validate()?;
        if matches!(
            config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) && !matches!(config.column_mapping, MysqlColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "mysql sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSON blob)"
                    .into(),
            ));
        }

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .after_connect(|conn, _meta| Box::pin(async move { strict_session(conn).await }))
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL connection failed: {e}")))?;

        let engine = SqlSinkEngine::new(Mysql, config.write.clone());
        let mut layout = SqlLayout::new(config.table_name.clone())
            .create_table(config.create_table)
            .batch_size(config.batch_size);
        if let MysqlColumnMapping::Json { column } = &config.column_mapping {
            layout = layout.json_column(column.clone());
        }
        let driver = MysqlDriver {
            config,
            pool,
            journal_ready: std::sync::atomic::AtomicBool::new(false),
            engine,
            layout,
        };

        // For upsert/delete, MySQL's anonymous `ON DUPLICATE KEY UPDATE` /
        // `DELETE … WHERE (key) IN (…)` only resolves correctly when the
        // configured `key` matches a real PRIMARY/UNIQUE index. Assert that here
        // so a silent mismatch (finding F33) fails fast at construction.
        if matches!(
            driver.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            driver.assert_key_is_unique_index().await?;
        }

        Ok(Self {
            inner: DriverSink::new(SqlSinkDriver::new(driver)),
        })
    }
}

impl MysqlSink {
    /// Join the overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): every instance of
    /// one run (begin, writers, commit) stages in `<table>__faucet_ovw_<token>`,
    /// so a concurrent overwrite run of the same table neither drops nor
    /// shares it.
    pub fn with_overwrite_run(mut self, token: &str) -> Result<Self, FaucetError> {
        faucet_core::idempotency::check_overwrite_run_token(token)?;
        let driver = self.inner.driver_mut().driver_mut();
        driver.layout = driver.layout.clone().overwrite_run(Some(token.to_string()));
        Ok(self)
    }
}

#[async_trait]
impl faucet_core::Sink for MysqlSink {
    faucet_core::forward_sink!(inner);
}

impl MysqlDriver {
    /// A column's definition and whether it is nullable, or `None` when the
    /// column does not exist.
    async fn column_definition(
        &self,
        conn: &mut MySqlConnection,
        col: &str,
    ) -> Result<Option<(MysqlColumnDef, bool)>, FaucetError> {
        let row = sqlx::query(
            "SELECT CAST(COLUMN_TYPE AS CHAR) AS COLUMN_TYPE, \
                    CAST(CHARACTER_SET_NAME AS CHAR) AS CHARACTER_SET_NAME, \
                    CAST(COLLATION_NAME AS CHAR) AS COLLATION_NAME, \
                    CAST(COLUMN_DEFAULT AS CHAR) AS COLUMN_DEFAULT, \
                    CAST(EXTRA AS CHAR) AS EXTRA, \
                    CAST(COLUMN_COMMENT AS CHAR) AS COLUMN_COMMENT, \
                    CAST(IS_NULLABLE AS CHAR) AS IS_NULLABLE, \
                    CAST(VERSION() AS CHAR) AS SERVER_VERSION \
               FROM INFORMATION_SCHEMA.COLUMNS \
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
        )
        .bind(&self.config.table_name)
        .bind(col)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| FaucetError::Sink(format!("MySQL read definition of {col} failed: {e}")))?;
        Ok(row.map(|row| {
            let def = MysqlColumnDef {
                column_type: row.get("COLUMN_TYPE"),
                charset: row.get("CHARACTER_SET_NAME"),
                collation: row.get("COLLATION_NAME"),
                default: row.get("COLUMN_DEFAULT"),
                extra: row.get::<Option<String>, _>("EXTRA").unwrap_or_default(),
                comment: row
                    .get::<Option<String>, _>("COLUMN_COMMENT")
                    .unwrap_or_default(),
                mariadb: row
                    .get::<Option<String>, _>("SERVER_VERSION")
                    .is_some_and(|v| v.to_ascii_lowercase().contains("mariadb")),
            };
            let nullable = row
                .get::<Option<String>, _>("IS_NULLABLE")
                .is_some_and(|n| n.eq_ignore_ascii_case("YES"));
            (def, nullable)
        }))
    }

    pub(crate) async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name = ?",
        )
        .bind(table)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("MySQL table probe failed: {e}")))?;
        Ok(exists.is_some())
    }

    /// Staging table name used while an overwrite run is in flight.
    fn staging_table_name(&self) -> String {
        self.layout.staging_table()
    }

    /// The table the current-target-old is renamed to during the atomic swap.
    pub(crate) fn old_table_name(&self) -> String {
        match &self.layout.overwrite_run {
            // One per run, so two runs' swaps never rename onto one name.
            Some(_) => format!("{}_old", self.staging_table_name()),
            None => format!(
                "{}{}",
                self.config.table_name,
                faucet_core::idempotency::OVERWRITE_STAGING_OLD_SUFFIX
            ),
        }
    }

    /// Every table in the database named like a tokened staging of the target.
    async fn tokened_staging_tables(&self) -> Result<Vec<String>, FaucetError> {
        let pattern = format!(
            "{}%",
            faucet_core::idempotency::like_literal(
                &faucet_core::idempotency::overwrite_staging_prefix(&self.config.table_name),
                '\\'
            )
        );
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT CAST(table_name AS CHAR) FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name LIKE ?",
        )
        .bind(pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("mysql overwrite: list staging tables: {e}")))?;
        Ok(names)
    }

    /// The table the data-write path targets. For `write_mode: overwrite` every
    /// write in this sink's lifetime lands in the staging table; otherwise the
    /// configured table.
    fn effective_table_name(&self) -> String {
        if self.config.write.is_overwrite() {
            self.staging_table_name()
        } else {
            self.config.table_name.clone()
        }
    }

    /// Read the target table's PRIMARY/UNIQUE indexes from
    /// `INFORMATION_SCHEMA.STATISTICS`, each as the full set of its column names.
    ///
    /// `STATISTICS` lists one row per index column; only non-unique-flag-zero
    /// rows (`NON_UNIQUE = 0`) are unique indexes (the PRIMARY KEY is reported as
    /// an index named `PRIMARY` and is also `NON_UNIQUE = 0`). Returns an empty
    /// `Vec` when the table does not exist or has no unique indexes — the caller
    /// decides what to do with an absent table.
    ///
    /// Thin I/O shim; the pure decision is [`key_matches_unique_index`].
    async fn read_unique_indexes(
        &self,
    ) -> Result<Vec<std::collections::BTreeSet<String>>, FaucetError> {
        // INFORMATION_SCHEMA string columns use a binary collation that sqlx
        // decodes as Vec<u8>; CAST to CHAR so they decode as String.
        let rows = sqlx::query(
            "SELECT CAST(INDEX_NAME AS CHAR) AS INDEX_NAME, \
                    CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME \
             FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() AND NON_UNIQUE = 0 \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        )
        .bind(&self.config.table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table indexes: {e}")))?;

        let mut by_index: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for row in &rows {
            let index_name: String = row.get("INDEX_NAME");
            let column_name: String = row.get("COLUMN_NAME");
            by_index.entry(index_name).or_default().insert(column_name);
        }
        Ok(by_index.into_values().collect())
    }

    /// Assert that the configured `key` exactly matches a PRIMARY/UNIQUE index on
    /// the target table, or fail with a clear typed [`FaucetError::Config`].
    ///
    /// **Table-absent behaviour:** if the table has no unique indexes — which is
    /// the case when it does not exist yet — the assertion is **skipped** with a
    /// warning, matching the rest of this sink which auto-discovers columns and
    /// lets the first write surface a missing-table error. (`current_schema`
    /// likewise returns `None` for an absent table.) The check is a guard against
    /// a *mismatched* index on an existing table, not a table-existence preflight.
    async fn assert_key_is_unique_index(&self) -> Result<(), FaucetError> {
        let unique_indexes = self.read_unique_indexes().await?;
        if unique_indexes.is_empty() {
            tracing::warn!(
                table = %self.config.table_name,
                "mysql sink: no PRIMARY/UNIQUE index found on target table (it may not exist \
                 yet); skipping upsert key validation — the first write will surface a \
                 missing-table or missing-constraint error"
            );
            return Ok(());
        }
        if !key_matches_unique_index(&unique_indexes, &self.config.write.key) {
            let available: Vec<String> = unique_indexes
                .iter()
                .map(|idx| {
                    let mut cols: Vec<&str> = idx.iter().map(String::as_str).collect();
                    cols.sort_unstable();
                    format!("({})", cols.join(", "))
                })
                .collect();
            return Err(FaucetError::Config(format!(
                "mysql sink: write_mode {} requires `key` {:?} to exactly match a PRIMARY KEY or \
                 UNIQUE index on table '{}' — MySQL's `ON DUPLICATE KEY UPDATE` resolves on the \
                 table's real unique indexes, so an unmatched key would silently upsert on the \
                 wrong index. Existing unique indexes: {}",
                self.config.write.write_mode.as_str(),
                self.config.write.key,
                self.config.table_name,
                available.join(", "),
            )));
        }
        if self.config.write.write_mode == faucet_core::WriteMode::Upsert {
            let others = other_unique_indexes(&unique_indexes, &self.config.write.key);
            if !others.is_empty() {
                return Err(FaucetError::Config(format!(
                    "mysql sink: write_mode upsert on table '{}' needs `key` {:?} to be its only \
                     PRIMARY KEY / UNIQUE index — `ON DUPLICATE KEY UPDATE` fires on whichever \
                     unique index a row collides with, so a collision on {} would update a \
                     different row instead of inserting this one (#789 SQL-76)",
                    self.config.table_name,
                    self.config.write.key,
                    others.join(", "),
                )));
            }
        }
        Ok(())
    }

    /// The engine's executor over `conn`, binding binary columns per the
    /// configured `binary_encoding`.
    pub(crate) fn exec<'c>(&'c self, conn: &'c mut MySqlConnection) -> MysqlExec<'c> {
        MysqlExec {
            conn,
            sink: self,
            binary: self.config.binary_encoding,
        }
    }

    /// Core auto-map insert on the provided connection, upserting by the
    /// configured key (`ON DUPLICATE KEY UPDATE …`, per set of carried
    /// columns so an absent column keeps its stored value) when
    /// `conflict_key` is `Some`.
    ///
    /// The engine maps fields to the `INFORMATION_SCHEMA.COLUMNS` of the
    /// target case-insensitively (#789 SQL-50), refuses a record matching no
    /// column, and writes the union of carried columns (#146 H1) in
    /// sub-INSERTs under the 65535-placeholder cap (#146 H14). Values bind as
    /// native MySQL types (#78/#4); a string bound for a binary column is
    /// decoded per `binary`.
    pub(crate) async fn insert_auto_map_with_conflict(
        &self,
        conn: &mut MySqlConnection,
        records: &[Value],
        conflict_key: Option<&[String]>,
        binary: BinaryEncoding,
    ) -> Result<usize, FaucetError> {
        let mut exec = MysqlExec {
            conn,
            sink: self,
            binary,
        };
        self.engine
            .insert(
                &mut exec,
                &target(&self.effective_table_name()),
                records,
                conflict_key.is_some(),
            )
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((?, …), …)`, chunked at MySQL's
    /// 65535-placeholder limit. Runs inside the caller's transaction.
    pub(crate) async fn delete_by_keys(
        &self,
        conn: &mut MySqlConnection,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        let mut exec = self.exec(conn);
        self.engine
            .delete(&mut exec, &target(&self.config.table_name), deletes)
            .await
    }

    /// Read `(COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME)` for
    /// the target table, on the caller's connection.
    ///
    /// Distinct from [`read_columns`](Self::read_columns), which reads the
    /// coarse `DATA_TYPE` for schema-drift diffing: the cleanup temp table needs
    /// the *full* type spec plus charset/collation so its key columns compare
    /// against the destination's without a collation mismatch. It also runs on
    /// the caller's connection rather than `&self.pool`, so it participates in
    /// the cleanup transaction.
    ///
    /// `INFORMATION_SCHEMA` string columns use a binary collation that sqlx
    /// decodes as `Vec<u8>`; each is `CAST … AS CHAR` so it decodes as `String`.
    /// An empty result means the table does not exist (or has no columns).
    async fn read_cleanup_columns(
        &self,
        conn: &mut MySqlConnection,
    ) -> Result<Vec<(String, String, Option<String>, Option<String>)>, FaucetError> {
        let rows = sqlx::query(
            "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
                    CAST(COLUMN_TYPE AS CHAR) AS COLUMN_TYPE, \
                    CAST(CHARACTER_SET_NAME AS CHAR) AS CHARACTER_SET_NAME, \
                    CAST(COLLATION_NAME AS CHAR) AS COLLATION_NAME \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION",
        )
        .bind(&self.config.table_name)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| FaucetError::Sink(format!("cleanup: column query failed: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("COLUMN_NAME"),
                    row.get::<String, _>("COLUMN_TYPE"),
                    row.get::<Option<String>, _>("CHARACTER_SET_NAME"),
                    row.get::<Option<String>, _>("COLLATION_NAME"),
                )
            })
            .collect())
    }

    /// Read the live destination columns as `(name, data_type, nullable)`
    /// tuples, in declared order, from `INFORMATION_SCHEMA.COLUMNS`. An empty
    /// vector means the table does not exist (or has no columns).
    ///
    /// Shared by `current_schema` and the schema evolution — the latter needs
    /// the current set to make `ADD COLUMN` idempotent (MySQL lacks
    /// `ADD COLUMN IF NOT EXISTS`).
    async fn read_columns(&self) -> Result<Vec<(String, String, bool)>, FaucetError> {
        // MySQL's INFORMATION_SCHEMA string columns are typed as a binary/blob
        // collation, which sqlx decodes as `Vec<u8>` rather than `String`; cast
        // each to CHAR so they decode as `String`.
        let rows = sqlx::query(
            "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
                    CAST(DATA_TYPE AS CHAR) AS DATA_TYPE, \
                    CAST(IS_NULLABLE AS CHAR) AS IS_NULLABLE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION",
        )
        .bind(&self.config.table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("COLUMN_NAME"),
                    // DATA_TYPE is already lowercase per the SQL standard, but
                    // normalize defensively.
                    row.get::<String, _>("DATA_TYPE").to_ascii_lowercase(),
                    row.get::<String, _>("IS_NULLABLE")
                        .eq_ignore_ascii_case("YES"),
                )
            })
            .collect())
    }

    /// Create the commit-token watermark table if it does not yet exist.
    ///
    /// The table holds one row per pipeline scope (state key). MySQL requires a
    /// fixed-length column as the primary key, so `scope` is `VARCHAR(255)`; the
    /// `token` column is `TEXT` because a `#291` commit token embeds the page's
    /// resume bookmark (`{20-digit seq}#{bookmark-json}`) and easily exceeds the
    /// old `VARCHAR(32)`, which truncated/rejected every bookmark-bearing page
    /// and broke exactly-once delivery (audit #321 C3).
    pub(crate) async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        sqlx::query(&Mysql.commit_token_table_sql())
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL commit-table create failed: {e}")))?;
        Ok(())
    }

    async fn acquire(&self) -> Result<sqlx::pool::PoolConnection<sqlx::MySql>, FaucetError> {
        self.pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL pool acquire failed: {e}")))
    }
}

#[async_trait]
impl SqlDriver for MysqlDriver {
    type Dialect = Mysql;
    type Conn<'a> = MysqlConn<'a>;

    fn engine(&self) -> &SqlSinkEngine<Mysql> {
        &self.engine
    }

    fn layout(&self) -> &SqlLayout {
        &self.layout
    }

    fn connector_name(&self) -> &'static str {
        "mysql"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?table={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.table_name
        )
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database is
    /// reachable and the credentials are accepted without writing anything.
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
                    "check connection_url / credentials / that the database is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check connection_url / credentials / that the database is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    /// Every operation but the cleanup is bounded by `write_timeout_secs`
    /// (#789 SQL-109).
    fn guard<'a, T: Send + 'a>(
        &'a self,
        what: &'static str,
        fut: SqlFuture<'a, T>,
    ) -> SqlFuture<'a, T> {
        match what {
            label::CLEANUP_SCOPE => fut,
            _ => Box::pin(bounded(self.config.write_timeout_secs, what, fut)),
        }
    }

    fn target(&self, table: &str) -> TableTarget {
        target(table)
    }

    async fn begin(&self) -> Result<MysqlConn<'_>, FaucetError> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction begin failed: {e}")))?;
        Ok(MysqlConn::Tx(tx, self))
    }

    async fn commit(&self, conn: MysqlConn<'_>) -> Result<(), FaucetError> {
        match conn {
            MysqlConn::Tx(tx, _) => tx
                .commit()
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL transaction commit failed: {e}"))),
            MysqlConn::Pooled(..) => Ok(()),
        }
    }

    async fn connection(&self) -> Result<MysqlConn<'_>, FaucetError> {
        let conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL connection acquire failed: {e}")))?;
        Ok(MysqlConn::Pooled(conn, self))
    }

    async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        MysqlDriver::table_exists(self, table).await
    }

    async fn overwrite_staging_tables(&self) -> Result<Vec<String>, FaucetError> {
        self.tokened_staging_tables().await
    }

    async fn ddl(&self, sql: &str, what: &'static str) -> Result<(), FaucetError> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL {what} failed: {e}")))?;
        Ok(())
    }

    /// The scope's token, under its current key and then its pre-1.13 key.
    async fn read_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let sql = Mysql.commit_token_select_sql();
        for key in std::iter::once(scope_key(scope)).chain(legacy_scope_key(scope)) {
            let row = sqlx::query(&sql)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL token read failed: {e}")))?;
            if let Some(r) = row {
                return Ok(Some(r.get::<String, _>(0)));
            }
        }
        Ok(None)
    }

    /// The token under the scope's fitted key, retiring its pre-1.13 key in
    /// the same transaction.
    async fn write_token(
        &self,
        conn: &mut MysqlConn<'_>,
        scope: &str,
        token: &str,
    ) -> Result<(), FaucetError> {
        self.engine
            .write_token(conn, &scope_key(scope), token)
            .await?;
        if let Some(legacy) = legacy_scope_key(scope) {
            let raw: &mut MySqlConnection = match conn {
                MysqlConn::Tx(tx, _) => tx,
                MysqlConn::Pooled(c, _) => c,
            };
            sqlx::query(&Mysql.commit_token_delete_sql())
                .bind(legacy)
                .execute(raw)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL legacy token delete failed: {e}")))?;
        }
        Ok(())
    }

    /// MySQL commits the open transaction on any `CREATE TABLE`, so the
    /// rollback journal is created before a keyed write begins.
    async fn prepare_keyed_write(&self) -> Result<(), FaucetError> {
        if self.config.write.journals() {
            self.ensure_journal().await?;
        }
        Ok(())
    }

    /// The staging table and the `<table>__faucet_ovw_old` a swap interrupted
    /// mid-`RENAME` leaves.
    async fn drop_staging(&self, stale: bool) -> Result<(), FaucetError> {
        let mut conn = self.acquire().await?;
        for stmt in [
            format!(
                "DROP TABLE IF EXISTS {}",
                quote_ident_mysql(&self.staging_table_name())
            ),
            format!(
                "DROP TABLE IF EXISTS {}",
                quote_ident_mysql(&self.old_table_name())
            ),
        ] {
            sqlx::query(&stmt).execute(&mut *conn).await.map_err(|e| {
                FaucetError::Sink(if stale {
                    format!(
                        "mysql overwrite: prepare staging from '{}' (does the table exist?): {e}",
                        self.config.table_name
                    )
                } else {
                    format!("mysql overwrite: drop staging: {e}")
                })
            })?;
        }
        Ok(())
    }

    /// The `<table>__faucet_ovw` staging table, or the `<table>__faucet_ovw_old`
    /// a swap interrupted mid-`RENAME` leaves.
    async fn staging_exists(&self) -> Result<bool, FaucetError> {
        Ok(self.table_exists(&self.staging_table_name()).await?
            || self.table_exists(&self.old_table_name()).await?)
    }

    /// `CREATE TABLE staging LIKE target` (columns AND indexes), after
    /// refusing a target other tables reference by foreign key.
    async fn create_staging(&self) -> Result<(), FaucetError> {
        let mut conn = self.acquire().await?;
        let deps = table_dependents(&mut conn, &self.config.table_name).await?;
        if !deps.referenced_by.is_empty() {
            return Err(referenced_overwrite_error(
                &self.config.table_name,
                &deps.referenced_by,
            ));
        }
        sqlx::query(&format!(
            "CREATE TABLE {} LIKE {}",
            quote_ident_mysql(&self.staging_table_name()),
            quote_ident_mysql(&self.config.table_name)
        ))
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "mysql overwrite: prepare staging from '{}' (does the table exist?): {e}",
                self.config.table_name
            ))
        })?;
        Ok(())
    }

    /// First run: staging holds everything; rename it into place.
    async fn publish_staging(&self) -> Result<(), FaucetError> {
        sqlx::query(&format!(
            "RENAME TABLE {} TO {}",
            quote_ident_mysql(&self.staging_table_name()),
            quote_ident_mysql(&self.config.table_name)
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("mysql overwrite: publish staging: {e}")))?;
        Ok(())
    }

    /// Atomically replace the destination via `RENAME TABLE` — MySQL performs a
    /// multi-table rename atomically and, unlike `TRUNCATE`/`DROP`, without an
    /// implicit commit that could strand a half-done swap. `target → old,
    /// staging → target` publishes the freshly-loaded staging table as the new
    /// target in one step; the old table is then dropped.
    async fn swap_staging(&self) -> Result<(), FaucetError> {
        let staging = quote_ident_mysql(&self.staging_table_name());
        let old = quote_ident_mysql(&self.old_table_name());
        let target = quote_ident_mysql(&self.config.table_name);
        let mut conn = self.acquire().await?;
        // A target with foreign keys or triggers keeps its definition: its
        // rows are replaced in place instead of renaming staging over it,
        // which would publish a table without them (#789 SQL-108).
        let deps = table_dependents(&mut conn, &self.config.table_name).await?;
        if !deps.referenced_by.is_empty() {
            return Err(referenced_overwrite_error(
                &self.config.table_name,
                &deps.referenced_by,
            ));
        }
        if !deps.pinned.is_empty() {
            if self.config.write.keeps_previous() {
                let prev = self.previous_table_name();
                let q = quote_ident_mysql(&prev);
                for stmt in [
                    format!("DROP TABLE IF EXISTS {q}"),
                    format!("CREATE TABLE {q} LIKE {target}"),
                ] {
                    sqlx::query(&stmt).execute(&mut *conn).await.map_err(|e| {
                        FaucetError::Sink(format!("mysql overwrite: keep previous copy: {e}"))
                    })?;
                }
                replace_rows_in_place(&mut conn, &prev, &self.config.table_name).await?;
            }
            replace_rows_in_place(
                &mut conn,
                &self.config.table_name,
                &self.staging_table_name(),
            )
            .await?;
            sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
                .execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("mysql overwrite: drop staging: {e}")))?;
            return Ok(());
        }
        // `rollback.keep_previous`: the replaced table is kept as
        // `<table>__faucet_prev` (the rename is already the atomic unit, so
        // no copy is needed) instead of being dropped (#706).
        if self.config.write.keeps_previous() {
            let prev = quote_ident_mysql(&self.previous_table_name());
            sqlx::query(&format!("DROP TABLE IF EXISTS {prev}"))
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("mysql overwrite: drop older previous: {e}"))
                })?;
            sqlx::query(&format!(
                "RENAME TABLE {target} TO {prev}, {staging} TO {target}"
            ))
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("mysql overwrite swap (RENAME) failed: {e}")))?;
            return Ok(());
        }
        sqlx::query(&format!(
            "RENAME TABLE {target} TO {old}, {staging} TO {target}"
        ))
        .execute(&mut *conn)
        .await
        .map_err(|e| FaucetError::Sink(format!("mysql overwrite swap (RENAME) failed: {e}")))?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {old}"))
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("mysql overwrite: drop old table: {e}")))?;
        Ok(())
    }

    /// Read the live destination schema from `INFORMATION_SCHEMA.COLUMNS` as an
    /// `infer_schema`-shaped object (`{"type":"object","properties":{…}}`), or
    /// `None` when the target table does not exist yet (issue #194).
    ///
    /// The MySQL database is implicit (`DATABASE()`), so there is no schema
    /// qualifier to thread. `DATA_TYPE` / `IS_NULLABLE` round-trip through
    /// `mysql_data_type_to_json_schema`.
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
        // JSON-column mode stores each record whole; the physical columns are
        // not the record's fields, so there is nothing to drift against
        // (#789 SQL-20).
        if !matches!(self.config.column_mapping, MysqlColumnMapping::AutoMap) {
            return Ok(None);
        }
        let columns = self.read_columns().await?;
        if columns.is_empty() {
            return Ok(None); // table does not exist yet
        }

        let mut props = serde_json::Map::new();
        for (name, data_type, nullable) in columns {
            props.insert(name, mysql_data_type_to_json_schema(&data_type, nullable));
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    fn evolution(&self) -> Option<&dyn SqlEvolution> {
        Some(self)
    }

    /// A binary value `binary_encoding` refuses fails only its own row.
    fn split_page(
        &self,
        _target: &TableTarget,
        records: &[Value],
        columns: &[TableColumn],
        require_match: bool,
    ) -> SplitPage {
        split_columns(records, columns, self.config.binary_encoding, require_match)
    }

    fn splits_keyed_pages(&self) -> bool {
        true
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
impl SqlEvolution for MysqlDriver {
    /// MySQL has no `ADD COLUMN IF NOT EXISTS` (pre-8.0.x), so the current
    /// column set is read first and an addition already present is skipped.
    async fn existing_columns(
        &self,
    ) -> Result<Option<std::collections::HashSet<String>>, FaucetError> {
        Ok(Some(
            self.read_columns()
                .await?
                .into_iter()
                .map(|(n, _, _)| n)
                .collect(),
        ))
    }

    /// Apply an additive schema evolution (new columns, lossless widenings,
    /// nullability relaxations) to the destination table (issue #194).
    /// Widenings use `MODIFY COLUMN` (re-running the same MODIFY is a no-op);
    /// nullability relaxations re-emit the column as its current definition
    /// with an explicit `NULL`.
    async fn apply_evolution(&self, steps: Vec<EvolutionStep>) -> Result<(), FaucetError> {
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL evolve acquire failed: {e}")))?;

        for step in steps {
            match step {
                EvolutionStep::Add { column, base } => {
                    sqlx::query(&build_add_column_sql(&table_ref, &column, base))
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| {
                            FaucetError::Sink(format!("MySQL ADD COLUMN {column} failed: {e}"))
                        })?;
                }
                EvolutionStep::Widen {
                    column,
                    type_name: new_type,
                    ..
                } => {
                    // An integer column that starts receiving fractions becomes
                    // an exact DECIMAL, never DOUBLE (which would round every
                    // stored value above 2^53), and MODIFY re-emits the rest of
                    // the definition so NOT NULL, the default and the comment
                    // survive (#789 SQL-78).
                    let sql = match self.column_definition(&mut conn, &column).await? {
                        Some((def, nullable)) => {
                            if def.extra.to_ascii_lowercase().contains("auto_increment") {
                                return Err(FaucetError::SchemaDrift {
                                    columns: vec![column.clone()],
                                    message: format!(
                                        "mysql: column {column} is AUTO_INCREMENT and cannot widen to {new_type}"
                                    ),
                                });
                            }
                            redefine_column_sql(&table_ref, &column, &def, Some(new_type), nullable)
                        }
                        None => None,
                    };
                    let Some(sql) = sql else { continue };
                    sqlx::query(&sql).execute(&mut *conn).await.map_err(|e| {
                        FaucetError::Sink(format!("MySQL MODIFY COLUMN {column} failed: {e}"))
                    })?;
                }
                EvolutionStep::Relax { column: col } => {
                    // MySQL's MODIFY COLUMN replaces the whole definition, so
                    // re-emit the column exactly as it is — type, charset,
                    // collation, default, AUTO_INCREMENT, ON UPDATE, comment —
                    // with only NOT NULL dropped (#789 SQL-33).
                    let Some((def, _)) = self.column_definition(&mut conn, &col).await? else {
                        continue;
                    };
                    let Some(sql) = relax_column_sql(&table_ref, &col, &def) else {
                        tracing::warn!(column = %col, "mysql: not relaxing NOT NULL on a generated column");
                        continue;
                    };
                    sqlx::query(&sql).execute(&mut *conn).await.map_err(|e| {
                        FaucetError::Sink(format!("MySQL DROP NOT NULL {col} failed: {e}"))
                    })?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RollbackDriver for MysqlDriver {
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
            .call_once(
                "forget_run",
                bounded(
                    self.config.write_timeout_secs,
                    "forget_run",
                    self.forget_run_impl(run_id),
                ),
            )
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
                bounded(
                    self.config.write_timeout_secs,
                    "rewind_commit_token",
                    self.rewind_commit_token_impl(scope, token),
                ),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_writes_time_out_and_pass_results_through() {
        let ok = bounded(5, "write", async { Ok::<_, FaucetError>(7) }).await;
        assert_eq!(ok.unwrap(), 7);
        let unbounded = bounded(0, "write", async { Ok::<_, FaucetError>(8) }).await;
        assert_eq!(unbounded.unwrap(), 8);
        let err = bounded(
            1,
            "flush",
            std::future::pending::<Result<(), FaucetError>>(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("mysql flush timed out after 1s"),
            "{err}"
        );
    }

    #[test]
    fn fk_violation_sql_checks_every_key_column() {
        let fk = ForeignKey {
            name: "fk_parent".into(),
            referenced_schema: "app".into(),
            referenced_table: "cat".into(),
            local: true,
            columns: vec![
                ("parent_id".into(), "id".into()),
                ("org".into(), "org".into()),
            ],
        };
        assert_eq!(
            fk_violation_sql("cat", &fk),
            "SELECT COUNT(*) FROM `cat` c WHERE c.`parent_id` IS NOT NULL AND c.`org` IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM `app`.`cat` p WHERE p.`id` = c.`parent_id` AND p.`org` = c.`org`)"
        );
        assert!(fk.references("cat"));
        let elsewhere = ForeignKey {
            local: false,
            ..fk.clone()
        };
        assert!(
            !elsewhere.references("cat"),
            "a same-named table in another database"
        );
        assert!(!fk.references("dog"));
    }

    #[test]
    fn defaults_render_per_server_flavour() {
        let mysql57 = MysqlColumnDef {
            column_type: "timestamp".into(),
            ..Default::default()
        };
        assert_eq!(
            render_default("CURRENT_TIMESTAMP", &mysql57),
            "CURRENT_TIMESTAMP"
        );
        let maria = MysqlColumnDef {
            column_type: "varchar(10)".into(),
            mariadb: true,
            ..Default::default()
        };
        assert_eq!(render_default("'n/a'", &maria), "'n/a'");
        assert_eq!(
            render_default("current_timestamp()", &maria),
            "current_timestamp()"
        );
        let bit = MysqlColumnDef {
            column_type: "bit(1)".into(),
            ..Default::default()
        };
        assert_eq!(render_default("b'0'", &bit), "b'0'");
        assert_eq!(render_default("b'2'", &bit), "'b''2'''");
        let text = MysqlColumnDef {
            column_type: "varchar(10)".into(),
            ..Default::default()
        };
        assert_eq!(render_default("b'0'", &text), "'b''0'''");
        assert_eq!(render_default(r"a\b", &text), r"'a\\b'");
    }

    #[test]
    fn relax_column_sql_keeps_the_definition() {
        let def = MysqlColumnDef {
            column_type: "decimal(12,2)".into(),
            charset: None,
            collation: None,
            default: Some("0.00".into()),
            extra: String::new(),
            comment: "it's".into(),
            mariadb: false,
        };
        assert_eq!(
            relax_column_sql("`t`", "amount", &def).as_deref(),
            Some(
                "ALTER TABLE `t` MODIFY COLUMN `amount` decimal(12,2) NULL DEFAULT '0.00' COMMENT 'it''s'"
            )
        );
        let def = MysqlColumnDef {
            column_type: "varchar(20)".into(),
            charset: Some("utf8mb4".into()),
            collation: Some("utf8mb4_bin".into()),
            default: Some("uuid()".into()),
            extra: "DEFAULT_GENERATED".into(),
            comment: String::new(),
            mariadb: false,
        };
        assert_eq!(
            relax_column_sql("`t`", "c", &def).as_deref(),
            Some(
                "ALTER TABLE `t` MODIFY COLUMN `c` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT (uuid())"
            )
        );
        let def = MysqlColumnDef {
            column_type: "datetime(3)".into(),
            default: Some("CURRENT_TIMESTAMP(3)".into()),
            extra: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)".into(),
            ..Default::default()
        };
        assert_eq!(
            relax_column_sql("`t`", "d", &def).as_deref(),
            Some(
                "ALTER TABLE `t` MODIFY COLUMN `d` datetime(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"
            )
        );
        let def = MysqlColumnDef {
            column_type: "bigint".into(),
            extra: "auto_increment".into(),
            ..Default::default()
        };
        assert_eq!(
            relax_column_sql("`t`", "id", &def).as_deref(),
            Some("ALTER TABLE `t` MODIFY COLUMN `id` bigint NULL AUTO_INCREMENT")
        );
        let def = MysqlColumnDef {
            column_type: "int".into(),
            extra: "VIRTUAL GENERATED".into(),
            ..Default::default()
        };
        assert_eq!(relax_column_sql("`t`", "g", &def), None);
        let def = MysqlColumnDef {
            column_type: "varchar(5)".into(),
            default: Some("a\\b".into()),
            ..Default::default()
        };
        assert_eq!(
            relax_column_sql("`t`", "s", &def).as_deref(),
            Some("ALTER TABLE `t` MODIFY COLUMN `s` varchar(5) NULL DEFAULT 'a\\\\b'")
        );
    }

    #[test]
    fn binary_columns_take_base64_as_bytes() {
        assert!(is_binary_type("varbinary"));
        assert!(is_binary_type("longblob"));
        assert!(!is_binary_type("text"));
        // A full COLUMN_TYPE, as the cleanup key table is typed from.
        assert!(is_binary_type("varbinary(32)"));
        assert!(is_binary_type("BINARY(16)"));
        assert!(!is_binary_type("varchar(32)"));
        assert_eq!(
            statement_context(StatementKind::Delete),
            "MySQL delete failed"
        );
        assert_eq!(
            statement_context(StatementKind::CleanupLoad),
            "cleanup: loading keys failed"
        );
    }

    // dataset_uri test is skipped: MysqlSink::new() requires a live pool
    // (connects to MySQL in new()), and no offline constructor exists.

    #[test]
    fn commit_token_table_is_the_shared_constant() {
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            "_faucet_commit_token"
        );
    }

    #[test]
    fn quote_ident_mysql_simple() {
        assert_eq!(quote_ident_mysql("my_table"), "`my_table`");
    }

    #[test]
    fn quote_ident_mysql_with_backtick() {
        assert_eq!(quote_ident_mysql("has`tick"), "`has``tick`");
    }

    #[test]
    fn quote_ident_mysql_empty() {
        assert_eq!(quote_ident_mysql(""), "``");
    }

    #[test]
    fn quote_ident_mysql_special_chars() {
        assert_eq!(quote_ident_mysql("table; DROP"), "`table; DROP`");
    }

    #[test]
    fn mysql_on_duplicate_clause() {
        let clause =
            on_duplicate_clause(&["id".to_string()], &["id".to_string(), "name".to_string()]);
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)");
    }

    #[test]
    fn mysql_on_duplicate_all_keys_self_assign() {
        let clause = on_duplicate_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `id` = `id`");
    }

    #[test]
    fn mysql_on_duplicate_composite_key_partial_update() {
        let clause = on_duplicate_clause(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
        );
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)");
    }

    #[test]
    fn mysql_add_column_ddl() {
        let sql = build_add_column_sql("`t`", "email", SqlBaseType::Text);
        assert_eq!(sql, "ALTER TABLE `t` ADD COLUMN `email` LONGTEXT");

        // A backtick in the column name is doubled (SQL-injection safety).
        let sql = build_add_column_sql("`t`", "ev`il", SqlBaseType::Integer);
        assert_eq!(sql, "ALTER TABLE `t` ADD COLUMN `ev``il` BIGINT");
    }

    #[test]
    fn widening_redefines_the_column_with_an_exact_type() {
        assert_eq!(
            widened_type(Some(SqlBaseType::Integer), SqlBaseType::Double),
            "DECIMAL(65,30)"
        );
        assert_eq!(widened_type(None, SqlBaseType::Double), "DOUBLE");
        let def = MysqlColumnDef {
            column_type: "bigint".into(),
            default: Some("0".into()),
            comment: "cents".into(),
            ..Default::default()
        };
        assert_eq!(
            redefine_column_sql("`t`", "amount", &def, Some("DECIMAL(65,30)"), false).as_deref(),
            Some(
                "ALTER TABLE `t` MODIFY COLUMN `amount` DECIMAL(65,30) NOT NULL DEFAULT '0' COMMENT 'cents'"
            )
        );
        let text = MysqlColumnDef {
            column_type: "varchar(10)".into(),
            charset: Some("utf8mb4".into()),
            ..Default::default()
        };
        assert_eq!(
            redefine_column_sql("`t`", "s", &text, Some("LONGTEXT"), true).as_deref(),
            Some("ALTER TABLE `t` MODIFY COLUMN `s` LONGTEXT NULL")
        );
    }

    #[test]
    fn other_unique_indexes_lists_every_index_but_the_key() {
        let idx = |cols: &[&str]| -> std::collections::BTreeSet<String> {
            cols.iter().map(|c| c.to_string()).collect()
        };
        let indexes = vec![idx(&["id"]), idx(&["email"]), idx(&["a", "b"])];
        assert_eq!(
            other_unique_indexes(&indexes, &keyvec(&["id"])),
            vec!["(email)".to_string(), "(a, b)".to_string()]
        );
        assert!(other_unique_indexes(&[idx(&["id"])], &keyvec(&["id"])).is_empty());
    }

    #[test]
    fn unmatched_records_split_off_case_insensitively() {
        let cols = vec![
            ("userId".to_string(), "int".to_string()),
            ("kind".to_string(), "varchar".to_string()),
        ];
        let obj = serde_json::json!({"USERID": 1, "Kind": "a"});
        let matched = match_columns(&cols, obj.as_object().unwrap());
        assert_eq!(matched.len(), 2);
        let (outcomes, writable, _) = split_writable(
            &[
                obj.clone(),
                serde_json::json!({"other": 1}),
                serde_json::json!(1),
            ],
            &cols,
            BinaryEncoding::Auto,
            true,
        );
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok());
        let err = outcomes[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("record 1 has no field matching"), "{err}");
        assert_eq!(writable.len(), 2);
    }

    #[test]
    fn a_binary_value_the_encoding_refuses_fails_only_its_row() {
        let cols = vec![
            ("id".to_string(), "int".to_string()),
            ("Blob".to_string(), "varbinary".to_string()),
        ];
        let page = [
            serde_json::json!({"id": 1, "blob": "SGVsbG8="}),
            serde_json::json!({"id": 2, "blob": "plain token!"}),
            serde_json::json!({"id": 3, "blob": 7}),
        ];
        let (outcomes, writable, positions) =
            split_writable(&page, &cols, BinaryEncoding::Base64, false);
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok());
        let err = outcomes[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("row 1: column Blob"), "{err}");
        assert_eq!(writable.len(), 2);
        assert_eq!(positions, vec![0, 2]);
        let (outcomes, _, _) = split_writable(&page, &cols, BinaryEncoding::Text, false);
        assert!(outcomes.iter().all(Result::is_ok));
    }

    #[test]
    fn create_table_sql_for_a_keyed_write_bounds_key_text_and_adds_a_primary_key() {
        let key = vec!["sku".to_string()];
        let cols = faucet_core::plan_keyed_columns(
            &[serde_json::json!({ "sku": "a", "note": "long text" })],
            &key,
        )
        .expect("a plan");
        let sql = build_create_table_sql("t", &cols, None, &key);
        assert!(sql.contains("`sku` VARCHAR(191)"), "{sql}");
        assert!(sql.contains("`note` LONGTEXT"), "{sql}");
        assert!(sql.ends_with(", PRIMARY KEY (`sku`))"), "{sql}");
        assert_eq!(mysql_key_keyword(SqlBaseType::Integer), "BIGINT");
    }

    #[test]
    fn mysql_keyword_mapping() {
        assert_eq!(mysql_keyword(SqlBaseType::Integer), "BIGINT");
        assert_eq!(mysql_keyword(SqlBaseType::Double), "DOUBLE");
        assert_eq!(mysql_keyword(SqlBaseType::Boolean), "TINYINT(1)");
        assert_eq!(mysql_keyword(SqlBaseType::Text), "LONGTEXT");
        assert_eq!(mysql_keyword(SqlBaseType::Json), "JSON");
    }

    fn idx(cols: &[&str]) -> std::collections::BTreeSet<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    fn keyvec(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn key_matches_single_column_index() {
        let indexes = vec![idx(&["id"])];
        assert!(key_matches_unique_index(&indexes, &keyvec(&["id"])));
    }

    #[test]
    fn key_matches_composite_index_reordered() {
        // A UNIQUE index on (a, b) matches a configured key of [b, a].
        let indexes = vec![idx(&["a", "b"])];
        assert!(key_matches_unique_index(&indexes, &keyvec(&["b", "a"])));
    }

    #[test]
    fn key_does_not_match_subset_of_index() {
        // Index is (a, b); key [a] is a prefix/subset — not an exact match.
        let indexes = vec![idx(&["a", "b"])];
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["a"])));
    }

    #[test]
    fn key_does_not_match_superset_of_index() {
        // Index is (a); key [a, b] is a superset — not an exact match.
        let indexes = vec![idx(&["a"])];
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["a", "b"])));
    }

    #[test]
    fn key_does_not_match_disjoint_index() {
        let indexes = vec![idx(&["id"])];
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["other"])));
    }

    #[test]
    fn key_matches_one_of_multiple_indexes() {
        // Table has a PRIMARY (id) and a UNIQUE (email); key on email matches.
        let indexes = vec![idx(&["id"]), idx(&["email"])];
        assert!(key_matches_unique_index(&indexes, &keyvec(&["email"])));
        assert!(key_matches_unique_index(&indexes, &keyvec(&["id"])));
        // A key on neither matches.
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["name"])));
        // A key spanning two distinct indexes is not itself an index.
        assert!(!key_matches_unique_index(
            &indexes,
            &keyvec(&["id", "email"])
        ));
    }

    #[test]
    fn key_does_not_match_empty_index_set() {
        // No unique indexes (e.g. table absent / no constraints) → never matches.
        let indexes: Vec<std::collections::BTreeSet<String>> = vec![];
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["id"])));
    }

    #[test]
    fn empty_key_never_matches() {
        let indexes = vec![idx(&["id"])];
        assert!(!key_matches_unique_index(&indexes, &keyvec(&[])));
        // Even against an (impossible) empty index, an empty key is rejected.
        let empty_idx = vec![idx(&[])];
        assert!(!key_matches_unique_index(&empty_idx, &keyvec(&[])));
    }

    #[test]
    fn key_matches_composite_index_among_several() {
        let indexes = vec![idx(&["id"]), idx(&["tenant", "slug"])];
        assert!(key_matches_unique_index(
            &indexes,
            &keyvec(&["slug", "tenant"])
        ));
        assert!(!key_matches_unique_index(&indexes, &keyvec(&["tenant"])));
    }

    // ---------------------------------------------------------------------
    // Scoped cleanup (#478) — SQL generation and column validation
    // ---------------------------------------------------------------------

    #[test]
    fn cleanup_temp_column_mirrors_type_charset_and_collation() {
        // A string key column carries its charset + collation, or comparing it
        // against the destination raises "Illegal mix of collations".
        assert_eq!(
            cleanup_temp_column_def("slug", "varchar(255)", Some("utf8mb4"), Some("utf8mb4_bin")),
            "`slug` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );
        // A numeric column has neither (INFORMATION_SCHEMA reports NULL).
        assert_eq!(
            cleanup_temp_column_def("id", "bigint unsigned", None, None),
            "`id` bigint unsigned"
        );
    }

    #[test]
    fn cleanup_temp_column_falls_back_for_an_unusable_type() {
        // MySQL has no typeless column, so an unrecognisable spec becomes
        // LONGTEXT rather than being pasted into DDL.
        assert_eq!(
            cleanup_temp_column_def("id", "int); DROP TABLE x", None, None),
            "`id` LONGTEXT"
        );
        assert_eq!(
            cleanup_temp_column_def("id", "", None, None),
            "`id` LONGTEXT"
        );
        // A hostile charset/collation name is dropped, not emitted.
        assert_eq!(
            cleanup_temp_column_def("s", "text", Some("utf8mb4; DROP"), Some("x'y")),
            "`s` text"
        );
    }

    #[test]
    fn safe_type_spec_accepts_real_types_and_rejects_the_rest() {
        assert_eq!(safe_type_spec("varchar(255)"), Some("varchar(255)"));
        assert_eq!(safe_type_spec("decimal(10,2)"), Some("decimal(10,2)"));
        assert_eq!(safe_type_spec("enum('a','b')"), Some("enum('a','b')"));
        assert_eq!(safe_type_spec("  int  "), Some("int"));
        assert_eq!(safe_type_spec(""), None);
        assert_eq!(safe_type_spec("int`"), None);
        assert_eq!(safe_type_spec("int;"), None);
    }

    #[test]
    fn cleanup_temp_table_ddl() {
        let sql = build_cleanup_temp_table_sql(&[
            "`contact_id` int".to_string(),
            "`id` bigint".to_string(),
        ]);
        assert_eq!(
            sql,
            "CREATE TEMPORARY TABLE `faucet_cleanup_keys` (`contact_id` int, `id` bigint)"
        );
    }

    #[test]
    fn cleanup_insert_emits_one_tuple_per_row() {
        let sql = build_cleanup_insert_sql(&keyvec(&["a", "b"]), 3);
        assert_eq!(
            sql,
            "INSERT INTO `faucet_cleanup_keys` (`a`, `b`) VALUES (?, ?), (?, ?), (?, ?)"
        );
    }

    #[test]
    fn cleanup_delete_ands_the_scope_and_excludes_written_keys() {
        let sql = build_cleanup_delete_sql("assoc", &keyvec(&["contact_id"]), &keyvec(&["id"]));
        assert_eq!(
            sql,
            "DELETE FROM `assoc` WHERE `assoc`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM `faucet_cleanup_keys` c \
             WHERE c.`id` = `assoc`.`id`)"
        );
    }

    #[test]
    fn cleanup_delete_composite_scope_and_key() {
        let sql = build_cleanup_delete_sql(
            "t",
            &keyvec(&["tenant", "contact_id"]),
            &keyvec(&["a", "b"]),
        );
        assert_eq!(
            sql,
            "DELETE FROM `t` WHERE `t`.`tenant` = ? AND `t`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM `faucet_cleanup_keys` c \
             WHERE c.`a` = `t`.`a` AND c.`b` = `t`.`b`)"
        );
    }

    #[test]
    fn cleanup_validation_names_a_missing_scope_column() {
        let existing: std::collections::HashSet<String> =
            keyvec(&["id", "name"]).into_iter().collect();
        let err =
            validate_cleanup_columns(&existing, &keyvec(&["contact_id"]), &keyvec(&["id"]), "t")
                .expect_err("unknown scope column must be refused");
        let msg = err.to_string();
        assert!(msg.contains("contact_id"), "{msg}");
        assert!(msg.contains("'t'"), "{msg}");
    }

    #[test]
    fn cleanup_validation_names_a_missing_key_column() {
        let existing: std::collections::HashSet<String> =
            keyvec(&["contact_id"]).into_iter().collect();
        let err =
            validate_cleanup_columns(&existing, &keyvec(&["contact_id"]), &keyvec(&["id"]), "t")
                .expect_err("unknown key column must be refused");
        assert!(err.to_string().contains("id"), "{err}");
    }

    #[test]
    fn cleanup_validation_passes_when_every_column_exists() {
        let existing: std::collections::HashSet<String> =
            keyvec(&["id", "contact_id"]).into_iter().collect();
        assert!(
            validate_cleanup_columns(&existing, &keyvec(&["contact_id"]), &keyvec(&["id"]), "t")
                .is_ok()
        );
    }

    #[test]
    fn mysql_data_type_round_trips_to_json_schema() {
        use serde_json::json;
        assert_eq!(
            mysql_data_type_to_json_schema("bigint", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            mysql_data_type_to_json_schema("int", false),
            json!({"type":"integer"})
        );
        // tinyint maps to integer (precision is not exposed by DATA_TYPE, so we
        // never guess boolean for safety).
        assert_eq!(
            mysql_data_type_to_json_schema("tinyint", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            mysql_data_type_to_json_schema("double", false),
            json!({"type":"number"})
        );
        assert_eq!(
            mysql_data_type_to_json_schema("decimal", false),
            json!({"type":"number"})
        );
        assert_eq!(
            mysql_data_type_to_json_schema("json", false),
            json!({"type":"object"})
        );
        assert_eq!(
            mysql_data_type_to_json_schema("varchar", false),
            json!({"type":"string"})
        );
        // Unknown types fall back to string; nullable widens the type array.
        assert_eq!(
            mysql_data_type_to_json_schema("datetime", true),
            json!({"type":["string","null"]})
        );
    }
}
