//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::{FaucetError, SchemaEvolution, SqlBaseType, json_schema_base_type};
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
    format!("`{}`", name.replace('`', "``"))
}

/// Transient table holding the key tuples this run wrote, joined against by the
/// scoped-cleanup DELETE (#478).
///
/// Always created in — and referenced through — the `temp` schema, so it can
/// never be confused with (or, on the defensive `DROP`, destroy) a real table of
/// the same name in the main database.
const CLEANUP_KEYS_TABLE: &str = "faucet_cleanup_keys";

/// Schema-qualified, quoted reference to [`CLEANUP_KEYS_TABLE`].
fn cleanup_keys_ref() -> String {
    format!("temp.{}", quote_ident_sqlite(CLEANUP_KEYS_TABLE))
}

/// A declared SQLite column type (`PRAGMA table_info.type`) that is safe to
/// re-emit verbatim in the cleanup temp table's DDL, or `None`.
///
/// The declared type comes from the database's own catalog, but SQLite lets a
/// column be declared with *arbitrary quoted text*, so it is filtered to the
/// shape real type specs take (`VARCHAR(255)`, `DOUBLE PRECISION`,
/// `DECIMAL(10, 2)`) rather than pasted in blind. `None` means "declare the temp
/// column without a type" — legal in SQLite, and only costs the column its type
/// affinity.
fn safe_type_spec(declared: &str) -> Option<&str> {
    let t = declared.trim();
    if t.is_empty() {
        return None;
    }
    t.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '(' | ')' | ',' | '.'))
        .then_some(t)
}

/// `CREATE TEMP TABLE temp.`faucet_cleanup_keys` (…)` — one column per key
/// column, mirroring the destination column's declared type so the join
/// comparison sees matching type affinities.
fn build_cleanup_temp_table_sql(key_types: &[(String, String)]) -> String {
    let cols = key_types
        .iter()
        .map(|(col, declared)| match safe_type_spec(declared) {
            Some(t) => format!("{} {t}", quote_ident_sqlite(col)),
            None => quote_ident_sqlite(col),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TEMP TABLE {} ({cols})", cleanup_keys_ref())
}

/// `INSERT INTO temp.`faucet_cleanup_keys` (…) VALUES (?, …), …` for `rows`
/// key tuples. The caller chunks `rows` to stay under SQLite's bind-variable
/// cap.
fn build_cleanup_insert_sql(key: &[String], rows: usize) -> String {
    let col_list = key
        .iter()
        .map(|k| quote_ident_sqlite(k))
        .collect::<Vec<_>>()
        .join(", ");
    let tuple = format!("({})", vec!["?"; key.len()].join(", "));
    let tuples = vec![tuple; rows].join(", ");
    format!(
        "INSERT INTO {} ({col_list}) VALUES {tuples}",
        cleanup_keys_ref()
    )
}

/// The cleanup DELETE: every row matching the scope (equality predicates,
/// AND-ed, one bind each) whose key is absent from the written-key table.
///
/// The target table is referenced by name rather than an alias — a single-table
/// `DELETE … AS alias` is a newer SQLite grammar, and the correlated reference
/// works identically through the table name.
fn build_cleanup_delete_sql(table: &str, scope_cols: &[String], key: &[String]) -> String {
    let t = quote_ident_sqlite(table);
    let scope_pred = scope_cols
        .iter()
        .map(|c| format!("{t}.{} = ?", quote_ident_sqlite(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let join_pred = key
        .iter()
        .map(|k| {
            let q = quote_ident_sqlite(k);
            format!("{t}.{q} = c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {t} WHERE {scope_pred} AND NOT EXISTS (SELECT 1 FROM {} c WHERE {join_pred})",
        cleanup_keys_ref()
    )
}

/// Check that every scope and key column exists on the destination table.
///
/// Fails with a clear message rather than letting SQLite reject an unknown
/// column mid-DELETE. The scope is written in *destination* terms, so a name
/// that isn't a real column is a config error worth naming.
fn validate_cleanup_columns(
    existing: &std::collections::HashSet<String>,
    scope_cols: &[String],
    key: &[String],
    table: &str,
) -> Result<(), FaucetError> {
    for col in scope_cols.iter().chain(key.iter()) {
        if !existing.contains(col) {
            return Err(FaucetError::Sink(format!(
                "cleanup: column '{col}' does not exist on table '{table}' — the \
                 completeness claim and `key` are in destination column terms"
            )));
        }
    }
    Ok(())
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

/// The `(column, value)` pairs a record carries, in table order. SQLite column
/// names are case-insensitive, so a field matches its column exactly first and
/// otherwise ignoring ASCII case (#789 SQL-70).
fn match_columns<'a>(
    columns: &'a [String],
    obj: &'a serde_json::Map<String, Value>,
) -> Vec<(&'a String, &'a Value)> {
    columns
        .iter()
        .filter_map(|col| {
            obj.get(col)
                .or_else(|| {
                    obj.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(col))
                        .map(|(_, v)| v)
                })
                .map(|v| (col, v))
        })
        .collect()
}

/// Record fields that match no column (exactly or ignoring ASCII case).
fn unmatched_fields<'a>(
    obj: &'a serde_json::Map<String, Value>,
    columns: &[String],
) -> Vec<&'a str> {
    obj.keys()
        .filter(|k| !columns.iter().any(|c| c.eq_ignore_ascii_case(k)))
        .map(String::as_str)
        .collect()
}

/// The per-row error for a record that matches no column of `table`.
fn no_matching_column_error(
    idx: usize,
    obj: &serde_json::Map<String, Value>,
    columns: &[String],
    table: &str,
) -> FaucetError {
    FaucetError::Sink(format!(
        "sqlite: record {idx} has no field matching a column of table '{table}' \
         (record fields: {:?}; table columns: {columns:?})",
        obj.keys().collect::<Vec<_>>()
    ))
}

/// Rows grouped by the exact set of table columns they carry, in first-seen
/// order: an upsert of one group never names a column its rows omit, so an
/// absent column keeps its stored value instead of becoming NULL (#789 SQL-69).
type RowCells<'a> = Vec<(&'a String, &'a Value)>;
type ColumnGroup<'r, 'a> = (Vec<String>, Vec<&'r RowCells<'a>>);

fn group_by_present_columns<'r, 'a>(
    columns: &[String],
    rows: &'r [RowCells<'a>],
) -> Vec<ColumnGroup<'r, 'a>> {
    let mut groups: Vec<ColumnGroup<'r, 'a>> = Vec::new();
    for row in rows {
        let present: Vec<String> = columns
            .iter()
            .filter(|c| row.iter().any(|(rc, _)| *rc == *c))
            .cloned()
            .collect();
        match groups.iter_mut().find(|(p, _)| *p == present) {
            Some((_, members)) => members.push(row),
            None => groups.push((present, vec![row])),
        }
    }
    groups
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
///
/// `IF NOT EXISTS` rather than probe-then-create: two matrix rows writing the
/// same table would otherwise race between the probe and the DDL.
fn build_create_table_sql(
    table: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
    key: &[String],
) -> String {
    let cols = match json_column {
        // JSON mode stores the whole record in one column, so the page's own
        // shape is irrelevant — the table is the same whatever arrives.
        Some(col) => format!(
            "{} INTEGER PRIMARY KEY AUTOINCREMENT, {} TEXT NOT NULL",
            quote_ident_sqlite("id"),
            quote_ident_sqlite(col)
        ),
        None => {
            let defs = faucet_core::render_columns(columns, quote_ident_sqlite, sqlite_keyword);
            // A keyed write needs a PRIMARY KEY for its ON CONFLICT target (#676).
            match faucet_core::render_primary_key(key, quote_ident_sqlite) {
                Some(pk) => format!("{defs}, {pk}"),
                None => defs,
            }
        }
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({cols})",
        quote_ident_sqlite(table)
    )
}

/// Map a [`SqlBaseType`] to the SQLite column-type keyword used when adding a
/// column during schema evolution (issue #194). SQLite uses dynamic typing
/// (type affinity), so these are advisory affinities rather than strict types:
/// `Boolean` maps to `INTEGER` (SQLite has no native boolean) and `Json` to
/// `TEXT` (JSON is stored as text).
fn sqlite_keyword(t: SqlBaseType) -> &'static str {
    match t {
        SqlBaseType::Integer => "INTEGER",
        SqlBaseType::Double => "REAL",
        SqlBaseType::Boolean => "INTEGER",
        SqlBaseType::Text => "TEXT",
        SqlBaseType::Json => "TEXT",
    }
}

/// `ALTER TABLE <table> ADD COLUMN "<col>" <kw>` — SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so [`SqliteSink::evolve_schema`] only emits this
/// for columns it has already verified are absent (idempotency by pre-check).
/// `table` is the unquoted table name; it is quoted here via [`quote_ident_sqlite`].
fn build_add_column_sql(table: &str, col: &str, t: SqlBaseType) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        quote_ident_sqlite(table),
        quote_ident_sqlite(col),
        sqlite_keyword(t)
    )
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
    let key_list = key
        .iter()
        .map(|k| quote_ident_sqlite(k))
        .collect::<Vec<_>>()
        .join(", ");
    let updates: Vec<String> = all_cols
        .iter()
        .filter(|c| !key.iter().any(|k| k == *c))
        .map(|c| format!("{q} = excluded.{q}", q = quote_ident_sqlite(c)))
        .collect();
    if updates.is_empty() {
        format!("ON CONFLICT({key_list}) DO NOTHING")
    } else {
        format!(
            "ON CONFLICT({key_list}) DO UPDATE SET {}",
            updates.join(", ")
        )
    }
}

/// How every write transaction starts. A deferred `BEGIN` takes a read lock and
/// upgrades on its first write; in WAL mode that upgrade fails at once with
/// `SQLITE_BUSY` when another connection committed in between, and the busy
/// timeout is not consulted — so two streams writing one database file failed
/// at random. `IMMEDIATE` takes the write lock up front, where the busy timeout
/// does apply.
pub(crate) const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";

/// A sink that writes JSON records to a SQLite table.
pub struct SqliteSink {
    pub(crate) config: SqliteSinkConfig,
    pub(crate) pool: SqlitePool,
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    /// Whether fields matching no column have been reported for this sink.
    unmatched_warned: std::sync::atomic::AtomicBool,
}

impl SqliteSink {
    /// Make sure the target table exists before the first write (#580).
    async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.config.create_table {
            if !self.table_exists(&self.config.table_name).await? {
                return Err(faucet_core::missing_target_error(
                    "sqlite sink",
                    &self.config.table_name,
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }

        let json_column = match &self.config.column_mapping {
            SqliteColumnMapping::AutoMap => None,
            SqliteColumnMapping::Json { column } => Some(column.as_str()),
        };
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
        let columns = match (json_column, planned) {
            (Some(_), _) => Vec::new(),
            (None, Some(c)) => c,
            (None, None) => return Ok(()),
        };
        // An overwrite writes to staging. On a first run (no target) nothing
        // else creates staging, so it is created here from the page (#676).
        let sql = build_create_table_sql(&self.effective_table(), &columns, json_column, key);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite CREATE TABLE failed: {e}")))?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

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

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            table_ready: std::sync::atomic::AtomicBool::new(false),
            unmatched_warned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The staging table name used while an overwrite run is in flight.
    fn staging_table(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }

    /// The table the data-write path targets. For `write_mode: overwrite` every
    /// write in this sink's lifetime lands in the staging table (created by
    /// [`begin_overwrite`], swapped into the real table by
    /// [`commit_overwrite`]); otherwise it is the configured table.
    fn effective_table(&self) -> String {
        if self.config.write.is_overwrite() {
            self.staging_table()
        } else {
            self.config.table_name.clone()
        }
    }

    /// Insert JSON-column records within an existing transaction, sub-chunking
    /// at SQLite's bind-variable cap. JSON mode binds one variable per row.
    async fn insert_json_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        // SQLite caps bind params per statement at 32766 (>=3.32). JSON mode
        // binds one variable per row, so chunk at that cap.
        const MAX_SQLITE_VARS: usize = 32766;
        for chunk in records.chunks(MAX_SQLITE_VARS) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "(?)").collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident_sqlite(&self.effective_table()),
                quote_ident_sqlite(column),
                placeholders.join(", ")
            );
            let mut q = sqlx::query(&insert_sql);
            for record in chunk {
                let json_str = serde_json::to_string(record)
                    .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
                q = q.bind(json_str);
            }
            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }
        Ok(records.len())
    }

    /// Insert a batch of records using JSON column mode.
    /// Opens its own `BEGIN`/`COMMIT` transaction and delegates to
    /// [`Self::insert_json_tx`], which sub-chunks at SQLite's bind-variable cap.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;
        let n = self.insert_json_tx(&mut tx, records, column).await?;
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(n)
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from `pragma_table_info` and maps
    /// top-level JSON fields to columns. Uses a single multi-row INSERT
    /// wrapped in a transaction.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let written = self.insert_auto_map_tx(&mut tx, records).await?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(written)
    }

    /// Auto-map insert against an in-progress transaction.
    ///
    /// This is the reusable core shared by [`Self::insert_auto_map`] (which
    /// opens its own `BEGIN`/`COMMIT`) and [`faucet_core::Sink::write_batch_idempotent`]
    /// (which folds the insert and the commit-token upsert into one
    /// transaction). The read-only `PRAGMA table_info` column-discovery query
    /// runs on the transaction's own connection (`&mut **tx`), not on
    /// `&self.pool` — otherwise, with the default single-connection pool, it
    /// would deadlock waiting for a connection the open transaction is holding.
    ///
    /// When `conflict_key` is `Some(key)`, each sub-chunk's INSERT is given an
    /// `ON CONFLICT(key) DO UPDATE …` tail so it upserts by the key columns
    /// (last-write-wins within the batch is handled by the planner's dedup,
    /// so a single sub-chunk never double-hits the same conflict target).
    pub(crate) async fn insert_auto_map_with_conflict_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        conflict_key: Option<&[String]>,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table using pragma_table_info. Use the
        // transaction's connection so a single-connection pool doesn't deadlock.
        let effective_table = self.effective_table();
        let columns = table_columns(tx, &effective_table).await?;

        // A record that matches no column is an error, never a silent skip
        // (#789 SQL-70); fields matching no column are reported once.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());
        for (idx, record) in records.iter().enumerate() {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;
            let matching = match_columns(&columns, obj);
            if matching.is_empty() {
                return Err(no_matching_column_error(
                    idx,
                    obj,
                    &columns,
                    &effective_table,
                ));
            }
            self.warn_unmatched_fields(obj, &columns, &effective_table);
            matched_rows.push(matching);
        }

        match conflict_key {
            // Grouped by present columns so an absent column is never
            // overwritten with NULL (#789 SQL-69).
            Some(_) => {
                for (present, rows) in group_by_present_columns(&columns, &matched_rows) {
                    self.insert_rows(tx, &effective_table, &present, &rows, conflict_key)
                        .await?;
                }
            }
            None => {
                // Table columns (in declared order) present in at least one
                // record; a row missing one binds SQL NULL (audit #146 H1).
                let insert_columns: Vec<String> = columns
                    .iter()
                    .filter(|c| {
                        matched_rows
                            .iter()
                            .any(|row| row.iter().any(|(rc, _)| *rc == *c))
                    })
                    .cloned()
                    .collect();
                let rows: Vec<&Vec<(&String, &Value)>> = matched_rows.iter().collect();
                self.insert_rows(tx, &effective_table, &insert_columns, &rows, None)
                    .await?;
            }
        }
        Ok(matched_rows.len())
    }

    /// One multi-row `INSERT` (chunked under SQLite's bind-variable cap) of
    /// `rows` over `insert_columns`, with the upsert tail when `conflict_key`
    /// is set. A row missing a column binds SQL NULL.
    async fn insert_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table: &str,
        insert_columns: &[String],
        rows: &[&Vec<(&String, &Value)>],
        conflict_key: Option<&[String]>,
    ) -> Result<(), FaucetError> {
        let num_cols = insert_columns.len();
        let col_names: Vec<String> = insert_columns
            .iter()
            .map(|c| quote_ident_sqlite(c))
            .collect();

        // SQLite caps bind parameters per statement at SQLITE_MAX_VARIABLE_NUMBER
        // (32766 since 3.32); split into sub-INSERTs under it (#78/#21).
        const MAX_SQLITE_VARS: usize = 32766;
        let max_rows_per_insert = (MAX_SQLITE_VARS / num_cols.max(1)).max(1);

        for sub in rows.chunks(max_rows_per_insert) {
            let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
            let value_tuples: Vec<&str> =
                (0..sub.len()).map(|_| row_placeholder.as_str()).collect();
            let base_query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident_sqlite(table),
                col_names.join(", "),
                value_tuples.join(", ")
            );
            let query = match conflict_key {
                Some(key) => format!("{base_query} {}", on_conflict_clause(key, insert_columns)),
                None => base_query,
            };

            let mut q = sqlx::query(&query);
            for matched in sub {
                for col in insert_columns {
                    // Native SQLite types so affinity and typed reads round-trip (#78/#4).
                    q = match matched.iter().find(|(c, _)| *c == col) {
                        Some((_, v)) => bind_value(q, v),
                        None => q.bind(None::<String>),
                    };
                }
            }

            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }
        Ok(())
    }

    /// Log, once per sink, record fields that match no column of `table`.
    fn warn_unmatched_fields(
        &self,
        obj: &serde_json::Map<String, Value>,
        columns: &[String],
        table: &str,
    ) {
        use std::sync::atomic::Ordering;
        if self.unmatched_warned.load(Ordering::Relaxed) {
            return;
        }
        let unmatched = unmatched_fields(obj, columns);
        if !unmatched.is_empty() && !self.unmatched_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                table = %table,
                fields = ?unmatched,
                "sqlite sink: record fields match no table column and are not written \
                 (configure a `schema:` drift policy to add or quarantine them)"
            );
        }
    }

    /// Auto-map insert against an in-progress transaction with plain append
    /// semantics (no `ON CONFLICT` tail).
    ///
    /// Thin wrapper over
    /// [`insert_auto_map_with_conflict_tx`](Self::insert_auto_map_with_conflict_tx)
    /// so the append path and the idempotent-write path keep their original
    /// signature.
    async fn insert_auto_map_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        self.insert_auto_map_with_conflict_tx(tx, records, None)
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((?, …), …)`, chunked at
    /// SQLite's bind-variable cap. Runs inside the caller's transaction.
    pub(crate) async fn delete_by_keys(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident_sqlite(&self.config.table_name);
        let col_list = key
            .iter()
            .map(|k| quote_ident_sqlite(k))
            .collect::<Vec<_>>()
            .join(", ");

        const MAX_SQLITE_VARS: usize = 32766;
        let per = (MAX_SQLITE_VARS / key.len().max(1)).max(1);
        let mut total = 0usize;

        for chunk in deletes.chunks(per) {
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| format!("({})", vec!["?"; key.len()].join(", ")))
                .collect();
            let sql = format!(
                "DELETE FROM {table_ref} WHERE ({col_list}) IN ({})",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    // Bind native SQLite types — same logic as in the INSERT path.
                    q = bind_value(q, v);
                }
            }
            let res = q
                .execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite delete failed: {e}")))?;
            total += res.rows_affected() as usize;
        }
        Ok(total)
    }

    /// Apply a planned upsert/delete batch inside one `BEGIN`/`COMMIT`
    /// transaction. Upserts and deletes are wrapped together so they commit
    /// atomically.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // `rollback.journal`: before-images commit with the writes (#706).
        if self.config.write.journals()
            && let Some(run_id) = self.config.write.rollback_run_id()
        {
            self.journal_plan(&mut tx, plan, run_id).await?;
        }
        let mut affected = 0usize;
        if !plan.upserts.is_empty() {
            affected += self
                .insert_auto_map_with_conflict_tx(
                    &mut tx,
                    &plan.upserts,
                    Some(&self.config.write.key),
                )
                .await?;
        }
        if !plan.deletes.is_empty() {
            affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
        }

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(affected)
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// Uses a temp table + `NOT EXISTS` rather than `key NOT IN (…)` because the
    /// written-key set routinely exceeds SQLite's 32766 bind-variable limit (the
    /// cleanup ceiling defaults to 100k rows). It also makes the whole thing one
    /// transaction, so the delete is all-or-nothing: a partial delete would
    /// remove rows the run actually wrote.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every row in it is stale and must go. That
    /// is the case this feature exists for, and `NOT EXISTS` against an empty
    /// table handles it without a special branch.
    ///
    /// Every statement runs on the transaction's own connection: the temp table
    /// lives in that connection's `temp` schema, and with the default
    /// single-connection pool any query sent to `&self.pool` instead would
    /// deadlock waiting for the connection the open transaction is holding.
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
        let table = &self.config.table_name;

        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // Live column set + declared types, from the table this DELETE targets.
        let declared: std::collections::HashMap<String, String> =
            sqlx::query(&format!("PRAGMA table_info({})", quote_ident_sqlite(table)))
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: table_info query failed: {e}")))?
                .iter()
                .map(|row| (row.get::<String, _>("name"), row.get::<String, _>("type")))
                .collect();

        let scope_cols: Vec<String> = scope.keys().cloned().collect();
        let existing: std::collections::HashSet<String> = declared.keys().cloned().collect();
        validate_cleanup_columns(&existing, &scope_cols, key, table)?;

        // A previous cleanup that failed *after* its COMMIT-less DROP could not
        // leave the table behind (SQLite rolls DDL back), but the pooled
        // connection is shared, so drop defensively before creating.
        let keys_ref = cleanup_keys_ref();
        sqlx::query(&format!("DROP TABLE IF EXISTS {keys_ref}"))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table drop failed: {e}")))?;

        let key_types: Vec<(String, String)> = key
            .iter()
            .map(|k| (k.clone(), declared.get(k).cloned().unwrap_or_default()))
            .collect();
        sqlx::query(&build_cleanup_temp_table_sql(&key_types))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table creation failed: {e}")))?;

        // Load the written keys, chunked at SQLite's bind-variable cap.
        const MAX_SQLITE_VARS: usize = 32766;
        let per = (MAX_SQLITE_VARS / key.len()).max(1);
        for chunk in seen.keys().chunks(per) {
            let sql = build_cleanup_insert_sql(key, chunk.len());
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    q = bind_value(q, v);
                }
            }
            q.execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: loading keys failed: {e}")))?;
        }

        // DELETE everything in scope that isn't in the written-key set.
        let sql = build_cleanup_delete_sql(table, &scope_cols, key);
        let mut q = sqlx::query(&sql);
        for v in scope.values() {
            q = bind_value(q, v);
        }
        let res = q
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: delete failed: {e}")))?;

        // Drop inside the transaction so the pooled connection goes back clean.
        sqlx::query(&format!("DROP TABLE IF EXISTS {keys_ref}"))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table drop failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: commit failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Ensure the commit-token watermark table exists.
    pub(crate) async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))",
            t = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite commit-table create failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
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

    fn supports_cleanup(&self) -> bool {
        // Column-mapping mode only: the scope + key predicates address real
        // columns, which a single JSON payload column does not have.
        matches!(self.config.column_mapping, SqliteColumnMapping::AutoMap)
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
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

    /// Column-mapping mode only: the run-id column, the journaled keys and the
    /// kept previous table all address real columns (#706).
    fn supports_rollback(&self) -> bool {
        self.rollback_supported()
    }

    async fn rollback_run(
        &self,
        run_id: &str,
        opts: &faucet_core::rollback::RollbackOptions,
    ) -> Result<faucet_core::rollback::RollbackOutcome, FaucetError> {
        self.rollback_run_impl(run_id, opts).await
    }

    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        self.forget_run_impl(run_id).await
    }

    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.rewind_commit_token_impl(scope, token).await
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.readback_source_impl()
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Create the staging table from the target's own definition (so defaults,
    /// generated columns and constraints carry over), dropping any leftover
    /// staging table from a previously-crashed run first. A target that another
    /// table references by foreign key is refused: the swap deletes and
    /// re-inserts its rows, which would cascade into or break that table.
    ///
    /// A missing target with `create_table: true` (a first run) has no shape to
    /// clone: the first write creates staging from the page, and the commit
    /// renames it into place (#676). Every step reads the database rather than
    /// sink-instance memory, because the CLI runs begin, the writes and the
    /// commit on different sink instances.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let staging = quote_ident_sqlite(&self.staging_table());
        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: drop stale staging: {e}")))?;
        if self.config.create_table && !self.table_exists(&self.config.table_name).await? {
            return Ok(());
        }
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
                .map_err(|e| {
                    FaucetError::Sink(format!("sqlite overwrite: read target definition: {e}"))
                })?;
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

    /// Atomically replace the destination with the staged rows in one
    /// transaction: `DELETE FROM target; INSERT INTO target (cols) SELECT cols
    /// FROM staging; DROP TABLE staging`, over the target's non-generated
    /// columns. SQLite DDL is transactional, so a failure
    /// anywhere rolls the whole swap back and the prior rows survive.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let staging = quote_ident_sqlite(&self.staging_table());
        if !self.table_exists(&self.config.table_name).await? {
            // First run: staging holds everything; publish it as the target.
            // A run that wrote nothing has no staging either, and leaves no table.
            if self.table_exists(&self.staging_table()).await? {
                let mut tx = self.pool.begin_with(BEGIN_WRITE).await.map_err(|e| {
                    FaucetError::Sink(format!("sqlite overwrite: begin publish: {e}"))
                })?;
                // An empty previous copy makes the first run undoable: its
                // rollback empties the table it created (#789 SQL-151).
                if self.config.write.keeps_previous() {
                    let prev = quote_ident_sqlite(&self.previous_table());
                    for stmt in [
                        format!("DROP TABLE IF EXISTS {prev}"),
                        format!("CREATE TABLE {prev} AS SELECT * FROM {staging} WHERE 0"),
                    ] {
                        sqlx::query(&stmt).execute(&mut *tx).await.map_err(|e| {
                            FaucetError::Sink(format!("sqlite overwrite: keep previous copy: {e}"))
                        })?;
                    }
                }
                sqlx::query(&format!(
                    "ALTER TABLE {staging} RENAME TO {}",
                    quote_ident_sqlite(&self.config.table_name)
                ))
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("sqlite overwrite: publish staging: {e}"))
                })?;
                tx.commit().await.map_err(|e| {
                    FaucetError::Sink(format!("sqlite overwrite: commit publish: {e}"))
                })?;
            }
            return Ok(());
        }
        let target = quote_ident_sqlite(&self.config.table_name);
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: begin swap: {e}")))?;
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
                .map_err(|e| FaucetError::Sink(format!("sqlite overwrite swap failed: {e}")))?;
        }
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: commit swap: {e}")))?;
        Ok(())
    }

    /// Probe for the `<table>__faucet_ovw` staging table (read-only).
    async fn overwrite_staging_exists(&self) -> Result<Option<bool>, FaucetError> {
        Ok(Some(self.table_exists(&self.staging_table()).await?))
    }

    /// Drop the staging table so a failed/cancelled overwrite leaves nothing
    /// behind. Best-effort — the destination was never touched (on a first run
    /// it was never created).
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {}",
            quote_ident_sqlite(&self.staging_table())
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: drop staging: {e}")))?;
        Ok(())
    }

    fn supports_schema_evolution(&self) -> bool {
        true
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
        .map_err(|e| FaucetError::Sink(format!("sqlite current_schema query failed: {e}")))?;

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

    /// Apply an additive schema evolution to the destination table (issue #194).
    ///
    /// - **Additions** — `ALTER TABLE … ADD COLUMN`. SQLite has no
    ///   `ADD COLUMN IF NOT EXISTS`, so the current columns are read first and a
    ///   column already present is silently skipped (idempotency by pre-check).
    /// - **Widenings** — a no-op under SQLite's dynamic typing: a column already
    ///   accepts a value of any type, so there is nothing to ALTER. Logged once
    ///   at `debug`.
    /// - **Nullability relaxations** — a no-op: SQLite cannot drop a `NOT NULL`
    ///   constraint in place (it requires a full table rebuild, which is out of
    ///   scope here). Logged once at `debug`; the column is left as-is.
    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        // Read the current column set so additions are idempotent (no
        // `ADD COLUMN IF NOT EXISTS` in SQLite).
        let existing: std::collections::HashSet<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident_sqlite(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite evolve table_info failed: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        for c in &evolution.additions {
            if existing.contains(&c.name) {
                continue; // already present — ADD COLUMN would error
            }
            let t = json_schema_base_type(&c.to).unwrap_or(SqlBaseType::Text);
            sqlx::query(&build_add_column_sql(&self.config.table_name, &c.name, t))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("sqlite ADD COLUMN {} failed: {e}", c.name))
                })?;
        }

        if !evolution.widenings.is_empty() {
            tracing::debug!("sqlite: type widening is a no-op under dynamic typing");
        }
        for col in &evolution.relax_nullability {
            tracing::debug!("sqlite cannot relax NOT NULL in place; column {col} left as-is");
        }

        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records).await?;

        // Upsert/delete modes: plan the writes and apply atomically. Append and
        // overwrite are insert-shaped and fall through (overwrite lands in the
        // staging table via `effective_table`).
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
        }

        // `batch_size = 0` is the "no batching" sentinel: write the entire
        // upstream slice as a single multi-row INSERT inside one
        // `BEGIN`/`COMMIT` transaction, preserving `StreamPage` framing.
        // Otherwise re-chunk into `batch_size` slices so each transaction
        // stays near SQLite's sweet spot (~1000 rows per multi-row INSERT).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
            total += match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => self.insert_json(chunk, column).await?,
                SqliteColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "SQLite write complete"
        );
        Ok(total)
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](faucet_core::Sink::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        // The DLQ and exactly-once paths must create a missing target too (#676).
        self.ensure_table_ready(records).await?;
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            // Append and overwrite: insert-shaped. A record matching no column
            // is that row's failure, not a silent skip (#789 SQL-70).
            if !matches!(self.config.column_mapping, SqliteColumnMapping::AutoMap) {
                self.write_batch(records).await?;
                return Ok(records.iter().map(|_| Ok(())).collect());
            }
            let table = self.effective_table();
            let mut conn =
                self.pool.acquire().await.map_err(|e| {
                    FaucetError::Sink(format!("SQLite connection acquire failed: {e}"))
                })?;
            let columns = table_columns(&mut conn, &table).await?;
            drop(conn);
            let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());
            let mut writable: Vec<Value> = Vec::with_capacity(records.len());
            for (idx, record) in records.iter().enumerate() {
                match record.as_object() {
                    Some(obj) if match_columns(&columns, obj).is_empty() => {
                        outcomes.push(Err(no_matching_column_error(idx, obj, &columns, &table)));
                    }
                    _ => {
                        outcomes.push(Ok(()));
                        writable.push(record.clone());
                    }
                }
            }
            self.write_batch(&writable).await?;
            return Ok(outcomes);
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "sqlite {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let sql = format!(
            "SELECT {k} FROM {t} WHERE {s} = ?",
            t = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            k = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
            s = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        // The DLQ and exactly-once paths must create a missing target too (#676).
        self.ensure_table_ready(records).await?;
        self.ensure_commit_table().await?;

        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            None
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        };

        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // Data write and the commit-token upsert share ONE transaction so the
        // page is committed atomically with its watermark: on crash either both
        // land or neither does, which is what makes a replay skip-on-resume
        // produce zero duplicates. For upsert/delete the planned upserts/deletes
        // commit together with the watermark in this same tx (no nested tx —
        // we reuse `apply_plan`'s helpers directly on this transaction).
        let written = match &plan {
            Some(plan) => {
                if self.config.write.journals()
                    && let Some(run_id) = self.config.write.rollback_run_id()
                {
                    self.journal_plan(&mut tx, plan, run_id).await?;
                }
                let mut affected = 0usize;
                if !plan.upserts.is_empty() {
                    affected += self
                        .insert_auto_map_with_conflict_tx(
                            &mut tx,
                            &plan.upserts,
                            Some(&self.config.write.key),
                        )
                        .await?;
                }
                if !plan.deletes.is_empty() {
                    affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
                }
                affected
            }
            None => match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => {
                    self.insert_json_tx(&mut tx, records, column).await?
                }
                SqliteColumnMapping::AutoMap => self.insert_auto_map_tx(&mut tx, records).await?,
            },
        };

        let upsert = format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON CONFLICT({s}) DO UPDATE SET {k} = excluded.{k}, updated_at = datetime('now')",
            t = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident_sqlite(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&upsert)
            .bind(scope)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token upsert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
