//! SQLite.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::rollback::JournalSql;
use faucet_core::{FaucetError, SqlBaseType};

/// SQLite: backtick-quoted identifiers (a double-quoted identifier that names
/// no column silently becomes a string literal), `?` placeholders, 32766
/// parameters, case-insensitive column names, `ON CONFLICT(…) DO UPDATE SET
/// … = excluded.…`, dynamic typing (widening is a no-op).
#[derive(Debug, Clone, Copy, Default)]
pub struct Sqlite;

/// SQLite's bind-variable ceiling (`SQLITE_MAX_VARIABLE_NUMBER`, ≥ 3.32).
pub const PARAM_LIMIT: usize = 32766;

/// Backtick-quote an identifier, doubling embedded backticks.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn placeholder(_n: usize) -> String {
    "?".to_string()
}

/// A declared column type (`PRAGMA table_info.type`) safe to re-emit in DDL,
/// or `None` (SQLite lets a column be declared with arbitrary text).
pub fn safe_type_spec(declared: &str) -> Option<&str> {
    let t = declared.trim();
    if t.is_empty() {
        return None;
    }
    t.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '(' | ')' | ',' | '.'))
        .then_some(t)
}

impl SqlDialect for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn quote_ident(&self, name: &str) -> String {
        quote_ident(name)
    }

    fn placeholder(&self, _n: usize, _sql_type: Option<&str>) -> String {
        "?".to_string()
    }

    fn param_limit(&self) -> usize {
        PARAM_LIMIT
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::CaseInsensitive
    }

    /// Advisory affinities: SQLite has no boolean (`INTEGER`) and stores JSON
    /// as `TEXT`.
    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "INTEGER",
            SqlBaseType::Double => "REAL",
            SqlBaseType::Boolean => "INTEGER",
            SqlBaseType::Text => "TEXT",
            SqlBaseType::Json => "TEXT",
        }
    }

    fn upsert_tail(&self, key: &[String], columns: &[String]) -> Option<String> {
        let key_list = key
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        let updates: Vec<String> = columns
            .iter()
            .filter(|c| !key.iter().any(|k| k == *c))
            .map(|c| format!("{q} = excluded.{q}", q = quote_ident(c)))
            .collect();
        Some(if updates.is_empty() {
            format!("ON CONFLICT({key_list}) DO NOTHING")
        } else {
            format!(
                "ON CONFLICT({key_list}) DO UPDATE SET {}",
                updates.join(", ")
            )
        })
    }

    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!(
            "{} INTEGER PRIMARY KEY AUTOINCREMENT, {} TEXT NOT NULL",
            quote_ident("id"),
            quote_ident(column)
        ))
    }

    /// Always in the `temp` schema, so it can never be confused with (or, on
    /// the defensive drop, destroy) a main-database table of the same name.
    fn cleanup_keys_table(&self) -> String {
        format!("temp.{}", quote_ident("faucet_cleanup_keys"))
    }

    fn cleanup_drop_sql(&self) -> Option<String> {
        Some(format!(
            "DROP TABLE IF EXISTS {}",
            self.cleanup_keys_table()
        ))
    }

    /// Declared types are filtered through [`safe_type_spec`]; an unusable
    /// one leaves the column typeless (legal in SQLite).
    fn cleanup_create_sql(
        &self,
        _target: &TableTarget,
        key: &[String],
        key_types: &[Option<&str>],
    ) -> Result<String, FaucetError> {
        let cols = key
            .iter()
            .zip(key_types)
            .map(|(col, declared)| match declared.and_then(safe_type_spec) {
                Some(t) => format!("{} {t}", quote_ident(col)),
                None => quote_ident(col),
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "CREATE TEMP TABLE {} ({cols})",
            self.cleanup_keys_table()
        ))
    }

    /// The join references the target by name: a single-table `DELETE … AS
    /// alias` is newer SQLite grammar.
    fn cleanup_delete_sql(
        &self,
        target: &TableTarget,
        scope: &[String],
        _scope_types: &[Option<&str>],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let t = &target.reference;
        let scope_pred = scope
            .iter()
            .map(|c| format!("{t}.{} = ?", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let join_pred = key
            .iter()
            .map(|k| {
                let q = quote_ident(k);
                format!("{t}.{q} = c.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(format!(
            "DELETE FROM {t} WHERE {scope_pred} AND NOT EXISTS (SELECT 1 FROM {} c WHERE {join_pred})",
            self.cleanup_keys_table()
        ))
    }

    fn commit_token_table_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))"
        )
    }

    fn commit_token_upsert_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON CONFLICT({s}) DO UPDATE SET {k} = excluded.{k}, updated_at = datetime('now')"
        )
    }

    fn journal_sql(&self) -> Option<JournalSql> {
        Some(JournalSql {
            quote: quote_ident,
            placeholder,
            before_type: "TEXT",
            insert_prefix: "INSERT OR IGNORE",
            insert_suffix: "",
            now: "(datetime('now'))",
            key_column: faucet_core::rollback::KEY_COLUMN_TEXT,
            primary_key: faucet_core::rollback::PRIMARY_KEY_TEXT,
        })
    }

    fn no_matching_column_message(
        &self,
        index: usize,
        fields: &[&String],
        columns: &[&String],
        table: &str,
    ) -> String {
        format!(
            "sqlite: record {index} has no field matching a column of table '{table}' \
             (record fields: {fields:?}; table columns: {columns:?})"
        )
    }
}
