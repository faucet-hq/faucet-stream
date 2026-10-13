//! MySQL / MariaDB.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::rollback::JournalSql;
use faucet_core::{FaucetError, SqlBaseType};

/// MySQL: backtick-quoted identifiers, `?` placeholders, 65535 parameters,
/// case-insensitive column names, `ON DUPLICATE KEY UPDATE … = VALUES(…)`,
/// bounded `VARCHAR(191)` primary-key text (InnoDB's 3072-byte index limit).
#[derive(Debug, Clone, Copy, Default)]
pub struct Mysql;

/// MySQL's prepared-statement placeholder ceiling.
pub const PARAM_LIMIT: usize = 65535;

/// Width of the watermark table's `scope` primary-key column.
pub const SCOPE_COL_WIDTH: usize = 255;

/// Backtick-quote an identifier, doubling embedded backticks.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn placeholder(_n: usize) -> String {
    "?".to_string()
}

impl SqlDialect for Mysql {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn quote_ident(&self, name: &str) -> String {
        quote_ident(name)
    }

    /// Backslashes are escapes inside a MySQL string literal (unless
    /// `NO_BACKSLASH_ESCAPES`), so they are doubled as well.
    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
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

    /// Integers widen to `BIGINT`, floats to `DOUBLE`, text to `LONGTEXT` (so
    /// a long value never truncates) and JSON to the native `JSON`.
    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "BIGINT",
            SqlBaseType::Double => "DOUBLE",
            SqlBaseType::Boolean => "TINYINT(1)",
            SqlBaseType::Text => "LONGTEXT",
            SqlBaseType::Json => "JSON",
        }
    }

    /// 191 characters keeps even a four-column utf8mb4 key under InnoDB's
    /// 3072-byte index limit.
    fn key_type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Text | SqlBaseType::Json => "VARCHAR(191)",
            other => self.type_name(other),
        }
    }

    /// Exact `DECIMAL(65,30)` for an integer column that starts receiving
    /// fractions: a value outside its range fails loudly instead of rounding.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "DECIMAL(65,30)",
            _ => self.type_name(to),
        }
    }

    /// With every column a key column there is nothing to update; a
    /// self-assignment keeps the statement valid and the row untouched.
    fn upsert_tail(&self, key: &[String], columns: &[String]) -> Option<String> {
        let updates: Vec<String> = columns
            .iter()
            .filter(|c| !key.iter().any(|k| k == *c))
            .map(|c| {
                let q = quote_ident(c);
                format!("{q} = VALUES({q})")
            })
            .collect();
        Some(if updates.is_empty() {
            let q = quote_ident(key.first().map(String::as_str).unwrap_or_default());
            format!("ON DUPLICATE KEY UPDATE {q} = {q}")
        } else {
            format!("ON DUPLICATE KEY UPDATE {}", updates.join(", "))
        })
    }

    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!(
            "{} BIGINT AUTO_INCREMENT PRIMARY KEY, {} JSON NOT NULL",
            quote_ident("id"),
            quote_ident(column)
        ))
    }

    /// A `TEMPORARY` table is session-scoped and shadows a base table of the
    /// same name for that session only; `DROP TEMPORARY TABLE` never touches
    /// a base table.
    fn cleanup_keys_table(&self) -> String {
        quote_ident("faucet_cleanup_keys")
    }

    fn cleanup_drop_sql(&self) -> Option<String> {
        Some(format!(
            "DROP TEMPORARY TABLE IF EXISTS {}",
            self.cleanup_keys_table()
        ))
    }

    /// `key_types` carry the full column definition tail (type, character
    /// set, collation) so key comparisons never mix collations; `LONGTEXT`
    /// when unknown (MySQL has no typeless column).
    fn cleanup_create_sql(
        &self,
        _target: &TableTarget,
        key: &[String],
        key_types: &[Option<&str>],
    ) -> Result<String, FaucetError> {
        let defs = key
            .iter()
            .zip(key_types)
            .map(|(k, t)| format!("{} {}", quote_ident(k), t.unwrap_or("LONGTEXT")))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "CREATE TEMPORARY TABLE {} ({defs})",
            self.cleanup_keys_table()
        ))
    }

    fn commit_token_table_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} VARCHAR({SCOPE_COL_WIDTH}) PRIMARY KEY, {k} TEXT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
    }

    fn commit_token_upsert_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON DUPLICATE KEY UPDATE {k} = VALUES({k})"
        )
    }

    /// InnoDB caps an index at 3072 bytes and utf8mb4 text is 4 bytes a
    /// character, so the journal keys on a stored SHA-256 of the key JSON.
    fn journal_sql(&self) -> Option<JournalSql> {
        Some(JournalSql {
            quote: quote_ident,
            placeholder,
            before_type: "LONGTEXT",
            insert_prefix: "INSERT IGNORE",
            insert_suffix: "",
            now: "CURRENT_TIMESTAMP",
            key_column: "key_json TEXT NOT NULL, key_hash CHAR(64) CHARACTER SET ascii \
                         AS (SHA2(key_json, 256)) STORED",
            primary_key: "PRIMARY KEY (run_id, table_name, key_hash)",
        })
    }

    fn no_matching_column_message(
        &self,
        index: usize,
        fields: &[&String],
        columns: &[&String],
        _table: &str,
    ) -> String {
        format!(
            "mysql: record {index} has no field matching a column of the target table \
             (record fields: {fields:?}; table columns: {columns:?})"
        )
    }
}
