//! DuckDB.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::{FaucetError, SqlBaseType};

/// DuckDB: double-quoted identifiers (each segment of a qualified name quoted
/// on its own), `?` placeholders with no practical parameter cap (a page is
/// one statement), case-insensitive column names, `ADD COLUMN IF NOT
/// EXISTS`, nested values stored as JSON text. Append-only.
#[derive(Debug, Clone, Copy, Default)]
pub struct Duckdb;

/// Quote a possibly schema-qualified table one segment at a time, so
/// `analytics.events` is `"analytics"."events"`, not one identifier with a
/// dot in it.
pub fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(faucet_core::util::quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

impl SqlDialect for Duckdb {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn quote_ident(&self, name: &str) -> String {
        faucet_core::util::quote_ident(name)
    }

    fn placeholder(&self, _n: usize, _sql_type: Option<&str>) -> String {
        "?".to_string()
    }

    fn param_limit(&self) -> usize {
        usize::MAX
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::CaseInsensitive
    }

    /// Nested values land as `TEXT` holding their JSON, as the writer
    /// serialises them.
    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "BIGINT",
            SqlBaseType::Double => "DOUBLE",
            SqlBaseType::Boolean => "BOOLEAN",
            SqlBaseType::Text | SqlBaseType::Json => "TEXT",
        }
    }

    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "DECIMAL(38,10)",
            _ => self.type_name(to),
        }
    }

    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!("{} TEXT NOT NULL", self.quote_ident(column)))
    }

    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
            target.reference,
            self.quote_ident(column),
            self.type_name(t)
        ))
    }
}
