//! The REST statement-API warehouses: Snowflake, Databricks SQL and
//! BigQuery. Their sinks ship a page as one JSON payload or inline literals
//! rather than one bound parameter per cell, so they take quoting, literal
//! escaping, type names, widening and DDL text from these dialects and keep
//! their own row planning.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::{FaucetError, PlannedColumn, SqlBaseType};

/// Snowflake: ANSI double-quoted identifiers, single-quoted literals with
/// `''` doubling and backslash escapes, `?` bind placeholders, fields folded
/// to the one column equal ignoring case. Created columns are all `STRING`:
/// the row writer projects every value with `::string`, so a typed column
/// would reject its own writer's cast.
#[derive(Debug, Clone, Copy, Default)]
pub struct Snowflake;

impl SqlDialect for Snowflake {
    fn name(&self) -> &'static str {
        "snowflake"
    }

    fn quote_ident(&self, name: &str) -> String {
        faucet_core::util::quote_ident(name)
    }

    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
    }

    fn placeholder(&self, _n: usize, _sql_type: Option<&str>) -> String {
        "?".to_string()
    }

    fn param_limit(&self) -> usize {
        usize::MAX
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::FoldUnique
    }

    fn type_name(&self, _t: SqlBaseType) -> &'static str {
        "STRING"
    }

    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "NUMBER(38,10)",
            _ => self.type_name(to),
        }
    }
}

/// Databricks SQL: backtick identifiers (a backtick doubled), literals with
/// `\` and `'` backslash-escaped (`''` is two adjacent literals there, not an
/// escaped quote), `:pN` named parameters, case-insensitive names, Delta
/// tables (`USING DELTA`), columns added through `ADD COLUMNS (…)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Databricks;

impl SqlDialect for Databricks {
    fn name(&self) -> &'static str {
        "databricks"
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn quote_literal(&self, value: &str) -> String {
        let mut out = String::with_capacity(value.len() + 2);
        out.push('\'');
        for ch in value.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '\'' => out.push_str("\\'"),
                c => out.push(c),
            }
        }
        out.push('\'');
        out
    }

    fn placeholder(&self, n: usize, _sql_type: Option<&str>) -> String {
        format!(":p{n}")
    }

    fn param_limit(&self) -> usize {
        usize::MAX
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::CaseInsensitive
    }

    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "BIGINT",
            SqlBaseType::Double => "DOUBLE",
            SqlBaseType::Boolean => "BOOLEAN",
            SqlBaseType::Text | SqlBaseType::Json => "STRING",
        }
    }

    /// Delta type widening reaches `DOUBLE` in place only from the narrow
    /// integer types (exact: a 32-bit value fits a double's mantissa); the
    /// sink refuses to widen a `BIGINT` column rather than round it.
    fn widened_type_name(&self, _from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        self.type_name(to)
    }

    fn create_table_sql(
        &self,
        target: &TableTarget,
        columns: &[PlannedColumn],
        _key: &[String],
    ) -> Result<String, FaucetError> {
        let defs: Vec<String> = columns
            .iter()
            .map(|c| {
                format!(
                    "{} {}",
                    self.quote_ident(&c.name),
                    self.type_name(c.base_type)
                )
            })
            .collect();
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) USING DELTA",
            target.reference,
            defs.join(", ")
        ))
    }

    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMNS ({} {})",
            target.reference,
            self.quote_ident(column),
            self.type_name(t)
        ))
    }
}

/// BigQuery (GoogleSQL): backtick identifiers with `\` and `` ` ``
/// backslash-escaped (stripping a backtick would silently name a different
/// column), literals with `\` and `'` backslash-escaped, `@pN` named
/// parameters, case-insensitive column names, `BIGNUMERIC` as the exact
/// widening of an `INT64` column.
#[derive(Debug, Clone, Copy, Default)]
pub struct BigQuery;

impl SqlDialect for BigQuery {
    fn name(&self) -> &'static str {
        "bigquery"
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }

    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
    }

    fn placeholder(&self, n: usize, _sql_type: Option<&str>) -> String {
        format!("@p{n}")
    }

    fn param_limit(&self) -> usize {
        usize::MAX
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::CaseInsensitive
    }

    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "INT64",
            SqlBaseType::Double => "FLOAT64",
            SqlBaseType::Boolean => "BOOL",
            SqlBaseType::Text => "STRING",
            SqlBaseType::Json => "JSON",
        }
    }

    /// `BIGNUMERIC` is exact for every `INT64` and for the fraction; BigQuery
    /// allows `INT64 → BIGNUMERIC` in place.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "BIGNUMERIC",
            _ => self.type_name(to),
        }
    }
}
