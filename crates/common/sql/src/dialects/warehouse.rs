//! The REST statement-API warehouses: Snowflake, Databricks SQL and
//! BigQuery. Their sinks ship a page as one JSON payload or inline literals
//! rather than one bound parameter per cell, so they take quoting, literal
//! escaping, type names, widening and DDL text from these dialects and keep
//! their own row planning.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::{FaucetError, LogicalType, PlannedColumn, SqlBaseType};

/// Snowflake: ANSI double-quoted identifiers, single-quoted literals with
/// `''` doubling and backslash escapes, `?` bind placeholders, fields folded
/// to the one column equal ignoring case. An inferred column is `STRING`
/// (the row writer projects every value with `::string`); a declared one
/// (#955) takes its type, into which Snowflake coerces that text on `INSERT`.
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

    fn time_fraction_digits(&self) -> Option<usize> {
        Some(9)
    }

    /// Declared columns (#955) take their type: the row writer's `::string`
    /// projection is coerced into it on `INSERT`. Binary and JSON stay
    /// `STRING` (a `::string` value is neither hex nor parsed JSON).
    fn declared_type_name(&self, t: &LogicalType) -> Option<String> {
        Some(match t {
            LogicalType::Int64 => "NUMBER(38,0)".into(),
            LogicalType::Float64 => "FLOAT".into(),
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Decimal { .. } => super::declared_decimal(t, 38, None, "NUMBER")?,
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME(9)".into(),
            LogicalType::Timestamp { tz: true } => "TIMESTAMP_TZ(9)".into(),
            LogicalType::Timestamp { tz: false } => "TIMESTAMP_NTZ(9)".into(),
            _ => return None,
        })
    }

    /// Precision is capped at 38 digits, so no scale keeps both a wide
    /// integer range and every fractional digit. Scale 10 keeps every value
    /// with up to 28 integer digits (every 64-bit integer, signed or not, and
    /// floats up to 10^28); the database rounds a later value with more than
    /// 10 fractional digits. Declare the column with the scale the data needs
    /// to keep more.
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

    fn time_fraction_digits(&self) -> Option<usize> {
        Some(6)
    }

    fn declared_type_name(&self, t: &LogicalType) -> Option<String> {
        Some(match t {
            LogicalType::Int64 => "BIGINT".into(),
            LogicalType::Float64 => "DOUBLE".into(),
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Decimal { .. } => super::declared_decimal(t, 38, None, "DECIMAL")?,
            LogicalType::Date => "DATE".into(),
            LogicalType::Timestamp { tz: true } => "TIMESTAMP".into(),
            LogicalType::Timestamp { tz: false } => "TIMESTAMP_NTZ".into(),
            LogicalType::Bytes => "BINARY".into(),
            _ => return None,
        })
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
                    self.planned_type_name(c, false)
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

    /// A value fits the column created: `NUMERIC` (29 integer, 9 fractional
    /// digits) or `BIGNUMERIC` (38 and 38); timestamps keep microseconds.
    fn declared_fits(&self, t: &LogicalType, value: &faucet_core::Value) -> bool {
        let created = match *t {
            LogicalType::Decimal { precision, scale } => {
                let s = scale.unwrap_or(0);
                match precision {
                    Some(p) if s <= 9 && p.saturating_sub(s) <= 29 => (38, 9),
                    // Created as STRING, which holds any digits exactly.
                    Some(p) if s > 38 || p.saturating_sub(s) > 38 => {
                        return LogicalType::Decimal {
                            precision: None,
                            scale: None,
                        }
                        .accepts(value);
                    }
                    _ => (76, 38),
                }
            }
            _ => {
                return super::fits_bounded(
                    t,
                    value,
                    Some((76, Some(38))),
                    self.time_fraction_digits(),
                );
            }
        };
        LogicalType::Decimal {
            precision: Some(created.0),
            scale: Some(created.1),
        }
        .accepts(value)
    }

    fn time_fraction_digits(&self) -> Option<usize> {
        Some(6)
    }

    /// `NUMERIC` holds 29 integer and 9 fractional digits, `BIGNUMERIC` 38
    /// and 38; a decimal without precision, or wider than `NUMERIC`, is
    /// `BIGNUMERIC`, and one wider than that stays `STRING` (exact). A
    /// timestamp without a zone is a `DATETIME`; a UUID is `STRING`.
    fn declared_type_name(&self, t: &LogicalType) -> Option<String> {
        Some(match *t {
            LogicalType::Int64 => "INT64".into(),
            LogicalType::Float64 => "FLOAT64".into(),
            LogicalType::Bool => "BOOL".into(),
            LogicalType::String | LogicalType::Uuid => "STRING".into(),
            LogicalType::Decimal { precision, scale } => {
                let s = scale.unwrap_or(0);
                match precision {
                    None => "BIGNUMERIC".into(),
                    Some(p) if s <= 9 && p.saturating_sub(s) <= 29 => "NUMERIC".into(),
                    Some(p) if s <= 38 && p.saturating_sub(s) <= 38 => "BIGNUMERIC".into(),
                    Some(_) => return None,
                }
            }
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
            LogicalType::Timestamp { tz: true } => "TIMESTAMP".into(),
            LogicalType::Timestamp { tz: false } => "DATETIME".into(),
            LogicalType::Bytes => "BYTES".into(),
            LogicalType::Json => "JSON".into(),
            _ => return None,
        })
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
