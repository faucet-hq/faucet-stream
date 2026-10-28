//! ClickHouse.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::{FaucetError, LogicalType, PlannedColumn, SqlBaseType};

/// ClickHouse: double-quoted identifiers and single-quoted literals that both
/// take C-style escapes (so `\` and the quote are backslash-escaped — doubling
/// the quote alone is unsafe), case-sensitive names, every created column
/// `Nullable(…)`, `MergeTree` tables with the neutral `ORDER BY tuple()`.
/// Rows travel as `INSERT … FORMAT JSONEachRow` bodies, never as bound
/// placeholders; append-only.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClickHouse;

/// Quote an identifier, backslash-escaping `\` and `"`.
pub fn quote_ident(name: &str) -> String {
    let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Quote a possibly database-qualified table one segment at a time.
pub fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

impl SqlDialect for ClickHouse {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn quote_ident(&self, name: &str) -> String {
        quote_ident(name)
    }

    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
    }

    fn placeholder(&self, n: usize, sql_type: Option<&str>) -> String {
        format!("{{p{n}:{}}}", sql_type.unwrap_or("String"))
    }

    fn param_limit(&self) -> usize {
        usize::MAX
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::Exact
    }

    /// `Nullable(…)` throughout: ClickHouse rejects a null into a non-nullable
    /// column, so a type inferred from a page without nulls would fail the
    /// first page where a field is absent.
    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "Nullable(Int64)",
            SqlBaseType::Double => "Nullable(Float64)",
            SqlBaseType::Boolean => "Nullable(Bool)",
            SqlBaseType::Text | SqlBaseType::Json => "Nullable(String)",
        }
    }

    fn declared_type_name(&self, t: &LogicalType) -> Option<String> {
        let inner = match t {
            LogicalType::Int64 => "Int64".into(),
            LogicalType::Float64 => "Float64".into(),
            LogicalType::Bool => "Bool".into(),
            LogicalType::Decimal { .. } => {
                super::declared_decimal(t, 76, Some("Decimal(76, 38)"), "Decimal")?
            }
            LogicalType::Date => "Date32".into(),
            LogicalType::Timestamp { tz: true } => "DateTime64(6, 'UTC')".into(),
            LogicalType::Timestamp { tz: false } => "DateTime64(6)".into(),
            LogicalType::Uuid => "UUID".into(),
            _ => return None,
        };
        Some(format!("Nullable({inner})"))
    }

    /// `Decimal(76, 38)` (a 256-bit decimal): 38 integer and 38 fractional
    /// digits, so every 64-bit integer, every float up to 10^38 and every
    /// fraction a double carries stay exact.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "Nullable(Decimal(76, 38))",
            _ => self.type_name(to),
        }
    }

    /// `ORDER BY tuple()` is the neutral sort key: the sink has no basis to
    /// pick one, and a wrong one is baked into the table's primary index.
    fn create_table_sql(
        &self,
        target: &TableTarget,
        columns: &[PlannedColumn],
        _key: &[String],
    ) -> Result<String, FaucetError> {
        let cols = columns
            .iter()
            .map(|c| {
                format!(
                    "{} {}",
                    quote_ident(&c.name),
                    self.planned_type_name(c, false)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({cols}) ENGINE = MergeTree ORDER BY tuple()",
            target.reference
        ))
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
            quote_ident(column),
            self.type_name(t)
        ))
    }
}
