//! Amazon Redshift.

use crate::dialect::SqlDialect;
use crate::mapping::ColumnMatching;
use faucet_core::SqlBaseType;

/// Redshift: PostgreSQL-style double-quoted identifiers and `$n`
/// placeholders, 32767 parameters, identifiers folded to lower case (fields
/// match columns ignoring case), no JSON type (nested values are
/// `VARCHAR(65535)` text), no `ON CONFLICT` (append-only).
#[derive(Debug, Clone, Copy, Default)]
pub struct Redshift;

/// Redshift's per-statement bind-parameter ceiling.
pub const PARAM_LIMIT: usize = 32_767;

impl SqlDialect for Redshift {
    fn name(&self) -> &'static str {
        "redshift"
    }

    fn quote_ident(&self, name: &str) -> String {
        faucet_core::util::quote_ident(name)
    }

    /// `$n`, cast to `sql_type` when given: a value bound as text then
    /// lands in an exact-decimal column parsed by the server (#885).
    fn placeholder(&self, n: usize, sql_type: Option<&str>) -> String {
        match sql_type {
            Some(t) => format!("${n}::{t}"),
            None => format!("${n}"),
        }
    }

    fn param_limit(&self) -> usize {
        PARAM_LIMIT
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::CaseInsensitive
    }

    /// `VARCHAR(65535)` rather than the `VARCHAR(MAX)` alias: the same type on
    /// Redshift, and valid standard SQL too.
    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "BIGINT",
            SqlBaseType::Double => "DOUBLE PRECISION",
            SqlBaseType::Boolean => "BOOLEAN",
            SqlBaseType::Text | SqlBaseType::Json => "VARCHAR(65535)",
        }
    }

    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "DECIMAL(38,10)",
            _ => self.type_name(to),
        }
    }
}
