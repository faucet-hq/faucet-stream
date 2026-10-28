//! The dialects the built-in SQL sinks speak. Each is a unit struct; the
//! shared planning matrix (`tests/dialect_matrix.rs`) runs every planning case
//! against all of them.
//!
//! - Driven by [`SqlSinkEngine`](crate::SqlSinkEngine) end to end:
//!   [`Postgres`], [`Mysql`], [`Sqlite`].
//! - Planning and SQL text from the engine and dialect, execution by the
//!   backend: [`Mssql`] (MERGE, typed `CAST` placeholders, retry-classified
//!   chunk execution), [`Duckdb`] (blocking driver).
//! - SQL text (quoting, escaping, types, DDL, upserts) from the dialect, row
//!   handling by the backend: [`Redshift`], [`ClickHouse`], [`Oracle`],
//!   [`Snowflake`], [`Databricks`], [`BigQuery`].

pub mod clickhouse;
pub mod duckdb;
pub mod mssql;
pub mod mysql;
pub mod oracle;
pub mod postgres;
pub mod redshift;
pub mod sqlite;
pub mod warehouse;

pub use clickhouse::ClickHouse;
pub use duckdb::Duckdb;
pub use mssql::Mssql;
pub use mysql::Mysql;
pub use oracle::Oracle;
pub use postgres::Postgres;
pub use redshift::Redshift;
pub use sqlite::Sqlite;
pub use warehouse::{BigQuery, Databricks, Snowflake};

/// [`SqlDialect::declared_fits`](crate::SqlDialect::declared_fits) for a
/// dialect that creates an unbounded declared decimal as `unbounded`
/// `(precision, scale)` (`scale: None` = that many significant digits at any
/// scale) and keeps time values to `fraction` fractional-second digits.
pub(crate) fn fits_bounded(
    t: &faucet_core::LogicalType,
    value: &faucet_core::Value,
    unbounded: (u32, Option<u32>),
    fraction: Option<usize>,
) -> bool {
    use faucet_core::LogicalType;
    match *t {
        LogicalType::Decimal {
            precision: None,
            scale: None,
        } => LogicalType::Decimal {
            precision: Some(unbounded.0),
            scale: unbounded.1,
        }
        .accepts(value),
        LogicalType::Decimal {
            precision: Some(p),
            scale,
        } => LogicalType::Decimal {
            precision: Some(p),
            scale: Some(scale.unwrap_or(0)),
        }
        .accepts(value),
        LogicalType::Time | LogicalType::Timestamp { .. } => {
            t.accepts(value)
                && fraction
                    .is_none_or(|max| value.as_str().is_none_or(|s| fraction_digits(s) <= max))
        }
        _ => t.accepts(value),
    }
}

/// The fractional-second digits of a time or timestamp rendering.
fn fraction_digits(s: &str) -> usize {
    let time = s.rsplit(['T', ' ']).next().unwrap_or(s);
    time.split_once('.').map_or(0, |(_, rest)| {
        rest.bytes().take_while(u8::is_ascii_digit).count()
    })
}

/// `keyword(p,s)` for a declared decimal of at most `max` digits whose scale
/// fits it; `unbounded` for one without precision; `None` (text) for anything
/// the dialect cannot hold exactly.
pub(crate) fn declared_decimal(
    t: &faucet_core::LogicalType,
    max: u32,
    unbounded: Option<&str>,
    keyword: &str,
) -> Option<String> {
    use faucet_core::LogicalType;
    match *t {
        LogicalType::Decimal {
            precision: Some(p),
            scale,
        } if (1..=max).contains(&p) && scale.unwrap_or(0) <= p => {
            Some(format!("{keyword}({p},{})", scale.unwrap_or(0)))
        }
        LogicalType::Decimal {
            precision: None,
            scale: None,
        } => unbounded.map(str::to_string),
        _ => None,
    }
}
