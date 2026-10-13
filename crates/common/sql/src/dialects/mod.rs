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
