//! The dialects the built-in SQL sinks speak. Each is a unit struct; the
//! shared planning matrix (`tests/dialect_matrix.rs`) runs every planning case
//! against all of them.

pub mod mssql;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

pub use mssql::Mssql;
pub use mysql::Mysql;
pub use postgres::Postgres;
pub use sqlite::Sqlite;
