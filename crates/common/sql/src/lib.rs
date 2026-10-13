#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-sql
//!
//! The shared SQL layer of the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) SQL sinks
//! (RFC 0013, #884).
//!
//! - [`SqlDialect`] — how a backend spells SQL: identifier quoting, literal
//!   escaping, placeholders and the parameter limit, created / key / widened
//!   type names, the keyed upsert (`ON CONFLICT`, `ON DUPLICATE KEY`,
//!   `MERGE`) and delete statements, the scoped-cleanup statements, the
//!   commit-token table and the rollback-journal SQL
//!   ([`faucet_core::rollback::JournalSql`]).
//! - [`dialects`] — the built-in dialects ([`dialects::Postgres`],
//!   [`dialects::Mysql`], [`dialects::Sqlite`], [`dialects::Mssql`]).
//! - [`mapping`] — record-to-column mapping: [`ColumnMatching`] (exact,
//!   case-insensitive, fold-to-unique), unmatched fields, union and
//!   present-column groups.
//! - [`SqlSinkEngine`] — the planning every SQL sink shares, run over a
//!   backend's [`SqlExecutor`]: mapping, page-ordered keyed writes, chunked
//!   multi-row statements, create-table and evolution DDL, scoped cleanup,
//!   commit tokens, journal ordering and the row report.
//!
//! A backend crate implements [`SqlExecutor`] over its driver (binding each
//! [`Param`] as its column needs, wording its own errors) and keeps only what
//! is genuinely its own: transactions, bulk-load paths, overwrite swaps and
//! catalog queries.
//!
//! ```
//! use faucet_common_sql::{SqlDialect, dialects::Postgres};
//!
//! let d = Postgres;
//! assert_eq!(
//!     d.upsert_tail(&["id".into()], &["id".into(), "name".into()]).unwrap(),
//!     "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
//! );
//! assert_eq!(d.placeholder(3, Some("int4")), "$3::int4");
//! ```

pub mod dialect;
pub mod dialects;
pub mod engine;
pub mod mapping;
pub mod statement;

pub use dialect::{SqlDialect, TableTarget};
pub use engine::{
    EngineOptions, EvolutionStep, SplitPage, SqlExecutor, SqlSinkEngine, check_cleanup_columns,
    no_match_message, split_page,
};
pub use mapping::{ColumnMatching, MapError, MatchedRow, TableColumn};
pub use statement::{Param, Statement, StatementKind};
