#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-mssql
//!
//! Shared configuration, TLS, and connection-pool types for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Microsoft SQL
//! Server source and sink connectors.
//!
//! - [`MssqlConnectionConfig`] — `connection_url` **or** `connection_string`,
//!   plus a [`MssqlTls`] block. Flattened into both end configs so the wire
//!   shape matches the other SQL connectors.
//! - [`build_config`] / [`build_pool`] — the single place a `tiberius`
//!   [`tiberius::Config`] and a `bb8` connection pool are constructed.
//! - [`with_statement_timeout`] — wraps a query future in a timeout.
//! - [`replace_connection`] — swaps a timed-out pooled connection for a fresh one.
//! - [`quote_ident_mssql`] — `[bracket]` identifier quoting (T-SQL idiom).
//! - [`PARAM_LIMIT`] — MSSQL's 2100-parameter-per-request ceiling.
//!
//! Auth is SQL Server authentication (username + password) only in v1;
//! Windows/Integrated and Azure AD auth are out of scope.

mod config;
mod pool;

pub use config::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, PARAM_LIMIT, quote_ident_mssql};
pub use pool::{
    MssqlPool, MssqlPooledConnection, build_config, build_pool, replace_connection,
    with_statement_timeout,
};
