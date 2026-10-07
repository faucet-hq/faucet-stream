#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-redshift
//!
//! Shared connection, credentials, and connection-pool types for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Amazon Redshift
//! source and sink connectors.
//!
//! Redshift is wire-compatible with PostgreSQL, so both connectors talk to it
//! through `sqlx`'s Postgres driver.
//!
//! - [`RedshiftCredentials`] — `password` (implemented in v1), plus reserved
//!   `iam` / `redshift_data_api` variants that currently return a typed error.
//! - [`RedshiftConnection`] — host / port / database / user + a TLS toggle;
//!   flattened into both end configs so the wire shape matches the other SQL
//!   connectors.
//! - [`build_connect_options`] / [`build_pool`] / [`build_pool_lazy`] — the
//!   single place a `sqlx` [`PgConnectOptions`](sqlx::postgres::PgConnectOptions)
//!   and a [`PgPool`](sqlx::PgPool) are constructed.
//! - [`resolve_password`] — extracts the password (and is where an unsupported
//!   credential variant surfaces its error).

mod config;
mod pool;

pub use config::{DEFAULT_PORT, RedshiftConnection, RedshiftCredentials, RedshiftTlsMode};
pub use pool::{build_connect_options, build_pool, build_pool_lazy, resolve_password};
