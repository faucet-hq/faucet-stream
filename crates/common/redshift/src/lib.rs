#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-redshift
//!
//! Shared connection, credentials, and connection-pool types for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Amazon Redshift
//! source and sink connectors.
//!
//! - [`RedshiftCredentials`] — `password`, `iam` (temporary credentials from
//!   `GetClusterCredentials` / serverless `GetCredentials`, refreshed before
//!   they expire) and `redshift_data_api` (statements over HTTPS through the
//!   Redshift Data API).
//! - [`RedshiftConnection`] — host / port / database / user + TLS; flattened
//!   into both end configs so the wire shape matches the other SQL connectors.
//! - [`connect_lazy`] / [`RedshiftPool`] — the PostgreSQL wire pool for
//!   `password` and `iam` (Redshift is wire-compatible with PostgreSQL, so both
//!   connectors use `sqlx`'s Postgres driver); [`build_connect_options`] /
//!   [`build_pool`] / [`build_pool_lazy`] for `password`.
//! - [`DataApiClient`] — submit, poll and page statements through the Data
//!   API, with [`data_api::field_to_json`] rendering its typed fields.

mod aws;
pub mod calls;
mod config;
pub mod data_api;
pub mod iam;
mod pool;

pub use config::{
    DEFAULT_PORT, RedshiftAwsCredentials, RedshiftConnection, RedshiftCredentials, RedshiftTlsMode,
};
pub use data_api::DataApiClient;
pub use pool::{
    RedshiftPool, build_connect_options, build_pool, build_pool_lazy, connect_lazy,
    resolve_password,
};
