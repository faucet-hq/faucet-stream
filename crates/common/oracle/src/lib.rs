#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-oracle
//!
//! Shared Oracle Database connection, TLS and auth types for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Oracle source,
//! CDC source and sink, built on the [`oracle`](https://crates.io/crates/oracle)
//! driver (ODPI-C).
//!
//! - [`OracleConnectionConfig`] — `connect_string` **or** `host` +
//!   `service_name`/`sid`, username/password or external auth, and a TCPS
//!   [`OracleTls`] block. Flattened into every Oracle connector config.
//! - [`connect_pool`] / [`blocking`] — the session pool and the
//!   `spawn_blocking` bridge for the synchronous driver.
//! - [`quote_ident_oracle`] / [`quote_table_oracle`] / [`shard_wrap`] — SQL text helpers.
//! - [`TypeFamily`] / [`cell_to_json`] / [`number_text_to_json`] — exact type mapping.
//!
//! **Runtime requirement:** ODPI-C loads Oracle Instant Client at runtime. The
//! crate compiles without it; connecting without it fails with a typed error.

mod config;
mod decode;
mod pool;
mod sql;
mod types;

pub use config::{OracleConnectionConfig, OracleTls};
pub use decode::{ExtractKind, extract_kind, parse_json_column, row_to_json};
pub use pool::{
    CLIENT_HINT, NLS_SESSION_SQL, OraclePool, POOL_WAIT, Side, blocking, build_pool_blocking,
    call_timeout, checkout, connect_pool, is_client_missing, ora_code, ora_err,
};
pub use sql::{
    MAX_IDENTIFIER_BYTES, quote_ident_oracle, quote_table_oracle, quote_validated,
    shard_bounds_query, shard_wrap, split_table, string_literal, trim_statement,
};
pub use types::{
    Cell, TypeFamily, bytes_to_hex, cell_to_json, format_interval_ds, format_interval_ym,
    hex_to_bytes, iso_to_oracle_interval, normalize_datetime_text, number_text_to_json,
    oracle_interval_ds_to_iso, oracle_interval_ym_to_iso, typed_text_to_json,
};

/// Re-export of the driver so connector crates share one version.
pub use oracle;
