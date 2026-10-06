#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-databricks
//!
//! Databricks SQL query source for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! ecosystem. Runs a SQL statement against a Databricks **SQL Warehouse** via
//! the [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
//! (plain REST — no JDBC/ODBC driver) and streams the result rows as typed
//! JSON objects.
//!
//! This is the **query-results** read path for Databricks (joins / aggregates /
//! filtered extracts). For full-table lakehouse scans and the write path, use
//! the Delta Lake connectors (`faucet-source-delta` / `faucet-sink-delta`).
//!
//! - Async statement lifecycle: submit → poll until terminal → stream result
//!   chunks (INLINE + `JSON_ARRAY`), following `next_chunk_internal_link`.
//! - Type-aware decode from the response `manifest` column schema.
//! - Incremental replication via a bookmark column + a `${bookmark}` token
//!   (server-side named param) plus a client-side filter backstop.
//! - Bearer auth (PAT / OAuth M2M), inline or via the shared `auth:` catalog.

mod config;
mod convert;
mod stream;

pub use config::{
    DatabricksAuth, DatabricksParam, DatabricksReplication, DatabricksSourceConfig,
    ResultDisposition,
};
pub use stream::DatabricksSource;
