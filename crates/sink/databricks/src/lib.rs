#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-databricks
//!
//! Databricks SQL warehouse sink for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Loads
//! pages into a Delta table through a SQL warehouse's Statement Execution API
//! (plain REST — no JDBC/ODBC driver):
//!
//! - **Load paths** — a multi-row `INSERT … FROM VALUES` for small pages, or a
//!   staged all-string Parquet file (Unity Catalog volume via the Files API,
//!   or S3 / GCS / ADLS behind the `staging` feature) loaded with `COPY INTO`.
//!   Every value is cast to the column's declared type on the server.
//! - **Write modes** — `append`, `upsert` / `delete` via one `MERGE` per
//!   statement (with `delete_marker`), and `overwrite` via a staging table and
//!   an atomic `INSERT OVERWRITE` swap.
//! - **Exactly-once** — a `_faucet_commit_token` watermark table plus a data
//!   write that is idempotent per page token (`INSERT … REPLACE WHERE` for
//!   append, `MERGE` for upsert), so a replayed page never duplicates.
//! - **Schema** — `current_schema` from `information_schema.columns`,
//!   `ALTER TABLE ADD COLUMNS` / Delta type widening for drift, and table
//!   auto-create from the first page.

mod config;
mod sink;
mod sql;
mod stage;

pub use config::{
    DatabricksAuth, DatabricksLoadMethod, DatabricksSinkConfig, DatabricksStagingConfig,
    STATEMENT_TEXT_LIMIT,
};
pub use sink::DatabricksSink;
