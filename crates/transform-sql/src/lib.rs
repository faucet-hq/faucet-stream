#![cfg_attr(docsrs, feature(doc_cfg))]
//! SQL-as-transform for faucet-stream, backed by embedded DuckDB.
//!
//! Each pipeline page is exposed to a SQL query as the relation `batch`; the
//! result set becomes the new page. [`SqlTransformConfig`] is the user-facing
//! config (the `query` plus optional reference [`RelationSpec`]s, whose data
//! comes from a [`RelationSource`]). [`SqlTransform`] is the compiled runtime
//! that owns the DuckDB connection and runs the query per page.

mod compile;
mod config;
mod http;
mod runtime;
mod shovel;

pub use config::{HttpMethod, RelationSource, RelationSpec, SqlTransformConfig};
pub use runtime::SqlTransform;
