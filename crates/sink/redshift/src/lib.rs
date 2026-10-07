#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-redshift
//!
//! Amazon Redshift sink connector for the faucet-stream ecosystem.
//!
//! Two load paths, chosen by `write_strategy`:
//! - **`copy`** (default) — stage each page to S3 (JSONL or CSV) and bulk-load
//!   it with `COPY … FROM 's3://…' IAM_ROLE '…'`, then delete the staged object.
//!   Redshift's recommended, fastest bulk-load path.
//! - **`insert`** — multi-row `INSERT INTO … VALUES (…), (…)`. Portable and
//!   needs no S3, but slower for bulk data.
//!
//! Append-only: Redshift has no `ON CONFLICT` and `COPY` cannot upsert.

pub mod config;
pub mod copy;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{RedshiftCopyFormat, RedshiftSinkConfig, RedshiftWriteStrategy};
pub use faucet_common_redshift::{RedshiftConnection, RedshiftCredentials, RedshiftTlsMode};
pub use sink::RedshiftSink;
