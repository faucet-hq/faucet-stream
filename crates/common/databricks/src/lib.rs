#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-databricks
//!
//! Shared Databricks SQL warehouse connection and auth types for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Databricks
//! connectors (`faucet-source-databricks`, `faucet-sink-databricks`).
//!
//! - [`DatabricksAuth`] — Personal Access Token / OAuth M2M bearer auth, plus
//!   [`resolve_authorization`] which prefers a shared `auth: { ref }` provider.
//! - [`StatementClient`] — a thin client over the
//!   [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution):
//!   submit, poll until terminal (tolerating a cold serverless warehouse),
//!   retry `429` / `503` with backoff (honouring `Retry-After`), cancel on a
//!   client-side deadline, and follow result chunks.
//! - Response types ([`StatementResponse`], [`StatementStatus`],
//!   [`ResultColumn`], …) and [`value_to_param_string`] for named parameters.

mod auth;
mod statement;

pub use auth::{DatabricksAuth, resolve_authorization};
pub use statement::{
    ErrorSide, Manifest, ResultColumn, ResultSchema, StatementClient, StatementError,
    StatementOptions, StatementParam, StatementRequest, StatementResponse, StatementState,
    StatementStatus, backoff_delay, value_to_param_string,
};
