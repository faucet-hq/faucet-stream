#![cfg_attr(docsrs, feature(doc_cfg))]
//! Google Cloud Spanner query source connector for
//! [faucet-stream](https://crates.io/crates/faucet-stream).
//!
//! Runs a GoogleSQL query against a Spanner database via the streaming read
//! API (`ExecuteStreamingSql`) and emits rows as JSON with bounded memory.
//! Supports named bind parameters, stale reads, incremental replication via
//! a monotonic column bookmark, live dataset discovery
//! (`INFORMATION_SCHEMA`), and PK-range sharding for clustered execution.
//!
//! ```yaml
//! source:
//!   kind: spanner
//!   config:
//!     project_id: my-project
//!     instance: my-instance
//!     database: my-db
//!     query: "SELECT * FROM orders WHERE updated_at > TIMESTAMP(@bookmark)"
//!     replication:
//!       type: incremental
//!       column: updated_at
//!       initial_value: "1970-01-01T00:00:00Z"
//! ```
//!
//! Authentication defaults to Application Default Credentials; a service
//! account key can be supplied by path or inline via `auth`. Set
//! `emulator_host` to target the Spanner emulator.

mod config;
mod stream;

pub use config::{ShardConfig, SpannerReplication, SpannerSourceConfig};
pub use faucet_common_spanner::{SpannerConnection, SpannerCredentials};
pub use stream::SpannerSource;

mod descriptor;
pub use descriptor::DESCRIPTOR;
