#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-oracle-cdc
//!
//! Oracle Database change data capture for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! LogMiner (`DBMS_LOGMNR`) over the online and archived redo logs.
//!
//! Each committed transaction is emitted as one page of change envelopes
//! (`{op, schema, table, before, after, scn, commit_scn, xid, ts}`, compatible
//! with the `cdc_unwrap` transform) bookmarked by SCN, so a resumed run
//! continues with no gap and no duplicate. Transactions are buffered until
//! their `COMMIT`; rolled-back work is never emitted. Missing redo fails the run
//! naming the lost SCN range. `capture_resume_position` returns the current
//! SCN for the snapshot → CDC handoff.
//!
//! **Runtime requirement:** Oracle Instant Client must be installed where
//! ODPI-C can load it. The database needs minimal supplemental logging (and
//! all-column logging for full update images).

mod config;
mod logs;
mod miner;
mod redo;
mod sql;
mod state;
mod stream;

pub use config::{OnUnsupported, OracleCdcSourceConfig, StartPosition};
pub use faucet_common_oracle::{OracleConnectionConfig, OracleTls};
pub use state::Position;
pub use stream::OracleCdcSource;
