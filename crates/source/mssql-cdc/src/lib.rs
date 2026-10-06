#![cfg_attr(docsrs, feature(doc_cfg))]
//! Microsoft SQL Server CDC (change data capture) source for the faucet-stream
//! ecosystem.
//!
//! Polls native SQL Server change data capture — `sys.fn_cdc_get_max_lsn()` for
//! the high-water LSN, then `cdc.fn_cdc_get_all_changes_<capture_instance>()`
//! per configured capture instance — and emits per-row change events as CDC
//! envelopes, resumable via a per-instance LSN bookmark. Each committed
//! transaction is emitted as its own [`StreamPage`](faucet_core::StreamPage),
//! giving per-transaction durability and exactly-once-capable replay.
//!
//! Pair the emitted `{op, before, after, …}` envelopes with the `cdc_unwrap`
//! transform stage and an upsert-capable sink to build a live mirror pipeline.

mod change;
mod config;
mod decode;
mod lsn;
mod state;
mod stream;

pub use config::{MssqlCdcSourceConfig, OnGap, StartPosition};
pub use lsn::Lsn;
pub use state::Bookmarks;
pub use stream::MssqlCdcSource;
