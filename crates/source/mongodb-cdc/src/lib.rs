#![cfg_attr(docsrs, feature(doc_cfg))]
//! MongoDB Change Streams (CDC) source for the faucet-stream ecosystem.
//!
//! Tails a collection / database / cluster change stream and emits per-document
//! change events as a CDC envelope, resumable via the opaque `resumeToken`.

mod config;
mod envelope;
mod state;
mod stream;

pub use config::{FullDocument, FullDocumentBeforeChange, MongoCdcSourceConfig, Scope, StartFrom};
pub use state::{Bookmark, STATE_SCHEMA, migrate_state, state_key};
pub use stream::MongoCdcSource;
