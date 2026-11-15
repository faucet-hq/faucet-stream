#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-feed
//!
//! The `feed` source: reads one delivery's events from a durable feed store
//! ([`faucet_common_feed::FeedStore`]) in order, and moves the delivery's
//! cursor only after the pipeline wrote, flushed and bookmarked each page.
//!
//! Under a delivery run on a server the store, the delivery and the claimed
//! range come from the [`FeedExecution`](faucet_common_feed::FeedExecution)
//! the server scopes over the run. Under `faucet run` the config names them
//! and the source takes its own range claim.

mod bookmark;
pub mod config;
mod decode;
mod error;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use bookmark::FeedBookmark;
pub use config::{FeedSourceConfig, FeedStoreSpec, PostgresStoreConfig, SqliteStoreConfig};
pub use decode::{METADATA_KEY, VALUE_KEY};
pub use error::FeedSourceError;
pub use stream::FeedSource;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("feed", capabilities);

/// A read claims a range and moves the delivery's cursor as its pages are
/// written, so it cannot be repeated; a resumed read replays exactly what
/// follows the bookmark.
fn capabilities(_config: &faucet_core::Value) -> faucet_core::Capabilities {
    faucet_core::Capabilities::builder()
        .exactly_once(true)
        .consumes_destructively(true)
        .read_side_effects(true)
        .build()
}
