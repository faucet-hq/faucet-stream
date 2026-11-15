#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-feed
//!
//! The durable event store behind
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) feeds: senders
//! push events in, deliveries read them out in order.
//!
//! - [`FeedStore`] — the contract: an atomic, durable [`append`](FeedStore::append)
//!   that assigns gap-free sequence numbers under one lock per feed (so the
//!   watermark is the committed head and a reader can never skip an event),
//!   key-based dedupe and quotas checked in the same transaction, per-delivery
//!   cursors moved only by a fenced [`commit`](FeedStore::commit) or an
//!   explicit [`move_cursor`](FeedStore::move_cursor), range claims with a
//!   holder token, retention and the maintenance claim.
//! - [`SqliteFeedStore`] (feature `sqlite`) — one server per database file.
//! - [`PostgresFeedStore`] (feature `postgres`) — safe for several servers.
//! - [`conformance`] (feature `conformance`) — the suite every backend passes.
//!
//! A delivery's cursor is the **next** sequence number to read. Claim expiry,
//! dedupe expiry and retention age follow the store's clock.

mod error;
mod store;
mod types;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod sql;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "conformance")]
pub mod conformance;

pub use error::{FeedError, Result};
pub use store::FeedStore;
pub use types::{
    AppendAck, Appended, Batch, BatchLimits, Claim, ClaimId, ClaimInfo, ClaimOwner, DeliveryHead,
    DeliveryState, DeliveryUid, Epoch, FeedUid, FullLimit, Gap, Head, Holder, Move, NewEvent,
    PurgeStats, Seq, Start, StoreCaps, StoreLimits, StoredEvent, TenantId, Timestamp, TrimStats,
};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use sql::SCHEMA_VERSION;

#[cfg(feature = "postgres")]
pub use postgres::{PostgresFeedStore, PostgresOptions};
#[cfg(feature = "sqlite")]
pub use sqlite::{SqliteFeedStore, SqliteOptions};
