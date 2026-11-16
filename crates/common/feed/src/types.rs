//! The feed store's data types.

use crate::error::FeedError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// A point in time as the store reports it.
pub type Timestamp = DateTime<Utc>;

const MAX_ID_LEN: usize = 128;

fn check_id(kind: &'static str, value: &str) -> Result<(), FeedError> {
    let ok = !value.is_empty()
        && value.len() <= MAX_ID_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/'));
    if ok {
        Ok(())
    } else {
        Err(FeedError::InvalidId {
            kind,
            value: value.chars().take(MAX_ID_LEN).collect(),
        })
    }
}

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Wrap `value`: 1 to 128 characters of ASCII letters, digits and
            /// `- _ . : /`.
            pub fn new(value: impl Into<String>) -> Result<Self, FeedError> {
                let value = value.into();
                check_id($kind, &value)?;
                Ok(Self(value))
            }

            /// The id text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = FeedError;
            fn try_from(value: String) -> Result<Self, FeedError> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(
    /// A feed's server-generated uid; never its name.
    FeedUid,
    "feed"
);
string_id!(
    /// The tenant a feed belongs to.
    TenantId,
    "tenant"
);
string_id!(
    /// A range claim's id.
    ClaimId,
    "claim"
);
string_id!(
    /// Who took a claim, e.g. `serve:<instance>` or `cli:<host>:<pid>`.
    ClaimOwner,
    "claim owner"
);

impl FeedUid {
    /// A fresh random uid.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }
}

impl ClaimId {
    /// A fresh random claim id.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }
}

/// A delivery's uid: the feed it reads and its own server-generated id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryUid {
    feed: FeedUid,
    id: String,
}

impl DeliveryUid {
    /// The delivery `id` of `feed`; `id` follows the [`FeedUid`] rules.
    pub fn new(feed: FeedUid, id: impl Into<String>) -> Result<Self, FeedError> {
        let id = id.into();
        check_id("delivery", &id)?;
        Ok(Self { feed, id })
    }

    /// A delivery of `feed` with a fresh random id.
    pub fn generate(feed: FeedUid) -> Self {
        Self {
            feed,
            id: uuid::Uuid::new_v4().simple().to_string(),
        }
    }

    /// The feed this delivery reads.
    pub fn feed(&self) -> &FeedUid {
        &self.feed
    }

    /// The delivery's own id.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for DeliveryUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.feed, self.id)
    }
}

/// A per-feed sequence number. The first event is `1`. A delivery's cursor is
/// the **next** sequence number to read: everything below it is delivered.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Seq(pub u64);

impl Seq {
    /// The first sequence number a feed assigns.
    pub const FIRST: Seq = Seq(1);

    /// The number itself.
    pub fn get(self) -> u64 {
        self.0
    }

    /// The following sequence number.
    pub fn next(self) -> Seq {
        Seq(self.0.saturating_add(1))
    }

    /// `self + n`, saturating.
    pub fn plus(self, n: u64) -> Seq {
        Seq(self.0.saturating_add(n))
    }

    /// Events in `[self, end)`.
    pub fn distance_to(self, end: Seq) -> u64 {
        end.0.saturating_sub(self.0)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A delivery's epoch; every cursor move outside a commit bumps it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Epoch(pub u64);

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// An event to append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEvent {
    /// When the ingest node received it (informational; never orders events).
    pub received_at: Timestamp,
    /// The payload's content type.
    pub content_type: String,
    /// The allow-listed request headers kept with the event.
    pub headers: BTreeMap<String, String>,
    /// The raw payload.
    pub payload: Vec<u8>,
    /// Whether `payload` is sealed with the vault.
    pub sealed: bool,
    /// Sender-supplied key; a retry with the same key inside the feed's
    /// dedupe window is not stored again.
    pub dedupe_key: Option<String>,
    /// The payload's size before sealing; `None` when `payload` is as sent.
    pub plain_size: Option<u64>,
}

impl NewEvent {
    /// An event received now.
    pub fn new(content_type: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            received_at: Utc::now(),
            content_type: content_type.into(),
            headers: BTreeMap::new(),
            payload: payload.into(),
            sealed: false,
            dedupe_key: None,
            plain_size: None,
        }
    }

    /// Set the receive time.
    pub fn received_at(mut self, at: Timestamp) -> Self {
        self.received_at = at;
        self
    }

    /// Keep one request header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Mark the payload as sealed.
    pub fn sealed(mut self, sealed: bool) -> Self {
        self.sealed = sealed;
        self
    }

    /// Set the dedupe key.
    pub fn dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }

    /// Set the size the payload had before it was sealed.
    pub fn plain_size(mut self, bytes: u64) -> Self {
        self.plain_size = Some(bytes);
        self
    }

    /// The payload size in bytes as counted against quotas: the size as
    /// sent, whatever sealing added.
    pub fn size(&self) -> u64 {
        self.plain_size.unwrap_or(self.payload.len() as u64)
    }
}

/// An event as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// Its sequence number in the feed.
    pub seq: Seq,
    /// Its time-ordered id (UUID v7), returned to the sender.
    pub id: String,
    /// When the ingest node received it.
    pub received_at: Timestamp,
    /// When the store committed it, by the store's clock.
    pub stored_at: Timestamp,
    /// The payload's content type.
    pub content_type: String,
    /// The headers kept with the event.
    pub headers: BTreeMap<String, String>,
    /// The raw payload.
    pub payload: Vec<u8>,
    /// The payload size in bytes.
    pub size: u64,
    /// Whether `payload` is sealed.
    pub sealed: bool,
    /// The sender's dedupe key.
    pub dedupe_key: Option<String>,
}

/// One event of an append, in request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Appended {
    /// The stored event's id (the earlier event's for a duplicate).
    pub id: String,
    /// The stored event's sequence number (the earlier event's for a duplicate).
    pub seq: Seq,
    /// Whether the event was already stored under its dedupe key.
    pub duplicate: bool,
}

/// The answer to an append, sent only after the events are durable.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppendAck {
    /// One entry per offered event, in order.
    pub events: Vec<Appended>,
    /// The first sequence number this append stored.
    pub first_seq: Option<Seq>,
    /// The last sequence number this append stored.
    pub last_seq: Option<Seq>,
    /// Offered events that were duplicates and not stored again.
    pub duplicates: u64,
}

impl AppendAck {
    /// The ids, one per offered event, in order.
    pub fn ids(&self) -> Vec<&str> {
        self.events.iter().map(|e| e.id.as_str()).collect()
    }

    /// Events this append stored.
    pub fn stored(&self) -> u64 {
        self.events.len() as u64 - self.duplicates
    }
}

/// A live range claim, as [`heads`](crate::FeedStore::heads) reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimInfo {
    /// The claim's id.
    pub id: ClaimId,
    /// The exclusive end of the claimed range.
    pub to: Seq,
    /// Who took it.
    pub owner: ClaimOwner,
    /// When it ends unless renewed, by the store's clock.
    pub expires_at: Timestamp,
    /// Whether a reader has attached.
    pub attached: bool,
}

/// One delivery's position, as [`heads`](crate::FeedStore::heads) reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryHead {
    /// The delivery.
    pub delivery: DeliveryUid,
    /// The next sequence number it reads.
    pub cursor: Seq,
    /// Its epoch.
    pub epoch: Epoch,
    /// Its live claim, if any.
    pub claim: Option<ClaimInfo>,
    /// When the store committed the oldest event it has not delivered.
    pub oldest_undelivered_at: Option<Timestamp>,
}

/// A feed's committed state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    /// The feed.
    pub feed: FeedUid,
    /// Its tenant.
    pub tenant: Option<TenantId>,
    /// The highest committed sequence number (`0` before the first event). No
    /// event at or below it can still appear.
    pub watermark: Seq,
    /// Payload bytes the feed holds.
    pub bytes: u64,
    /// The oldest retained sequence number ([`end`](Self::end) when empty).
    pub oldest: Seq,
    /// The store's clock when this was read.
    pub now: Timestamp,
    /// The feed's deliveries.
    pub deliveries: Vec<DeliveryHead>,
}

impl Head {
    /// The exclusive end of the readable range: the next sequence number the
    /// feed will assign, and the highest value a cursor may take.
    pub fn end(&self) -> Seq {
        self.watermark.next()
    }
}

/// Events `[from, to)` of a feed, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// The feed.
    pub feed: FeedUid,
    /// The first sequence number read.
    pub from: Seq,
    /// The exclusive end actually read (never past the watermark).
    pub to: Seq,
    /// The events, one per sequence number.
    pub events: Vec<StoredEvent>,
}

impl Batch {
    /// Payload bytes in the batch.
    pub fn bytes(&self) -> u64 {
        self.events.iter().map(|e| e.size).sum()
    }
}

/// The most one read should take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    /// Most events.
    pub events: u64,
    /// Most payload bytes; one event larger than this still forms a batch.
    pub bytes: u64,
}

impl BatchLimits {
    /// Limits of `events` events and `bytes` payload bytes.
    pub fn new(events: u64, bytes: u64) -> Self {
        Self { events, bytes }
    }
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            events: 10_000,
            bytes: 64 * 1024 * 1024,
        }
    }
}

/// A delivery's stored position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryState {
    /// The next sequence number it reads.
    pub cursor: Seq,
    /// Its epoch.
    pub epoch: Epoch,
    /// Whether this call created the delivery.
    pub created: bool,
}

/// Where a new delivery starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// The oldest retained event.
    Earliest,
    /// After the last committed event.
    Latest,
    /// At this sequence number, which must be retained or the feed's end.
    Seq(Seq),
    /// At the first event the store committed at or after this time.
    Time(Timestamp),
}

/// A granted range claim over `[from, to)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// The claim's id.
    pub id: ClaimId,
    /// The delivery it belongs to.
    pub delivery: DeliveryUid,
    /// The delivery's cursor when it was taken.
    pub from: Seq,
    /// The exclusive end of the range.
    pub to: Seq,
    /// Who took it.
    pub owner: ClaimOwner,
    /// The delivery's epoch when it was taken.
    pub epoch: Epoch,
}

/// A reader attached to a claim. Its token fences out every earlier holder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// The claim.
    pub claim: ClaimId,
    /// This holder's token.
    pub token: String,
    /// The delivery the claim belongs to.
    pub delivery: DeliveryUid,
    /// The delivery's cursor when this holder attached.
    pub cursor: Seq,
    /// The exclusive end of the claimed range.
    pub to: Seq,
    /// The delivery's epoch.
    pub epoch: Epoch,
}

/// Why a cursor moves outside a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Move {
    /// Back, to deliver retained events again.
    Replay,
    /// Forward past events that will not be delivered; always records a gap.
    Skip {
        /// Why they are skipped.
        reason: String,
        /// Who skipped them.
        by: String,
    },
    /// To a valid position after the store was restored; records a gap.
    Recover,
}

/// A range a delivery did not deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    /// The feed.
    pub feed: FeedUid,
    /// The delivery.
    pub delivery: Option<DeliveryUid>,
    /// The first sequence number of the range.
    pub from: Seq,
    /// The exclusive end of the range.
    pub to: Seq,
    /// Why.
    pub reason: String,
    /// When, by the store's clock.
    pub at: Timestamp,
    /// Who.
    pub by: String,
}

/// A feed's retention and admission limits, checked inside the append
/// transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLimits {
    /// How long a delivered event stays for replay.
    pub max_age: Duration,
    /// How long a dedupe key is remembered; zero turns dedupe off.
    pub dedupe_window: Duration,
    /// Most payload bytes the feed's tenant may hold across its feeds.
    pub tenant_max_bytes: Option<u64>,
    /// Most events not yet delivered to every delivery.
    pub undelivered_max_events: Option<u64>,
    /// Most payload bytes not yet delivered to every delivery.
    pub undelivered_max_bytes: Option<u64>,
    /// Greatest age of an event not yet delivered to every delivery.
    pub undelivered_max_age: Option<Duration>,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(7 * 24 * 60 * 60),
            dedupe_window: Duration::from_secs(24 * 60 * 60),
            tenant_max_bytes: None,
            undelivered_max_events: None,
            undelivered_max_bytes: None,
            undelivered_max_age: None,
        }
    }
}

impl StoreLimits {
    /// The defaults: 7 days of replay, a 24 hour dedupe window, no ceilings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the replay window.
    pub fn max_age(mut self, age: Duration) -> Self {
        self.max_age = age;
        self
    }

    /// Set the dedupe window.
    pub fn dedupe_window(mut self, window: Duration) -> Self {
        self.dedupe_window = window;
        self
    }

    /// Set the tenant's stored-bytes quota.
    pub fn tenant_max_bytes(mut self, bytes: u64) -> Self {
        self.tenant_max_bytes = Some(bytes);
        self
    }

    /// Set the undelivered event ceiling.
    pub fn undelivered_max_events(mut self, events: u64) -> Self {
        self.undelivered_max_events = Some(events);
        self
    }

    /// Set the undelivered bytes ceiling.
    pub fn undelivered_max_bytes(mut self, bytes: u64) -> Self {
        self.undelivered_max_bytes = Some(bytes);
        self
    }

    /// Set the undelivered age ceiling.
    pub fn undelivered_max_age(mut self, age: Duration) -> Self {
        self.undelivered_max_age = Some(age);
        self
    }
}

/// The limit an append would cross.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FullLimit {
    /// The tenant's stored-bytes quota.
    TenantBytes,
    /// The feed's undelivered event ceiling.
    UndeliveredEvents,
    /// The feed's undelivered bytes ceiling.
    UndeliveredBytes,
    /// The feed's undelivered age ceiling, in milliseconds.
    UndeliveredAge,
}

impl FullLimit {
    /// A stable label.
    pub fn as_str(self) -> &'static str {
        match self {
            FullLimit::TenantBytes => "tenant_bytes",
            FullLimit::UndeliveredEvents => "undelivered_events",
            FullLimit::UndeliveredBytes => "undelivered_bytes",
            FullLimit::UndeliveredAge => "undelivered_age",
        }
    }
}

impl fmt::Display for FullLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a trim removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrimStats {
    /// Events removed.
    pub events: u64,
    /// Payload bytes removed.
    pub bytes: u64,
    /// Whether the row limit stopped the trim before it ran out of events.
    pub more: bool,
}

/// What a purge removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PurgeStats {
    /// Events removed.
    pub events: u64,
    /// Deliveries (cursors and claims) removed.
    pub deliveries: u64,
    /// Dedupe rows removed.
    pub dedupe: u64,
    /// Gap rows removed.
    pub gaps: u64,
}

/// What a store backend can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreCaps {
    /// The backend's name.
    pub backend: &'static str,
    /// Whether several servers may share the store.
    pub cluster_safe: bool,
}
