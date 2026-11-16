//! The feed store's typed error.

use crate::types::{ClaimId, DeliveryUid, FeedUid, FullLimit, Seq, TenantId};
use faucet_core::FaucetError;

/// Why a feed-store operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FeedError {
    /// An id is empty, too long or holds a character outside the allowed set.
    #[error("invalid {kind} id {value:?}: use 1 to 128 ASCII letters, digits or `- _ . : /`")]
    InvalidId {
        /// What the id names.
        kind: &'static str,
        /// The refused text.
        value: String,
    },

    /// The store has no such feed.
    #[error("feed {0} is not in the store")]
    FeedNotFound(FeedUid),

    /// The store has no such delivery.
    #[error("delivery {0} is not in the store")]
    DeliveryNotFound(DeliveryUid),

    /// The feed is stored under another tenant.
    #[error("feed {feed} belongs to {}", .stored.as_ref().map(|t| format!("tenant {t}")).unwrap_or_else(|| "no tenant".into()))]
    TenantMismatch {
        /// The feed.
        feed: FeedUid,
        /// The tenant it is stored under.
        stored: Option<TenantId>,
    },

    /// The append would cross a limit; nothing was stored.
    #[error("feed {feed} is full: {limit} would reach {actual} of {max}")]
    Full {
        /// The feed.
        feed: FeedUid,
        /// The limit.
        limit: FullLimit,
        /// Its value.
        max: u64,
        /// What the append would have brought it to.
        actual: u64,
    },

    /// The claim expired, was released or replaced, or a newer holder attached.
    #[error("claim {claim} is no longer held")]
    ClaimLost {
        /// The claim.
        claim: ClaimId,
    },

    /// The delivery has a live claim, so its cursor cannot be moved.
    #[error("delivery {delivery} has a live claim")]
    ClaimLive {
        /// The delivery.
        delivery: DeliveryUid,
    },

    /// The cursor is not where the caller expected it.
    #[error("delivery {delivery} cursor is {actual}, not {expected}")]
    CursorConflict {
        /// The delivery.
        delivery: DeliveryUid,
        /// The cursor the caller expected.
        expected: Seq,
        /// The stored cursor.
        actual: Seq,
    },

    /// A position past the feed's committed end.
    #[error("sequence {requested} is past the end of feed {feed} ({end})")]
    PastWatermark {
        /// The feed.
        feed: FeedUid,
        /// The refused position.
        requested: Seq,
        /// The feed's exclusive end.
        end: Seq,
    },

    /// A position outside the retained events.
    #[error("sequence {requested} is outside feed {feed}'s retained events [{oldest}, {end}]")]
    OutOfRange {
        /// The feed.
        feed: FeedUid,
        /// The refused position.
        requested: Seq,
        /// The oldest retained sequence number.
        oldest: Seq,
        /// The feed's exclusive end.
        end: Seq,
    },

    /// Events of the requested range are no longer in the store.
    #[error(
        "feed {feed} no longer holds every event of [{from}, {to}); the oldest retained is {oldest}"
    )]
    RangeUnavailable {
        /// The feed.
        feed: FeedUid,
        /// The range start.
        from: Seq,
        /// The range's exclusive end.
        to: Seq,
        /// The oldest retained sequence number.
        oldest: Seq,
    },

    /// The arguments do not describe a valid move or range.
    #[error("invalid request: {0}")]
    Invalid(String),

    /// The store was written by a newer release.
    #[error("the feed store has format version {found}; this release reads up to {supported}")]
    NewerSchema {
        /// The stored version.
        found: i64,
        /// The newest version this release reads.
        supported: i64,
    },

    /// The backend is busy, unreachable, timed out or out of space; retry later.
    #[error("feed store unavailable: {0}")]
    Unavailable(String),

    /// The backend refused a statement.
    #[error("feed store error: {0}")]
    Backend(String),

    /// A stored row cannot be read.
    #[error("feed store holds an unreadable row: {0}")]
    Corrupt(String),

    /// The call was cancelled or ran out of time.
    #[error(transparent)]
    Call(FaucetError),
}

impl FeedError {
    /// Whether the same request may succeed later without a change.
    pub fn is_transient(&self) -> bool {
        match self {
            FeedError::Unavailable(_) => true,
            FeedError::Call(e) => e.is_retriable(),
            _ => false,
        }
    }
}

impl From<FeedError> for FaucetError {
    fn from(err: FeedError) -> Self {
        match err {
            FeedError::Call(e) => e,
            other => FaucetError::Custom(Box::new(other)),
        }
    }
}

/// A feed-store result.
pub type Result<T> = std::result::Result<T, FeedError>;
