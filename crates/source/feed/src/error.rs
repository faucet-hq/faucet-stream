//! The feed source's typed errors.

use faucet_core::FaucetError;

/// Why a feed read stopped. Carried inside [`FaucetError::Custom`]; find it
/// with [`FeedSourceError::of`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FeedSourceError {
    /// The stored bookmark belongs to another feed or delivery: two pipelines
    /// share one state key.
    #[error(
        "the stored bookmark belongs to delivery {found}, not {expected}: two pipelines are \
         sharing one state key, so each would overwrite the other's position and a page could \
         be written twice. Give this pipeline its own `name`"
    )]
    SharedState {
        /// The delivery this run reads.
        expected: String,
        /// The delivery the bookmark names.
        found: String,
    },

    /// The sink's bookmark points at an event the store does not hold: the
    /// store was restored and sequence numbers were reused.
    #[error(
        "delivery {delivery} is held: the destination's bookmark says event {seq} ({id}) was \
         written, but the feed store {reason}. The store was restored from an older copy; \
         nothing was read or skipped. Run `faucet feed recover` for this delivery"
    )]
    Held {
        /// The delivery.
        delivery: String,
        /// The sequence number the bookmark names.
        seq: u64,
        /// The event id the bookmark names.
        id: String,
        /// What the store holds instead.
        reason: String,
    },

    /// Another reader holds the delivery's range.
    #[error(
        "delivery {delivery} is being read by another run (claim owner {owner}); a second \
         reader cannot start until that run ends or its claim expires"
    )]
    ClaimBusy {
        /// The delivery.
        delivery: String,
        /// Who holds the claim, when the store reports it.
        owner: String,
    },

    /// The range claim expired, was released or replaced, or a newer attempt
    /// of the run attached to it.
    #[error(
        "the range claim on delivery {delivery} is no longer held by this run (it expired, was \
         replaced, or a newer attempt attached); the read stopped before committing"
    )]
    ClaimLost {
        /// The delivery.
        delivery: String,
    },

    /// A sealed event cannot be opened.
    #[error("event {seq} ({id}) of feed {feed} is sealed and cannot be opened: {reason}")]
    Sealed {
        /// The feed.
        feed: String,
        /// The event's sequence number.
        seq: u64,
        /// The event's id.
        id: String,
        /// Why.
        reason: String,
    },

    /// A stored event does not decode as its content type says.
    #[error("event {seq} ({id}) of feed {feed} is not valid {content_type}: {reason}")]
    Undecodable {
        /// The feed.
        feed: String,
        /// The event's sequence number.
        seq: u64,
        /// The event's id.
        id: String,
        /// The stored content type.
        content_type: String,
        /// Why.
        reason: String,
    },

    /// The feed or the delivery is not in the store.
    #[error("{what} is not in the feed store; create it with `faucet feed create`")]
    Missing {
        /// What is missing, e.g. `feed orders`.
        what: String,
    },

    /// The buffered fetch was called.
    #[error(
        "the feed source is read only by a pipeline's streaming read: a buffered fetch would \
         move the delivery's position past events nobody wrote"
    )]
    BufferedFetch,
}

impl FeedSourceError {
    /// The feed source error inside `err`, if that is what it is.
    pub fn of(err: &FaucetError) -> Option<&FeedSourceError> {
        match err {
            FaucetError::Custom(inner) => inner.downcast_ref::<FeedSourceError>(),
            _ => None,
        }
    }
}

impl From<FeedSourceError> for FaucetError {
    fn from(err: FeedSourceError) -> Self {
        FaucetError::Custom(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_error_is_found_inside_a_faucet_error() {
        let err: FaucetError = FeedSourceError::BufferedFetch.into();
        assert_eq!(
            FeedSourceError::of(&err),
            Some(&FeedSourceError::BufferedFetch)
        );
        assert!(FeedSourceError::of(&FaucetError::Config("x".into())).is_none());
        let store: FaucetError = faucet_common_feed::FeedError::Invalid("x".into()).into();
        assert!(FeedSourceError::of(&store).is_none());
    }

    #[test]
    fn the_messages_name_the_remedy() {
        let shared = FeedSourceError::SharedState {
            expected: "a/x".into(),
            found: "b/y".into(),
        };
        assert!(shared.to_string().contains("its own `name`"));
        let held = FeedSourceError::Held {
            delivery: "a/x".into(),
            seq: 7,
            id: "e7".into(),
            reason: "ends at event 3".into(),
        };
        let text = held.to_string();
        assert!(text.contains("is held") && text.contains("restored") && text.contains("recover"));
        let missing = FeedSourceError::Missing {
            what: "feed orders".into(),
        };
        assert!(missing.to_string().contains("faucet feed create"));
    }
}
