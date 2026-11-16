//! The feed source's bookmark and the resume rules.

use crate::error::FeedSourceError;
use faucet_common_feed::{DeliveryUid, Epoch, Seq};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The position a page leaves the delivery at. `next` is the cursor once the
/// page is delivered; `id` is the id of the page's last event, which proves
/// the store still holds the events the bookmark speaks of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedBookmark {
    /// The feed's uid.
    pub feed: String,
    /// The delivery's id within the feed.
    pub delivery: String,
    /// The delivery's epoch when the page was read.
    pub epoch: u64,
    /// The cursor once the page is delivered.
    pub next: u64,
    /// The id of the last event in the page.
    pub id: String,
}

impl FeedBookmark {
    /// The bookmark of a page of `delivery` that ends with event `last`
    /// (`id`).
    pub fn after(delivery: &DeliveryUid, epoch: Epoch, last: Seq, id: &str) -> Self {
        Self {
            feed: delivery.feed().to_string(),
            delivery: delivery.id().to_string(),
            epoch: epoch.0,
            next: last.next().get(),
            id: id.to_string(),
        }
    }

    /// The bookmark as the pipeline stores it.
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            "feed": self.feed,
            "delivery": self.delivery,
            "epoch": self.epoch,
            "next": self.next,
            "id": self.id,
        })
    }
}

/// What a stored bookmark asks of a read that starts at `cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resume {
    /// Read from the cursor.
    FromCursor,
    /// The sink holds a page the cursor does not: prove event `next - 1` is
    /// `id`, then move the cursor to `next`.
    Ahead { next: Seq, id: String },
}

/// Apply the resume rules to a stored bookmark.
pub(crate) fn resume(
    stored: Option<&Value>,
    delivery: &DeliveryUid,
    epoch: Epoch,
    cursor: Seq,
) -> Result<Resume, FeedSourceError> {
    let Some(stored) = stored else {
        return Ok(Resume::FromCursor);
    };
    let shared = |found: String| FeedSourceError::SharedState {
        expected: delivery.to_string(),
        found,
    };
    let bookmark: FeedBookmark =
        serde_json::from_value(stored.clone()).map_err(|_| shared("another source".to_string()))?;
    if bookmark.feed != delivery.feed().as_str() || bookmark.delivery != delivery.id() {
        return Err(shared(format!("{}/{}", bookmark.feed, bookmark.delivery)));
    }
    if bookmark.epoch != epoch.0 || bookmark.next <= cursor.get() {
        return Ok(Resume::FromCursor);
    }
    Ok(Resume::Ahead {
        next: Seq(bookmark.next),
        id: bookmark.id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_feed::FeedUid;
    use serde_json::json;

    fn delivery() -> DeliveryUid {
        DeliveryUid::new(FeedUid::new("orders").unwrap(), "warehouse").unwrap()
    }

    fn stored(next: u64, epoch: u64) -> Value {
        json!({"feed": "orders", "delivery": "warehouse", "epoch": epoch, "next": next, "id": "e"})
    }

    #[test]
    fn the_bookmark_has_the_frozen_shape() {
        let bookmark = FeedBookmark::after(&delivery(), Epoch(3), Seq(9), "0193-e9");
        assert_eq!(
            bookmark.to_value(),
            json!({"feed": "orders", "delivery": "warehouse", "epoch": 3, "next": 10, "id": "0193-e9"})
        );
        assert_eq!(
            serde_json::from_value::<FeedBookmark>(bookmark.to_value()).unwrap(),
            bookmark
        );
    }

    #[test]
    fn no_bookmark_reads_from_the_cursor() {
        assert_eq!(
            resume(None, &delivery(), Epoch(0), Seq(5)),
            Ok(Resume::FromCursor)
        );
    }

    #[test]
    fn another_feed_or_delivery_is_refused_with_the_remedy() {
        for other in [
            json!({"feed": "billing", "delivery": "warehouse", "epoch": 0, "next": 5, "id": "e"}),
            json!({"feed": "orders", "delivery": "lake", "epoch": 0, "next": 5, "id": "e"}),
            json!({"offset": 12}),
            json!("2026-01-01"),
        ] {
            let err = resume(Some(&other), &delivery(), Epoch(0), Seq(5)).unwrap_err();
            assert!(matches!(err, FeedSourceError::SharedState { .. }), "{err}");
            assert!(err.to_string().contains("its own `name`"), "{err}");
        }
    }

    #[test]
    fn another_epoch_is_ignored() {
        assert_eq!(
            resume(Some(&stored(50, 1)), &delivery(), Epoch(2), Seq(5)),
            Ok(Resume::FromCursor)
        );
    }

    #[test]
    fn a_bookmark_at_or_behind_the_cursor_is_silent() {
        for next in [5, 4, 1] {
            assert_eq!(
                resume(Some(&stored(next, 2)), &delivery(), Epoch(2), Seq(5)),
                Ok(Resume::FromCursor)
            );
        }
    }

    #[test]
    fn a_bookmark_ahead_of_the_cursor_asks_for_proof() {
        assert_eq!(
            resume(Some(&stored(8, 2)), &delivery(), Epoch(2), Seq(5)),
            Ok(Resume::Ahead {
                next: Seq(8),
                id: "e".into()
            })
        );
    }
}
