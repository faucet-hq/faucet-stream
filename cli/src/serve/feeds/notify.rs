//! Feed notifications (#1034): `feed_near_full` and `feed_stalled`, sent
//! through each delivery's `notifications:` and, for a tenant feed, the
//! tenant's. Each condition sends once across every server until it clears:
//! a stall is recorded in the delivery's shared status, a crossing of a
//! `retention.warn_at` threshold in a shared counter.

use super::Feed;
use crate::serve::history::RunHistory;
use crate::serve::state::ServerState;
use std::sync::Arc;

const ATTEMPTS: usize = 5;

/// What happened to a feed.
#[derive(Debug, Clone, PartialEq)]
pub enum FeedEvent {
    /// Undelivered events reached `threshold` of the feed's limits.
    NearFull { ratio: f64, threshold: f64 },
    /// `delivery` stalled on event `seq`.
    Stalled {
        delivery: String,
        seq: u64,
        error: String,
    },
}

/// The settings key of the number of `warn_at` thresholds a feed is past.
pub fn warned_key(feed_uid: &str) -> String {
    format!("feed_warned:{feed_uid}")
}

/// Record that the feed is past `crossed` thresholds. `Ok(true)` when this
/// call raised the shared count: the caller sends the notification. A
/// lowered count (the feed drained) is stored so the next crossing sends
/// again.
pub async fn crossing(
    history: &dyn RunHistory,
    feed_uid: &str,
    crossed: usize,
) -> Result<bool, String> {
    let key = warned_key(feed_uid);
    for _ in 0..ATTEMPTS {
        let current = history.meta_get(&key).await.map_err(|e| e.to_string())?;
        let stored: usize = current.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0);
        if stored == crossed {
            return Ok(false);
        }
        if history
            .meta_compare_and_set(&key, current.as_deref(), &crossed.to_string())
            .await
            .map_err(|e| e.to_string())?
        {
            return Ok(crossed > stored);
        }
    }
    Err("the feed's warning count kept changing".into())
}

/// Forget a deleted feed's warning count.
pub async fn forget(history: &dyn RunHistory, feed_uid: &str) {
    if let Err(e) = history.meta_put(&warned_key(feed_uid), "").await {
        tracing::warn!(feed = feed_uid, error = %e, "could not clear a deleted feed's warning count");
    }
}

/// Send `event` in the background: a slow channel never holds the scheduler.
pub fn spawn(state: &ServerState, feed: Arc<Feed>, event: FeedEvent) {
    let state = state.clone();
    tokio::spawn(async move { send(&state, &feed, event).await });
}

/// Send `event` through the notifications of the deliveries it concerns and
/// of the feed's tenant. Never fails: a channel that cannot be built or
/// reached is logged.
#[cfg(feature = "notify")]
pub async fn send(state: &ServerState, feed: &Feed, event: FeedEvent) {
    use crate::notify::{Notifier, NotifyEvent};
    let label = feed.label();
    let (deliveries, notify): (Vec<_>, NotifyEvent) = match &event {
        FeedEvent::NearFull { ratio, threshold } => (
            feed.spec.deliveries.iter().collect(),
            NotifyEvent::feed_near_full(&label, *ratio, *threshold),
        ),
        FeedEvent::Stalled {
            delivery,
            seq,
            error,
        } => (
            feed.spec.delivery(delivery).into_iter().collect(),
            NotifyEvent::feed_stalled(&label, delivery, *seq, error),
        ),
    };
    let mut sent = std::collections::HashSet::new();
    for d in deliveries {
        let Some(dref) = feed.delivery(&d.name) else {
            continue;
        };
        let loaded = match super::service::delivery_config(state, feed, d, dref).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(feed = %label, delivery = %d.name, error = %e.api_error().error.message, "feed notification skipped: the delivery's config does not load");
                continue;
            }
        };
        let specs = &loaded.cfg.notifications;
        if !sent.insert(serde_json::to_string(specs).unwrap_or_default()) {
            continue;
        }
        match Notifier::from_specs(specs) {
            Ok(Some(n)) => n.emit(notify.clone()).await,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(feed = %label, delivery = %d.name, error = %e, "feed notification skipped")
            }
        }
    }
    #[cfg(feature = "tenants")]
    if let Some(tenant) = feed.tenant() {
        crate::serve::tenants::notify_tenant(state, tenant, notify).await;
    }
}

#[cfg(not(feature = "notify"))]
pub async fn send(state: &ServerState, feed: &Feed, event: FeedEvent) {
    let _ = (state, feed, event);
}
