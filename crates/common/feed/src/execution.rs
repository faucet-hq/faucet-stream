//! The run-scoped handle a `feed` source reads through.
//!
//! A host that runs a delivery scopes a [`FeedExecution`] over the run's task
//! ([`scope`]); the source picks it up with [`current`]. The store, the claim
//! and the payload opener therefore never enter a run config and never live
//! in a process-global, so several hosts in one process do not cross.

use crate::store::FeedStore;
use crate::types::{ClaimId, DeliveryUid, Epoch, FeedUid, Seq, TenantId};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::sync::Arc;

/// What a delivery run is bound to: names and ids only, safe to store on a
/// run record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FeedBinding {
    /// The feed's uid.
    pub feed: FeedUid,
    /// The delivery's uid.
    pub delivery: DeliveryUid,
    /// The delivery's epoch when the range was claimed.
    pub epoch: Epoch,
    /// The claim the run reads under.
    pub claim: ClaimId,
    /// The first sequence number of the claimed range.
    pub from: Seq,
    /// The exclusive end of the claimed range.
    pub to: Seq,
    /// The name of the server-defined store that holds the feed.
    pub store: String,
}

impl FeedBinding {
    /// A binding of `delivery` to the claim `claim` over `[from, to)` in the
    /// store named `store`.
    pub fn new(
        delivery: DeliveryUid,
        epoch: Epoch,
        claim: ClaimId,
        from: Seq,
        to: Seq,
        store: impl Into<String>,
    ) -> Self {
        Self {
            feed: delivery.feed().clone(),
            delivery,
            epoch,
            claim,
            from,
            to,
            store: store.into(),
        }
    }
}

/// Why a sealed payload could not be opened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct OpenError(pub String);

/// Opens a sealed payload: `(sealed bytes, sealing context) -> plain bytes`.
/// The context is [`sealing_context`].
pub type PayloadOpener = Arc<dyn Fn(&[u8], &str) -> Result<Vec<u8>, OpenError> + Send + Sync>;

/// The context an event payload of `feed` is sealed under:
/// `feed:{uid}:tenant:{tenant|-}:event`.
pub fn sealing_context(feed: &FeedUid, tenant: Option<&TenantId>) -> String {
    format!(
        "feed:{feed}:tenant:{}:event",
        tenant.map_or("-", TenantId::as_str)
    )
}

/// Everything a bound `feed` source needs for one run.
#[derive(Clone)]
#[non_exhaustive]
pub struct FeedExecution {
    /// The claim the run reads under.
    pub binding: FeedBinding,
    /// The store that holds the feed, resolved by the executing host.
    pub store: Arc<dyn FeedStore>,
    /// Opens sealed payloads; `None` when the host has no key.
    pub opener: Option<PayloadOpener>,
    /// The feed's name (`tenant/name` for a tenant feed), for the dataset URI.
    pub dataset: Option<String>,
}

impl FeedExecution {
    /// A handle for `binding` over `store`, with no opener.
    pub fn new(binding: FeedBinding, store: Arc<dyn FeedStore>) -> Self {
        Self {
            binding,
            store,
            opener: None,
            dataset: None,
        }
    }

    /// Set the sealed-payload opener.
    pub fn opener(mut self, opener: PayloadOpener) -> Self {
        self.opener = Some(opener);
        self
    }

    /// Set the feed's name as lineage reports it (`tenant/name` for a tenant
    /// feed).
    pub fn dataset(mut self, dataset: impl Into<String>) -> Self {
        self.dataset = Some(dataset.into());
        self
    }
}

impl fmt::Debug for FeedExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeedExecution")
            .field("binding", &self.binding)
            .field("store", &self.store.capabilities().backend)
            .field("opener", &self.opener.is_some())
            .field("dataset", &self.dataset)
            .finish()
    }
}

tokio::task_local! {
    static SCOPED: FeedExecution;
}

/// Run `fut` with `execution` as the feed handle of the current task. A task
/// spawned from `fut` does not see it unless wrapped with [`inherit`].
pub async fn scope<F: Future>(execution: FeedExecution, fut: F) -> F::Output {
    SCOPED.scope(execution, fut).await
}

/// The feed handle scoped over the current task, if any.
pub fn current() -> Option<FeedExecution> {
    SCOPED.try_with(Clone::clone).ok()
}

/// `fut` under the feed handle of the current task, for a future about to be
/// spawned onto another task.
pub fn inherit<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    let execution = current();
    async move {
        match execution {
            Some(execution) => SCOPED.scope(execution, fut).await,
            None => fut.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stalled::StalledStore;

    fn execution(store: &'static str) -> FeedExecution {
        let feed = FeedUid::new(format!("feed-{store}")).unwrap();
        let delivery = DeliveryUid::new(feed, "main").unwrap();
        let binding = FeedBinding::new(
            delivery,
            Epoch(2),
            ClaimId::new("claim-1").unwrap(),
            Seq(4),
            Seq(9),
            store,
        );
        FeedExecution::new(binding, Arc::new(StalledStore::named(store)))
    }

    #[tokio::test]
    async fn the_handle_is_visible_inside_its_scope_and_nowhere_else() {
        assert!(current().is_none());
        let seen = scope(execution("a"), async { current() }).await.unwrap();
        assert_eq!(seen.binding.store, "a");
        assert_eq!(seen.binding.feed.as_str(), "feed-a");
        assert_eq!((seen.binding.from, seen.binding.to), (Seq(4), Seq(9)));
        assert!(seen.opener.is_none());
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn a_spawned_task_sees_the_handle_only_when_it_inherits_it() {
        let (inherited, plain) = scope(execution("a"), async {
            let inherited = tokio::spawn(inherit(async { current().map(|e| e.binding.store) }));
            let plain = tokio::spawn(async { current().is_some() });
            (inherited.await.unwrap(), plain.await.unwrap())
        })
        .await;
        assert_eq!(inherited.as_deref(), Some("a"));
        assert!(!plain);
        assert!(inherit(async { current().is_none() }).await);
    }

    #[tokio::test]
    async fn concurrent_scopes_do_not_cross() {
        let read = |name: &'static str| {
            scope(execution(name), async move {
                for _ in 0..20 {
                    tokio::task::yield_now().await;
                    let seen = current().unwrap();
                    assert_eq!(seen.binding.store, name);
                    assert_eq!(seen.store.capabilities().backend, name);
                }
                name
            })
        };
        let (a, b) = tokio::join!(tokio::spawn(read("a")), tokio::spawn(read("b")));
        assert_eq!((a.unwrap(), b.unwrap()), ("a", "b"));
    }

    #[tokio::test]
    async fn the_handle_carries_an_opener_and_a_dataset_name() {
        let opener: PayloadOpener = Arc::new(|sealed, context| {
            if context.ends_with(":event") {
                Ok(sealed.iter().rev().copied().collect())
            } else {
                Err(OpenError("wrong context".into()))
            }
        });
        let exec = execution("a").opener(opener).dataset("acme/orders");
        let feed = exec.binding.feed.clone();
        let tenant = TenantId::new("acme").unwrap();
        let open = exec.opener.clone().unwrap();
        assert_eq!(
            open(b"abc", &sealing_context(&feed, Some(&tenant))).unwrap(),
            b"cba"
        );
        assert_eq!(
            open(b"abc", "other").unwrap_err().to_string(),
            "wrong context"
        );
        assert_eq!(sealing_context(&feed, None), "feed:feed-a:tenant:-:event");
        assert_eq!(
            sealing_context(&feed, Some(&tenant)),
            "feed:feed-a:tenant:acme:event"
        );
        let shown = format!("{exec:?}");
        assert!(
            shown.contains("acme/orders") && shown.contains("opener: true"),
            "{shown}"
        );
    }

    #[test]
    fn a_binding_round_trips_through_json() {
        let binding = execution("default").binding;
        let text = serde_json::to_string(&binding).unwrap();
        let back: FeedBinding = serde_json::from_str(&text).unwrap();
        assert_eq!(back, binding);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["store"], "default");
        assert_eq!(value["claim"], "claim-1");
        assert_eq!(value["from"], 4);
    }
}
