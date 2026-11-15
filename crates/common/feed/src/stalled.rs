//! A store double whose every call waits until its call context ends.

use crate::error::{FeedError, Result};
use crate::store::FeedStore;
use crate::types::{
    AppendAck, Batch, BatchLimits, Claim, ClaimId, ClaimOwner, DeliveryState, DeliveryUid, Epoch,
    FeedUid, Gap, Head, Holder, Move, NewEvent, PurgeStats, Seq, Start, StoreCaps, StoreLimits,
    TenantId, Timestamp, TrimStats,
};
use async_trait::async_trait;
use faucet_core::io::CallContext;
use std::time::Duration;

/// A [`FeedStore`] that never answers: every call waits through its
/// [`CallContext`] and ends only with the context's timeout, deadline or
/// cancel. For tests of what a caller does when the store hangs.
#[derive(Debug, Clone, Copy)]
pub struct StalledStore(&'static str);

impl StalledStore {
    /// A stalled store that reports the backend name `stalled`.
    pub fn new() -> Self {
        Self("stalled")
    }

    /// A stalled store that reports `backend` as its name.
    pub fn named(backend: &'static str) -> Self {
        Self(backend)
    }
}

impl Default for StalledStore {
    fn default() -> Self {
        Self::new()
    }
}

async fn stall<T>(calls: &CallContext, op: &'static str) -> Result<T> {
    match calls
        .call_once(
            op,
            std::future::pending::<std::result::Result<T, faucet_core::FaucetError>>(),
        )
        .await
    {
        Ok(value) => Ok(value),
        Err(e) => Err(FeedError::Call(e)),
    }
}

#[async_trait]
impl FeedStore for StalledStore {
    async fn ensure_feed(
        &self,
        calls: &CallContext,
        _: &FeedUid,
        _: Option<&TenantId>,
        _: &StoreLimits,
    ) -> Result<()> {
        stall(calls, "ensure_feed").await
    }

    async fn ensure_delivery(
        &self,
        calls: &CallContext,
        _: &DeliveryUid,
        _: Start,
    ) -> Result<DeliveryState> {
        stall(calls, "ensure_delivery").await
    }

    async fn delete_delivery(&self, calls: &CallContext, _: &DeliveryUid) -> Result<()> {
        stall(calls, "delete_delivery").await
    }

    async fn purge_feed(&self, calls: &CallContext, _: &FeedUid) -> Result<PurgeStats> {
        stall(calls, "purge_feed").await
    }

    async fn append(
        &self,
        calls: &CallContext,
        _: &FeedUid,
        _: Vec<NewEvent>,
    ) -> Result<AppendAck> {
        stall(calls, "append").await
    }

    async fn heads(&self, calls: &CallContext, _: &[FeedUid]) -> Result<Vec<Head>> {
        stall(calls, "heads").await
    }

    async fn read(&self, calls: &CallContext, _: &FeedUid, _: Seq, _: Seq) -> Result<Batch> {
        stall(calls, "read").await
    }

    async fn bound(&self, calls: &CallContext, _: &FeedUid, _: Seq, _: BatchLimits) -> Result<Seq> {
        stall(calls, "bound").await
    }

    async fn seq_at(&self, calls: &CallContext, _: &FeedUid, _: Timestamp) -> Result<Seq> {
        stall(calls, "seq_at").await
    }

    async fn claim(
        &self,
        calls: &CallContext,
        _: &DeliveryUid,
        _: Seq,
        _: Seq,
        _: &ClaimOwner,
        _: Duration,
    ) -> Result<Option<Claim>> {
        stall(calls, "claim").await
    }

    async fn attach(&self, calls: &CallContext, _: &ClaimId) -> Result<Holder> {
        stall(calls, "attach").await
    }

    async fn renew(&self, calls: &CallContext, _: &Holder, _: Duration) -> Result<bool> {
        stall(calls, "renew").await
    }

    async fn renew_claim(&self, calls: &CallContext, _: &ClaimId, _: Duration) -> Result<bool> {
        stall(calls, "renew_claim").await
    }

    async fn commit(&self, calls: &CallContext, _: &Holder, _: Seq, _: Seq) -> Result<()> {
        stall(calls, "commit").await
    }

    async fn release(&self, calls: &CallContext, _: &ClaimId) -> Result<()> {
        stall(calls, "release").await
    }

    async fn move_cursor(
        &self,
        calls: &CallContext,
        _: &DeliveryUid,
        _: Seq,
        _: Seq,
        _: Move,
    ) -> Result<Epoch> {
        stall(calls, "move_cursor").await
    }

    async fn gaps(
        &self,
        calls: &CallContext,
        _: &FeedUid,
        _: Option<&DeliveryUid>,
    ) -> Result<Vec<Gap>> {
        stall(calls, "gaps").await
    }

    async fn trim(&self, calls: &CallContext, _: &FeedUid, _: u32) -> Result<TrimStats> {
        stall(calls, "trim").await
    }

    async fn purge_dedupe(&self, calls: &CallContext, _: &FeedUid, _: u32) -> Result<u64> {
        stall(calls, "purge_dedupe").await
    }

    async fn maintenance(
        &self,
        calls: &CallContext,
        _: &FeedUid,
        _: &ClaimOwner,
        _: Duration,
    ) -> Result<bool> {
        stall(calls, "maintenance").await
    }

    fn capabilities(&self) -> StoreCaps {
        StoreCaps {
            backend: self.0,
            cluster_safe: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::FaucetError;

    #[tokio::test]
    async fn a_call_ends_with_its_context() {
        let store = StalledStore::new();
        assert_eq!(store.capabilities().backend, "stalled");
        assert_eq!(StalledStore::default().capabilities().backend, "stalled");
        let calls = CallContext::new().with_timeout(Duration::from_millis(20));
        let feed = FeedUid::new("f").unwrap();
        let err = store
            .heads(&calls, std::slice::from_ref(&feed))
            .await
            .unwrap_err();
        assert!(
            matches!(err, FeedError::Call(FaucetError::Timeout { .. })),
            "{err}"
        );
        assert!(err.is_transient());
        let token = faucet_core::CancellationToken::new();
        token.cancel();
        let calls = CallContext::new().with_cancel(token);
        let err = store.purge_feed(&calls, &feed).await.unwrap_err();
        assert!(
            matches!(err, FeedError::Call(FaucetError::Cancelled { .. })),
            "{err}"
        );
    }
}
