//! The [`FeedStore`] contract.

use crate::error::Result;
use crate::types::{
    AppendAck, Batch, BatchLimits, Claim, ClaimId, ClaimOwner, DeliveryState, DeliveryUid, Epoch,
    FeedUid, Gap, Head, Holder, Move, NewEvent, PurgeStats, Seq, Start, StoreCaps, StoreLimits,
    TenantId, Timestamp, TrimStats,
};
use async_trait::async_trait;
use faucet_core::io::CallContext;
use std::time::Duration;

/// A durable, ordered event store with per-delivery cursors and range claims.
///
/// Every method is one backend call through `calls` (timeout, deadline and
/// cancellation apply). Claim expiry, dedupe expiry and retention age follow
/// the **store's** clock, never the caller's.
#[async_trait]
pub trait FeedStore: Send + Sync {
    /// This store's identity: a random number written once, when its schema
    /// is first created, and read back on every open. A restored backup
    /// keeps it; a store recreated empty gets a new one. `None`: the backend
    /// keeps none (the default), so a caller cannot tell two stores apart.
    fn store_id(&self) -> Option<i64> {
        None
    }

    /// Create the feed if it is missing and store its limits. The tenant is
    /// fixed at creation: [`FeedError::TenantMismatch`](crate::FeedError::TenantMismatch)
    /// when it differs.
    async fn ensure_feed(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        tenant: Option<&TenantId>,
        limits: &StoreLimits,
    ) -> Result<()>;

    /// Create the delivery at `start` if it is missing; an existing delivery
    /// keeps its position. Starting at the earliest event after older ones
    /// were trimmed records a gap.
    async fn ensure_delivery(
        &self,
        calls: &CallContext,
        delivery: &DeliveryUid,
        start: Start,
    ) -> Result<DeliveryState>;

    /// Remove the delivery's cursor, claim and gap rows. A missing delivery is
    /// not an error.
    async fn delete_delivery(&self, calls: &CallContext, delivery: &DeliveryUid) -> Result<()>;

    /// Remove the feed with its events, cursors, dedupe rows and gaps.
    async fn purge_feed(&self, calls: &CallContext, feed: &FeedUid) -> Result<PurgeStats>;

    /// Store `events` atomically and durably, assigning consecutive sequence
    /// numbers. Dedupe keys, the tenant quota and the undelivered limits are
    /// checked in the same transaction; a refusal
    /// ([`FeedError::Full`](crate::FeedError::Full)) stores nothing.
    async fn append(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        events: Vec<NewEvent>,
    ) -> Result<AppendAck>;

    /// The committed state of every listed feed that exists, in one query
    /// (one per 10,000 feeds on the SQL backends).
    async fn heads(&self, calls: &CallContext, feeds: &[FeedUid]) -> Result<Vec<Head>>;

    /// Events `[from, to)`, with `to` capped at the watermark.
    /// [`FeedError::RangeUnavailable`](crate::FeedError::RangeUnavailable)
    /// when part of the range was trimmed.
    async fn read(&self, calls: &CallContext, feed: &FeedUid, from: Seq, to: Seq) -> Result<Batch>;

    /// The exclusive end of the largest range starting at `from` that fits
    /// `max`; one event when the first alone is larger; `from` when there is
    /// nothing to read.
    async fn bound(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        from: Seq,
        max: BatchLimits,
    ) -> Result<Seq>;

    /// The first retained sequence number the store committed at or after
    /// `time`; the feed's end when there is none.
    async fn seq_at(&self, calls: &CallContext, feed: &FeedUid, time: Timestamp) -> Result<Seq>;

    /// Payload bytes of the retained events at or after `from`. `None`: the
    /// backend cannot say without reading them (the default).
    async fn bytes_from(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        from: Seq,
    ) -> Result<Option<u64>> {
        let _ = (calls, feed, from);
        Ok(None)
    }

    /// Payload bytes every feed of `tenant` in this store holds together:
    /// the figure the tenant's byte quota is checked against, delivered
    /// events kept for replay included. `None`: the backend cannot say.
    async fn tenant_bytes(&self, calls: &CallContext, tenant: &TenantId) -> Result<Option<u64>> {
        let _ = (calls, tenant);
        Ok(None)
    }

    /// Retained events stored in each of the last `hours` hours by the
    /// store's clock, oldest hour first, in one query. `None`: the backend
    /// cannot say.
    async fn hourly_counts(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        hours: u32,
    ) -> Result<Option<Vec<u64>>> {
        let _ = (calls, feed, hours);
        Ok(None)
    }

    /// When the newest retained event of each of `feeds` was received, in
    /// one query; a feed with no event is left out. `None`: the backend
    /// cannot say.
    async fn newest_received(
        &self,
        calls: &CallContext,
        feeds: &[FeedUid],
    ) -> Result<Option<Vec<(FeedUid, Timestamp)>>> {
        let _ = (calls, feeds);
        Ok(None)
    }

    /// Claim `[expected_cursor, to)` for `ttl`. `None` when the cursor moved,
    /// another claim is live, or `to` is past the watermark: exactly one of
    /// several concurrent claimers wins.
    async fn claim(
        &self,
        calls: &CallContext,
        delivery: &DeliveryUid,
        expected_cursor: Seq,
        to: Seq,
        owner: &ClaimOwner,
        ttl: Duration,
    ) -> Result<Option<Claim>>;

    /// Become the claim's reader. Rotates the holder token, so an earlier
    /// holder's [`renew`](Self::renew) and [`commit`](Self::commit) fail.
    async fn attach(&self, calls: &CallContext, claim: &ClaimId) -> Result<Holder>;

    /// Extend the claim as its holder. `false` when it expired, was released
    /// or replaced, or a newer holder attached.
    async fn renew(&self, calls: &CallContext, holder: &Holder, ttl: Duration) -> Result<bool>;

    /// Extend the claim by its id, without a holder and without rotating one.
    /// `false` when it expired or is gone.
    async fn renew_claim(
        &self,
        calls: &CallContext,
        claim: &ClaimId,
        ttl: Duration,
    ) -> Result<bool>;

    /// Move the cursor from `expected` to `new` as the claim's holder. `new`
    /// may pass the claim's end but never the watermark, and never goes back.
    async fn commit(
        &self,
        calls: &CallContext,
        holder: &Holder,
        expected: Seq,
        new: Seq,
    ) -> Result<()>;

    /// Drop the claim. A claim that is already gone is not an error.
    async fn release(&self, calls: &CallContext, claim: &ClaimId) -> Result<()>;

    /// Move the cursor outside a commit and bump the epoch. Refused while a
    /// claim is live. A replay goes back no further than the oldest retained
    /// event; a skip goes forward and records a gap; a recover records a gap.
    async fn move_cursor(
        &self,
        calls: &CallContext,
        delivery: &DeliveryUid,
        expected: Seq,
        new: Seq,
        why: Move,
    ) -> Result<Epoch>;

    /// The feed's recorded gaps, oldest first; one delivery's when given.
    async fn gaps(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        delivery: Option<&DeliveryUid>,
    ) -> Result<Vec<Gap>>;

    /// Remove at most `max_rows` events that are older than the feed's
    /// `max_age` and below every delivery's cursor (so never inside a claimed
    /// range). A feed with no delivery is trimmed by age alone.
    async fn trim(&self, calls: &CallContext, feed: &FeedUid, max_rows: u32) -> Result<TrimStats>;

    /// Remove at most `max_rows` expired dedupe rows.
    async fn purge_dedupe(&self, calls: &CallContext, feed: &FeedUid, max_rows: u32)
    -> Result<u64>;

    /// Take or keep the feed's maintenance claim for `ttl`. `false` while
    /// another owner holds it.
    async fn maintenance(
        &self,
        calls: &CallContext,
        feed: &FeedUid,
        owner: &ClaimOwner,
        ttl: Duration,
    ) -> Result<bool>;

    /// What this backend can do.
    fn capabilities(&self) -> StoreCaps;
}
