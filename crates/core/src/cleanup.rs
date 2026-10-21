//! Scoped cleanup — delete destination rows a run did not write (#478).
//!
//! An incremental sync into an upsert sink **cannot remove records deleted at the
//! source**: a record that disappears simply stops appearing in the
//! "updated since X" feed, so `write_mode: upsert` keeps it in the destination
//! forever. The destination looks healthy, the run reports success, and stale
//! rows accumulate indefinitely with nothing surfacing the divergence.
//!
//! Scoped cleanup closes that hole for the case where the source can make a
//! **completeness claim**: "for scope S, these are *all* the records". The
//! canonical shape is a parent/child incremental sync — a child row fetching one
//! contact's associations is authoritative for `contact_id = <that contact>`.
//!
//! ## Why the claim comes from the source
//!
//! A sink cannot make it. It observes a page of records and cannot distinguish a
//! complete set from page 1 of 3, or from a partial page preceding a failure. Two
//! further reasons the scope is declared upstream rather than on the sink:
//!
//! 1. **Drift safety.** The predicate already exists in the source config
//!    (`/contacts/${contacts.id}/associations`). Declaring it a second time on
//!    the sink means two copies of one predicate, and when they diverge the
//!    pipeline deletes the wrong rows.
//! 2. **The empty-result case, which is decisive.** A contact that had five
//!    associations and now has none produces a fetch returning **zero records**.
//!    Any design inferring scopes from observed records never learns the scope
//!    existed, so the five stale rows survive — i.e. it fails precisely the case
//!    the feature exists to fix. A scope declared by the invocation comes from
//!    the parent record and survives an empty result set.
//!
//! ## When it runs
//!
//! **Once per invocation, after a fully successful run — never per page.** Two
//! ways to get this wrong, both silent data loss:
//!
//! - *Per page*: a scope's records spanning two pages → page 2's
//!   "delete what I didn't see" wipes what page 1 just wrote.
//! - *After a partial run*: the fetch dies at 40% → the delete removes the 60%
//!   that had not yet arrived.
//!
//! So [`run_stream`](crate::run_stream) only invokes it when the stream reached
//! its natural end **uncancelled**, and the CLI additionally attaches the policy
//! only for real root invocations (never `--dry-run` / `--limit` / a shard).
//!
//! ## Key accumulation and the ceiling
//!
//! Deleting "what this run did not write" requires knowing what it wrote. The
//! pipeline accumulates the **key tuples only** (not records) as pages are
//! written — see [`SeenKeys`]. That is bounded by the scope's row count, which is
//! small for the per-parent shape this targets, so a ceiling
//! ([`CleanupPolicy::max_keys`]) guards the pathological case: on breach the
//! cleanup **aborts with a typed error and deletes nothing**, rather than issuing
//! a partial delete that would destroy rows it simply forgot about.
//!
//! ## Complete reads
//!
//! A source that sometimes reads its **whole dataset** in one run (a change
//! stream that fell past its retention and re-reads the table) declares it
//! through [`ReadCompleteness`], returned by `Source::read_completeness`. A
//! policy built with [`CleanupPolicy::for_complete_read`] has an empty scope
//! (the whole destination) and fires only when that run's read finished.
//! Keys are tracked only once the read has begun, and the run's bookmarks are
//! held back until the delete succeeds, so a crash or a failed delete makes
//! the next run read the whole dataset again instead of skipping the delete.

use crate::error::FaucetError;
use crate::traits::Sink;
use crate::write_mode::KeyTuple;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Default ceiling on accumulated keys for a [complete-read](CleanupPolicy::for_complete_read)
/// cleanup: every key of the dataset is tracked, so it is sized for tables
/// rather than per-parent scopes.
pub const DEFAULT_COMPLETE_READ_MAX_KEYS: usize = 1_000_000;

/// Default ceiling on accumulated keys for one invocation's cleanup.
///
/// Sized for the shape this feature targets (a scope per parent record — tens to
/// low thousands of rows). Above it the cleanup refuses rather than guessing.
pub const DEFAULT_MAX_KEYS: usize = 100_000;

/// What to do about destination rows inside the claimed scope that this run did
/// not write.
///
/// Opted into on the **source**, through its `complete_for.on_missing: delete`
/// claim; no sink config field selects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CleanupMode {
    /// Delete rows in the scope whose key was not written by this invocation.
    DeleteMissing,
}

/// When a [`CleanupPolicy`] deletes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CleanupTrigger {
    /// After every successful, uncancelled run.
    #[default]
    Always,
    /// Only after a run in which the source read its whole dataset
    /// ([`ReadCompleteness`] reached [`ReadState::Complete`]).
    CompleteRead,
}

/// How far a source got through reading its whole dataset in the current run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadState {
    /// The run is not a complete read.
    Partial,
    /// A complete read has started and not finished.
    Reading,
    /// The run read every record of the dataset.
    Complete,
}

/// A shared signal through which a source reports a complete read of its
/// dataset. Cheap to clone; the source keeps one and hands out clones.
#[derive(Debug, Clone, Default)]
pub struct ReadCompleteness(std::sync::Arc<std::sync::atomic::AtomicU8>);

impl ReadCompleteness {
    const PARTIAL: u8 = 0;
    const READING: u8 = 1;
    const COMPLETE: u8 = 2;

    /// A signal in [`ReadState::Partial`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Back to [`ReadState::Partial`] (start of a run).
    pub fn reset(&self) {
        self.set(Self::PARTIAL);
    }

    /// A complete read has started. Call before its first record is yielded.
    pub fn begin(&self) {
        self.set(Self::READING);
    }

    /// The complete read has yielded every record.
    pub fn complete(&self) {
        self.set(Self::COMPLETE);
    }

    /// The current state.
    pub fn state(&self) -> ReadState {
        match self.0.load(std::sync::atomic::Ordering::Acquire) {
            Self::READING => ReadState::Reading,
            Self::COMPLETE => ReadState::Complete,
            _ => ReadState::Partial,
        }
    }

    fn set(&self, v: u8) {
        self.0.store(v, std::sync::atomic::Ordering::Release);
    }
}

/// A compiled cleanup instruction for one invocation.
#[derive(Debug, Clone)]
pub struct CleanupPolicy {
    /// The completeness claim, in **destination column** terms: the rows this
    /// invocation is authoritative for. Every entry is an equality predicate,
    /// AND-ed together.
    ///
    /// Destination terms rather than source terms because the `DELETE` executes
    /// against destination columns, and a transform chain may rename fields
    /// between the two.
    pub scope: BTreeMap<String, Value>,
    /// Key columns identifying a row — mirrors the sink's `key`.
    pub key: Vec<String>,
    /// Ceiling on accumulated keys before the cleanup refuses (see module docs).
    pub max_keys: usize,
    /// When the delete fires.
    pub trigger: CleanupTrigger,
}

impl CleanupPolicy {
    /// Build a policy, validating it is actionable.
    pub fn new(
        scope: BTreeMap<String, Value>,
        key: Vec<String>,
        max_keys: usize,
    ) -> Result<Self, FaucetError> {
        if scope.is_empty() {
            // An empty scope is an every-row predicate. Refusing here is the
            // difference between "delete this contact's stale rows" and
            // "truncate the table".
            return Err(FaucetError::Config(
                "cleanup: the completeness claim (`complete_for`) must name at least one \
                 column — an empty scope would match every row in the destination"
                    .into(),
            ));
        }
        if key.is_empty() {
            return Err(FaucetError::Config(
                "cleanup: requires a non-empty `key` so a written row can be told apart \
                 from a stale one"
                    .into(),
            ));
        }
        if scope.values().any(Value::is_null) {
            return Err(FaucetError::Config(
                "cleanup: the completeness claim contains a null value — an unresolved \
                 scope token would delete the wrong rows"
                    .into(),
            ));
        }
        Ok(Self {
            scope,
            key,
            max_keys: max_keys.max(1),
            trigger: CleanupTrigger::Always,
        })
    }

    /// A policy over the **whole destination** that fires only after a run in
    /// which the source read its whole dataset (see the module docs).
    pub fn for_complete_read(key: Vec<String>, max_keys: usize) -> Result<Self, FaucetError> {
        if key.is_empty() {
            return Err(FaucetError::Config(
                "cleanup: a complete-read cleanup requires a non-empty `key` so a written row \
                 can be told apart from a deleted one"
                    .into(),
            ));
        }
        Ok(Self {
            scope: BTreeMap::new(),
            key,
            max_keys: max_keys.max(1),
            trigger: CleanupTrigger::CompleteRead,
        })
    }
}

/// The set of key tuples an invocation wrote, accumulated across pages.
///
/// Deliberately stores only keys, not records: memory is O(rows in scope × key
/// width) rather than O(payload).
#[derive(Debug, Default)]
pub struct SeenKeys {
    keys: Vec<KeyTuple>,
    /// Set once the ceiling is breached. Sticky: a cleanup that lost track of
    /// even one key must not run at all.
    overflowed: bool,
}

impl SeenKeys {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the keys written for one page. Rows missing a key column, or
    /// carrying a null there, are ignored — they cannot be matched by a keyed
    /// delete anyway, and the write path already routes them to the DLQ or fails
    /// the batch.
    pub fn record_page(&mut self, page: &[Value], key: &[String], max_keys: usize) {
        if self.overflowed {
            return;
        }
        for rec in page {
            let Some(obj) = rec.as_object() else { continue };
            let mut tuple = Vec::with_capacity(key.len());
            let mut complete = true;
            for k in key {
                match obj.get(k) {
                    Some(v) if !v.is_null() => tuple.push((k.clone(), v.clone())),
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            if !complete {
                continue;
            }
            if self.keys.len() >= max_keys {
                self.overflowed = true;
                self.keys.clear(); // free the buffer; the cleanup will refuse
                return;
            }
            self.keys.push(KeyTuple(tuple));
        }
    }

    /// Whether the ceiling was breached, which makes the cleanup unsafe to run.
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &[KeyTuple] {
        &self.keys
    }

    /// The typed error a breached ceiling produces. Separate from the accumulator
    /// so the caller decides whether to fail the run or log — but never to
    /// delete.
    pub fn overflow_error(&self, max_keys: usize) -> FaucetError {
        FaucetError::Config(format!(
            "cleanup: this invocation wrote more than {max_keys} rows in the claimed scope, \
             so the set of written keys could not be tracked. Nothing was deleted — a \
             partial delete would remove rows the run actually wrote. Narrow the scope \
             (a smaller `complete_for`), or raise the ceiling if the destination can take \
             a delete of this size"
        ))
    }
}

/// A sink wrapper that records the key tuples written through it (#478).
///
/// This is how scoped cleanup tracks "what this run wrote" **without** adding a
/// field to [`RunStreamOptions`](crate::RunStreamOptions), which is an
/// externally-constructible struct whose shape is part of the public API. It is
/// also the more honest home for the bookkeeping: the thing that writes the rows
/// is the thing that knows which rows were written, and it composes with the
/// existing sink-decorator pattern (`InstrumentedSink`) rather than threading a
/// second concern through the page loop.
///
/// Every write path is counted, including [`write_batch_partial`](Sink::write_batch_partial).
/// That is deliberate: a row handed to the sink that fails and lands in the DLQ
/// is still a record the source claimed present, so it must count as seen or the
/// cleanup would delete its destination row.
///
/// Records that never reach the sink at all — quarantined by a quality, contract,
/// or drift policy — are consequently *not* counted, which is why those
/// combinations are rejected at config-load time rather than silently deleting
/// the quarantined rows' destination counterparts.
pub struct CleanupTracker<'a, S: Sink + ?Sized> {
    inner: &'a S,
    key: Vec<String>,
    max_keys: usize,
    seen: std::sync::Mutex<SeenKeys>,
    gate: Option<ReadCompleteness>,
}

impl<'a, S: Sink + ?Sized> CleanupTracker<'a, S> {
    pub fn new(inner: &'a S, policy: &CleanupPolicy) -> Self {
        Self {
            inner,
            key: policy.key.clone(),
            max_keys: policy.max_keys,
            seen: std::sync::Mutex::new(SeenKeys::new()),
            gate: None,
        }
    }

    /// Record keys only while `signal` reports a complete read under way or
    /// finished, so an ordinary run tracks nothing.
    #[must_use]
    pub fn gated_by(mut self, signal: ReadCompleteness) -> Self {
        self.gate = Some(signal);
        self
    }

    fn record(&self, records: &[Value]) {
        if self
            .gate
            .as_ref()
            .is_some_and(|g| g.state() == ReadState::Partial)
        {
            return;
        }
        if let Ok(mut seen) = self.seen.lock() {
            seen.record_page(records, &self.key, self.max_keys);
        }
    }

    /// Run the scoped delete against the wrapped sink. Call **only** after a
    /// fully successful, uncancelled run — see the module docs.
    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    pub async fn finish(&self, policy: &CleanupPolicy) -> Result<u64, FaucetError> {
        // Take the set out and drop the guard *before* awaiting: holding a
        // `MutexGuard` across an await makes the whole run future non-`Send`,
        // which the executor's `JoinSet` requires.
        let seen = {
            let mut guard = self
                .seen
                .lock()
                .map_err(|_| FaucetError::Sink("cleanup: key tracker poisoned".into()))?;
            if guard.overflowed() {
                // Refuse rather than partially delete: the tracked set is
                // incomplete, so a delete would remove rows the run wrote.
                return Err(guard.overflow_error(policy.max_keys));
            }
            std::mem::take(&mut *guard)
        };
        self.inner.cleanup_scope(&policy.scope, &seen).await
    }

    /// Whether more keys were written than the policy's ceiling, so the
    /// cleanup was (or will be) refused rather than run.
    pub fn overflowed(&self) -> bool {
        self.seen.lock().map(|g| g.overflowed()).unwrap_or(true)
    }

    /// Number of keys tracked so far (for logging).
    pub fn tracked(&self) -> usize {
        self.seen.lock().map(|g| g.len()).unwrap_or(0)
    }
}

/// Holds every bookmark write back while a complete read is under way, so
/// the run's position becomes durable only after its cleanup succeeded: a
/// crash, a cancel or a failed delete leaves the previous bookmark, and the
/// next run reads the whole dataset again instead of skipping the delete.
pub(crate) struct HoldingStateStore {
    inner: std::sync::Arc<dyn crate::state::StateStore>,
    signal: ReadCompleteness,
    held: std::sync::Mutex<BTreeMap<String, Value>>,
}

impl HoldingStateStore {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn crate::state::StateStore>,
        signal: ReadCompleteness,
    ) -> Self {
        Self {
            inner,
            signal,
            held: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Value>> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Write the held bookmarks through. Call only after the cleanup succeeded.
    pub(crate) async fn release(&self) -> Result<(), FaucetError> {
        let held = std::mem::take(&mut *self.held());
        for (key, value) in held {
            self.inner.put(&key, &value).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::state::StateStore for HoldingStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        if let Some(v) = self.held().get(key) {
            return Ok(Some(v.clone()));
        }
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        if self.signal.state() == ReadState::Partial {
            return self.inner.put(key, value).await;
        }
        self.held().insert(key.to_string(), value.clone());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.held().remove(key);
        self.inner.delete(key).await
    }
}

#[async_trait::async_trait]
impl<S: Sink + ?Sized> Sink for CleanupTracker<'_, S> {
    // The columnar and native paths carry no keys to record, so they stay at the
    // trait default (off); everything else reaches the wrapped sink.
    crate::forward_sink!(inner; except
        write_batch, write_batch_reported, write_governed, write_batch_partial,
        write_batch_idempotent, write_batch_idempotent_reported,
        supports_columnar, write_batch_columnar, native_load_capabilities, load_native);

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.record(records);
        Ok(n)
    }

    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self.inner.write_batch_reported(records).await?;
        self.record(records);
        Ok(r)
    }

    async fn write_governed(
        &self,
        page: &crate::governance::GovernedPage<'_>,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self.inner.write_governed(page).await?;
        self.record(page.records());
        Ok(r)
    }

    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<crate::traits::RowOutcome>, FaucetError> {
        let out = self.inner.write_batch_partial(records).await;
        // Count every row handed to the sink, including the ones that failed —
        // per row or as a whole batch under `on_batch_error: dlq_all` — and will
        // be routed to the DLQ. See the type docs.
        self.record(records);
        out
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let n = self
            .inner
            .write_batch_idempotent(records, scope, token)
            .await?;
        self.record(records);
        Ok(n)
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self
            .inner
            .write_batch_idempotent_reported(records, scope, token)
            .await?;
        self.record(records);
        Ok(r)
    }
}

#[allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_holding_store_holds_writes_only_during_a_complete_read() {
        use crate::state::{MemoryStateStore, StateStore};
        let inner: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let signal = ReadCompleteness::new();
        let store = HoldingStateStore::new(std::sync::Arc::clone(&inner), signal.clone());
        store.put("k", &serde_json::json!(1)).await.unwrap();
        assert_eq!(inner.get("k").await.unwrap(), Some(serde_json::json!(1)));
        signal.begin();
        assert_eq!(signal.state(), ReadState::Reading);
        store.put("k", &serde_json::json!(2)).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(serde_json::json!(2)));
        assert_eq!(inner.get("k").await.unwrap(), Some(serde_json::json!(1)));
        signal.complete();
        assert_eq!(signal.state(), ReadState::Complete);
        store.release().await.unwrap();
        assert_eq!(inner.get("k").await.unwrap(), Some(serde_json::json!(2)));
        store.put("gone", &serde_json::json!(3)).await.unwrap();
        store.delete("gone").await.unwrap();
        store.release().await.unwrap();
        assert_eq!(inner.get("gone").await.unwrap(), None);
        signal.reset();
        assert_eq!(signal.state(), ReadState::Partial);
    }
    use serde_json::json;

    fn scope() -> BTreeMap<String, Value> {
        BTreeMap::from([("contact_id".to_string(), json!(123))])
    }

    #[test]
    fn policy_requires_a_non_empty_scope() {
        // An empty scope is a truncate, not a cleanup.
        let err = CleanupPolicy::new(BTreeMap::new(), vec!["id".into()], 10)
            .expect_err("empty scope must be refused");
        assert!(err.to_string().contains("at least one"), "{err}");
    }

    #[test]
    fn policy_requires_a_key() {
        let err = CleanupPolicy::new(scope(), vec![], 10).expect_err("no key must be refused");
        assert!(err.to_string().contains("`key`"), "{err}");
    }

    #[test]
    fn policy_refuses_a_null_scope_value() {
        // An unresolved `${parent.id}` would land here as null and delete the
        // wrong rows.
        let s = BTreeMap::from([("contact_id".to_string(), Value::Null)]);
        let err = CleanupPolicy::new(s, vec!["id".into()], 10).expect_err("null must be refused");
        assert!(err.to_string().contains("null"), "{err}");
    }

    #[test]
    fn policy_floors_max_keys_at_one() {
        let p = CleanupPolicy::new(scope(), vec!["id".into()], 0).unwrap();
        assert_eq!(p.max_keys, 1);
    }

    #[tokio::test]
    async fn cleanup_tracker_forwards_overwrite_lifecycle() {
        // The tracker must forward the overwrite lifecycle to the inner sink so
        // an overwrite run still stages/swaps when scoped cleanup wraps it.
        struct OvwSink {
            log: std::sync::Mutex<Vec<&'static str>>,
        }
        #[async_trait::async_trait]
        impl Sink for OvwSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            fn is_overwrite(&self) -> bool {
                true
            }
            async fn begin_overwrite(&self) -> Result<(), FaucetError> {
                self.log.lock().unwrap().push("begin");
                Ok(())
            }
            async fn commit_overwrite(&self) -> Result<(), FaucetError> {
                self.log.lock().unwrap().push("commit");
                Ok(())
            }
            async fn abort_overwrite(&self) -> Result<(), FaucetError> {
                self.log.lock().unwrap().push("abort");
                Ok(())
            }
        }
        let inner = OvwSink {
            log: std::sync::Mutex::new(Vec::new()),
        };
        let policy = CleanupPolicy::new(scope(), vec!["id".into()], 10).unwrap();
        let tracker = CleanupTracker::new(&inner, &policy);
        assert!(tracker.is_overwrite());
        assert_eq!(
            tracker.batch_atomicity(),
            crate::dlq::BatchAtomicity::BestEffort
        );
        // A write through the tracker must forward to the inner sink too.
        assert_eq!(tracker.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
        tracker.begin_overwrite().await.unwrap();
        tracker.commit_overwrite().await.unwrap();
        tracker.abort_overwrite().await.unwrap();
        tracker.complete_run().await.unwrap();
        assert_eq!(*inner.log.lock().unwrap(), vec!["begin", "commit", "abort"]);
    }

    #[test]
    fn accumulates_keys_across_pages() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        seen.record_page(&[json!({"id": 1}), json!({"id": 2})], &key, 100);
        seen.record_page(&[json!({"id": 3})], &key, 100);
        assert_eq!(seen.len(), 3);
        assert!(!seen.overflowed());
    }

    #[test]
    fn accumulates_composite_keys_in_declared_order() {
        let mut seen = SeenKeys::new();
        let key = vec!["a".to_string(), "b".to_string()];
        seen.record_page(&[json!({"b": 2, "a": 1})], &key, 100);
        assert_eq!(seen.len(), 1);
        let t = &seen.keys()[0].0;
        assert_eq!(
            t[0].0, "a",
            "key order follows the declared `key`, not the record"
        );
        assert_eq!(t[1].0, "b");
    }

    #[test]
    fn skips_rows_with_a_missing_or_null_key() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        seen.record_page(
            &[
                json!({"id": 1}),
                json!({"other": 9}), // missing key
                json!({"id": null}), // null key
                json!("not an object"),
            ],
            &key,
            100,
        );
        assert_eq!(seen.len(), 1, "only the well-keyed row is tracked");
    }

    #[test]
    fn overflow_is_sticky_and_frees_the_buffer() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        let page: Vec<Value> = (0..5).map(|i| json!({"id": i})).collect();
        seen.record_page(&page, &key, 3);
        assert!(seen.overflowed(), "ceiling of 3 must trip on a 5-row page");
        assert!(seen.is_empty(), "buffer is freed — the cleanup will refuse");
        // Sticky: a later page cannot un-overflow it.
        seen.record_page(&[json!({"id": 99})], &key, 3);
        assert!(seen.overflowed());
        assert!(seen.is_empty());
    }

    #[test]
    fn overflow_error_explains_that_nothing_was_deleted() {
        let seen = SeenKeys::new();
        let msg = seen.overflow_error(50).to_string();
        assert!(msg.contains("Nothing was deleted"), "{msg}");
        assert!(msg.contains("50"), "{msg}");
    }

    #[tokio::test]
    async fn cleanup_tracker_forwards_every_hook() {
        let probe = crate::forwarding::HookSink::default();
        let policy = CleanupPolicy::new(scope(), vec!["id".into()], 10).unwrap();
        let t = CleanupTracker::new(&probe, &policy);
        crate::forwarding::assert_sink_forwards_every_hook(
            &t,
            &probe,
            &[
                "supports_columnar",
                "write_batch_columnar",
                "native_load_capabilities",
                "load_native",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn overflowed_tells_a_refusal_from_a_backend_failure() {
        struct Fails;
        #[async_trait::async_trait]
        impl Sink for Fails {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            fn supports_cleanup(&self) -> bool {
                true
            }
            async fn cleanup_scope(
                &self,
                _: &BTreeMap<String, Value>,
                _: &SeenKeys,
            ) -> Result<u64, FaucetError> {
                Err(FaucetError::Sink("db down".into()))
            }
        }
        let policy = CleanupPolicy::new(scope(), vec!["id".into()], 1).unwrap();
        let t = CleanupTracker::new(&Fails, &policy);
        t.write_batch(&[json!({"id": 1})]).await.unwrap();
        assert!(t.finish(&policy).await.is_err());
        assert!(!t.overflowed());
        let t = CleanupTracker::new(&Fails, &policy);
        t.write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        assert!(t.finish(&policy).await.is_err());
        assert!(t.overflowed());
    }
}
