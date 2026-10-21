//! `object_arrival` trigger: incremental S3/GCS prefix listing. The pure
//! `Cursor` decides which listed objects are new; the watcher (Task 13) does IO.

use super::context::TriggerEvent;
use super::enqueue::{self, FireOutcome};
use super::spec::{ArrivalMode, StartAt, StoreSpec};
use super::watcher::Watcher;
use crate::serve::state::ServerState;
use async_trait::async_trait;
use futures::StreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// One listed object, decoupled from `object_store::ObjectMeta` so the cursor is
/// pure and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    pub etag: Option<String>,
}

/// Which listed objects are new. Remembers every object fired within
/// `lookback` of the newest one (by key, ETag and `last_modified`), so an
/// object that becomes listable after a newer one fired — a multipart upload
/// carries its start time, a slow writer an earlier one — is still fired
/// once, while an object already fired never is again. A rewritten object
/// (new ETag or time) is new.
#[derive(Debug)]
pub struct Cursor {
    /// `start_at: now`: objects last modified at or before this were there
    /// before the watcher started.
    floor: Option<chrono::DateTime<chrono::Utc>>,
    /// The newest `last_modified` fired.
    watermark: Option<chrono::DateTime<chrono::Utc>>,
    lookback: chrono::Duration,
    /// Fired objects within the window, by identity.
    seen: std::collections::HashMap<String, chrono::DateTime<chrono::Utc>>,
}

/// The default look-back window: 24 hours.
pub const DEFAULT_LOOKBACK: Duration = Duration::from_secs(86_400);

impl Default for Cursor {
    fn default() -> Self {
        Self {
            floor: None,
            watermark: None,
            lookback: chrono::Duration::from_std(DEFAULT_LOOKBACK).unwrap_or_default(),
            seen: std::collections::HashMap::new(),
        }
    }
}

fn identity(o: &ListedObject) -> String {
    format!(
        "{}\u{0}{}\u{0}{}",
        o.key,
        o.last_modified.timestamp_nanos_opt().unwrap_or_default(),
        o.etag.as_deref().unwrap_or("")
    )
}

impl Cursor {
    /// Seed for `start_at: now` — ignore everything at/below `now`.
    pub fn starting_now(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            floor: Some(now),
            ..Self::default()
        }
    }

    /// Seed for `start_at: beginning` — emit every existing object once.
    pub fn starting_beginning() -> Self {
        Self::default()
    }

    /// The same cursor remembering fired objects for `lookback` behind the
    /// newest one.
    pub fn with_lookback(mut self, lookback: Duration) -> Self {
        self.lookback = chrono::Duration::from_std(lookback).unwrap_or(chrono::Duration::MAX);
        self
    }

    /// The oldest `last_modified` still considered: the watermark minus the
    /// look-back window.
    fn horizon(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.watermark.map(|w| {
            w.checked_sub_signed(self.lookback)
                .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC)
        })
    }

    /// Return the listed objects not yet fired: after the start floor,
    /// within the look-back window, and not remembered. Does NOT record them
    /// — call [`Cursor::commit`] after a successful fire so a dropped fire is
    /// retried.
    pub fn new_objects(&self, listing: &[ListedObject]) -> Vec<ListedObject> {
        let horizon = self.horizon();
        listing
            .iter()
            .filter(|o| self.floor.is_none_or(|f| o.last_modified > f))
            .filter(|o| horizon.is_none_or(|h| o.last_modified >= h))
            .filter(|o| !self.seen.contains_key(&identity(o)))
            .cloned()
            .collect()
    }

    /// Whether `o` is newer than every object fired so far.
    pub fn is_newest(&self, o: &ListedObject) -> bool {
        self.watermark.is_none_or(|w| o.last_modified > w)
    }

    /// Record `o` as fired, and forget objects that fell out of the window.
    pub fn commit(&mut self, o: &ListedObject) {
        if self.is_newest(o) {
            self.watermark = Some(o.last_modified);
        }
        self.seen.insert(identity(o), o.last_modified);
        if let Some(h) = self.horizon() {
            self.seen.retain(|_, lm| *lm >= h);
        }
    }
}

/// A stable digest of a batch's objects, so two batches with the same newest
/// time but different objects get different idempotency keys.
fn batch_digest(objects: &[ListedObject]) -> String {
    use sha2::Digest as _;
    let mut ids: Vec<String> = objects.iter().map(identity).collect();
    ids.sort();
    let mut h = sha2::Sha256::new();
    for id in &ids {
        h.update(id.as_bytes());
        h.update(b"\n");
    }
    h.finalize()
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Return type of [`ObjectArrivalWatcher::build_store`]: the constructed
/// store, the bucket name, and an optional key prefix.
type StoreTriple = (Arc<dyn ObjectStore>, String, Option<String>);

pub struct ObjectArrivalWatcher {
    name: String,
    store: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: Option<String>,
    mode: ArrivalMode,
    poll: Duration,
    cursor: Cursor,
    compiled: Arc<super::compiled::CompiledTrigger>,
}

impl ObjectArrivalWatcher {
    /// Build the object_store client for the configured store.
    pub fn build_store(store: &StoreSpec) -> Result<StoreTriple, String> {
        match store {
            StoreSpec::S3 {
                bucket,
                prefix,
                region,
                endpoint,
            } => {
                let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(r) = region {
                    b = b.with_region(r);
                }
                if let Some(e) = endpoint {
                    b = b.with_endpoint(e).with_allow_http(true);
                }
                let s = b.build().map_err(|e| format!("building S3 client: {e}"))?;
                Ok((Arc::new(s), bucket.clone(), prefix.clone()))
            }
            StoreSpec::Gcs { bucket, prefix } => {
                let s = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| format!("building GCS client: {e}"))?;
                Ok((Arc::new(s), bucket.clone(), prefix.clone()))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compiled: Arc<super::compiled::CompiledTrigger>,
        store: Arc<dyn ObjectStore>,
        bucket: String,
        prefix: Option<String>,
        mode: ArrivalMode,
        poll: Duration,
        start_at: StartAt,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let cursor = match start_at {
            StartAt::Now => Cursor::starting_now(now),
            StartAt::Beginning => Cursor::starting_beginning(),
        };
        Self {
            name: compiled.name().to_string(),
            store,
            bucket,
            prefix,
            mode,
            poll,
            cursor,
            compiled,
        }
    }

    /// Remember fired objects for `lookback` behind the newest one, so a
    /// late-visible object is still fired (default [`DEFAULT_LOOKBACK`]).
    pub fn with_lookback(mut self, lookback: Duration) -> Self {
        self.cursor = std::mem::take(&mut self.cursor).with_lookback(lookback);
        self
    }

    async fn list(&self) -> Result<Vec<ListedObject>, String> {
        let prefix_path = self.prefix.as_deref().map(object_store::path::Path::from);
        let mut stream = self.store.list(prefix_path.as_ref());
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| format!("listing objects: {e}"))?;
            out.push(ListedObject {
                key: meta.location.to_string(),
                last_modified: meta.last_modified,
                size: meta.size,
                etag: meta.e_tag,
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl Watcher for ObjectArrivalWatcher {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "object_arrival"
    }

    fn poll_interval(&self) -> Duration {
        self.poll
    }

    async fn poll(&mut self, state: &ServerState) -> Result<bool, String> {
        let listing = self.list().await?;
        let mut new = self.cursor.new_objects(&listing);
        if new.is_empty() {
            return Ok(false);
        }
        // Deterministic order: oldest first so the watermark advances monotonically.
        new.sort_by(|a, b| {
            a.last_modified
                .cmp(&b.last_modified)
                .then(a.key.cmp(&b.key))
        });
        let fired_at = chrono::Utc::now().to_rfc3339();
        let mut fired = false;
        // A fire a retry cannot fix (a config that does not load for this
        // object) is skipped past and reported, so one bad object cannot
        // stall the prefix (#789 SERVE-21); a transient one stops the poll
        // and is retried. Either way the poll reports the error, so the
        // supervisor backs off and marks the watcher unhealthy.
        let mut problem: Option<String> = None;

        match self.mode {
            ArrivalMode::PerObject => {
                for o in new {
                    let event = TriggerEvent::Object {
                        bucket: self.bucket.clone(),
                        key: o.key.clone(),
                        size: o.size,
                        last_modified: o.last_modified.to_rfc3339(),
                    };
                    match enqueue::fire(state, &self.compiled, event, &fired_at).await {
                        outcome if outcome.committed() => {
                            self.cursor.commit(&o);
                            fired = true;
                        }
                        FireOutcome::Failed(e) => {
                            tracing::error!(
                                trigger = self.compiled.name(),
                                object = %o.key,
                                error = %e,
                                "object skipped: its run cannot start"
                            );
                            self.cursor.commit(&o);
                            problem = Some(format!("object '{}' skipped: {e}", o.key));
                        }
                        FireOutcome::Error(e) => {
                            problem = Some(format!("object '{}': {e}", o.key));
                            break;
                        }
                        _ => break, // dropped (backpressure): retry next poll
                    }
                }
            }
            ArrivalMode::Batch => {
                let newest = new.iter().map(|o| o.last_modified).max().unwrap();
                // A batch holding a newer object than any fired keys on that
                // time (unique, it only grows); a batch of late objects only
                // shares its newest time with an earlier batch, so it also
                // keys on what it holds.
                let watermark = if new.iter().any(|o| self.cursor.is_newest(o)) {
                    newest.to_rfc3339()
                } else {
                    format!("{}:late:{}", newest.to_rfc3339(), batch_digest(&new))
                };
                let event = TriggerEvent::ObjectBatch {
                    bucket: self.bucket.clone(),
                    count: new.len(),
                    watermark,
                };
                match enqueue::fire(state, &self.compiled, event, &fired_at).await {
                    outcome if outcome.committed() => {
                        for o in &new {
                            self.cursor.commit(o);
                        }
                        fired = true;
                    }
                    FireOutcome::Failed(e) => {
                        tracing::error!(
                            trigger = self.compiled.name(),
                            objects = new.len(),
                            error = %e,
                            "batch skipped: its run cannot start"
                        );
                        for o in &new {
                            self.cursor.commit(o);
                        }
                        problem = Some(format!("batch of {} skipped: {e}", new.len()));
                    }
                    FireOutcome::Error(e) => problem = Some(e),
                    _ => {} // dropped: cursor unchanged, retry next poll
                }
            }
        }
        match problem {
            Some(e) => Err(e),
            None => Ok(fired),
        }
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use chrono::TimeZone;

    fn obj(key: &str, secs: i64) -> ListedObject {
        ListedObject {
            key: key.into(),
            last_modified: chrono::Utc.timestamp_opt(secs, 0).unwrap(),
            size: 1,
            etag: None,
        }
    }

    #[test]
    fn starting_now_ignores_existing() {
        let now = chrono::Utc.timestamp_opt(1000, 0).unwrap();
        let c = Cursor::starting_now(now);
        // Existing object at t=900 is older → not new.
        assert!(c.new_objects(&[obj("a", 900)]).is_empty());
        // Newer object at t=1100 → new.
        assert_eq!(c.new_objects(&[obj("b", 1100)]).len(), 1);
    }

    #[test]
    fn starting_beginning_emits_all_then_commits() {
        let mut c = Cursor::starting_beginning();
        let listing = vec![obj("a", 100), obj("b", 200)];
        let new = c.new_objects(&listing);
        assert_eq!(new.len(), 2);
        for o in &new {
            c.commit(o);
        }
        // After commit, none are new.
        assert!(c.new_objects(&listing).is_empty());
    }

    #[test]
    fn handles_ties_at_watermark() {
        let mut c = Cursor::starting_beginning();
        let a = obj("a", 100);
        c.commit(&c.new_objects(std::slice::from_ref(&a))[0].clone());
        // A second object at the SAME timestamp is still new (unseen key).
        let b = obj("b", 100);
        let new = c.new_objects(&[a.clone(), b.clone()]);
        assert_eq!(new, vec![b.clone()]);
        c.commit(&b);
        assert!(c.new_objects(&[a, b]).is_empty());
    }

    /// A multipart upload started before a small object that fired, and
    /// listed only after it, carries the older time: it is still fired, once.
    #[test]
    fn a_late_visible_object_is_fired_once() {
        let mut c = Cursor::starting_beginning();
        let small = obj("small", 610);
        c.commit(&small);
        let big = obj("big", 600);
        assert_eq!(
            c.new_objects(&[small.clone(), big.clone()]),
            vec![big.clone()]
        );
        c.commit(&big);
        assert!(c.new_objects(&[small.clone(), big.clone()]).is_empty());
        assert!(!c.is_newest(&big));
        // Rewritten in place (new ETag): new again.
        let mut rewritten = big.clone();
        rewritten.etag = Some("v2".into());
        assert_eq!(c.new_objects(&[rewritten.clone()]), vec![rewritten]);
    }

    #[test]
    fn objects_older_than_the_lookback_are_ignored_and_forgotten() {
        let mut c = Cursor::starting_beginning().with_lookback(Duration::from_secs(100));
        c.commit(&obj("a", 1000));
        assert!(c.new_objects(&[obj("old", 899)]).is_empty());
        assert_eq!(c.new_objects(&[obj("edge", 900)]).len(), 1);
        c.commit(&obj("b", 1200));
        assert!(!c.seen.contains_key(&identity(&obj("a", 1000))), "pruned");
        assert!(
            c.new_objects(&[obj("a", 1000)]).is_empty(),
            "outside the window"
        );
        let mut huge = Cursor::starting_beginning().with_lookback(Duration::MAX);
        huge.commit(&obj("a", 1000));
        assert_eq!(huge.new_objects(&[obj("ancient", 1)]).len(), 1);
    }

    #[test]
    fn late_batches_key_on_their_objects() {
        let a = vec![obj("a", 1), obj("b", 2)];
        let b = vec![obj("c", 2)];
        assert_ne!(batch_digest(&a), batch_digest(&b));
        let mut swapped = a.clone();
        swapped.reverse();
        assert_eq!(batch_digest(&a), batch_digest(&swapped), "order-free");
    }

    #[test]
    fn dropped_fire_is_retried_until_committed() {
        let mut c = Cursor::starting_beginning();
        let a = obj("a", 100);
        // new_objects without commit → still new next time (simulating a drop).
        assert_eq!(c.new_objects(std::slice::from_ref(&a)).len(), 1);
        assert_eq!(c.new_objects(std::slice::from_ref(&a)).len(), 1);
        c.commit(&a);
        assert!(c.new_objects(&[a]).is_empty());
    }
}
