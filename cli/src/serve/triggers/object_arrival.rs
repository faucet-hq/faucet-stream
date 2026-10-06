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

/// Tracks a high-water `last_modified` + the set of keys seen exactly at that
/// timestamp (ties), so the same object is never re-emitted while genuinely new
/// objects (even at the same second) are.
#[derive(Debug, Default)]
pub struct Cursor {
    watermark: Option<chrono::DateTime<chrono::Utc>>,
    seen_at_watermark: std::collections::HashSet<String>,
}

impl Cursor {
    /// Seed for `start_at: now` — ignore everything at/below `now`.
    pub fn starting_now(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            watermark: Some(now),
            seen_at_watermark: std::collections::HashSet::new(),
        }
    }

    /// Seed for `start_at: beginning` — emit every existing object once.
    pub fn starting_beginning() -> Self {
        Self::default()
    }

    /// Return objects strictly newer than the watermark (or at-watermark but
    /// unseen). Does NOT advance the watermark — call [`Cursor::commit`] after a
    /// successful fire so a dropped fire is retried.
    pub fn new_objects(&self, listing: &[ListedObject]) -> Vec<ListedObject> {
        let mut out = Vec::new();
        for o in listing {
            match self.watermark {
                None => out.push(o.clone()),
                Some(w) if o.last_modified > w => out.push(o.clone()),
                Some(w) if o.last_modified == w && !self.seen_at_watermark.contains(&o.key) => {
                    out.push(o.clone())
                }
                _ => {}
            }
        }
        out
    }

    /// Mark an object committed (advance the watermark past it).
    pub fn commit(&mut self, o: &ListedObject) {
        match self.watermark {
            Some(w) if o.last_modified > w => {
                self.watermark = Some(o.last_modified);
                self.seen_at_watermark.clear();
                self.seen_at_watermark.insert(o.key.clone());
            }
            Some(w) if o.last_modified == w => {
                self.seen_at_watermark.insert(o.key.clone());
            }
            None => {
                self.watermark = Some(o.last_modified);
                self.seen_at_watermark.insert(o.key.clone());
            }
            _ => {}
        }
    }
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
                        FireOutcome::Dropped(_) => break, // backpressure: stop; retry next poll
                        FireOutcome::Error(_) | FireOutcome::Failed(_) => break,
                        _ => {}
                    }
                }
            }
            ArrivalMode::Batch => {
                let watermark = new.iter().map(|o| o.last_modified).max().unwrap();
                let event = TriggerEvent::ObjectBatch {
                    bucket: self.bucket.clone(),
                    count: new.len(),
                    watermark: watermark.to_rfc3339(),
                };
                match enqueue::fire(state, &self.compiled, event, &fired_at).await {
                    outcome if outcome.committed() => {
                        for o in &new {
                            self.cursor.commit(o);
                        }
                        fired = true;
                    }
                    _ => {} // dropped/error: cursor unchanged, retry next poll
                }
            }
        }
        Ok(fired)
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
