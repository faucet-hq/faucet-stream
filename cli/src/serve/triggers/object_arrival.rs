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
use object_store::list::{PaginatedListOptions, PaginatedListStore};
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
        listing.iter().filter(|o| self.is_new(o)).cloned().collect()
    }

    /// Whether `o` is newer than the floor, within the look-back window, and
    /// not remembered — the per-object test behind [`Cursor::new_objects`].
    pub fn is_new(&self, o: &ListedObject) -> bool {
        self.floor.is_none_or(|f| o.last_modified > f)
            && self.horizon().is_none_or(|h| o.last_modified >= h)
            && !self.seen.contains_key(&identity(o))
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

/// The durable form of a [`Cursor`], kept in the history's settings store
/// so a restart resumes where the watcher stopped.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredCursor {
    /// The listing this cursor belongs to; a changed bucket or prefix starts
    /// over.
    bucket: String,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    floor: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    watermark: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    seen: Vec<(String, chrono::DateTime<chrono::Utc>)>,
}

impl Cursor {
    fn to_stored(&self, bucket: &str, prefix: Option<&str>) -> StoredCursor {
        let mut seen: Vec<_> = self.seen.iter().map(|(k, t)| (k.clone(), *t)).collect();
        seen.sort();
        StoredCursor {
            bucket: bucket.to_string(),
            prefix: prefix.map(str::to_string),
            floor: self.floor,
            watermark: self.watermark,
            seen,
        }
    }

    /// Take the fired state from `stored`, keeping this cursor's look-back.
    fn restore(&mut self, stored: StoredCursor) {
        self.floor = stored.floor;
        self.watermark = stored.watermark;
        self.seen = stored.seen.into_iter().collect();
    }
}

/// The settings-store key of a trigger's cursor.
fn cursor_key(trigger: &str) -> String {
    format!("trigger_cursor:{trigger}")
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
    /// Whether the stored cursor has been read (once, on the first poll).
    resumed: bool,
    compiled: Arc<super::compiled::CompiledTrigger>,
    /// Limit on the whole poll (`poll_timeout_secs`); `None` = unbounded.
    poll_timeout: Option<Duration>,
    /// Longest wait for the next listing page before the poll fails.
    page_timeout: Duration,
    /// The same store, listed page by page so a key object_store cannot
    /// read is skipped rather than failing the listing.
    pager: Option<Arc<dyn PaginatedListStore>>,
}

/// Keys asked for per listing page.
const LIST_PAGE: usize = 1000;

/// Unreadable keys skipped in one poll before it gives up.
const MAX_UNREADABLE_KEYS: usize = 1000;

/// The key a listing error names when object_store cannot read it (an empty,
/// `.` or `..` segment, or a control character).
fn unreadable_key(e: &object_store::Error) -> Option<&str> {
    use object_store::path::Error as PathError;
    match e {
        object_store::Error::InvalidPath {
            source: PathError::EmptySegment { path } | PathError::BadSegment { path, .. },
        } => Some(path),
        _ => None,
    }
}

impl ObjectArrivalWatcher {
    /// Build the object_store client for the configured store.
    pub fn build_store(store: &StoreSpec) -> Result<StoreTriple, String> {
        Self::build_paged_store(store).map(|(triple, _)| triple)
    }

    /// [`Self::build_store`], with the same client as a page-by-page lister.
    pub fn build_paged_store(
        store: &StoreSpec,
    ) -> Result<(StoreTriple, Arc<dyn PaginatedListStore>), String> {
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
                let s = Arc::new(b.build().map_err(|e| format!("building S3 client: {e}"))?);
                Ok(((s.clone(), bucket.clone(), prefix.clone()), s))
            }
            StoreSpec::Gcs { bucket, prefix } => {
                let s = Arc::new(
                    object_store::gcp::GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| format!("building GCS client: {e}"))?,
                );
                Ok(((s.clone(), bucket.clone(), prefix.clone()), s))
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
            resumed: false,
            compiled,
            poll_timeout: None,
            page_timeout: super::watcher::default_poll_timeout(poll),
            pager: None,
        }
    }

    /// List through `pager` (the same store, page by page).
    #[must_use]
    pub fn with_pager(mut self, pager: Arc<dyn PaginatedListStore>) -> Self {
        self.pager = Some(pager);
        self
    }

    /// Bound the whole poll (`poll_timeout_secs`); without it only each
    /// listing page is bounded.
    #[must_use]
    pub fn with_poll_timeout(mut self, limit: Option<Duration>) -> Self {
        self.poll_timeout = limit;
        self
    }

    /// Override how long the watcher waits for the next listing page.
    #[must_use]
    pub fn with_page_timeout(mut self, limit: Duration) -> Self {
        self.page_timeout = limit;
        self
    }

    /// Resume from the cursor a previous process stored, so objects that
    /// arrived while the server was down are still fired (`start_at: now`
    /// applies to the first start only) and fired ones are not fired again.
    async fn resume(&mut self, state: &ServerState) -> Result<(), String> {
        if self.resumed {
            return Ok(());
        }
        let stored = state
            .history()
            .meta_get(&cursor_key(&self.name))
            .await
            .map_err(|e| format!("reading the stored cursor: {e}"))?;
        match stored.and_then(|s| serde_json::from_str::<StoredCursor>(&s).ok()) {
            Some(c) if c.bucket == self.bucket && c.prefix == self.prefix => self.cursor.restore(c),
            _ => self.save(state).await,
        }
        self.resumed = true;
        Ok(())
    }

    /// Store the cursor. A failure is logged: the cursor is still held in
    /// memory, and a restart before the next good write re-fires (replay-safe
    /// through the idempotency keys) rather than skips.
    async fn save(&self, state: &ServerState) {
        let stored = self.cursor.to_stored(&self.bucket, self.prefix.as_deref());
        let Ok(text) = serde_json::to_string(&stored) else {
            return;
        };
        if let Err(e) = state
            .history()
            .meta_put(&cursor_key(&self.name), &text)
            .await
        {
            tracing::warn!(trigger = %self.name, error = %e, "could not store the object_arrival cursor");
        }
    }

    /// Remember fired objects for `lookback` behind the newest one, so a
    /// late-visible object is still fired (default [`DEFAULT_LOOKBACK`]).
    pub fn with_lookback(mut self, lookback: Duration) -> Self {
        self.cursor = std::mem::take(&mut self.cursor).with_lookback(lookback);
        self
    }

    /// List the prefix, keeping only the objects the cursor has not fired:
    /// memory follows the new objects, not the size of the prefix.
    async fn list_new(&self) -> Result<Vec<ListedObject>, String> {
        if let Some(pager) = &self.pager {
            return self.list_paged(pager.as_ref()).await;
        }
        let prefix_path = self.prefix.as_deref().map(object_store::path::Path::from);
        let mut stream = self.store.list(prefix_path.as_ref());
        let mut out = Vec::new();
        // Bound each wait for the store, not the whole listing: a large
        // prefix takes as long as it takes while pages keep arriving.
        loop {
            let next = tokio::time::timeout(self.page_timeout, stream.next())
                .await
                .map_err(|_| {
                    format!(
                        "listing objects: no response from the store within {}s",
                        self.page_timeout.as_secs()
                    )
                })?;
            let Some(meta) = next else { break };
            let meta = meta.map_err(|e| match unreadable_key(&e) {
                Some(key) => format!(
                    "listing objects: the store lists key {key:?}, which cannot be read as an \
                     object path; nothing in the prefix fires until it is removed"
                ),
                None => format!("listing objects: {e}"),
            })?;
            self.keep_if_new(meta, &mut out);
        }
        Ok(out)
    }

    fn keep_if_new(&self, meta: object_store::ObjectMeta, out: &mut Vec<ListedObject>) {
        let o = ListedObject {
            key: meta.location.to_string(),
            last_modified: meta.last_modified,
            size: meta.size,
            etag: meta.e_tag,
        };
        if self.cursor.is_new(&o) {
            out.push(o);
        }
    }

    /// [`Self::list_new`] page by page. A page holding a key object_store
    /// cannot read is asked for again in smaller pages until that key stands
    /// alone; it is then skipped (counted and logged) and the listing resumes
    /// right after it, so every other object in the prefix is still seen.
    async fn list_paged(
        &self,
        pager: &dyn PaginatedListStore,
    ) -> Result<Vec<ListedObject>, String> {
        let prefix = self
            .prefix
            .as_deref()
            .map(object_store::path::Path::from)
            .filter(|p| !p.as_ref().is_empty())
            .map(|p| format!("{}/", p.as_ref()));
        let mut out = Vec::new();
        let mut offset: Option<String> = None;
        let mut token: Option<String> = None;
        let mut size = LIST_PAGE;
        let mut skipped = 0usize;
        loop {
            let opts = PaginatedListOptions {
                offset: offset.clone(),
                max_keys: Some(size),
                page_token: token.clone(),
                ..Default::default()
            };
            let page = tokio::time::timeout(
                self.page_timeout,
                pager.list_paginated(prefix.as_deref(), opts),
            )
            .await
            .map_err(|_| {
                format!(
                    "listing objects: no response from the store within {}s",
                    self.page_timeout.as_secs()
                )
            })?;
            match page {
                Ok(page) => {
                    let empty = page.result.objects.is_empty();
                    for meta in page.result.objects {
                        self.keep_if_new(meta, &mut out);
                    }
                    match page.page_token {
                        Some(t) if !empty || token.as_deref() != Some(t.as_str()) => {
                            token = Some(t)
                        }
                        _ => return Ok(out),
                    }
                }
                Err(e) => {
                    let Some(key) = unreadable_key(&e) else {
                        return Err(format!("listing objects: {e}"));
                    };
                    if size > 1 {
                        size /= 2;
                        continue;
                    }
                    skipped += 1;
                    super::metrics::error(&self.name, "object_arrival");
                    tracing::warn!(
                        trigger = %self.name,
                        key = ?key,
                        "object skipped: its key cannot be read as an object path"
                    );
                    if skipped > MAX_UNREADABLE_KEYS {
                        return Err(format!(
                            "listing objects: more than {MAX_UNREADABLE_KEYS} keys in the prefix \
                             cannot be read as object paths (the last: {key:?})"
                        ));
                    }
                    offset = Some(key.to_string());
                    token = None;
                    size = LIST_PAGE;
                }
            }
        }
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

    fn poll_timeout(&self) -> Option<Duration> {
        self.poll_timeout
    }

    async fn poll(&mut self, state: &ServerState) -> Result<bool, super::watcher::PollError> {
        self.resume(state).await?;
        let mut new = self.list_new().await?;
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
                        FireOutcome::Failed(e) | FireOutcome::Refused(e) => {
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
                    FireOutcome::Failed(e) | FireOutcome::Refused(e) => {
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
        if fired || problem.is_some() {
            self.save(state).await;
        }
        match problem {
            Some(e) => Err(super::watcher::PollError::fired(e)),
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
    fn is_new_is_the_per_object_form_of_new_objects() {
        let mut c = Cursor::starting_beginning();
        let (a, b) = (obj("a", 100), obj("b", 200));
        c.commit(&a);
        assert!(!c.is_new(&a));
        assert!(c.is_new(&b));
        assert_eq!(c.new_objects(&[a, b.clone()]), vec![b]);
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

#[cfg(test)]
mod listing_tests {
    use super::*;
    use crate::serve::triggers::spec::{RunTemplate, TriggerKind, TriggerSpec};

    /// A ListObjectsV2 endpoint over a fixed key set: honours `prefix`,
    /// `start-after`, `continuation-token` (the last key returned) and
    /// `max-keys`.
    struct FakeS3 {
        keys: Vec<&'static str>,
    }

    impl wiremock::Respond for FakeS3 {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            let q: std::collections::HashMap<String, String> =
                req.url.query_pairs().into_owned().collect();
            let after = q
                .get("continuation-token")
                .or_else(|| q.get("start-after"))
                .cloned()
                .unwrap_or_default();
            let prefix = q.get("prefix").cloned().unwrap_or_default();
            let max: usize = q
                .get("max-keys")
                .and_then(|m| m.parse().ok())
                .unwrap_or(1000);
            let mut keys: Vec<&str> = self
                .keys
                .iter()
                .copied()
                .filter(|k| k.starts_with(prefix.as_str()) && *k > after.as_str())
                .collect();
            keys.sort_unstable();
            let more = keys.len() > max;
            keys.truncate(max);
            let mut xml =
                String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
            for k in &keys {
                xml.push_str(&format!(
                    "<Contents><Key>{k}</Key><Size>1</Size>\
                     <LastModified>2026-11-11T00:00:00.000Z</LastModified></Contents>"
                ));
            }
            if more {
                xml.push_str(&format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    keys.last().unwrap()
                ));
            }
            xml.push_str("</ListBucketResult>");
            wiremock::ResponseTemplate::new(200).set_body_string(xml)
        }
    }

    fn compiled() -> Arc<super::super::compiled::CompiledTrigger> {
        Arc::new(super::super::compiled::CompiledTrigger {
            spec: TriggerSpec {
                name: "drop".into(),
                enabled: true,
                config: None,
                template: None,
                tenants: None,
                run: RunTemplate {
                    name: None,
                    labels: Default::default(),
                    timeout_secs: None,
                    selection: None,
                    depends_on: None,
                    pool: None,
                },
                kind: TriggerKind::Webhook {
                    methods: vec!["POST".into()],
                    dedupe_header: None,
                    debounce_secs: 0,
                    content_types: super::super::spec::default_webhook_content_types(),
                },
            },
            webhook_path: None,
        })
    }

    #[tokio::test]
    async fn an_unreadable_key_is_skipped_and_the_rest_still_fire() {
        let server = wiremock::MockServer::start().await;
        let mut keys = vec!["in//empty.csv", "in/b/./c.csv", "in/d.csv", "out/x.csv"];
        keys.extend([
            "in/a0.csv",
            "in/a1.csv",
            "in/a2.csv",
            "in/a3.csv",
            "in/a4.csv",
        ]);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/b"))
            .respond_with(FakeS3 { keys })
            .mount(&server)
            .await;
        let s3 = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("b")
                .with_region("us-east-1")
                .with_access_key_id("k")
                .with_secret_access_key("s")
                .with_endpoint(server.uri())
                .with_allow_http(true)
                .build()
                .unwrap(),
        );
        let watcher = |pager: bool| {
            let w = ObjectArrivalWatcher::new(
                compiled(),
                s3.clone(),
                "b".into(),
                Some("in".into()),
                ArrivalMode::PerObject,
                Duration::from_secs(30),
                StartAt::Beginning,
                chrono::Utc::now(),
            );
            if pager { w.with_pager(s3.clone()) } else { w }
        };
        let listed: Vec<String> = watcher(true)
            .list_new()
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(
            listed,
            [
                "in/a0.csv",
                "in/a1.csv",
                "in/a2.csv",
                "in/a3.csv",
                "in/a4.csv",
                "in/d.csv"
            ]
        );
        // A store listed only as a stream reports the key it cannot read.
        let err = watcher(false).list_new().await.unwrap_err();
        assert!(err.contains("in//empty.csv"), "{err}");
    }
}
