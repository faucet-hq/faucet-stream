//! Boundary-precise failure injection for the reliability suites (#651
//! Category A).
//!
//! The engine's guarantees are all statements about **ordering across a
//! boundary** — "the bookmark is persisted only after the sink confirms", "the
//! staging table is swapped only after a fully successful run", "a cancel still
//! flushes". A unit test over pure logic cannot observe an ordering, and a
//! hand-rolled `FailingSink` that fails on *every* call cannot fail at one
//! specific boundary. So each such test used to re-invent a slightly different
//! double, and the interesting case — fail on the *third* write, succeed
//! before and after — was rarely covered.
//!
//! This module supplies the two things those tests actually need:
//!
//! - [`EventLog`] — one shared, ordered record of everything the sink and the
//!   state store did. Assertions are then written against the *sequence*
//!   (`writes_before_each_bookmark`), which is what the guarantee says, rather
//!   than against a final count, which is what a weaker test checks.
//! - [`ScriptedSink`] / [`ScriptedStateStore`] — doubles that fail (or stall)
//!   at exactly one named [`Boundary`] and behave normally everywhere else.
//!
//! Both are cheap clones over shared state, so a test keeps a handle to inspect
//! after moving the double into the pipeline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use faucet_core::{FaucetError, Sink, StateStore, Value, async_trait, json};

/// One thing that happened, in the order it happened.
///
/// The variants deliberately carry the *payload size or identity* rather than
/// the payload: an assertion about ordering needs to know that write #3 landed
/// before the bookmark that followed it, not what was in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `write_batch` (or one of its idempotent / partial variants) succeeded
    /// with this many records.
    Write(usize),
    /// A write was attempted and the script failed it. Carries the record count
    /// so a test can tell *which* batch was rejected.
    WriteFailed(usize),
    /// `flush` succeeded.
    Flush,
    /// `flush` was attempted and the script failed it.
    FlushFailed,
    /// `write_batch_idempotent` succeeded, carrying the commit token the sink
    /// stored. The token is the one thing a watermark test must see verbatim.
    IdempotentWrite { records: usize, token: String },
    /// `begin_overwrite` succeeded.
    BeginOverwrite,
    /// `commit_overwrite` succeeded — the atomic swap actually happened.
    CommitOverwrite,
    /// `abort_overwrite` succeeded — the staged data was discarded.
    AbortOverwrite,
    /// `StateStore::put` succeeded for `key`, carrying the persisted value so a
    /// test can assert *which* bookmark was durable at that point.
    StatePut { key: String, value: Value },
    /// A `StateStore::put` was attempted and the script failed it.
    StatePutFailed { key: String },
    /// `cleanup_scope` ran with this many tracked keys.
    Cleanup(usize),
}

/// The shared, ordered event log. Clone it to keep a handle after the doubles
/// are moved into the pipeline.
#[derive(Debug, Clone, Default)]
pub struct EventLog(Arc<Mutex<Vec<Event>>>);

impl EventLog {
    /// A fresh, empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one event. Never blocks on a poisoned lock — a panicking test
    /// should fail on its own assertion, not on a secondary lock panic.
    pub fn push(&self, event: Event) {
        if let Ok(mut g) = self.0.lock() {
            g.push(event);
        }
    }

    /// Every event, in order.
    pub fn events(&self) -> Vec<Event> {
        self.0.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Total records across successful writes of every kind.
    pub fn records_written(&self) -> usize {
        self.events()
            .iter()
            .map(|e| match e {
                Event::Write(n) | Event::IdempotentWrite { records: n, .. } => *n,
                _ => 0,
            })
            .sum()
    }

    /// The values persisted by `StateStore::put`, in order.
    pub fn bookmarks(&self) -> Vec<Value> {
        self.events()
            .iter()
            .filter_map(|e| match e {
                Event::StatePut { value, .. } => Some(value.clone()),
                _ => None,
            })
            .collect()
    }

    /// For each successful `StateStore::put`, how many records had been
    /// **confirmed written** before it.
    ///
    /// This is the shape the core durability guarantee is stated in: a bookmark
    /// must never be persisted covering data the sink has not confirmed. A test
    /// asserts the returned counts against the data each bookmark claims.
    pub fn writes_before_each_bookmark(&self) -> Vec<usize> {
        let mut confirmed = 0usize;
        let mut out = Vec::new();
        for e in self.events() {
            match e {
                Event::Write(n) | Event::IdempotentWrite { records: n, .. } => confirmed += n,
                Event::StatePut { .. } => out.push(confirmed),
                _ => {}
            }
        }
        out
    }

    /// Whether `event` occurs anywhere in the log.
    pub fn contains(&self, event: &Event) -> bool {
        self.events().iter().any(|e| e == event)
    }

    /// Whether any event matches `pred` — for variants carrying a payload a
    /// test does not want to spell out in full.
    pub fn any(&self, pred: impl Fn(&Event) -> bool) -> bool {
        self.events().iter().any(pred)
    }

    /// Index of the first event matching `pred`, for order assertions.
    pub fn position(&self, pred: impl Fn(&Event) -> bool) -> Option<usize> {
        self.events().iter().position(pred)
    }
}

/// Assert the core durability guarantee over an [`EventLog`]: **no bookmark is
/// persisted before the sink confirmed the data that bookmark claims.**
///
/// `records_claimed` maps a persisted bookmark to the number of records it
/// asserts are durable. A bookmark that claims more than had been confirmed at
/// the moment it was written is a data-loss bug: a resumed run starts after it
/// and the unconfirmed records are never read again.
///
/// This lives here, rather than inline in each test, so the *same* assertion
/// can be pointed at a deliberately-wrong event log — which is how the suites
/// prove the check can actually fail (see this module's tests).
pub fn assert_bookmarks_backed_by_writes(
    log: &EventLog,
    records_claimed: impl Fn(&Value) -> usize,
) {
    let mut confirmed = 0usize;
    for (i, event) in log.events().into_iter().enumerate() {
        match event {
            Event::Write(n) | Event::IdempotentWrite { records: n, .. } => confirmed += n,
            Event::StatePut { ref value, .. } => {
                let claimed = records_claimed(value);
                assert!(
                    claimed <= confirmed,
                    "durability violated at event {i}: bookmark {value} claims {claimed} \
                     records are durable, but only {confirmed} had been confirmed by the sink. \
                     A resumed run would start after this bookmark and never re-read the \
                     difference.\nfull log: {:?}",
                    log.events()
                );
            }
            _ => {}
        }
    }
}

/// The boundary a script fails at.
///
/// Every variant names a real transition the engine makes, and each one is a
/// place a naive implementation gets the ordering wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Boundary {
    /// Never fail — the control arm every failure test needs for comparison.
    #[default]
    Never,
    /// Fail the write of the (0-indexed) `nth` batch. Batches before and after
    /// it succeed, so the test observes a *partial* run rather than a total
    /// one.
    Write(usize),
    /// Fail `flush`. The writes themselves succeeded, so this is the case where
    /// the sink accepted rows it cannot guarantee are durable — the bookmark
    /// must not advance.
    Flush,
    /// Fail the (0-indexed) `nth` `StateStore::put`. Models a crash in the
    /// window between a confirmed sink write and a durable bookmark: on resume
    /// the pipeline must re-read, producing duplicates under at-least-once and
    /// none under exactly-once.
    StatePut(usize),
    /// Fail `commit_overwrite` — the atomic swap itself. The prior destination
    /// must survive.
    CommitOverwrite,
}

/// Shared script state behind a [`ScriptedSink`] / [`ScriptedStateStore`].
#[derive(Debug)]
struct Script {
    boundary: Boundary,
    writes: usize,
    state_puts: usize,
}

/// A sink that logs every call into a shared [`EventLog`] and fails at exactly
/// one [`Boundary`].
///
/// Capability advertisement is configurable because several guarantees are
/// *about* the advertisement: the retry gate keys off
/// [`Sink::write_batch_is_replay_safe`], and the exactly-once path off
/// [`Sink::supports_idempotent_writes`]. A test that wants to prove the engine
/// consults the right one needs a sink that can advertise each independently —
/// including the combination that caused the real defect (token-capable, plain
/// write *not* replay-safe).
pub struct ScriptedSink {
    log: EventLog,
    script: Arc<Mutex<Script>>,
    idempotent: bool,
    replay_safe: bool,
    keyed: bool,
    overwrite: bool,
    /// Commit token last stored per scope, for `last_committed_token`.
    tokens: Arc<Mutex<std::collections::HashMap<String, String>>>,
    /// Delay injected into each write, for backpressure / latency tests.
    write_delay: Option<Duration>,
}

impl ScriptedSink {
    /// An append-only sink that never fails.
    pub fn new(log: EventLog) -> Self {
        Self {
            log,
            script: Arc::new(Mutex::new(Script {
                boundary: Boundary::Never,
                writes: 0,
                state_puts: 0,
            })),
            idempotent: false,
            replay_safe: false,
            keyed: false,
            overwrite: false,
            tokens: Arc::new(Mutex::new(std::collections::HashMap::new())),
            write_delay: None,
        }
    }

    /// Fail at `boundary`.
    pub fn failing_at(mut self, boundary: Boundary) -> Self {
        self.script = Arc::new(Mutex::new(Script {
            boundary,
            writes: 0,
            state_puts: 0,
        }));
        self
    }

    /// Advertise [`Sink::supports_idempotent_writes`] and honour the
    /// commit-token protocol.
    pub fn idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Advertise [`Sink::write_batch_is_replay_safe`] — i.e. claim the *plain*
    /// write converges on replay. Independent of [`Self::idempotent`] on
    /// purpose: the two were conflated once, and the engine retried
    /// multi-row INSERTs as a result.
    pub fn replay_safe(mut self) -> Self {
        self.replay_safe = true;
        self
    }

    /// Advertise keyed dedup (`dedups_by_key`), the keyed-upsert exactly-once
    /// mechanism.
    pub fn keyed(mut self) -> Self {
        self.keyed = true;
        self.replay_safe = true;
        self
    }

    /// Advertise the overwrite lifecycle so `begin`/`commit`/`abort` are driven.
    pub fn overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Sleep this long inside every write, to model a slow destination.
    pub fn with_write_delay(mut self, delay: Duration) -> Self {
        self.write_delay = Some(delay);
        self
    }

    /// Re-arm the script at runtime, resetting the write / state-put counters.
    ///
    /// This is what makes a two-phase crash test expressible against **one**
    /// sink instance: run 1 fails at a boundary, then the same sink — still
    /// holding its commit tokens, exactly as a real destination would across a
    /// process restart — is re-armed to [`Boundary::Never`] for run 2. A second
    /// sink instance would start with an empty token store and quietly turn the
    /// interesting test into a trivial one.
    pub fn rearm(&self, boundary: Boundary) {
        let mut s = self.script.lock().expect("script lock");
        s.boundary = boundary;
        s.writes = 0;
        s.state_puts = 0;
    }

    /// A [`ScriptedStateStore`] sharing this sink's log and script, so a single
    /// [`Boundary::StatePut`] script drives both sides and the resulting event
    /// log interleaves writes and bookmarks in true order.
    pub fn state_store(&self) -> ScriptedStateStore {
        ScriptedStateStore {
            log: self.log.clone(),
            script: Arc::clone(&self.script),
            values: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// The shared event log.
    pub fn log(&self) -> EventLog {
        self.log.clone()
    }

    /// Decide whether this write should fail, counting it either way.
    fn should_fail_write(&self) -> bool {
        let mut s = self.script.lock().expect("script lock");
        let nth = s.writes;
        s.writes += 1;
        matches!(s.boundary, Boundary::Write(n) if n == nth)
    }

    fn should_fail_flush(&self) -> bool {
        let s = self.script.lock().expect("script lock");
        s.boundary == Boundary::Flush
    }

    fn should_fail_commit(&self) -> bool {
        let s = self.script.lock().expect("script lock");
        s.boundary == Boundary::CommitOverwrite
    }

    async fn record_write(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if let Some(d) = self.write_delay {
            tokio::time::sleep(d).await;
        }
        if self.should_fail_write() {
            self.log.push(Event::WriteFailed(records.len()));
            return Err(FaucetError::Sink(format!(
                "scripted sink: write of {} records failed at the scripted boundary",
                records.len()
            )));
        }
        self.log.push(Event::Write(records.len()));
        Ok(records.len())
    }
}

#[async_trait]
impl Sink for ScriptedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.record_write(records).await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        if self.should_fail_flush() {
            self.log.push(Event::FlushFailed);
            return Err(FaucetError::Sink(
                "scripted sink: flush failed at the scripted boundary".into(),
            ));
        }
        self.log.push(Event::Flush);
        Ok(())
    }

    fn supports_idempotent_writes(&self) -> bool {
        self.idempotent
    }

    fn write_batch_is_replay_safe(&self) -> bool {
        self.replay_safe
    }

    fn dedups_by_key(&self) -> bool {
        self.keyed
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        if let Some(d) = self.write_delay {
            tokio::time::sleep(d).await;
        }
        if self.should_fail_write() {
            self.log.push(Event::WriteFailed(records.len()));
            return Err(FaucetError::Sink(
                "scripted sink: idempotent write failed at the scripted boundary".into(),
            ));
        }
        // Records and token commit together — that is the whole point of this
        // path, so the double must not be able to store one without the other.
        self.tokens
            .lock()
            .expect("token lock")
            .insert(scope.to_string(), token.to_string());
        self.log.push(Event::IdempotentWrite {
            records: records.len(),
            token: token.to_string(),
        });
        Ok(records.len())
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.tokens.lock().expect("token lock").get(scope).cloned())
    }

    fn is_overwrite(&self) -> bool {
        self.overwrite
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.log.push(Event::BeginOverwrite);
        Ok(())
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        if self.should_fail_commit() {
            return Err(FaucetError::Sink(
                "scripted sink: commit_overwrite failed at the scripted boundary".into(),
            ));
        }
        self.log.push(Event::CommitOverwrite);
        Ok(())
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.log.push(Event::AbortOverwrite);
        Ok(())
    }

    fn config_schema(&self) -> Value {
        json!({ "type": "object", "title": "ScriptedSink" })
    }

    fn connector_name(&self) -> &'static str {
        "scripted"
    }
}

/// A source that emits `pages` pages of `per_page` records, **each page
/// carrying its own bookmark** — the CDC/streaming shape.
///
/// This is what makes the durability guarantee observable at all. The
/// batch-shaped [`CountingSource`](crate::doubles::CountingSource) emits one
/// bookmark at the very end, so there is only ever one write/bookmark pair and
/// no ordering to get wrong. Here every page produces a
/// write-then-bookmark pair, so a test can assert the *interleaving* across a
/// mid-run failure.
///
/// Records are `{"page": p, "n": global_index}` and the bookmark is
/// `{"page": p}` (0-indexed). `apply_start_bookmark` resumes *after* the given
/// page, so a resumed run emits only what the first run had not confirmed.
pub struct PagedSource {
    pages: usize,
    per_page: usize,
    /// Pages already covered by a durable bookmark; the run starts after this.
    start_after: Arc<Mutex<Option<usize>>>,
    /// Pages the source actually emitted, for asserting resume behaviour.
    emitted: Arc<Mutex<Vec<usize>>>,
}

impl PagedSource {
    /// `pages` pages of `per_page` records each.
    pub fn new(pages: usize, per_page: usize) -> Self {
        Self {
            pages,
            per_page,
            start_after: Arc::new(Mutex::new(None)),
            emitted: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Page indices this source emitted, in order — so a resume test can show
    /// it re-read exactly the unconfirmed tail and nothing more.
    pub fn emitted_pages(&self) -> Vec<usize> {
        self.emitted.lock().expect("emitted lock").clone()
    }

    /// Total records this source would emit from a cold start.
    pub fn total_records(&self) -> usize {
        self.pages * self.per_page
    }
}

#[async_trait]
impl faucet_core::Source for PagedSource {
    async fn fetch_with_context(
        &self,
        _context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // The streaming path is the one under test; this exists only to satisfy
        // the trait and returns the same data in one shot.
        let start = self
            .start_after
            .lock()
            .expect("start lock")
            .map(|p| p + 1)
            .unwrap_or(0);
        let mut out = Vec::new();
        for p in start..self.pages {
            for i in 0..self.per_page {
                out.push(json!({ "page": p, "n": p * self.per_page + i }));
            }
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> std::pin::Pin<
        Box<
            dyn futures_core::Stream<Item = Result<faucet_core::StreamPage, FaucetError>>
                + Send
                + 'a,
        >,
    > {
        let start = self
            .start_after
            .lock()
            .expect("start lock")
            .map(|p| p + 1)
            .unwrap_or(0);
        Box::pin(async_stream::try_stream! {
            for p in start..self.pages {
                self.emitted.lock().expect("emitted lock").push(p);
                let records: Vec<Value> = (0..self.per_page)
                    .map(|i| json!({ "page": p, "n": p * self.per_page + i }))
                    .collect();
                yield faucet_core::StreamPage {
                    records,
                    bookmark: Some(json!({ "page": p })),
                };
            }
        })
    }

    fn state_key(&self) -> Option<String> {
        Some("scripted:paged".into())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        // Accept both the bare bookmark and an exactly-once envelope, since the
        // engine hands back whichever the delivery mode produced.
        let bare = faucet_core::idempotency::unwrap_state(&bookmark)
            .0
            .unwrap_or(bookmark);
        if let Some(p) = bare.get("page").and_then(|v| v.as_u64()) {
            *self.start_after.lock().expect("start lock") = Some(p as usize);
        }
        Ok(())
    }

    fn supports_exactly_once(&self) -> bool {
        // An immutable, completely-bookmarked log: replaying from a bookmark
        // yields byte-identical pages, which is the precondition the
        // atomic-watermark mechanism needs.
        true
    }

    fn config_schema(&self) -> Value {
        json!({ "type": "object", "title": "PagedSource" })
    }

    fn connector_name(&self) -> &'static str {
        "paged"
    }
}

/// A [`StateStore`] that logs every `put` into the shared [`EventLog`] and can
/// fail the nth one.
///
/// Build it from [`ScriptedSink::state_store`] so the sink and the store share
/// one script and one log — otherwise the interleaving that the ordering
/// guarantee is *about* cannot be observed.
pub struct ScriptedStateStore {
    log: EventLog,
    script: Arc<Mutex<Script>>,
    values: Arc<Mutex<std::collections::HashMap<String, Value>>>,
}

impl ScriptedStateStore {
    /// A store that logs but never fails, with no shared sink.
    pub fn new(log: EventLog) -> Self {
        Self {
            log,
            script: Arc::new(Mutex::new(Script {
                boundary: Boundary::Never,
                writes: 0,
                state_puts: 0,
            })),
            values: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Seed a key, to model resuming a run whose bookmark is already durable.
    pub fn seed(self, key: &str, value: Value) -> Self {
        self.values
            .lock()
            .expect("value lock")
            .insert(key.to_string(), value);
        self
    }

    /// The value currently stored under `key` — what a *restart* would read.
    pub fn stored(&self, key: &str) -> Option<Value> {
        self.values.lock().expect("value lock").get(key).cloned()
    }

    fn should_fail_put(&self) -> bool {
        let mut s = self.script.lock().expect("script lock");
        let nth = s.state_puts;
        s.state_puts += 1;
        matches!(s.boundary, Boundary::StatePut(n) if n == nth)
    }
}

#[async_trait]
impl StateStore for ScriptedStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        Ok(self.values.lock().expect("value lock").get(key).cloned())
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        if self.should_fail_put() {
            self.log.push(Event::StatePutFailed {
                key: key.to_string(),
            });
            return Err(FaucetError::State(
                "scripted state store: put failed at the scripted boundary".into(),
            ));
        }
        self.values
            .lock()
            .expect("value lock")
            .insert(key.to_string(), value.clone());
        self.log.push(Event::StatePut {
            key: key.to_string(),
            value: value.clone(),
        });
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.values.lock().expect("value lock").remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(n: usize) -> Vec<Value> {
        (0..n).map(|i| json!({ "n": i })).collect()
    }

    #[tokio::test]
    async fn log_orders_writes_and_bookmarks_as_they_happen() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone());
        let store = sink.state_store();

        sink.write_batch(&rec(2)).await.expect("write 1");
        store.put("k", &json!({"at": 2})).await.expect("put 1");
        sink.write_batch(&rec(3)).await.expect("write 2");
        store.put("k", &json!({"at": 5})).await.expect("put 2");

        assert_eq!(
            log.events(),
            vec![
                Event::Write(2),
                Event::StatePut {
                    key: "k".into(),
                    value: json!({"at": 2})
                },
                Event::Write(3),
                Event::StatePut {
                    key: "k".into(),
                    value: json!({"at": 5})
                },
            ]
        );
        assert_eq!(log.records_written(), 5);
        // The guarantee shape: each bookmark is backed by that many confirmed
        // records.
        assert_eq!(log.writes_before_each_bookmark(), vec![2, 5]);
    }

    #[tokio::test]
    async fn write_boundary_fails_exactly_the_nth_batch() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Write(1));

        assert_eq!(sink.write_batch(&rec(2)).await.expect("batch 0 ok"), 2);
        sink.write_batch(&rec(3))
            .await
            .expect_err("batch 1 must fail");
        assert_eq!(sink.write_batch(&rec(4)).await.expect("batch 2 ok"), 4);

        assert_eq!(
            log.events(),
            vec![Event::Write(2), Event::WriteFailed(3), Event::Write(4)],
            "only the scripted batch fails; the rest behave normally"
        );
        assert_eq!(log.records_written(), 6, "the failed batch is not counted");
    }

    #[tokio::test]
    async fn state_put_boundary_fails_exactly_the_nth_put() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::StatePut(1));
        let store = sink.state_store();

        store.put("k", &json!(1)).await.expect("put 0 ok");
        store
            .put("k", &json!(2))
            .await
            .expect_err("put 1 must fail");
        store.put("k", &json!(3)).await.expect("put 2 ok");

        // The failed put left the *old* value durable — which is what makes the
        // crash-window test meaningful.
        assert_eq!(store.stored("k"), Some(json!(3)));
        assert_eq!(log.bookmarks(), vec![json!(1), json!(3)]);
        assert!(log.contains(&Event::StatePutFailed { key: "k".into() }));
    }

    #[tokio::test]
    async fn flush_boundary_fails_only_flush() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Flush);
        sink.write_batch(&rec(1)).await.expect("write succeeds");
        sink.flush().await.expect_err("flush must fail");
        assert_eq!(log.events(), vec![Event::Write(1), Event::FlushFailed]);
    }

    #[tokio::test]
    async fn capabilities_are_advertised_independently() {
        let log = EventLog::new();

        // The combination behind the real defect: the sink advertises the
        // commit-token protocol, but its plain write is NOT replay-safe.
        let token_only = ScriptedSink::new(log.clone()).idempotent();
        assert!(token_only.supports_idempotent_writes());
        assert!(!token_only.write_batch_is_replay_safe());

        // A keyed sink converges on replay without any watermark.
        let keyed = ScriptedSink::new(log.clone()).keyed();
        assert!(keyed.dedups_by_key());
        assert!(keyed.write_batch_is_replay_safe());
        assert!(!keyed.supports_idempotent_writes());
    }

    #[tokio::test]
    async fn idempotent_write_commits_records_and_token_together() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone()).idempotent();
        assert_eq!(sink.last_committed_token("s").await.expect("read"), None);

        sink.write_batch_idempotent(&rec(3), "s", "t1")
            .await
            .expect("write");
        assert_eq!(
            sink.last_committed_token("s").await.expect("read"),
            Some("t1".into())
        );
        assert!(log.contains(&Event::IdempotentWrite {
            records: 3,
            token: "t1".into()
        }));

        // A failed idempotent write stores neither.
        let failing = ScriptedSink::new(log.clone())
            .idempotent()
            .failing_at(Boundary::Write(0));
        failing
            .write_batch_idempotent(&rec(1), "s2", "t2")
            .await
            .expect_err("must fail");
        assert_eq!(
            failing.last_committed_token("s2").await.expect("read"),
            None,
            "a failed write must not leave a token behind"
        );
    }

    #[tokio::test]
    async fn overwrite_lifecycle_is_logged_and_commit_can_be_failed() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone())
            .overwrite()
            .failing_at(Boundary::CommitOverwrite);
        assert!(sink.is_overwrite());
        sink.begin_overwrite().await.expect("begin");
        sink.commit_overwrite().await.expect_err("commit must fail");
        sink.abort_overwrite().await.expect("abort");
        assert_eq!(
            log.events(),
            vec![Event::BeginOverwrite, Event::AbortOverwrite],
            "a failed commit logs no CommitOverwrite — the swap did not happen"
        );
    }

    #[tokio::test]
    async fn never_boundary_is_the_control_arm() {
        let log = EventLog::new();
        let sink = ScriptedSink::new(log.clone());
        for _ in 0..5 {
            sink.write_batch(&rec(1)).await.expect("no failure");
        }
        sink.flush().await.expect("no failure");
        assert!(!log.any(|e| matches!(
            e,
            Event::WriteFailed(_) | Event::FlushFailed | Event::StatePutFailed { .. }
        )));
    }

    #[tokio::test]
    async fn seeded_store_models_resuming_a_durable_bookmark() {
        let store = ScriptedStateStore::new(EventLog::new()).seed("k", json!({"at": 7}));
        assert_eq!(store.get("k").await.expect("get"), Some(json!({"at": 7})));
        store.delete("k").await.expect("delete");
        assert_eq!(store.get("k").await.expect("get"), None);
    }

    /// Bookmark shape used by the guarantee-assertion tests: `{"at": n}`
    /// claims `n` records are durable.
    fn claims(v: &Value) -> usize {
        v["at"].as_u64().unwrap_or(0) as usize
    }

    #[test]
    fn guarantee_holds_when_the_bookmark_follows_its_writes() {
        let log = EventLog::new();
        log.push(Event::Write(3));
        log.push(Event::StatePut {
            key: "k".into(),
            value: json!({"at": 3}),
        });
        log.push(Event::Write(2));
        log.push(Event::StatePut {
            key: "k".into(),
            value: json!({"at": 5}),
        });
        assert_bookmarks_backed_by_writes(&log, claims);
    }

    /// The failing-first proof: a check that cannot fail is worthless. This is
    /// the naive ordering — persist the bookmark, *then* write — which is
    /// exactly the bug the guarantee exists to prevent.
    #[test]
    #[should_panic(expected = "durability violated")]
    fn guarantee_fails_when_the_bookmark_precedes_its_writes() {
        let log = EventLog::new();
        log.push(Event::StatePut {
            key: "k".into(),
            value: json!({"at": 3}),
        });
        log.push(Event::Write(3));
        assert_bookmarks_backed_by_writes(&log, claims);
    }

    #[test]
    #[should_panic(expected = "durability violated")]
    fn guarantee_fails_when_a_bookmark_overclaims_after_a_partial_write() {
        // Two pages were read, only one confirmed, but the bookmark advanced
        // for both — the silent-loss shape.
        let log = EventLog::new();
        log.push(Event::Write(3));
        log.push(Event::WriteFailed(3));
        log.push(Event::StatePut {
            key: "k".into(),
            value: json!({"at": 6}),
        });
        assert_bookmarks_backed_by_writes(&log, claims);
    }

    #[test]
    fn guarantee_counts_idempotent_writes_as_confirmation() {
        // The exactly-once path confirms through a different method; the
        // guarantee must recognise it, or every EO run would look like a
        // violation.
        let log = EventLog::new();
        log.push(Event::IdempotentWrite {
            records: 4,
            token: "t".into(),
        });
        log.push(Event::StatePut {
            key: "k".into(),
            value: json!({"at": 4}),
        });
        assert_bookmarks_backed_by_writes(&log, claims);
    }

    #[test]
    fn position_finds_the_first_match_for_order_assertions() {
        let log = EventLog::new();
        log.push(Event::Write(1));
        log.push(Event::Flush);
        log.push(Event::Write(2));
        assert_eq!(log.position(|e| matches!(e, Event::Flush)), Some(1));
        assert_eq!(log.position(|e| matches!(e, Event::Cleanup(_))), None);
    }
}
