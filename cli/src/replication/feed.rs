//! One change stream, many tables (#731). The demultiplexer reads the shared
//! CDC source once and hands each table's records — with the stream's own
//! bookmarks — to that table's pipeline through a [`ChannelSource`]. Every
//! table pipeline is an ordinary executor run with its own sink, governance,
//! delivery mode and state key, so a table commits its position only after its
//! own sink has flushed; the stream resumes from the earliest table and each
//! table skips what it has already applied.

use crate::replication::tables::{Route, Router};
use chrono::{DateTime, Utc};
use faucet_core::{FaucetError, Source, StreamPage};
use futures::StreamExt;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Pages buffered per table before the stream waits on that table's sink.
const CHANNEL_CAPACITY: usize = 4;

/// How often an idle table is sent a bookmark-only page, so its committed
/// position (and the stream's resume point) keeps up with the stream.
pub const IDLE_FLUSH: Duration = Duration::from_secs(5);

/// The demux side of one table's feed.
pub struct TableFeed {
    pub table: String,
    tx: mpsc::Sender<StreamPage>,
    ready: oneshot::Receiver<Option<Value>>,
}

/// A [`Source`] fed by the demultiplexer. It reports the underlying CDC
/// source's identity (connector name, state schema, replay guarantee) so the
/// table's state and exactly-once watermark are those of the real stream.
pub struct ChannelSource {
    inner: Arc<dyn Source>,
    start: Mutex<Option<Value>>,
    ready: Mutex<Option<oneshot::Sender<Option<Value>>>>,
    rx: tokio::sync::Mutex<Option<mpsc::Receiver<StreamPage>>>,
}

/// A connected feed pair for `table`.
pub fn channel(table: &str, inner: Arc<dyn Source>) -> (TableFeed, ChannelSource) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (ready_tx, ready_rx) = oneshot::channel();
    (
        TableFeed {
            table: table.to_string(),
            tx,
            ready: ready_rx,
        },
        ChannelSource {
            inner,
            start: Mutex::new(None),
            ready: Mutex::new(Some(ready_tx)),
            rx: tokio::sync::Mutex::new(Some(rx)),
        },
    )
}

#[async_trait::async_trait]
impl Source for ChannelSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut pages = self.stream_pages(ctx, faucet_core::DEFAULT_BATCH_SIZE);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(faucet_core::async_stream::try_stream! {
            let start = self.start.lock().map(|g| g.clone()).unwrap_or(None);
            let ready = self.ready.lock().ok().and_then(|mut g| g.take());
            if let Some(tx) = ready {
                let _ = tx.send(start);
            }
            let rx = self.rx.lock().await.take();
            if let Some(mut rx) = rx {
                while let Some(page) = rx.recv().await {
                    yield page;
                }
            }
        })
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        if let Ok(mut g) = self.start.lock() {
            *g = Some(bookmark);
        }
        Ok(())
    }

    fn config_schema(&self) -> Value {
        self.inner.config_schema()
    }

    fn state_key(&self) -> Option<String> {
        Some(
            self.inner
                .state_key()
                .unwrap_or_else(|| "mirror".to_string()),
        )
    }

    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }

    fn state_schema(&self) -> u32 {
        self.inner.state_schema()
    }

    fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
        self.inner.migrate_state(from, data)
    }

    fn supports_exactly_once(&self) -> bool {
        self.inner.supports_exactly_once()
    }

    fn replay_guarantee(&self) -> faucet_core::ReplayGuarantee {
        self.inner.replay_guarantee()
    }

    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
}

/// A source with nothing to read and no identity of its own — backs the empty
/// feed of an atomic destination truncate.
pub struct EmptySource;

#[async_trait::async_trait]
impl Source for EmptySource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }

    fn connector_name(&self) -> &'static str {
        "mirror"
    }
}

/// Live per-table counters the orchestrator folds into the marker.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableLive {
    /// Change records routed this cycle.
    pub changes: u64,
    /// Last time the table was sent a bookmark.
    pub last_applied_at: Option<DateTime<Utc>>,
}

/// Shared live counters, keyed by table.
pub type LiveStats = Arc<Mutex<BTreeMap<String, TableLive>>>;

/// How one stream cycle ended.
#[derive(Debug, Default)]
pub struct DemuxOutcome {
    /// Matching tables seen on the stream that the mirror does not know.
    pub new_tables: BTreeSet<String>,
    /// Tables whose pipeline went away before or during the cycle.
    pub dead: BTreeSet<String>,
    /// The stream position the cycle started from.
    pub started_at: Option<Value>,
}

struct FeedState {
    table: String,
    tx: Option<mpsc::Sender<StreamPage>>,
    position: Option<Value>,
    last_sent: Option<Value>,
    last_sent_at: Instant,
}

/// Run one cycle of the shared stream: wait for every table pipeline to open
/// (reporting its committed position), resume the source from the earliest
/// one, route each record to its table, and close every feed when the source
/// ends, `cancel` fires, or a new matching table appears.
pub async fn run_demux(
    source: Arc<dyn Source>,
    feeds: Vec<TableFeed>,
    router: Router,
    cancel: CancellationToken,
    live: LiveStats,
) -> Result<DemuxOutcome, FaucetError> {
    let mut outcome = DemuxOutcome::default();
    let mut states: Vec<FeedState> = Vec::with_capacity(feeds.len());
    for feed in feeds {
        let opened = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            r = feed.ready => r.ok(),
        };
        match opened {
            Some(position) => states.push(FeedState {
                table: feed.table,
                tx: Some(feed.tx),
                position,
                last_sent: None,
                last_sent_at: Instant::now(),
            }),
            None => {
                outcome.dead.insert(feed.table);
            }
        }
    }
    if states.is_empty() || cancel.is_cancelled() {
        return Ok(outcome);
    }

    let positions: Vec<Value> = states.iter().filter_map(|s| s.position.clone()).collect();
    if !positions.is_empty() {
        let start = source.position_min(&positions).ok_or_else(|| {
            FaucetError::Config(format!(
                "mirror: '{}' cannot order the tables' stream positions, so the shared \
                 stream has no safe resume point",
                source.connector_name()
            ))
        })?;
        source.apply_start_bookmark(start.clone()).await?;
        outcome.started_at = Some(start);
    }

    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, faucet_core::DEFAULT_BATCH_SIZE);
    let mut last_bookmark: Option<Value> = None;
    let mut result: Result<(), FaucetError> = Ok(());
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            p = pages.next() => p,
        };
        let page = match next {
            None => break,
            Some(Err(e)) => {
                result = Err(e);
                break;
            }
            Some(Ok(page)) => page,
        };
        let mut buckets: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for record in page.records {
            let Some(name) = source.record_table(&record) else {
                continue;
            };
            match router.route(&name) {
                Route::Active(t) => buckets.entry(t).or_default().push(record),
                Route::New(t) => {
                    outcome.new_tables.insert(t);
                }
                Route::Held | Route::Ignored => {}
            }
        }
        if page.bookmark.is_some() {
            last_bookmark = page.bookmark.clone();
        }
        for st in states.iter_mut() {
            let records = buckets.remove(&st.table).unwrap_or_default();
            let covered = match (&page.bookmark, &st.position) {
                (Some(b), Some(p)) => source.position_le(b, p) == Some(true),
                _ => false,
            };
            if covered {
                continue;
            }
            let due = page.bookmark.is_some() && st.last_sent_at.elapsed() >= IDLE_FLUSH;
            if records.is_empty() && !due {
                continue;
            }
            let n = records.len() as u64;
            send(
                st,
                StreamPage {
                    records,
                    bookmark: page.bookmark.clone(),
                },
                &live,
                n,
                &mut outcome.dead,
            )
            .await;
        }
        if !outcome.new_tables.is_empty() {
            break;
        }
    }
    drop(pages);

    if let Some(last) = last_bookmark {
        for st in states.iter_mut() {
            let covered = st
                .position
                .as_ref()
                .is_some_and(|p| source.position_le(&last, p) == Some(true));
            if st.last_sent.as_ref() != Some(&last) && !covered {
                send(
                    st,
                    StreamPage {
                        records: Vec::new(),
                        bookmark: Some(last.clone()),
                    },
                    &live,
                    0,
                    &mut outcome.dead,
                )
                .await;
            }
        }
    }
    drop(states);
    result.map(|()| outcome)
}

async fn send(
    st: &mut FeedState,
    page: StreamPage,
    live: &LiveStats,
    records: u64,
    dead: &mut BTreeSet<String>,
) {
    let Some(tx) = st.tx.as_ref() else {
        return;
    };
    let bookmark = page.bookmark.clone();
    if tx.send(page).await.is_err() {
        st.tx = None;
        dead.insert(st.table.clone());
        return;
    }
    if bookmark.is_some() {
        st.last_sent = bookmark;
        st.last_sent_at = Instant::now();
    }
    if let Ok(mut g) = live.lock() {
        let entry = g.entry(st.table.clone()).or_default();
        entry.changes += records;
        if st.last_sent.is_some() {
            entry.last_applied_at = Some(Utc::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A scripted shared stream: pages of `{table, v}` records whose bookmark is
    /// a plain integer, ordered numerically.
    struct Script {
        pages: Vec<StreamPage>,
        started: Mutex<Option<Value>>,
        fail_after: Option<usize>,
        polls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Source for Script {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }

        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> Pin<Box<dyn futures::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
        {
            let start = self
                .started
                .lock()
                .unwrap()
                .clone()
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let pages: Vec<Result<StreamPage, FaucetError>> = self
                .pages
                .iter()
                .filter(|p| {
                    p.bookmark
                        .as_ref()
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX)
                        > start
                })
                .cloned()
                .enumerate()
                .map(|(i, p)| {
                    self.polls.fetch_add(1, Ordering::SeqCst);
                    match self.fail_after {
                        Some(n) if i >= n => Err(FaucetError::Source("stream broke".into())),
                        _ => Ok(p),
                    }
                })
                .collect();
            Box::pin(futures::stream::iter(pages))
        }

        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            *self.started.lock().unwrap() = Some(bookmark);
            Ok(())
        }

        fn record_table(&self, record: &Value) -> Option<String> {
            record.get("table")?.as_str().map(str::to_string)
        }

        fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
            Some(a.as_u64()? <= b.as_u64()?)
        }

        fn connector_name(&self) -> &'static str {
            "script"
        }
    }

    fn page(bm: u64, tables: &[&str]) -> StreamPage {
        StreamPage {
            records: tables
                .iter()
                .map(|t| json!({"table": t, "v": bm}))
                .collect(),
            bookmark: Some(json!(bm)),
        }
    }

    fn script(pages: Vec<StreamPage>) -> Arc<Script> {
        Arc::new(Script {
            pages,
            started: Mutex::new(None),
            fail_after: None,
            polls: AtomicUsize::new(0),
        })
    }

    fn router(active: &[&str], follow: bool) -> Router {
        let set: BTreeSet<String> = active.iter().map(|s| s.to_string()).collect();
        Router {
            active: set.clone(),
            known: set,
            qualifier: None,
            follow: follow.then(|| serde_yaml::from_str("{}").unwrap()),
        }
    }

    /// Open a table pipeline stand-in: apply its start position, stream, and
    /// collect every page it receives.
    async fn consume(src: ChannelSource, start: Option<u64>) -> Vec<StreamPage> {
        if let Some(s) = start {
            src.apply_start_bookmark(json!(s)).await.unwrap();
        }
        let ctx = HashMap::new();
        let mut pages = src.stream_pages(&ctx, 0);
        let mut out = Vec::new();
        while let Some(p) = pages.next().await {
            out.push(p.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn routes_by_table_and_resumes_each_from_its_own_position() {
        let src = script(vec![
            page(1, &["a", "b"]),
            page(2, &["a"]),
            page(3, &["b", "x"]),
        ]);
        let (fa, ca) = channel("a", src.clone());
        let (fb, cb) = channel("b", src.clone());
        let live: LiveStats = Default::default();
        let a = tokio::spawn(consume(ca, Some(0)));
        let b = tokio::spawn(consume(cb, Some(1)));
        let out = run_demux(
            src.clone(),
            vec![fa, fb],
            router(&["a", "b"], false),
            CancellationToken::new(),
            live.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            out.started_at,
            Some(json!(0)),
            "resumes from the earliest table"
        );
        let a = a.await.unwrap();
        let b = b.await.unwrap();
        let recs = |ps: &[StreamPage]| {
            ps.iter()
                .flat_map(|p| p.records.iter().map(|r| r["v"].as_u64().unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(recs(&a), vec![1, 2]);
        assert_eq!(recs(&b), vec![3], "page 1 was already applied by b");
        assert_eq!(
            a.last().unwrap().bookmark,
            Some(json!(3)),
            "idle tables get the final position"
        );
        assert_eq!(b.last().unwrap().bookmark, Some(json!(3)));
        assert!(out.new_tables.is_empty() && out.dead.is_empty());
        let live = live.lock().unwrap();
        assert_eq!(live["a"].changes, 2);
        assert_eq!(live["b"].changes, 1);
        assert!(live["a"].last_applied_at.is_some());
    }

    #[tokio::test]
    async fn a_new_matching_table_ends_the_cycle() {
        let src = script(vec![page(1, &["a"]), page(2, &["fresh"]), page(3, &["a"])]);
        let (fa, ca) = channel("a", src.clone());
        let a = tokio::spawn(consume(ca, Some(0)));
        let out = run_demux(
            src.clone(),
            vec![fa],
            router(&["a"], true),
            CancellationToken::new(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(out.new_tables, BTreeSet::from(["fresh".to_string()]));
        let a = a.await.unwrap();
        assert_eq!(
            a.last().unwrap().bookmark,
            Some(json!(2)),
            "stopped after page 2"
        );
    }

    #[tokio::test]
    async fn a_table_that_never_opens_is_dead_and_the_rest_stream() {
        let src = script(vec![page(1, &["a", "b"])]);
        let (fa, ca) = channel("a", src.clone());
        let (fb, cb) = channel("b", src.clone());
        drop(cb);
        let a = tokio::spawn(consume(ca, None));
        let out = run_demux(
            src.clone(),
            vec![fa, fb],
            router(&["a", "b"], false),
            CancellationToken::new(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(out.dead, BTreeSet::from(["b".to_string()]));
        assert_eq!(out.started_at, None, "no table reported a position");
        assert_eq!(a.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_table_that_goes_away_mid_cycle_is_dead() {
        let src = script((1..=8).map(|i| page(i, &["a", "b"])).collect());
        let (fa, ca) = channel("a", src.clone());
        let (fb, cb) = channel("b", src.clone());
        let a = tokio::spawn(consume(ca, Some(0)));
        let b = tokio::spawn(async move {
            cb.apply_start_bookmark(json!(0)).await.unwrap();
            let ctx = HashMap::new();
            let mut pages = cb.stream_pages(&ctx, 0);
            pages.next().await;
        });
        let out = run_demux(
            src.clone(),
            vec![fa, fb],
            router(&["a", "b"], false),
            CancellationToken::new(),
            Default::default(),
        )
        .await
        .unwrap();
        b.await.unwrap();
        assert_eq!(out.dead, BTreeSet::from(["b".to_string()]));
        assert_eq!(a.await.unwrap().len(), 8, "the other table keeps streaming");
    }

    #[tokio::test]
    async fn source_errors_surface_after_flushing_positions() {
        let src = Arc::new(Script {
            pages: vec![page(1, &["a"]), page(2, &["b"]), page(3, &["a"])],
            started: Mutex::new(None),
            fail_after: Some(2),
            polls: AtomicUsize::new(0),
        });
        let (fa, ca) = channel("a", src.clone());
        let a = tokio::spawn(consume(ca, Some(0)));
        let err = run_demux(
            src.clone(),
            vec![fa],
            router(&["a"], false),
            CancellationToken::new(),
            Default::default(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("stream broke"), "{err}");
        let a = a.await.unwrap();
        assert_eq!(
            a.last().unwrap().bookmark,
            Some(json!(2)),
            "the last good position is committed"
        );
        assert!(src.polls.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn cancellation_before_open_streams_nothing() {
        let src = script(vec![page(1, &["a"])]);
        let (fa, _ca) = channel("a", src.clone());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let out = run_demux(
            src.clone(),
            vec![fa],
            router(&["a"], false),
            cancel,
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(out.dead, BTreeSet::from(["a".to_string()]));
    }

    struct Unordered;

    #[async_trait::async_trait]
    impl Source for Unordered {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn positions_the_source_cannot_order_are_refused() {
        let src: Arc<dyn Source> = Arc::new(Unordered);
        let (fa, ca) = channel("a", src.clone());
        let (fb, cb) = channel("b", src.clone());
        tokio::spawn(consume(ca, Some(1)));
        tokio::spawn(consume(cb, Some(2)));
        let err = run_demux(
            src,
            vec![fa, fb],
            router(&["a", "b"], false),
            CancellationToken::new(),
            Default::default(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("cannot order"), "{err}");
    }

    #[tokio::test]
    async fn a_closed_feed_is_an_empty_source() {
        let (feed, source) = channel("t", Arc::new(EmptySource));
        drop(feed);
        assert!(
            source
                .fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(source.connector_name(), "mirror");
        assert!(
            EmptySource
                .fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn channel_source_reports_the_inner_identity() {
        let src = script(vec![]);
        let (_f, c) = channel("a", src.clone());
        assert_eq!(c.connector_name(), "script");
        assert_eq!(
            c.state_key().as_deref(),
            Some("mirror"),
            "resumable even when the inner has no key"
        );
        assert_eq!(c.state_schema(), 0);
        assert_eq!(c.migrate_state(0, json!(1)).unwrap(), json!(1));
        assert!(!c.supports_exactly_once());
        assert_eq!(c.replay_guarantee(), src.replay_guarantee());
        assert_eq!(c.dataset_uri(), src.dataset_uri());
        assert_eq!(c.config_schema(), src.config_schema());
        drop(_f);
        assert!(
            c.fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
    }
}
