//! The store conformance suite against the SQLite backend.

use async_trait::async_trait;
use faucet_common_feed::conformance::{self, Backend, CHECKS};
use faucet_common_feed::{FeedError, FeedStore, Result, SqliteFeedStore, SqliteOptions};
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};
use std::path::PathBuf;
use std::sync::Arc;

struct SqliteBackend {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl SqliteBackend {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("feeds.db");
        Self { _dir: dir, path }
    }

    async fn raw(&self) -> Result<SqliteConnection> {
        SqliteConnectOptions::new()
            .filename(&self.path)
            .connect()
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    async fn open(&self) -> Result<Arc<dyn FeedStore>> {
        Ok(Arc::new(
            SqliteFeedStore::open(&self.path, &SqliteOptions::new()).await?,
        ))
    }

    async fn schema_version(&self) -> Result<i64> {
        let mut conn = self.raw().await?;
        let row = sqlx::query("SELECT v FROM faucet_feed_meta WHERE k = 'schema_version'")
            .fetch_one(&mut conn)
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        let version = row
            .try_get(0)
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        conn.close()
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        Ok(version)
    }

    async fn set_schema_version(&self, version: i64) -> Result<()> {
        let mut conn = self.raw().await?;
        sqlx::query("UPDATE faucet_feed_meta SET v = ? WHERE k = 'schema_version'")
            .bind(version)
            .execute(&mut conn)
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        conn.close()
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))
    }
}

const COVERED: &[&str] = &[
    "append_is_durable_ordered_and_gap_free",
    "concurrent_appends_never_expose_a_gap",
    "dedupe_within_the_window",
    "dedupe_replaces_an_expired_row",
    "dedupe_under_concurrent_retries",
    "limits_refuse_and_store_nothing",
    "one_claimer_wins",
    "an_expired_claim_is_gone",
    "attach_fences_the_earlier_holder",
    "commit_is_fenced_and_bounded",
    "cursor_moves_are_guarded_and_recorded",
    "trim_respects_age_cursors_and_claims",
    "bound_seq_at_and_heads",
    "delivery_start_delete_and_purge",
    "state_survives_a_restart",
    "a_newer_schema_is_refused",
    "one_maintenance_holder",
];

macro_rules! conformance {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            assert!(COVERED.contains(&stringify!($name)));
            conformance::run(&SqliteBackend::new(), stringify!($name)).await;
        }
    )*};
}

conformance!(
    append_is_durable_ordered_and_gap_free,
    concurrent_appends_never_expose_a_gap,
    dedupe_within_the_window,
    dedupe_replaces_an_expired_row,
    dedupe_under_concurrent_retries,
    limits_refuse_and_store_nothing,
    one_claimer_wins,
    an_expired_claim_is_gone,
    attach_fences_the_earlier_holder,
    commit_is_fenced_and_bounded,
    cursor_moves_are_guarded_and_recorded,
    trim_respects_age_cursors_and_claims,
    bound_seq_at_and_heads,
    delivery_start_delete_and_purge,
    state_survives_a_restart,
    a_newer_schema_is_refused,
    one_maintenance_holder,
);

#[test]
fn every_check_of_the_suite_runs_here() {
    let suite: Vec<&str> = CHECKS.iter().map(|c| c.name).collect();
    assert_eq!(suite, COVERED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sqlite_store_is_not_cluster_safe() {
    let backend = SqliteBackend::new();
    let store = backend.open().await.expect("open");
    let caps = store.capabilities();
    assert_eq!(caps.backend, "sqlite");
    assert!(!caps.cluster_safe);
    let none = faucet_common_feed::FeedUid::generate();
    assert_eq!(
        store
            .bytes_from(
                &faucet_core::io::CallContext::new(),
                &none,
                faucet_common_feed::Seq(1)
            )
            .await
            .expect("bytes_from"),
        Some(0),
        "the backend sums retained bytes itself"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_stores_have_different_identities() {
    let (one, two) = (SqliteBackend::new(), SqliteBackend::new());
    let (a, b) = (
        one.open().await.expect("open"),
        two.open().await.expect("open"),
    );
    let (a, b) = (a.store_id().expect("an id"), b.store_id().expect("an id"));
    assert_ne!(a, b);
    assert_eq!(one.open().await.expect("reopen").store_id(), Some(a));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_store_answers_unavailable() {
    use faucet_common_feed::{FeedUid, StoreLimits};
    use faucet_core::io::CallContext;
    let backend = SqliteBackend::new();
    let store = SqliteFeedStore::open(&backend.path, &SqliteOptions::new())
        .await
        .expect("open");
    store.close().await;
    let err = store
        .ensure_feed(
            &CallContext::new(),
            &FeedUid::generate(),
            None,
            &StoreLimits::new(),
        )
        .await
        .expect_err("a closed store refuses");
    assert!(matches!(err, FeedError::Unavailable(_)), "{err}");
    assert!(err.is_transient());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_call_is_a_typed_call_error() {
    use faucet_common_feed::FeedUid;
    use faucet_core::CancellationToken;
    use faucet_core::io::CallContext;
    let backend = SqliteBackend::new();
    let store = backend.open().await.expect("open");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let calls = CallContext::new().with_cancel(cancel);
    let err = store
        .heads(&calls, &[FeedUid::generate()])
        .await
        .expect_err("a cancelled call fails");
    assert!(matches!(err, FeedError::Call(_)), "{err}");
    let core: faucet_core::FaucetError = err.into();
    assert!(matches!(core, faucet_core::FaucetError::Cancelled { .. }));
    let typed: faucet_core::FaucetError = FeedError::FeedNotFound(FeedUid::generate()).into();
    match typed {
        faucet_core::FaucetError::Custom(inner) => {
            assert!(inner.downcast_ref::<FeedError>().is_some());
        }
        other => panic!("expected a custom error, got {other}"),
    }
}

mod broken {
    //! A store whose watermark runs one ahead of its events, to prove the
    //! reordering check can fail.

    use super::*;
    use faucet_common_feed::{
        AppendAck, Batch, BatchLimits, Claim, ClaimId, ClaimOwner, DeliveryState, DeliveryUid,
        Epoch, FeedUid, Gap, Head, Holder, Move, NewEvent, PurgeStats, Seq, Start, StoreCaps,
        StoreLimits, TenantId, Timestamp, TrimStats,
    };
    use faucet_core::io::CallContext;
    use std::time::Duration;

    struct WatermarkAhead(Arc<dyn FeedStore>);

    #[async_trait]
    impl FeedStore for WatermarkAhead {
        async fn ensure_feed(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            tenant: Option<&TenantId>,
            limits: &StoreLimits,
        ) -> Result<()> {
            self.0.ensure_feed(calls, feed, tenant, limits).await
        }
        async fn ensure_delivery(
            &self,
            calls: &CallContext,
            delivery: &DeliveryUid,
            start: Start,
        ) -> Result<DeliveryState> {
            self.0.ensure_delivery(calls, delivery, start).await
        }
        async fn delete_delivery(&self, calls: &CallContext, delivery: &DeliveryUid) -> Result<()> {
            self.0.delete_delivery(calls, delivery).await
        }
        async fn purge_feed(&self, calls: &CallContext, feed: &FeedUid) -> Result<PurgeStats> {
            self.0.purge_feed(calls, feed).await
        }
        async fn append(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            events: Vec<NewEvent>,
        ) -> Result<AppendAck> {
            self.0.append(calls, feed, events).await
        }
        async fn heads(&self, calls: &CallContext, feeds: &[FeedUid]) -> Result<Vec<Head>> {
            let mut heads = self.0.heads(calls, feeds).await?;
            for head in &mut heads {
                if head.watermark > Seq(0) {
                    head.watermark = head.watermark.next();
                }
            }
            Ok(heads)
        }
        async fn read(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            from: Seq,
            to: Seq,
        ) -> Result<Batch> {
            self.0.read(calls, feed, from, to).await
        }
        async fn bound(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            from: Seq,
            max: BatchLimits,
        ) -> Result<Seq> {
            self.0.bound(calls, feed, from, max).await
        }
        async fn seq_at(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            time: Timestamp,
        ) -> Result<Seq> {
            self.0.seq_at(calls, feed, time).await
        }
        async fn claim(
            &self,
            calls: &CallContext,
            delivery: &DeliveryUid,
            expected_cursor: Seq,
            to: Seq,
            owner: &ClaimOwner,
            ttl: Duration,
        ) -> Result<Option<Claim>> {
            self.0
                .claim(calls, delivery, expected_cursor, to, owner, ttl)
                .await
        }
        async fn attach(&self, calls: &CallContext, claim: &ClaimId) -> Result<Holder> {
            self.0.attach(calls, claim).await
        }
        async fn renew(&self, calls: &CallContext, holder: &Holder, ttl: Duration) -> Result<bool> {
            self.0.renew(calls, holder, ttl).await
        }
        async fn renew_claim(
            &self,
            calls: &CallContext,
            claim: &ClaimId,
            ttl: Duration,
        ) -> Result<bool> {
            self.0.renew_claim(calls, claim, ttl).await
        }
        async fn commit(
            &self,
            calls: &CallContext,
            holder: &Holder,
            expected: Seq,
            new: Seq,
        ) -> Result<()> {
            self.0.commit(calls, holder, expected, new).await
        }
        async fn release(&self, calls: &CallContext, claim: &ClaimId) -> Result<()> {
            self.0.release(calls, claim).await
        }
        async fn move_cursor(
            &self,
            calls: &CallContext,
            delivery: &DeliveryUid,
            expected: Seq,
            new: Seq,
            why: Move,
        ) -> Result<Epoch> {
            self.0
                .move_cursor(calls, delivery, expected, new, why)
                .await
        }
        async fn gaps(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            delivery: Option<&DeliveryUid>,
        ) -> Result<Vec<Gap>> {
            self.0.gaps(calls, feed, delivery).await
        }
        async fn trim(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            max_rows: u32,
        ) -> Result<TrimStats> {
            self.0.trim(calls, feed, max_rows).await
        }
        async fn purge_dedupe(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            max_rows: u32,
        ) -> Result<u64> {
            self.0.purge_dedupe(calls, feed, max_rows).await
        }
        async fn maintenance(
            &self,
            calls: &CallContext,
            feed: &FeedUid,
            owner: &ClaimOwner,
            ttl: Duration,
        ) -> Result<bool> {
            self.0.maintenance(calls, feed, owner, ttl).await
        }
        fn capabilities(&self) -> StoreCaps {
            self.0.capabilities()
        }
    }

    struct BrokenBackend(SqliteBackend);

    #[async_trait]
    impl Backend for BrokenBackend {
        async fn open(&self) -> Result<Arc<dyn FeedStore>> {
            Ok(Arc::new(WatermarkAhead(self.0.open().await?)))
        }
        async fn schema_version(&self) -> Result<i64> {
            self.0.schema_version().await
        }
        async fn set_schema_version(&self, version: i64) -> Result<()> {
            self.0.set_schema_version(version).await
        }
    }

    /// A store that leaves the optional reads to the trait answers "cannot
    /// say" for each, which is what callers fall back on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_store_without_the_optional_reads_answers_none() {
        let store = BrokenBackend(SqliteBackend::new())
            .open()
            .await
            .expect("open");
        let calls = CallContext::default();
        let feed = FeedUid::generate();
        assert_eq!(store.bytes_from(&calls, &feed, Seq(1)).await.unwrap(), None);
        assert_eq!(store.hourly_counts(&calls, &feed, 24).await.unwrap(), None);
        assert_eq!(
            store
                .newest_received(&calls, std::slice::from_ref(&feed))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[should_panic(expected = "the reader finished")]
    async fn a_watermark_ahead_of_its_events_fails_the_reordering_check() {
        conformance::run(
            &BrokenBackend(SqliteBackend::new()),
            "concurrent_appends_never_expose_a_gap",
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stored_format_is_frozen() {
    let backend = SqliteBackend::new();
    drop(backend.open().await.expect("open"));
    let mut conn = backend.raw().await.expect("a connection");
    let rows = sqlx::query(
        "SELECT m.name, p.name, p.type, p.\"notnull\", p.pk \
         FROM sqlite_master m JOIN pragma_table_info(m.name) p \
         WHERE m.type = 'table' AND m.name LIKE 'faucet_feed_%' ORDER BY m.name, p.cid",
    )
    .fetch_all(&mut conn)
    .await
    .expect("the schema");
    let mut tables = serde_json::Map::new();
    for row in rows {
        let table: String = row.try_get(0).expect("table");
        let column: String = row.try_get(1).expect("column");
        let kind: String = row.try_get(2).expect("type");
        let not_null: i64 = row.try_get(3).expect("notnull");
        let key: i64 = row.try_get(4).expect("pk");
        tables
            .entry(table)
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .expect("an array")
            .push(serde_json::json!({
                "column": column, "type": kind, "not_null": not_null == 1, "key": key,
            }));
    }
    let found = serde_json::json!({
        "schema_version": backend.schema_version().await.expect("schema_version"),
        "tables": tables,
    });
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sqlite-store-v1.json"
    );
    if std::env::var_os("FAUCET_FEED_WRITE_FIXTURES").is_some() {
        let text = serde_json::to_string_pretty(&found).expect("json") + "\n";
        std::fs::write(path, text).expect("write the fixture");
    }
    let frozen: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the frozen fixture"))
            .expect("fixture json");
    assert_eq!(
        found, frozen,
        "the store's tables changed: bump SCHEMA_VERSION, migrate, and add a new fixture"
    );
    assert_eq!(
        found["schema_version"],
        serde_json::json!(faucet_common_feed::SCHEMA_VERSION)
    );
}
