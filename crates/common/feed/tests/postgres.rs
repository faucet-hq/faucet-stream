//! The store conformance suite against the PostgreSQL backend, plus the
//! PostgreSQL-only guarantees. Needs Docker.

use async_trait::async_trait;
use faucet_common_feed::conformance::{self, Backend};
use faucet_common_feed::{
    FeedError, FeedStore, FeedUid, NewEvent, PostgresFeedStore, PostgresOptions, Result, Seq,
    StoreLimits,
};
use faucet_core::io::CallContext;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, Row};
use std::sync::Arc;
use std::time::{Duration, Instant};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

struct PostgresBackend {
    _container: ContainerAsync<Postgres>,
    url: String,
}

impl PostgresBackend {
    async fn start() -> Self {
        let container =
            faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine"))
                .await;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("postgres port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        Self {
            _container: container,
            url,
        }
    }

    async fn raw(&self) -> Result<PgConnection> {
        PgConnection::connect(&self.url)
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))
    }

    async fn store(&self, options: &PostgresOptions) -> PostgresFeedStore {
        PostgresFeedStore::connect(&self.url, options)
            .await
            .expect("connect")
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn open(&self) -> Result<Arc<dyn FeedStore>> {
        Ok(Arc::new(
            PostgresFeedStore::connect(&self.url, &PostgresOptions::new()).await?,
        ))
    }

    async fn schema_version(&self) -> Result<i64> {
        let mut conn = self.raw().await?;
        let row = sqlx::query("SELECT v FROM faucet_feed_meta WHERE k = 'schema_version'")
            .fetch_one(&mut conn)
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        row.try_get(0)
            .map_err(|e| FeedError::Backend(e.to_string()))
    }

    async fn set_schema_version(&self, version: i64) -> Result<()> {
        let mut conn = self.raw().await?;
        sqlx::query("UPDATE faucet_feed_meta SET v = $1 WHERE k = 'schema_version'")
            .bind(version)
            .execute(&mut conn)
            .await
            .map_err(|e| FeedError::Backend(e.to_string()))?;
        Ok(())
    }
}

async fn new_feed(store: &PostgresFeedStore) -> FeedUid {
    let feed = FeedUid::generate();
    store
        .ensure_feed(&CallContext::new(), &feed, None, &StoreLimits::new())
        .await
        .expect("ensure_feed");
    feed
}

async fn hold_append_lock(conn: &mut PgConnection, feed: &FeedUid) {
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("feed:{feed}"))
        .execute(&mut *conn)
        .await
        .expect("take the feed's append lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes() {
    let backend = PostgresBackend::start().await;
    for check in conformance::CHECKS {
        eprintln!("postgres conformance: {}", check.name);
        check.run(&backend).await;
    }
    let caps = backend.open().await.expect("open").capabilities();
    assert_eq!(caps.backend, "postgres");
    assert!(caps.cluster_safe);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_stores_on_one_database_share_claims_and_sequences() {
    let backend = PostgresBackend::start().await;
    let (one, two) = (
        backend.store(&PostgresOptions::new()).await,
        backend.store(&PostgresOptions::new()).await,
    );
    let calls = CallContext::new();
    let feed = new_feed(&one).await;
    let mut tasks = Vec::new();
    for (i, store) in [one.clone(), two.clone()].into_iter().enumerate() {
        let feed = feed.clone();
        tasks.push(tokio::spawn(async move {
            let calls = CallContext::new();
            for n in 0..25 {
                store
                    .append(
                        &calls,
                        &feed,
                        vec![NewEvent::new("text/plain", format!("{i}-{n}").into_bytes())],
                    )
                    .await
                    .expect("append");
            }
        }));
    }
    for task in tasks {
        task.await.expect("an appender finished");
    }
    let batch = two
        .read(&calls, &feed, Seq(1), Seq(51))
        .await
        .expect("every sequence number is there once");
    assert_eq!(batch.events.len(), 50);

    let delivery = faucet_common_feed::DeliveryUid::generate(feed.clone());
    one.ensure_delivery(&calls, &delivery, faucet_common_feed::Start::Earliest)
        .await
        .expect("ensure_delivery");
    let owner = faucet_common_feed::ClaimOwner::new("serve:one").expect("owner");
    let ttl = Duration::from_secs(60);
    let (a, b) = tokio::join!(
        one.claim(&calls, &delivery, Seq(1), Seq(51), &owner, ttl),
        two.claim(&calls, &delivery, Seq(1), Seq(51), &owner, ttl),
    );
    let granted = [a.expect("claim"), b.expect("claim")]
        .into_iter()
        .flatten()
        .count();
    assert_eq!(granted, 1, "one server wins the range");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_append_lock_times_out_as_unavailable() {
    let backend = PostgresBackend::start().await;
    let options = PostgresOptions::new()
        .lock_timeout(Duration::from_millis(1500))
        .statement_timeout(Duration::from_secs(7))
        .idle_in_transaction_timeout(Duration::from_secs(9))
        .max_connections(2);
    let store = backend.store(&options).await;
    let feed = new_feed(&store).await;
    let calls = CallContext::new();

    let mut holder = backend.raw().await.expect("a second session");
    hold_append_lock(&mut holder, &feed).await;
    let started = Instant::now();
    let err = store
        .append(&calls, &feed, vec![NewEvent::new("text/plain", "x")])
        .await
        .expect_err("the append gives up at lock_timeout");
    let waited = started.elapsed();
    assert!(matches!(err, FeedError::Unavailable(_)), "{err}");
    assert!(err.is_transient());
    assert!(
        waited >= Duration::from_millis(1400) && waited < Duration::from_secs(6),
        "waited {waited:?}"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("rollback");
    let ack = store
        .append(&calls, &feed, vec![NewEvent::new("text/plain", "x")])
        .await
        .expect("the feed is writable again");
    assert_eq!(ack.first_seq, Some(Seq(1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stored_format_is_frozen() {
    let backend = PostgresBackend::start().await;
    drop(backend.open().await.expect("open"));
    let mut conn = backend.raw().await.expect("a connection");
    let rows = sqlx::query(
        "SELECT c.table_name::text, c.column_name::text, c.data_type::text, \
         c.is_nullable::text, COALESCE(k.ordinal_position, 0)::bigint \
         FROM information_schema.columns c \
         LEFT JOIN information_schema.table_constraints t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
          AND t.constraint_type = 'PRIMARY KEY' \
         LEFT JOIN information_schema.key_column_usage k \
           ON k.constraint_schema = t.constraint_schema \
          AND k.constraint_name = t.constraint_name AND k.column_name = c.column_name \
         WHERE c.table_schema = 'public' AND c.table_name LIKE 'faucet_feed_%' \
         ORDER BY c.table_name, c.ordinal_position",
    )
    .fetch_all(&mut conn)
    .await
    .expect("the schema");
    let mut tables = serde_json::Map::new();
    for row in rows {
        let table: String = row.try_get(0).expect("table");
        let column: String = row.try_get(1).expect("column");
        let kind: String = row.try_get(2).expect("type");
        let nullable: String = row.try_get(3).expect("nullable");
        let key: i64 = row.try_get(4).expect("key position");
        tables
            .entry(table)
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .expect("an array")
            .push(serde_json::json!({
                "column": column, "type": kind, "not_null": nullable == "NO", "key": key,
            }));
    }
    let found = serde_json::json!({
        "schema_version": backend.schema_version().await.expect("schema_version"),
        "tables": tables,
    });
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/postgres-store-v1.json"
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
