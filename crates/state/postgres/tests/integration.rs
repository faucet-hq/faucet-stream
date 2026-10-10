//! Integration tests for `PostgresStateStore` against a real Postgres instance
//! via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::state::StateStore;
use faucet_state_postgres::PostgresStateStore;
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return the handle (keeps it alive) plus a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn full_lifecycle_get_put_overwrite_delete() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");

    // ensure_table must be idempotent — calling it twice is a no-op.
    store.ensure_table().await.expect("ensure_table #1");
    store.ensure_table().await.expect("ensure_table #2");

    // A key that was never written reads back as None.
    assert_eq!(store.get("missing").await.expect("get missing"), None);

    // Write then read back the exact value.
    let v1 = json!({"page": 1, "cursor": "abc"});
    store.put("bookmark", &v1).await.expect("put v1");
    assert_eq!(
        store.get("bookmark").await.expect("get v1"),
        Some(v1),
        "get must return the value that was put"
    );

    // Overwriting an existing key (UPSERT) replaces the value.
    let v2 = json!({"page": 2, "cursor": "def"});
    store.put("bookmark", &v2).await.expect("put v2");
    assert_eq!(
        store.get("bookmark").await.expect("get v2"),
        Some(v2),
        "second put must overwrite the first via ON CONFLICT DO UPDATE"
    );

    // Delete removes the key; subsequent get is None.
    store.delete("bookmark").await.expect("delete");
    assert_eq!(
        store.get("bookmark").await.expect("get after delete"),
        None,
        "get after delete must return None"
    );

    // Deleting an absent key is a no-op (no error).
    store.delete("bookmark").await.expect("delete idempotent");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_json_value_roundtrips_exactly() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");

    let nested = json!({
        "lsn": "0/1A2B3C",
        "tables": ["public.users", "public.orders"],
        "meta": {"committed": true, "rows": 42, "ratio": 0.75, "note": null}
    });
    store.put("cdc", &nested).await.expect("put nested");
    assert_eq!(
        store.get("cdc").await.expect("get nested"),
        Some(nested),
        "nested JSON must survive a JSONB round-trip byte-for-byte"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_probe_passes_against_live_db() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");

    let report = store
        .check(&CheckContext::default())
        .await
        .expect("check returns Ok");
    assert_eq!(report.failed_count(), 0, "sentinel probe should pass");
    assert_eq!(report.probes.len(), 1);
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected a passing probe, got {:?}",
        report.probes[0].status
    );

    // The sentinel round-trip must leave no residue behind.
    assert_eq!(
        store
            .get(faucet_core::state::DOCTOR_SENTINEL_KEY)
            .await
            .expect("get sentinel"),
        None,
        "check() must clean up its sentinel key"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn from_pool_with_custom_table_name() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let store =
        PostgresStateStore::from_pool(pool, "custom_state").expect("from_pool with valid name");
    assert_eq!(store.table(), "custom_state");

    store.ensure_table().await.expect("ensure custom table");
    let v = json!({"ok": true});
    store.put("k", &v).await.expect("put");
    assert_eq!(store.get("k").await.expect("get"), Some(v));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_by_prefix_and_atomic_batch() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");
    assert!(store.supports_list() && store.supports_atomic_batch());

    // `_` is a LIKE wildcard; the listing must treat the prefix literally.
    for k in ["o_x::a", "oax::a", "o_x::b::__sla__", "other::a"] {
        store.put(k, &json!(k)).await.expect("put");
    }
    assert_eq!(
        store.list("o_x::").await.expect("list"),
        vec!["o_x::a", "o_x::b::__sla__"]
    );

    store
        .put_batch(&[
            ("o_x::a".to_string(), json!({"v": 2})),
            ("o_x::c".to_string(), json!(3)),
        ])
        .await
        .expect("batch");
    assert_eq!(store.get("o_x::a").await.unwrap(), Some(json!({"v": 2})));
    assert_eq!(store.get("o_x::c").await.unwrap(), Some(json!(3)));

    // An invalid key aborts the batch before anything is written.
    assert!(
        store
            .put_batch(&[
                ("o_x::d".to_string(), json!(1)),
                ("../bad".to_string(), json!(1)),
            ])
            .await
            .is_err()
    );
    assert_eq!(store.get("o_x::d").await.unwrap(), None);
}

/// #789 SQL-165: a value holding U+0000 (an opaque cursor) persists and reads
/// back unchanged, alone and in a batch.
#[tokio::test(flavor = "multi_thread")]
async fn values_containing_nul_round_trip() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");
    let v = json!({"cursor": "abc\u{0}def"});
    store.put("nul", &v).await.expect("put with NUL");
    assert_eq!(store.get("nul").await.expect("get"), Some(v.clone()));
    store
        .put_batch(&[("nul2".into(), v.clone())])
        .await
        .expect("batch with NUL");
    assert_eq!(store.get("nul2").await.expect("get"), Some(v));
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_and_put_is_atomic_across_concurrent_callers() {
    let (_container, url) = start_postgres().await;
    let store = std::sync::Arc::new(
        PostgresStateStore::connect_with(&url, 16, "faucet_state")
            .await
            .expect("connect"),
    );
    store.ensure_table().await.expect("ensure_table");
    assert!(store.supports_compare_and_put());

    let mut tasks = Vec::new();
    for i in 0..32 {
        let s = store.clone();
        tasks.push(tokio::spawn(async move {
            s.compare_and_put("lease", None, &json!({ "by": i }))
                .await
                .expect("cas")
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.expect("join") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one absent-expected take may win");
    let held = store.get("lease").await.expect("get").expect("lease held");

    let mut tasks = Vec::new();
    for i in 0..32 {
        let (s, held) = (store.clone(), held.clone());
        tasks.push(tokio::spawn(async move {
            s.compare_and_put("lease", Some(&held), &json!({ "next": i }))
                .await
                .expect("cas")
        }));
    }
    let mut winners = 0;
    for t in tasks {
        if t.await.expect("join") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one swap from the held value may win");
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_and_put_mismatch_leaves_the_value() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");
    let a = json!({"a": 1, "b": [1, 2], "c": {"x": null}});
    let b = json!({"b": 2});

    assert!(store.compare_and_put("k", None, &a).await.unwrap());
    assert!(!store.compare_and_put("k", None, &b).await.unwrap());
    assert!(!store.compare_and_put("k", Some(&b), &b).await.unwrap());
    assert!(
        !store.compare_and_put("absent", Some(&a), &b).await.unwrap(),
        "an expected value never matches an absent key"
    );
    assert_eq!(store.get("absent").await.unwrap(), None);
    assert_eq!(store.get("k").await.unwrap(), Some(a.clone()));

    // The value read back (JSONB re-orders keys) compares equal.
    let read_back = store.get("k").await.unwrap().unwrap();
    assert!(
        store
            .compare_and_put("k", Some(&read_back), &b)
            .await
            .unwrap()
    );
    assert_eq!(store.get("k").await.unwrap(), Some(b));
    assert!(store.compare_and_put("bad key!", None, &a).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_and_put_handles_values_containing_nul() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");
    let a = json!({"cursor": "abc\u{0}def"});
    let b = json!({"cursor": "next\u{0}"});
    assert!(store.compare_and_put("k", None, &a).await.unwrap());
    let held = store.get("k").await.unwrap().unwrap();
    assert_eq!(held, a);
    assert!(!store.compare_and_put("k", Some(&b), &b).await.unwrap());
    assert!(store.compare_and_put("k", Some(&held), &b).await.unwrap());
    assert_eq!(store.get("k").await.unwrap(), Some(b));
}
