//! Integration tests for `RedisStateStore` against a real Redis instance via
//! testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::state::StateStore;
use faucet_state_redis::RedisStateStore;
use serde_json::json;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

/// Start a Redis container and return the handle (keeps it alive) plus a URL.
async fn start_redis() -> (ContainerAsync<Redis>, String) {
    let container = Redis::default()
        .start()
        .await
        .expect("redis container start");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("redis port");
    let url = format!("redis://127.0.0.1:{port}");
    // The mapped port can accept before Redis does; wait for a PING.
    let client = redis::Client::open(url.as_str()).expect("client");
    for _ in 0..50 {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await
            && redis::cmd("PING")
                .query_async::<String>(&mut conn)
                .await
                .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn full_lifecycle_get_put_overwrite_delete() {
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "faucet")
        .await
        .expect("connect");

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

    // Overwriting an existing key replaces the value.
    let v2 = json!({"page": 2, "cursor": "def"});
    store.put("bookmark", &v2).await.expect("put v2");
    assert_eq!(
        store.get("bookmark").await.expect("get v2"),
        Some(v2),
        "second put must overwrite the first"
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
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "faucet")
        .await
        .expect("connect");

    let nested = json!({
        "offset": 12345,
        "partitions": [0, 1, 2],
        "meta": {"done": false, "ratio": 0.5, "note": null}
    });
    store.put("kafka", &nested).await.expect("put nested");
    assert_eq!(
        store.get("kafka").await.expect("get nested"),
        Some(nested),
        "nested JSON must survive the serialize/deserialize round-trip"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_probe_passes_against_live_redis() {
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "faucet")
        .await
        .expect("connect");

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
async fn from_connection_and_namespace_isolation() {
    let (_container, url) = start_redis().await;
    // Build a raw multiplexed connection and share it across two stores that
    // use different namespaces.
    let client = redis::Client::open(url).expect("client open");
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("multiplexed connection");
    let team_a = RedisStateStore::from_connection(conn.clone(), "team_a").expect("store a");
    let team_b = RedisStateStore::from_connection(conn, "team_b").expect("store b");

    // The namespace prefixes the physical Redis key.
    assert_eq!(team_a.redis_key("cursor"), "team_a:cursor");
    assert_eq!(team_b.redis_key("cursor"), "team_b:cursor");

    // Writing the same logical key under one namespace does not leak into the
    // other.
    let value = json!({"v": 7});
    team_a.put("cursor", &value).await.expect("put a");
    assert_eq!(
        team_a.get("cursor").await.expect("get a"),
        Some(value),
        "team_a sees its own value"
    );
    assert_eq!(
        team_b.get("cursor").await.expect("get b"),
        None,
        "team_b must not see team_a's namespaced key"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_by_prefix_and_atomic_batch() {
    let (_container, url) = start_redis().await;
    // The published port can lag the container's readiness on some hosts.
    let mut store = None;
    for _ in 0..50 {
        if let Ok(s) = RedisStateStore::connect(&url, "ns1").await {
            store = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let store = store.expect("connect");
    let other = RedisStateStore::connect(&url, "ns2")
        .await
        .expect("connect");
    assert!(store.supports_list() && store.supports_atomic_batch());
    for k in ["o::a", "o::b::__sla__", "p::a"] {
        store.put(k, &json!(k)).await.expect("put");
    }
    other.put("o::z", &json!(1)).await.expect("put");
    assert_eq!(
        store.list("o::").await.expect("list"),
        vec!["o::a", "o::b::__sla__"]
    );

    store.put_batch(&[]).await.expect("empty batch");
    store
        .put_batch(&[
            ("o::a".to_string(), json!({"v": 2})),
            ("o::c".to_string(), json!(3)),
        ])
        .await
        .expect("batch");
    assert_eq!(store.get("o::a").await.unwrap(), Some(json!({"v": 2})));
    assert_eq!(store.get("o::c").await.unwrap(), Some(json!(3)));
    assert!(
        store
            .put_batch(&[("../bad".to_string(), json!(1))])
            .await
            .is_err()
    );
}

/// #789 SQL-89 / SQL-120: a dropped connection is re-established instead of
/// failing every later call, and `list` walks every SCAN page.
#[tokio::test(flavor = "multi_thread")]
async fn reconnects_after_a_dropped_connection_and_lists_every_page() {
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "faucet")
        .await
        .expect("connect");
    store.put("before", &json!(1)).await.expect("put");

    let client = redis::Client::open(url.as_str()).expect("client");
    let mut admin = client
        .get_multiplexed_async_connection()
        .await
        .expect("admin");
    let killed: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .arg("SKIPME")
        .arg("yes")
        .query_async(&mut admin)
        .await
        .expect("client kill");
    assert!(killed >= 1, "the store's connection must have been killed");

    assert_eq!(
        store.get("before").await.expect("get after kill"),
        Some(json!(1))
    );
    store.put("after", &json!(2)).await.expect("put after kill");

    let entries: Vec<(String, serde_json::Value)> = (0..1_200)
        .map(|i| (format!("orders::{i:04}"), json!(i)))
        .collect();
    store.put_batch(&entries).await.expect("batch");
    let listed = store.list("orders::").await.expect("list");
    assert_eq!(listed.len(), 1_200);
    assert_eq!(listed[0], "orders::0000");
}

#[tokio::test(flavor = "multi_thread")]
async fn compare_and_put_is_atomic_across_concurrent_callers() {
    let (_container, url) = start_redis().await;
    let store = std::sync::Arc::new(
        RedisStateStore::connect(&url, "faucet")
            .await
            .expect("connect"),
    );
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
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "faucet")
        .await
        .expect("connect");
    let a = json!({"a": 1, "b": [1, 2]});
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

    assert!(store.compare_and_put("k", Some(&a), &b).await.unwrap());
    assert_eq!(store.get("k").await.unwrap(), Some(b));
    assert!(store.compare_and_put("bad key!", None, &a).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn server_errors_and_corrupt_values_surface_as_state_errors() {
    let (_container, url) = start_redis().await;
    let store = RedisStateStore::connect(&url, "ns").await.expect("connect");
    let client = redis::Client::open(url.as_str()).expect("client");
    let mut raw = client
        .get_multiplexed_async_connection()
        .await
        .expect("conn");
    let _: i64 = redis::cmd("LPUSH")
        .arg("ns:listkey")
        .arg("x")
        .query_async(&mut raw)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("ns:corrupt")
        .arg("not json")
        .query_async(&mut raw)
        .await
        .unwrap();

    let err = store.get("listkey").await.unwrap_err().to_string();
    assert!(err.contains("Redis GET for key 'listkey' failed"), "{err}");
    let err = store
        .compare_and_put("listkey", None, &json!(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Redis GET for key 'listkey' failed"), "{err}");
    let err = store
        .compare_and_put("corrupt", None, &json!(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("is not valid JSON"), "{err}");

    // A server that refuses writes (out of memory) fails every write path.
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("noeviction")
        .query_async(&mut raw)
        .await
        .unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("1")
        .query_async(&mut raw)
        .await
        .unwrap();
    let err = store.put("k", &json!(1)).await.unwrap_err().to_string();
    assert!(err.contains("Redis SET for key 'k' failed"), "{err}");
    let err = store
        .put_batch(&[("k".to_string(), json!(1))])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Redis MSET failed"), "{err}");
    let err = store
        .compare_and_put("fresh", None, &json!(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Redis compare-and-set for key 'fresh' failed"),
        "{err}"
    );
    assert_eq!(store.get("fresh").await.unwrap(), None);
}
