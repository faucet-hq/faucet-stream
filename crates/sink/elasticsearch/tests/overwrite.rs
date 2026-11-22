//! Integration tests for the Elasticsearch sink's `write_mode: overwrite` (#494)
//! — the alias-swap lifecycle against a stateful stand-in cluster. Begin, the
//! writes and the commit each run on their own sink instance, the way the CLI
//! executor drives them, so every step must find its state in the cluster.

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
#[path = "support/fake_cluster.rs"]
mod fake_cluster;

use fake_cluster::FakeCluster;
use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::json;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer};

const INDEX: &str = "orders";

fn overwrite_sink(base_url: &str) -> ElasticsearchSink {
    let mut cfg = ElasticsearchSinkConfig::new(base_url, INDEX);
    cfg.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        ..Default::default()
    };
    ElasticsearchSink::new(cfg).unwrap()
}

async fn cluster() -> (MockServer, FakeCluster) {
    let server = MockServer::start().await;
    let fake = FakeCluster::default();
    Mock::given(any())
        .respond_with(fake.clone())
        .mount(&server)
        .await;
    (server, fake)
}

#[tokio::test]
async fn overwrite_stages_then_swaps_alias_across_sink_instances() {
    let (server, fake) = cluster().await;
    fake.add_index("orders-old", vec![json!({"id": 0})], Some(INDEX));

    overwrite_sink(&server.uri())
        .begin_overwrite()
        .await
        .expect("begin");
    let n = overwrite_sink(&server.uri())
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    assert_eq!(
        fake.alias_docs(INDEX),
        vec![json!({"id": 0})],
        "the live alias is untouched until commit"
    );
    overwrite_sink(&server.uri())
        .commit_overwrite()
        .await
        .expect("commit");

    assert_eq!(
        fake.alias_docs(INDEX),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
    let indices = fake.index_names();
    assert_eq!(indices.len(), 2, "{indices:?}");
    assert!(
        indices.contains(&"orders-old".to_string()),
        "an index no overwrite created is left in place: {indices:?}"
    );
    assert!(indices.iter().any(|i| i.starts_with("orders-faucet-ovw-")));
    assert_eq!(
        fake.alias_names(),
        vec![INDEX.to_string()],
        "marker detached"
    );
}

async fn overwrite_once(base: &str, docs: &[serde_json::Value]) {
    overwrite_sink(base).begin_overwrite().await.expect("begin");
    overwrite_sink(base).write_batch(docs).await.expect("write");
    overwrite_sink(base)
        .commit_overwrite()
        .await
        .expect("commit");
}

#[tokio::test]
async fn commit_drops_an_earlier_overwrite_index_nothing_else_reads() {
    let (server, fake) = cluster().await;
    fake.add_index(
        "orders-faucet-ovw-1a2b",
        vec![json!({"id": 0})],
        Some(INDEX),
    );
    overwrite_once(&server.uri(), &[json!({"id": 1})]).await;
    let indices = fake.index_names();
    assert_eq!(indices.len(), 1, "{indices:?}");
    assert_ne!(indices[0], "orders-faucet-ovw-1a2b");
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 1})]);
}

#[tokio::test]
async fn commit_keeps_an_earlier_overwrite_index_another_alias_reads() {
    let (server, fake) = cluster().await;
    fake.add_index(
        "orders-faucet-ovw-1a2b",
        vec![json!({"id": 0})],
        Some(INDEX),
    );
    fake.0
        .lock()
        .unwrap()
        .aliases
        .entry("all-data".into())
        .or_default()
        .insert("orders-faucet-ovw-1a2b".into());
    overwrite_once(&server.uri(), &[json!({"id": 1})]).await;
    assert!(
        fake.index_names()
            .contains(&"orders-faucet-ovw-1a2b".to_string())
    );
    assert_eq!(fake.alias_docs("all-data"), vec![json!({"id": 0})]);
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 1})]);
}

#[tokio::test]
async fn commit_keeps_the_old_index_when_its_aliases_cannot_be_read() {
    let (server, fake) = cluster().await;
    fake.add_index(
        "orders-faucet-ovw-1a2b",
        vec![json!({"id": 0})],
        Some(INDEX),
    );
    fake.0.lock().unwrap().fail_index_alias_reads = true;
    overwrite_once(&server.uri(), &[json!({"id": 1})]).await;
    assert!(
        fake.index_names()
            .contains(&"orders-faucet-ovw-1a2b".to_string())
    );
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 1})]);
}

#[tokio::test]
async fn overwrite_refuses_a_concrete_index() {
    let (server, fake) = cluster().await;
    fake.add_index(INDEX, Vec::new(), None);

    let err = overwrite_sink(&server.uri())
        .begin_overwrite()
        .await
        .expect_err("concrete index must be refused");
    assert!(err.to_string().contains("concrete index"), "{err}");
}

#[tokio::test]
async fn overwrite_first_run_creates_alias_without_a_concrete_index() {
    let (server, fake) = cluster().await;

    overwrite_sink(&server.uri())
        .begin_overwrite()
        .await
        .expect("begin");
    overwrite_sink(&server.uri())
        .write_batch(&[json!({"id": 1})])
        .await
        .expect("write");
    overwrite_sink(&server.uri())
        .commit_overwrite()
        .await
        .expect("commit");

    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 1})]);
    assert!(
        !fake.index_names().contains(&INDEX.to_string()),
        "no concrete index may take the alias's name"
    );
    overwrite_sink(&server.uri())
        .begin_overwrite()
        .await
        .expect("a second overwrite still works");
}

#[tokio::test]
async fn overwrite_abort_on_another_instance_drops_staging_without_swap() {
    let (server, fake) = cluster().await;
    fake.add_index("orders-old", vec![json!({"id": 0})], Some(INDEX));

    overwrite_sink(&server.uri())
        .begin_overwrite()
        .await
        .expect("begin");
    overwrite_sink(&server.uri())
        .write_batch(&[json!({"id": 1})])
        .await
        .expect("write");
    overwrite_sink(&server.uri())
        .abort_overwrite()
        .await
        .expect("abort");

    assert_eq!(fake.index_names(), vec!["orders-old".to_string()]);
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 0})]);
    assert_eq!(fake.alias_names(), vec![INDEX.to_string()]);
}

#[tokio::test]
async fn begin_drops_a_staging_index_a_crashed_run_left_behind() {
    let (server, fake) = cluster().await;
    fake.add_index("orders-old", vec![json!({"id": 0})], Some(INDEX));
    fake.add_index(
        "orders-faucet-ovw-dead",
        vec![json!({"id": 99})],
        Some("orders-faucet-ovw-staging"),
    );

    let sink = overwrite_sink(&server.uri());
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    sink.commit_overwrite().await.expect("commit");

    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 1})]);
    assert!(
        !fake
            .index_names()
            .contains(&"orders-faucet-ovw-dead".to_string())
    );
}

#[tokio::test]
async fn writes_and_commit_without_begin_are_refused() {
    let (server, fake) = cluster().await;
    fake.add_index("orders-old", vec![json!({"id": 0})], Some(INDEX));

    overwrite_sink(&server.uri())
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("no staging alias, no write");
    let err = overwrite_sink(&server.uri())
        .commit_overwrite()
        .await
        .expect_err("nothing to commit");
    assert!(err.to_string().contains("no staging index"), "{err}");
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 0})]);
    assert_eq!(fake.index_names(), vec!["orders-old".to_string()]);
}

#[tokio::test]
async fn commit_refuses_a_marker_on_several_indices() {
    let (server, fake) = cluster().await;
    fake.add_index(
        "orders-faucet-ovw-a",
        Vec::new(),
        Some("orders-faucet-ovw-staging"),
    );
    fake.add_index(
        "orders-faucet-ovw-b",
        Vec::new(),
        Some("orders-faucet-ovw-staging"),
    );

    let err = overwrite_sink(&server.uri())
        .commit_overwrite()
        .await
        .expect_err("ambiguous staging");
    assert!(err.to_string().contains("points at 2 indices"), "{err}");
}

/// Two overwrite runs of one alias at once: each run's begin, writer and
/// commit instances share its run token and its own marker alias, so neither
/// begin drops the other's staging index and neither commit swaps in the
/// other's documents. A tokened begin drops the untokened marker's staging
/// and staging a run abandoned more than a day ago.
#[tokio::test]
async fn concurrent_overwrite_runs_keep_their_own_documents() {
    use faucet_core::idempotency::new_overwrite_run_token;
    let (server, fake) = cluster().await;
    fake.add_index("orders-old", vec![json!({"id": 0})], Some(INDEX));
    fake.add_index(
        "orders-faucet-ovw-legacy",
        vec![json!({"id": 98})],
        Some("orders-faucet-ovw-staging"),
    );
    fake.add_index(
        "orders-faucet-ovw-dead",
        vec![json!({"id": 99})],
        Some("orders-faucet-ovw-staging-10000000aaaaaa"),
    );
    let sink = |token: &str| {
        overwrite_sink(&server.uri())
            .with_overwrite_run(token)
            .unwrap()
    };
    let (a, b) = (new_overwrite_run_token(), new_overwrite_run_token());
    sink(&a).begin_overwrite().await.expect("begin a");
    sink(&b).begin_overwrite().await.expect("begin b");
    let names = fake.index_names();
    assert!(
        !names.contains(&"orders-faucet-ovw-legacy".to_string()),
        "{names:?}"
    );
    assert!(
        !names.contains(&"orders-faucet-ovw-dead".to_string()),
        "{names:?}"
    );
    sink(&a)
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write a");
    sink(&b)
        .write_batch(&[json!({"id": 3})])
        .await
        .expect("write b");
    sink(&a).commit_overwrite().await.expect("commit a");
    assert_eq!(
        fake.alias_docs(INDEX),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
    sink(&b).commit_overwrite().await.expect("commit b");
    assert_eq!(fake.alias_docs(INDEX), vec![json!({"id": 3})]);
    assert_eq!(fake.alias_names(), vec![INDEX.to_string()]);
    let names = fake.index_names();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(
        names.contains(&"orders-old".to_string()),
        "an index no overwrite created stays: {names:?}"
    );
}
