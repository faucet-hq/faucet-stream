//! `write_mode: overwrite` into Elasticsearch through the CLI executor, which
//! runs begin, the writes and the commit on different sink instances (#789
//! MSG-04).
#![cfg(all(feature = "source-csv", feature = "sink-elasticsearch"))]

#[path = "support/fake_es_cluster.rs"]
mod fake_es_cluster;

use fake_es_cluster::FakeCluster;
use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use serde_json::json;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer};

fn opts(name: &str) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: name.into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
        usage: Default::default(),
        budget: None,
    }
}

async fn run(csv: &std::path::Path, base_url: &str) -> faucet_cli::executor::RunSummary {
    let yaml = format!(
        r#"
version: 1
name: orders_refresh
pipeline:
  source:
    type: csv
    config: {{ path: "{}" }}
  sink:
    type: elasticsearch
    config:
      base_url: "{base_url}"
      index: orders
      write_mode: overwrite
      auth: {{ type: none }}
"#,
        csv.display()
    );
    let cfg =
        PipelineConfig::from_text(&yaml, std::path::Path::new("faucet.yaml")).expect("config");
    let nodes = expand(&cfg).expect("expand");
    run_expanded(nodes, opts("orders_refresh"))
        .await
        .expect("run")
}

#[tokio::test]
async fn an_overwrite_run_replaces_the_alias_and_leaves_no_staging_behind() {
    let server = MockServer::start().await;
    let fake = FakeCluster::default();
    Mock::given(any())
        .respond_with(fake.clone())
        .mount(&server)
        .await;
    fake.add_index("orders-old", vec![json!({"id": "0"})], Some("orders"));

    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("orders.csv");
    std::fs::write(&csv, "id\n1\n2\n").unwrap();

    let summary = run(&csv, &server.uri()).await;
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(
        fake.alias_docs("orders"),
        vec![json!({"id": "1"}), json!({"id": "2"})]
    );
    assert_eq!(fake.alias_names(), vec!["orders".to_string()]);
    assert_eq!(fake.index_names().len(), 1);

    std::fs::write(&csv, "id\n3\n").unwrap();
    let summary = run(&csv, &server.uri()).await;
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(fake.alias_docs("orders"), vec![json!({"id": "3"})]);
    assert_eq!(fake.index_names().len(), 1);
}
