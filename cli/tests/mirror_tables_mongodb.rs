//! Multi-table `faucet mirror` from a MongoDB database (#731): one change
//! stream for every collection, routed per collection, converging after a
//! crash (verified with `faucet verify`). Requires Docker.
#![cfg(all(
    feature = "source-mongodb-cdc",
    feature = "source-mongodb",
    feature = "sink-mongodb",
    feature = "transform-cdc-unwrap"
))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::replication::compiled::CompiledReplication;
use faucet_cli::replication::multi_state::{MirrorState, TablePhase};
use faucet_cli::replication::{ReplicationOptions, run_replication};
use faucet_cli::verify::{VerifyInputs, VerifySpec};
use faucet_core::StateStore as _;
use std::path::Path;
use std::time::{Duration, Instant};
use testcontainers::ContainerAsync;
use testcontainers::core::ExecCommand;
use testcontainers_modules::mongo::Mongo;

async fn start() -> (ContainerAsync<Mongo>, String) {
    let container = faucet_conformance::containers::start(Mongo::repl_set).await;
    let port = container.get_host_port_ipv4(27017).await.expect("port");
    (
        container,
        format!("mongodb://127.0.0.1:{port}/?directConnection=true"),
    )
}

async fn mongosh(c: &ContainerAsync<Mongo>, script: &str) -> String {
    let mut out = c
        .exec(ExecCommand::new(["mongosh", "--quiet", "--eval", script]))
        .await
        .expect("mongosh");
    String::from_utf8_lossy(&out.stdout_to_vec().await.expect("stdout"))
        .trim()
        .to_string()
}

fn config(uri: &str, state: &Path, continuous: bool) -> String {
    format!(
        r#"
version: 1
name: shop
pipeline:
  source:
    type: mongodb-cdc
    config:
      connection_uri: "{uri}"
      scope: {{ type: database, database: shop }}
      full_document: update_lookup
      idle_timeout: 2
  transforms:
    - type: cdc_unwrap
      config: {{}}
  sink:
    type: mongodb
    config:
      connection_uri: "{uri}"
      database: mirror
      collection: unused
      delete_marker: {{ field: __op, values: [d] }}
  state:
    type: file
    config: {{ path: "{state}" }}
mirror:
  mode: snapshot_then_cdc
  continuous: {continuous}
  snapshot:
    source:
      type: mongodb
      config: {{ connection_uri: "{uri}", database: shop, collection: unused }}
  tables:
    include: ["*"]
    discover_interval_secs: 2
"#,
        state = state.display()
    )
}

fn load(yaml: &str) -> (PipelineConfig, CompiledReplication) {
    let cfg = PipelineConfig::from_text(yaml, Path::new("shop.yaml")).unwrap();
    let compiled = CompiledReplication::compile(cfg.replication.as_ref().unwrap(), &cfg).unwrap();
    (cfg, compiled)
}

fn options() -> ReplicationOptions {
    ReplicationOptions {
        pipeline_name: "shop".into(),
        execution: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        usage: Default::default(),
        budget: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
    }
}

async fn marker(state: &Path) -> Option<MirrorState> {
    let store = faucet_core::FileStateStore::new(state);
    let v = store.get("shop::__replication__").await.ok()??;
    MirrorState::from_value(v).ok()
}

async fn verify(uri: &str, state: &Path, coll: &str) {
    let yaml = format!(
        r#"
version: 1
name: verify_{coll}
pipeline:
  source: {{ type: mongodb, config: {{ connection_uri: "{uri}", database: shop, collection: {coll} }} }}
  sink: {{ type: mongodb, config: {{ connection_uri: "{uri}", database: mirror, collection: {coll}, write_mode: upsert, key: [_id] }} }}
  state: {{ type: file, config: {{ path: "{}" }} }}
"#,
        state.join("verify").display()
    );
    let cfg = PipelineConfig::from_text(&yaml, Path::new("v.yaml")).unwrap();
    let spec: VerifySpec = serde_yaml::from_str(&format!(
        "key: [_id]\nexclude: [\"_faucet_*\", \"__op\"]\ndestination: {{ type: mongodb, config: {{ connection_uri: \"{uri}\", database: mirror, collection: {coll} }} }}"
    ))
    .unwrap();
    let outcome = faucet_cli::verify::verify(
        &cfg,
        &spec,
        VerifyInputs {
            row: None,
            repair: false,
            allow_delete: false,
            dry_run: false,
            pipeline_name: format!("verify_{coll}"),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .expect("verify");
    assert!(outcome.report.equal(), "{coll}: {:?}", outcome.report);
}

#[tokio::test(flavor = "multi_thread")]
async fn mongodb_collections_share_one_change_stream() {
    let (c, uri) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    mongosh(
        &c,
        "const s = db.getSiblingDB('shop'); \
         s.orders.insertMany([{_id: 1, amount: 10}, {_id: 2, amount: 20}]); \
         s.items.insertMany([{_id: 1, sku: 'a'}]);",
    )
    .await;
    let live = config(&uri, &state, true);
    let handle = {
        let live = live.clone();
        tokio::spawn(async move {
            let (cfg, compiled) = load(&live);
            let _ = run_replication(&cfg, &compiled, options()).await;
        })
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let m = marker(&state).await;
        if m.is_some_and(|m| m.in_phase(TablePhase::Active).len() == 2) {
            break;
        }
        assert!(Instant::now() < deadline, "collections never became active");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    mongosh(
        &c,
        "const s = db.getSiblingDB('shop'); \
         s.orders.updateOne({_id: 1}, {$set: {amount: 99}}); \
         s.orders.deleteOne({_id: 2}); \
         s.items.insertOne({_id: 2, sku: 'b'});",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let n = mongosh(&c, "db.getSiblingDB('mirror').items.countDocuments()").await;
        if n == "2" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "changes never reached the mirror ({n})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    handle.abort();
    let _ = handle.await;

    mongosh(
        &c,
        "db.getSiblingDB('shop').orders.insertOne({_id: 3, amount: 30});",
    )
    .await;
    let (cfg, compiled) = load(&config(&uri, &state, false));
    run_replication(&cfg, &compiled, options())
        .await
        .expect("resume");
    run_replication(&cfg, &compiled, options())
        .await
        .expect("drain");

    verify(&uri, &state, "orders").await;
    verify(&uri, &state, "items").await;
    let m = marker(&state).await.unwrap();
    assert_eq!(m.tables["orders"].key, vec!["_id"]);
    assert_eq!(m.tables["orders"].snapshot.attempts, 1);
}
