//! The sink against a REST catalog that vends storage credentials (#1110,
//! #1108): stand-ins for the catalog and for a store that accepts only the
//! vended credentials.
#![cfg(feature = "catalog-rest")]

mod vending_support;

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::{FaucetError, Sink};
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use serde_json::{Value, json};
use vending_support::{Fakes, STATIC_KEY, Vend};

const TABLE: &str = "ns.t";

fn config(fakes: &Fakes, extra: Value) -> IcebergSinkConfig {
    config_with(fakes.catalog(extra), json!({}))
}

fn config_with(catalog: Value, more: Value) -> IcebergSinkConfig {
    let mut cfg = json!({
        "catalog": catalog,
        "namespace": ["ns"],
        "table": "t",
    });
    for (k, v) in more.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    serde_json::from_value(cfg).unwrap()
}

fn page(n: i64) -> Vec<Value> {
    vec![json!({"id": n, "name": format!("row-{n}")})]
}

async fn new_sink(fakes: &Fakes) -> IcebergSink {
    IcebergSink::new(config(fakes, json!({}))).await.unwrap()
}

async fn delegating_sink(fakes: &Fakes) -> IcebergSink {
    let mut catalog = fakes.catalog(json!({}));
    catalog["access_delegation"] = json!("vended_credentials");
    IcebergSink::new(config_with(catalog, json!({})))
        .await
        .unwrap()
}

/// Write and commit one page through a vending catalog, so the table exists.
async fn seeded(fakes: &Fakes) -> IcebergSink {
    let sink = new_sink(fakes).await;
    sink.write_batch(&page(1)).await.unwrap();
    sink.flush().await.unwrap();
    sink
}

fn refusal(err: FaucetError) -> String {
    assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    err.to_string()
}

fn addition(name: &str) -> faucet_core::SchemaEvolution {
    let mut evolution = faucet_core::SchemaEvolution::default();
    evolution.additions.push(faucet_core::ColumnChange {
        name: name.to_string(),
        from: None,
        to: json!({"type": ["string", "null"]}),
    });
    evolution
}

#[tokio::test(flavor = "multi_thread")]
async fn three_flushes_in_one_run_all_commit() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = IcebergSink::new(config(&fakes, json!({}))).await.unwrap();

    sink.write_batch(&page(1)).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 1, "the first page commits");
    let after_first = fakes.s3_calls().len();

    sink.write_batch(&page(2)).await.unwrap();
    let second = sink.flush().await;
    let calls = fakes.s3_calls();
    let first_upload = calls[after_first..]
        .iter()
        .find(|c| c.method == "PUT" || c.method == "POST");
    assert!(
        first_upload.is_some_and(|c| c.generation.is_some() && c.status < 300),
        "the second flush's first upload must carry vended credentials the store accepts; \
         flush: {second:?}; upload: {first_upload:?}"
    );
    second.unwrap();

    sink.write_batch(&page(3)).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 3);
    assert!(
        fakes.uploads().iter().all(|c| c.generation.is_some()),
        "every upload is signed with vended credentials"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_opened_after_the_credentials_expired_gets_new_ones() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = seeded(&fakes).await;
    fakes.expire_issued();
    let expired = fakes.issued();
    let before = fakes.s3_calls().len();

    sink.write_batch(&page(2)).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 2);
    let uploads: Vec<_> = fakes.s3_calls()[before..]
        .iter()
        .filter(|c| c.method == "PUT")
        .cloned()
        .collect();
    assert!(!uploads.is_empty());
    assert!(
        uploads
            .iter()
            .all(|c| c.generation.is_some_and(|g| g > expired) && c.status < 300),
        "{uploads:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_refresh_keeps_the_previous_handle() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = seeded(&fakes).await;
    fakes.fail_next_loads(1);
    sink.write_batch(&page(2)).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 2);
    assert!(
        fakes
            .catalog_calls()
            .iter()
            .any(|c| c.method == "GET" && c.status == 500),
        "the refresh was tried and failed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flush_right_after_schema_evolution_commits() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = new_sink(&fakes).await;
    sink.write_batch(&page(1)).await.unwrap();
    sink.evolve_schema(&addition("extra")).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(
        fakes.snapshots(TABLE),
        1,
        "the files written before evolve commit"
    );
    sink.write_batch(&page(2)).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 2);
    assert!(fakes.uploads().iter().all(|c| c.status < 300));
}

#[tokio::test(flavor = "multi_thread")]
#[allow(deprecated)]
async fn the_commit_token_is_found_after_each_commit() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = new_sink(&fakes).await;
    for seq in 1..=3u64 {
        let token = faucet_core::idempotency::format_token(seq);
        sink.write_batch_idempotent(&page(seq as i64), "p::r", &token)
            .await
            .unwrap();
        sink.flush().await.unwrap();
        assert_eq!(
            sink.last_committed_token("p::r").await.unwrap(),
            Some(token)
        );
    }
    assert_eq!(fakes.snapshots(TABLE), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn static_keys_with_a_vended_token_are_refused_before_any_upload() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = IcebergSink::new(config(&fakes, Fakes::static_keys()))
        .await
        .unwrap();
    let err = refusal(sink.write_batch(&page(1)).await.unwrap_err());
    assert!(err.contains("Remove the static keys"), "{err}");
    assert!(fakes.uploads().is_empty(), "{:?}", fakes.uploads());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_ignores_the_token_keeps_working() {
    let fakes = Fakes::start(Vend::Always).await;
    fakes.accept_static_key(STATIC_KEY);
    let sink = IcebergSink::new(config(&fakes, Fakes::static_keys()))
        .await
        .unwrap();
    for n in 1..=2 {
        sink.write_batch(&page(n)).await.unwrap();
        sink.flush().await.unwrap();
    }
    assert_eq!(fakes.snapshots(TABLE), 2);
    assert!(
        fakes
            .uploads()
            .iter()
            .all(|c| c.access_key.as_deref() == Some(STATIC_KEY))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_credentials_fail_before_any_upload() {
    let fakes = Fakes::start(Vend::Never).await;
    let sink = new_sink(&fakes).await;
    let err = refusal(sink.write_batch(&page(1)).await.unwrap_err());
    assert!(err.contains("no usable storage credentials"), "{err}");
    assert!(err.contains("the catalog returned none"), "{err}");
    assert!(
        err.contains("access_delegation: vended_credentials")
            && err.contains("s3.access-key-id and s3.secret-access-key"),
        "names both fixes: {err}"
    );
    assert!(fakes.uploads().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn access_delegation_makes_the_catalog_vend() {
    let fakes = Fakes::start(Vend::WhenAsked).await;
    let sink = delegating_sink(&fakes).await;
    for n in 1..=2 {
        sink.write_batch(&page(n)).await.unwrap();
        sink.flush().await.unwrap();
    }
    assert_eq!(fakes.snapshots(TABLE), 2);
    let calls = fakes.catalog_calls();
    assert!(
        calls
            .iter()
            .all(|c| c.delegation.as_deref() == Some("vended-credentials")),
        "{calls:?}"
    );

    let unasked = Fakes::start(Vend::WhenAsked).await;
    let err = refusal(
        new_sink(&unasked)
            .await
            .write_batch(&page(1))
            .await
            .unwrap_err(),
    );
    assert!(
        err.contains("access_delegation: vended_credentials"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_ignores_the_header_is_named() {
    let fakes = Fakes::start(Vend::Never).await;
    let err = refusal(
        delegating_sink(&fakes)
            .await
            .write_batch(&page(1))
            .await
            .unwrap_err(),
    );
    assert!(
        err.contains("although access_delegation: vended_credentials asks for them"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_reports_the_storage() {
    let ctx = CheckContext::default();
    let fakes = Fakes::start(Vend::Always).await;
    let fresh = new_sink(&fakes).await;
    let report = fresh.check(&ctx).await.unwrap();
    assert_eq!(report.probes.len(), 2, "{report:?}");
    assert!(matches!(report.probes[1].status, ProbeStatus::Skip { .. }));

    let seeded = seeded(&fakes).await;
    let report = seeded.check(&ctx).await.unwrap();
    assert_eq!(report.probes[1].name, "storage");
    assert!(
        matches!(report.probes[1].status, ProbeStatus::Pass),
        "{report:?}"
    );

    fakes.set_vend(Vend::Never);
    let report = seeded.check(&ctx).await.unwrap();
    match &report.probes[1].status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("no usable storage credentials"), "{reason}")
        }
        other => panic!("{other:?}"),
    }
    assert!(report.probes[1].hint.is_some());

    fakes.drop_table(TABLE);
    let report = seeded.check(&ctx).await.unwrap();
    assert!(matches!(report.probes[1].status, ProbeStatus::Skip { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_preloaded_table_is_checked_at_construction() {
    let fakes = Fakes::start(Vend::Always).await;
    seeded(&fakes).await;
    let existing = config_with(
        fakes.catalog(json!({})),
        json!({"create_if_missing": false}),
    );
    let ok = IcebergSink::new(existing.clone()).await.unwrap();
    ok.write_batch(&page(2)).await.unwrap();
    ok.flush().await.unwrap();
    assert_eq!(fakes.snapshots(TABLE), 2);

    fakes.set_vend(Vend::Never);
    let err = refusal(IcebergSink::new(existing).await.unwrap_err());
    assert!(err.contains("no usable storage credentials"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn orphans_are_deleted_with_working_credentials() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = IcebergSink::new(config_with(
        fakes.catalog(json!({})),
        json!({
            "cleanup_orphans_on_failure": true,
            "snapshot_properties": {"commit.retry.num-retries": "0"}
        }),
    ))
    .await
    .unwrap();
    sink.write_batch(&page(1)).await.unwrap();
    sink.flush().await.unwrap();
    fakes.conflict_on_commit();
    sink.write_batch(&page(2)).await.unwrap();
    assert!(sink.flush().await.is_err());
    assert_eq!(fakes.object_count("/data/"), 1, "the orphan was deleted");
    let deletes: Vec<_> = fakes
        .s3_calls()
        .into_iter()
        .filter(|c| c.method == "DELETE")
        .collect();
    assert!(!deletes.is_empty());
    assert!(
        deletes
            .iter()
            .all(|c| c.generation.is_some() && c.status < 300),
        "{deletes:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_table_fails_the_write_before_any_upload() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = seeded(&fakes).await;
    fakes.drop_table(TABLE);
    let uploads = fakes.uploads().len();
    let err = sink.write_batch(&page(2)).await.unwrap_err().to_string();
    assert!(err.contains("no longer exists in the catalog"), "{err}");
    assert_eq!(fakes.uploads().len(), uploads);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_table_fails_the_write_before_any_upload() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = seeded(&fakes).await;
    fakes.replace_table(TABLE);
    let uploads = fakes.uploads().len();
    let err = sink.write_batch(&page(2)).await.unwrap_err().to_string();
    assert!(err.contains("was replaced by another table"), "{err}");
    assert_eq!(fakes.uploads().len(), uploads);
}

fn parquet_columns(bytes: Vec<u8>) -> Vec<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_catalog_cannot_bring_back_the_old_schema() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink = seeded(&fakes).await;
    sink.evolve_schema(&addition("extra")).await.unwrap();
    let before: Vec<String> = fakes
        .objects("/data/")
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    fakes.stale_next_loads(1);
    sink.write_batch(&[json!({"id": 2, "name": "row-2", "extra": "kept"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let written: Vec<_> = fakes
        .objects("/data/")
        .into_iter()
        .filter(|(k, _)| !before.contains(k))
        .collect();
    assert_eq!(written.len(), 1);
    let columns = parquet_columns(written[0].1.clone());
    assert!(columns.contains(&"extra".to_string()), "{columns:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handle_stored_by_evolve_is_still_preflighted() {
    let fakes = Fakes::start(Vend::Always).await;
    seeded(&fakes).await;
    fakes.set_vend(Vend::Never);
    let sink = new_sink(&fakes).await;
    sink.evolve_schema(&addition("extra")).await.unwrap();
    let uploads = fakes.uploads().len();
    let err = refusal(sink.write_batch(&page(2)).await.unwrap_err());
    assert!(err.contains("no usable storage credentials"), "{err}");
    assert_eq!(fakes.uploads().len(), uploads);
}
