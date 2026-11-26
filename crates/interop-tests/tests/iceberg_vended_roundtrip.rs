//! Iceberg sink and source on a REST catalog that vends storage credentials
//! (#1110, #1108): pages the sink commits in one run are read back by the
//! source with the credentials the catalog hands out, and one catalog block
//! configures both.

#[path = "../../sink/iceberg/tests/vending_support/mod.rs"]
mod vending_support;

use std::collections::HashMap;

use faucet_core::{Sink, Source};
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use vending_support::{Fakes, Vend};

#[tokio::test(flavor = "multi_thread")]
async fn pages_the_sink_commits_are_read_back_on_vended_credentials() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": fakes.catalog(json!({})), "namespace": ["ns"], "table": "t"
    }))
    .unwrap();
    let sink = IcebergSink::new(sink).await.unwrap();
    for n in 1..=2 {
        sink.write_batch(&[json!({"id": n})]).await.unwrap();
        sink.flush().await.unwrap();
    }
    fakes.expire_issued();
    let source: IcebergSourceConfig = serde_json::from_value(json!({
        "catalog": fakes.catalog(json!({})), "table": "ns.t"
    }))
    .unwrap();
    let mut ids: Vec<i64> = IcebergSource::new(source)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, [1, 2]);
    assert!(fakes.s3_calls().iter().all(|c| c.status < 400));
}

#[test]
fn one_catalog_block_configures_the_sink_and_the_source() {
    let catalog = json!({
        "type": "rest",
        "uri": "http://catalog:8181",
        "warehouse": "lake",
        "access_delegation": "vended_credentials",
        "properties": {"s3.endpoint": "http://store:9000"}
    });
    let sink: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog, "namespace": ["ns"], "table": "t"
    }))
    .unwrap();
    sink.validate().unwrap();
    let source: IcebergSourceConfig =
        serde_json::from_value(json!({"catalog": catalog, "table": "ns.t"})).unwrap();
    source.validate().unwrap();
    assert_eq!(
        source.catalog.inner().access_delegation,
        Some(faucet_common_iceberg::AccessDelegation::VendedCredentials)
    );
    assert_eq!(
        serde_json::to_value(&sink.catalog).unwrap(),
        serde_json::to_value(&source.catalog).unwrap()
    );
}

// ── Fresh credentials during a long read (#1117) ─────────────────────────────

/// Write one data file per id through the sink.
async fn seed(fakes: &Fakes, catalog: Value, ids: std::ops::RangeInclusive<i64>) {
    let sink: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog, "namespace": ["ns"], "table": "t"
    }))
    .unwrap();
    let sink = IcebergSink::new(sink).await.unwrap();
    for n in ids {
        sink.write_batch(&[json!({"id": n})]).await.unwrap();
        sink.flush().await.unwrap();
    }
    let _ = fakes;
}

/// A source that takes fresh credentials before every data file, one file at
/// a time.
async fn file_by_file(catalog: Value, extra: Value) -> IcebergSource {
    let mut config = json!({
        "catalog": catalog, "table": "ns.t", "credential_refresh_secs": 0, "concurrency": 1
    });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    IcebergSource::new(serde_json::from_value(config).unwrap())
        .await
        .unwrap()
}

fn sorted_ids(rows: &[Value]) -> Vec<i64> {
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    ids
}

/// Read the first page, run `between`, then read the rest.
async fn read_around(
    source: &IcebergSource,
    between: impl FnOnce(),
) -> Result<Vec<Value>, faucet_core::FaucetError> {
    let context = HashMap::new();
    let mut pages = source.stream_pages(&context, 0);
    let mut rows = pages.next().await.expect("a first page")?.records;
    assert_eq!(rows.len(), 1, "one data file per page");
    between();
    while let Some(page) = pages.next().await {
        rows.extend(page?.records);
    }
    Ok(rows)
}

fn loads(fakes: &Fakes) -> usize {
    fakes
        .catalog_calls()
        .iter()
        .filter(|c| c.method == "GET" && c.path.ends_with("/tables/t"))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_that_outlives_its_credentials_takes_fresh_ones_and_reads_every_row() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=4).await;
    let source = file_by_file(fakes.catalog(json!({})), json!({})).await;
    let before = fakes.issued();
    let rows = read_around(&source, || fakes.expire_issued())
        .await
        .unwrap();
    assert_eq!(sorted_ids(&rows), [1, 2, 3, 4]);
    assert!(fakes.issued() > before, "the catalog vended again");
    assert!(
        fakes.s3_calls().iter().all(|c| c.status < 400),
        "no request went out on expired credentials"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_file_is_read_once_with_many_readers_in_turn() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=12).await;
    let source = file_by_file(fakes.catalog(json!({})), json!({ "concurrency": 4 })).await;
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(sorted_ids(&rows), (1..=12).collect::<Vec<_>>());

    let context = HashMap::new();
    let mut batches = source.stream_batches(&context, 0);
    let mut columnar = Vec::new();
    while let Some(page) = batches.next().await {
        columnar.extend(faucet_core::record_batch_to_values(&page.unwrap().batch).unwrap());
    }
    assert_eq!(sorted_ids(&columnar), (1..=12).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_incremental_run_refreshes_between_its_steps() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=2).await;
    let incremental = json!({ "mode": "incremental" });
    let first = file_by_file(fakes.catalog(json!({})), incremental.clone()).await;
    let context = HashMap::new();
    let mut bookmark = None;
    let mut pages = first.stream_pages(&context, 0);
    while let Some(page) = pages.next().await {
        bookmark = page.unwrap().bookmark.or(bookmark);
    }
    drop(pages);
    seed(&fakes, fakes.catalog(json!({})), 3..=5).await;

    let second = file_by_file(fakes.catalog(json!({})), incremental).await;
    second
        .apply_start_bookmark(bookmark.expect("a bookmark"))
        .await
        .unwrap();
    let rows = read_around(&second, || fakes.expire_issued())
        .await
        .unwrap();
    assert_eq!(sorted_ids(&rows), [3, 4, 5]);
    assert!(fakes.s3_calls().iter().all(|c| c.status < 400));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_refresh_keeps_reading_while_the_credentials_still_work() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=3).await;
    let source = file_by_file(fakes.catalog(json!({})), json!({})).await;
    let rows = read_around(&source, || fakes.fail_next_loads(1))
        .await
        .unwrap();
    assert_eq!(sorted_ids(&rows), [1, 2, 3]);
    assert!(
        fakes.catalog_calls().iter().any(|c| c.status == 500),
        "one refresh did fail"
    );
}

/// A reload that comes back without vended credentials must not replace the
/// credentials the read is working with.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_that_brings_no_credentials_keeps_the_ones_in_use() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=3).await;
    let source = file_by_file(fakes.catalog(json!({})), json!({})).await;
    let rows = read_around(&source, || fakes.set_vend(Vend::Never))
        .await
        .unwrap();
    assert_eq!(sorted_ids(&rows), [1, 2, 3]);
    assert!(
        fakes.s3_calls().iter().all(|c| c.status < 400),
        "every read went out on the vended credentials"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_never_answers_a_refresh_does_not_stall_the_read() {
    let fakes = Fakes::start(Vend::Always).await;
    seed(&fakes, fakes.catalog(json!({})), 1..=2).await;
    let source = file_by_file(fakes.catalog(json!({})), json!({})).await;
    let started = std::time::Instant::now();
    let rows = read_around(&source, || fakes.hang_next_loads(1))
        .await
        .unwrap();
    assert_eq!(sorted_ids(&rows), [1, 2]);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(90),
        "the refresh was given up, not waited out: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_dropped_or_replaced_during_the_read_fails_it_by_name() {
    for (replace, says) in [(false, "no longer exists"), (true, "was replaced")] {
        let fakes = Fakes::start(Vend::Always).await;
        seed(&fakes, fakes.catalog(json!({})), 1..=3).await;
        let source = file_by_file(fakes.catalog(json!({})), json!({})).await;
        let err = read_around(&source, || match replace {
            true => fakes.replace_table("ns.t"),
            false => fakes.drop_table("ns.t"),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(says) && err.contains("ns.t"), "{err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_the_catalog_did_not_vend_are_never_refreshed() {
    let fakes = Fakes::start(Vend::Never).await;
    fakes.accept_static_key(vending_support::STATIC_KEY);
    let catalog = fakes.catalog(Fakes::static_keys());
    seed(&fakes, catalog.clone(), 1..=3).await;
    let before = loads(&fakes);
    let source = file_by_file(catalog, json!({})).await;
    assert_eq!(sorted_ids(&source.fetch_all().await.unwrap()), [1, 2, 3]);
    assert_eq!(
        loads(&fakes) - before,
        1,
        "one table load for the whole read"
    );
}
