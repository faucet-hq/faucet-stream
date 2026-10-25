//! Arrow columnar load-job path (#380): assert the sink advertises the
//! columnar fast path exactly when a `bulk_load` staging config is present and
//! the write mode is `append` — the "columnar path is taken" acceptance
//! criterion. No network: `supports_columnar` is pure, and the offline client
//! is never driven.
#![cfg(feature = "arrow")]

use faucet_core::Sink;
use faucet_sink_bigquery::config::BigQueryLoadConfig;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde_json::json;

/// Throwaway service-account JSON — enough for the client builder to assemble
/// an authenticator offline (it is never used to make a call here).
fn dummy_sa_json() -> serde_json::Value {
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": include_str!("fixtures/bigquery_test_key.pem"),
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": "https://example.invalid/o/oauth2/auth",
        "token_uri": "https://example.invalid/token",
    })
}

async fn offline_client() -> (gcp_bigquery_client::Client, tempfile::NamedTempFile) {
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_sa_json()).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_v2_base_url("https://example.invalid".into())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("offline client");
    (client, sa_file)
}

fn load_cfg() -> BigQueryLoadConfig {
    BigQueryLoadConfig {
        staging_bucket: "stage-bucket".into(),
        staging_prefix: "faucet-bq-load/".into(),
        gcs_auth: Default::default(),
        write_disposition: "WRITE_APPEND".into(),
        storage_host: None,
    }
}

#[tokio::test]
async fn columnar_is_advertised_for_append_and_overwrite_with_or_without_a_bucket() {
    let (client, _sa) = offline_client().await;

    // #635: a staging bucket is no longer a precondition. Without one the
    // batch is uploaded with the load job itself, so the columnar path is
    // still available — that is the whole point of the bucket-free route.
    let plain = BigQuerySink::from_parts(
        BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault),
        client.clone(),
    );
    assert!(
        plain.supports_columnar(),
        "bucket-free append must advertise the columnar path (#635)"
    );

    // With a bucket it stays available — that route just stages on GCS first.
    let staged = BigQuerySink::from_parts(
        BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
            .with_bulk_load(load_cfg()),
        client.clone(),
    );
    assert!(staged.supports_columnar());

    // Overwrite is a load-job disposition (`WRITE_TRUNCATE`), so it is
    // columnar-capable too (#635).
    let mut ow = BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault);
    ow.write.write_mode = faucet_core::WriteMode::Overwrite;
    let overwrite = BigQuerySink::from_parts(ow, client.clone());
    assert!(overwrite.supports_columnar());

    // Upsert/delete are NOT: a load job cannot express a MERGE, so those must
    // stay on the `Value` path or the pipeline would silently append.
    for mode in [
        faucet_core::WriteMode::Upsert,
        faucet_core::WriteMode::Delete,
    ] {
        let mut cfg =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
                .with_bulk_load(load_cfg());
        cfg.write.write_mode = mode;
        cfg.write.key = vec!["id".into()];
        let sink = BigQuerySink::from_parts(cfg, client.clone());
        assert!(
            !sink.supports_columnar(),
            "{mode:?} must not negotiate the columnar loop"
        );
    }
}

/// A staged load runs one job per batch, so a truncating disposition would
/// keep only the last batch (SQL-100): it is refused before any upload.
#[tokio::test]
async fn a_staged_truncate_disposition_is_refused_before_any_upload() {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let (client, _sa) = offline_client().await;
    for disposition in ["WRITE_TRUNCATE", "WRITE_EMPTY"] {
        let mut load = load_cfg();
        load.write_disposition = disposition.into();
        let sink = BigQuerySink::from_parts(
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
                .with_bulk_load(load),
            client.clone(),
        );
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
        )])
        .expect("batch");
        let err = sink
            .write_batch_columnar(&batch)
            .await
            .expect_err("a non-append staged disposition must be refused");
        assert!(err.to_string().contains(disposition), "{err}");
    }
}

/// A staged (`bulk_load`) overwrite is refused rather than silently producing
/// the wrong result: that path's write disposition is fixed per config, so it
/// cannot truncate on the first batch and append on the rest, and loading
/// every batch with `WRITE_TRUNCATE` would leave only the last one.
#[tokio::test]
async fn a_staged_overwrite_is_refused_instead_of_replacing_its_own_output() {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let (client, _sa) = offline_client().await;
    let mut cfg = BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
        .with_bulk_load(load_cfg());
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    let sink = BigQuerySink::from_parts(cfg, client);

    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
    )])
    .expect("batch");
    let err = sink
        .write_batch_columnar(&batch)
        .await
        .expect_err("a staged overwrite must be refused");
    assert!(
        err.to_string().contains("bucket-free only"),
        "the refusal must name the way out: {err}"
    );
}
