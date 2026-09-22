//! Arrow columnar load-job helpers (#380) — Parquet encode + the BigQuery
//! `PARQUET` load-job builder.
//!
//! The columnar sink path buffers each Arrow `RecordBatch` to a self-contained
//! Parquet file, uploads it to a GCS staging bucket, and then runs a BigQuery
//! load job (`jobs.insert`) with `sourceFormat = PARQUET`. The pure pieces here
//! (Parquet encode, the `Job` builder) are unit-tested; the GCS upload + job
//! polling live in `sink.rs`.

use crate::config::BigQuerySinkConfig;
use arrow::array::RecordBatch;
use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::job::Job;
use gcp_bigquery_client::model::job_configuration::JobConfiguration;
use gcp_bigquery_client::model::job_configuration_load::JobConfigurationLoad;
use gcp_bigquery_client::model::table_reference::TableReference;
use google_cloud_storage::client::Storage;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::OnceCell;

/// Max wall-clock spent polling a columnar Parquet load job to `DONE`. Load
/// jobs can move a lot of data, so this is generous.
const LOAD_JOB_TIMEOUT: Duration = Duration::from_secs(3600);

/// Full columnar write: encode `batch` to Parquet, stage it on GCS, then run a
/// BigQuery `PARQUET` load job to completion. This is the sink's
/// `write_batch_columnar` body, factored out here because it is pure cloud
/// I/O (GCS staging upload + BigQuery load job) that cannot run in CI — the
/// `codecov.yml` `ignore` list excludes this file for that reason, mirroring
/// the GCS connectors.
pub async fn write_columnar(
    client: &Client,
    config: &BigQuerySinkConfig,
    gcs_store: &OnceCell<Storage>,
    batch: &RecordBatch,
) -> Result<usize, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let cfg = config.bulk_load.as_ref().ok_or_else(|| {
        FaucetError::Sink("BigQuery columnar write requested with no `bulk_load` config".into())
    })?;

    // Parquet encode is CPU-bound — off the async runtime.
    let batch_owned = batch.clone();
    let bytes = tokio::task::spawn_blocking(move || encode_parquet(&batch_owned))
        .await
        .map_err(|e| FaucetError::Sink(format!("parquet encode task panicked: {e}")))??;

    // Build (once) the GCS client, then stage the Parquet object.
    let store: &Storage = gcs_store
        .get_or_try_init(|| async {
            faucet_common_gcs::build_storage(&cfg.gcs_auth, cfg.storage_host.as_deref()).await
        })
        .await?;
    let prefix = if cfg.staging_prefix.is_empty() || cfg.staging_prefix.ends_with('/') {
        cfg.staging_prefix.clone()
    } else {
        format!("{}/", cfg.staging_prefix)
    };
    let key = format!("{prefix}faucet-{}.parquet", uuid::Uuid::now_v7());
    let bucket_path = format!("projects/_/buckets/{}", cfg.staging_bucket);
    store
        .write_object(bucket_path, key.clone(), bytes::Bytes::from(bytes))
        .set_content_type("application/vnd.apache.parquet")
        .send_unbuffered()
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "BigQuery load staging upload failed for {key}: {e}"
            ))
        })?;

    // Insert + poll the load job.
    let source_uri = format!("gs://{}/{key}", cfg.staging_bucket);
    let job = build_load_job(
        &config.project_id,
        &config.dataset_id,
        &config.table_id,
        &source_uri,
        &cfg.write_disposition,
    );
    let inserted = client
        .job()
        .insert(&config.project_id, job)
        .await
        .map_err(|e| FaucetError::Sink(format!("BigQuery load jobs.insert failed: {e}")))?;
    let job_ref = inserted
        .job_reference
        .ok_or_else(|| FaucetError::Sink("BigQuery load job returned no jobReference".into()))?;
    let job_id = job_ref
        .job_id
        .ok_or_else(|| FaucetError::Sink("BigQuery load job returned no jobId".into()))?;
    await_load_job(
        client,
        &config.project_id,
        &job_id,
        job_ref.location.as_deref(),
    )
    .await?;

    tracing::info!(
        table = %format!("{}.{}.{}", config.project_id, config.dataset_id, config.table_id),
        rows = batch.num_rows(),
        uri = %source_uri,
        "BigQuery columnar Parquet load job complete"
    );
    Ok(batch.num_rows())
}

/// Bucket-free columnar write (#635): encode `batch` to Parquet and POST it
/// straight to BigQuery's media-upload endpoint as a `multipart` load job.
///
/// The GCS-staged [`write_columnar`] needs a `bulk_load` bucket, which is real
/// operational friction — a bucket to create, grant, and garbage-collect — for
/// a file that exists only to be read once, immediately, by the load job that
/// follows it. Uploading the bytes with the job removes the bucket from the
/// picture entirely, exactly as the NDJSON byte path (#633) does.
///
/// `multipart` rather than `resumable`: a Parquet file is produced whole, in
/// memory, per batch, so there is no incremental stream to feed — and the
/// resumable machinery here is built around a gzip encoder that must not touch
/// Parquet bytes. Batch size is what bounds the body; a very large batch is
/// what `bulk_load` staging remains for.
#[allow(clippy::too_many_arguments)]
pub async fn write_columnar_media(
    client: &Client,
    config: &BigQuerySinkConfig,
    upload_base: &str,
    token: &str,
    table_id: &str,
    write_disposition: &str,
    batch: &RecordBatch,
) -> Result<usize, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    // Parquet encode is CPU-bound — off the async runtime.
    let batch_owned = batch.clone();
    let media = tokio::task::spawn_blocking(move || encode_parquet(&batch_owned))
        .await
        .map_err(|e| FaucetError::Sink(format!("parquet encode task panicked: {e}")))??;

    let job_json = serde_json::json!({
        "configuration": {
            "load": {
                "sourceFormat": "PARQUET",
                "writeDisposition": write_disposition,
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": config.project_id,
                    "datasetId": config.dataset_id,
                    "tableId": table_id,
                },
            }
        }
    })
    .to_string();

    // Reuse the sink's multipart framing rather than hand-rolling a second
    // copy — the same "one escaper, one convention" rule #654 M14 was about.
    let boundary = crate::sink::media_boundary(&media);
    let body = crate::sink::build_multipart_related(&boundary, &job_json, &media);
    let url = format!(
        "{upload_base}/upload/bigquery/v2/projects/{}/jobs?uploadType=multipart",
        config.project_id
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth(token)
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/related; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .map_err(|e| FaucetError::Sink(format!("BigQuery Parquet media upload failed: {e}")))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(FaucetError::Sink(format!(
            "BigQuery Parquet media upload returned HTTP {status}: {text}"
        )));
    }
    // A load job returns 200 with the job resource; the job itself can still
    // fail later, so poll it rather than trusting the upload's status code.
    let job: Value = serde_json::from_str(&text)
        .map_err(|e| FaucetError::Sink(format!("BigQuery media upload returned non-JSON: {e}")))?;
    let job_id = job["jobReference"]["jobId"].as_str().ok_or_else(|| {
        FaucetError::Sink("BigQuery media load job returned no jobReference.jobId".into())
    })?;
    let location = job["jobReference"]["location"].as_str();
    await_load_job(client, &config.project_id, job_id, location).await?;

    tracing::info!(
        table = %format!("{}.{}.{table_id}", config.project_id, config.dataset_id),
        rows = batch.num_rows(),
        bytes = media.len(),
        disposition = write_disposition,
        "BigQuery bucket-free Parquet load job complete"
    );
    Ok(batch.num_rows())
}

/// Poll a load job by id until it reports `DONE`, mapping a runtime
/// `status.error_result` to `Err` (a failed job still returns HTTP 200 with the
/// error in the body — the trap `await_query_complete` also guards).
async fn await_load_job(
    client: &Client,
    project_id: &str,
    job_id: &str,
    location: Option<&str>,
) -> Result<(), FaucetError> {
    let deadline = std::time::Instant::now() + LOAD_JOB_TIMEOUT;
    loop {
        let job = client
            .job()
            .get_job(project_id, job_id, location)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery load jobs.get failed: {e}")))?;
        let status = job.status.ok_or_else(|| {
            FaucetError::Sink("BigQuery load job returned no status; cannot confirm".into())
        })?;
        if let Some(err) = status.error_result {
            return Err(FaucetError::Sink(format!(
                "BigQuery load job {job_id} failed: {err}"
            )));
        }
        if status.state.as_deref() == Some("DONE") {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(FaucetError::Sink(format!(
                "BigQuery load job {job_id} did not finish within {LOAD_JOB_TIMEOUT:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Encode one Arrow `RecordBatch` as a self-contained ZSTD-compressed Parquet
/// file in memory. Mirrors the S3/GCS sinks' `encode_parquet`.
pub fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>, FaucetError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| FaucetError::Sink(format!("parquet writer init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FaucetError::Sink(format!("parquet finalize failed: {e}")))?;
    }
    Ok(buf)
}

/// Build a BigQuery `PARQUET` load `Job` that loads `source_uri` (a `gs://…`
/// object) into the fully-qualified destination table. Pure.
pub fn build_load_job(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    source_uri: &str,
    write_disposition: &str,
) -> Job {
    let load = JobConfigurationLoad {
        source_uris: Some(vec![source_uri.to_string()]),
        source_format: Some("PARQUET".to_string()),
        write_disposition: Some(write_disposition.to_string()),
        create_disposition: Some("CREATE_IF_NEEDED".to_string()),
        destination_table: Some(TableReference::new(project_id, dataset_id, table_id)),
        ..Default::default()
    };
    Job {
        configuration: Some(JobConfiguration {
            load: Some(load),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn load_job_sets_parquet_source_and_destination() {
        let job = build_load_job(
            "proj",
            "ds",
            "events",
            "gs://bucket/faucet-bq-load/x.parquet",
            "WRITE_APPEND",
        );
        let load = job.configuration.unwrap().load.unwrap();
        assert_eq!(load.source_format.as_deref(), Some("PARQUET"));
        assert_eq!(load.write_disposition.as_deref(), Some("WRITE_APPEND"));
        assert_eq!(
            load.source_uris.as_deref(),
            Some(["gs://bucket/faucet-bq-load/x.parquet".to_string()].as_slice())
        );
        let dest = load.destination_table.unwrap();
        assert_eq!(dest.project_id, "proj");
        assert_eq!(dest.dataset_id, "ds");
        assert_eq!(dest.table_id, "events");
    }

    #[test]
    fn encode_parquet_roundtrips_a_batch() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .unwrap();
        let bytes = encode_parquet(&batch).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }
}
