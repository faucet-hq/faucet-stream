//! BigQuery **Storage Read API** (gRPC) Arrow path (#380).
//!
//! Reads a table directly as Arrow `RecordBatch`es via the
//! `google.cloud.bigquery.storage.v1` gRPC service — no `jobs.query`, no
//! per-row JSON. Used both by the columnar fast path
//! ([`BigQuerySource::stream_batches`](crate::stream::BigQuerySource)) and, when
//! the sink is not columnar, by the row path (Arrow → JSON) so a `read_api`
//! source works with any sink.
//!
//! The `gcloud-*` stack here (gRPC over tonic 0.14, auth via `gcloud-auth`) is
//! a deliberately separate family from the REST `gcp-bigquery-client` used for
//! the query path; it is only compiled with the `arrow` feature.

use crate::stream::BigQuerySource;
use arrow::array::RecordBatch;
use faucet_core::columnar::ColumnarPage;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::StreamExt;
use gcloud_gax::conn::{ConnectionManager, ConnectionOptions, Environment};
use gcloud_googleapis::cloud::bigquery::storage::v1::big_query_read_client::BigQueryReadClient;
use gcloud_googleapis::cloud::bigquery::storage::v1::read_rows_response::{Rows, Schema};
use gcloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions;
use gcloud_googleapis::cloud::bigquery::storage::v1::{
    CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession,
};
use serde_json::Value;
use std::pin::Pin;

use faucet_common_bigquery::BigQueryCredentials;

const STORAGE_DOMAIN: &str = "bigquerystorage.googleapis.com";
const STORAGE_AUDIENCE: &str = "https://bigquerystorage.googleapis.com/";
const STORAGE_SCOPES: [&str; 2] = [
    "https://www.googleapis.com/auth/bigquery.readonly",
    "https://www.googleapis.com/auth/cloud-platform",
];
/// Bump the client's decode cap well above the 4 MiB default — Arrow batches
/// from the Storage Read API can be large.
const MAX_DECODE_BYTES: usize = 1 << 30; // 1 GiB

/// Resolve `dataset.table` / `project.dataset.table` into the Storage Read API
/// resource name `projects/{p}/datasets/{d}/tables/{t}`. Pure.
pub fn resolve_table(project_id: &str, read_table: Option<&str>) -> Result<String, FaucetError> {
    let t = read_table.ok_or_else(|| {
        FaucetError::Config(
            "BigQuery read_api requires `read_table` (dataset.table or project.dataset.table)"
                .into(),
        )
    })?;
    match t.split('.').collect::<Vec<_>>().as_slice() {
        [dataset, table] => Ok(format!(
            "projects/{project_id}/datasets/{dataset}/tables/{table}"
        )),
        [project, dataset, table] => Ok(format!(
            "projects/{project}/datasets/{dataset}/tables/{table}"
        )),
        _ => Err(FaucetError::Config(format!(
            "BigQuery read_table '{t}' must be 'dataset.table' or 'project.dataset.table'"
        ))),
    }
}

/// Decode one Storage Read API Arrow message. The API sends the IPC schema
/// once (first response) and each batch as a standalone IPC record-batch
/// message; concatenating schema + batch bytes yields a decodable IPC stream.
fn decode_arrow(schema: &[u8], batch: &[u8]) -> Result<Vec<RecordBatch>, FaucetError> {
    use arrow::ipc::reader::StreamReader;
    let mut buf = Vec::with_capacity(schema.len() + batch.len());
    buf.extend_from_slice(schema);
    buf.extend_from_slice(batch);
    let reader = StreamReader::try_new(std::io::Cursor::new(buf), None)
        .map_err(|e| FaucetError::Source(format!("BigQuery Storage Read arrow decode: {e}")))?;
    let mut out = Vec::new();
    for b in reader {
        out.push(
            b.map_err(|e| FaucetError::Source(format!("BigQuery Storage Read arrow batch: {e}")))?,
        );
    }
    Ok(out)
}

/// Build the gRPC auth `Environment` from the connector's BigQuery credentials.
async fn build_environment(auth: &BigQueryCredentials) -> Result<Environment, FaucetError> {
    use gcloud_auth::credentials::CredentialsFile;
    use gcloud_auth::token::DefaultTokenSourceProvider;

    let cfg = gcloud_auth::project::Config::default()
        .with_audience(STORAGE_AUDIENCE)
        .with_scopes(&STORAGE_SCOPES);
    let tsp = match auth {
        BigQueryCredentials::ApplicationDefault => DefaultTokenSourceProvider::new(cfg)
            .await
            .map_err(|e| FaucetError::Auth(format!("BigQuery Storage Read ADC auth: {e}")))?,
        BigQueryCredentials::ServiceAccountKeyPath { path } => {
            let cf = CredentialsFile::new_from_file(path.clone())
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery Storage Read key file: {e}")))?;
            DefaultTokenSourceProvider::new_with_credentials(cfg, Box::new(cf))
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery Storage Read key file: {e}")))?
        }
        BigQueryCredentials::ServiceAccountKey { json } => {
            let cf = CredentialsFile::new_from_str(json)
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery Storage Read inline key: {e}")))?;
            DefaultTokenSourceProvider::new_with_credentials(cfg, Box::new(cf))
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery Storage Read inline key: {e}")))?
        }
    };
    Ok(Environment::GoogleCloud(Box::new(tsp)))
}

/// Open a read session for the configured table and stream its Arrow batches.
fn read_batches(
    src: &BigQuerySource,
) -> impl Stream<Item = Result<RecordBatch, FaucetError>> + Send + '_ {
    let cfg = src.config();
    async_stream::try_stream! {
        let env = build_environment(&cfg.auth).await?;
        let cm = ConnectionManager::new(
            1,
            STORAGE_DOMAIN,
            STORAGE_AUDIENCE,
            &env,
            &ConnectionOptions::default(),
        )
        .await
        .map_err(|e| FaucetError::Source(format!("BigQuery Storage Read connect: {e}")))?;
        let mut client = BigQueryReadClient::new(cm.conn()).max_decoding_message_size(MAX_DECODE_BYTES);

        // Either a table named outright, or — new in #621 — the destination
        // table of the configured SQL. Without the second, arbitrary SQL fell
        // back to `getQueryResults` REST JSON and never saw the fast gRPC
        // Arrow path, which is precisely where a large result needs it.
        let table = match cfg.read_table.as_deref() {
            Some(t) if !t.is_empty() => resolve_table(&cfg.project_id, Some(t))?,
            _ => src.query_destination_table().await?,
        };
        let read_options = TableReadOptions {
            selected_fields: cfg.selected_fields.clone(),
            row_restriction: cfg.row_restriction.clone().unwrap_or_default(),
            ..Default::default()
        };
        let session = ReadSession {
            data_format: DataFormat::Arrow as i32,
            table,
            read_options: Some(read_options),
            ..Default::default()
        };
        let request = CreateReadSessionRequest {
            parent: format!("projects/{}", cfg.project_id),
            read_session: Some(session),
            max_stream_count: cfg.max_streams.max(1),
            ..Default::default()
        };
        let created = client
            .create_read_session(request)
            .await
            .map_err(|e| FaucetError::Source(format!("BigQuery CreateReadSession failed: {e}")))?
            .into_inner();

        // Read the session's streams **concurrently** (#621). `max_streams`
        // asked BigQuery to shard the table, but the shards were then consumed
        // one after another, so the request bought nothing: the read stayed as
        // slow as a single stream.
        //
        // Unordered is correct here, not a shortcut: the Storage Read API
        // explicitly gives no ordering guarantee across streams — that is what
        // sharding means — so interleaving them changes nothing a caller could
        // have relied on. `stream_concurrency` bounds how many are in flight,
        // since each carries its own decode buffer.
        let concurrency = cfg.stream_concurrency.max(1);
        let mut readers = Vec::with_capacity(created.streams.len());
        for stream in &created.streams {
            let mut client = client.clone();
            let name = stream.name.clone();
            readers.push(Box::pin(async_stream::try_stream! {
                let rr = ReadRowsRequest { read_stream: name, offset: 0 };
                let mut responses = client
                    .read_rows(rr)
                    .await
                    .map_err(|e| FaucetError::Source(format!("BigQuery ReadRows failed: {e}")))?
                    .into_inner();
                // Per stream, not shared: each stream's messages carry their
                // own schema, and a batch decoded against a neighbour's schema
                // would be silently wrong rather than an error.
                let mut schema_bytes: Option<Vec<u8>> = None;
                while let Some(msg) = responses
                    .message()
                    .await
                    .map_err(|e| FaucetError::Source(format!("BigQuery ReadRows stream error: {e}")))?
                {
                    if let Some(Schema::ArrowSchema(s)) = msg.schema {
                        schema_bytes = Some(s.serialized_schema);
                    }
                    if let Some(Rows::ArrowRecordBatch(rb)) = msg.rows {
                        let sch = schema_bytes.as_deref().ok_or_else(|| {
                            FaucetError::Source(
                                "BigQuery Storage Read: record batch arrived before the Arrow schema"
                                    .into(),
                            )
                        })?;
                        for batch in decode_arrow(sch, &rb.serialized_record_batch)? {
                            if batch.num_rows() > 0 {
                                yield batch;
                            }
                        }
                    }
                }
            }) as Pin<Box<dyn Stream<Item = Result<RecordBatch, FaucetError>> + Send>>);
        }
        let mut merged = futures::stream::iter(readers).flatten_unordered(Some(concurrency));
        while let Some(batch) = merged.next().await {
            yield batch?;
        }
    }
}

/// Columnar fast path: yield one [`ColumnarPage`] per Arrow batch.
pub fn stream_batches_arrow(
    src: &BigQuerySource,
) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + '_>> {
    let cfg = src.config();
    Box::pin(async_stream::try_stream! {
        let inner = read_batches(src);
        futures::pin_mut!(inner);
        while let Some(batch) = inner.next().await {
            yield ColumnarPage::new(batch?, None);
        }
        tracing::info!(table = ?cfg.read_table, "BigQuery Storage Read columnar stream complete");
    })
}

/// Row path for a non-columnar sink: decode Arrow batches to JSON and re-frame
/// into [`StreamPage`]s of `batch_size` records.
pub fn stream_pages_arrow(
    src: &BigQuerySource,
) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + '_>> {
    let cfg = src.config();
    let batch_size = cfg.batch_size;
    Box::pin(async_stream::try_stream! {
        let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
        let mut buffer: Vec<Value> = Vec::new();
        let inner = read_batches(src);
        futures::pin_mut!(inner);
        while let Some(batch) = inner.next().await {
            let batch = batch?;
            for v in faucet_core::columnar::record_batch_to_values(&batch)? {
                buffer.push(v);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(chunk));
                    yield StreamPage { records: page, bookmark: None };
                }
            }
        }
        if !buffer.is_empty() {
            yield StreamPage { records: buffer, bookmark: None };
        }
        tracing::info!(table = ?cfg.read_table, "BigQuery Storage Read row stream complete");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_table_two_and_three_part() {
        assert_eq!(
            resolve_table("billing-proj", Some("ds.events")).unwrap(),
            "projects/billing-proj/datasets/ds/tables/events"
        );
        assert_eq!(
            resolve_table("billing-proj", Some("other-proj.ds.events")).unwrap(),
            "projects/other-proj/datasets/ds/tables/events"
        );
    }

    #[test]
    fn resolve_table_requires_table_and_valid_shape() {
        assert!(resolve_table("p", None).is_err());
        assert!(resolve_table("p", Some("just_a_name")).is_err());
        assert!(resolve_table("p", Some("a.b.c.d")).is_err());
    }
}
