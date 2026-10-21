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
use gcloud_gax::grpc::{Code, Status, Streaming};
use gcloud_googleapis::cloud::bigquery::storage::v1::big_query_read_client::BigQueryReadClient;
use gcloud_googleapis::cloud::bigquery::storage::v1::read_rows_response::{Rows, Schema};
use gcloud_googleapis::cloud::bigquery::storage::v1::read_session::TableReadOptions;
use gcloud_googleapis::cloud::bigquery::storage::v1::{
    CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadRowsResponse, ReadSession,
};
use serde_json::Value;
use std::collections::HashMap;
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

/// How long a Storage Read call — opening the session or a stream, or waiting
/// for the next message — may go without a response.
pub(crate) const GRPC_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// `fut` bounded by [`GRPC_IDLE_TIMEOUT`], so a half-open connection fails the
/// read instead of hanging it.
async fn within<F: std::future::Future>(fut: F, what: &str) -> Result<F::Output, FaucetError> {
    within_for(GRPC_IDLE_TIMEOUT, fut, what).await
}

/// `fut` as one `op` backend call through `calls` (#880) — ended by the run's
/// cancel token and deadline — and bounded by [`GRPC_IDLE_TIMEOUT`].
async fn call<F: std::future::Future>(
    calls: &faucet_core::CallContext,
    op: &'static str,
    fut: F,
    what: &str,
) -> Result<F::Output, FaucetError> {
    calls.call_once(op, within(fut, what)).await
}

async fn within_for<F: std::future::Future>(
    limit: std::time::Duration,
    fut: F,
    what: &str,
) -> Result<F::Output, FaucetError> {
    tokio::time::timeout(limit, fut).await.map_err(|_| {
        FaucetError::Source(format!(
            "BigQuery Storage Read {what}: no response within {}s",
            limit.as_secs()
        ))
    })
}

/// The messages of one `ReadRows` call.
type MessageStream = Pin<Box<dyn Stream<Item = Result<ReadRowsResponse, Status>> + Send>>;

/// Consecutive failed attempts (no row received in between) after which a
/// `ReadRows` stream is given up.
const MAX_STREAM_RESUMES: u32 = 5;

/// The wait before resume attempt `attempt` (1-based): 1 s doubling, at most 30 s.
fn resume_backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_secs((1u64 << attempt.saturating_sub(1).min(5)).min(30))
}

/// Whether a `ReadRows` failure is transient, so the stream is reopened at
/// the rows already read. Long `ReadRows` streams are reset by the service as a
/// matter of course (`UNAVAILABLE`, `INTERNAL` RST_STREAM).
fn resumable(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::Internal
            | Code::Aborted
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
    )
}

fn message_stream(responses: Streaming<ReadRowsResponse>) -> MessageStream {
    Box::pin(futures::stream::unfold(
        Some(responses),
        |responses| async move {
            let mut responses = responses?;
            match responses.message().await {
                Ok(Some(m)) => Some((Ok(m), Some(responses))),
                Ok(None) => None,
                Err(e) => Some((Err(e), None)),
            }
        },
    ))
}

/// The messages of one Storage Read stream, reopened at `offset` = the rows
/// already received after a transient failure — as the Storage Read client
/// libraries do — so a routine stream reset neither fails the read nor
/// repeats a row. `open(offset)` starts a `ReadRows` call at that row.
fn resumable_messages<O, F>(
    calls: faucet_core::CallContext,
    mut open: O,
) -> impl Stream<Item = Result<ReadRowsResponse, FaucetError>> + Send
where
    O: FnMut(i64) -> F + Send,
    F: std::future::Future<Output = Result<MessageStream, Status>> + Send,
{
    async_stream::try_stream! {
        let mut offset: i64 = 0;
        let mut failures: u32 = 0;
        'open: loop {
            let opened = call(&calls, "read_rows", open(offset), "ReadRows").await?;
            let mut messages = match opened {
                Ok(m) => m,
                Err(status) => {
                    failures += 1;
                    give_up_unless_resumable(&status, failures, "BigQuery ReadRows failed")?;
                    calls.wait(resume_backoff(failures)).await?;
                    continue 'open;
                }
            };
            loop {
                match call(&calls, "read_rows", messages.next(), "ReadRows message").await? {
                    None => break 'open,
                    Some(Ok(msg)) => {
                        if msg.row_count > 0 {
                            offset += msg.row_count;
                            failures = 0;
                        }
                        yield msg;
                    }
                    Some(Err(status)) => {
                        failures += 1;
                        give_up_unless_resumable(
                            &status,
                            failures,
                            "BigQuery ReadRows stream error",
                        )?;
                        tracing::warn!(
                            offset,
                            attempt = failures,
                            error = %status,
                            "BigQuery ReadRows stream interrupted; resuming at the rows already read"
                        );
                        calls.wait(resume_backoff(failures)).await?;
                        continue 'open;
                    }
                }
            }
        }
    }
}

fn give_up_unless_resumable(status: &Status, failures: u32, what: &str) -> Result<(), FaucetError> {
    if resumable(status.code()) && failures <= MAX_STREAM_RESUMES {
        Ok(())
    } else {
        Err(FaucetError::Source(format!("{what}: {status}")))
    }
}

/// Open a read session for the configured table and stream its Arrow batches.
fn read_batches<'a>(
    src: &'a BigQuerySource,
    context: &'a HashMap<String, Value>,
) -> impl Stream<Item = Result<RecordBatch, FaucetError>> + Send + 'a {
    let cfg = src.config();
    let calls = src.call_context().without_retry();
    async_stream::try_stream! {
        let env = build_environment(&cfg.auth).await?;
        let cm = call(
            &calls,
            "connect",
            ConnectionManager::new(
                1,
                STORAGE_DOMAIN,
                STORAGE_AUDIENCE,
                &env,
                &ConnectionOptions::default(),
            ),
            "connect",
        )
        .await?
        .map_err(|e| FaucetError::Source(format!("BigQuery Storage Read connect: {e}")))?;
        let mut client = BigQueryReadClient::new(cm.conn()).max_decoding_message_size(MAX_DECODE_BYTES);

        // Either a table named outright, or — new in #621 — the destination
        // table of the configured SQL. Without the second, arbitrary SQL fell
        // back to `getQueryResults` REST JSON and never saw the fast gRPC
        // Arrow path, which is precisely where a large result needs it.
        let table = match cfg.read_table.as_deref() {
            Some(t) if !t.is_empty() => resolve_table(&cfg.project_id, Some(t))?,
            _ => src.query_destination_table(context).await?,
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
        let created = call(&calls, "read_session", client.create_read_session(request), "CreateReadSession")
            .await?
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
            let client = client.clone();
            let name = stream.name.clone();
            let calls = calls.clone();
            let open = move |offset: i64| {
                let mut client = client.clone();
                let read_stream = name.clone();
                async move {
                    let responses = client
                        .read_rows(ReadRowsRequest { read_stream, offset })
                        .await?
                        .into_inner();
                    Ok(message_stream(responses))
                }
            };
            readers.push(Box::pin(async_stream::try_stream! {
                let messages = resumable_messages(calls, open);
                futures::pin_mut!(messages);
                // Per stream, not shared: each stream's messages carry their
                // own schema, and a batch decoded against a neighbour's schema
                // would be silently wrong rather than an error. Kept across a
                // resume: the stream's schema does not change.
                let mut schema_bytes: Option<Vec<u8>> = None;
                while let Some(msg) = messages.next().await {
                    let msg = msg?;
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
    stream_batches_arrow_with(src, &NO_CONTEXT)
}

static NO_CONTEXT: std::sync::LazyLock<HashMap<String, Value>> =
    std::sync::LazyLock::new(HashMap::new);

/// [`stream_batches_arrow`] with a parent-record context, bound into the query
/// the same way the REST path binds it.
pub fn stream_batches_arrow_with<'a>(
    src: &'a BigQuerySource,
    context: &'a HashMap<String, Value>,
) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
    let cfg = src.config();
    Box::pin(async_stream::try_stream! {
        let inner = read_batches(src, context);
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
    stream_pages_arrow_with(src, &NO_CONTEXT)
}

/// [`stream_pages_arrow`] with a parent-record context.
pub fn stream_pages_arrow_with<'a>(
    src: &'a BigQuerySource,
    context: &'a HashMap<String, Value>,
) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
    let cfg = src.config();
    let batch_size = cfg.batch_size;
    Box::pin(async_stream::try_stream! {
        let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
        let mut buffer: Vec<Value> = Vec::new();
        let inner = read_batches(src, context);
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
    #[tokio::test]
    async fn grpc_waits_are_bounded() {
        let ok = super::within(async { 1 }, "x").await.unwrap();
        assert_eq!(ok, 1);
        let err = super::within_for(
            std::time::Duration::from_millis(5),
            futures::future::pending::<()>(),
            "ReadRows",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("ReadRows"), "{err}");
    }

    use super::*;
    use std::sync::{Arc, Mutex};

    fn msg(rows: i64) -> ReadRowsResponse {
        ReadRowsResponse {
            row_count: rows,
            ..Default::default()
        }
    }

    /// An `open` that serves `script[n]` for the n-th call and records the
    /// offsets it was asked for.
    fn scripted(
        script: Vec<Result<Vec<Result<ReadRowsResponse, Status>>, Status>>,
        offsets: Arc<Mutex<Vec<i64>>>,
    ) -> impl FnMut(i64) -> futures::future::Ready<Result<MessageStream, Status>> + Send {
        let mut script = script.into_iter();
        move |offset| {
            offsets.lock().unwrap().push(offset);
            futures::future::ready(
                script
                    .next()
                    .expect("opened more often than scripted")
                    .map(|m| Box::pin(futures::stream::iter(m)) as MessageStream),
            )
        }
    }

    async fn drain(
        script: Vec<Result<Vec<Result<ReadRowsResponse, Status>>, Status>>,
    ) -> (Result<Vec<i64>, FaucetError>, Vec<i64>) {
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let calls = faucet_core::CallContext::new();
        let s = resumable_messages(calls, scripted(script, offsets.clone()));
        futures::pin_mut!(s);
        let mut rows = Vec::new();
        let mut out = Ok(());
        while let Some(m) = s.next().await {
            match m {
                Ok(m) => rows.push(m.row_count),
                Err(e) => {
                    out = Err(e);
                    break;
                }
            }
        }
        let got = offsets.lock().unwrap().clone();
        (out.map(|()| rows), got)
    }

    #[tokio::test(start_paused = true)]
    async fn a_reset_stream_resumes_at_the_rows_already_read() {
        let (rows, offsets) = drain(vec![
            Ok(vec![
                Ok(msg(10)),
                Ok(msg(5)),
                Err(Status::unavailable("stream reset")),
            ]),
            Err(Status::internal("RST_STREAM")),
            Ok(vec![Ok(msg(7))]),
        ])
        .await;
        assert_eq!(rows.unwrap(), vec![10, 5, 7]);
        assert_eq!(offsets, vec![0, 15, 15]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_error_is_not_resumed() {
        let (rows, offsets) = drain(vec![Ok(vec![
            Ok(msg(3)),
            Err(Status::permission_denied("no")),
        ])])
        .await;
        let err = rows.unwrap_err().to_string();
        assert!(err.contains("ReadRows stream error"), "{err}");
        assert_eq!(offsets, vec![0]);
    }

    #[tokio::test(start_paused = true)]
    async fn resumes_are_bounded_without_progress() {
        let script = (0..=MAX_STREAM_RESUMES)
            .map(|_| Err(Status::unavailable("down")))
            .collect();
        let (rows, offsets) = drain(script).await;
        assert!(rows.unwrap_err().to_string().contains("ReadRows failed"));
        assert_eq!(offsets.len() as u32, MAX_STREAM_RESUMES + 1);
    }

    #[test]
    fn resume_policy() {
        assert!(resumable(Code::Unavailable));
        assert!(resumable(Code::Internal));
        assert!(!resumable(Code::InvalidArgument));
        assert!(!resumable(Code::NotFound));
        assert_eq!(resume_backoff(1).as_secs(), 1);
        assert_eq!(resume_backoff(3).as_secs(), 4);
        assert_eq!(resume_backoff(40).as_secs(), 30);
    }

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
