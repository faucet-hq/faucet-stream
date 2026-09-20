//! BigQuery streaming insert sink.

use crate::config::BigQuerySinkConfig;
use crate::idempotent;
use crate::merge;
use async_trait::async_trait;
use faucet_common_bigquery::{BigQueryCredentials, build_client};
use faucet_core::FaucetError;
use faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::job::Job;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::{QueryResponse, ResultSet};
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_response::TableDataInsertAllResponse;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;

/// Max wall-clock spent polling an idempotent-write / token-read job to
/// completion before giving up. Exactly-once pages are small, so this is a
/// generous safety cap, not a steady-state wait.
const IDEMPOTENT_JOB_TIMEOUT: Duration = Duration::from_secs(120);

/// Server-side long-poll window per `getQueryResults` completion check —
/// BigQuery holds the connection open up to this long, so we don't busy-wait.
const JOB_POLL_LONG_POLL_MS: i32 = 10_000;

/// OAuth scope minted for the `media_load` upload endpoint (which is not covered
/// by the `gcp_bigquery_client` client's own authenticator surface).
const BQ_OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

/// Max wall-clock spent polling a media-upload **load** job to completion.
/// A bucket-free page load is a single job; this is a generous safety cap so a
/// wedged job can't hang the run forever.
const LOAD_JOB_TIMEOUT: Duration = Duration::from_secs(600);

/// `true` when a `tables.get` error is a 404 (table does not exist) — used by
/// `current_schema` to report a not-yet-created target as `Ok(None)` rather
/// than a hard error.
fn is_table_not_found(err: &BQError) -> bool {
    matches!(err, BQError::ResponseError { error } if error.error.code == 404)
}

/// Max attempts (including the first) for a transient BigQuery control-plane call.
const BQ_CP_ATTEMPTS: u32 = 4;

/// A transient BigQuery control-plane failure worth retrying, classified
/// **typed-only** (never by matching error-message text — messages are not API
/// and change under dependency bumps): a 429/5xx response status, or a
/// transport-level `reqwest` failure (connect / timeout / mid-body drop).
/// `tables.get` and the DDL/`jobs.query` calls are idempotent, so a blind retry
/// is safe.
fn is_transient_bq_error(err: &BQError) -> bool {
    match err {
        BQError::ResponseError { error } => {
            matches!(error.error.code, 429 | 500 | 502 | 503 | 504)
        }
        BQError::RequestError(e) => match e.status() {
            // A status error that leaked through as a reqwest error.
            Some(s) => s.is_server_error() || s.as_u16() == 429,
            // No status ⇒ transport-level: connection, timeout, send/receive,
            // or a response body cut off mid-decode.
            None => {
                e.is_connect() || e.is_timeout() || e.is_request() || e.is_body() || e.is_decode()
            }
        },
        _ => false,
    }
}

/// Retry a BigQuery control-plane call on [`is_transient_bq_error`], with
/// exponential backoff (500ms → 8s cap). Without this a transient connection
/// blip fails the whole object — or, for the pre-write overwrite DDL, aborts the
/// entire run.
///
/// Deliberately a local runner rather than `faucet_core::retry`: the core runner
/// is typed over `FaucetError`, and these call sites need the **typed
/// `BQError`** after retrying (e.g. `tables.get` 404 → "table absent", a
/// legitimate `Ok(None)` outcome, not a failure).
async fn retry_control_plane<F, Fut, T>(what: &str, mut call: F) -> Result<T, BQError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, BQError>>,
{
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=BQ_CP_ATTEMPTS {
        match call().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < BQ_CP_ATTEMPTS && is_transient_bq_error(&e) => {
                tracing::warn!(
                    what, attempt, error = %e,
                    "BigQuery control-plane transient error; retrying"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(8));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// Rows a completed DML job reported as affected
/// (`statistics.query.numDmlAffectedRows`, which BigQuery sends as a string).
///
/// Reported as `0` when the field is absent or unparseable: the delete has
/// already been verified to have committed by then, so this is a metric, never
/// a correctness signal — an unreadable count must not fail a successful
/// cleanup.
fn dml_affected_rows(job: &Job) -> u64 {
    job.statistics
        .as_ref()
        .and_then(|s| s.query.as_ref())
        .and_then(|q| q.num_dml_affected_rows.as_deref())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Rows a **load job** ingested, from `statistics.load.outputRows` (a string).
/// `0` when absent/unparseable — a metric, never a correctness signal (the load
/// is already verified committed by the caller).
fn load_output_rows(job: &Job) -> u64 {
    job.statistics
        .as_ref()
        .and_then(|s| s.load.as_ref())
        .and_then(|l| l.output_rows.as_deref())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Serialize the cleanup scope — a map of destination column → value — into the
/// single JSON object bound as `@scope`.
fn scope_to_payload(scope: &std::collections::BTreeMap<String, Value>) -> String {
    let obj: serde_json::Map<String, Value> =
        scope.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    Value::Object(obj).to_string()
}

/// Serialize planned delete key tuples into a JSON array of `{key_col: value}`
/// objects for the `@deletes` parameter consumed by the semi-join `DELETE`.
fn deletes_to_payload(deletes: &[faucet_core::KeyTuple]) -> String {
    let arr: Vec<Value> = deletes
        .iter()
        .map(|kt| {
            let mut obj = serde_json::Map::new();
            for (k, v) in &kt.0 {
                obj.insert(k.clone(), v.clone());
            }
            Value::Object(obj)
        })
        .collect();
    Value::Array(arr).to_string()
}

/// Serialize a page of records to newline-delimited JSON (one compact object
/// per line) for a `NEWLINE_DELIMITED_JSON` load job. Each record must be a
/// JSON object; a non-object surfaces as a `FaucetError::Sink`.
fn records_to_ndjson(records: &[Value]) -> Result<String, FaucetError> {
    let mut out = String::new();
    for (i, rec) in records.iter().enumerate() {
        if !rec.is_object() {
            return Err(FaucetError::Sink(format!(
                "BigQuery media load: record {i} is not a JSON object"
            )));
        }
        serde_json::to_string(rec)
            .map_err(|e| FaucetError::Sink(format!("BigQuery media load: serialize record: {e}")))
            .map(|line| {
                out.push_str(&line);
                out.push('\n');
            })?;
    }
    Ok(out)
}

/// Build the BigQuery load-job resource JSON for a `NEWLINE_DELIMITED_JSON`
/// media upload into `project.dataset.table`. `ignoreUnknownValues` mirrors the
/// typed `INSERT … SELECT` path (which projects only the target columns), so a
/// page carrying an extra field never fails the load.
fn build_load_job_json(
    project: &str,
    dataset: &str,
    table: &str,
    write_disposition: &str,
    location: Option<&str>,
) -> Value {
    build_load_job_json_fmt(
        project,
        dataset,
        table,
        write_disposition,
        location,
        "NEWLINE_DELIMITED_JSON",
        None,
        false,
    )
}

/// Generalized load-job JSON: `source_format` selects NDJSON vs CSV, `skip_rows`
/// sets `skipLeadingRows` (CSV headers), and `schema`/`autodetect` control typing.
///
/// **`schema` (an explicit `{fields:[…]}`) takes precedence over `autodetect`.**
/// The native byte-passthrough path (#633) always passes an all-`STRING` schema
/// derived from the payload's own columns, because BigQuery `autodetect` on
/// all-string JSON *infers* DATE/NUMERIC/etc. from the first rows and then a later
/// row that doesn't match that inferred type fails the whole load ("JSON table
/// encountered too many errors"). An explicit all-`STRING` schema matches the
/// `Value` write path exactly (CSV fields are all strings) and never mis-types.
/// `autodetect` is the fallback only when no schema can be derived. Keeps
/// `ignoreUnknownValues` so an extra column never fails a load.
#[allow(clippy::too_many_arguments)]
fn build_load_job_json_fmt(
    project: &str,
    dataset: &str,
    table: &str,
    write_disposition: &str,
    location: Option<&str>,
    source_format: &str,
    skip_rows: Option<u64>,
    autodetect: bool,
) -> Value {
    build_load_job_json_full(
        project,
        dataset,
        table,
        write_disposition,
        location,
        source_format,
        skip_rows,
        None,
        autodetect,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_load_job_json_full(
    project: &str,
    dataset: &str,
    table: &str,
    write_disposition: &str,
    location: Option<&str>,
    source_format: &str,
    skip_rows: Option<u64>,
    schema: Option<Value>,
    autodetect: bool,
) -> Value {
    let mut load = serde_json::json!({
        "destinationTable": {
            "projectId": project,
            "datasetId": dataset,
            "tableId": table,
        },
        "sourceFormat": source_format,
        "writeDisposition": write_disposition,
        "ignoreUnknownValues": true,
    });
    match schema {
        // Explicit schema wins; autodetect must be off or BigQuery ignores the schema.
        Some(s) => {
            load["schema"] = s;
            load["autodetect"] = serde_json::json!(false);
        }
        None => {
            load["autodetect"] = serde_json::json!(autodetect);
        }
    }
    if let Some(n) = skip_rows {
        load["skipLeadingRows"] = serde_json::json!(n);
    }
    let mut job = serde_json::json!({ "configuration": { "load": load } });
    if let Some(loc) = location {
        job["jobReference"] = serde_json::json!({
            "projectId": project,
            "location": loc,
        });
    }
    job
}

/// Build an all-`STRING` BigQuery load schema (`{fields:[{name,type:STRING,mode:NULLABLE}]}`)
/// from a column-name iterator. Used by the native path so the load never relies
/// on `autodetect` — every column is `STRING`, matching the `Value` write path
/// (CSV fields are all strings). Returns `None` for an empty column set.
fn all_string_schema<I: IntoIterator<Item = String>>(columns: I) -> Option<Value> {
    let fields: Vec<Value> = columns
        .into_iter()
        .map(|name| serde_json::json!({"name": name, "type": "STRING", "mode": "NULLABLE"}))
        .collect();
    if fields.is_empty() {
        None
    } else {
        Some(serde_json::json!({ "fields": fields }))
    }
}

/// Convert an `infer_schema`-shaped JSON Schema (`{"type":"object","properties":
/// {"col":{"type":…}}}`) into a BigQuery load-job schema
/// (`{"fields":[{"name","type","mode":"NULLABLE"}]}`). Field types map via the
/// same rules as [`idempotent::bq_column_type`] but rendered with the load-API
/// type keywords. All columns are `NULLABLE` (a page need not carry every field).
/// Returns `None` when no columns can be derived, so the caller falls back to
/// the prior autodetect/inference behaviour.
fn json_schema_to_load_schema(schema: &Value) -> Option<Value> {
    let props = schema.get("properties")?.as_object()?;
    if props.is_empty() {
        return None;
    }
    let mut names: Vec<&String> = props.keys().collect();
    names.sort();
    let fields: Vec<Value> = names
        .iter()
        .map(|n| {
            // `bq_column_type` returns GoogleSQL names (INT64/FLOAT64/BOOL); the
            // load API's `type` field wants the legacy keywords.
            let ty = match crate::idempotent::bq_column_type(&props[*n]) {
                "INT64" => "INTEGER",
                "FLOAT64" => "FLOAT",
                "BOOL" => "BOOLEAN",
                "JSON" => "JSON",
                "TIMESTAMP" => "TIMESTAMP",
                "DATE" => "DATE",
                _ => "STRING",
            };
            serde_json::json!({ "name": n, "type": ty, "mode": "NULLABLE" })
        })
        .collect();
    Some(serde_json::json!({ "fields": fields }))
}

/// Column names in first-appearance order from the first record of a native batch:
/// the keys of the first NDJSON object, or the header cells of the first CSV line.
/// Cheap — reads only up to the first newline. Returns `None` if none can be read.
fn native_batch_columns(
    raw: &[u8],
    format: faucet_core::NativeFormat,
    delimiter: u8,
) -> Option<Vec<String>> {
    let end = raw.iter().position(|&b| b == b'\n').unwrap_or(raw.len());
    let first = &raw[..end];
    if first.is_empty() {
        return None;
    }
    match format {
        faucet_core::NativeFormat::NdJson => {
            let v: Value = serde_json::from_slice(first).ok()?;
            let obj = v.as_object()?;
            Some(obj.keys().cloned().collect())
        }
        faucet_core::NativeFormat::Csv => {
            let line = std::str::from_utf8(first).ok()?.trim_end_matches('\r');
            Some(
                line.split(delimiter as char)
                    .map(|s| s.to_string())
                    .collect(),
            )
        }
        _ => None,
    }
}

/// A `multipart/related` boundary that is guaranteed absent from the payload:
/// derived from a hash of the NDJSON so it is deterministic (testable) yet
/// effectively never a substring of the data.
#[cfg(test)]
fn multipart_boundary(ndjson: &str) -> String {
    media_boundary(ndjson.as_bytes())
}

/// Byte-oriented boundary derivation, shared by the `Value` and native load
/// paths: a hash of the (already gzipped, effectively random) media bytes, so it
/// is deterministic yet never a substring of the payload.
fn media_boundary(media: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    media.hash(&mut h);
    format!("faucetbq{:016x}boundary", h.finish())
}

/// Assemble the `multipart/related` request body: part 1 is the load-job JSON
/// (text), part 2 is the media (gzipped NDJSON — arbitrary bytes). Uses CRLF
/// line endings as the multipart spec requires. The media part is binary, so
/// the body is assembled byte-wise rather than via `format!`.
fn build_multipart_related(boundary: &str, job_json: &str, media: &[u8]) -> Vec<u8> {
    let head = format!(
        "--{boundary}\r\n\
         Content-Type: application/json; charset=UTF-8\r\n\r\n\
         {job_json}\r\n\
         --{boundary}\r\n\
         Content-Type: application/octet-stream\r\n\r\n"
    );
    let tail = format!("\r\n--{boundary}--\r\n");
    let mut body = Vec::with_capacity(head.len() + media.len() + tail.len());
    body.extend_from_slice(head.as_bytes());
    body.extend_from_slice(media);
    body.extend_from_slice(tail.as_bytes());
    body
}

/// Whether an overwrite run loads directly into the target (no staging table).
///
/// True for a **solo** whole-table overwrite via media load: one writer, no
/// scope. A `WRITE_TRUNCATE` load job is atomic on its own, so staging buys
/// nothing there — the first page truncates the target, the rest append. The
/// executor sets `_overwrite_staging` for a *grouped* fan-out (several
/// independent writer instances → one table), where a shared staging table is
/// the only safe swap; a scoped/windowed overwrite (`scope`) likewise needs
/// staging (it replaces only matching rows). The non-media `jobs.query`
/// overwrite path always stages (`media_load` is false there).
fn is_direct_overwrite(config: &BigQuerySinkConfig) -> bool {
    config.media_load && !config.overwrite_staging && config.scope.is_none()
}

/// Whether an **append** page is written by a bulk load job rather than the
/// per-row `tabledata.insertAll` path.
///
/// `insert_id_field` is the carve-out: it is an explicit request for
/// BigQuery's streaming best-effort dedup, which only `insertAll` implements.
/// A load job would accept the rows and silently drop the dedup, so an
/// explicit `insert_id_field` keeps the streaming path even though
/// `media_load` now defaults on (#612).
fn appends_via_media_load(config: &BigQuerySinkConfig) -> bool {
    config.media_load && config.insert_id_field.is_none()
}

/// Gzip-compress bytes for the BigQuery load media part. BigQuery auto-detects
/// gzip for `NEWLINE_DELIMITED_JSON` loads, so no source-format flag is needed —
/// the wire payload just shrinks (often 5–10× for JSON).
fn gzip(data: &[u8]) -> Result<Vec<u8>, FaucetError> {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    let mut enc = GzEncoder::new(
        Vec::with_capacity(data.len() / 4 + 64),
        Compression::default(),
    );
    enc.write_all(data)
        .map_err(|e| FaucetError::Sink(format!("gzip NDJSON media: {e}")))?;
    enc.finish()
        .map_err(|e| FaucetError::Sink(format!("gzip NDJSON media: {e}")))
}

/// Upload one intermediate/final chunk to a resumable session and PUT its bytes.
/// Chunk size the streaming resumable upload flushes at (8 MiB, a multiple of
/// the 256 KiB alignment the resumable protocol requires for non-final chunks).
const RESUMABLE_CHUNK: usize = 8 * 1024 * 1024;
/// Resumable-upload chunk alignment: every non-final `Content-Range` upper bound
/// must be a multiple of 256 KiB.
const RESUMABLE_ALIGN: usize = 256 * 1024;

/// An in-flight BigQuery **resumable upload** session for a single load job fed
/// incrementally across many source pages (so peak memory is O(chunk + one
/// page), not O(table)). NDJSON is streamed into one gzip member; compressed
/// bytes are PUT to the session URI in 256-KiB-aligned chunks as they accumulate
/// past [`RESUMABLE_CHUNK`], and the tail + gzip trailer are PUT on finalize.
struct UploadSession {
    /// The `Location` returned by the resumable-initiate POST — the capability
    /// URI subsequent chunk PUTs address (no re-auth needed).
    session_uri: String,
    /// Bytes already committed to the session (next `Content-Range` start).
    offset: u64,
    /// Incremental gzip encoder; its inner `Vec<u8>` buffers compressed bytes
    /// not yet PUT. `None` after [`finish`](Self::finish) consumes it.
    encoder: Option<flate2::write::GzEncoder<Vec<u8>>>,
    /// HTTP client with redirects disabled (a 308 "Resume Incomplete" must not
    /// be auto-followed).
    http: reqwest::Client,
    /// Set once the terminating chunk has been accepted, so a second
    /// flush/commit is a no-op.
    finalized: bool,
    /// Compressed-buffer size at which a mid-stream chunk is PUT. Production uses
    /// [`RESUMABLE_CHUNK`] (8 MiB); tests lower it via `config.resumable_chunk`
    /// so the multi-chunk path is reachable without an 8 MiB payload.
    chunk_threshold: usize,
}

impl UploadSession {
    /// Feed one page: compress its NDJSON into the gzip stream, then PUT every
    /// fully-accumulated 256-KiB-aligned chunk (keeps the buffer bounded).
    async fn feed(&mut self, records: &[Value]) -> Result<(), FaucetError> {
        let ndjson = records_to_ndjson(records)?;
        self.feed_bytes(ndjson.as_bytes()).await
    }

    /// Feed raw NDJSON bytes: compress into the gzip stream, then PUT every
    /// fully-accumulated 256-KiB-aligned chunk. The shared core of [`feed`](Self::feed)
    /// (which serializes `Value` rows first) and the native byte path (#633).
    async fn feed_bytes(&mut self, ndjson: &[u8]) -> Result<(), FaucetError> {
        use std::io::Write;
        self.encoder
            .as_mut()
            .expect("resumable encoder present before finalize")
            .write_all(ndjson)
            .map_err(|e| FaucetError::Sink(format!("resumable gzip write: {e}")))?;
        // Alignment is capped at the (possibly test-lowered) threshold so a
        // small threshold still makes forward progress; production keeps the
        // 256-KiB protocol alignment (threshold 8 MiB ≫ 256 KiB).
        let align = RESUMABLE_ALIGN.min(self.chunk_threshold).max(1);
        loop {
            let avail = self.encoder.as_ref().unwrap().get_ref().len();
            if avail < self.chunk_threshold {
                break;
            }
            let n = (avail / align) * align;
            let chunk: Vec<u8> = self
                .encoder
                .as_mut()
                .unwrap()
                .get_mut()
                .drain(..n)
                .collect();
            let end = self.offset + chunk.len() as u64 - 1;
            let range = format!("bytes {}-{}/*", self.offset, end);
            let resp = self
                .http
                .put(&self.session_uri)
                .header(reqwest::header::CONTENT_RANGE, range)
                .body(chunk.clone())
                .send()
                .await
                .map_err(|e| FaucetError::Sink(format!("resumable chunk PUT failed: {e}")))?;
            if resp.status().as_u16() != 308 {
                let s = resp.status();
                let t = resp.text().await.unwrap_or_default();
                return Err(FaucetError::Sink(format!(
                    "resumable chunk PUT expected 308 Resume Incomplete, got {s}: {t}"
                )));
            }
            self.offset += chunk.len() as u64;
        }
        Ok(())
    }

    /// Finish the gzip stream, returning the final bytes (remaining buffered
    /// output + trailer) to PUT as the terminating chunk.
    fn finish(&mut self) -> Result<Vec<u8>, FaucetError> {
        self.encoder
            .take()
            .expect("resumable encoder present at finalize")
            .finish()
            .map_err(|e| FaucetError::Sink(format!("resumable gzip finish: {e}")))
    }
}

/// A sink that writes JSON records to a BigQuery table using the streaming
/// insert API (`tabledata.insertAll`).
pub struct BigQuerySink {
    config: BigQuerySinkConfig,
    client: Client,
    /// Target table schema, fetched lazily on the first exactly-once / upsert
    /// call and reused for every page in the run. `None` until first read, and
    /// reset to `None` by [`evolve_schema`](faucet_core::Sink::evolve_schema) so
    /// the next page diffs against the evolved table. Unused on the plain
    /// streaming path.
    schema_cache: RwLock<Option<Vec<idempotent::FieldSpec>>>,
    /// Set once the target table has been confirmed to exist (or been created)
    /// this run, so the existence probe / create runs at most once.
    table_ready: AtomicBool,
    /// Overwrite-only once-gate: has *this* sink instance ensured the target +
    /// staging clone exist? Set on the first overwrite page. It is per-instance
    /// on purpose — the executor runs `begin_overwrite`, the page writes, and
    /// `commit_overwrite` on three *different* sink instances, so the write path
    /// self-heals (creating a missing target from the first page's sample)
    /// rather than trusting a flag another instance set. See
    /// [`insert_overwrite_page`](Self::insert_overwrite_page).
    overwrite_setup: AtomicBool,
    /// In-flight streaming resumable-upload load session (`media_load` append and
    /// solo-direct overwrite). Opened lazily on the first page, fed per page, and
    /// finalized in [`flush`](faucet_core::Sink::flush) — which runs on the same
    /// (writer) sink instance that holds it, so it works even though the executor
    /// drives `begin_overwrite`/`commit_overwrite` on *other* instances. `None`
    /// on every non-streaming path (staging overwrite, `insertAll`, upsert, EO).
    upload_session: tokio::sync::Mutex<Option<UploadSession>>,
    /// Lazily-built GCS client for the Arrow columnar load-job staging upload
    /// (#380). Built once from `config.bulk_load.gcs_auth` on the first
    /// columnar write and reused for every staged file.
    #[cfg(feature = "arrow")]
    gcs_store: tokio::sync::OnceCell<google_cloud_storage::client::Storage>,
}

impl BigQuerySink {
    /// Create a new BigQuery sink from the given configuration.
    ///
    /// This initialises the BigQuery client and authenticates with GCP.
    /// Returns a [`FaucetError::Auth`] if authentication fails.
    pub async fn new(config: BigQuerySinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        config.write.validate()?;
        if config.media_load && config.insert_id_field.is_some() {
            tracing::warn!(
                insert_id_field = config.insert_id_field.as_deref(),
                "insert_id_field is set, so appends use streaming inserts rather than \
                 the default bulk load path (a load job cannot honour insertId dedup)"
            );
        }
        let client = build_client(&config.auth).await?;
        Ok(Self {
            config,
            client,
            schema_cache: RwLock::new(None),
            table_ready: AtomicBool::new(false),
            overwrite_setup: AtomicBool::new(false),
            upload_session: tokio::sync::Mutex::new(None),
            #[cfg(feature = "arrow")]
            gcs_store: tokio::sync::OnceCell::new(),
        })
    }

    /// Construct a sink from a pre-built BigQuery client.
    ///
    /// This is a low-level escape hatch for callers that build their own
    /// [`gcp_bigquery_client::Client`] — for example to target the
    /// [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) via
    /// [`ClientBuilder::with_v2_base_url`](gcp_bigquery_client::client_builder::ClientBuilder::with_v2_base_url),
    /// or to drive a wiremock-backed test fixture. Production code should
    /// prefer [`BigQuerySink::new`], which handles credential loading.
    #[doc(hidden)]
    pub fn from_parts(config: BigQuerySinkConfig, client: Client) -> Self {
        Self {
            config,
            client,
            schema_cache: RwLock::new(None),
            table_ready: AtomicBool::new(false),
            overwrite_setup: AtomicBool::new(false),
            upload_session: tokio::sync::Mutex::new(None),
            #[cfg(feature = "arrow")]
            gcs_store: tokio::sync::OnceCell::new(),
        }
    }

    /// Issue a single `tabledata.insertAll` call and return the raw response.
    ///
    /// Returns `Err` only on transport-level or HTTP-level failures. Per-row
    /// `insertErrors` in the response body are surfaced to the caller as-is;
    /// it is the caller's responsibility to inspect them.
    ///
    /// `skip_invalid_rows` maps to BigQuery's `skipInvalidRows` flag. When
    /// `false` (the all-or-nothing [`write_batch`](Self::write_batch) path) a
    /// single invalid row makes BigQuery commit *nothing* and return per-row
    /// errors. When `true` (the [`write_batch_partial`] DLQ path) BigQuery
    /// commits every valid row and reports `insertErrors` only for the rejected
    /// ones — which is what makes the per-row `Ok`/`Err` mapping in
    /// `write_batch_partial` truthful (without it, the "good" siblings are
    /// reported `Ok` but were never actually committed → silent data loss).
    ///
    /// [`write_batch_partial`]: faucet_core::Sink::write_batch_partial
    async fn insert_chunk_raw(
        &self,
        rows: &[Value],
        skip_invalid_rows: bool,
    ) -> Result<TableDataInsertAllResponse, FaucetError> {
        let mut insert_request = TableDataInsertAllRequest::new();
        if skip_invalid_rows {
            insert_request.skip_invalid_rows();
        }
        for row in rows {
            // When `insert_id_field` is configured, send that field's value as
            // the streaming `insertId` so BigQuery can de-duplicate retries
            // (#78/#31). A row lacking the field is inserted without one.
            let insert_id = self.config.insert_id_field.as_ref().and_then(|field| {
                row.get(field).map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            });
            insert_request.add_row(insert_id, row).map_err(|e| {
                FaucetError::Sink(format!("failed to serialize row for BigQuery: {e}"))
            })?;
        }
        self.client
            .tabledata()
            .insert_all(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                insert_request,
            )
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery insertAll failed: {e}")))
    }

    /// Insert a single chunk of rows in one `tabledata.insertAll` call,
    /// collapsing any per-row errors into a single [`FaucetError::Sink`].
    ///
    /// Used by [`write_batch`](Self::write_batch). Callers that need per-row
    /// error granularity should use
    /// [`write_batch_partial`](faucet_core::Sink::write_batch_partial) instead,
    /// which calls [`insert_chunk_raw`](Self::insert_chunk_raw) directly.
    async fn insert_batch(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // All-or-nothing path: `skipInvalidRows=false` so BigQuery commits the
        // whole chunk or nothing. Any `insertErrors` below becomes an outer
        // `Err`, so the pipeline aborts before the bookmark advances — no
        // partial commit to resume past.
        let response = self.insert_chunk_raw(rows, false).await?;

        // Check for per-row errors.
        if let Some(errors) = response.insert_errors
            && !errors.is_empty()
        {
            let count = errors.len();
            let first = &errors[0];
            return Err(FaucetError::Sink(format!(
                "BigQuery insertAll: {count} row(s) failed; first error on row {:?}: {:?}",
                first.index,
                first
                    .errors
                    .as_ref()
                    .and_then(|errs| errs.first())
                    .map(|e| &e.message),
            )));
        }

        Ok(rows.len())
    }

    // -----------------------------------------------------------------------
    // Exactly-once helpers
    // -----------------------------------------------------------------------

    /// Build a NAMED STRING query parameter.
    fn string_param(name: &str, value: &str) -> QueryParameter {
        QueryParameter {
            name: Some(name.to_string()),
            parameter_type: Some(QueryParameterType {
                r#type: "STRING".to_string(),
                array_type: None,
                struct_types: None,
            }),
            parameter_value: Some(QueryParameterValue {
                value: Some(value.to_string()),
                array_values: None,
                struct_values: None,
            }),
        }
    }

    /// Fetch the target table's schema fields directly via `tables.get`, with no
    /// caching. Returns the raw [`idempotent::FieldSpec`]s (possibly empty for a
    /// schemaless table); a missing table surfaces as the client's `BQError`.
    async fn fetch_schema_fields(&self) -> Result<Vec<idempotent::FieldSpec>, BQError> {
        let table = retry_control_plane("tables.get (schema)", || {
            self.client.table().get(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                None,
            )
        })
        .await?;
        // Table.schema is TableSchema (not Option); TableSchema.fields is Option<Vec<...>>.
        Ok(table
            .schema
            .fields
            .as_ref()
            .map(|fs| {
                fs.iter()
                    .map(idempotent::FieldSpec::from_table_field)
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Fetch (once) and cache the target table's schema as
    /// [`idempotent::FieldSpec`]s, returning an owned clone. Used by the
    /// exactly-once / upsert write paths, which require a table with a defined
    /// schema — a missing table or empty schema is a hard error here.
    ///
    /// The cache is reset by [`evolve_schema`](faucet_core::Sink::evolve_schema)
    /// so a later page re-fetches the evolved schema.
    async fn target_schema(&self) -> Result<Vec<idempotent::FieldSpec>, FaucetError> {
        if let Some(fields) = self.schema_cache.read().await.as_ref() {
            return Ok(fields.clone());
        }
        // Miss: fetch under the write lock so concurrent callers don't each
        // issue a redundant tables.get. Re-check after acquiring in case a
        // racing writer already filled it.
        let mut guard = self.schema_cache.write().await;
        if let Some(fields) = guard.as_ref() {
            return Ok(fields.clone());
        }
        let fields = self
            .fetch_schema_fields()
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery tables.get (schema) failed: {e}")))?;
        if fields.is_empty() {
            return Err(FaucetError::Sink(format!(
                "BigQuery target table {}.{}.{} has no schema fields; exactly-once \
                 delivery requires a table with a defined schema",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            )));
        }
        *guard = Some(fields.clone());
        Ok(fields)
    }

    /// Backtick-quoted fully-qualified `` `project.dataset.table` `` reference.
    fn table_ref(&self) -> String {
        idempotent::table_ref(
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        )
    }

    /// Temp table id used while an overwrite run is in flight.
    fn overwrite_temp_id(&self) -> String {
        format!("{}__faucet_ovw", self.config.table_id)
    }

    /// Backtick-quoted reference to the overwrite staging table.
    fn overwrite_temp_ref(&self) -> String {
        idempotent::table_ref(
            &self.config.project_id,
            &self.config.dataset_id,
            &self.overwrite_temp_id(),
        )
    }

    /// Whether an overwrite run loads directly into the target (no staging).
    /// See [`is_direct_overwrite`] for the rule.
    fn direct_overwrite(&self) -> bool {
        is_direct_overwrite(&self.config)
    }

    /// Load one page into the overwrite staging table via the typed, buffer-free
    /// `INSERT … SELECT FROM UNNEST(JSON_QUERY_ARRAY(@payload))` query path (the
    /// same generator the exactly-once write uses). Streaming `insertAll` is
    /// avoided deliberately: its rows sit in a streaming buffer that the commit
    /// swap's `SELECT` might not see yet.
    async fn insert_overwrite_page(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let first_page = !self.overwrite_setup.swap(true, Ordering::AcqRel);

        // Direct mode (solo whole-table overwrite via media load): load straight
        // into the target — no staging table, no swap, no second data-write. A
        // BigQuery `WRITE_TRUNCATE` load job is atomic on its own (the target's
        // prior data survives a failed load), so a single-load refresh is fully
        // atomic: the first page truncates the target, the rest append. This is
        // the iPaaS `load_table_from_dataframe(WRITE_TRUNCATE)` shape. Grouped
        // fan-outs (several writers → one table) set `_overwrite_staging` and
        // take the staging path below; a scoped/windowed overwrite also stages.
        if self.direct_overwrite() {
            if first_page && !self.target_has_schema().await? {
                if !self.config.create_table {
                    return Err(FaucetError::Sink(format!(
                        "BigQuery overwrite target {}.{}.{} does not exist (or has no \
                         schema) and `create_table` is disabled",
                        self.config.project_id, self.config.dataset_id, self.config.table_id
                    )));
                }
                self.create_target_from_sample(records).await?;
                self.table_ready.store(true, Ordering::Release);
            }
            // Stream the whole overwrite as ONE `WRITE_TRUNCATE` resumable load
            // fed across every page; it is finalized in `flush()` (which runs on
            // this same writer instance — see `finalize_session`). One atomic
            // load replaces the target at end-of-stream, so a mid-run failure
            // leaves the prior data intact, and peak memory is O(chunk + page)
            // rather than O(table). (Overwrite sources are full-refresh with an
            // end-of-run bookmark, so `flush` finalizes exactly once.)
            self.feed_session(&self.config.table_id, "WRITE_TRUNCATE", records)
                .await?;
            return Ok(records.len());
        }

        // Staging mode. Self-heal once, on this instance's first page.
        // `begin_overwrite` may have run on a *different* sink instance (the
        // executor drives it on a throwaway sink for group-level overwrite), so
        // this instance can't trust an in-memory flag — it verifies the real
        // target table instead. A target that already existed had its staging
        // clone created by `begin_overwrite`; a missing/schemaless one is created
        // here from this page's sample (the schema `CREATE … LIKE` needs),
        // followed by its staging clone.
        if first_page && !self.target_has_schema().await? {
            if !self.config.create_table {
                return Err(FaucetError::Sink(format!(
                    "BigQuery overwrite target {}.{}.{} does not exist (or has no \
                     schema) and `create_table` is disabled",
                    self.config.project_id, self.config.dataset_id, self.config.table_id
                )));
            }
            self.create_target_from_sample(records).await?;
            self.table_ready.store(true, Ordering::Release);
            self.run_ddl(format!(
                "CREATE OR REPLACE TABLE {} LIKE {}",
                self.overwrite_temp_ref(),
                self.table_ref()
            ))
            .await?;
        }
        // Fast path: stream the page straight into staging with one bucket-free
        // load job (NDJSON media upload) instead of a per-page `jobs.query`
        // INSERT. The staging table already exists (created above or by
        // `begin_overwrite`); WRITE_APPEND accumulates pages, and the atomic
        // swap still runs in `commit_overwrite`.
        if self.config.media_load {
            self.load_ndjson(&self.overwrite_temp_id(), records, "WRITE_APPEND")
                .await?;
            return Ok(records.len());
        }
        let columns = self.target_schema().await?;
        let payload = serde_json::to_string(records).map_err(|e| {
            FaucetError::Sink(format!("BigQuery overwrite: serialize page payload: {e}"))
        })?;
        let sql = idempotent::build_insert_select(
            &columns,
            &self.config.project_id,
            &self.config.dataset_id,
            &self.overwrite_temp_id(),
        );
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.query_parameters = Some(vec![Self::string_param("payload", &payload)]);
        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("BigQuery overwrite page insert failed: {e}"))
            })?;
        self.await_query_complete(resp).await?;
        Ok(records.len())
    }

    /// Mint an OAuth2 access token for the BigQuery scope from the configured
    /// credentials, reusing `gcp_bigquery_client`'s re-exported `yup-oauth2` (no
    /// direct dependency). Used only by the `media_load` upload path — the
    /// `gcp_bigquery_client::Client` handles auth for every other call itself.
    /// The token value is never logged.
    async fn access_token(&self) -> Result<String, FaucetError> {
        use gcp_bigquery_client::yup_oauth2::{
            self, ApplicationDefaultCredentialsAuthenticator,
            ApplicationDefaultCredentialsFlowOpts, ServiceAccountAuthenticator,
            authenticator::ApplicationDefaultCredentialsTypes,
        };

        let scopes = [BQ_OAUTH_SCOPE];
        let token = match &self.config.auth {
            BigQueryCredentials::ServiceAccountKey { json } => {
                let key = yup_oauth2::parse_service_account_key(json)
                    .map_err(|e| FaucetError::Auth(format!("invalid service account JSON: {e}")))?;
                let auth = ServiceAccountAuthenticator::builder(key)
                    .build()
                    .await
                    .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))?;
                auth.token(&scopes)
                    .await
                    .map_err(|e| FaucetError::Auth(format!("BigQuery token mint failed: {e}")))?
            }
            BigQueryCredentials::ServiceAccountKeyPath { path } => {
                let key = yup_oauth2::read_service_account_key(path)
                    .await
                    .map_err(|e| FaucetError::Auth(format!("read service account key: {e}")))?;
                let auth = ServiceAccountAuthenticator::builder(key)
                    .build()
                    .await
                    .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))?;
                auth.token(&scopes)
                    .await
                    .map_err(|e| FaucetError::Auth(format!("BigQuery token mint failed: {e}")))?
            }
            BigQueryCredentials::ApplicationDefault => {
                let opts = ApplicationDefaultCredentialsFlowOpts::default();
                let auth = match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
                    ApplicationDefaultCredentialsTypes::ServiceAccount(b) => b.build().await,
                    ApplicationDefaultCredentialsTypes::InstanceMetadata(b) => b.build().await,
                }
                .map_err(|e| FaucetError::Auth(format!("BigQuery ADC auth failed: {e}")))?;
                auth.token(&scopes)
                    .await
                    .map_err(|e| FaucetError::Auth(format!("BigQuery token mint failed: {e}")))?
            }
        };
        token
            .token()
            .map(str::to_string)
            .ok_or_else(|| FaucetError::Auth("BigQuery access token had no value".to_string()))
    }

    /// Load one page into `table_id` via a bucket-free BigQuery **load job**:
    /// a single `multipart/related` media upload of newline-delimited JSON to
    /// the upload endpoint, then poll the returned job to completion. This is
    /// the fast path the `media_load` config selects for overwrite — one load
    /// job per page instead of one `jobs.query` `INSERT … SELECT` per page.
    async fn load_ndjson(
        &self,
        table_id: &str,
        records: &[Value],
        write_disposition: &str,
    ) -> Result<(), FaucetError> {
        if records.is_empty() {
            return Ok(());
        }
        let ndjson = records_to_ndjson(records)?;
        let job_json = build_load_job_json(
            &self.config.project_id,
            &self.config.dataset_id,
            table_id,
            write_disposition,
            self.config.location.as_deref(),
        )
        .to_string();
        let media = gzip(ndjson.as_bytes())?;
        self.load_media(&job_json, &media).await.map(|_| ())
    }

    /// Shared media-upload core: POST a `multipart/related` load-job request
    /// (part 1 = the load-job JSON, part 2 = the gzipped media bytes) and poll the
    /// job to completion. Reused by [`load_ndjson`](Self::load_ndjson) (which
    /// serializes `Value` rows) and by [`load_native`](faucet_core::Sink::load_native)
    /// (which forwards the source's raw wire bytes, #633).
    async fn load_media(&self, job_json: &str, media_gzipped: &[u8]) -> Result<u64, FaucetError> {
        let boundary = media_boundary(media_gzipped);
        let body = build_multipart_related(&boundary, job_json, media_gzipped);

        let token = self.access_token().await?;
        let url = format!(
            "{}/upload/bigquery/v2/projects/{}/jobs?uploadType=multipart",
            self.upload_base(),
            self.config.project_id
        );
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .bearer_auth(&token)
            .header(
                reqwest::header::CONTENT_TYPE,
                format!("multipart/related; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery media load upload failed: {e}")))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery media load: read response: {e}")))?;
        if !status.is_success() {
            return Err(FaucetError::Sink(format!(
                "BigQuery media load upload returned HTTP {status}: {text}"
            )));
        }
        let job: Job = serde_json::from_str(&text).map_err(|e| {
            FaucetError::Sink(format!("BigQuery media load: parse job response: {e}"))
        })?;
        let job_ref = job.job_reference.as_ref().ok_or_else(|| {
            FaucetError::Sink("BigQuery media load response missing jobReference".to_string())
        })?;
        let job_id = job_ref.job_id.clone().ok_or_else(|| {
            FaucetError::Sink("BigQuery media load jobReference missing jobId".to_string())
        })?;
        let location = job_ref.location.clone();
        self.await_load_job(&job_id, location.as_deref()).await
    }

    /// Poll a load job by id until it reaches a terminal `DONE` state, then
    /// verify it committed (no `errorResult`) — the same fail-safe check the
    /// query path uses. A non-`DONE` terminal state or a present `errorResult`
    /// is an error, so the overwrite never swaps in a partially-loaded staging
    /// table.
    async fn await_load_job(
        &self,
        job_id: &str,
        location: Option<&str>,
    ) -> Result<u64, FaucetError> {
        let started = std::time::Instant::now();
        loop {
            let job = self
                .client
                .job()
                .get_job(&self.config.project_id, job_id, location)
                .await
                .map_err(|e| FaucetError::Sink(format!("BigQuery load jobs.get failed: {e}")))?;
            let (state, error_result) = {
                let status = job.status.as_ref().ok_or_else(|| {
                    FaucetError::Sink(format!(
                        "BigQuery load job '{job_id}' returned no status; cannot confirm commit"
                    ))
                })?;
                (
                    status.state.clone(),
                    status.error_result.as_ref().map(|e| e.to_string()),
                )
            };
            if state.as_deref() == Some("DONE") {
                if let Some(err) = error_result {
                    return Err(FaucetError::Sink(format!(
                        "BigQuery load job '{job_id}' failed: {err}"
                    )));
                }
                return Ok(load_output_rows(&job));
            }
            if started.elapsed() >= LOAD_JOB_TIMEOUT {
                return Err(FaucetError::Sink(format!(
                    "BigQuery load job '{job_id}' did not complete within {}s",
                    LOAD_JOB_TIMEOUT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Base URL for the media/resumable **upload** endpoint. Fixed Google host in
    /// production; overridable via `config.upload_base_url` so tests can point the
    /// streaming load at a wiremock server.
    fn upload_base(&self) -> &str {
        self.config
            .upload_base_url
            .as_deref()
            .unwrap_or("https://bigquery.googleapis.com")
    }

    /// Initiate a BigQuery **resumable upload** for one load job into `table_id`
    /// with `write_disposition`. Returns the open [`UploadSession`] (its
    /// `Location` header is the session URI). Authenticated once here; the
    /// per-chunk PUTs address the returned URI without re-auth.
    async fn initiate_session(
        &self,
        table_id: &str,
        write_disposition: &str,
        schema: Option<Value>,
    ) -> Result<UploadSession, FaucetError> {
        let job_json = build_load_job_json_full(
            &self.config.project_id,
            &self.config.dataset_id,
            table_id,
            write_disposition,
            self.config.location.as_deref(),
            "NEWLINE_DELIMITED_JSON",
            None,
            schema,
            false,
        )
        .to_string();
        let token = self.access_token().await?;
        let url = format!(
            "{}/upload/bigquery/v2/projects/{}/jobs?uploadType=resumable",
            self.upload_base(),
            self.config.project_id
        );
        // Redirects disabled: a 308 "Resume Incomplete" must not be auto-followed.
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| FaucetError::Sink(format!("resumable HTTP client: {e}")))?;
        let resp = http
            .post(&url)
            .bearer_auth(&token)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json; charset=UTF-8",
            )
            .header("X-Upload-Content-Type", "application/octet-stream")
            .body(job_json)
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("resumable init failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let t = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "resumable init returned HTTP {status}: {t}"
            )));
        }
        let session_uri = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                FaucetError::Sink("resumable init response had no Location header".to_string())
            })?;
        Ok(UploadSession {
            session_uri,
            offset: 0,
            encoder: Some(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            )),
            http,
            finalized: false,
            chunk_threshold: self.config.resumable_chunk.unwrap_or(RESUMABLE_CHUNK),
        })
    }

    /// Feed one page into the streaming resumable session, opening (or
    /// re-opening, after a prior finalize) the session on demand. Empty pages
    /// are a no-op (never open a session for zero rows — an empty source leaves
    /// the destination untouched, matching the non-streaming path).
    async fn feed_session(
        &self,
        table_id: &str,
        write_disposition: &str,
        records: &[Value],
    ) -> Result<(), FaucetError> {
        if records.is_empty() {
            return Ok(());
        }
        let need_open = {
            let guard = self.upload_session.lock().await;
            guard.as_ref().is_none_or(|s| s.finalized)
        };
        if need_open {
            let schema = self
                .config
                .schema
                .as_ref()
                .and_then(json_schema_to_load_schema)
                .or_else(|| {
                    // WRITE_TRUNCATE with autodetect keeps an *existing* table's
                    // schema (BigQuery ignores autodetect on an existing target),
                    // so a renamed/changed column would leave the old schema in
                    // place. An explicit schema forces the replace overwrite means.
                    (write_disposition == "WRITE_TRUNCATE")
                        .then(|| {
                            json_schema_to_load_schema(&faucet_core::schema::infer_schema(records))
                        })
                        .flatten()
                });
            let sess = self
                .initiate_session(table_id, write_disposition, schema)
                .await?;
            let mut guard = self.upload_session.lock().await;
            *guard = Some(sess);
        }
        let mut guard = self.upload_session.lock().await;
        guard
            .as_mut()
            .expect("resumable session opened")
            .feed(records)
            .await
    }

    /// Feed raw NDJSON **bytes** into the streaming resumable session — the native
    /// byte-passthrough path (#633). Identical lifecycle to [`feed_session`](Self::feed_session)
    /// (one load job per object, ~8 MiB bounded memory, finalized in `flush`), but
    /// the bytes come straight from the source (no `Value` round-trip). `schema` is
    /// the all-STRING schema applied when the session opens (first batch); on later
    /// batches it is ignored (the session is already open with its disposition +
    /// schema). Empty input is a no-op.
    async fn feed_session_bytes(
        &self,
        table_id: &str,
        write_disposition: &str,
        schema: Option<Value>,
        ndjson: &[u8],
    ) -> Result<(), FaucetError> {
        if ndjson.is_empty() {
            return Ok(());
        }
        let need_open = {
            let guard = self.upload_session.lock().await;
            guard.as_ref().is_none_or(|s| s.finalized)
        };
        if need_open {
            let sess = self
                .initiate_session(table_id, write_disposition, schema)
                .await?;
            let mut guard = self.upload_session.lock().await;
            *guard = Some(sess);
        }
        let mut guard = self.upload_session.lock().await;
        guard
            .as_mut()
            .expect("resumable session opened")
            .feed_bytes(ndjson)
            .await
    }

    /// Finalize the open resumable session (if any): flush the gzip trailer,
    /// PUT the terminating chunk, then poll the resulting load job to `DONE`.
    /// A no-op when no session is open or it is already finalized, so it is safe
    /// to call from `flush` on every path. Runs on the writer sink instance,
    /// which is the one that holds the session.
    async fn finalize_session(&self) -> Result<(), FaucetError> {
        let (job_id, location) = {
            let mut guard = self.upload_session.lock().await;
            let Some(sess) = guard.as_mut() else {
                return Ok(());
            };
            if sess.finalized {
                return Ok(());
            }
            let remaining = sess.finish()?;
            let total = sess.offset + remaining.len() as u64;
            let range = if remaining.is_empty() {
                format!("bytes */{total}")
            } else {
                format!("bytes {}-{}/{}", sess.offset, total - 1, total)
            };
            let resp = sess
                .http
                .put(&sess.session_uri)
                .header(reqwest::header::CONTENT_RANGE, range)
                .body(remaining)
                .send()
                .await
                .map_err(|e| FaucetError::Sink(format!("resumable finalize PUT failed: {e}")))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| {
                FaucetError::Sink(format!("resumable finalize: read response: {e}"))
            })?;
            if !status.is_success() {
                return Err(FaucetError::Sink(format!(
                    "resumable finalize returned HTTP {status}: {text}"
                )));
            }
            sess.finalized = true;
            let job: Job = serde_json::from_str(&text).map_err(|e| {
                FaucetError::Sink(format!("resumable finalize: parse job response: {e}"))
            })?;
            let job_ref = job.job_reference.as_ref().ok_or_else(|| {
                FaucetError::Sink("resumable finalize response missing jobReference".to_string())
            })?;
            let job_id = job_ref.job_id.clone().ok_or_else(|| {
                FaucetError::Sink("resumable finalize jobReference missing jobId".to_string())
            })?;
            (job_id, job_ref.location.clone())
        };
        self.await_load_job(&job_id, location.as_deref())
            .await
            .map(|_| ())
    }

    /// Best-effort cancel of an un-finalized resumable session (DELETE the
    /// session URI) so an aborted run doesn't leave a dangling upload. BigQuery
    /// also garbage-collects incomplete resumable sessions, so a failure here is
    /// only logged.
    async fn cancel_session(&self) {
        let sess = { self.upload_session.lock().await.take() };
        if let Some(sess) = sess
            && !sess.finalized
        {
            let _ = sess
                .http
                .delete(&sess.session_uri)
                .header(reqwest::header::CONTENT_LENGTH, "0")
                .send()
                .await;
        }
    }

    /// Run one schema-evolution / DDL statement through the same `jobs.query` +
    /// authoritative job-status-verify path the data writes use, mapping any
    /// failure to [`FaucetError::Sink`]. The configured `location` (if any) is
    /// applied so dataset/table creation lands in the intended region.
    async fn run_ddl(&self, sql: String) -> Result<(), FaucetError> {
        let resp = retry_control_plane("jobs.query (DDL)", || {
            let mut req = QueryRequest::new(sql.clone());
            req.use_legacy_sql = false;
            req.location = self.config.location.clone();
            self.client.job().query(&self.config.project_id, req)
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("BigQuery schema-evolution DDL failed: {e}")))?;
        self.await_query_complete(resp).await
    }

    /// Is the target table present **with a usable schema** (≥1 field)? A
    /// `tables.get` 404 → `Ok(false)` (missing); a table with an empty schema
    /// → `Ok(false)` (schemaless, e.g. created by a bare `bq mk` — `CREATE …
    /// LIKE` / typed inserts fail against it, so it must be (re)created); any
    /// other error is surfaced.
    async fn target_has_schema(&self) -> Result<bool, FaucetError> {
        match self.fetch_schema_fields().await {
            Ok(fields) => Ok(!fields.is_empty()),
            Err(e) if is_table_not_found(&e) => Ok(false),
            Err(e) => Err(FaucetError::Sink(format!(
                "BigQuery tables.get (schema probe) failed: {e}"
            ))),
        }
    }

    /// Whether `table_id` exists in the configured project/dataset. A `tables.get`
    /// 404 → `Ok(false)`; any other error is surfaced. Used by
    /// [`commit_overwrite`](faucet_core::Sink::commit_overwrite) to decide whether
    /// there is anything staged to swap — keyed off the real table, not an
    /// in-memory flag, since begin/write/commit run on different sink instances.
    async fn table_exists(&self, table_id: &str) -> Result<bool, FaucetError> {
        // Request the `schema` field (as `fetch_schema_fields` does): the client's
        // `Table` type has a non-optional `schema`, so a narrower field mask would
        // yield a response it can't deserialize.
        match retry_control_plane("tables.get (exists)", || {
            self.client.table().get(
                &self.config.project_id,
                &self.config.dataset_id,
                table_id,
                None,
            )
        })
        .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_table_not_found(&e) => Ok(false),
            Err(e) => Err(FaucetError::Sink(format!(
                "BigQuery tables.get on {table_id} failed: {e}"
            ))),
        }
    }

    /// Best-effort `CREATE SCHEMA IF NOT EXISTS` so `create_table` can target a
    /// dataset that does not exist yet. Idempotent; runs in `config.location`.
    async fn ensure_dataset(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS `{}`.`{}`",
            self.config.project_id.replace('`', ""),
            self.config.dataset_id.replace('`', "")
        );
        self.run_ddl(sql).await
    }

    /// Create the target dataset (if missing) and table, inferring the table's
    /// schema from `sample`. Invalidates the schema cache so the freshly-created
    /// schema is fetched on the next read.
    async fn create_target_from_sample(&self, sample: &[Value]) -> Result<(), FaucetError> {
        // Prefer an explicit configured schema (authoritative column types, e.g.
        // from OData `$metadata`) over inferring from the first page.
        let ddl = match &self.config.schema {
            Some(schema) => idempotent::build_create_table_ddl_from_schema(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                schema,
            ),
            None => idempotent::build_create_table_ddl(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                sample,
            ),
        }
        .ok_or_else(|| {
            FaucetError::Sink(format!(
                "BigQuery create_table: cannot infer a schema for {}.{}.{} from the first page \
                 (no object fields); pre-create the table or ensure records carry fields",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ))
        })?;
        self.ensure_dataset().await?;
        self.run_ddl(ddl).await?;
        *self.schema_cache.write().await = None;
        Ok(())
    }

    /// Ensure the target table exists before a (non-overwrite) write. When
    /// `create_table` is on and the table is missing, create it (and its
    /// dataset) from `sample`; when off, surface a clear error. The probe /
    /// create runs at most once per run.
    async fn ensure_table_ready(&self, sample: &[Value]) -> Result<(), FaucetError> {
        if self.table_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.target_has_schema().await? {
            self.table_ready.store(true, Ordering::Release);
            return Ok(());
        }
        if !self.config.create_table {
            return Err(FaucetError::Sink(format!(
                "BigQuery table {}.{}.{} does not exist (or has no schema) and \
                 `create_table` is disabled",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            )));
        }
        self.create_target_from_sample(sample).await?;
        self.table_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Create the commit-token watermark table if it does not exist.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql =
            idempotent::build_create_commit_table(&self.config.project_id, &self.config.dataset_id);
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery commit-table create failed: {e}")))?;
        self.await_query_complete(resp).await
    }

    /// [`await_query_job`](Self::await_query_job), discarding the job body —
    /// the shape every caller that only needs "did it commit?" wants.
    async fn await_query_complete(&self, initial: QueryResponse) -> Result<(), FaucetError> {
        self.await_query_job(initial).await.map(|_| ())
    }

    /// Wait for a query/script job to finish, then authoritatively verify it
    /// succeeded. Returns the terminal `Job` only once it reached a terminal
    /// state with no `errorResult` — callers that need DML statistics (the
    /// scoped-cleanup delete count) read them off the returned body.
    ///
    /// Why `get_job` rather than the response `errors` field: the client maps
    /// only non-2xx HTTP to `Err`, so a job that fails at *runtime* (a CAST
    /// failure, a NULL into a REQUIRED column, …) comes back as `Ok` with the
    /// failure recorded in the job body. `Job.status.error_result` is the
    /// authoritative terminal-failure signal; the `errors` array can also carry
    /// non-fatal warnings, so it must not be treated as failure on its own.
    async fn await_query_job(&self, initial: QueryResponse) -> Result<Job, FaucetError> {
        let (job_id, location) = Self::job_reference(&initial)?;

        // Phase 1 — wait for completion via server-side long-poll (not a busy wait).
        if !initial.job_complete.unwrap_or(false) {
            let started = std::time::Instant::now();
            loop {
                let params = GetQueryResultsParameters {
                    location: location.clone(),
                    timeout_ms: Some(JOB_POLL_LONG_POLL_MS),
                    max_results: Some(0),
                    ..Default::default()
                };
                let resp = self
                    .client
                    .job()
                    .get_query_results(&self.config.project_id, &job_id, params)
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("BigQuery jobs.getQueryResults failed: {e}"))
                    })?;
                if resp.job_complete.unwrap_or(false) {
                    break;
                }
                if started.elapsed() >= IDEMPOTENT_JOB_TIMEOUT {
                    return Err(FaucetError::Sink(format!(
                        "BigQuery job '{job_id}' did not complete within {}s",
                        IDEMPOTENT_JOB_TIMEOUT.as_secs()
                    )));
                }
                // The server long-poll normally blocks until completion, but if
                // it returns early, back off so a still-running job can't turn
                // this into a tight request-hammering loop.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }

        // Phase 2 — authoritative success check via the job's errorResult.
        //
        // We require an explicit terminal `DONE` state with no `errorResult`.
        // A missing `status`, a non-`DONE` state, or a present `errorResult` all
        // mean we cannot confirm the transaction durably committed — fail safe
        // (returning `Ok` here would advance the bookmark over data that may
        // never have landed, the silent-data-loss failure mode).
        let job = self
            .client
            .job()
            .get_job(&self.config.project_id, &job_id, location.as_deref())
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery jobs.get failed: {e}")))?;
        // Read the two fields we judge on, then drop the borrow so the job body
        // itself can be handed back to the caller.
        let (state, error_result) = {
            let status = job.status.as_ref().ok_or_else(|| {
                FaucetError::Sink(format!(
                    "BigQuery job '{job_id}' returned no status; cannot confirm durable commit"
                ))
            })?;
            (
                status.state.clone(),
                status.error_result.as_ref().map(|e| e.to_string()),
            )
        };
        if let Some(err) = error_result {
            return Err(FaucetError::Sink(format!(
                "BigQuery query job '{job_id}' failed: {err}"
            )));
        }
        match state.as_deref() {
            Some("DONE") => Ok(job),
            other => Err(FaucetError::Sink(format!(
                "BigQuery job '{job_id}' is in state {other:?}, not DONE; cannot confirm durable commit"
            ))),
        }
    }

    /// Extract `(job_id, location)` from a query response's job reference.
    fn job_reference(qr: &QueryResponse) -> Result<(String, Option<String>), FaucetError> {
        let r = qr.job_reference.as_ref().ok_or_else(|| {
            FaucetError::Sink("BigQuery query response missing jobReference".to_string())
        })?;
        let job_id = r
            .job_id
            .clone()
            .ok_or_else(|| FaucetError::Sink("BigQuery jobReference missing jobId".to_string()))?;
        Ok((job_id, r.location.clone()))
    }

    /// Run a planned upsert/delete page as one BigQuery multi-statement
    /// transaction. When `token` is `Some((scope, tok))` the watermark `MERGE`
    /// is appended inside the same transaction (exactly-once + upsert).
    ///
    /// The caller must have already validated `plan.failed` is empty (and, for
    /// the exactly-once path, ensured the commit table exists). Returns the
    /// number of rows applied (upserts + deletes).
    async fn run_upsert_script(
        &self,
        plan: &faucet_core::WritePlan,
        token: Option<(&str, &str)>,
    ) -> Result<usize, FaucetError> {
        let columns = self.target_schema().await?;
        merge::validate_keys_present(&columns, &self.config.write.key)?;

        let has_upserts = !plan.upserts.is_empty();
        let has_deletes = !plan.deletes.is_empty();
        if !has_upserts && !has_deletes {
            // Nothing planned; for the exactly-once path the bookmark still
            // needs its token, so emit the watermark MERGE alone.
            if token.is_none() {
                return Ok(0);
            }
        }

        let key = &self.config.write.key;
        let (project, dataset, table) = (
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        );
        let sql = match token {
            Some(_) => merge::build_upsert_idempotent_sql(
                &columns,
                key,
                has_upserts,
                has_deletes,
                project,
                dataset,
                table,
            ),
            None => merge::build_upsert_transaction_sql(
                &columns,
                key,
                has_upserts,
                has_deletes,
                project,
                dataset,
                table,
            ),
        };

        let mut params = Vec::new();
        if has_upserts {
            let payload = serde_json::to_string(&plan.upserts).map_err(|e| {
                FaucetError::Sink(format!("bigquery upsert: serialize payload: {e}"))
            })?;
            params.push(Self::string_param("payload", &payload));
        }
        if has_deletes {
            let deletes = deletes_to_payload(&plan.deletes);
            params.push(Self::string_param("deletes", &deletes));
        }
        if let Some((scope, tok)) = token {
            params.push(Self::string_param("scope", scope));
            params.push(Self::string_param("token", tok));
        }

        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        // MERGE-by-key is idempotent, so a retried request is harmless; for the
        // exactly-once path a deterministic request_id additionally suppresses
        // duplicate jobs from a retried HTTP request within BigQuery's window.
        if let Some((scope, tok)) = token {
            req.request_id = Some(idempotent::build_request_id(scope, tok));
        }
        req.query_parameters = Some(params);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("bigquery upsert write failed: {e}")))?;
        self.await_query_complete(resp).await?;

        Ok(plan.upserts.len() + plan.deletes.len())
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// One `DELETE` statement, which BigQuery applies atomically — so the
    /// cleanup is all-or-nothing, as it must be: a partial delete would remove
    /// rows the run actually wrote. Both the scope predicate and the written-key
    /// set travel as a single bound JSON STRING parameter each (`@scope` /
    /// `@keys`), the same shape the typed `INSERT … SELECT FROM
    /// UNNEST(JSON_QUERY_ARRAY(@payload))` write path uses; one parameter per
    /// key would exceed BigQuery's parameter limits well before the scope did.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every row in it is stale and must go.
    /// That is the case this feature exists for, and `NOT EXISTS` over an empty
    /// `UNNEST` expresses it directly.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "bigquery cleanup requires a non-empty `key`".to_string(),
            ));
        }
        let columns = self.target_schema().await?;
        let scope_cols: Vec<String> = scope.keys().cloned().collect();
        merge::validate_cleanup_columns(&columns, &scope_cols, key)?;

        let keys_payload = deletes_to_payload(seen.keys());
        merge::check_cleanup_payload_size(keys_payload.len(), seen.len())?;

        let sql = merge::build_cleanup_delete(
            &columns,
            &scope_cols,
            key,
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        );
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.query_parameters = Some(vec![
            Self::string_param("scope", &scope_to_payload(scope)),
            Self::string_param("keys", &keys_payload),
        ]);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("bigquery cleanup delete failed: {e}")))?;
        let job = self.await_query_job(resp).await?;
        let deleted = dml_affected_rows(&job);

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ),
            deleted,
            written_keys = seen.len(),
            "BigQuery scoped cleanup complete"
        );
        Ok(deleted)
    }
}

#[async_trait]
impl faucet_core::Sink for BigQuerySink {
    fn connector_name(&self) -> &'static str {
        "bigquery"
    }

    /// BigQuery bulk-loads via a GCS-staged Parquet load job under `bulk_load`
    /// (#528). Advertise the `staging` capability.
    fn supports_staged_load(&self) -> bool {
        true
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(BigQuerySinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "bigquery://{}.{}.{}",
            self.config.project_id, self.config.dataset_id, self.config.table_id
        )
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Runs a single read-only `tables.get` against the configured
    /// `project_id.dataset_id.table_id` using the already-authenticated
    /// client built in [`BigQuerySink::new`]. This mints/uses the access
    /// token and confirms the credentials can read the target table's
    /// metadata — without inserting any rows. Auth, missing-dataset,
    /// missing-table, and permission errors all surface as a `Fail` probe
    /// with a remediation hint. The access token is never included in the
    /// reason or hint.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let fqn = format!(
            "{}.{}.{}",
            self.config.project_id, self.config.dataset_id, self.config.table_id
        );

        let result = tokio::time::timeout(
            ctx.timeout,
            self.client.table().get(
                &self.config.project_id,
                &self.config.dataset_id,
                &self.config.table_id,
                Some(vec!["tableReference"]),
            ),
        )
        .await;

        let probe = match result {
            Ok(Ok(_table)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("BigQuery tables.get on {fqn} failed: {e}"),
                "Verify the service account has roles/bigquery.dataViewer (or \
                 read access) on the dataset and that the project, dataset, and \
                 table IDs are correct.",
            ),
            Err(_elapsed) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!(
                    "BigQuery tables.get on {fqn} timed out after {:?}",
                    ctx.timeout
                ),
                "Check network reachability to bigquery.googleapis.com and that \
                 credentials can be minted within the timeout.",
            ),
        };

        Ok(CheckReport::single(probe))
    }

    /// Write records to BigQuery.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate `tabledata.insertAll` call. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `insertAll` request — useful when upstream `StreamPage`s are already
    /// sized for BigQuery's per-request limits.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            self.ensure_table_ready(records).await?;
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "bigquery {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.run_upsert_script(&plan, None).await;
        }

        // Overwrite: load the page into the staging table via the buffer-free
        // query path (not streaming `insertAll`); the atomic swap runs in
        // `commit_overwrite`. Table/staging creation is handled by
        // `begin_overwrite` + the deferred create in `insert_overwrite_page`.
        if self.config.write.is_overwrite() {
            return self.insert_overwrite_page(records).await;
        }

        self.ensure_table_ready(records).await?;

        // Append via a bucket-free load job when `media_load` is on: stream the
        // page into one `WRITE_APPEND` resumable load job (finalized in `flush`)
        // instead of the streaming `insertAll` chunk loop — no streaming buffer,
        // no per-row insertAll quota, gzip-compressed, and peak memory O(chunk +
        // page) regardless of table size.
        if appends_via_media_load(&self.config) {
            self.feed_session(&self.config.table_id, "WRITE_APPEND", records)
                .await?;
            return Ok(records.len());
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // insertAll call. Subject to BigQuery's ~10MB request limit.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            total += self.insert_batch(chunk).await?;
        }

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ),
            rows = total,
            "BigQuery write complete"
        );
        Ok(total)
    }

    /// Write records to BigQuery, returning a per-row outcome vector.
    ///
    /// Unlike [`write_batch`](faucet_core::Sink::write_batch), which collapses all
    /// `insertErrors` into a single `FaucetError`, this method maps each row
    /// to `Ok(())` if BigQuery accepted it or `Err(FaucetError::Sink(...))` if
    /// BigQuery reported a per-row error for it. This allows the pipeline's DLQ
    /// router to quarantine only the rows that BigQuery actually rejected while
    /// keeping already-committed siblings out of the dead-letter queue.
    ///
    /// Transport-level or HTTP-level failures (e.g. network errors, 4xx/5xx
    /// responses) are still returned as an outer `Err` because no rows can be
    /// considered committed in that case.
    ///
    /// Chunking follows the same `batch_size` semantics as `write_batch`:
    /// `batch_size == 0` sends the entire slice in one call; `batch_size > 0`
    /// splits the slice into chunks and concatenates the per-row outcomes in
    /// input order.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        use std::collections::HashMap;

        if records.is_empty() {
            return Ok(Vec::new());
        }

        if self.config.write.is_overwrite() {
            // Overwrite is insert-shaped with no per-row key failures.
            self.insert_overwrite_page(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            self.ensure_table_ready(records).await?;
            let plan = faucet_core::plan_writes(records, &self.config.write);
            self.run_upsert_script(&plan, None).await?;
            let mut outcomes: Vec<faucet_core::RowOutcome> =
                records.iter().map(|_| Ok(())).collect();
            for (idx, msg) in &plan.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "bigquery {}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return Ok(outcomes);
        }

        self.ensure_table_ready(records).await?;

        // NOTE: no `media_load` branch here, deliberately. This method is only
        // called when a **DLQ is configured**, and a BigQuery load job is
        // all-or-nothing: it cannot say *which* rows were rejected. Taking it
        // here would silently convert "three bad rows quarantined, the rest
        // written" into "the whole page failed" (or, worse, "all rows reported
        // OK") — the operator asked for row isolation by configuring a DLQ, so
        // the per-row `insertAll` path is what they get, whatever `media_load`
        // says. Runs without a DLQ go through `write_batch`, which does take
        // the load path (#612).

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());

        for chunk in chunks {
            // `skipInvalidRows=true`: BigQuery commits every valid row and
            // returns `insertErrors` only for the rejected ones. This is what
            // makes mapping the flagged indices to `Err` and the rest to
            // `Ok(())` correct — the unflagged rows really were committed, so
            // the DLQ router quarantines only the bad rows and the bookmark
            // advances over genuinely-persisted data.
            let response = self.insert_chunk_raw(chunk, true).await?;

            // Build a set of failed row indices → first error message.
            let failed: HashMap<usize, String> = response
                .insert_errors
                .unwrap_or_default()
                .into_iter()
                .filter_map(|e| {
                    let idx = e.index? as usize;
                    let msg = e
                        .errors
                        .as_ref()
                        .and_then(|v| v.first())
                        .map(|er| er.message.clone().unwrap_or_default())
                        .unwrap_or_default();
                    Some((idx, msg))
                })
                .collect();

            for i in 0..chunk.len() {
                match failed.get(&i) {
                    Some(msg) => outcomes.push(Err(FaucetError::Sink(format!(
                        "BigQuery row rejected: {msg}"
                    )))),
                    None => outcomes.push(Ok(())),
                }
            }
        }

        Ok(outcomes)
    }

    /// Finalize a streaming `media_load` resumable session, if one is open
    /// (append or solo-direct overwrite). Runs on the writer sink instance that
    /// holds the session — the reliable place to finalize, since the executor
    /// drives `begin_overwrite`/`commit_overwrite` on *other* instances. For an
    /// overwrite this commits the single `WRITE_TRUNCATE` load atomically at
    /// end-of-stream (so `commit_overwrite` is a no-op); a no-op on every
    /// non-streaming path.
    async fn flush(&self) -> Result<(), FaucetError> {
        self.finalize_session().await
    }

    /// Native byte-passthrough load (#633): BigQuery can bulk-load NDJSON or CSV
    /// bytes directly via a load job, so a source that emits either format
    /// streams straight in without ever building `Value` rows. The pipeline's
    /// own gates already exclude transforms/governance/DLQ/exactly-once and
    /// upsert/delete write modes (defense in depth: the upsert check below
    /// stays so this sink never advertises a path it can't honor even if
    /// called outside the pipeline).
    fn native_load_capabilities(&self) -> Vec<faucet_core::NativeLoadCapability> {
        // Upsert/delete need per-row keys, so they are not passthrough-eligible.
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            return Vec::new();
        }
        vec![
            // NDJSON feeds one resumable session finalized only by the terminal
            // `flush`, so it satisfies the all-or-nothing overwrite contract:
            // nothing is visible until the single WRITE_TRUNCATE load commits.
            faucet_core::NativeLoadCapability {
                format: faucet_core::NativeFormat::NdJson,
                mechanism: "bigquery-load-job",
                write_modes: &[
                    faucet_core::WriteMode::Append,
                    faucet_core::WriteMode::Overwrite,
                ],
            },
            // CSV runs a discrete load job per batch, each committing on its
            // own — a multi-batch overwrite would be truncate + partial appends
            // rather than one atomic replace, so CSV is append-only here.
            // (CSV + overwrite falls through to the Value path's staging swap.)
            faucet_core::NativeLoadCapability {
                format: faucet_core::NativeFormat::Csv,
                mechanism: "bigquery-load-job",
                write_modes: &[faucet_core::WriteMode::Append],
            },
        ]
    }

    async fn load_native(
        &self,
        batch: faucet_core::NativeBatch,
        scope: &str,
        ctx: faucet_core::NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        let _ = scope; // v1: no per-scope isolation on the native load path.
        // Overwrite truncates on the first batch (which opens the session), appends
        // thereafter. Because every page of an object feeds ONE resumable session
        // finalized in `flush`, this is one atomic WRITE_TRUNCATE load per object —
        // not a truncate-then-append across pages.
        let write_disposition =
            if ctx.write_mode == faucet_core::WriteMode::Overwrite && ctx.first_batch {
                "WRITE_TRUNCATE"
            } else {
                "WRITE_APPEND"
            };
        let table = self.config.table_id.clone();
        let csv = batch.csv;

        match (batch.format, batch.payload) {
            // NDJSON **streaming** — the memory-optimal path (#633). Feed each
            // ~256 KiB chunk into the per-object resumable session as it arrives;
            // the full page is never buffered on either side. Schema is derived
            // from the first chunk; the row count is the NDJSON line count.
            (faucet_core::NativeFormat::NdJson, faucet_core::NativePayload::Stream(mut s)) => {
                use futures::StreamExt;
                let mut rows = 0usize;
                let mut first = true;
                while let Some(chunk) = s.next().await {
                    let chunk = chunk?;
                    if chunk.is_empty() {
                        continue;
                    }
                    // The explicit all-STRING schema (from the first chunk) opens the
                    // session; later chunks pass `None` (session already open).
                    let schema = if first {
                        native_batch_columns(&chunk, faucet_core::NativeFormat::NdJson, b',')
                            .and_then(all_string_schema)
                    } else {
                        None
                    };
                    rows += chunk.iter().filter(|&&b| b == b'\n').count();
                    self.feed_session_bytes(&table, write_disposition, schema, &chunk)
                        .await?;
                    first = false;
                }
                Ok(rows)
            }
            // NDJSON already buffered — feed the whole batch into the same resumable
            // session (used by tests / any source that emits `Bytes`).
            (faucet_core::NativeFormat::NdJson, faucet_core::NativePayload::Bytes(raw)) => {
                if raw.is_empty() {
                    return Ok(0);
                }
                let rows = batch
                    .records
                    .map(|r| r as usize)
                    .unwrap_or_else(|| raw.iter().filter(|&&b| b == b'\n').count());
                let schema = native_batch_columns(&raw, faucet_core::NativeFormat::NdJson, b',')
                    .and_then(all_string_schema);
                self.feed_session_bytes(&table, write_disposition, schema, &raw)
                    .await?;
                Ok(rows)
            }
            // CSV isn't NDJSON-shaped for the resumable session, so it takes a
            // discrete CSV load job per batch (no built-in source emits this today).
            (faucet_core::NativeFormat::Csv, faucet_core::NativePayload::Bytes(raw)) => {
                if raw.is_empty() {
                    return Ok(0);
                }
                let rows = batch.records.unwrap_or(0) as usize;
                let skip_rows = if csv.has_header { Some(1) } else { None };
                let schema =
                    native_batch_columns(&raw, faucet_core::NativeFormat::Csv, csv.delimiter)
                        .and_then(all_string_schema);
                let autodetect_fallback = schema.is_none();
                let media = gzip(&raw)?;
                let job_json = build_load_job_json_full(
                    &self.config.project_id,
                    &self.config.dataset_id,
                    &table,
                    write_disposition,
                    self.config.location.as_deref(),
                    "CSV",
                    skip_rows,
                    schema,
                    autodetect_fallback,
                )
                .to_string();
                self.load_media(&job_json, &media).await?;
                Ok(rows)
            }
            (other, _) => Err(FaucetError::Sink(format!(
                "bigquery load_native: unsupported format {other:?} for this payload"
            ))),
        }
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Create the staging table as an empty structural clone of the target
    /// (`CREATE OR REPLACE TABLE temp LIKE target`, which also copies
    /// partitioning/clustering), dropping any leftover staging from a crashed
    /// run. Bucket-free: no GCS staging is involved. The target must already
    /// exist (LIKE requires it) — overwrite replaces its rows, not its schema.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        // Direct mode: no staging table to prepare. The first write truncates the
        // target (or creates it from the sample), so all begin has to do is make
        // sure the dataset exists when we're allowed to create it.
        if self.direct_overwrite() {
            if self.config.create_table {
                self.ensure_dataset().await?;
            }
            return Ok(());
        }
        if self.target_has_schema().await? {
            self.table_ready.store(true, Ordering::Release);
            return self
                .run_ddl(format!(
                    "CREATE OR REPLACE TABLE {} LIKE {}",
                    self.overwrite_temp_ref(),
                    self.table_ref()
                ))
                .await;
        }
        if self.config.create_table {
            // Nothing usable to overwrite yet — the target is missing or
            // schemaless. Ensure the dataset exists; the target and staging clone
            // are created by the first page's write (which supplies the schema
            // `CREATE … LIKE` needs), on whichever sink instance handles it — see
            // `insert_overwrite_page`'s self-heal. No in-memory flag is set here
            // because begin and write run on different instances.
            self.ensure_dataset().await?;
            return Ok(());
        }
        Err(FaucetError::Sink(format!(
            "BigQuery overwrite target {}.{}.{} does not exist (or has no schema) and \
             `create_table` is disabled",
            self.config.project_id, self.config.dataset_id, self.config.table_id
        )))
    }

    /// Atomically replace the destination in one BigQuery multi-statement
    /// transaction — `TRUNCATE TABLE target; INSERT INTO target SELECT * FROM
    /// temp;` — so a failure rolls back and the prior rows survive.
    /// `TRUNCATE`+`INSERT` (rather than `CREATE OR REPLACE … AS SELECT`)
    /// preserves the target's own partitioning, clustering, and description. The
    /// staging table is dropped afterwards. Staging was loaded via the query
    /// path, so there is no streaming buffer to miss.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        // Direct mode wrote straight into the target (WRITE_TRUNCATE first page +
        // WRITE_APPEND rest); a completed load is already durable, so there is
        // nothing to swap or drop.
        if self.direct_overwrite() {
            return Ok(());
        }
        // Key off the real staging table, not an in-memory flag: begin, write,
        // and commit may each run on a different sink instance (the executor uses
        // throwaway sinks), so only the BQ objects are reliable cross-instance.
        // No staging table means no page was ever written (empty source) — for a
        // previously-missing target there is nothing to create or swap, so leave
        // the destination exactly as it was rather than failing the run.
        if !self.table_exists(&self.overwrite_temp_id()).await? {
            tracing::warn!(
                table = %format!(
                    "{}.{}.{}",
                    self.config.project_id, self.config.dataset_id, self.config.table_id
                ),
                "BigQuery overwrite: no staging table (source produced no rows); \
                 destination left unchanged"
            );
            return Ok(());
        }
        let temp = self.overwrite_temp_ref();
        let sql = match &self.config.scope {
            Some(scope) => {
                // Backtick-quote the scoped column (strip any backticks).
                let col = format!("`{}`", scope.column().replace('`', ""));
                idempotent::build_scoped_overwrite_commit_sql(
                    &self.table_ref(),
                    &temp,
                    &scope.render_where_literal(&col),
                )
            }
            None => idempotent::build_overwrite_commit_sql(&self.table_ref(), &temp),
        };
        self.run_ddl(sql).await?;
        self.run_ddl(format!("DROP TABLE IF EXISTS {temp}")).await
    }

    /// Drop the staging table so a failed/cancelled overwrite leaves nothing
    /// behind. Best-effort — the destination was never touched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        // Direct mode has no staging table to drop. A solo direct overwrite
        // can't roll back a completed WRITE_TRUNCATE — a single-page refresh is
        // atomic (the truncate+load is one job), but a multi-page failure may
        // leave a partially-loaded target. Nothing to clean up here.
        if self.direct_overwrite() {
            // Cancel an un-finalized resumable session so a failed run leaves no
            // dangling upload (BigQuery also GCs incomplete sessions). No staging
            // table exists in direct mode.
            self.cancel_session().await;
            tracing::debug!(
                "BigQuery direct overwrite abort: cancelled resumable session (if any)"
            );
            return Ok(());
        }
        self.run_ddl(format!(
            "DROP TABLE IF EXISTS {}",
            self.overwrite_temp_ref()
        ))
        .await
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    /// Scoped cleanup is always available: a BigQuery table's scope and key
    /// predicates address real columns, and the exactly-once/upsert paths
    /// already require the target table to carry a defined schema.
    fn supports_cleanup(&self) -> bool {
        true
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Read the last durably-committed token for `scope` from the watermark
    /// table, so the pipeline can skip already-committed pages on resume.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let mut req = QueryRequest::new(idempotent::build_select_token(
            &self.config.project_id,
            &self.config.dataset_id,
        ));
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.query_parameters = Some(vec![Self::string_param("scope", scope)]);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery token read failed: {e}")))?;

        // The watermark is a single tiny row, so `jobs.query` returns it inline.
        // If BigQuery did not complete the read synchronously, fail safe: a
        // wrong `None` here would re-run an already-committed page and produce
        // duplicates, defeating exactly-once.
        if !resp.job_complete.unwrap_or(false) {
            return Err(FaucetError::Sink(
                "BigQuery watermark read did not complete synchronously".to_string(),
            ));
        }
        // `ResultSet` only yields rows when the response carries a schema; a
        // completed `SELECT` always returns one. If it is somehow absent we
        // cannot tell "no committed token" from "row present but unreadable",
        // and a wrong `None` would replay committed pages — fail safe instead.
        if resp.schema.is_none() {
            return Err(FaucetError::Sink(
                "BigQuery watermark read returned no schema; cannot trust the token result"
                    .to_string(),
            ));
        }

        let mut rs = ResultSet::new_from_query_response(resp);
        if rs.next_row() {
            rs.get_string_by_name(COMMIT_TOKEN_TOKEN_COL)
                .map_err(|e| FaucetError::Sink(format!("BigQuery token decode failed: {e}")))
        } else {
            Ok(None)
        }
    }

    /// Atomically write `records` and record `token` for `scope` in one BigQuery
    /// multi-statement transaction: a typed `INSERT … SELECT FROM
    /// UNNEST(JSON_QUERY_ARRAY(@payload))` plus a watermark `MERGE`. Either both
    /// the rows and the token commit, or neither does — so a crash/resume skips
    /// the already-committed page (zero duplicates) and a failed page replays
    /// cleanly.
    ///
    /// The entire page is one atomic unit (no `batch_size` re-chunking — core
    /// issues exactly one token per page), so the page must serialize within
    /// BigQuery's ~10 MB `jobs.query` request limit.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;
        self.ensure_table_ready(records).await?;

        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "bigquery {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.run_upsert_script(&plan, Some((scope, token))).await;
        }

        let columns = self.target_schema().await?;

        let payload = serde_json::to_string(records).map_err(|e| {
            FaucetError::Sink(format!(
                "BigQuery exactly-once: serialize page payload: {e}"
            ))
        })?;

        let sql = idempotent::build_transaction_sql(
            &columns,
            &self.config.project_id,
            &self.config.dataset_id,
            &self.config.table_id,
        );
        let mut req = QueryRequest::new(sql);
        req.use_legacy_sql = false;
        req.parameter_mode = Some("NAMED".to_string());
        req.request_id = Some(idempotent::build_request_id(scope, token));
        req.query_parameters = Some(vec![
            Self::string_param("payload", &payload),
            Self::string_param("scope", scope),
            Self::string_param("token", token),
        ]);

        let resp = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Sink(format!("BigQuery idempotent write failed: {e}")))?;
        self.await_query_complete(resp).await?;

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.project_id, self.config.dataset_id, self.config.table_id
            ),
            rows = records.len(),
            token = %token,
            "BigQuery exactly-once page committed"
        );
        Ok(records.len())
    }

    // -----------------------------------------------------------------------
    // Schema drift (issue #194)
    // -----------------------------------------------------------------------

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the live destination schema via a schema-only `tables.get`, mapped
    /// to an `infer_schema`-shaped object so the drift policy can diff a page
    /// against the real table.
    ///
    /// Returns `Ok(None)` when the target table does not exist yet (404) or
    /// carries no field definitions — both mean "no schema to diff against",
    /// so the drift pass treats every page column as new.
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        match self.fetch_schema_fields().await {
            Ok(fields) if fields.is_empty() => Ok(None),
            Ok(fields) => Ok(Some(idempotent::fieldspecs_to_json_schema(&fields))),
            Err(e) if is_table_not_found(&e) => Ok(None),
            Err(e) => Err(FaucetError::Sink(format!(
                "BigQuery current_schema (tables.get) failed: {e}"
            ))),
        }
    }

    /// Apply an additive schema evolution to the target table via `ALTER TABLE`
    /// DDL (issue #194):
    ///
    /// - additions → `ADD COLUMN IF NOT EXISTS <col> <type>`
    /// - widenings → `ALTER COLUMN <col> SET DATA TYPE <type>`
    /// - nullability relaxations → `ALTER COLUMN <col> DROP NOT NULL`
    ///
    /// Each statement runs as its own `jobs.query` job, verified to completion
    /// via the authoritative job-status check. Every statement is idempotent so
    /// concurrent runs converge. The cached schema is invalidated afterwards so
    /// the next page re-fetches the evolved table.
    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let table_ref = self.table_ref();

        for c in &evolution.additions {
            let bq = idempotent::base_to_bq(
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text),
            );
            self.run_ddl(idempotent::build_add_column_ddl(&table_ref, &c.name, bq))
                .await?;
        }
        for c in &evolution.widenings {
            let bq = idempotent::base_to_bq(
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text),
            );
            self.run_ddl(idempotent::build_alter_type_ddl(&table_ref, &c.name, bq))
                .await?;
        }
        for col in &evolution.relax_nullability {
            self.run_ddl(idempotent::build_drop_not_null_ddl(&table_ref, col))
                .await?;
        }

        // Invalidate the cached schema so the next exactly-once / upsert page
        // (and the next drift diff) reads the evolved table.
        *self.schema_cache.write().await = None;
        Ok(())
    }

    /// Columnar load-job is available only when a `bulk_load` staging config is
    /// set **and** the write mode is `append` (#380). Load jobs are
    /// append/truncate only; upsert/delete stay on the `Value` MERGE path, so
    /// the pipeline never negotiates the columnar loop for them.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config.bulk_load.is_some()
            && self.config.write.write_mode == faucet_core::WriteMode::Append
    }

    /// Write one Arrow `RecordBatch` by encoding it to Parquet, staging it on
    /// GCS, and running a BigQuery `PARQUET` load job to completion. Append-only.
    /// The body lives in `load.rs` (pure cloud I/O — a GCS-SDK staging upload +
    /// live load job — that can't run in CI, so `codecov.yml` excludes that file
    /// exactly as it does the GCS connectors).
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        crate::load::write_columnar(&self.client, &self.config, &self.gcs_store, batch).await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BigQueryCredentials, BigQuerySinkConfig, Job, all_string_schema,
        appends_via_media_load, build_load_job_json,
        build_load_job_json_fmt, build_load_job_json_full, build_multipart_related,
        deletes_to_payload, dml_affected_rows, gzip, is_direct_overwrite,
        json_schema_to_load_schema, media_boundary, multipart_boundary, native_batch_columns,
        records_to_ndjson, scope_to_payload,
    };
    use faucet_core::{FaucetError, KeyTuple};
    use serde_json::json;

    #[test]
    fn json_schema_to_load_schema_maps_types_and_nullability() {
        // `infer_schema`-shaped input, including a nullable `[T,null]` fragment.
        let schema = json!({
            "type": "object",
            "properties": {
                "rec_id": { "type": "integer" },
                "amount": { "type": "number" },
                "posted": { "type": ["boolean", "null"] },
                "name":   { "type": "string" },
                "blob":   { "type": "object" },
            }
        });
        let out = json_schema_to_load_schema(&schema).unwrap();
        let fields = out["fields"].as_array().unwrap();
        // Sorted by name for determinism.
        let by: std::collections::HashMap<&str, &str> = fields
            .iter()
            .map(|f| (f["name"].as_str().unwrap(), f["type"].as_str().unwrap()))
            .collect();
        assert_eq!(by["rec_id"], "INTEGER");
        assert_eq!(by["amount"], "FLOAT");
        assert_eq!(by["posted"], "BOOLEAN"); // non-null token picked from [T,null]
        assert_eq!(by["name"], "STRING");
        assert_eq!(by["blob"], "JSON");
        assert!(fields.iter().all(|f| f["mode"] == "NULLABLE"));
    }

    #[test]
    fn json_schema_to_load_schema_none_when_no_columns() {
        assert!(
            json_schema_to_load_schema(&json!({ "type": "object", "properties": {} })).is_none()
        );
        assert!(json_schema_to_load_schema(&json!({ "type": "object" })).is_none());
    }

    // dataset_uri test is skipped: BigQuerySink::new() requires GCP credentials
    // (build_client fetches auth in new()), and from_parts() requires a
    // gcp_bigquery_client::Client which cannot be constructed without auth.

    #[test]
    fn deletes_to_payload_preserves_number_type() {
        // The delete payload must keep an integer key as a JSON number (not the
        // string "2"), so the matching `CAST(JSON_VALUE(d, '$.id') AS INT64)`
        // semi-join compares like-for-like.
        let p = deletes_to_payload(&[KeyTuple(vec![("id".into(), json!(2))])]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"id": 2}]));
        assert!(v[0]["id"].is_number(), "id must serialize as a number: {p}");
    }

    #[test]
    fn deletes_to_payload_composite_key_roundtrips() {
        let p = deletes_to_payload(&[KeyTuple(vec![
            ("tenant".into(), json!("acme")),
            ("id".into(), json!(7)),
        ])]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"tenant": "acme", "id": 7}]));
    }

    // --- scoped cleanup (issue #478) ---

    #[test]
    fn scope_to_payload_is_one_object_with_typed_values() {
        let scope = std::collections::BTreeMap::from([
            ("contact_id".to_string(), json!(42)),
            ("region".to_string(), json!("eu")),
        ]);
        let v: serde_json::Value = serde_json::from_str(&scope_to_payload(&scope)).expect("JSON");
        assert_eq!(v, json!({"contact_id": 42, "region": "eu"}));
        // An integer scope value must stay a JSON number so the matching
        // `CAST(JSON_VALUE(@scope, '$.contact_id') AS INT64)` compares like-for-like.
        assert!(v["contact_id"].is_number());
    }

    #[test]
    fn seen_keys_serialize_through_the_same_payload_shape() {
        // An empty written-key set is meaningful (the source reported the scope
        // empty), and must serialize to `[]` so `NOT EXISTS` deletes the scope.
        assert_eq!(deletes_to_payload(&[]), "[]");
    }

    #[test]
    fn dml_affected_rows_reads_the_job_statistics() {
        use gcp_bigquery_client::model::job_statistics::JobStatistics;
        use gcp_bigquery_client::model::job_statistics2::JobStatistics2;

        let job = |n: Option<&str>| Job {
            statistics: Some(JobStatistics {
                query: Some(JobStatistics2 {
                    num_dml_affected_rows: n.map(str::to_string),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(dml_affected_rows(&job(Some("7"))), 7);
        assert_eq!(dml_affected_rows(&job(Some("0"))), 0);
        // Absent / unparseable stats report 0 rather than failing: by this point
        // the delete has already been verified to have committed.
        assert_eq!(dml_affected_rows(&job(None)), 0);
        assert_eq!(dml_affected_rows(&job(Some("not-a-number"))), 0);
        assert_eq!(dml_affected_rows(&Job::default()), 0);
    }

    // --- media-upload load job (`media_load`) ---

    #[test]
    fn records_to_ndjson_emits_one_compact_object_per_line() {
        let ndjson = records_to_ndjson(&[json!({"a": 1}), json!({"b": "x"})]).expect("ndjson");
        assert_eq!(ndjson, "{\"a\":1}\n{\"b\":\"x\"}\n");
    }

    #[test]
    fn records_to_ndjson_rejects_non_object_record() {
        let err = records_to_ndjson(&[json!({"a": 1}), json!(5)]).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(m) if m.contains("record 1")));
    }

    #[test]
    fn records_to_ndjson_empty_is_empty_string() {
        assert_eq!(records_to_ndjson(&[]).expect("ndjson"), "");
    }

    #[test]
    fn build_load_job_json_shape_ndjson_append() {
        let job = build_load_job_json("proj", "ds", "tbl", "WRITE_APPEND", None);
        let load = &job["configuration"]["load"];
        assert_eq!(load["sourceFormat"], "NEWLINE_DELIMITED_JSON");
        assert_eq!(load["writeDisposition"], "WRITE_APPEND");
        assert_eq!(load["ignoreUnknownValues"], true);
        assert_eq!(load["destinationTable"]["projectId"], "proj");
        assert_eq!(load["destinationTable"]["datasetId"], "ds");
        assert_eq!(load["destinationTable"]["tableId"], "tbl");
        // No location → no jobReference.
        assert!(job.get("jobReference").is_none());
    }

    #[test]
    fn build_load_job_json_includes_location_when_set() {
        let job = build_load_job_json("proj", "ds", "tbl", "WRITE_APPEND", Some("EU"));
        assert_eq!(job["jobReference"]["location"], "EU");
        assert_eq!(job["jobReference"]["projectId"], "proj");
    }

    #[test]
    fn build_load_job_json_fmt_csv_with_header_and_autodetect() {
        // The native byte-passthrough path (#633): CSV source format, skip the
        // header row, and let BigQuery autodetect the schema.
        let job = build_load_job_json_fmt(
            "proj",
            "ds",
            "tbl",
            "WRITE_TRUNCATE",
            None,
            "CSV",
            Some(1),
            true,
        );
        let load = &job["configuration"]["load"];
        assert_eq!(load["sourceFormat"], "CSV");
        assert_eq!(load["writeDisposition"], "WRITE_TRUNCATE");
        assert_eq!(load["skipLeadingRows"], 1);
        assert_eq!(load["autodetect"], true);
        assert_eq!(load["ignoreUnknownValues"], true);
    }

    #[test]
    fn build_load_job_json_fmt_ndjson_no_skip_rows_key() {
        let job = build_load_job_json_fmt(
            "p",
            "d",
            "t",
            "WRITE_APPEND",
            None,
            "NEWLINE_DELIMITED_JSON",
            None,
            true,
        );
        let load = &job["configuration"]["load"];
        assert_eq!(load["sourceFormat"], "NEWLINE_DELIMITED_JSON");
        assert_eq!(load["autodetect"], true);
        // No CSV header → the key is absent, not null.
        assert!(load.get("skipLeadingRows").is_none());
    }

    #[test]
    fn all_string_schema_builds_nullable_string_fields() {
        let s = all_string_schema(["Id".to_string(), "Name".to_string()]).unwrap();
        let fields = s["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "Id");
        assert_eq!(fields[0]["type"], "STRING");
        assert_eq!(fields[0]["mode"], "NULLABLE");
        assert_eq!(fields[1]["name"], "Name");
        // Empty column set → no schema (fall back to autodetect).
        assert!(all_string_schema(Vec::<String>::new()).is_none());
    }

    #[test]
    fn native_batch_columns_reads_first_ndjson_and_csv_line() {
        let nd = b"{\"Id\":\"1\",\"Name\":\"a\"}\n{\"Id\":\"2\"}\n";
        let cols = native_batch_columns(nd, faucet_core::NativeFormat::NdJson, b',').unwrap();
        assert_eq!(cols, vec!["Id".to_string(), "Name".to_string()]);
        let csv = b"Id,Name\r\n1,a\n";
        let cols = native_batch_columns(csv, faucet_core::NativeFormat::Csv, b',').unwrap();
        assert_eq!(cols, vec!["Id".to_string(), "Name".to_string()]);
        // Empty input → None.
        assert!(native_batch_columns(b"", faucet_core::NativeFormat::NdJson, b',').is_none());
    }

    #[test]
    fn load_job_with_explicit_schema_disables_autodetect() {
        // The native path passes an all-STRING schema; it must win over autodetect
        // (the bug that broke the first native run was autodetect type-inference).
        let schema = all_string_schema(["Id".to_string(), "Amount".to_string()]);
        let job = build_load_job_json_full(
            "p",
            "d",
            "t",
            "WRITE_TRUNCATE",
            None,
            "NEWLINE_DELIMITED_JSON",
            None,
            schema,
            true, // even with autodetect requested, an explicit schema forces it off
        );
        let load = &job["configuration"]["load"];
        assert_eq!(load["autodetect"], false);
        assert_eq!(load["schema"]["fields"][1]["name"], "Amount");
        assert_eq!(load["schema"]["fields"][1]["type"], "STRING");
    }

    #[test]
    fn media_boundary_matches_str_wrapper_and_is_absent() {
        let payload = b"{\"a\":1}\n";
        let b = media_boundary(payload);
        assert_eq!(b, multipart_boundary("{\"a\":1}\n"));
        assert!(b.starts_with("faucetbq") && b.ends_with("boundary"));
        assert!(!String::from_utf8_lossy(payload).contains(&b));
    }

    #[test]
    fn multipart_boundary_is_deterministic_and_absent_from_payload() {
        let ndjson = "{\"a\":1}\n";
        let b1 = multipart_boundary(ndjson);
        let b2 = multipart_boundary(ndjson);
        assert_eq!(b1, b2, "same payload → same boundary");
        assert!(!ndjson.contains(&b1), "boundary must not appear in payload");
        assert!(b1.starts_with("faucetbq") && b1.ends_with("boundary"));
    }

    #[test]
    fn build_multipart_related_has_both_parts_and_closing_delimiter() {
        let ndjson = "{\"a\":1}\n";
        let boundary = multipart_boundary(ndjson);
        let job_json = build_load_job_json("p", "d", "t", "WRITE_APPEND", None).to_string();
        // Media part is raw bytes (uncompressed here so the assertions can read it).
        let body = build_multipart_related(&boundary, &job_json, ndjson.as_bytes());
        let text = String::from_utf8(body).expect("utf8");
        // Two opening delimiters + one closing delimiter.
        assert_eq!(text.matches(&format!("--{boundary}\r\n")).count(), 2);
        assert!(text.ends_with(&format!("--{boundary}--\r\n")));
        assert!(text.contains("Content-Type: application/json; charset=UTF-8"));
        assert!(text.contains("Content-Type: application/octet-stream"));
        assert!(text.contains("NEWLINE_DELIMITED_JSON"));
        assert!(text.contains("{\"a\":1}"));
    }

    #[test]
    fn is_direct_overwrite_matrix() {
        let base = || {
            let mut c =
                BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault);
            c.write.write_mode = faucet_core::WriteMode::Overwrite;
            c
        };

        // Solo media-load overwrite ⇒ direct (no staging).
        let mut c = base();
        c.media_load = true;
        assert!(is_direct_overwrite(&c));

        // Grouped (executor set `_overwrite_staging`) ⇒ stage.
        let mut c = base();
        c.media_load = true;
        c.overwrite_staging = true;
        assert!(!is_direct_overwrite(&c));

        // Scoped/windowed overwrite ⇒ stage (partial replace can't truncate).
        let mut c = base();
        c.media_load = true;
        c.scope = Some(faucet_core::OverwriteScope::Window {
            column: "d".into(),
            from: json!("2024-01-01"),
            to: json!("2024-02-01"),
        });
        assert!(!is_direct_overwrite(&c));

        // Non-media overwrite (jobs.query path) ⇒ always stage. `media_load`
        // now defaults to true (#612), so opting out is what selects that path.
        let mut c = base();
        c.media_load = false;
        assert!(!is_direct_overwrite(&c));

        // And the default config takes the direct load — the whole point of
        // the flip: a solo overwrite is one atomic `WRITE_TRUNCATE` load.
        assert!(
            is_direct_overwrite(&base()),
            "a default solo overwrite must take the direct load path"
        );
    }

    #[test]
    fn appends_via_media_load_yields_to_an_explicit_insert_id_field() {
        let base =
            || BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault);

        // The #612 default: appends take the bulk load path.
        assert!(
            appends_via_media_load(&base()),
            "a default append must take the load path"
        );

        // Opting out selects the per-page insertAll path.
        let mut c = base();
        c.media_load = false;
        assert!(!appends_via_media_load(&c));

        // `insert_id_field` asks for insertAll's best-effort dedup, which a
        // load job cannot honour — so it wins over the default. Losing it
        // silently is the regression this guards.
        let c = base().with_insert_id_field("event_id");
        assert!(
            !appends_via_media_load(&c),
            "an explicit insert_id_field must keep appends on the streaming path"
        );

        // Both opted out ⇒ still streaming, for the same reason.
        let mut c = base().with_insert_id_field("event_id");
        c.media_load = false;
        assert!(!appends_via_media_load(&c));
    }

    #[test]
    fn gzip_media_round_trips_to_original_ndjson() {
        use std::io::Read;
        let ndjson =
            records_to_ndjson(&[json!({"a": 1, "b": "x"}), json!({"a": 2, "b": "y"})]).unwrap();
        let compressed = gzip(ndjson.as_bytes()).expect("gzip");
        // Compressed bytes are not the raw payload (a real gzip stream).
        assert_ne!(compressed.as_slice(), ndjson.as_bytes());
        assert_eq!(&compressed[..2], &[0x1f, 0x8b], "gzip magic bytes");
        let mut dec = flate2::read::GzDecoder::new(&compressed[..]);
        let mut out = String::new();
        dec.read_to_string(&mut out).expect("gunzip");
        assert_eq!(out, ndjson);
    }

    #[test]
    fn build_multipart_related_embeds_binary_media_verbatim() {
        // A gzip stream contains bytes like 0x00 that must survive the byte-wise
        // assembly (a `format!`-based body would corrupt them).
        let media = gzip(b"{\"a\":1}\n").expect("gzip");
        let boundary = "faucetbqTESTboundary";
        let job_json = build_load_job_json("p", "d", "t", "WRITE_TRUNCATE", None).to_string();
        let body = build_multipart_related(boundary, &job_json, &media);
        // The exact gzip bytes appear contiguously in the assembled body.
        assert!(
            body.windows(media.len()).any(|w| w == media.as_slice()),
            "gzip media must be embedded verbatim"
        );
        assert!(body.ends_with(format!("\r\n--{boundary}--\r\n").as_bytes()));
    }

    #[test]
    fn deletes_to_payload_multiple_rows() {
        let p = deletes_to_payload(&[
            KeyTuple(vec![("id".into(), json!(1))]),
            KeyTuple(vec![("id".into(), json!(2))]),
        ]);
        let v: serde_json::Value = serde_json::from_str(&p).expect("valid JSON");
        assert_eq!(v, json!([{"id": 1}, {"id": 2}]));
    }

    #[test]
    fn transient_classification_is_typed_never_message_based() {
        use super::{is_table_not_found, is_transient_bq_error};
        use gcp_bigquery_client::error::BQError;

        fn response_err(code: i64) -> BQError {
            BQError::ResponseError {
                error: gcp_bigquery_client::error::ResponseError {
                    error: gcp_bigquery_client::error::NestedResponseError {
                        code,
                        errors: Vec::new(),
                        // Deliberately a message that mentions nothing about
                        // retriability: classification must read the code only.
                        message: "whatever the vendor says today".to_string(),
                        status: String::new(),
                    },
                },
            }
        }

        // Retriable statuses.
        for code in [429, 500, 502, 503, 504] {
            assert!(is_transient_bq_error(&response_err(code)), "{code}");
        }
        // Permanent statuses — a bad request or a missing table must fail fast.
        for code in [400, 401, 403, 404, 409] {
            assert!(!is_transient_bq_error(&response_err(code)), "{code}");
        }
        // 404 is the table-not-found probe signal (used to decide create-vs-use).
        assert!(is_table_not_found(&response_err(404)));
        assert!(!is_table_not_found(&response_err(500)));
    }

    #[test]
    fn native_batch_columns_reads_a_csv_header_and_ignores_other_formats() {
        use faucet_core::NativeFormat;
        // CSV: the first line is the header, CR stripped, delimiter honored.
        assert_eq!(
            native_batch_columns(b"a;b;c\r\n1;2;3\n", NativeFormat::Csv, b';'),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        // A format with no header line yields nothing to declare.
        assert_eq!(
            native_batch_columns(b"PAR1", NativeFormat::Parquet, b','),
            None
        );
        // Non-UTF8 CSV bytes are not a panic, just "no columns".
        assert_eq!(
            native_batch_columns(&[0xff, 0xfe, b'\n'], NativeFormat::Csv, b','),
            None
        );
    }
}
