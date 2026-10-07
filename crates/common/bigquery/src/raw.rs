//! Schema-tolerant fallbacks for BigQuery REST calls (SQL-172).
//!
//! `gcp-bigquery-client`'s `FieldType` enum has no catch-all, so a response
//! naming a column type it does not know (`RANGE`, or anything BigQuery adds
//! later) fails to deserialize and takes the whole call down. The `tolerant_*`
//! functions issue the typed call first and, only when it fails to decode,
//! repeat the same request raw, rewrite every unknown field type to `STRING`
//! (BigQuery renders such values as strings in REST JSON) and decode again.

use crate::{BigQueryCredentials, access_token_bq};
use gcp_bigquery_client::Client;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::get_query_results_response::GetQueryResultsResponse;
use gcp_bigquery_client::model::job::Job;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::QueryResponse;
use gcp_bigquery_client::model::table::Table;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The production BigQuery API host.
pub const API_HOST: &str = "https://bigquery.googleapis.com";

/// Field types `gcp-bigquery-client` can decode.
const KNOWN_FIELD_TYPES: &[&str] = &[
    "STRING",
    "BYTES",
    "INTEGER",
    "INT64",
    "FLOAT",
    "FLOAT64",
    "NUMERIC",
    "BIGNUMERIC",
    "BOOLEAN",
    "BOOL",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "DATETIME",
    "RECORD",
    "STRUCT",
    "GEOGRAPHY",
    "JSON",
    "INTERVAL",
];

/// Where a raw request is sent and how it is authenticated.
#[derive(Debug, Clone, Copy)]
pub struct RawTarget<'a> {
    /// Credentials the bearer token is minted from.
    pub creds: &'a BigQueryCredentials,
    /// API host (`None` = [`API_HOST`]); the path `/bigquery/v2/…` is appended.
    pub host: Option<&'a str>,
}

/// Whether a typed call failed while decoding its response body.
pub fn is_decode_error(e: &BQError) -> bool {
    match e {
        BQError::RequestError(r) => r.is_decode(),
        BQError::SerializationError(_) => true,
        _ => false,
    }
}

/// Rewrite every schema field whose `type` the client cannot decode to
/// `STRING`, at any depth. Returns how many fields were rewritten.
pub fn normalize_field_types(value: &mut Value) -> usize {
    let mut rewritten = 0;
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(fields)) = map.get_mut("fields") {
                for field in fields.iter_mut() {
                    if let Some(Value::String(t)) = field.get_mut("type") {
                        let upper = t.to_ascii_uppercase();
                        if KNOWN_FIELD_TYPES.contains(&upper.as_str()) {
                            *t = upper;
                        } else {
                            *t = "STRING".to_string();
                            rewritten += 1;
                        }
                    }
                }
            }
            for v in map.values_mut() {
                rewritten += normalize_field_types(v);
            }
        }
        Value::Array(items) => {
            for v in items {
                rewritten += normalize_field_types(v);
            }
        }
        _ => {}
    }
    rewritten
}

/// A `requestId` for `jobs.query`, so a raw retry of the same request is
/// deduplicated by BigQuery instead of running the statement twice.
pub fn new_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    format!(
        "f{:012x}{:06x}{:08x}",
        nanos & 0xffff_ffff_ffff,
        std::process::id() & 0xff_ffff,
        SEQ.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff
    )
}

fn url(target: RawTarget<'_>, segments: &[&str]) -> Result<reqwest_bq::Url, BQError> {
    let host = target.host.unwrap_or(API_HOST).trim_end_matches('/');
    let mut url = reqwest_bq::Url::parse(&format!("{host}/bigquery/v2"))
        .map_err(|e| BQError::ConnectionPoolError(format!("invalid BigQuery host {host}: {e}")))?;
    url.path_segments_mut()
        .map_err(|_| BQError::ConnectionPoolError(format!("invalid BigQuery host {host}")))?
        .extend(segments);
    Ok(url)
}

async fn execute<T: DeserializeOwned>(req: reqwest_bq::RequestBuilder) -> Result<T, BQError> {
    let resp = req.send().await?;
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if !status.is_success() {
        return Err(BQError::ResponseError {
            error: serde_json::from_slice(&bytes)?,
        });
    }
    let mut value: Value = serde_json::from_slice(&bytes)?;
    let rewritten = normalize_field_types(&mut value);
    if rewritten > 0 {
        tracing::debug!(
            rewritten,
            "BigQuery: decoded unknown column types as STRING"
        );
    }
    Ok(serde_json::from_value(value)?)
}

async fn get<T: DeserializeOwned>(
    target: RawTarget<'_>,
    segments: &[&str],
    query: &(impl serde::Serialize + ?Sized),
) -> Result<T, BQError> {
    let token = access_token_bq(target.creds).await?;
    let req = crate::http_client_bq()?
        .get(url(target, segments)?)
        .query(query)
        .bearer_auth(token);
    execute(req).await
}

/// `tables.get`, tolerating unknown column types.
pub async fn tolerant_get_table(
    client: &Client,
    target: RawTarget<'_>,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<Table, BQError> {
    match client
        .table()
        .get(project_id, dataset_id, table_id, None)
        .await
    {
        Err(e) if is_decode_error(&e) => {
            get(
                target,
                &[
                    "projects", project_id, "datasets", dataset_id, "tables", table_id,
                ],
                &[] as &[(&str, &str)],
            )
            .await
        }
        other => other,
    }
}

/// `jobs.get`, tolerating unknown column types in the job's statistics.
pub async fn tolerant_get_job(
    client: &Client,
    target: RawTarget<'_>,
    project_id: &str,
    job_id: &str,
    location: Option<&str>,
) -> Result<Job, BQError> {
    match client.job().get_job(project_id, job_id, location).await {
        Err(e) if is_decode_error(&e) => {
            let query: Vec<(&str, &str)> = location.map(|l| ("location", l)).into_iter().collect();
            get(target, &["projects", project_id, "jobs", job_id], &query).await
        }
        other => other,
    }
}

/// `jobs.getQueryResults`, tolerating unknown column types in the schema.
pub async fn tolerant_get_query_results(
    client: &Client,
    target: RawTarget<'_>,
    project_id: &str,
    job_id: &str,
    params: GetQueryResultsParameters,
) -> Result<GetQueryResultsResponse, BQError> {
    let raw_params = params.clone();
    match client
        .job()
        .get_query_results(project_id, job_id, params)
        .await
    {
        Err(e) if is_decode_error(&e) => {
            get(
                target,
                &["projects", project_id, "queries", job_id],
                &raw_params,
            )
            .await
        }
        other => other,
    }
}

/// `jobs.query`, tolerating unknown column types in the result schema. A
/// `requestId` is set when the request has none, so the raw retry returns the
/// same job instead of running the statement a second time.
pub async fn tolerant_query(
    client: &Client,
    target: RawTarget<'_>,
    project_id: &str,
    mut request: QueryRequest,
) -> Result<QueryResponse, BQError> {
    if request.request_id.is_none() {
        request.request_id = Some(new_request_id());
    }
    let body = serde_json::to_vec(&request)?;
    match client.job().query(project_id, request).await {
        Err(e) if is_decode_error(&e) => {
            let token = access_token_bq(target.creds).await?;
            let req = crate::http_client_bq()?
                .post(url(target, &["projects", project_id, "queries"])?)
                .header("content-type", "application/json")
                .body(body)
                .bearer_auth(token);
            execute(req).await
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_field_types_become_string_at_any_depth() {
        let mut v = json!({
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "span", "type": "RANGE", "rangeElementType": {"type": "DATE"}},
                {"name": "rec", "type": "RECORD", "fields": [
                    {"name": "inner", "type": "range"},
                    {"name": "ok", "type": "json"}
                ]}
            ]},
            "rows": [{"f": [{"v": "1"}]}]
        });
        assert_eq!(normalize_field_types(&mut v), 2);
        let fields = &v["schema"]["fields"];
        assert_eq!(fields[0]["type"], "INTEGER");
        assert_eq!(fields[1]["type"], "STRING");
        assert_eq!(fields[1]["rangeElementType"]["type"], "DATE");
        assert_eq!(fields[2]["fields"][0]["type"], "STRING");
        assert_eq!(fields[2]["fields"][1]["type"], "JSON");
        let table: Table = serde_json::from_value(json!({
            "tableReference": {"projectId": "p", "datasetId": "d", "tableId": "t"},
            "schema": v["schema"].clone()
        }))
        .expect("normalized schema decodes");
        assert_eq!(table.schema.fields.unwrap().len(), 3);
    }

    #[test]
    fn request_ids_are_unique_and_short() {
        let a = new_request_id();
        let b = new_request_id();
        assert_ne!(a, b);
        assert!(a.len() <= 36 && a.is_ascii(), "{a}");
    }

    #[test]
    fn urls_escape_segments_and_honour_the_host() {
        let creds = BigQueryCredentials::ApplicationDefault;
        let t = RawTarget {
            creds: &creds,
            host: Some("http://127.0.0.1:1/"),
        };
        assert_eq!(
            url(t, &["projects", "p", "jobs", "a/b"]).unwrap().as_str(),
            "http://127.0.0.1:1/bigquery/v2/projects/p/jobs/a%2Fb"
        );
        let prod = RawTarget {
            creds: &creds,
            host: None,
        };
        assert!(url(prod, &["x"]).unwrap().as_str().starts_with(API_HOST));
        assert!(
            url(
                RawTarget {
                    creds: &creds,
                    host: Some("not a url")
                },
                &[]
            )
            .is_err()
        );
    }
}
