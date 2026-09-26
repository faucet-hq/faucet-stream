//! `GET /v1/usage` (#704) — the cost & usage report over the invocations
//! this server has recorded, aggregated by pipeline / row / dataset / sink /
//! day. Requires `UsageRead` (viewer+): it is a priced read of volumes the
//! catalog already shows.

use crate::commands::usage::{parse_when, report_currency};
use crate::serve::error::ServeError;
use crate::serve::state::ServerState;
use crate::usage::{GroupBy, UsageFilter, UsageRecord, UsageReport, aggregate};
use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 5_000;
const MAX_LIMIT: usize = 50_000;

/// `GET /v1/usage` query string.
#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// RFC 3339 or `YYYY-MM-DD`; only invocations recorded at or after it.
    pub since: Option<String>,
    /// RFC 3339 or `YYYY-MM-DD`; only invocations recorded before it.
    pub until: Option<String>,
    /// Only this pipeline.
    pub pipeline: Option<String>,
    /// `pipeline` (default) / `row` / `dataset` / `sink` / `day`.
    pub by: Option<String>,
    /// Most invocation records to read (newest first).
    pub limit: Option<usize>,
    /// Also return the raw records the report was built from.
    #[serde(default)]
    pub include_records: bool,
}

#[derive(Debug, Serialize)]
pub struct UsageResponse {
    pub report: UsageReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<Vec<UsageRecord>>,
}

pub async fn list_usage(
    State(state): State<ServerState>,
    Query(q): Query<UsageQuery>,
) -> Result<Json<UsageResponse>, ServeError> {
    let by = match q.by.as_deref() {
        None => GroupBy::Pipeline,
        Some(s) => GroupBy::parse(s).ok_or_else(|| {
            ServeError::BadConfig(format!(
                "`by` must be one of pipeline, row, dataset, sink, day (got `{s}`)"
            ))
        })?,
    };
    let filter = UsageFilter {
        since: q
            .since
            .as_deref()
            .map(parse_when)
            .transpose()
            .map_err(|e| ServeError::BadConfig(format!("since: {e}")))?,
        until: q
            .until
            .as_deref()
            .map(parse_when)
            .transpose()
            .map_err(|e| ServeError::BadConfig(format!("until: {e}")))?,
        pipeline: q.pipeline,
        limit: q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
    };
    let records = state
        .history()
        .usage_list(&filter)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    let report = aggregate(&records, by, &report_currency(&records));
    Ok(Json(UsageResponse {
        report,
        records: q.include_records.then_some(records),
    }))
}
