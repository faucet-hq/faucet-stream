//! `/v1/runs*` HTTP handlers. Thin glue: deserialize, call into `runner`/history,
//! map to status codes. All run-mutating logic lives in `runner.rs`.

use crate::serve::error::ServeError;
use crate::serve::history::{DeleteOutcome, ListFilter, RunRecord, RunStatus};
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, ConfigFormatWire, SubmitOutcome, SubmitRequest};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Scrub any resolved secret that reached a run's error fields before the record
/// is serialized into an HTTP response. The serve log subscriber's redaction
/// writer only covers tracing/log output — API response bodies are a separate
/// egress and must be scrubbed here.
fn redact_record(rec: &mut RunRecord) {
    if let Some(e) = &rec.error {
        rec.error = Some(crate::secrets::registry::redact(e).into_owned());
    }
    for inv in &mut rec.invocations {
        if let Some(e) = &inv.error {
            inv.error = Some(crate::secrets::registry::redact(e).into_owned());
        }
    }
}

/// `POST /v1/runs` → 202. The auth middleware injects the resolved
/// [`AuthContext`] so `submit` can attribute the `run.submit` audit record.
pub async fn submit_run(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<SubmitRequest>,
) -> Result<axum::response::Response, ServeError> {
    match runner::submit_gated(state, req, actor).await? {
        SubmitOutcome::Accepted(resp) => Ok((StatusCode::ACCEPTED, Json(resp)).into_response()),
        // The server (or the caller) wants approval first (#703): the body
        // is the pending change request, not a run.
        SubmitOutcome::PendingApproval(change) => Ok((
            StatusCode::ACCEPTED,
            Json(runner::pending_approval_body(&change)),
        )
            .into_response()),
    }
}

/// `GET /v1/runs/{id}` → 200 RunRecord. Fills live `elapsed_secs` for running runs.
pub async fn get_run(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<RunRecord>, ServeError> {
    let mut rec = state
        .history()
        .get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if rec.status == RunStatus::Running
        && let Some(started) = rec.started_at
    {
        rec.elapsed_secs = (Utc::now() - started)
            .to_std()
            .ok()
            .map(|d| d.as_secs_f64());
    }
    redact_record(&mut rec);
    Ok(Json(rec))
}

/// `POST /v1/runs/{id}/cancel` → 202 (cancel requested) / 200 (terminal no-op) / 404.
/// In cluster mode, cancel is best-effort: the run may complete on its owning
/// instance before that instance processes the flag.
pub async fn cancel_run(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ServeError> {
    // 1. A live local token (this instance is running/queued it) → cancel now.
    if state.registry().cancel(&id) {
        crate::serve::audit::write(&state, &actor, "run.cancel", Some(id.clone()), None, "ok")
            .await;
        return Ok(StatusCode::ACCEPTED);
    }

    // 2. Look up the record.
    let rec = match state
        .history()
        .get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
    {
        Some(r) => r,
        None => return Err(ServeError::NotFound),
    };

    if rec.status.is_terminal() {
        return Ok(StatusCode::OK); // already done — no-op.
    }

    // 3. Cluster mode: the run is owned by another instance (or unclaimed).
    if state.cluster().enabled() {
        // Unclaimed (Pending) → cancel it directly.
        if state
            .history()
            .cancel_pending(&id)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?
        {
            crate::serve::audit::write(&state, &actor, "run.cancel", Some(id.clone()), None, "ok")
                .await;
            // Terminal transition that bypasses `runner::finalize` entirely, so
            // it must fire the completion callback itself (#481).
            match state.history().get(&id).await {
                Ok(Some(rec)) => crate::serve::callback::fire(&rec).await,
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    run_id = %id,
                    error = %e,
                    "could not read cancelled run for its completion callback"
                ),
            }
            return Ok(StatusCode::ACCEPTED);
        }
        // Otherwise it is running on a peer → flag it; the peer cancels on its
        // next claim-loop tick.
        state
            .history()
            .request_cancel(&id)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?;
        crate::serve::audit::write(&state, &actor, "run.cancel", Some(id.clone()), None, "ok")
            .await;
        return Ok(StatusCode::ACCEPTED);
    }

    // Single-instance, non-terminal, no local token: a transient race (the token
    // was removed between the registry check and the history read as the run
    // completes). Treat as a no-op — the run is finishing or just finished.
    Ok(StatusCode::OK)
}

/// `DELETE /v1/runs/{id}` → 204 / 404 / 409 (still running).
pub async fn delete_run(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<StatusCode, ServeError> {
    match state
        .history()
        .delete(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
    {
        DeleteOutcome::Deleted => {
            crate::serve::audit::write(&state, &actor, "run.delete", Some(id.clone()), None, "ok")
                .await;
            Ok(StatusCode::NO_CONTENT)
        }
        DeleteOutcome::NotFound => Err(ServeError::NotFound),
        DeleteOutcome::StillRunning => Err(ServeError::Conflict(
            "run is still in flight — cancel it before deleting".into(),
        )),
    }
}

/// Query-param wrapper for an RFC3339 timestamp. `application/x-www-form-urlencoded`
/// decodes `+` as a space, which corrupts explicit UTC offsets like `+05:30`; we
/// restore the `+` before parsing, so both `…Z` and `…+05:30` query values work.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DateTimeUtcParam(DateTime<Utc>);

impl<'de> Deserialize<'de> for DateTimeUtcParam {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        // Undo form-encoding's '+' → ' ' substitution before RFC3339 parsing.
        let restored = raw.replace(' ', "+");
        DateTime::parse_from_rfc3339(&restored)
            .map(|dt| DateTimeUtcParam(dt.to_utc()))
            .map_err(serde::de::Error::custom)
    }
}

/// `GET /v1/runs` query string.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Comma-separated status names to include (e.g. `running,completed`); empty
    /// / absent = every status. An unknown token is a 400 — silently ignoring it
    /// would return *unfiltered* results to a caller who typo'd `failed`.
    pub status: Option<String>,
    pub name: Option<String>,
    pub(crate) since: Option<DateTimeUtcParam>,
    pub(crate) until: Option<DateTimeUtcParam>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

/// `GET /v1/runs` response body.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub runs: Vec<RunRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;

impl ListQuery {
    fn into_filter(self) -> Result<ListFilter, ServeError> {
        // Reject unknown status tokens instead of dropping them: a dropped
        // token leaves the vec empty, and an empty vec means "every status" —
        // a monitoring script that typo'd `?status=faild` would silently
        // receive ALL runs instead of an error.
        let status = match self.status.as_deref() {
            None => Vec::new(),
            Some(raw) => raw
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(|t| {
                    RunStatus::parse(t).ok_or_else(|| {
                        ServeError::BadConfig(format!(
                            "unknown status '{t}' — valid values: queued, pending, running, \
                             sharded, completed, failed, cancelled"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(ListFilter {
            status,
            name: self.name,
            since: self.since.map(|p| p.0),
            until: self.until.map(|p| p.0),
            limit: self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
            cursor: self.cursor,
        })
    }
}

/// `GET /v1/runs` → 200.
pub async fn list_runs(
    State(state): State<ServerState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse>, ServeError> {
    let page = state
        .history()
        .list(&query.into_filter()?)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    let mut runs = page.runs;
    for rec in &mut runs {
        redact_record(rec);
    }
    Ok(Json(ListResponse {
        runs,
        next_cursor: page.next_cursor,
    }))
}

/// `POST /v1/runs/{id}/rollback` request body (#706).
#[derive(Debug, Deserialize, Default)]
pub struct RollbackRequest {
    /// The invocation to undo — one of the run's `invocations[].run_id`.
    /// Optional when the run has exactly one invocation.
    #[serde(default)]
    pub invocation_id: Option<String>,
    /// The row that invocation wrote (default: search every root's state).
    #[serde(default)]
    pub row: Option<String>,
    /// The pipeline config the run was made with. Optional when the server
    /// stored the run's config (cluster mode); required otherwise.
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
}

/// `POST /v1/runs/{id}/rollback` → 200 with the
/// [`RollbackReport`](crate::rollback::RollbackReport) (`applied: false` +
/// `conflicts > 0` means the rollback was refused because a later run changed
/// the keys — pass `force`). Admin-only; audited as `run.rollback`.
pub async fn rollback_run(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    body: Option<Json<RollbackRequest>>,
) -> Result<Json<crate::rollback::RollbackReport>, ServeError> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let rec = state
        .history()
        .get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if !rec.status.is_terminal() {
        return Err(ServeError::Conflict(format!(
            "run {id} is {} — a run can only be rolled back once it has finished",
            rec.status.as_str()
        )));
    }
    // Which invocation: the caller's pick, else the only one.
    let known: Vec<String> = rec
        .invocations
        .iter()
        .filter_map(|i| i.run_id.clone())
        .collect();
    let invocation_id = match req.invocation_id {
        Some(inv) => {
            if !known.is_empty() && !known.contains(&inv) {
                return Err(ServeError::BadConfig(format!(
                    "invocation_id {inv} is not one of run {id}'s invocations ({})",
                    known.join(", ")
                )));
            }
            inv
        }
        None if known.len() == 1 => known[0].clone(),
        None if known.is_empty() => {
            return Err(ServeError::Unprocessable {
                message: format!(
                    "run {id} recorded no invocation ids (it predates rollback support, or \
                     no pipeline ran); pass `invocation_id` explicitly"
                ),
                details: None,
            });
        }
        None => {
            return Err(ServeError::BadConfig(format!(
                "run {id} has {} invocations — pass `invocation_id` (one of: {})",
                known.len(),
                known.join(", ")
            )));
        }
    };
    // Which config: the caller's, else the stored one (cluster mode).
    let (config, format) = match (req.config, rec.config_body.as_deref()) {
        (Some(c), _) => (c, req.config_format.into()),
        (None, Some(stored)) => (stored.to_string(), rec.config_format.unwrap_or_default()),
        (None, None) => {
            return Err(ServeError::Unprocessable {
                message: format!(
                    "run {id}'s config is not stored on this server; pass `config` (the \
                     pipeline config the run was made with) in the request body"
                ),
                details: None,
            });
        }
    };
    let loaded = crate::serve::load::load_submission(
        &config,
        format,
        state.default_base().as_ref(),
        crate::serve::runner::server_policy(&state).as_deref(),
    )
    .await?;
    let auth = crate::auth_catalog::build_auth_catalog(loaded.cfg.auth.as_ref())
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let pipeline_name = loaded
        .cfg
        .name
        .clone()
        .or(rec.name.clone())
        .unwrap_or_else(|| "pipeline".to_string());
    let (node, store, marker) = crate::rollback::locate(
        &loaded.nodes,
        &pipeline_name,
        &invocation_id,
        req.row.as_deref(),
    )
    .await
    .map_err(cli_to_serve)?;
    let report = crate::rollback::rollback_node(
        &node,
        store,
        marker,
        &crate::rollback::RollbackInputs {
            run_id: invocation_id,
            row: req.row,
            dry_run: req.dry_run,
            force: req.force,
            pipeline_name,
            auth,
        },
    )
    .await
    .map_err(cli_to_serve)?;
    let result = if req.dry_run {
        "dry_run"
    } else if report.outcome.applied {
        "ok"
    } else {
        "blocked"
    };
    crate::serve::audit::write(&state, &actor, "run.rollback", Some(id), None, result).await;
    Ok(Json(report))
}

/// Map a CLI-layer error to an HTTP-facing one: a bad config / missing run is
/// a client error (400); anything else is internal (500).
fn cli_to_serve(e: crate::error::CliError) -> ServeError {
    match e {
        crate::error::CliError::Config(m) => ServeError::BadConfig(m),
        other => ServeError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_param_restores_form_encoded_plus() {
        // form-encoding turns the '+' of an offset into a space; the wrapper must
        // restore it so both offset and Z forms parse to the same UTC instant.
        let spaced: DateTimeUtcParam =
            serde_json::from_value(serde_json::json!("2026-01-01T00:00:00 05:30")).unwrap();
        let plus = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00+05:30")
            .unwrap()
            .to_utc();
        assert_eq!(spaced.0, plus);

        let zulu: DateTimeUtcParam =
            serde_json::from_value(serde_json::json!("2026-01-01T00:00:00Z")).unwrap();
        assert_eq!(
            zulu.0,
            chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .to_utc()
        );
    }

    #[test]
    fn list_query_clamps_limit() {
        let q = ListQuery {
            status: None,
            name: None,
            since: None,
            until: None,
            limit: Some(99999),
            cursor: None,
        };
        assert_eq!(q.into_filter().unwrap().limit, MAX_LIMIT);
        let q = ListQuery {
            status: Some("failed, completed".to_string()),
            name: None,
            since: None,
            until: None,
            limit: None,
            cursor: None,
        };
        let f = q.into_filter().unwrap();
        assert_eq!(f.limit, DEFAULT_LIMIT);
        assert_eq!(f.status, vec![RunStatus::Failed, RunStatus::Completed]);
    }

    #[test]
    fn list_query_rejects_unknown_status_token() {
        // A typo must be a 400, not a silently-unfiltered result: dropping the
        // token would leave the vec empty, and empty means "every status".
        let q = ListQuery {
            status: Some("faild".to_string()),
            name: None,
            since: None,
            until: None,
            limit: None,
            cursor: None,
        };
        let err = q.into_filter().unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(ref m) if m.contains("faild")));
        // …and one bad token among good ones still rejects the request.
        let q = ListQuery {
            status: Some("failed,Completed".to_string()),
            name: None,
            since: None,
            until: None,
            limit: None,
            cursor: None,
        };
        assert!(q.into_filter().is_err(), "case-sensitive tokens");
        // A trailing comma (empty token) is tolerated.
        let q = ListQuery {
            status: Some("failed,".to_string()),
            name: None,
            since: None,
            until: None,
            limit: None,
            cursor: None,
        };
        assert_eq!(q.into_filter().unwrap().status, vec![RunStatus::Failed]);
    }

    #[tokio::test]
    async fn cancel_pending_run_in_cluster_mode_cancels_it() {
        use crate::serve::history::{RunRecord, RunStatus};
        use crate::serve::test_support::test_state_clustered;
        use chrono::Utc;

        let state = test_state_clustered();
        // A pending run with no local token (simulating an unclaimed cluster run).
        let mut rec = RunRecord::queued("p1".into(), None, Default::default(), None, Utc::now());
        rec.status = RunStatus::Pending;
        state.history().upsert(&rec).await.unwrap();

        let resp = cancel_run(
            axum::extract::State(state.clone()),
            axum::extract::Extension(AuthContext {
                principal: "test".into(),
                role: crate::serve::rbac::Role::Admin,
                source_ip: None,
            }),
            axum::extract::Path("p1".into()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            state.history().get("p1").await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }
}
