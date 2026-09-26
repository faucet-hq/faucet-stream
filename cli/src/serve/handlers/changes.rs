//! `/v1/changes*` — change requests (#703): propose, list, inspect, approve,
//! reject.
//!
//! | route | permission |
//! |---|---|
//! | `POST /v1/changes` | `ChangeRequest` (operator+) |
//! | `GET /v1/changes`, `GET /v1/changes/{id}` | `ChangeRead` (viewer+) |
//! | `POST /v1/changes/{id}/approve`, `POST /v1/changes/{id}/reject` | `ChangeApprove` (operator+; the `approvals:` policy narrows further) |
//!
//! The permission says who may *reach* the route; the approval policy in
//! `--auth-config` says who may approve which kind, how many approvals a
//! request needs, and whether a requester may approve their own. A refusal by
//! the policy is a `403` naming the rule.

use crate::serve::changes::{
    self, ChangeKind, ChangeListFilter, ChangeRequest, ChangeStatus, NewChange,
};
use crate::serve::error::ServeError;
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 2000;

/// `POST /v1/changes` → 201 with the pending request (plan included).
pub async fn create_change(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(body): Json<NewChange>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    let change = changes::create(&state, &actor, body).await?;
    Ok((StatusCode::CREATED, Json(change)))
}

/// `GET /v1/changes` query string.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
    pub kind: Option<String>,
    pub requester: Option<String>,
    pub limit: Option<usize>,
}

/// `GET /v1/changes` → newest first, plan rows omitted.
pub async fn list_changes(
    State(state): State<ServerState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<ChangeRequest>>, ServeError> {
    let status = q
        .status
        .as_deref()
        .map(str::parse::<ChangeStatus>)
        .transpose()
        .map_err(ServeError::BadConfig)?;
    let kind = q
        .kind
        .as_deref()
        .map(str::parse::<ChangeKind>)
        .transpose()
        .map_err(ServeError::BadConfig)?;
    let filter = ChangeListFilter {
        status,
        kind,
        requester: q.requester,
        limit: q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
    };
    Ok(Json(changes::list(&state, &filter).await?))
}

/// `GET /v1/changes/{id}` → the full record, plan included.
pub async fn get_change(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<ChangeRequest>, ServeError> {
    Ok(Json(changes::get(&state, &id).await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct ApproveBody {
    #[serde(default)]
    pub comment: Option<String>,
}

/// `POST /v1/changes/{id}/approve` → 200 with the record: still `pending`
/// below quorum, else `executed` / `invalidated` / `failed`. `403` when the
/// approval policy refuses this approver, `409` when the request is not
/// pending or this approver already approved.
pub async fn approve_change(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    body: Option<Json<ApproveBody>>,
) -> Result<Json<ChangeRequest>, ServeError> {
    let comment = body.and_then(|b| b.0.comment);
    Ok(Json(changes::approve(&state, &actor, &id, comment).await?))
}

#[derive(Debug, Deserialize)]
pub struct RejectBody {
    pub reason: String,
}

/// `POST /v1/changes/{id}/reject` → 200 with the rejected record. The
/// requester may reject (withdraw) their own request.
pub async fn reject_change(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<RejectBody>,
) -> Result<Json<ChangeRequest>, ServeError> {
    if body.reason.trim().is_empty() {
        return Err(ServeError::BadConfig("a rejection needs a reason".into()));
    }
    Ok(Json(
        changes::reject(&state, &actor, &id, body.reason).await?,
    ))
}
