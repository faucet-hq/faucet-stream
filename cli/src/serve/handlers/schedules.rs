//! `/v1/schedules` (#1044): HTTP adapters over
//! [`crate::serve::schedules::service`], which holds every rule.
//!
//! | Route | Permission |
//! |---|---|
//! | `GET /v1/schedules[?tenant=]` | `ScheduleRead` |
//! | `GET /v1/schedules/{name}[?tenant=]` | `ScheduleRead` |
//! | `POST /v1/schedules/preview` | `ScheduleRead` |
//! | `POST /v1/schedules`, `PUT`/`DELETE /v1/schedules/{name}` | `ScheduleWrite` |
//! | `POST /v1/schedules/{name}/{pause,resume,run}` | `ScheduleWrite` |
//!
//! A build without the `schedule` and `templates` features answers
//! `GET /v1/schedules` with `available: false` (the console says why) and
//! registers no other route.

use serde::Deserialize;

/// `?tenant=` — an operator's view scoped to one tenant.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantQuery {
    #[serde(default)]
    pub tenant: Option<String>,
}

#[cfg(not(all(feature = "schedule", feature = "templates")))]
pub use disabled::*;

#[cfg(not(all(feature = "schedule", feature = "templates")))]
mod disabled {
    use axum::Json;

    /// The console's "off" state: this build ticks no schedules.
    pub async fn list() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "available": false,
            "reason": "this server was built without the `schedule` and `templates` features",
            "schedules": [],
        }))
    }
}

#[cfg(all(feature = "schedule", feature = "templates"))]
pub use enabled::*;

#[cfg(all(feature = "schedule", feature = "templates"))]
mod enabled {
    use super::TenantQuery;
    use crate::serve::error::ServeError;
    use crate::serve::history::schedules::ScheduleSpec;
    use crate::serve::rbac::AuthContext;
    use crate::serve::schedules::service::{
        self, ChangeAction, ChangePayload, Changed, Detail, ListResponse, PreviewRequest,
        PreviewResponse, RunNowResponse, ScheduleView,
    };
    use crate::serve::state::ServerState;
    use axum::Json;
    use axum::extract::{Extension, Path, Query, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};

    pub async fn list(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Query(q): Query<TenantQuery>,
    ) -> Result<Json<ListResponse>, ServeError> {
        service::list(&state, &actor, q.tenant.as_deref())
            .await
            .map(Json)
    }

    pub async fn get(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
        Query(q): Query<TenantQuery>,
    ) -> Result<Json<Detail>, ServeError> {
        service::get(&state, &actor, &name, q.tenant.as_deref())
            .await
            .map(Json)
    }

    pub async fn preview(
        Json(req): Json<PreviewRequest>,
    ) -> Result<Json<PreviewResponse>, ServeError> {
        Ok(Json(service::preview(&req, chrono::Utc::now())))
    }

    /// `?reason=` — why, kept on the change request when the server gates
    /// schedule changes (`--require-approval schedule`).
    #[derive(Debug, Default, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ReasonQuery {
        #[serde(default)]
        pub reason: Option<String>,
    }

    /// The response for a change: `ok` with the view, or 202 with the
    /// pending change request.
    fn respond(changed: Changed, ok: StatusCode) -> Response {
        match changed {
            Changed::Applied(Some(v)) => (ok, Json(v)).into_response(),
            Changed::Applied(None) => StatusCode::NO_CONTENT.into_response(),
            Changed::Pending(c) => (
                StatusCode::ACCEPTED,
                Json(crate::serve::runner::pending_approval_body(&c)),
            )
                .into_response(),
        }
    }

    pub async fn create(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Query(q): Query<ReasonQuery>,
        Json(spec): Json<ScheduleSpec>,
    ) -> Result<Response, ServeError> {
        let p = ChangePayload {
            action: ChangeAction::Create,
            name: spec.name.clone(),
            spec: Some(spec),
        };
        let changed = service::change(&state, &actor, p, q.reason).await?;
        Ok(respond(changed, StatusCode::CREATED))
    }

    pub async fn update(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
        Query(q): Query<ReasonQuery>,
        Json(spec): Json<ScheduleSpec>,
    ) -> Result<Response, ServeError> {
        let p = ChangePayload {
            action: ChangeAction::Update,
            name,
            spec: Some(spec),
        };
        let changed = service::change(&state, &actor, p, q.reason).await?;
        Ok(respond(changed, StatusCode::OK))
    }

    pub async fn delete(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
        Query(q): Query<ReasonQuery>,
    ) -> Result<Response, ServeError> {
        let p = ChangePayload {
            action: ChangeAction::Delete,
            name,
            spec: None,
        };
        let changed = service::change(&state, &actor, p, q.reason).await?;
        Ok(respond(changed, StatusCode::NO_CONTENT))
    }

    /// `POST /v1/schedules/{name}/pause` body (optional).
    #[derive(Debug, Default, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct PauseBody {
        /// Why — shown on the schedule and kept in its history.
        #[serde(default)]
        pub reason: Option<String>,
    }

    pub async fn pause(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
        body: Option<Json<PauseBody>>,
    ) -> Result<Json<ScheduleView>, ServeError> {
        let reason = body.and_then(|Json(b)| b.reason);
        service::set_paused(&state, &actor, &name, true, reason)
            .await
            .map(Json)
    }

    pub async fn resume(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
        body: Option<Json<PauseBody>>,
    ) -> Result<Json<ScheduleView>, ServeError> {
        let reason = body.and_then(|Json(b)| b.reason);
        service::set_paused(&state, &actor, &name, false, reason)
            .await
            .map(Json)
    }

    pub async fn run(
        State(state): State<ServerState>,
        Extension(actor): Extension<AuthContext>,
        Path(name): Path<String>,
    ) -> Result<(StatusCode, Json<RunNowResponse>), ServeError> {
        let r = service::run_now(&state, &actor, &name).await?;
        Ok((StatusCode::ACCEPTED, Json(r)))
    }
}
