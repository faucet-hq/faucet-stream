//! `/v1/catalog/*` — browse the Data Movement Catalog (#279): the
//! accumulated cross-run picture of every dataset the server's pipelines have
//! touched. The three `GET` routes are read-only (`CatalogRead`, every role
//! from `viewer` up); `POST /v1/catalog/datasets/{id}/consumers` (#707)
//! annotates a dataset with owners and declared consumers (`CatalogAnnotate`,
//! `operator` up). Both enforced by the auth middleware.

use crate::serve::error::ServeError;
use crate::serve::history::catalog::{
    CatalogAnnotation, CatalogConsumer, CatalogDatasetDetail, CatalogDatasetPage,
    CatalogLineageEdge, CatalogListFilter, LINEAGE_DEFAULT_DEPTH,
};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_DEPTH: u32 = 32;

/// `GET /v1/catalog/datasets` query string.
#[derive(Debug, Deserialize)]
pub struct DatasetsQuery {
    /// Exact connector-kind filter (`csv`, `postgres`, …).
    pub kind: Option<String>,
    /// Case-insensitive substring match on the dataset URI.
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

/// `GET /v1/catalog/datasets` → 200.
pub async fn list_datasets(
    State(state): State<ServerState>,
    Query(query): Query<DatasetsQuery>,
) -> Result<Json<CatalogDatasetPage>, ServeError> {
    let filter = CatalogListFilter {
        kind: query.kind,
        q: query.q,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        cursor: query.cursor,
    };
    let page = state
        .history()
        .catalog_list_datasets(&filter)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(page))
}

/// `GET /v1/catalog/datasets/{id}` → 200 / 404.
pub async fn get_dataset(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<CatalogDatasetDetail>, ServeError> {
    let detail = state
        .history()
        .catalog_get_dataset(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    Ok(Json(detail))
}

/// `POST /v1/catalog/datasets/{id}/consumers` request body (#707).
#[derive(Debug, Deserialize)]
pub struct AnnotateRequest {
    /// Replace the owner list (an empty list clears it); omitted = unchanged.
    #[serde(default)]
    pub owners: Option<Vec<String>>,
    /// Consumers to upsert by `name`.
    #[serde(default)]
    pub consumers: Vec<ConsumerBody>,
    /// Drop every consumer not listed in `consumers` first.
    #[serde(default)]
    pub replace: bool,
}

/// One consumer in an [`AnnotateRequest`].
#[derive(Debug, Deserialize)]
pub struct ConsumerBody {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub contact: Option<String>,
    #[serde(default)]
    pub columns: Vec<String>,
}

/// `POST /v1/catalog/datasets/{id}/consumers` → 200 with the updated detail /
/// 404 unknown dataset / 422 empty or malformed annotation.
pub async fn annotate_dataset(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<AnnotateRequest>,
) -> Result<Json<CatalogDatasetDetail>, ServeError> {
    let now = chrono::Utc::now();
    let mut names = std::collections::HashSet::new();
    for c in &req.consumers {
        if c.name.trim().is_empty() {
            return Err(ServeError::Unprocessable {
                message: "consumers[].name must not be empty".into(),
                details: None,
            });
        }
        if !names.insert(c.name.as_str()) {
            return Err(ServeError::Unprocessable {
                message: format!("consumer `{}` is listed twice", c.name),
                details: None,
            });
        }
    }
    let annotation = CatalogAnnotation {
        owners: req.owners,
        consumers: req
            .consumers
            .into_iter()
            .map(|c| CatalogConsumer {
                name: c.name,
                kind: c.kind,
                contact: c.contact,
                columns: c.columns,
                registered_by: actor.principal.clone(),
                registered_at: now,
            })
            .collect(),
        replace_consumers: req.replace,
    };
    if annotation.is_empty() {
        return Err(ServeError::Unprocessable {
            message: "nothing to annotate: pass `owners` and/or `consumers`".into(),
            details: None,
        });
    }
    let found = state
        .history()
        .catalog_annotate(&id, &annotation)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    if !found {
        return Err(ServeError::NotFound);
    }
    crate::serve::audit::write(&state, &actor, "catalog.annotate", None, None, "ok").await;
    let detail = state
        .history()
        .catalog_get_dataset(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    Ok(Json(detail))
}

/// `GET /v1/catalog/lineage` query string.
#[derive(Debug, Deserialize)]
pub struct LineageQuery {
    /// Dataset id to root the graph at; omitted = the whole graph.
    pub root: Option<String>,
    /// BFS hop bound around `root` (ignored without one).
    pub depth: Option<u32>,
}

/// `GET /v1/catalog/lineage` response body.
#[derive(Debug, Serialize)]
pub struct LineageResponse {
    pub edges: Vec<CatalogLineageEdge>,
}

/// `GET /v1/catalog/lineage` → 200.
pub async fn lineage(
    State(state): State<ServerState>,
    Query(query): Query<LineageQuery>,
) -> Result<Json<LineageResponse>, ServeError> {
    let depth = query
        .depth
        .unwrap_or(LINEAGE_DEFAULT_DEPTH)
        .clamp(1, MAX_DEPTH);
    let edges = state
        .history()
        .catalog_lineage(query.root.as_deref(), depth)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(LineageResponse { edges }))
}
