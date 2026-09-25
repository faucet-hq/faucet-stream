//! `GET /v1/schemas` (catalog) and `GET /v1/schemas/{kind}/{name}` (one JSON
//! Schema). Read-only; reuses the CLI's schema introspection so the catalog
//! always reflects the compiled connectors/transforms.

use crate::serve::error::ServeError;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Path, State};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct SchemaItem {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct SchemasResponse {
    pub sources: Vec<SchemaItem>,
    pub sinks: Vec<SchemaItem>,
    pub transforms: Vec<SchemaItem>,
    pub state: Vec<String>,
    /// Config blocks a submitted pipeline can add beside its source, sink, and
    /// transforms; fetch each one's schema from `/v1/schemas/block/{name}`.
    pub blocks: Vec<crate::commands::schema::PipelineBlock>,
}

fn items(pairs: Vec<(&'static str, &'static str)>) -> Vec<SchemaItem> {
    pairs
        .into_iter()
        .map(|(name, description)| SchemaItem {
            name: name.to_string(),
            description: description.to_string(),
        })
        .collect()
}

/// `GET /v1/schemas` → catalog of compiled connectors/transforms + state kinds.
pub async fn list_schemas(State(_state): State<ServerState>) -> Json<SchemasResponse> {
    Json(SchemasResponse {
        sources: items(crate::registry::source_descriptions()),
        sinks: items(crate::registry::sink_descriptions()),
        transforms: items(crate::transforms::transform_descriptions()),
        state: crate::state::available_state_kinds()
            .into_iter()
            .map(String::from)
            .collect(),
        blocks: crate::commands::schema::pipeline_blocks(),
    })
}

/// `GET /v1/schemas/{kind}/{name}` → the JSON Schema for one connector,
/// transform, or pipeline block (`kind` = `source` | `sink` | `transform` | `block`).
/// Unknown kind or name → 404.
pub async fn get_schema(
    State(_state): State<ServerState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<Value>, ServeError> {
    let schema = match kind.as_str() {
        "source" => crate::registry::source_schema(&name),
        "sink" => crate::registry::sink_schema(&name),
        "transform" => crate::transforms::transform_schema(&name),
        "block" => {
            return crate::commands::schema::block_schema(&name)
                .map(Json)
                .ok_or(ServeError::NotFound);
        }
        _ => return Err(ServeError::NotFound),
    }
    .map_err(|_| ServeError::NotFound)?;
    Ok(Json(schema))
}
