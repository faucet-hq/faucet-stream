//! `POST/PUT /v1/triggers/{name}` — the webhook trigger endpoint. Looks the name
//! up in the `TriggersHandle` webhook table, checks the method allowlist, applies
//! a leading-edge debounce (coalesce fires that arrive within `debounce_secs` of
//! the last accepted fire), builds a `TriggerEvent::Webhook`, and fires. Bearer
//! auth is inherited from the `/v1` route_layer.

use super::context::TriggerEvent;
use super::enqueue::{self, FireOutcome};
use crate::serve::error::ServeError;
use crate::serve::state::ServerState;
use crate::serve::triggers::spec::TriggerKind;
use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use serde_json::json;
use std::collections::BTreeMap;

pub async fn handle(
    State(state): State<ServerState>,
    Path(name): Path<String>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: String,
) -> Result<(StatusCode, Json<serde_json::Value>), ServeError> {
    let compiled = state
        .triggers()
        .webhook(&name)
        .ok_or(ServeError::NotFound)?;

    let (methods, dedupe_header, debounce_secs) = match &compiled.spec.kind {
        TriggerKind::Webhook {
            methods,
            dedupe_header,
            debounce_secs,
        } => (methods, dedupe_header, *debounce_secs),
        _ => return Err(ServeError::NotFound),
    };
    let m = method.as_str().to_ascii_uppercase();
    if !methods
        .iter()
        .any(|allowed| allowed.to_ascii_uppercase() == m)
    {
        return Err(ServeError::BadConfig(format!(
            "method {m} not allowed for webhook trigger '{name}'"
        )));
    }

    // Leading-edge debounce: coalesce fires that arrive within `debounce_secs` of
    // the last accepted fire for this trigger. The first fire (and any after the
    // window has fully elapsed) is accepted; the rest return `coalesced`.
    if debounce_secs > 0 {
        let now_ms = chrono::Utc::now().timestamp_millis();
        if !state
            .triggers()
            .allow_fire(&name, (debounce_secs as i64) * 1000, now_ms)
        {
            crate::serve::triggers::metrics::coalesced(&name);
            return Ok((StatusCode::OK, Json(json!({ "status": "coalesced" }))));
        }
    }

    // Idempotency key: dedupe header value, else a per-request UUID.
    let idem = dedupe_header
        .as_ref()
        .and_then(|h| headers.get(h.as_str()))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let header_map: BTreeMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|val| (k.as_str().to_ascii_lowercase(), val.to_string()))
        })
        .collect();
    let query_map = parse_query(raw_query.as_deref());

    let event = TriggerEvent::Webhook {
        method: m,
        body,
        headers: header_map,
        query: query_map,
        idem,
    };
    let fired_at = chrono::Utc::now().to_rfc3339();
    match enqueue::fire(&state, &compiled, event, &fired_at).await {
        FireOutcome::Enqueued(run_id) => {
            state.triggers().record_ok(&name, Some(fired_at));
            Ok((
                StatusCode::ACCEPTED,
                Json(json!({ "run_id": run_id, "status": "queued" })),
            ))
        }
        FireOutcome::Coalesced => Ok((StatusCode::OK, Json(json!({ "status": "coalesced" })))),
        FireOutcome::Dropped(reason) => {
            tracing::warn!(trigger = %name, %reason, "webhook fire dropped");
            Err(ServeError::QueueFull {
                retry_after_secs: 5,
            })
        }
        FireOutcome::Error(msg) | FireOutcome::Failed(msg) => {
            state
                .triggers()
                .record_err(&name, msg.clone(), super::watcher::UNHEALTHY_THRESHOLD);
            Err(ServeError::Internal(msg))
        }
    }
}

fn parse_query(raw: Option<&str>) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if let Some(q) = raw {
        for pair in q.split('&').filter(|s| !s.is_empty()) {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or_default().to_string();
            let v = it.next().unwrap_or_default().to_string();
            m.insert(k, v);
        }
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_query_pairs() {
        let q = parse_query(Some("mode=full&tenant=acme"));
        assert_eq!(q.get("mode").map(String::as_str), Some("full"));
        assert_eq!(q.get("tenant").map(String::as_str), Some("acme"));
        assert!(parse_query(None).is_empty());
    }
}
