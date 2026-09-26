//! `GET /v1/runs/{id}/logs` — a run's captured log lines.
//!
//! Default (no `format`): a Server-Sent Events stream that replays the ephemeral
//! ring then forwards the live tail until the run ends. With `?format=jsonl` or
//! `?format=text` (#529): the **persisted** logs from the history backend,
//! paginated with `?after=<seq>&limit=<n>` — fetchable any time after the run
//! ends, past the SSE drain window. Thin glue over [`crate::serve::logs`] and
//! `RunHistory::list_run_logs`.

use crate::serve::error::ServeError;
use crate::serve::logs::{LogEvent, log_events};
use crate::serve::state::ServerState;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use futures::StreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::broadcast;

/// Keep-alive comment interval — defeats idle-timeout proxies on a quiet stream.
const KEEP_ALIVE_SECS: u64 = 15;

/// Default / maximum page size for the persisted-log read.
const DEFAULT_LOG_LIMIT: usize = 1_000;
const MAX_LOG_LIMIT: usize = 10_000;

/// Query params for `GET /v1/runs/{id}/logs`.
#[derive(Debug, Deserialize)]
pub struct LogQuery {
    /// `jsonl` / `text` select the persisted read; absent = the SSE stream.
    #[serde(default)]
    pub format: Option<String>,
    /// Return only lines with `seq > after` (persisted read pagination).
    #[serde(default)]
    pub after: Option<u64>,
    /// Max lines per page (persisted read), capped at `MAX_LOG_LIMIT`.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `GET /v1/runs/{id}/logs`.
///
/// - Default → `text/event-stream`: `event: log` / `truncated` / `end`.
/// - `?format=jsonl` → `application/x-ndjson`, one `{seq,ts,level,line}` per line,
///   oldest-first, `?after`/`?limit` paginated; a trailing `{"truncated":true}`
///   record when earlier lines were dropped by the per-run cap.
/// - `?format=text` → `text/plain`, the lines concatenated.
///
/// 404 if the run is entirely unknown.
pub async fn stream_logs(
    State(state): State<ServerState>,
    axum::extract::Extension(actor): axum::extract::Extension<crate::serve::rbac::AuthContext>,
    Path(id): Path<String>,
    Query(q): Query<LogQuery>,
) -> Result<Response, ServeError> {
    crate::serve::handlers::runs::ensure_visible(&state, &actor, &id).await?;
    match q.format.as_deref() {
        None => stream_logs_sse(state, id)
            .await
            .map(IntoResponse::into_response),
        Some("jsonl") | Some("text") => persisted_logs(state, id, q).await,
        Some(other) => Err(ServeError::BadConfig(format!(
            "unknown log format '{other}'; use 'jsonl' or 'text' (or omit for the SSE stream)"
        ))),
    }
}

/// The persisted (durable) log read (#529).
async fn persisted_logs(
    state: ServerState,
    id: String,
    q: LogQuery,
) -> Result<Response, ServeError> {
    // A completely unknown run is a 404 (mirrors the SSE path).
    let known = state
        .history()
        .get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .is_some();
    if !known {
        return Err(ServeError::NotFound);
    }
    let limit = q.limit.unwrap_or(DEFAULT_LOG_LIMIT).clamp(1, MAX_LOG_LIMIT);
    let page = state
        .history()
        .list_run_logs(&id, q.after, limit)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;

    if q.format.as_deref() == Some("text") {
        let mut body = String::new();
        for l in &page.lines {
            body.push_str(&l.line);
            body.push('\n');
        }
        if page.truncated {
            body.push_str("… (earlier lines truncated: per-run cap reached)\n");
        }
        return Ok((
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            body,
        )
            .into_response());
    }

    // jsonl (NDJSON): one JSON object per line, then a trailing truncation marker.
    let mut body = String::new();
    for l in &page.lines {
        body.push_str(&serde_json::to_string(l).unwrap_or_default());
        body.push('\n');
    }
    if page.truncated {
        body.push_str(r#"{"truncated":true}"#);
        body.push('\n');
    }
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    )
        .into_response())
}

/// The live SSE stream (unchanged behavior).
async fn stream_logs_sse(
    state: ServerState,
    id: String,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>>, ServeError> {
    let (snapshot, rx, ended) = match state.log_hub().reader(&id) {
        Some(reader) => reader,
        None => {
            // No buffer: 404 for an unknown run, or an immediate `end` for a known
            // run whose logs have already been dropped after the drain window.
            let known = state
                .history()
                .get(&id)
                .await
                .map_err(|e| ServeError::Internal(e.to_string()))?
                .is_some();
            if !known {
                return Err(ServeError::NotFound);
            }
            // A dropped sender's receiver is never polled (ended == true).
            (Vec::new(), broadcast::channel(1).1, true)
        }
    };

    let stream = log_events(snapshot, rx, ended)
        .map(|ev| Ok::<Event, std::convert::Infallible>(to_sse_event(ev)));
    Ok(
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(KEEP_ALIVE_SECS))),
    )
}

/// Map an internal [`LogEvent`] to an SSE wire event.
fn to_sse_event(ev: LogEvent) -> Event {
    match ev {
        LogEvent::Log(line) => Event::default().event("log").data(line),
        LogEvent::Truncated(n) => Event::default().event("truncated").data(format!(
            "{n} log line(s) dropped; rely on the persisted logs (?format=jsonl) or the centralized log sink"
        )),
        LogEvent::End => Event::default().event("end").data("done"),
    }
}
