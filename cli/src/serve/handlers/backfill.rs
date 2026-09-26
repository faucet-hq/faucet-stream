//! `POST /v1/backfill` — submit a windowed backfill (#282) over the control
//! plane. The range is planned server-side (same pure planner as `faucet
//! backfill`) and **one tracked run is submitted per window unit** through the
//! standard runner path, so every unit gets the full run lifecycle: history
//! record, SSE logs, cancel, `timeout_secs`, cluster pull-balancing, and —
//! when the config carries `shard: { count }` — Mode-B sharding tracked via
//! `shard_progress` (a single wide window becomes one sharded run).
//!
//! Per unit, the submitted document is rewritten: `${backfill.*}` tokens are
//! substituted, the pipeline `name` is suffixed (`{name}-backfill-{unit}`) so
//! unit state keys never touch the forward-sync bookmark, and `delivery` is
//! forced to `at_least_once` (pair with `write_mode: upsert` for idempotent
//! replays). Deterministic idempotency keys (`backfill:{hash}:{unit}`) make
//! re-POSTing the same backfill replay-safe: already-submitted units are
//! replayed, unsubmitted ones proceed — the API-level resume.
//!
//! Bookmark-range backfills (`--from-bookmark`) are CLI-only: they seed
//! scoped state and wrap the source in-process, which a fire-and-forget
//! submission cannot do.

use crate::backfill::plan::{parse_boundary, parse_window, range_hash, substitute_unit_tokens};
use crate::backfill::spec::{has_scoping_tokens, parse_timezone};
use crate::serve::error::ServeError;
use crate::serve::load::load_submission;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, ConfigFormatWire, SubmitRequest};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// `POST /v1/backfill` request body.
#[derive(Debug, Deserialize)]
pub struct BackfillSubmitRequest {
    /// The pipeline config document (same body shape as `POST /v1/runs`).
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// Window start (inclusive): RFC3339 or a date (midnight in `timezone`).
    pub from: String,
    /// Window end (exclusive): RFC3339 or a date.
    pub to: String,
    /// Chunk duration (`45s`, `30m`, `6h`, `1d`, `1w`). Defaults to the
    /// config's `backfill.window`; omitted = one unit for the whole range.
    #[serde(default)]
    pub window: Option<String>,
    /// IANA timezone for date boundaries / `${now.*}` rendering. Defaults to
    /// the config's `backfill.timezone`, else UTC.
    #[serde(default)]
    pub timezone: Option<String>,
    /// Base run name; unit runs are named `{name}-backfill-{unit}`. Defaults
    /// to the config's `name`.
    #[serde(default)]
    pub name: Option<String>,
    /// Labels merged onto every unit run (plus the generated
    /// `backfill` / `backfill_unit` labels).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Per-unit run timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Accepted only so it can be **rejected** with an explanation (#481). One
    /// `POST /v1/backfill` submits one tracked run per window unit, so a single
    /// caller-supplied callback has no single run to attach to — firing it N
    /// times is almost never what the caller means, and silently dropping it
    /// would leave them waiting forever.
    #[serde(default)]
    pub callback: Option<crate::serve::callback::CallbackSpec>,
}

/// One planned unit's submission outcome.
#[derive(Debug, Serialize)]
pub struct BackfillUnitRun {
    pub unit: String,
    pub start: String,
    pub end: String,
    /// `submitted` | `not_submitted` (queue full — re-POST to continue).
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `POST /v1/backfill` success body (202).
#[derive(Debug, Serialize)]
pub struct BackfillSubmitResponse {
    /// Stable range hash — the `backfill` label on every unit run.
    pub backfill: String,
    pub descriptor: String,
    pub planned: usize,
    pub submitted: usize,
    pub units: Vec<BackfillUnitRun>,
}

/// `POST /v1/backfill` → 202 with one tracked run per window unit.
pub async fn submit_backfill(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<BackfillSubmitRequest>,
) -> Result<(StatusCode, Json<BackfillSubmitResponse>), ServeError> {
    // A backfill fans out into one run per window unit, so a single completion
    // callback is ambiguous. Refuse it explicitly rather than dropping it: a
    // caller who set it would otherwise wait on a callback that never arrives.
    if req.callback.is_some() {
        return Err(ServeError::Unprocessable {
            message: "`callback` is not supported on /v1/backfill: this submits one run \
                      per window unit, so there is no single run for a completion callback \
                      to describe. Poll `GET /v1/runs?labels=backfill:<hash>` for unit \
                      status, or submit the units individually via `POST /v1/runs` with a \
                      callback on each"
                .to_string(),
            details: None,
        });
    }

    // Validate the config loads/expands and gate the window scoping exactly
    // like the CLI: every root's source must reference a `${backfill.*}` /
    // `${now.*}` token or each unit would replay identical data.
    let loaded = load_submission(
        &req.config,
        req.config_format.into(),
        state.default_base().as_ref(),
        crate::serve::runner::server_policy(&state).as_deref(),
    )
    .await?;
    let unscoped: Vec<&str> = loaded
        .nodes
        .iter()
        .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
        .filter(|n| !has_scoping_tokens(&n.source.config.to_string()))
        .map(|n| n.id.as_str())
        .collect();
    if !unscoped.is_empty() {
        return Err(ServeError::BadConfig(format!(
            "root row(s) {} are not scoped to the backfill window — their source configs \
             reference no `${{backfill.start}}` / `${{backfill.end}}` / `${{now.*}}` token, \
             so every window would replay identical data (bookmark-positioned backfills \
             are CLI-only: `faucet backfill --from-bookmark`)",
            unscoped.join(", ")
        )));
    }

    let spec = loaded.cfg.backfill.clone().unwrap_or_default();
    let tz = match req.timezone.as_deref().or(spec.timezone.as_deref()) {
        Some(name) => parse_timezone(name).map_err(|e| ServeError::BadConfig(e.to_string()))?,
        None => chrono_tz::Tz::UTC,
    };
    let window = match req.window.as_deref().or(spec.window.as_deref()) {
        Some(w) => Some(parse_window(w).map_err(|e| ServeError::BadConfig(e.to_string()))?),
        None => None,
    };
    let from = parse_boundary(&req.from, tz).map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let to = parse_boundary(&req.to, tz).map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let units = crate::backfill::plan::plan_windows(from, to, window, tz)
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;

    let base_name = req
        .name
        .clone()
        .or_else(|| loaded.cfg.name.clone())
        .unwrap_or_else(|| "pipeline".to_string());
    let descriptor = format!(
        "time|{}|{}|{}|{base_name}",
        from.to_rfc3339(),
        to.to_rfc3339(),
        window
            .map(|w| w.to_string())
            .unwrap_or_else(|| "whole".into()),
    );
    let hash = range_hash(&descriptor);

    // Parse the RAW submitted document once; each unit rewrites a copy. The
    // raw body (not the default-merged config) is submitted so the runner's
    // own merge/validate path applies per unit.
    let doc: Value = serde_yaml::from_str(&req.config)
        .map_err(|e| ServeError::BadConfig(format!("config is not valid YAML/JSON: {e}")))?;

    crate::serve::audit::write(
        &state,
        &actor,
        "backfill.submit",
        None,
        Some(hash.clone()),
        "ok",
    )
    .await;

    let planned = units.len();
    let mut reports = Vec::with_capacity(planned);
    let mut submitted = 0usize;
    let mut queue_full = false;
    for unit in units {
        if queue_full {
            reports.push(BackfillUnitRun {
                unit: unit.id.clone(),
                start: unit.start.to_rfc3339(),
                end: unit.end.to_rfc3339(),
                status: "not_submitted".into(),
                run_id: None,
                error: Some("run queue full — re-POST the same request to continue".into()),
            });
            continue;
        }
        let unit_name = format!("{base_name}-backfill-{}", unit.id);
        let unit_doc = rewrite_unit_doc(&doc, &unit, &unit_name)
            .map_err(|e| ServeError::BadConfig(e.to_string()))?;
        let mut labels = req.labels.clone();
        labels.insert("backfill".into(), hash.clone());
        labels.insert("backfill_unit".into(), unit.id.clone());
        let submit = SubmitRequest {
            config: unit_doc,
            config_format: ConfigFormatWire::Yaml,
            name: Some(unit_name),
            labels,
            timeout_secs: req.timeout_secs,
            doctor_first: false,
            callback: None,
            idempotency_key: Some(format!("backfill:{hash}:{}", unit.id)),
            clock: Some(unit.start.to_rfc3339()),
            concurrency: None,
        };
        match runner::submit(state.clone(), submit, actor.clone()).await {
            Ok(resp) => {
                submitted += 1;
                reports.push(BackfillUnitRun {
                    unit: unit.id.clone(),
                    start: unit.start.to_rfc3339(),
                    end: unit.end.to_rfc3339(),
                    status: "submitted".into(),
                    run_id: Some(resp.run_id),
                    error: None,
                });
            }
            Err(ServeError::QueueFull { .. }) => {
                // Deterministic idempotency keys make the whole request
                // re-POSTable: submitted units replay, the rest submit then.
                queue_full = true;
                reports.push(BackfillUnitRun {
                    unit: unit.id.clone(),
                    start: unit.start.to_rfc3339(),
                    end: unit.end.to_rfc3339(),
                    status: "not_submitted".into(),
                    run_id: None,
                    error: Some("run queue full — re-POST the same request to continue".into()),
                });
            }
            Err(other) => return Err(other),
        }
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(BackfillSubmitResponse {
            backfill: hash,
            descriptor,
            planned,
            submitted,
            units: reports,
        }),
    ))
}

/// Rewrite the submitted document for one unit: substitute `${backfill.*}`
/// tokens across the whole document, namespace the pipeline `name` (so unit
/// state keys are `{name}-backfill-{unit}::…`, never the live keys), and
/// force `delivery: at_least_once`. Pure.
fn rewrite_unit_doc(
    doc: &Value,
    unit: &crate::backfill::plan::BackfillUnit,
    unit_name: &str,
) -> crate::error::CliResult<String> {
    let mut d = doc.clone();
    substitute_unit_tokens(&mut d, unit)?;
    if let Some(map) = d.as_object_mut() {
        map.insert("name".into(), Value::String(unit_name.to_string()));
        map.insert("delivery".into(), Value::String("at_least_once".into()));
    }
    serde_yaml::to_string(&d)
        .map_err(|e| crate::error::CliError::Internal(format!("unit config render: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rewrite_substitutes_namespaces_and_forces_at_least_once() {
        let utc: chrono_tz::Tz = "UTC".parse().unwrap();
        let unit = crate::backfill::plan::BackfillUnit {
            id: "20260601T000000Z".into(),
            start: parse_boundary("2026-06-01T00:00:00Z", utc).unwrap(),
            end: parse_boundary("2026-06-02T00:00:00Z", utc).unwrap(),
        };
        let doc = json!({
            "version": 1,
            "name": "orders",
            "delivery": "exactly_once",
            "pipeline": {
                "source": {"type": "rest", "config": {"url": "https://x/o?s=${backfill.start}&e=${backfill.end}"}},
                "sink": {"type": "jsonl", "config": {"path": "./out-${backfill.start_date}.jsonl"}}
            }
        });
        let out = rewrite_unit_doc(&doc, &unit, "orders-backfill-20260601T000000Z").unwrap();
        let back: Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(back["name"], "orders-backfill-20260601T000000Z");
        assert_eq!(back["delivery"], "at_least_once");
        let url = back["pipeline"]["source"]["config"]["url"]
            .as_str()
            .unwrap();
        assert!(url.contains("s=2026-06-01T00:00:00+00:00"), "{url}");
        assert!(url.contains("e=2026-06-02T00:00:00+00:00"), "{url}");
        assert_eq!(
            back["pipeline"]["sink"]["config"]["path"],
            "./out-2026-06-01.jsonl"
        );
        // The rewritten document is itself a loadable pipeline config.
        crate::config::parse_with_extension(&out, "yaml").expect("unit doc parses");
    }

    #[test]
    fn rewrite_rejects_unknown_token() {
        let utc: chrono_tz::Tz = "UTC".parse().unwrap();
        let unit = crate::backfill::plan::BackfillUnit {
            id: "u".into(),
            start: parse_boundary("2026-06-01", utc).unwrap(),
            end: parse_boundary("2026-06-02", utc).unwrap(),
        };
        let doc = json!({"pipeline": {"source": {"config": {"q": "${backfill.oops}"}}}});
        assert!(rewrite_unit_doc(&doc, &unit, "n").is_err());
    }
}
