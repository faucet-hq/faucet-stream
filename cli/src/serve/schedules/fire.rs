//! Firing one tick of one schedule (#1044): one run of the schedule's
//! template — or one per tenant for a fan-out — submitted through the same
//! template path as `POST /v1/templates/{id}/runs`, so loading, policy,
//! budgets, tenant admission and governance all apply.
//!
//! The idempotency key is the *scheduled* tick (`sched:<name>:<tick>`, plus
//! `:<tenant>` for a fan-out), so every cluster member firing the same tick
//! derives the same key and the shared idempotency claim lets exactly one run
//! through.

use crate::serve::error::ServeError;
use crate::serve::history::schedules::{OverlapMode, ScheduleSpec};
use crate::serve::history::{ListFilter, RunStatus, labels::LabelFilter};
use crate::serve::state::ServerState;
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::BTreeMap;

/// The run label naming the schedule that started a run.
pub const LABEL_SCHEDULE: &str = "schedule";
/// The run label carrying the tick (RFC 3339 UTC) that started a run.
pub const LABEL_TICK: &str = "schedule_tick";

/// What firing a tick should do beyond the schedule's own definition.
#[derive(Debug, Clone, Default)]
pub struct FireOptions {
    /// A run started by hand (`POST /v1/schedules/{name}/run`): its own
    /// idempotency key, and the overlap rule does not apply.
    pub manual: Option<String>,
    /// Defined in the `--schedules` file: its params are the operator's. A
    /// schedule made through the API carries its creator's params, held to
    /// the caller reference policy like a template run (#1051).
    pub from_file: bool,
    /// Made (or last changed) through an approved change request.
    pub approved: bool,
    /// The stored schedule's id and owner, which its sealed params open
    /// under; looked up when unset.
    pub schedule_id: Option<String>,
}

/// Fan-out targets submitted side by side.
const CONCURRENT_TARGETS: usize = 8;

/// One target's result.
#[derive(Debug, Clone, PartialEq)]
pub enum TargetResult {
    /// A run started (or another member already started it for this key).
    Started(String),
    /// Nothing started on purpose (an earlier run still going, a tenant at
    /// its concurrency limit).
    Skipped(String),
    /// Refused for a reason a retry cannot fix.
    Failed(String),
    /// A transient error (queue full, the store unreachable): retry the tick.
    Retry(String),
    /// `overlap: queue`: an earlier run is still going; start once it ends.
    Wait(String),
}

/// The result of one tick, per target (`None` = the server itself).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickResult {
    pub targets: Vec<(Option<String>, TargetResult)>,
}

impl TickResult {
    /// Whether the tick must be retried: some target hit a transient error.
    /// Retrying is replay-safe (per-target idempotency keys).
    pub fn retry(&self) -> Option<String> {
        let errors: Vec<String> = self
            .targets
            .iter()
            .filter_map(|(t, r)| match r {
                TargetResult::Retry(e) => Some(prefixed(t, e)),
                _ => None,
            })
            .collect();
        (!errors.is_empty()).then(|| errors.join("; "))
    }

    /// Whether the tick is held for an earlier run (`overlap: queue`).
    pub fn waiting(&self) -> Option<String> {
        let waits: Vec<String> = self
            .targets
            .iter()
            .filter_map(|(t, r)| match r {
                TargetResult::Wait(e) => Some(prefixed(t, e)),
                _ => None,
            })
            .collect();
        (!waits.is_empty()).then(|| waits.join("; "))
    }

    pub fn run_ids(&self) -> Vec<String> {
        self.targets
            .iter()
            .filter_map(|(_, r)| match r {
                TargetResult::Started(id) => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn failures(&self) -> Vec<String> {
        self.targets
            .iter()
            .filter_map(|(t, r)| match r {
                TargetResult::Failed(e) => Some(prefixed(t, e)),
                _ => None,
            })
            .collect()
    }

    pub fn skipped(&self) -> Vec<String> {
        self.targets
            .iter()
            .filter_map(|(t, r)| match r {
                TargetResult::Skipped(e) => Some(prefixed(t, e)),
                _ => None,
            })
            .collect()
    }

    /// The outcome the status record and metrics count: started when any
    /// run started, else failed when any target failed, else skipped.
    pub fn outcome(&self) -> super::status::Outcome {
        use super::status::Outcome;
        if let Some(e) = self.retry() {
            return Outcome::Retrying(e);
        }
        if let Some(w) = self.waiting() {
            return Outcome::Waiting(w);
        }
        let failures = self.failures();
        if !self.run_ids().is_empty() {
            if failures.is_empty() {
                Outcome::Started
            } else {
                // Some tenants ran and some were refused: the refusals are
                // the tick's story.
                Outcome::Failed(failures)
            }
        } else if !failures.is_empty() {
            Outcome::Failed(failures)
        } else if self.targets.is_empty() {
            Outcome::Skipped("no tenants to run for".into())
        } else {
            Outcome::Skipped(self.skipped().join("; "))
        }
    }
}

fn prefixed(tenant: &Option<String>, e: &str) -> String {
    match tenant {
        Some(t) => format!("{t}: {e}"),
        None => e.to_string(),
    }
}

/// RFC 3339 with whole seconds and `Z`.
pub fn tick_text(tick: DateTime<Utc>) -> String {
    tick.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// The idempotency key for one target of a tick.
pub fn idempotency_key(
    name: &str,
    tick: &str,
    manual: Option<&str>,
    tenant: Option<&str>,
) -> String {
    let base = match manual {
        Some(m) => format!("sched:{name}:manual:{m}"),
        None => format!("sched:{name}:{tick}"),
    };
    match tenant {
        Some(t) => format!("{base}:{t}"),
        None => base,
    }
}

/// The template run body for one target. Pure.
pub fn body(
    spec: &ScheduleSpec,
    tick: DateTime<Utc>,
    fired_at: DateTime<Utc>,
    opts: &FireOptions,
    tenant: Option<&str>,
) -> Result<crate::serve::handlers::templates::TriggerBody, String> {
    use crate::serve::history::templates::VersionSelector;
    let selector = |field: &str, v: &Option<String>| -> Result<Option<VersionSelector>, String> {
        v.as_ref()
            .map(|s| {
                serde_json::from_value(serde_json::Value::String(s.clone()))
                    .map_err(|e| format!("template {field} '{s}': {e}"))
            })
            .transpose()
    };
    let name = spec.name.as_str();
    let tick_s = tick_text(tick);
    let fired_s = tick_text(fired_at);
    let params: BTreeMap<String, serde_json::Value> = spec
        .template
        .params
        .iter()
        .map(|(k, v)| {
            let v = match v {
                serde_json::Value::String(s) => serde_json::Value::String(
                    super::validate::substitute(s, name, &tick_s, &fired_s),
                ),
                other => other.clone(),
            };
            (k.clone(), v)
        })
        .collect();
    let mut labels = spec.run.labels.clone();
    labels.insert(LABEL_SCHEDULE.into(), name.to_string());
    labels.insert(LABEL_TICK.into(), tick_s.clone());
    let run_name = spec
        .run
        .name
        .as_deref()
        .map(|n| {
            super::validate::substitute(n, name, &tick_s, &fired_s)
                .replace("{name}", name)
                .replace("{tick}", &tick_s)
        })
        .unwrap_or_else(|| name.to_string());
    Ok(crate::serve::handlers::templates::TriggerBody {
        params,
        version: selector("version", &spec.template.version)?,
        sink: spec.template.sink.clone(),
        sink_version: selector("sink_version", &spec.template.sink_version)?,
        overlay: spec
            .template
            .overlay
            .as_ref()
            .map(|o| crate::serve::handlers::templates::OverlayRef::Id(o.clone())),
        overlay_version: selector("overlay_version", &spec.template.overlay_version)?,
        name: Some(run_name),
        labels,
        timeout_secs: spec.run.timeout_secs,
        idempotency_key: Some(idempotency_key(
            name,
            &tick_s,
            opts.manual.as_deref(),
            tenant,
        )),
        selection: spec.run.selection.clone(),
        pool: spec.run.pool.clone(),
        // A schedule is a standing approval: it was created (and, under
        // `--require-approval`, approved) once; `fire_target` refuses one
        // that needs an approval it does not have.
        standing_approval: true,
        operator_params: opts.from_file,
        ..Default::default()
    })
}

/// Who a target's run is made as: the schedule, scoped to the tenant.
pub fn actor(name: &str, tenant: Option<&str>) -> crate::serve::rbac::AuthContext {
    crate::serve::rbac::AuthContext {
        principal: format!("schedule:{name}"),
        role: crate::serve::rbac::Role::Operator,
        source_ip: None,
        tenant: tenant.map(str::to_string),
    }
}

/// Map a submission error to a target result.
pub fn classify(e: ServeError, tenant: Option<&str>) -> TargetResult {
    match e {
        // The idempotency key already started a run for another payload:
        // another member fired this tick with an older definition.
        ServeError::Conflict(m) if m == crate::serve::runner::IDEMPOTENCY_CONFLICT => {
            TargetResult::Started("(already fired by another server)".into())
        }
        // Any other conflict (a tenant connection missing or needing
        // re-authorization) is the target's own refusal.
        ServeError::Conflict(m) => TargetResult::Failed(m),
        ServeError::TooManyRequests(m) if tenant.is_some() => {
            TargetResult::Skipped(format!("tenant at its max_concurrent_runs limit ({m})"))
        }
        // Only what passes with time is retried: a full queue, a shutdown,
        // an unreachable store. Anything else fails this tick and the
        // schedule moves on (a retry would hit the same error forever).
        ServeError::QueueFull { .. } => TargetResult::Retry("the run queue is full".into()),
        ServeError::Draining => TargetResult::Retry("the server is shutting down".into()),
        ServeError::TooManyRequests(m) => TargetResult::Retry(m),
        e @ ServeError::Unavailable(_) => TargetResult::Retry(e.api_error().error.message),
        ServeError::NotFound => TargetResult::Failed("the template was not found".to_string()),
        other => TargetResult::Failed(other.api_error().error.message),
    }
}

/// The refusal of a tick that would start runs without an approval this
/// server requires (#1051): `--require-approval run` covers schedules made
/// through the API unless an approved change request made them, and then
/// only while they name pinned template versions, so an approved schedule
/// runs exactly what was approved.
pub fn approval_missing(
    state: &ServerState,
    spec: &ScheduleSpec,
    opts: &FireOptions,
) -> Option<String> {
    if !state.requires_approval(crate::serve::changes::ChangeKind::Run) || opts.from_file {
        return None;
    }
    if !opts.approved {
        return Some(
            "this server requires approval for runs (--require-approval run), and this schedule \
             was not made through an approved change request; propose an update of it with \
             POST /v1/changes (kind `schedule`) to approve it"
                .to_string(),
        );
    }
    follows_a_channel(spec).map(|field| {
        format!(
            "this server requires approval for runs (--require-approval run), and this schedule \
             follows a channel ({field}) that a later launch can move without an approval; \
             pin a version and approve the change"
        )
    })
}

/// The first template selector of `spec` that names a channel, not a
/// numbered version: what a launch can change under an approved schedule.
pub fn follows_a_channel(spec: &ScheduleSpec) -> Option<&'static str> {
    let pinned = |v: &Option<String>| v.as_deref().is_some_and(|s| s.parse::<u32>().is_ok());
    let t = &spec.template;
    if !pinned(&t.version) {
        return Some("template.version");
    }
    if t.sink.is_some() && !pinned(&t.sink_version) {
        return Some("template.sink_version");
    }
    if t.overlay.is_some() && !pinned(&t.overlay_version) {
        return Some("template.overlay_version");
    }
    None
}

/// The targets of a tick: the owner, the fan-out's tenants, or the server.
async fn targets(
    state: &ServerState,
    spec: &ScheduleSpec,
    owner: Option<&str>,
) -> Result<Vec<Option<String>>, String> {
    if let Some(t) = owner {
        return Ok(vec![Some(t.to_string())]);
    }
    let Some(selector) = &spec.tenants else {
        return Ok(vec![None]);
    };
    #[cfg(feature = "tenants")]
    {
        let (run, skipped) = crate::serve::handlers::tenants::resolve_tenants(state, selector)
            .await
            .map_err(|e| e.api_error().error.message)?;
        for s in skipped {
            tracing::info!(
                schedule = %spec.name,
                tenant = %s.tenant,
                reason = s.reason.as_deref().unwrap_or(""),
                "tenant skipped"
            );
        }
        Ok(run.into_iter().map(Some).collect())
    }
    #[cfg(not(feature = "tenants"))]
    {
        let _ = (state, selector);
        Err("`tenants:` needs a build with the `tenants` feature".into())
    }
}

/// Runs of one schedule looked at for an earlier one still going.
const OVERLAP_SCAN: usize = 50;

/// Whether a run an *earlier* tick of this schedule started for `tenant` is
/// still queued or running. A run of `tick` itself (started by another
/// cluster member, or by this tick before a retry) is not an earlier run.
pub async fn earlier_run_going(
    state: &ServerState,
    name: &str,
    tenant: Option<&str>,
    tick: &str,
) -> Result<Option<String>, String> {
    let filter = ListFilter {
        status: vec![
            RunStatus::Queued,
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Sharded,
        ],
        tenant: tenant.map(str::to_string),
        labels: vec![LabelFilter {
            key: LABEL_SCHEDULE.into(),
            value: Some(name.to_string()),
        }],
        limit: OVERLAP_SCAN,
        ..Default::default()
    };
    let page = state
        .history()
        .list(&filter)
        .await
        .map_err(|e| e.to_string())?;
    Ok(page
        .runs
        .into_iter()
        .find(|r| r.labels.get(LABEL_TICK).map(String::as_str) != Some(tick))
        .map(|r| r.run_id))
}

/// `spec` with its sealed params opened under the stored schedule's id and
/// owner (#1051).
async fn open_params(
    state: &ServerState,
    spec: &ScheduleSpec,
    opts: &FireOptions,
) -> Result<ScheduleSpec, TargetResult> {
    if !spec.template.params.values().any(super::secrets::is_sealed) {
        return Ok(spec.clone());
    }
    let record = match state.history().schedule_get(&spec.name).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return Err(TargetResult::Failed(
                "the schedule is no longer stored".into(),
            ));
        }
        Err(e) => return Err(classify(ServeError::from(e), None)),
    };
    if opts.schedule_id.as_ref().is_some_and(|id| *id != record.id) {
        return Err(TargetResult::Failed(
            "the schedule was replaced since this tick was planned".into(),
        ));
    }
    let scope = super::secrets::Scope {
        id: &record.id,
        tenant: record.tenant.as_deref(),
    };
    super::secrets::opened(state, spec, scope).map_err(TargetResult::Failed)
}

/// Fire one tick. `owner` is the tenant that owns the schedule, if any.
pub async fn fire_tick(
    state: &ServerState,
    spec: &ScheduleSpec,
    owner: Option<&str>,
    tick: DateTime<Utc>,
    fired_at: DateTime<Utc>,
    opts: &FireOptions,
) -> TickResult {
    let targets = match targets(state, spec, owner).await {
        Ok(t) => t,
        Err(e) => {
            return TickResult {
                targets: vec![(None, TargetResult::Retry(e))],
            };
        }
    };
    // Sealed params are opened once per tick, for every target.
    let spec = match open_params(state, spec, opts).await {
        Ok(s) => s,
        Err(e) => {
            return TickResult {
                targets: targets.into_iter().map(|t| (t, e.clone())).collect(),
            };
        }
    };
    let spec = &spec;
    // A fan-out's tenants are submitted side by side (bounded), in order.
    use futures::StreamExt;
    let results: Vec<(Option<String>, TargetResult)> = futures::stream::iter(targets)
        .map(|tenant| async move {
            let result = fire_target(state, spec, tenant.as_deref(), tick, fired_at, opts).await;
            (tenant, result)
        })
        .buffered(CONCURRENT_TARGETS)
        .collect()
        .await;
    for (tenant, result) in &results {
        if let (Some(t), TargetResult::Failed(_) | TargetResult::Skipped(_)) = (tenant, result) {
            super::metrics::tenant_failed(&spec.name, t);
        }
    }
    TickResult { targets: results }
}

async fn fire_target(
    state: &ServerState,
    spec: &ScheduleSpec,
    tenant: Option<&str>,
    tick: DateTime<Utc>,
    fired_at: DateTime<Utc>,
    opts: &FireOptions,
) -> TargetResult {
    if let Some(why) = approval_missing(state, spec, opts) {
        return TargetResult::Failed(why);
    }
    if opts.manual.is_none() {
        match earlier_run_going(state, &spec.name, tenant, &tick_text(tick)).await {
            Ok(Some(run)) => {
                let going = format!("run {run} from an earlier tick is still going");
                return match spec.overlap {
                    OverlapMode::Skip => TargetResult::Skipped(going),
                    OverlapMode::Queue => TargetResult::Wait(going),
                    OverlapMode::Forbid => {
                        TargetResult::Failed(format!("{going} (overlap: forbid)"))
                    }
                };
            }
            Ok(None) => {}
            Err(e) => return TargetResult::Retry(format!("checking for an earlier run: {e}")),
        }
    }
    let body = match body(spec, tick, fired_at, opts, tenant) {
        Ok(b) => b,
        Err(e) => return TargetResult::Failed(e),
    };
    let principal =
        crate::runtime::Principal::new(actor(&spec.name, tenant), crate::runtime::Origin::Trusted);
    use crate::runtime::server::{TriggerOutcome, trigger_template};
    match trigger_template(state.clone(), principal, spec.template.id.clone(), body).await {
        Ok(TriggerOutcome::Run(r)) => TargetResult::Started(r.run.run_id),
        Ok(TriggerOutcome::PendingApproval(c)) => TargetResult::Started(format!("change:{}", c.id)),
        Err(e) => classify(e, tenant),
    }
}

#[cfg(test)]
mod tests {
    use super::super::status::Outcome;
    use super::*;
    use serde_json::json;

    fn spec() -> ScheduleSpec {
        serde_json::from_value(json!({
            "name": "nightly",
            "cron": "0 2 * * *",
            "template": {
                "id": "acme/billing",
                "version": "prod",
                "sink": "pg",
                "sink_version": "3",
                "overlay": "eu",
                "overlay_version": "stable",
                "params": {"since": "${schedule.tick}", "n": 3}
            },
            "run": {"name": "{name} at ${schedule.tick}", "labels": {"team": "billing"}, "timeout_secs": 60, "pool": "big"}
        }))
        .unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn the_body_carries_the_tick_params_labels_and_key() {
        let tick = at("2026-11-05T02:00:00Z");
        let b = body(
            &spec(),
            tick,
            at("2026-11-05T02:00:01Z"),
            &FireOptions::default(),
            Some("acme"),
        )
        .unwrap();
        assert_eq!(b.params["since"], json!("2026-11-05T02:00:00Z"));
        assert_eq!(b.params["n"], json!(3));
        assert_eq!(b.labels[LABEL_SCHEDULE], "nightly");
        assert_eq!(b.labels[LABEL_TICK], "2026-11-05T02:00:00Z");
        assert_eq!(b.labels["team"], "billing");
        assert_eq!(b.name.as_deref(), Some("nightly at 2026-11-05T02:00:00Z"));
        assert_eq!(
            b.idempotency_key.as_deref(),
            Some("sched:nightly:2026-11-05T02:00:00Z:acme")
        );
        assert_eq!(b.timeout_secs, Some(60));
        assert_eq!(b.pool.as_deref(), Some("big"));
        assert!(b.standing_approval);
        // A schedule made through the API carries its creator's params.
        assert!(!b.operator_params);
        let file = FireOptions {
            from_file: true,
            ..Default::default()
        };
        assert!(
            body(&spec(), tick, tick, &file, None)
                .unwrap()
                .operator_params
        );
        assert_eq!(b.sink.as_deref(), Some("pg"));
        assert!(b.version.is_some() && b.sink_version.is_some() && b.overlay_version.is_some());
        let manual = FireOptions {
            manual: Some("u1".into()),
            ..Default::default()
        };
        let m = body(&spec(), tick, tick, &manual, None).unwrap();
        assert_eq!(
            m.idempotency_key.as_deref(),
            Some("sched:nightly:manual:u1")
        );
        let mut plain = spec();
        plain.run.name = None;
        assert_eq!(
            body(&plain, tick, tick, &manual, None)
                .unwrap()
                .name
                .as_deref(),
            Some("nightly")
        );
        let mut bad = spec();
        bad.template.version = Some("latest".into());
        assert!(
            body(&bad, tick, tick, &manual, None)
                .unwrap_err()
                .contains("template version")
        );
    }

    #[test]
    fn errors_map_to_retry_skip_or_fail() {
        assert!(matches!(
            classify(
                ServeError::Conflict(crate::serve::runner::IDEMPOTENCY_CONFLICT.into()),
                None
            ),
            TargetResult::Started(_)
        ));
        assert!(matches!(
            classify(
                ServeError::Conflict("connection needs re-auth".into()),
                Some("acme")
            ),
            TargetResult::Failed(_)
        ));
        assert!(matches!(
            classify(ServeError::TooManyRequests("limit".into()), Some("acme")),
            TargetResult::Skipped(_)
        ));
        assert!(matches!(
            classify(ServeError::TooManyRequests("limit".into()), None),
            TargetResult::Retry(_)
        ));
        assert!(matches!(
            classify(
                ServeError::QueueFull {
                    retry_after_secs: 1
                },
                None
            ),
            TargetResult::Retry(_)
        ));
        assert!(matches!(
            classify(ServeError::Draining, None),
            TargetResult::Retry(_)
        ));
        assert_eq!(
            classify(ServeError::NotFound, None),
            TargetResult::Failed("the template was not found".into())
        );
        assert!(matches!(
            classify(ServeError::BadConfig("x".into()), None),
            TargetResult::Failed(_)
        ));
        // Only transient errors retry; a deterministic one fails the tick.
        assert!(matches!(
            classify(ServeError::Internal("x".into()), None),
            TargetResult::Failed(_)
        ));
        assert!(matches!(
            classify(ServeError::Unavailable("store down".into()), None),
            TargetResult::Retry(_)
        ));
        assert!(matches!(
            classify(ServeError::Forbidden("no".into()), None),
            TargetResult::Failed(_)
        ));
    }

    fn pinned() -> ScheduleSpec {
        let mut s = spec();
        s.template.version = Some("4".into());
        s.template.overlay_version = Some("2".into());
        s
    }

    #[test]
    fn a_schedule_needs_an_approval_when_runs_do() {
        let state = crate::serve::test_support::test_state();
        assert!(approval_missing(&state, &spec(), &FireOptions::default()).is_none());
        let mut config = crate::serve::test_support::test_config();
        config.require_approval = vec![crate::serve::changes::ChangeKind::Run];
        let gated = crate::serve::test_support::state_from(&config);
        let why = approval_missing(&gated, &pinned(), &FireOptions::default()).unwrap();
        assert!(why.contains("--require-approval run"), "{why}");
        let approved = FireOptions {
            approved: true,
            ..Default::default()
        };
        let from_file = FireOptions {
            from_file: true,
            ..Default::default()
        };
        assert!(approval_missing(&gated, &spec(), &from_file).is_none());
        assert!(approval_missing(&gated, &pinned(), &approved).is_none());
        // Approved, but a launch could move what it runs.
        let why = approval_missing(&gated, &spec(), &approved).unwrap();
        assert!(why.contains("template.version"), "{why}");
        let mut sink = pinned();
        sink.template.sink_version = None;
        assert_eq!(follows_a_channel(&sink), Some("template.sink_version"));
        let mut overlay = pinned();
        overlay.template.overlay_version = Some("stable".into());
        assert_eq!(
            follows_a_channel(&overlay),
            Some("template.overlay_version")
        );
        assert_eq!(follows_a_channel(&pinned()), None);
    }

    #[tokio::test]
    async fn a_run_of_the_same_tick_is_not_an_earlier_run() {
        let state = crate::serve::test_support::test_state();
        let labels = |tick: &str| {
            BTreeMap::from([
                (LABEL_SCHEDULE.to_string(), "nightly".to_string()),
                (LABEL_TICK.to_string(), tick.to_string()),
            ])
        };
        let same = crate::serve::history::RunRecord::queued(
            "same".into(),
            None,
            labels("2026-11-05T02:00:00Z"),
            None,
            Utc::now(),
        );
        state.history().upsert(&same).await.unwrap();
        let tick = "2026-11-05T02:00:00Z";
        assert_eq!(
            earlier_run_going(&state, "nightly", None, tick)
                .await
                .unwrap(),
            None,
            "another member already started this tick"
        );
        let earlier = crate::serve::history::RunRecord::queued(
            "earlier".into(),
            None,
            labels("2026-11-05T01:55:00Z"),
            None,
            Utc::now(),
        );
        state.history().upsert(&earlier).await.unwrap();
        assert_eq!(
            earlier_run_going(&state, "nightly", None, tick)
                .await
                .unwrap()
                .as_deref(),
            Some("earlier")
        );
    }

    #[test]
    fn a_tick_result_folds_into_one_outcome() {
        let r = |targets: Vec<(Option<&str>, TargetResult)>| TickResult {
            targets: targets
                .into_iter()
                .map(|(t, r)| (t.map(str::to_string), r))
                .collect(),
        };
        assert_eq!(
            r(vec![(None, TargetResult::Started("a".into()))]).outcome(),
            Outcome::Started
        );
        assert_eq!(
            r(vec![(Some("acme"), TargetResult::Retry("q".into()))]).outcome(),
            Outcome::Retrying("acme: q".into())
        );
        let mixed = r(vec![
            (Some("acme"), TargetResult::Started("a".into())),
            (Some("globex"), TargetResult::Failed("gone".into())),
        ]);
        assert_eq!(
            mixed.outcome(),
            Outcome::Failed(vec!["globex: gone".into()])
        );
        assert_eq!(mixed.run_ids(), vec!["a".to_string()]);
        assert_eq!(
            r(vec![(None, TargetResult::Failed("gone".into()))]).outcome(),
            Outcome::Failed(vec!["gone".into()])
        );
        assert_eq!(
            r(vec![(None, TargetResult::Skipped("still going".into()))]).outcome(),
            Outcome::Skipped("still going".into())
        );
        assert_eq!(
            r(vec![]).outcome(),
            Outcome::Skipped("no tenants to run for".into())
        );
        assert_eq!(
            r(vec![(None, TargetResult::Wait("run r still going".into()))]).outcome(),
            Outcome::Waiting("run r still going".into())
        );
        assert_eq!(idempotency_key("n", "t", None, None), "sched:n:t");
        assert_eq!(actor("n", Some("acme")).principal, "schedule:n");
    }

    fn plain() -> ScheduleSpec {
        serde_json::from_value(json!({
            "name": "plain", "cron": "0 2 * * *", "template": {"id": "nope"}
        }))
        .unwrap()
    }

    fn failed(r: &TickResult) -> &str {
        match &r.targets[0].1 {
            TargetResult::Failed(e) => e,
            other => panic!("expected a failed target, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn each_target_reports_why_it_did_not_start() {
        let state = crate::serve::test_support::test_state();
        let tick = at("2026-11-05T02:00:00Z");
        let opts = FireOptions::default();

        // An owned schedule targets its owner only.
        let r = fire_tick(&state, &plain(), Some("acme"), tick, tick, &opts).await;
        assert_eq!(r.targets.len(), 1);
        assert_eq!(r.targets[0].0.as_deref(), Some("acme"));

        // A definition whose body cannot be built.
        let mut bad = plain();
        bad.template.version = Some("latest".into());
        let r = fire_tick(&state, &bad, None, tick, tick, &opts).await;
        assert!(failed(&r).contains("template version"), "{r:?}");

        // A sealed param this server has no key for.
        let mut sealed = plain();
        sealed.template.params.insert(
            "token".into(),
            json!({ super::super::secrets::SEALED: "x" }),
        );
        // Not stored: there is no id to open it under (#1051).
        let r = fire_tick(&state, &sealed, None, tick, tick, &opts).await;
        assert!(failed(&r).contains("no longer stored"), "{r:?}");
        let record = crate::serve::history::schedules::ScheduleRecord::new(
            sealed.clone(),
            None,
            "ops",
            tick,
        );
        assert!(state.history().schedule_insert(&record).await.unwrap());
        let r = fire_tick(&state, &sealed, None, tick, tick, &opts).await;
        assert!(failed(&r).contains("template.params.token"), "{r:?}");
        // A tick planned for a schedule since replaced does not open it.
        let replaced = FireOptions {
            schedule_id: Some("another".into()),
            ..Default::default()
        };
        let r = fire_tick(&state, &sealed, None, tick, tick, &replaced).await;
        assert!(failed(&r).contains("replaced"), "{r:?}");
    }

    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_fan_out_runs_for_active_tenants_and_skips_suspended_ones() {
        let state = crate::serve::test_support::test_state();
        for (id, suspended) in [("acme", false), ("globex", true)] {
            let t: crate::serve::history::tenants::TenantRecord = serde_json::from_value(json!({
                "id": id, "suspended": suspended, "created_by": "ops",
                "created_at": "2026-11-05T00:00:00Z", "updated_at": "2026-11-05T00:00:00Z"
            }))
            .unwrap();
            state.history().tenant_upsert(&t).await.unwrap();
        }
        use crate::serve::history::tenants::TenantSelector;
        let tick = at("2026-11-05T02:00:00Z");
        for selector in [
            TenantSelector::All("all".into()),
            TenantSelector::Named(vec!["acme".into(), "globex".into(), "ghost".into()]),
        ] {
            let mut fan = plain();
            fan.tenants = Some(selector);
            let r = fire_tick(&state, &fan, None, tick, tick, &FireOptions::default()).await;
            let tenants: Vec<_> = r.targets.iter().map(|(t, _)| t.as_deref()).collect();
            assert_eq!(tenants, vec![Some("acme")], "{r:?}");
        }
    }

    /// An in-memory history that counts schedule reads.
    #[cfg(feature = "tenants")]
    struct CountingGets {
        inner: crate::serve::history::memory::MemoryHistory,
        gets: std::sync::atomic::AtomicUsize,
    }

    #[cfg(feature = "tenants")]
    #[async_trait::async_trait]
    impl crate::serve::history::RunHistory for CountingGets {
        async fn claim_idempotency(
            &self,
            key: &str,
            fingerprint: &str,
            run_id: &str,
            window: std::time::Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            self.inner
                .claim_idempotency(key, fingerprint, run_id, window)
                .await
        }
        async fn upsert(
            &self,
            rec: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            self.inner.upsert(rec).await
        }
        async fn get(
            &self,
            id: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            self.inner.get(id).await
        }
        async fn list(
            &self,
            filter: &ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            self.inner.list(filter).await
        }
        async fn delete(
            &self,
            id: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            self.inner.delete(id).await
        }
        async fn purge_expired(
            &self,
            retain_for: std::time::Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            self.inner.purge_expired(retain_for).await
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            self.inner.recover_orphans().await
        }
        fn degraded(&self) -> bool {
            false
        }
        async fn schedule_insert(
            &self,
            r: &crate::serve::history::schedules::ScheduleRecord,
        ) -> Result<bool, crate::serve::history::HistoryError> {
            self.inner.schedule_insert(r).await
        }
        async fn schedule_get(
            &self,
            name: &str,
        ) -> Result<
            Option<crate::serve::history::schedules::ScheduleRecord>,
            crate::serve::history::HistoryError,
        > {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.schedule_get(name).await
        }
        async fn tenant_upsert(
            &self,
            t: &crate::serve::history::tenants::TenantRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            self.inner.tenant_upsert(t).await
        }
        async fn tenant_get(
            &self,
            id: &str,
        ) -> Result<
            Option<crate::serve::history::tenants::TenantRecord>,
            crate::serve::history::HistoryError,
        > {
            self.inner.tenant_get(id).await
        }
        async fn tenant_list(
            &self,
        ) -> Result<
            Vec<crate::serve::history::tenants::TenantRecord>,
            crate::serve::history::HistoryError,
        > {
            self.inner.tenant_list().await
        }
    }

    /// The counting double passes every call through and counts only the
    /// schedule reads.
    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn the_counting_history_passes_calls_through() {
        use crate::serve::history::RunHistory as _;
        let h = CountingGets {
            inner: crate::serve::history::memory::MemoryHistory::new(
                std::time::Duration::from_secs(60),
            ),
            gets: Default::default(),
        };
        let window = std::time::Duration::from_secs(60);
        assert!(matches!(
            h.claim_idempotency("k", "f", "r", window).await.unwrap(),
            crate::serve::history::Claim::Fresh
        ));
        let rec = crate::serve::history::RunRecord::queued(
            "r".into(),
            None,
            Default::default(),
            None,
            Utc::now(),
        );
        h.upsert(&rec).await.unwrap();
        assert!(h.get("r").await.unwrap().is_some());
        assert_eq!(h.list(&ListFilter::default()).await.unwrap().runs.len(), 1);
        assert!(!matches!(
            h.delete("r").await.unwrap(),
            crate::serve::history::DeleteOutcome::NotFound
        ));
        assert_eq!(h.purge_expired(window).await.unwrap(), 0);
        assert_eq!(h.recover_orphans().await.unwrap(), 0);
        assert!(h.tenant_get("acme").await.unwrap().is_none());
        assert_eq!(h.gets.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A fan-out tick reads the stored schedule and opens its sealed params
    /// once, not once per tenant.
    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_fan_out_tick_reads_the_schedule_once() {
        let history = std::sync::Arc::new(CountingGets {
            inner: crate::serve::history::memory::MemoryHistory::new(
                std::time::Duration::from_secs(60),
            ),
            gets: Default::default(),
        });
        let state = ServerState::new(
            &crate::serve::test_support::test_config(),
            None,
            tokio_util::sync::CancellationToken::new(),
            history.clone(),
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );
        for id in ["acme", "globex", "initech"] {
            let t: crate::serve::history::tenants::TenantRecord = serde_json::from_value(json!({
                "id": id, "created_by": "ops",
                "created_at": "2026-11-05T00:00:00Z", "updated_at": "2026-11-05T00:00:00Z"
            }))
            .unwrap();
            state.history().tenant_upsert(&t).await.unwrap();
        }
        let mut fan = plain();
        fan.tenants = Some(crate::serve::history::tenants::TenantSelector::All(
            "all".into(),
        ));
        fan.template.params.insert(
            "token".into(),
            json!({ super::super::secrets::SEALED: "x" }),
        );
        let record = crate::serve::history::schedules::ScheduleRecord::new(
            fan.clone(),
            None,
            "ops",
            Utc::now(),
        );
        assert!(state.history().schedule_insert(&record).await.unwrap());
        let tick = at("2026-11-05T02:00:00Z");
        let r = fire_tick(&state, &fan, None, tick, tick, &FireOptions::default()).await;
        assert_eq!(r.targets.len(), 3, "{r:?}");
        assert_eq!(history.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
        // Every tenant reports why its target did not start.
        for (_, result) in &r.targets {
            assert!(
                matches!(result, TargetResult::Failed(e) if e.contains("template.params.token")),
                "{result:?}"
            );
        }
    }
}
