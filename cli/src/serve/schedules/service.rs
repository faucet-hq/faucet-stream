//! The rules behind `/v1/schedules` (#1044), shared by the HTTP handlers and
//! the change-request flow:
//!
//! - **Who sees what.** An operator sees every schedule, or with `?tenant=`
//!   the ones that run for that tenant. A tenant-scoped principal sees its
//!   tenant's own schedules and the fan-out schedules that include its
//!   tenant; anything else is a 404.
//! - **Who changes what.** `ScheduleWrite` (operator+) changes a schedule
//!   made through the API. A tenant principal changes only its tenant's own
//!   schedules; a fan-out schedule is the operator's (read-only to it). A
//!   `--schedules` file schedule is read-only for everyone (run now works).
//! - **Tenant guardrails.** A tenant's schedules run only for it, never fan
//!   out, count against its `max_schedules` (paused ones too) and may tick
//!   at most every `--tenant-schedule-min-interval-secs`.
//! - **Approval.** Under `--require-approval schedule`, create / edit /
//!   delete answer 409 and are proposed through `POST /v1/changes`.

use super::status::{self, FailedTick, MissedRange, ScheduleStatus, SkippedTick};
use super::{Source, fire, secrets, validate};
use crate::serve::changes::ChangeKind;
use crate::serve::error::ServeError;
use crate::serve::history::schedules::{
    ScheduleChange, ScheduleRecord, ScheduleRun, ScheduleSpec, ScheduleTemplate, spec_diff,
};
use crate::serve::history::tenants::TenantSelector;
use crate::serve::history::{ListFilter, RunStatus, labels::LabelFilter};
use crate::serve::rbac::{AuthContext, Permission};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Runs listed per tick in a schedule's detail.
const RECENT_RUNS: usize = 200;
/// Ticks listed in a schedule's detail.
const RECENT_TICKS: usize = 20;
/// Upcoming ticks previewed.
const NEXT_TICKS: usize = 5;

/// A schedule as found: where it is defined and who owns it.
#[derive(Debug, Clone)]
pub struct Found {
    pub spec: ScheduleSpec,
    pub owner: Option<String>,
    pub source: Source,
    pub record: Option<ScheduleRecord>,
}

fn store_err(e: crate::serve::history::HistoryError) -> ServeError {
    ServeError::from_history("schedules", e)
}

fn unprocessable(message: impl Into<String>) -> ServeError {
    ServeError::Unprocessable {
        message: message.into(),
        details: None,
    }
}

/// Find a schedule by name: the file's first, then the stored ones.
pub async fn find(state: &ServerState, name: &str) -> Result<Option<Found>, ServeError> {
    if let Some(spec) = state.schedules().file_schedule(name) {
        return Ok(Some(Found {
            spec: spec.clone(),
            owner: None,
            source: Source::File,
            record: None,
        }));
    }
    Ok(state
        .history()
        .schedule_get(name)
        .await
        .map_err(store_err)?
        .map(|r| Found {
            spec: r.spec.clone(),
            owner: r.tenant.clone(),
            source: Source::Console,
            record: Some(r),
        }))
}

/// Every schedule, the file's first.
pub async fn all(state: &ServerState) -> Result<Vec<Found>, ServeError> {
    let mut out: Vec<Found> = state
        .schedules()
        .file()
        .iter()
        .map(|s| Found {
            spec: s.clone(),
            owner: None,
            source: Source::File,
            record: None,
        })
        .collect();
    for r in state.history().schedule_list().await.map_err(store_err)? {
        if out.iter().any(|f| f.spec.name == r.spec.name) {
            continue;
        }
        out.push(Found {
            spec: r.spec.clone(),
            owner: r.tenant.clone(),
            source: Source::Console,
            record: Some(r),
        });
    }
    Ok(out)
}

/// The tenant a request is scoped to: the principal's own, else `?tenant=`.
pub fn scope(actor: &AuthContext, query: Option<&str>) -> Option<String> {
    actor
        .tenant
        .clone()
        .or_else(|| query.filter(|t| !t.is_empty()).map(str::to_string))
}

fn visible(found: &Found, scope: Option<&str>) -> bool {
    scope.is_none_or(|t| found.spec.runs_for(found.owner.as_deref(), t))
}

/// Whether `actor` may edit, pause or delete the schedule.
pub fn can_edit(actor: &AuthContext, found: &Found) -> bool {
    found.source == Source::Console
        && actor.role.grants(Permission::ScheduleWrite)
        && actor
            .tenant
            .as_deref()
            .is_none_or(|t| found.owner.as_deref() == Some(t))
}

/// Whether `actor` may start the schedule's run now.
pub fn can_run(actor: &AuthContext, found: &Found) -> bool {
    actor.role.grants(Permission::ScheduleWrite)
        && actor
            .tenant
            .as_deref()
            .is_none_or(|t| found.owner.as_deref() == Some(t))
}

/// Refuse a change `actor` may not make, saying why.
fn check_edit(actor: &AuthContext, found: &Found) -> Result<(), ServeError> {
    if found.source == Source::File {
        return Err(ServeError::Conflict(format!(
            "schedule '{}' is defined in the --schedules file; edit the file and restart the server",
            found.spec.name
        )));
    }
    if !can_edit(actor, found) {
        return Err(ServeError::Forbidden(format!(
            "schedule '{}' runs for several tenants and is managed by the server operator",
            found.spec.name
        )));
    }
    Ok(())
}

/// 409 when `--require-approval` gates `kind`.
pub fn approval_gate(state: &ServerState, kind: ChangeKind) -> Result<(), ServeError> {
    if state.requires_approval(kind) {
        return Err(ServeError::Conflict(format!(
            "this server requires an approved change request for {kind} (--require-approval): \
             propose it with POST /v1/changes (kind `{kind}`) and have it approved"
        )));
    }
    Ok(())
}

// ── Views ───────────────────────────────────────────────────────────────────

/// A schedule as the API lists it.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub source: Source,
    /// The tenant that owns it (runs only for that tenant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The fan-out selector of a schedule that runs for several tenants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenants: Option<TenantSelector>,
    /// Whether it runs for several tenants (a fan-out).
    pub shared: bool,
    pub cron: String,
    pub timezone: String,
    pub catch_up: bool,
    pub overlap: &'static str,
    pub paused: bool,
    pub template: ScheduleTemplate,
    #[serde(skip_serializing_if = "ScheduleRun::is_empty")]
    pub run: ScheduleRun,
    /// `active`, `paused`, `failing` (the last tick was refused) or
    /// `retrying` (the last tick hit a transient error).
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_tick: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tick: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// Ticks that never ran in the last 30 days, and in the last 24 hours.
    pub missed_30d: u64,
    pub missed_24h: u64,
    /// Ticks due since 00:00 UTC today.
    pub ticks_today: u64,
    /// Ticks in the next 12 hours (at most 6).
    pub upcoming: Vec<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// While paused: who paused it, when, why, and the ticks skipped since.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_while_paused: Option<u64>,
    /// Whether the caller may edit, pause or delete it.
    pub can_edit: bool,
    /// Whether the caller may start its run now.
    pub can_run: bool,
}

/// Ticks that came due since the schedule was paused (`None` when not paused).
fn skipped_while_paused(r: &ScheduleRecord, now: DateTime<Utc>) -> Option<u64> {
    let since = r.paused_at?;
    let c = validate::compile(&r.spec.cron, &r.spec.timezone).ok()?;
    Some(
        std::iter::successors(c.next_after(since), |t| c.next_after(*t))
            .take_while(|t| *t <= now)
            .take(100_000)
            .count() as u64,
    )
}

fn view(actor: &AuthContext, f: &Found, st: &ScheduleStatus, now: DateTime<Utc>) -> ScheduleView {
    let compiled = validate::compile(&f.spec.cron, &f.spec.timezone).ok();
    let next_tick = if f.spec.paused {
        None
    } else {
        compiled.as_ref().and_then(|c| c.next_after(now))
    };
    let upcoming = match (&compiled, f.spec.paused) {
        (Some(c), false) => validate::next_ticks(c, now, 6)
            .into_iter()
            .take_while(|t| *t <= now + chrono::Duration::hours(12))
            .collect(),
        _ => Vec::new(),
    };
    let midnight = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|d| d.and_utc())
        .unwrap_or(now);
    let ticks_today = compiled.as_ref().map_or(0, |c| {
        std::iter::successors(c.next_after(midnight - chrono::Duration::seconds(1)), |t| {
            c.next_after(*t)
        })
        .take_while(|t| *t <= now)
        .take(100_000)
        .count() as u64
    });
    let status = if f.spec.paused {
        "paused"
    } else {
        match st.last_outcome.as_deref() {
            Some("failed") => "failing",
            Some("retrying") => "retrying",
            _ => "active",
        }
    };
    let r = f.record.as_ref();
    let mut shown = secrets::masked(&f.spec);
    // A tenant sees an operator's fan-out as running for itself only: never
    // the other tenants, nor the operator's params.
    let outsider = actor
        .tenant
        .as_deref()
        .filter(|t| f.owner.as_deref() != Some(*t));
    let tenants = match outsider {
        Some(t) if f.spec.tenants.is_some() => Some(TenantSelector::Named(vec![t.to_string()])),
        _ => f.spec.tenants.clone(),
    };
    if outsider.is_some() {
        shown.template.params.clear();
    }
    ScheduleView {
        name: f.spec.name.clone(),
        description: f.spec.description.clone(),
        source: f.source,
        tenant: f.owner.clone(),
        tenants,
        shared: f.spec.tenants.is_some(),
        cron: f.spec.cron.clone(),
        timezone: f.spec.timezone.clone(),
        catch_up: f.spec.catch_up,
        overlap: f.spec.overlap.as_str(),
        paused: f.spec.paused,
        template: shown.template,
        run: f.spec.run.clone(),
        status,
        next_tick,
        last_tick: st.last_tick,
        last_outcome: st.last_outcome.clone(),
        last_error: st.last_error.clone(),
        consecutive_failures: st.consecutive_failures,
        missed_30d: st.missed_since(now - chrono::Duration::days(30)),
        missed_24h: st.missed_since(now - chrono::Duration::hours(24)),
        ticks_today,
        upcoming,
        created_at: r.map(|r| r.created_at),
        created_by: r.map(|r| r.created_by.clone()),
        updated_at: r.map(|r| r.updated_at),
        updated_by: r.map(|r| r.updated_by.clone()),
        revision: r.map(|r| r.revision),
        paused_at: r.and_then(|r| r.paused_at),
        paused_by: r.and_then(|r| r.paused_by.clone()),
        pause_reason: r.and_then(|r| r.pause_reason.clone()),
        skipped_while_paused: r.and_then(|r| skipped_while_paused(r, now)),
        can_edit: can_edit(actor, f),
        can_run: can_run(actor, f),
    }
}

/// `GET /v1/schedules` response.
#[derive(Debug, Clone, Serialize)]
pub struct ListResponse {
    /// Whether this build ticks schedules (`schedule` + `templates`).
    pub available: bool,
    /// The tenant the list is scoped to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The shortest gap allowed between a tenant schedule's ticks (seconds).
    pub tenant_min_interval_secs: u64,
    /// Whether creating, editing or deleting needs an approved change request.
    pub requires_approval: bool,
    /// For a tenant scope: its `max_schedules` and how many it owns.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_schedules: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned: Option<usize>,
    /// For an operator's tenant-scoped view: every schedule on the server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    pub schedules: Vec<ScheduleView>,
}

pub async fn list(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
) -> Result<ListResponse, ServeError> {
    let scope = scope(actor, tenant);
    let now = Utc::now();
    let mut out = Vec::new();
    let mut owned = 0;
    let found = all(state).await?;
    let total = (actor.tenant.is_none() && scope.is_some()).then_some(found.len());
    for f in found {
        if !visible(&f, scope.as_deref()) {
            continue;
        }
        if scope.is_some() && f.owner == scope {
            owned += 1;
        }
        let st = status::read(&*state.history(), &f.spec.name).await;
        out.push(view(actor, &f, &st, now));
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    let max_schedules = match &scope {
        Some(t) => state
            .history()
            .tenant_get(t)
            .await
            .map_err(store_err)?
            .and_then(|r| r.limits.max_schedules),
        None => None,
    };
    Ok(ListResponse {
        available: true,
        tenant: scope.clone(),
        tenant_min_interval_secs: state.schedules().tenant_min_interval().as_secs(),
        requires_approval: state.requires_approval(ChangeKind::Schedule),
        max_schedules,
        owned: scope.map(|_| owned),
        total,
        schedules: out,
    })
}

/// A run a tick started.
#[derive(Debug, Clone, Serialize)]
pub struct RunRef {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub records_written: u64,
    pub status: RunStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub submitted_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_secs: Option<f64>,
}

/// One tick and what it did.
#[derive(Debug, Clone, Serialize)]
pub struct TickView {
    pub tick: DateTime<Utc>,
    /// `started`, `failed` or `skipped`.
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<RunRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether it was started by hand (`run now`).
    pub manual: bool,
}

/// `GET /v1/schedules/{name}` response.
#[derive(Debug, Clone, Serialize)]
pub struct Detail {
    pub schedule: ScheduleView,
    pub next_ticks: Vec<DateTime<Utc>>,
    /// Newest first.
    pub recent_ticks: Vec<TickView>,
    /// Runs the schedule started (scoped to the tenant, when there is one).
    pub runs_total: usize,
    pub missed: Vec<MissedRange>,
    pub failures: Vec<FailedTick>,
    pub skipped: Vec<SkippedTick>,
    pub changes: Vec<ScheduleChange>,
    /// For a fan-out: the tenants it runs for now (edits apply to all).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applies_to: Option<Vec<String>>,
    /// The template version the selector resolves to now (`None` when it
    /// does not resolve).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_version: Option<u32>,
    /// The template's kind (`pipeline`, `source-template`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_kind: Option<String>,
}

/// What the schedule's template selector resolves to now, and the kind of
/// that template; `(None, None)` when it does not resolve.
async fn resolved_target(
    state: &ServerState,
    spec: &ScheduleSpec,
) -> (Option<u32>, Option<String>) {
    let s = state.history();
    let selector = match spec.template.version.as_ref() {
        Some(v) => match serde_json::from_value(serde_json::Value::String(v.clone())) {
            Ok(sel) => sel,
            Err(_) => return (None, None),
        },
        None => Default::default(),
    };
    let Ok(v) = crate::templates::resolve_version(&s, &spec.template.id, selector).await else {
        return (None, None);
    };
    let kind = match s.template_get(&spec.template.id, Some(v)).await {
        Ok(Some(r)) => serde_json::to_value(r.kind)
            .ok()
            .and_then(|k| k.as_str().map(str::to_string)),
        _ => None,
    };
    (Some(v), kind)
}

/// The ticks the schedule's runs and status record tell about, newest first.
fn ticks(
    runs: Vec<RunRef>,
    labels: &[BTreeMap<String, String>],
    st: &ScheduleStatus,
    scope: Option<&str>,
) -> Vec<TickView> {
    let mut by_tick: BTreeMap<(DateTime<Utc>, bool), Vec<RunRef>> = BTreeMap::new();
    for (run, l) in runs.into_iter().zip(labels) {
        let Some(tick) = l
            .get(fire::LABEL_TICK)
            .and_then(|t| t.parse::<DateTime<Utc>>().ok())
        else {
            continue;
        };
        let manual = l.get("schedule_manual").is_some();
        by_tick.entry((tick, manual)).or_default().push(run);
    }
    let mut out: Vec<TickView> = by_tick
        .into_iter()
        .map(|((tick, manual), runs)| TickView {
            tick,
            outcome: "started",
            errors: st
                .failures
                .iter()
                .find(|f| f.tick == tick && !manual)
                .map(|f| f.errors.clone())
                .unwrap_or_default(),
            runs,
            reason: None,
            manual,
        })
        .collect();
    // A scoped view shows its tenant's errors only (a tenant target's error
    // is prefixed with the tenant id).
    let mine = |e: &String| scope.is_none_or(|t| e.starts_with(&format!("{t}: ")));
    for f in &st.failures {
        let errors: Vec<String> = f.errors.iter().filter(|e| mine(e)).cloned().collect();
        if errors.is_empty() || out.iter().any(|t| t.tick == f.tick && !t.manual) {
            continue;
        }
        out.push(TickView {
            tick: f.tick,
            outcome: "failed",
            runs: Vec::new(),
            errors,
            reason: None,
            manual: false,
        });
    }
    for s in &st.skipped {
        if out.iter().any(|t| t.tick == s.tick && !t.manual) {
            continue;
        }
        out.push(TickView {
            tick: s.tick,
            outcome: "skipped",
            runs: Vec::new(),
            errors: Vec::new(),
            reason: Some(s.reason.clone()),
            manual: false,
        });
    }
    out.sort_by_key(|a| std::cmp::Reverse(a.tick));
    out.truncate(RECENT_TICKS);
    out
}

/// The tenants a fan-out runs for now.
async fn applies_to(state: &ServerState, spec: &ScheduleSpec) -> Option<Vec<String>> {
    let selector = spec.tenants.as_ref()?;
    #[cfg(feature = "tenants")]
    {
        crate::serve::handlers::tenants::resolve_tenants(state, selector)
            .await
            .ok()
            .map(|(run, _)| run)
    }
    #[cfg(not(feature = "tenants"))]
    {
        let _ = (state, selector);
        None
    }
}

pub async fn get(
    state: &ServerState,
    actor: &AuthContext,
    name: &str,
    tenant: Option<&str>,
) -> Result<Detail, ServeError> {
    let scope = scope(actor, tenant);
    let f = find(state, name)
        .await?
        .filter(|f| visible(f, scope.as_deref()))
        .ok_or(ServeError::NotFound)?;
    let now = Utc::now();
    let history = state.history();
    let st = status::read(&*history, name).await;
    let filter = ListFilter {
        tenant: scope.clone(),
        labels: vec![LabelFilter {
            key: fire::LABEL_SCHEDULE.into(),
            value: Some(name.to_string()),
        }],
        limit: RECENT_RUNS,
        ..Default::default()
    };
    let page = history.list(&filter).await.map_err(store_err)?;
    let runs_total = page.runs.len();
    let labels: Vec<BTreeMap<String, String>> =
        page.runs.iter().map(|r| r.labels.clone()).collect();
    let runs: Vec<RunRef> = page
        .runs
        .into_iter()
        .map(|r| RunRef {
            run_id: r.run_id,
            name: r.name,
            records_written: r.records_written,
            status: r.status,
            tenant: r.tenant,
            submitted_at: r.submitted_at,
            finished_at: r.finished_at,
            elapsed_secs: r.elapsed_secs,
        })
        .collect();
    let next_ticks = if f.spec.paused {
        Vec::new()
    } else {
        validate::compile(&f.spec.cron, &f.spec.timezone)
            .map(|c| validate::next_ticks(&c, now, NEXT_TICKS))
            .unwrap_or_default()
    };
    let (resolved_version, template_kind) = resolved_target(state, &f.spec).await;
    Ok(Detail {
        schedule: view(actor, &f, &st, now),
        next_ticks,
        recent_ticks: ticks(runs, &labels, &st, scope.as_deref()),
        runs_total,
        missed: st.missed.clone(),
        failures: st.failures.clone(),
        skipped: st.skipped.clone(),
        changes: f
            .record
            .as_ref()
            .map(|r| r.changes.iter().rev().cloned().collect())
            .unwrap_or_default(),
        applies_to: if actor.tenant.is_some() {
            None
        } else {
            applies_to(state, &f.spec).await
        },
        resolved_version,
        template_kind,
    })
}

// ── Changes ─────────────────────────────────────────────────────────────────

/// What a schedule change request (`kind: schedule`) proposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangePayload {
    pub action: ChangeAction,
    pub name: String,
    /// The definition, for `create` and `update`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ScheduleSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeAction {
    Create,
    Update,
    Delete,
}

impl ChangeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeAction::Create => "create",
            ChangeAction::Update => "update",
            ChangeAction::Delete => "delete",
        }
    }
}

/// Check `spec` as `actor` would store it under `owner`; returns the names of
/// the template's secret params (their values are sealed when stored).
async fn check_spec(
    state: &ServerState,
    actor: &AuthContext,
    spec: &ScheduleSpec,
    owner: Option<&str>,
    replacing: Option<&str>,
) -> Result<Vec<String>, ServeError> {
    let compiled = validate::validate(spec).map_err(unprocessable)?;
    if let Some(t) = owner {
        if spec.tenants.is_some() {
            return Err(ServeError::Forbidden(
                "a tenant's schedule runs only for that tenant; `tenants` (a fan-out) is operator-only"
                    .into(),
            ));
        }
        let min = state.schedules().tenant_min_interval();
        validate::check_min_interval(spec, &compiled, min, t, Utc::now()).map_err(unprocessable)?;
        let limit = state
            .history()
            .tenant_get(t)
            .await
            .map_err(store_err)?
            .and_then(|r| r.limits.max_schedules);
        if let Some(max) = limit {
            let used = state
                .history()
                .schedule_list()
                .await
                .map_err(store_err)?
                .iter()
                .filter(|r| r.tenant.as_deref() == Some(t) && Some(r.name()) != replacing)
                .count();
            if used >= max as usize {
                return Err(unprocessable(format!(
                    "{t} can have {max} schedule(s) and {used} are in use (paused schedules count); \
                     delete one or raise limits.max_schedules"
                )));
            }
        }
    } else if spec.tenants.is_some() && actor.tenant.is_some() {
        return Err(ServeError::Forbidden(
            "`tenants` (a fan-out) is operator-only".into(),
        ));
    }
    // The template must resolve now; one that stops resolving later fails its
    // ticks visibly.
    let s = state.history();
    let mut secret = Vec::new();
    for (what, id, version) in [
        ("template", Some(&spec.template.id), &spec.template.version),
        (
            "sink template",
            spec.template.sink.as_ref(),
            &spec.template.sink_version,
        ),
        (
            "overlay",
            spec.template.overlay.as_ref(),
            &spec.template.overlay_version,
        ),
    ] {
        let Some(id) = id else { continue };
        let selector = version
            .as_ref()
            .map(|v| serde_json::from_value(serde_json::Value::String(v.clone())))
            .transpose()
            .map_err(|e| unprocessable(format!("{what} version: {e}")))?
            .unwrap_or_default();
        let resolved = crate::templates::resolve_version(&s, id, selector)
            .await
            .map_err(|e| unprocessable(format!("{what} '{id}': {e}")))?;
        let record = s
            .template_get(id, Some(resolved))
            .await
            .map_err(store_err)?
            .ok_or_else(|| unprocessable(format!("{what} '{id}' has no version {resolved}")))?;
        secret.extend(
            record
                .params
                .iter()
                .filter(|(_, p)| p.is_secret())
                .map(|(k, _)| k.clone()),
        );
    }
    Ok(secret)
}

/// The definition as it will be stored: secret params sealed (a keep
/// marker takes the stored value).
fn sealed(
    state: &ServerState,
    spec: &ScheduleSpec,
    secret: &[String],
    current: Option<&ScheduleSpec>,
) -> Result<ScheduleSpec, ServeError> {
    let mut out = spec.clone();
    secrets::seal(state, &mut out, secret, current).map_err(unprocessable)?;
    Ok(out)
}

/// A plan: what will be stored (sealed), and the rows a reviewer sees.
struct Planned {
    spec: Option<ScheduleSpec>,
    /// The template's secret param names.
    secret: Vec<String>,
    rows: Vec<serde_json::Value>,
}

async fn plan_inner(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
) -> Result<Planned, ServeError> {
    let existing = find(state, &p.name).await?;
    let current = existing.as_ref().and_then(|f| f.record.as_ref());
    let mut secret = Vec::new();
    let stored = match p.action {
        ChangeAction::Create => {
            if existing.is_some() {
                return Err(ServeError::Conflict(format!(
                    "schedule '{}' already exists",
                    p.name
                )));
            }
            let spec = p
                .spec
                .as_ref()
                .ok_or_else(|| unprocessable("create needs `spec`"))?;
            if spec.name != p.name {
                return Err(unprocessable("spec.name must equal name"));
            }
            secret = check_spec(state, actor, spec, actor.tenant.as_deref(), None).await?;
            Some(sealed(state, spec, &secret, None)?)
        }
        ChangeAction::Update => {
            let f = existing.as_ref().ok_or(ServeError::NotFound)?;
            if !visible(f, actor.tenant.as_deref()) {
                return Err(ServeError::NotFound);
            }
            check_edit(actor, f)?;
            let spec = p
                .spec
                .as_ref()
                .ok_or_else(|| unprocessable("update needs `spec`"))?;
            if spec.name != p.name {
                return Err(unprocessable("a schedule's name cannot change"));
            }
            secret = check_spec(state, actor, spec, f.owner.as_deref(), Some(&p.name)).await?;
            let mut out = sealed(state, spec, &secret, current.map(|r| &r.spec))?;
            // A pause is its own action; an edit keeps it.
            out.paused = f.spec.paused;
            Some(out)
        }
        ChangeAction::Delete => {
            let f = existing.as_ref().ok_or(ServeError::NotFound)?;
            if !visible(f, actor.tenant.as_deref()) {
                return Err(ServeError::NotFound);
            }
            check_edit(actor, f)?;
            None
        }
    };
    // The rows carry the revision being edited: an edit made meanwhile
    // changes the material and invalidates an approval. Secrets show masked.
    let shown = stored.as_ref().map(secrets::masked);
    let row = serde_json::json!({
        "schedule": p.name,
        "action": p.action.as_str(),
        "revision": current.map(|r| r.revision),
        "cron": shown.as_ref().map(|s| s.cron.clone()),
        "timezone": shown.as_ref().map(|s| s.timezone.clone()),
        "template": shown.as_ref().map(|s| s.template.clone()),
        "tenants": shown.as_ref().and_then(|s| s.tenants.clone()),
        "run": shown.as_ref().map(|s| s.run.clone()),
        "overlap": shown.as_ref().map(|s| s.overlap.as_str()),
        "catch_up": shown.as_ref().map(|s| s.catch_up),
        "diff": match (current, shown.as_ref()) {
            (Some(c), Some(s)) => spec_diff(&secrets::masked(&c.spec), s),
            _ => Vec::new(),
        },
    });
    Ok(Planned {
        spec: stored,
        secret,
        rows: vec![row],
    })
}

/// Plan a change: validate it as `actor`, return the rows a reviewer sees.
/// A change request stores its payload as sent, so it may not carry a new
/// secret param value (keeping the stored one is fine).
pub async fn plan(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
) -> Result<Vec<serde_json::Value>, ServeError> {
    let planned = plan_inner(state, actor, p).await?;
    if let Some(spec) = &p.spec
        && let Some(k) = planned.secret.iter().find(|k| {
            spec.template
                .params
                .get(*k)
                .is_some_and(|v| !v.is_null() && !secrets::is_keep_marker(v))
        })
    {
        return Err(unprocessable(format!(
            "template.params.{k}: a change request cannot carry a secret param value (it would be \
             stored as sent); keep the stored value, or make the change on a server that does not \
             gate schedules"
        )));
    }
    Ok(planned.rows)
}

/// Apply a change as `actor` (already planned / approved, or ungated).
/// Returns the schedule's view after the change (`None` for a delete).
pub async fn apply(
    state: &ServerState,
    actor: &AuthContext,
    p: ChangePayload,
) -> Result<Option<ScheduleView>, ServeError> {
    let planned = plan_inner(state, actor, &p).await?;
    let history = state.history();
    let now = Utc::now();
    let by = actor.principal.as_str();
    let out = match p.action {
        ChangeAction::Create => {
            let spec = planned.spec.expect("planned");
            let record = ScheduleRecord::new(spec, actor.tenant.clone(), by, now);
            if !history.schedule_insert(&record).await.map_err(store_err)? {
                return Err(ServeError::Conflict(format!(
                    "schedule '{}' already exists",
                    p.name
                )));
            }
            status::forget(&*history, &p.name).await;
            status::reset_tick(
                &*history,
                &p.name,
                &record.spec.cron,
                &record.spec.timezone,
                now,
            )
            .await;
            Some(record)
        }
        ChangeAction::Update => {
            let spec = planned.spec.expect("planned");
            Some(
                revise(state, actor, &p.name, "updated", |r| {
                    r.spec = ScheduleSpec {
                        paused: r.spec.paused,
                        ..spec.clone()
                    };
                    Vec::new()
                })
                .await?,
            )
        }
        ChangeAction::Delete => {
            if !history.schedule_delete(&p.name).await.map_err(store_err)? {
                return Err(ServeError::NotFound);
            }
            status::forget(&*history, &p.name).await;
            None
        }
    };
    state.schedules().notify();
    let audit = format!("schedule.{}", p.action.as_str());
    crate::serve::audit::write_target_detail(
        state,
        actor,
        &audit,
        format!("schedule:{}", p.name),
        String::new(),
        "ok",
    )
    .await;
    Ok(out.map(|r| {
        let f = Found {
            spec: r.spec.clone(),
            owner: r.tenant.clone(),
            source: Source::Console,
            record: Some(r),
        };
        view(actor, &f, &ScheduleStatus::default(), now)
    }))
}

/// Rewrite a stored schedule with `edit`, retrying a lost compare-and-set;
/// logs `action` with the spec's diff (secrets masked).
async fn revise(
    state: &ServerState,
    actor: &AuthContext,
    name: &str,
    action: &str,
    edit: impl Fn(&mut ScheduleRecord) -> Vec<String>,
) -> Result<ScheduleRecord, ServeError> {
    let history = state.history();
    for _ in 0..5 {
        let current = history
            .schedule_get(name)
            .await
            .map_err(store_err)?
            .ok_or(ServeError::NotFound)?;
        let mut edited = current.clone();
        let extra = edit(&mut edited);
        let mut detail = spec_diff(
            &secrets::masked(&current.spec),
            &secrets::masked(&edited.spec),
        );
        detail.extend(extra);
        let mut next = current.revised(
            edited.spec.clone(),
            &actor.principal,
            Utc::now(),
            action,
            detail,
        );
        next.paused_at = edited.paused_at;
        next.paused_by = edited.paused_by;
        next.pause_reason = edited.pause_reason;
        if history
            .schedule_replace(&current, &next)
            .await
            .map_err(store_err)?
        {
            return Ok(next);
        }
    }
    Err(ServeError::Conflict(format!(
        "schedule '{name}' kept changing; try again"
    )))
}

/// What a create / update / delete did.
pub enum Changed {
    /// Applied now (`None` for a delete).
    Applied(Option<Box<ScheduleView>>),
    /// `--require-approval schedule`: proposed as a change request instead.
    Pending(Box<crate::serve::changes::ChangeRequest>),
}

/// Create, update or delete: applied now, or — under
/// `--require-approval schedule` — proposed as a change request that applies
/// it once approved (for any caller, tenant principals included).
pub async fn change(
    state: &ServerState,
    actor: &AuthContext,
    p: ChangePayload,
    reason: Option<String>,
) -> Result<Changed, ServeError> {
    if state.requires_approval(ChangeKind::Schedule) {
        let payload = serde_json::to_value(&p).map_err(|e| ServeError::Internal(e.to_string()))?;
        let change = crate::serve::changes::create(
            state,
            actor,
            crate::serve::changes::NewChange {
                kind: ChangeKind::Schedule,
                payload,
                reason,
                budget: None,
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await?;
        return Ok(Changed::Pending(Box::new(change)));
    }
    apply(state, actor, p)
        .await
        .map(|v| Changed::Applied(v.map(Box::new)))
}

/// Pause or resume.
pub async fn set_paused(
    state: &ServerState,
    actor: &AuthContext,
    name: &str,
    paused: bool,
    reason: Option<String>,
) -> Result<ScheduleView, ServeError> {
    let f = find(state, name)
        .await?
        .filter(|f| visible(f, actor.tenant.as_deref()))
        .ok_or(ServeError::NotFound)?;
    check_edit(actor, &f)?;
    let reason = reason
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty());
    if reason.as_ref().is_some_and(|r| r.len() > 500) {
        return Err(unprocessable("reason must be at most 500 characters"));
    }
    let action = if paused { "paused" } else { "resumed" };
    let now = Utc::now();
    let by = actor.principal.clone();
    let r = revise(state, actor, name, action, |r| {
        let mut extra: Vec<String> = reason.iter().map(|x| format!("reason: {x}")).collect();
        if paused {
            if !r.spec.paused {
                r.paused_at = Some(now);
                r.paused_by = Some(by.clone());
            }
            r.pause_reason = reason.clone();
        } else {
            if let Some(n) = skipped_while_paused(r, now) {
                extra.push(format!("{n} tick(s) skipped while paused"));
            }
            r.paused_at = None;
            r.paused_by = None;
            r.pause_reason = None;
        }
        r.spec.paused = paused;
        extra
    })
    .await?;
    if !paused {
        // Ticks while paused are not missed.
        status::reset_tick(
            &*state.history(),
            name,
            &r.spec.cron,
            &r.spec.timezone,
            Utc::now(),
        )
        .await;
    }
    state.schedules().notify();
    crate::serve::audit::write_target_detail(
        state,
        actor,
        if paused {
            "schedule.pause"
        } else {
            "schedule.resume"
        },
        format!("schedule:{name}"),
        String::new(),
        "ok",
    )
    .await;
    let st = status::read(&*state.history(), name).await;
    let f = Found {
        spec: r.spec.clone(),
        owner: r.tenant.clone(),
        source: Source::Console,
        record: Some(r),
    };
    Ok(view(actor, &f, &st, Utc::now()))
}

/// `POST /v1/schedules/{name}/run` response.
#[derive(Debug, Clone, Serialize)]
pub struct RunNowResponse {
    pub schedule: String,
    pub tick: DateTime<Utc>,
    pub runs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// Start the schedule's run(s) now, outside its cron.
pub async fn run_now(
    state: &ServerState,
    actor: &AuthContext,
    name: &str,
) -> Result<RunNowResponse, ServeError> {
    let f = find(state, name)
        .await?
        .filter(|f| visible(f, actor.tenant.as_deref()))
        .ok_or(ServeError::NotFound)?;
    if !can_run(actor, &f) {
        return Err(ServeError::Forbidden(format!(
            "schedule '{name}' runs for several tenants and is managed by the server operator"
        )));
    }
    approval_gate(state, ChangeKind::Run)?;
    let now = Utc::now();
    let opts = fire::FireOptions {
        manual: Some(uuid::Uuid::new_v4().to_string()),
    };
    let mut spec = f.spec.clone();
    spec.run
        .labels
        .insert("schedule_manual".into(), actor.principal.clone());
    let result = fire::fire_tick(state, &spec, f.owner.as_deref(), now, now, &opts).await;
    if let Some(e) = result.retry() {
        return Err(ServeError::Unavailable(e));
    }
    if f.source == Source::Console {
        // Best effort: the run started either way.
        let _ = revise(state, actor, name, "run_now", |_| Vec::new()).await;
    }
    crate::serve::audit::write_target_detail(
        state,
        actor,
        "schedule.run",
        format!("schedule:{name}"),
        result.run_ids().join(","),
        if result.failures().is_empty() {
            "ok"
        } else {
            "error"
        },
    )
    .await;
    let runs = result.run_ids();
    let errors = result.failures();
    if runs.is_empty() && !errors.is_empty() {
        return Err(unprocessable(errors.join("; ")));
    }
    Ok(RunNowResponse {
        schedule: name.to_string(),
        tick: now,
        runs,
        skipped: result.skipped(),
        errors,
    })
}

/// `POST /v1/schedules/preview` request and response.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewRequest {
    pub cron: String,
    #[serde(default = "utc")]
    pub timezone: String,
}

fn utc() -> String {
    "UTC".into()
}

#[derive(Debug, Clone, Serialize)]
pub struct PreviewResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub next_ticks: Vec<DateTime<Utc>>,
    /// The shortest gap between upcoming ticks (seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shortest_gap_secs: Option<u64>,
}

pub fn preview(req: &PreviewRequest, now: DateTime<Utc>) -> PreviewResponse {
    match validate::compile(&req.cron, &req.timezone) {
        Ok(c) => PreviewResponse {
            valid: true,
            error: None,
            next_ticks: validate::next_ticks(&c, now, NEXT_TICKS),
            shortest_gap_secs: validate::shortest_gap(&c, now).map(|d| d.as_secs()),
        },
        Err(e) => PreviewResponse {
            valid: false,
            error: Some(e),
            next_ticks: Vec::new(),
            shortest_gap_secs: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::rbac::Role;
    use serde_json::json;

    fn actor(role: Role, tenant: Option<&str>) -> AuthContext {
        AuthContext {
            principal: "p".into(),
            role,
            source_ip: None,
            tenant: tenant.map(str::to_string),
        }
    }

    fn found(owner: Option<&str>, tenants: Option<TenantSelector>, source: Source) -> Found {
        let mut spec: ScheduleSpec = serde_json::from_value(json!({
            "name": "n", "cron": "0 2 * * *", "template": {"id": "t"}
        }))
        .unwrap();
        spec.tenants = tenants;
        Found {
            spec,
            owner: owner.map(str::to_string),
            source,
            record: None,
        }
    }

    #[test]
    fn edit_and_run_rights() {
        let op = actor(Role::Operator, None);
        let viewer = actor(Role::Viewer, None);
        let acme = actor(Role::Operator, Some("acme"));
        let mine = found(Some("acme"), None, Source::Console);
        let shared = found(
            None,
            Some(TenantSelector::All("all".into())),
            Source::Console,
        );
        let file = found(None, None, Source::File);
        assert!(can_edit(&op, &mine) && can_edit(&op, &shared));
        assert!(!can_edit(&op, &file) && can_run(&op, &file));
        assert!(!can_edit(&viewer, &mine) && !can_run(&viewer, &mine));
        assert!(can_edit(&acme, &mine) && can_run(&acme, &mine));
        assert!(!can_edit(&acme, &shared) && !can_run(&acme, &shared));
        assert!(
            check_edit(&op, &file)
                .unwrap_err()
                .to_string()
                .contains("--schedules file")
        );
        assert!(matches!(
            check_edit(&acme, &shared),
            Err(ServeError::Forbidden(_))
        ));
        assert!(check_edit(&acme, &mine).is_ok());
    }

    #[test]
    fn visibility_and_scope() {
        let op = actor(Role::Admin, None);
        let acme = actor(Role::Viewer, Some("acme"));
        assert_eq!(scope(&op, Some("globex")).as_deref(), Some("globex"));
        assert_eq!(scope(&op, Some("")), None);
        assert_eq!(scope(&acme, Some("globex")).as_deref(), Some("acme"));
        let mine = found(Some("acme"), None, Source::Console);
        let theirs = found(Some("globex"), None, Source::Console);
        let shared = found(
            None,
            Some(TenantSelector::Named(vec!["acme".into()])),
            Source::Console,
        );
        let server = found(None, None, Source::File);
        assert!(visible(&mine, Some("acme")) && visible(&shared, Some("acme")));
        assert!(!visible(&theirs, Some("acme")) && !visible(&server, Some("acme")));
        assert!(visible(&theirs, None));
    }

    #[test]
    fn views_report_status_and_next_tick() {
        let op = actor(Role::Operator, None);
        let now: DateTime<Utc> = "2026-11-05T00:30:00Z".parse().unwrap();
        let f = found(None, None, Source::File);
        let mut st = ScheduleStatus::default();
        let v = view(&op, &f, &st, now);
        assert_eq!(v.status, "active");
        assert_eq!(v.next_tick, Some("2026-11-05T02:00:00Z".parse().unwrap()));
        st.last_outcome = Some("failed".into());
        assert_eq!(view(&op, &f, &st, now).status, "failing");
        st.last_outcome = Some("retrying".into());
        assert_eq!(view(&op, &f, &st, now).status, "retrying");
        let mut paused = f.clone();
        paused.spec.paused = true;
        let v = view(&op, &paused, &st, now);
        assert_eq!((v.status, v.next_tick), ("paused", None));
    }

    #[test]
    fn ticks_merge_runs_failures_and_skips() {
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        let run = |id: &str| RunRef {
            run_id: id.into(),
            name: None,
            records_written: 0,
            status: RunStatus::Completed,
            tenant: None,
            submitted_at: at("2026-11-05T02:00:01Z"),
            finished_at: None,
            elapsed_secs: None,
        };
        let label = |t: &str, manual: bool| {
            let mut l = BTreeMap::from([(fire::LABEL_TICK.to_string(), t.to_string())]);
            if manual {
                l.insert("schedule_manual".into(), "ops".into());
            }
            l
        };
        let mut st = ScheduleStatus::default();
        st.failures.push(FailedTick {
            tick: at("2026-11-04T02:00:00Z"),
            at: at("2026-11-04T02:00:01Z"),
            errors: vec!["acme: refused".into(), "globex: gone".into()],
        });
        st.skipped.push(SkippedTick {
            tick: at("2026-11-03T02:00:00Z"),
            reason: "still going".into(),
        });
        let ticks_all = ticks(
            vec![run("a"), run("b"), run("x")],
            &[
                label("2026-11-05T02:00:00Z", false),
                label("2026-11-05T09:00:00Z", true),
                BTreeMap::new(),
            ],
            &st,
            None,
        );
        assert_eq!(ticks_all.len(), 4);
        assert!(ticks_all[0].manual);
        assert_eq!(ticks_all[2].outcome, "failed");
        assert_eq!(ticks_all[2].errors.len(), 2);
        assert_eq!(ticks_all[3].outcome, "skipped");
        // Scoped to acme: only acme's errors.
        let scoped = ticks(Vec::new(), &[], &st, Some("acme"));
        assert_eq!(scoped[0].errors, vec!["acme: refused".to_string()]);
    }

    #[test]
    fn previews() {
        let now: DateTime<Utc> = "2026-11-05T00:30:00Z".parse().unwrap();
        let ok = preview(
            &PreviewRequest {
                cron: "0 * * * *".into(),
                timezone: utc(),
            },
            now,
        );
        assert!(ok.valid);
        assert_eq!(ok.next_ticks.len(), NEXT_TICKS);
        assert_eq!(ok.shortest_gap_secs, Some(3600));
        let bad = preview(
            &PreviewRequest {
                cron: "0 2 1 13 *".into(),
                timezone: utc(),
            },
            now,
        );
        assert!(!bad.valid && bad.error.unwrap().contains("invalid cron"));
        assert_eq!(ChangeAction::Delete.as_str(), "delete");
    }

    fn spec_of(v: serde_json::Value) -> ScheduleSpec {
        serde_json::from_value(v).unwrap()
    }

    async fn store(state: &ServerState, spec: ScheduleSpec, tenant: Option<&str>) {
        let r = ScheduleRecord::new(spec, tenant.map(str::to_string), "ops", Utc::now());
        assert!(state.history().schedule_insert(&r).await.unwrap());
    }

    #[test]
    fn the_approval_gate_refuses_only_gated_kinds() {
        let mut config = crate::serve::test_support::test_config();
        config.require_approval = vec![ChangeKind::Schedule];
        let state = crate::serve::test_support::state_from(&config);
        let e = approval_gate(&state, ChangeKind::Schedule).unwrap_err();
        assert!(matches!(&e, ServeError::Conflict(m) if m.contains("--require-approval")));
        assert!(approval_gate(&state, ChangeKind::Run).is_ok());
    }

    #[tokio::test]
    async fn a_stored_copy_of_a_file_schedule_is_listed_once() {
        let state = crate::serve::test_support::test_state();
        let dup = spec_of(json!({"name": "dup", "cron": "0 2 * * *", "template": {"id": "t"}}));
        state.set_schedules(super::super::SchedulesRuntime::new(
            vec![dup.clone()],
            std::time::Duration::from_secs(300),
        ));
        store(&state, dup, None).await;
        let found = all(&state).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].source, Source::File);
    }

    #[tokio::test]
    async fn detail_of_a_paused_fan_out_with_an_unresolvable_template() {
        let state = crate::serve::test_support::test_state();
        let op = actor(Role::Operator, None);
        let mut fan = spec_of(json!({
            "name": "fan", "cron": "0 2 * * *",
            "template": {"id": "t", "version": "latest"},
            "tenants": ["acme", "globex"]
        }));
        fan.paused = true;
        store(&state, fan, None).await;
        let d = get(&state, &op, "fan", None).await.unwrap();
        assert!(
            d.next_ticks.is_empty(),
            "a paused schedule has no next ticks"
        );
        assert_eq!((d.resolved_version, d.template_kind), (None, None));

        let missing = spec_of(
            json!({"name": "missing", "cron": "0 2 * * *", "template": {"id": "nope", "version": "stable"}}),
        );
        store(&state, missing, None).await;
        let d = get(&state, &op, "missing", None).await.unwrap();
        assert_eq!(d.resolved_version, None);
        assert!(!d.next_ticks.is_empty());
    }

    #[tokio::test]
    async fn planned_changes_refuse_bad_names_and_other_tenants_schedules() {
        let state = crate::serve::test_support::test_state();
        let op = actor(Role::Operator, None);
        let acme = actor(Role::Operator, Some("acme"));
        let n = || spec_of(json!({"name": "n", "cron": "0 2 * * *", "template": {"id": "t"}}));
        store(&state, n(), Some("globex")).await;
        let payload = |action, name: &str, spec: Option<ScheduleSpec>| ChangePayload {
            action,
            name: name.into(),
            spec,
        };
        let msg = |e: ServeError| e.to_string();

        let e = plan(
            &state,
            &op,
            &payload(ChangeAction::Create, "other", Some(n())),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("spec.name must equal name"));
        assert!(matches!(
            plan(
                &state,
                &acme,
                &payload(ChangeAction::Update, "n", Some(n()))
            )
            .await,
            Err(ServeError::NotFound)
        ));
        let mut renamed = n();
        renamed.name = "renamed".into();
        let e = plan(
            &state,
            &op,
            &payload(ChangeAction::Update, "n", Some(renamed)),
        )
        .await
        .unwrap_err();
        assert!(msg(e).contains("name cannot change"));
        assert!(matches!(
            plan(&state, &acme, &payload(ChangeAction::Delete, "n", None)).await,
            Err(ServeError::NotFound)
        ));
    }

    #[tokio::test]
    async fn spec_checks_for_tenants_and_fan_outs() {
        let state = crate::serve::test_support::test_state();
        let tenant: crate::serve::history::tenants::TenantRecord = serde_json::from_value(json!({
            "id": "acme", "limits": {"max_schedules": 5}, "created_by": "ops",
            "created_at": "2026-11-05T00:00:00Z", "updated_at": "2026-11-05T00:00:00Z"
        }))
        .unwrap();
        state.history().tenant_upsert(&tenant).await.unwrap();
        let acme = actor(Role::Admin, Some("acme"));
        let plain = spec_of(json!({"name": "n", "cron": "0 2 * * *", "template": {"id": "nope"}}));
        // Under the limit, so the template check runs (and fails: none is registered).
        let e = check_spec(&state, &acme, &plain, Some("acme"), None)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("nope"), "{e}");
        // A tenant principal never stores a fan-out, whatever the owner.
        let mut fan = plain.clone();
        fan.tenants = Some(TenantSelector::All("all".into()));
        assert!(matches!(
            check_spec(&state, &acme, &fan, None, None).await,
            Err(ServeError::Forbidden(_))
        ));
    }

    #[tokio::test]
    async fn deleting_a_tenant_deletes_its_schedules() {
        let state = crate::serve::test_support::test_state();
        let tenant: crate::serve::history::tenants::TenantRecord = serde_json::from_value(json!({
            "id": "acme", "created_by": "ops",
            "created_at": "2026-11-05T00:00:00Z", "updated_at": "2026-11-05T00:00:00Z"
        }))
        .unwrap();
        state.history().tenant_upsert(&tenant).await.unwrap();
        store(
            &state,
            spec_of(json!({"name": "mine", "cron": "0 2 * * *", "template": {"id": "t"}})),
            Some("acme"),
        )
        .await;
        assert!(state.history().tenant_delete("acme").await.unwrap());
        assert!(find(&state, "mine").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pause_reasons_are_bounded() {
        let state = crate::serve::test_support::test_state();
        let op = actor(Role::Operator, None);
        store(
            &state,
            spec_of(json!({"name": "n", "cron": "0 2 * * *", "template": {"id": "t"}})),
            None,
        )
        .await;
        let e = set_paused(&state, &op, "n", true, Some("x".repeat(501)))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("at most 500"), "{e}");
    }

    #[tokio::test]
    async fn run_now_refuses_shared_schedules_to_tenants_and_reports_a_failed_start() {
        let state = crate::serve::test_support::test_state();
        let acme = actor(Role::Operator, Some("acme"));
        let op = actor(Role::Operator, None);
        store(
            &state,
            spec_of(json!({"name": "shared", "cron": "0 2 * * *", "template": {"id": "t"}, "tenants": ["acme"]})),
            None,
        )
        .await;
        let e = run_now(&state, &acme, "shared").await.unwrap_err();
        assert!(matches!(&e, ServeError::Forbidden(m) if m.contains("server operator")));

        store(
            &state,
            spec_of(json!({"name": "broken", "cron": "0 2 * * *", "template": {"id": "nope"}})),
            None,
        )
        .await;
        let e = run_now(&state, &op, "broken").await.unwrap_err();
        assert!(
            matches!(
                &e,
                ServeError::Unprocessable { .. } | ServeError::Unavailable(_)
            ),
            "{e}"
        );
    }

    #[tokio::test]
    async fn run_now_while_shutting_down_is_unavailable() {
        let state = crate::serve::test_support::test_state();
        crate::templates::register(
            &state.history(),
            crate::templates::store::RegisterRequest {
                id: Some("t".into()),
                body: "version: 1\nkind: pipeline\nname: t\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout }\ntests:\n  suite:\n    auto: { defaults_baseline: true }\n".into(),
                format: crate::serve::load::ConfigFormat::Yaml,
                description: None,
                tags: Vec::new(),
                launch: true,
                created_by: None,
            },
        )
        .await
        .unwrap();
        store(
            &state,
            spec_of(json!({"name": "n", "cron": "0 2 * * *", "template": {"id": "t"}})),
            None,
        )
        .await;
        state.set_draining();
        let e = run_now(&state, &actor(Role::Operator, None), "n")
            .await
            .unwrap_err();
        assert!(matches!(e, ServeError::Unavailable(_)), "{e}");
    }

    #[test]
    fn a_tick_already_listed_is_not_listed_again_as_failed_or_skipped() {
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        let tick = at("2026-11-05T02:00:00Z");
        let run = RunRef {
            run_id: "a".into(),
            name: None,
            records_written: 0,
            status: RunStatus::Completed,
            tenant: None,
            submitted_at: tick,
            finished_at: None,
            elapsed_secs: None,
        };
        let mut st = ScheduleStatus::default();
        st.failures.push(FailedTick {
            tick,
            at: tick,
            errors: vec!["refused".into()],
        });
        st.skipped.push(SkippedTick {
            tick,
            reason: "still going".into(),
        });
        let label = BTreeMap::from([(
            fire::LABEL_TICK.to_string(),
            "2026-11-05T02:00:00Z".to_string(),
        )]);
        let out = ticks(vec![run], &[label], &st, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].runs.len(), 1);
    }
}
