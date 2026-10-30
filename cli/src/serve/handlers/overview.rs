//! `GET /v1/overview` (#1006): fleet trends from the run history — run
//! outcomes, records, governance and cost per bucket, the pipelines failing
//! or running slowest, catalog freshness, and what needs attention. Viewer+
//! (`RunRead`); a tenant-scoped principal sees its own tenant's runs.

use crate::serve::error::ServeError;
use crate::serve::history::labels;
use crate::serve::history::overview::{
    Bucket, MAX_BUCKETS, MAX_TOP, OverviewAggregates, OverviewQuery,
};
use crate::serve::rbac::{AuthContext, Permission};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, RawQuery, State};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;

/// Items listed per attention group, and requests scanned for them.
pub const ATTENTION_LIMIT: usize = 20;
/// Templates whose newest version's tests are read for the attention card.
pub const TEMPLATE_SCAN: usize = 200;
/// Tenants whose connections are read for the attention card.
pub const TENANT_SCAN: usize = 200;

/// The Grafana dashboards (`observability/grafana/`) the sections link to.
pub const DASHBOARDS: [(&str, &str); 5] = [
    ("runs", "faucet-pipeline-overview"),
    ("reliability", "faucet-reliability"),
    ("governance", "faucet-governance"),
    ("cost", "faucet-cost"),
    ("serve", "faucet-serve"),
];

/// The parsed query string.
#[derive(Debug, Clone, PartialEq)]
pub struct OverviewParams {
    pub since: Option<String>,
    pub until: Option<String>,
    pub bucket: Option<String>,
    pub pipeline: Option<String>,
    pub pool: Option<String>,
    pub tenant: Option<String>,
    pub stale_after: Option<String>,
    pub top: Option<String>,
    pub label: Vec<String>,
    pub labels: Vec<String>,
}

impl OverviewParams {
    pub fn parse(raw: Option<&str>) -> Result<Self, ServeError> {
        let mut p = Self {
            since: None,
            until: None,
            bucket: None,
            pipeline: None,
            pool: None,
            tenant: None,
            stale_after: None,
            top: None,
            label: Vec::new(),
            labels: Vec::new(),
        };
        for (k, v) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
            let v = v.into_owned();
            let slot = match k.as_ref() {
                "label" => {
                    p.label.push(v);
                    continue;
                }
                "labels" => {
                    p.labels.push(v);
                    continue;
                }
                "since" => &mut p.since,
                "until" => &mut p.until,
                "bucket" => &mut p.bucket,
                "pipeline" => &mut p.pipeline,
                "pool" => &mut p.pool,
                "tenant" => &mut p.tenant,
                "stale_after" => &mut p.stale_after,
                "top" => &mut p.top,
                other => {
                    return Err(ServeError::BadConfig(format!(
                        "unknown query parameter `{other}`"
                    )));
                }
            };
            *slot = Some(v).filter(|s| !s.trim().is_empty());
        }
        Ok(p)
    }

    /// Resolve against `now` into a backend query.
    pub fn into_query(
        self,
        now: DateTime<Utc>,
        tenant: Option<String>,
    ) -> Result<OverviewQuery, ServeError> {
        let bad = |m: String| ServeError::BadConfig(m);
        let until = match self.until.as_deref() {
            None => now,
            Some(s) => parse_instant(s).map_err(|e| bad(format!("until: {e}")))?,
        };
        let since = match self.since.as_deref() {
            None => until - TimeDelta::days(7),
            Some(s) => match parse_duration(s) {
                Some(d) => until - d,
                None => parse_instant(s).map_err(|e| bad(format!("since: {e}")))?,
            },
        };
        if since >= until {
            return Err(bad("`since` must be before `until`".into()));
        }
        let bucket = match self.bucket.as_deref() {
            None => Bucket::for_range(since, until),
            Some(b) => Bucket::parse(b)
                .ok_or_else(|| bad(format!("`bucket` must be `hour` or `day` (got `{b}`)")))?,
        };
        let stale_after = match self.stale_after.as_deref() {
            None => TimeDelta::hours(24),
            Some(s) => parse_duration(s).ok_or_else(|| {
                bad(format!(
                    "`stale_after` must be a duration such as `24h` or `7d` (got `{s}`)"
                ))
            })?,
        };
        let top = match self.top.as_deref() {
            None => crate::serve::history::overview::DEFAULT_TOP,
            Some(t) => t
                .parse::<usize>()
                .ok()
                .filter(|n| (1..=MAX_TOP).contains(n))
                .ok_or_else(|| bad(format!("`top` must be 1..={MAX_TOP} (got `{t}`)")))?,
        };
        let labels = labels::parse_filters(&self.label, &self.labels).map_err(bad)?;
        let q = OverviewQuery {
            since,
            until,
            bucket,
            pipeline: self.pipeline,
            pool: self.pool,
            labels,
            tenant,
            top,
            stale_before: Some(now - stale_after),
        };
        if q.bucket_count() > MAX_BUCKETS {
            return Err(bad(format!(
                "the window spans {} {:?} buckets, more than {MAX_BUCKETS}: \
                 use `bucket=day` or a shorter window",
                q.bucket_count(),
                q.bucket
            )));
        }
        Ok(q)
    }
}

/// `30m`, `24h`, `7d`, `2w`.
pub fn parse_duration(s: &str) -> Option<TimeDelta> {
    let s = s.trim();
    let (n, unit) = s.split_at(s.len().checked_sub(1)?);
    let n: i64 = n.parse().ok().filter(|n| *n > 0)?;
    match unit {
        "m" => TimeDelta::try_minutes(n),
        "h" => TimeDelta::try_hours(n),
        "d" => TimeDelta::try_days(n),
        "w" => TimeDelta::try_weeks(n),
        _ => None,
    }
}

fn parse_instant(s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s.trim())
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| format!("`{s}` is not a duration (`7d`) or an RFC 3339 timestamp"))
}

#[derive(Debug, Serialize)]
pub struct PendingChange {
    pub id: String,
    pub kind: String,
    pub requester: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct UnhealthyTrigger {
    pub name: String,
    pub kind: String,
    pub consecutive_failures: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FailingTemplate {
    pub id: String,
    pub version: u32,
    pub recorded_at: DateTime<Utc>,
    pub failed_cases: usize,
}

#[derive(Debug, Serialize)]
pub struct ReauthConnection {
    pub tenant: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One attention group: how many, and the first [`ATTENTION_LIMIT`].
#[derive(Debug, Serialize)]
pub struct AttentionGroup<T> {
    pub count: usize,
    /// More exist than were counted.
    pub more: bool,
    pub items: Vec<T>,
}

impl<T> AttentionGroup<T> {
    fn of(mut items: Vec<T>, scanned_all: bool) -> Self {
        let count = items.len();
        items.truncate(ATTENTION_LIMIT);
        Self {
            count,
            more: !scanned_all,
            items,
        }
    }
}

/// What needs someone. A group is present only when its feature is built in
/// and in use on this server.
#[derive(Debug, Default, Serialize)]
pub struct Attention {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_changes: Option<AttentionGroup<PendingChange>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unhealthy_triggers: Option<AttentionGroup<UnhealthyTrigger>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failing_templates: Option<AttentionGroup<FailingTemplate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connections_needing_reauth: Option<AttentionGroup<ReauthConnection>>,
}

#[derive(Debug, Serialize)]
pub struct OverviewResponse {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub bucket: Bucket,
    #[serde(flatten)]
    pub aggregates: OverviewAggregates,
    pub attention: Attention,
    /// Dashboard links per section, over the same range (`--grafana-url`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grafana: Option<std::collections::BTreeMap<String, String>>,
}

/// Dashboard links for `[since, until)` under `base`.
pub fn grafana_links(
    base: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> std::collections::BTreeMap<String, String> {
    DASHBOARDS
        .iter()
        .map(|(section, uid)| {
            (
                section.to_string(),
                format!(
                    "{base}/d/{uid}?from={}&to={}",
                    since.timestamp_millis(),
                    until.timestamp_millis()
                ),
            )
        })
        .collect()
}

pub async fn overview(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    RawQuery(raw): RawQuery,
) -> Result<Json<OverviewResponse>, ServeError> {
    let params = OverviewParams::parse(raw.as_deref())?;
    let tenant = actor.tenant_filter(params.tenant.clone())?;
    let mut q = params.into_query(Utc::now(), tenant)?;
    let catalog_visible = cfg!(feature = "catalog")
        && actor.tenant.is_none()
        && actor.role.grants(Permission::CatalogRead);
    if !catalog_visible {
        q.stale_before = None;
    }
    let mut aggregates = state
        .history()
        .overview_aggregates(&q)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    if !cfg!(feature = "catalog") {
        aggregates.cost = None;
    }
    let attention = attention(&state, &actor).await;
    crate::serve::audit::write(&state, &actor, "overview.get", None, None, "ok").await;
    Ok(Json(OverviewResponse {
        since: q.since,
        until: q.until,
        bucket: q.bucket,
        grafana: state
            .grafana_url()
            .map(|base| grafana_links(base, q.since, q.until)),
        aggregates,
        attention,
    }))
}

/// A group whose store read failed is left out (and logged): the rest of the
/// page still renders.
fn soft<T>(group: &str, r: Result<Option<T>, ServeError>) -> Option<T> {
    r.unwrap_or_else(|e| {
        tracing::warn!(group, error = %e, "overview: an attention group could not be read");
        None
    })
}

async fn attention(state: &ServerState, actor: &AuthContext) -> Attention {
    #[cfg_attr(
        not(any(feature = "triggers", feature = "templates", feature = "tenants")),
        allow(unused_mut)
    )]
    let mut out = Attention {
        pending_changes: soft("pending_changes", pending_changes(state, actor).await),
        ..Default::default()
    };
    #[cfg(feature = "triggers")]
    if actor.tenant.is_none() && !state.triggers().is_empty() {
        let bad: Vec<UnhealthyTrigger> = state
            .triggers()
            .snapshot()
            .into_iter()
            .filter(|t| !t.healthy)
            .map(|t| UnhealthyTrigger {
                name: t.name,
                kind: t.kind,
                consecutive_failures: t.consecutive_failures,
                last_error: t
                    .last_error
                    .as_deref()
                    .map(crate::secrets::registry::redact_message),
            })
            .collect();
        out.unhealthy_triggers = Some(AttentionGroup::of(bad, true));
    }
    #[cfg(feature = "templates")]
    {
        out.failing_templates = soft("failing_templates", failing_templates(state).await);
    }
    #[cfg(feature = "tenants")]
    {
        out.connections_needing_reauth = soft(
            "connections_needing_reauth",
            reauth_connections(state, actor).await,
        );
    }
    out
}

async fn pending_changes(
    state: &ServerState,
    actor: &AuthContext,
) -> Result<Option<AttentionGroup<PendingChange>>, ServeError> {
    let pending = crate::serve::changes::list(
        state,
        &crate::serve::changes::ChangeListFilter {
            status: Some(crate::serve::changes::ChangeStatus::Pending),
            tenant: actor.tenant.clone(),
            limit: ATTENTION_LIMIT * 5,
            ..Default::default()
        },
    )
    .await?;
    let pending: Vec<PendingChange> = pending
        .into_iter()
        .filter(|c| c.status == crate::serve::changes::ChangeStatus::Pending)
        .map(|c| PendingChange {
            id: c.id,
            kind: c.kind.as_str().to_string(),
            requester: c.requester,
            created_at: c.created_at,
            expires_at: c.expires_at,
        })
        .collect();
    if pending.is_empty() && state.require_approval().is_empty() {
        return Ok(None);
    }
    let all = pending.len() < ATTENTION_LIMIT * 5;
    Ok(Some(AttentionGroup::of(pending, all)))
}

#[cfg(feature = "templates")]
async fn failing_templates(
    state: &ServerState,
) -> Result<Option<AttentionGroup<FailingTemplate>>, ServeError> {
    let history = state.history();
    let templates = history
        .template_list()
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    if templates.is_empty() {
        return Ok(None);
    }
    let all = templates.len() <= TEMPLATE_SCAN;
    let mut failing = Vec::new();
    for t in templates.iter().take(TEMPLATE_SCAN) {
        let newest = history
            .template_test_results(&t.id, Some(t.version), 1)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?;
        if let Some(r) = newest.into_iter().next().filter(|r| !r.passed) {
            failing.push(FailingTemplate {
                id: t.id.clone(),
                version: t.version,
                recorded_at: r.recorded_at,
                failed_cases: r.cases.iter().filter(|c| !c.passed).count(),
            });
        }
    }
    failing.sort_by(|a, b| {
        b.recorded_at
            .cmp(&a.recorded_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(Some(AttentionGroup::of(failing, all)))
}

#[cfg(feature = "tenants")]
async fn reauth_connections(
    state: &ServerState,
    actor: &AuthContext,
) -> Result<Option<AttentionGroup<ReauthConnection>>, ServeError> {
    let history = state.history();
    let internal = |e: crate::serve::history::HistoryError| ServeError::Internal(e.to_string());
    let tenants: Vec<String> = match &actor.tenant {
        Some(t) => vec![t.clone()],
        None => history
            .tenant_list()
            .await
            .map_err(internal)?
            .into_iter()
            .map(|t| t.id)
            .collect(),
    };
    if tenants.is_empty() {
        return Ok(None);
    }
    let all = tenants.len() <= TENANT_SCAN;
    let mut items = Vec::new();
    for t in tenants.iter().take(TENANT_SCAN) {
        for c in history.connection_list(t).await.map_err(internal)? {
            if c.status == crate::serve::history::tenants::ConnectionStatus::NeedsReauth {
                items.push(ReauthConnection {
                    tenant: c.tenant,
                    name: c.name,
                    reason: c.reauth_reason,
                });
            }
        }
    }
    Ok(Some(AttentionGroup::of(items, all)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-30T12:34:56Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn durations_parse_with_a_unit() {
        assert_eq!(parse_duration("30m"), Some(TimeDelta::minutes(30)));
        assert_eq!(parse_duration("24h"), Some(TimeDelta::hours(24)));
        assert_eq!(parse_duration("7d"), Some(TimeDelta::days(7)));
        assert_eq!(parse_duration("2w"), Some(TimeDelta::weeks(2)));
        assert_eq!(parse_duration("0d"), None);
        assert_eq!(parse_duration("7"), None);
        assert_eq!(parse_duration("7y"), None);
        assert_eq!(parse_duration(""), None);
    }

    #[test]
    fn defaults_are_seven_days_by_day_with_a_day_stale_window() {
        let q = OverviewParams::parse(None)
            .unwrap()
            .into_query(now(), None)
            .unwrap();
        assert_eq!(q.until, now());
        assert_eq!(q.since, now() - TimeDelta::days(7));
        assert_eq!(q.bucket, Bucket::Day);
        assert_eq!(q.top, 10);
        assert_eq!(q.stale_before, Some(now() - TimeDelta::hours(24)));
    }

    #[test]
    fn a_day_buckets_by_hour_and_filters_are_carried() {
        let q = OverviewParams::parse(Some(
            "since=24h&pipeline=orders&pool=gpu&label=team:data&top=3&stale_after=2h",
        ))
        .unwrap()
        .into_query(now(), Some("acme".into()))
        .unwrap();
        assert_eq!(q.bucket, Bucket::Hour);
        assert_eq!(q.since, now() - TimeDelta::hours(24));
        assert_eq!(q.pipeline.as_deref(), Some("orders"));
        assert_eq!(q.pool.as_deref(), Some("gpu"));
        assert_eq!(q.tenant.as_deref(), Some("acme"));
        assert_eq!(q.labels.len(), 1);
        assert_eq!(q.top, 3);
        assert_eq!(q.stale_before, Some(now() - TimeDelta::hours(2)));
    }

    #[test]
    fn absolute_bounds_and_refusals() {
        let q = OverviewParams::parse(Some(
            "since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z&bucket=day",
        ))
        .unwrap()
        .into_query(now(), None)
        .unwrap();
        assert_eq!(q.bucket, Bucket::Day);
        assert_eq!(q.bucket_count(), 1);
        for bad in [
            "since=yesterday",
            "until=nope",
            "bucket=week",
            "top=0",
            "top=51",
            "stale_after=soon",
            "since=2026-10-31T00:00:00Z",
            "since=90d&bucket=hour",
            "label=",
        ] {
            let r = OverviewParams::parse(Some(bad)).and_then(|p| p.into_query(now(), None));
            assert!(r.is_err(), "{bad} must be refused");
        }
        assert!(OverviewParams::parse(Some("color=red")).is_err());
    }

    #[test]
    fn grafana_links_name_each_dashboard_over_the_range() {
        let links = grafana_links(
            "https://grafana.example",
            now() - TimeDelta::hours(1),
            now(),
        );
        assert_eq!(links.len(), DASHBOARDS.len());
        let from = (now() - TimeDelta::hours(1)).timestamp_millis();
        assert_eq!(
            links["cost"],
            format!(
                "https://grafana.example/d/faucet-cost?from={from}&to={}",
                now().timestamp_millis()
            )
        );
        assert!(links["runs"].contains("/d/faucet-pipeline-overview?"));
    }

    #[test]
    fn attention_groups_cap_their_items_and_keep_the_count() {
        let g = AttentionGroup::of((0..25).collect::<Vec<_>>(), true);
        assert_eq!(g.count, 25);
        assert_eq!(g.items.len(), ATTENTION_LIMIT);
        assert!(!g.more);
        assert!(AttentionGroup::of(vec![1], false).more);
    }
}
