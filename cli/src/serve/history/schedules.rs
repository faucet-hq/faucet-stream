//! Stored schedules (#1044): the definition (`ScheduleSpec`, also the shape of
//! an entry in the `--schedules` file) and the record the run history keeps
//! for a schedule made through the API or the console. Pure data; the
//! scheduler, validation and API live in `crate::serve::schedules`.

use super::tenants::TenantSelector;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Most edits a record keeps in its change log.
pub const CHANGE_LOG_RETAIN: usize = 50;

/// One schedule: when it runs and which registered template it runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    /// Unique name: letters, digits, `_` and `-`, at most 64 characters.
    /// Names the schedule in the API, metrics and the runs it starts.
    pub name: String,
    /// What the schedule is for, shown in the console.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Cron expression, 5 fields (`min hour day month weekday`) or 6 with
    /// leading seconds.
    pub cron: String,
    /// IANA timezone the cron is evaluated in. Default `UTC`.
    #[serde(default = "default_timezone")]
    pub timezone: String,
    /// After an outage, run the most recent missed tick once when a server
    /// comes back. `false` skips missed ticks. Default `true`.
    #[serde(default = "default_true")]
    pub catch_up: bool,
    /// What a tick does while a run the previous tick started (for the same
    /// tenant) is still queued or running. Default `skip`.
    #[serde(default)]
    pub overlap: OverlapMode,
    /// Paused schedules keep their definition but start no runs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paused: bool,
    /// The registered template each tick runs.
    pub template: ScheduleTemplate,
    /// Run once per tenant on every tick: `all` (every tenant that is not
    /// suspended) or a list of tenant ids. Each tenant's run is admitted
    /// against that tenant's limits. Omit for one run on the server itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenants: Option<TenantSelector>,
    /// Optional run-shaping for the runs a tick starts.
    #[serde(default, skip_serializing_if = "ScheduleRun::is_empty")]
    pub run: ScheduleRun,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_true() -> bool {
    true
}

/// What a tick does while an earlier tick's run is still going — the same
/// vocabulary as `faucet schedule`'s `overlap_policy`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverlapMode {
    /// Drop the tick (recorded as skipped).
    #[default]
    Skip,
    /// Hold the tick and start it once the earlier run finishes; ticks that
    /// come due meanwhile collapse into it.
    Queue,
    /// Record the tick as failed.
    Forbid,
}

impl OverlapMode {
    pub fn as_str(self) -> &'static str {
        match self {
            OverlapMode::Skip => "skip",
            OverlapMode::Queue => "queue",
            OverlapMode::Forbid => "forbid",
        }
    }
}

/// The registered template a schedule runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleTemplate {
    /// Template id.
    pub id: String,
    /// Version: a number or a channel. Default `stable`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub version: Option<String>,
    /// Values for the template's `params:`. A string value may use
    /// `${schedule.tick}` (the scheduled instant, RFC 3339 UTC),
    /// `${schedule.name}` and `${schedule.fired_at}`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
    /// For a source template: the sink template to compose in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// The sink template's version: a number or a channel.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub sink_version: Option<String>,
    /// A registered deployment overlay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    /// The overlay's version: a number or a channel.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub overlay_version: Option<String>,
}

/// A template version as text: a channel name or numeric string as given, or a
/// non-negative integer rendered in decimal, so a body or file may write
/// `version: 3` or `version: "3"` and both store `"3"`.
fn version_text<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = Option<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a version number or a channel name")
        }
        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
            Ok(Some(s.to_owned()))
        }
        fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Self::Value, E> {
            Ok(Some(n.to_string()))
        }
        fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Self::Value, E> {
            u64::try_from(n).map(|n| Some(n.to_string())).map_err(|_| {
                E::custom(format!(
                    "version {n} is negative; expected a version number or a channel name"
                ))
            })
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }
    d.deserialize_any(V)
}

fn version_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "description": "A version number or a channel name.",
        "type": ["string", "integer"],
        "minimum": 0
    })
}

/// Run-shaping for the runs a tick starts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRun {
    /// Run name; `{name}` and `{tick}` are replaced. Default: the schedule name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Static labels added to every run.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Per-run timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Run only some of the template's streams / rows (#741).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<crate::select::SelectionRequest>,
    /// The execution pool the runs go to (#957).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
}

impl ScheduleRun {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl ScheduleSpec {
    /// Whether this schedule runs for `tenant`: owned by it (`owner`) or a
    /// fan-out whose selector includes it.
    pub fn runs_for(&self, owner: Option<&str>, tenant: &str) -> bool {
        owner == Some(tenant)
            || match &self.tenants {
                Some(TenantSelector::All(_)) => true,
                Some(TenantSelector::Named(ids)) => ids.iter().any(|t| t == tenant),
                None => false,
            }
    }
}

/// One edit in a schedule's change log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleChange {
    pub at: DateTime<Utc>,
    /// The principal that made it.
    pub by: String,
    /// `created`, `updated`, `paused`, `resumed` or `run_now`.
    pub action: String,
    /// What changed, in short (`cron: 0 2 * * * → 0 3 * * *`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub detail: Vec<String>,
}

/// A schedule made through the API or the console, as the run history keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleRecord {
    /// A stable id, kept across edits (#1051): secret param values are
    /// sealed to it. Set by the server, never from the wire.
    #[serde(default)]
    pub id: String,
    pub spec: ScheduleSpec,
    /// The tenant that owns the schedule (made by its own principal): it runs
    /// only for that tenant. `None` for a server-wide schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
    /// Bumped on every write, so a reader can tell whether it changed.
    pub revision: u64,
    /// Who paused it, when and why (while paused).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
    /// Newest last, at most [`CHANGE_LOG_RETAIN`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ScheduleChange>,
    /// The current definition came through an approved change request
    /// (#1051): under `--require-approval run`, only such a schedule (or a
    /// `--schedules` file one) starts runs without a per-run approval. Set
    /// by the server, never from the wire.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub approved: bool,
}

impl ScheduleRecord {
    pub fn new(spec: ScheduleSpec, tenant: Option<String>, by: &str, now: DateTime<Utc>) -> Self {
        let mut r = Self {
            id: uuid::Uuid::new_v4().to_string(),
            spec,
            tenant,
            created_at: now,
            created_by: by.to_string(),
            updated_at: now,
            updated_by: by.to_string(),
            revision: 1,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            changes: Vec::new(),
            approved: false,
        };
        r.log(now, by, "created", Vec::new());
        r
    }

    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// Append to the change log, keeping the newest [`CHANGE_LOG_RETAIN`].
    pub fn log(&mut self, at: DateTime<Utc>, by: &str, action: &str, detail: Vec<String>) {
        self.changes.push(ScheduleChange {
            at,
            by: by.to_string(),
            action: action.to_string(),
            detail,
        });
        let over = self.changes.len().saturating_sub(CHANGE_LOG_RETAIN);
        self.changes.drain(..over);
    }

    /// The next revision of this record: `spec` replaced, stamped by `by`.
    pub fn revised(
        &self,
        spec: ScheduleSpec,
        by: &str,
        now: DateTime<Utc>,
        action: &str,
        detail: Vec<String>,
    ) -> Self {
        let mut r = self.clone();
        r.spec = spec;
        r.updated_at = now;
        r.updated_by = by.to_string();
        r.revision += 1;
        r.log(now, by, action, detail);
        r
    }
}

/// The fields that differ between two specs, one line each
/// (`cron: 0 2 * * * → 0 3 * * *`); empty when equal.
pub fn spec_diff(before: &ScheduleSpec, after: &ScheduleSpec) -> Vec<String> {
    let (Ok(serde_json::Value::Object(a)), Ok(serde_json::Value::Object(b))) =
        (serde_json::to_value(before), serde_json::to_value(after))
    else {
        return Vec::new();
    };
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();
    let show = |v: Option<&serde_json::Value>| match v {
        None => "—".to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    };
    keys.into_iter()
        .filter(|k| a.get(*k) != b.get(*k))
        .map(|k| format!("{k}: {} → {}", show(a.get(k)), show(b.get(k))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> ScheduleSpec {
        serde_json::from_value(json!({
            "name": "nightly",
            "cron": "0 2 * * *",
            "template": {"id": "acme/billing"}
        }))
        .unwrap()
    }

    #[test]
    fn defaults_and_round_trip() {
        let s = spec();
        assert_eq!(s.timezone, "UTC");
        assert!(s.catch_up);
        assert_eq!(s.overlap, OverlapMode::Skip);
        assert!(!s.paused);
        assert!(s.run.is_empty());
        let v = serde_json::to_value(&s).unwrap();
        assert!(v.get("paused").is_none() && v.get("run").is_none(), "{v}");
        assert_eq!(serde_json::from_value::<ScheduleSpec>(v).unwrap(), s);
        assert_eq!(OverlapMode::Queue.as_str(), "queue");
        assert_eq!(OverlapMode::Forbid.as_str(), "forbid");
        assert_eq!(OverlapMode::Skip.as_str(), "skip");
    }

    #[test]
    fn template_versions_take_a_number_or_text_and_store_text() {
        let t: ScheduleTemplate = serde_json::from_value(json!({
            "id": "t", "version": 3, "sink": "s", "sink_version": "7",
            "overlay": "o", "overlay_version": "stable"
        }))
        .unwrap();
        assert_eq!(t.version.as_deref(), Some("3"));
        assert_eq!(t.sink_version.as_deref(), Some("7"));
        assert_eq!(t.overlay_version.as_deref(), Some("stable"));
        assert_eq!(serde_json::to_value(&t).unwrap()["version"], json!("3"));
        let y: ScheduleTemplate =
            crate::yaml::from_str("id: t\nversion: 4\nsink_version: 0\n").unwrap();
        assert_eq!(
            (y.version.as_deref(), y.sink_version.as_deref()),
            (Some("4"), Some("0"))
        );
        let none: ScheduleTemplate =
            serde_json::from_value(json!({"id": "t", "version": null})).unwrap();
        assert_eq!(none.version, None);
        for bad in [json!(-1), json!(1.5), json!(true), json!(["1"])] {
            let err = serde_json::from_value::<ScheduleTemplate>(
                json!({"id": "t", "overlay_version": bad}),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("a version number or a channel name"), "{err}");
        }
    }

    #[test]
    fn the_version_schema_allows_a_number() {
        let schema = serde_json::to_value(schemars::schema_for!(ScheduleTemplate)).unwrap();
        for field in ["version", "sink_version", "overlay_version"] {
            assert_eq!(
                schema["properties"][field]["type"],
                json!(["string", "integer"]),
                "{field}"
            );
        }
    }

    #[test]
    fn unknown_fields_are_refused() {
        let err = serde_json::from_value::<ScheduleSpec>(json!({
            "name": "n", "cron": "* * * * *", "template": {"id": "t"}, "crons": 1
        }))
        .unwrap_err();
        assert!(err.to_string().contains("crons"), "{err}");
    }

    #[test]
    fn runs_for_owner_and_fan_out() {
        let mut s = spec();
        assert!(s.runs_for(Some("acme"), "acme"));
        assert!(!s.runs_for(None, "acme"));
        s.tenants = Some(TenantSelector::Named(vec!["acme".into(), "globex".into()]));
        assert!(s.runs_for(None, "globex"));
        assert!(!s.runs_for(None, "initech"));
        s.tenants = Some(TenantSelector::All("all".into()));
        assert!(s.runs_for(None, "initech"));
    }

    #[test]
    fn records_keep_a_bounded_change_log_and_count_revisions() {
        let now = Utc::now();
        let mut r = ScheduleRecord::new(spec(), None, "ops", now);
        assert_eq!(r.revision, 1);
        assert_eq!(r.name(), "nightly");
        assert_eq!(r.changes[0].action, "created");
        let mut edited = spec();
        edited.cron = "0 3 * * *".into();
        let detail = spec_diff(&r.spec, &edited);
        assert_eq!(detail, vec!["cron: 0 2 * * * → 0 3 * * *".to_string()]);
        let r2 = r.revised(edited, "raj", now, "updated", detail);
        assert_eq!(r2.revision, 2);
        assert_eq!(r2.updated_by, "raj");
        assert_eq!(r2.changes.last().unwrap().action, "updated");
        for _ in 0..CHANGE_LOG_RETAIN + 5 {
            r.log(now, "x", "paused", Vec::new());
        }
        assert_eq!(r.changes.len(), CHANGE_LOG_RETAIN);
    }

    #[test]
    fn spec_diff_names_added_and_removed_fields() {
        let a = spec();
        let mut b = spec();
        b.description = Some("rollup".into());
        b.paused = true;
        let d = spec_diff(&a, &b);
        assert!(d.contains(&"description: — → rollup".to_string()), "{d:?}");
        assert!(d.contains(&"paused: — → true".to_string()), "{d:?}");
        assert!(spec_diff(&a, &a).is_empty());
    }
}
