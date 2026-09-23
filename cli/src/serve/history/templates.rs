//! Pipeline-template registry records (#444) — the persistent, versioned store
//! behind `faucet template …`, the `/v1/templates` endpoints, and the MCP
//! template tools.
//!
//! A template is a **config document registered once** plus the typed `params:`
//! it declares. Thereafter a caller triggers runs by `{id, params}` instead of
//! re-sending (and re-validating) the whole config. Storage rides the
//! `RunHistory` backends, like the Data Movement Catalog: an in-memory map for
//! the default backend, a `faucet_templates` table for the SQL ones, forwarded
//! by `FallbackHistory`.
//!
//! **Nothing secret is persisted.** The body is stored *verbatim* as submitted,
//! so `${env:…}` / `${vault:…}` remain unresolved tokens that are resolved at
//! trigger time, on the instance that runs the pipeline — the same privilege
//! surface as any other submitted config. Caller-supplied `secret: true` param
//! values are never written here at all: they exist only for the duration of one
//! trigger.

use crate::error::{CliError, CliResult};
use crate::params::ParamsSpec;
use crate::serve::load::ConfigFormat;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum length of a template id. Long enough for `team-service-purpose`,
/// short enough to stay readable in a URL path and a CLI table.
pub const MAX_ID_LEN: usize = 64;

/// The channel an unpinned request resolves to: the **launched** version.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Rejected spelling. `latest` is ambiguous in this model — it could mean the
/// blessed release (`stable`) or the highest build number (`newest`) — so it is
/// refused rather than silently picking one.
pub const REJECTED_LATEST: &str = "latest";

/// A **named version channel** — a pointer at one numeric version.
///
/// Versions are numeric builds; channels are the human-facing names a build is
/// promoted *into*, like npm dist-tags. Three are **derived** (computed, never
/// assignable):
///
/// - [`Self::Stable`] — the **launched** version, and what an unpinned request
///   resolves to. Moved only by an explicit `launch`, so a newly registered
///   build (a nightly, a feature branch) never drags existing callers with it.
/// - [`Self::Previous`] — the version launched *before* the current one, for a
///   one-step rollback. Empty until a second launch has happened.
/// - [`Self::Newest`] — the highest version number, launched or not. The "run
///   what I just pushed" selector for development and CI.
///
/// The rest are assignable environment pointers moved with `promote`. The set is
/// deliberately **closed**: an open tag namespace becomes a second, unreviewable
/// naming system in which a typo (`prd`) silently creates a channel nobody
/// watches. Free-form labels belong on the *run*, not in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum VersionChannel {
    /// The launched version — the default for an unpinned request. Derived from
    /// the launch log; moved by `launch`, never by `promote`.
    #[default]
    Stable,
    /// The previously launched version. Derived; the rollback target.
    Previous,
    /// The highest registered version number, launched or not. Derived.
    Newest,
    /// Production.
    Prod,
    /// Pre-production / release-candidate soak.
    PreProd,
    /// Staging.
    Staging,
    /// A partial-traffic canary ahead of `prod`.
    Canary,
    /// QA / integration testing.
    Test,
    /// Day-to-day development.
    Dev,
}

impl VersionChannel {
    /// Every channel: the derived release pointers first, then the assignable
    /// environments in promotion order (`dev` → … → `prod`). Drives help text,
    /// error messages, and `--tag` completion.
    pub const ALL: &'static [Self] = &[
        Self::Stable,
        Self::Previous,
        Self::Newest,
        Self::Dev,
        Self::Test,
        Self::Staging,
        Self::PreProd,
        Self::Canary,
        Self::Prod,
    ];

    /// The channels a caller may `promote`. Excludes the three derived release
    /// pointers — `stable` moves via `launch`, and `previous` / `newest` are
    /// computed.
    pub const ASSIGNABLE: &'static [Self] = &[
        Self::Dev,
        Self::Test,
        Self::Staging,
        Self::PreProd,
        Self::Canary,
        Self::Prod,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stable => DEFAULT_CHANNEL,
            Self::Previous => "previous",
            Self::Newest => "newest",
            Self::Prod => "prod",
            Self::PreProd => "pre-prod",
            Self::Staging => "staging",
            Self::Canary => "canary",
            Self::Test => "test",
            Self::Dev => "dev",
        }
    }

    /// Whether this channel is computed rather than assigned. A derived channel
    /// can never be the target of `promote`.
    pub fn is_derived(self) -> bool {
        matches!(self, Self::Stable | Self::Previous | Self::Newest)
    }

    /// Parse a channel name. Case-insensitive, and `-`/`_` separators are
    /// interchangeable, so `pre-prod`, `pre_prod`, `PreProd`, and `preprod` all
    /// name the same channel.
    ///
    /// `latest` gets a bespoke error rather than the generic one: it is the most
    /// likely thing a newcomer types, and both plausible meanings exist under
    /// other names, so naming them is more useful than listing all nine.
    pub fn parse(raw: &str) -> CliResult<Self> {
        let normalized = normalize(raw);
        if normalized == REJECTED_LATEST {
            return Err(CliError::Config(format!(
                "`{REJECTED_LATEST}` is not a version channel here because it is ambiguous. \
                 Did you mean `{DEFAULT_CHANNEL}` (the launched version — also the default when \
                 no version is given), or `newest` (the highest version number, launched or not)?"
            )));
        }
        Self::ALL
            .iter()
            .copied()
            .find(|c| normalize(c.as_str()) == normalized)
            .ok_or_else(|| {
                CliError::Config(format!(
                    "unknown template version channel '{raw}' — the named channels are fixed: {}",
                    Self::ALL
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }
}

/// Lowercase and strip separators so every spelling of a channel compares equal.
fn normalize(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

impl std::fmt::Display for VersionChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for VersionChannel {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VersionChannel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// The lifecycle state of a **template** (not of an individual version).
///
/// Derived, so it can never disagree with the registry's actual contents: only
/// the deprecation marker is stored, and `draft` vs `launched` falls out of
/// whether anything has been launched yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateStatus {
    /// Registered but never launched — the work-in-progress state. An unpinned
    /// request fails (there is no blessed version); explicit selectors still
    /// work, so a draft template is fully testable.
    Draft,
    /// A version has been launched; `stable` points at it and unpinned requests
    /// resolve to it.
    Launched,
    /// Explicitly retired. Unpinned requests still resolve `stable` — retiring
    /// must not hard-break existing callers — but they warn, and listings mark
    /// it. `delete` is the hard stop.
    Deprecated,
}

impl TemplateStatus {
    /// Derive the status from the two facts that determine it.
    pub fn derive(has_launch: bool, deprecated: bool) -> Self {
        match (deprecated, has_launch) {
            (true, _) => Self::Deprecated,
            (false, true) => Self::Launched,
            (false, false) => Self::Draft,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Launched => "launched",
            Self::Deprecated => "deprecated",
        }
    }
}

impl std::fmt::Display for TemplateStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which version of a template to use: a named [`VersionChannel`] or an exact
/// number.
///
/// Omitting the selector is the same as `stable`, so a caller that never mentions
/// versions rides the **launched** version — not whatever was registered most
/// recently. Registering a nightly therefore moves nobody; only an explicit
/// `launch` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelector {
    /// A named channel — `stable` (the default), another derived pointer, or a
    /// promoted environment.
    Channel(VersionChannel),
    /// An exact version, for a pin or a rollback.
    Pinned(u32),
}

impl Default for VersionSelector {
    fn default() -> Self {
        Self::stable()
    }
}

impl VersionSelector {
    /// The default selector: the launched version.
    pub const fn stable() -> Self {
        Self::Channel(VersionChannel::Stable)
    }

    /// The highest registered version, launched or not.
    pub const fn newest() -> Self {
        Self::Channel(VersionChannel::Newest)
    }

    /// Parse a channel name or a positive integer. A numeric string is a pin; a
    /// name must be one of the closed channel set.
    pub fn parse(raw: &str) -> CliResult<Self> {
        let s = raw.trim();
        // Digits are always a pin — never confused with a channel name.
        if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
            return match s.parse::<u32>() {
                Ok(0) | Err(_) => Err(CliError::Config(format!(
                    "invalid template version '{raw}' — versions are numbered from 1"
                ))),
                Ok(n) => Ok(Self::Pinned(n)),
            };
        }
        VersionChannel::parse(s).map(Self::Channel)
    }

    /// True when this selector means "the launched version" — i.e. the default.
    pub fn is_stable(self) -> bool {
        matches!(self, Self::Channel(VersionChannel::Stable))
    }

    /// The exact version, when the selector already names one. **Every** channel
    /// — derived or assigned — needs a registry lookup, so callers must go
    /// through [`crate::templates::resolve_version`] rather than treating a
    /// `None` here as "the newest".
    pub fn pinned(self) -> Option<u32> {
        match self {
            Self::Pinned(n) => Some(n),
            Self::Channel(_) => None,
        }
    }

    /// The channel this selector names, if any.
    pub fn channel(self) -> Option<VersionChannel> {
        match self {
            Self::Channel(c) => Some(c),
            Self::Pinned(_) => None,
        }
    }
}

impl std::fmt::Display for VersionSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(c) => f.write_str(c.as_str()),
            Self::Pinned(n) => write!(f, "{n}"),
        }
    }
}

// Accepts a JSON/query value in any of the natural spellings — `"latest"`,
// `"pre-prod"`, `"3"`, or the bare number `3` — so an HTTP query string, a JSON
// body, and an MCP tool argument all deserialize the same way.
impl<'de> Deserialize<'de> for VersionSelector {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = VersionSelector;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "a named version channel or a version number")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                VersionSelector::parse(s).map_err(serde::de::Error::custom)
            }
            fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Self::Value, E> {
                VersionSelector::parse(&n.to_string()).map_err(serde::de::Error::custom)
            }
            fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Self::Value, E> {
                VersionSelector::parse(&n.to_string()).map_err(serde::de::Error::custom)
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for VersionSelector {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

/// A validated template id: lowercase kebab/snake slug, `^[a-z0-9][a-z0-9_-]*$`.
///
/// Ids appear in URL paths (`/v1/templates/{id}`) and as CLI arguments, so the
/// charset is deliberately narrow — no slashes, dots, whitespace, or uppercase,
/// which rules out path traversal and case-collision surprises across backends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TemplateId(String);

impl TemplateId {
    pub fn parse(raw: &str) -> CliResult<Self> {
        let s = raw.trim();
        if s.is_empty() {
            return Err(CliError::Config(
                "template id must not be empty — pass one with `--id`, or give the config a `name:`"
                    .into(),
            ));
        }
        if s.len() > MAX_ID_LEN {
            return Err(CliError::Config(format!(
                "template id '{s}' is longer than {MAX_ID_LEN} characters"
            )));
        }
        let mut chars = s.chars();
        let ok = match chars.next() {
            Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
                chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
            }
            _ => false,
        };
        if !ok {
            return Err(CliError::Config(format!(
                "invalid template id '{s}' — ids must match ^[a-z0-9][a-z0-9_-]*$ (lowercase \
                 letters, digits, `-`, `_`; first character alphanumeric)"
            )));
        }
        Ok(Self(s.to_string()))
    }

    /// Derive an id from a config's `name:` — lowercased, with runs of
    /// unsupported characters collapsed to `-`. Used when the caller registers
    /// without an explicit id.
    pub fn from_config_name(name: &str) -> CliResult<Self> {
        let mut slug = String::with_capacity(name.len());
        for c in name.chars() {
            if c.is_ascii_alphanumeric() {
                slug.push(c.to_ascii_lowercase());
            } else if !slug.ends_with('-') {
                slug.push('-');
            }
        }
        let trimmed = slug.trim_matches('-');
        let capped: String = trimmed.chars().take(MAX_ID_LEN).collect();
        Self::parse(capped.trim_matches('-'))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TemplateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for TemplateId {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value).map_err(|e| e.to_string())
    }
}

impl From<TemplateId> for String {
    fn from(value: TemplateId) -> Self {
        value.0
    }
}

/// One registered version of a template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateRecord {
    /// Stable registry id (a slug — see [`TemplateId`]).
    pub id: String,
    /// Monotonic version, starting at 1. `register` always appends a new one.
    pub version: u32,
    /// What the body is: a hub `source-template` / `sink-template` (composed
    /// with a partner at trigger time) or a complete `pipeline`. Records
    /// written before kinds existed read back as `pipeline`.
    #[serde(default = "crate::hub::TemplateKind::pipeline")]
    pub kind: crate::hub::TemplateKind,
    /// The config's own `name:`, if it has one (informational).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Free-text description supplied at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The config document, stored **verbatim** (unresolved directives intact).
    pub body: String,
    /// Wire format of `body`, so the trigger path parses it the same way.
    pub format: ConfigFormat,
    /// The declared `params:` block, extracted at registration so callers can
    /// discover the trigger surface without parsing the body.
    #[serde(default)]
    pub params: ParamsSpec,
    pub created_at: DateTime<Utc>,
    /// Principal that registered this version (`None` for CLI registration).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
}

impl TemplateRecord {
    /// The summary view — everything except the (potentially large) body, with no
    /// release state attached. Use [`Self::summary_with`] where the state is known.
    pub fn summary(&self) -> TemplateSummary {
        TemplateSummary {
            id: self.id.clone(),
            version: self.version,
            kind: self.kind,
            name: self.name.clone(),
            description: self.description.clone(),
            params: self.params.clone(),
            created_at: self.created_at,
            created_by: self.created_by.clone(),
            state: None,
        }
    }

    /// The summary view carrying the template's release state.
    pub fn summary_with(&self, state: TemplateState) -> TemplateSummary {
        TemplateSummary {
            state: Some(state),
            ..self.summary()
        }
    }
}

/// Body-free view of a template version, used by list endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSummary {
    pub id: String,
    /// The newest registered version (the build tip). Present for continuity with
    /// the per-version record; `state.stable` is what an unpinned run uses.
    pub version: u32,
    #[serde(default = "crate::hub::TemplateKind::pipeline")]
    pub kind: crate::hub::TemplateKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub params: ParamsSpec,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Release state of the template as a whole (status, `stable` / `previous` /
    /// `newest`, channel pointers). Populated by the read paths; `None` on a
    /// record that was built without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<TemplateState>,
}

/// One entry in a template's append-only **launch log**.
///
/// The log is the single source of truth for the release pointers: `stable` is
/// the newest entry's version and `previous` is the one before it. Because it is
/// append-only it doubles as the launch/rollback audit trail — who blessed which
/// build, and when.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchRecord {
    /// Monotonic per-template sequence, from 1.
    pub seq: u32,
    /// The version that was launched.
    pub version: u32,
    pub launched_at: DateTime<Utc>,
    /// Principal that launched it (`None` for a CLI launch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launched_by: Option<String>,
}

/// Why and when a template was retired. Stored only while deprecated; clearing it
/// is the `--undo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecationRecord {
    pub deprecated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Everything a surface needs to describe a template's release state, read in one
/// go so the CLI, HTTP handlers, MCP tools, and UI can never assemble it
/// inconsistently from separate calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateState {
    /// Derived lifecycle status of the **template**.
    pub status: TemplateStatus,
    /// Every stored version, newest first.
    pub versions: Vec<u32>,
    /// The launched version — what `stable` and an unpinned request resolve to.
    /// `None` while the template is a draft.
    pub stable: Option<u32>,
    /// The version launched before the current one; the rollback target.
    pub previous: Option<u32>,
    /// Highest version number, launched or not.
    pub newest: Option<u32>,
    /// Assignable channel pointers (`{channel: version}`), excluding the derived
    /// ones.
    pub tags: BTreeMap<String, u32>,
    /// Present only when `status` is `deprecated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecation: Option<DeprecationRecord>,
}

impl TemplateState {
    /// Assemble the state from the raw facts. Pure, so the memory and SQL
    /// backends cannot disagree about what a set of rows means.
    pub fn assemble(
        versions: Vec<u32>,
        launches: &[LaunchRecord],
        tags: BTreeMap<String, u32>,
        deprecation: Option<DeprecationRecord>,
    ) -> Self {
        let stable = stable_version(launches);
        Self {
            status: TemplateStatus::derive(stable.is_some(), deprecation.is_some()),
            newest: versions.first().copied(),
            stable,
            previous: previous_version(launches),
            versions,
            tags,
            deprecation,
        }
    }

    /// Resolve a derived channel against this state.
    pub fn derived(&self, channel: VersionChannel) -> Option<u32> {
        match channel {
            VersionChannel::Stable => self.stable,
            VersionChannel::Previous => self.previous,
            VersionChannel::Newest => self.newest,
            other => self.tags.get(other.as_str()).copied(),
        }
    }
}

/// The launched version: the newest launch-log entry. `launches` must be ordered
/// newest-first (highest `seq` at index 0).
pub fn stable_version(launches: &[LaunchRecord]) -> Option<u32> {
    launches.first().map(|l| l.version)
}

/// The version launched *before* the current one — the rollback target.
///
/// Relies on the store never appending a launch for the version that is already
/// stable (a re-launch is a no-op), so entry 1 is genuinely the prior release
/// rather than a duplicate of the current one. Defensively skips any leading
/// duplicates anyway, so a hand-edited log can't make `previous == stable`.
pub fn previous_version(launches: &[LaunchRecord]) -> Option<u32> {
    let current = stable_version(launches)?;
    launches.iter().map(|l| l.version).find(|v| *v != current)
}

/// A registration request, after validation. `version` is assigned by the store.
#[derive(Debug, Clone)]
pub struct TemplateDraft {
    pub id: TemplateId,
    pub kind: crate::hub::TemplateKind,
    pub name: Option<String>,
    pub description: Option<String>,
    pub body: String,
    pub format: ConfigFormat,
    pub params: ParamsSpec,
    pub created_by: Option<String>,
}

/// How many versions of one template the store keeps. Older versions are pruned
/// on register, so a template re-registered on every deploy can't grow the table
/// without bound while the recent history (for pinning / rollback) stays.
pub const VERSION_RETAIN: usize = 20;

/// Reduce a full version list to the latest version per id, preserving the
/// caller's ordering intent (newest-created first). Shared by the memory and SQL
/// backends so `template_list` can never disagree between them.
pub fn latest_per_id(mut records: Vec<TemplateRecord>) -> Vec<TemplateSummary> {
    // Highest version wins per id; ties are impossible (version is unique).
    records.sort_by(|a, b| a.id.cmp(&b.id).then(b.version.cmp(&a.version)));
    records.dedup_by(|a, b| a.id == b.id);
    let mut out: Vec<TemplateSummary> = records.iter().map(TemplateRecord::summary).collect();
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
    out
}

/// The version numbers to delete so at most [`VERSION_RETAIN`] remain for an id.
pub fn versions_to_prune(mut versions: Vec<u32>) -> Vec<u32> {
    if versions.len() <= VERSION_RETAIN {
        return Vec::new();
    }
    versions.sort_unstable_by(|a, b| b.cmp(a)); // newest first
    versions.split_off(VERSION_RETAIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, version: u32, secs: i64) -> TemplateRecord {
        TemplateRecord {
            kind: crate::hub::TemplateKind::Pipeline,
            id: id.into(),
            version,
            name: None,
            description: None,
            body: "version: 1".into(),
            format: ConfigFormat::Yaml,
            params: ParamsSpec::new(),
            created_at: DateTime::from_timestamp(secs, 0).unwrap(),
            created_by: None,
        }
    }

    #[test]
    fn latest_per_id_keeps_highest_version_newest_first() {
        let out = latest_per_id(vec![
            rec("a", 1, 10),
            rec("a", 3, 30),
            rec("a", 2, 20),
            rec("b", 1, 40),
        ]);
        assert_eq!(out.len(), 2);
        // `b` was created most recently, so it leads.
        assert_eq!(out[0].id, "b");
        assert_eq!(out[1].id, "a");
        assert_eq!(out[1].version, 3);
    }

    #[test]
    fn latest_per_id_handles_empty() {
        assert!(latest_per_id(Vec::new()).is_empty());
    }

    #[test]
    fn summary_drops_the_body() {
        let s = rec("a", 1, 1).summary();
        let v = serde_json::to_value(&s).unwrap();
        assert!(v.get("body").is_none());
        assert_eq!(v["version"], 1);
    }

    #[test]
    fn prunes_only_beyond_the_retain_window() {
        assert!(versions_to_prune((1..=VERSION_RETAIN as u32).collect()).is_empty());
        let prune = versions_to_prune((1..=(VERSION_RETAIN as u32 + 3)).collect());
        // The three oldest go.
        assert_eq!(prune, vec![3, 2, 1]);
        assert!(versions_to_prune(vec![]).is_empty());
    }

    #[test]
    fn channels_are_a_closed_set_with_forgiving_spellings() {
        // Every spelling of the same channel normalizes identically.
        for raw in ["pre-prod", "pre_prod", "PreProd", "PRE-PROD", "  preprod "] {
            assert_eq!(VersionChannel::parse(raw).unwrap(), VersionChannel::PreProd);
        }
        for (raw, want) in [
            ("stable", VersionChannel::Stable),
            ("previous", VersionChannel::Previous),
            ("newest", VersionChannel::Newest),
            ("prod", VersionChannel::Prod),
            ("staging", VersionChannel::Staging),
            ("canary", VersionChannel::Canary),
            ("test", VersionChannel::Test),
            ("dev", VersionChannel::Dev),
        ] {
            assert_eq!(VersionChannel::parse(raw).unwrap(), want);
            assert_eq!(want.as_str(), raw);
        }
        // The point of the closed set: an invented or mistyped channel is
        // rejected, and the error lists the valid ones.
        for bad in ["", "prd", "production", "my-channel", "v2", "PROD1"] {
            let err = VersionChannel::parse(bad).unwrap_err().to_string();
            assert!(err.contains("fixed:"), "{bad:?}: {err}");
            assert!(err.contains("pre-prod"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn latest_is_rejected_by_name_with_both_alternatives() {
        // `latest` is the most likely thing a newcomer types and both meanings
        // exist under other names, so it gets a bespoke error rather than being
        // silently resolved to one of them.
        for raw in ["latest", "LATEST", " Latest "] {
            let err = VersionChannel::parse(raw).unwrap_err().to_string();
            assert!(err.contains("ambiguous"), "{raw:?}: {err}");
            assert!(err.contains("stable"), "{raw:?}: {err}");
            assert!(err.contains("newest"), "{raw:?}: {err}");
        }
        assert!(VersionSelector::parse("latest").is_err());
    }

    #[test]
    fn derived_channels_are_not_assignable() {
        // `stable` moves via `launch`; `previous` / `newest` are computed. None
        // of the three may be a `promote` target.
        for c in [
            VersionChannel::Stable,
            VersionChannel::Previous,
            VersionChannel::Newest,
        ] {
            assert!(c.is_derived(), "{c} must be derived");
            assert!(!VersionChannel::ASSIGNABLE.contains(&c), "{c}");
        }
        for c in VersionChannel::ASSIGNABLE {
            assert!(!c.is_derived(), "{c} must be assignable");
        }
        assert_eq!(
            VersionChannel::ALL.len(),
            VersionChannel::ASSIGNABLE.len() + 3,
            "ALL is ASSIGNABLE plus the three derived release pointers"
        );
        // The default channel is `stable` — an unpinned request rides the
        // launched version, not the newest build.
        assert_eq!(VersionChannel::default(), VersionChannel::Stable);
        assert_eq!(VersionChannel::default().as_str(), DEFAULT_CHANNEL);
    }

    #[test]
    fn channel_serde_round_trips_by_name() {
        use serde_json::json;
        assert_eq!(
            serde_json::to_value(VersionChannel::PreProd).unwrap(),
            json!("pre-prod")
        );
        assert_eq!(
            serde_json::from_value::<VersionChannel>(json!("pre_prod")).unwrap(),
            VersionChannel::PreProd
        );
        assert!(serde_json::from_value::<VersionChannel>(json!("nope")).is_err());
        assert!(serde_json::from_value::<VersionChannel>(json!("latest")).is_err());
        assert!(serde_json::from_value::<VersionChannel>(json!(2)).is_err());
    }

    #[test]
    fn selector_distinguishes_channels_from_pins() {
        assert_eq!(
            VersionSelector::parse("prod").unwrap(),
            VersionSelector::Channel(VersionChannel::Prod)
        );
        assert_eq!(
            VersionSelector::parse("prod").unwrap().channel(),
            Some(VersionChannel::Prod)
        );
        // Every channel needs a registry lookup — `pinned()` is None for all of
        // them, including the derived ones.
        for c in VersionChannel::ALL {
            assert!(VersionSelector::Channel(*c).pinned().is_none(), "{c}");
        }
        assert!(VersionSelector::parse("stable").unwrap().is_stable());
        assert!(VersionSelector::default().is_stable());
        assert!(!VersionSelector::parse("newest").unwrap().is_stable());
        assert_eq!(VersionSelector::parse("4").unwrap().pinned(), Some(4));
        assert!(VersionSelector::parse("4").unwrap().channel().is_none());
        assert_eq!(
            VersionSelector::Channel(VersionChannel::Dev).to_string(),
            "dev"
        );
        // A channel-shaped typo is rejected, not silently treated as a pin.
        assert!(VersionSelector::parse("prd").is_err());
    }

    #[test]
    fn version_selector_parses_channels_and_numbers() {
        assert_eq!(
            VersionSelector::parse("stable").unwrap(),
            VersionSelector::stable()
        );
        assert_eq!(
            VersionSelector::parse("newest").unwrap(),
            VersionSelector::newest()
        );
        assert_eq!(
            VersionSelector::parse("3").unwrap(),
            VersionSelector::Pinned(3)
        );
        // A version is 1-based; 0, negatives, and junk are all rejected.
        for bad in ["0", "-1", "", "v2", "1.5"] {
            assert!(
                VersionSelector::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn version_selector_maps_to_a_lookup_and_back() {
        assert_eq!(VersionSelector::stable().pinned(), None);
        assert_eq!(VersionSelector::Pinned(7).pinned(), Some(7));
        assert_eq!(VersionSelector::default(), VersionSelector::stable());
        assert_eq!(VersionSelector::stable().to_string(), "stable");
        assert_eq!(VersionSelector::newest().to_string(), "newest");
        assert_eq!(VersionSelector::Pinned(2).to_string(), "2");
    }

    #[test]
    fn version_selector_serde_accepts_every_wire_spelling() {
        use serde_json::json;
        for wire in [json!("stable"), json!("STABLE")] {
            assert_eq!(
                serde_json::from_value::<VersionSelector>(wire).unwrap(),
                VersionSelector::stable()
            );
        }
        // A string (query string / CLI) and a bare number (JSON body) agree.
        assert_eq!(
            serde_json::from_value::<VersionSelector>(json!("4")).unwrap(),
            VersionSelector::Pinned(4)
        );
        assert_eq!(
            serde_json::from_value::<VersionSelector>(json!(4)).unwrap(),
            VersionSelector::Pinned(4)
        );
        assert!(serde_json::from_value::<VersionSelector>(json!(0)).is_err());
        assert!(serde_json::from_value::<VersionSelector>(json!("nope")).is_err());
        assert!(serde_json::from_value::<VersionSelector>(json!(true)).is_err());
        // Round-trips as the channel name / the number.
        assert_eq!(
            serde_json::to_value(VersionSelector::stable()).unwrap(),
            json!("stable")
        );
        assert_eq!(
            serde_json::to_value(VersionSelector::Pinned(9)).unwrap(),
            json!("9")
        );
    }

    #[test]
    fn template_status_is_derived_from_launch_and_deprecation() {
        use TemplateStatus::*;
        // Only two facts determine it, so the status can never disagree with the
        // registry's contents.
        assert_eq!(TemplateStatus::derive(false, false), Draft);
        assert_eq!(TemplateStatus::derive(true, false), Launched);
        assert_eq!(TemplateStatus::derive(false, true), Deprecated);
        // Deprecation wins over having a launched version.
        assert_eq!(TemplateStatus::derive(true, true), Deprecated);
        assert_eq!(Draft.as_str(), "draft");
        assert_eq!(Launched.to_string(), "launched");
        assert_eq!(
            serde_json::to_value(Deprecated).unwrap(),
            serde_json::json!("deprecated")
        );
    }

    #[test]
    fn id_parsing_accepts_slugs_and_rejects_the_rest() {
        for good in ["a", "tenant-sync", "t1_2", "9lives"] {
            assert_eq!(TemplateId::parse(good).unwrap().as_str(), good);
        }
        for bad in [
            "",
            "  ",
            "-lead",
            "_lead",
            "Upper",
            "has space",
            "has/slash",
            "has.dot",
            "../etc/passwd",
        ] {
            assert!(
                TemplateId::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        // Trimmed, and length-capped.
        assert_eq!(TemplateId::parse("  ok  ").unwrap().as_str(), "ok");
        assert!(TemplateId::parse(&"a".repeat(MAX_ID_LEN + 1)).is_err());
        assert!(TemplateId::parse(&"a".repeat(MAX_ID_LEN)).is_ok());
    }

    #[test]
    fn id_derives_from_a_config_name() {
        assert_eq!(
            TemplateId::from_config_name("Tenant Sync (prod)")
                .unwrap()
                .as_str(),
            "tenant-sync-prod"
        );
        assert_eq!(
            TemplateId::from_config_name("already-fine")
                .unwrap()
                .as_str(),
            "already-fine"
        );
        // Nothing usable → a clear error rather than an empty id.
        assert!(TemplateId::from_config_name("!!!").is_err());
        assert!(TemplateId::from_config_name("").is_err());
        // Over-long names are capped without leaving a trailing separator.
        let long = TemplateId::from_config_name(&format!("{} x", "a".repeat(MAX_ID_LEN))).unwrap();
        assert_eq!(long.as_str().len(), MAX_ID_LEN);
    }

    #[test]
    fn id_serde_round_trips_and_rejects_bad_values() {
        let id = TemplateId::parse("ok-id").unwrap();
        assert_eq!(
            serde_json::to_value(&id).unwrap(),
            serde_json::json!("ok-id")
        );
        let back: TemplateId = serde_json::from_value(serde_json::json!("ok-id")).unwrap();
        assert_eq!(back, id);
        assert!(serde_json::from_value::<TemplateId>(serde_json::json!("Bad Id")).is_err());
        assert_eq!(id.to_string(), "ok-id");
        assert_eq!(String::from(id), "ok-id");
    }

    #[test]
    fn record_round_trips_through_json() {
        let mut r = rec("a", 2, 5);
        r.params
            .insert("t".into(), crate::params::ParamSpec::string_default("v"));
        let text = serde_json::to_string(&r).unwrap();
        let back: TemplateRecord = serde_json::from_str(&text).unwrap();
        assert_eq!(back.id, "a");
        assert_eq!(back.version, 2);
        assert_eq!(back.params["t"].default, Some(serde_json::json!("v")));
    }
}
