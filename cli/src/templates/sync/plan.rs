//! Pure sync planning: remote listing × local registry state → a list of
//! actions. No I/O, so every branch is unit-testable and a `--dry-run` is
//! exactly the plan the real run would execute.
//!
//! The invariants the plan preserves (RFC 0006):
//!
//! - **Pull only appends.** A changed body becomes a *new version*; nothing is
//!   ever overwritten or deleted. Idempotence comes from hashing the
//!   canonicalized body: an unchanged file plans `Unchanged`, so re-pulling is
//!   free and never inflates the version counter.
//! - **`launch: ignore` moves nobody.** Only `follow` (the sidecar asked) or
//!   `always` may move `stable`.
//! - **Orphans are reported or deprecated, never deleted.** A delete would
//!   cascade to the launch log and silently repoint `stable`.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::fetch::RemoteTemplate;
use super::spec::{LaunchPolicy, Origin, PrunePolicy, Sidecar};
use crate::error::{CliError, CliResult};
use crate::serve::history::templates::{TemplateId, TemplateStatus, VersionChannel};
use crate::serve::load::ConfigFormat;

/// What the planner needs to know about a locally registered template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalTemplate {
    pub id: String,
    pub status: TemplateStatus,
    /// Highest registered version.
    pub newest: Option<u32>,
    /// [`body_hash`] of the newest version's body.
    pub newest_hash: Option<String>,
    /// The launched version, if any.
    pub stable: Option<u32>,
}

/// One planned step. Serializes with an `action` tag for `--json` / the HTTP
/// dry-run body; the body itself is omitted from the wire form.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SyncAction {
    /// Append a new version (the template is new, or its body changed).
    Register {
        id: String,
        #[serde(skip)]
        body: String,
        #[serde(skip)]
        format: ConfigFormat,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        /// Launch the new version immediately (per the origin's launch policy).
        launch: bool,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tags: Vec<VersionChannel>,
        /// The version this one supersedes, when the template already existed.
        #[serde(skip_serializing_if = "Option::is_none")]
        replaces: Option<u32>,
    },
    /// Launch an already-registered version that the policy says should be
    /// `stable` but is not (policy changed, or a sidecar flipped `launch`).
    Launch { id: String, version: u32 },
    /// Lift a deprecation this origin placed: the template is back upstream.
    Revive { id: String },
    /// Body identical to the newest registered version — nothing to do.
    Unchanged { id: String, version: u32 },
    /// Registered under this origin's prefix but gone upstream; `prune: keep`.
    Orphaned { id: String },
    /// Gone upstream; `prune: deprecate` marks it retired (never deleted).
    Deprecate { id: String },
    /// A remote file the plan could not act on (bad id, unparseable body, bad
    /// sidecar tag). Reported, never fatal — one broken file must not block
    /// the rest of the origin.
    Skipped { name: String, reason: String },
}

impl SyncAction {
    /// Whether applying this action changes the registry.
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Register { .. }
                | Self::Launch { .. }
                | Self::Revive { .. }
                | Self::Deprecate { .. }
        )
    }
}

/// The full plan for one origin.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SyncPlan {
    pub origin: String,
    pub actions: Vec<SyncAction>,
}

impl SyncPlan {
    /// Number of actions that would change the registry.
    pub fn mutations(&self) -> usize {
        self.actions.iter().filter(|a| a.is_mutation()).count()
    }
}

/// Content hash of a template body after canonicalization (comments stripped,
/// keys re-emitted in canonical YAML), so a formatting-only edit upstream does
/// not register a new version. JSON bodies canonicalize the same way.
pub fn body_hash(body: &str) -> CliResult<String> {
    let value: serde_yaml::Value = serde_yaml::from_str(body)
        .map_err(|e| CliError::Config(format!("body is not valid YAML/JSON: {e}")))?;
    let canonical = serde_yaml::to_string(&value)
        .map_err(|e| CliError::Config(format!("body could not be canonicalized: {e}")))?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(format!("{digest:x}"))
}

/// Decide whether a pulled version should be launched under `policy`.
fn should_launch(policy: LaunchPolicy, sidecar: &Sidecar) -> bool {
    match policy {
        LaunchPolicy::Ignore => false,
        LaunchPolicy::Follow => sidecar.launch,
        LaunchPolicy::Always => true,
    }
}

/// Build the plan for `origin`. `local` may hold templates outside the
/// origin's namespace; they are ignored (the origin only ever touches ids
/// carrying its prefix).
pub fn plan(origin: &Origin, remote: &[RemoteTemplate], local: &[LocalTemplate]) -> SyncPlan {
    let local_by_id: BTreeMap<&str, &LocalTemplate> = local
        .iter()
        .filter(|l| l.id.starts_with(&origin.prefix))
        .map(|l| (l.id.as_str(), l))
        .collect();

    let mut remote: Vec<&RemoteTemplate> = remote.iter().collect();
    remote.sort_by(|a, b| a.stem.cmp(&b.stem));

    let mut actions = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for r in remote {
        if let Some(why) = &r.retired {
            actions.push(SyncAction::Skipped {
                name: r.stem.clone(),
                reason: why.clone(),
            });
            continue;
        }
        let id = format!("{}{}", origin.prefix, r.stem);
        if let Err(e) = TemplateId::parse(&id) {
            actions.push(SyncAction::Skipped {
                name: r.stem.clone(),
                reason: e.to_string(),
            });
            continue;
        }
        if !seen.insert(id.clone()) {
            actions.push(SyncAction::Skipped {
                name: r.stem.clone(),
                reason: format!("duplicate template id '{id}'"),
            });
            continue;
        }
        let hash = match body_hash(&r.body) {
            Ok(h) => h,
            Err(e) => {
                actions.push(SyncAction::Skipped {
                    name: r.stem.clone(),
                    reason: e.to_string(),
                });
                continue;
            }
        };
        let sidecar = r.sidecar.clone().unwrap_or_default();
        let tags: Vec<VersionChannel> = match sidecar
            .tags
            .iter()
            .map(|t| VersionChannel::parse(t))
            .collect::<CliResult<Vec<_>>>()
        {
            Ok(t) => t,
            Err(e) => {
                actions.push(SyncAction::Skipped {
                    name: r.stem.clone(),
                    reason: format!("sidecar tags: {e}"),
                });
                continue;
            }
        };
        let launch = should_launch(origin.launch, &sidecar);

        match local_by_id.get(id.as_str()) {
            Some(l) if l.newest.is_some() && l.newest_hash.as_deref() == Some(hash.as_str()) => {
                let newest = l.newest.expect("checked");
                let mut acted = false;
                if l.status == TemplateStatus::Deprecated && origin.prune == PrunePolicy::Deprecate
                {
                    actions.push(SyncAction::Revive { id: id.clone() });
                    acted = true;
                }
                if launch && l.stable != Some(newest) {
                    actions.push(SyncAction::Launch {
                        id: id.clone(),
                        version: newest,
                    });
                    acted = true;
                }
                if !acted {
                    actions.push(SyncAction::Unchanged {
                        id,
                        version: newest,
                    });
                }
            }
            other => actions.push(SyncAction::Register {
                id,
                body: r.body.clone(),
                format: r.format,
                description: sidecar.description.clone(),
                launch,
                tags,
                replaces: other.and_then(|l| l.newest),
            }),
        }
    }

    for (id, l) in local_by_id {
        if seen.contains(id) {
            continue;
        }
        match origin.prune {
            PrunePolicy::Keep => actions.push(SyncAction::Orphaned { id: id.to_string() }),
            PrunePolicy::Deprecate if l.status == TemplateStatus::Deprecated => {
                actions.push(SyncAction::Orphaned { id: id.to_string() })
            }
            PrunePolicy::Deprecate => actions.push(SyncAction::Deprecate { id: id.to_string() }),
        }
    }

    SyncPlan {
        origin: origin.name.clone(),
        actions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::templates::sync::spec::{GithubSource, OriginSource};

    const BODY_A: &str = "version: 1\nname: a\npipeline:\n  source: {type: rest, config: {base_url: \"https://x\", path: /e}}\n  sink: {type: stdout, config: {}}\n";

    fn origin(prefix: &str, launch: LaunchPolicy, prune: PrunePolicy) -> Origin {
        Origin {
            name: "o".into(),
            source: OriginSource::Github(GithubSource {
                repo: "acme/t".into(),
                r#ref: "main".into(),
                path: String::new(),
                paths: Vec::new(),
                token: None,
                api_base: "https://api.github.com".into(),
            }),
            prefix: prefix.into(),
            launch,
            prune,
            interval_secs: None,
        }
    }

    fn remote(stem: &str, body: &str, sidecar: Option<Sidecar>) -> RemoteTemplate {
        RemoteTemplate {
            stem: stem.into(),
            body: body.into(),
            format: ConfigFormat::Yaml,
            sidecar,
            retired: None,
        }
    }

    fn local(
        id: &str,
        status: TemplateStatus,
        newest: u32,
        body: &str,
        stable: Option<u32>,
    ) -> LocalTemplate {
        LocalTemplate {
            id: id.into(),
            status,
            newest: Some(newest),
            newest_hash: Some(body_hash(body).unwrap()),
            stable,
        }
    }

    fn ids(plan: &SyncPlan) -> Vec<String> {
        plan.actions
            .iter()
            .map(|a| match a {
                SyncAction::Register { id, .. }
                | SyncAction::Launch { id, .. }
                | SyncAction::Revive { id }
                | SyncAction::Unchanged { id, .. }
                | SyncAction::Orphaned { id }
                | SyncAction::Deprecate { id } => id.clone(),
                SyncAction::Skipped { name, .. } => format!("skipped:{name}"),
            })
            .collect()
    }

    /// Comments and key order do not change the hash; content does.
    #[test]
    fn body_hash_is_canonical() {
        let a = body_hash("version: 1\nname: a\n").unwrap();
        let b = body_hash("# hello\nversion:   1\nname: a   # trailing\n\n").unwrap();
        let c = body_hash("version: 1\nname: b\n").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        // JSON canonicalizes to the same YAML.
        assert_eq!(a, body_hash(r#"{"version":1,"name":"a"}"#).unwrap());
        assert!(body_hash(": : :").is_err());
    }

    #[test]
    fn new_template_registers_with_prefix_and_policy() {
        let o = origin("plat-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let p = plan(&o, &[remote("sync", BODY_A, None)], &[]);
        assert_eq!(p.actions.len(), 1);
        match &p.actions[0] {
            SyncAction::Register {
                id,
                launch,
                replaces,
                tags,
                ..
            } => {
                assert_eq!(id, "plat-sync");
                assert!(!launch, "launch: ignore never launches");
                assert_eq!(*replaces, None);
                assert!(tags.is_empty());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(p.mutations(), 1);
    }

    #[test]
    fn unchanged_body_is_a_noop_and_changed_body_appends() {
        let o = origin("", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let l = [local("sync", TemplateStatus::Launched, 3, BODY_A, Some(3))];
        let p = plan(&o, &[remote("sync", ": : :", None)], &l);
        assert!(matches!(&p.actions[0], SyncAction::Skipped { .. }), "{p:?}");

        let p = plan(&o, &[remote("sync", BODY_A, None)], &l);
        assert_eq!(
            p.actions,
            vec![SyncAction::Unchanged {
                id: "sync".into(),
                version: 3
            }]
        );
        assert_eq!(p.mutations(), 0);

        let changed = BODY_A.replace("path: /e", "path: /e2");
        let p = plan(&o, &[remote("sync", &changed, None)], &l);
        match &p.actions[0] {
            SyncAction::Register { replaces, .. } => assert_eq!(*replaces, Some(3)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn launch_policy_follow_reads_the_sidecar_and_always_ignores_it() {
        let side = Sidecar {
            launch: true,
            description: Some("d".into()),
            tags: vec!["dev".into()],
            ..Default::default()
        };
        let follow = origin("", LaunchPolicy::Follow, PrunePolicy::Keep);
        let p = plan(&follow, &[remote("a", BODY_A, Some(side.clone()))], &[]);
        match &p.actions[0] {
            SyncAction::Register {
                launch,
                description,
                tags,
                ..
            } => {
                assert!(launch);
                assert_eq!(description.as_deref(), Some("d"));
                assert_eq!(tags, &[VersionChannel::parse("dev").unwrap()]);
            }
            other => panic!("{other:?}"),
        }
        // No sidecar under `follow` → not launched.
        let p = plan(&follow, &[remote("a", BODY_A, None)], &[]);
        assert!(matches!(
            &p.actions[0],
            SyncAction::Register { launch: false, .. }
        ));
        // `always` launches with or without a sidecar.
        let always = origin("", LaunchPolicy::Always, PrunePolicy::Keep);
        let p = plan(&always, &[remote("a", BODY_A, None)], &[]);
        assert!(matches!(
            &p.actions[0],
            SyncAction::Register { launch: true, .. }
        ));
    }

    /// Policy flipped to `always` after the version was pulled: the body is
    /// unchanged, but `stable` lags — plan a launch, not a re-register.
    #[test]
    fn unchanged_but_unlaunched_plans_a_launch_under_launch_policies() {
        let always = origin("", LaunchPolicy::Always, PrunePolicy::Keep);
        let l = [local("a", TemplateStatus::Draft, 2, BODY_A, None)];
        let p = plan(&always, &[remote("a", BODY_A, None)], &l);
        assert_eq!(
            p.actions,
            vec![SyncAction::Launch {
                id: "a".into(),
                version: 2
            }]
        );
        // Already stable → unchanged.
        let l = [local("a", TemplateStatus::Launched, 2, BODY_A, Some(2))];
        let p = plan(&always, &[remote("a", BODY_A, None)], &l);
        assert!(matches!(&p.actions[0], SyncAction::Unchanged { .. }));
        // `ignore` never launches, even when stable lags.
        let ignore = origin("", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let l = [local("a", TemplateStatus::Draft, 2, BODY_A, None)];
        let p = plan(&ignore, &[remote("a", BODY_A, None)], &l);
        assert!(matches!(&p.actions[0], SyncAction::Unchanged { .. }));
    }

    #[test]
    fn orphans_are_reported_under_keep_and_deprecated_under_deprecate() {
        let keep = origin("p-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let l = [
            local("p-gone", TemplateStatus::Launched, 1, BODY_A, Some(1)),
            local("other-ns", TemplateStatus::Launched, 1, BODY_A, Some(1)),
        ];
        let p = plan(&keep, &[], &l);
        assert_eq!(ids(&p), vec!["p-gone"]);
        assert!(matches!(&p.actions[0], SyncAction::Orphaned { .. }));
        assert_eq!(p.mutations(), 0, "keep never mutates");

        let dep = origin("p-", LaunchPolicy::Ignore, PrunePolicy::Deprecate);
        let p = plan(&dep, &[], &l);
        assert_eq!(
            p.actions,
            vec![SyncAction::Deprecate {
                id: "p-gone".into()
            }]
        );

        // Already deprecated → just reported, not re-deprecated.
        let l = [local(
            "p-gone",
            TemplateStatus::Deprecated,
            1,
            BODY_A,
            Some(1),
        )];
        let p = plan(&dep, &[], &l);
        assert_eq!(
            p.actions,
            vec![SyncAction::Orphaned {
                id: "p-gone".into()
            }]
        );
    }

    /// A template this origin deprecated comes back upstream: under `prune:
    /// deprecate` the origin owns that marker and lifts it. Under `keep` a
    /// deprecation was placed by a human, so sync leaves it alone.
    #[test]
    fn a_returning_template_is_revived_only_when_sync_owns_deprecation() {
        let l = [local("a", TemplateStatus::Deprecated, 1, BODY_A, Some(1))];
        let dep = origin("", LaunchPolicy::Ignore, PrunePolicy::Deprecate);
        let p = plan(&dep, &[remote("a", BODY_A, None)], &l);
        assert_eq!(p.actions, vec![SyncAction::Revive { id: "a".into() }]);

        let keep = origin("", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let p = plan(&keep, &[remote("a", BODY_A, None)], &l);
        assert!(matches!(&p.actions[0], SyncAction::Unchanged { .. }));

        // Revive + launch when the policy also wants it live: revive comes
        // first, because `launch` refuses a deprecated template.
        let always = origin("", LaunchPolicy::Always, PrunePolicy::Deprecate);
        let l = [local("a", TemplateStatus::Deprecated, 2, BODY_A, Some(1))];
        let p = plan(&always, &[remote("a", BODY_A, None)], &l);
        assert_eq!(
            p.actions,
            vec![
                SyncAction::Revive { id: "a".into() },
                SyncAction::Launch {
                    id: "a".into(),
                    version: 2
                },
            ]
        );
    }

    #[test]
    fn a_body_the_catalog_retired_is_skipped_with_its_reason() {
        // #691: never register a version the catalog deprecated.
        let o = origin("", LaunchPolicy::Always, PrunePolicy::Keep);
        let mut r = remote("acme/erp", BODY_A, None);
        r.retired = Some("catalog v3 is deprecated: drops invoices".into());
        let p = plan(&o, &[r], &[]);
        assert_eq!(
            p.actions,
            vec![SyncAction::Skipped {
                name: "acme/erp".into(),
                reason: "catalog v3 is deprecated: drops invoices".into(),
            }]
        );
        assert_eq!(p.mutations(), 0);
    }

    #[test]
    fn bad_ids_tags_and_duplicates_are_skipped_not_fatal() {
        let o = origin("", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let bad_tag = Sidecar {
            tags: vec!["latest".into()],
            ..Default::default()
        };
        let p = plan(
            &o,
            &[
                remote("Has Space", BODY_A, None),
                remote("ok", BODY_A, Some(bad_tag)),
                remote("fine", BODY_A, None),
                remote("fine", BODY_A, None),
            ],
            &[],
        );
        let skipped: Vec<&str> = p
            .actions
            .iter()
            .filter_map(|a| match a {
                SyncAction::Skipped { name, reason } => {
                    assert!(!reason.is_empty());
                    Some(name.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(skipped, vec!["Has Space", "fine", "ok"]);
        assert_eq!(p.mutations(), 1, "the first `fine` still registers");
    }

    #[test]
    fn actions_serialize_with_a_tag_and_without_the_body() {
        let a = SyncAction::Register {
            id: "x".into(),
            body: "secret-ish body".into(),
            format: ConfigFormat::Yaml,
            description: None,
            launch: true,
            tags: vec![],
            replaces: Some(1),
        };
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(v["action"], "register");
        assert_eq!(v["replaces"], 1);
        assert!(v.get("body").is_none());
        assert!(v.get("description").is_none());
    }
}
