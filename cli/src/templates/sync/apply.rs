//! Execute a [`SyncPlan`] against the registry through the ordinary
//! `register` / `launch` / `set_deprecated` verbs, so every pulled template
//! passes the same structural validation a `faucet template register` would
//! and lands in the same append-only launch log.
//!
//! Per-template failures are collected, not propagated: one template that
//! fails validation must not stop the rest of the origin from syncing.

use serde::Serialize;

use super::plan::{SyncAction, SyncPlan};
use crate::error::CliResult;
use crate::serve::history::templates::VersionSelector;
use crate::templates::{RegisterRequest, TemplateStore};

/// A version appended by the sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Registered {
    pub id: String,
    pub version: u32,
    pub launched: bool,
}

/// A per-template failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Failure {
    pub id: String,
    pub error: String,
}

/// What an applied plan did.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ApplyOutcome {
    pub registered: Vec<Registered>,
    /// Launches planned on their own (not the launch folded into a register).
    pub launched: Vec<Registered>,
    pub revived: Vec<String>,
    pub deprecated: Vec<String>,
    pub unchanged: usize,
    pub orphaned: Vec<String>,
    pub skipped: Vec<(String, String)>,
    pub failed: Vec<Failure>,
}

impl ApplyOutcome {
    /// Registry mutations performed.
    pub fn mutations(&self) -> usize {
        self.registered.len() + self.launched.len() + self.revived.len() + self.deprecated.len()
    }
}

async fn run_action(
    store: &TemplateStore,
    origin: &str,
    actor: &str,
    action: SyncAction,
    out: &mut ApplyOutcome,
) -> CliResult<()> {
    match action {
        SyncAction::Register {
            id,
            body,
            format,
            description,
            launch,
            tags,
            ..
        } => {
            let rec = crate::templates::register(
                store,
                RegisterRequest {
                    id: Some(id.clone()),
                    body,
                    format,
                    description,
                    tags,
                    launch,
                    created_by: Some(actor.to_string()),
                },
            )
            .await?;
            out.registered.push(Registered {
                id,
                version: rec.version,
                launched: launch,
            });
        }
        SyncAction::Launch { id, version } => {
            crate::templates::launch(store, &id, VersionSelector::Pinned(version), Some(actor))
                .await?;
            out.launched.push(Registered {
                id,
                version,
                launched: true,
            });
        }
        SyncAction::Revive { id } => {
            crate::templates::set_deprecated(store, &id, None, Some(actor), false).await?;
            out.revived.push(id);
        }
        SyncAction::Deprecate { id } => {
            crate::templates::set_deprecated(
                store,
                &id,
                Some(format!("removed from origin '{origin}'")),
                Some(actor),
                true,
            )
            .await?;
            out.deprecated.push(id);
        }
        SyncAction::Unchanged { .. } => out.unchanged += 1,
        SyncAction::Orphaned { id } => out.orphaned.push(id),
        SyncAction::Skipped { name, reason } => out.skipped.push((name, reason)),
    }
    Ok(())
}

fn action_id(a: &SyncAction) -> String {
    match a {
        SyncAction::Register { id, .. }
        | SyncAction::Launch { id, .. }
        | SyncAction::Revive { id }
        | SyncAction::Unchanged { id, .. }
        | SyncAction::Orphaned { id }
        | SyncAction::Deprecate { id } => id.clone(),
        SyncAction::Skipped { name, .. } => name.clone(),
    }
}

/// Apply every action in order. `actor` is recorded as `created_by` /
/// `launched_by` / `deprecated_by` (`sync:<origin>` from a pull; the
/// principal's name when a person triggers the sync over HTTP).
pub async fn apply(store: &TemplateStore, plan: SyncPlan, actor: &str) -> ApplyOutcome {
    let mut out = ApplyOutcome::default();
    for action in plan.actions {
        let id = action_id(&action);
        if let Err(e) = run_action(store, &plan.origin, actor, action, &mut out).await {
            tracing::warn!(origin = %plan.origin, template = %id, error = %e, "template sync action failed");
            out.failed.push(Failure {
                id,
                error: e.to_string(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::templates::{TemplateStatus, VersionChannel};
    use crate::serve::load::ConfigFormat;
    use std::sync::Arc;
    use std::time::Duration;

    const BODY: &str = "version: 1\nname: t\npipeline:\n  source: {type: rest, config: {base_url: \"https://x\", path: /e}}\n  sink: {type: stdout, config: {}}\n";

    fn store() -> TemplateStore {
        Arc::new(MemoryHistory::new(Duration::from_secs(60)))
    }

    fn register(id: &str, launch: bool, tags: Vec<VersionChannel>) -> SyncAction {
        SyncAction::Register {
            id: id.into(),
            body: BODY.into(),
            format: ConfigFormat::Yaml,
            description: Some("from sync".into()),
            launch,
            tags,
            replaces: None,
        }
    }

    #[tokio::test]
    async fn register_launch_deprecate_revive_round_trip() {
        let s = store();
        let plan = SyncPlan {
            origin: "o".into(),
            actions: vec![
                register("a", false, vec![VersionChannel::parse("dev").unwrap()]),
                register("b", true, vec![]),
                SyncAction::Unchanged {
                    id: "z".into(),
                    version: 1,
                },
                SyncAction::Orphaned {
                    id: "orphan".into(),
                },
                SyncAction::Skipped {
                    name: "junk".into(),
                    reason: "bad".into(),
                },
            ],
        };
        let out = apply(&s, plan, "sync:o").await;
        assert!(out.failed.is_empty(), "{:?}", out.failed);
        assert_eq!(out.registered.len(), 2);
        assert_eq!(out.unchanged, 1);
        assert_eq!(out.orphaned, vec!["orphan"]);
        assert_eq!(out.skipped, vec![("junk".to_string(), "bad".to_string())]);
        assert_eq!(out.mutations(), 2);

        let a = crate::templates::template_state(&s, "a").await.unwrap();
        assert_eq!(
            a.status,
            TemplateStatus::Draft,
            "launch: false stays a draft"
        );
        assert_eq!(a.tags.get("dev"), Some(&1));
        let b = crate::templates::template_state(&s, "b").await.unwrap();
        assert_eq!(b.status, TemplateStatus::Launched);
        assert_eq!(b.stable, Some(1));
        let rec = s.template_get("a", Some(1)).await.unwrap().unwrap();
        assert_eq!(rec.created_by.as_deref(), Some("sync:o"));
        assert_eq!(rec.description.as_deref(), Some("from sync"));

        // Launch a draft, deprecate the other, then revive it.
        let plan = SyncPlan {
            origin: "o".into(),
            actions: vec![
                SyncAction::Launch {
                    id: "a".into(),
                    version: 1,
                },
                SyncAction::Deprecate { id: "b".into() },
            ],
        };
        let out = apply(&s, plan, "sync:o").await;
        assert!(out.failed.is_empty(), "{:?}", out.failed);
        assert_eq!(out.launched[0].version, 1);
        assert_eq!(out.deprecated, vec!["b"]);
        let a = crate::templates::template_state(&s, "a").await.unwrap();
        assert_eq!(a.stable, Some(1));
        let b = crate::templates::template_state(&s, "b").await.unwrap();
        assert_eq!(b.status, TemplateStatus::Deprecated);

        let out = apply(
            &s,
            SyncPlan {
                origin: "o".into(),
                actions: vec![SyncAction::Revive { id: "b".into() }],
            },
            "sync:o",
        )
        .await;
        assert_eq!(out.revived, vec!["b"]);
        let b = crate::templates::template_state(&s, "b").await.unwrap();
        assert_eq!(b.status, TemplateStatus::Launched);
    }

    /// One broken template must not stop the others: the failure is recorded
    /// per id and the rest of the plan still applies.
    #[tokio::test]
    async fn failures_are_collected_per_template() {
        let s = store();
        let plan = SyncPlan {
            origin: "o".into(),
            actions: vec![
                SyncAction::Register {
                    id: "bad".into(),
                    body: "version: 1\npipeline: {source: {type: nope, config: {}}, sink: {type: stdout, config: {}}}\n".into(),
                    format: ConfigFormat::Yaml,
                    description: None,
                    launch: false,
                    tags: vec![],
                    replaces: None,
                },
                SyncAction::Launch {
                    id: "missing".into(),
                    version: 7,
                },
                SyncAction::Deprecate { id: "missing".into() },
                register("good", false, vec![]),
            ],
        };
        let out = apply(&s, plan, "sync:o").await;
        assert_eq!(out.registered.len(), 1);
        assert_eq!(out.registered[0].id, "good");
        let failed: Vec<&str> = out.failed.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(failed, vec!["bad", "missing", "missing"]);
        assert!(out.failed.iter().all(|f| !f.error.is_empty()));
    }
}
