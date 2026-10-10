//! Revalidation (#856): re-run the tests of every version that holds a label
//! or is a rollback target, under the running faucet release.
//!
//! A faucet upgrade makes recorded evidence stale. Labels stay where they are
//! (they authorize the runs through them), but the next move onto one of
//! those versions needs fresh evidence — and a rollback target that no longer
//! passes on this binary is better found now than during an incident.
//! Revalidation records results only; it never moves a label.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::error::CliResult;
use crate::hub::TemplateKind;
use crate::templates::TemplateStore;
use crate::templates::store::registry_err;

/// The history-database setting holding the faucet release line templates
/// were last revalidated under.
pub const META_KEY: &str = "template_tests.revalidated_line";

/// One re-tested version.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Revalidated {
    pub id: String,
    pub version: u32,
    /// What it is: `stable`, `previous`, or a channel name.
    pub labels: Vec<String>,
    /// `pass`, `fail`, `no_tests` (nothing to run) or `error` (the run could
    /// not be recorded).
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Everything one revalidation did.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RevalidateReport {
    /// The faucet release line it ran under.
    pub faucet: String,
    pub versions: Vec<Revalidated>,
}

impl RevalidateReport {
    /// Versions whose tests no longer pass, or could not run.
    pub fn failed(&self) -> usize {
        self.versions
            .iter()
            .filter(|v| matches!(v.outcome, "fail" | "error"))
            .count()
    }

    pub fn render_human(&self) -> String {
        let mut out = format!(
            "revalidated {} version(s) under faucet {}\n",
            self.versions.len(),
            self.faucet
        );
        for v in &self.versions {
            out.push_str(&format!(
                "  {:<8} {}@{} ({}){}\n",
                v.outcome,
                v.id,
                v.version,
                v.labels.join(", "),
                v.detail
                    .as_deref()
                    .map(|d| format!(" — {d}"))
                    .unwrap_or_default()
            ));
        }
        out
    }
}

/// The versions of `id` to re-test, with what each one is.
pub async fn targets(store: &TemplateStore, id: &str) -> CliResult<BTreeMap<u32, Vec<String>>> {
    let state = store
        .template_state(id)
        .await
        .map_err(|e| registry_err("template registry read", e))?;
    let mut out: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    if let Some(v) = state.stable {
        out.entry(v).or_default().push("stable".into());
    }
    if let Some(v) = state.previous {
        out.entry(v).or_default().push("previous".into());
    }
    for (tag, v) in &state.tags {
        out.entry(*v).or_default().push(tag.clone());
    }
    Ok(out)
}

/// Re-test every label holder and rollback target in the registry.
pub async fn revalidate(
    store: &TemplateStore,
    requested_by: Option<&str>,
) -> CliResult<RevalidateReport> {
    let mut versions = Vec::new();
    let templates = store
        .template_list()
        .await
        .map_err(|e| registry_err("template registry read", e))?;
    for t in templates {
        if t.kind == TemplateKind::TestSuite {
            continue;
        }
        for (version, labels) in targets(store, &t.id).await? {
            let has_tests = match store.template_get(&t.id, Some(version)).await {
                Ok(Some(rec)) => crate::templates::bundle::bundle_in(&rec)
                    .map(|b| b.is_some())
                    .unwrap_or(true),
                _ => true,
            };
            let (outcome, detail) = if !has_tests {
                ("no_tests", None)
            } else {
                match crate::templates::bundle::test_version(store, &t.id, version, requested_by)
                    .await
                {
                    Ok(r) if r.passed => ("pass", None),
                    Ok(r) => ("fail", Some(r.outcome().failing().join("; "))),
                    Err(e) => ("error", Some(e.to_string())),
                }
            };
            if outcome != "no_tests" {
                crate::templates::metrics::record_revalidation(outcome);
            }
            versions.push(Revalidated {
                id: t.id.clone(),
                version,
                labels,
                outcome,
                detail,
            });
        }
    }
    Ok(RevalidateReport {
        faucet: crate::template_tests::result::running_line(),
        versions,
    })
}

/// [`revalidate`] when the registry was last revalidated under another faucet
/// release line (or never), then record this one. `None` when nothing was due.
pub async fn revalidate_if_new_release(
    store: &TemplateStore,
) -> CliResult<Option<RevalidateReport>> {
    let line = crate::template_tests::result::running_line();
    let marker = store
        .meta_get(META_KEY)
        .await
        .map_err(|e| registry_err("server setting read", e))?;
    if marker.as_deref() == Some(line.as_str()) {
        return Ok(None);
    }
    let report = revalidate(store, Some("revalidate")).await?;
    store
        .meta_put(META_KEY, &line)
        .await
        .map_err(|e| registry_err("server setting write", e))?;
    Ok(Some(report))
}
