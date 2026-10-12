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

/// The setting holding an in-progress revalidation's claim:
/// `<line>@<unix expiry>`. The completed line is written to [`META_KEY`] only
/// once a run has finished.
pub const CLAIM_KEY: &str = "template_tests.revalidation_claim";

/// How long a revalidation claim holds. An instance that dies mid-run leaves
/// its claim behind; once it expires the next start revalidates again.
pub const CLAIM_TTL_SECS: i64 = 2 * 60 * 60;

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

/// The versions of `id` to re-test, with what each one is: every label
/// holder (`stable`, `newest` once launched, each channel) and the rollback
/// target.
pub async fn targets(store: &TemplateStore, id: &str) -> CliResult<BTreeMap<u32, Vec<String>>> {
    let state = crate::templates::template_state(store, id).await?;
    let mut out: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    if let Some(v) = state.stable {
        out.entry(v).or_default().push("stable".into());
        if let Some(n) = state.newest {
            out.entry(n).or_default().push("newest".into());
        }
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

/// [`revalidate`] when the registry was last revalidated under an older faucet
/// release line (or never). `None` when nothing was due.
///
/// A run is claimed first with a compare-and-set on [`CLAIM_KEY`] that holds
/// for [`CLAIM_TTL_SECS`], so of several instances starting on the same new
/// line only one revalidates; the completed line is written to [`META_KEY`]
/// only after the run, and an instance on an older line never claims a newer
/// one back. A run in which any version could not be tested (`error`) records
/// no line, and an instance that dies mid-run leaves a claim that expires, so
/// either way a later start tries again.
pub async fn revalidate_if_new_release(
    store: &TemplateStore,
) -> CliResult<Option<RevalidateReport>> {
    let line = crate::template_tests::result::running_line();
    let marker = store
        .meta_get(META_KEY)
        .await
        .map_err(|e| registry_err("server setting read", e))?;
    if !is_newer(&line, marker.as_deref()) {
        return Ok(None);
    }
    let claim = store
        .meta_get(CLAIM_KEY)
        .await
        .map_err(|e| registry_err("server setting read", e))?;
    let now = chrono::Utc::now().timestamp();
    if claim_blocks(&line, claim.as_deref(), now) {
        return Ok(None);
    }
    let mine = format!("{line}@{}", now + CLAIM_TTL_SECS);
    let claimed = store
        .meta_compare_and_set(CLAIM_KEY, claim.as_deref(), &mine)
        .await
        .map_err(|e| registry_err("server setting write", e))?;
    if !claimed {
        return Ok(None);
    }
    let report = revalidate(store, Some("revalidate")).await;
    let errored = match &report {
        Ok(r) => r.versions.iter().any(|v| v.outcome == "error"),
        Err(_) => true,
    };
    if !errored
        && let Err(e) = store
            .meta_compare_and_set(META_KEY, marker.as_deref(), &line)
            .await
    {
        tracing::warn!(error = %e, "could not record the revalidated release line");
    }
    if let Err(e) = store.meta_compare_and_set(CLAIM_KEY, Some(&mine), "").await {
        tracing::warn!(error = %e, "could not release the revalidation claim");
    }
    report.map(Some)
}

/// Whether a stored claim stops a run of `line` at `now`: a live claim for
/// the same or a newer line. An expired, empty or unreadable claim does not.
fn claim_blocks(line: &str, claim: Option<&str>, now: i64) -> bool {
    let Some((claimed_line, expiry)) = claim.and_then(|c| c.rsplit_once('@')) else {
        return false;
    };
    let Ok(expiry) = expiry.parse::<i64>() else {
        return false;
    };
    expiry > now && !is_newer(line, Some(claimed_line))
}

/// After a revalidation run outside the server's start-up claim: record its
/// release line when every version could be tested and the line is newer
/// than the one recorded.
pub async fn record_line(store: &TemplateStore, report: &RevalidateReport) -> CliResult<()> {
    if report.versions.iter().any(|v| v.outcome == "error") {
        return Ok(());
    }
    let marker = store
        .meta_get(META_KEY)
        .await
        .map_err(|e| registry_err("server setting read", e))?;
    if is_newer(&report.faucet, marker.as_deref()) {
        store
            .meta_compare_and_set(META_KEY, marker.as_deref(), &report.faucet)
            .await
            .map_err(|e| registry_err("server setting write", e))?;
    }
    Ok(())
}

/// Whether release line `line` (`major.minor`) is newer than `marker`. An
/// empty or unreadable marker is older than anything.
pub fn is_newer(line: &str, marker: Option<&str>) -> bool {
    let parse = |s: &str| -> Option<(u64, u64)> {
        let (a, b) = s.split_once('.')?;
        Some((a.parse().ok()?, b.parse().ok()?))
    };
    match (parse(line), marker.and_then(parse)) {
        (Some(l), Some(m)) => l > m,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_claim_for_this_or_a_newer_line_blocks_and_anything_else_does_not() {
        assert!(!claim_blocks("1.14", None, 100));
        assert!(!claim_blocks("1.14", Some(""), 100));
        assert!(!claim_blocks("1.14", Some("garbage"), 100));
        assert!(!claim_blocks("1.14", Some("1.14@soon"), 100));
        assert!(claim_blocks("1.14", Some("1.14@200"), 100));
        assert!(claim_blocks("1.14", Some("1.15@200"), 100));
        assert!(!claim_blocks("1.14", Some("1.14@100"), 100), "expired");
        assert!(!claim_blocks("1.15", Some("1.14@200"), 100), "older line");
    }
}
