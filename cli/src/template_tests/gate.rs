//! The launch gate (#856): whether a template version may be launched, given
//! its recorded bundle results. Pure — the registry reads the facts, this
//! decides.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::result::{TemplateTestResult, faucet_version, major_of};
use crate::error::{CliError, CliResult};

/// What a launch is checked against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchGate {
    /// Refuse a version without a passing result recorded under this binary's
    /// major version.
    #[serde(default)]
    pub require_tests: bool,
    /// Admin override: launch anyway, recording this reason on the launch log
    /// and in the audit log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_tests_reason: Option<String>,
}

impl LaunchGate {
    /// The gate a server or CLI applies, with no override.
    pub fn new(require_tests: bool) -> Self {
        Self {
            require_tests,
            skip_tests_reason: None,
        }
    }

    /// This gate with an override reason. A blank reason is refused: an
    /// override exists to leave a record of why.
    pub fn with_skip(mut self, reason: Option<String>) -> CliResult<Self> {
        if let Some(r) = reason {
            let r = r.trim().to_string();
            if r.is_empty() {
                return Err(CliError::Config(
                    "skipping the test gate needs a reason (`--skip-tests-reason \"<why>\"`)"
                        .into(),
                ));
            }
            self.skip_tests_reason = Some(r);
        }
        Ok(self)
    }
}

/// Where a version stands against the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    /// A passing result for this body under this major.
    Passed,
    /// The latest valid result failed.
    Failed,
    /// Results exist, but none for this body under this faucet major.
    Stale,
    /// The version has a bundle that has never run.
    NotRun,
    /// The version carries no `tests:` block.
    NoTests,
}

/// The gate's decision for one version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateVerdict {
    pub status: GateStatus,
    /// Whether a launch would go ahead.
    pub allowed: bool,
    /// Whether the server requires tests.
    pub require_tests: bool,
    /// Why a launch is (or would be, with `require_tests`) refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `case: failure` lines from the deciding result.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failing: Vec<String>,
    /// When the deciding result was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_at: Option<DateTime<Utc>>,
    /// Set when an admin override let a refused launch through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
}

/// Decide for `version` (body hash `body_sha256`) given its results, newest
/// first.
pub fn evaluate(
    gate: &LaunchGate,
    id: &str,
    version: u32,
    has_tests: bool,
    body_sha256: &str,
    results: &[TemplateTestResult],
) -> GateVerdict {
    let major = major_of(faucet_version());
    let deciding = results.iter().find(|r| r.counts_for(body_sha256, major));
    let (status, reason, failing, result_at) = if !has_tests {
        (
            GateStatus::NoTests,
            Some(format!(
                "v{version} has no `tests:` block, and this server requires a passing test \
                 bundle before a launch — register a version with tests"
            )),
            Vec::new(),
            None,
        )
    } else {
        match deciding {
            Some(r) if r.passed => (GateStatus::Passed, None, Vec::new(), Some(r.recorded_at)),
            Some(r) => (
                GateStatus::Failed,
                Some(format!(
                    "the latest test run of v{version} failed — fix the template and register a \
                     new version, or rerun with `faucet template test {id}@{version}`"
                )),
                r.outcome().failing(),
                Some(r.recorded_at),
            ),
            None if results.is_empty() => (
                GateStatus::NotRun,
                Some(format!(
                    "v{version}'s tests have never run — run `faucet template test {id}@{version}`"
                )),
                Vec::new(),
                None,
            ),
            None => (
                GateStatus::Stale,
                Some(format!(
                    "v{version}'s recorded results are from another faucet major version (or \
                     another body) — rerun `faucet template test {id}@{version}` under faucet {}",
                    faucet_version()
                )),
                Vec::new(),
                None,
            ),
        }
    };
    let blocked = gate.require_tests && status != GateStatus::Passed;
    let skipped = (blocked && gate.skip_tests_reason.is_some())
        .then(|| gate.skip_tests_reason.clone())
        .flatten();
    GateVerdict {
        allowed: !blocked || skipped.is_some(),
        status,
        require_tests: gate.require_tests,
        reason,
        failing,
        result_at,
        skipped,
    }
}

impl GateVerdict {
    /// The refusal for a launch this verdict does not allow.
    pub fn refusal(&self, id: &str, version: u32) -> CliError {
        CliError::LaunchGated {
            id: id.to_string(),
            version,
            reason: self
                .reason
                .clone()
                .unwrap_or_else(|| "tests have not passed".into()),
            failing: self.failing.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template_tests::result::{BundleOutcome, CaseResult};

    fn result(passed: bool, sha: &str, faucet: &str) -> TemplateTestResult {
        let mut r = TemplateTestResult::new(
            "t",
            1,
            sha.into(),
            BundleOutcome {
                cases: vec![CaseResult {
                    name: "a".into(),
                    source: "bundle".into(),
                    origin: "explicit".into(),
                    passed,
                    attempts: 1,
                    duration_ms: 0,
                    failure: (!passed).then(|| "bad".to_string()),
                }],
                ..Default::default()
            },
            None,
        );
        r.faucet_version = faucet.into();
        r
    }

    #[test]
    fn a_passing_result_for_this_body_and_major_allows_the_launch() {
        let v = evaluate(
            &LaunchGate::new(true),
            "t",
            1,
            true,
            "h",
            &[result(true, "h", faucet_version())],
        );
        assert_eq!(v.status, GateStatus::Passed);
        assert!(v.allowed && v.reason.is_none() && v.result_at.is_some());
    }

    #[test]
    fn the_newest_counting_result_decides_and_lists_failures() {
        let v = evaluate(
            &LaunchGate::new(true),
            "t",
            1,
            true,
            "h",
            &[
                result(false, "h", faucet_version()),
                result(true, "h", faucet_version()),
            ],
        );
        assert_eq!(v.status, GateStatus::Failed);
        assert!(!v.allowed);
        assert_eq!(v.failing, vec!["a: bad".to_string()]);
        assert!(v.refusal("t", 1).to_string().contains("failed"));
    }

    #[test]
    fn results_from_another_major_or_body_are_stale() {
        let v = evaluate(
            &LaunchGate::new(true),
            "t",
            1,
            true,
            "h",
            &[
                result(true, "h", "0.1.0"),
                result(true, "other", faucet_version()),
            ],
        );
        assert_eq!(v.status, GateStatus::Stale);
        assert!(!v.allowed);
        assert!(v.reason.unwrap().contains("another faucet major"));
    }

    #[test]
    fn never_run_and_no_tests_are_refused_only_when_required() {
        let v = evaluate(&LaunchGate::new(true), "t", 2, true, "h", &[]);
        assert_eq!(v.status, GateStatus::NotRun);
        assert!(!v.allowed);
        let v = evaluate(&LaunchGate::new(true), "t", 2, false, "h", &[]);
        assert_eq!(v.status, GateStatus::NoTests);
        assert!(!v.allowed);
        let v = evaluate(&LaunchGate::new(false), "t", 2, false, "h", &[]);
        assert!(v.allowed, "require_tests off keeps today's behaviour");
        assert_eq!(v.status, GateStatus::NoTests);
        assert!(v.skipped.is_none());
    }

    #[test]
    fn an_override_lets_a_refused_launch_through_and_says_so() {
        let gate = LaunchGate::new(true)
            .with_skip(Some("  hotfix INC-12  ".into()))
            .unwrap();
        let v = evaluate(&gate, "t", 1, true, "h", &[]);
        assert!(v.allowed);
        assert_eq!(v.skipped.as_deref(), Some("hotfix INC-12"));
        let passing = evaluate(
            &gate,
            "t",
            1,
            true,
            "h",
            &[result(true, "h", faucet_version())],
        );
        assert!(
            passing.skipped.is_none(),
            "nothing to override when tests pass"
        );
        assert!(
            LaunchGate::new(true)
                .with_skip(Some("   ".into()))
                .unwrap_err()
                .to_string()
                .contains("needs a reason")
        );
        assert!(
            LaunchGate::new(true)
                .with_skip(None)
                .unwrap()
                .skip_tests_reason
                .is_none()
        );
    }

    #[test]
    fn a_refusal_without_a_reason_still_explains() {
        let v = GateVerdict {
            status: GateStatus::Failed,
            allowed: false,
            require_tests: true,
            reason: None,
            failing: Vec::new(),
            result_at: None,
            skipped: None,
        };
        assert!(
            v.refusal("t", 3)
                .to_string()
                .contains("tests have not passed")
        );
    }
}
