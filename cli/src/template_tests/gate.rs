//! A template version's test status (#856), and what it allows. Pure — the
//! registry reads the facts, this decides.
//!
//! One vocabulary everywhere: `passing` (its tests ran and passed under this
//! faucet major.minor), `failing` (ran and failed), `untested` (has tests, but
//! no result for this body under this major.minor) and `no_tests`. Only a
//! `passing` version may take a label — `stable` or any channel.
//!
//! Whether a recorded result still counts is decided by [`evidence_counts`]
//! alone, so the policy can change in one place.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::result::{
    RESULT_SCHEMA, TemplateTestResult, faucet_version, release_line, running_line,
};
use crate::error::CliError;

/// Where the rules for template tests and labels are explained.
pub const TESTS_DOC: &str =
    "https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#template-tests-and-labels";

/// A version's test status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    /// Its tests ran and passed under this faucet major.minor.
    Passing,
    /// Its latest run under this faucet major.minor failed.
    Failing,
    /// It has tests, but no result for this body under this faucet major.minor.
    Untested,
    /// It carries no `tests:` block, or its block generates no cases.
    NoTests,
    /// Its `tests:` block or its recorded results cannot be read, so nothing
    /// can be said about it. Never takes a label and never counts as `newest`.
    Unknown,
}

impl TestStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passing => "passing",
            Self::Failing => "failing",
            Self::Untested => "untested",
            Self::NoTests => "no_tests",
            Self::Unknown => "unknown",
        }
    }

    /// Whether a version in this status may be launched, rolled back to, or
    /// pointed at by a channel.
    pub fn can_hold_label(self) -> bool {
        self == Self::Passing
    }
}

impl std::fmt::Display for TestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A version's status with what explains it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionStatus {
    pub status: TestStatus,
    /// Why the version cannot take a label, when it cannot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `case: failure` lines from the deciding run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failing: Vec<String>,
    /// When the deciding run was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_at: Option<DateTime<Utc>>,
    /// The deciding result's id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
}

/// The validity policy: whether `r` is evidence for a version whose body
/// hashes to `body_sha256`, judged by the running binary. Today: the same
/// body, run by a faucet of the same major.minor, in the current result
/// schema. Every status, label move and run check reads results through this.
pub fn evidence_counts(r: &TemplateTestResult, body_sha256: &str) -> bool {
    r.schema >= RESULT_SCHEMA
        && !r.result_id.is_empty()
        && r.body_sha256 == body_sha256
        && release_line(&r.faucet_version).is_some()
        && release_line(&r.faucet_version) == release_line(faucet_version())
}

/// The result that decides a version's status: the newest one that counts.
/// `results` are newest first.
pub fn deciding_evidence<'a>(
    results: &'a [TemplateTestResult],
    body_sha256: &str,
) -> Option<&'a TemplateTestResult> {
    results.iter().find(|r| evidence_counts(r, body_sha256))
}

/// The eligibility rule for `newest` once a template has been launched:
/// whether this exact body has **ever** had a passing result, under any
/// faucet release. Distinct from [`evidence_counts`], which governs label
/// moves: `newest` is sticky, so a later failing run, a failed revalidation
/// or a stale release line never moves it — only a newer version passing, or
/// deprecating or deleting this one, does.
pub fn ever_passed(results: &[TemplateTestResult], body_sha256: &str) -> bool {
    results
        .iter()
        .any(|r| r.schema >= RESULT_SCHEMA && r.body_sha256 == body_sha256 && r.passed)
}

/// The error a bundle that generated no cases records.
pub const NO_CASES: &str = "the bundle produced no cases";

/// A version's status when its tests or results cannot be read.
pub fn unknown(why: String) -> VersionStatus {
    VersionStatus {
        status: TestStatus::Unknown,
        reason: Some(why),
        failing: Vec::new(),
        result_at: None,
        evidence: None,
    }
}

/// The status of `version` (body hash `body_sha256`) given its results,
/// newest first.
pub fn status_of(
    id: &str,
    version: u32,
    has_tests: bool,
    body_sha256: &str,
    results: &[TemplateTestResult],
) -> VersionStatus {
    if !has_tests {
        return VersionStatus {
            status: TestStatus::NoTests,
            reason: Some(format!(
                "v{version} has no `tests:` block — register a new version that adds one, \
                 then launch that ({TESTS_DOC})"
            )),
            failing: Vec::new(),
            result_at: None,
            evidence: None,
        };
    }
    match deciding_evidence(results, body_sha256) {
        Some(r) if r.cases.is_empty() && r.error.as_deref() == Some(NO_CASES) => VersionStatus {
            status: TestStatus::NoTests,
            reason: Some(format!(
                "v{version}'s `tests:` block generates no cases — register a new version whose \
                 tests run at least one case ({TESTS_DOC})"
            )),
            failing: Vec::new(),
            result_at: Some(r.recorded_at),
            evidence: Some(r.result_id.clone()),
        },
        Some(r) if r.passed => VersionStatus {
            status: TestStatus::Passing,
            reason: None,
            failing: Vec::new(),
            result_at: Some(r.recorded_at),
            evidence: Some(r.result_id.clone()),
        },
        Some(r) => VersionStatus {
            status: TestStatus::Failing,
            reason: Some(format!(
                "the latest test run of v{version} failed — fix the template and register a new \
                 version, or rerun `faucet template test {id}@{version}`"
            )),
            failing: r.outcome().failing(),
            result_at: Some(r.recorded_at),
            evidence: Some(r.result_id.clone()),
        },
        None => VersionStatus {
            status: TestStatus::Untested,
            reason: Some(format!(
                "v{version} has no test result under faucet {} — run `faucet template test \
                 {id}@{version}`",
                running_line()
            )),
            failing: Vec::new(),
            result_at: None,
            evidence: None,
        },
    }
}

impl VersionStatus {
    /// The refusal for `action` (`launch`, `channel prod`, …) on a version that
    /// may not hold a label.
    pub fn refusal(&self, id: &str, version: u32, action: &str) -> CliError {
        CliError::VersionNotPassing {
            id: id.to_string(),
            version,
            status: self.status,
            action: action.to_string(),
            reason: self
                .reason
                .clone()
                .unwrap_or_else(|| "its tests have not passed".into()),
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
    fn a_passing_result_for_this_body_and_major_is_passing() {
        let v = status_of("t", 1, true, "h", &[result(true, "h", faucet_version())]);
        assert_eq!(v.status, TestStatus::Passing);
        assert!(v.status.can_hold_label() && v.reason.is_none() && v.result_at.is_some());
    }

    #[test]
    fn the_newest_counting_result_decides_and_lists_failures() {
        let v = status_of(
            "t",
            1,
            true,
            "h",
            &[
                result(false, "h", faucet_version()),
                result(true, "h", faucet_version()),
            ],
        );
        assert_eq!(v.status, TestStatus::Failing);
        assert!(!v.status.can_hold_label());
        assert_eq!(v.failing, vec!["a: bad".to_string()]);
        let e = v.refusal("t", 1, "launch").to_string();
        assert!(e.contains("v1 is failing") && e.contains("a: bad"), "{e}");
    }

    fn other_minor() -> String {
        let (major, minor) = release_line(faucet_version()).unwrap();
        format!("{major}.{}.0", minor + 1)
    }

    #[test]
    fn results_from_another_release_line_body_or_schema_are_untested() {
        let mut legacy = result(true, "h", faucet_version());
        legacy.schema = 0;
        let mut anonymous = result(true, "h", faucet_version());
        anonymous.result_id.clear();
        let v = status_of(
            "t",
            1,
            true,
            "h",
            &[
                result(true, "h", "0.1.0"),
                result(true, "h", &other_minor()),
                result(true, "other", faucet_version()),
                legacy,
                anonymous,
            ],
        );
        assert_eq!(v.status, TestStatus::Untested);
        assert!(v.reason.unwrap().contains(&running_line()));
        assert_eq!(
            status_of("t", 2, true, "h", &[]).status,
            TestStatus::Untested
        );
        assert!(!evidence_counts(&result(true, "h", "garbage"), "h"));
    }

    #[test]
    fn the_same_major_and_minor_counts_whatever_the_patch() {
        let (major, minor) = release_line(faucet_version()).unwrap();
        let r = result(true, "h", &format!("{major}.{minor}.999"));
        assert!(evidence_counts(&r, "h"));
        let v = status_of("t", 1, true, "h", std::slice::from_ref(&r));
        assert_eq!(v.status, TestStatus::Passing);
        assert_eq!(v.evidence.as_deref(), Some(r.result_id.as_str()));
        assert_eq!(deciding_evidence(std::slice::from_ref(&r), "h"), Some(&r));
    }

    #[test]
    fn newest_eligibility_is_sticky_on_any_pass_of_this_body() {
        // A later failure (a flake, a failed revalidation) keeps it eligible.
        assert!(ever_passed(
            &[
                result(false, "h", faucet_version()),
                result(true, "h", &other_minor())
            ],
            "h"
        ));
        assert!(ever_passed(&[result(true, "h", &other_minor())], "h"));
        assert!(!ever_passed(&[result(false, "h", faucet_version())], "h"));
        assert!(!ever_passed(&[result(true, "x", faucet_version())], "h"));
        let mut legacy = result(true, "h", faucet_version());
        legacy.schema = 0;
        assert!(!ever_passed(&[legacy], "h"));
    }

    #[test]
    fn a_bundle_with_no_cases_is_no_tests_and_unknown_never_takes_a_label() {
        let mut r = result(false, "h", faucet_version());
        r.cases.clear();
        r.error = Some(NO_CASES.into());
        let v = status_of("t", 1, true, "h", &[r]);
        assert_eq!(v.status, TestStatus::NoTests);
        assert!(v.reason.unwrap().contains("generates no cases"));
        let u = unknown("bad".into());
        assert!(!u.status.can_hold_label());
        assert_eq!(u.status.to_string(), "unknown");
    }

    #[test]
    fn a_version_without_tests_is_no_tests_and_cannot_hold_a_label() {
        let v = status_of("t", 2, false, "h", &[result(true, "h", faucet_version())]);
        assert_eq!(v.status, TestStatus::NoTests);
        assert!(!v.status.can_hold_label());
        assert_eq!(v.status.to_string(), "no_tests");
    }

    #[test]
    fn a_refusal_without_a_reason_still_explains() {
        let v = VersionStatus {
            status: TestStatus::Failing,
            reason: None,
            failing: Vec::new(),
            result_at: None,
            evidence: None,
        };
        assert!(
            v.refusal("t", 3, "channel prod")
                .to_string()
                .contains("its tests have not passed")
        );
    }
}
