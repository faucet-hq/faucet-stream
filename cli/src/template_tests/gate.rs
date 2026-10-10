//! A template version's test status (#856), and what it allows. Pure — the
//! registry reads the facts, this decides.
//!
//! One vocabulary everywhere: `passing` (its tests ran and passed under this
//! faucet major), `failing` (ran and failed), `untested` (has tests, but no
//! result for this body under this major) and `no_tests`. Only a `passing`
//! version may hold a label — `stable` or any channel.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::result::{TemplateTestResult, faucet_version, major_of};
use crate::error::CliError;

/// Where the rules for template tests and labels are explained.
pub const TESTS_DOC: &str =
    "https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#template-tests-and-labels";

/// A version's test status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    /// Its tests ran and passed under this faucet major.
    Passing,
    /// Its latest run under this faucet major failed.
    Failing,
    /// It has tests, but no result for this body under this faucet major.
    Untested,
    /// It carries no `tests:` block.
    NoTests,
}

impl TestStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passing => "passing",
            Self::Failing => "failing",
            Self::Untested => "untested",
            Self::NoTests => "no_tests",
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
        };
    }
    let major = major_of(faucet_version());
    match results.iter().find(|r| r.counts_for(body_sha256, major)) {
        Some(r) if r.passed => VersionStatus {
            status: TestStatus::Passing,
            reason: None,
            failing: Vec::new(),
            result_at: Some(r.recorded_at),
        },
        Some(r) => VersionStatus {
            status: TestStatus::Failing,
            reason: Some(format!(
                "the latest test run of v{version} failed — fix the template and register a new \
                 version, or rerun `faucet template test {id}@{version}`"
            )),
            failing: r.outcome().failing(),
            result_at: Some(r.recorded_at),
        },
        None => VersionStatus {
            status: TestStatus::Untested,
            reason: Some(format!(
                "v{version} has no test result under faucet {} — run `faucet template test \
                 {id}@{version}`",
                faucet_version()
            )),
            failing: Vec::new(),
            result_at: None,
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
            status: self.status.as_str().to_string(),
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

    #[test]
    fn results_from_another_major_or_body_or_none_are_untested() {
        let v = status_of(
            "t",
            1,
            true,
            "h",
            &[result(true, "h", "0.1.0"), result(true, "other", faucet_version())],
        );
        assert_eq!(v.status, TestStatus::Untested);
        assert_eq!(status_of("t", 2, true, "h", &[]).status, TestStatus::Untested);
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
        };
        assert!(
            v.refusal("t", 3, "channel prod")
                .to_string()
                .contains("its tests have not passed")
        );
    }
}
