//! The recorded outcome of running a template version's test bundle (#856),
//! and the launch gate that reads it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::runner::CaseOutcome;

/// One case of a bundle run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseResult {
    pub name: String,
    /// `bundle` for the version's own cases, `suite:<id>@<release>` for a
    /// shared suite's.
    pub source: String,
    /// `explicit` / `combine` / `auto` / `behavioral` / `fixture`.
    pub origin: String,
    pub passed: bool,
    /// Runs it took: more than `1` only when the case asked for retries.
    pub attempts: u32,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl CaseResult {
    pub fn from_outcome(c: &CaseOutcome, source: &str) -> Self {
        Self {
            name: c.name.clone(),
            source: source.to_string(),
            origin: c.origin.to_string(),
            passed: c.passed,
            attempts: c.attempts,
            duration_ms: c.duration_ms,
            failure: c.failure.clone(),
        }
    }
}

/// A shared suite a bundle ran, as resolved for this run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSuiteRef {
    /// The requirement's `name`.
    pub name: String,
    /// The requirement's range.
    pub range: String,
    /// The registry version picked, when resolved from a registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// The suite's `release`.
    pub release: String,
}

/// What running a bundle produced, before it is tied to a stored version.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BundleOutcome {
    pub cases: Vec<CaseResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suites: Vec<ResolvedSuiteRef>,
    /// Why the bundle could not run at all (a companion missing, a shared
    /// suite unresolvable, a generation error). Set = the run failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_ms: u64,
}

impl BundleOutcome {
    /// Passed: no setup error, at least one case, every case green.
    pub fn passed(&self) -> bool {
        self.error.is_none() && !self.cases.is_empty() && self.cases.iter().all(|c| c.passed)
    }

    /// `name: failure` for every failing case, plus the setup error.
    pub fn failing(&self) -> Vec<String> {
        let mut out: Vec<String> = self.error.iter().cloned().collect();
        out.extend(
            self.cases
                .iter()
                .filter(|c| !c.passed)
                .map(|c| format!("{}: {}", c.name, c.failure.as_deref().unwrap_or("failed"))),
        );
        out
    }
}

/// A bundle run recorded on one template version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemplateTestResult {
    pub id: String,
    pub version: u32,
    /// sha256 of the version body the run tested. A result whose hash differs
    /// from the stored body (a version number reused after a delete) never
    /// satisfies the gate.
    pub body_sha256: String,
    pub passed: bool,
    /// The faucet binary that ran it. The gate accepts a result only from the
    /// running binary's major version.
    pub faucet_version: String,
    pub recorded_at: DateTime<Utc>,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_by: Option<String>,
    pub cases: Vec<CaseResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suites: Vec<ResolvedSuiteRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// How many results the store keeps per template version.
pub const RESULTS_RETAIN: usize = 20;

/// The running faucet binary's version.
pub fn faucet_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Major component of a faucet version string (`None` when unparseable).
pub fn major_of(version: &str) -> Option<u64> {
    semver::Version::parse(version).ok().map(|v| v.major)
}

impl TemplateTestResult {
    pub fn new(
        id: &str,
        version: u32,
        body_sha256: String,
        outcome: BundleOutcome,
        recorded_by: Option<String>,
    ) -> Self {
        Self {
            id: id.to_string(),
            version,
            body_sha256,
            passed: outcome.passed(),
            faucet_version: faucet_version().to_string(),
            recorded_at: Utc::now(),
            duration_ms: outcome.duration_ms,
            recorded_by,
            cases: outcome.cases,
            suites: outcome.suites,
            error: outcome.error,
        }
    }

    /// The outcome part, for re-rendering.
    pub fn outcome(&self) -> BundleOutcome {
        BundleOutcome {
            cases: self.cases.clone(),
            suites: self.suites.clone(),
            error: self.error.clone(),
            duration_ms: self.duration_ms,
        }
    }

    /// Whether this result may stand in for `body_sha256` under `major`.
    pub fn counts_for(&self, body_sha256: &str, major: Option<u64>) -> bool {
        self.body_sha256 == body_sha256
            && major.is_some()
            && major_of(&self.faucet_version) == major
    }
}

/// Human rendering of one bundle run.
pub fn render_human(title: &str, outcome: &BundleOutcome) -> String {
    let mut out = format!("{title}\n");
    for s in &outcome.suites {
        out.push_str(&format!(
            "  suite {} {} → release {}{}\n",
            s.name,
            s.range,
            s.release,
            s.version.map(|v| format!(" (v{v})")).unwrap_or_default()
        ));
    }
    for c in &outcome.cases {
        let mark = if c.passed { "ok  " } else { "FAIL" };
        let tries = if c.attempts > 1 {
            format!(" (after {} attempts)", c.attempts)
        } else {
            String::new()
        };
        let from = if c.source == "bundle" {
            String::new()
        } else {
            format!(" {}", c.source)
        };
        out.push_str(&format!(
            "  {mark} [{}{from}] {}{tries}\n",
            c.origin, c.name
        ));
        if let Some(f) = &c.failure {
            out.push_str(&format!("       {f}\n"));
        }
    }
    if let Some(e) = &outcome.error {
        out.push_str(&format!("  ERROR {e}\n"));
    }
    let failed = outcome.cases.iter().filter(|c| !c.passed).count();
    out.push_str(&format!(
        "\n{} case(s): {} passed, {} failed — {}\n",
        outcome.cases.len(),
        outcome.cases.len() - failed,
        failed,
        if outcome.passed() { "PASS" } else { "FAIL" }
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(name: &str, passed: bool) -> CaseResult {
        CaseResult {
            name: name.into(),
            source: "bundle".into(),
            origin: "explicit".into(),
            passed,
            attempts: if passed { 1 } else { 2 },
            duration_ms: 1,
            failure: (!passed).then(|| "boom".to_string()),
        }
    }

    #[test]
    fn an_outcome_passes_only_with_cases_all_green_and_no_error() {
        let mut o = BundleOutcome {
            cases: vec![case("a", true)],
            ..Default::default()
        };
        assert!(o.passed());
        o.cases.push(case("b", false));
        assert!(!o.passed());
        assert_eq!(o.failing(), vec!["b: boom".to_string()]);
        assert!(!BundleOutcome::default().passed(), "no case is not a pass");
        let errored = BundleOutcome {
            cases: vec![case("a", true)],
            error: Some("no sink".into()),
            ..Default::default()
        };
        assert!(!errored.passed());
        assert_eq!(errored.failing(), vec!["no sink".to_string()]);
    }

    #[test]
    fn a_result_counts_only_for_its_body_and_the_running_major() {
        let r = TemplateTestResult::new(
            "t",
            1,
            "abc".into(),
            BundleOutcome {
                cases: vec![case("a", true)],
                ..Default::default()
            },
            Some("ci".into()),
        );
        assert!(r.passed);
        let major = major_of(faucet_version());
        assert!(r.counts_for("abc", major));
        assert!(!r.counts_for("other", major));
        assert!(!r.counts_for("abc", major.map(|m| m + 1)));
        assert!(!r.counts_for("abc", None));
        assert_eq!(r.outcome().cases.len(), 1);
        assert_eq!(major_of("not a version"), None);
    }

    #[test]
    fn the_human_report_names_retries_shared_suites_and_errors() {
        let mut shared = case("conf@1.2.0:auto:defaults", false);
        shared.source = "suite:conf@1.2.0".into();
        let o = BundleOutcome {
            cases: vec![case("a", true), shared],
            suites: vec![ResolvedSuiteRef {
                name: "conf".into(),
                range: ">=1".into(),
                version: Some(3),
                release: "1.2.0".into(),
            }],
            error: Some("setup".into()),
            duration_ms: 5,
        };
        let text = render_human("orders v2", &o);
        assert!(
            text.contains("suite conf >=1 → release 1.2.0 (v3)"),
            "{text}"
        );
        assert!(text.contains("after 2 attempts"), "{text}");
        assert!(text.contains("suite:conf@1.2.0"), "{text}");
        assert!(text.contains("ERROR setup"), "{text}");
        assert!(text.contains("FAIL\n"), "{text}");
    }
}
