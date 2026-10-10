//! The recorded outcome of running a template version's test bundle (#856),
//! with the provenance that decides whether it still counts.

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

/// The result shape this binary writes. A result with another (older)
/// schema never counts as evidence.
pub const RESULT_SCHEMA: u32 = 1;

/// Who and what produced a result, besides the body and the binary.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    /// The connectors and transforms compiled into the binary
    /// (`source:csv`, `sink:jsonl`, `transform:flatten`, …), sorted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub build_features: Vec<String>,
    /// Custom connectors registered through a `PluginRegistry`, sorted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    /// The companion templates the bundle ran with (`id@version`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub companions: Vec<String>,
    /// The `faucet serve` instance that ran it; `None` for the CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

/// A bundle run recorded on one template version: the evidence a label move
/// relies on. Produced only by a test run, never accepted from a caller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemplateTestResult {
    /// This result's id — what a label move records as its evidence.
    #[serde(default)]
    pub result_id: String,
    /// Result shape version ([`RESULT_SCHEMA`]).
    #[serde(default)]
    pub schema: u32,
    pub id: String,
    pub version: u32,
    /// sha256 of the version body the run tested.
    pub body_sha256: String,
    pub passed: bool,
    /// The full version of the faucet binary that ran it.
    pub faucet_version: String,
    pub recorded_at: DateTime<Utc>,
    pub duration_ms: u64,
    /// Who asked for the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_by: Option<String>,
    #[serde(default, flatten)]
    pub provenance: Provenance,
    pub cases: Vec<CaseResult>,
    /// The shared-suite releases the run used.
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

/// `(major, minor)` of a faucet version string (`None` when unparseable).
pub fn release_line(version: &str) -> Option<(u64, u64)> {
    semver::Version::parse(version)
        .ok()
        .map(|v| (v.major, v.minor))
}

/// `major.minor` of the running binary, as stored in the revalidation marker.
pub fn running_line() -> String {
    release_line(faucet_version())
        .map(|(a, b)| format!("{a}.{b}"))
        .unwrap_or_else(|| faucet_version().to_string())
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
            result_id: uuid::Uuid::new_v4().to_string(),
            schema: RESULT_SCHEMA,
            id: id.to_string(),
            version,
            body_sha256,
            passed: outcome.passed(),
            faucet_version: faucet_version().to_string(),
            recorded_at: Utc::now(),
            duration_ms: outcome.duration_ms,
            recorded_by,
            provenance: Provenance::default(),
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
    fn a_new_result_carries_an_id_the_schema_and_the_binary() {
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
        assert!(r.passed && !r.result_id.is_empty());
        assert_eq!(r.schema, RESULT_SCHEMA);
        assert_eq!(r.faucet_version, faucet_version());
        assert_eq!(r.outcome().cases.len(), 1);
        assert_eq!(release_line("1.13.3"), Some((1, 13)));
        assert_eq!(release_line("not a version"), None);
        assert_eq!(running_line().matches('.').count(), 1);
        let json = serde_json::to_value(&r).unwrap();
        let back: TemplateTestResult = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
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
