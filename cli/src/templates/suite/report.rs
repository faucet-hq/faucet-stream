//! Rendering for `faucet template test` (#648).
//!
//! The human checklist names each case's **origin** as well as its result,
//! because a red case an operator never wrote — one the `auto:` generator
//! derived — is otherwise a mystery, and "where did this come from?" is the
//! first question they will ask.

use serde::Serialize;

use super::runner::SuiteOutcome;

/// Machine-readable report (`--json`).
#[derive(Debug, Serialize)]
pub struct SuiteReport {
    pub template: String,
    pub version: Option<u32>,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub cases: Vec<ReportedCase>,
}

/// One case in the report.
#[derive(Debug, Serialize)]
pub struct ReportedCase {
    pub name: String,
    /// `explicit` / `combine` / `auto` / `behavioral`.
    pub origin: String,
    /// `"pass"` or `"fail"`.
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub params: serde_json::Value,
}

impl SuiteReport {
    pub fn new(template: &str, version: Option<u32>, outcome: &SuiteOutcome) -> Self {
        let cases: Vec<ReportedCase> = outcome
            .cases
            .iter()
            .map(|c| ReportedCase {
                name: c.name.clone(),
                origin: c.origin.to_string(),
                status: if c.passed { "pass" } else { "fail" },
                failure: c.failure.clone(),
                params: serde_json::to_value(&c.params).unwrap_or(serde_json::Value::Null),
            })
            .collect();
        Self {
            template: template.to_string(),
            version,
            total: cases.len(),
            passed: outcome.passed(),
            failed: outcome.failed(),
            cases,
        }
    }

    /// Human checklist.
    pub fn render_human(&self) -> String {
        let mut out = String::new();
        let version = self.version.map(|v| format!(" v{v}")).unwrap_or_default();
        out.push_str(&format!("template {}{version}\n", self.template));
        for c in &self.cases {
            let mark = if c.status == "pass" { "ok  " } else { "FAIL" };
            out.push_str(&format!("  {mark} [{}] {}\n", c.origin, c.name));
            if let Some(f) = &c.failure {
                out.push_str(&format!("       {f}\n"));
            }
        }
        out.push_str(&format!(
            "\n{} case(s): {} passed, {} failed\n",
            self.total, self.passed, self.failed
        ));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::templates::suite::runner::CaseOutcome;
    use std::collections::BTreeMap;

    fn outcome() -> SuiteOutcome {
        SuiteOutcome {
            cases: vec![
                CaseOutcome {
                    name: "jsonl".into(),
                    origin: "explicit",
                    params: BTreeMap::new(),
                    passed: true,
                    failure: None,
                },
                CaseOutcome {
                    name: "auto:missing-tenant".into(),
                    origin: "auto",
                    params: BTreeMap::new(),
                    passed: false,
                    failure: Some("expected this combination to fail".into()),
                },
            ],
        }
    }

    #[test]
    fn human_report_names_the_origin_of_each_case() {
        let r = SuiteReport::new("t", Some(3), &outcome());
        let text = r.render_human();
        assert!(text.contains("template t v3"), "{text}");
        assert!(text.contains("ok   [explicit] jsonl"), "{text}");
        assert!(
            text.contains("FAIL [auto] auto:missing-tenant"),
            "a case the operator never wrote must say where it came from: {text}"
        );
        assert!(text.contains("expected this combination to fail"), "{text}");
        assert!(text.contains("2 case(s): 1 passed, 1 failed"), "{text}");
    }

    #[test]
    fn counts_come_from_the_outcome() {
        let r = SuiteReport::new("t", None, &outcome());
        assert_eq!(r.total, 2);
        assert_eq!(r.passed, 1);
        assert_eq!(r.failed, 1);
        // No version renders without a stray "v".
        assert!(!r.render_human().contains(" v"), "{}", r.render_human());
    }

    #[test]
    fn json_is_serializable_and_omits_a_missing_failure() {
        let r = SuiteReport::new("t", Some(1), &outcome());
        let v = serde_json::to_value(&r).expect("serializes");
        assert_eq!(v["cases"][0]["status"], "pass");
        assert!(
            v["cases"][0].get("failure").is_none(),
            "a passing case carries no failure field"
        );
        assert_eq!(v["cases"][1]["status"], "fail");
    }
}
