//! Config types for data-flow policies (#702): classifications that tag
//! columns with labels, and rules that say where a labelled column may land.

use crate::masking::{Detector, MaskAction};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Mask actions a rule may name in `mask:`.
pub const MASK_ACTIONS: [&str; 4] = ["redact", "hash", "tokenize", "partial"];

/// A data-flow policy: **classifications** (which columns carry which label)
/// and **rules** (what a labelled column requires of the sink it lands in).
/// Declared once — in a policy file passed with `--policy`, in a deployment
/// overlay, or as a top-level `policy:` block — and evaluated against every
/// pipeline before any data moves; a value detector re-checks at run time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicySpec {
    /// Policy grammar version. Only `1` exists.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Free-form description shown by `faucet policy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// How columns get their labels.
    #[serde(default)]
    pub classifications: Vec<Classification>,

    /// What each label demands of a destination.
    #[serde(default)]
    pub rules: Vec<PolicyRule>,
}

fn default_version() -> u32 {
    1
}

/// One way a column earns a label: by exact name, by a regex over the column
/// name, or by a value detector (run at run time over the real values, and
/// statically over a sample when one is available).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Classification {
    /// The label this classification assigns (`pii`, `finance`, `health`, …).
    pub label: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Exact column names (top-level or dot-path) that carry the label.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,

    /// Regex over the column's dot-path; a match assigns the label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_pattern: Option<String>,

    /// A value detector (`email`, `credit_card`, `ssn`, `phone`, `ipv4`): a
    /// column whose values look like the class carries the label. Reuses the
    /// masking detectors, so a masked column no longer matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_detector: Option<Detector>,
}

/// A rule over one label: which sinks it applies to, and what satisfies it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRule {
    /// Stable rule name — named in every violation, audit entry and metric.
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// What the rule applies to.
    pub when: RuleWhen,

    /// Sink attributes a labelled column's destination must carry, each with
    /// its allowed values: `{ residency: [eu], environment: [prod, staging] }`.
    /// A sink missing the attribute violates the rule.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub require: BTreeMap<String, Vec<String>>,

    /// Mask actions that satisfy the rule instead: a column masked by one of
    /// these (`redact` / `hash` / `tokenize` / `partial`) may land anywhere the
    /// rule would otherwise refuse. Empty = masking does not satisfy the rule.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mask: Vec<String>,

    /// The labelled column may not reach a matching sink at all (unless a
    /// listed `mask` action applies).
    #[serde(default)]
    pub deny: bool,

    /// What a run does when a value detector finds the label at run time in a
    /// column the static pass could not see: fail the run before the page is
    /// written, or quarantine the offending records to the DLQ.
    #[serde(default)]
    pub on_runtime: RuntimeAction,
}

/// The scope of a rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuleWhen {
    /// The label the rule governs.
    pub label: String,

    /// Only sinks of these connector kinds (empty = every sink).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sink_kind: Vec<String>,

    /// Only sinks whose attributes match all of these (empty = every sink) —
    /// e.g. `{ environment: [prod] }` to govern production destinations only.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sink: BTreeMap<String, Vec<String>>,
}

/// Runtime consequence of a value-detector hit that violates a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAction {
    /// Fail the run before the page reaches the sink.
    #[default]
    Fail,
    /// Route the offending records to the DLQ; the rest of the page is written.
    Quarantine,
}

impl RuntimeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeAction::Fail => "fail",
            RuntimeAction::Quarantine => "quarantine",
        }
    }
}

impl PolicySpec {
    /// Fail-fast validation, surfaced at config load. A policy that does not
    /// validate is refused outright — a broken policy must fail closed.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err(format!("unsupported policy version {}", self.version));
        }
        let mut labels = std::collections::BTreeSet::new();
        for (i, c) in self.classifications.iter().enumerate() {
            if c.label.trim().is_empty() {
                return Err(format!("classifications[{i}]: label is empty"));
            }
            if c.fields.is_empty() && c.field_pattern.is_none() && c.value_detector.is_none() {
                return Err(format!(
                    "classification '{}': set fields, field_pattern, or value_detector",
                    c.label
                ));
            }
            if let Some(p) = &c.field_pattern {
                regex::Regex::new(p).map_err(|e| {
                    format!("classification '{}': invalid field_pattern: {e}", c.label)
                })?;
            }
            if let Some(f) = c.fields.iter().find(|f| f.trim().is_empty()) {
                return Err(format!(
                    "classification '{}': fields contains an empty name {f:?}",
                    c.label
                ));
            }
            labels.insert(c.label.as_str());
        }
        let mut names = std::collections::BTreeSet::new();
        for (i, r) in self.rules.iter().enumerate() {
            if r.name.trim().is_empty() {
                return Err(format!("rules[{i}]: name is empty"));
            }
            if !names.insert(r.name.as_str()) {
                return Err(format!("rule '{}' is declared twice", r.name));
            }
            if !labels.contains(r.when.label.as_str()) {
                return Err(format!(
                    "rule '{}': label '{}' has no classification",
                    r.name, r.when.label
                ));
            }
            if !r.deny && r.require.is_empty() && r.mask.is_empty() {
                return Err(format!(
                    "rule '{}': set `deny: true`, at least one `require` attribute, or a `mask` \
                     list (the column may reach the sink only masked)",
                    r.name
                ));
            }
            for (attr, allowed) in &r.require {
                if attr.trim().is_empty() {
                    return Err(format!("rule '{}': require has an empty attribute", r.name));
                }
                if allowed.is_empty() {
                    return Err(format!(
                        "rule '{}': require.{attr} lists no allowed values",
                        r.name
                    ));
                }
            }
            for (attr, allowed) in &r.when.sink {
                if attr.trim().is_empty() || allowed.is_empty() {
                    return Err(format!(
                        "rule '{}': when.sink.{attr} needs an attribute and at least one value",
                        r.name
                    ));
                }
            }
            for m in &r.mask {
                if !MASK_ACTIONS.contains(&m.as_str()) {
                    return Err(format!(
                        "rule '{}': mask action {m:?} is not one of {}",
                        r.name,
                        MASK_ACTIONS.join(", ")
                    ));
                }
            }
        }
        Ok(())
    }

    /// Merge another policy in (classifications and rules appended). A rule
    /// name present on both sides is an error — two sources must not silently
    /// override each other.
    pub fn merge(mut self, other: PolicySpec) -> Result<PolicySpec, String> {
        for r in &other.rules {
            if self.rules.iter().any(|m| m.name == r.name) {
                return Err(format!(
                    "rule '{}' is declared by two policy sources",
                    r.name
                ));
            }
        }
        self.classifications.extend(other.classifications);
        self.rules.extend(other.rules);
        if self.description.is_none() {
            self.description = other.description;
        }
        Ok(self)
    }

    /// Whether any classification uses a value detector (so the runtime
    /// backstop has something to do).
    pub fn has_value_detectors(&self) -> bool {
        self.classifications
            .iter()
            .any(|c| c.value_detector.is_some())
    }
}

/// The action label a masking action satisfies a rule with.
pub fn mask_action_label(action: &MaskAction) -> &'static str {
    action.label()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> PolicySpec {
        serde_json::from_value(v).unwrap()
    }

    fn valid() -> PolicySpec {
        spec(json!({
            "classifications": [
                {"label": "pii", "fields": ["email"], "value_detector": "email"},
                {"label": "finance", "field_pattern": "^(amount|iban)$"}
            ],
            "rules": [
                {"name": "pii-eu", "when": {"label": "pii"}, "require": {"residency": ["eu"]}, "mask": ["hash", "redact"]},
                {"name": "no-finance-to-files", "when": {"label": "finance", "sink_kind": ["jsonl"]}, "deny": true, "on_runtime": "quarantine"}
            ]
        }))
    }

    #[test]
    fn a_valid_policy_parses_with_defaults() {
        let p = valid();
        assert!(p.validate().is_ok());
        assert_eq!(p.version, 1);
        assert!(p.has_value_detectors());
        assert_eq!(p.rules[0].on_runtime, RuntimeAction::Fail);
        assert_eq!(p.rules[1].on_runtime, RuntimeAction::Quarantine);
        assert_eq!(RuntimeAction::Quarantine.as_str(), "quarantine");
        assert_eq!(mask_action_label(&MaskAction::Hash), "hash");
    }

    #[test]
    fn validation_names_each_defect() {
        let bad = |f: fn(&mut PolicySpec)| {
            let mut p = valid();
            f(&mut p);
            p.validate().unwrap_err()
        };
        assert!(bad(|p| p.version = 2).contains("version"));
        assert!(bad(|p| p.classifications[0].label = " ".into()).contains("label is empty"));
        assert!(
            bad(|p| p.classifications[1] = Classification {
                label: "x".into(),
                ..Default::default()
            })
            .contains("set fields")
        );
        assert!(
            bad(|p| p.classifications[1].field_pattern = Some("(".into()))
                .contains("invalid field_pattern")
        );
        assert!(bad(|p| p.classifications[0].fields = vec!["".into()]).contains("empty name"));
        assert!(bad(|p| p.rules[0].name = "".into()).contains("name is empty"));
        assert!(bad(|p| p.rules[1].name = "pii-eu".into()).contains("declared twice"));
        assert!(bad(|p| p.rules[0].when.label = "nope".into()).contains("no classification"));
        assert!(
            bad(|p| {
                p.rules[0].require.clear();
                p.rules[0].mask.clear();
            })
            .contains("deny: true")
        );
        assert!(
            bad(|p| {
                p.rules[0].require.insert("".into(), vec!["x".into()]);
            })
            .contains("empty attribute")
        );
        assert!(
            bad(|p| {
                p.rules[0].require.insert("region".into(), vec![]);
            })
            .contains("no allowed values")
        );
        assert!(
            bad(|p| {
                p.rules[0].when.sink.insert("env".into(), vec![]);
            })
            .contains("when.sink")
        );
        assert!(bad(|p| p.rules[0].mask = vec!["encrypt".into()]).contains("not one of"));
    }

    #[test]
    fn merge_appends_and_refuses_duplicate_rule_names() {
        let a = valid();
        let b = spec(json!({
            "description": "team policy",
            "classifications": [{"label": "health", "fields": ["diagnosis"]}],
            "rules": [{"name": "health-masked", "when": {"label": "health"}, "deny": true, "mask": ["redact"]}]
        }));
        let merged = a.clone().merge(b.clone()).unwrap();
        assert_eq!(merged.classifications.len(), 3);
        assert_eq!(merged.rules.len(), 3);
        assert_eq!(merged.description.as_deref(), Some("team policy"));
        assert!(merged.validate().is_ok());
        let err = a.merge(valid()).unwrap_err();
        assert!(err.contains("two policy sources"), "{err}");
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(serde_json::from_value::<PolicySpec>(json!({"rulez": []})).is_err());
        assert!(
            serde_json::from_value::<PolicyRule>(
                json!({"name": "r", "when": {"label": "x"}, "bogus": 1})
            )
            .is_err()
        );
    }
}
