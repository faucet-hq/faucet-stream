//! Pure policy evaluation (#702): given a sink's attributes and the labelled
//! columns heading into it, which rules are violated. No I/O; shared by the
//! static pass (`validate` / `plan` / `doctor` / `run` / serve submit) and the
//! runtime backstop.

use super::compile::CompiledPolicy;
use super::spec::PolicyRule;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The destination side of an evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkFacts {
    /// The sink template name (`default` for the singular `pipeline.sink`).
    pub id: String,
    /// Connector kind (`postgres`, `jsonl`, …).
    pub kind: String,
    /// Declared `attributes` (`residency`, `environment`, `region`, …).
    pub attributes: BTreeMap<String, String>,
}

/// One column as the policy sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ColumnFacts {
    /// Column dot-path as it lands in the sink.
    pub name: String,
    /// Labels the column carries.
    pub labels: BTreeSet<String>,
    /// The mask action applied to it before the sink (`hash`, `redact`, …),
    /// when the masking policy provably covers this column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masked: Option<String>,
    /// The column may or may not reach the sink under this name — an opaque
    /// transform hides the mapping, so the label is carried conservatively.
    #[serde(default)]
    pub conservative: bool,
    /// How the label was assigned (`name`, `value`, `lineage`), for reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<String>,
}

/// Why a rule was violated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ViolationKind {
    /// The rule denies the label at this sink outright.
    Denied,
    /// The sink declares no such attribute.
    MissingAttribute { attribute: String },
    /// The sink's attribute value is not in the allowed set.
    AttributeNotAllowed {
        attribute: String,
        value: String,
        allowed: Vec<String>,
    },
    /// The rule lets the label reach the sink only masked, and the column is
    /// not masked with one of the listed actions.
    Unmasked { allowed: Vec<String> },
}

/// One violated rule for one column at one sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Violation {
    pub rule: String,
    pub label: String,
    pub column: String,
    /// The sink template name.
    pub sink: String,
    pub sink_kind: String,
    #[serde(flatten)]
    pub kind: ViolationKind,
    /// The column's presence is inferred conservatively (opaque transform).
    #[serde(default)]
    pub conservative: bool,
    /// Mask actions that would have satisfied the rule.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub satisfied_by_mask: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match &self.kind {
            ViolationKind::Denied => format!(
                "label `{}` may not reach sink `{}` ({})",
                self.label, self.sink, self.sink_kind
            ),
            ViolationKind::MissingAttribute { attribute } => format!(
                "label `{}` requires sink attribute `{attribute}`, which sink `{}` ({}) does not declare",
                self.label, self.sink, self.sink_kind
            ),
            ViolationKind::AttributeNotAllowed {
                attribute,
                value,
                allowed,
            } => format!(
                "label `{}` requires sink `{attribute}` in [{}]; sink `{}` ({}) has `{value}`",
                self.label,
                allowed.join(", "),
                self.sink,
                self.sink_kind
            ),
            ViolationKind::Unmasked { allowed } => format!(
                "label `{}` may reach sink `{}` ({}) only masked with {}",
                self.label,
                self.sink,
                self.sink_kind,
                allowed.join(" or ")
            ),
        };
        write!(
            f,
            "rule `{}`: column `{}`{} — {what}",
            self.rule,
            self.column,
            if self.conservative {
                " (may be present: opaque transform)"
            } else {
                ""
            }
        )?;
        if !self.satisfied_by_mask.is_empty() {
            write!(
                f,
                "; masking it with {} would satisfy the rule",
                self.satisfied_by_mask.join(" or ")
            )?;
        }
        Ok(())
    }
}

/// Whether `rule` governs a column carrying `label` heading into `sink`.
pub fn rule_applies(rule: &PolicyRule, label: &str, sink: &SinkFacts) -> bool {
    if rule.when.label != label {
        return false;
    }
    if !rule.when.sink_kind.is_empty() && !rule.when.sink_kind.iter().any(|k| k == &sink.kind) {
        return false;
    }
    rule.when.sink.iter().all(|(attr, allowed)| {
        sink.attributes
            .get(attr)
            .is_some_and(|v| allowed.iter().any(|a| a == v))
    })
}

/// Evaluate every rule against every labelled column heading into `sink`.
/// Deterministic order: rules in declaration order, columns as given.
pub fn evaluate(
    policy: &CompiledPolicy,
    sink: &SinkFacts,
    columns: &[ColumnFacts],
) -> Vec<Violation> {
    let mut out = Vec::new();
    for rule in policy.rules() {
        for col in columns {
            if !col.labels.iter().any(|l| rule_applies(rule, l, sink)) {
                continue;
            }
            if let Some(action) = &col.masked
                && rule.mask.iter().any(|m| m == action)
            {
                continue;
            }
            let base = |kind: ViolationKind| Violation {
                rule: rule.name.clone(),
                label: rule.when.label.clone(),
                column: col.name.clone(),
                sink: sink.id.clone(),
                sink_kind: sink.kind.clone(),
                kind,
                conservative: col.conservative,
                satisfied_by_mask: rule.mask.clone(),
            };
            if rule.deny {
                out.push(base(ViolationKind::Denied));
                continue;
            }
            if rule.require.is_empty() {
                // A mask-only rule: reaching the sink unmasked is the breach.
                out.push(base(ViolationKind::Unmasked {
                    allowed: rule.mask.clone(),
                }));
                continue;
            }
            for (attr, allowed) in &rule.require {
                match sink.attributes.get(attr) {
                    None => out.push(base(ViolationKind::MissingAttribute {
                        attribute: attr.clone(),
                    })),
                    Some(v) if !allowed.iter().any(|a| a == v) => {
                        out.push(base(ViolationKind::AttributeNotAllowed {
                            attribute: attr.clone(),
                            value: v.clone(),
                            allowed: allowed.clone(),
                        }))
                    }
                    Some(_) => {}
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicySpec;
    use serde_json::json;

    fn policy() -> CompiledPolicy {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [
                {"label": "pii", "fields": ["email"]},
                {"label": "finance", "fields": ["iban"]}
            ],
            "rules": [
                {"name": "pii-eu", "when": {"label": "pii"}, "require": {"residency": ["eu"]}, "mask": ["hash", "redact"]},
                {"name": "no-finance-prod-files", "when": {"label": "finance", "sink_kind": ["jsonl"], "sink": {"environment": ["prod"]}}, "deny": true}
            ]
        }))
        .unwrap();
        CompiledPolicy::compile(&spec).unwrap()
    }

    fn sink(kind: &str, attrs: &[(&str, &str)]) -> SinkFacts {
        SinkFacts {
            id: "default".into(),
            kind: kind.into(),
            attributes: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn col(name: &str, labels: &[&str], masked: Option<&str>) -> ColumnFacts {
        ColumnFacts {
            name: name.into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            masked: masked.map(String::from),
            conservative: false,
            via: None,
        }
    }

    #[test]
    fn require_checks_presence_and_value_and_masking_satisfies() {
        let p = policy();
        let cols = [col("email", &["pii"], None), col("id", &[], None)];
        let v = evaluate(&p, &sink("postgres", &[("residency", "us")]), &cols);
        assert_eq!(v.len(), 1);
        assert!(
            matches!(&v[0].kind, ViolationKind::AttributeNotAllowed { value, .. } if value == "us")
        );
        assert_eq!(v[0].satisfied_by_mask, vec!["hash", "redact"]);
        let text = v[0].to_string();
        assert!(
            text.contains("rule `pii-eu`") && text.contains("masking it with hash or redact"),
            "{text}"
        );

        let v = evaluate(&p, &sink("postgres", &[]), &cols);
        assert!(
            matches!(&v[0].kind, ViolationKind::MissingAttribute { attribute } if attribute == "residency")
        );
        assert!(v[0].to_string().contains("does not declare"));

        assert!(evaluate(&p, &sink("postgres", &[("residency", "eu")]), &cols).is_empty());
        let masked = [col("email", &["pii"], Some("hash"))];
        assert!(evaluate(&p, &sink("postgres", &[("residency", "us")]), &masked).is_empty());
        let partial = [col("email", &["pii"], Some("partial"))];
        assert_eq!(
            evaluate(&p, &sink("postgres", &[("residency", "us")]), &partial).len(),
            1
        );
    }

    #[test]
    fn deny_rules_scope_by_sink_kind_and_attributes() {
        let p = policy();
        let cols = [col("iban", &["finance"], None)];
        let hit = evaluate(&p, &sink("jsonl", &[("environment", "prod")]), &cols);
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].kind, ViolationKind::Denied);
        assert!(hit[0].to_string().contains("may not reach sink"));
        assert!(evaluate(&p, &sink("jsonl", &[("environment", "dev")]), &cols).is_empty());
        assert!(evaluate(&p, &sink("postgres", &[("environment", "prod")]), &cols).is_empty());
        assert!(
            evaluate(&p, &sink("jsonl", &[]), &cols).is_empty(),
            "a `when.sink` attribute the sink lacks means the rule does not apply"
        );
    }

    #[test]
    fn conservative_columns_are_flagged_as_such() {
        let p = policy();
        let mut c = col("email", &["pii"], None);
        c.conservative = true;
        let v = evaluate(&p, &sink("postgres", &[("residency", "us")]), &[c]);
        assert!(v[0].conservative);
        assert!(v[0].to_string().contains("opaque transform"));
        let j = serde_json::to_value(&v[0]).unwrap();
        assert_eq!(j["kind"], "attribute_not_allowed");
        assert_eq!(j["conservative"], true);
    }

    #[test]
    fn mask_only_rule_requires_the_mask() {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "finance", "fields": ["salary"]}],
            "rules": [{"name": "hashed-only", "when": {"label": "finance"}, "mask": ["hash"]}]
        }))
        .unwrap();
        let p = CompiledPolicy::compile(&spec).unwrap();
        let sink = SinkFacts {
            id: "s".into(),
            kind: "jsonl".into(),
            attributes: BTreeMap::new(),
        };
        let col = |masked: Option<&str>| ColumnFacts {
            name: "salary".into(),
            labels: ["finance".to_string()].into(),
            masked: masked.map(String::from),
            conservative: false,
            via: None,
        };
        let v = evaluate(&p, &sink, &[col(None)]);
        assert_eq!(v.len(), 1);
        assert!(matches!(&v[0].kind, ViolationKind::Unmasked { allowed } if allowed == &["hash"]));
        assert!(
            v[0].to_string().contains("only masked with hash"),
            "{}",
            v[0]
        );
        assert!(evaluate(&p, &sink, &[col(Some("hash"))]).is_empty());
        assert_eq!(evaluate(&p, &sink, &[col(Some("redact"))]).len(), 1);
        // A rule with none of deny / require / mask is still refused.
        let bad: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "finance", "fields": ["salary"]}],
            "rules": [{"name": "empty", "when": {"label": "finance"}}]
        }))
        .unwrap();
        assert!(bad.validate().unwrap_err().contains("mask"));
    }
}
