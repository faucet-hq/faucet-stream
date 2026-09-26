//! Compiled form of a [`PolicySpec`]: regexes built once, labels resolvable
//! by column name and by value.

use super::spec::{PolicyRule, PolicySpec};
use crate::masking::{Detector, detect};
use regex::Regex;
use std::collections::{BTreeSet, HashSet};

/// One compiled classification.
#[derive(Debug, Clone)]
pub struct CompiledClassification {
    pub label: String,
    pub fields: HashSet<String>,
    pub field_pattern: Option<Regex>,
    pub value_detector: Option<Detector>,
}

/// A validated, compiled policy.
#[derive(Debug, Clone)]
pub struct CompiledPolicy {
    classifications: Vec<CompiledClassification>,
    rules: Vec<PolicyRule>,
}

impl CompiledPolicy {
    /// Validate and compile. Every defect surfaces here, at config load.
    pub fn compile(spec: &PolicySpec) -> Result<Self, String> {
        spec.validate()?;
        let classifications = spec
            .classifications
            .iter()
            .map(|c| {
                Ok(CompiledClassification {
                    label: c.label.clone(),
                    fields: c.fields.iter().cloned().collect(),
                    field_pattern: match &c.field_pattern {
                        Some(p) => Some(Regex::new(p).map_err(|e| e.to_string())?),
                        None => None,
                    },
                    value_detector: c.value_detector,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            classifications,
            rules: spec.rules.clone(),
        })
    }

    pub fn rules(&self) -> &[PolicyRule] {
        &self.rules
    }

    pub fn classifications(&self) -> &[CompiledClassification] {
        &self.classifications
    }

    /// Labels a column earns from its **name**: an exact `fields` entry
    /// matches the full dot-path or its leaf key (`ssn` covers `user.ssn`),
    /// `field_pattern` is tested against the full dot-path.
    pub fn labels_for_name(&self, name: &str) -> BTreeSet<String> {
        let leaf = name.rsplit('.').next().unwrap_or(name);
        self.classifications
            .iter()
            .filter(|c| {
                c.fields.contains(name)
                    || c.fields.contains(leaf)
                    || c.field_pattern.as_ref().is_some_and(|re| re.is_match(name))
            })
            .map(|c| c.label.clone())
            .collect()
    }

    /// Labels a scalar string **value** earns from the value detectors.
    pub fn labels_for_value(&self, value: &str) -> BTreeSet<String> {
        self.classifications
            .iter()
            .filter(|c| c.value_detector.is_some_and(|d| detect::detects(d, value)))
            .map(|c| c.label.clone())
            .collect()
    }

    /// The value-detector classifications (the runtime backstop's work list).
    pub fn value_detectors(&self) -> impl Iterator<Item = (&str, Detector)> {
        self.classifications
            .iter()
            .filter_map(|c| c.value_detector.map(|d| (c.label.as_str(), d)))
    }

    /// Whether any rule governs `label` at all.
    pub fn governs(&self, label: &str) -> bool {
        self.rules.iter().any(|r| r.when.label == label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy() -> CompiledPolicy {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [
                {"label": "pii", "fields": ["email", "user.phone"], "value_detector": "email"},
                {"label": "pii", "field_pattern": "(?i)ssn$", "value_detector": "ssn"},
                {"label": "finance", "field_pattern": "^amount"}
            ],
            "rules": [
                {"name": "r", "when": {"label": "pii"}, "deny": true}
            ]
        }))
        .unwrap();
        CompiledPolicy::compile(&spec).unwrap()
    }

    #[test]
    fn labels_by_name_and_pattern() {
        let p = policy();
        assert_eq!(p.labels_for_name("email"), ["pii".to_string()].into());
        assert_eq!(p.labels_for_name("user.phone"), ["pii".to_string()].into());
        assert_eq!(
            p.labels_for_name("contact.email"),
            ["pii".to_string()].into(),
            "a bare field name covers the leaf key"
        );
        assert_eq!(
            p.labels_for_name("customer_SSN"),
            ["pii".to_string()].into()
        );
        assert_eq!(
            p.labels_for_name("amount_cents"),
            ["finance".to_string()].into()
        );
        assert!(p.labels_for_name("id").is_empty());
    }

    #[test]
    fn labels_by_value_use_the_masking_detectors() {
        let p = policy();
        assert_eq!(p.labels_for_value("a@b.io"), ["pii".to_string()].into());
        assert_eq!(
            p.labels_for_value("123-45-6789"),
            ["pii".to_string()].into()
        );
        assert!(p.labels_for_value("hello").is_empty());
        assert_eq!(p.value_detectors().count(), 2);
        assert!(p.governs("pii") && !p.governs("finance"));
        assert_eq!(p.rules().len(), 1);
        assert_eq!(p.classifications().len(), 3);
    }

    #[test]
    fn compile_rejects_an_invalid_spec() {
        let spec: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "x", "field_pattern": "("}],
            "rules": []
        }))
        .unwrap();
        assert!(CompiledPolicy::compile(&spec).is_err());
    }
}
