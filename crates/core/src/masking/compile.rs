//! Compilation + fail-fast validation for the masking layer (issue #206).
//! Turns a [`MaskingSpec`] into a [`CompiledMasking`] with pre-compiled
//! regexes and a resolved `Hasher`, optionally scoped to a single
//! destination sink. Bad regexes / empty rule sets surface as
//! [`FaucetError::Config`] at config-load time, never mid-run.

use super::config::{Detector, MaskAction, MaskRule, MaskingSpec, MatchSpec};
use super::hash::Hasher;
use crate::error::FaucetError;
use regex::Regex;
use std::collections::HashSet;

fn config_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("masking: {}", msg.into()))
}

/// A masking policy compiled and ready to apply to pages.
#[derive(Debug, Clone)]
pub struct CompiledMasking {
    pub(crate) rules: Vec<CompiledRule>,
    pub(crate) hasher: Hasher,
}

/// One compiled rule: its matchers plus the action to apply.
#[derive(Debug, Clone)]
pub(crate) struct CompiledRule {
    /// Stable label for logs + the `rule` metric dimension.
    pub(crate) label: String,
    /// Compiled field-name (dot-path) regex.
    pub(crate) field_pattern: Option<Regex>,
    /// Value-based detector.
    pub(crate) value_detector: Option<Detector>,
    /// Explicit dot-paths.
    pub(crate) fields: HashSet<String>,
    /// The action to apply to a matching field.
    pub(crate) action: MaskAction,
    /// Stable action label for the `action` metric dimension.
    pub(crate) action_label: &'static str,
}

impl CompiledMasking {
    /// Compile the full policy (every rule, ignoring `applies_to`). Use this
    /// for library callers that don't scope by destination.
    pub fn compile(spec: &MaskingSpec) -> Result<Self, FaucetError> {
        Self::compile_scoped(spec, None)
    }

    /// Compile the policy scoped to one destination sink, identified by any of
    /// `sink_ids` (typically its template name and its connector kind): rules
    /// with a non-empty `applies_to` that names none of `sink_ids` are dropped.
    /// Every rule is still validated (so a bad regex fails the load even if the
    /// rule doesn't apply to this sink).
    pub fn compile_for_sink(spec: &MaskingSpec, sink_ids: &[&str]) -> Result<Self, FaucetError> {
        Self::compile_scoped(spec, Some(sink_ids))
    }

    fn compile_scoped(spec: &MaskingSpec, sink_ids: Option<&[&str]>) -> Result<Self, FaucetError> {
        if spec.rules.is_empty() {
            return Err(config_err("at least one rule is required"));
        }
        let mut rules = Vec::with_capacity(spec.rules.len());
        for (i, rule) in spec.rules.iter().enumerate() {
            let compiled = compile_rule(rule, i)?; // validate every rule
            if applies(rule, sink_ids) {
                rules.push(compiled);
            }
        }
        Ok(Self {
            rules,
            hasher: Hasher::from_key(spec.key.as_deref()),
        })
    }

    /// `true` when no rule applies (e.g. every rule is scoped to other sinks).
    /// The caller can skip attaching the pass entirely.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Number of active (post-scoping) rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// The action a name-based rule applies to the field at the dot-path
    /// `column`, matched on the field itself or on any ancestor — the same
    /// depth-first walk [`apply_masking`](super::apply_masking) performs, so a
    /// container matched by name masks every leaf beneath it. Value-detector
    /// rules are not consulted: they depend on the value, not the name.
    pub fn name_action_for(&self, column: &str) -> Option<&'static str> {
        let mut end = 0;
        while end < column.len() {
            end = column[end..].find('.').map_or(column.len(), |i| end + i);
            let prefix = &column[..end];
            if let Some(rule) = self.rules.iter().find(|r| {
                r.fields.contains(prefix)
                    || r.field_pattern
                        .as_ref()
                        .is_some_and(|re| re.is_match(prefix))
            }) {
                return Some(rule.action_label);
            }
            end += 1;
        }
        None
    }
}

/// Whether `rule` applies to a sink identified by any of `sink_ids` (unscoped
/// rules — empty `applies_to` — apply everywhere).
fn applies(rule: &MaskRule, sink_ids: Option<&[&str]>) -> bool {
    match sink_ids {
        None => true,
        Some(ids) => {
            rule.applies_to.is_empty() || rule.applies_to.iter().any(|t| ids.contains(&t.as_str()))
        }
    }
}

fn compile_rule(rule: &MaskRule, index: usize) -> Result<CompiledRule, FaucetError> {
    let MatchSpec {
        field_pattern,
        value_detector,
        fields,
    } = &rule.matcher;

    if field_pattern.is_none() && value_detector.is_none() && fields.is_empty() {
        return Err(config_err(format!(
            "rule {index}: `match` must set at least one of field_pattern / value_detector / fields"
        )));
    }

    let field_pattern = match field_pattern {
        Some(p) => Some(Regex::new(p).map_err(|e| {
            config_err(format!(
                "rule {index}: invalid field_pattern regex '{p}': {e}"
            ))
        })?),
        None => None,
    };

    // Validate the partial action's config.
    if let MaskAction::Tokenize { prefix: Some(p) } = &rule.action
        && p.is_empty()
    {
        return Err(config_err(format!(
            "rule {index}: tokenize prefix, when set, must be non-empty (omit it for no prefix)"
        )));
    }

    let label = rule.name.clone().unwrap_or_else(|| format!("rule_{index}"));

    Ok(CompiledRule {
        label,
        field_pattern,
        value_detector: *value_detector,
        fields: fields.iter().cloned().collect(),
        action: rule.action.clone(),
        action_label: rule.action.label(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> MaskingSpec {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn compiles_valid_policy() {
        let c = CompiledMasking::compile(&spec(json!({
            "rules": [
                { "match": { "field_pattern": "email" }, "action": { "type": "redact" } },
                { "match": { "value_detector": "ssn" }, "action": { "type": "hash" } }
            ]
        })))
        .unwrap();
        assert_eq!(c.rule_count(), 2);
        assert!(!c.is_empty());
        assert_eq!(c.rules[0].label, "rule_0");
        assert_eq!(c.rules[0].action_label, "redact");
    }

    #[test]
    fn empty_rules_rejected() {
        let err = CompiledMasking::compile(&spec(json!({ "rules": [] }))).unwrap_err();
        assert!(matches!(err, FaucetError::Config(m) if m.contains("at least one rule")));
    }

    #[test]
    fn empty_match_rejected() {
        let err = CompiledMasking::compile(&spec(json!({
            "rules": [{ "match": {}, "action": { "type": "redact" } }]
        })))
        .unwrap_err();
        assert!(matches!(err, FaucetError::Config(m) if m.contains("at least one of")));
    }

    #[test]
    fn bad_regex_rejected() {
        let err = CompiledMasking::compile(&spec(json!({
            "rules": [{ "match": { "field_pattern": "(" }, "action": { "type": "hash" } }]
        })))
        .unwrap_err();
        assert!(matches!(err, FaucetError::Config(m) if m.contains("invalid field_pattern")));
    }

    #[test]
    fn empty_tokenize_prefix_rejected() {
        let err = CompiledMasking::compile(&spec(json!({
            "rules": [{ "match": { "fields": ["t"] }, "action": { "type": "tokenize", "prefix": "" } }]
        })))
        .unwrap_err();
        assert!(matches!(err, FaucetError::Config(m) if m.contains("tokenize prefix")));
    }

    #[test]
    fn name_action_for_matches_the_field_or_an_ancestor() {
        let c = CompiledMasking::compile_for_sink(
            &spec(json!({
                "rules": [
                    { "match": { "value_detector": "email" }, "action": { "type": "redact" } },
                    { "match": { "fields": ["phones"] }, "action": { "type": "hash" } },
                    { "match": { "field_pattern": "^card$" }, "action": { "type": "partial" } },
                    { "match": { "fields": ["user.ssn"] }, "action": { "type": "tokenize" } },
                    { "match": { "fields": ["other"] }, "action": { "type": "redact" },
                      "applies_to": ["elsewhere"] }
                ]
            })),
            &["here"],
        )
        .unwrap();
        assert_eq!(c.name_action_for("phones"), Some("hash"));
        assert_eq!(c.name_action_for("phones.0"), Some("hash"));
        assert_eq!(c.name_action_for("card.number"), Some("partial"));
        assert_eq!(c.name_action_for("card"), Some("partial"));
        assert_eq!(c.name_action_for("user.ssn"), Some("tokenize"));
        assert_eq!(
            c.name_action_for("ssn"),
            None,
            "fields match the full path only"
        );
        assert_eq!(c.name_action_for("user"), None);
        assert_eq!(c.name_action_for("cardholder"), None);
        assert_eq!(c.name_action_for("other"), None, "scoped to another sink");
        assert_eq!(
            c.name_action_for("email"),
            None,
            "detectors are value-based"
        );
        assert_eq!(c.name_action_for(""), None);
    }

    #[test]
    fn custom_rule_name_used_as_label() {
        let c = CompiledMasking::compile(&spec(json!({
            "rules": [{ "name": "cards", "match": { "value_detector": "credit_card" },
                        "action": { "type": "partial" } }]
        })))
        .unwrap();
        assert_eq!(c.rules[0].label, "cards");
    }

    #[test]
    fn sink_scoping_filters_rules_but_validates_all() {
        let s = spec(json!({
            "rules": [
                { "match": { "fields": ["a"] }, "action": { "type": "redact" }, "applies_to": ["secure"] },
                { "match": { "fields": ["b"] }, "action": { "type": "redact" }, "applies_to": ["default"] },
                { "match": { "fields": ["c"] }, "action": { "type": "redact" } }
            ]
        }));
        let def = CompiledMasking::compile_for_sink(&s, &["default"]).unwrap();
        // rule b (default) + rule c (unscoped) apply; rule a (secure) does not.
        assert_eq!(def.rule_count(), 2);
        let sec = CompiledMasking::compile_for_sink(&s, &["secure"]).unwrap();
        assert_eq!(sec.rule_count(), 2); // rule a + rule c
        let other = CompiledMasking::compile_for_sink(&s, &["elsewhere"]).unwrap();
        assert_eq!(other.rule_count(), 1); // only unscoped rule c
        assert!(!other.is_empty());
        // Multiple identifiers (template name + kind): matches on either.
        let by_kind = CompiledMasking::compile_for_sink(&s, &["postgres", "secure"]).unwrap();
        assert_eq!(by_kind.rule_count(), 2); // rule a (secure) + rule c
    }

    #[test]
    fn sink_scoping_still_validates_inapplicable_rule_regex() {
        // The bad-regex rule is scoped to "secure" but must still fail when
        // compiling for "default".
        let s = spec(json!({
            "rules": [{ "match": { "field_pattern": "(" }, "action": { "type": "hash" },
                        "applies_to": ["secure"] }]
        }));
        assert!(CompiledMasking::compile_for_sink(&s, &["default"]).is_err());
    }
}
