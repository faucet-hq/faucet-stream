//! Config-shaped types for the PII detection + column-masking layer (issue
//! #206). Pure declarations — validation and compilation live in `compile.rs`,
//! evaluation in the module root.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The `masking:` config block: a declarative, destination-scoped policy that
/// classifies sensitive fields (by field-name pattern, by value detector, or
/// by explicit field list) and applies a masking action per page. The masking
/// pass runs *first* — before the quality, contract, and schema-drift passes
/// and before every sink write — so PII never reaches a sink, the DLQ, or a
/// lineage sample unmasked.
///
/// Masking never fails a run or quarantines records: every matching field is
/// rewritten in place. Rules are evaluated in declared order; for a given
/// field, the first matching rule wins.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaskingSpec {
    /// Human-readable description of the policy (documentation metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Secret key for keyed hashing/tokenization (`hash` / `tokenize`
    /// actions). When present, masked values are HMAC-SHA256 keyed digests, so
    /// the mapping cannot be reversed by an attacker who lacks the key yet stays
    /// deterministic (equal inputs → equal tokens) so masked values remain
    /// joinable downstream. Pull it from a secrets manager
    /// (`${vault:...}` / `${aws-sm:...}` / …) rather than hard-coding it.
    ///
    /// When absent, `hash` / `tokenize` fall back to an *unkeyed* SHA-256
    /// digest — still deterministic, but not secret (anyone can recompute it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// The masking rules. Must be non-empty. Evaluated in declared order; the
    /// first rule that matches a given field wins for that field.
    pub rules: Vec<MaskRule>,
}

/// One masking rule: a matcher + the action to apply to matching fields,
/// optionally scoped to a subset of destination sinks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaskRule {
    /// Optional stable label for this rule — surfaced in logs and as the
    /// `rule` metric label. Defaults to a generated `rule_<n>` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// How this rule decides a field is sensitive. At least one of
    /// `field_pattern` / `value_detector` / `fields` must be set.
    #[serde(rename = "match")]
    pub matcher: MatchSpec,

    /// What to do with a matching field's value.
    pub action: MaskAction,

    /// Destination sinks this rule applies to, by template name (e.g.
    /// `default`, `secure`) or connector kind (e.g. `bigquery`). Empty (the
    /// default) means the rule applies to every sink — so the same source can
    /// be fully masked to one sink and partially masked to another by scoping
    /// rules per destination.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applies_to: Vec<String>,
}

/// How a rule classifies a field as sensitive. Any combination is allowed;
/// a field matches the rule if *any* configured criterion matches (name
/// pattern OR value detector OR explicit field name).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MatchSpec {
    /// Regex matched against each field's dot-path (e.g. `user.email`,
    /// `contacts.0.phone`). Case-sensitive unless the pattern opts in with
    /// `(?i)`. Name-based classification — cheap and precise when you know
    /// your field names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_pattern: Option<String>,

    /// Value-based PII detector run over each *string* field value (and
    /// integer values, for `credit_card`). Conservative (fully anchored) by
    /// default to avoid over-masking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_detector: Option<Detector>,

    /// Explicit dot-paths to mask unconditionally — the pre-classification /
    /// tagging escape hatch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

/// Built-in value detectors. All are conservative (anchored full-string
/// matches; the card detector additionally requires a valid Luhn checksum) so
/// false positives stay rare — masking silently rewrites data, so a
/// false positive is a data-quality bug, not just noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Detector {
    /// RFC-5322-ish email address.
    Email,
    /// 13–19 digit card number (spaces/dashes allowed, or a JSON integer)
    /// passing the Luhn check.
    CreditCard,
    /// US Social Security Number `NNN-NN-NNNN`.
    Ssn,
    /// E.164 (`+` then 8–15 digits) or a North-American number written with
    /// separators; a bare run of digits never matches.
    Phone,
    /// IPv4 dotted-quad address.
    Ipv4,
}

impl std::fmt::Display for Detector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Detector::Email => "email",
            Detector::CreditCard => "credit_card",
            Detector::Ssn => "ssn",
            Detector::Phone => "phone",
            Detector::Ipv4 => "ipv4",
        })
    }
}

/// What to do with a value once a rule matches it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaskAction {
    /// Replace the value wholesale with a fixed mask (default `"***"`).
    /// Irreversible and not joinable — use for values you never need again.
    Redact {
        /// Replacement value. Defaults to the JSON string `"***"`. Set it to an
        /// explicit `null` to null the field (e.g. for a nullable DB column),
        /// or to any other JSON scalar you prefer.
        #[serde(default = "default_mask")]
        mask: Value,
    },
    /// Replace the value with its hex HMAC-SHA256 (keyed) / SHA-256 (unkeyed)
    /// digest. Deterministic → joinable; irreversible.
    Hash,
    /// Replace the value with a short opaque token derived from a keyed digest,
    /// optionally with a stable prefix (e.g. `tok_`). Deterministic → joinable.
    Tokenize {
        /// Literal prefix prepended to every token (e.g. `"tok_"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// Reveal only the last `keep_last` characters, replacing the rest with
    /// `mask_char` (default `*`). Preserves format/length for readability
    /// (e.g. `****1234`). `keep_last: 0` masks everything.
    Partial {
        /// Number of trailing characters to keep in the clear. Default `4`.
        #[serde(default = "default_keep_last")]
        keep_last: usize,
        /// Character used for the masked prefix. Default `*`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mask_char: Option<char>,
    },
}

fn default_keep_last() -> usize {
    4
}

fn default_mask() -> Value {
    Value::String("***".to_string())
}

impl MaskAction {
    /// Stable action label (closed set — safe as a metric label).
    pub fn label(&self) -> &'static str {
        match self {
            MaskAction::Redact { .. } => "redact",
            MaskAction::Hash => "hash",
            MaskAction::Tokenize { .. } => "tokenize",
            MaskAction::Partial { .. } => "partial",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_full_masking_block() {
        let spec: MaskingSpec = serde_json::from_value(json!({
            "description": "customer PII",
            "key": "s3cr3t",
            "rules": [
                { "name": "emails",
                  "match": { "value_detector": "email" },
                  "action": { "type": "redact" } },
                { "match": { "field_pattern": "(?i)ssn|social" },
                  "action": { "type": "hash" },
                  "applies_to": ["default"] },
                { "match": { "fields": ["user.card"] },
                  "action": { "type": "partial", "keep_last": 4 } },
                { "match": { "field_pattern": "^token$" },
                  "action": { "type": "tokenize", "prefix": "tok_" } }
            ]
        }))
        .unwrap();
        assert_eq!(spec.key.as_deref(), Some("s3cr3t"));
        assert_eq!(spec.rules.len(), 4);
        assert_eq!(spec.rules[0].name.as_deref(), Some("emails"));
        assert_eq!(spec.rules[0].matcher.value_detector, Some(Detector::Email));
        assert_eq!(spec.rules[1].applies_to, vec!["default".to_string()]);
        assert!(matches!(
            spec.rules[2].action,
            MaskAction::Partial { keep_last: 4, .. }
        ));
    }

    #[test]
    fn partial_keep_last_defaults_to_four() {
        let a: MaskAction = serde_json::from_value(json!({ "type": "partial" })).unwrap();
        assert!(matches!(
            a,
            MaskAction::Partial {
                keep_last: 4,
                mask_char: None
            }
        ));
    }

    #[test]
    fn action_labels_are_stable() {
        assert_eq!(MaskAction::Hash.label(), "hash");
        assert_eq!(
            MaskAction::Redact {
                mask: default_mask()
            }
            .label(),
            "redact"
        );
        assert_eq!(MaskAction::Tokenize { prefix: None }.label(), "tokenize");
        assert_eq!(
            MaskAction::Partial {
                keep_last: 2,
                mask_char: None
            }
            .label(),
            "partial"
        );
    }

    #[test]
    fn detector_display_is_snake_case() {
        assert_eq!(Detector::CreditCard.to_string(), "credit_card");
        let d: Detector = serde_json::from_str("\"ipv4\"").unwrap();
        assert_eq!(d, Detector::Ipv4);
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(
            serde_json::from_value::<MaskingSpec>(json!({
                "rules": [], "nope": 1
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<MaskRule>(json!({
                "match": {}, "action": {"type":"hash"}, "bogus": true
            }))
            .is_err()
        );
    }
}
