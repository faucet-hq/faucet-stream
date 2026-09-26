//! PII detection + column-level masking policies (issue #206): a declarative,
//! destination-scoped policy that classifies sensitive fields — by field-name
//! pattern, by value detector (email / card-with-Luhn / SSN / phone / IPv4), or
//! by explicit field list — and rewrites them per action (`redact` / `hash` /
//! `tokenize` / `partial`).
//!
//! **Ordering.** The masking pass runs *first* in the page loop — before the
//! quality, contract, and schema-drift passes and before every sink write. So
//! every downstream consumer (those passes, the sink, the DLQ, and the
//! sink-side lineage sample) observes only masked values; PII never leaks to a
//! sink, a dead-letter queue, or a lineage facet. Masking is value-only and
//! key-preserving, and it keeps determinism (equal inputs → equal masked
//! outputs), non-null-ness, and per-key uniqueness, so the later quality /
//! contract checks stay meaningful over masked data.
//!
//! Masking never fails a run or quarantines a record: matching fields are
//! rewritten in place. The pipeline wires metrics in `instrumented_apply_masking`.

pub mod compile;
pub mod config;
pub mod detect;
mod hash;

pub use compile::CompiledMasking;
pub use config::{Detector, MaskAction, MaskRule, MaskingSpec, MatchSpec};

use compile::CompiledRule;
use serde_json::Value;

/// One field-masking event, for metrics/observability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaskHit {
    /// The rule's stable label.
    pub rule: String,
    /// The action applied (`redact` / `hash` / `tokenize` / `partial`).
    pub action: &'static str,
    /// The value detector that matched, if the match was value-based.
    pub detector: Option<&'static str>,
}

/// Result of masking one page.
#[derive(Debug, Clone, Default)]
pub struct MaskingOutcome {
    /// The page with every matching field rewritten in place.
    pub records: Vec<Value>,
    /// One entry per masked field (order = discovery order).
    pub hits: Vec<MaskHit>,
}

/// Apply the masking policy to one page. Pure: no metrics, no I/O, infallible.
///
/// Each record is walked depth-first; for every field (including nested object
/// keys and array elements, addressed by dot-path such as `user.contacts.0.email`)
/// the first rule that matches wins and its action rewrites the value. A
/// name-based match (`field_pattern` / `fields`) can target a whole subtree
/// (redacted wholesale); value detectors and the hash/tokenize/partial actions
/// operate on scalar leaves.
pub fn apply_masking(records: Vec<Value>, masking: &CompiledMasking) -> MaskingOutcome {
    let mut out = MaskingOutcome {
        records: Vec::with_capacity(records.len()),
        hits: Vec::new(),
    };
    for mut rec in records {
        mask_node(&mut rec, "", masking, &mut out.hits);
        out.records.push(rec);
    }
    out
}

/// Recursively mask `node`, addressed by `path`. Returns nothing; mutates in
/// place and appends any hits.
fn mask_node(node: &mut Value, path: &str, m: &CompiledMasking, hits: &mut Vec<MaskHit>) {
    // 1. Does a rule match this node as a whole?
    if let Some((rule, detector)) = first_match(node, path, m) {
        match &rule.action {
            MaskAction::Redact { mask } => {
                *node = mask.clone();
                hits.push(hit(rule, detector));
                return; // replaced wholesale — do not recurse
            }
            // Scalar-only actions: apply to a scalar leaf, otherwise fall
            // through and recurse into the container's children.
            action => {
                if let Some(s) = scalar_to_string(node) {
                    *node = Value::String(apply_scalar_action(action, &s, &m.hasher));
                    hits.push(hit(rule, detector));
                    return;
                }
            }
        }
    }

    // 2. No terminal match — recurse into children.
    match node {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let child = child_path(path, k);
                mask_node(v, &child, m, hits);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter_mut().enumerate() {
                let child = child_path(path, &i.to_string());
                mask_node(v, &child, m, hits);
            }
        }
        _ => {}
    }
}

/// Find the first rule that matches `node` at `path`. Returns the rule and, if
/// the match was value-based, the detector that fired.
fn first_match<'a>(
    node: &Value,
    path: &str,
    m: &'a CompiledMasking,
) -> Option<(&'a CompiledRule, Option<&'static str>)> {
    // The scalar string form, computed once, for value-detector matching.
    let scalar = scalar_to_string(node);
    for rule in &m.rules {
        // Name-based: field pattern over the dot-path, or explicit field list.
        // The empty root path never matches a name rule.
        if !path.is_empty() {
            if let Some(re) = &rule.field_pattern
                && re.is_match(path)
            {
                return Some((rule, None));
            }
            if rule.fields.contains(path) {
                return Some((rule, None));
            }
        }
        // Value-based: run the detector over the scalar string form.
        if let (Some(det), Some(s)) = (rule.value_detector, scalar.as_deref())
            && detect::detects(det, s)
        {
            return Some((rule, Some(detector_label(det))));
        }
    }
    None
}

fn apply_scalar_action(action: &MaskAction, value: &str, hasher: &hash::Hasher) -> String {
    match action {
        MaskAction::Hash => hasher.digest_hex(value),
        MaskAction::Tokenize { prefix } => hasher.token(value, prefix.as_deref().unwrap_or("")),
        MaskAction::Partial {
            keep_last,
            mask_char,
        } => partial(value, *keep_last, mask_char.unwrap_or('*')),
        // Redact is handled at the node level (works on any type).
        MaskAction::Redact { .. } => "***".to_string(),
    }
}

/// Reveal only the last `keep_last` characters; replace the rest with
/// `mask_char`. Counts by Unicode scalar so multibyte strings mask correctly.
fn partial(value: &str, keep_last: usize, mask_char: char) -> String {
    let chars: Vec<char> = value.chars().collect();
    let n = chars.len();
    // Reveal at most `keep_last` trailing chars, but never the whole value
    // (a short secret with keep_last ≥ len would otherwise leak in the clear).
    let revealed = if keep_last >= n { 0 } else { keep_last };
    let masked_len = n - revealed;
    let mut s = String::with_capacity(n);
    for _ in 0..masked_len {
        s.push(mask_char);
    }
    s.extend(&chars[masked_len..]);
    s
}

/// String form of a scalar JSON value (`string` / `number` / `bool`); `None`
/// for null, objects, and arrays (nothing to mask as a scalar).
fn scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn child_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

fn hit(rule: &CompiledRule, detector: Option<&'static str>) -> MaskHit {
    MaskHit {
        rule: rule.label.clone(),
        action: rule.action_label,
        detector,
    }
}

fn detector_label(d: Detector) -> &'static str {
    match d {
        Detector::Email => "email",
        Detector::CreditCard => "credit_card",
        Detector::Ssn => "ssn",
        Detector::Phone => "phone",
        Detector::Ipv4 => "ipv4",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn compile(v: Value) -> CompiledMasking {
        let spec: MaskingSpec = serde_json::from_value(v).unwrap();
        CompiledMasking::compile(&spec).unwrap()
    }

    #[test]
    fn redact_replaces_named_field() {
        let m = compile(json!({
            "rules": [{ "match": { "field_pattern": "^email$" }, "action": { "type": "redact" } }]
        }));
        let out = apply_masking(vec![json!({"email": "a@b.com", "name": "Al"})], &m);
        assert_eq!(out.records[0], json!({"email": "***", "name": "Al"}));
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.hits[0].action, "redact");
        assert_eq!(out.hits[0].detector, None);
    }

    #[test]
    fn redact_custom_mask_value() {
        // Explicit null mask nulls the field (e.g. for a nullable DB column).
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["ssn"] },
                        "action": { "type": "redact", "mask": null } }]
        }));
        let out = apply_masking(vec![json!({"ssn": "123-45-6789"})], &m);
        assert_eq!(out.records[0], json!({"ssn": null}));

        // A custom scalar mask replaces the value with that scalar.
        let m2 = compile(json!({
            "rules": [{ "match": { "fields": ["x"] },
                        "action": { "type": "redact", "mask": "REDACTED" } }]
        }));
        let out2 = apply_masking(vec![json!({"x": "secret"})], &m2);
        assert_eq!(out2.records[0], json!({"x": "REDACTED"}));
    }

    #[test]
    fn value_detector_matches_across_field_names() {
        let m = compile(json!({
            "rules": [{ "match": { "value_detector": "email" }, "action": { "type": "redact" } }]
        }));
        let out = apply_masking(
            vec![json!({"contact": "a@b.com", "note": "hi", "n": 5})],
            &m,
        );
        assert_eq!(
            out.records[0],
            json!({"contact": "***", "note": "hi", "n": 5})
        );
        assert_eq!(out.hits[0].detector, Some("email"));
    }

    #[test]
    fn hash_is_deterministic_and_joinable() {
        let m = compile(json!({
            "key": "k",
            "rules": [{ "match": { "fields": ["uid"] }, "action": { "type": "hash" } }]
        }));
        let out = apply_masking(
            vec![
                json!({"uid": "u1"}),
                json!({"uid": "u1"}),
                json!({"uid": "u2"}),
            ],
            &m,
        );
        let h1 = out.records[0]["uid"].as_str().unwrap();
        assert_eq!(
            h1,
            out.records[1]["uid"].as_str().unwrap(),
            "same input → same hash"
        );
        assert_ne!(h1, out.records[2]["uid"].as_str().unwrap());
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn tokenize_with_prefix() {
        let m = compile(json!({
            "key": "k",
            "rules": [{ "match": { "fields": ["id"] },
                        "action": { "type": "tokenize", "prefix": "tok_" } }]
        }));
        let out = apply_masking(vec![json!({"id": "abc"})], &m);
        assert!(out.records[0]["id"].as_str().unwrap().starts_with("tok_"));
    }

    #[test]
    fn partial_keeps_last_n() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["card"] },
                        "action": { "type": "partial", "keep_last": 4 } }]
        }));
        let out = apply_masking(vec![json!({"card": "4111111111111111"})], &m);
        assert_eq!(out.records[0]["card"], json!("************1111"));
    }

    #[test]
    fn partial_masks_all_when_keep_ge_len() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["pin"] },
                        "action": { "type": "partial", "keep_last": 10 } }]
        }));
        let out = apply_masking(vec![json!({"pin": "1234"})], &m);
        assert_eq!(
            out.records[0]["pin"],
            json!("****"),
            "never reveal a short value whole"
        );
    }

    #[test]
    fn hash_coerces_number_to_string() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["n"] }, "action": { "type": "hash" } }]
        }));
        let out = apply_masking(vec![json!({"n": 42})], &m);
        assert!(out.records[0]["n"].is_string());
    }

    #[test]
    fn nested_object_and_array_paths_masked() {
        let m = compile(json!({
            "rules": [{ "match": { "field_pattern": r"contacts\.\d+\.email" },
                        "action": { "type": "redact" } }]
        }));
        let out = apply_masking(
            vec![json!({
                "user": {"name": "Al"},
                "contacts": [ {"email": "a@b.com"}, {"email": "c@d.com"} ]
            })],
            &m,
        );
        assert_eq!(out.records[0]["contacts"][0]["email"], json!("***"));
        assert_eq!(out.records[0]["contacts"][1]["email"], json!("***"));
        assert_eq!(out.records[0]["user"]["name"], json!("Al"));
        assert_eq!(out.hits.len(), 2);
    }

    #[test]
    fn redact_whole_subtree_by_name() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["address"] }, "action": { "type": "redact" } }]
        }));
        let out = apply_masking(
            vec![json!({"address": {"street": "1 Main", "zip": "94103"}, "id": 1})],
            &m,
        );
        assert_eq!(out.records[0], json!({"address": "***", "id": 1}));
    }

    #[test]
    fn first_matching_rule_wins() {
        let m = compile(json!({
            "rules": [
                { "name": "first", "match": { "fields": ["x"] }, "action": { "type": "redact" } },
                { "name": "second", "match": { "fields": ["x"] }, "action": { "type": "hash" } }
            ]
        }));
        let out = apply_masking(vec![json!({"x": "v"})], &m);
        assert_eq!(out.records[0]["x"], json!("***"));
        assert_eq!(out.hits[0].rule, "first");
    }

    #[test]
    fn scalar_action_on_container_falls_through_to_children() {
        // A hash rule matching an object by name can't hash the object, so it
        // recurses; the inner email detector then fires.
        let m = compile(json!({
            "rules": [
                { "match": { "field_pattern": "^blob$" }, "action": { "type": "hash" } },
                { "match": { "value_detector": "email" }, "action": { "type": "redact" } }
            ]
        }));
        let out = apply_masking(vec![json!({"blob": {"e": "a@b.com"}})], &m);
        assert_eq!(out.records[0]["blob"]["e"], json!("***"));
    }

    #[test]
    fn null_is_not_masked() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["x"] }, "action": { "type": "hash" } }]
        }));
        let out = apply_masking(vec![json!({"x": null})], &m);
        assert_eq!(out.records[0]["x"], json!(null));
        assert!(out.hits.is_empty());
    }

    #[test]
    fn non_object_record_untouched() {
        let m = compile(json!({
            "rules": [{ "match": { "value_detector": "email" }, "action": { "type": "redact" } }]
        }));
        // top-level string that is an email — root path is empty so name rules
        // don't fire, but value detector does.
        let out = apply_masking(vec![json!("a@b.com")], &m);
        assert_eq!(out.records[0], json!("***"));
    }

    #[test]
    fn no_match_leaves_page_untouched() {
        let m = compile(json!({
            "rules": [{ "match": { "fields": ["absent"] }, "action": { "type": "redact" } }]
        }));
        let page = vec![json!({"a": 1, "b": "x"})];
        let out = apply_masking(page.clone(), &m);
        assert_eq!(out.records, page);
        assert!(out.hits.is_empty());
    }
}
