//! Tests for the composed top-level config JSON Schema (`faucet schema config`,
//! #213).
//!
//! These run under any feature set: the composed schema always includes every
//! compiled-in connector (the `default` feature already pulls the `source` /
//! `sink` aggregates) and the top-level grammar. Example validation skips any
//! example whose top-level uses a block not compiled into this build, so a
//! slim build never fails on a feature-gated block it doesn't know.

use faucet_cli::schema_compose::config_schema;
use serde_json::{Value, json};
use std::path::Path;

/// Every shipped example config that is a full pipeline document must validate
/// against the composed schema. Compose-time-only fragments (`extends:` /
/// `!include` / `profiles:`) and non-config example files are skipped.
#[test]
fn shipped_examples_validate_against_composed_schema() {
    let schema = config_schema();
    // Top-level keys this build's schema knows about (feature-gated blocks like
    // `schedule` / `lineage` are absent from a slim build).
    let known_keys: Vec<String> = schema["properties"]
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    let validator = jsonschema::validator_for(&schema).expect("composed schema compiles");

    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&examples).expect("examples dir") {
        let path = entry.unwrap().path();
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if !matches!(ext, "yaml" | "yml") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        // Compose-time directives are stripped before `PipelineConfig` parsing;
        // the raw document would fail the strict (deny-unknown-fields) schema.
        if text.contains("!include") {
            continue;
        }
        let value: Value = match serde_yaml::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        // Only validate full pipeline documents.
        if !obj.contains_key("pipeline") {
            continue;
        }
        if obj.contains_key("extends") || obj.contains_key("profiles") {
            continue;
        }
        // Skip examples that use a top-level block this build doesn't compile in
        // (e.g. a `schedule:` example on a build without the `schedule` feature).
        if obj.keys().any(|k| !known_keys.contains(k)) {
            continue;
        }
        let errors: Vec<String> = validator
            .iter_errors(&value)
            .map(|e| e.to_string())
            .collect();
        assert!(
            errors.is_empty(),
            "example `{}` failed schema validation:\n{}",
            path.display(),
            errors.join("\n")
        );
        checked += 1;
    }
    assert!(checked > 0, "no example configs were validated");
}

/// The composed schema rejects an unknown top-level key (the top-level grammar
/// stays strict — `deny_unknown_fields`).
#[test]
fn rejects_unknown_top_level_key() {
    let schema = config_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let bad = json!({
        "version": 1,
        "pipeline": { "source": { "type": "csv", "config": {} }, "sink": { "type": "jsonl", "config": {} } },
        "not_a_real_top_level_key": true,
    });
    assert!(
        !validator.is_valid(&bad),
        "unknown top-level key must be rejected"
    );
}

/// The composed schema rejects an unknown connector `type`.
#[test]
fn rejects_unknown_connector_kind() {
    let schema = config_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let bad = json!({
        "version": 1,
        "pipeline": { "source": { "type": "not-a-connector", "config": {} }, "sink": { "type": "jsonl", "config": {} } },
    });
    assert!(
        !validator.is_valid(&bad),
        "unknown connector kind must be rejected"
    );
}

/// A minimal valid document validates.
#[test]
fn minimal_valid_document_passes() {
    let schema = config_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let good = json!({
        "version": 1,
        "pipeline": { "source": { "type": "csv", "config": { "path": "in.csv" } }, "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } } },
    });
    let errors: Vec<String> = validator
        .iter_errors(&good)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// The committed `schemas/faucet.schema.json` (generated under `--all-features`)
/// covers the current build's composed schema. Guards against forgetting to
/// regenerate after a connector/config change: every node the current build
/// emits must be present in the committed file. Regenerate with:
/// `cargo run --all-features -- schema config > schemas/faucet.schema.json`.
///
/// A **subset** check (not exact equality) so a smaller feature set than
/// `--all-features` — which produces fewer connector `oneOf` branches / config
/// fields — still passes, while an added or changed schema node in the current
/// build (in the all-features CI job the two are identical) is caught.
#[test]
fn committed_schema_covers_current_build() {
    let current = config_schema();
    let committed: Value = serde_json::from_str(include_str!("../../schemas/faucet.schema.json"))
        .expect("committed schema parses");
    if let Err(path) = is_subset(&current, &committed, String::new()) {
        // A bare JSON pointer costs a debugging round-trip when the divergence
        // is inside an order-insensitive `oneOf` (the index is the *current*
        // build's, which need not match the committed order). Print the node
        // itself so the offending connector is named.
        let node = path
            .split('/')
            .filter(|s| !s.is_empty())
            .try_fold(&current, |v: &Value, seg| match v {
                Value::Object(m) => m.get(seg),
                Value::Array(a) => seg.parse::<usize>().ok().and_then(|i| a.get(i)),
                _ => None,
            })
            .map(|v| {
                v.get("title")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        let mut s = v.to_string();
                        s.truncate(400);
                        s
                    })
            })
            .unwrap_or_else(|| "<node not found>".to_string());
        eprintln!("divergent node at {path}: {node}");
        panic!(
            "schemas/faucet.schema.json is stale at `{path}` — regenerate with \
             `cargo run --all-features -- schema config > schemas/faucet.schema.json`"
        );
    }
}

/// Every node in `sub` must be present (deep-equal) in `sup`. Returns the JSON
/// pointer of the first divergence on failure.
fn is_subset(sub: &Value, sup: &Value, path: String) -> Result<(), String> {
    match (sub, sup) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, va) in a {
                match b.get(k) {
                    Some(vb) => is_subset(va, vb, format!("{path}/{k}"))?,
                    None => return Err(format!("{path}/{k}")),
                }
            }
            Ok(())
        }
        (Value::Array(a), Value::Array(b)) => {
            // Order-insensitive containment: every element of `sub` must be a
            // *subset* of some element of `sup` (handles connector `oneOf`).
            //
            // Subset, not deep equality — that was the bug this comment used
            // to describe wrongly. The test's whole premise is that a smaller
            // feature set than the one the file was generated under still
            // passes, but an equality check fails the moment the committed
            // node carries one extra feature-gated field (e.g. BigQuery's
            // arrow-only `bulk_load`), which is exactly the situation the
            // subset check exists for.
            for (i, va) in a.iter().enumerate() {
                if !b.iter().any(|vb| is_subset(va, vb, String::new()).is_ok()) {
                    return Err(format!("{path}/{i}"));
                }
            }
            Ok(())
        }
        _ if sub == sup => Ok(()),
        _ => Err(path),
    }
}

/// Regenerate `schemas/faucet.schema.json` from **this test binary's** view of
/// the composed schema.
///
/// Ignored by default, so it never runs in CI. It exists because the binary's
/// `faucet schema config` output and the test binary's `config_schema()` can
/// differ: a test build unifies features across dev-dependencies, so a
/// hand-run `cargo run -- schema config` can emit a field the checked build
/// does not, and the committed file then fails the very test it is meant to
/// satisfy. Run with:
///
/// ```text
/// cargo test -p faucet-cli --all-features --test schema_config -- --ignored regenerate
/// ```
#[test]
#[ignore]
fn regenerate_committed_schema() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../schemas/faucet.schema.json");
    let mut text = serde_json::to_string_pretty(&config_schema()).expect("serialize");
    text.push('\n');
    std::fs::write(path, text).expect("write the committed schema");
}
