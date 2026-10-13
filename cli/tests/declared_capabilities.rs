//! RFC 0013 (#878): `crates/conformance/declared-capabilities.json` is the
//! snapshot `scripts/conformance-coverage.py` reads in CI's `Doc counts` job,
//! which builds no Rust. It holds the required-assertion table
//! (`faucet_conformance::required`) and every built-in connector's kind-level
//! capabilities. This test fails when the committed copy drifts.
//!
//! Regenerate (the build compiles every built-in connector):
//!   cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate
//! A slimmer build updates only the connectors it compiles and keeps the rest.

use std::collections::BTreeMap;

use faucet_cli::registry;
use faucet_cli::registry_index::RegistryIndex;
use serde_json::{Value, json};

const PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../crates/conformance/declared-capabilities.json"
);
const COMMITTED: &str = include_str!("../../crates/conformance/declared-capabilities.json");

fn every_builtin_compiled() -> bool {
    RegistryIndex::embedded()
        .connectors
        .iter()
        .filter(|c| c.verified)
        .all(|c| match c.kind.as_str() {
            "source" => registry::source_exists(&c.name),
            "sink" => registry::sink_exists(&c.name),
            _ => true,
        })
}

fn compiled(role: &str) -> BTreeMap<String, Value> {
    let kinds = if role == "sources" {
        registry::source_kinds()
    } else {
        registry::sink_kinds()
    };
    kinds
        .into_iter()
        .map(|k| {
            let caps = if role == "sources" {
                registry::source_kind_capabilities(k)
            } else {
                registry::sink_kind_capabilities(k)
            };
            (k.to_string(), serde_json::to_value(caps).unwrap())
        })
        .collect()
}

fn render(sources: BTreeMap<String, Value>, sinks: BTreeMap<String, Value>) -> String {
    let doc = json!({
        "_generated": "cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate",
        "requirements": faucet_conformance::required::requirements_json(),
        "sources": sources,
        "sinks": sinks,
    });
    format!("{}\n", serde_json::to_string_pretty(&doc).unwrap())
}

fn committed_role(doc: &Value, role: &str) -> BTreeMap<String, Value> {
    doc[role]
        .as_object()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

#[test]
fn committed_snapshot_matches_the_declarations() {
    let hint = "crates/conformance/declared-capabilities.json is stale — regenerate it:\n  \
         cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate";
    if every_builtin_compiled() {
        assert_eq!(
            COMMITTED,
            render(compiled("sources"), compiled("sinks")),
            "{hint}"
        );
        return;
    }
    let doc: Value = serde_json::from_str(COMMITTED).expect("snapshot is JSON");
    assert_eq!(
        doc["requirements"],
        faucet_conformance::required::requirements_json(),
        "{hint}"
    );
    for role in ["sources", "sinks"] {
        let committed = committed_role(&doc, role);
        for (kind, caps) in compiled(role) {
            assert_eq!(committed.get(&kind), Some(&caps), "{hint}\n{role}.{kind}");
        }
    }
}

#[test]
#[ignore = "writes the snapshot; run explicitly to regenerate"]
fn regenerate() {
    let mut sources = BTreeMap::new();
    let mut sinks = BTreeMap::new();
    if !every_builtin_compiled() {
        let doc: Value = serde_json::from_str(COMMITTED).unwrap_or(Value::Null);
        sources = committed_role(&doc, "sources");
        sinks = committed_role(&doc, "sinks");
    }
    sources.extend(compiled("sources"));
    sinks.extend(compiled("sinks"));
    std::fs::write(PATH, render(sources, sinks)).expect("write snapshot");
}
