//! The Connector Hub export (#572) keeps its documented JSON shape and lists
//! every connector in the registry index.

use faucet_cli::connector_export::{EXPORT_FORMAT, EXPORT_VERSION, build_export};
use faucet_cli::registry_index::RegistryIndex;
use serde_json::Value;

fn export_json() -> Value {
    serde_json::to_value(build_export(&RegistryIndex::embedded())).unwrap()
}

#[test]
fn top_level_shape_is_stable() {
    let doc = export_json();
    let keys: Vec<&str> = doc
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    for k in [
        "format",
        "version",
        "faucet_version",
        "categories",
        "tiers",
        "connectors",
    ] {
        assert!(keys.contains(&k), "missing `{k}`");
    }
    assert_eq!(doc["format"], EXPORT_FORMAT);
    assert_eq!(doc["version"], EXPORT_VERSION);
    assert_eq!(doc["tiers"].as_array().unwrap().len(), 4);
    assert!(
        doc["categories"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["id"].is_string() && c["label"].is_string())
    );
}

#[test]
fn every_registry_connector_appears_with_the_documented_fields() {
    let doc = export_json();
    let rows = doc["connectors"].as_array().unwrap();
    let index = RegistryIndex::embedded();
    for e in &index.connectors {
        let id = format!("{}-{}", e.kind, e.name);
        let row = rows
            .iter()
            .find(|r| r["id"] == id.as_str())
            .unwrap_or_else(|| panic!("{id} missing"));
        for k in [
            "name",
            "kind",
            "title",
            "category",
            "description",
            "verified",
            "crate",
            "feature",
            "keywords",
            "repository_path",
            "docs_url",
            "crates_io_url",
            "compiled",
            "conformance",
            "capabilities",
            "config_fields",
            "config_schema",
            "init_snippet",
        ] {
            assert!(row.get(k).is_some(), "{id}: missing `{k}`");
        }
        assert!(row["conformance"]["score"].is_u64(), "{id}");
        assert!(row["conformance"]["tier"].is_string(), "{id}");
        assert!(row["capabilities"]["delivery"].is_string(), "{id}");
        if row["compiled"] == true {
            assert!(row["config_schema"].is_object(), "{id}");
            assert!(
                row["init_snippet"]
                    .as_str()
                    .unwrap()
                    .contains(&format!("type: {}", e.name)),
                "{id}"
            );
            for f in row["config_fields"].as_array().unwrap() {
                for k in ["name", "type", "required", "default", "description"] {
                    assert!(f.get(k).is_some(), "{id}: field missing `{k}`");
                }
            }
        }
    }
    assert_eq!(rows.len(), index.connectors.len());
}
