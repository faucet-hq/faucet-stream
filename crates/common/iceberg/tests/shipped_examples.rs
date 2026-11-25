//! Every `catalog.properties` name in a shipped Iceberg example is one a
//! catalog client reads: a warning there would be a defect.

use std::collections::HashMap;

use serde_json::Value;

fn catalogs(v: &Value, out: &mut Vec<(String, HashMap<String, String>)>) {
    match v {
        Value::Object(map) => {
            if let Some(Value::Object(c)) = map.get("catalog")
                && let Some(Value::String(kind)) = c.get("type")
            {
                let props = c
                    .get("properties")
                    .and_then(Value::as_object)
                    .map(|p| p.iter().map(|(k, v)| (k.clone(), v.to_string())).collect())
                    .unwrap_or_default();
                out.push((kind.clone(), props));
            }
            map.values().for_each(|v| catalogs(v, out));
        }
        Value::Array(items) => items.iter().for_each(|v| catalogs(v, out)),
        _ => {}
    }
}

#[test]
fn no_shipped_iceberg_example_names_a_property_no_catalog_reads() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../cli/examples");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !(name.contains("iceberg") && name.ends_with(".yaml")) {
            continue;
        }
        let doc: Value = yaml_serde::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let mut found = Vec::new();
        catalogs(&doc, &mut found);
        for (kind, props) in found {
            checked += 1;
            let unread = faucet_common_iceberg::config::unread_properties(&kind, &props);
            assert!(unread.is_empty(), "{name}: {unread:?}");
        }
    }
    assert!(checked >= 4, "{checked}");
}
