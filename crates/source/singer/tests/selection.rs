//! End-to-end catalog-selection test: run the fake tap's discovery, then apply
//! `select_streams` and assert the target stream *and* its inferred parent are
//! marked selected — the behaviour DB / parent-keyed taps require.

use faucet_source_singer::{SingerSourceConfig, discover, select_streams};

fn fake_tap() -> String {
    format!("{}/tests/fake_taps/fake_tap.sh", env!("CARGO_MANIFEST_DIR"))
}

fn is_selected(catalog: &serde_json::Value, id: &str) -> bool {
    catalog["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["tap_stream_id"] == id)
        .and_then(|s| s["metadata"].as_array())
        .map(|md| {
            md.iter().any(|m| {
                m["breadcrumb"]
                    .as_array()
                    .map(|b| b.is_empty())
                    .unwrap_or(false)
                    && m["metadata"]["selected"] == serde_json::json!(true)
            })
        })
        .unwrap_or(false)
}

#[tokio::test]
async fn discovery_selects_child_and_its_parent() {
    let cfg = SingerSourceConfig {
        args: vec!["--parent-child".into()],
        ..SingerSourceConfig::new(fake_tap(), "issues")
    };
    let catalog = discover(&cfg).await.expect("discover");

    // The raw discovered catalog selects nothing.
    assert!(!is_selected(&catalog, "issues"));

    let sel = select_streams(&catalog, "issues");
    assert!(
        sel.selected.contains(&"issues".to_string())
            && sel.selected.contains(&"repositories".to_string()),
        "expected both issues and its parent repositories to be selected, got {:?}",
        sel.selected
    );
    assert!(is_selected(&sel.catalog, "issues"));
    assert!(is_selected(&sel.catalog, "repositories"));
    // The parent relationship was inferable, so there is no manual-selection
    // warning.
    assert!(sel.warnings.is_empty(), "warnings: {:?}", sel.warnings);
}

#[test]
fn a_target_named_by_its_record_stream_name_is_selected() {
    let catalog = serde_json::json!({"streams": [
        {"tap_stream_id": "public-orders", "stream": "orders", "metadata": [
            {"breadcrumb": [], "metadata": {"parent": "customers"}}
        ]},
        {"tap_stream_id": "public-customers", "stream": "customers",
         "metadata": [{"breadcrumb": [], "metadata": {}}]},
    ]});
    let sel = select_streams(&catalog, "orders");
    assert!(sel.warnings.is_empty(), "warnings: {:?}", sel.warnings);
    assert!(is_selected(&sel.catalog, "public-orders"));
    assert!(is_selected(&sel.catalog, "public-customers"));
}
