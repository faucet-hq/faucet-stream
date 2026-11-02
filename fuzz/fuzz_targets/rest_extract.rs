#![no_main]

use std::collections::HashMap;

use faucet_source_rest::config::RecordsMultiSpec;
use faucet_source_rest::extract::{extract_configured, extract_multi, extract_records};
use libfuzzer_sys::fuzz_target;
use serde_json::json;

// NUL-separated: response body, records path, ancestor-relative path.
fuzz_target!(|data: &str| {
    let mut parts = data.splitn(3, '\0');
    let body_text = parts.next().unwrap_or_default();
    let path = parts.next();
    let relative = parts.next().unwrap_or_default();
    let Ok(body) = faucet_core::fidelity::from_str(body_text) else {
        return;
    };
    let _ = extract_records(&body, path);
    let Some(path) = path else {
        return;
    };
    let ancestors = HashMap::from([("a".to_owned(), relative.to_owned())]);
    let _ = extract_configured(&body, Some(path), Some(&ancestors), &[], "_op");
    let multi: Vec<RecordsMultiSpec> = serde_json::from_value(
        json!([{ "path": path, "op": "u" }, { "path": relative, "op": "d" }]),
    )
    .expect("a records_multi list");
    let _ = extract_multi(&body, &multi, "_op");
});
