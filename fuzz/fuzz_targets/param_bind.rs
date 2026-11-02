#![no_main]

use faucet_cli::params::{BindMode, SuppliedParams, bind_document};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

// Text before the first NUL is the config document; text after it is the
// supplied-params JSON object.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else {
        return;
    };
    let Ok(text) = std::str::from_utf8(rest) else {
        return;
    };
    let (doc_text, supplied_text) = text.split_once('\0').unwrap_or((text, "{}"));
    let doc: Value = if selector & 1 == 0 {
        match faucet_cli::yaml::from_str(doc_text) {
            Ok(v) => v,
            Err(_) => return,
        }
    } else {
        match serde_json::from_str(doc_text) {
            Ok(v) => v,
            Err(_) => return,
        }
    };
    let supplied: SuppliedParams = serde_json::from_str(supplied_text).unwrap_or_default();
    let mode = if selector & 2 == 0 {
        BindMode::Strict
    } else {
        BindMode::Placeholder
    };
    let mut doc = doc;
    if let Ok(bound) = bind_document(&mut doc, &supplied, mode) {
        let _ = bound.redacted();
        let _ = bound.secret_texts();
    }
});
