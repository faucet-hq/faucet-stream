//! The hand-written reference module `examples/wasm-transforms/add_field.wat`,
//! loaded from its text form, behaves like the Rust reference module.

use faucet_core::stage::TransformStage;
use faucet_transform_wasm::{WasmTransform, WasmTransformConfig};
use serde_json::{Value, json};

const ADD_FIELD_WAT: &str = include_str!("../../../examples/wasm-transforms/add_field.wat");

fn run(records: Vec<Value>) -> Result<Vec<Value>, faucet_core::FaucetError> {
    let dir = tempfile::tempdir().unwrap();
    let module = dir.path().join("add_field.wat");
    std::fs::write(&module, ADD_FIELD_WAT).unwrap();
    let cfg: WasmTransformConfig =
        serde_json::from_value(json!({ "module": module.to_str().unwrap() })).unwrap();
    let TransformStage::PageFn(page) = WasmTransform::compile(&cfg).unwrap().into_page_stage()
    else {
        panic!("the wasm transform is a page stage");
    };
    page(records)
}

#[test]
fn uppercases_name_and_stamps_the_flag() {
    let out = run(vec![
        json!({"name": "alice", "id": "1"}),
        json!({"id": 2, "name": "bob-9 x"}),
    ])
    .unwrap();
    assert_eq!(
        out[0],
        json!({"name": "ALICE", "id": "1", "wasm_processed": true})
    );
    assert_eq!(
        out[1],
        json!({"id": 2, "name": "BOB-9 X", "wasm_processed": true})
    );
}

#[test]
fn only_the_top_level_name_string_changes() {
    let out = run(vec![
        json!({"user": {"name": "inner"}, "names": ["name", "x"], "label": "name"}),
        json!({"name": 7, "other": "lower"}),
        json!({"name": {"first": "a"}}),
        json!({"name": "tab\tand \"quoted\" \\n é", "x": "keep"}),
    ])
    .unwrap();
    assert_eq!(
        out[0],
        json!({"user": {"name": "inner"}, "names": ["name", "x"], "label": "name", "wasm_processed": true})
    );
    assert_eq!(
        out[1],
        json!({"name": 7, "other": "lower", "wasm_processed": true})
    );
    assert_eq!(
        out[2],
        json!({"name": {"first": "a"}, "wasm_processed": true})
    );
    assert_eq!(
        out[3],
        json!({"name": "TAB\tAND \"QUOTED\" \\N é", "x": "keep", "wasm_processed": true})
    );
}

#[test]
fn empty_and_existing_flag() {
    let out = run(vec![json!({}), json!({"wasm_processed": false, "a": 1})]).unwrap();
    assert_eq!(out[0], json!({"wasm_processed": true}));
    assert_eq!(out[1], json!({"wasm_processed": true, "a": 1}));
}

#[test]
fn a_non_object_record_is_an_error() {
    let err = run(vec![json!(["not", "an", "object"])]).unwrap_err();
    assert!(err.to_string().contains("expected a JSON object"), "{err}");
}

#[test]
fn many_records_reuse_memory() {
    let records: Vec<Value> = (0..5000)
        .map(|i| json!({"name": format!("user{i}"), "pad": "x".repeat(2000)}))
        .collect();
    let out = run(records).unwrap();
    assert_eq!(out.len(), 5000);
    assert_eq!(out[4999]["name"], "USER4999");
}
