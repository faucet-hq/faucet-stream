#![no_main]

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use tokio::runtime::Runtime;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
});

fn step(code: u8) -> Value {
    match code % 10 {
        0 => json!("base64"),
        1 => json!("gunzip"),
        2 => json!({ "unzip": {} }),
        3 => json!({ "unzip": { "member": "*.csv" } }),
        4 => json!({ "extract": "$.d" }),
        5 => json!({ "parse": { "format": "csv" } }),
        6 => json!({ "parse": { "format": "csv", "delimiter": 59, "has_headers": false } }),
        7 => json!({ "parse": { "format": "xml", "records_path": "$.r[*]" } }),
        8 => json!({ "parse": { "format": "xlsx", "header_row": 1 } }),
        _ => json!({ "parse": { "format": "json", "records_path": "$.r[*]" } }),
    }
}

// First byte: number of chain steps (0-3); the next bytes pick each step; the
// rest is the response body. The same chain runs through the REST and the
// XML source decoders.
fuzz_target!(|data: &[u8]| {
    let Some((&count, rest)) = data.split_first() else {
        return;
    };
    let count = usize::from(count % 4);
    if rest.len() < count {
        return;
    }
    let (codes, body) = rest.split_at(count);
    let steps = Value::Array(codes.iter().map(|c| step(*c)).collect());
    let rest_steps: Vec<faucet_source_rest::DecodeStep> =
        serde_json::from_value(steps.clone()).expect("a REST decode chain");
    let xml_steps: Vec<faucet_source_xml::DecodeStep> =
        serde_json::from_value(steps).expect("an XML decode chain");
    RT.block_on(async {
        let _ = faucet_source_rest::decode::run_decode(body, &rest_steps).await;
        let _ = faucet_source_xml::run_decode(body, &xml_steps).await;
    });
});
