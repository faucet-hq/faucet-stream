//! Descriptor conformance for the Singer target bridge: building the sink only
//! validates its config, so no target process is spawned here.

use faucet_sink_singer::{SingerSink, SingerSinkConfig};
use serde_json::json;

#[test]
fn conformance_descriptor_matches_built_sink() {
    for v in [
        json!({"target_command": "target-x"}),
        json!({"target_command": "target-x", "write_mode": "upsert", "key": ["id"]}),
        json!({"target_command": "target-x", "write_mode": "overwrite"}),
    ] {
        let cfg: SingerSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SingerSink::new(cfg).unwrap();
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_singer::DESCRIPTOR,
            "singer",
            &v,
            &built,
        );
    }
}
