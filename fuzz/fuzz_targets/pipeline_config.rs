#![no_main]

use std::path::Path;

use faucet_cli::config::PipelineConfig;
use faucet_cli::expand::{check_sharding, expand};
use libfuzzer_sys::fuzz_target;

// The parse and expansion of already-interpolated text: `${env:}` / `${file:}`
// and `extends` / `!include` composition read the host, so they are not driven.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else {
        return;
    };
    let Ok(text) = std::str::from_utf8(rest) else {
        return;
    };
    let path = if selector & 1 == 0 {
        Path::new("pipeline.yaml")
    } else {
        Path::new("pipeline.json")
    };
    let Ok(cfg) = PipelineConfig::from_text(text, path) else {
        return;
    };
    if let Ok(nodes) = expand(&cfg) {
        let _ = check_sharding(&cfg, &nodes);
    }
});
