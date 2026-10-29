#![no_main]

use libfuzzer_sys::fuzz_target;

#[allow(dead_code)]
#[path = "../../crates/source/postgres-cdc/src/pgoutput/mod.rs"]
mod pgoutput;

use pgoutput::decoder::decode_message;

// The decoder reads bytes straight off the replication stream: any input must
// decode or fail with an error, never panic or allocate without bound.
fuzz_target!(|data: &[u8]| {
    let _ = decode_message(data);
});
