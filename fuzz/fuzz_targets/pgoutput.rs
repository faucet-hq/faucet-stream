#![no_main]

use faucet_source_postgres_cdc::pgoutput::decoder::decode_message;
use libfuzzer_sys::fuzz_target;

// The decoder reads bytes straight off the replication stream: any input must
// decode or fail with an error, never panic or allocate without bound.
fuzz_target!(|data: &[u8]| {
    let _ = decode_message(data);
});
