#![no_main]

use libfuzzer_sys::fuzz_target;

#[allow(dead_code)]
#[path = "../../crates/source/mysql-cdc/src/query.rs"]
mod query;

// The binlog statement and XA-identifier decoders: bytes from the replication
// stream must decode or be rejected, never panic.
fuzz_target!(|data: &[u8]| {
    let _ = query::parse_xa_prepare(data);
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let (statement, schema) = text.split_once('\0').unwrap_or((text, "db"));
    let _ = query::classify_query(statement, schema);
    if let Some(xid) = query::parse_query_xid(statement) {
        let _ = xid.to_string();
    }
});
