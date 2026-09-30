#![cfg(feature = "file-formats")]

//! File sink construction checks. The end-to-end runs that read the output
//! back with the file source live in
//! `crates/interop-tests/tests/file_sink_integration.rs`.

use faucet_sink_file::{FileSink, FileSinkConfig};
use serde_json::json;

#[tokio::test]
async fn refusals_at_construction() {
    for (cfg, needle) in [
        (json!({"path": "x.orc"}), "read-only"),
        (json!({"path": "x.bin"}), "x.bin"),
        (json!({"path": "x.xlsx", "mode": "append"}), "append"),
    ] {
        let c: FileSinkConfig = serde_json::from_value(cfg.clone()).unwrap();
        let e = FileSink::new(c).err().unwrap().to_string();
        assert!(e.contains(needle), "{cfg}: {e}");
    }
    let gz: FileSinkConfig =
        serde_json::from_value(json!({"path": "x.parquet", "compression": "gzip"})).unwrap();
    assert!(
        FileSink::new(gz).is_ok(),
        "a compressed parquet file is written, not refused"
    );
}
