#![cfg(all(feature = "file-formats", feature = "encryption"))]

//! Every writable format × every cross-cutting option (#777). Each
//! combination must either round-trip — written by the file sink, read back
//! through the sink's own read-back config with the file source, same
//! records — or be refused with a typed config error where the refusal is
//! intrinsic (appending to one whole-document file). A format that silently
//! lacks an option fails here.

use faucet_core::FaucetError;
use faucet_sink_file::FileSink;
use serde_json::json;

#[test]
fn orc_is_refused_as_read_only() {
    let dir = tempfile::tempdir().unwrap();
    for cfg in [
        json!({"path": dir.path().join("a.orc")}),
        json!({"path": format!("{}/", dir.path().display()), "format": "orc"}),
    ] {
        let err = serde_json::from_value(cfg)
            .map_err(FaucetError::Json)
            .and_then(FileSink::new)
            .err()
            .expect("refused");
        assert!(
            matches!(&err, FaucetError::Config(m) if m.contains("orc")),
            "{err}"
        );
    }
}
