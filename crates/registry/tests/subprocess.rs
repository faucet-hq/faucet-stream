//! A connector that runs a program is built only with an explicit opt-in.

#![cfg(feature = "source-singer")]

use faucet_registry::{
    AuthCatalog, BuildPolicy, RegistryError, build_source, build_source_with, source_runs_program,
};
use serde_json::json;

#[tokio::test]
async fn the_singer_tap_bridge_needs_allow_subprocess() {
    let cfg = json!({ "executable": "tap-example", "stream": "s" });
    let auth = AuthCatalog::new();
    assert!(source_runs_program("singer"));
    assert!(!source_runs_program("no-such-kind"));
    match build_source("singer", cfg.clone(), &auth, None).await {
        Err(e @ RegistryError::SubprocessRefused { .. }) => {
            assert!(e.to_string().contains("allow_subprocess"), "{e}")
        }
        other => panic!("expected SubprocessRefused, got {:?}", other.map(|_| ())),
    }
    let policy = BuildPolicy::default().allow_subprocess(true);
    let source = build_source_with("singer", cfg, &auth, None, &policy)
        .await
        .unwrap();
    assert_eq!(source.connector_name(), "singer");
}
