//! End-to-end Vault test. Gated on `VAULT_TEST=1` so CI without a Vault
//! container skips it. To run locally:
//!   docker run -d --cap-add=IPC_LOCK -e VAULT_DEV_ROOT_TOKEN_ID=root \
//!     -p 8200:8200 hashicorp/vault
//!   export VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root VAULT_TEST=1
//!   vault kv put secret/faucet/api token=live-token-value
//!   cargo test -p faucet-cli --features secrets-vault --test secrets_vault -- --ignored

#[tokio::test]
#[ignore = "requires a live Vault (set VAULT_TEST=1 and run with --ignored)"]
async fn resolves_vault_secret_end_to_end() {
    if std::env::var("VAULT_TEST").as_deref() != Ok("1") {
        assert!(
            std::env::var_os("FAUCET_REQUIRE_BACKENDS").is_none(),
            "VAULT_TEST is unset and FAUCET_REQUIRE_BACKENDS is set"
        );
        return;
    }
    use faucet_cli::config::PipelineConfig;
    let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x, auth: { type: bearer, config: { token: "${vault:secret/data/faucet/api#token}" } } } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, yaml).unwrap();
    let cfg = PipelineConfig::from_path_async(&path, None).await.unwrap();
    let token = &cfg.pipeline.source.unwrap().config["auth"]["config"]["token"];
    assert_eq!(token, "live-token-value");
}
