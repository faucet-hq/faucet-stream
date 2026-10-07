//! AWS Secrets Manager test against LocalStack. Gated on `AWS_SM_TEST=1`.
//! Local setup:
//!   docker run -d -p 4566:4566 localstack/localstack
//!   export AWS_ENDPOINT_URL=http://127.0.0.1:4566 AWS_ACCESS_KEY_ID=test \
//!     AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_SM_TEST=1
//!   aws --endpoint-url=$AWS_ENDPOINT_URL secretsmanager create-secret \
//!     --name prod/faucet --secret-string '{"token":"aws-live-token"}'
//!   cargo test -p faucet-cli --features secrets-aws-sm --test secrets_aws_sm -- --ignored

#[tokio::test]
#[ignore = "requires LocalStack (set AWS_SM_TEST=1 and run with --ignored)"]
async fn resolves_aws_secret_field_end_to_end() {
    if std::env::var("AWS_SM_TEST").as_deref() != Ok("1") {
        assert!(
            std::env::var_os("FAUCET_REQUIRE_BACKENDS").is_none(),
            "AWS_SM_TEST is unset and FAUCET_REQUIRE_BACKENDS is set"
        );
        return;
    }
    use faucet_cli::config::PipelineConfig;
    let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x, auth: { type: bearer, config: { token: "${aws-sm:prod/faucet#token}" } } } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.yaml");
    std::fs::write(&path, yaml).unwrap();
    let cfg = PipelineConfig::from_path_async(&path, None).await.unwrap();
    let token = &cfg.pipeline.source.unwrap().config["auth"]["config"]["token"];
    assert_eq!(token, "aws-live-token");
}
