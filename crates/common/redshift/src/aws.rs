//! AWS SDK configuration for the `iam` and `redshift_data_api` methods.

use crate::config::RedshiftAwsCredentials;
use faucet_core::{FaucetError, Value};

/// Whether the process environment names an AWS region (`AWS_REGION` or
/// `AWS_DEFAULT_REGION`), so a client whose config names none does not ask
/// the instance metadata service for one.
pub fn region_from_env() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

/// The config fields that make a Redshift connection authenticate with the
/// identity of the host it runs on: for `iam` and `redshift_data_api`,
/// `aws_credentials` of any variant but `access_key` (absent = `default`),
/// and `region` when neither the config nor the process environment names
/// one. `password` uses none.
pub fn host_identity(config: &Value) -> Vec<String> {
    use faucet_core::capabilities::config::{non_empty_str_at, tag_at};
    let mut fields = Vec::new();
    if !matches!(
        tag_at(config, "/credentials"),
        Some("iam" | "redshift_data_api")
    ) {
        return fields;
    }
    if tag_at(config, "/credentials/config/aws_credentials").unwrap_or("default") != "access_key" {
        fields.push("credentials.config.aws_credentials".to_string());
    }
    if !non_empty_str_at(config, "/credentials/config/region") && !region_from_env() {
        fields.push("credentials.config.region".to_string());
    }
    fields
}

fn check_host_identity(
    region: Option<&str>,
    credentials: &RedshiftAwsCredentials,
) -> Result<(), FaucetError> {
    if !matches!(credentials, RedshiftAwsCredentials::AccessKey { .. }) {
        faucet_core::resolve::require_host_credentials("redshift", "aws_credentials")?;
    }
    if region.is_none_or(|r| r.trim().is_empty()) && !region_from_env() {
        faucet_core::resolve::require_host_credentials("redshift", "region")?;
    }
    Ok(())
}

/// Load the SDK configuration from region and credentials. `region: None`
/// defers to the SDK default chain; credential resolution is delegated to
/// `aws-config`, so rotating credentials refresh on their own. Refused when
/// the task may not use the host's identity and these would.
pub(crate) async fn sdk_config(
    region: Option<&str>,
    credentials: &RedshiftAwsCredentials,
) -> Result<aws_config::SdkConfig, FaucetError> {
    check_host_identity(region, credentials)?;
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region.to_owned()));
    }
    match credentials {
        RedshiftAwsCredentials::Default | RedshiftAwsCredentials::WebIdentity => {}
        RedshiftAwsCredentials::Profile { name } => {
            loader = loader.profile_name(name);
        }
        RedshiftAwsCredentials::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            loader = loader.credentials_provider(aws_sdk_redshiftdata::config::Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
                None,
                "faucet-config",
            ));
        }
        RedshiftAwsCredentials::AssumeRole {
            role_arn,
            session_name,
            external_id,
        } => {
            let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn)
                .session_name(session_name.as_deref().unwrap_or("faucet-stream"));
            if let Some(region) = region {
                builder = builder.region(aws_config::Region::new(region.to_owned()));
            }
            if let Some(external_id) = external_id {
                builder = builder.external_id(external_id);
            }
            loader = loader.credentials_provider(builder.build().await);
        }
    }
    Ok(loader.load().await)
}

/// An AWS SDK error with its whole source chain, for an error message.
pub(crate) fn describe<E: std::error::Error>(e: &E) -> String {
    let mut out = e.to_string();
    let mut next = e.source();
    while let Some(cause) = next {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        next = cause.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let no_region = |c: Value| -> Vec<String> {
            host_identity(&c)
                .into_iter()
                .filter(|f| !f.ends_with("region"))
                .collect()
        };
        assert!(
            host_identity(
                &json!({"credentials": {"type": "password", "config": {"password": "p"}}})
            )
            .is_empty()
        );
        assert!(host_identity(&json!({})).is_empty());
        for t in ["iam", "redshift_data_api"] {
            assert_eq!(
                no_region(json!({"credentials": {"type": t, "config": {"region": "us-east-1"}}})),
                vec!["credentials.config.aws_credentials"]
            );
            assert_eq!(
                no_region(json!({"credentials": {"type": t, "config": {
                    "aws_credentials": {"type": "profile", "config": {"name": "p"}}}}})),
                vec!["credentials.config.aws_credentials"]
            );
            let keyed = json!({"credentials": {"type": t, "config": {"region": "us-east-1",
                "aws_credentials": {"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}}}});
            assert!(host_identity(&keyed).is_empty());
            let unregioned = json!({"credentials": {"type": t, "config": {
                "aws_credentials": {"type": "access_key"}}}});
            assert_eq!(host_identity(&unregioned).is_empty(), region_from_env());
        }
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        faucet_core::resolve::with_host_credentials(false, async {
            let err = sdk_config(Some("us-east-1"), &RedshiftAwsCredentials::Default)
                .await
                .expect_err("refused");
            assert!(
                err.to_string().contains("redshift: `aws_credentials`"),
                "{err}"
            );
            let keys = RedshiftAwsCredentials::AccessKey {
                access_key_id: "a".into(),
                secret_access_key: "s".into(),
                session_token: None,
            };
            assert!(sdk_config(Some("us-east-1"), &keys).await.is_ok());
            assert_eq!(sdk_config(None, &keys).await.is_ok(), region_from_env());
        })
        .await;
        assert!(
            sdk_config(Some("us-east-1"), &RedshiftAwsCredentials::Default)
                .await
                .is_ok()
        );
    }

    #[derive(Debug)]
    struct Outer(Inner);
    #[derive(Debug)]
    struct Inner;
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "service error")
        }
    }
    impl std::fmt::Display for Inner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ValidationException: bad sql")
        }
    }
    impl std::error::Error for Inner {}
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn describe_joins_the_source_chain() {
        assert_eq!(
            describe(&Outer(Inner)),
            "service error: ValidationException: bad sql"
        );
    }
}
