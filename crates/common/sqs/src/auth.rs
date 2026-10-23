//! Credential configuration + client construction shared by the SQS source
//! and sink.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with AWS SQS.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum SqsCredentials {
    /// The AWS SDK default provider chain: environment variables, shared
    /// config/credentials files, ECS/EKS container credentials, web-identity
    /// tokens (`AWS_WEB_IDENTITY_TOKEN_FILE` / EKS IRSA), and EC2 instance
    /// profiles — with automatic refresh/rotation.
    #[default]
    Default,
    /// A named profile from the shared AWS config/credentials files.
    Profile {
        /// Profile name (as in `~/.aws/credentials`).
        name: String,
    },
    /// Static access keys. Prefer `${env:…}` / secrets-manager interpolation
    /// over literals in config files.
    AccessKey {
        /// AWS access key id.
        access_key_id: String,
        /// AWS secret access key.
        secret_access_key: String,
        /// Optional session token (for temporary credentials).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<String>,
    },
    /// Assume an IAM role via STS on top of the default provider chain.
    AssumeRole {
        /// ARN of the role to assume.
        role_arn: String,
        /// Session name recorded in CloudTrail. Defaults to `faucet-stream`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_name: Option<String>,
        /// Optional external id for cross-account trust policies.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        external_id: Option<String>,
    },
    /// Web-identity federation (e.g. EKS IRSA). Equivalent to the `default`
    /// chain, which honors `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` —
    /// kept as an explicit variant so intent is visible in configs.
    WebIdentity,
}

impl std::fmt::Debug for SqsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default"),
            Self::Profile { name } => f.debug_struct("Profile").field("name", name).finish(),
            // Never print key material.
            Self::AccessKey { access_key_id, .. } => f
                .debug_struct("AccessKey")
                .field("access_key_id", access_key_id)
                .field("secret_access_key", &"***")
                .finish(),
            Self::AssumeRole { role_arn, .. } => f
                .debug_struct("AssumeRole")
                .field("role_arn", role_arn)
                .finish(),
            Self::WebIdentity => write!(f, "WebIdentity"),
        }
    }
}

/// Build an `aws_sdk_sqs::Client` from region / endpoint / credentials.
///
/// `region: None` defers to the SDK default chain (env, profile, IMDS);
/// `endpoint_url` overrides the endpoint for LocalStack / VPC endpoints.
/// Credential resolution is delegated to `aws-config`, so rotating
/// credentials (web identity, instance profiles, assumed roles) refresh
/// automatically — the client never caches static credentials itself.
pub async fn build_client(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: &SqsCredentials,
) -> Result<aws_sdk_sqs::Client, FaucetError> {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region.to_owned()));
    }
    match credentials {
        SqsCredentials::Default | SqsCredentials::WebIdentity => {}
        SqsCredentials::Profile { name } => {
            loader = loader.profile_name(name);
        }
        SqsCredentials::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            let creds = aws_sdk_sqs::config::Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
                None,
                "faucet-config",
            );
            loader = loader.credentials_provider(creds);
        }
        SqsCredentials::AssumeRole {
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
    let sdk_config = loader.load().await;
    let mut sqs_config = aws_sdk_sqs::config::Builder::from(&sdk_config);
    if let Some(endpoint) = endpoint_url {
        sqs_config = sqs_config.endpoint_url(endpoint);
    }
    Ok(aws_sdk_sqs::Client::from_conf(sqs_config.build()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_parse_the_consistent_wire_shape() {
        let yaml = "type: default\n";
        let c: SqsCredentials = serde_yaml_bw::from_str(yaml).unwrap();
        assert!(matches!(c, SqsCredentials::Default));

        let yaml = "type: profile\nconfig: { name: prod }\n";
        let c: SqsCredentials = serde_yaml_bw::from_str(yaml).unwrap();
        assert!(matches!(c, SqsCredentials::Profile { name } if name == "prod"));

        let yaml =
            "type: access_key\nconfig:\n  access_key_id: AKIA\n  secret_access_key: s3cr3t\n";
        let c: SqsCredentials = serde_yaml_bw::from_str(yaml).unwrap();
        match &c {
            SqsCredentials::AccessKey {
                access_key_id,
                session_token,
                ..
            } => {
                assert_eq!(access_key_id, "AKIA");
                assert!(session_token.is_none());
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Debug never leaks the secret.
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("s3cr3t"), "{dbg}");

        let yaml = "type: assume_role\nconfig: { role_arn: 'arn:aws:iam::1:role/x' }\n";
        let c: SqsCredentials = serde_yaml_bw::from_str(yaml).unwrap();
        assert!(matches!(c, SqsCredentials::AssumeRole { .. }));

        let yaml = "type: web_identity\n";
        let c: SqsCredentials = serde_yaml_bw::from_str(yaml).unwrap();
        assert!(matches!(c, SqsCredentials::WebIdentity));
    }

    #[test]
    fn default_is_default() {
        assert!(matches!(SqsCredentials::default(), SqsCredentials::Default));
    }

    #[tokio::test]
    async fn build_client_honors_endpoint_and_static_keys() {
        // Offline: constructing the client performs no network I/O.
        let creds = SqsCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        let client = build_client(Some("us-east-1"), Some("http://127.0.0.1:4566"), &creds)
            .await
            .expect("client builds");
        assert_eq!(
            client.config().region().map(|r| r.as_ref()),
            Some("us-east-1")
        );

        // Every other variant also constructs offline (no network I/O).
        for creds in [
            SqsCredentials::Default,
            SqsCredentials::WebIdentity,
            SqsCredentials::Profile {
                name: "no-such-profile".into(),
            },
            SqsCredentials::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/x".into(),
                session_name: Some("t".into()),
                external_id: Some("e".into()),
            },
        ] {
            build_client(Some("us-east-1"), None, &creds)
                .await
                .expect("client builds offline");
        }
    }
}
