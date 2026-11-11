//! Credential configuration + client construction shared by the DynamoDB
//! source and sink.

use faucet_core::{FaucetError, Value};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with AWS DynamoDB (and DynamoDB Streams).
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the same shape as the Kinesis and SQS
/// connectors, so one AWS credential block reads identically everywhere.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum DynamoDbCredentials {
    /// The AWS SDK default provider chain: environment variables, shared
    /// config/credentials files, ECS/EKS container credentials, web-identity
    /// tokens, and EC2 instance profiles — with automatic refresh/rotation.
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

impl std::fmt::Debug for DynamoDbCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default"),
            Self::Profile { name } => f.debug_struct("Profile").field("name", name).finish(),
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

/// Whether the process environment names an AWS region (`AWS_REGION` or
/// `AWS_DEFAULT_REGION`), so a client whose config names none does not ask
/// the instance metadata service for one.
pub fn region_from_env() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

/// The config fields that make a client built from `config` authenticate
/// with the identity of the host it runs on: `credentials` for every variant
/// but `access_key` (absent = `default`), and `region` when neither the
/// config nor the process environment names one.
pub fn host_identity(config: &Value) -> Vec<String> {
    host_identity_at(config, "/credentials", "/region")
}

/// [`host_identity`] for credentials and region at other JSON pointers.
pub fn host_identity_at(config: &Value, credentials: &str, region: &str) -> Vec<String> {
    let mut fields = Vec::new();
    if faucet_core::capabilities::config::tag_at(config, credentials).unwrap_or("default")
        != "access_key"
    {
        fields.push(dotted(credentials));
    }
    if !faucet_core::capabilities::config::non_empty_str_at(config, region) && !region_from_env() {
        fields.push(dotted(region));
    }
    fields
}

fn dotted(pointer: &str) -> String {
    pointer.trim_start_matches('/').replace('/', ".")
}

fn check_host_identity(
    region: Option<&str>,
    credentials: &DynamoDbCredentials,
) -> Result<(), FaucetError> {
    if !matches!(credentials, DynamoDbCredentials::AccessKey { .. }) {
        faucet_core::resolve::require_host_credentials("dynamodb", "credentials")?;
    }
    if region.is_none_or(|r| r.trim().is_empty()) && !region_from_env() {
        faucet_core::resolve::require_host_credentials("dynamodb", "region")?;
    }
    Ok(())
}

/// Resolve an `aws_config::SdkConfig` from region + credentials.
///
/// `region: None` defers to the SDK default chain (env, profile, IMDS).
/// Credential resolution is delegated to `aws-config`, so rotating
/// credentials (web identity, instance profiles, assumed roles) refresh
/// automatically.
pub async fn build_sdk_config(
    region: Option<&str>,
    credentials: &DynamoDbCredentials,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region.to_owned()));
    }
    match credentials {
        DynamoDbCredentials::Default | DynamoDbCredentials::WebIdentity => {}
        DynamoDbCredentials::Profile { name } => {
            loader = loader.profile_name(name);
        }
        DynamoDbCredentials::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            let creds = aws_sdk_dynamodb::config::Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
                None,
                "faucet-config",
            );
            loader = loader.credentials_provider(creds);
        }
        DynamoDbCredentials::AssumeRole {
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
    loader.load().await
}

/// Build an `aws_sdk_dynamodb::Client`. `endpoint_url` overrides the endpoint
/// for DynamoDB Local / LocalStack / VPC endpoints. No network I/O.
pub async fn build_client(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: &DynamoDbCredentials,
) -> Result<aws_sdk_dynamodb::Client, FaucetError> {
    check_host_identity(region, credentials)?;
    let sdk_config = build_sdk_config(region, credentials).await;
    let mut builder = aws_sdk_dynamodb::config::Builder::from(&sdk_config);
    if let Some(endpoint) = endpoint_url {
        builder = builder.endpoint_url(endpoint);
    }
    Ok(aws_sdk_dynamodb::Client::from_conf(builder.build()))
}

/// Build an `aws_sdk_dynamodbstreams::Client` with the same settings as
/// [`build_client`] (DynamoDB Local serves Streams on the same endpoint).
pub async fn build_streams_client(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: &DynamoDbCredentials,
) -> Result<aws_sdk_dynamodbstreams::Client, FaucetError> {
    check_host_identity(region, credentials)?;
    let sdk_config = build_sdk_config(region, credentials).await;
    let mut builder = aws_sdk_dynamodbstreams::config::Builder::from(&sdk_config);
    if let Some(endpoint) = endpoint_url {
        builder = builder.endpoint_url(endpoint);
    }
    Ok(aws_sdk_dynamodbstreams::Client::from_conf(builder.build()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_identity_follows_the_selected_credentials() {
        use faucet_core::json;
        let region = |fields: Vec<String>| {
            fields
                .into_iter()
                .filter(|f| f != "region")
                .collect::<Vec<_>>()
        };
        for c in [
            json!({}),
            json!({"credentials": {"type": "default"}}),
            json!({"credentials": {"type": "web_identity"}}),
            json!({"credentials": {"type": "profile", "config": {"name": "p"}}}),
            json!({"credentials": {"type": "assume_role", "config": {"role_arn": "r"}}}),
        ] {
            assert_eq!(region(host_identity(&c)), vec!["credentials"], "{c}");
        }
        let keyed = json!({
            "region": "us-east-1",
            "credentials": {"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}
        });
        assert!(host_identity(&keyed).is_empty());
        let no_region = json!({"credentials": {"type": "access_key"}});
        assert_eq!(
            host_identity(&no_region).is_empty(),
            region_from_env(),
            "a region comes from the config or the environment"
        );
        assert_eq!(
            host_identity_at(
                &json!({"x": {"c": {"type": "profile"}}, "r": "eu"}),
                "/x/c",
                "/r"
            ),
            vec!["x.c"]
        );
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        faucet_core::resolve::with_host_credentials(false, async {
            let err = build_client(Some("us-east-1"), None, &DynamoDbCredentials::Default)
                .await
                .expect_err("refused");
            assert!(err.to_string().contains("`credentials`"), "{err}");
            let keys = DynamoDbCredentials::AccessKey {
                access_key_id: "a".into(),
                secret_access_key: "s".into(),
                session_token: None,
            };
            assert!(build_client(Some("us-east-1"), None, &keys).await.is_ok());
            assert_eq!(
                build_client(None, None, &keys).await.is_ok(),
                region_from_env()
            );
        })
        .await;
        faucet_core::resolve::with_host_credentials(true, async {
            assert!(
                build_client(Some("us-east-1"), None, &DynamoDbCredentials::Default)
                    .await
                    .is_ok()
            );
        })
        .await;
    }

    #[test]
    fn credentials_parse_the_consistent_wire_shape() {
        let c: DynamoDbCredentials = yaml_serde::from_str("type: default\n").unwrap();
        assert!(matches!(c, DynamoDbCredentials::Default));

        let c: DynamoDbCredentials =
            yaml_serde::from_str("type: profile\nconfig: { name: prod }\n").unwrap();
        assert!(matches!(c, DynamoDbCredentials::Profile { name } if name == "prod"));

        let yaml =
            "type: access_key\nconfig:\n  access_key_id: AKIA\n  secret_access_key: s3cr3t\n";
        let c: DynamoDbCredentials = yaml_serde::from_str(yaml).unwrap();
        assert!(matches!(
            &c,
            DynamoDbCredentials::AccessKey { access_key_id, session_token: None, .. }
                if access_key_id == "AKIA"
        ));
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("s3cr3t") && dbg.contains("AKIA"), "{dbg}");

        let c: DynamoDbCredentials = yaml_serde::from_str(
            "type: assume_role\nconfig: { role_arn: 'arn:aws:iam::1:role/x' }\n",
        )
        .unwrap();
        assert!(format!("{c:?}").contains("arn:aws:iam::1:role/x"));

        let c: DynamoDbCredentials = yaml_serde::from_str("type: web_identity\n").unwrap();
        assert_eq!(format!("{c:?}"), "WebIdentity");
        assert_eq!(format!("{:?}", DynamoDbCredentials::default()), "Default");
        let p = DynamoDbCredentials::Profile { name: "p".into() };
        assert!(format!("{p:?}").contains('p'));
    }

    #[tokio::test]
    async fn clients_build_offline_for_every_variant() {
        let creds = DynamoDbCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: Some("tok".into()),
        };
        let client = build_client(Some("us-east-1"), Some("http://127.0.0.1:8000"), &creds)
            .await
            .unwrap();
        assert_eq!(
            client.config().region().map(|r| r.as_ref()),
            Some("us-east-1")
        );
        let streams =
            build_streams_client(Some("us-east-1"), Some("http://127.0.0.1:8000"), &creds)
                .await
                .unwrap();
        assert_eq!(
            streams.config().region().map(|r| r.as_ref()),
            Some("us-east-1")
        );
        for creds in [
            DynamoDbCredentials::Default,
            DynamoDbCredentials::WebIdentity,
            DynamoDbCredentials::Profile {
                name: "no-such-profile".into(),
            },
            DynamoDbCredentials::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/x".into(),
                session_name: Some("t".into()),
                external_id: Some("e".into()),
            },
        ] {
            build_client(Some("us-east-1"), None, &creds).await.unwrap();
            build_streams_client(None, None, &creds).await.unwrap();
        }
    }
}
