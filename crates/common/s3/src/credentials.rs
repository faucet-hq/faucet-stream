//! How an S3 client authenticates, and building one from config.

use faucet_core::{FaucetError, Value};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with S3.
///
/// Serializes as `{ type: <method>, config: { … } }`, the auth wire shape
/// every faucet connector shares (the Kinesis / SQS / DynamoDB connectors use
/// the same variants).
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum S3Credentials {
    /// The AWS SDK default provider chain: environment variables, shared
    /// config/credentials files, container credentials, web-identity tokens
    /// and the instance profile. The identity of the host faucet runs on.
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
    /// Web-identity federation (`AWS_WEB_IDENTITY_TOKEN_FILE` +
    /// `AWS_ROLE_ARN`), resolved by the default chain.
    WebIdentity,
    /// Unsigned requests, for public buckets.
    Anonymous,
}

impl std::fmt::Debug for S3Credentials {
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
            Self::Anonymous => write!(f, "Anonymous"),
        }
    }
}

impl S3Credentials {
    /// Whether these credentials come from the host faucet runs on rather
    /// than the config.
    pub fn uses_host_identity(&self) -> bool {
        !matches!(self, Self::AccessKey { .. } | Self::Anonymous)
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

/// The config fields that make an S3 client built from `config` authenticate
/// with the identity of the host it runs on: `credentials` for every variant
/// but `access_key` and `anonymous` (absent = `default`), and `region` when
/// neither the config nor the process environment names one.
pub fn host_identity(config: &Value) -> Vec<String> {
    host_identity_at(config, "/credentials", "/region")
}

/// [`host_identity`] for credentials and region at other JSON pointers.
pub fn host_identity_at(config: &Value, credentials: &str, region: &str) -> Vec<String> {
    use faucet_core::capabilities::config::{non_empty_str_at, tag_at};
    let mut fields = Vec::new();
    if !matches!(
        tag_at(config, credentials).unwrap_or("default"),
        "access_key" | "anonymous"
    ) {
        fields.push(dotted(credentials));
    }
    if !non_empty_str_at(config, region) && !region_from_env() {
        fields.push(dotted(region));
    }
    fields
}

fn dotted(pointer: &str) -> String {
    pointer.trim_start_matches('/').replace('/', ".")
}

/// Refuse, for a task that may not use the host's identity, credentials or a
/// missing region that would make the SDK use it.
pub fn check_host_identity(
    connector: &str,
    region: Option<&str>,
    credentials: &S3Credentials,
) -> Result<(), FaucetError> {
    if credentials.uses_host_identity() {
        faucet_core::resolve::require_host_credentials(connector, "credentials")?;
    }
    if region.is_none_or(|r| r.trim().is_empty()) && !region_from_env() {
        faucet_core::resolve::require_host_credentials(connector, "region")?;
    }
    Ok(())
}

/// The SDK configuration for `credentials`, in `region` (else the chain's)
/// and against `endpoint_url` when set. Refused first by
/// [`check_host_identity`].
pub async fn sdk_config(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: &S3Credentials,
) -> Result<aws_config::SdkConfig, FaucetError> {
    check_host_identity("s3", region, credentials)?;
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region.filter(|r| !r.trim().is_empty()) {
        loader = loader.region(aws_config::Region::new(region.to_string()));
    }
    if let Some(endpoint) = endpoint_url {
        loader = loader.endpoint_url(endpoint);
    }
    match credentials {
        S3Credentials::Default | S3Credentials::WebIdentity => {}
        S3Credentials::Profile { name } => loader = loader.profile_name(name),
        S3Credentials::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            loader = loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
                None,
                "faucet-config",
            ));
        }
        S3Credentials::AssumeRole {
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
        S3Credentials::Anonymous => loader = loader.no_credentials(),
    }
    Ok(loader.load().await)
}

/// An S3 client for `credentials`, in `region` (else the chain's) and
/// against `endpoint_url` (an S3-compatible store, addressed path-style) when
/// set. Refused first by [`check_host_identity`].
pub async fn build_client_with(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: &S3Credentials,
) -> Result<aws_sdk_s3::Client, FaucetError> {
    let conf = sdk_config(region, endpoint_url, credentials).await?;
    Ok(aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&conf)
            .force_path_style(endpoint_url.is_some())
            .build(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    fn keys() -> S3Credentials {
        S3Credentials::AccessKey {
            access_key_id: "a".into(),
            secret_access_key: "s".into(),
            session_token: None,
        }
    }

    #[test]
    fn credentials_parse_the_shared_wire_shape() {
        let parse = |v: Value| faucet_core::serde_json::from_value::<S3Credentials>(v).unwrap();
        assert!(matches!(
            parse(json!({"type": "default"})),
            S3Credentials::Default
        ));
        assert!(matches!(
            parse(json!({"type": "anonymous"})),
            S3Credentials::Anonymous
        ));
        assert!(matches!(
            parse(json!({"type": "web_identity"})),
            S3Credentials::WebIdentity
        ));
        assert!(matches!(
            parse(json!({"type": "profile", "config": {"name": "p"}})),
            S3Credentials::Profile { name } if name == "p"
        ));
        assert!(matches!(
            parse(json!({"type": "assume_role", "config": {"role_arn": "r"}})),
            S3Credentials::AssumeRole { .. }
        ));
        let c = parse(json!({"type": "access_key", "config": {
            "access_key_id": "AKIA", "secret_access_key": "s3cr3t"}}));
        let dbg = format!("{c:?}");
        assert!(dbg.contains("AKIA") && !dbg.contains("s3cr3t"), "{dbg}");
        assert!(matches!(S3Credentials::default(), S3Credentials::Default));
        assert_eq!(
            format!(
                "{:?}",
                parse(json!({"type": "assume_role", "config": {"role_arn": "r"}}))
            ),
            "AssumeRole { role_arn: \"r\" }"
        );
    }

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let without_region = |c: Value| -> Vec<String> {
            host_identity(&c)
                .into_iter()
                .filter(|f| f != "region")
                .collect()
        };
        for c in [
            json!({}),
            json!({"credentials": {"type": "default"}}),
            json!({"credentials": {"type": "web_identity"}}),
            json!({"credentials": {"type": "profile", "config": {"name": "p"}}}),
            json!({"credentials": {"type": "assume_role", "config": {"role_arn": "r"}}}),
        ] {
            assert_eq!(without_region(c.clone()), vec!["credentials"], "{c}");
        }
        for t in ["access_key", "anonymous"] {
            let c = json!({"region": "us-east-1", "credentials": {"type": t}});
            assert!(host_identity(&c).is_empty(), "{t}");
        }
        assert_eq!(
            host_identity(&json!({"credentials": {"type": "anonymous"}})).is_empty(),
            region_from_env()
        );
        assert_eq!(
            host_identity_at(
                &json!({"s3": {"region": "eu"}}),
                "/s3/credentials",
                "/s3/region"
            ),
            vec!["s3.credentials"]
        );
        assert!(!keys().uses_host_identity() && S3Credentials::Default.uses_host_identity());
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        faucet_core::resolve::with_host_credentials(false, async {
            for c in [
                S3Credentials::Default,
                S3Credentials::WebIdentity,
                S3Credentials::Profile { name: "p".into() },
            ] {
                let err = build_client_with(Some("us-east-1"), None, &c)
                    .await
                    .expect_err("refused");
                assert!(err.to_string().contains("s3: `credentials`"), "{err}");
            }
            for c in [keys(), S3Credentials::Anonymous] {
                assert!(build_client_with(Some("us-east-1"), None, &c).await.is_ok());
                assert_eq!(
                    build_client_with(None, None, &c).await.is_ok(),
                    region_from_env()
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn trusted_runs_keep_the_server_identity() {
        for c in [
            S3Credentials::Default,
            S3Credentials::WebIdentity,
            S3Credentials::Profile {
                name: "no-such-profile".into(),
            },
            S3Credentials::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/x".into(),
                session_name: Some("t".into()),
                external_id: Some("e".into()),
            },
            S3Credentials::Anonymous,
            keys(),
        ] {
            let client = build_client_with(Some("eu-west-1"), Some("http://127.0.0.1:9"), &c)
                .await
                .expect("builds offline");
            assert_eq!(
                client.config().region().map(|r| r.as_ref()),
                Some("eu-west-1")
            );
        }
    }
}
