//! Shared Amazon Redshift connection + credentials configuration.
//!
//! Redshift speaks the PostgreSQL wire protocol, so both the source and the
//! sink connect through `sqlx`'s Postgres driver. This module holds the
//! connection block (host / port / database / user + a TLS toggle) and the
//! credentials enum that both connectors flatten into their own configs.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default Redshift port.
pub const DEFAULT_PORT: u16 = 5439;

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_tls() -> bool {
    true
}

/// How to authenticate with Amazon Redshift.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by every
/// faucet connector.
///
/// - [`Password`](Self::Password) — a database user and password over the
///   PostgreSQL wire protocol.
/// - [`Iam`](Self::Iam) — temporary database credentials from
///   `redshift:GetClusterCredentials` (provisioned cluster) or
///   `redshift-serverless:GetCredentials` (workgroup), refreshed before they
///   expire, over the PostgreSQL wire protocol.
/// - [`RedshiftDataApi`](Self::RedshiftDataApi) — every statement runs through
///   the Redshift Data API over HTTPS; no network path to the cluster is
///   needed.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum RedshiftCredentials {
    /// Username/password authentication (the user comes from
    /// [`RedshiftConnection::user`]).
    Password {
        /// The password (use `${env:…}` / `${vault:…}` to inject it, never a
        /// literal).
        password: String,
    },
    /// IAM authentication with temporary database credentials. Set exactly
    /// one of `cluster_identifier` (provisioned, `GetClusterCredentials`) or
    /// `workgroup_name` (serverless, `GetCredentials`). The connection then
    /// uses the returned user and password over the PostgreSQL wire protocol
    /// (`host`, `port`, `database` and TLS as configured), and new pool
    /// connections get fresh credentials before the old ones expire.
    Iam {
        /// AWS region of the cluster or workgroup. `None` uses the SDK
        /// default chain (`AWS_REGION`, profile).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// Provisioned cluster identifier (`GetClusterCredentials`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster_identifier: Option<String>,
        /// Database user to request credentials for (provisioned only).
        /// Defaults to [`RedshiftConnection::user`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_user: Option<String>,
        /// Serverless workgroup name (`redshift-serverless:GetCredentials`);
        /// the database user is derived from the IAM identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workgroup_name: Option<String>,
        /// Create `db_user` when it does not exist (provisioned only).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        auto_create: bool,
        /// Database groups the user joins for the session (provisioned only).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        db_groups: Vec<String>,
        /// Lifetime of the temporary credentials, 900–3600 seconds. `None`
        /// uses the service default (900).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_secs: Option<u32>,
        /// AWS credentials used to call the credentials API. Defaults to the
        /// SDK default chain.
        #[serde(default, skip_serializing_if = "RedshiftAwsCredentials::is_default")]
        aws_credentials: RedshiftAwsCredentials,
        /// Endpoint override for the credentials API (VPC endpoints, tests).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(extend("x-faucet-resource" = "url"))]
        endpoint_url: Option<String>,
    },
    /// Every statement runs through the Redshift Data API over HTTPS
    /// (`ExecuteStatement`, `DescribeStatement`, `GetStatementResult`), not the
    /// PostgreSQL wire protocol, so `host`, `port`, `user` and the TLS settings
    /// are not used. Set exactly one of `cluster_identifier` or
    /// `workgroup_name`; a cluster also needs `db_user` (temporary
    /// credentials) or `secret_arn`.
    RedshiftDataApi {
        /// AWS region. `None` uses the SDK default chain.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// Provisioned cluster identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster_identifier: Option<String>,
        /// Serverless workgroup name (alternative to `cluster_identifier`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workgroup_name: Option<String>,
        /// Secrets Manager ARN holding the database credentials.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret_arn: Option<String>,
        /// Database user to authenticate as with temporary credentials
        /// (provisioned clusters only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_user: Option<String>,
        /// AWS credentials used to call the Data API. Defaults to the SDK
        /// default chain.
        #[serde(default, skip_serializing_if = "RedshiftAwsCredentials::is_default")]
        aws_credentials: RedshiftAwsCredentials,
        /// Endpoint override for the Data API (VPC endpoints, tests).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(extend("x-faucet-resource" = "url"))]
        endpoint_url: Option<String>,
    },
}

/// The AWS credentials the `iam` and `redshift_data_api` methods call AWS
/// with. The same shape as the other AWS connectors' `credentials`.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum RedshiftAwsCredentials {
    /// The AWS SDK default provider chain: environment variables, shared
    /// config/credentials files, container credentials, web-identity tokens
    /// and instance profiles, refreshed automatically.
    #[default]
    Default,
    /// A named profile from the shared AWS config/credentials files.
    Profile {
        /// Profile name.
        name: String,
    },
    /// Static access keys. Prefer `${env:…}` / secrets-manager interpolation
    /// over literals.
    AccessKey {
        /// AWS access key id.
        access_key_id: String,
        /// AWS secret access key.
        secret_access_key: String,
        /// Optional session token (temporary credentials).
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
    /// `AWS_ROLE_ARN`, e.g. EKS IRSA), through the default chain.
    WebIdentity,
}

impl RedshiftAwsCredentials {
    /// Whether this is the default provider chain.
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }
}

impl std::fmt::Debug for RedshiftAwsCredentials {
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

impl std::fmt::Debug for RedshiftCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password { .. } => write!(f, "Password(***)"),
            Self::Iam {
                region,
                cluster_identifier,
                db_user,
                workgroup_name,
                aws_credentials,
                ..
            } => f
                .debug_struct("Iam")
                .field("region", region)
                .field("cluster_identifier", cluster_identifier)
                .field("db_user", db_user)
                .field("workgroup_name", workgroup_name)
                .field("aws_credentials", aws_credentials)
                .finish_non_exhaustive(),
            Self::RedshiftDataApi {
                region,
                cluster_identifier,
                workgroup_name,
                secret_arn,
                db_user,
                aws_credentials,
                ..
            } => f
                .debug_struct("RedshiftDataApi")
                .field("region", region)
                .field("cluster_identifier", cluster_identifier)
                .field("workgroup_name", workgroup_name)
                .field("secret_arn", secret_arn)
                .field("db_user", db_user)
                .field("aws_credentials", aws_credentials)
                .finish_non_exhaustive(),
        }
    }
}

/// A Redshift connection: endpoint, database, user, credentials, and a TLS
/// toggle. Flattened (`#[serde(flatten)]`) into both the source and sink
/// configs so the connection fields appear at the config top level.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedshiftConnection {
    /// Cluster / endpoint host (e.g. `my-cluster.abc123.us-east-1.redshift.amazonaws.com`).
    /// Required except with `credentials: { type: redshift_data_api }`.
    #[serde(default)]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub host: String,
    /// Port. Defaults to [`DEFAULT_PORT`] (5439).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Database user. Required for `password`; for a provisioned `iam`
    /// cluster it is the default `db_user`; unused by `redshift_data_api`.
    #[serde(default)]
    pub user: String,
    /// Authentication credentials.
    pub credentials: RedshiftCredentials,
    /// Whether to require TLS. Defaults to `true` (Redshift clusters require SSL
    /// by default). `true` maps to `sslmode=require` (encrypted, but the server
    /// certificate is **not** verified); `false` maps to `sslmode=prefer`
    /// (opportunistic TLS with plaintext fallback). Ignored when `tls_mode` is
    /// set.
    #[serde(default = "default_tls")]
    pub tls: bool,
    /// Explicit TLS mode; overrides `tls`. `verify_ca` / `verify_full` check
    /// the server certificate against `ssl_root_cert` (or, without one, the
    /// Mozilla root store bundled into the binary — not the operating
    /// system's), the latter also its host name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_mode: Option<RedshiftTlsMode>,
    /// PEM file of the CA that signed the server certificate (the Redshift CA
    /// bundle), for `tls_mode: verify_ca | verify_full`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub ssl_root_cert: Option<String>,
}

/// How a Redshift connection uses TLS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftTlsMode {
    /// Plaintext only.
    Disable,
    /// TLS when the server offers it, else plaintext.
    Prefer,
    /// TLS required; the certificate is not verified.
    Require,
    /// TLS required and the certificate chain verified.
    VerifyCa,
    /// As `verify_ca`, plus the certificate must name the host.
    VerifyFull,
}

impl RedshiftConnection {
    /// Build a `Password` connection with sensible defaults (port 5439, TLS on).
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_PORT,
            database: database.into(),
            user: user.into(),
            credentials: RedshiftCredentials::Password {
                password: password.into(),
            },
            tls: true,
            tls_mode: None,
            ssl_root_cert: None,
        }
    }

    /// Whether statements run through the Redshift Data API rather than the
    /// PostgreSQL wire protocol.
    pub fn uses_data_api(&self) -> bool {
        matches!(
            self.credentials,
            RedshiftCredentials::RedshiftDataApi { .. }
        )
    }

    /// What the connection addresses, for dataset URIs and logs: the host, or
    /// `cluster:<id>` / `workgroup:<name>` for the Data API.
    pub fn endpoint_label(&self) -> String {
        match &self.credentials {
            RedshiftCredentials::RedshiftDataApi {
                cluster_identifier,
                workgroup_name,
                ..
            } => match (cluster_identifier, workgroup_name) {
                (Some(c), _) => format!("cluster:{c}"),
                (None, Some(w)) => format!("workgroup:{w}"),
                (None, None) => String::new(),
            },
            _ => self.host.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_password() {
        let c = RedshiftCredentials::Password {
            password: "s3cr3t".into(),
        };
        let dbg = format!("{c:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("s3cr3t"));
    }

    #[test]
    fn connection_debug_does_not_leak_password() {
        let conn = RedshiftConnection::new("host", "db", "user", "hunter2");
        let dbg = format!("{conn:?}");
        assert!(!dbg.contains("hunter2"));
        assert!(dbg.contains("host"));
        assert!(dbg.contains("user"));
    }

    #[test]
    fn password_credentials_round_trip() {
        let c = RedshiftCredentials::Password {
            password: "pw".into(),
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, r#"{"type":"password","config":{"password":"pw"}}"#);
        let back: RedshiftCredentials = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, RedshiftCredentials::Password { .. }));
    }

    #[test]
    fn connection_defaults_port_and_tls() {
        let json = r#"{
            "host": "h",
            "database": "db",
            "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}}
        }"#;
        let conn: RedshiftConnection = serde_json::from_str(json).unwrap();
        assert_eq!(conn.port, DEFAULT_PORT);
        assert!(conn.tls);
    }

    #[test]
    fn iam_variant_deserializes() {
        let json = r#"{"type":"iam","config":{"region":"us-east-1","db_user":"analyst"}}"#;
        let c: RedshiftCredentials = serde_json::from_str(json).unwrap();
        match c {
            RedshiftCredentials::Iam {
                region, db_user, ..
            } => {
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(db_user.as_deref(), Some("analyst"));
            }
            _ => panic!("expected Iam"),
        }
    }

    #[test]
    fn redshift_data_api_variant_deserializes() {
        let json =
            r#"{"type":"redshift_data_api","config":{"workgroup_name":"wg","secret_arn":"arn:x"}}"#;
        let c: RedshiftCredentials = serde_json::from_str(json).unwrap();
        assert!(matches!(c, RedshiftCredentials::RedshiftDataApi { .. }));
    }

    #[test]
    fn iam_debug_renders_fields_and_masks_keys() {
        let c: RedshiftCredentials = serde_json::from_value(serde_json::json!({
            "type": "iam",
            "config": {
                "region": "us-west-2",
                "cluster_identifier": "prod-cluster",
                "db_user": "analyst",
                "aws_credentials": {"type": "access_key", "config": {
                    "access_key_id": "AKIDX", "secret_access_key": "topsecret"}}
            }
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        for want in ["Iam", "us-west-2", "prod-cluster", "analyst", "AKIDX"] {
            assert!(dbg.contains(want), "{dbg}");
        }
        assert!(!dbg.contains("topsecret"), "{dbg}");
    }

    #[test]
    fn redshift_data_api_debug_renders_fields() {
        let c: RedshiftCredentials = serde_json::from_value(serde_json::json!({
            "type": "redshift_data_api",
            "config": {"region": "eu-central-1", "workgroup_name": "wg-1",
                       "secret_arn": "arn:aws:secretsmanager:x", "db_user": "svc"}
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        for want in ["RedshiftDataApi", "eu-central-1", "wg-1", "svc", "Default"] {
            assert!(dbg.contains(want), "{dbg}");
        }
    }

    #[test]
    fn aws_credentials_parse_and_debug() {
        for (json, want) in [
            (r#"{"type":"default"}"#, "Default"),
            (r#"{"type":"profile","config":{"name":"p"}}"#, "Profile"),
            (
                r#"{"type":"assume_role","config":{"role_arn":"arn:r"}}"#,
                "arn:r",
            ),
            (r#"{"type":"web_identity"}"#, "WebIdentity"),
        ] {
            let c: RedshiftAwsCredentials = serde_json::from_str(json).unwrap();
            assert!(format!("{c:?}").contains(want), "{json}");
        }
        assert!(RedshiftAwsCredentials::default().is_default());
    }

    #[test]
    fn endpoint_label_names_the_host_or_the_data_api_target() {
        let mut c = RedshiftConnection::new("h.example", "db", "u", "pw");
        assert_eq!(c.endpoint_label(), "h.example");
        assert!(!c.uses_data_api());
        for (cfg, want) in [
            (
                serde_json::json!({"cluster_identifier": "c1"}),
                "cluster:c1",
            ),
            (serde_json::json!({"workgroup_name": "wg"}), "workgroup:wg"),
            (serde_json::json!({}), ""),
        ] {
            c.credentials = serde_json::from_value(
                serde_json::json!({"type": "redshift_data_api", "config": cfg}),
            )
            .unwrap();
            assert!(c.uses_data_api());
            assert_eq!(c.endpoint_label(), want);
        }
    }

    #[test]
    fn data_api_connection_needs_no_host_or_user() {
        let conn: RedshiftConnection = serde_json::from_str(
            r#"{"database":"dev","credentials":{"type":"redshift_data_api","config":{"workgroup_name":"wg"}}}"#,
        )
        .unwrap();
        assert!(conn.host.is_empty() && conn.user.is_empty());
    }

    #[test]
    fn iam_and_data_api_round_trip_full_fields() {
        let iam = r#"{"type":"iam","config":{"region":"us-east-1","cluster_identifier":"c1","db_user":"u"}}"#;
        let back: RedshiftCredentials = serde_json::from_str(iam).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), iam);

        let api = r#"{"type":"redshift_data_api","config":{"region":"us-east-1","cluster_identifier":"c1","workgroup_name":"wg","secret_arn":"arn","db_user":"u"}}"#;
        let back: RedshiftCredentials = serde_json::from_str(api).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), api);
    }
}
