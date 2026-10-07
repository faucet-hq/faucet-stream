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
/// v1 implements only [`RedshiftCredentials::Password`]. The [`Iam`] and
/// [`RedshiftDataApi`] variants are accepted by the config parser (so a future
/// version can add them without a breaking change) but currently return a typed
/// [`FaucetError::Config`](faucet_core::FaucetError::Config) at client-build
/// time.
///
/// [`Iam`]: RedshiftCredentials::Iam
/// [`RedshiftDataApi`]: RedshiftCredentials::RedshiftDataApi
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum RedshiftCredentials {
    /// Username/password authentication (the user comes from
    /// [`RedshiftConnection::user`]). The only mechanism supported in v1.
    Password {
        /// The password (use `${env:…}` / `${vault:…}` to inject it, never a
        /// literal).
        password: String,
    },
    /// IAM authentication via temporary cluster credentials
    /// (`GetClusterCredentials`). **Not yet supported** — reserved for a future
    /// version; building a client with this variant returns a typed error.
    Iam {
        /// AWS region of the cluster.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// Provisioned cluster identifier used to request temporary credentials.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster_identifier: Option<String>,
        /// Database user to authenticate as.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_user: Option<String>,
    },
    /// Authentication through the Redshift Data API (HTTP, not the PG wire).
    /// **Not yet supported** — reserved for a future version; building a client
    /// with this variant returns a typed error.
    RedshiftDataApi {
        /// AWS region.
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
        /// Database user to authenticate as.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        db_user: Option<String>,
    },
}

impl std::fmt::Debug for RedshiftCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password { .. } => write!(f, "Password(***)"),
            Self::Iam {
                region,
                cluster_identifier,
                db_user,
            } => f
                .debug_struct("Iam")
                .field("region", region)
                .field("cluster_identifier", cluster_identifier)
                .field("db_user", db_user)
                .finish(),
            Self::RedshiftDataApi {
                region,
                cluster_identifier,
                workgroup_name,
                secret_arn,
                db_user,
            } => f
                .debug_struct("RedshiftDataApi")
                .field("region", region)
                .field("cluster_identifier", cluster_identifier)
                .field("workgroup_name", workgroup_name)
                .field("secret_arn", secret_arn)
                .field("db_user", db_user)
                .finish(),
        }
    }
}

/// A Redshift connection: endpoint, database, user, credentials, and a TLS
/// toggle. Flattened (`#[serde(flatten)]`) into both the source and sink
/// configs so the connection fields appear at the config top level.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedshiftConnection {
    /// Cluster / endpoint host (e.g. `my-cluster.abc123.us-east-1.redshift.amazonaws.com`).
    pub host: String,
    /// Port. Defaults to [`DEFAULT_PORT`] (5439).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Database user.
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
    /// the server certificate against `ssl_root_cert` (or the system roots),
    /// the latter also its host name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_mode: Option<RedshiftTlsMode>,
    /// PEM file of the CA that signed the server certificate (the Redshift CA
    /// bundle), for `tls_mode: verify_ca | verify_full`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    fn iam_debug_renders_fields() {
        let c = RedshiftCredentials::Iam {
            region: Some("us-west-2".into()),
            cluster_identifier: Some("prod-cluster".into()),
            db_user: Some("analyst".into()),
        };
        let dbg = format!("{c:?}");
        assert!(dbg.contains("Iam"));
        assert!(dbg.contains("us-west-2"));
        assert!(dbg.contains("prod-cluster"));
        assert!(dbg.contains("analyst"));
    }

    #[test]
    fn redshift_data_api_debug_renders_fields() {
        let c = RedshiftCredentials::RedshiftDataApi {
            region: Some("eu-central-1".into()),
            cluster_identifier: None,
            workgroup_name: Some("wg-1".into()),
            secret_arn: Some("arn:aws:secretsmanager:x".into()),
            db_user: Some("svc".into()),
        };
        let dbg = format!("{c:?}");
        assert!(dbg.contains("RedshiftDataApi"));
        assert!(dbg.contains("eu-central-1"));
        assert!(dbg.contains("wg-1"));
        assert!(dbg.contains("svc"));
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
