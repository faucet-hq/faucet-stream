//! Shared Oracle connection configuration and the pure logic that turns it into
//! an ODPI-C connect string. No I/O lives here — pooling is in [`crate::pool`].

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DEFAULT_TCP_PORT: u16 = 1521;
const DEFAULT_TCPS_PORT: u16 = 2484;

/// Connection settings shared by the Oracle source, CDC source and sink.
///
/// Set **either** [`connect_string`](Self::connect_string) (an Easy Connect
/// string, a TNS alias resolved through `tnsnames.ora`, or a full connect
/// descriptor) **or** [`host`](Self::host) plus exactly one of
/// [`service_name`](Self::service_name) / [`sid`](Self::sid). The host form is
/// rendered into a connect descriptor, which is where the [`tls`](Self::tls)
/// block takes effect.
#[derive(Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
pub struct OracleConnectionConfig {
    /// Easy Connect (`host:1521/FREEPDB1`), TNS alias, or connect descriptor.
    /// Mutually exclusive with `host`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_string: Option<String>,
    /// Database host. Mutually exclusive with `connect_string`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Listener port. Defaults to 1521 (2484 when `tls.enabled`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Service name (e.g. a PDB such as `FREEPDB1`). Mutually exclusive with `sid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// Legacy SID. Mutually exclusive with `service_name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    /// Database user. Required unless `external_auth` is set.
    #[serde(default)]
    pub username: String,
    /// Password for `username`.
    #[serde(default)]
    pub password: String,
    /// Authenticate with external credentials (OS authentication or a wallet
    /// holding the credentials) instead of `username` / `password`.
    #[serde(default)]
    pub external_auth: bool,
    /// Transport encryption (TCPS) for the `host` form.
    #[serde(default)]
    pub tls: OracleTls,
}

/// TCPS settings applied to a `host`-form connection.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct OracleTls {
    /// Connect over TCPS. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Directory holding the Oracle wallet (`cwallet.sso` / `ewallet.p12`) with
    /// the trusted CA and, for mutual TLS, the client certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wallet_location: Option<String>,
    /// Verify that the server certificate's DN matches the host. Defaults to `true`.
    #[serde(default = "default_true")]
    pub server_dn_match: bool,
    /// Expected server certificate DN, when it differs from the host name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_cert_dn: Option<String>,
}

fn default_true() -> bool {
    true
}

impl Default for OracleTls {
    fn default() -> Self {
        Self {
            enabled: false,
            wallet_location: None,
            server_dn_match: true,
            server_cert_dn: None,
        }
    }
}

impl std::fmt::Debug for OracleConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleConnectionConfig")
            .field("connect_string", &self.connect_string)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("service_name", &self.service_name)
            .field("sid", &self.sid)
            .field("username", &self.username)
            .field("password", &"***")
            .field("external_auth", &self.external_auth)
            .field("tls", &self.tls)
            .finish()
    }
}

impl OracleConnectionConfig {
    /// A `host`/`service_name` connection with username and password.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        service_name: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: Some(host.into()),
            port: Some(port),
            service_name: Some(service_name.into()),
            username: username.into(),
            password: password.into(),
            ..Default::default()
        }
    }

    /// Validate the fail-fast invariants.
    pub fn validate(&self) -> Result<(), FaucetError> {
        match (&self.connect_string, &self.host) {
            (Some(_), Some(_)) => {
                return Err(cfg_err(
                    "sets both `connect_string` and `host`; set exactly one",
                ));
            }
            (None, None) => {
                return Err(cfg_err("requires either `connect_string` or `host`"));
            }
            (Some(cs), None) => {
                if cs.trim().is_empty() {
                    return Err(cfg_err("`connect_string` must not be empty"));
                }
                if self.service_name.is_some() || self.sid.is_some() || self.port.is_some() {
                    return Err(cfg_err(
                        "`port` / `service_name` / `sid` only apply to the `host` form; \
                         put them in `connect_string` instead",
                    ));
                }
                if self.tls != OracleTls::default() {
                    return Err(cfg_err(
                        "the `tls` block only applies to the `host` form; use a `tcps://` \
                         connect string (or a descriptor with PROTOCOL=TCPS) instead",
                    ));
                }
            }
            (None, Some(host)) => {
                check_descriptor_value("host", host)?;
                match (&self.service_name, &self.sid) {
                    (Some(_), Some(_)) => {
                        return Err(cfg_err("sets both `service_name` and `sid`; set one"));
                    }
                    (None, None) => {
                        return Err(cfg_err("the `host` form needs `service_name` or `sid`"));
                    }
                    (Some(s), None) => check_descriptor_value("service_name", s)?,
                    (None, Some(s)) => check_descriptor_value("sid", s)?,
                }
                if let Some(w) = &self.tls.wallet_location {
                    check_descriptor_value("tls.wallet_location", w)?;
                }
                if let Some(dn) = &self.tls.server_cert_dn
                    && dn.contains('"')
                {
                    return Err(cfg_err("`tls.server_cert_dn` must not contain '\"'"));
                }
            }
        }
        if self.external_auth {
            if !self.username.is_empty() || !self.password.is_empty() {
                return Err(cfg_err(
                    "`external_auth: true` takes its credentials from the wallet / OS; \
                     leave `username` and `password` unset",
                ));
            }
        } else if self.username.trim().is_empty() {
            return Err(cfg_err("requires a `username` (or `external_auth: true`)"));
        }
        Ok(())
    }

    /// The connect string handed to ODPI-C: `connect_string` verbatim, or a
    /// connect descriptor rendered from the `host` form.
    pub fn resolve_connect_string(&self) -> Result<String, FaucetError> {
        self.validate()?;
        if let Some(cs) = &self.connect_string {
            return Ok(cs.trim().to_string());
        }
        let host = self.host.as_deref().unwrap_or_default();
        let (protocol, default_port) = if self.tls.enabled {
            ("TCPS", DEFAULT_TCPS_PORT)
        } else {
            ("TCP", DEFAULT_TCP_PORT)
        };
        let port = self.port.unwrap_or(default_port);
        let connect_data = match (&self.service_name, &self.sid) {
            (Some(s), _) => format!("(SERVICE_NAME={s})"),
            (None, Some(s)) => format!("(SID={s})"),
            (None, None) => unreachable!("validate() requires service_name or sid"),
        };
        let mut out = format!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL={protocol})(HOST={host})(PORT={port}))\
             (CONNECT_DATA={connect_data})"
        );
        if self.tls.enabled {
            let mut security = format!(
                "(SSL_SERVER_DN_MATCH={})",
                if self.tls.server_dn_match {
                    "YES"
                } else {
                    "NO"
                }
            );
            if let Some(dn) = &self.tls.server_cert_dn {
                security.push_str(&format!("(SSL_SERVER_CERT_DN=\"{dn}\")"));
            }
            if let Some(w) = &self.tls.wallet_location {
                security.push_str(&format!("(MY_WALLET_DIRECTORY={w})"));
            }
            out.push_str(&format!("(SECURITY={security})"));
        }
        out.push(')');
        Ok(out)
    }

    /// A credential-free URI naming the database, for lineage `dataset_uri`s and
    /// state-key derivation.
    pub fn display_uri(&self) -> String {
        if let Some(cs) = &self.connect_string {
            return format!(
                "oracle://{}",
                faucet_core::redact_uri_credentials(cs.trim())
            );
        }
        let host = self.host.as_deref().unwrap_or_default();
        let port = self.port.unwrap_or(if self.tls.enabled {
            DEFAULT_TCPS_PORT
        } else {
            DEFAULT_TCP_PORT
        });
        let db = self
            .service_name
            .as_deref()
            .or(self.sid.as_deref())
            .unwrap_or_default();
        format!("oracle://{host}:{port}/{db}")
    }

    /// A short, key-safe scope (service / SID / host) used when deriving state keys.
    pub fn scope_label(&self) -> String {
        let raw = self
            .service_name
            .as_deref()
            .or(self.sid.as_deref())
            .or(self.host.as_deref())
            .or(self.connect_string.as_deref())
            .unwrap_or("oracle")
            .trim();
        let label: String = raw
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        label.trim_matches('_').to_string()
    }
}

fn cfg_err(msg: &str) -> FaucetError {
    FaucetError::Config(format!("oracle connection {msg}"))
}

/// Reject characters that would break out of a connect-descriptor value.
fn check_descriptor_value(field: &str, value: &str) -> Result<(), FaucetError> {
    if value.trim().is_empty() {
        return Err(cfg_err(&format!("`{field}` must not be empty")));
    }
    if value.chars().any(|c| matches!(c, '(' | ')' | '=' | '\0')) {
        return Err(cfg_err(&format!(
            "`{field}` must not contain '(', ')', '=' or NUL"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn host_cfg() -> OracleConnectionConfig {
        OracleConnectionConfig::new("db.example.com", 1521, "FREEPDB1", "app", "s3cret")
    }

    #[test]
    fn host_form_renders_descriptor() {
        let cs = host_cfg().resolve_connect_string().unwrap();
        assert_eq!(
            cs,
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))\
             (CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
        );
    }

    #[test]
    fn sid_and_default_port() {
        let cfg = OracleConnectionConfig {
            port: None,
            service_name: None,
            sid: Some("ORCL".into()),
            ..host_cfg()
        };
        let cs = cfg.resolve_connect_string().unwrap();
        assert!(cs.contains("(PORT=1521)"), "{cs}");
        assert!(cs.contains("(CONNECT_DATA=(SID=ORCL))"), "{cs}");
    }

    #[test]
    fn tls_renders_security_block_and_tcps_port() {
        let cfg = OracleConnectionConfig {
            port: None,
            tls: OracleTls {
                enabled: true,
                wallet_location: Some("/opt/wallet".into()),
                server_dn_match: false,
                server_cert_dn: Some("CN=db,O=Acme".into()),
            },
            ..host_cfg()
        };
        let cs = cfg.resolve_connect_string().unwrap();
        assert!(cs.contains("(PROTOCOL=TCPS)"), "{cs}");
        assert!(cs.contains("(PORT=2484)"), "{cs}");
        assert!(cs.contains("(SSL_SERVER_DN_MATCH=NO)"), "{cs}");
        assert!(cs.contains("(SSL_SERVER_CERT_DN=\"CN=db,O=Acme\")"), "{cs}");
        assert!(cs.contains("(MY_WALLET_DIRECTORY=/opt/wallet)"), "{cs}");
        assert!(cs.ends_with("))"), "{cs}");
        assert_eq!(cfg.display_uri(), "oracle://db.example.com:2484/FREEPDB1");
    }

    #[test]
    fn tls_with_dn_match_only() {
        let cfg = OracleConnectionConfig {
            tls: OracleTls {
                enabled: true,
                ..Default::default()
            },
            ..host_cfg()
        };
        let cs = cfg.resolve_connect_string().unwrap();
        assert!(cs.contains("(SECURITY=(SSL_SERVER_DN_MATCH=YES))"), "{cs}");
    }

    #[test]
    fn connect_string_passes_through() {
        let cfg = OracleConnectionConfig {
            connect_string: Some("  localhost:1521/FREEPDB1 ".into()),
            username: "app".into(),
            ..Default::default()
        };
        assert_eq!(
            cfg.resolve_connect_string().unwrap(),
            "localhost:1521/FREEPDB1"
        );
        assert_eq!(cfg.display_uri(), "oracle://localhost:1521/FREEPDB1");
        assert_eq!(cfg.scope_label(), "localhost_1521_FREEPDB1");
    }

    #[test]
    fn validate_rejects_bad_combinations() {
        let both = OracleConnectionConfig {
            connect_string: Some("x".into()),
            ..host_cfg()
        };
        assert!(both.validate().is_err());
        let neither = OracleConnectionConfig {
            username: "u".into(),
            ..Default::default()
        };
        assert!(neither.validate().is_err());
        let empty_cs = OracleConnectionConfig {
            connect_string: Some(" ".into()),
            username: "u".into(),
            ..Default::default()
        };
        assert!(empty_cs.validate().is_err());
        let cs_with_service = OracleConnectionConfig {
            connect_string: Some("x".into()),
            service_name: Some("s".into()),
            username: "u".into(),
            ..Default::default()
        };
        assert!(cs_with_service.validate().is_err());
        let cs_with_tls = OracleConnectionConfig {
            connect_string: Some("x".into()),
            username: "u".into(),
            tls: OracleTls {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cs_with_tls.validate().is_err());
        let both_ids = OracleConnectionConfig {
            sid: Some("X".into()),
            ..host_cfg()
        };
        assert!(both_ids.validate().is_err());
        let no_ids = OracleConnectionConfig {
            service_name: None,
            ..host_cfg()
        };
        assert!(no_ids.validate().is_err());
        let injected = OracleConnectionConfig {
            host: Some("h)(PORT=1)".into()),
            ..host_cfg()
        };
        assert!(injected.validate().is_err());
        let empty_host = OracleConnectionConfig {
            host: Some(" ".into()),
            ..host_cfg()
        };
        assert!(empty_host.validate().is_err());
        let bad_sid = OracleConnectionConfig {
            service_name: None,
            sid: Some("a=b".into()),
            ..host_cfg()
        };
        assert!(bad_sid.validate().is_err());
        let bad_wallet = OracleConnectionConfig {
            tls: OracleTls {
                enabled: true,
                wallet_location: Some("(x".into()),
                ..Default::default()
            },
            ..host_cfg()
        };
        assert!(bad_wallet.validate().is_err());
        let bad_dn = OracleConnectionConfig {
            tls: OracleTls {
                enabled: true,
                server_cert_dn: Some("CN=\"x\"".into()),
                ..Default::default()
            },
            ..host_cfg()
        };
        assert!(bad_dn.validate().is_err());
    }

    #[test]
    fn validate_credentials() {
        let no_user = OracleConnectionConfig {
            username: String::new(),
            ..host_cfg()
        };
        assert!(no_user.validate().is_err());
        let ext_with_user = OracleConnectionConfig {
            external_auth: true,
            ..host_cfg()
        };
        assert!(ext_with_user.validate().is_err());
        let ext = OracleConnectionConfig {
            external_auth: true,
            username: String::new(),
            password: String::new(),
            ..host_cfg()
        };
        ext.validate().unwrap();
    }

    #[test]
    fn serde_defaults_and_debug_masks_password() {
        let cfg: OracleConnectionConfig = serde_json::from_value(json!({
            "host": "h", "service_name": "S", "username": "u", "password": "topsecret"
        }))
        .unwrap();
        assert!(!cfg.tls.enabled);
        assert!(cfg.tls.server_dn_match);
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("topsecret"));
        assert_eq!(cfg.display_uri(), "oracle://h:1521/S");
        assert_eq!(cfg.scope_label(), "S");
    }

    #[test]
    fn scope_label_falls_back() {
        let cfg = OracleConnectionConfig {
            service_name: None,
            sid: Some("ORCL".into()),
            ..host_cfg()
        };
        assert_eq!(cfg.scope_label(), "ORCL");
        assert_eq!(OracleConnectionConfig::default().scope_label(), "oracle");
    }
}
