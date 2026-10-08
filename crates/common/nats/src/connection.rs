//! Shared NATS connection configuration and the single client builder used by
//! both the source and the sink.

use crate::auth::NatsAuth;
use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_servers() -> Vec<String> {
    vec!["nats://127.0.0.1:4222".to_string()]
}

/// Connection settings shared by the NATS source and sink.
///
/// This struct is `#[serde(flatten)]`ed into each connector's config so a
/// single `servers` / `auth` / `tls` / `name` surface is presented to users.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NatsConnectionConfig {
    /// One or more NATS server URLs, e.g. `["nats://127.0.0.1:4222"]`. The
    /// client connects to the first reachable server and uses the rest for
    /// failover.
    #[serde(default = "default_servers")]
    pub servers: Vec<String>,
    /// Authentication mode. Defaults to [`NatsAuth::None`] (anonymous).
    #[serde(default)]
    pub auth: NatsAuth,
    /// Require a TLS connection to the server. Defaults to `false`.
    #[serde(default)]
    pub tls: bool,
    /// Optional client connection name (surfaced in NATS server monitoring).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Default for NatsConnectionConfig {
    fn default() -> Self {
        Self {
            servers: default_servers(),
            auth: NatsAuth::None,
            tls: false,
            name: None,
        }
    }
}

impl NatsConnectionConfig {
    /// Validate the connection settings. Callers should run this at config-load
    /// time (a connector's `validate`), before any lazy connect.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.servers.is_empty() {
            return Err(FaucetError::Config(
                "nats: `servers` must contain at least one server URL".into(),
            ));
        }
        if self.servers.iter().any(|s| s.trim().is_empty()) {
            return Err(FaucetError::Config(
                "nats: `servers` entries must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// Reconnect attempts after a lost connection before the client gives up
/// (each waits with the client's growing backoff, a few minutes in total).
pub const MAX_RECONNECTS: usize = 60;

/// Connect to NATS using the shared connection config.
///
/// Applies the configured authentication mode, TLS requirement and connection
/// name, then dials the server list. This does **not** enable
/// retry-on-initial-connect, so an unreachable server surfaces as an immediate
/// typed error rather than blocking — which is what lets a lazy connector's
/// first poll fail cleanly.
pub async fn connect(cfg: &NatsConnectionConfig) -> Result<async_nats::Client, FaucetError> {
    cfg.validate()?;

    let mut options = async_nats::ConnectOptions::new();

    match &cfg.auth {
        NatsAuth::None => {}
        NatsAuth::Token { token } => {
            options = options.token(token.clone());
        }
        NatsAuth::UserPassword { username, password } => {
            options = options.user_and_password(username.clone(), password.clone());
        }
        NatsAuth::NKey { nkey } => {
            options = options.nkey(nkey.clone());
        }
        NatsAuth::CredsFile { path } => {
            options = options.credentials_file(path).await.map_err(|e| {
                FaucetError::Config(format!(
                    "nats: failed to read credentials file '{}': {e}",
                    path.display()
                ))
            })?;
        }
    }

    if cfg.tls {
        options = options.require_tls(true);
    }
    // async-nats reconnects forever by default, so a long outage would wedge a
    // run with no error (#789 MSG-59).
    options = options.max_reconnects(MAX_RECONNECTS);
    if let Some(name) = &cfg.name {
        options = options.name(name.clone());
    }

    async_nats::connect_with_options(cfg.servers.clone(), options)
        .await
        .map_err(|e| FaucetError::Custom(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_has_one_server_and_no_auth() {
        let cfg = NatsConnectionConfig::default();
        assert_eq!(cfg.servers.len(), 1);
        assert!(matches!(cfg.auth, NatsAuth::None));
        assert!(!cfg.tls);
        assert!(cfg.name.is_none());
    }

    #[test]
    fn validate_rejects_empty_servers() {
        let cfg = NatsConnectionConfig {
            servers: vec![],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_blank_server_entry() {
        let cfg = NatsConnectionConfig {
            servers: vec!["   ".into()],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn deserializes_with_flattened_defaults() {
        let cfg: NatsConnectionConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.servers, vec!["nats://127.0.0.1:4222".to_string()]);
    }

    #[test]
    fn deserializes_full() {
        let cfg: NatsConnectionConfig = serde_json::from_value(json!({
            "servers": ["nats://a:4222", "nats://b:4222"],
            "auth": {"type": "token", "config": {"token": "t"}},
            "tls": true,
            "name": "faucet"
        }))
        .unwrap();
        assert_eq!(cfg.servers.len(), 2);
        assert!(cfg.tls);
        assert_eq!(cfg.name.as_deref(), Some("faucet"));
        assert!(matches!(cfg.auth, NatsAuth::Token { .. }));
    }

    #[tokio::test]
    async fn connect_to_unreachable_server_errors_not_panics() {
        let cfg = NatsConnectionConfig {
            servers: vec!["nats://127.0.0.1:1".into()],
            ..Default::default()
        };
        let result = connect(&cfg).await;
        assert!(
            result.is_err(),
            "expected connect to fail on unreachable server"
        );
    }
}
