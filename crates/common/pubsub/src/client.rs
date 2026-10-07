//! Pub/Sub client construction. **This is the only module in the crate that
//! touches the `gcloud-pubsub` / `gcloud-auth` SDK**, so a real-compile fixup
//! (if the pinned SDK version's API differs) is localised here.

use crate::config::{PubsubConnection, PubsubCredentials};
use faucet_core::FaucetError;
use gcloud_auth::credentials::CredentialsFile;
use gcloud_gax::conn::{ConnectionOptions, Environment};
use gcloud_pubsub::client::{Client, ClientConfig};
use std::time::Duration;

/// Per-RPC deadline: a call outliving it fails instead of hanging the run.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// gRPC connection options: a connect timeout, a per-RPC deadline and HTTP/2
/// keepalive pings, so a connection a NAT or conntrack table silently dropped
/// is detected instead of wedging the run (#789 MSG-60).
pub fn connection_options() -> ConnectionOptions {
    ConnectionOptions {
        timeout: Some(RPC_TIMEOUT),
        connect_timeout: Some(Duration::from_secs(10)),
        http2_keep_alive_interval: Some(Duration::from_secs(30)),
        keep_alive_timeout: Some(Duration::from_secs(20)),
        keep_alive_while_idle: Some(true),
    }
}

fn auth_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Auth(format!("pubsub auth ({context}): {e}"))
}

/// Build a Pub/Sub [`Client`] from a [`PubsubConnection`].
///
/// * When an emulator host is configured (config `emulator_host` or the
///   `PUBSUB_EMULATOR_HOST` env var), auth is skipped entirely and the client
///   talks plaintext to the emulator.
/// * Otherwise credentials are resolved per [`PubsubCredentials`] — ADC, a
///   service-account key file, or an inline key — and all failures map to
///   [`FaucetError::Auth`].
///
/// Construction is I/O-light: ADC token acquisition may touch the metadata
/// server, but gRPC channels connect lazily on first RPC.
pub async fn build_client(conn: &PubsubConnection) -> Result<Client, FaucetError> {
    // An explicit `emulator_host` is set on this client's config only. It used
    // to be exported as `PUBSUB_EMULATOR_HOST`, which redirected every later
    // Pub/Sub client in the process (a `faucet serve` or matrix run) to the
    // emulator, and wrote the environment from a multi-threaded runtime
    // (#789 MSG-38). The SDK still honours the variable when it is set.
    let mut config = ClientConfig::default();
    config.connection_option = connection_options();
    if let Some(host) = conn.explicit_emulator_host() {
        config.environment = Environment::Emulator(host);
        if config.project_id.is_none() {
            config.project_id = Some("local-project".to_string());
        }
    }
    if let Some(project) = &conn.project_id {
        config.project_id = Some(project.clone());
    }
    if let Some(endpoint) = &conn.endpoint {
        config.endpoint = endpoint.clone();
    }

    // Emulator (or explicitly anonymous): no credentials.
    let use_auth = conn.effective_emulator_host().is_none()
        && conn.credentials != PubsubCredentials::Anonymous;

    let config = if use_auth {
        match &conn.credentials {
            PubsubCredentials::Anonymous => config, // unreachable: filtered above
            PubsubCredentials::ApplicationDefault => config
                .with_auth()
                .await
                .map_err(|e| auth_err("application default", e))?,
            PubsubCredentials::ServiceAccountJsonFile { path } => {
                let cf = CredentialsFile::new_from_file(path.clone())
                    .await
                    .map_err(|e| auth_err("service-account file", e))?;
                config
                    .with_credentials(cf)
                    .await
                    .map_err(|e| auth_err("service-account file", e))?
            }
            PubsubCredentials::ServiceAccountJsonInline { json } => {
                let cf = CredentialsFile::new_from_str(json)
                    .await
                    .map_err(|e| auth_err("inline service-account key", e))?;
                config
                    .with_credentials(cf)
                    .await
                    .map_err(|e| auth_err("inline service-account key", e))?
            }
        }
    } else {
        config
    };

    Client::new(config)
        .await
        .map_err(|e| FaucetError::Source(format!("pubsub: client build failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicit `emulator_host` configures this client only; it no longer
    /// leaks into the process environment for every later client (#789
    /// MSG-38).
    #[tokio::test]
    async fn an_explicit_emulator_host_never_touches_the_environment() {
        assert!(std::env::var_os("PUBSUB_EMULATOR_HOST").is_none());
        let conn = PubsubConnection {
            emulator_host: Some("127.0.0.1:1".into()),
            credentials: PubsubCredentials::ApplicationDefault,
            ..Default::default()
        };
        // Nothing listens there; only the environment matters.
        let _ = build_client(&conn).await;
        assert!(std::env::var_os("PUBSUB_EMULATOR_HOST").is_none());
        let opts = connection_options();
        assert_eq!(opts.timeout, Some(RPC_TIMEOUT));
        assert_eq!(opts.keep_alive_while_idle, Some(true));
    }
}
