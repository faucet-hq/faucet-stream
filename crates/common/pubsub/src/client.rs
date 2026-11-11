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

/// The config fields that make a Pub/Sub client built from `config`
/// authenticate with the identity of the host it runs on: `credentials` when
/// it selects `application_default` (also when absent) and no
/// `emulator_host` is set.
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    use faucet_core::capabilities::config::{non_empty_str_at, tag_at};
    let adc =
        tag_at(config, "/credentials").unwrap_or("application_default") == "application_default";
    if adc && !non_empty_str_at(config, "/emulator_host") {
        vec!["credentials".to_string()]
    } else {
        Vec::new()
    }
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
    let mut config = ClientConfig {
        connection_option: connection_options(),
        ..Default::default()
    };
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
            PubsubCredentials::ApplicationDefault => {
                faucet_core::resolve::require_host_credentials("pubsub", "credentials")?;
                config
                    .with_auth()
                    .await
                    .map_err(|e| auth_err("application default", e))?
            }
            PubsubCredentials::ServiceAccountJsonFile { path } => {
                faucet_core::resolve::check_google_credentials_file(
                    "pubsub",
                    "credentials",
                    std::path::Path::new(path),
                )?;
                let cf = CredentialsFile::new_from_file(path.clone())
                    .await
                    .map_err(|e| auth_err("service-account file", e))?;
                config
                    .with_credentials(cf)
                    .await
                    .map_err(|e| auth_err("service-account file", e))?
            }
            PubsubCredentials::ServiceAccountJsonInline { json } => {
                faucet_core::resolve::check_google_credentials("pubsub", "credentials", json)?;
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

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        use faucet_core::json;
        assert_eq!(host_identity(&json!({})), vec!["credentials"]);
        assert!(host_identity(&json!({"emulator_host": "localhost:8085"})).is_empty());
        for t in [
            "anonymous",
            "service_account_json_file",
            "service_account_json_inline",
        ] {
            assert!(
                host_identity(&json!({"credentials": {"type": t}})).is_empty(),
                "{t}"
            );
        }
    }

    #[tokio::test]
    async fn caller_runs_take_only_service_account_keys() {
        if std::env::var_os("PUBSUB_EMULATOR_HOST").is_some() {
            return;
        }
        let conn = PubsubConnection {
            project_id: Some("p".into()),
            credentials: PubsubCredentials::ServiceAccountJsonInline {
                json: r#"{"type": "authorized_user", "client_id": "c", "client_secret": "s",
                    "refresh_token": "r", "token_uri": "http://127.0.0.1:1/token"}"#
                    .into(),
            },
            ..Default::default()
        };
        let caller = std::sync::Arc::new(
            faucet_core::resolve::ReferencePolicy::deny_all()
                .with_path_roots(["/srv/data"])
                .any_url_scheme()
                .any_url_host(),
        );
        let err = faucet_core::http::with_reference_policy(caller, build_client(&conn))
            .await
            .expect_err("refused")
            .to_string();
        assert!(err.contains("pubsub: `credentials`"), "{err}");
        assert!(err.contains("authorized_user"), "{err}");
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        if std::env::var_os("PUBSUB_EMULATOR_HOST").is_some() {
            return;
        }
        let conn = PubsubConnection {
            project_id: Some("p".into()),
            credentials: PubsubCredentials::ApplicationDefault,
            ..Default::default()
        };
        let err = faucet_core::resolve::with_host_credentials(false, build_client(&conn))
            .await
            .expect_err("refused")
            .to_string();
        assert!(err.contains("pubsub: `credentials`"), "{err}");
        let emulated = PubsubConnection {
            emulator_host: Some("127.0.0.1:1".into()),
            ..conn
        };
        let emulated = faucet_core::resolve::with_host_credentials(false, build_client(&emulated))
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(!emulated.contains("--allow-host-credentials"), "{emulated}");
    }

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
