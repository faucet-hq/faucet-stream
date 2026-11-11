//! Google credential JSON (a service-account key, an `authorized_user` or
//! `external_account` file) a config hands to a Google client library.
//! Re-exported from [`crate::resolve`].
//!
//! A config the host does not trust (one run under a reference policy with
//! path roots) may use only a `service_account` key, and the key's
//! `token_uri` must pass that policy: the library posts to it while the
//! client is built, outside the policy's other checks.

use crate::error::FaucetError;
use crate::resolve::ReferencePolicy;
use std::sync::Arc;

/// The largest key file read to check it.
const MAX_KEY_FILE_BYTES: u64 = 1 << 20;

/// The policy a config the host does not trust runs under, if the current
/// task runs one.
fn caller_policy() -> Option<Arc<ReferencePolicy>> {
    match crate::http::current_reference_policy() {
        Some(p) => p.path_roots().is_some().then_some(p),
        None if crate::resolve::scoped_host_credentials().is_none()
            && crate::resolve::unscoped_host_credentials_denied() =>
        {
            Some(Arc::new(
                ReferencePolicy::deny_all().any_url_scheme().any_url_host(),
            ))
        }
        None => None,
    }
}

fn refused(connector: &str, field: &str, reason: impl std::fmt::Display) -> FaucetError {
    FaucetError::Config(format!("{connector}: `{field}`: {reason}"))
}

fn check_json(
    policy: &ReferencePolicy,
    connector: &str,
    field: &str,
    json: &str,
) -> Result<(), FaucetError> {
    let Ok(key) = serde_json::from_str::<serde_json::Value>(json) else {
        return Err(refused(
            connector,
            field,
            "the credential JSON does not parse",
        ));
    };
    let kind = key.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if kind != "service_account" {
        return Err(refused(
            connector,
            field,
            format!(
                "credentials of type `{kind}` are not accepted for this config; use a \
                 service-account key (`type: service_account`)"
            ),
        ));
    }
    if let Some(uri) = key.get("token_uri") {
        let Some(uri) = uri.as_str() else {
            return Err(refused(connector, field, "`token_uri` must be a string"));
        };
        policy
            .check_url(uri)
            .map_err(|e| refused(connector, field, format!("`token_uri` {}", e.reason)))?;
    }
    Ok(())
}

/// Check Google credential JSON a `connector` was given in `field` before the
/// client is built. In a task running a config the host does not trust, only
/// a `service_account` key is accepted and its `token_uri` must pass that
/// task's reference policy; elsewhere nothing is checked.
pub fn check_google_credentials(
    connector: &str,
    field: &str,
    json: &str,
) -> Result<(), FaucetError> {
    match caller_policy() {
        Some(policy) => check_json(&policy, connector, field, json),
        None => Ok(()),
    }
}

/// [`check_google_credentials`] for a key file at `path`, read only when the
/// task runs a config the host does not trust.
pub fn check_google_credentials_file(
    connector: &str,
    field: &str,
    path: &std::path::Path,
) -> Result<(), FaucetError> {
    let Some(policy) = caller_policy() else {
        return Ok(());
    };
    let text = std::fs::File::open(path)
        .and_then(|f| {
            use std::io::Read;
            let mut text = String::new();
            f.take(MAX_KEY_FILE_BYTES).read_to_string(&mut text)?;
            Ok(text)
        })
        .map_err(|e| refused(connector, field, format!("reading the key file: {e}")))?;
    check_json(&policy, connector, field, &text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller() -> Arc<ReferencePolicy> {
        Arc::new(
            ReferencePolicy::deny_all()
                .with_path_roots(["/srv/data"])
                .any_url_scheme()
                .any_url_host(),
        )
    }

    const SA: &str =
        r#"{"type": "service_account", "token_uri": "https://oauth2.googleapis.com/token"}"#;

    #[tokio::test]
    async fn inline_credentials_other_than_service_accounts_are_refused() {
        let user =
            r#"{"type": "authorized_user", "token_uri": "https://oauth2.googleapis.com/token"}"#;
        let external = r#"{"type": "external_account", "credential_source": {"url": "http://169.254.169.254/x"}}"#;
        crate::http::with_reference_policy(caller(), async {
            for json in [user, external, "{}", "not json"] {
                let err = check_google_credentials("pubsub", "credentials", json)
                    .unwrap_err()
                    .to_string();
                assert!(err.contains("pubsub: `credentials`"), "{err}");
            }
            assert!(check_google_credentials("pubsub", "credentials", SA).is_ok());
            assert!(
                check_google_credentials("pubsub", "credentials", r#"{"type": "service_account"}"#)
                    .is_ok()
            );
        })
        .await;
        crate::http::with_reference_policy(Arc::new(ReferencePolicy::open()), async {
            assert!(check_google_credentials("pubsub", "credentials", user).is_ok());
        })
        .await;
    }

    #[tokio::test]
    async fn a_service_account_token_uri_must_be_googles_for_callers() {
        let internal =
            r#"{"type": "service_account", "token_uri": "http://169.254.169.254/token"}"#;
        let listed = Arc::new(
            ReferencePolicy::deny_all()
                .with_path_roots(["/srv/data"])
                .any_url_scheme()
                .with_url_hosts(["oauth2.googleapis.com", "accounts.google.com"]),
        );
        crate::http::with_reference_policy(listed, async {
            let err = check_google_credentials("spanner", "auth", internal)
                .unwrap_err()
                .to_string();
            assert!(err.contains("`token_uri`"), "{err}");
            let elsewhere = r#"{"type": "service_account", "token_uri": "https://tokens.example/t"}"#;
            assert!(check_google_credentials("spanner", "auth", elsewhere).is_err());
            assert!(check_google_credentials("spanner", "auth", SA).is_ok());
            let legacy = r#"{"type": "service_account", "token_uri": "https://accounts.google.com/o/oauth2/token"}"#;
            assert!(check_google_credentials("spanner", "auth", legacy).is_ok());
            let numeric = r#"{"type": "service_account", "token_uri": 7}"#;
            assert!(check_google_credentials("spanner", "auth", numeric).is_err());
        })
        .await;
        crate::http::with_reference_policy(caller(), async {
            assert!(check_google_credentials("spanner", "auth", internal).is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn key_files_are_read_only_for_callers() {
        let dir = std::env::temp_dir().join(format!("faucet-gcred-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let user = dir.join("user.json");
        std::fs::write(&user, r#"{"type": "authorized_user"}"#).unwrap();
        let sa = dir.join("sa.json");
        std::fs::write(&sa, SA).unwrap();
        let missing = dir.join("missing.json");
        let open = Arc::new(ReferencePolicy::open());
        crate::http::with_reference_policy(open, async {
            assert!(check_google_credentials_file("bigquery", "auth", &missing).is_ok());
            assert!(check_google_credentials_file("bigquery", "auth", &user).is_ok());
        })
        .await;
        crate::http::with_reference_policy(caller(), async {
            assert!(check_google_credentials_file("bigquery", "auth", &user).is_err());
            assert!(check_google_credentials_file("bigquery", "auth", &sa).is_ok());
            let err = check_google_credentials_file("bigquery", "auth", &missing)
                .unwrap_err()
                .to_string();
            assert!(err.contains("reading the key file"), "{err}");
        })
        .await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
