//! The top-level `auth:` provider catalog.
//!
//! The catalog lives in [`faucet_registry::auth_catalog`] (#995); this module
//! re-exports it under its old path. The CLI's loader leaves `$${` escapes in
//! `auth:` specs, so every spec is unescaped here, exactly once, before the
//! registry builds it, and key material found while building is added to the
//! CLI's redaction registry. The `faucet validate` legacy-persist warnings
//! stay here.

use std::collections::HashMap;

use faucet_core::SharedAuthProvider;
use serde_json::Value;

use crate::error::CliResult;

pub use faucet_registry::auth_catalog::{AuthCatalog, auth_ref};

/// Build the catalog from the config's optional `auth:` block.
pub fn build_auth_catalog(specs: Option<&HashMap<String, Value>>) -> CliResult<AuthCatalog> {
    crate::registry::install_secret_registrar();
    let unescaped = specs.map(|specs| {
        specs
            .iter()
            .map(|(k, v)| (k.clone(), crate::interpolate::unescaped(v.clone())))
            .collect::<HashMap<_, _>>()
    });
    Ok(faucet_registry::auth_catalog::build_auth_catalog(
        unescaped.as_ref(),
    )?)
}

/// Build one provider from a config-authored `auth:` spec. The load leaves
/// `$${` escapes in `auth:` specs, so every caller that builds one goes through
/// here to see the same value.
pub fn build_provider(spec: &Value) -> Result<SharedAuthProvider, faucet_core::FaucetError> {
    crate::registry::install_secret_registrar();
    faucet_registry::auth_catalog::build_provider(&crate::interpolate::unescaped(spec.clone()))
}

/// Resolve a provider name against the catalog, or error with the known names.
pub fn resolve(catalog: &AuthCatalog, name: &str) -> CliResult<SharedAuthProvider> {
    Ok(faucet_registry::auth_catalog::resolve(catalog, name)?)
}

/// The remedy for a refresh token left under the shared persist key earlier
/// versions used (#862 M-077).
pub fn legacy_persist_message(name: &str, key: &str) -> String {
    format!(
        "auth provider `{name}` (oauth2_refresh): its rotated refresh token is persisted under \
         `{key}`, the shared key earlier faucet versions used, which this version no longer \
         reads — the next refresh would send the config's seed token, which a rotating IdP has \
         already revoked. If `{name}` is the only grant of its app, add `key: {key}` to its \
         `persist:` block"
    )
}

/// `oauth2_refresh` providers in the `auth:` catalog that use a `persist:`
/// block, each with the legacy key its token still sits under (`None` when
/// nothing needs migrating). Read-only; a provider that does not build is left
/// to the catalog build to report.
pub async fn legacy_persist_status(
    specs: Option<&HashMap<String, Value>>,
) -> Vec<(String, Option<String>)> {
    let Some(specs) = specs else {
        return Vec::new();
    };
    let mut names: Vec<&String> = specs
        .iter()
        .filter(|(_, s)| {
            s.get("type").and_then(Value::as_str) == Some("oauth2_refresh")
                && s.pointer("/config/persist").is_some_and(|p| !p.is_null())
        })
        .map(|(n, _)| n)
        .collect();
    names.sort();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let spec = crate::interpolate::unescaped(specs[name].clone());
        if let Ok(key) = faucet_auth::legacy_persisted_refresh_key(&spec).await {
            out.push((name.clone(), key));
        }
    }
    out
}

/// The `faucet validate` warnings for [`legacy_persist_status`].
pub async fn legacy_persist_warnings(specs: Option<&HashMap<String, Value>>) -> Vec<String> {
    legacy_persist_status(specs)
        .await
        .into_iter()
        .filter_map(|(name, key)| key.map(|k| legacy_persist_message(&name, &k)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;

    #[tokio::test]
    async fn a_provider_is_built_from_the_unescaped_spec() {
        let spec = serde_json::json!({"type": "static", "config": {"token": "abc$${x}"}});
        let cred = build_provider(&spec).unwrap().credential().await.unwrap();
        assert_eq!(cred, faucet_core::Credential::Bearer("abc${x}".into()));
    }

    #[tokio::test]
    async fn legacy_persist_status_covers_only_persisting_refresh_providers() {
        use faucet_core::StateStore as _;
        assert!(legacy_persist_status(None).await.is_empty());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let refresh = |persist: Value| {
            serde_json::json!({ "type": "oauth2_refresh", "config": {
                "token_url": "https://idp/token", "client_id": "app",
                "client_secret": "s", "refresh_token": "seed", "persist": persist } })
        };
        let mut specs = HashMap::new();
        specs.insert(
            "b".to_string(),
            refresh(serde_json::json!({ "path": path })),
        );
        specs.insert(
            "a".to_string(),
            refresh(serde_json::json!({ "path": path, "key": "k" })),
        );
        specs.insert("mem".to_string(), refresh(Value::Null));
        specs.insert(
            "broken".to_string(),
            refresh(serde_json::json!({ "path": "" })),
        );
        specs.insert(
            "static".to_string(),
            serde_json::json!({ "type": "static", "config": { "token": "t" } }),
        );
        assert_eq!(
            legacy_persist_status(Some(&specs)).await,
            vec![("a".to_string(), None), ("b".to_string(), None)]
        );
        assert!(legacy_persist_warnings(Some(&specs)).await.is_empty());

        let legacy = faucet_auth::legacy_persisted_refresh_key(&specs["b"]).await;
        assert_eq!(legacy.unwrap(), None);
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in "https://idp/token\u{0}app".as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let key = format!("oauth2_refresh_{hash:016x}");
        faucet_core::FileStateStore::new(path)
            .put(&key, &serde_json::json!({ "refresh_token": "old" }))
            .await
            .unwrap();
        let warnings = legacy_persist_warnings(Some(&specs)).await;
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("`b`") && warnings[0].contains(&format!("key: {key}")));
    }

    #[test]
    fn empty_block_yields_empty_catalog() {
        assert!(build_auth_catalog(None).unwrap().is_empty());
    }

    #[test]
    fn builds_static_provider_and_resolves() {
        let mut specs = HashMap::new();
        specs.insert(
            "tok".to_string(),
            serde_json::json!({"type": "static", "config": {"token": "abc"}}),
        );
        let catalog = build_auth_catalog(Some(&specs)).unwrap();
        assert!(resolve(&catalog, "tok").is_ok());
        let err = resolve(&catalog, "missing").unwrap_err();
        assert!(matches!(err, CliError::UnknownAuthProvider { .. }));
    }

    #[tokio::test]
    async fn escaped_token_is_built_literal() {
        let mut specs = HashMap::new();
        specs.insert(
            "tok".to_string(),
            serde_json::json!({"type": "static", "config": {"token": "$${vars.literal}"}}),
        );
        let catalog = build_auth_catalog(Some(&specs)).unwrap();
        let cred = resolve(&catalog, "tok")
            .unwrap()
            .credential()
            .await
            .unwrap();
        assert_eq!(
            cred.authorization_value().as_deref(),
            Some("Bearer ${vars.literal}")
        );
    }

    #[test]
    fn bad_spec_errors_with_name() {
        let mut specs = HashMap::new();
        specs.insert("bad".to_string(), serde_json::json!({"type": "nope"}));
        let err = build_auth_catalog(Some(&specs)).unwrap_err();
        assert!(matches!(err, CliError::AuthProviderBuild { name, .. } if name == "bad"));
    }

    #[test]
    fn service_account_key_material_is_registered_for_redaction() {
        let pk = "-----BEGIN PRIVATE KEY-----\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755\n-----END PRIVATE KEY-----\n";
        let key = serde_json::json!({"client_email": "a@b", "private_key": pk});
        let _ = build_provider(&serde_json::json!({
            "type": "google_service_account",
            "config": {"key_json": key}
        }));
        let out =
            crate::secrets::registry::redact("leak QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755 here");
        assert!(
            !out.contains("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755"),
            "{out}"
        );
    }
}
