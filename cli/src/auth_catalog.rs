//! The top-level `auth:` provider catalog.
//!
//! [`build_auth_catalog`] turns the config's `auth:` block (a map of named
//! `{ type, config }` specs) into a map of shared [`SharedAuthProvider`]s, built
//! **once** so that every connector referencing a provider via `auth: { ref }`
//! gets the *same* `Arc` — one token cache, single-flight refresh, shared across
//! all matrix rows.

use std::collections::HashMap;

use faucet_core::SharedAuthProvider;
use serde_json::Value;

use crate::error::{CliError, CliResult};

/// Name → shared provider. An empty catalog is valid (configs with no `auth:`
/// block); a connector that then references `{ ref }` errors with
/// [`CliError::UnknownAuthProvider`].
pub type AuthCatalog = HashMap<String, SharedAuthProvider>;

/// Build the catalog from the config's optional `auth:` block.
pub fn build_auth_catalog(specs: Option<&HashMap<String, Value>>) -> CliResult<AuthCatalog> {
    let mut catalog = AuthCatalog::new();
    let Some(specs) = specs else {
        return Ok(catalog);
    };
    for (name, spec) in specs {
        register_key_material(spec);
        let provider =
            faucet_auth::build_provider(spec).map_err(|e| CliError::AuthProviderBuild {
                name: name.clone(),
                message: e.to_string(),
            })?;
        catalog.insert(name.clone(), provider);
    }
    Ok(catalog)
}

/// Register a `google_service_account` key's private key (and the raw key
/// JSON) for log redaction, even when it was written inline rather than
/// resolved from a secrets manager (#755).
fn register_key_material(spec: &Value) {
    if spec.get("type").and_then(Value::as_str) != Some("google_service_account") {
        return;
    }
    let Some(config) = spec.get("config") else {
        return;
    };
    let raw = match (config.get("key_json"), config.get("key_file")) {
        (Some(Value::String(s)), _) => Some(s.clone()),
        (Some(v @ Value::Object(_)), _) => Some(v.to_string()),
        (_, Some(Value::String(path))) => std::fs::read_to_string(path).ok(),
        _ => None,
    };
    let Some(raw) = raw else {
        return;
    };
    crate::secrets::registry::register(&raw);
    if let Some(pk) = serde_json::from_str::<Value>(&raw).ok().and_then(|v| {
        v.get("private_key")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }) {
        crate::secrets::registry::register(&pk);
        for line in pk
            .lines()
            .filter(|l| l.len() >= 16 && !l.starts_with("-----"))
        {
            crate::secrets::registry::register(line);
        }
    }
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
        if let Ok(key) = faucet_auth::legacy_persisted_refresh_key(&specs[name]).await {
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

/// Extract a connector config's `auth: { ref: <name> }` reference, if present.
/// Returns `None` for inline auth or no auth.
pub fn auth_ref(config: &Value) -> Option<String> {
    config
        .get("auth")
        .and_then(|a| a.get("ref"))
        .and_then(|r| r.as_str())
        .map(String::from)
}

/// Resolve a provider name against the catalog, or error with the known names.
pub fn resolve(catalog: &AuthCatalog, name: &str) -> CliResult<SharedAuthProvider> {
    catalog
        .get(name)
        .cloned()
        .ok_or_else(|| CliError::UnknownAuthProvider {
            name: name.to_string(),
            known: {
                let mut k: Vec<String> = catalog.keys().cloned().collect();
                k.sort();
                k
            },
        })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn bad_spec_errors_with_name() {
        let mut specs = HashMap::new();
        specs.insert("bad".to_string(), serde_json::json!({"type": "nope"}));
        let err = build_auth_catalog(Some(&specs)).unwrap_err();
        assert!(matches!(err, CliError::AuthProviderBuild { name, .. } if name == "bad"));
    }

    #[test]
    fn auth_ref_extracts_reference() {
        assert_eq!(
            auth_ref(&serde_json::json!({"auth": {"ref": "sf"}})),
            Some("sf".to_string())
        );
        assert_eq!(
            auth_ref(&serde_json::json!({"auth": {"type": "bearer", "config": {"token": "x"}}})),
            None
        );
        assert_eq!(auth_ref(&serde_json::json!({})), None);
    }

    #[test]
    fn service_account_key_material_is_registered_for_redaction() {
        let pk = "-----BEGIN PRIVATE KEY-----\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755\n-----END PRIVATE KEY-----\n";
        let key = serde_json::json!({"client_email": "a@b", "private_key": pk});
        register_key_material(&serde_json::json!({
            "type": "google_service_account",
            "config": {"key_json": key}
        }));
        let out =
            crate::secrets::registry::redact("leak QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755 here");
        assert!(
            !out.contains("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755"),
            "{out}"
        );

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("k.json");
        let pk2 = pk.replace("755", "766");
        std::fs::write(
            &file,
            serde_json::json!({"client_email": "a@b", "private_key": pk2}).to_string(),
        )
        .unwrap();
        register_key_material(&serde_json::json!({
            "type": "google_service_account",
            "config": {"key_file": file.to_str().unwrap()}
        }));
        let out = crate::secrets::registry::redact("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo766");
        assert!(!out.contains("766"), "{out}");
        register_key_material(&serde_json::json!({"type": "static", "config": {"token": "t"}}));
        register_key_material(&serde_json::json!({"type": "google_service_account"}));
        register_key_material(
            &serde_json::json!({"type": "google_service_account", "config": {"key_json": 5}}),
        );
    }
}
