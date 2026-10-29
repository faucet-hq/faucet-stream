//! The named `auth:` provider catalog.
//!
//! [`build_auth_catalog`] turns a config's `auth:` block (a map of named
//! `{ type, config }` specs) into a map of shared [`SharedAuthProvider`]s, built
//! **once** so that every connector referencing a provider via `auth: { ref }`
//! gets the *same* `Arc` — one token cache, single-flight refresh, shared across
//! every connector built from the catalog.
//!
//! Specs are used exactly as given: no `${…}` reference is resolved and no
//! escape is undone (see the crate docs, "Configs are used as given").

use std::collections::HashMap;

use faucet_core::SharedAuthProvider;
use serde_json::Value;

use crate::error::{RegistryError, RegistryResult};

/// Name → shared provider. An empty catalog is valid (configs with no `auth:`
/// block); a connector that then references `{ ref }` errors with
/// [`RegistryError::UnknownAuthProvider`].
pub type AuthCatalog = HashMap<String, SharedAuthProvider>;

/// Build the catalog from a config's optional `auth:` block.
///
/// A spec that fails to build is reported as
/// [`RegistryError::AuthProviderBuild`] naming the provider.
pub fn build_auth_catalog(specs: Option<&HashMap<String, Value>>) -> RegistryResult<AuthCatalog> {
    let mut catalog = AuthCatalog::new();
    let Some(specs) = specs else {
        return Ok(catalog);
    };
    for (name, spec) in specs {
        let provider = build_provider(spec).map_err(|e| RegistryError::AuthProviderBuild {
            name: name.clone(),
            message: e.to_string(),
        })?;
        catalog.insert(name.clone(), provider);
    }
    Ok(catalog)
}

/// Build one provider from an `auth:` spec (`{ type, config }`).
///
/// The key material of a `google_service_account` spec (its key JSON, private
/// key and the key's lines) is handed to the secret registrar
/// ([`crate::secrets`]) so a host can scrub it from its output, even when it
/// was written inline.
pub fn build_provider(spec: &Value) -> Result<SharedAuthProvider, faucet_core::FaucetError> {
    register_key_material(spec);
    faucet_auth::build_provider(spec)
}

/// Report a `google_service_account` key's private key (and the raw key JSON)
/// to the secret registrar (#755).
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
    crate::secrets::report(&raw);
    if let Some(pk) = serde_json::from_str::<Value>(&raw).ok().and_then(|v| {
        v.get("private_key")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }) {
        crate::secrets::report(&pk);
        for line in pk
            .lines()
            .filter(|l| l.len() >= 16 && !l.starts_with("-----"))
        {
            crate::secrets::report(line);
        }
    }
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
pub fn resolve(catalog: &AuthCatalog, name: &str) -> RegistryResult<SharedAuthProvider> {
    catalog
        .get(name)
        .cloned()
        .ok_or_else(|| RegistryError::UnknownAuthProvider {
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
        assert!(matches!(err, RegistryError::UnknownAuthProvider { .. }));
        assert_eq!(
            err.to_string(),
            "auth references unknown provider 'missing'. Declared providers: tok"
        );
        let empty = AuthCatalog::new();
        assert_eq!(
            resolve(&empty, "x").unwrap_err().to_string(),
            "auth references unknown provider 'x'. Declared providers: (none)"
        );
    }

    #[tokio::test]
    async fn a_spec_is_used_as_given() {
        let spec = serde_json::json!({"type": "static", "config": {"token": "abc$${x}"}});
        let cred = build_provider(&spec).unwrap().credential().await.unwrap();
        assert_eq!(cred, faucet_core::Credential::Bearer("abc$${x}".into()));
    }

    #[test]
    fn bad_spec_errors_with_name() {
        let mut specs = HashMap::new();
        specs.insert("bad".to_string(), serde_json::json!({"type": "nope"}));
        let err = build_auth_catalog(Some(&specs)).unwrap_err();
        assert!(matches!(&err, RegistryError::AuthProviderBuild { name, .. } if name == "bad"));
        assert!(
            err.to_string()
                .starts_with("failed to build auth provider 'bad': "),
            "{err}"
        );
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
    fn service_account_key_material_is_reported() {
        let reported = crate::secrets::test_support::reported;
        crate::secrets::test_support::install();
        let pk = "-----BEGIN PRIVATE KEY-----\nQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755\n-----END PRIVATE KEY-----\n";
        let key = serde_json::json!({"client_email": "a@b", "private_key": pk});
        register_key_material(&serde_json::json!({
            "type": "google_service_account",
            "config": {"key_json": key}
        }));
        assert!(reported("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo755"));
        assert!(reported(pk));

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
        assert!(reported("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo766"));
        let raw = serde_json::json!({"client_email": "a@b", "private_key": "plain"}).to_string();
        register_key_material(&serde_json::json!({
            "type": "google_service_account",
            "config": {"key_json": raw}
        }));
        assert!(reported(&raw));
        register_key_material(&serde_json::json!({"type": "static", "config": {"token": "t"}}));
        register_key_material(&serde_json::json!({"type": "google_service_account"}));
        register_key_material(
            &serde_json::json!({"type": "google_service_account", "config": {"key_json": 5}}),
        );
    }
}
