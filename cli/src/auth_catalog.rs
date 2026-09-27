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
