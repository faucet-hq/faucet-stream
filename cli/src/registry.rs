//! Feature-gated dispatch from a string `type` to a concrete connector.
//!
//! The registry itself lives in [`faucet_registry`] (#995), a semver-checked
//! library crate; this module re-exports it under its old path and adds the
//! CLI's side of each build. The CLI's loader leaves `$${` escapes in
//! connector configs and `auth:` specs, so every config is unescaped here,
//! exactly once, before the registry sees it; the CLI trusts the configs it
//! loads, so connectors that run programs are built
//! ([`BuildPolicy::trusted`]); and secrets found while building are added to
//! the CLI's redaction registry. The fallible entry points keep returning
//! [`CliResult`], with the registry's error rendered unchanged.

pub use faucet_registry::internal::*;
pub use faucet_registry::*;

use crate::error::CliResult;
use faucet_core::{Sink, Source};
use serde_json::Value;

/// Route the registry's secret reports into the CLI's redaction registry.
/// Idempotent; called before every build.
pub fn install_secret_registrar() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        faucet_registry::secrets::install(Box::new(|s: &str| {
            crate::secrets::registry::register(s);
        }));
    });
}

/// Build a [`Source`] trait object from a `(kind, config)` pair. When the
/// config carries `auth: { ref: <name> }`, the named provider is resolved from
/// `auth` (the catalog) and injected into the connector.
pub async fn build_source(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    retry_policy: Option<&faucet_core::RetryPolicy>,
) -> CliResult<Box<dyn Source>> {
    install_secret_registrar();
    Ok(faucet_registry::build_source_with(
        kind,
        crate::interpolate::unescaped(config),
        auth,
        retry_policy,
        &BuildPolicy::trusted(),
    )
    .await?)
}

/// Build a [`Sink`] trait object from a `(kind, config)` pair. See
/// [`build_source`].
pub async fn build_sink(kind: &str, config: Value, auth: &AuthCatalog) -> CliResult<Box<dyn Sink>> {
    install_secret_registrar();
    Ok(faucet_registry::build_sink_with(
        kind,
        crate::interpolate::unescaped(config),
        auth,
        &BuildPolicy::trusted(),
    )
    .await?)
}

/// Validate a source's `config` without building it
/// ([`faucet_registry::validate_source_config`]).
pub fn validate_source_config(kind: &str, name: &str, config: Value) -> CliResult<()> {
    Ok(faucet_registry::validate_source_config(
        kind,
        name,
        crate::interpolate::unescaped(config),
    )?)
}

/// Validate a sink's `config` without building it
/// ([`faucet_registry::validate_sink_config`]).
pub fn validate_sink_config(kind: &str, name: &str, config: Value) -> CliResult<()> {
    Ok(faucet_registry::validate_sink_config(
        kind,
        name,
        crate::interpolate::unescaped(config),
    )?)
}

/// The JSON Schema of a source kind's config.
pub fn source_schema(kind: &str) -> CliResult<Value> {
    Ok(faucet_registry::source_schema(kind)?)
}

/// The JSON Schema of a sink kind's config.
pub fn sink_schema(kind: &str) -> CliResult<Value> {
    Ok(faucet_registry::sink_schema(kind)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;
    use serde_json::json;

    #[test]
    fn errors_keep_their_cli_variants_and_text() {
        match source_schema("no-such-source").unwrap_err() {
            CliError::UnknownConnector {
                kind: "source",
                name,
                ..
            } => assert_eq!(name, "no-such-source"),
            other => panic!("expected UnknownConnector, got {other:?}"),
        }
        let err = sink_schema("no-such-sink").unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unknown sink 'no-such-sink'. Available: "),
            "{err}"
        );
        assert!(validate_source_config("no-such-source", "r", json!({})).is_err());
        assert!(validate_sink_config("no-such-sink", "r", json!({})).is_err());
    }

    #[test]
    fn first_party_kinds_match_the_verified_index() {
        let index = crate::registry_index::RegistryIndex::embedded();
        for (role, listed) in [("source", FIRST_PARTY_SOURCES), ("sink", FIRST_PARTY_SINKS)] {
            let mut verified: Vec<String> = index
                .connectors
                .iter()
                .filter(|c| c.verified && c.kind == role)
                .map(|c| c.name.clone())
                .collect();
            verified.sort();
            let mut listed: Vec<String> = listed.iter().map(|s| s.to_string()).collect();
            listed.sort();
            assert_eq!(
                listed,
                verified,
                "faucet_registry::internal::FIRST_PARTY_{}S must list every verified {role} in \
                 cli/connectors/registry.json",
                role.to_uppercase()
            );
        }
    }

    #[cfg(feature = "sink-jsonl")]
    #[tokio::test]
    async fn a_sink_config_is_unescaped_once_before_it_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let escaped = dir.path().join("a$${x}.jsonl");
        let sink = build_sink(
            "jsonl",
            json!({ "path": escaped.to_str().unwrap() }),
            &AuthCatalog::new(),
        )
        .await
        .unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        assert!(dir.path().join("a${x}.jsonl").exists());
        assert!(!escaped.exists());
    }

    #[cfg(feature = "sink-singer")]
    #[tokio::test]
    async fn singer_sink_builds_and_its_secrets_are_redacted() {
        let cfg = json!({
            "target_command": "target-jsonl",
            "target_config": {"api_token": "singer-cli-secret-5678"},
            "stream": "s"
        });
        let sink = build_sink("singer", cfg.clone(), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(sink.connector_name(), "singer");
        assert_eq!(
            crate::secrets::registry::redact("x singer-cli-secret-5678 y"),
            "x *** y"
        );
        assert!(validate_sink_config("singer", "row", cfg).is_ok());
    }
}
