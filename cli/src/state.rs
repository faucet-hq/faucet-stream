//! Build a `StateStore` from a `(kind, config)` pair, with feature-gated
//! backends for Redis and PostgreSQL.

use crate::config::StateStoreSpec;
use crate::error::{CliError, CliResult};
use faucet_core::{FileStateStore, MemoryStateStore, StateStore};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

/// One JSON file per bookmark key, written with fsync and an atomic rename.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(title = "file state store")]
struct FileStateConfig {
    /// Directory holding one JSON file per bookmark.
    path: PathBuf,
    /// Encrypt bookmark files at rest (needs the `encryption` build feature).
    #[serde(default)]
    #[cfg_attr(
        feature = "encryption",
        schemars(with = "Option<faucet_core::EncryptionSpec>")
    )]
    encryption: Option<serde_json::Value>,
}

/// Bookmarks as Redis keys under a namespace.
#[cfg(feature = "state-redis")]
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(title = "redis state store")]
struct RedisStateConfig {
    /// Redis connection URL (`redis://` or `rediss://`).
    url: String,
    /// Prefix for every key, as `{namespace}:{key}`.
    #[serde(default = "default_redis_namespace")]
    namespace: String,
    /// TLS for a `rediss://` URL: private CA, client certificate.
    #[serde(default)]
    tls: faucet_state_redis::RedisTls,
}

#[cfg(feature = "state-redis")]
fn default_redis_namespace() -> String {
    "faucet".to_owned()
}

/// Bookmarks as rows of one table, upserted per commit.
#[cfg(feature = "state-postgres")]
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(title = "postgres state store")]
struct PostgresStateConfig {
    /// PostgreSQL connection URL.
    url: String,
    /// Bookmark table.
    #[serde(default = "default_pg_table")]
    table: String,
    /// Create the table if it does not exist.
    #[serde(default)]
    ensure_table: bool,
    /// Size of the connection pool backing the state store. Defaults to
    /// [`DEFAULT_PG_POOL_SIZE`] when omitted. Must be greater than zero.
    #[serde(default)]
    max_connections: Option<u32>,
}

#[cfg(feature = "state-postgres")]
fn default_pg_table() -> String {
    "faucet_state".to_owned()
}

/// Default Postgres state-store pool size, matching the connector's own
/// `PostgresStateStore::connect` default. Used when `max_connections` is absent.
#[cfg(feature = "state-postgres")]
const DEFAULT_PG_POOL_SIZE: u32 = 5;

/// Construct a state store from the parsed `state:` block.
pub async fn build_state_store(spec: &StateStoreSpec) -> CliResult<Arc<dyn StateStore>> {
    let unescaped = StateStoreSpec {
        kind: spec.kind.clone(),
        config: crate::interpolate::unescaped(spec.config.clone()),
    };
    let spec = &unescaped;
    match spec.kind.as_str() {
        "memory" => Ok(Arc::new(MemoryStateStore::new())),
        "file" => {
            let cfg = decode::<FileStateConfig>("file", spec.config.clone())?;
            match cfg.encryption {
                None => Ok(Arc::new(FileStateStore::new(cfg.path))),
                #[cfg(feature = "encryption")]
                Some(raw) => {
                    let enc_spec: faucet_core::EncryptionSpec = serde_json::from_value(raw)
                        .map_err(|e| CliError::Config(format!("state.config.encryption: {e}")))?;
                    let compiled = faucet_core::CompiledEncryption::compile(&enc_spec)?;
                    Ok(Arc::new(
                        FileStateStore::new(cfg.path).with_encryption(compiled),
                    ))
                }
                #[cfg(not(feature = "encryption"))]
                Some(_) => Err(CliError::Config(
                    "state.config.encryption requires a faucet build with the `encryption` \
                     feature (cargo install faucet-cli --features encryption)"
                        .into(),
                )),
            }
        }
        #[cfg(feature = "state-redis")]
        "redis" => {
            let cfg = decode::<RedisStateConfig>("redis", spec.config.clone())?;
            Ok(Arc::new(
                faucet_state_redis::RedisStateStore::connect_with_tls(
                    &cfg.url,
                    &cfg.namespace,
                    &cfg.tls,
                )
                .await?,
            ))
        }
        #[cfg(feature = "state-postgres")]
        "postgres" => {
            let cfg = decode::<PostgresStateConfig>("postgres", spec.config.clone())?;
            let max_connections = cfg.max_connections.unwrap_or(DEFAULT_PG_POOL_SIZE);
            if max_connections == 0 {
                return Err(CliError::Config(
                    "state.config.max_connections must be greater than 0".to_owned(),
                ));
            }
            let store = faucet_state_postgres::PostgresStateStore::connect_with(
                &cfg.url,
                max_connections,
                &cfg.table,
            )
            .await?;
            if cfg.ensure_table {
                store.ensure_table().await?;
            }
            Ok(Arc::new(store))
        }
        other => Err(CliError::UnknownStateStore {
            name: other.to_owned(),
            available: available_state_kinds().join(", "),
        }),
    }
}

/// Names of every state-store backend compiled into this build.
pub fn available_state_kinds() -> Vec<&'static str> {
    let mut v = vec!["memory", "file"];
    #[cfg(feature = "state-redis")]
    v.push("redis");
    #[cfg(feature = "state-postgres")]
    v.push("postgres");
    v
}

/// The JSON Schema of one compiled backend's `state.config`, or `None` for an
/// unknown or uncompiled backend.
pub fn state_store_schema(kind: &str) -> Option<serde_json::Value> {
    let schema = match kind {
        "memory" => {
            return Some(serde_json::json!({
                "title": "memory state store",
                "description": "Kept in memory only; lost when the process exits.",
                "type": "object",
                "properties": {},
            }));
        }
        "file" => schemars::schema_for!(FileStateConfig),
        #[cfg(feature = "state-redis")]
        "redis" => schemars::schema_for!(RedisStateConfig),
        #[cfg(feature = "state-postgres")]
        "postgres" => schemars::schema_for!(PostgresStateConfig),
        _ => return None,
    };
    serde_json::to_value(schema).ok()
}

fn decode<T: serde::de::DeserializeOwned>(name: &str, config: serde_json::Value) -> CliResult<T> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidConnectorConfig {
        kind: "state",
        name: name.to_owned(),
        message: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_compiled_backend_has_a_config_schema() {
        for kind in available_state_kinds() {
            let schema = state_store_schema(kind).unwrap();
            assert!(schema["properties"].is_object(), "{kind}");
        }
        assert!(state_store_schema("nope").is_none());
        let file = state_store_schema("file").unwrap();
        assert_eq!(file["required"], serde_json::json!(["path"]));
        assert!(!file["properties"]["encryption"].is_null());
    }

    #[cfg(feature = "state-redis")]
    #[test]
    fn redis_schema_lists_what_the_loader_accepts() {
        let redis = state_store_schema("redis").unwrap();
        for field in ["url", "namespace", "tls"] {
            assert!(redis["properties"][field].is_object(), "{field}");
        }
        assert_eq!(redis["properties"]["namespace"]["default"], "faucet");
    }
    use serde_json::json;

    #[tokio::test]
    async fn builds_memory_store() {
        let spec = StateStoreSpec {
            kind: "memory".into(),
            config: json!({}),
        };
        let store = build_state_store(&spec).await.unwrap();
        store.put("k", &json!(1)).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(json!(1)));
    }

    #[tokio::test]
    async fn builds_file_store() {
        let dir = tempfile::tempdir().unwrap();
        let spec = StateStoreSpec {
            kind: "file".into(),
            config: json!({"path": dir.path().to_str().unwrap()}),
        };
        let store = build_state_store(&spec).await.unwrap();
        store.put("k", &json!("v")).await.unwrap();
    }

    // `max_connections: 0` is rejected at config-load time with a typed
    // `Config` error, before any connection attempt — so the assertion holds
    // offline without a running Postgres.
    #[cfg(feature = "state-postgres")]
    #[tokio::test]
    async fn postgres_state_rejects_zero_max_connections() {
        let spec = StateStoreSpec {
            kind: "postgres".into(),
            config: json!({
                "url": "postgres://user:pass@localhost/faucet",
                "max_connections": 0,
            }),
        };
        let err = build_state_store(&spec).await.err().expect("should fail");
        match err {
            CliError::Config(msg) => assert!(msg.contains("max_connections"), "{msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "state-redis")]
    #[tokio::test]
    async fn redis_state_applies_its_tls_block() {
        let spec = StateStoreSpec {
            kind: "redis".into(),
            config: json!({
                "url": "redis://127.0.0.1:1/0",
                "tls": {"insecure_skip_verify": true},
            }),
        };
        let err = build_state_store(&spec).await.err().expect("should fail");
        assert!(err.to_string().contains("rediss://"), "{err}");
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn builds_encrypted_file_store_and_seals_bookmarks() {
        let dir = tempfile::tempdir().unwrap();
        let spec = StateStoreSpec {
            kind: "file".into(),
            config: json!({
                "path": dir.path().to_str().unwrap(),
                "encryption": { "key": "test-key" },
            }),
        };
        let store = build_state_store(&spec).await.unwrap();
        store.put("bk", &json!({"pos": 7})).await.unwrap();
        assert_eq!(store.get("bk").await.unwrap(), Some(json!({"pos": 7})));
        let raw = std::fs::read(dir.path().join("bk.json")).unwrap();
        assert!(raw.starts_with(b"FCT1"), "bookmark must be ciphertext");
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn encrypted_file_store_rejects_bad_block() {
        let spec = StateStoreSpec {
            kind: "file".into(),
            config: json!({"path": "/tmp/x", "encryption": {"key": ""}}),
        };
        assert!(build_state_store(&spec).await.is_err());
        let spec = StateStoreSpec {
            kind: "file".into(),
            config: json!({"path": "/tmp/x", "encryption": {"key": "k", "nope": 1}}),
        };
        assert!(build_state_store(&spec).await.is_err());
    }

    #[tokio::test]
    async fn unknown_kind_errors() {
        let spec = StateStoreSpec {
            kind: "nope".into(),
            config: json!({}),
        };
        let err = build_state_store(&spec).await.err().expect("should fail");
        match err {
            CliError::UnknownStateStore { name, .. } => assert_eq!(name, "nope"),
            other => panic!("expected UnknownStateStore, got {other:?}"),
        }
    }
}
