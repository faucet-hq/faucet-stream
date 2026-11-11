//! Iceberg `StorageFactory` selection.
//!
//! Every catalog builds its `FileIO` in-process and needs a `StorageFactory`.
//! The REST catalog gets [`rest_storage_factory`]: its warehouse is usually a
//! logical name, and the table locations (and any vended credentials) come
//! from the server, so the backend is picked per path. The SQL / Glue / HMS
//! catalogs pick one from the warehouse URI:
//!
//! - Local (`file://` / bare path) warehouses → `iceberg::io::LocalFsStorageFactory`.
//! - Cloud (`s3://` / `s3a://` / `gs://`) warehouses → an `OpendalPropInjector`
//!   wrapping `iceberg_storage_opendal::OpenDalStorageFactory`.
//!
//! The wrapper exists because `iceberg-catalog-sql` builds its `FileIO` with
//! **empty** properties, so the user's `catalog.properties` (`s3.region`,
//! `s3.endpoint`, `gcs.credentials-json`, …) would otherwise never reach the
//! storage layer. The wrapper re-supplies them, letting the catalog-threaded
//! props (Glue/HMS) overlay on top.

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::FaucetError;
use iceberg::io::{LocalFsStorageFactory, Storage, StorageConfig, StorageFactory};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalResolvingStorageFactory,
    OpenDalStorageFactory, ProvideCredential,
};
use serde::{Deserialize, Serialize};

use crate::config::{CatalogInner, WarehouseScheme, warehouse_scheme};

/// Merge faucet's configured catalog properties (`base`) with the properties
/// the catalog threads into its `FileIO` (`overlay`). The overlay wins on key
/// collisions: for SQL the overlay is empty (faucet props win); for Glue/HMS
/// the overlay carries the catalog's own threaded + enriched props.
fn merge_props(
    base: &HashMap<String, String>,
    overlay: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base.clone();
    merged.extend(overlay.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

/// The S3 credentials a storage config names (`s3.access-key-id`,
/// `s3.secret-access-key`, `s3.session-token`) and nothing else: no
/// environment, profile, web-identity, container or instance credentials, so
/// a task that may not use the host's identity signs with the config's keys
/// (including vended ones) or not at all.
#[derive(Debug)]
struct NamedS3Credentials {
    credential: Option<AwsCredential>,
    anonymous: bool,
}

impl NamedS3Credentials {
    fn from_props(props: &HashMap<String, String>) -> Self {
        let get = |k: &str| props.get(k).filter(|v| !v.trim().is_empty()).cloned();
        let credential = match (get("s3.access-key-id"), get("s3.secret-access-key")) {
            (Some(access_key_id), Some(secret_access_key)) => Some(AwsCredential {
                access_key_id,
                secret_access_key,
                session_token: get("s3.session-token"),
                ..Default::default()
            }),
            _ => None,
        };
        let anonymous = props.get("s3.allow-anonymous").is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        });
        Self {
            credential,
            anonymous,
        }
    }
}

impl ProvideCredential for NamedS3Credentials {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<AwsCredential>> {
        match (&self.credential, self.anonymous) {
            (Some(c), _) => Ok(Some(c.clone())),
            (None, true) => Ok(None),
            (None, false) => Err(reqsign_core::Error::credential_invalid(refusal(
                "catalog.properties",
            ))),
        }
    }
}

fn refusal(field: &str) -> String {
    faucet_core::resolve::require_host_credentials("iceberg", field)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default()
}

/// The storage settings for a task that may not use the host's identity:
/// refuse an assumed role, turn off the GCS backend's environment and VM
/// metadata lookups, and hand the S3 backend [`NamedS3Credentials`]. `None`
/// when the task may use it.
fn guarded(
    props: &HashMap<String, String>,
) -> iceberg::Result<Option<(HashMap<String, String>, CustomAwsCredentialLoader)>> {
    if faucet_core::resolve::host_credentials_allowed() {
        return Ok(None);
    }
    if props
        .get(crate::config::ASSUME_ROLE_PROP)
        .is_some_and(|v| !v.trim().is_empty())
    {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            refusal(&format!(
                "catalog.properties.{}",
                crate::config::ASSUME_ROLE_PROP
            )),
        ));
    }
    let mut props = props.clone();
    for key in [
        "gcs.disable-vm-metadata",
        "gcs.disable-config-load",
        "s3.disable-ec2-metadata",
        "s3.disable-config-load",
    ] {
        props.insert(key.to_string(), "true".to_string());
    }
    let loader = CustomAwsCredentialLoader::new(NamedS3Credentials::from_props(&props));
    Ok(Some((props, loader)))
}

/// A `StorageFactory` that injects faucet's configured `catalog.properties`
/// into the `StorageConfig` before delegating to an OpenDAL-backed factory.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct OpendalPropInjector {
    inner: OpenDalStorageFactory,
    props: HashMap<String, String>,
}

impl std::fmt::Debug for OpendalPropInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys: Vec<&String> = self.props.keys().collect();
        keys.sort();
        f.debug_struct("OpendalPropInjector")
            .field("inner", &self.inner)
            .field("props", &keys)
            .finish()
    }
}

impl OpendalPropInjector {
    fn new(inner: OpenDalStorageFactory, props: HashMap<String, String>) -> Self {
        Self { inner, props }
    }
}

#[typetag::serde(name = "faucet-opendal-prop-injector")]
impl StorageFactory for OpendalPropInjector {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        let merged = merge_props(&self.props, config.props());
        match (guarded(&merged)?, &self.inner) {
            (None, inner) => inner.build(&StorageConfig::from_props(merged)),
            (Some((props, loader)), OpenDalStorageFactory::S3 { .. }) => {
                OpenDalStorageFactory::S3 {
                    customized_credential_load: Some(loader),
                }
                .build(&StorageConfig::from_props(props))
            }
            (Some((props, _)), inner) => inner.build(&StorageConfig::from_props(props)),
        }
    }
}

/// The REST catalog's storage factory: OpenDAL storage resolved per path,
/// built from the catalog's properties after the server's overrides and the
/// table's (vended) config are merged, with [`guarded`] applied.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct GuardedResolvingFactory;

#[typetag::serde(name = "faucet-guarded-resolving")]
impl StorageFactory for GuardedResolvingFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        match guarded(config.props())? {
            None => OpenDalResolvingStorageFactory::new().build(config),
            Some((props, loader)) => OpenDalResolvingStorageFactory::new()
                .with_s3_credential_loader(loader)
                .build(&StorageConfig::from_props(props)),
        }
    }
}

/// Build an S3 OpenDAL factory, threading faucet's `catalog.properties`.
///
/// As of `iceberg-storage-opendal` 0.10.0 the `S3` variant accepts any
/// S3-family URL (`s3://` / `s3a://` / `s3n://`) internally, so the scheme is
/// no longer passed in.
fn cloud_factory_s3(props: &HashMap<String, String>) -> Arc<dyn StorageFactory> {
    Arc::new(OpendalPropInjector::new(
        OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        },
        props.clone(),
    ))
}

/// Build a GCS OpenDAL factory, threading faucet's `catalog.properties`.
fn cloud_factory_gcs(props: &HashMap<String, String>) -> Arc<dyn StorageFactory> {
    Arc::new(OpendalPropInjector::new(
        OpenDalStorageFactory::Gcs,
        props.clone(),
    ))
}

/// The REST catalog's `StorageFactory`: OpenDAL storage chosen from each
/// file path's scheme, configured from the catalog properties merged with the
/// server's config and the table's (vended) config.
pub fn rest_storage_factory() -> Arc<dyn StorageFactory> {
    crate::checked::checked(Arc::new(GuardedResolvingFactory))
}

/// Select an Iceberg `StorageFactory` from a catalog's warehouse URI scheme.
///
/// - local (`file://` / bare path / none) → `LocalFsStorageFactory`
/// - `s3://` / `s3a://` → OpenDAL S3 (scheme matched to the URI)
/// - `gs://` → OpenDAL GCS
/// - anything else → `FaucetError::Config`
pub fn select_storage_factory(
    inner: &CatalogInner,
) -> Result<Arc<dyn StorageFactory>, FaucetError> {
    let warehouse = inner.warehouse.as_deref().unwrap_or("");
    let factory: Arc<dyn StorageFactory> = match warehouse_scheme(warehouse) {
        WarehouseScheme::Local => Arc::new(LocalFsStorageFactory),
        WarehouseScheme::S3(_) => cloud_factory_s3(&inner.properties),
        WarehouseScheme::Gcs => cloud_factory_gcs(&inner.properties),
        WarehouseScheme::Unsupported(s) => {
            return Err(FaucetError::Config(format!(
                "iceberg: warehouse scheme '{s}://' has no storage factory; supported \
                 schemes are file://, s3://, s3a://, gs:// (use the REST catalog for \
                 other object stores)"
            )));
        }
    };
    Ok(crate::checked::checked(factory))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn storage_without_named_credentials_is_refused_inside_a_caller_run() {
        use iceberg_storage_opendal::ProvideCredential as _;
        let keys = HashMap::from([
            ("s3.access-key-id".to_string(), "a".to_string()),
            ("s3.secret-access-key".to_string(), "s".to_string()),
            ("s3.session-token".to_string(), "t".to_string()),
        ]);
        assert!(
            guarded(&keys).unwrap().is_none(),
            "trusted runs keep the chain"
        );
        faucet_core::resolve::with_host_credentials(false, async {
            let (props, _) = guarded(&keys).unwrap().expect("guarded");
            assert_eq!(props["gcs.disable-vm-metadata"], "true");
            assert_eq!(props["s3.disable-ec2-metadata"], "true");
            let ctx = reqsign_core::Context::new();
            let named = NamedS3Credentials::from_props(&keys)
                .provide_credential(&ctx)
                .await
                .unwrap()
                .expect("the config's keys");
            assert_eq!(named.access_key_id, "a");
            assert_eq!(named.session_token.as_deref(), Some("t"));
            let err = NamedS3Credentials::from_props(&HashMap::new())
                .provide_credential(&ctx)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("--allow-host-credentials"), "{err}");
            let anonymous = HashMap::from([("s3.allow-anonymous".to_string(), "true".to_string())]);
            assert!(
                NamedS3Credentials::from_props(&anonymous)
                    .provide_credential(&ctx)
                    .await
                    .unwrap()
                    .is_none()
            );
            let role = HashMap::from([(
                crate::config::ASSUME_ROLE_PROP.to_string(),
                "arn:aws:iam::1:role/r".to_string(),
            )]);
            let err = guarded(&role).unwrap_err().to_string();
            assert!(err.contains("client.assume-role.arn"), "{err}");
            let cfg = StorageConfig::from_props(keys.clone());
            assert!(GuardedResolvingFactory.build(&cfg).is_ok());
            assert!(
                OpendalPropInjector::new(
                    OpenDalStorageFactory::S3 {
                        customized_credential_load: None,
                    },
                    keys.clone(),
                )
                .build(&StorageConfig::from_props(HashMap::new()))
                .is_ok()
            );
            assert!(
                OpendalPropInjector::new(OpenDalStorageFactory::Gcs, HashMap::new())
                    .build(&StorageConfig::from_props(HashMap::new()))
                    .is_ok()
            );
            assert!(
                GuardedResolvingFactory
                    .build(&StorageConfig::from_props(role))
                    .is_err()
            );
        })
        .await;
        assert!(
            GuardedResolvingFactory
                .build(&StorageConfig::from_props(HashMap::new()))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn iceberg_catalog_overrides_cannot_reenable_host_credentials() {
        let overridden = HashMap::from([
            ("s3.disable-ec2-metadata".to_string(), "false".to_string()),
            ("s3.disable-config-load".to_string(), "false".to_string()),
            ("gcs.disable-vm-metadata".to_string(), "false".to_string()),
            ("gcs.disable-config-load".to_string(), "false".to_string()),
        ]);
        faucet_core::resolve::with_host_credentials(false, async {
            let (props, _) = guarded(&overridden).unwrap().expect("guarded");
            for key in overridden.keys() {
                assert_eq!(props[key], "true", "{key}");
            }
            let vended_role = merge_props(
                &HashMap::new(),
                &HashMap::from([(
                    crate::config::ASSUME_ROLE_PROP.to_string(),
                    "arn:aws:iam::1:role/r".to_string(),
                )]),
            );
            assert!(guarded(&vended_role).is_err());
        })
        .await;
    }

    #[test]
    fn injector_debug_shows_property_names_only() {
        let props = HashMap::from([("s3.secret-access-key".to_string(), "S3CRET-1".to_string())]);
        let dbg = format!(
            "{:?}",
            OpendalPropInjector::new(
                OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                },
                props,
            )
        );
        assert!(
            !dbg.contains("S3CRET") && dbg.contains("s3.secret-access-key"),
            "{dbg}"
        );
    }

    #[test]
    fn merge_props_overlay_wins() {
        let base = HashMap::from([
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.endpoint".to_string(), "http://base".to_string()),
        ]);
        let overlay = HashMap::from([("s3.endpoint".to_string(), "http://overlay".to_string())]);
        let merged = merge_props(&base, &overlay);
        assert_eq!(merged.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(merged.get("s3.endpoint").unwrap(), "http://overlay");
    }

    #[test]
    fn merge_props_empty_overlay_keeps_base() {
        let base = HashMap::from([("s3.region".to_string(), "eu-west-1".to_string())]);
        let merged = merge_props(&base, &HashMap::new());
        assert_eq!(merged.get("s3.region").unwrap(), "eu-west-1");
    }

    fn inner_with_warehouse(warehouse: Option<&str>) -> CatalogInner {
        CatalogInner {
            uri: None,
            warehouse: warehouse.map(str::to_string),
            credential: None,
            properties: HashMap::from([("s3.region".to_string(), "us-east-1".to_string())]),
        }
    }

    #[test]
    fn select_local_for_file_and_bare_paths() {
        for w in [None, Some("/tmp/wh"), Some("file:///tmp/wh")] {
            let f = select_storage_factory(&inner_with_warehouse(w)).expect("ok");
            assert!(
                format!("{f:?}").contains("LocalFsStorageFactory"),
                "expected LocalFs for {w:?}, got {f:?}"
            );
        }
    }

    #[test]
    fn select_s3_family_selects_s3_factory_and_threads_props() {
        // iceberg-storage-opendal 0.10.0's `S3` variant accepts any S3-family
        // URL internally (no per-scheme field), so both s3:// and s3a:// map to
        // the same S3 factory; faucet's props are still threaded through.
        let f = select_storage_factory(&inner_with_warehouse(Some("s3://bucket/wh"))).expect("ok");
        let dbg = format!("{f:?}");
        assert!(dbg.contains("S3"), "should build an S3 factory: {dbg}");
        assert!(dbg.contains("s3.region"), "props must be threaded: {dbg}");

        let f_a =
            select_storage_factory(&inner_with_warehouse(Some("s3a://bucket/wh"))).expect("ok");
        assert!(
            format!("{f_a:?}").contains("S3"),
            "s3a:// must also select the S3 factory"
        );
    }

    #[test]
    fn select_gcs() {
        let f = select_storage_factory(&inner_with_warehouse(Some("gs://bucket/wh"))).expect("ok");
        assert!(format!("{f:?}").contains("Gcs"), "got {f:?}");
    }

    #[test]
    fn select_unsupported_scheme_errors() {
        let err =
            select_storage_factory(&inner_with_warehouse(Some("oss://bucket/wh"))).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(msg.contains("oss"), "should name the scheme: {msg}");
        assert!(
            msg.contains("s3://"),
            "should list supported schemes: {msg}"
        );
    }
}
