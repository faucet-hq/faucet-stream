//! Iceberg `StorageFactory` selection for the non-REST catalogs.
//!
//! REST catalogs resolve `FileIO` server-side. The SQL / Glue / HMS catalogs
//! build `FileIO` in-process and need an explicit `StorageFactory`:
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
use iceberg_storage_opendal::OpenDalStorageFactory;
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

/// A `StorageFactory` that injects faucet's configured `catalog.properties`
/// into the `StorageConfig` before delegating to an OpenDAL-backed factory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OpendalPropInjector {
    inner: OpenDalStorageFactory,
    props: HashMap<String, String>,
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
        self.inner.build(&StorageConfig::from_props(merged))
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
    match warehouse_scheme(warehouse) {
        WarehouseScheme::Local => Ok(Arc::new(LocalFsStorageFactory)),
        WarehouseScheme::S3(_) => Ok(cloud_factory_s3(&inner.properties)),
        WarehouseScheme::Gcs => Ok(cloud_factory_gcs(&inner.properties)),
        WarehouseScheme::Unsupported(s) => Err(FaucetError::Config(format!(
            "iceberg: warehouse scheme '{s}://' has no storage factory; supported \
             schemes are file://, s3://, s3a://, gs:// (use the REST catalog for \
             other object stores)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
