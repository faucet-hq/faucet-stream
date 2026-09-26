//! Feature-gated factory for Iceberg catalog clients.
//!
//! `build_catalog` maps a [`CatalogConfig`] to an `Arc<dyn iceberg::Catalog>`.
//! Each catalog type is gated behind a Cargo feature; attempting to use a
//! catalog whose feature is not enabled returns `FaucetError::Config` with a
//! clear message rather than failing to compile.
//!
//! | Catalog | Feature flag      | Dependency              |
//! |---------|-------------------|-------------------------|
//! | REST    | `catalog-rest`    | `iceberg-catalog-rest`  |
//! | Glue    | `catalog-glue`    | `iceberg-catalog-glue`  |
//! | SQL     | `catalog-sql`     | `iceberg-catalog-sql`   |
//! | HMS     | `catalog-hms`     | `iceberg-catalog-hms`   |

#[cfg(any(
    feature = "catalog-rest",
    feature = "catalog-glue",
    feature = "catalog-sql",
    feature = "catalog-hms"
))]
use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::FaucetError;
use iceberg::Catalog;

use crate::config::{CatalogConfig, CatalogInner};

/// Build an `Arc<dyn Catalog>` from the supplied configuration.
///
/// Returns `FaucetError::Config` when:
/// - The chosen catalog type's Cargo feature is not enabled.
/// - The catalog client fails to initialise (bad URI, auth failure, etc.).
pub async fn build_catalog(cfg: &CatalogConfig) -> Result<Arc<dyn Catalog>, FaucetError> {
    match cfg {
        CatalogConfig::Rest(inner) => build_rest(inner).await,
        CatalogConfig::Glue(inner) => build_glue(inner).await,
        CatalogConfig::Sql(inner) => build_sql(inner).await,
        CatalogConfig::Hms(inner) => build_hms(inner).await,
    }
}

// ── REST ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "catalog-rest")]
async fn build_rest(inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };

    let mut props: HashMap<String, String> = inner.properties.clone();

    if let Some(uri) = &inner.uri {
        props.insert(REST_CATALOG_PROP_URI.to_string(), uri.clone());
    }
    if let Some(warehouse) = &inner.warehouse {
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }
    if let Some(credential) = &inner.credential {
        // REST catalog treats a `token` property as the bearer token.
        props.insert("token".to_string(), credential.clone());
    }

    let catalog = RestCatalogBuilder::default()
        .load(crate::CATALOG_NAME, props)
        .await
        .map_err(|e| FaucetError::Config(format!("iceberg: REST catalog init failed: {e}")))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "catalog-rest"))]
async fn build_rest(_inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    Err(FaucetError::Config(
        "iceberg: catalog 'rest' requires the 'catalog-rest' Cargo feature".to_string(),
    ))
}

// ── Glue ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "catalog-glue")]
async fn build_glue(inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder};

    let mut props: HashMap<String, String> = inner.properties.clone();

    if let Some(warehouse) = &inner.warehouse {
        props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }
    // Glue uses AWS credentials from the SDK chain; `uri` is typically unused
    // for Glue but thread it through in case the user needs a custom endpoint.
    if let Some(uri) = &inner.uri {
        props.insert("uri".to_string(), uri.clone());
    }

    let storage_factory = crate::storage_factory::select_storage_factory(inner)?;

    let catalog = GlueCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load(crate::CATALOG_NAME, props)
        .await
        .map_err(|e| FaucetError::Config(format!("iceberg: Glue catalog init failed: {e}")))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "catalog-glue"))]
async fn build_glue(_inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    Err(FaucetError::Config(
        "iceberg: catalog 'glue' requires the 'catalog-glue' Cargo feature".to_string(),
    ))
}

// ── SQL ───────────────────────────────────────────────────────────────────────

#[cfg(feature = "catalog-sql")]
async fn build_sql(inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{
        SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
        SqlBindStyle, SqlCatalogBuilder,
    };

    let mut props: HashMap<String, String> = inner.properties.clone();

    if let Some(uri) = &inner.uri {
        props.insert(SQL_CATALOG_PROP_URI.to_string(), uri.clone());
    }
    if let Some(warehouse) = &inner.warehouse {
        props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }

    // Infer the SQL bind style from the catalog URI when not explicitly
    // overridden by the user.  SQLite uses `?` placeholders (QMark); other
    // databases (Postgres, MySQL) use `$N` (DollarNumeric).
    if !props.contains_key(SQL_CATALOG_PROP_BIND_STYLE) {
        let catalog_uri = props
            .get(SQL_CATALOG_PROP_URI)
            .map(String::as_str)
            .unwrap_or("");
        let bind_style = if catalog_uri.starts_with("sqlite:") {
            SqlBindStyle::QMark
        } else {
            SqlBindStyle::DollarNumeric
        };
        props.insert(
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            bind_style.to_string(),
        );
    }

    // Select the storage factory from the warehouse URI scheme: local FS for
    // file:// / bare paths, OpenDAL S3/GCS for cloud warehouses. (#181)
    let storage_factory = crate::storage_factory::select_storage_factory(inner)?;

    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load(crate::CATALOG_NAME, props)
        .await
        .map_err(|e| FaucetError::Config(format!("iceberg: SQL catalog init failed: {e}")))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "catalog-sql"))]
async fn build_sql(_inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    Err(FaucetError::Config(
        "iceberg: catalog 'sql' requires the 'catalog-sql' Cargo feature".to_string(),
    ))
}

// ── HMS ───────────────────────────────────────────────────────────────────────

#[cfg(feature = "catalog-hms")]
async fn build_hms(inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_hms::{
        HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE, HmsCatalogBuilder,
    };

    let mut props: HashMap<String, String> = inner.properties.clone();

    if let Some(uri) = &inner.uri {
        props.insert(HMS_CATALOG_PROP_URI.to_string(), uri.clone());
    }
    if let Some(warehouse) = &inner.warehouse {
        props.insert(HMS_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }

    let storage_factory = crate::storage_factory::select_storage_factory(inner)?;

    let catalog = HmsCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load(crate::CATALOG_NAME, props)
        .await
        .map_err(|e| FaucetError::Config(format!("iceberg: HMS catalog init failed: {e}")))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "catalog-hms"))]
async fn build_hms(_inner: &CatalogInner) -> Result<Arc<dyn Catalog>, FaucetError> {
    Err(FaucetError::Config(
        "iceberg: catalog 'hms' requires the 'catalog-hms' Cargo feature".to_string(),
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CatalogInner;

    #[allow(dead_code)]
    fn empty_inner() -> CatalogInner {
        CatalogInner {
            uri: None,
            warehouse: None,
            credential: None,
            properties: Default::default(),
        }
    }

    #[cfg(not(feature = "catalog-rest"))]
    #[tokio::test]
    async fn rest_without_feature_returns_config_error() {
        let err = build_catalog(&CatalogConfig::Rest(empty_inner()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("catalog-rest"), "{err}");
    }

    /// Regardless of which catalog features are compiled in, trying to build a
    /// Glue catalog without the `catalog-glue` feature must return a typed
    /// Config error (not a compile error and not a panic).
    #[cfg(not(feature = "catalog-glue"))]
    #[tokio::test]
    async fn glue_without_feature_returns_config_error() {
        let cfg = CatalogConfig::Glue(empty_inner());
        let err = build_catalog(&cfg).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::Config(_)),
            "expected Config error, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("catalog-glue"),
            "error should mention the missing feature: {msg}"
        );
    }

    /// Same guard for SQL.
    #[cfg(not(feature = "catalog-sql"))]
    #[tokio::test]
    async fn sql_without_feature_returns_config_error() {
        let cfg = CatalogConfig::Sql(empty_inner());
        let err = build_catalog(&cfg).await.unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("catalog-sql"),
            "should mention catalog-sql: {msg}"
        );
    }

    /// Same guard for HMS.
    #[cfg(not(feature = "catalog-hms"))]
    #[tokio::test]
    async fn hms_without_feature_returns_config_error() {
        let cfg = CatalogConfig::Hms(empty_inner());
        let err = build_catalog(&cfg).await.unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("catalog-hms"),
            "should mention catalog-hms: {msg}"
        );
    }

    /// With the default features (catalog-rest only), Glue/SQL/HMS all return
    /// Config errors — test all three in one shot when no catalog-* extras enabled.
    #[cfg(all(
        not(feature = "catalog-glue"),
        not(feature = "catalog-sql"),
        not(feature = "catalog-hms")
    ))]
    #[tokio::test]
    async fn disabled_catalog_types_all_return_config_errors() {
        for cfg in [
            CatalogConfig::Glue(empty_inner()),
            CatalogConfig::Sql(empty_inner()),
            CatalogConfig::Hms(empty_inner()),
        ] {
            let err = build_catalog(&cfg).await.unwrap_err();
            assert!(
                matches!(err, FaucetError::Config(_)),
                "expected Config error for {cfg:?}"
            );
        }
    }
}
