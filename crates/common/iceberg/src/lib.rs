#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-iceberg
//!
//! Shared Apache Iceberg catalog and storage types for the faucet-stream
//! Iceberg connectors (`faucet-source-iceberg` and `faucet-sink-iceberg`):
//! the [`CatalogConfig`] enum, [`build_catalog`] (REST / Glue / SQL / HMS, each
//! behind its own `catalog-*` feature) and the warehouse-scheme driven
//! storage-factory selection used by the non-REST catalogs.

pub mod catalog;
pub mod config;
#[cfg(feature = "storage-opendal")]
pub mod storage_factory;

pub use catalog::build_catalog;
pub use config::{CatalogConfig, CatalogInner, WarehouseScheme, warehouse_scheme};
#[cfg(feature = "storage-opendal")]
pub use storage_factory::select_storage_factory;

/// Catalog name every faucet Iceberg connector registers with.
///
/// The SQL catalog keys tables by `(catalog_name, namespace, table)`, so the
/// source and sink must agree on it to see each other's tables.
pub const CATALOG_NAME: &str = "faucet-iceberg";
