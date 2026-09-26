# faucet-common-iceberg

Shared Apache **Iceberg** catalog plumbing for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) Iceberg connectors — [`faucet-source-iceberg`](https://crates.io/crates/faucet-source-iceberg) and [`faucet-sink-iceberg`](https://crates.io/crates/faucet-sink-iceberg). Connector authors and end users normally depend on one of those crates rather than this one.

## What's inside

- **`CatalogConfig` / `CatalogInner`** — the `catalog:` block (`type: rest | glue | sql | hms` plus `uri`, `warehouse`, `credential`, `properties`). `Debug` redacts `uri` and `credential`. `validate_connection()` enforces a non-empty `uri` for REST / SQL / HMS and a supported warehouse scheme for the non-REST catalogs; `kind()` returns the discriminator.
- **`build_catalog(&CatalogConfig) -> Arc<dyn iceberg::Catalog>`** — builds the client for the configured catalog. A catalog whose Cargo feature is not compiled in returns a typed `FaucetError::Config` naming the feature. SQL catalogs infer the bind style from the URI (`sqlite:` → `?`, otherwise `$N`).
- **`select_storage_factory`** (feature `storage-opendal`) — picks the Iceberg `StorageFactory` for the SQL / Glue / HMS catalogs from the `warehouse` scheme: `file://` / bare path → local FS, `s3://` / `s3a://` → OpenDAL S3, `gs://` → OpenDAL GCS. `catalog.properties` (`s3.region`, `s3.endpoint`, `gcs.credentials-json`, …) are re-injected into the storage config, because `iceberg-catalog-sql` builds its `FileIO` with empty properties.
- **`warehouse_scheme` / `WarehouseScheme`** — the scheme classifier behind both of the above.
- **`CATALOG_NAME`** — the catalog name every faucet connector registers with (`faucet-iceberg`). The SQL catalog keys tables by `(catalog_name, namespace, table)`, so the source and sink must agree on it.

## Feature flags

No catalog is enabled by default; each connector's `catalog-*` features forward here so the source and sink gate catalogs identically.

| Feature | Enables |
|---|---|
| `catalog-rest` | REST catalog (`iceberg-catalog-rest`) |
| `catalog-glue` | AWS Glue catalog (+ `storage-opendal`) |
| `catalog-sql` | SQL-backed catalog (+ `storage-opendal`) |
| `catalog-hms` | Hive Metastore catalog (+ `storage-opendal`) |
| `storage-opendal` | OpenDAL S3 / GCS / local storage factory for the non-REST catalogs |

## License

MIT OR Apache-2.0.
