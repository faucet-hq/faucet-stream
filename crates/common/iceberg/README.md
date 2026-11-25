# faucet-common-iceberg

Shared Apache **Iceberg** catalog plumbing for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) Iceberg connectors — [`faucet-source-iceberg`](https://crates.io/crates/faucet-source-iceberg) and [`faucet-sink-iceberg`](https://crates.io/crates/faucet-sink-iceberg). Connector authors and end users normally depend on one of those crates rather than this one.

## What's inside

- **`CatalogConfig` / `CatalogInner`** — the `catalog:` block (`type: rest | glue | sql | hms` plus `uri`, `warehouse`, `credential`, `properties`). `Debug` redacts `uri` and `credential`. `validate_connection()` enforces a non-empty `uri` for REST / SQL / HMS and a supported warehouse scheme for the non-REST catalogs; `kind()` returns the discriminator.
- **`build_catalog(&CatalogConfig) -> Arc<dyn iceberg::Catalog>`** — builds the client for the configured catalog. A catalog whose Cargo feature is not compiled in returns a typed `FaucetError::Config` naming the feature. SQL catalogs infer the bind style from the URI (`sqlite:` → `?`, otherwise `$N`); an HMS `uri` of `thrift://host:port` is dialled as `host:port`, and the HMS catalog is an `HmsCommitCatalog` (feature `catalog-hms`): iceberg-rust's HMS client with a working `update_table` — an `EXCLUSIVE` metastore table lock, the next metadata file written through the table's `FileIO`, an optimistic check that `metadata_location` has not moved, then `alter_table` to the new file with `previous_metadata_location` set (a lost race is a retryable `CatalogCommitConflicts`; an unanswered `alter_table` is not retried). `hms.lock-timeout-ms` in `properties` bounds the lock wait (default 180000).
- **`rest_storage_factory`** (feature `storage-opendal`, enabled by `catalog-rest`) — the REST catalog's `StorageFactory`: OpenDAL storage resolved per file path scheme, configured from the catalog properties merged with the server's and the table's (vended) config (#962).
- **`select_storage_factory`** (feature `storage-opendal`) — picks the Iceberg `StorageFactory` for the SQL / Glue / HMS catalogs from the `warehouse` scheme: `file://` / bare path → local FS, `s3://` / `s3a://` → OpenDAL S3, `gs://` → OpenDAL GCS. `catalog.properties` (`s3.region`, `s3.endpoint`, `gcs.credentials-json`, …) are re-injected into the storage config, because `iceberg-catalog-sql` builds its `FileIO` with empty properties.
- **Configs the host does not trust.** Under a reference policy with path roots (`faucet serve` running a config it did not load itself), both factories check every location the catalog hands them (table metadata, manifests, data files) against the policy — local paths against the path roots, remote locations by host — and refuse a vended storage endpoint (`s3.endpoint`, `gcs.service.host` or its earlier name `gcs.service.path`, `adls.endpoint`) the policy refuses. Before a REST catalog is built, its `/v1/config` is read through `faucet_core::http::Client` (with the configured `token`, or a token fetched for `credential`); a `uri` override on another host, or a `uri` / `oauth2-server-uri` the policy refuses, fails the run with a `Config` error.
- **`AccessDelegation`** — `catalog.access_delegation` (REST only): `vended_credentials` sends `X-Iceberg-Access-Delegation: vended-credentials` on every catalog request. `validate_connection()` refuses it on the other catalogs, refuses a `header.X-Iceberg-Access-Delegation` property of another value, and refuses S3 remote signing by name (`remote-signing` in that header, `s3.remote-signing-enabled: true`).
- **`holds_vended_credentials`** — whether a table's storage holds an expiring credential (`s3.session-token`, `gcs.oauth2.token`, `adls.sas-token`) the user did not write: one the catalog vended, which the sink refreshes before each new writer.
- **`preflight`** (feature `storage-opendal`) — `check_table_storage(&Table, &StorageExpectation) -> StorageVerdict`: one `HEAD` of the table's current metadata file through its `FileIO` (S3 and GCS table locations). It refuses only when the store or the signer reports a credential failure (`PermissionDenied`, `ConfigInvalid`, or a signing credential that could not be loaded) and the config explains it: no storage credentials at all, or static S3 keys mixed with a vended session token the store refuses. Anything else is `Warn`.
- **`unread_properties` / `warn_unread_properties`** — the `catalog.properties` names a catalog does not read (warned once per process by `build_catalog`), with the faucet setting for names other clients use (`rest.vended-credentials-enabled`, `io-impl`, `s3.remote-signing-enabled`).
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
| `storage-opendal` | OpenDAL S3 / GCS / local storage factories (enabled by every catalog feature) |

## License

MIT OR Apache-2.0.
