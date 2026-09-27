//! Catalog connection configuration shared by the Iceberg source and sink.

use std::collections::HashMap;
use std::fmt;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Warehouse scheme classification ─────────────────────────────────────────

/// Classification of a warehouse URI by scheme, used to select an Iceberg
/// `StorageFactory` (see `select_storage_factory`) and to validate configs.
///
/// The set of recognised schemes is intentionally small and feature-independent:
/// it is the set faucet's storage-factory selector understands. REST catalogs
/// resolve FileIO server-side and are exempt from this classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarehouseScheme {
    /// No scheme, a bare path, or `file://` — local filesystem.
    Local,
    /// `s3://` or `s3a://`. Carries the exact scheme string ("s3" / "s3a"),
    /// which the OpenDAL S3 operator requires to match the warehouse URI.
    S3(&'static str),
    /// `gs://` — Google Cloud Storage.
    Gcs,
    /// Any other scheme (e.g. `oss`, `abfss`) — no storage factory available.
    Unsupported(String),
}

/// Classify a warehouse URI by its scheme.
///
/// A URI with no `://` (empty, bare path, or relative path) is treated as a
/// local-filesystem warehouse. Scheme matching is case-insensitive.
pub fn warehouse_scheme(warehouse: &str) -> WarehouseScheme {
    let scheme = match warehouse.trim().split_once("://") {
        Some((s, _)) => s.to_ascii_lowercase(),
        None => return WarehouseScheme::Local,
    };
    match scheme.as_str() {
        "file" => WarehouseScheme::Local,
        "s3" => WarehouseScheme::S3("s3"),
        "s3a" => WarehouseScheme::S3("s3a"),
        "gs" => WarehouseScheme::Gcs,
        other => WarehouseScheme::Unsupported(other.to_string()),
    }
}

// ── Catalog config ────────────────────────────────────────────────────────────

/// Configuration fields shared by every catalog variant.
///
/// Individual variants carry the same fields so `CatalogConfig` stays a
/// well-typed tagged enum without a separate inner struct (which would make
/// the JSON Schema less readable).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub struct CatalogInner {
    /// Catalog endpoint URI.
    ///
    /// For REST: `https://catalog.example.com`.
    /// For HMS: `thrift://hms:9083`.
    /// For SQL: the JDBC/SQLx connection string, e.g. `postgres://…`.
    #[serde(default)]
    pub uri: Option<String>,

    /// Object-storage warehouse root, e.g. `s3://lake/warehouse`.
    #[serde(default)]
    pub warehouse: Option<String>,

    /// REST bearer token or other catalog-specific credential.
    ///
    /// Redacted in `Debug` output — never logged.
    #[serde(default)]
    pub credential: Option<String>,

    /// Arbitrary catalog properties passed through to the catalog builder
    /// (e.g. S3 region, endpoint, access key).
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Iceberg catalog type and its connection settings.
///
/// Uses serde's internally-tagged enum: the JSON/YAML `type` key selects the
/// variant. Each variant carries the same inner fields (`uri`, `warehouse`,
/// `credential`, `properties`); the relevant set differs per catalog type and
/// is documented in each variant.
///
/// | Variant | Cargo feature required   |
/// |---------|--------------------------|
/// | `rest`  | `catalog-rest` (default) |
/// | `glue`  | `catalog-glue`           |
/// | `sql`   | `catalog-sql`            |
/// | `hms`   | `catalog-hms`            |
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogConfig {
    /// Apache Iceberg REST catalog. `uri` is the catalog endpoint; `credential`
    /// becomes the REST bearer token.
    Rest(CatalogInner),

    /// AWS Glue catalog. `warehouse` is the S3 root; AWS credentials are
    /// supplied via `properties` or the default AWS credential chain.
    Glue(CatalogInner),

    /// SQL-backed catalog (e.g. JDBC/postgres). `uri` is the connection string.
    Sql(CatalogInner),

    /// Hive Metastore catalog. `uri` is the Thrift endpoint
    /// (`thrift://hms:9083`).
    Hms(CatalogInner),
}

impl CatalogConfig {
    /// The connection settings shared by every catalog variant.
    pub fn inner(&self) -> &CatalogInner {
        match self {
            CatalogConfig::Rest(i)
            | CatalogConfig::Glue(i)
            | CatalogConfig::Sql(i)
            | CatalogConfig::Hms(i) => i,
        }
    }
}

// Redact credential and URI from Debug output.
impl fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = match self {
            CatalogConfig::Rest(_) => "rest",
            CatalogConfig::Glue(_) => "glue",
            CatalogConfig::Sql(_) => "sql",
            CatalogConfig::Hms(_) => "hms",
        };
        let inner = self.inner();
        // Redact the credential field entirely, and the uri (may contain userinfo/token).
        let uri_display = inner.uri.as_deref().map(|_| "***").unwrap_or("<none>");
        let cred_display = inner
            .credential
            .as_deref()
            .map(|_| "***")
            .unwrap_or("<none>");
        f.debug_struct("CatalogConfig")
            .field("type", &type_name)
            .field("uri", &uri_display)
            .field("warehouse", &inner.warehouse)
            .field("credential", &cred_display)
            .field(
                "properties_keys",
                &inner.properties.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl CatalogConfig {
    /// The catalog type discriminator as written in config (`rest` / `glue` /
    /// `sql` / `hms`).
    pub fn kind(&self) -> &'static str {
        match self {
            CatalogConfig::Rest(_) => "rest",
            CatalogConfig::Glue(_) => "glue",
            CatalogConfig::Sql(_) => "sql",
            CatalogConfig::Hms(_) => "hms",
        }
    }

    /// Validate the connection settings at config-load time.
    ///
    /// REST / SQL / HMS need a non-empty endpoint `uri`; Glue resolves its
    /// endpoint from AWS config. The non-REST catalogs build `FileIO`
    /// in-process, so their `warehouse` scheme must be one faucet has a storage
    /// factory for; REST resolves `FileIO` server-side and may use any scheme.
    pub fn validate_connection(&self) -> Result<(), FaucetError> {
        let kind = self.kind();
        let uri_required = !matches!(self, CatalogConfig::Glue(_));
        if uri_required
            && self
                .inner()
                .uri
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(FaucetError::Config(format!(
                "iceberg: catalog '{kind}' requires a non-empty `uri`"
            )));
        }
        if !matches!(self, CatalogConfig::Rest(_)) {
            let warehouse = self.inner().warehouse.as_deref().unwrap_or("");
            if let WarehouseScheme::Unsupported(s) = warehouse_scheme(warehouse) {
                return Err(FaucetError::Config(format!(
                    "iceberg: warehouse scheme '{s}://' is not supported for the \
                     '{kind}' catalog; use file://, s3://, s3a://, or gs:// (or the \
                     REST catalog for other object stores)"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat(v: serde_json::Value) -> CatalogConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn kind_names_every_variant() {
        for ty in ["rest", "glue", "sql", "hms"] {
            assert_eq!(cat(serde_json::json!({ "type": ty })).kind(), ty);
        }
    }

    #[test]
    fn validate_connection_requires_uri_except_glue() {
        for ty in ["rest", "sql", "hms"] {
            let err = cat(serde_json::json!({ "type": ty }))
                .validate_connection()
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains(&format!("catalog '{ty}' requires"))
            );
        }
        assert!(
            cat(serde_json::json!({ "type": "rest", "uri": "  " }))
                .validate_connection()
                .is_err()
        );
        cat(serde_json::json!({ "type": "glue", "warehouse": "s3://b/w" }))
            .validate_connection()
            .expect("glue needs no uri");
    }

    #[test]
    fn validate_connection_checks_warehouse_scheme_for_non_rest() {
        let err = cat(serde_json::json!({ "type": "sql", "uri": "sqlite::memory:", "warehouse": "oss://b/w" }))
            .validate_connection()
            .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        assert!(err.to_string().contains("oss"));
        for w in [
            "s3://b/w",
            "s3a://b/w",
            "gs://b/w",
            "file:///tmp/w",
            "/tmp/w",
        ] {
            cat(serde_json::json!({ "type": "hms", "uri": "thrift://h:9083", "warehouse": w }))
                .validate_connection()
                .unwrap_or_else(|e| panic!("{w}: {e}"));
        }
        cat(serde_json::json!({ "type": "rest", "uri": "http://x", "warehouse": "oss://b/w" }))
            .validate_connection()
            .expect("rest accepts any scheme");
    }

    #[test]
    fn catalog_rest_round_trip() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://catalog.example.com",
            "warehouse": "s3://lake/wh",
            "credential": "my-token",
            "properties": { "region": "us-east-1" }
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Rest(_)));
        let inner = cat.inner();
        assert_eq!(inner.uri.as_deref(), Some("https://catalog.example.com"));
        assert_eq!(inner.credential.as_deref(), Some("my-token"));
        assert_eq!(
            inner.properties.get("region").map(String::as_str),
            Some("us-east-1")
        );

        // Re-serialize and re-parse.
        let json = serde_json::to_value(&cat).unwrap();
        assert_eq!(json["type"], "rest");
        let _cat2: CatalogConfig = serde_json::from_value(json).unwrap();
    }

    #[test]
    fn catalog_glue_round_trip() {
        let v = serde_json::json!({ "type": "glue", "warehouse": "s3://lake/wh" });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Glue(_)));
    }

    #[test]
    fn catalog_sql_round_trip() {
        let v = serde_json::json!({
            "type": "sql",
            "uri": "postgres://localhost/meta",
            "warehouse": "s3://lake/wh"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Sql(_)));
    }

    #[test]
    fn catalog_hms_round_trip() {
        let v = serde_json::json!({ "type": "hms", "uri": "thrift://hms:9083" });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        assert!(matches!(cat, CatalogConfig::Hms(_)));
    }

    #[test]
    fn debug_redacts_credential() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://catalog.example.com/api",
            "credential": "super-secret-token"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        let debug_str = format!("{cat:?}");
        assert!(
            !debug_str.contains("super-secret-token"),
            "credential must be redacted: {debug_str}"
        );
        assert!(
            debug_str.contains("***"),
            "should show *** placeholder: {debug_str}"
        );
    }

    #[test]
    fn debug_redacts_uri() {
        let v = serde_json::json!({
            "type": "rest",
            "uri": "https://user:pass@catalog.example.com"
        });
        let cat: CatalogConfig = serde_json::from_value(v).unwrap();
        let debug_str = format!("{cat:?}");
        // The URI field is redacted to "***" when present.
        assert!(
            !debug_str.contains("user:pass"),
            "uri userinfo must be redacted: {debug_str}"
        );
    }

    #[test]
    fn warehouse_scheme_local_variants() {
        use super::{WarehouseScheme, warehouse_scheme};
        for w in [
            "",
            "/tmp/warehouse",
            "./wh",
            "relative/dir",
            "file:///tmp/wh",
        ] {
            assert!(
                matches!(warehouse_scheme(w), WarehouseScheme::Local),
                "{w:?} should be Local"
            );
        }
    }

    #[test]
    fn warehouse_scheme_s3_preserves_scheme() {
        use super::{WarehouseScheme, warehouse_scheme};
        assert!(matches!(
            warehouse_scheme("s3://bucket/wh"),
            WarehouseScheme::S3("s3")
        ));
        assert!(matches!(
            warehouse_scheme("s3a://bucket/wh"),
            WarehouseScheme::S3("s3a")
        ));
        assert!(matches!(
            warehouse_scheme("S3://bucket/wh"),
            WarehouseScheme::S3("s3")
        ));
    }

    #[test]
    fn warehouse_scheme_gcs() {
        use super::{WarehouseScheme, warehouse_scheme};
        assert!(matches!(
            warehouse_scheme("gs://bucket/wh"),
            WarehouseScheme::Gcs
        ));
    }

    #[test]
    fn warehouse_scheme_unsupported() {
        use super::{WarehouseScheme, warehouse_scheme};
        match warehouse_scheme("oss://bucket/wh") {
            WarehouseScheme::Unsupported(s) => assert_eq!(s, "oss"),
            other => panic!("expected Unsupported, got {other:?}"),
        }
        assert!(matches!(
            warehouse_scheme("abfss://x/y"),
            WarehouseScheme::Unsupported(_)
        ));
    }
}
