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

// ── Host identity ─────────────────────────────────────────────────────────────

/// The catalog property that makes the warehouse storage assume a role.
pub const ASSUME_ROLE_PROP: &str = "client.assume-role.arn";

fn region_from_env() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()))
}

/// Whether `props` name storage credentials of their own for a warehouse of
/// `scheme`: S3 keys (`s3.access-key-id` and `s3.secret-access-key`) or
/// anonymous access; a GCS key, token or anonymous access. A local warehouse
/// needs none. The Glue catalog also hands its `aws_*` keys to storage, which
/// [`host_identity`] counts for `glue` only.
pub fn storage_names_credentials(
    scheme: &WarehouseScheme,
    props: &std::collections::HashMap<String, String>,
) -> bool {
    let set = |k: &str| props.get(k).is_some_and(|v| !v.trim().is_empty());
    let on = |k: &str| {
        props.get(k).is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
    };
    match scheme {
        WarehouseScheme::Local | WarehouseScheme::Unsupported(_) => true,
        WarehouseScheme::S3(_) => {
            (set("s3.access-key-id") && set("s3.secret-access-key")) || on("s3.allow-anonymous")
        }
        WarehouseScheme::Gcs => {
            set("gcs.credentials-json")
                || set("gcs.oauth2.token")
                || on("gcs.allow-anonymous")
                || on("gcs.no-auth")
        }
    }
}

/// The fields of an Iceberg source or sink config (its `catalog` block) that
/// make it authenticate with the identity of the host faucet runs on:
///
/// - `catalog.properties.client.assume-role.arn` (the storage assumes a role
///   through the host's credentials);
/// - for `glue`: `catalog.properties` when the Glue client has no
///   `aws_access_key_id` / `aws_secret_access_key`,
///   `catalog.properties.profile_name`, and `catalog.properties.region_name`
///   when neither it nor the process environment names a region;
/// - for `glue`, `sql` and `hms`: `catalog.properties` when an `s3://` or
///   `gs://` warehouse's storage names no credential of its own.
///
/// A `rest` catalog may hand out storage credentials at run time, so it is
/// checked when its storage is built.
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    use faucet_core::capabilities::config::{at, str_at, tag_at};
    let Some(kind) = tag_at(config, "/catalog") else {
        return Vec::new();
    };
    let props: std::collections::HashMap<String, String> = at(config, "/catalog/properties")
        .and_then(|p| p.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let set = |k: &str| props.get(k).is_some_and(|v| !v.trim().is_empty());
    let mut fields = Vec::new();
    if set(ASSUME_ROLE_PROP) {
        fields.push(format!("catalog.properties.{ASSUME_ROLE_PROP}"));
    }
    if kind == "glue" {
        if !(set("aws_access_key_id") && set("aws_secret_access_key")) {
            fields.push("catalog.properties".to_string());
        }
        if set("profile_name") {
            fields.push("catalog.properties.profile_name".to_string());
        }
        if !set("region_name") && !region_from_env() {
            fields.push("catalog.properties.region_name".to_string());
        }
    }
    if matches!(kind, "glue" | "sql" | "hms") {
        let scheme = warehouse_scheme(str_at(config, "/catalog/warehouse").unwrap_or(""));
        let glue_keys = kind == "glue"
            && matches!(scheme, WarehouseScheme::S3(_))
            && set("aws_access_key_id")
            && set("aws_secret_access_key");
        if !(glue_keys || storage_names_credentials(&scheme, &props))
            && !fields.iter().any(|f| f == "catalog.properties")
        {
            fields.push("catalog.properties".to_string());
        }
    }
    fields
}

/// Refuse, where the task may not use the host's identity, a Glue catalog
/// client that would use it ([`host_identity`]'s Glue rules).
pub fn check_glue_client(
    props: &std::collections::HashMap<String, String>,
) -> Result<(), faucet_core::FaucetError> {
    let set = |k: &str| props.get(k).is_some_and(|v| !v.trim().is_empty());
    if !(set("aws_access_key_id") && set("aws_secret_access_key")) {
        faucet_core::resolve::require_host_credentials("iceberg", "catalog.properties")?;
    }
    if set("profile_name") {
        faucet_core::resolve::require_host_credentials(
            "iceberg",
            "catalog.properties.profile_name",
        )?;
    }
    if !set("region_name") && !region_from_env() {
        faucet_core::resolve::require_host_credentials(
            "iceberg",
            "catalog.properties.region_name",
        )?;
    }
    Ok(())
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
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub uri: Option<String>,

    /// Object-storage warehouse root, e.g. `s3://lake/warehouse`.
    #[serde(default)]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub warehouse: Option<String>,

    /// REST bearer token or other catalog-specific credential.
    ///
    /// Redacted in `Debug` output — never logged.
    #[serde(default)]
    pub credential: Option<String>,

    /// Arbitrary catalog properties passed through to the catalog builder
    /// and the warehouse file IO (e.g. S3 region, endpoint, access key).
    /// Keys that name an endpoint (`*endpoint*`, `*uri`, `*url`, a proxy,
    /// `gcs.service.path`) or a local file (`*-file`, `*_file`, `*.file`,
    /// `warehouse`) are checked like the typed fields by a host that confines
    /// configs (`faucet serve`).
    #[serde(default)]
    #[schemars(extend("x-faucet-resource-keys" = {
        "*endpoint*": "url",
        "*uri": "url",
        "*url": "url",
        "*proxy*": "url",
        "gcs.service.path": "url",
        "*-file": "path",
        "*_file": "path",
        "*.file": "path",
        "warehouse": "path"
    }))]
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

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        use faucet_core::json;
        let keys = json!({"s3.access-key-id": "a", "s3.secret-access-key": "s"});
        for kind in ["sql", "hms"] {
            let bare = json!({"catalog": {"type": kind, "warehouse": "s3://b/wh"}});
            assert_eq!(host_identity(&bare), vec!["catalog.properties"], "{kind}");
            let keyed =
                json!({"catalog": {"type": kind, "warehouse": "s3://b/wh", "properties": keys}});
            assert!(host_identity(&keyed).is_empty(), "{kind}");
            let local = json!({"catalog": {"type": kind, "warehouse": "/tmp/wh"}});
            assert!(host_identity(&local).is_empty(), "{kind}");
            let gcs = json!({"catalog": {"type": kind, "warehouse": "gs://b/wh",
                "properties": {"gcs.credentials-json": "{}"}}});
            assert!(host_identity(&gcs).is_empty(), "{kind}");
            let role = json!({"catalog": {"type": kind, "warehouse": "s3://b/wh", "properties": {
                "s3.access-key-id": "a", "s3.secret-access-key": "s",
                "client.assume-role.arn": "arn:aws:iam::1:role/r"}}});
            assert_eq!(
                host_identity(&role),
                vec!["catalog.properties.client.assume-role.arn"]
            );
        }
        let rest = json!({"catalog": {"type": "rest", "uri": "https://cat.example"}});
        assert!(host_identity(&rest).is_empty());
        let glue = json!({"catalog": {"type": "glue", "warehouse": "s3://b/wh",
            "properties": {"region_name": "us-east-1"}}});
        assert_eq!(host_identity(&glue), vec!["catalog.properties"]);
        let glue_keyed = json!({"catalog": {"type": "glue", "warehouse": "s3://b/wh", "properties": {
            "region_name": "us-east-1", "aws_access_key_id": "a", "aws_secret_access_key": "s"}}});
        assert!(host_identity(&glue_keyed).is_empty());
        let glue_profile = json!({"catalog": {"type": "glue", "warehouse": "s3://b/wh", "properties": {
            "region_name": "us-east-1", "aws_access_key_id": "a", "aws_secret_access_key": "s",
            "profile_name": "p"}}});
        assert_eq!(
            host_identity(&glue_profile),
            vec!["catalog.properties.profile_name"]
        );
        let unregioned = json!({"catalog": {"type": "glue", "warehouse": "s3://b/wh", "properties": {
            "aws_access_key_id": "a", "aws_secret_access_key": "s"}}});
        assert_eq!(host_identity(&unregioned).is_empty(), region_from_env());
        assert!(host_identity(&json!({})).is_empty());
    }

    #[test]
    fn glue_client_keys_name_storage_only_for_glue() {
        use faucet_core::json;
        for kind in ["sql", "hms"] {
            let aws_keys = json!({"catalog": {"type": kind, "warehouse": "s3://b/wh", "properties": {
                "aws_access_key_id": "a", "aws_secret_access_key": "s"}}});
            assert_eq!(
                host_identity(&aws_keys),
                vec!["catalog.properties"],
                "{kind}"
            );
        }
        let glue = json!({"catalog": {"type": "glue", "warehouse": "s3://b/wh", "properties": {
            "region_name": "us-east-1", "aws_access_key_id": "a", "aws_secret_access_key": "s"}}});
        assert!(host_identity(&glue).is_empty());
        let props: std::collections::HashMap<String, String> =
            [("aws_access_key_id", "a"), ("aws_secret_access_key", "s")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        assert!(!storage_names_credentials(
            &WarehouseScheme::S3("s3"),
            &props
        ));
    }

    #[tokio::test]
    async fn glue_clients_without_keys_are_refused_inside_a_caller_run() {
        let keyed: std::collections::HashMap<String, String> = [
            ("aws_access_key_id", "a"),
            ("aws_secret_access_key", "s"),
            ("region_name", "us-east-1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        faucet_core::resolve::with_host_credentials(false, async {
            let err = check_glue_client(&Default::default())
                .unwrap_err()
                .to_string();
            assert!(err.contains("iceberg: `catalog.properties`"), "{err}");
            assert!(check_glue_client(&keyed).is_ok());
            let mut profiled = keyed.clone();
            profiled.insert("profile_name".into(), "p".into());
            assert!(check_glue_client(&profiled).is_err());
        })
        .await;
        assert!(check_glue_client(&Default::default()).is_ok());
    }

    fn cat(v: serde_json::Value) -> CatalogConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn endpoint_and_file_properties_are_resources() {
        use faucet_core::resolve::{ReferencePolicy, ResourceKind, annotated_resources};
        let schema = serde_json::to_value(schemars::schema_for!(CatalogInner)).unwrap();
        let config = serde_json::json!({
            "warehouse": "s3://lake/wh",
            "properties": {
                "s3.endpoint": "http://10.0.0.7:9000",
                "gcs.service.path": "http://10.0.0.8",
                "oauth2-server-uri": "http://10.0.0.9/token",
                "uri": "http://10.0.0.10",
                "gcs.credentials-file": "/etc/key.json",
                "s3.region": "us-east-1",
                "s3.path-style-access": "true"
            }
        });
        let mut found: Vec<(String, ResourceKind)> = annotated_resources(&schema, &config)
            .into_iter()
            .filter(|r| r.pointer.starts_with("/properties/"))
            .map(|r| (r.pointer, r.kind))
            .collect();
        found.sort();
        assert_eq!(
            found,
            vec![
                (
                    "/properties/gcs.credentials-file".into(),
                    ResourceKind::Path
                ),
                ("/properties/gcs.service.path".into(), ResourceKind::Url),
                ("/properties/oauth2-server-uri".into(), ResourceKind::Url),
                ("/properties/s3.endpoint".into(), ResourceKind::Url),
                ("/properties/uri".into(), ResourceKind::Url),
            ]
        );
        let policy = ReferencePolicy::open().with_url_hosts(["lake"]);
        let refused = annotated_resources(&schema, &config)
            .iter()
            .filter(|r| r.kind == ResourceKind::Url && policy.check(r).is_err())
            .count();
        assert_eq!(refused, 4);
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
