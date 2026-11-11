//! Staged bulk load (#528): stage a page to object storage, then have the
//! warehouse pull it with its native bulk-load command (`COPY` / `COPY INTO` /
//! load-from-URI / `s3()` table function).
//!
//! Row-by-row / multi-row `INSERT` is dramatically slower than the native bulk
//! path on every cloud warehouse — for Redshift and Snowflake the staged
//! `COPY` is the documented, production path. This module holds the **shared,
//! I/O-light** pieces every capable sink reuses:
//!
//! - [`StagingSpec`] — the user-facing `staging:` config block.
//! - [`StagingLocation`] — a parsed `s3://` / `gs://` / `az://` location.
//! - [`StagedFile`] — a written object's coordinates (uri / rows / bytes).
//! - record → bytes serialization for `jsonl` / `csv` (+ optional compression),
//!   and run-scoped object-key planning.
//! - [`Sink::supports_staged_load`](crate::Sink::supports_staged_load) — a
//!   defaulted, object-safe capability flag.
//!
//! The object-store **upload** itself lives behind the crate `staging` Cargo
//! feature (it pulls `object_store`), so `faucet-core` stays lightweight for
//! connector authors who don't need it. Each warehouse sink turns the
//! [`StagedFile`]s into its own load SQL — those generators are pure functions in
//! the individual sink crates and unit-tested there.

use crate::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Physical file format written to the stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StagingFormat {
    /// One JSON object per line. The default — every warehouse can load it and
    /// it maps by column name (no positional column list needed).
    #[default]
    Jsonl,
    /// Comma-separated values with a header row.
    Csv,
    /// Apache Parquet. Columnar + compressed; the fastest to load where the
    /// warehouse supports it. Written by the sink (needs Arrow), not by this
    /// module's byte serializers.
    Parquet,
}

impl StagingFormat {
    /// The file extension (before any compression suffix).
    pub fn extension(self) -> &'static str {
        match self {
            StagingFormat::Jsonl => "jsonl",
            StagingFormat::Csv => "csv",
            StagingFormat::Parquet => "parquet",
        }
    }

    /// Whether text compression (`gzip`/`zstd`) is meaningful for this format.
    /// Parquet compresses internally, so an outer codec is rejected.
    pub fn allows_text_compression(self) -> bool {
        matches!(self, StagingFormat::Jsonl | StagingFormat::Csv)
    }
}

/// Outer compression for text staging formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StagingCompression {
    /// No compression (the default).
    #[default]
    None,
    /// gzip (`.gz`).
    Gzip,
    /// zstd (`.zst`).
    Zstd,
}

impl StagingCompression {
    /// The filename suffix this codec adds (`""` for `None`).
    pub fn suffix(self) -> &'static str {
        match self {
            StagingCompression::None => "",
            StagingCompression::Gzip => ".gz",
            StagingCompression::Zstd => ".zst",
        }
    }
}

/// When staged objects are deleted after a load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StagingCleanup {
    /// Delete staged objects whether the load succeeded or failed (the default —
    /// keeps the stage tidy).
    #[default]
    Always,
    /// Delete only after a successful load; leave them on failure for inspection.
    OnSuccess,
    /// Never delete — the caller/operator manages the stage lifecycle.
    Never,
}

impl StagingCleanup {
    /// Whether staged objects should be removed given the load `succeeded`.
    pub fn should_delete(self, succeeded: bool) -> bool {
        match self {
            StagingCleanup::Always => true,
            StagingCleanup::OnSuccess => succeeded,
            StagingCleanup::Never => false,
        }
    }
}

/// The object-store scheme of a [`StagingLocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StagingScheme {
    /// Amazon S3 (`s3://`).
    S3,
    /// Google Cloud Storage (`gs://`).
    Gcs,
    /// Azure Blob Storage (`az://`).
    Azure,
}

impl StagingScheme {
    /// The canonical URI scheme prefix (`s3` / `gs` / `az`).
    pub fn as_str(self) -> &'static str {
        match self {
            StagingScheme::S3 => "s3",
            StagingScheme::Gcs => "gs",
            StagingScheme::Azure => "az",
        }
    }
}

/// The user-facing `staging:` config block, flattened onto a capable sink's
/// config. Opt-in: absent means the sink uses its ordinary `INSERT` path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StagingSpec {
    /// Object-store location to stage files under, e.g.
    /// `s3://my-bucket/faucet-stage/`. Scheme selects the backend
    /// (`s3://` / `gs://` / `az://`).
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub location: String,
    /// File format written to the stage. Default `parquet`.
    #[serde(default = "default_format")]
    pub format: StagingFormat,
    /// Outer compression for `csv`/`jsonl` (ignored for `parquet`). Default `none`.
    #[serde(default)]
    pub compression: StagingCompression,
    /// When staged objects are deleted. Default `always`.
    #[serde(default)]
    pub cleanup: StagingCleanup,
    /// Extra options appended verbatim to the warehouse's native load command
    /// (e.g. Redshift `"TIMEFORMAT 'auto' STATUPDATE ON"`). Passed through
    /// unquoted — the operator owns its correctness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_options: Option<String>,
    /// How faucet authenticates to the stage when it writes the files
    /// (default: the store's default chain, i.e. the identity of the host
    /// faucet runs on). The warehouse reads them with its own grant.
    #[serde(default, skip_serializing_if = "StagingCredentials::is_default")]
    pub credentials: StagingCredentials,
}

/// How faucet authenticates to a staging location. Serializes as
/// `{ type: <method>, config: { … } }`, the variant shapes of the S3, GCS and
/// Azure connectors' credentials.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum StagingCredentials {
    /// The store's default chain (environment, instance or workload role,
    /// local credential files): the identity of the host faucet runs on.
    #[default]
    Default,
    /// Static S3 access keys (`s3://`).
    AccessKey {
        /// Access key id.
        access_key_id: String,
        /// Secret access key.
        secret_access_key: String,
        /// Optional session token (temporary credentials).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<String>,
    },
    /// A GCS service-account JSON key file (`gs://`).
    ServiceAccountJsonFile {
        /// Path to the key file.
        #[schemars(extend("x-faucet-resource" = "path"))]
        path: String,
    },
    /// A GCS service-account JSON key, inline (`gs://`).
    ServiceAccountJsonInline {
        /// The key JSON.
        json: String,
    },
    /// An Azure storage-account access key (`az://`).
    AccountKey {
        /// Base64 account key.
        account_key: String,
    },
    /// An Azure shared-access-signature token (`az://`).
    SasToken {
        /// SAS token string.
        sas_token: String,
    },
}

impl std::fmt::Debug for StagingCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::util::fmt_redacted(
            f,
            "StagingCredentials",
            self,
            &[
                "secret_access_key",
                "session_token",
                "json",
                "account_key",
                "sas_token",
            ],
        )
    }
}

impl StagingCredentials {
    /// Whether this is [`StagingCredentials::Default`].
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    /// The scheme these credentials belong to, `None` for the default chain.
    pub fn scheme(&self) -> Option<StagingScheme> {
        match self {
            Self::Default => None,
            Self::AccessKey { .. } => Some(StagingScheme::S3),
            Self::ServiceAccountJsonFile { .. } | Self::ServiceAccountJsonInline { .. } => {
                Some(StagingScheme::Gcs)
            }
            Self::AccountKey { .. } | Self::SasToken { .. } => Some(StagingScheme::Azure),
        }
    }

    /// The `object_store` option keys that select these credentials (empty
    /// for the default chain).
    pub fn store_options(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::Default => Vec::new(),
            Self::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut out = vec![
                    ("aws_access_key_id", access_key_id.clone()),
                    ("aws_secret_access_key", secret_access_key.clone()),
                ];
                if let Some(token) = session_token {
                    out.push(("aws_session_token", token.clone()));
                }
                out
            }
            Self::ServiceAccountJsonFile { path } => {
                vec![("google_service_account", path.clone())]
            }
            Self::ServiceAccountJsonInline { json } => {
                vec![("google_service_account_key", json.clone())]
            }
            Self::AccountKey { account_key } => {
                vec![("azure_storage_account_key", account_key.clone())]
            }
            Self::SasToken { sas_token } => vec![(
                "azure_storage_sas_key",
                sas_token.trim_start_matches('?').to_string(),
            )],
        }
    }

    /// Refuse credentials of another scheme than the location's.
    pub fn check_scheme(&self, scheme: StagingScheme) -> Result<(), FaucetError> {
        match self.scheme() {
            Some(own) if own != scheme => Err(FaucetError::Config(format!(
                "staging: these `credentials` are for `{}://` locations, not `{}://`",
                own.as_str(),
                scheme.as_str()
            ))),
            _ => Ok(()),
        }
    }
}

/// The config fields that make the staging block at `pointer` authenticate
/// with the identity of the host faucet runs on: its `credentials` when they
/// select (or default to) the default chain. Empty when no block is set.
pub fn staging_host_identity(config: &Value, pointer: &str) -> Vec<String> {
    use crate::capabilities::config::{is_set_at, tag_at};
    let credentials = format!("{pointer}/credentials");
    if is_set_at(config, pointer) && tag_at(config, &credentials).unwrap_or("default") == "default"
    {
        vec![credentials.trim_start_matches('/').replace('/', ".")]
    } else {
        Vec::new()
    }
}

fn default_format() -> StagingFormat {
    StagingFormat::Parquet
}

impl StagingSpec {
    /// Parse + validate the block at config-load time. `allowed_schemes` /
    /// `allowed_formats` are the warehouse's capabilities (e.g. Redshift `COPY`
    /// reads S3 only). Returns the parsed [`StagingLocation`] on success.
    pub fn validate(
        &self,
        allowed_schemes: &[StagingScheme],
        allowed_formats: &[StagingFormat],
    ) -> Result<StagingLocation, FaucetError> {
        let loc = StagingLocation::parse(&self.location)?;
        if !allowed_schemes.contains(&loc.scheme) {
            return Err(FaucetError::Config(format!(
                "staging: location scheme `{}://` is not supported by this sink (supported: {})",
                loc.scheme.as_str(),
                allowed_schemes
                    .iter()
                    .map(|s| format!("{}://", s.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if !allowed_formats.contains(&self.format) {
            return Err(FaucetError::Config(format!(
                "staging: format `{:?}` is not supported by this sink (supported: {})",
                self.format,
                allowed_formats
                    .iter()
                    .map(|f| f.extension())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        self.credentials.check_scheme(loc.scheme)?;
        if self.compression != StagingCompression::None && !self.format.allows_text_compression() {
            return Err(FaucetError::Config(
                "staging: `compression` applies only to csv/jsonl (parquet compresses internally)"
                    .into(),
            ));
        }
        Ok(loc)
    }
}

/// A parsed staging location: a scheme, a bucket/container, and a key prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingLocation {
    /// Backend scheme.
    pub scheme: StagingScheme,
    /// Bucket (S3/GCS) or container (Azure).
    pub bucket: String,
    /// Key prefix within the bucket, no leading slash, trailing slash trimmed.
    pub prefix: String,
    /// Azure storage account, when the URI names one
    /// (`abfss://container@account.dfs.core.windows.net/…`).
    pub account: Option<String>,
}

impl StagingLocation {
    /// Parse an `s3://bucket/prefix` / `gs://…` / `az://…` URI.
    pub fn parse(uri: &str) -> Result<Self, FaucetError> {
        let uri = uri.trim();
        let (scheme_str, rest) = uri.split_once("://").ok_or_else(|| {
            FaucetError::Config(format!(
                "staging: location `{uri}` must be a URI like `s3://bucket/prefix/`"
            ))
        })?;
        let scheme = match scheme_str {
            "s3" | "s3a" => StagingScheme::S3,
            "gs" | "gcs" => StagingScheme::Gcs,
            "az" | "azure" | "abfs" | "abfss" => StagingScheme::Azure,
            other => {
                return Err(FaucetError::Config(format!(
                    "staging: unsupported location scheme `{other}://` (use s3://, gs://, or az://)"
                )));
            }
        };
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p),
            None => (rest, ""),
        };
        // ADLS form: `container@account.dfs.core.windows.net` (CORE-101).
        let (bucket, account) = match (scheme, bucket.split_once('@')) {
            (StagingScheme::Azure, Some((container, host))) => {
                let account = host.split('.').next().unwrap_or_default();
                if account.is_empty() {
                    return Err(FaucetError::Config(format!(
                        "staging: location `{uri}` names no storage account after `@`"
                    )));
                }
                (container, Some(account.to_string()))
            }
            (_, Some(_)) => {
                return Err(FaucetError::Config(format!(
                    "staging: location `{uri}`: `@` is only valid in an Azure \
                     `container@account.host` location"
                )));
            }
            (_, None) => (bucket, None),
        };
        if bucket.trim().is_empty() {
            return Err(FaucetError::Config(format!(
                "staging: location `{uri}` has no bucket/container"
            )));
        }
        Ok(StagingLocation {
            scheme,
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
            account,
        })
    }

    /// Build the run-scoped object key for one staged part. A per-run prefix
    /// (`{prefix}/{scope}/{run_id}/`) keeps concurrent runs from colliding and
    /// makes an orphan sweep possible.
    pub fn object_key(
        &self,
        scope: &str,
        run_id: &str,
        seq: usize,
        format: StagingFormat,
        compression: StagingCompression,
    ) -> String {
        let mut parts: Vec<String> = Vec::new();
        if !self.prefix.is_empty() {
            parts.push(self.prefix.clone());
        }
        let scope = sanitize_segment(scope);
        if !scope.is_empty() {
            parts.push(scope);
        }
        let run = sanitize_segment(run_id);
        if !run.is_empty() {
            parts.push(run);
        }
        parts.push(format!(
            "part-{seq:05}.{}{}",
            format.extension(),
            compression.suffix()
        ));
        parts.join("/")
    }

    /// The full URI (`{scheme}://{bucket}/{key}`) for a key under this location.
    pub fn uri_for(&self, key: &str) -> String {
        format!("{}://{}/{}", self.scheme.as_str(), self.bucket, key)
    }
}

/// Sanitize one object-key path segment: keep it filesystem/URI-safe so a
/// pipeline/run name with `:` or spaces can't produce a malformed key.
fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '=') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

/// Coordinates of one object written to the stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    /// Full object URI (`s3://bucket/key`).
    pub uri: String,
    /// Object key within the bucket.
    pub key: String,
    /// Number of records written into it.
    pub rows: usize,
    /// Encoded byte length.
    pub bytes: usize,
}

/// Serialize a page of records to the staging `format`'s raw bytes (before
/// compression). Pure — the upload path applies compression and writes.
///
/// `parquet` is **not** serialized here (it needs Arrow, which lives in the
/// sink); callers that pick parquet must serialize it themselves.
pub fn serialize_records(records: &[Value], format: StagingFormat) -> Result<Vec<u8>, FaucetError> {
    match format {
        StagingFormat::Jsonl => serialize_jsonl(records),
        StagingFormat::Csv => serialize_csv(records),
        StagingFormat::Parquet => Err(FaucetError::Sink(
            "staging: parquet bytes must be serialized by the sink (Arrow), not `serialize_records`"
                .into(),
        )),
    }
}

fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut out = Vec::new();
    for r in records {
        let line = serde_json::to_vec(r)
            .map_err(|e| FaucetError::Sink(format!("staging: jsonl encode failed: {e}")))?;
        out.extend_from_slice(&line);
        out.push(b'\n');
    }
    Ok(out)
}

/// CSV with a header row. The column set is the union of top-level keys across
/// the page, in first-seen order, so a jagged page still produces a rectangular
/// file (missing and null cells are bare empty fields; an empty string is
/// `""`). Nested objects/arrays are JSON-encoded.
fn serialize_csv(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut columns: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for r in records {
        if let Some(map) = r.as_object() {
            for k in map.keys() {
                if seen.insert(k.as_str()) {
                    columns.push(k.clone());
                }
            }
        } else {
            return Err(FaucetError::Sink(
                "staging: csv format requires JSON-object records".into(),
            ));
        }
    }
    let mut out = String::new();
    out.push_str(
        &columns
            .iter()
            .map(|c| csv_field(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');
    for r in records {
        let map = r.as_object().expect("checked above");
        let row = columns
            .iter()
            .map(|c| match map.get(c) {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => csv_field(s),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Number(n)) => n.to_string(),
                Some(other) => csv_field(&crate::util::canonical_json(other)),
            })
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&row);
        out.push('\n');
    }
    Ok(out.into_bytes())
}

/// Quote a CSV field per RFC 4180 when it contains a comma, quote, or newline.
/// The empty string is always quoted (`""`) so a loader can tell it apart from
/// a null, which is written as a bare empty field (CORE-36).
fn csv_field(s: &str) -> String {
    if s.is_empty() || s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(feature = "staging")]
pub use upload::{StageUploader, build_object_store, build_object_store_with, object_store_for};

#[cfg(feature = "staging")]
mod upload {
    use super::*;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::sync::Arc;

    /// Build the object store for a location from the store's default
    /// credential chain ([`build_object_store_with`] with
    /// [`StagingCredentials::Default`]).
    pub fn build_object_store(loc: &StagingLocation) -> Result<Arc<dyn ObjectStore>, FaucetError> {
        build_object_store_with(loc, &StagingCredentials::Default)
    }

    /// Build the object store faucet writes a location's files through (the
    /// warehouse reads them with its own grant). The default chain is refused
    /// where the task may not use the host's identity
    /// ([`crate::resolve::require_host_credentials`]).
    pub fn build_object_store_with(
        loc: &StagingLocation,
        credentials: &StagingCredentials,
    ) -> Result<Arc<dyn ObjectStore>, FaucetError> {
        object_store_for(loc.scheme, &loc.bucket, loc.account.as_deref(), credentials)
    }

    /// [`build_object_store_with`] for a bucket (or container, with its
    /// account) of `scheme`.
    pub fn object_store_for(
        scheme: StagingScheme,
        bucket: &str,
        account: Option<&str>,
        credentials: &StagingCredentials,
    ) -> Result<Arc<dyn ObjectStore>, FaucetError> {
        credentials.check_scheme(scheme)?;
        if credentials.is_default() {
            crate::resolve::require_host_credentials("staging", "credentials")?;
        }
        let failed = |what: &str| {
            let what = what.to_string();
            let bucket = bucket.to_string();
            move |e: object_store::Error| {
                FaucetError::Sink(format!("staging: {what} store for `{bucket}`: {e}"))
            }
        };
        let store: Arc<dyn ObjectStore> = match scheme {
            StagingScheme::S3 => {
                let builder = match credentials {
                    StagingCredentials::AccessKey {
                        access_key_id,
                        secret_access_key,
                        session_token,
                    } => {
                        let builder = s3_settings_from_env()
                            .with_access_key_id(access_key_id)
                            .with_secret_access_key(secret_access_key);
                        match session_token {
                            Some(token) => builder.with_token(token),
                            None => builder,
                        }
                    }
                    _ => object_store::aws::AmazonS3Builder::from_env(),
                };
                Arc::new(
                    builder
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(failed("S3"))?,
                )
            }
            StagingScheme::Gcs => {
                let builder = match credentials {
                    StagingCredentials::ServiceAccountJsonFile { path } => {
                        object_store::gcp::GoogleCloudStorageBuilder::new()
                            .with_service_account_path(path)
                    }
                    StagingCredentials::ServiceAccountJsonInline { json } => {
                        object_store::gcp::GoogleCloudStorageBuilder::new()
                            .with_service_account_key(json)
                    }
                    _ => object_store::gcp::GoogleCloudStorageBuilder::from_env(),
                };
                Arc::new(
                    builder
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(failed("GCS"))?,
                )
            }
            StagingScheme::Azure => {
                use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
                let builder = match credentials {
                    StagingCredentials::AccountKey { account_key } => {
                        MicrosoftAzureBuilder::new().with_access_key(account_key)
                    }
                    StagingCredentials::SasToken { sas_token } => MicrosoftAzureBuilder::new()
                        .with_config(AzureConfigKey::SasKey, sas_token.trim_start_matches('?')),
                    _ => MicrosoftAzureBuilder::from_env(),
                };
                let account = account.map(str::to_string).or_else(|| {
                    (!credentials.is_default())
                        .then(|| std::env::var("AZURE_STORAGE_ACCOUNT_NAME").ok())
                        .flatten()
                });
                let builder = builder.with_container_name(bucket);
                Arc::new(
                    match account {
                        Some(account) => builder.with_account(account),
                        None => builder,
                    }
                    .build()
                    .map_err(failed("Azure"))?,
                )
            }
        };
        Ok(store)
    }

    /// An S3 builder with only the non-credential `AWS_*` settings of the
    /// environment (region, endpoint, addressing, HTTP client), so the keys
    /// a config names are the only credential the store uses.
    fn s3_settings_from_env() -> object_store::aws::AmazonS3Builder {
        use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .filter(|(k, _)| k.starts_with("AWS_"))
            .filter_map(|(k, v)| {
                let key: AmazonS3ConfigKey = k.to_ascii_lowercase().parse().ok()?;
                matches!(
                    key,
                    AmazonS3ConfigKey::Region
                        | AmazonS3ConfigKey::DefaultRegion
                        | AmazonS3ConfigKey::Endpoint
                        | AmazonS3ConfigKey::VirtualHostedStyleRequest
                        | AmazonS3ConfigKey::Client(_)
                )
                .then_some((key, v))
            })
            .fold(AmazonS3Builder::new(), |b, (k, v)| b.with_config(k, v))
    }

    /// Uploads staged objects to an [`ObjectStore`] and cleans them up. The store
    /// is injected so tests drive it against `object_store::memory::InMemory`.
    pub struct StageUploader {
        store: Arc<dyn ObjectStore>,
        location: StagingLocation,
        calls: crate::io::CallContext,
    }

    impl StageUploader {
        /// Build an uploader over an explicit store (used in tests) + location.
        pub fn new(store: Arc<dyn ObjectStore>, location: StagingLocation) -> Self {
            Self {
                store,
                location,
                calls: crate::io::CallContext::new(),
            }
        }

        /// Run every upload and delete as a backend call under `ctx` (run
        /// deadline, cancellation, round-trip metering). A delete is also bound
        /// by the context's attempt timeout; an upload is not, since its
        /// duration grows with the page and a timeout would fail a large page
        /// that is still making progress.
        pub fn with_call_context(mut self, ctx: crate::io::CallContext) -> Self {
            self.calls = ctx;
            self
        }

        /// Build an uploader whose store is constructed from the location's
        /// scheme + ambient credentials.
        pub fn from_location(location: StagingLocation) -> Result<Self, FaucetError> {
            Self::from_location_with(location, &StagingCredentials::Default)
        }

        /// [`StageUploader::from_location`] with the credentials the config
        /// names ([`build_object_store_with`]).
        pub fn from_location_with(
            location: StagingLocation,
            credentials: &StagingCredentials,
        ) -> Result<Self, FaucetError> {
            let store = build_object_store_with(&location, credentials)?;
            Ok(Self::new(store, location))
        }

        /// The location this uploader stages into.
        pub fn location(&self) -> &StagingLocation {
            &self.location
        }

        /// Serialize + (optionally) compress + upload one page as a single
        /// object, returning its [`StagedFile`]. `pre_encoded` lets a sink pass
        /// already-serialized bytes (e.g. Parquet from Arrow); otherwise the
        /// records are serialized per `spec.format`.
        pub async fn stage_page(
            &self,
            spec: &StagingSpec,
            scope: &str,
            run_id: &str,
            seq: usize,
            records: &[Value],
            pre_encoded: Option<Vec<u8>>,
        ) -> Result<StagedFile, FaucetError> {
            let raw = match pre_encoded {
                Some(bytes) => bytes,
                None => serialize_records(records, spec.format)?,
            };
            let body = compress(raw, spec.compression)?;
            let key = self
                .location
                .object_key(scope, run_id, seq, spec.format, spec.compression);
            let path = object_store::path::Path::from(key.clone());
            let bytes = body.len();
            self.calls
                .clone()
                .without_timeout()
                .call_once("stage_put", async {
                    self.store.put(&path, body.into()).await.map_err(|e| {
                        FaucetError::Sink(format!("staging: upload `{key}` failed: {e}"))
                    })
                })
                .await?;
            Ok(StagedFile {
                uri: self.location.uri_for(&key),
                key,
                rows: records.len(),
                bytes,
            })
        }

        /// Delete staged objects per the `cleanup` policy + whether the load
        /// `succeeded`. Best-effort: a delete error is logged, not propagated,
        /// so it never masks the load result.
        pub async fn cleanup(
            &self,
            files: &[StagedFile],
            cleanup: StagingCleanup,
            succeeded: bool,
        ) {
            if !cleanup.should_delete(succeeded) {
                return;
            }
            for f in files {
                let path = object_store::path::Path::from(f.key.clone());
                let deleted = self
                    .calls
                    .call("stage_delete", || async {
                        self.store
                            .delete(&path)
                            .await
                            .map_err(|e| FaucetError::Sink(e.to_string()))
                    })
                    .await;
                if let Err(e) = deleted {
                    tracing::warn!(key = %f.key, error = %e, "staging: cleanup delete failed");
                }
            }
        }
    }

    fn compress(raw: Vec<u8>, c: StagingCompression) -> Result<Vec<u8>, FaucetError> {
        match c {
            StagingCompression::None => Ok(raw),
            #[cfg(feature = "compression")]
            StagingCompression::Gzip => {
                crate::compression::compress_buf(&raw, crate::compression::Compression::Gzip)
            }
            #[cfg(feature = "compression")]
            StagingCompression::Zstd => {
                crate::compression::compress_buf(&raw, crate::compression::Compression::Zstd)
            }
            #[cfg(not(feature = "compression"))]
            StagingCompression::Gzip | StagingCompression::Zstd => Err(FaucetError::Config(
                "staging: compression requires the `compression` feature".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        assert!(staging_host_identity(&json!({}), "/staging").is_empty());
        assert_eq!(
            staging_host_identity(&json!({"staging": {"location": "s3://b/p/"}}), "/staging"),
            vec!["staging.credentials"]
        );
        let keyed = json!({"staging": {"location": "s3://b/p/", "credentials": {
            "type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}}});
        assert!(staging_host_identity(&keyed, "/staging").is_empty());
    }

    #[test]
    fn staging_credentials_must_match_the_location_scheme() {
        let spec: StagingSpec = serde_json::from_value(json!({
            "location": "gs://b/p/",
            "credentials": {"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}
        }))
        .unwrap();
        let err = spec
            .validate(&[StagingScheme::Gcs], &[StagingFormat::Parquet])
            .unwrap_err()
            .to_string();
        assert!(err.contains("`s3://`") && err.contains("`gs://`"), "{err}");
        for (creds, scheme) in [
            (
                json!({"type": "service_account_json_inline", "config": {"json": "{}"}}),
                StagingScheme::Gcs,
            ),
            (
                json!({"type": "service_account_json_file", "config": {"path": "k.json"}}),
                StagingScheme::Gcs,
            ),
            (
                json!({"type": "account_key", "config": {"account_key": "k"}}),
                StagingScheme::Azure,
            ),
            (
                json!({"type": "sas_token", "config": {"sas_token": "sv=1"}}),
                StagingScheme::Azure,
            ),
            (json!({"type": "default"}), StagingScheme::S3),
        ] {
            let c: StagingCredentials = serde_json::from_value(creds).unwrap();
            assert!(c.check_scheme(scheme).is_ok());
            let dbg = format!("{c:?}");
            assert!(!dbg.contains("sv=1") && !dbg.contains("\"k\""), "{dbg}");
        }
        assert!(StagingCredentials::default().is_default());
    }

    #[cfg(feature = "staging")]
    #[tokio::test]
    async fn staging_uploads_use_their_own_credentials() {
        let loc = |uri: &str| StagingLocation::parse(uri).unwrap();
        crate::resolve::with_host_credentials(false, async {
            for uri in [
                "s3://b/p/",
                "gs://b/p/",
                "az://c@acct.dfs.core.windows.net/p/",
            ] {
                let err = build_object_store(&loc(uri))
                    .expect_err("refused")
                    .to_string();
                assert!(err.contains("staging: `credentials`"), "{err}");
            }
            let s3 = StagingCredentials::AccessKey {
                access_key_id: "a".into(),
                secret_access_key: "s".into(),
                session_token: Some("t".into()),
            };
            assert!(build_object_store_with(&loc("s3://b/p/"), &s3).is_ok());
            let az = StagingCredentials::AccountKey {
                account_key: "a2V5".into(),
            };
            assert!(
                build_object_store_with(&loc("az://c@acct.dfs.core.windows.net/p/"), &az).is_ok()
            );
            let sas = StagingCredentials::SasToken {
                sas_token: "?sv=2024&sig=x".into(),
            };
            assert!(
                build_object_store_with(&loc("az://c@acct.dfs.core.windows.net/p/"), &sas).is_ok()
            );
            assert!(StageUploader::from_location_with(loc("s3://b/p/"), &s3).is_ok());
            assert!(StageUploader::from_location(loc("s3://b/p/")).is_err());
            let err = build_object_store_with(&loc("gs://b/p/"), &s3)
                .expect_err("wrong scheme")
                .to_string();
            assert!(err.contains("`s3://`"), "{err}");
        })
        .await;
        let trusted = crate::resolve::with_host_credentials(true, async {
            build_object_store(&loc("s3://b/p/")).is_ok()
        })
        .await;
        assert!(trusted);
    }

    /// An object inserted in reverse-sorted key order (#817).
    fn login_then_id() -> Value {
        let mut m = serde_json::Map::new();
        m.insert("login".into(), json!("o"));
        m.insert("id".into(), json!(2));
        Value::Object(m)
    }

    #[test]
    fn csv_encodes_nested_objects_with_sorted_keys() {
        let csv = serialize_csv(&[json!({"o": login_then_id()})]).unwrap();
        assert_eq!(
            String::from_utf8(csv).unwrap(),
            "o\n\"{\"\"id\"\":2,\"\"login\"\":\"\"o\"\"}\"\n"
        );
    }

    #[test]
    fn parse_location_variants() {
        let s3 = StagingLocation::parse("s3://bucket/faucet-stage/").unwrap();
        assert_eq!(s3.scheme, StagingScheme::S3);
        assert_eq!(s3.bucket, "bucket");
        assert_eq!(s3.prefix, "faucet-stage");

        let gs = StagingLocation::parse("gs://b/p/q").unwrap();
        assert_eq!(gs.scheme, StagingScheme::Gcs);
        assert_eq!(gs.prefix, "p/q");

        let az = StagingLocation::parse("az://container").unwrap();
        assert_eq!(az.scheme, StagingScheme::Azure);
        assert_eq!(az.prefix, "");

        // scheme aliases
        assert_eq!(
            StagingLocation::parse("s3a://b/p").unwrap().scheme,
            StagingScheme::S3
        );
        assert_eq!(
            StagingLocation::parse("gcs://b/p").unwrap().scheme,
            StagingScheme::Gcs
        );
    }

    #[test]
    fn parse_location_rejects_bad() {
        assert!(StagingLocation::parse("bucket/prefix").is_err()); // no scheme
        assert!(StagingLocation::parse("ftp://b/p").is_err()); // unknown scheme
        assert!(StagingLocation::parse("s3:///prefix").is_err()); // no bucket
        assert!(StagingLocation::parse("s3://a@b/p").is_err()); // @ outside azure
        assert!(StagingLocation::parse("abfss://c@/p").is_err()); // no account
    }

    #[test]
    fn parse_adls_container_at_account() {
        let l = StagingLocation::parse("abfss://cont@acct.dfs.core.windows.net/stage/x").unwrap();
        assert_eq!(l.scheme, StagingScheme::Azure);
        assert_eq!(l.bucket, "cont");
        assert_eq!(l.account.as_deref(), Some("acct"));
        assert_eq!(l.prefix, "stage/x");
        assert_eq!(StagingLocation::parse("az://c/p").unwrap().account, None);
    }

    #[test]
    fn object_key_is_run_scoped_and_sanitized() {
        let loc = StagingLocation::parse("s3://b/stage").unwrap();
        let k = loc.object_key(
            "pipe::row",
            "run 42",
            7,
            StagingFormat::Jsonl,
            StagingCompression::Gzip,
        );
        assert_eq!(k, "stage/pipe__row/run_42/part-00007.jsonl.gz");
        // no prefix
        let loc2 = StagingLocation::parse("s3://b").unwrap();
        let k2 = loc2.object_key(
            "s",
            "r",
            0,
            StagingFormat::Parquet,
            StagingCompression::None,
        );
        assert_eq!(k2, "s/r/part-00000.parquet");
        assert_eq!(loc2.uri_for(&k2), "s3://b/s/r/part-00000.parquet");
    }

    #[test]
    fn validate_enforces_scheme_and_format_and_compression() {
        let spec = StagingSpec {
            location: "s3://b/p".into(),
            format: StagingFormat::Parquet,
            compression: StagingCompression::None,
            cleanup: StagingCleanup::Always,
            copy_options: None,
            credentials: StagingCredentials::Default,
        };
        // ok for an S3+parquet sink
        assert!(
            spec.validate(&[StagingScheme::S3], &[StagingFormat::Parquet])
                .is_ok()
        );
        // wrong scheme
        assert!(
            spec.validate(&[StagingScheme::Gcs], &[StagingFormat::Parquet])
                .is_err()
        );
        // wrong format
        assert!(
            spec.validate(&[StagingScheme::S3], &[StagingFormat::Csv])
                .is_err()
        );
        // compression on parquet
        let mut bad = spec.clone();
        bad.compression = StagingCompression::Gzip;
        assert!(
            bad.validate(&[StagingScheme::S3], &[StagingFormat::Parquet])
                .is_err()
        );
    }

    #[test]
    fn cleanup_policy_matrix() {
        assert!(StagingCleanup::Always.should_delete(true));
        assert!(StagingCleanup::Always.should_delete(false));
        assert!(StagingCleanup::OnSuccess.should_delete(true));
        assert!(!StagingCleanup::OnSuccess.should_delete(false));
        assert!(!StagingCleanup::Never.should_delete(true));
        assert!(!StagingCleanup::Never.should_delete(false));
    }

    #[test]
    fn serialize_jsonl_one_object_per_line() {
        let recs = vec![json!({"a": 1}), json!({"a": 2, "b": "x"})];
        let bytes = serialize_records(&recs, StagingFormat::Jsonl).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert_eq!(s, "{\"a\":1}\n{\"a\":2,\"b\":\"x\"}\n");
    }

    #[test]
    fn serialize_csv_union_header_and_quoting() {
        let recs = vec![
            json!({"id": 1, "name": "a,b"}),
            json!({"id": 2, "note": "he said \"hi\""}),
        ];
        let bytes = serialize_records(&recs, StagingFormat::Csv).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        let mut lines = s.lines();
        assert_eq!(lines.next().unwrap(), "id,name,note");
        assert_eq!(lines.next().unwrap(), "1,\"a,b\",");
        assert_eq!(lines.next().unwrap(), "2,,\"he said \"\"hi\"\"\"");
        // An empty string is quoted; a null stays a bare empty field.
        let bytes = serialize_records(&[json!({"a": "", "b": null})], StagingFormat::Csv).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), "a,b\n\"\",\n");
    }

    #[test]
    fn serialize_parquet_is_rejected_here() {
        assert!(serialize_records(&[json!({"a":1})], StagingFormat::Parquet).is_err());
    }

    #[test]
    fn format_helpers() {
        assert_eq!(StagingFormat::Jsonl.extension(), "jsonl");
        assert!(StagingFormat::Csv.allows_text_compression());
        assert!(!StagingFormat::Parquet.allows_text_compression());
        assert_eq!(StagingCompression::Zstd.suffix(), ".zst");
        assert_eq!(StagingScheme::Gcs.as_str(), "gs");
    }

    #[test]
    fn spec_deserializes_with_defaults() {
        let spec: StagingSpec = serde_json::from_value(json!({"location": "s3://b/p"})).unwrap();
        assert_eq!(spec.format, StagingFormat::Parquet);
        assert_eq!(spec.compression, StagingCompression::None);
        assert_eq!(spec.cleanup, StagingCleanup::Always);
    }

    #[cfg(feature = "staging")]
    #[tokio::test]
    async fn uploader_stages_and_cleans_via_in_memory_store() {
        use object_store::{ObjectStore, ObjectStoreExt};
        use std::sync::Arc;
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let loc = StagingLocation::parse("s3://b/stage").unwrap();
        let up = StageUploader::new(store.clone(), loc);
        let spec = StagingSpec {
            location: "s3://b/stage".into(),
            format: StagingFormat::Jsonl,
            compression: StagingCompression::None,
            cleanup: StagingCleanup::OnSuccess,
            copy_options: None,
            credentials: StagingCredentials::Default,
        };
        let recs = vec![json!({"a": 1}), json!({"a": 2})];
        let f = up
            .stage_page(&spec, "pipe", "run1", 0, &recs, None)
            .await
            .unwrap();
        assert_eq!(f.rows, 2);
        assert_eq!(f.uri, "s3://b/stage/pipe/run1/part-00000.jsonl");
        // object exists
        let path = object_store::path::Path::from(f.key.clone());
        assert!(store.get(&path).await.is_ok());
        // cleanup on_success + succeeded -> deleted
        up.cleanup(std::slice::from_ref(&f), StagingCleanup::OnSuccess, true)
            .await;
        assert!(store.get(&path).await.is_err());
    }

    #[cfg(feature = "staging")]
    #[tokio::test]
    async fn uploader_runs_uploads_under_its_call_context() {
        use object_store::{ObjectStore, ObjectStoreExt};
        use std::sync::Arc;
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let loc = StagingLocation::parse("s3://b/stage").unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let up = StageUploader::new(store.clone(), loc)
            .with_call_context(crate::io::CallContext::new().with_cancel(cancel));
        let spec = StagingSpec {
            location: "s3://b/stage".into(),
            format: StagingFormat::Jsonl,
            compression: StagingCompression::None,
            cleanup: StagingCleanup::Always,
            copy_options: None,
            credentials: StagingCredentials::Default,
        };
        let err = up
            .stage_page(&spec, "p", "r", 0, &[json!({"a": 1})], None)
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Cancelled { .. }), "{err}");
        let path = object_store::path::Path::from("stage/p/r/part-00000.jsonl");
        assert!(store.get(&path).await.is_err(), "nothing was uploaded");
    }

    #[cfg(feature = "staging")]
    #[tokio::test]
    async fn an_upload_is_not_cut_by_the_attempt_timeout() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        use object_store::{ObjectStore, ObjectStoreExt};
        use std::sync::Arc;
        use std::time::Duration;
        let slow = ThrottledStore::new(
            object_store::memory::InMemory::new(),
            ThrottleConfig {
                wait_put_per_call: Duration::from_millis(200),
                ..ThrottleConfig::default()
            },
        );
        let store: Arc<dyn ObjectStore> = Arc::new(slow);
        let loc = StagingLocation::parse("s3://b/stage").unwrap();
        let up = StageUploader::new(store.clone(), loc).with_call_context(
            crate::io::CallContext::new().with_timeout(Duration::from_millis(20)),
        );
        let spec = StagingSpec {
            location: "s3://b/stage".into(),
            format: StagingFormat::Jsonl,
            compression: StagingCompression::None,
            cleanup: StagingCleanup::Always,
            copy_options: None,
            credentials: StagingCredentials::Default,
        };
        let f = up
            .stage_page(&spec, "p", "r", 0, &[json!({"a": 1})], None)
            .await
            .expect("an upload slower than the attempt timeout still lands");
        let path = object_store::path::Path::from(f.key.clone());
        assert!(store.get(&path).await.is_ok());
    }

    #[cfg(feature = "staging")]
    #[tokio::test]
    async fn uploader_cleanup_respects_policy_on_failure() {
        use object_store::{ObjectStore, ObjectStoreExt};
        use std::sync::Arc;
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let loc = StagingLocation::parse("s3://b/stage").unwrap();
        let up = StageUploader::new(store.clone(), loc);
        let spec = StagingSpec {
            location: "s3://b/stage".into(),
            format: StagingFormat::Jsonl,
            compression: StagingCompression::None,
            cleanup: StagingCleanup::OnSuccess,
            copy_options: None,
            credentials: StagingCredentials::Default,
        };
        let f = up
            .stage_page(&spec, "p", "r", 1, &[json!({"a":1})], None)
            .await
            .unwrap();
        let path = object_store::path::Path::from(f.key.clone());
        // on_success + failed -> kept
        up.cleanup(std::slice::from_ref(&f), StagingCleanup::OnSuccess, false)
            .await;
        assert!(store.get(&path).await.is_ok());
    }
}
