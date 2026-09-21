//! Azure Blob source configuration.

use faucet_common_azure::{AzureConnection, AzureCredentials};
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of objects stored in the container.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AzureFileFormat {
    /// Each line in the object is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire object is a JSON array of records.
    JsonArray,
    /// Each object becomes a single record with `"key"` and `"content"` fields.
    RawText,
    /// Delimited text, decoded through [`faucet_core::file_format`] so the
    /// records match what every other connector produces for the same file.
    /// Dialect from [`csv`](AzureBlobSourceConfig::csv). Requires
    /// `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, decoded to the compact element→object mapping. The repeated
    /// element is named by [`xml`](AzureBlobSourceConfig::xml). Requires
    /// `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet and header row from
    /// [`excel`](AzureBlobSourceConfig::excel). **Buffered whole** — a
    /// workbook is a zip container whose directory sits at the end. Requires
    /// `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
}

impl AzureFileFormat {
    /// The shared format this variant maps onto, or `None` for `RawText`,
    /// whose `{key, content}` envelope is the connector's own shape.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    pub(crate) fn shared(self) -> Option<faucet_core::FileFormat> {
        match self {
            Self::JsonLines => Some(faucet_core::FileFormat::JsonLines),
            Self::JsonArray => Some(faucet_core::FileFormat::JsonArray),
            Self::RawText => None,
            #[cfg(feature = "file-format-csv")]
            Self::Csv => Some(faucet_core::FileFormat::Csv),
            #[cfg(feature = "file-format-xml")]
            Self::Xml => Some(faucet_core::FileFormat::Xml),
            #[cfg(feature = "file-format-excel")]
            Self::Xlsx => Some(faucet_core::FileFormat::Xlsx),
        }
    }
}

/// Configuration for the Azure Blob source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AzureBlobSourceConfig {
    /// Azure connection (container, account, credentials, endpoint, …).
    #[serde(flatten)]
    pub connection: AzureConnection,
    /// Object name prefix filter. Ignored when `object_keys` is set.
    pub prefix: Option<String>,
    /// Explicit object names. When set, listing is skipped and `prefix`
    /// is ignored.
    pub object_keys: Option<Vec<String>>,
    /// File format.
    #[serde(default)]
    pub file_format: AzureFileFormat,
    /// Hard cap on the number of objects read (after listing).
    pub max_objects: Option<usize>,
    /// Maximum concurrent object reads (default: 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted `StreamPage`. `batch_size = 0` is the "no batching"
    /// sentinel and emits one page per object.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Verify each blob's byte length against the length the store
    /// advertises, failing the read with
    /// [`FaucetError::Source`](faucet_core::FaucetError::Source) on a short
    /// (truncated) or over-long transfer. Cheap — a byte counter over a body
    /// that is read anyway — so it defaults to `true`.
    ///
    /// Skipped (with a debug log, not a failure) when the store advertises no
    /// length, or when it reports a non-empty `Content-Encoding`: the body on
    /// the wire is then transcoded and its length legitimately differs from
    /// the stored blob's.
    #[serde(default = "default_true")]
    pub verify_length: bool,
    /// Not supported on Azure Blob: `object_store` exposes no `Content-MD5`
    /// attribute and an Azure ETag is not a content hash, so there is nothing
    /// to verify against. Setting it `true` is **rejected at config load**
    /// rather than silently ignored — see `validate()`.
    #[serde(default)]
    pub verify_checksum: bool,
    /// Compression codec applied to each downloaded object. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) — the
    /// codec is resolved per-object-key, so a single source can read a mix of
    /// compressed and uncompressed objects. Requires the crate-local
    /// `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// CSV dialect, used when `file_format: csv` (#604).
    #[serde(default)]
    pub csv: faucet_core::CsvOptions,
    /// Worksheet selection, used when `file_format: xlsx` (#604).
    #[serde(default)]
    pub excel: faucet_core::ExcelOptions,
    /// Record framing, used when `file_format: xml` (#604).
    #[serde(default)]
    pub xml: faucet_core::XmlOptions,
}

/// Serde default for the integrity flags that default on.
fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl AzureBlobSourceConfig {
    /// Create a new config for `container` with sensible defaults.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            connection: AzureConnection::new(container),
            prefix: None,
            object_keys: None,
            file_format: AzureFileFormat::default(),
            max_objects: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            verify_length: true,
            verify_checksum: false,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
        }
    }

    /// The per-format option blocks in the shape
    /// [`faucet_core::file_format::decode`] wants.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    pub(crate) fn format_options(&self) -> faucet_core::FormatOptions {
        faucet_core::FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
        }
    }

    /// Set the storage-account name.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.connection = self.connection.account(account);
        self
    }

    /// Set the credential source.
    pub fn auth(mut self, creds: AzureCredentials) -> Self {
        self.connection = self.connection.auth(creds);
        self
    }

    /// Set a custom blob endpoint (emulator / sovereign cloud).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.connection = self.connection.endpoint(endpoint);
        self
    }

    /// Permit plaintext HTTP (required for the Azurite emulator).
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.connection = self.connection.allow_http(allow);
        self
    }

    /// Target the Azurite emulator.
    pub fn use_emulator(mut self, use_emulator: bool) -> Self {
        self.connection = self.connection.use_emulator(use_emulator);
        self
    }

    /// Filter objects by name prefix.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Read an explicit set of object names (skips listing).
    pub fn object_keys(mut self, keys: Vec<String>) -> Self {
        self.object_keys = Some(keys);
        self
    }

    /// Set the object file format.
    pub fn file_format(mut self, format: AzureFileFormat) -> Self {
        self.file_format = format;
        self
    }

    /// Cap the number of objects read.
    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    /// Set the maximum concurrent object reads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the records-per-`StreamPage` hint.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable or disable the per-object length verification (default `true`).
    /// Sets [`verify_length`](Self::verify_length).
    pub fn verify_length(mut self, verify: bool) -> Self {
        self.verify_length = verify;
        self
    }

    /// Validate the config at load time so a bad config fails with a typed
    /// [`FaucetError::Config`](faucet_core::FaucetError::Config) before the
    /// first byte moves: rejects an out-of-range `batch_size`
    /// (`> MAX_BATCH_SIZE`), an empty `container`, and `verify_checksum: true`
    /// (unsupported here — see [`verify_checksum`](Self::verify_checksum)).
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if self.connection.container.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "azure-blob source requires a non-empty `container`".into(),
            ));
        }
        if self.verify_checksum {
            return Err(faucet_core::FaucetError::Config(
                "azure-blob source: `verify_checksum` is not supported — Azure Blob does not \
                 expose a body checksum through the object-store read API. Leave it unset and \
                 rely on `verify_length`, which is enforced against the reported blob size."
                    .into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// The container name.
    pub fn container(&self) -> &str {
        &self.connection.container
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = AzureBlobSourceConfig::new("my-container");
        assert_eq!(config.container(), "my-container");
        assert!(config.prefix.is_none());
        assert!(config.object_keys.is_none());
        assert_eq!(config.connection.auth, AzureCredentials::Default);
        assert_eq!(config.file_format, AzureFileFormat::JsonLines);
        assert!(config.max_objects.is_none());
        assert_eq!(config.concurrency, 10);
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_methods() {
        let config = AzureBlobSourceConfig::new("c")
            .account("acct")
            .prefix("data/")
            .file_format(AzureFileFormat::JsonArray)
            .max_objects(5)
            .concurrency(20)
            .with_batch_size(250)
            .auth(AzureCredentials::AccountKey {
                account_key: "k".into(),
            });

        assert_eq!(config.container(), "c");
        assert_eq!(config.connection.account.as_deref(), Some("acct"));
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert_eq!(config.file_format, AzureFileFormat::JsonArray);
        assert_eq!(config.max_objects, Some(5));
        assert_eq!(config.concurrency, 20);
        assert_eq!(config.batch_size, 250);
        assert!(matches!(
            config.connection.auth,
            AzureCredentials::AccountKey { .. }
        ));
    }

    #[test]
    fn file_format_default_is_json_lines() {
        assert_eq!(AzureFileFormat::default(), AzureFileFormat::JsonLines);
    }

    #[test]
    fn deserializes_flattened_connection_and_auth() {
        let json = r#"{
            "container": "c",
            "account": "acct",
            "auth": { "type": "account_key", "config": { "account_key": "k" } },
            "prefix": "data/",
            "file_format": "json_array"
        }"#;
        let config: AzureBlobSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.container(), "c");
        assert_eq!(config.connection.account.as_deref(), Some("acct"));
        assert_eq!(config.file_format, AzureFileFormat::JsonArray);
        assert!(matches!(
            config.connection.auth,
            AzureCredentials::AccountKey { .. }
        ));
        // batch_size / concurrency default when omitted.
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(config.concurrency, 10);
    }

    #[test]
    fn auth_defaults_to_default_when_absent_from_json() {
        let json = r#"{ "container": "c" }"#;
        let config: AzureBlobSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.connection.auth, AzureCredentials::Default);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = AzureBlobSourceConfig::new("c").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected() {
        let config =
            AzureBlobSourceConfig::new("c").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn schema_generates_without_panicking() {
        let _ = faucet_core::schema_for!(AzureBlobSourceConfig);
    }

    #[test]
    fn verify_defaults_length_on_checksum_off() {
        let cfg = AzureBlobSourceConfig::new("c");
        assert!(cfg.verify_length, "length verification defaults on");
        assert!(!cfg.verify_checksum);
    }

    #[test]
    fn verify_length_builder_overrides() {
        let cfg = AzureBlobSourceConfig::new("c").verify_length(false);
        assert!(!cfg.verify_length);
    }

    /// The connection block is `#[serde(flatten)]`ed, so both verify keys must
    /// still land at the top level beside its own.
    #[test]
    fn verify_keys_stay_top_level_on_the_wire() {
        let json = r#"{ "container": "c", "verify_length": false }"#;
        let config: AzureBlobSourceConfig = serde_json::from_str(json).unwrap();
        assert!(!config.verify_length);

        let out = serde_json::to_value(&config).unwrap();
        assert_eq!(out["verify_length"], serde_json::json!(false));
        assert_eq!(out["verify_checksum"], serde_json::json!(false));
        assert!(out.get("verify").is_none(), "no nested block: {out}");
    }

    #[test]
    fn verify_defaults_when_absent_from_json() {
        let config: AzureBlobSourceConfig =
            serde_json::from_str(r#"{ "container": "c" }"#).unwrap();
        assert!(config.verify_length);
        assert!(!config.verify_checksum);
    }

    #[test]
    fn validate_accepts_a_default_config() {
        assert!(AzureBlobSourceConfig::new("c").validate().is_ok());
    }

    #[test]
    fn validate_rejects_verify_checksum() {
        let mut cfg = AzureBlobSourceConfig::new("c");
        cfg.verify_checksum = true;
        match cfg.validate() {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("verify_checksum"), "got: {m}")
            }
            other => panic!("expected a verify_checksum Config error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_empty_container() {
        assert!(matches!(
            AzureBlobSourceConfig::new("   ").validate(),
            Err(faucet_core::FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let cfg = AzureBlobSourceConfig::new("c").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(
            cfg.validate(),
            Err(faucet_core::FaucetError::Config(_))
        ));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = AzureBlobSourceConfig::new("c");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    // ── file formats (#604) ───────────────────────────────────────────────

    /// Each variant maps onto the one shared format every other file
    /// connector uses for the same bytes. The connector-owned shapes map to
    /// `None` so they are never routed through the shared decoder — raw text
    /// keeps this connector's own envelope.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    #[test]
    fn formats_map_onto_the_shared_vocabulary_or_opt_out() {
        assert_eq!(
            AzureFileFormat::JsonLines.shared(),
            Some(faucet_core::FileFormat::JsonLines)
        );
        assert_eq!(
            AzureFileFormat::JsonArray.shared(),
            Some(faucet_core::FileFormat::JsonArray)
        );
        assert_eq!(AzureFileFormat::RawText.shared(), None);
        #[cfg(feature = "file-format-csv")]
        assert_eq!(
            AzureFileFormat::Csv.shared(),
            Some(faucet_core::FileFormat::Csv)
        );
        #[cfg(feature = "file-format-xml")]
        assert_eq!(
            AzureFileFormat::Xml.shared(),
            Some(faucet_core::FileFormat::Xml)
        );
        #[cfg(feature = "file-format-excel")]
        assert_eq!(
            AzureFileFormat::Xlsx.shared(),
            Some(faucet_core::FileFormat::Xlsx)
        );
    }

    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    #[test]
    fn the_format_option_blocks_reach_the_decoder() {
        let mut cfg = AzureBlobSourceConfig::new("c");
        cfg.csv = faucet_core::CsvOptions {
            delimiter: "\\t".into(),
            has_headers: false,
        };
        cfg.excel = faucet_core::ExcelOptions {
            sheet: Some("Q3".into()),
            header_row: 1,
        };
        cfg.xml = faucet_core::XmlOptions {
            record_element: "order".into(),
            root_element: "orders".into(),
        };
        let opts = cfg.format_options();
        assert_eq!(opts.csv.delimiter_byte().expect("tab"), b'\t');
        assert!(!opts.csv.has_headers);
        assert_eq!(opts.excel.sheet.as_deref(), Some("Q3"));
        assert_eq!(opts.excel.header_row, 1);
        assert_eq!(opts.xml.record_element, "order");
    }
}
