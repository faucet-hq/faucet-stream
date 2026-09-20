//! Azure Blob sink configuration.

use faucet_common_azure::{AzureConnection, AzureCredentials};
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the Azure Blob sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AzureBlobSinkConfig {
    /// Azure connection (container, account, credentials, endpoint, …).
    #[serde(flatten)]
    pub connection: AzureConnection,
    /// Object-name prefix for written objects.
    #[serde(default)]
    pub prefix: String,
    /// File extension for written objects (default `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Hard cap on records per uploaded object. `None` means a single object
    /// per `write_batch` call (still subject to `batch_size`).
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per object before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for object size, so a rows-only cap either writes
    /// tiny objects for narrow data or unbounded ones for wide data. Counted
    /// on the uncompressed body, before any `compression` codec. `None` (the
    /// default) removes the byte cap; a single record larger than the cap
    /// still gets its own object rather than being split or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Maximum number of concurrent uploads (default 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per uploaded object from a single `write_batch` call.
    /// `batch_size = 0` writes whatever upstream hands the sink as one object.
    /// Recommended value for object stores is `0` — many tiny objects is a
    /// well-known anti-pattern.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Compression codec applied to each uploaded object body. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// resolves against `file_extension` (so `.jsonl.gz` triggers gzip).
    /// Requires the crate-local `compression` feature. Note: this sink does
    /// **not** set any `Content-Encoding` metadata, so consumers must
    /// decompress explicitly.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl AzureBlobSinkConfig {
    /// Create a new config for `container` with sensible defaults.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            connection: AzureConnection::new(container),
            prefix: String::new(),
            file_extension: default_file_extension(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
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

    /// Set the written-object name prefix.
    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }

    /// Set the written-object file extension.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Cap records per uploaded object.
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set the per-object record cap.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }

    /// Set the maximum concurrent uploads.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// Set the records-per-object `batch_size`.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
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
    fn defaults() {
        let c = AzureBlobSinkConfig::new("cont");
        assert_eq!(c.container(), "cont");
        assert_eq!(c.prefix, "");
        assert_eq!(c.connection.auth, AzureCredentials::Default);
        assert_eq!(c.file_extension, ".jsonl");
        assert!(c.max_records_per_file.is_none());
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_methods() {
        let c = AzureBlobSinkConfig::new("cont")
            .account("acct")
            .prefix("out/")
            .file_extension(".ndjson")
            .max_records_per_file(500)
            .concurrency(4)
            .with_batch_size(0)
            .auth(AzureCredentials::SasToken {
                sas_token: "sv=x".into(),
            });
        assert_eq!(c.connection.account.as_deref(), Some("acct"));
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.file_extension, ".ndjson");
        assert_eq!(c.max_records_per_file, Some(500));
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.batch_size, 0);
        assert!(matches!(
            c.connection.auth,
            AzureCredentials::SasToken { .. }
        ));
    }

    #[test]
    fn batch_size_sentinel_accepted_and_above_max_rejected() {
        assert!(faucet_core::validate_batch_size(0).is_ok());
        assert!(faucet_core::validate_batch_size(faucet_core::MAX_BATCH_SIZE + 1).is_err());
    }

    #[test]
    fn deserializes_flattened_connection() {
        let json = r#"{
            "container": "cont",
            "account": "acct",
            "prefix": "out/",
            "auth": { "type": "sas_token", "config": { "sas_token": "sv=x" } }
        }"#;
        let c: AzureBlobSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.container(), "cont");
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(matches!(
            c.connection.auth,
            AzureCredentials::SasToken { .. }
        ));
    }

    #[test]
    fn schema_generates_without_panicking() {
        let _ = faucet_core::schema_for!(AzureBlobSinkConfig);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = AzureBlobSinkConfig::new("cont");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }
}
