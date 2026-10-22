//! Azure Blob sink executor: the shared [`FileSink`] over an
//! [`AzureBackend`] (RFC 0013, #886).

use std::sync::Arc;

use faucet_common_azure::{AzureBackend, build_azure};
use faucet_common_file::write::{FileSink, SinkIdentity};
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;

/// A sink that writes records to Azure blobs in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// JSON Lines and raw text go up as blocks while they are written, so
/// neither memory nor local disk grows with the blob; every other format is
/// built in a local scratch file and published with one upload (a committed
/// block list past 8 MiB) when it closes: at the row / byte cap or at
/// `flush`. Up to `concurrency` uploads (blobs and blocks) run at once,
/// while the next data is encoded; `flush` waits for all of them, so a
/// bookmark never advances past a blob that is not in the container.
pub struct AzureBlobSink {
    inner: FileSink<AzureBackend>,
}

impl AzureBlobSink {
    /// Construct the sink, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let store = build_azure(&config.connection)?;
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let backend = AzureBackend::builder_for(store, config.container())
            .concurrency(config.concurrency)
            .scratch_dir(config.scratch_dir.as_deref().map(std::path::Path::new))
            .build(base, &template)?;
        let identity = AzureIdentity {
            dataset_uri: dataset_uri(&config),
        };
        Ok(Self {
            inner: FileSink::build(settings, template, Arc::new(backend), identity)?,
        })
    }

    /// The format blobs are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

impl AzureBlobSink {
    /// Join the overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): every instance of
    /// one run (begin, writers, commit) stages in that run's own swap area,
    /// so a concurrent overwrite run of the same output neither drops nor
    /// moves in the other's files.
    pub fn with_overwrite_run(mut self, token: &str) -> Result<Self, FaucetError> {
        self.inner = self.inner.with_overwrite_run(token)?;
        Ok(self)
    }
}

faucet_common_file::delegate_sink!(AzureBlobSink, inner);

/// What the Azure Blob sink supplies to the shared sink; requests, metering
/// and the preflight probe are the backend's.
struct AzureIdentity {
    dataset_uri: String,
}

/// The dataset a run writes: `az://<account>/<container>/<prefix><path>`.
/// Container names are only unique within a storage account, so the account
/// is part of the identity whenever the config names one (explicitly, in a
/// connection string, or through the emulator); `path` is the key template.
fn dataset_uri(config: &AzureBlobSinkConfig) -> String {
    let key = format!(
        "{}{}",
        config.prefix,
        config.path.as_deref().unwrap_or_default()
    );
    match account_name(&config.connection) {
        Some(account) => format!("az://{account}/{}/{key}", config.container()),
        None => format!("az://{}/{key}", config.container()),
    }
}

/// The storage account a connection addresses, when the config says.
fn account_name(conn: &faucet_common_azure::AzureConnection) -> Option<String> {
    if let Some(account) = conn.account.as_deref().map(str::trim)
        && !account.is_empty()
    {
        return Some(account.to_string());
    }
    let mut dev_storage = false;
    if let faucet_common_azure::AzureCredentials::ConnectionString { connection_string } =
        &conn.auth
    {
        for segment in connection_string.split(';') {
            match segment.trim().split_once('=') {
                Some((k, v)) if k.trim() == "AccountName" && !v.trim().is_empty() => {
                    return Some(v.trim().to_string());
                }
                Some((k, v)) if k.trim() == "UseDevelopmentStorage" => {
                    dev_storage = v.trim().eq_ignore_ascii_case("true");
                }
                _ => {}
            }
        }
    }
    (dev_storage || conn.use_emulator).then(|| EMULATOR_ACCOUNT.to_string())
}

/// The account name of the Azurite storage emulator.
const EMULATOR_ACCOUNT: &str = "devstoreaccount1";

impl SinkIdentity for AzureIdentity {
    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        self.dataset_uri.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = AzureBlobSinkConfig::new("cont");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            Ok(_) => panic!("expected a batch_size Config error, got Ok(sink)"),
            Err(e) => panic!("expected a batch_size Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_container() {
        let config = AzureBlobSinkConfig::new("   ");
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("container"), "got: {m}"),
            Ok(_) => panic!("expected a container Config error, got Ok(sink)"),
            Err(e) => panic!("expected a container Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_builds_lazily_with_emulator() {
        use faucet_core::Sink as _;
        let config = AzureBlobSinkConfig::new("cont")
            .prefix("out/")
            .use_emulator(true)
            .allow_http(true);
        let sink = AzureBlobSink::new(config).await.unwrap();
        assert_eq!(sink.connector_name(), "azure-blob");
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert_eq!(sink.dataset_uri(), "az://devstoreaccount1/cont/out/");
        assert_eq!(sink.inner.backend().container(), "cont");
        assert!(sink.config_schema().is_object());
    }

    #[test]
    fn dataset_uri_names_the_account_and_the_path() {
        let mut a = AzureBlobSinkConfig::new("exports").account("prodeu");
        a.path = Some("orders/part-{part}.parquet".into());
        let mut b = AzureBlobSinkConfig::new("exports").account("produs");
        b.path = Some("users.csv".into());
        assert_eq!(
            dataset_uri(&a),
            "az://prodeu/exports/orders/part-{part}.parquet"
        );
        assert_eq!(dataset_uri(&b), "az://produs/exports/users.csv");
        assert_ne!(dataset_uri(&a), dataset_uri(&b));
    }

    #[test]
    fn account_comes_from_a_connection_string() {
        let mut c = AzureBlobSinkConfig::new("cont").prefix("p/");
        c.connection.auth = faucet_common_azure::AzureCredentials::ConnectionString {
            connection_string: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=k=="
                .into(),
        };
        assert_eq!(dataset_uri(&c), "az://acct/cont/p/");
        c.connection.auth = faucet_common_azure::AzureCredentials::ConnectionString {
            connection_string: "UseDevelopmentStorage=true".into(),
        };
        assert_eq!(dataset_uri(&c), "az://devstoreaccount1/cont/p/");
        c.connection.auth = faucet_common_azure::AzureCredentials::default();
        assert_eq!(dataset_uri(&c), "az://cont/p/");
    }
}
