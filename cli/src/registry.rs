//! Feature-gated dispatch from a string `type` to a concrete connector.
//!
//! Every arm in this file is guarded by the matching `source-*` / `sink-*`
//! Cargo feature so users can build a slim binary with just the connectors
//! they need. The string keys here are the public contract of the CLI's
//! `type:` field in YAML/JSON pipeline configs.

use crate::auth_catalog::{self, AuthCatalog};
use crate::error::{CliError, CliResult};
use faucet_core::{Sink, Source};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

/// A factory that builds a [`Source`] trait object from its JSON/YAML config
/// (`source.config`). This is the extension point third-party connectors plug
/// into: a custom-CLI author registers one per connector `type:` they want
/// their `faucet` binary to understand.
///
/// The factory is **synchronous** — a connector that needs to connect eagerly
/// should do so lazily on first use (the pattern every built-in file/DB
/// connector already follows). Return a typed error (wrap your own error in
/// [`faucet_core::FaucetError::Custom`]) on invalid config.
pub type SourceFactory = Arc<dyn Fn(Value) -> CliResult<Box<dyn Source>> + Send + Sync>;

/// A factory that builds a [`Sink`] trait object from its `sink.config`. See
/// [`SourceFactory`] for the contract.
pub type SinkFactory = Arc<dyn Fn(Value) -> CliResult<Box<dyn Sink>> + Send + Sync>;

/// A closure returning the JSON Schema for a custom connector's config. Powers
/// `faucet schema source <name>` / `faucet schema sink <name>` for third-party
/// connectors without instantiating one. Typically `|| schema_for!(MyConfig)`.
pub type SchemaFn = Arc<dyn Fn() -> Value + Send + Sync>;

struct SourceEntry {
    factory: SourceFactory,
    schema: SchemaFn,
    description: &'static str,
}

struct SinkEntry {
    factory: SinkFactory,
    schema: SchemaFn,
    description: &'static str,
}

/// A registry of connector factories keyed by their YAML `type:` string.
///
/// The built-in connectors are dispatched by a compile-time `match` (gated by
/// the `source-*` / `sink-*` Cargo features); this registry holds **only the
/// third-party connectors** a custom-CLI author registers on top. The two are
/// merged transparently — [`build_source`] / [`source_schema`] /
/// [`source_descriptions`] (and their sink counterparts) consult the registered
/// customs first and fall back to the built-in `match`, so a custom connector is
/// usable from `faucet.yaml` exactly like a built-in one, across every command
/// (`run`, `validate`, `schema`, `list`, `preview`, `serve`, …).
///
/// # Example
///
/// ```no_run
/// use faucet_cli::registry::PluginRegistry;
/// # use faucet_core::{Source, async_trait, serde_json::Value};
/// # use std::collections::HashMap;
/// # struct MySource;
/// # impl MySource { fn from_value(_: Value) -> Result<Self, faucet_core::FaucetError> { Ok(MySource) } }
/// # #[async_trait]
/// # impl Source for MySource {
/// #     async fn fetch_with_context(&self, _: &HashMap<String, Value>) -> Result<Vec<Value>, faucet_core::FaucetError> { Ok(vec![]) }
/// #     fn config_schema(&self) -> Value { Value::Null }
/// # }
/// let registry = PluginRegistry::with_builtins()
///     .register_source("my", |cfg| Ok(Box::new(MySource::from_value(cfg)?)));
/// faucet_cli::run_main(registry);
/// ```
#[derive(Default)]
pub struct PluginRegistry {
    sources: BTreeMap<&'static str, SourceEntry>,
    sinks: BTreeMap<&'static str, SinkEntry>,
    /// Registration errors (name collisions) stashed during the builder chain
    /// and surfaced by [`PluginRegistry::install`] so `register_*` can stay
    /// chainable.
    errors: Vec<String>,
}

impl PluginRegistry {
    /// An empty registry. Custom connectors registered on top of the built-ins.
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry seeded with the built-in connectors. The built-ins are
    /// dispatched by the compile-time `match`, so this is currently equivalent
    /// to [`PluginRegistry::new`] — but it is the canonical constructor a
    /// custom `main.rs` should call so that future changes to how built-ins are
    /// registered are picked up automatically.
    pub fn with_builtins() -> Self {
        Self::default()
    }

    /// Register a custom source connector under `name` (its YAML `type:`).
    /// Chainable. A collision with a built-in or a previously-registered custom
    /// name is recorded and surfaced by [`install`](Self::install).
    #[must_use]
    pub fn register_source<F>(self, name: &str, factory: F) -> Self
    where
        F: Fn(Value) -> CliResult<Box<dyn Source>> + Send + Sync + 'static,
    {
        self.register_source_with(name, factory, || serde_json::json!({"type": "object"}), "")
    }

    /// Register a custom source with an explicit schema closure and one-line
    /// description (shown by `faucet list` and `faucet schema source <name>`).
    #[must_use]
    pub fn register_source_with<F, S>(
        mut self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> CliResult<Box<dyn Source>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        let key = leak_str(name);
        if builtin_source_descriptions().iter().any(|(n, _)| *n == key) {
            self.errors.push(format!(
                "cannot register source `{name}`: a built-in source already uses that name"
            ));
            return self;
        }
        if self.sources.contains_key(key) {
            self.errors
                .push(format!("source `{name}` is registered more than once"));
            return self;
        }
        self.sources.insert(
            key,
            SourceEntry {
                factory: Arc::new(factory),
                schema: Arc::new(schema),
                description: leak_str(description),
            },
        );
        self
    }

    /// Register a custom sink connector under `name` (its YAML `type:`).
    #[must_use]
    pub fn register_sink<F>(self, name: &str, factory: F) -> Self
    where
        F: Fn(Value) -> CliResult<Box<dyn Sink>> + Send + Sync + 'static,
    {
        self.register_sink_with(name, factory, || serde_json::json!({"type": "object"}), "")
    }

    /// Register a custom sink with an explicit schema closure and description.
    #[must_use]
    pub fn register_sink_with<F, S>(
        mut self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> CliResult<Box<dyn Sink>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        let key = leak_str(name);
        if builtin_sink_descriptions().iter().any(|(n, _)| *n == key) {
            self.errors.push(format!(
                "cannot register sink `{name}`: a built-in sink already uses that name"
            ));
            return self;
        }
        if self.sinks.contains_key(key) {
            self.errors
                .push(format!("sink `{name}` is registered more than once"));
            return self;
        }
        self.sinks.insert(
            key,
            SinkEntry {
                factory: Arc::new(factory),
                schema: Arc::new(schema),
                description: leak_str(description),
            },
        );
        self
    }

    /// Install this registry as the process-global custom-connector registry.
    /// Called once by [`crate::run_main`]. Returns an error if any `register_*`
    /// call collided, or if a registry was already installed.
    pub fn install(self) -> CliResult<()> {
        if !self.errors.is_empty() {
            return Err(CliError::Config(self.errors.join("; ")));
        }
        GLOBAL_REGISTRY
            .set(self)
            .map_err(|_| CliError::Config("connector registry already installed".to_owned()))
    }

    fn custom_source_descriptions(&self) -> Vec<(&'static str, &'static str)> {
        self.sources
            .iter()
            .map(|(name, e)| {
                (
                    *name,
                    if e.description.is_empty() {
                        "custom source connector"
                    } else {
                        e.description
                    },
                )
            })
            .collect()
    }

    fn custom_sink_descriptions(&self) -> Vec<(&'static str, &'static str)> {
        self.sinks
            .iter()
            .map(|(name, e)| {
                (
                    *name,
                    if e.description.is_empty() {
                        "custom sink connector"
                    } else {
                        e.description
                    },
                )
            })
            .collect()
    }
}

/// Leak a string into a `&'static str`. Connector names/descriptions are
/// registered once at process start and live for the whole run, so leaking a
/// handful of small strings is the right tradeoff to keep the `&'static str`
/// listing signatures (`source_kinds`, `source_descriptions`) unchanged.
fn leak_str(s: &str) -> &'static str {
    Box::leak(s.to_owned().into_boxed_str())
}

static GLOBAL_REGISTRY: OnceLock<PluginRegistry> = OnceLock::new();

/// The process-global custom-connector registry, or an empty one if none was
/// installed (the default `faucet` binary registers no customs).
fn global() -> &'static PluginRegistry {
    GLOBAL_REGISTRY.get_or_init(PluginRegistry::default)
}

/// Build a [`Source`] trait object from a `(kind, config)` pair. When the
/// config carries `auth: { ref: <name> }`, the named provider is resolved from
/// `auth` (the catalog) and injected into the connector.
pub async fn build_source(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    retry_policy: Option<&faucet_core::RetryPolicy>,
) -> CliResult<Box<dyn Source>> {
    // Third-party connectors registered via `PluginRegistry` win first. Names
    // can never collide with a built-in (registration rejects that), so this is
    // safe to check ahead of the built-in `match`. Custom factories receive the
    // raw config and manage their own auth (the shared `auth:` catalog is not
    // injected into custom connectors).
    if let Some(entry) = global().sources.get(kind) {
        return (entry.factory)(config);
    }
    let auth_ref = auth_catalog::auth_ref(&config);
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => {
            let cfg = decode::<faucet_source_rest::RestStreamConfig>("source", "rest", config)?;
            let mut s = faucet_source_rest::RestStream::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            if let Some(rp) = retry_policy {
                s = s.with_retry_policy(rp.clone());
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-graphql")]
        "graphql" => {
            let cfg =
                decode::<faucet_source_graphql::GraphqlStreamConfig>("source", "graphql", config)?;
            cfg.validate()?;
            // `try_new` builds the client (incl. any mutual-TLS identity), so a
            // bad `tls:` block is a typed error instead of a panic.
            let mut s = faucet_source_graphql::GraphqlStream::try_new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            if let Some(rp) = retry_policy {
                s = s.with_retry_policy(rp.clone());
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-xml")]
        "xml" => {
            let cfg = decode::<faucet_source_xml::XmlStreamConfig>("source", "xml", config)?;
            // `try_new` validates the config and builds the client (incl. any
            // mutual-TLS identity), so a bad `tls:` block is a typed error.
            let mut s = faucet_source_xml::XmlStream::try_new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            if let Some(rp) = retry_policy {
                s = s.with_retry_policy(rp.clone());
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-grpc")]
        "grpc" => {
            let cfg = decode::<faucet_source_grpc::GrpcStreamConfig>("source", "grpc", config)?;
            let mut s = faucet_source_grpc::GrpcStream::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-postgres")]
        "postgres" => {
            let cfg = decode::<faucet_source_postgres::PostgresSourceConfig>(
                "source", "postgres", config,
            )?;
            Ok(Box::new(
                faucet_source_postgres::PostgresSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => {
            let cfg = decode::<faucet_source_postgres_cdc::PostgresCdcSourceConfig>(
                "source",
                "postgres-cdc",
                config,
            )?;
            Ok(Box::new(
                faucet_source_postgres_cdc::PostgresCdcSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-mysql")]
        "mysql" => {
            let cfg = decode::<faucet_source_mysql::MysqlSourceConfig>("source", "mysql", config)?;
            Ok(Box::new(faucet_source_mysql::MysqlSource::new(cfg).await?))
        }
        #[cfg(feature = "source-mssql")]
        "mssql" => {
            let cfg = decode::<faucet_source_mssql::MssqlSourceConfig>("source", "mssql", config)?;
            Ok(Box::new(faucet_source_mssql::MssqlSource::new(cfg).await?))
        }
        #[cfg(feature = "source-sqlite")]
        "sqlite" => {
            let cfg =
                decode::<faucet_source_sqlite::SqliteSourceConfig>("source", "sqlite", config)?;
            Ok(Box::new(
                faucet_source_sqlite::SqliteSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-duckdb")]
        "duckdb" => {
            let cfg =
                decode::<faucet_source_duckdb::DuckdbSourceConfig>("source", "duckdb", config)?;
            Ok(Box::new(
                faucet_source_duckdb::DuckdbSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-sqs")]
        "sqs" => {
            let cfg = decode::<faucet_source_sqs::SqsSourceConfig>("source", "sqs", config)?;
            Ok(Box::new(faucet_source_sqs::SqsSource::new(cfg).await?))
        }
        #[cfg(feature = "source-nats")]
        "nats" => {
            let cfg = decode::<faucet_source_nats::NatsSourceConfig>("source", "nats", config)?;
            Ok(Box::new(faucet_source_nats::NatsSource::new(cfg).await?))
        }
        #[cfg(feature = "source-sftp")]
        "sftp" => {
            let cfg = decode::<faucet_source_sftp::SftpSourceConfig>("source", "sftp", config)?;
            Ok(Box::new(faucet_source_sftp::SftpSource::new(cfg)?))
        }
        #[cfg(feature = "source-s3")]
        "s3" => {
            let cfg = decode::<faucet_source_s3::S3SourceConfig>("source", "s3", config)?;
            Ok(Box::new(faucet_source_s3::S3Source::new(cfg).await?))
        }
        #[cfg(feature = "source-mongodb")]
        "mongodb" => {
            let cfg =
                decode::<faucet_source_mongodb::MongoSourceConfig>("source", "mongodb", config)?;
            Ok(Box::new(
                faucet_source_mongodb::MongoSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => {
            let cfg = decode::<faucet_source_mongodb_cdc::MongoCdcSourceConfig>(
                "source",
                "mongodb-cdc",
                config,
            )?;
            Ok(Box::new(
                faucet_source_mongodb_cdc::MongoCdcSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-mysql-cdc")]
        "mysql-cdc" => {
            let cfg = decode::<faucet_source_mysql_cdc::MysqlCdcSourceConfig>(
                "source",
                "mysql-cdc",
                config,
            )?;
            Ok(Box::new(
                faucet_source_mysql_cdc::MysqlCdcSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-redis")]
        "redis" => {
            let cfg = decode::<faucet_source_redis::RedisSourceConfig>("source", "redis", config)?;
            Ok(Box::new(faucet_source_redis::RedisSource::new(cfg)?))
        }
        #[cfg(feature = "source-webhook")]
        "webhook" => {
            let cfg =
                decode::<faucet_source_webhook::WebhookSourceConfig>("source", "webhook", config)?;
            Ok(Box::new(faucet_source_webhook::WebhookSource::new(cfg)))
        }
        #[cfg(feature = "source-websocket")]
        "websocket" => {
            let cfg = decode::<faucet_source_websocket::WebsocketSourceConfig>(
                "source",
                "websocket",
                config,
            )?;
            let mut s = faucet_source_websocket::WebsocketSource::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-csv")]
        "csv" => {
            let cfg = decode::<faucet_source_csv::CsvSourceConfig>("source", "csv", config)?;
            cfg.validate()?;
            Ok(Box::new(faucet_source_csv::CsvSource::new(cfg)))
        }
        #[cfg(feature = "source-singer")]
        "singer" => {
            let cfg =
                decode::<faucet_source_singer::SingerSourceConfig>("source", "singer", config)?;
            Ok(Box::new(faucet_source_singer::SingerSource::new(cfg)))
        }
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => {
            let cfg = decode::<faucet_source_elasticsearch::ElasticsearchSourceConfig>(
                "source",
                "elasticsearch",
                config,
            )?;
            let mut s = faucet_source_elasticsearch::ElasticsearchSource::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-kafka")]
        "kafka" => {
            let cfg = decode::<faucet_source_kafka::KafkaSourceConfig>("source", "kafka", config)?;
            Ok(Box::new(faucet_source_kafka::KafkaSource::new(cfg).await?))
        }
        #[cfg(feature = "source-kinesis")]
        "kinesis" => {
            let cfg =
                decode::<faucet_source_kinesis::KinesisSourceConfig>("source", "kinesis", config)?;
            Ok(Box::new(
                faucet_source_kinesis::KinesisSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-spanner")]
        "spanner" => {
            let cfg =
                decode::<faucet_source_spanner::SpannerSourceConfig>("source", "spanner", config)?;
            Ok(Box::new(
                faucet_source_spanner::SpannerSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-parquet")]
        "parquet" => {
            let cfg =
                decode::<faucet_source_parquet::ParquetSourceConfig>("source", "parquet", config)?;
            Ok(Box::new(
                faucet_source_parquet::ParquetSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-delta")]
        "delta" => {
            let cfg = decode::<faucet_source_delta::DeltaSourceConfig>("source", "delta", config)?;
            Ok(Box::new(faucet_source_delta::DeltaSource::new(cfg).await?))
        }
        #[cfg(feature = "source-databricks")]
        "databricks" => {
            let cfg = decode::<faucet_source_databricks::DatabricksSourceConfig>(
                "source",
                "databricks",
                config,
            )?;
            let mut s = faucet_source_databricks::DatabricksSource::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_source_gcs::GcsSourceConfig>("source", "gcs", config)?;
            Ok(Box::new(faucet_source_gcs::GcsSource::new(cfg).await?))
        }
        #[cfg(feature = "source-bigquery")]
        "bigquery" => {
            let cfg = decode::<faucet_source_bigquery::BigQuerySourceConfig>(
                "source", "bigquery", config,
            )?;
            Ok(Box::new(
                faucet_source_bigquery::BigQuerySource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-snowflake")]
        "snowflake" => {
            let cfg = decode::<faucet_source_snowflake::SnowflakeSourceConfig>(
                "source",
                "snowflake",
                config,
            )?;
            let mut s = faucet_source_snowflake::SnowflakeSource::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "source-mssql-cdc")]
        "mssql-cdc" => {
            let cfg = decode::<faucet_source_mssql_cdc::MssqlCdcSourceConfig>(
                "source",
                "mssql-cdc",
                config,
            )?;
            Ok(Box::new(
                faucet_source_mssql_cdc::MssqlCdcSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-redshift")]
        "redshift" => {
            let cfg = decode::<faucet_source_redshift::RedshiftSourceConfig>(
                "source", "redshift", config,
            )?;
            Ok(Box::new(faucet_source_redshift::RedshiftSource::new(cfg)?))
        }
        #[cfg(feature = "source-pubsub")]
        "pubsub" => {
            let cfg =
                decode::<faucet_source_pubsub::PubsubSourceConfig>("source", "pubsub", config)?;
            Ok(Box::new(
                faucet_source_pubsub::PubsubSource::new(cfg).await?,
            ))
        }
        #[cfg(feature = "source-clickhouse")]
        "clickhouse" => {
            let cfg = decode::<faucet_source_clickhouse::ClickHouseSourceConfig>(
                "source",
                "clickhouse",
                config,
            )?;
            Ok(Box::new(faucet_source_clickhouse::ClickHouseSource::new(
                cfg,
            )?))
        }
        #[cfg(feature = "source-azure-blob")]
        "azure-blob" => {
            let cfg = decode::<faucet_source_azure_blob::AzureBlobSourceConfig>(
                "source",
                "azure-blob",
                config,
            )?;
            Ok(Box::new(
                faucet_source_azure_blob::AzureBlobSource::new(cfg).await?,
            ))
        }
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// Build a [`Sink`] trait object from a `(kind, config)` pair. When the config
/// carries `auth: { ref: <name> }`, the named provider is resolved from `auth`
/// (the catalog) and injected into the connector.
pub async fn build_sink(kind: &str, config: Value, auth: &AuthCatalog) -> CliResult<Box<dyn Sink>> {
    if let Some(entry) = global().sinks.get(kind) {
        return (entry.factory)(config);
    }
    let auth_ref = auth_catalog::auth_ref(&config);
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => {
            let cfg =
                decode::<faucet_sink_bigquery::BigQuerySinkConfig>("sink", "bigquery", config)?;
            Ok(Box::new(
                faucet_sink_bigquery::BigQuerySink::new(cfg).await?,
            ))
        }
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => {
            let cfg = decode::<faucet_sink_iceberg::IcebergSinkConfig>("sink", "iceberg", config)?;
            Ok(Box::new(faucet_sink_iceberg::IcebergSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-delta")]
        "delta" => {
            let cfg = decode::<faucet_sink_delta::DeltaSinkConfig>("sink", "delta", config)?;
            Ok(Box::new(faucet_sink_delta::DeltaSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-postgres")]
        "postgres" => {
            let cfg =
                decode::<faucet_sink_postgres::PostgresSinkConfig>("sink", "postgres", config)?;
            Ok(Box::new(
                faucet_sink_postgres::PostgresSink::new(cfg).await?,
            ))
        }
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => {
            let cfg = decode::<faucet_sink_jsonl::JsonlSinkConfig>("sink", "jsonl", config)?;
            Ok(Box::new(faucet_sink_jsonl::JsonlSink::new(cfg)))
        }
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => {
            let cfg =
                decode::<faucet_sink_snowflake::SnowflakeSinkConfig>("sink", "snowflake", config)?;
            let mut s = faucet_sink_snowflake::SnowflakeSink::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-mysql")]
        "mysql" => {
            let cfg = decode::<faucet_sink_mysql::MysqlSinkConfig>("sink", "mysql", config)?;
            Ok(Box::new(faucet_sink_mysql::MysqlSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-mssql")]
        "mssql" => {
            let cfg = decode::<faucet_sink_mssql::MssqlSinkConfig>("sink", "mssql", config)?;
            Ok(Box::new(faucet_sink_mssql::MssqlSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => {
            let cfg = decode::<faucet_sink_sqlite::SqliteSinkConfig>("sink", "sqlite", config)?;
            Ok(Box::new(faucet_sink_sqlite::SqliteSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => {
            let cfg = decode::<faucet_sink_duckdb::DuckdbSinkConfig>("sink", "duckdb", config)?;
            Ok(Box::new(faucet_sink_duckdb::DuckdbSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-sqs")]
        "sqs" => {
            let cfg = decode::<faucet_sink_sqs::SqsSinkConfig>("sink", "sqs", config)?;
            Ok(Box::new(faucet_sink_sqs::SqsSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-nats")]
        "nats" => {
            let cfg = decode::<faucet_sink_nats::NatsSinkConfig>("sink", "nats", config)?;
            Ok(Box::new(faucet_sink_nats::NatsSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-sftp")]
        "sftp" => {
            let cfg = decode::<faucet_sink_sftp::SftpSinkConfig>("sink", "sftp", config)?;
            Ok(Box::new(faucet_sink_sftp::SftpSink::new(cfg)?))
        }
        #[cfg(feature = "sink-s3")]
        "s3" => {
            let cfg = decode::<faucet_sink_s3::S3SinkConfig>("sink", "s3", config)?;
            Ok(Box::new(faucet_sink_s3::S3Sink::new(cfg).await?))
        }
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => {
            let cfg = decode::<faucet_sink_mongodb::MongoSinkConfig>("sink", "mongodb", config)?;
            Ok(Box::new(faucet_sink_mongodb::MongoSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-redis")]
        "redis" => {
            let cfg = decode::<faucet_sink_redis::RedisSinkConfig>("sink", "redis", config)?;
            Ok(Box::new(faucet_sink_redis::RedisSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-csv")]
        "csv" => {
            let cfg = decode::<faucet_sink_csv::CsvSinkConfig>("sink", "csv", config)?;
            Ok(Box::new(faucet_sink_csv::CsvSink::new(cfg)))
        }
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => {
            let cfg = decode::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>(
                "sink",
                "elasticsearch",
                config,
            )?;
            let mut s = faucet_sink_elasticsearch::ElasticsearchSink::new(cfg)?;
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-kafka")]
        "kafka" => {
            let cfg = decode::<faucet_sink_kafka::KafkaSinkConfig>("sink", "kafka", config)?;
            Ok(Box::new(faucet_sink_kafka::KafkaSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => {
            let cfg = decode::<faucet_sink_kinesis::KinesisSinkConfig>("sink", "kinesis", config)?;
            Ok(Box::new(faucet_sink_kinesis::KinesisSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-spanner")]
        "spanner" => {
            let cfg = decode::<faucet_sink_spanner::SpannerSinkConfig>("sink", "spanner", config)?;
            Ok(Box::new(faucet_sink_spanner::SpannerSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-http")]
        "http" => {
            let cfg = decode::<faucet_sink_http::HttpSinkConfig>("sink", "http", config)?;
            let mut s = faucet_sink_http::HttpSink::new(cfg);
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-stdout")]
        "stdout" => {
            let cfg = decode::<faucet_sink_stdout::StdoutSinkConfig>("sink", "stdout", config)?;
            Ok(Box::new(faucet_sink_stdout::StdoutSink::new(cfg)))
        }
        #[cfg(feature = "sink-parquet")]
        "parquet" => {
            let cfg = decode::<faucet_sink_parquet::ParquetSinkConfig>("sink", "parquet", config)?;
            Ok(Box::new(faucet_sink_parquet::ParquetSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_sink_gcs::GcsSinkConfig>("sink", "gcs", config)?;
            Ok(Box::new(faucet_sink_gcs::GcsSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-redshift")]
        "redshift" => {
            let cfg =
                decode::<faucet_sink_redshift::RedshiftSinkConfig>("sink", "redshift", config)?;
            Ok(Box::new(
                faucet_sink_redshift::RedshiftSink::new(cfg).await?,
            ))
        }
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => {
            let cfg = decode::<faucet_sink_pubsub::PubsubSinkConfig>("sink", "pubsub", config)?;
            Ok(Box::new(faucet_sink_pubsub::PubsubSink::new(cfg).await?))
        }
        #[cfg(feature = "sink-clickhouse")]
        "clickhouse" => {
            let cfg = decode::<faucet_sink_clickhouse::ClickHouseSinkConfig>(
                "sink",
                "clickhouse",
                config,
            )?;
            Ok(Box::new(faucet_sink_clickhouse::ClickHouseSink::new(cfg)?))
        }
        #[cfg(feature = "sink-azure-blob")]
        "azure-blob" => {
            let cfg = decode::<faucet_sink_azure_blob::AzureBlobSinkConfig>(
                "sink",
                "azure-blob",
                config,
            )?;
            Ok(Box::new(
                faucet_sink_azure_blob::AzureBlobSink::new(cfg).await?,
            ))
        }
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// Source connector kinds that deterministically replay (exactly-once-capable).
/// Mirrors `Source::supports_exactly_once` overrides — keep in sync when a new
/// source opts in. The single source of truth for both the boolean gate and the
/// human-readable list shown in error messages (F44). `kafka` qualifies because
/// partitions are immutable logs and every page carries a complete offsets
/// bookmark (#291).
pub const EXACTLY_ONCE_SOURCE_KINDS: &[&str] = &[
    "postgres-cdc",
    "mysql-cdc",
    "mssql-cdc",
    "mongodb-cdc",
    "kafka",
];

/// Sink connector kinds that can durably commit a token atomically with data.
/// Mirrors `Sink::supports_idempotent_writes` overrides — keep in sync when a
/// new sink opts in. Single source of truth for the gate + the error-message
/// list (F44).
pub const IDEMPOTENT_SINK_KINDS: &[&str] = &[
    "sqlite",
    "postgres",
    "mysql",
    "mssql",
    "iceberg",
    "bigquery",
    "kafka",
    "snowflake",
    "redis",
    "mongodb",
    "spanner",
];

/// Sink kinds that can apply additive/widening DDL via `Sink::evolve_schema`.
/// Mirrors each sink's `supports_schema_evolution()` override. Iceberg is
/// additive-only (new columns) via iceberg-rust 0.10.0's `update_schema`
/// action (#255).
pub const SCHEMA_EVOLUTION_SINK_KINDS: &[&str] = &[
    "postgres",
    "mysql",
    "mssql",
    "sqlite",
    "bigquery",
    "elasticsearch",
    "spanner",
    "iceberg",
];

/// Sink kinds that support `write_mode: upsert|delete`. Mirrors each sink's
/// `Sink::supported_write_modes()` override. Single source of truth for the gate
/// + the error-message list (F44).
pub const UPSERT_SINK_KINDS: &[&str] = &[
    "postgres",
    "sqlite",
    "mysql",
    "mssql",
    "mongodb",
    "elasticsearch",
    "bigquery",
    "spanner",
];

/// Sink kinds that implement scoped cleanup (`Sink::cleanup_scope`, #478) —
/// deleting destination rows inside a source's declared completeness scope that
/// the run did not write. Kept in sync with each sink's `supports_cleanup()`
/// override; `cli/tests/registry_capability_parity.rs` asserts they agree.
///
/// Currently the same set as [`UPSERT_SINK_KINDS`]: cleanup is only meaningful
/// alongside `write_mode: upsert`, which is exactly what those sinks support.
/// They are separate constants because that coincidence is not a guarantee — a
/// future upsert-capable sink whose backend cannot express a scoped delete would
/// belong in one list and not the other.
pub const CLEANUP_SINK_KINDS: &[&str] = &[
    "postgres",
    "sqlite",
    "mysql",
    "mssql",
    "mongodb",
    "elasticsearch",
    "bigquery",
    "spanner",
];

/// Sink kinds that support `write_mode: overwrite` (#492) — full-destination
/// replacement via the atomic begin/commit staging lifecycle. Mirrors each
/// sink's `Sink::supported_write_modes()` override (which must list
/// `WriteMode::Overwrite`); `cli/tests/registry_capability_parity.rs` asserts
/// they agree. A subset of [`UPSERT_SINK_KINDS`] — overwrite lands on the
/// keyed-write sinks as their lifecycle is added.
pub const OVERWRITE_SINK_KINDS: &[&str] = &[
    "sqlite",
    "postgres",
    "mysql",
    "mssql",
    "mongodb",
    "bigquery",
    // elasticsearch overwrites via an atomic alias swap (#494) rather than a
    // staging-table swap, but exposes the same begin/commit/abort lifecycle.
    "elasticsearch",
];

/// Whether a sink kind supports `write_mode: overwrite`.
pub fn sink_supports_overwrite(kind: &str) -> bool {
    OVERWRITE_SINK_KINDS.contains(&kind)
}

/// Sinks that support a **scoped/windowed** overwrite (#518) — replacing only
/// the rows matching a `scope` (a date window) instead of the whole table. A
/// subset of [`OVERWRITE_SINK_KINDS`]; the others still support full overwrite.
/// (v1: the atomic scoped delete + staged insert; the wider SQL family and
/// key-scope are follow-ups.)
pub const SCOPED_OVERWRITE_SINK_KINDS: &[&str] = &["postgres", "bigquery"];

/// Whether a sink kind supports a scoped overwrite `scope`.
pub fn sink_supports_scoped_overwrite(kind: &str) -> bool {
    SCOPED_OVERWRITE_SINK_KINDS.contains(&kind)
}

/// Sinks with a **direct** (staging-free) overwrite path: a solo full-refresh
/// loads straight into the target (e.g. an atomic BigQuery `WRITE_TRUNCATE`
/// load, which replaces the table only on success), so no staging table is
/// needed. The executor injects the internal `_overwrite_staging` signal into
/// *these* sinks only when the overwrite is a grouped fan-out (>1 writer → one
/// physical table), where a shared staging table is the only safe cross-writer
/// swap. Non-listed sinks always stage via their own begin/commit lifecycle.
pub const DIRECT_OVERWRITE_SINK_KINDS: &[&str] = &["bigquery"];

/// Whether a sink kind has a staging-free direct overwrite path (and therefore
/// understands the internal `_overwrite_staging` grouped-signal).
pub fn sink_supports_direct_overwrite(kind: &str) -> bool {
    DIRECT_OVERWRITE_SINK_KINDS.contains(&kind)
}

/// Sink kinds that bulk-load via an object-store stage + native load command
/// (staged bulk load, #528). These mirror each sink's
/// `Sink::supports_staged_load` override. Redshift (`write_strategy: copy`), Snowflake (`bulk_load`), and
/// BigQuery (`bulk_load`) load from S3 / an external stage / GCS respectively;
/// ClickHouse pulls a staged S3/GCS object with the `s3()` / `gcs()` table
/// function, and MSSQL/Synapse pulls a staged Azure Blob object with `COPY INTO`
/// (`staging:` blocks, #528). The load SQL/URL generators are unit-tested; the
/// server-side execution is not exercised in CI (no live warehouse + object
/// store), same as Redshift/Snowflake/BigQuery.
pub const STAGED_LOAD_SINK_KINDS: &[&str] =
    &["redshift", "snowflake", "bigquery", "clickhouse", "mssql"];

/// Whether a sink kind supports staged bulk load. See [`STAGED_LOAD_SINK_KINDS`].
pub fn sink_supports_staged_load(kind: &str) -> bool {
    STAGED_LOAD_SINK_KINDS.contains(&kind)
}

/// Source kinds that implement live dataset discovery (`Source::discover`,
/// issue #211) — mirrors the discoverable-source list in the connector docs.
/// Single source of truth for the conformance scorecard (#330).
pub const DISCOVER_SOURCE_KINDS: &[&str] = &[
    "postgres",
    "mysql",
    "mssql",
    "sqlite",
    "mongodb",
    "elasticsearch",
    "bigquery",
    "snowflake",
    "spanner",
    "s3",
    "gcs",
];

/// Whether a source kind supports `faucet discover` (dataset introspection).
pub fn source_supports_discover(kind: &str) -> bool {
    DISCOVER_SOURCE_KINDS.contains(&kind)
}

/// The typed replay capability a source kind advertises
/// (`Source::replay_guarantee`, issue #292). Derived from
/// [`EXACTLY_ONCE_SOURCE_KINDS`] — the kind table stays the single source of
/// truth; this is the typed view the delivery-guarantee derivation consumes.
pub fn source_replay_guarantee(kind: &str) -> faucet_core::ReplayGuarantee {
    if EXACTLY_ONCE_SOURCE_KINDS.contains(&kind) {
        faucet_core::ReplayGuarantee::Deterministic
    } else {
        faucet_core::ReplayGuarantee::NonDeterministic
    }
}

/// The strongest delivery guarantee a sink kind can uphold
/// (`Sink::sink_guarantee`, issue #292). Derived from
/// [`IDEMPOTENT_SINK_KINDS`] / [`UPSERT_SINK_KINDS`].
pub fn sink_guarantee(kind: &str) -> faucet_core::SinkGuarantee {
    if IDEMPOTENT_SINK_KINDS.contains(&kind) {
        faucet_core::SinkGuarantee::AtomicWatermark
    } else if UPSERT_SINK_KINDS.contains(&kind) {
        faucet_core::SinkGuarantee::KeyedUpsert
    } else {
        faucet_core::SinkGuarantee::AtLeastOnce
    }
}

/// See [`EXACTLY_ONCE_SOURCE_KINDS`].
pub fn source_supports_exactly_once(kind: &str) -> bool {
    source_replay_guarantee(kind) == faucet_core::ReplayGuarantee::Deterministic
}

/// See [`IDEMPOTENT_SINK_KINDS`].
pub fn sink_supports_idempotent_writes(kind: &str) -> bool {
    sink_guarantee(kind) == faucet_core::SinkGuarantee::AtomicWatermark
}

/// See [`SCHEMA_EVOLUTION_SINK_KINDS`].
pub fn sink_supports_schema_evolution(kind: &str) -> bool {
    SCHEMA_EVOLUTION_SINK_KINDS.contains(&kind)
}

/// See [`CLEANUP_SINK_KINDS`].
pub fn sink_supports_cleanup(kind: &str) -> bool {
    CLEANUP_SINK_KINDS.contains(&kind)
}

/// Write modes each sink kind supports. Kept in sync with each sink's
/// `Sink::supported_write_modes()` override via [`UPSERT_SINK_KINDS`].
pub fn sink_supported_write_modes(kind: &str) -> &'static [faucet_core::WriteMode] {
    use faucet_core::WriteMode;
    match (
        UPSERT_SINK_KINDS.contains(&kind),
        OVERWRITE_SINK_KINDS.contains(&kind),
    ) {
        (true, true) => &[
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ],
        (true, false) => &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete],
        (false, true) => &[WriteMode::Append, WriteMode::Overwrite],
        (false, false) => &[WriteMode::Append],
    }
}

/// Return the JSON Schema for the named source's config struct.
pub fn source_schema(kind: &str) -> CliResult<Value> {
    if let Some(entry) = global().sources.get(kind) {
        return Ok((entry.schema)());
    }
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => Ok(schema::<faucet_source_rest::RestStreamConfig>()),
        #[cfg(feature = "source-graphql")]
        "graphql" => Ok(schema::<faucet_source_graphql::GraphqlStreamConfig>()),
        #[cfg(feature = "source-xml")]
        "xml" => Ok(schema::<faucet_source_xml::XmlStreamConfig>()),
        #[cfg(feature = "source-grpc")]
        "grpc" => Ok(schema::<faucet_source_grpc::GrpcStreamConfig>()),
        #[cfg(feature = "source-postgres")]
        "postgres" => Ok(schema::<faucet_source_postgres::PostgresSourceConfig>()),
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => Ok(schema::<faucet_source_postgres_cdc::PostgresCdcSourceConfig>()),
        #[cfg(feature = "source-mysql")]
        "mysql" => Ok(schema::<faucet_source_mysql::MysqlSourceConfig>()),
        #[cfg(feature = "source-mssql")]
        "mssql" => Ok(schema::<faucet_source_mssql::MssqlSourceConfig>()),
        #[cfg(feature = "source-sqlite")]
        "sqlite" => Ok(schema::<faucet_source_sqlite::SqliteSourceConfig>()),
        #[cfg(feature = "source-duckdb")]
        "duckdb" => Ok(schema::<faucet_source_duckdb::DuckdbSourceConfig>()),
        #[cfg(feature = "source-sqs")]
        "sqs" => Ok(schema::<faucet_source_sqs::SqsSourceConfig>()),
        #[cfg(feature = "source-nats")]
        "nats" => Ok(schema::<faucet_source_nats::NatsSourceConfig>()),
        #[cfg(feature = "source-sftp")]
        "sftp" => Ok(schema::<faucet_source_sftp::SftpSourceConfig>()),
        #[cfg(feature = "source-s3")]
        "s3" => Ok(schema::<faucet_source_s3::S3SourceConfig>()),
        #[cfg(feature = "source-mongodb")]
        "mongodb" => Ok(schema::<faucet_source_mongodb::MongoSourceConfig>()),
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => Ok(schema::<faucet_source_mongodb_cdc::MongoCdcSourceConfig>()),
        #[cfg(feature = "source-mysql-cdc")]
        "mysql-cdc" => Ok(schema::<faucet_source_mysql_cdc::MysqlCdcSourceConfig>()),
        #[cfg(feature = "source-redis")]
        "redis" => Ok(schema::<faucet_source_redis::RedisSourceConfig>()),
        #[cfg(feature = "source-webhook")]
        "webhook" => Ok(schema::<faucet_source_webhook::WebhookSourceConfig>()),
        #[cfg(feature = "source-websocket")]
        "websocket" => Ok(schema::<faucet_source_websocket::WebsocketSourceConfig>()),
        #[cfg(feature = "source-csv")]
        "csv" => Ok(schema::<faucet_source_csv::CsvSourceConfig>()),
        #[cfg(feature = "source-singer")]
        "singer" => Ok(schema::<faucet_source_singer::SingerSourceConfig>()),
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => Ok(schema::<
            faucet_source_elasticsearch::ElasticsearchSourceConfig,
        >()),
        #[cfg(feature = "source-kafka")]
        "kafka" => Ok(schema::<faucet_source_kafka::KafkaSourceConfig>()),
        #[cfg(feature = "source-kinesis")]
        "kinesis" => Ok(schema::<faucet_source_kinesis::KinesisSourceConfig>()),
        #[cfg(feature = "source-spanner")]
        "spanner" => Ok(schema::<faucet_source_spanner::SpannerSourceConfig>()),
        #[cfg(feature = "source-parquet")]
        "parquet" => Ok(schema::<faucet_source_parquet::ParquetSourceConfig>()),
        #[cfg(feature = "source-delta")]
        "delta" => Ok(schema::<faucet_source_delta::DeltaSourceConfig>()),
        #[cfg(feature = "source-databricks")]
        "databricks" => Ok(schema::<faucet_source_databricks::DatabricksSourceConfig>()),
        #[cfg(feature = "source-gcs")]
        "gcs" => Ok(schema::<faucet_source_gcs::GcsSourceConfig>()),
        #[cfg(feature = "source-bigquery")]
        "bigquery" => Ok(schema::<faucet_source_bigquery::BigQuerySourceConfig>()),
        #[cfg(feature = "source-snowflake")]
        "snowflake" => Ok(schema::<faucet_source_snowflake::SnowflakeSourceConfig>()),
        #[cfg(feature = "source-mssql-cdc")]
        "mssql-cdc" => Ok(schema::<faucet_source_mssql_cdc::MssqlCdcSourceConfig>()),
        #[cfg(feature = "source-redshift")]
        "redshift" => Ok(schema::<faucet_source_redshift::RedshiftSourceConfig>()),
        #[cfg(feature = "source-pubsub")]
        "pubsub" => Ok(schema::<faucet_source_pubsub::PubsubSourceConfig>()),
        #[cfg(feature = "source-clickhouse")]
        "clickhouse" => Ok(schema::<faucet_source_clickhouse::ClickHouseSourceConfig>()),
        #[cfg(feature = "source-azure-blob")]
        "azure-blob" => Ok(schema::<faucet_source_azure_blob::AzureBlobSourceConfig>()),
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// Check if a source kind is registered (not unknown or disabled by feature gate).
pub fn source_exists(kind: &str) -> bool {
    source_schema(kind).is_ok()
}

/// Check if a sink kind is registered (not unknown or disabled by feature gate).
pub fn sink_exists(kind: &str) -> bool {
    sink_schema(kind).is_ok()
}

/// Return the JSON Schema for the named sink's config struct.
pub fn sink_schema(kind: &str) -> CliResult<Value> {
    if let Some(entry) = global().sinks.get(kind) {
        return Ok((entry.schema)());
    }
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => Ok(schema::<faucet_sink_bigquery::BigQuerySinkConfig>()),
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => Ok(schema::<faucet_sink_iceberg::IcebergSinkConfig>()),
        #[cfg(feature = "sink-delta")]
        "delta" => Ok(schema::<faucet_sink_delta::DeltaSinkConfig>()),
        #[cfg(feature = "sink-postgres")]
        "postgres" => Ok(schema::<faucet_sink_postgres::PostgresSinkConfig>()),
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => Ok(schema::<faucet_sink_jsonl::JsonlSinkConfig>()),
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => Ok(schema::<faucet_sink_snowflake::SnowflakeSinkConfig>()),
        #[cfg(feature = "sink-mysql")]
        "mysql" => Ok(schema::<faucet_sink_mysql::MysqlSinkConfig>()),
        #[cfg(feature = "sink-mssql")]
        "mssql" => Ok(schema::<faucet_sink_mssql::MssqlSinkConfig>()),
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => Ok(schema::<faucet_sink_sqlite::SqliteSinkConfig>()),
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => Ok(schema::<faucet_sink_duckdb::DuckdbSinkConfig>()),
        #[cfg(feature = "sink-sqs")]
        "sqs" => Ok(schema::<faucet_sink_sqs::SqsSinkConfig>()),
        #[cfg(feature = "sink-nats")]
        "nats" => Ok(schema::<faucet_sink_nats::NatsSinkConfig>()),
        #[cfg(feature = "sink-sftp")]
        "sftp" => Ok(schema::<faucet_sink_sftp::SftpSinkConfig>()),
        #[cfg(feature = "sink-s3")]
        "s3" => Ok(schema::<faucet_sink_s3::S3SinkConfig>()),
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => Ok(schema::<faucet_sink_mongodb::MongoSinkConfig>()),
        #[cfg(feature = "sink-redis")]
        "redis" => Ok(schema::<faucet_sink_redis::RedisSinkConfig>()),
        #[cfg(feature = "sink-csv")]
        "csv" => Ok(schema::<faucet_sink_csv::CsvSinkConfig>()),
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => Ok(schema::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>()),
        #[cfg(feature = "sink-kafka")]
        "kafka" => Ok(schema::<faucet_sink_kafka::KafkaSinkConfig>()),
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => Ok(schema::<faucet_sink_kinesis::KinesisSinkConfig>()),
        #[cfg(feature = "sink-spanner")]
        "spanner" => Ok(schema::<faucet_sink_spanner::SpannerSinkConfig>()),
        #[cfg(feature = "sink-http")]
        "http" => Ok(schema::<faucet_sink_http::HttpSinkConfig>()),
        #[cfg(feature = "sink-stdout")]
        "stdout" => Ok(schema::<faucet_sink_stdout::StdoutSinkConfig>()),
        #[cfg(feature = "sink-parquet")]
        "parquet" => Ok(schema::<faucet_sink_parquet::ParquetSinkConfig>()),
        #[cfg(feature = "sink-gcs")]
        "gcs" => Ok(schema::<faucet_sink_gcs::GcsSinkConfig>()),
        #[cfg(feature = "sink-redshift")]
        "redshift" => Ok(schema::<faucet_sink_redshift::RedshiftSinkConfig>()),
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => Ok(schema::<faucet_sink_pubsub::PubsubSinkConfig>()),
        #[cfg(feature = "sink-clickhouse")]
        "clickhouse" => Ok(schema::<faucet_sink_clickhouse::ClickHouseSinkConfig>()),
        #[cfg(feature = "sink-azure-blob")]
        "azure-blob" => Ok(schema::<faucet_sink_azure_blob::AzureBlobSinkConfig>()),
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// One-line summary of every source connector — the compiled-in built-ins plus
/// any third-party connectors registered via [`PluginRegistry`]. Used by
/// `faucet list`.
pub fn source_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v = builtin_source_descriptions();
    v.extend(global().custom_source_descriptions());
    v
}

/// One-line summary of every compiled-in built-in source connector (no customs).
#[allow(clippy::vec_init_then_push)]
fn builtin_source_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> = Vec::new();
    #[cfg(feature = "source-rest")]
    v.push(("rest", "REST API source with pagination, auth, transforms"));
    #[cfg(feature = "source-graphql")]
    v.push(("graphql", "GraphQL API source with cursor pagination"));
    #[cfg(feature = "source-xml")]
    v.push(("xml", "XML / SOAP API source with XML→JSON conversion"));
    #[cfg(feature = "source-grpc")]
    v.push(("grpc", "gRPC source with dynamic protobuf"));
    #[cfg(feature = "source-postgres")]
    v.push(("postgres", "PostgreSQL query source"));
    #[cfg(feature = "source-postgres-cdc")]
    v.push((
        "postgres-cdc",
        "PostgreSQL CDC source (logical replication)",
    ));
    #[cfg(feature = "source-mysql")]
    v.push(("mysql", "MySQL query source"));
    #[cfg(feature = "source-mssql")]
    v.push(("mssql", "Microsoft SQL Server query source"));
    #[cfg(feature = "source-sqlite")]
    v.push(("sqlite", "SQLite query source"));
    #[cfg(feature = "source-duckdb")]
    v.push((
        "duckdb",
        "DuckDB query source. Runs SQL against a DuckDB file or in-memory database and streams rows as JSON with bounded memory.",
    ));
    #[cfg(feature = "source-sqs")]
    v.push((
        "sqs",
        "AWS SQS source. Long-polls ReceiveMessage, deletes after the batch is emitted (at-least-once), with idle/max-messages termination.",
    ));
    #[cfg(feature = "source-nats")]
    v.push((
        "nats",
        "NATS source. Subscribes to a subject (or a JetStream durable consumer) and drains with idle/max-messages termination.",
    ));
    #[cfg(feature = "source-sftp")]
    v.push((
        "sftp",
        "SFTP source. Lists/globs a remote directory and streams JSONL / JSON-array / raw-text files over SSH.",
    ));
    #[cfg(feature = "source-s3")]
    v.push(("s3", "AWS S3 object source"));
    #[cfg(feature = "source-mongodb")]
    v.push(("mongodb", "MongoDB query source"));
    #[cfg(feature = "source-mongodb-cdc")]
    v.push(("mongodb-cdc", "MongoDB CDC source (Change Streams)"));
    #[cfg(feature = "source-mysql-cdc")]
    v.push(("mysql-cdc", "MySQL CDC source (binlog replication)"));
    #[cfg(feature = "source-mssql-cdc")]
    v.push((
        "mssql-cdc",
        "Microsoft SQL Server CDC source (change data capture, exactly-once capable)",
    ));
    #[cfg(feature = "source-redshift")]
    v.push((
        "redshift",
        "Amazon Redshift query source (PostgreSQL wire; streaming rows, incremental replication)",
    ));
    #[cfg(feature = "source-pubsub")]
    v.push((
        "pubsub",
        "Google Cloud Pub/Sub consumer — streaming pull with per-message records, attribute mapping, and ack at durable page boundaries (at-least-once)",
    ));
    #[cfg(feature = "source-clickhouse")]
    v.push((
        "clickhouse",
        "ClickHouse query source (HTTP interface, JSONEachRow streaming)",
    ));
    #[cfg(feature = "source-azure-blob")]
    v.push((
        "azure-blob",
        "Azure Blob Storage / ADLS Gen2 source — JSONL, JSON array, or raw text",
    ));
    #[cfg(feature = "source-redis")]
    v.push(("redis", "Redis (streams, lists, keys) source"));
    #[cfg(feature = "source-webhook")]
    v.push(("webhook", "Webhook HTTP receiver source"));
    #[cfg(feature = "source-websocket")]
    v.push((
        "websocket",
        "WebSocket streaming source — connects, subscribes, streams each message as a record",
    ));
    #[cfg(feature = "source-csv")]
    v.push(("csv", "CSV file source"));
    #[cfg(feature = "source-singer")]
    v.push((
        "singer",
        "Singer tap bridge (runs an external Singer tap; single-stream v0, Tier-2/experimental)",
    ));
    #[cfg(feature = "source-elasticsearch")]
    v.push(("elasticsearch", "Elasticsearch search / scroll source"));
    #[cfg(feature = "source-kafka")]
    v.push(("kafka", "Apache Kafka consumer (rdkafka). Subscribes to topics and drains messages with idle/max-messages termination."));
    #[cfg(feature = "source-kinesis")]
    v.push(("kinesis", "AWS Kinesis Data Streams consumer. Per-shard workers with resumable sequence-number checkpoints and idle/max-messages termination."));
    #[cfg(feature = "source-spanner")]
    v.push(("spanner", "Google Cloud Spanner query source. Streaming SQL reads with incremental replication bookmarks, stale reads, and PK-range sharding."));
    #[cfg(feature = "source-parquet")]
    v.push(("parquet", "Apache Parquet file source (local path, glob, or S3). Streams record batches via the Arrow async reader."));
    #[cfg(feature = "source-delta")]
    v.push(("delta", "Apache Delta Lake source (local FS or S3/Azure/GCS). Streams active data files with time travel and projection pushdown."));
    #[cfg(feature = "source-databricks")]
    v.push(("databricks", "Databricks SQL query source (Statement Execution API). Streams typed query results with chunk pagination and incremental replication."));
    #[cfg(feature = "source-gcs")]
    v.push((
        "gcs",
        "Google Cloud Storage source — JSONL, JSON array, or raw text",
    ));
    #[cfg(feature = "source-bigquery")]
    v.push((
        "bigquery",
        "Google BigQuery query source (jobs.query + jobs.getQueryResults)",
    ));
    #[cfg(feature = "source-snowflake")]
    v.push((
        "snowflake",
        "Snowflake query source (SQL REST API with partition paging)",
    ));
    v
}

/// One-line summary of every sink connector — the compiled-in built-ins plus
/// any third-party connectors registered via [`PluginRegistry`]. Used by
/// `faucet list`.
pub fn sink_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v = builtin_sink_descriptions();
    v.extend(global().custom_sink_descriptions());
    v
}

/// One-line summary of every compiled-in built-in sink connector (no customs).
#[allow(clippy::vec_init_then_push)]
fn builtin_sink_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> = Vec::new();
    #[cfg(feature = "sink-bigquery")]
    v.push(("bigquery", "Google BigQuery streaming-insert sink"));
    #[cfg(feature = "sink-iceberg")]
    v.push((
        "iceberg",
        "Apache Iceberg sink (append, REST/Glue/SQL/HMS catalogs)",
    ));
    #[cfg(feature = "sink-postgres")]
    v.push(("postgres", "PostgreSQL sink (JSONB or auto-mapped columns)"));
    #[cfg(feature = "sink-jsonl")]
    v.push(("jsonl", "JSON Lines file sink"));
    #[cfg(feature = "sink-snowflake")]
    v.push(("snowflake", "Snowflake SQL REST API sink"));
    #[cfg(feature = "sink-mysql")]
    v.push(("mysql", "MySQL sink"));
    #[cfg(feature = "sink-mssql")]
    v.push((
        "mssql",
        "Microsoft SQL Server sink (auto-mapped columns or JSON column)",
    ));
    #[cfg(feature = "sink-sqlite")]
    v.push(("sqlite", "SQLite sink"));
    #[cfg(feature = "sink-duckdb")]
    v.push((
        "duckdb",
        "DuckDB sink. Transaction-wrapped multi-row INSERT (JSON column or auto-mapped columns).",
    ));
    #[cfg(feature = "sink-sqs")]
    v.push((
        "sqs",
        "AWS SQS sink. Batched SendMessageBatch (10-message chunks) with per-entry partial-failure retry; FIFO group/dedup support.",
    ));
    #[cfg(feature = "sink-nats")]
    v.push((
        "nats",
        "NATS sink. Publishes records to a subject (optionally subject-per-record) and flushes per batch.",
    ));
    #[cfg(feature = "sink-sftp")]
    v.push((
        "sftp",
        "SFTP sink. Writes JSONL files over SSH with atomic temp-then-rename uploads.",
    ));
    #[cfg(feature = "sink-s3")]
    v.push(("s3", "AWS S3 object sink"));
    #[cfg(feature = "sink-mongodb")]
    v.push(("mongodb", "MongoDB insert sink"));
    #[cfg(feature = "sink-redis")]
    v.push(("redis", "Redis (streams, lists, key-value) sink"));
    #[cfg(feature = "sink-csv")]
    v.push(("csv", "CSV file sink"));
    #[cfg(feature = "sink-elasticsearch")]
    v.push(("elasticsearch", "Elasticsearch bulk index sink"));
    #[cfg(feature = "sink-kafka")]
    v.push(("kafka", "Apache Kafka producer (rdkafka). FuturesUnordered batched sends with QueueFull retry; supports fixed or per-record topic routing."));
    #[cfg(feature = "sink-kinesis")]
    v.push(("kinesis", "AWS Kinesis Data Streams producer. Batched PutRecords with partition-key routing and partial-failure retry (DLQ-routable)."));
    #[cfg(feature = "sink-spanner")]
    v.push(("spanner", "Google Cloud Spanner sink. Batched mutations with upsert/delete write modes, exactly-once commit tokens, and schema evolution."));
    #[cfg(feature = "sink-http")]
    v.push(("http", "HTTP POST sink (individual or array batch)"));
    #[cfg(feature = "sink-stdout")]
    v.push(("stdout", "Stdout / stderr sink (JSON Lines, pretty, TSV)"));
    #[cfg(feature = "sink-parquet")]
    v.push(("parquet", "Apache Parquet file sink (local path or S3). Schema-inferred, configurable compression, row/byte rollover."));
    #[cfg(feature = "sink-delta")]
    v.push(("delta", "Apache Delta Lake sink (local FS or S3/Azure/GCS). Append-only, schema-inferred table creation, one commit per flush."));
    #[cfg(feature = "sink-gcs")]
    v.push(("gcs", "Google Cloud Storage sink — JSONL files"));
    #[cfg(feature = "sink-redshift")]
    v.push((
        "redshift",
        "Amazon Redshift sink (COPY-from-S3 or multi-row INSERT)",
    ));
    #[cfg(feature = "sink-pubsub")]
    v.push((
        "pubsub",
        "Google Cloud Pub/Sub producer — batched publish with optional ordering keys, bounded concurrency, and partial-failure retry (DLQ-routable)",
    ));
    #[cfg(feature = "sink-clickhouse")]
    v.push((
        "clickhouse",
        "ClickHouse sink (HTTP INSERT … FORMAT JSONEachRow; optional async inserts)",
    ));
    #[cfg(feature = "sink-azure-blob")]
    v.push((
        "azure-blob",
        "Azure Blob Storage / ADLS Gen2 sink — JSONL files",
    ));
    v
}

/// Names of every compiled-in source connector.
pub fn source_kinds() -> Vec<&'static str> {
    source_descriptions().into_iter().map(|(k, _)| k).collect()
}

/// Names of every compiled-in sink connector.
pub fn sink_kinds() -> Vec<&'static str> {
    sink_descriptions().into_iter().map(|(k, _)| k).collect()
}

fn decode<T: DeserializeOwned>(kind: &'static str, name: &str, config: Value) -> CliResult<T> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidConnectorConfig {
        kind,
        name: name.to_owned(),
        message: scrub_config_error(&e.to_string()),
    })
}

/// Sanitise a serde deserialization error before it reaches stderr/logs.
///
/// serde_json's `invalid type:` errors echo the offending value as a
/// double-quoted literal — which can be a secret injected via
/// `${secret:...}` / `${env:...}`. Replace every double-quoted run with a
/// placeholder (field/type names use backticks and are preserved for
/// diagnostics) and cap the length so a huge value can't flood the log
/// (#78/#38). Note: `${secret:}` is currently an `${env:}` alias with no
/// at-rest redaction — this only scrubs error *output*.
fn scrub_config_error(msg: &str) -> String {
    const MAX_CHARS: usize = 200;
    let mut out = String::with_capacity(msg.len());
    let mut in_quote = false;
    for c in msg.chars() {
        if c == '"' {
            if !in_quote {
                out.push_str("\"<redacted>\"");
            }
            in_quote = !in_quote;
            continue;
        }
        if !in_quote {
            out.push(c);
        }
    }
    if out.chars().count() > MAX_CHARS {
        let truncated: String = out.chars().take(MAX_CHARS).collect();
        return format!("{truncated}…");
    }
    out
}

fn schema<T: faucet_core::JsonSchema>() -> Value {
    serde_json::to_value(faucet_core::schema_for!(T))
        .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

fn unknown(name: &str, kind: &'static str, available: Vec<&'static str>) -> CliError {
    CliError::UnknownConnector {
        kind,
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the relevant feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_load_allowlist_matches_capable_sinks() {
        for k in ["redshift", "snowflake", "bigquery", "clickhouse", "mssql"] {
            assert!(
                sink_supports_staged_load(k),
                "{k} should support staged load"
            );
        }
        for k in ["jsonl", "postgres", "sqlite", "stdout"] {
            assert!(
                !sink_supports_staged_load(k),
                "{k} should not (yet) support staged load"
            );
        }
    }

    // A trivial in-memory source used to exercise custom registration without
    // any I/O.
    #[derive(Clone)]
    struct DummySource;
    #[faucet_core::async_trait]
    impl Source for DummySource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, faucet_core::FaucetError> {
            Ok(vec![serde_json::json!({"ok": true})])
        }
        fn config_schema(&self) -> Value {
            serde_json::json!({"type": "object"})
        }
    }

    #[test]
    fn register_source_rejects_builtin_collision() {
        // `csv` is a built-in whenever that feature is on; use a name we know is
        // built-in under --all-features to assert the collision guard fires.
        let reg = PluginRegistry::with_builtins()
            .register_source("csv", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
        // install() surfaces the stashed error WITHOUT touching the global
        // (errors are checked before the OnceLock is set), so this is race-free.
        let err = reg
            .install()
            .expect_err("built-in collision must be rejected");
        match err {
            CliError::Config(msg) => assert!(msg.contains("built-in source"), "{msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn register_source_rejects_duplicate() {
        let reg = PluginRegistry::new()
            .register_source("dup", |_| Ok(Box::new(DummySource) as Box<dyn Source>))
            .register_source("dup", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
        let err = reg
            .install()
            .expect_err("duplicate registration must be rejected");
        match err {
            CliError::Config(msg) => assert!(msg.contains("more than once"), "{msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn register_sink_rejects_duplicate() {
        // Build a registry with a duplicate sink and confirm the error is
        // stashed; we inspect it via the private field rather than install()
        // so no global state is touched even indirectly.
        let reg = PluginRegistry::new()
            .register_sink("dupsink", |_| Err(CliError::Config("unused".into())))
            .register_sink("dupsink", |_| Err(CliError::Config("unused".into())));
        assert!(
            reg.errors.iter().any(|e| e.contains("more than once")),
            "{:?}",
            reg.errors
        );
    }

    #[test]
    fn custom_descriptions_use_default_when_blank() {
        let reg = PluginRegistry::new()
            .register_source("acme", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
        let descs = reg.custom_source_descriptions();
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].0, "acme");
        assert_eq!(descs[0].1, "custom source connector");
    }

    #[test]
    fn custom_descriptions_carry_explicit_summary() {
        let reg = PluginRegistry::new().register_source_with(
            "acme",
            |_| Ok(Box::new(DummySource) as Box<dyn Source>),
            || serde_json::json!({"type": "object", "title": "acme"}),
            "Acme widget source",
        );
        let descs = reg.custom_source_descriptions();
        assert_eq!(descs[0], ("acme", "Acme widget source"));
        // The schema closure is what `faucet schema source acme` would print.
        assert_eq!(
            (reg.sources.get("acme").unwrap().schema)()["title"],
            serde_json::json!("acme")
        );
    }

    #[test]
    fn capability_constants_match_their_predicates() {
        // F44: the human-readable lists in error messages derive from these
        // constants, which must stay in lockstep with the boolean gates. In
        // particular the idempotent-sink list must include bigquery AND kafka,
        // and the upsert-sink list must include bigquery — the values the old
        // hand-maintained message strings had drifted away from.
        for &k in EXACTLY_ONCE_SOURCE_KINDS {
            assert!(
                source_supports_exactly_once(k),
                "{k} should be exactly-once"
            );
        }
        for &k in IDEMPOTENT_SINK_KINDS {
            assert!(
                sink_supports_idempotent_writes(k),
                "{k} should be idempotent"
            );
        }
        for &k in UPSERT_SINK_KINDS {
            use faucet_core::WriteMode;
            assert!(
                sink_supported_write_modes(k).contains(&WriteMode::Upsert),
                "{k} should support upsert"
            );
        }
        assert!(IDEMPOTENT_SINK_KINDS.contains(&"bigquery"));
        assert!(IDEMPOTENT_SINK_KINDS.contains(&"kafka"));
        assert!(UPSERT_SINK_KINDS.contains(&"bigquery"));
    }

    #[cfg(feature = "source-rest")]
    #[test]
    fn rest_source_appears_in_listings() {
        assert!(source_kinds().contains(&"rest"));
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn jsonl_sink_appears_in_listings() {
        assert!(sink_kinds().contains(&"jsonl"));
    }

    #[tokio::test]
    async fn unknown_source_kind_errors() {
        let err = build_source("nope", serde_json::json!({}), &AuthCatalog::new(), None)
            .await
            .err()
            .expect("should fail");
        match err {
            CliError::UnknownConnector { kind, name, .. } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "nope");
            }
            other => panic!("expected UnknownConnector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_sink_kind_errors() {
        let err = build_sink("nope", serde_json::json!({}), &AuthCatalog::new())
            .await
            .err()
            .expect("should fail");
        assert!(matches!(
            err,
            CliError::UnknownConnector { kind: "sink", .. }
        ));
    }

    #[cfg(feature = "source-rest")]
    #[test]
    fn rest_schema_is_object() {
        let s = source_schema("rest").unwrap();
        assert!(s.is_object());
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn jsonl_schema_is_object() {
        let s = sink_schema("jsonl").unwrap();
        assert!(s.is_object());
    }

    #[test]
    fn scrub_config_error_redacts_quoted_values() {
        // A serde "invalid type" error echoes the offending value in double
        // quotes — must be redacted so a secret can't reach the log (#78/#38).
        let msg =
            r#"invalid type: string "sk-super-secret-123", expected a sequence at line 1 column 9"#;
        let scrubbed = scrub_config_error(msg);
        assert!(!scrubbed.contains("sk-super-secret-123"), "{scrubbed}");
        assert!(scrubbed.contains("<redacted>"), "{scrubbed}");
        // Structural context outside the quotes is preserved.
        assert!(scrubbed.contains("invalid type"), "{scrubbed}");
        assert!(scrubbed.contains("expected a sequence"), "{scrubbed}");
    }

    #[test]
    fn scrub_config_error_truncates_long_messages() {
        let msg = "x".repeat(500);
        let scrubbed = scrub_config_error(&msg);
        assert!(
            scrubbed.chars().count() <= 201,
            "len {}",
            scrubbed.chars().count()
        );
        assert!(scrubbed.ends_with('…'));
    }

    // A `(kind, config)` pair that builds without performing any network/disk
    // I/O — the CSV source's `new()` only stores config, so we can drive the
    // real `build_source` dispatch arm and inspect the resulting trait object.
    #[cfg(feature = "source-csv")]
    #[tokio::test]
    async fn build_source_csv_succeeds_without_io() {
        let src = build_source(
            "csv",
            serde_json::json!({ "path": "/tmp/does-not-need-to-exist.csv" }),
            &AuthCatalog::new(),
            None,
        )
        .await
        .expect("csv source should build without I/O");
        // The CSV source overrides `connector_name()` with its friendly,
        // YAML-`type`-matching label (#61).
        assert_eq!(src.connector_name(), "csv");
    }

    // The JSONL sink's `new()` is also pure (it opens the file lazily on first
    // write), so building it exercises the sink dispatch arm with no I/O.
    #[cfg(feature = "sink-jsonl")]
    #[tokio::test]
    async fn build_sink_jsonl_succeeds_without_io() {
        let sink = build_sink(
            "jsonl",
            serde_json::json!({ "path": "/tmp/does-not-need-to-exist.jsonl" }),
            &AuthCatalog::new(),
        )
        .await
        .expect("jsonl sink should build without I/O");
        assert_eq!(sink.connector_name(), "jsonl");
    }

    // The stdout sink builds without any config fields and without I/O.
    #[cfg(feature = "sink-stdout")]
    #[tokio::test]
    async fn build_sink_stdout_succeeds_without_io() {
        let sink = build_sink("stdout", serde_json::json!({}), &AuthCatalog::new())
            .await
            .expect("stdout sink should build without I/O");
        // The stdout sink overrides `connector_name()` with its friendly,
        // YAML-`type`-matching label (#61).
        assert_eq!(sink.connector_name(), "stdout");
    }

    // Exercise the Delta source+sink registry arms end to end: build both via
    // the registry, round-trip a page through a real local table, and confirm
    // the schema + description arms resolve.
    #[cfg(all(feature = "source-delta", feature = "sink-delta"))]
    #[tokio::test]
    async fn delta_registry_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("reg_delta").to_string_lossy().into_owned();

        assert!(source_schema("delta").is_ok());
        assert!(sink_schema("delta").is_ok());
        assert!(source_descriptions().iter().any(|(n, _)| *n == "delta"));
        assert!(sink_descriptions().iter().any(|(n, _)| *n == "delta"));

        let sink = build_sink(
            "delta",
            serde_json::json!({ "table_uri": uri }),
            &AuthCatalog::new(),
        )
        .await
        .expect("delta sink builds");
        assert_eq!(sink.connector_name(), "delta");
        let n = sink
            .write_batch(&[serde_json::json!({"id": 1}), serde_json::json!({"id": 2})])
            .await
            .expect("write");
        assert_eq!(n, 2);
        sink.flush().await.expect("flush");

        let source = build_source(
            "delta",
            serde_json::json!({ "table_uri": uri }),
            &AuthCatalog::new(),
            None,
        )
        .await
        .expect("delta source builds");
        assert_eq!(source.connector_name(), "delta");
        let rows = source
            .fetch_with_context(&std::collections::HashMap::new())
            .await
            .expect("read");
        assert_eq!(rows.len(), 2);
    }

    // The Databricks source builds from the registry (no I/O in `new`), and its
    // schema + description arms resolve.
    #[cfg(feature = "source-databricks")]
    #[tokio::test]
    async fn databricks_registry_source_builds() {
        assert!(source_schema("databricks").is_ok());
        assert!(
            source_descriptions()
                .iter()
                .any(|(n, _)| *n == "databricks")
        );
        let cfg = serde_json::json!({
            "workspace_url": "https://x.cloud.databricks.com",
            "warehouse_id": "wh1",
            "sql": "SELECT 1",
            "auth": { "type": "pat", "config": { "token": "t" } }
        });
        let src = build_source("databricks", cfg, &AuthCatalog::new(), None)
            .await
            .expect("databricks source builds");
        assert_eq!(src.connector_name(), "databricks");
    }

    // A malformed config for a known connector must surface as a typed
    // `InvalidConnectorConfig` from the `decode` helper, not a panic.
    #[cfg(feature = "source-csv")]
    #[tokio::test]
    async fn build_source_csv_invalid_config_errors() {
        // `path` is a required String; supplying an integer is a type error.
        // `Box<dyn Source>` is not `Debug`, so match the Result directly rather
        // than using `expect_err`.
        let res = build_source(
            "csv",
            serde_json::json!({ "path": 42 }),
            &AuthCatalog::new(),
            None,
        )
        .await;
        match res {
            Err(CliError::InvalidConnectorConfig { kind, name, .. }) => {
                assert_eq!(kind, "source");
                assert_eq!(name, "csv");
            }
            Ok(_) => panic!("expected InvalidConnectorConfig, got Ok"),
            Err(other) => panic!("expected InvalidConnectorConfig, got {other:?}"),
        }
    }

    // The CSV / GraphQL sources construct infallibly (`new() -> Self`), so the
    // registry is their config-load fail-fast point: an out-of-range
    // `batch_size` or an empty required field must be rejected at `build_source`
    // (a typed `FaucetError::Config`), not surfaced deep in a run.
    #[cfg(feature = "source-csv")]
    #[tokio::test]
    async fn build_source_csv_rejects_oversized_batch_size() {
        let res = build_source(
            "csv",
            serde_json::json!({
                "path": "/tmp/x.csv",
                "batch_size": faucet_core::MAX_BATCH_SIZE + 1
            }),
            &AuthCatalog::new(),
            None,
        )
        .await;
        assert!(matches!(
            res,
            Err(CliError::Faucet(faucet_core::FaucetError::Config(_)))
        ));
    }

    #[cfg(feature = "source-graphql")]
    #[tokio::test]
    async fn build_source_graphql_rejects_empty_endpoint() {
        let res = build_source(
            "graphql",
            serde_json::json!({
                "endpoint": "",
                "query": "query { x }",
                "variables": {},
                "auth": { "type": "none" }
            }),
            &AuthCatalog::new(),
            None,
        )
        .await;
        assert!(matches!(
            res,
            Err(CliError::Faucet(faucet_core::FaucetError::Config(_)))
        ));
    }

    // `source_schema` must return a JSON object that surfaces the connector's
    // config fields (here: the required `path`).
    #[cfg(feature = "source-csv")]
    #[test]
    fn source_schema_csv_exposes_path_property() {
        let schema = source_schema("csv").expect("csv schema");
        let props = schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("schema should have a properties object");
        assert!(props.contains_key("path"), "schema props: {props:?}");
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn sink_schema_jsonl_exposes_path_property() {
        let schema = sink_schema("jsonl").expect("jsonl schema");
        let props = schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("schema should have a properties object");
        assert!(props.contains_key("path"), "schema props: {props:?}");
    }

    #[test]
    fn unknown_source_schema_errors_with_available_list() {
        let err = source_schema("definitely-not-a-source").expect_err("unknown source");
        match err {
            CliError::UnknownConnector {
                kind,
                name,
                available,
            } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "definitely-not-a-source");
                // Under `--all-features` the available list is non-empty.
                assert!(!available.is_empty());
            }
            other => panic!("expected UnknownConnector, got {other:?}"),
        }
    }

    #[test]
    fn unknown_sink_schema_errors() {
        let err = sink_schema("definitely-not-a-sink").expect_err("unknown sink");
        assert!(matches!(
            err,
            CliError::UnknownConnector { kind: "sink", .. }
        ));
    }

    #[cfg(feature = "source-csv")]
    #[test]
    fn source_exists_is_true_for_known_and_false_for_unknown() {
        assert!(source_exists("csv"));
        assert!(!source_exists("definitely-not-a-source"));
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn sink_exists_is_true_for_known_and_false_for_unknown() {
        assert!(sink_exists("jsonl"));
        assert!(!sink_exists("definitely-not-a-sink"));
    }

    // Descriptions back `faucet list`: non-empty, with a one-line summary, and
    // each name must resolve to a real schema (no orphan listing).
    #[test]
    fn source_descriptions_are_non_empty_and_consistent() {
        let descs = source_descriptions();
        assert!(!descs.is_empty());
        for (name, summary) in &descs {
            assert!(!name.is_empty(), "empty connector name");
            assert!(!summary.is_empty(), "empty summary for {name}");
            assert!(
                source_schema(name).is_ok(),
                "listed source `{name}` has no schema"
            );
        }
    }

    #[test]
    fn sink_descriptions_are_non_empty_and_consistent() {
        let descs = sink_descriptions();
        assert!(!descs.is_empty());
        for (name, summary) in &descs {
            assert!(!name.is_empty(), "empty connector name");
            assert!(!summary.is_empty(), "empty summary for {name}");
            assert!(
                sink_schema(name).is_ok(),
                "listed sink `{name}` has no schema"
            );
        }
    }

    // `*_kinds()` is derived from `*_descriptions()`; under `--all-features`
    // the canonical built-in connectors must be present.
    #[cfg(all(feature = "source-csv", feature = "source-rest"))]
    #[test]
    fn source_kinds_contains_expected_builtins() {
        let kinds = source_kinds();
        assert!(kinds.contains(&"csv"));
        assert!(kinds.contains(&"rest"));
    }

    #[cfg(all(feature = "sink-jsonl", feature = "sink-stdout"))]
    #[test]
    fn sink_kinds_contains_expected_builtins() {
        let kinds = sink_kinds();
        assert!(kinds.contains(&"jsonl"));
        assert!(kinds.contains(&"stdout"));
    }

    // Build a catalog holding one `static` bearer provider, then build a
    // connector whose config carries `auth: { ref: "tok" }` — exercising the
    // `with_auth_provider` injection branch in the dispatch arm.
    #[cfg(feature = "source-rest")]
    #[tokio::test]
    async fn build_source_injects_referenced_auth_provider() {
        let mut specs = std::collections::HashMap::new();
        specs.insert(
            "tok".to_string(),
            serde_json::json!({"type": "static", "config": {"token": "abc"}}),
        );
        let catalog = auth_catalog::build_auth_catalog(Some(&specs)).expect("catalog");

        let src = build_source("rest", rest_config_with_auth_ref("tok"), &catalog, None)
            .await
            .expect("rest source with a resolvable auth ref should build");
        assert_eq!(src.connector_name(), "rest");
    }

    // A minimal, fully-valid rest config (built from the real constructor so
    // every required field is present) carrying an `auth: { ref }` pointer.
    #[cfg(feature = "source-rest")]
    fn rest_config_with_auth_ref(name: &str) -> Value {
        let cfg = faucet_source_rest::RestStreamConfig::new("https://api.example.com", "/v1");
        let mut v = serde_json::to_value(cfg).expect("serialize rest config");
        v.as_object_mut()
            .unwrap()
            .insert("auth".to_string(), serde_json::json!({ "ref": name }));
        v
    }

    // An `auth: { ref }` pointing at a name absent from the catalog must surface
    // as `UnknownAuthProvider`, not silently build without auth.
    #[cfg(feature = "source-rest")]
    #[tokio::test]
    async fn build_source_unknown_auth_ref_errors() {
        let res = build_source(
            "rest",
            rest_config_with_auth_ref("missing"),
            &AuthCatalog::new(),
            None,
        )
        .await;
        match res {
            Err(CliError::UnknownAuthProvider { name, .. }) => assert_eq!(name, "missing"),
            Ok(_) => panic!("expected UnknownAuthProvider, got Ok"),
            Err(other) => panic!("expected UnknownAuthProvider, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_capability_allowlists() {
        assert!(source_supports_exactly_once("postgres-cdc"));
        assert!(source_supports_exactly_once("mysql-cdc"));
        assert!(source_supports_exactly_once("mongodb-cdc"));
        assert!(source_supports_exactly_once("kafka"));
        assert!(!source_supports_exactly_once("rest"));

        assert!(sink_supports_idempotent_writes("postgres"));
        assert!(sink_supports_idempotent_writes("iceberg"));
        assert!(sink_supports_idempotent_writes("bigquery"));
        assert!(sink_supports_idempotent_writes("kafka"));
        assert!(sink_supports_idempotent_writes("snowflake"));
        assert!(sink_supports_idempotent_writes("redis"));
        assert!(sink_supports_idempotent_writes("mongodb"));
        assert!(!sink_supports_idempotent_writes("jsonl"));
    }

    #[test]
    fn typed_delivery_capabilities_derive_from_kind_tables() {
        use faucet_core::{ReplayGuarantee, SinkGuarantee};
        assert_eq!(
            source_replay_guarantee("kafka"),
            ReplayGuarantee::Deterministic
        );
        assert_eq!(
            source_replay_guarantee("rest"),
            ReplayGuarantee::NonDeterministic
        );
        assert_eq!(sink_guarantee("postgres"), SinkGuarantee::AtomicWatermark);
        // Upsert-capable but not atomic: elasticsearch dedups by key only.
        assert_eq!(sink_guarantee("elasticsearch"), SinkGuarantee::KeyedUpsert);
        assert_eq!(sink_guarantee("jsonl"), SinkGuarantee::AtLeastOnce);
    }

    #[test]
    fn sink_supported_write_modes_allowlist() {
        use faucet_core::WriteMode;
        assert!(sink_supported_write_modes("postgres").contains(&WriteMode::Upsert));
        assert!(sink_supported_write_modes("elasticsearch").contains(&WriteMode::Delete));
        assert!(sink_supported_write_modes("bigquery").contains(&WriteMode::Upsert));
        // a sink without upsert support is append-only
        assert_eq!(sink_supported_write_modes("jsonl"), &[WriteMode::Append]);
        assert_eq!(sink_supported_write_modes("kafka"), &[WriteMode::Append]);

        // Overwrite (#492, #494) lands on the 6 staging-swap sinks plus
        // elasticsearch (atomic alias swap); the rest reject it.
        for k in [
            "postgres",
            "sqlite",
            "mysql",
            "mssql",
            "mongodb",
            "bigquery",
            "elasticsearch",
        ] {
            assert!(
                sink_supported_write_modes(k).contains(&WriteMode::Overwrite),
                "{k} should support overwrite"
            );
            assert!(sink_supports_overwrite(k), "{k} sink_supports_overwrite");
        }
        // jsonl supports neither upsert nor overwrite.
        assert!(!sink_supports_overwrite("jsonl"));
    }

    #[test]
    fn sink_supports_schema_evolution_allowlist() {
        assert!(sink_supports_schema_evolution("postgres"));
        assert!(sink_supports_schema_evolution("mysql"));
        assert!(sink_supports_schema_evolution("mssql"));
        assert!(sink_supports_schema_evolution("sqlite"));
        assert!(sink_supports_schema_evolution("bigquery"));
        assert!(sink_supports_schema_evolution("elasticsearch"));
        assert!(sink_supports_schema_evolution("iceberg"));
        assert!(!sink_supports_schema_evolution("jsonl"));
        assert!(!sink_supports_schema_evolution("kafka"));
    }
}
