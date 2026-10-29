//! Feature-gated dispatch from a string `type` to a concrete connector.
//!
//! Every arm in this file is guarded by the matching `source-*` / `sink-*`
//! Cargo feature so a build carries just the connectors it needs. The string
//! keys here are the public contract of the `type:` field in pipeline configs.
#![allow(deprecated)]
// A build with only some connectors leaves parts of the dispatch unused.
#![cfg_attr(
    not(all(feature = "source", feature = "sink")),
    allow(unused_variables, unused_mut, unused_imports, dead_code)
)]

use crate::auth_catalog::{self, AuthCatalog};
use crate::error::{RegistryError, RegistryResult};
use crate::policy::BuildPolicy;
use faucet_core::{Capabilities, ConnectorDescriptor, FaucetError, Sink, Source};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
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
pub type SourceFactory = Arc<dyn Fn(Value) -> RegistryResult<Box<dyn Source>> + Send + Sync>;

/// A factory that builds a [`Sink`] trait object from its `sink.config`. See
/// [`SourceFactory`] for the contract.
pub type SinkFactory = Arc<dyn Fn(Value) -> RegistryResult<Box<dyn Sink>> + Send + Sync>;

/// A closure returning the JSON Schema for a custom connector's config. Powers
/// `faucet schema source <name>` / `faucet schema sink <name>` for third-party
/// connectors without instantiating one. Typically `|| schema_for!(MyConfig)`.
pub type SchemaFn = Arc<dyn Fn() -> Value + Send + Sync>;

struct SourceEntry {
    factory: SourceFactory,
    schema: SchemaFn,
    description: &'static str,
    descriptor: Option<ConnectorDescriptor>,
}

struct SinkEntry {
    factory: SinkFactory,
    schema: SchemaFn,
    description: &'static str,
    descriptor: Option<ConnectorDescriptor>,
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
/// use faucet_registry::PluginRegistry;
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
/// registry.install()?;
/// # Ok::<(), faucet_registry::RegistryError>(())
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
        F: Fn(Value) -> RegistryResult<Box<dyn Source>> + Send + Sync + 'static,
    {
        self.register_source_with(name, factory, || serde_json::json!({"type": "object"}), "")
    }

    /// Register a custom source with an explicit schema closure and one-line
    /// description (shown by `faucet list` and `faucet schema source <name>`).
    /// It declares no capabilities to the load-time gates; use
    /// [`register_source_described`](Self::register_source_described) to
    /// declare them.
    #[must_use]
    pub fn register_source_with<F, S>(
        self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Source>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        self.insert_source(name, factory, schema, description, None)
    }

    /// Register a custom source under `descriptor.kind()` together with its
    /// [`ConnectorDescriptor`], so the load-time gates (`faucet validate`,
    /// exactly-once, discover, verify) see the capabilities it declares —
    /// exactly as they see a built-in's (RFC 0013).
    #[must_use]
    pub fn register_source_described<F, S>(
        self,
        descriptor: ConnectorDescriptor,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Source>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        self.insert_source(
            descriptor.kind(),
            factory,
            schema,
            description,
            Some(descriptor),
        )
    }

    fn insert_source<F, S>(
        mut self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
        descriptor: Option<ConnectorDescriptor>,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Source>> + Send + Sync + 'static,
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
                descriptor,
            },
        );
        self
    }

    /// Register a custom sink connector under `name` (its YAML `type:`).
    #[must_use]
    pub fn register_sink<F>(self, name: &str, factory: F) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Sink>> + Send + Sync + 'static,
    {
        self.register_sink_with(name, factory, || serde_json::json!({"type": "object"}), "")
    }

    /// Register a custom sink with an explicit schema closure and description.
    /// It declares append-only capabilities to the load-time gates; use
    /// [`register_sink_described`](Self::register_sink_described) to declare
    /// its write modes, exactly-once, cleanup and the rest.
    #[must_use]
    pub fn register_sink_with<F, S>(
        self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Sink>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        self.insert_sink(name, factory, schema, description, None)
    }

    /// Register a custom sink under `descriptor.kind()` together with its
    /// [`ConnectorDescriptor`], so the load-time gates (write modes, upsert,
    /// overwrite, exactly-once, cleanup, rollback, schema drift) accept what it
    /// declares — exactly as they accept a built-in's (RFC 0013).
    #[must_use]
    pub fn register_sink_described<F, S>(
        self,
        descriptor: ConnectorDescriptor,
        factory: F,
        schema: S,
        description: &str,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Sink>> + Send + Sync + 'static,
        S: Fn() -> Value + Send + Sync + 'static,
    {
        self.insert_sink(
            descriptor.kind(),
            factory,
            schema,
            description,
            Some(descriptor),
        )
    }

    fn insert_sink<F, S>(
        mut self,
        name: &str,
        factory: F,
        schema: S,
        description: &str,
        descriptor: Option<ConnectorDescriptor>,
    ) -> Self
    where
        F: Fn(Value) -> RegistryResult<Box<dyn Sink>> + Send + Sync + 'static,
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
                descriptor,
            },
        );
        self
    }

    /// Install this registry as the process-global custom-connector registry.
    /// Called once at startup (the `faucet` CLI's `run_main` does it).
    /// Returns an error if any `register_*`
    /// call collided, or if a registry was already installed.
    pub fn install(self) -> RegistryResult<()> {
        if !self.errors.is_empty() {
            return Err(RegistryError::Config(self.errors.join("; ")));
        }
        GLOBAL_REGISTRY
            .set(self)
            .map_err(|_| RegistryError::Config("connector registry already installed".to_owned()))
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

/// The custom connectors registered without a [`ConnectorDescriptor`]
/// (`source:<name>` / `sink:<name>`), sorted. Their capabilities cannot be
/// checked before they are built, so the load-time gates treat them as plain
/// (RFC 0014): `faucet doctor` lists each one, and
/// [`warn_undescribed_plugins`] logs them once at startup.
pub fn undescribed_plugins() -> Vec<String> {
    undescribed_in(global())
}

fn undescribed_in(registry: &PluginRegistry) -> Vec<String> {
    let mut out: Vec<String> = registry
        .sources
        .iter()
        .filter(|(_, e)| e.descriptor.is_none())
        .map(|(n, _)| format!("source:{n}"))
        .chain(
            registry
                .sinks
                .iter()
                .filter(|(_, e)| e.descriptor.is_none())
                .map(|(n, _)| format!("sink:{n}")),
        )
        .collect();
    out.sort();
    out
}

/// Log a warning naming every plugin registered without a descriptor (see
/// [`undescribed_plugins`]). Called once, after the process installs its log
/// subscriber.
pub fn warn_undescribed_plugins() {
    for plugin in undescribed_plugins() {
        tracing::warn!(
            plugin = %plugin,
            "connector plugin registered without a ConnectorDescriptor: its capabilities \
             cannot be checked at load time, so it is treated as a plain connector; register \
             it with register_source_described / register_sink_described (RFC 0014)"
        );
    }
}

/// The custom connectors registered through a [`PluginRegistry`]
/// (`source:<name>` / `sink:<name>`), sorted — part of a template test
/// result's provenance (#856).
pub fn plugin_connectors() -> Vec<String> {
    let g = global();
    let mut out: Vec<String> = g
        .sources
        .keys()
        .map(|n| format!("source:{n}"))
        .chain(g.sinks.keys().map(|n| format!("sink:{n}")))
        .collect();
    out.sort();
    out
}

type Built<T> = std::pin::Pin<Box<dyn std::future::Future<Output = RegistryResult<Box<T>>> + Send>>;

/// Await a source constructor on the heap, so the future of the `match` in
/// [`build_source`] stays small whichever arm it takes.
#[allow(dead_code)]
fn boxed_source<S, F>(fut: F) -> Built<dyn Source>
where
    S: Source + 'static,
    F: std::future::Future<Output = Result<S, faucet_core::FaucetError>> + Send + 'static,
{
    Box::pin(async move { Ok(Box::new(fut.await?) as Box<dyn Source>) })
}

/// [`boxed_source`] for sinks.
#[allow(dead_code)]
fn boxed_sink<S, F>(fut: F) -> Built<dyn Sink>
where
    S: Sink + 'static,
    F: std::future::Future<Output = Result<S, faucet_core::FaucetError>> + Send + 'static,
{
    Box::pin(async move { Ok(Box::new(fut.await?) as Box<dyn Sink>) })
}

/// Remove the executor's overwrite run token
/// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]) from a sink config: no
/// connector config declares it, so a sink that stages an overwrite is handed
/// it through its `with_overwrite_run` instead, and every other sink never
/// sees it.
fn take_overwrite_run(kind: &str, mut config: Value) -> RegistryResult<(Value, Option<String>)> {
    let Some(v) = config
        .as_object_mut()
        .and_then(|m| m.remove(faucet_core::idempotency::OVERWRITE_RUN_KEY))
    else {
        return Ok((config, None));
    };
    match v {
        Value::String(t) => Ok((config, Some(t))),
        Value::Null => Ok((config, None)),
        other => Err(RegistryError::InvalidConnectorConfig {
            kind: "sink",
            name: kind.to_owned(),
            message: format!(
                "`{}` must be an overwrite run token string, got {other}",
                faucet_core::idempotency::OVERWRITE_RUN_KEY
            ),
            path: Some(faucet_core::idempotency::OVERWRITE_RUN_KEY.to_owned()),
        }),
    }
}

/// Build a sink, then join it to the overwrite run `run` when there is one.
#[allow(dead_code)]
async fn joined<S, F>(
    fut: F,
    run: Option<String>,
    join: fn(S, &str) -> Result<S, faucet_core::FaucetError>,
) -> Result<S, faucet_core::FaucetError>
where
    F: std::future::Future<Output = Result<S, faucet_core::FaucetError>>,
{
    let sink = fut.await?;
    match run {
        Some(token) => join(sink, &token),
        None => Ok(sink),
    }
}

/// Build a [`Source`] trait object from a `(kind, config)` pair. When the
/// config carries `auth: { ref: <name> }`, the named provider is resolved from
/// `auth` (the catalog) and injected into the connector.
///
/// `config` is used exactly as given: it must already be resolved (no
/// `${env:…}` / `${file:…}` / secret-manager references are expanded here).
/// Builds under the default [`BuildPolicy`], which refuses a connector that
/// runs a program on this host ([`source_runs_program`]); use
/// [`build_source_with`] to allow one.
pub async fn build_source(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    retry_policy: Option<&faucet_core::RetryPolicy>,
) -> RegistryResult<Box<dyn Source>> {
    build_source_with(kind, config, auth, retry_policy, &BuildPolicy::default()).await
}

/// [`build_source`] under an explicit [`BuildPolicy`].
pub async fn build_source_with(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    retry_policy: Option<&faucet_core::RetryPolicy>,
    policy: &BuildPolicy,
) -> RegistryResult<Box<dyn Source>> {
    if !policy.allows_subprocess() && source_runs_program(kind) {
        return Err(RegistryError::SubprocessRefused {
            role: "source",
            kind: kind.to_owned(),
        });
    }
    // Boxed: the dispatch over every compiled connector makes this future
    // large, and callers run on 2 MiB worker and test thread stacks.
    Box::pin(build_source_inner(kind, config, auth, retry_policy)).await
}

async fn build_source_inner(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    retry_policy: Option<&faucet_core::RetryPolicy>,
) -> RegistryResult<Box<dyn Source>> {
    // Third-party connectors registered via `PluginRegistry` win first. Names
    // can never collide with a built-in (registration rejects that), so this is
    // safe to check ahead of the built-in `match`. Custom factories receive the
    // raw config and manage their own auth (the shared `auth:` catalog is not
    // injected into custom connectors).
    if let Some(entry) = global().sources.get(kind) {
        reject_unknown_config_keys("source", kind, kind, &config, &(entry.schema)())?;
        return (entry.factory)(config);
    }
    // Unknown keys are rejected here too, not only in `faucet validate`: a
    // config can reach a run without passing through validate (serve
    // submission, a template trigger, `run --from-env`), and a silently-ignored
    // key is the failure this gate exists to stop (#654 H9).
    if let Ok(schema) = source_schema(kind) {
        reject_unknown_config_keys("source", kind, kind, &config, &schema)?;
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
            boxed_source(faucet_source_postgres::PostgresSource::new(cfg)).await
        }
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => {
            let cfg = decode::<faucet_source_postgres_cdc::PostgresCdcSourceConfig>(
                "source",
                "postgres-cdc",
                config,
            )?;
            boxed_source(faucet_source_postgres_cdc::PostgresCdcSource::new(cfg)).await
        }
        #[cfg(feature = "source-mysql")]
        "mysql" => {
            let cfg = decode::<faucet_source_mysql::MysqlSourceConfig>("source", "mysql", config)?;
            boxed_source(faucet_source_mysql::MysqlSource::new(cfg)).await
        }
        #[cfg(feature = "source-mssql")]
        "mssql" => {
            let cfg = decode::<faucet_source_mssql::MssqlSourceConfig>("source", "mssql", config)?;
            boxed_source(faucet_source_mssql::MssqlSource::new(cfg)).await
        }
        #[cfg(feature = "source-sqlite")]
        "sqlite" => {
            let cfg =
                decode::<faucet_source_sqlite::SqliteSourceConfig>("source", "sqlite", config)?;
            boxed_source(faucet_source_sqlite::SqliteSource::new(cfg)).await
        }
        #[cfg(feature = "source-duckdb")]
        "duckdb" => {
            let cfg =
                decode::<faucet_source_duckdb::DuckdbSourceConfig>("source", "duckdb", config)?;
            boxed_source(faucet_source_duckdb::DuckdbSource::new(cfg)).await
        }
        #[cfg(feature = "source-sqs")]
        "sqs" => {
            let cfg = decode::<faucet_source_sqs::SqsSourceConfig>("source", "sqs", config)?;
            boxed_source(faucet_source_sqs::SqsSource::new(cfg)).await
        }
        #[cfg(feature = "source-nats")]
        "nats" => {
            let cfg = decode::<faucet_source_nats::NatsSourceConfig>("source", "nats", config)?;
            boxed_source(faucet_source_nats::NatsSource::new(cfg)).await
        }
        #[cfg(feature = "source-rabbitmq")]
        "rabbitmq" => {
            let cfg = decode::<faucet_source_rabbitmq::RabbitMqSourceConfig>(
                "source", "rabbitmq", config,
            )?;
            boxed_source(faucet_source_rabbitmq::RabbitMqSource::new(cfg)).await
        }
        #[cfg(feature = "source-sftp")]
        "sftp" => {
            let cfg = decode::<faucet_source_sftp::SftpSourceConfig>("source", "sftp", config)?;
            Ok(Box::new(faucet_source_sftp::SftpSource::new(cfg)?))
        }
        #[cfg(feature = "source-file")]
        "file" => {
            let cfg = decode::<faucet_source_file::FileSourceConfig>("source", "file", config)?;
            Ok(Box::new(faucet_source_file::FileSource::new(cfg)?))
        }
        #[cfg(feature = "source-s3")]
        "s3" => {
            let cfg = decode::<faucet_source_s3::S3SourceConfig>("source", "s3", config)?;
            boxed_source(faucet_source_s3::S3Source::new(cfg)).await
        }
        #[cfg(feature = "source-mongodb")]
        "mongodb" => {
            let cfg =
                decode::<faucet_source_mongodb::MongoSourceConfig>("source", "mongodb", config)?;
            boxed_source(faucet_source_mongodb::MongoSource::new(cfg)).await
        }
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => {
            let cfg = decode::<faucet_source_mongodb_cdc::MongoCdcSourceConfig>(
                "source",
                "mongodb-cdc",
                config,
            )?;
            boxed_source(faucet_source_mongodb_cdc::MongoCdcSource::new(cfg)).await
        }
        #[cfg(feature = "source-mysql-cdc")]
        "mysql-cdc" => {
            let cfg = decode::<faucet_source_mysql_cdc::MysqlCdcSourceConfig>(
                "source",
                "mysql-cdc",
                config,
            )?;
            boxed_source(faucet_source_mysql_cdc::MysqlCdcSource::new(cfg)).await
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
            cfg.validate()?;
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
            boxed_source(faucet_source_kafka::KafkaSource::new(cfg)).await
        }
        #[cfg(feature = "source-kinesis")]
        "kinesis" => {
            let cfg =
                decode::<faucet_source_kinesis::KinesisSourceConfig>("source", "kinesis", config)?;
            boxed_source(faucet_source_kinesis::KinesisSource::new(cfg)).await
        }
        #[cfg(feature = "source-spanner")]
        "spanner" => {
            let cfg =
                decode::<faucet_source_spanner::SpannerSourceConfig>("source", "spanner", config)?;
            boxed_source(faucet_source_spanner::SpannerSource::new(cfg)).await
        }
        #[cfg(feature = "source-parquet")]
        "parquet" => {
            let cfg =
                decode::<faucet_source_parquet::ParquetSourceConfig>("source", "parquet", config)?;
            boxed_source(faucet_source_parquet::ParquetSource::new(cfg)).await
        }
        #[cfg(feature = "source-delta")]
        "delta" => {
            let cfg = decode::<faucet_source_delta::DeltaSourceConfig>("source", "delta", config)?;
            boxed_source(faucet_source_delta::DeltaSource::new(cfg)).await
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
        #[cfg(feature = "source-iceberg")]
        "iceberg" => {
            let cfg =
                decode::<faucet_source_iceberg::IcebergSourceConfig>("source", "iceberg", config)?;
            boxed_source(faucet_source_iceberg::IcebergSource::new(cfg)).await
        }
        #[cfg(feature = "source-dynamodb")]
        "dynamodb" => {
            let cfg = decode::<faucet_source_dynamodb::DynamoDbSourceConfig>(
                "source", "dynamodb", config,
            )?;
            boxed_source(faucet_source_dynamodb::DynamoDbSource::new(cfg)).await
        }
        #[cfg(feature = "source-oracle")]
        "oracle" => {
            let cfg =
                decode::<faucet_source_oracle::OracleSourceConfig>("source", "oracle", config)?;
            boxed_source(faucet_source_oracle::OracleSource::new(cfg)).await
        }
        #[cfg(feature = "source-oracle-cdc")]
        "oracle-cdc" => {
            let cfg = decode::<faucet_source_oracle_cdc::OracleCdcSourceConfig>(
                "source",
                "oracle-cdc",
                config,
            )?;
            boxed_source(faucet_source_oracle_cdc::OracleCdcSource::new(cfg)).await
        }
        #[cfg(feature = "source-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_source_gcs::GcsSourceConfig>("source", "gcs", config)?;
            boxed_source(faucet_source_gcs::GcsSource::new(cfg)).await
        }
        #[cfg(feature = "source-bigquery")]
        "bigquery" => {
            let cfg = decode::<faucet_source_bigquery::BigQuerySourceConfig>(
                "source", "bigquery", config,
            )?;
            let mut s = Box::pin(faucet_source_bigquery::BigQuerySource::new(cfg)).await?;
            if let Some(rp) = retry_policy {
                s = s.with_retry_policy(rp.clone());
            }
            Ok(Box::new(s))
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
            boxed_source(faucet_source_mssql_cdc::MssqlCdcSource::new(cfg)).await
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
            boxed_source(faucet_source_pubsub::PubsubSource::new(cfg)).await
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
            boxed_source(faucet_source_azure_blob::AzureBlobSource::new(cfg)).await
        }
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// Build a [`Sink`] trait object from a `(kind, config)` pair. When the config
/// carries `auth: { ref: <name> }`, the named provider is resolved from `auth`
/// (the catalog) and injected into the connector.
///
/// `config` is used exactly as given (see [`build_source`]). Builds under the
/// default [`BuildPolicy`], which refuses a connector that runs a program on
/// this host ([`sink_runs_program`]); use [`build_sink_with`] to allow one.
pub async fn build_sink(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
) -> RegistryResult<Box<dyn Sink>> {
    build_sink_with(kind, config, auth, &BuildPolicy::default()).await
}

/// [`build_sink`] under an explicit [`BuildPolicy`].
pub async fn build_sink_with(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
    policy: &BuildPolicy,
) -> RegistryResult<Box<dyn Sink>> {
    if !policy.allows_subprocess() && sink_runs_program(kind) {
        return Err(RegistryError::SubprocessRefused {
            role: "sink",
            kind: kind.to_owned(),
        });
    }
    // Boxed for the same reason as `build_source`.
    Box::pin(build_sink_inner(kind, config, auth)).await
}

/// Whether a source of `kind` runs a program on this host: its config schema
/// declares a field annotated `x-faucet-resource: command` (the Singer tap
/// bridge, or a plugin that declares one). `false` for an unknown kind.
pub fn source_runs_program(kind: &str) -> bool {
    source_schema(kind).is_ok_and(|s| {
        faucet_core::resolve::declares(&s, faucet_core::resolve::ResourceKind::Command)
    })
}

/// [`source_runs_program`] for sinks (the Singer target bridge).
pub fn sink_runs_program(kind: &str) -> bool {
    sink_schema(kind).is_ok_and(|s| {
        faucet_core::resolve::declares(&s, faucet_core::resolve::ResourceKind::Command)
    })
}

async fn build_sink_inner(
    kind: &str,
    config: Value,
    auth: &AuthCatalog,
) -> RegistryResult<Box<dyn Sink>> {
    let (config, overwrite_run) = take_overwrite_run(kind, config)?;
    // Unused when no overwrite-staging sink is compiled in.
    let _ = &overwrite_run;
    if let Some(entry) = global().sinks.get(kind) {
        reject_unknown_config_keys("sink", kind, kind, &config, &(entry.schema)())?;
        return (entry.factory)(config);
    }
    if let Ok(schema) = sink_schema(kind) {
        reject_unknown_config_keys("sink", kind, kind, &config, &schema)?;
    }
    let auth_ref = auth_catalog::auth_ref(&config);
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => {
            let cfg =
                decode::<faucet_sink_bigquery::BigQuerySinkConfig>("sink", "bigquery", config)?;
            boxed_sink(joined(
                faucet_sink_bigquery::BigQuerySink::new(cfg),
                overwrite_run,
                faucet_sink_bigquery::BigQuerySink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => {
            let cfg = decode::<faucet_sink_iceberg::IcebergSinkConfig>("sink", "iceberg", config)?;
            boxed_sink(faucet_sink_iceberg::IcebergSink::new(cfg)).await
        }
        #[cfg(feature = "sink-delta")]
        "delta" => {
            let cfg = decode::<faucet_sink_delta::DeltaSinkConfig>("sink", "delta", config)?;
            boxed_sink(faucet_sink_delta::DeltaSink::new(cfg)).await
        }
        #[cfg(feature = "sink-postgres")]
        "postgres" => {
            let cfg =
                decode::<faucet_sink_postgres::PostgresSinkConfig>("sink", "postgres", config)?;
            boxed_sink(joined(
                faucet_sink_postgres::PostgresSink::new(cfg),
                overwrite_run,
                faucet_sink_postgres::PostgresSink::with_overwrite_run,
            ))
            .await
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
            boxed_sink(joined(
                faucet_sink_mysql::MysqlSink::new(cfg),
                overwrite_run,
                faucet_sink_mysql::MysqlSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-mssql")]
        "mssql" => {
            let cfg = decode::<faucet_sink_mssql::MssqlSinkConfig>("sink", "mssql", config)?;
            boxed_sink(joined(
                faucet_sink_mssql::MssqlSink::new(cfg),
                overwrite_run,
                faucet_sink_mssql::MssqlSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => {
            let cfg = decode::<faucet_sink_sqlite::SqliteSinkConfig>("sink", "sqlite", config)?;
            boxed_sink(joined(
                faucet_sink_sqlite::SqliteSink::new(cfg),
                overwrite_run,
                faucet_sink_sqlite::SqliteSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => {
            let cfg = decode::<faucet_sink_duckdb::DuckdbSinkConfig>("sink", "duckdb", config)?;
            boxed_sink(faucet_sink_duckdb::DuckdbSink::new(cfg)).await
        }
        #[cfg(feature = "sink-sqs")]
        "sqs" => {
            let cfg = decode::<faucet_sink_sqs::SqsSinkConfig>("sink", "sqs", config)?;
            boxed_sink(faucet_sink_sqs::SqsSink::new(cfg)).await
        }
        #[cfg(feature = "sink-nats")]
        "nats" => {
            let cfg = decode::<faucet_sink_nats::NatsSinkConfig>("sink", "nats", config)?;
            boxed_sink(faucet_sink_nats::NatsSink::new(cfg)).await
        }
        #[cfg(feature = "sink-rabbitmq")]
        "rabbitmq" => {
            let cfg =
                decode::<faucet_sink_rabbitmq::RabbitMqSinkConfig>("sink", "rabbitmq", config)?;
            boxed_sink(faucet_sink_rabbitmq::RabbitMqSink::new(cfg)).await
        }
        #[cfg(feature = "sink-sftp")]
        "sftp" => {
            let cfg = decode::<faucet_sink_sftp::SftpSinkConfig>("sink", "sftp", config)?;
            let mut s = faucet_sink_sftp::SftpSink::new(cfg)?;
            if let Some(token) = &overwrite_run {
                s = s.with_overwrite_run(token)?;
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-singer")]
        "singer" => {
            let cfg = decode::<faucet_sink_singer::SingerSinkConfig>("sink", "singer", config)?;
            let env = serde_json::to_value(&cfg.env).unwrap_or_default();
            for secret in faucet_sink_singer::secret_like_values(&cfg.target_config)
                .into_iter()
                .chain(faucet_sink_singer::secret_like_values(&env))
            {
                crate::secrets::report(&secret);
            }
            Ok(Box::new(faucet_sink_singer::SingerSink::new(cfg)?))
        }
        #[cfg(feature = "sink-s3")]
        "s3" => {
            let cfg = decode::<faucet_sink_s3::S3SinkConfig>("sink", "s3", config)?;
            boxed_sink(joined(
                faucet_sink_s3::S3Sink::new(cfg),
                overwrite_run,
                faucet_sink_s3::S3Sink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => {
            let cfg = decode::<faucet_sink_mongodb::MongoSinkConfig>("sink", "mongodb", config)?;
            boxed_sink(joined(
                faucet_sink_mongodb::MongoSink::new(cfg),
                overwrite_run,
                faucet_sink_mongodb::MongoSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-redis")]
        "redis" => {
            let cfg = decode::<faucet_sink_redis::RedisSinkConfig>("sink", "redis", config)?;
            boxed_sink(faucet_sink_redis::RedisSink::new(cfg)).await
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
            if let Some(token) = &overwrite_run {
                s = s.with_overwrite_run(token)?;
            }
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-kafka")]
        "kafka" => {
            let cfg = decode::<faucet_sink_kafka::KafkaSinkConfig>("sink", "kafka", config)?;
            boxed_sink(faucet_sink_kafka::KafkaSink::new(cfg)).await
        }
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => {
            let cfg = decode::<faucet_sink_kinesis::KinesisSinkConfig>("sink", "kinesis", config)?;
            boxed_sink(faucet_sink_kinesis::KinesisSink::new(cfg)).await
        }
        #[cfg(feature = "sink-spanner")]
        "spanner" => {
            let cfg = decode::<faucet_sink_spanner::SpannerSinkConfig>("sink", "spanner", config)?;
            boxed_sink(faucet_sink_spanner::SpannerSink::new(cfg)).await
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
            boxed_sink(faucet_sink_parquet::ParquetSink::new(cfg)).await
        }
        #[cfg(feature = "sink-file")]
        "file" => {
            let cfg = decode::<faucet_sink_file::FileSinkConfig>("sink", "file", config)?;
            let mut s = faucet_sink_file::FileSink::new(cfg)?;
            if let Some(token) = &overwrite_run {
                s = s.with_overwrite_run(token)?;
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-gcs")]
        "gcs" => {
            let cfg = decode::<faucet_sink_gcs::GcsSinkConfig>("sink", "gcs", config)?;
            boxed_sink(joined(
                faucet_sink_gcs::GcsSink::new(cfg),
                overwrite_run,
                faucet_sink_gcs::GcsSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-redshift")]
        "redshift" => {
            let cfg =
                decode::<faucet_sink_redshift::RedshiftSinkConfig>("sink", "redshift", config)?;
            boxed_sink(faucet_sink_redshift::RedshiftSink::new(cfg)).await
        }
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => {
            let cfg = decode::<faucet_sink_pubsub::PubsubSinkConfig>("sink", "pubsub", config)?;
            boxed_sink(faucet_sink_pubsub::PubsubSink::new(cfg)).await
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
            boxed_sink(joined(
                faucet_sink_azure_blob::AzureBlobSink::new(cfg),
                overwrite_run,
                faucet_sink_azure_blob::AzureBlobSink::with_overwrite_run,
            ))
            .await
        }
        #[cfg(feature = "sink-dynamodb")]
        "dynamodb" => {
            let cfg =
                decode::<faucet_sink_dynamodb::DynamoDbSinkConfig>("sink", "dynamodb", config)?;
            boxed_sink(faucet_sink_dynamodb::DynamoDbSink::new(cfg)).await
        }
        #[cfg(feature = "sink-databricks")]
        "databricks" => {
            let cfg = decode::<faucet_sink_databricks::DatabricksSinkConfig>(
                "sink",
                "databricks",
                config,
            )?;
            let mut s = faucet_sink_databricks::DatabricksSink::new(cfg)?;
            if let Some(token) = &overwrite_run {
                s = s.with_overwrite_run(token)?;
            }
            if let Some(name) = &auth_ref {
                s = s.with_auth_provider(auth_catalog::resolve(auth, name)?);
            }
            Ok(Box::new(s))
        }
        #[cfg(feature = "sink-oracle")]
        "oracle" => {
            let cfg = decode::<faucet_sink_oracle::OracleSinkConfig>("sink", "oracle", config)?;
            boxed_sink(joined(
                faucet_sink_oracle::OracleSink::new(cfg),
                overwrite_run,
                faucet_sink_oracle::OracleSink::with_overwrite_run,
            ))
            .await
        }
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// The bookmark shape version a source kind stores (#736) — the offline
/// answer `faucet state show`, `faucet migrate --state` and `faucet doctor`
/// use, matching the built source's `Source::state_schema`. A kind that
/// never changed its shape is at `0`.
pub fn source_state_schema(kind: &str) -> u32 {
    match kind {
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => faucet_source_mongodb_cdc::STATE_SCHEMA,
        _ => 0,
    }
}

/// Migrate a bookmark a source kind stored at shape version `from` to
/// [`source_state_schema`] offline (#736), with the same code the built
/// source's `Source::migrate_state` runs.
pub fn migrate_source_state(kind: &str, from: u32, data: Value) -> Result<Value, FaucetError> {
    match kind {
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => faucet_source_mongodb_cdc::migrate_state(from, data),
        _ if from == source_state_schema(kind) => Ok(data),
        _ => Err(FaucetError::State(format!(
            "{kind} has no migration from bookmark schema {from} to {}",
            source_state_schema(kind)
        ))),
    }
}

/// The owner and shape version a row's bookmark is stored under (#736): its
/// source kind, or `topology` for a graph whose sink nodes cannot be
/// attributed to one source.
pub fn state_codec_for(source: Option<(&str, &Value)>) -> faucet_core::state_version::StateCodec {
    match source {
        Some((kind, config)) => {
            if let Some(entry) = global().sources.get(kind)
                && let Ok(built) = (entry.factory)(config.clone())
            {
                return faucet_core::state_version::StateCodec::for_source(built.as_ref(), false);
            }
            faucet_core::state_version::StateCodec {
                owner: kind.to_string(),
                schema: source_state_schema(kind),
                legacy: false,
            }
        }
        None => faucet_core::state_version::StateCodec {
            owner: "topology".into(),
            schema: 0,
            legacy: false,
        },
    }
}

/// The destination path of a file sink config that replaces its output every
/// time an invocation writes to it (#752), outside the grouped
/// `write_mode: overwrite` lifecycle: `jsonl` / `csv` with `append: false`
/// (the default), `parquet` writing one fixed `*.parquet` file (no rollover),
/// and `file` in `if_exists: replace` (the default; formerly
/// `mode: overwrite`) without `write_mode: overwrite`. Run once per parent
/// record at one path, each invocation wipes the previous one's rows. `None`
/// otherwise.
pub fn sink_truncating_path<'a>(kind: &str, cfg: &'a Value) -> Option<&'a str> {
    let flag = |k: &str| cfg.get(k).and_then(Value::as_bool).unwrap_or(false);
    let text = |v: Option<&'a Value>| v.and_then(Value::as_str);
    match kind {
        "jsonl" | "csv" if !flag("append") => text(cfg.get("path")),
        "parquet" => {
            let dest = cfg.get("destination")?;
            let path = text(dest.get("path"))?;
            let rolls = ["max_rows_per_file", "max_bytes_per_file"]
                .iter()
                .any(|k| cfg.get(*k).is_some_and(|v| !v.is_null()));
            (dest.get("type").and_then(Value::as_str) == Some("local_path")
                && path.ends_with(".parquet")
                && !rolls)
                .then_some(path)
        }
        "file" => {
            let if_exists = cfg
                .get("if_exists")
                .or_else(|| cfg.get("mode"))
                .and_then(Value::as_str)
                .unwrap_or("replace");
            let staged = cfg.get("write_mode").and_then(Value::as_str) == Some("overwrite");
            if matches!(if_exists, "replace" | "overwrite") && !staged {
                text(cfg.get("path"))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The fixed object a remote file-writer sink (`s3`, `gcs`, `azure-blob`,
/// `sftp`) replaces every run: a `path` (or sftp `file_name`) in
/// `if_exists: replace` (the default). Each run republishes that object, so
/// only the last run's rows survive. `None` for a sink naming its objects
/// uniquely per run, or one that appends / refuses an existing object.
pub fn sink_replaced_object(kind: &str, cfg: &Value) -> Option<String> {
    if !matches!(kind, "s3" | "gcs" | "azure-blob" | "sftp") {
        return None;
    }
    let if_exists = cfg
        .get("if_exists")
        .or_else(|| cfg.get("mode"))
        .and_then(Value::as_str)
        .unwrap_or("replace");
    if !matches!(if_exists, "replace" | "overwrite") {
        return None;
    }
    sink_shared_destination(kind, cfg)
}

/// Where a sink on the shared file writer puts a fixed set of file names, as
/// one comparable string (`file:<path>`, `s3://bucket/key`, `gs://…`,
/// `az://account/container/key`, `sftp://host:port/dir/name`). Two writers
/// with the same destination overwrite each other's files and prune the parts
/// the other wrote, so rows must not share one, and a row that runs once per
/// parent record needs a per-invocation token in it. `None` when the sink
/// names every file uniquely per run (a remote sink without `path` /
/// `file_name`), or for any other kind.
pub fn sink_shared_destination(kind: &str, cfg: &Value) -> Option<String> {
    let text = |k: &str| cfg.get(k).and_then(Value::as_str);
    let prefix = || text("prefix").unwrap_or("");
    match kind {
        "file" => text("path").map(|p| format!("file:{p}")),
        "jsonl" | "csv" | "parquet" => sink_truncating_path(kind, cfg).map(|p| format!("file:{p}")),
        "s3" => Some(format!(
            "s3://{}/{}{}",
            text("bucket")?,
            prefix(),
            text("path")?
        )),
        "gcs" => Some(format!(
            "gs://{}/{}{}",
            text("bucket")?,
            prefix(),
            text("path")?
        )),
        "azure-blob" => Some(format!(
            "az://{}/{}/{}{}",
            text("account").unwrap_or(""),
            text("container")?,
            prefix(),
            text("path")?
        )),
        "sftp" => Some(format!(
            "sftp://{}:{}/{}/{}",
            text("host")?,
            cfg.get("port").and_then(Value::as_u64).unwrap_or(22),
            text("path")?.trim_end_matches('/'),
            text("file_name")?
        )),
        _ => None,
    }
}

/// A sink-config patch that gives one row of a fan-out its own destination,
/// for a sink [`sink_shared_destination`] reports a fixed destination for:
/// `tag` (a path-safe name such as a row id) goes into the file name before
/// its first extension (`out/orders.jsonl` → `out/orders.<tag>.jsonl`), or
/// becomes a subdirectory of a directory destination (`out/` →
/// `out/<tag>/`). The patch sets the same field the guard reads (`path`,
/// `destination.path` for `parquet`, `file_name` for `sftp`). `None` when
/// the sink has no shared destination.
pub fn sink_destination_patch(kind: &str, cfg: &Value, tag: &str) -> Option<Value> {
    sink_shared_destination(kind, cfg)?;
    let tagged = |name: &str| -> String {
        if let Some(dir) = name.strip_suffix('/') {
            return format!("{dir}/{tag}/");
        }
        let (dir, file) = match name.rfind('/') {
            Some(i) => name.split_at(i + 1),
            None => ("", name),
        };
        match file.find('.') {
            Some(i) if i > 0 => format!("{dir}{}.{tag}{}", &file[..i], &file[i..]),
            _ => format!("{dir}{file}.{tag}"),
        }
    };
    let text = |k: &str| cfg.get(k).and_then(Value::as_str);
    match kind {
        "parquet" => {
            let path = cfg.get("destination")?.get("path")?.as_str()?;
            let mut destination = cfg.get("destination")?.clone();
            destination["path"] = Value::String(tagged(path));
            Some(serde_json::json!({ "destination": destination }))
        }
        "sftp" => Some(serde_json::json!({ "file_name": tagged(text("file_name")?) })),
        _ => Some(serde_json::json!({ "path": tagged(text("path")?) })),
    }
}

/// Config-level mirror of `Source::consumes_destructively` for the built-in
/// queue sources: reading them acks, deletes or settles messages as the
/// pipeline moves past each page (`sqs`, `pubsub`, `rabbitmq`, and `nats` in
/// JetStream mode). Lets graph validation refuse them without building a
/// connector (#789 MSG-14).
pub fn source_kind_consumes_destructively(kind: &str, config: &Value) -> bool {
    match kind {
        "sqs" | "pubsub" | "rabbitmq" => true,
        "nats" => config.get("jetstream_stream").is_some_and(|v| !v.is_null()),
        _ => false,
    }
}

/// Whether a source configured this way resumes from a stored bookmark, so a
/// run with `state:` reads only what changed since the last one: an
/// incremental `replication_method`, a SQL source's `replication: { type:
/// incremental }`, the file source's `incremental:` block,
/// an incremental Iceberg / DynamoDB-streams read, a change-stream / log
/// source, or a Singer tap (handed its stored STATE). A full refresh (`write_mode: overwrite`) from such a source would
/// replace the destination with only the delta (#789 CLI-20).
pub fn source_resumes_from_bookmark(kind: &str, config: &Value) -> bool {
    let incremental_method = match config.get("replication_method") {
        Some(Value::String(s)) => s.eq_ignore_ascii_case("incremental"),
        Some(Value::Object(o)) => o
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.eq_ignore_ascii_case("incremental")),
        _ => false,
    };
    let mode = config.get("mode").and_then(Value::as_str);
    incremental_method
        || sql_incremental(config)
        || (kind == "file" && config.get("incremental").is_some_and(|v| !v.is_null()))
        || (kind == "iceberg" && mode == Some("incremental"))
        || (kind == "dynamodb" && mode == Some("streams"))
        || kind.ends_with("-cdc")
        || matches!(kind, "kafka" | "kinesis" | "singer")
}

/// Whether a source reads only what arrived since its last run whatever the
/// config: a queue it consumes from, a live subscription, or a listener that
/// collects what is sent to it during the run (`webhook`, `websocket`). Its
/// read is never a complete fetch (#862 M-022).
pub fn source_reads_a_delta(kind: &str, config: &Value) -> bool {
    source_kind_consumes_destructively(kind, config)
        || matches!(kind, "nats" | "webhook" | "websocket")
}

/// Whether a SQL query source reads with `replication: { type: incremental }`.
pub fn sql_incremental(config: &Value) -> bool {
    config
        .get("replication")
        .and_then(|r| r.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|t| t.eq_ignore_ascii_case("incremental"))
}

/// The record fields an exactly-once source reads to tell whether a replayed
/// record was already delivered (`Source::record_covered`).
pub fn source_position_fields(kind: &str) -> &'static [&'static str] {
    match kind {
        "kafka" => &["topic", "partition", "offset"],
        "postgres-cdc" | "mysql-cdc" => &["lsn"],
        "mssql-cdc" => &["lsn", "schema", "table"],
        "mongodb-cdc" => &["resume_token"],
        "oracle-cdc" => &["commit_scn", "xid"],
        _ => &[],
    }
}

/// The static descriptor of a source kind (RFC 0013, #876): a registered
/// custom connector's, else the built-in's. `None` for an unknown kind, or a
/// custom one registered without a descriptor.
pub fn source_descriptor(kind: &str) -> Option<ConnectorDescriptor> {
    match global().sources.get(kind) {
        Some(entry) => entry.descriptor,
        None => builtin_source_descriptor(kind),
    }
}

/// The static descriptor of a sink kind. See [`source_descriptor`].
pub fn sink_descriptor(kind: &str) -> Option<ConnectorDescriptor> {
    match global().sinks.get(kind) {
        Some(entry) => entry.descriptor,
        None => builtin_sink_descriptor(kind),
    }
}

/// A config value as a descriptor reads it: an absent config is an empty
/// object, never the `Null` that asks for the kind-level view.
fn config_object(config: &Value) -> std::borrow::Cow<'_, Value> {
    if config.is_null() {
        std::borrow::Cow::Owned(Value::Object(serde_json::Map::new()))
    } else {
        std::borrow::Cow::Borrowed(config)
    }
}

/// Whether `kind` names a sink connector: compiled in (built-in or plugin), or
/// a first-party one this binary was built without.
pub fn is_known_sink_kind(kind: &str) -> bool {
    sink_kinds().contains(&kind) || uncompiled_builtin(kind, "sink")
}

/// Whether `kind` is a first-party connector this build does not compile in.
fn uncompiled_builtin(kind: &str, role: &str) -> bool {
    let known = if role == "source" {
        FIRST_PARTY_SOURCES
    } else {
        FIRST_PARTY_SINKS
    };
    known.contains(&kind)
}

/// Every first-party source kind, compiled in or not. A test in `faucet-cli`
/// keeps it equal to the verified sources of the connector index
/// (`cli/connectors/registry.json`).
pub const FIRST_PARTY_SOURCES: &[&str] = &[
    "azure-blob",
    "bigquery",
    "clickhouse",
    "csv",
    "databricks",
    "delta",
    "duckdb",
    "dynamodb",
    "elasticsearch",
    "file",
    "gcs",
    "graphql",
    "grpc",
    "iceberg",
    "kafka",
    "kinesis",
    "mongodb",
    "mongodb-cdc",
    "mssql",
    "mssql-cdc",
    "mysql",
    "mysql-cdc",
    "nats",
    "oracle",
    "oracle-cdc",
    "parquet",
    "postgres",
    "postgres-cdc",
    "pubsub",
    "rabbitmq",
    "redis",
    "redshift",
    "rest",
    "s3",
    "sftp",
    "singer",
    "snowflake",
    "spanner",
    "sqlite",
    "sqs",
    "webhook",
    "websocket",
    "xml",
];

/// Every first-party sink kind, compiled in or not. See
/// [`FIRST_PARTY_SOURCES`].
pub const FIRST_PARTY_SINKS: &[&str] = &[
    "azure-blob",
    "bigquery",
    "clickhouse",
    "csv",
    "databricks",
    "delta",
    "duckdb",
    "dynamodb",
    "elasticsearch",
    "file",
    "gcs",
    "http",
    "iceberg",
    "jsonl",
    "kafka",
    "kinesis",
    "mongodb",
    "mssql",
    "mysql",
    "nats",
    "oracle",
    "parquet",
    "postgres",
    "pubsub",
    "rabbitmq",
    "redis",
    "redshift",
    "s3",
    "sftp",
    "singer",
    "snowflake",
    "spanner",
    "sqlite",
    "sqs",
    "stdout",
];

/// What a first-party connector this binary does not compile in is assumed to
/// support: everything. Its descriptor is not linked in, so a load-time gate
/// cannot check it; building it fails with "unknown kind" instead, exactly as
/// before it was declared.
fn uncompiled_capabilities(config: &Value) -> Capabilities {
    use faucet_core::WriteMode;
    Capabilities::builder()
        .write_modes([
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .keyed_dedup(faucet_core::capabilities::config::dedups_by_key(config))
        .cleanup(true)
        .rollback(true)
        .schema_evolution(true)
        .scoped_overwrite(true)
        .direct_overwrite(true)
        .staged_load(true)
        .exactly_once(true)
        .discover(true)
        .shardable(true)
        .range_read(true)
        .range_digest(true)
        .reports_lag(true)
        .build()
}

/// What a source of `kind` configured with `config` can do, read without
/// building it — what every load-time gate consults. A kind without a
/// descriptor declares nothing (a first-party one this binary was built
/// without is not refused).
pub fn source_capabilities(kind: &str, config: &Value) -> Capabilities {
    let config = config_object(config);
    match source_descriptor(kind) {
        Some(d) => d.capabilities(&config),
        None if !source_exists(kind) && uncompiled_builtin(kind, "source") => {
            uncompiled_capabilities(&config)
        }
        None => Capabilities::plain_source(),
    }
}

/// What a sink of `kind` configured with `config` can do, read without
/// building it. A kind without a descriptor is append-only (a first-party
/// one this binary was built without is not refused).
pub fn sink_capabilities(kind: &str, config: &Value) -> Capabilities {
    let config = config_object(config);
    match sink_descriptor(kind) {
        Some(d) => d.capabilities(&config),
        None if !sink_exists(kind) && uncompiled_builtin(kind, "sink") => {
            uncompiled_capabilities(&config)
        }
        None => Capabilities::append_only_sink(),
    }
}

/// Every capability some config of a source `kind` enables — the kind-level
/// view listings show ([`ConnectorDescriptor::kind_capabilities`]).
pub fn source_kind_capabilities(kind: &str) -> Capabilities {
    source_descriptor(kind).map_or_else(Capabilities::plain_source, |d| d.kind_capabilities())
}

/// [`source_kind_capabilities`] for sinks.
pub fn sink_kind_capabilities(kind: &str) -> Capabilities {
    sink_descriptor(kind).map_or_else(Capabilities::append_only_sink, |d| d.kind_capabilities())
}

/// The compiled source kinds whose kind-level capabilities satisfy `pred`,
/// sorted — the "supported: …" list in a gate's error message.
pub fn source_kinds_where(pred: impl Fn(&Capabilities) -> bool) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = source_kinds()
        .into_iter()
        .filter(|k| pred(&source_kind_capabilities(k)))
        .collect();
    v.sort_unstable();
    v
}

/// [`source_kinds_where`] for sinks.
pub fn sink_kinds_where(pred: impl Fn(&Capabilities) -> bool) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = sink_kinds()
        .into_iter()
        .filter(|k| pred(&sink_kind_capabilities(k)))
        .collect();
    v.sort_unstable();
    v
}

fn builtin_source_descriptor(kind: &str) -> Option<ConnectorDescriptor> {
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => Some(faucet_source_rest::DESCRIPTOR),
        #[cfg(feature = "source-graphql")]
        "graphql" => Some(faucet_source_graphql::DESCRIPTOR),
        #[cfg(feature = "source-xml")]
        "xml" => Some(faucet_source_xml::DESCRIPTOR),
        #[cfg(feature = "source-grpc")]
        "grpc" => Some(faucet_source_grpc::DESCRIPTOR),
        #[cfg(feature = "source-postgres")]
        "postgres" => Some(faucet_source_postgres::DESCRIPTOR),
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => Some(faucet_source_postgres_cdc::DESCRIPTOR),
        #[cfg(feature = "source-mysql")]
        "mysql" => Some(faucet_source_mysql::DESCRIPTOR),
        #[cfg(feature = "source-mssql")]
        "mssql" => Some(faucet_source_mssql::DESCRIPTOR),
        #[cfg(feature = "source-sqlite")]
        "sqlite" => Some(faucet_source_sqlite::DESCRIPTOR),
        #[cfg(feature = "source-duckdb")]
        "duckdb" => Some(faucet_source_duckdb::DESCRIPTOR),
        #[cfg(feature = "source-sqs")]
        "sqs" => Some(faucet_source_sqs::DESCRIPTOR),
        #[cfg(feature = "source-nats")]
        "nats" => Some(faucet_source_nats::DESCRIPTOR),
        #[cfg(feature = "source-rabbitmq")]
        "rabbitmq" => Some(faucet_source_rabbitmq::DESCRIPTOR),
        #[cfg(feature = "source-sftp")]
        "sftp" => Some(faucet_source_sftp::DESCRIPTOR),
        #[cfg(feature = "source-file")]
        "file" => Some(faucet_source_file::DESCRIPTOR),
        #[cfg(feature = "source-s3")]
        "s3" => Some(faucet_source_s3::DESCRIPTOR),
        #[cfg(feature = "source-mongodb")]
        "mongodb" => Some(faucet_source_mongodb::DESCRIPTOR),
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => Some(faucet_source_mongodb_cdc::DESCRIPTOR),
        #[cfg(feature = "source-mysql-cdc")]
        "mysql-cdc" => Some(faucet_source_mysql_cdc::DESCRIPTOR),
        #[cfg(feature = "source-redis")]
        "redis" => Some(faucet_source_redis::DESCRIPTOR),
        #[cfg(feature = "source-webhook")]
        "webhook" => Some(faucet_source_webhook::DESCRIPTOR),
        #[cfg(feature = "source-websocket")]
        "websocket" => Some(faucet_source_websocket::DESCRIPTOR),
        #[cfg(feature = "source-csv")]
        "csv" => Some(faucet_source_csv::DESCRIPTOR),
        #[cfg(feature = "source-singer")]
        "singer" => Some(faucet_source_singer::DESCRIPTOR),
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => Some(faucet_source_elasticsearch::DESCRIPTOR),
        #[cfg(feature = "source-kafka")]
        "kafka" => Some(faucet_source_kafka::DESCRIPTOR),
        #[cfg(feature = "source-kinesis")]
        "kinesis" => Some(faucet_source_kinesis::DESCRIPTOR),
        #[cfg(feature = "source-spanner")]
        "spanner" => Some(faucet_source_spanner::DESCRIPTOR),
        #[cfg(feature = "source-parquet")]
        "parquet" => Some(faucet_source_parquet::DESCRIPTOR),
        #[cfg(feature = "source-delta")]
        "delta" => Some(faucet_source_delta::DESCRIPTOR),
        #[cfg(feature = "source-databricks")]
        "databricks" => Some(faucet_source_databricks::DESCRIPTOR),
        #[cfg(feature = "source-iceberg")]
        "iceberg" => Some(faucet_source_iceberg::DESCRIPTOR),
        #[cfg(feature = "source-dynamodb")]
        "dynamodb" => Some(faucet_source_dynamodb::DESCRIPTOR),
        #[cfg(feature = "source-oracle")]
        "oracle" => Some(faucet_source_oracle::DESCRIPTOR),
        #[cfg(feature = "source-oracle-cdc")]
        "oracle-cdc" => Some(faucet_source_oracle_cdc::DESCRIPTOR),
        #[cfg(feature = "source-gcs")]
        "gcs" => Some(faucet_source_gcs::DESCRIPTOR),
        #[cfg(feature = "source-bigquery")]
        "bigquery" => Some(faucet_source_bigquery::DESCRIPTOR),
        #[cfg(feature = "source-snowflake")]
        "snowflake" => Some(faucet_source_snowflake::DESCRIPTOR),
        #[cfg(feature = "source-mssql-cdc")]
        "mssql-cdc" => Some(faucet_source_mssql_cdc::DESCRIPTOR),
        #[cfg(feature = "source-redshift")]
        "redshift" => Some(faucet_source_redshift::DESCRIPTOR),
        #[cfg(feature = "source-pubsub")]
        "pubsub" => Some(faucet_source_pubsub::DESCRIPTOR),
        #[cfg(feature = "source-clickhouse")]
        "clickhouse" => Some(faucet_source_clickhouse::DESCRIPTOR),
        #[cfg(feature = "source-azure-blob")]
        "azure-blob" => Some(faucet_source_azure_blob::DESCRIPTOR),
        _ => None,
    }
}

fn builtin_sink_descriptor(kind: &str) -> Option<ConnectorDescriptor> {
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => Some(faucet_sink_bigquery::DESCRIPTOR),
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => Some(faucet_sink_iceberg::DESCRIPTOR),
        #[cfg(feature = "sink-delta")]
        "delta" => Some(faucet_sink_delta::DESCRIPTOR),
        #[cfg(feature = "sink-postgres")]
        "postgres" => Some(faucet_sink_postgres::DESCRIPTOR),
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => Some(faucet_sink_jsonl::DESCRIPTOR),
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => Some(faucet_sink_snowflake::DESCRIPTOR),
        #[cfg(feature = "sink-mysql")]
        "mysql" => Some(faucet_sink_mysql::DESCRIPTOR),
        #[cfg(feature = "sink-mssql")]
        "mssql" => Some(faucet_sink_mssql::DESCRIPTOR),
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => Some(faucet_sink_sqlite::DESCRIPTOR),
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => Some(faucet_sink_duckdb::DESCRIPTOR),
        #[cfg(feature = "sink-sqs")]
        "sqs" => Some(faucet_sink_sqs::DESCRIPTOR),
        #[cfg(feature = "sink-nats")]
        "nats" => Some(faucet_sink_nats::DESCRIPTOR),
        #[cfg(feature = "sink-rabbitmq")]
        "rabbitmq" => Some(faucet_sink_rabbitmq::DESCRIPTOR),
        #[cfg(feature = "sink-sftp")]
        "sftp" => Some(faucet_sink_sftp::DESCRIPTOR),
        #[cfg(feature = "sink-singer")]
        "singer" => Some(faucet_sink_singer::DESCRIPTOR),
        #[cfg(feature = "sink-s3")]
        "s3" => Some(faucet_sink_s3::DESCRIPTOR),
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => Some(faucet_sink_mongodb::DESCRIPTOR),
        #[cfg(feature = "sink-redis")]
        "redis" => Some(faucet_sink_redis::DESCRIPTOR),
        #[cfg(feature = "sink-csv")]
        "csv" => Some(faucet_sink_csv::DESCRIPTOR),
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => Some(faucet_sink_elasticsearch::DESCRIPTOR),
        #[cfg(feature = "sink-kafka")]
        "kafka" => Some(faucet_sink_kafka::DESCRIPTOR),
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => Some(faucet_sink_kinesis::DESCRIPTOR),
        #[cfg(feature = "sink-spanner")]
        "spanner" => Some(faucet_sink_spanner::DESCRIPTOR),
        #[cfg(feature = "sink-http")]
        "http" => Some(faucet_sink_http::DESCRIPTOR),
        #[cfg(feature = "sink-stdout")]
        "stdout" => Some(faucet_sink_stdout::DESCRIPTOR),
        #[cfg(feature = "sink-parquet")]
        "parquet" => Some(faucet_sink_parquet::DESCRIPTOR),
        #[cfg(feature = "sink-file")]
        "file" => Some(faucet_sink_file::DESCRIPTOR),
        #[cfg(feature = "sink-gcs")]
        "gcs" => Some(faucet_sink_gcs::DESCRIPTOR),
        #[cfg(feature = "sink-redshift")]
        "redshift" => Some(faucet_sink_redshift::DESCRIPTOR),
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => Some(faucet_sink_pubsub::DESCRIPTOR),
        #[cfg(feature = "sink-clickhouse")]
        "clickhouse" => Some(faucet_sink_clickhouse::DESCRIPTOR),
        #[cfg(feature = "sink-azure-blob")]
        "azure-blob" => Some(faucet_sink_azure_blob::DESCRIPTOR),
        #[cfg(feature = "sink-dynamodb")]
        "dynamodb" => Some(faucet_sink_dynamodb::DESCRIPTOR),
        #[cfg(feature = "sink-databricks")]
        "databricks" => Some(faucet_sink_databricks::DESCRIPTOR),
        #[cfg(feature = "sink-oracle")]
        "oracle" => Some(faucet_sink_oracle::DESCRIPTOR),
        _ => None,
    }
}

/// Keys the executor itself injects into a connector config — hidden from the
/// schema, so the unknown-key gate must not refuse them (#789 CLI-72).
const CLI_INJECTED_KEYS: &[&str] = &[
    "_overwrite_staging",
    "_activate_version",
    faucet_core::idempotency::OVERWRITE_RUN_KEY,
];

/// Reject connector-config keys the connector does not declare (#654 H9).
///
/// `deny_unknown_fields` closes this at the serde layer, but serde refuses it
/// on any struct carrying a `#[serde(flatten)]` — which is the house pattern
/// for shared connection / `WriteSpec` blocks, so 25 of 66 connector configs
/// cannot use the attribute. Without a check, `verify_checksums` (one letter
/// off `verify_checksum`) silently runs with integrity checking **off**, and a
/// misspelled `batch_size` silently takes the default: the worst failure shape,
/// a green run doing the wrong thing.
///
/// The connector's own JSON Schema is the key list, so it stays correct by
/// construction — flattened members are inlined into `properties` by schemars,
/// and a third-party plugin gets the same treatment from its `config_schema`.
/// Skipped when the schema declares a genuine catch-all (`additionalProperties`
/// set to anything but `false`), which is how a config with a keyless
/// `flatten`-map says "any key is mine".
fn reject_unknown_config_keys(
    role: &'static str,
    kind: &str,
    name: &str,
    config: &Value,
    schema: &Value,
) -> RegistryResult<()> {
    let Some((unknown, hint)) = unknown_config_keys(config, schema) else {
        return Ok(());
    };
    Err(RegistryError::InvalidConnectorConfig {
        kind: if role == "source" { "source" } else { "sink" },
        name: name.to_owned(),
        message: format!(
            "unknown {role} `{kind}` config key(s): {unknown}{hint}. A key the connector does \
             not declare is silently ignored, so an integrity or batching knob would read as \
             set while doing nothing — run `faucet schema {role} {kind}` for the full list."
        ),
        path: None,
    })
}

/// Keys of `config` its JSON Schema does not declare, rendered as a
/// backticked list plus a `did you mean` hint; `None` when every key is known
/// or the schema declares a catch-all.
pub fn unknown_config_keys(config: &Value, schema: &Value) -> Option<(String, String)> {
    let given = config.as_object()?;
    let mut known = BTreeSet::new();
    if !collect_schema_keys(schema, schema, &mut known, 0) || known.is_empty() {
        return None;
    }
    let unknown: Vec<&str> = given
        .keys()
        .map(String::as_str)
        .filter(|k| !known.contains(*k) && !CLI_INJECTED_KEYS.contains(k))
        .collect();
    if unknown.is_empty() {
        return None;
    }
    let suggestions: Vec<String> = unknown
        .iter()
        .filter_map(|u| {
            nearest_key(u, known.iter()).map(|k| format!("`{u}` — did you mean `{k}`?"))
        })
        .collect();
    let hint = if suggestions.is_empty() {
        let declared: Vec<String> = known.iter().map(|k| format!("`{k}`")).collect();
        format!(" (declared: {})", declared.join(", "))
    } else {
        format!(" ({})", suggestions.join("; "))
    };
    let list = unknown
        .iter()
        .map(|k| format!("`{k}`"))
        .collect::<Vec<_>>()
        .join(", ");
    Some((list, hint))
}

/// Collect every top-level key `schema` can accept into `out`.
///
/// Returns `false` when the schema declares a catch-all (`additionalProperties`
/// as anything but `false`) or recurses too deep — the caller then skips the
/// check entirely rather than rejecting a config the connector would accept.
///
/// Subschema branches (`allOf` / `anyOf` / `oneOf`, and `$ref` into `$defs`)
/// are **unioned**, not intersected: a `#[serde(flatten)]` of an adjacently-
/// tagged enum renders as a `oneOf` of variant objects, and each variant's
/// keys are legitimate at the top level. Unioning keeps this a typo check, not
/// a validator — picking the wrong variant is serde's job to report.
fn collect_schema_keys(
    schema: &Value,
    root: &Value,
    out: &mut BTreeSet<String>,
    depth: usize,
) -> bool {
    const MAX_DEPTH: usize = 8;
    if depth > MAX_DEPTH {
        return false;
    }
    let Some(obj) = schema.as_object() else {
        return false;
    };
    match obj.get("additionalProperties") {
        None | Some(Value::Bool(false)) => {}
        Some(_) => return false,
    }
    if let Some(props) = obj.get("properties").and_then(Value::as_object) {
        out.extend(props.keys().cloned());
    }
    // Serde `alias`es are invisible to schemars, so a config declaring one
    // lists it under `x-faucet-aliases` (via `#[schemars(extend(...))]`).
    // Without this the gate would reject a key serde would have accepted —
    // which is exactly how a documented back-compat alias stops working.
    if let Some(aliases) = obj.get("x-faucet-aliases").and_then(Value::as_array) {
        out.extend(aliases.iter().filter_map(Value::as_str).map(str::to_string));
    }
    if let Some(r) = obj.get("$ref").and_then(Value::as_str) {
        let Some(target) = resolve_local_ref(root, r) else {
            return false;
        };
        if !collect_schema_keys(target, root, out, depth + 1) {
            return false;
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = obj.get(key).and_then(Value::as_array) {
            for b in branches {
                if !collect_schema_keys(b, root, out, depth + 1) {
                    return false;
                }
            }
        }
    }
    true
}

/// Resolve a `#/$defs/Name` pointer against the schema root. Only local `$defs`
/// refs are understood; anything else returns `None` (→ check skipped).
fn resolve_local_ref<'a>(root: &'a Value, r: &str) -> Option<&'a Value> {
    let name = r.strip_prefix("#/$defs/")?;
    root.get("$defs")?.get(name)
}

/// The declared key closest to `given`, when it is close enough to be a likely
/// typo (edit distance ≤ 2, or ≤ 1/3 of the key's length for longer names).
fn nearest_key<'a, I: Iterator<Item = &'a String>>(given: &str, known: I) -> Option<&'a str> {
    let budget = (given.len() / 3).max(2);
    known
        .map(|k| (edit_distance(given, k), k.as_str()))
        .filter(|(d, _)| *d <= budget)
        .min_by_key(|(d, k)| (*d, k.len()))
        .map(|(_, k)| k)
}

/// Levenshtein distance, two-row DP. Byte-wise: config keys are ASCII
/// identifiers, and a multi-byte key just yields a slightly larger distance
/// (it never panics or mis-slices, since we never index into the strings).
fn edit_distance(a: &str, b: &str) -> usize {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, &ac) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, &bc) in b.iter().enumerate() {
            let cost = usize::from(ac != bc);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Connector config keys that mean "how many concurrent connections/fetches",
/// in the order they are preferred when a run-level override is applied (#610).
///
/// Ordered rather than a per-kind table so a third-party connector that names
/// its knob one of these gets the override for free, and so the choice is
/// deterministic for a connector that declares more than one.
const CONCURRENCY_KNOBS: [&str; 5] = [
    "max_connections",
    "request_concurrency",
    "partition_concurrency",
    "shard_concurrency",
    "concurrency",
];

/// Apply a run-level concurrency override to one connector's config (#610).
///
/// Multi-tenant reality: the same template drives a customer with beefy read
/// replicas and one with a small instance, and the pool size is otherwise
/// baked into the config. This rewrites whichever knob the connector actually
/// **declares in its own JSON Schema** — so it is a no-op, not a config error,
/// for a connector that has no such knob (schemaless file sinks, stdout), and
/// it can never introduce a key that H9's unknown-key gate would then reject.
///
/// Returns the key it set, for logging and tests.
pub fn apply_concurrency_override(
    schema: &Value,
    config: &mut Value,
    n: usize,
) -> Option<&'static str> {
    let declared = schema.get("properties")?.as_object()?;
    let knob = CONCURRENCY_KNOBS
        .into_iter()
        .find(|k| declared.contains_key(*k))?;
    config.as_object_mut()?.insert(
        knob.to_string(),
        Value::Number(serde_json::Number::from(n as u64)),
    );
    Some(knob)
}

/// Apply [`apply_concurrency_override`] to a **source** config, resolving the
/// schema by kind. No-op when the kind is unknown to this build.
pub fn override_source_concurrency(
    kind: &str,
    config: &mut Value,
    n: usize,
) -> Option<&'static str> {
    let schema = source_schema(kind).ok()?;
    apply_concurrency_override(&schema, config, n)
}

/// Apply [`apply_concurrency_override`] to a **sink** config.
pub fn override_sink_concurrency(kind: &str, config: &mut Value, n: usize) -> Option<&'static str> {
    let schema = sink_schema(kind).ok()?;
    apply_concurrency_override(&schema, config, n)
}

fn check<T: DeserializeOwned>(kind: &'static str, name: &str, config: Value) -> RegistryResult<()> {
    decode::<T>(kind, name, config).map(|_| ())
}

/// Deserialize, then let the config check its own invariants.
///
/// Separate from [`check`] because `validate()` is an **inherent** method on
/// each config type, not a trait one, so it cannot be reached generically — the
/// caller passes it as a closure. Generic over the error so the few configs
/// returning `Result<(), String>` work alongside the majority returning
/// `Result<(), FaucetError>`.
///
/// This distinction is load-bearing rather than cosmetic. Deserialization alone
/// does **not** catch the case that motivated #609: `MssqlConnectionConfig` is
/// `#[serde(flatten)]`, so nesting it under a `connection:` key deserializes
/// cleanly into a config carrying no connection string at all — only
/// `validate()` notices. Most connector configs also do not set
/// `deny_unknown_fields`, so a typo'd field name is invisible to serde too
/// (tracked as the config-strictness debt in #654).
fn check_with<T, E, F>(
    kind: &'static str,
    name: &str,
    config: Value,
    validate: F,
) -> RegistryResult<()>
where
    T: DeserializeOwned,
    E: std::fmt::Display,
    F: Fn(&T) -> Result<(), E>,
{
    let cfg = decode::<T>(kind, name, config)?;
    validate(&cfg).map_err(|e| RegistryError::InvalidConnectorConfig {
        kind,
        name: name.to_owned(),
        message: e.to_string(),
        path: None,
    })
}

/// The BigQuery sink's construction-time refusals, so `faucet validate`
/// reports them instead of the first run.
#[cfg(feature = "sink-bigquery")]
fn bigquery_sink_checks(
    c: &faucet_sink_bigquery::BigQuerySinkConfig,
) -> Result<(), faucet_core::FaucetError> {
    #[cfg(feature = "arrow")]
    if let Some(load) = &c.bulk_load {
        load.validate()?;
    }
    let _ = c;
    Ok(())
}

/// Validate a source's `config` **without building it**: deserialize the raw
/// `Value` into the connector's typed config and discard the result.
///
/// This exists because a structurally wrong `config` used to survive
/// `faucet validate` and `faucet template register` and only fail on the first
/// triggered run (#609) — the config stayed an opaque `Value` until
/// [`build_source`] ran at execution time. A template could be registered,
/// launched, and only then discovered to be broken.
///
/// Deserialization alone catches the whole reported class: wrong nesting under
/// a `#[serde(flatten)]` block, an unknown field under `deny_unknown_fields`, a
/// wrong scalar type, a missing required field, a typo'd name. It needs no
/// credentials, opens no connections and builds no pools, so it is safe to run
/// at register/validate time — live reachability stays `faucet doctor`'s job.
///
/// A kind whose connector is **not compiled into this build** cannot be
/// checked. That returns [`RegistryError::Config`] naming the feature rather than
/// passing silently, so a slim build never reports a config valid that it
/// simply could not read.
pub fn validate_source_config(kind: &str, name: &str, config: Value) -> RegistryResult<()> {
    // A plugin-registered connector is validated by building it — the factory
    // is the only thing that knows its config shape.
    if let Some(entry) = global().sources.get(kind) {
        reject_unknown_config_keys("source", kind, name, &config, &(entry.schema)())?;
        return (entry.factory)(config).map(|_| ());
    }
    if let Ok(schema) = source_schema(kind) {
        reject_unknown_config_keys("source", kind, name, &config, &schema)?;
    }
    match kind {
        #[cfg(feature = "source-rest")]
        "rest" => {
            check_with::<faucet_source_rest::RestStreamConfig, _, _>("rest", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-graphql")]
        "graphql" => check_with::<faucet_source_graphql::GraphqlStreamConfig, _, _>(
            "graphql",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-xml")]
        "xml" => check_with::<faucet_source_xml::XmlStreamConfig, _, _>("xml", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "source-grpc")]
        "grpc" => check::<faucet_source_grpc::GrpcStreamConfig>("grpc", name, config),
        #[cfg(feature = "source-postgres")]
        "postgres" => check_with::<faucet_source_postgres::PostgresSourceConfig, _, _>(
            "postgres",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-postgres-cdc")]
        "postgres-cdc" => check_with::<faucet_source_postgres_cdc::PostgresCdcSourceConfig, _, _>(
            "postgres-cdc",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-mysql")]
        "mysql" => {
            check_with::<faucet_source_mysql::MysqlSourceConfig, _, _>("mysql", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-mssql")]
        "mssql" => {
            check_with::<faucet_source_mssql::MssqlSourceConfig, _, _>("mssql", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-sqlite")]
        "sqlite" => check::<faucet_source_sqlite::SqliteSourceConfig>("sqlite", name, config),
        #[cfg(feature = "source-duckdb")]
        "duckdb" => check_with::<faucet_source_duckdb::DuckdbSourceConfig, _, _>(
            "duckdb",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-sqs")]
        "sqs" => check_with::<faucet_source_sqs::SqsSourceConfig, _, _>("sqs", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "source-nats")]
        "nats" => {
            check_with::<faucet_source_nats::NatsSourceConfig, _, _>("nats", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-rabbitmq")]
        "rabbitmq" => check_with::<faucet_source_rabbitmq::RabbitMqSourceConfig, _, _>(
            "rabbitmq",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-sftp")]
        "sftp" => check::<faucet_source_sftp::SftpSourceConfig>("sftp", name, config),
        #[cfg(feature = "source-file")]
        "file" => {
            check_with::<faucet_source_file::FileSourceConfig, _, _>("file", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-s3")]
        "s3" => check::<faucet_source_s3::S3SourceConfig>("s3", name, config),
        #[cfg(feature = "source-mongodb")]
        "mongodb" => check::<faucet_source_mongodb::MongoSourceConfig>("mongodb", name, config),
        #[cfg(feature = "source-mongodb-cdc")]
        "mongodb-cdc" => check_with::<faucet_source_mongodb_cdc::MongoCdcSourceConfig, _, _>(
            "mongodb-cdc",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-mysql-cdc")]
        "mysql-cdc" => check_with::<faucet_source_mysql_cdc::MysqlCdcSourceConfig, _, _>(
            "mysql-cdc",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-redis")]
        "redis" => check::<faucet_source_redis::RedisSourceConfig>("redis", name, config),
        #[cfg(feature = "source-webhook")]
        "webhook" => check_with::<faucet_source_webhook::WebhookSourceConfig, _, _>(
            "webhook",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-websocket")]
        "websocket" => check_with::<faucet_source_websocket::WebsocketSourceConfig, _, _>(
            "websocket",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-csv")]
        "csv" => check_with::<faucet_source_csv::CsvSourceConfig, _, _>("csv", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "source-singer")]
        "singer" => check::<faucet_source_singer::SingerSourceConfig>("singer", name, config),
        #[cfg(feature = "source-elasticsearch")]
        "elasticsearch" => {
            check_with::<faucet_source_elasticsearch::ElasticsearchSourceConfig, _, _>(
                "elasticsearch",
                name,
                config,
                |c| c.validate(),
            )
        }
        #[cfg(feature = "source-kafka")]
        "kafka" => {
            check_with::<faucet_source_kafka::KafkaSourceConfig, _, _>("kafka", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-kinesis")]
        "kinesis" => check_with::<faucet_source_kinesis::KinesisSourceConfig, _, _>(
            "kinesis",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-spanner")]
        "spanner" => check_with::<faucet_source_spanner::SpannerSourceConfig, _, _>(
            "spanner",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-parquet")]
        "parquet" => check::<faucet_source_parquet::ParquetSourceConfig>("parquet", name, config),
        #[cfg(feature = "source-delta")]
        "delta" => {
            check_with::<faucet_source_delta::DeltaSourceConfig, _, _>("delta", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "source-databricks")]
        "databricks" => check_with::<faucet_source_databricks::DatabricksSourceConfig, _, _>(
            "databricks",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-iceberg")]
        "iceberg" => check_with::<faucet_source_iceberg::IcebergSourceConfig, _, _>(
            "iceberg",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-dynamodb")]
        "dynamodb" => check_with::<faucet_source_dynamodb::DynamoDbSourceConfig, _, _>(
            "dynamodb",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-oracle")]
        "oracle" => check_with::<faucet_source_oracle::OracleSourceConfig, _, _>(
            "oracle",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-oracle-cdc")]
        "oracle-cdc" => check_with::<faucet_source_oracle_cdc::OracleCdcSourceConfig, _, _>(
            "oracle-cdc",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-gcs")]
        "gcs" => check_with::<faucet_source_gcs::GcsSourceConfig, _, _>("gcs", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "source-bigquery")]
        "bigquery" => {
            check::<faucet_source_bigquery::BigQuerySourceConfig>("bigquery", name, config)
        }
        #[cfg(feature = "source-snowflake")]
        "snowflake" => {
            check::<faucet_source_snowflake::SnowflakeSourceConfig>("snowflake", name, config)
        }
        #[cfg(feature = "source-mssql-cdc")]
        "mssql-cdc" => check_with::<faucet_source_mssql_cdc::MssqlCdcSourceConfig, _, _>(
            "mssql-cdc",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-redshift")]
        "redshift" => check_with::<faucet_source_redshift::RedshiftSourceConfig, _, _>(
            "redshift",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-pubsub")]
        "pubsub" => check_with::<faucet_source_pubsub::PubsubSourceConfig, _, _>(
            "pubsub",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-clickhouse")]
        "clickhouse" => check_with::<faucet_source_clickhouse::ClickHouseSourceConfig, _, _>(
            "clickhouse",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "source-azure-blob")]
        "azure-blob" => check_with::<faucet_source_azure_blob::AzureBlobSourceConfig, _, _>(
            "azure-blob",
            name,
            config,
            |c| c.validate(),
        ),
        other => Err(unknown(other, "source", source_kinds())),
    }
}

/// What a failed batch write leaves behind for this sink kind + config (#737),
/// read offline from the typed config — the same answer the built sink's
/// `Sink::batch_atomicity` gives. `None` for an unknown kind or a config that
/// does not deserialize (the config check reports that). A plugin sink is
/// built through its factory and asked.
pub fn sink_batch_atomicity(kind: &str, config: &Value) -> Option<faucet_core::BatchAtomicity> {
    if let Some(entry) = global().sinks.get(kind) {
        return (entry.factory)(config.clone())
            .ok()
            .map(|s| s.batch_atomicity());
    }
    #[allow(dead_code)]
    fn atomicity_of<T: serde::de::DeserializeOwned>(
        config: &Value,
        f: impl Fn(&T) -> faucet_core::BatchAtomicity,
    ) -> Option<faucet_core::BatchAtomicity> {
        serde_json::from_value::<T>(config.clone())
            .ok()
            .map(|c| f(&c))
    }
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => atomicity_of::<faucet_sink_bigquery::BigQuerySinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => {
            atomicity_of::<faucet_sink_iceberg::IcebergSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-delta")]
        "delta" => {
            atomicity_of::<faucet_sink_delta::DeltaSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-postgres")]
        "postgres" => atomicity_of::<faucet_sink_postgres::PostgresSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => {
            atomicity_of::<faucet_sink_jsonl::JsonlSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => atomicity_of::<faucet_sink_snowflake::SnowflakeSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-mysql")]
        "mysql" => {
            atomicity_of::<faucet_sink_mysql::MysqlSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-mssql")]
        "mssql" => {
            atomicity_of::<faucet_sink_mssql::MssqlSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => {
            atomicity_of::<faucet_sink_sqlite::SqliteSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => {
            atomicity_of::<faucet_sink_duckdb::DuckdbSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-sqs")]
        "sqs" => atomicity_of::<faucet_sink_sqs::SqsSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-nats")]
        "nats" => atomicity_of::<faucet_sink_nats::NatsSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-rabbitmq")]
        "rabbitmq" => atomicity_of::<faucet_sink_rabbitmq::RabbitMqSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-sftp")]
        "sftp" => atomicity_of::<faucet_sink_sftp::SftpSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-singer")]
        "singer" => {
            atomicity_of::<faucet_sink_singer::SingerSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-s3")]
        "s3" => atomicity_of::<faucet_sink_s3::S3SinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => {
            atomicity_of::<faucet_sink_mongodb::MongoSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-redis")]
        "redis" => {
            atomicity_of::<faucet_sink_redis::RedisSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-csv")]
        "csv" => atomicity_of::<faucet_sink_csv::CsvSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => {
            atomicity_of::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>(config, |c| {
                c.batch_atomicity()
            })
        }
        #[cfg(feature = "sink-kafka")]
        "kafka" => {
            atomicity_of::<faucet_sink_kafka::KafkaSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => {
            atomicity_of::<faucet_sink_kinesis::KinesisSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-spanner")]
        "spanner" => {
            atomicity_of::<faucet_sink_spanner::SpannerSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-http")]
        "http" => atomicity_of::<faucet_sink_http::HttpSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-stdout")]
        "stdout" => {
            atomicity_of::<faucet_sink_stdout::StdoutSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-parquet")]
        "parquet" => {
            atomicity_of::<faucet_sink_parquet::ParquetSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-file")]
        "file" => atomicity_of::<faucet_sink_file::FileSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-gcs")]
        "gcs" => atomicity_of::<faucet_sink_gcs::GcsSinkConfig>(config, |c| c.batch_atomicity()),
        #[cfg(feature = "sink-redshift")]
        "redshift" => atomicity_of::<faucet_sink_redshift::RedshiftSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => {
            atomicity_of::<faucet_sink_pubsub::PubsubSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-clickhouse")]
        "clickhouse" => atomicity_of::<faucet_sink_clickhouse::ClickHouseSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-azure-blob")]
        "azure-blob" => atomicity_of::<faucet_sink_azure_blob::AzureBlobSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-oracle")]
        "oracle" => {
            atomicity_of::<faucet_sink_oracle::OracleSinkConfig>(config, |c| c.batch_atomicity())
        }
        #[cfg(feature = "sink-dynamodb")]
        "dynamodb" => atomicity_of::<faucet_sink_dynamodb::DynamoDbSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        #[cfg(feature = "sink-databricks")]
        "databricks" => atomicity_of::<faucet_sink_databricks::DatabricksSinkConfig>(config, |c| {
            c.batch_atomicity()
        }),
        _ => None,
    }
}

/// Validate a sink's `config` **without building it**: deserialize the raw
/// `Value` into the connector's typed config and discard the result.
///
/// This exists because a structurally wrong `config` used to survive
/// `faucet validate` and `faucet template register` and only fail on the first
/// triggered run (#609) — the config stayed an opaque `Value` until
/// [`build_sink`] ran at execution time. A template could be registered,
/// launched, and only then discovered to be broken.
///
/// Deserialization alone catches the whole reported class: wrong nesting under
/// a `#[serde(flatten)]` block, an unknown field under `deny_unknown_fields`, a
/// wrong scalar type, a missing required field, a typo'd name. It needs no
/// credentials, opens no connections and builds no pools, so it is safe to run
/// at register/validate time — live reachability stays `faucet doctor`'s job.
///
/// A kind whose connector is **not compiled into this build** cannot be
/// checked. That returns [`RegistryError::Config`] naming the feature rather than
/// passing silently, so a slim build never reports a config valid that it
/// simply could not read.
pub fn validate_sink_config(kind: &str, name: &str, config: Value) -> RegistryResult<()> {
    // A plugin-registered connector is validated by building it — the factory
    // is the only thing that knows its config shape.
    if let Some(entry) = global().sinks.get(kind) {
        reject_unknown_config_keys("sink", kind, name, &config, &(entry.schema)())?;
        return (entry.factory)(config).map(|_| ());
    }
    if let Ok(schema) = sink_schema(kind) {
        reject_unknown_config_keys("sink", kind, name, &config, &schema)?;
    }
    match kind {
        #[cfg(feature = "sink-bigquery")]
        "bigquery" => check_with::<faucet_sink_bigquery::BigQuerySinkConfig, _, _>(
            "bigquery",
            name,
            config,
            bigquery_sink_checks,
        ),
        #[cfg(feature = "sink-iceberg")]
        "iceberg" => check_with::<faucet_sink_iceberg::IcebergSinkConfig, _, _>(
            "iceberg",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-delta")]
        "delta" => {
            check_with::<faucet_sink_delta::DeltaSinkConfig, _, _>("delta", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "sink-postgres")]
        "postgres" => check::<faucet_sink_postgres::PostgresSinkConfig>("postgres", name, config),
        #[cfg(feature = "sink-jsonl")]
        "jsonl" => check::<faucet_sink_jsonl::JsonlSinkConfig>("jsonl", name, config),
        #[cfg(feature = "sink-snowflake")]
        "snowflake" => {
            check::<faucet_sink_snowflake::SnowflakeSinkConfig>("snowflake", name, config)
        }
        #[cfg(feature = "sink-mysql")]
        "mysql" => check::<faucet_sink_mysql::MysqlSinkConfig>("mysql", name, config),
        #[cfg(feature = "sink-mssql")]
        "mssql" => {
            check_with::<faucet_sink_mssql::MssqlSinkConfig, _, _>("mssql", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "sink-sqlite")]
        "sqlite" => check::<faucet_sink_sqlite::SqliteSinkConfig>("sqlite", name, config),
        #[cfg(feature = "sink-duckdb")]
        "duckdb" => check::<faucet_sink_duckdb::DuckdbSinkConfig>("duckdb", name, config),
        #[cfg(feature = "sink-sqs")]
        "sqs" => check_with::<faucet_sink_sqs::SqsSinkConfig, _, _>("sqs", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "sink-nats")]
        "nats" => check_with::<faucet_sink_nats::NatsSinkConfig, _, _>("nats", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "sink-rabbitmq")]
        "rabbitmq" => check_with::<faucet_sink_rabbitmq::RabbitMqSinkConfig, _, _>(
            "rabbitmq",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-sftp")]
        "sftp" => check_with::<faucet_sink_sftp::SftpSinkConfig, _, _>("sftp", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "sink-singer")]
        "singer" => {
            check_with::<faucet_sink_singer::SingerSinkConfig, _, _>("singer", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "sink-s3")]
        "s3" => {
            check_with::<faucet_sink_s3::S3SinkConfig, _, _>("s3", name, config, |c| c.validate())
        }
        #[cfg(feature = "sink-mongodb")]
        "mongodb" => check::<faucet_sink_mongodb::MongoSinkConfig>("mongodb", name, config),
        #[cfg(feature = "sink-redis")]
        "redis" => check::<faucet_sink_redis::RedisSinkConfig>("redis", name, config),
        #[cfg(feature = "sink-csv")]
        "csv" => check::<faucet_sink_csv::CsvSinkConfig>("csv", name, config),
        #[cfg(feature = "sink-elasticsearch")]
        "elasticsearch" => check::<faucet_sink_elasticsearch::ElasticsearchSinkConfig>(
            "elasticsearch",
            name,
            config,
        ),
        #[cfg(feature = "sink-kafka")]
        "kafka" => {
            check_with::<faucet_sink_kafka::KafkaSinkConfig, _, _>("kafka", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "sink-kinesis")]
        "kinesis" => check_with::<faucet_sink_kinesis::KinesisSinkConfig, _, _>(
            "kinesis",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-spanner")]
        "spanner" => check_with::<faucet_sink_spanner::SpannerSinkConfig, _, _>(
            "spanner",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-http")]
        "http" => check::<faucet_sink_http::HttpSinkConfig>("http", name, config),
        #[cfg(feature = "sink-stdout")]
        "stdout" => check::<faucet_sink_stdout::StdoutSinkConfig>("stdout", name, config),
        #[cfg(feature = "sink-parquet")]
        "parquet" => check_with::<faucet_sink_parquet::ParquetSinkConfig, _, _>(
            "parquet",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-file")]
        "file" => check_with::<faucet_sink_file::FileSinkConfig, _, _>("file", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "sink-gcs")]
        "gcs" => check_with::<faucet_sink_gcs::GcsSinkConfig, _, _>("gcs", name, config, |c| {
            c.validate()
        }),
        #[cfg(feature = "sink-redshift")]
        "redshift" => check_with::<faucet_sink_redshift::RedshiftSinkConfig, _, _>(
            "redshift",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-pubsub")]
        "pubsub" => {
            check_with::<faucet_sink_pubsub::PubsubSinkConfig, _, _>("pubsub", name, config, |c| {
                c.validate()
            })
        }
        #[cfg(feature = "sink-clickhouse")]
        "clickhouse" => check_with::<faucet_sink_clickhouse::ClickHouseSinkConfig, _, _>(
            "clickhouse",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-azure-blob")]
        "azure-blob" => check_with::<faucet_sink_azure_blob::AzureBlobSinkConfig, _, _>(
            "azure-blob",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-dynamodb")]
        "dynamodb" => check_with::<faucet_sink_dynamodb::DynamoDbSinkConfig, _, _>(
            "dynamodb",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-databricks")]
        "databricks" => check_with::<faucet_sink_databricks::DatabricksSinkConfig, _, _>(
            "databricks",
            name,
            config,
            |c| c.validate(),
        ),
        #[cfg(feature = "sink-oracle")]
        "oracle" => {
            check_with::<faucet_sink_oracle::OracleSinkConfig, _, _>("oracle", name, config, |c| {
                c.validate()
            })
        }
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

pub fn source_schema(kind: &str) -> RegistryResult<Value> {
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
        #[cfg(feature = "source-rabbitmq")]
        "rabbitmq" => Ok(schema::<faucet_source_rabbitmq::RabbitMqSourceConfig>()),
        #[cfg(feature = "source-sftp")]
        "sftp" => Ok(schema::<faucet_source_sftp::SftpSourceConfig>()),
        #[cfg(feature = "source-file")]
        "file" => Ok(schema::<faucet_source_file::FileSourceConfig>()),
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
        #[cfg(feature = "source-iceberg")]
        "iceberg" => Ok(schema::<faucet_source_iceberg::IcebergSourceConfig>()),
        #[cfg(feature = "source-dynamodb")]
        "dynamodb" => Ok(schema::<faucet_source_dynamodb::DynamoDbSourceConfig>()),
        #[cfg(feature = "source-oracle")]
        "oracle" => Ok(schema::<faucet_source_oracle::OracleSourceConfig>()),
        #[cfg(feature = "source-oracle-cdc")]
        "oracle-cdc" => Ok(schema::<faucet_source_oracle_cdc::OracleCdcSourceConfig>()),
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
pub fn sink_schema(kind: &str) -> RegistryResult<Value> {
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
        #[cfg(feature = "sink-rabbitmq")]
        "rabbitmq" => Ok(schema::<faucet_sink_rabbitmq::RabbitMqSinkConfig>()),
        #[cfg(feature = "sink-sftp")]
        "sftp" => Ok(schema::<faucet_sink_sftp::SftpSinkConfig>()),
        #[cfg(feature = "sink-singer")]
        "singer" => Ok(schema::<faucet_sink_singer::SingerSinkConfig>()),
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
        #[cfg(feature = "sink-file")]
        "file" => Ok(schema::<faucet_sink_file::FileSinkConfig>()),
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
        #[cfg(feature = "sink-dynamodb")]
        "dynamodb" => Ok(schema::<faucet_sink_dynamodb::DynamoDbSinkConfig>()),
        #[cfg(feature = "sink-databricks")]
        "databricks" => Ok(schema::<faucet_sink_databricks::DatabricksSinkConfig>()),
        #[cfg(feature = "sink-oracle")]
        "oracle" => Ok(schema::<faucet_sink_oracle::OracleSinkConfig>()),
        other => Err(unknown(other, "sink", sink_kinds())),
    }
}

/// One-line summary of every source connector — the compiled-in built-ins plus
/// any third-party connectors registered via [`PluginRegistry`]. Used by
/// `faucet list`.
/// The shared description of a deprecated file kind (`crate::vocabulary`).
#[allow(dead_code)]
fn deprecated(kind: &str) -> &'static str {
    crate::vocabulary::deprecated_kind_description(kind).unwrap_or("Deprecated connector.")
}

pub fn source_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v = builtin_source_descriptions();
    v.extend(global().custom_source_descriptions());
    v
}

/// One-line summary of every compiled-in built-in source connector (no customs).
#[allow(clippy::vec_init_then_push)]
pub fn builtin_source_descriptions() -> Vec<(&'static str, &'static str)> {
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
    #[cfg(feature = "source-rabbitmq")]
    v.push((
        "rabbitmq",
        "RabbitMQ (AMQP 0.9.1) source. Consumes a queue (optional exchange bindings), acks each page only after the sink confirms it (at-least-once), with idle/max-messages termination.",
    ));
    #[cfg(feature = "source-sftp")]
    v.push((
        "sftp",
        "SFTP source. Lists/globs a remote directory and reads its files over SSH, format and compression resolved per file (JSONL, JSON, CSV, Parquet, Avro, …).",
    ));
    #[cfg(feature = "source-file")]
    v.push((
        "file",
        "Local file source. Reads JSONL / JSON / CSV / Excel / XML / Parquet / Avro / ORC from a path, directory, glob or http(s) URL, format and compression resolved per file; incremental by mtime or name.",
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
        "Azure Blob Storage / ADLS Gen2 source — format and compression resolved per object (JSONL, JSON, CSV, Parquet, Avro, …)",
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
    v.push(("csv", deprecated("csv")));
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
    v.push(("parquet", deprecated("parquet")));
    #[cfg(feature = "source-delta")]
    v.push(("delta", "Apache Delta Lake source (local FS or S3/Azure/GCS). Streams active data files with time travel and projection pushdown."));
    #[cfg(feature = "source-databricks")]
    v.push(("databricks", "Databricks SQL query source (Statement Execution API). Streams typed query results with chunk pagination and incremental replication."));
    #[cfg(feature = "source-iceberg")]
    v.push(("iceberg", "Apache Iceberg table source (REST/Glue/SQL/HMS catalogs). Projection and filter pushdown, snapshot time travel, and incremental snapshot reads."));
    #[cfg(feature = "source-dynamodb")]
    v.push(("dynamodb", "Amazon DynamoDB source — parallel Scan, Query, or DynamoDB Streams change capture with resumable shard bookmarks."));
    #[cfg(feature = "source-oracle")]
    v.push(("oracle", "Oracle Database query source (ODPI-C session pool). Streams rows with incremental replication, PK-range sharding and discovery; needs Oracle Instant Client at runtime."));
    #[cfg(feature = "source-oracle-cdc")]
    v.push(("oracle-cdc", "Oracle CDC source via LogMiner. Committed-transaction change events with SCN bookmarks (exactly-once capable); needs Oracle Instant Client at runtime."));
    #[cfg(feature = "source-gcs")]
    v.push((
        "gcs",
        "Google Cloud Storage source — format and compression resolved per object (JSONL, JSON, CSV, Parquet, Avro, …)",
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
pub fn builtin_sink_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> = Vec::new();
    #[cfg(feature = "sink-bigquery")]
    v.push((
        "bigquery",
        "Google BigQuery sink — streaming inserts, upsert/delete via MERGE, overwrite, exactly-once",
    ));
    #[cfg(feature = "sink-iceberg")]
    v.push((
        "iceberg",
        "Apache Iceberg sink (append, REST/Glue/SQL/HMS catalogs)",
    ));
    #[cfg(feature = "sink-postgres")]
    v.push(("postgres", "PostgreSQL sink (JSONB or auto-mapped columns)"));
    #[cfg(feature = "sink-jsonl")]
    v.push(("jsonl", deprecated("jsonl")));
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
    #[cfg(feature = "sink-rabbitmq")]
    v.push((
        "rabbitmq",
        "RabbitMQ (AMQP 0.9.1) sink. Publishes to an exchange with a static/per-record routing key, publisher confirms, and mandatory-return rows surfaced for the DLQ.",
    ));
    #[cfg(feature = "sink-sftp")]
    v.push((
        "sftp",
        "SFTP sink. Writes JSONL, JSON, CSV, XML, Excel, Avro or Parquet files over SSH with atomic temp-then-rename uploads.",
    ));
    #[cfg(feature = "sink-singer")]
    v.push((
        "singer",
        "Singer target bridge. Runs a Singer target executable and feeds it SCHEMA/RECORD/STATE messages; bookmarks advance only after the target confirms (echoed STATE or clean exit).",
    ));
    #[cfg(feature = "sink-s3")]
    v.push((
        "s3",
        "AWS S3 sink — JSONL, JSON, CSV, XML, Excel, Avro or Parquet objects",
    ));
    #[cfg(feature = "sink-mongodb")]
    v.push(("mongodb", "MongoDB insert sink"));
    #[cfg(feature = "sink-redis")]
    v.push(("redis", "Redis (streams, lists, key-value) sink"));
    #[cfg(feature = "sink-csv")]
    v.push(("csv", deprecated("csv")));
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
    v.push(("parquet", deprecated("parquet")));
    #[cfg(feature = "sink-file")]
    v.push(("file", "Local file sink. JSONL, JSON, CSV, XML, Excel, Avro or Parquet by extension; rollover, compression, temp-then-rename finalisation, atomic overwrite."));
    #[cfg(feature = "sink-delta")]
    v.push(("delta", "Apache Delta Lake sink (local FS or S3/Azure/GCS). Append-only, schema-inferred table creation, one commit per flush."));
    #[cfg(feature = "sink-gcs")]
    v.push((
        "gcs",
        "Google Cloud Storage sink — JSONL, JSON, CSV, XML, Excel, Avro or Parquet objects",
    ));
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
        "Azure Blob Storage / ADLS Gen2 sink — JSONL, JSON, CSV, XML, Excel, Avro or Parquet blobs",
    ));
    #[cfg(feature = "sink-dynamodb")]
    v.push((
        "dynamodb",
        "Amazon DynamoDB sink — batched writes, upsert/delete by key, conditional writes",
    ));
    #[cfg(feature = "sink-databricks")]
    v.push((
        "databricks",
        "Databricks SQL warehouse sink — append/upsert/delete/overwrite into Delta tables, exactly-once, schema evolution, staged COPY INTO",
    ));
    #[cfg(feature = "sink-oracle")]
    v.push((
        "oracle",
        "Oracle Database sink — array-bound inserts, MERGE upsert/delete, overwrite, exactly-once, schema evolution (needs Oracle Instant Client at runtime)",
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

fn decode<T: DeserializeOwned>(kind: &'static str, name: &str, config: Value) -> RegistryResult<T> {
    serde_path_to_error::deserialize(config).map_err(|e| {
        let path = e.path().to_string();
        RegistryError::InvalidConnectorConfig {
            kind,
            name: name.to_owned(),
            message: scrub_config_error(&e.into_inner().to_string()),
            path: (path != ".").then(|| scrub_config_error(&path)),
        }
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
    const KEY_PREFIXES: [&str; 3] = ["unknown field ", "missing field ", "duplicate field "];
    let chars: Vec<char> = msg.chars().collect();
    let mut out = String::with_capacity(msg.len());
    let mut i = 0;
    // Serde echoes the offending value before `, expected …`; what follows
    // names types and variants, never input.
    let mut in_expected = false;
    while i < chars.len() {
        if !in_expected && chars[i..].starts_with(&[',', ' ', 'e', 'x', 'p', 'e', 'c', 't']) {
            in_expected = true;
        }
        let c = chars[i];
        if !in_expected && c == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                i += if chars[i] == '\\' { 2 } else { 1 };
            }
            i += 1;
            out.push_str("\"<redacted>\"");
            continue;
        }
        if !in_expected && c == '`' {
            let close = chars[i + 1..]
                .iter()
                .position(|&ch| ch == '`')
                .map_or(chars.len(), |p| i + 1 + p);
            if KEY_PREFIXES.iter().any(|p| out.ends_with(p)) {
                out.extend(&chars[i..(close + 1).min(chars.len())]);
            } else {
                out.push_str("`<redacted>`");
            }
            i = close + 1;
            continue;
        }
        out.push(c);
        i += 1;
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

fn unknown(name: &str, kind: &'static str, available: Vec<&'static str>) -> RegistryError {
    RegistryError::UnknownConnector {
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
mod tests;

#[cfg(test)]
mod truncating_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn destination_patch_tags_the_file_name_or_the_directory() {
        let p = |k: &str, v: Value| sink_destination_patch(k, &v, "orders");
        assert_eq!(
            p("jsonl", json!({"path": "out/all.jsonl.gz"})),
            Some(json!({"path": "out/all.orders.jsonl.gz"}))
        );
        assert_eq!(
            p("file", json!({"path": "out"})),
            Some(json!({"path": "out.orders"}))
        );
        assert_eq!(
            p("file", json!({"path": ".hidden"})),
            Some(json!({"path": ".hidden.orders"}))
        );
        assert_eq!(
            p("s3", json!({"bucket": "b", "path": "exports/"})),
            Some(json!({"path": "exports/orders/"}))
        );
        assert_eq!(
            p(
                "sftp",
                json!({"host": "h", "path": "/d", "file_name": "x-{part}.csv"})
            ),
            Some(json!({"file_name": "x-{part}.orders.csv"}))
        );
        assert_eq!(
            p(
                "parquet",
                json!({"destination": {"type": "local_path", "path": "o/a.parquet"}})
            ),
            Some(json!({"destination": {"type": "local_path", "path": "o/a.orders.parquet"}}))
        );
        assert_eq!(p("jsonl", json!({"path": "a.jsonl", "append": true})), None);
        assert_eq!(p("s3", json!({"bucket": "b"})), None);
    }

    #[test]
    fn shared_destination_by_kind() {
        let d = |k: &str, v: Value| sink_shared_destination(k, &v);
        assert_eq!(
            d("file", json!({"path": "o/a.jsonl"})).as_deref(),
            Some("file:o/a.jsonl")
        );
        assert_eq!(
            d("jsonl", json!({"path": "o/a.jsonl"})).as_deref(),
            Some("file:o/a.jsonl")
        );
        assert_eq!(
            d("jsonl", json!({"path": "o/a.jsonl", "append": true})),
            None
        );
        assert_eq!(
            d(
                "s3",
                json!({"bucket": "b", "prefix": "p/", "path": "x-{part}.csv"})
            )
            .as_deref(),
            Some("s3://b/p/x-{part}.csv")
        );
        assert_eq!(d("s3", json!({"bucket": "b", "prefix": "p/"})), None);
        assert_eq!(
            d("gcs", json!({"bucket": "b", "path": "x.csv"})).as_deref(),
            Some("gs://b/x.csv")
        );
        assert_eq!(
            d(
                "azure-blob",
                json!({"account": "a", "container": "c", "path": "x.csv"})
            )
            .as_deref(),
            Some("az://a/c/x.csv")
        );
        assert_eq!(
            d(
                "sftp",
                json!({"host": "h", "path": "/d/", "file_name": "x.csv"})
            )
            .as_deref(),
            Some("sftp://h:22//d/x.csv")
        );
        assert_eq!(
            d("sftp", json!({"host": "h", "port": 2222, "path": "/d"})),
            None
        );
        assert_eq!(d("postgres", json!({"table": "t"})), None);
    }

    #[test]
    fn replaced_object_needs_a_fixed_name_in_replace_mode() {
        let r = |k: &str, v: Value| sink_replaced_object(k, &v);
        assert_eq!(
            r("s3", json!({"bucket": "b", "path": "exports/orders.jsonl"})).as_deref(),
            Some("s3://b/exports/orders.jsonl")
        );
        assert_eq!(
            r(
                "gcs",
                json!({"bucket": "b", "path": "x.csv", "if_exists": "overwrite"})
            )
            .as_deref(),
            Some("gs://b/x.csv")
        );
        assert_eq!(
            r(
                "sftp",
                json!({"host": "h", "path": "/d", "file_name": "x.csv", "mode": "replace"})
            )
            .as_deref(),
            Some("sftp://h:22//d/x.csv")
        );
        assert_eq!(r("s3", json!({"bucket": "b", "prefix": "p/"})), None);
        assert_eq!(
            r(
                "azure-blob",
                json!({"container": "c", "path": "x.csv", "if_exists": "append"})
            ),
            None
        );
        assert_eq!(
            r("s3", json!({"bucket": "b", "path": "x", "mode": "error"})),
            None
        );
        assert_eq!(r("file", json!({"path": "x.jsonl"})), None);
    }

    #[test]
    fn truncating_path_by_kind() {
        let p = |k: &str, v: Value| sink_truncating_path(k, &v).map(str::to_string);
        assert_eq!(
            p("jsonl", json!({"path": "a.jsonl"})).as_deref(),
            Some("a.jsonl")
        );
        assert_eq!(
            p("csv", json!({"path": "a.csv", "append": false})).as_deref(),
            Some("a.csv")
        );
        assert_eq!(p("jsonl", json!({"path": "a.jsonl", "append": true})), None);
        let pq = json!({"destination": {"type": "local_path", "path": "o/a.parquet"}});
        assert_eq!(p("parquet", pq).as_deref(), Some("o/a.parquet"));
        let rolled = json!({
            "destination": {"type": "local_path", "path": "o/a.parquet"},
            "max_rows_per_file": 10
        });
        assert_eq!(p("parquet", rolled), None);
        let dir = json!({"destination": {"type": "local_path", "path": "o/"}});
        assert_eq!(p("parquet", dir), None);
        let s3 = json!({"destination": {"type": "s3", "bucket": "b"}});
        assert_eq!(p("parquet", s3), None);
        assert_eq!(p("parquet", json!({})), None);
        assert_eq!(
            p("file", json!({"path": "a.jsonl"})).as_deref(),
            Some("a.jsonl")
        );
        assert_eq!(
            p("file", json!({"path": "a.jsonl", "mode": "append"})),
            None
        );
        assert_eq!(
            p("file", json!({"path": "a.jsonl", "if_exists": "append"})),
            None
        );
        assert_eq!(
            p("file", json!({"path": "a.jsonl", "if_exists": "error"})),
            None
        );
        for replace in [
            json!({"if_exists": "replace"}),
            json!({"mode": "overwrite"}),
        ] {
            let mut cfg = replace;
            cfg["path"] = json!("b.jsonl");
            assert_eq!(p("file", cfg).as_deref(), Some("b.jsonl"));
        }
        assert_eq!(
            p(
                "file",
                json!({"path": "a.jsonl", "write_mode": "overwrite"})
            ),
            None
        );
        assert_eq!(p("postgres", json!({"path": "x"})), None);
    }
}
