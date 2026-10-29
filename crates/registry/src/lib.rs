#![cfg_attr(docsrs, feature(doc_cfg))]
//! # faucet-registry
//!
//! Build any faucet connector from its `type:` name and a JSON config — the
//! same name → connector map the `faucet` CLI runs pipelines with — without
//! depending on the CLI.
//!
//! ```no_run
//! # #[cfg(all(feature = "source-file", feature = "sink-jsonl"))]
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use faucet_core::{Pipeline, FileStateStore};
//! use faucet_registry::{auth_catalog, build_sink, build_source};
//! use serde_json::json;
//!
//! let auth = auth_catalog::build_auth_catalog(None)?;
//! let source = build_source("file", json!({ "path": "in.jsonl" }), &auth, None).await?;
//! let sink = build_sink("jsonl", json!({ "path": "out.jsonl" }), &auth).await?;
//! let result = Pipeline::new(source.as_ref(), sink.as_ref())
//!     .with_state_store(std::sync::Arc::new(FileStateStore::new("state")))
//!     .run()
//!     .await?;
//! println!("{} records", result.records_written);
//! # Ok(()) }
//! ```
//!
//! ## Features
//!
//! Every connector is behind its own feature: `source-<kind>` / `sink-<kind>`
//! (`source-postgres`, `sink-jsonl`, …), with `source`, `sink` and `full`
//! aggregates. A connector whose feature is off is reported as unknown. No
//! feature is on by default.
//!
//! ## Configs are used as given
//!
//! [`build_source`] / [`build_sink`] take an **already resolved** config:
//! `${env:…}`, `${file:…}` and secret-manager references are not expanded, and
//! no escape is undone. Resolve references with the trust policy your host
//! needs before calling them.
//!
//! ## Connectors that run programs
//!
//! A connector whose config names a program to run on this host (the Singer
//! bridges, or a plugin declaring an `x-faucet-resource: command` field) is
//! refused by [`build_source`] / [`build_sink`]. Allow it explicitly with
//! [`BuildPolicy::allow_subprocess`] and [`build_source_with`] /
//! [`build_sink_with`].
//!
//! ## Secrets found while building
//!
//! Some connectors surface credentials while they are built (inline service
//! account keys, a Singer target's config). Install a registrar with
//! [`secrets::install`] to be told about them, so they can be scrubbed from
//! your logs.
//!
//! ## Stability
//!
//! Everything documented here follows semver. The `internal` module (hidden
//! from the docs) holds helpers the `faucet` CLI shares; it is outside the
//! stability contract.

pub mod auth_catalog;
mod connectors;
mod error;
mod policy;
pub mod secrets;
mod vocabulary;

pub use auth_catalog::AuthCatalog;
pub use connectors::{
    PluginRegistry, SchemaFn, SinkFactory, SourceFactory, build_sink, build_sink_with,
    build_source, build_source_with, migrate_source_state, plugin_connectors, sink_capabilities,
    sink_descriptions, sink_descriptor, sink_exists, sink_kind_capabilities, sink_kinds,
    sink_kinds_where, sink_runs_program, sink_schema, source_capabilities, source_descriptions,
    source_descriptor, source_exists, source_kind_capabilities, source_kinds, source_kinds_where,
    source_position_fields, source_reads_a_delta, source_resumes_from_bookmark,
    source_runs_program, source_schema, source_state_schema, state_codec_for, undescribed_plugins,
    validate_sink_config, validate_source_config, warn_undescribed_plugins,
};
pub use error::{RegistryError, RegistryResult};
pub use policy::BuildPolicy;

/// Helpers the `faucet` CLI shares with this crate. **Not** part of the
/// stability contract: anything here may change or disappear in a minor
/// release.
#[doc(hidden)]
pub mod internal {
    pub use crate::connectors::{
        FIRST_PARTY_SINKS, FIRST_PARTY_SOURCES, apply_concurrency_override,
        builtin_sink_descriptions, builtin_source_descriptions, is_known_sink_kind,
        override_sink_concurrency, override_source_concurrency, sink_batch_atomicity,
        sink_destination_patch, sink_replaced_object, sink_shared_destination,
        sink_truncating_path, source_kind_consumes_destructively, sql_incremental,
        unknown_config_keys,
    };
    pub use crate::vocabulary::{deprecated_kind_description, file_kind_replacement};
}
