#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-core
//!
//! Shared types, traits, and utilities for the faucet-stream ecosystem.
//!
//! This crate provides the common foundation used by all faucet source and
//! sink connectors:
//!
//! - [`FaucetError`] — unified error type
//! - [`Source`] / [`Sink`] — async traits for data connectors
//! - [`RecordTransform`] — record transformation pipeline
//! - [`ReplicationMethod`] — incremental replication support
//! - [`schema::infer_schema`] — JSON Schema inference from record samples

pub mod adaptive;
pub mod auth;
pub mod check;
pub mod cleanup;
#[cfg(feature = "arrow")]
pub mod columnar;
pub mod config;
#[cfg(feature = "contract")]
pub mod contract;
#[cfg(feature = "transform-cross-join")]
pub mod cross_join;
pub mod discover;
pub mod dlq;
pub mod drift;
#[cfg(feature = "encryption")]
pub mod encryption;
pub mod error;
pub mod idempotency;
pub mod join;
pub mod local_outputs;
#[cfg(feature = "masking")]
pub mod masking;
pub mod metadata;
pub mod native;
pub mod observability;
pub mod pipeline;
#[cfg(feature = "quality")]
pub mod quality;
pub mod redact;
pub mod replication;
pub mod resilience;
pub mod retry;
pub mod schema;
pub mod shard;
pub mod stage;
pub mod staging;
pub mod state;
pub mod tls;
pub mod topology;
pub mod traits;
pub mod transform;
pub mod transforming_source;
#[cfg(feature = "transform-tree-flatten")]
pub mod tree;
pub mod util;
pub mod verify;
pub mod window;
pub mod write_mode;
#[cfg(feature = "transform-zip-columns")]
pub mod zip_columns;

#[cfg(feature = "compression")]
pub mod compression;

pub use adaptive::{
    AdaptiveBatchConfig, AdjustDirection, AdjustReason, Adjustment, AimdController, Observation,
};
pub use auth::{
    AuthProvider, AuthReference, AuthSpec, Credential, CredentialPlacement, RequestAuth,
    SharedAuthProvider,
};
pub use check::{CheckContext, CheckReport, Probe, ProbeStatus};
pub use cleanup::{CleanupMode, CleanupPolicy, DEFAULT_MAX_KEYS, SeenKeys};
#[cfg(feature = "arrow")]
pub use columnar::{
    ColumnarPage, infer_arrow_schema, record_batch_to_values, values_to_record_batch,
    values_to_record_batch_inferred,
};
#[cfg(feature = "transform-cross-join")]
pub use cross_join::{CompiledCrossJoin, CrossJoinSpec, OnEmpty as CrossJoinOnEmpty};
pub use discover::{DatasetDescriptor, columns_to_schema, nullable_type, sql_type_to_json_schema};
pub use dlq::{
    DlqConfig, DlqReason, DlqStats, EnvelopeError, OnBatchError, UnwrappedEnvelope, build_envelope,
    unwrap_envelope,
};
pub use drift::{
    ColumnChange, OnDrift, OnIncompatible, SchemaDiff, SchemaDriftPolicy, SchemaDriftSpec,
    SchemaEvolution, SqlBaseType, adds_null, base_widened, json_schema_base_type,
};
#[cfg(feature = "encryption")]
pub use encryption::{CompiledEncryption, EncryptionAlgorithm, EncryptionSpec};
pub use error::FaucetError;
pub use idempotency::{
    DeliveryGuarantee, DeliveryMode, EffectivelyOnceMechanism, GuaranteeInputs, ReplayGuarantee,
    SinkGuarantee, derive_delivery_guarantee, format_token, format_token_with_bookmark,
    parse_token, parse_token_parts, unwrap_state, wrap_state,
};
pub use join::{
    HashJoin, JoinConfig, JoinMode, JoinStats, KeyNormalize, OnCollision, OnDuplicate, Projection,
};
pub use local_outputs::{LocalOutput, LocalOutputLog, probe_pre_existing};
pub use metadata::{
    CompiledMetadata, MetadataColumn, MetadataColumnsSpec, MetadataContext, MetadataSink,
};
pub use native::{
    CsvDialect, NativeBatch, NativeFormat, NativeLoadCapability, NativeLoadContext, NativePayload,
    NativePlan, NativePlanInputs, NativePrerequisites, plan_native_transfer,
};
#[cfg(feature = "contract")]
pub use observability::instrumented_apply_contract;
#[cfg(feature = "masking")]
pub use observability::instrumented_apply_masking;
#[cfg(feature = "quality")]
pub use observability::instrumented_apply_quality;
pub use observability::otel::{OtelConfig, OtelProtocol, OtelSignal, shutdown_otel};
pub use observability::{
    DurationGuard, InstallError, InstallReport, InstrumentedSink, InstrumentedSource,
    InstrumentedStateStore, Labels, ObservabilityConfig, PrometheusConfig, RunStreamOptions,
    TracingConfig, install_observability, instrumented_apply_stages, register_build_info,
    update_bookmark_lag,
};
pub use pipeline::{
    DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE, Pipeline, PipelineResult, StreamPage, run_stream,
    validate_batch_size,
};
pub use replication::{
    BindFormat, BindTarget, ReplicationBind, ReplicationMethod, format_bookmark, format_instant,
    json_gt, parse_instant,
};
pub use resilience::{
    BackoffKind, CircuitBreaker, CircuitBreakerConfig, PoisonAction, PoisonPolicy,
    ResiliencePolicy, RetryClass, RetryClassSet, RetryMetrics, RetryPolicy, classify,
    execute_with_policy, execute_with_policy_metered,
};
pub use retry::execute_with_retry;
pub use shard::ShardSpec;
#[cfg(feature = "transform-cdc-unwrap")]
pub use stage::CdcUnwrapSpec;
#[cfg(feature = "transform-unpivot")]
pub use stage::{CompiledUnpivot, UnpivotSpec};
#[cfg(feature = "transform-explode")]
pub use stage::{ExplodeSpec, OnMissing};
#[cfg(feature = "transform-filter")]
pub use stage::{FilterOp, FilterSpec};
pub use stage::{TransformStage, apply_stages, compile_stage};
pub use staging::{
    StagedFile, StagingCleanup, StagingCompression, StagingFormat, StagingLocation, StagingScheme,
    StagingSpec, serialize_records,
};
pub use state::{FileStateStore, MemoryStateStore, StateStore};
pub use tls::TlsClientConfig;
pub use topology::{
    Edge, JoinNode, Node, NodeKind, Topology, TopologyBuilder, TopologyOnError, TopologyOptions,
    TopologyResult,
};
pub use traits::{RowOutcome, Sink, Source};
#[cfg(feature = "transform-json-parse")]
pub use transform::JsonParseOnError;
#[cfg(feature = "transform-lookup")]
pub use transform::LookupOnMissing;
pub use transform::RecordTransform;
#[cfg(feature = "transform-value-case")]
pub use transform::ValueCaseMode;
#[cfg(feature = "transform-cast")]
pub use transform::{CastOnError, CastType};
#[cfg(feature = "transform-hash")]
pub use transform::{HashAlgorithm, HashEncoding};
#[cfg(feature = "transform-keys-case")]
pub use transform::{KeyCaseMode, KeyCollision};
pub use transforming_source::TransformingSource;
#[cfg(feature = "transform-tree-flatten")]
pub use tree::{AncestorsSpec, ColumnsSpec, CompiledTreeFlatten, TreeFlattenSpec};
pub use util::redact_uri_credentials;
pub use verify::{IntegrityCheck, LengthCheck, VerifyingReader};
pub use window::{
    WINDOW_PLACEHOLDER, Window, WindowBind, WindowSpec, enumerate_windows, parse_step,
};
pub use write_mode::{
    DeleteMarker, KeyTuple, OverwriteScope, WriteMode, WritePlan, WriteSpec, key_to_doc_id,
    key_to_filter, plan_writes,
};
#[cfg(feature = "transform-zip-columns")]
pub use zip_columns::{CompiledZipColumns, ZipColumnsSpec};

// Re-export dependencies that connector authors need, so they only depend on
// `faucet-core` instead of adding `async-trait` and `serde_json` themselves.
pub use async_stream;
pub use async_trait::async_trait;
pub use futures_core::{self, Stream};
pub use schemars::{self, JsonSchema, schema_for};
pub use serde_json::{self, Value, json};
/// Re-exported so callers of [`Pipeline::with_cancel`](pipeline::Pipeline::with_cancel)
/// / [`RunStreamOptions::with_cancel`] can name the token type without adding
/// `tokio-util` themselves.
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "compression")]
pub use compression::{Compression, CompressionConfig, compress_buf, warn_mismatch};

#[cfg(feature = "quality")]
pub use quality::{
    BatchCheck, CheckTally, CompareOp, CompiledQuality, JsonType, OnFailure, QualityOutcome,
    QualitySpec, QuarantinedRecord, RecordCheck, apply_quality,
};

#[cfg(feature = "contract")]
pub use contract::{
    CompiledContract, ContractFieldType, ContractOutcome, ContractSpec, ContractViolation,
    FieldContract, OnBreach, ViolatingRecord, apply_contract, to_json_schema, to_openlineage_facet,
};

#[cfg(feature = "masking")]
pub use masking::{
    CompiledMasking, Detector, MaskAction, MaskHit, MaskRule, MaskingOutcome, MaskingSpec,
    MatchSpec, apply_masking,
};
