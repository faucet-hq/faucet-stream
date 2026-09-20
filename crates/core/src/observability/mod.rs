//! Pipeline-internal observability: tracing spans and `metrics` counters/
//! histograms wired automatically around every source, sink, transform, and
//! state-store operation. See
//! `docs/superpowers/specs/2026-05-23-observability-otel-prometheus-design.md`.

mod bookmark;
mod cleanup;
#[cfg(feature = "contract")]
mod contract;
pub(crate) mod decorator;
mod drift;
mod install;
mod labels;
#[cfg(feature = "masking")]
mod masking;
mod options;
pub mod otel;
#[cfg(feature = "quality")]
mod quality;
pub mod resilience;
mod roundtrip;
mod state;
mod strip;
mod timer;
mod transform;

pub use bookmark::update_bookmark_lag;
pub use cleanup::{cleanup_deleted, cleanup_run};
#[cfg(feature = "contract")]
pub use contract::instrumented_apply_contract;
pub use decorator::{InstrumentedSink, InstrumentedSource};
pub use drift::schema_drift;
pub use install::{
    InstallError, InstallReport, ObservabilityConfig, PrometheusConfig, TracingConfig,
    install_observability, register_build_info,
};
pub use labels::Labels;
#[cfg(feature = "masking")]
pub use masking::instrumented_apply_masking;
pub use options::RunStreamOptions;
#[cfg(feature = "quality")]
pub use quality::instrumented_apply_quality;
pub use roundtrip::{RoundtripRecorder, RoundtripSide, describe_roundtrip_metrics};
pub use state::InstrumentedStateStore;
pub use strip::strip_type_name;
pub use timer::DurationGuard;
pub use transform::instrumented_apply_stages;
