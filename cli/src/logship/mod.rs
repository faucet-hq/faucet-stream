//! Durable log shipping over OTLP (#806).
//!
//! Every captured run-log line is written to a local buffer first — the
//! run-history log store under `faucet serve`, a spool directory under
//! `faucet run` / `faucet schedule` — and a shipper delivers it to an OTLP
//! logs endpoint, advancing a per-run delivery watermark only after the
//! collector acknowledges the batch. Retention follows delivery: delivered
//! lines are kept for `retention_secs`, undelivered ones until they are
//! delivered or the buffer bounds (`buffer_max_age_secs`,
//! `buffer_max_bytes`) drop the oldest, which is counted and reported per run
//! as `partially_dropped`.

pub mod metrics;
pub mod process;
pub mod record;
pub mod spec;

#[cfg(feature = "observability")]
pub mod capture;
#[cfg(feature = "otel")]
pub mod otlp;
#[cfg(feature = "otel")]
pub mod session;
#[cfg(feature = "otel")]
pub mod spool;

pub use record::{
    DeliveryState, LinkVars, LogExportStatus, LogExportView, MAX_LINE_BYTES, ShipLine,
    derive_view, render_link, truncate_line,
};
pub use spec::LogsSpec;

/// Whether `cfg` asks for log shipping (`logs` in `observability.otel.export`).
pub fn session_wanted(cfg: &crate::config::PipelineConfig) -> bool {
    cfg.observability
        .as_ref()
        .and_then(|o| o.otel.as_ref())
        .is_some_and(|o| o.export.contains(&faucet_core::OtelSignal::Logs))
}
