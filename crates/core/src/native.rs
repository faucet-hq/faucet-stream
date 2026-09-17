//! Native byte-passthrough fast-load (#633) — the third capability-negotiated
//! transfer mechanism, alongside the Arrow **columnar** path (RFC 0002 / #375)
//! and **staged bulk load** (#528).
//!
//! When a source can emit its records as raw bytes in a wire format
//! ([`NativeFormat`]) and a sink can bulk-load that exact format directly, and
//! no `Value`-shaped stage sits between them, the pipeline streams the bytes
//! source → sink **without ever materializing [`serde_json::Value`] or an Arrow
//! `RecordBatch`** — the minimum-memory, minimum-CPU path.
//!
//! This is the byte analogue of the columnar path: columnar is for typed
//! columnar sources (parquet/delta) → typed sinks; byte-passthrough is for
//! format-matched wire-byte pairs (Salesforce Bulk CSV → BigQuery CSV load;
//! an S3 `.jsonl` object → BigQuery NDJSON load) with no object-store hop, where
//! the *destination* does the CSV/JSON → typed-column casting.
//!
//! Like the other two mechanisms it is **opt-in and additive**: a source
//! advertises formats via [`Source::native_output_formats`](crate::Source::native_output_formats)
//! and a sink advertises mechanisms via
//! [`Sink::native_load_capabilities`](crate::Sink::native_load_capabilities). The
//! pipeline uses the path only when [`plan_native_transfer`] finds a sink
//! capability whose format the source offers and whose write modes cover the
//! run — after the planner's own unconditional gates (no transforms, no
//! quality/contract/masking pass, no DLQ, at-least-once only) have passed.
//! The "no transformation between" gate is additionally enforced structurally —
//! a [`TransformingSource`](crate::TransformingSource) does not override
//! `native_output_formats`, so any attached transform makes the wrapped source
//! advertise no formats and the fast path falls through to the `Value` path.
//!
//! The core types carry no heavy dependencies (byte payloads are `Vec<u8>` /
//! boxed byte streams, matching [`crate::staging`]), so the negotiation is
//! always compiled — it is a first-class part of the `Source`/`Sink` contract,
//! not a Cargo feature. Individual connector implementations may still gate their
//! `stream_native` / `load_native` bodies behind their own features.

use crate::error::FaucetError;
use crate::idempotency::DeliveryMode;
use crate::write_mode::WriteMode;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;

/// A wire format that can move source → sink as raw bytes, bypassing per-record
/// `serde_json::Value` (and Arrow) materialization. This is the negotiation key:
/// a source advertises the formats it can emit and a sink the formats it can
/// load, and [`plan_native_transfer`] matches them.
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeFormat {
    /// Newline-delimited JSON — one JSON object per line.
    NdJson,
    /// CSV / TSV with the dialect carried on the [`NativeBatch`].
    Csv,
    /// Apache Parquet file bytes.
    Parquet,
    /// Apache Arrow IPC stream bytes.
    ArrowIpc,
}

impl NativeFormat {
    /// A stable lowercase token for logs, metrics labels, and config.
    pub fn as_str(self) -> &'static str {
        match self {
            NativeFormat::NdJson => "ndjson",
            NativeFormat::Csv => "csv",
            NativeFormat::Parquet => "parquet",
            NativeFormat::ArrowIpc => "arrow_ipc",
        }
    }
}

/// CSV specifics carried on a [`NativeBatch`] (ignored for other formats). The
/// sink must be able to honor the source's dialect, or refuse the match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CsvDialect {
    /// Whether the first row is a header row the sink should skip on load.
    pub has_header: bool,
    /// The field delimiter byte (e.g. `b','` or `b'\t'`).
    pub delimiter: u8,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
        }
    }
}

/// The bytes of one native batch — either fully buffered or a byte stream.
///
/// The [`Stream`](NativePayload::Stream) variant is what gives true O(1) memory:
/// the sink consumes it (e.g. into a resumable upload) without ever buffering the
/// whole batch. The [`Bytes`](NativePayload::Bytes) variant is the simple case,
/// bounded by the source's page / batch size.
pub enum NativePayload {
    /// A fully-buffered byte batch.
    Bytes(Vec<u8>),
    /// A streamed byte batch consumed chunk-by-chunk.
    Stream(Pin<Box<dyn Stream<Item = Result<Vec<u8>, FaucetError>> + Send>>),
}

impl std::fmt::Debug for NativePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NativePayload::Bytes(b) => f.debug_tuple("Bytes").field(&b.len()).finish(),
            NativePayload::Stream(_) => f.write_str("Stream(<byte stream>)"),
        }
    }
}

/// One native-format byte batch handed from a source's
/// [`stream_native`](crate::Source::stream_native) to a sink's
/// [`load_native`](crate::Sink::load_native).
///
/// `bookmark` carries the same checkpoint semantics as
/// [`StreamPage`](crate::StreamPage): whenever it is `Some`, the pipeline flushes
/// the sink and persists the bookmark before polling the next batch.
#[derive(Debug)]
pub struct NativeBatch {
    /// The wire format of `payload`.
    pub format: NativeFormat,
    /// The batch bytes.
    pub payload: NativePayload,
    /// CSV dialect (only meaningful when `format == Csv`).
    pub csv: CsvDialect,
    /// Row count if the source knows it (used for metrics; `None` if unknown).
    pub records: Option<u64>,
    /// Checkpoint bookmark, or `None` for a mid-stream batch.
    pub bookmark: Option<Value>,
}

impl NativeBatch {
    /// A fully-buffered batch with no bookmark and no known count.
    pub fn bytes(format: NativeFormat, payload: Vec<u8>) -> Self {
        Self {
            format,
            payload: NativePayload::Bytes(payload),
            csv: CsvDialect::default(),
            records: None,
            bookmark: None,
        }
    }

    /// Set the checkpoint bookmark (builder-style).
    pub fn with_bookmark(mut self, bookmark: Option<Value>) -> Self {
        self.bookmark = bookmark;
        self
    }

    /// Set the known row count (builder-style).
    pub fn with_records(mut self, records: Option<u64>) -> Self {
        self.records = records;
        self
    }

    /// Set the CSV dialect (builder-style).
    pub fn with_csv(mut self, csv: CsvDialect) -> Self {
        self.csv = csv;
        self
    }
}

/// An efficient native-load mechanism a sink offers. A sink returns one of
/// these per mechanism it supports from
/// [`Sink::native_load_capabilities`](crate::Sink::native_load_capabilities).
///
/// A capability declares only what genuinely varies per mechanism: the format
/// and the write modes it can honor. Everything a byte passthrough can never
/// support — transforms, the quality/contract/masking governance passes,
/// per-row DLQ routing, exactly-once delivery — is gated **unconditionally by
/// the pipeline** ([`plan_native_transfer`] rejects those runs outright). No
/// capability can waive those gates: they need `Value` access or a commit-token
/// protocol the byte path structurally does not have, so a declarative opt-out
/// could only ship silently-unenforced governance.
#[derive(Clone, Debug)]
pub struct NativeLoadCapability {
    /// The wire format this mechanism consumes.
    pub format: NativeFormat,
    /// A short, stable label for logs / metrics (e.g. `"bigquery-load-job"`).
    pub mechanism: &'static str,
    /// Write modes this mechanism can honor (typically `Append` and/or
    /// `Overwrite`; `Upsert`/`Delete` need per-row keys and so are never
    /// passthrough-eligible — the pipeline rejects them before matching).
    pub write_modes: &'static [WriteMode],
}

/// Per-call context the pipeline hands each
/// [`load_native`](crate::Sink::load_native) invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeLoadContext {
    /// The effective write mode for the run.
    pub write_mode: WriteMode,
    /// `true` for the first batch of the run — the signal an overwrite sink uses
    /// to truncate-then-append (e.g. BigQuery `WRITE_TRUNCATE` on the first load,
    /// `WRITE_APPEND` thereafter).
    pub first_batch: bool,
}

/// Inputs to the pure [`plan_native_transfer`] negotiation.
#[derive(Clone, Copy, Debug)]
pub struct NativePlanInputs<'a> {
    /// Formats the source can emit, in **preference order** (first = best).
    pub source_formats: &'a [NativeFormat],
    /// Mechanisms the sink offers.
    pub sink_caps: &'a [NativeLoadCapability],
    /// Whether any transform stage is attached to the pipeline.
    pub has_transforms: bool,
    /// Whether any quality / contract / masking / schema-drift pass is active.
    pub has_governance: bool,
    /// The run's delivery mode.
    pub delivery: DeliveryMode,
    /// The run's effective write mode.
    pub write_mode: WriteMode,
    /// Whether a DLQ is configured.
    pub has_dlq: bool,
}

/// The negotiated native transfer: which format to move and which sink mechanism
/// loads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativePlan {
    /// The wire format both sides agreed on.
    pub format: NativeFormat,
    /// The sink mechanism's label.
    pub mechanism: &'static str,
}

/// Decide whether a native byte-passthrough fast path applies, and which.
///
/// Deterministic and pure. First the **pipeline-owned gates** run — these are
/// unconditional and no capability can waive them:
///
/// - transforms or any quality/contract/masking pass (`has_transforms` /
///   `has_governance`): those need per-record `Value` access, which a byte
///   passthrough never produces — running them would silently skip governance;
/// - a configured DLQ (`has_dlq`): a byte batch cannot be split into per-row
///   successes/failures, so the DLQ would be silently inert;
/// - any delivery other than [`DeliveryMode::AtLeastOnce`]: the native runner
///   implements no commit-token protocol, so exactly-once is rejected
///   structurally rather than sold as a capability flag.
///
/// Then it walks the source's formats **in preference order** and returns the
/// first one for which some sink capability matches on format and lists the
/// run's write mode. Source preference order therefore breaks ties when the
/// sink offers several matching formats. Returns `None` when no mechanism
/// qualifies — the caller then falls through to the columnar or `Value` path
/// unchanged.
pub fn plan_native_transfer(inputs: &NativePlanInputs<'_>) -> Option<NativePlan> {
    if inputs.has_transforms || inputs.has_governance || inputs.has_dlq {
        return None;
    }
    if inputs.delivery != DeliveryMode::AtLeastOnce {
        return None;
    }
    for &format in inputs.source_formats {
        if let Some(cap) = inputs
            .sink_caps
            .iter()
            .find(|cap| cap.format == format && cap.write_modes.contains(&inputs.write_mode))
        {
            return Some(NativePlan {
                format: cap.format,
                mechanism: cap.mechanism,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A BigQuery-like capability: NDJSON + CSV, append or overwrite.
    fn bq_caps() -> Vec<NativeLoadCapability> {
        vec![
            NativeLoadCapability {
                format: NativeFormat::NdJson,
                mechanism: "bigquery-load-job",
                write_modes: &[WriteMode::Append, WriteMode::Overwrite],
            },
            NativeLoadCapability {
                format: NativeFormat::Csv,
                mechanism: "bigquery-load-job",
                write_modes: &[WriteMode::Append, WriteMode::Overwrite],
            },
        ]
    }

    fn base<'a>(
        source_formats: &'a [NativeFormat],
        sink_caps: &'a [NativeLoadCapability],
    ) -> NativePlanInputs<'a> {
        NativePlanInputs {
            source_formats,
            sink_caps,
            has_transforms: false,
            has_governance: false,
            delivery: DeliveryMode::AtLeastOnce,
            write_mode: WriteMode::Append,
            has_dlq: false,
        }
    }

    #[test]
    fn matches_when_format_and_prereqs_hold() {
        let caps = bq_caps();
        let src = [NativeFormat::Csv];
        let plan = plan_native_transfer(&base(&src, &caps));
        assert_eq!(
            plan,
            Some(NativePlan {
                format: NativeFormat::Csv,
                mechanism: "bigquery-load-job",
            })
        );
    }

    #[test]
    fn source_preference_order_breaks_ties() {
        let caps = bq_caps();
        // Source prefers NdJson; both offered by the sink → NdJson wins.
        let src = [NativeFormat::NdJson, NativeFormat::Csv];
        assert_eq!(
            plan_native_transfer(&base(&src, &caps)).unwrap().format,
            NativeFormat::NdJson
        );
        // Reverse the preference → Csv wins.
        let src = [NativeFormat::Csv, NativeFormat::NdJson];
        assert_eq!(
            plan_native_transfer(&base(&src, &caps)).unwrap().format,
            NativeFormat::Csv
        );
    }

    #[test]
    fn no_match_when_formats_disjoint() {
        let caps = bq_caps();
        let src = [NativeFormat::Parquet, NativeFormat::ArrowIpc];
        assert_eq!(plan_native_transfer(&base(&src, &caps)), None);
    }

    #[test]
    fn empty_source_or_sink_yields_none() {
        let caps = bq_caps();
        assert_eq!(plan_native_transfer(&base(&[], &caps)), None);
        let src = [NativeFormat::Csv];
        assert_eq!(plan_native_transfer(&base(&src, &[])), None);
    }

    #[test]
    fn transforms_and_governance_gates_block_unconditionally() {
        let caps = bq_caps();
        let src = [NativeFormat::Csv];
        let mut inp = base(&src, &caps);
        inp.has_transforms = true;
        assert_eq!(plan_native_transfer(&inp), None, "transforms must block");
        let mut inp = base(&src, &caps);
        inp.has_governance = true;
        assert_eq!(plan_native_transfer(&inp), None, "governance must block");
    }

    #[test]
    fn governance_and_dlq_gates_are_not_capability_waivable() {
        // Even the most permissive capability shape cannot opt out of the
        // pipeline-owned governance/DLQ gates — those need `Value` access, so
        // a waiver could only ship silently-unenforced governance.
        let caps = vec![NativeLoadCapability {
            format: NativeFormat::Csv,
            mechanism: "tolerant",
            write_modes: &[
                WriteMode::Append,
                WriteMode::Overwrite,
                WriteMode::Upsert,
                WriteMode::Delete,
            ],
        }];
        let src = [NativeFormat::Csv];
        let mut inp = base(&src, &caps);
        inp.has_transforms = true;
        assert_eq!(plan_native_transfer(&inp), None, "transforms must block");
        let mut inp = base(&src, &caps);
        inp.has_governance = true;
        assert_eq!(plan_native_transfer(&inp), None, "governance must block");
        let mut inp = base(&src, &caps);
        inp.has_dlq = true;
        assert_eq!(plan_native_transfer(&inp), None, "DLQ must block");
    }

    #[test]
    fn exactly_once_is_rejected_structurally() {
        // The native runner implements no commit-token protocol, so no
        // capability can declare exactly-once support.
        let caps = bq_caps();
        let src = [NativeFormat::Csv];
        let mut inp = base(&src, &caps);
        inp.delivery = DeliveryMode::ExactlyOnce;
        assert_eq!(plan_native_transfer(&inp), None);
    }

    #[test]
    fn write_mode_gate_blocks_unlisted_modes() {
        let caps = bq_caps();
        let src = [NativeFormat::Csv];
        let mut inp = base(&src, &caps);
        inp.write_mode = WriteMode::Upsert;
        assert_eq!(plan_native_transfer(&inp), None);
        // Overwrite is allowed by the bq caps.
        let mut inp = base(&src, &caps);
        inp.write_mode = WriteMode::Overwrite;
        assert!(plan_native_transfer(&inp).is_some());
    }

    #[test]
    fn dlq_gate_blocks_unconditionally() {
        let caps = bq_caps();
        let src = [NativeFormat::Csv];
        let mut inp = base(&src, &caps);
        inp.has_dlq = true;
        assert_eq!(plan_native_transfer(&inp), None);
    }

    #[test]
    fn format_helpers() {
        assert_eq!(NativeFormat::NdJson.as_str(), "ndjson");
        assert_eq!(NativeFormat::Csv.as_str(), "csv");
        assert_eq!(NativeFormat::Parquet.as_str(), "parquet");
        assert_eq!(NativeFormat::ArrowIpc.as_str(), "arrow_ipc");
        assert_eq!(CsvDialect::default().delimiter, b',');
        assert!(CsvDialect::default().has_header);
    }

    #[test]
    fn native_batch_builders_and_debug() {
        let b = NativeBatch::bytes(NativeFormat::NdJson, b"{}\n".to_vec())
            .with_bookmark(Some(serde_json::json!({"o": 1})))
            .with_records(Some(1))
            .with_csv(CsvDialect {
                has_header: false,
                delimiter: b'\t',
            });
        assert_eq!(b.format, NativeFormat::NdJson);
        assert_eq!(b.records, Some(1));
        assert!(b.bookmark.is_some());
        assert!(!b.csv.has_header);
        // Debug on the payload prints the length, not the bytes.
        let dbg = format!("{:?}", b.payload);
        assert!(dbg.contains("Bytes"), "{dbg}");
    }

    #[test]
    fn native_payload_stream_debug() {
        let s = NativePayload::Stream(Box::pin(futures::stream::empty()));
        assert!(format!("{s:?}").contains("byte stream"));
    }

    #[test]
    fn load_context_carries_first_batch_and_mode() {
        let ctx = NativeLoadContext {
            write_mode: WriteMode::Overwrite,
            first_batch: true,
        };
        assert!(ctx.first_batch);
        assert_eq!(ctx.write_mode, WriteMode::Overwrite);
    }
}
