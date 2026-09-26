//! Per-run usage metering (#704): what a run moved and what it asked its
//! backends to do, so the CLI can account for cost per pipeline / row /
//! dataset and enforce run budgets (#703).
//!
//! A [`UsageMeter`] is attached to a [`Pipeline`](crate::Pipeline) with
//! [`Pipeline::with_usage_meter`](crate::Pipeline::with_usage_meter). The
//! observability decorators count every record and an estimate of its
//! serialized size; the pre-labelled [`RoundtripRecorder`] each connector
//! already receives tallies backend round trips by `op` and carries
//! **cost signals** — backend-reported usage such as BigQuery's bytes
//! billed or a streaming insert's payload size — into the same meter. A
//! library caller that attaches no meter pays nothing: counting is skipped
//! entirely.
//!
//! Byte counts are **estimates** of the JSON serialization (no allocation,
//! one walk over each value) — good enough to attribute cost and enforce a
//! `max_bytes` budget, never a wire-accurate figure.
//!
//! [`RoundtripRecorder`]: crate::observability::RoundtripRecorder

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Approximate serialized JSON size of `v`, in bytes, without allocating.
///
/// Strings count their UTF-8 length plus quotes (escapes are ignored),
/// numbers their decimal rendering, containers their brackets, commas and
/// key quotes/colons. Close enough for accounting and budgets.
pub fn estimate_json_bytes(v: &Value) -> u64 {
    match v {
        Value::Null => 4,
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                digits_i64(i)
            } else if let Some(u) = n.as_u64() {
                digits_u64(u)
            } else {
                // Floats: `serde_json` renders the shortest round-trip form;
                // ~17 significant digits is the upper bound that matters.
                17
            }
        }
        Value::String(s) => s.len() as u64 + 2,
        Value::Array(items) => {
            let inner: u64 = items.iter().map(estimate_json_bytes).sum();
            inner + 2 + items.len().saturating_sub(1) as u64
        }
        Value::Object(map) => {
            let inner: u64 = map
                .iter()
                .map(|(k, val)| k.len() as u64 + 3 + estimate_json_bytes(val))
                .sum();
            inner + 2 + map.len().saturating_sub(1) as u64
        }
    }
}

/// Estimated serialized size of a page of records.
pub fn estimate_page_bytes(records: &[Value]) -> u64 {
    records.iter().map(estimate_json_bytes).sum()
}

fn digits_u64(mut u: u64) -> u64 {
    let mut n = 1;
    while u >= 10 {
        u /= 10;
        n += 1;
    }
    n
}

fn digits_i64(i: i64) -> u64 {
    if i < 0 {
        1 + digits_u64(i.unsigned_abs())
    } else {
        digits_u64(i as u64)
    }
}

/// Which side of the pipeline a round trip or signal came from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UsageSide {
    Source,
    Sink,
}

impl UsageSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
}

/// A backend-reported usage figure a connector learned during the run —
/// BigQuery's `totalBytesBilled`, the payload size of a streaming insert, a
/// warehouse's credits — attributed to the connector that reported it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CostSignal {
    /// What was measured: `bytes_billed`, `bytes_processed`, `bytes_streamed`,
    /// `credits`, … A closed set per connector, documented in its README.
    pub kind: String,
    /// The unit of `quantity`: `bytes`, `credits`, `requests`, …
    pub unit: String,
    pub quantity: f64,
    pub side: UsageSide,
    /// Connector kind (`bigquery`, `s3`, …).
    pub connector: String,
}

/// The counters a run accumulated, frozen for reporting and storage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UsageSnapshot {
    /// Records the source yielded.
    pub records_read: u64,
    /// Records the sink accepted.
    pub records_written: u64,
    /// Estimated serialized bytes read.
    pub bytes_read: u64,
    /// Estimated serialized bytes written.
    pub bytes_written: u64,
    /// Source backend round trips by `op` (`page`, `get`, `list`, …).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub source_roundtrips: BTreeMap<String, u64>,
    /// Sink backend round trips by `op` (`insert`, `put`, `merge`, …).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sink_roundtrips: BTreeMap<String, u64>,
    /// Backend-reported usage figures.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<CostSignal>,
}

impl UsageSnapshot {
    /// Total round trips on `side`.
    pub fn roundtrips(&self, side: UsageSide) -> u64 {
        match side {
            UsageSide::Source => self.source_roundtrips.values().sum(),
            UsageSide::Sink => self.sink_roundtrips.values().sum(),
        }
    }

    /// Fold another snapshot into this one (sums; signals concatenated).
    pub fn merge(&mut self, other: &UsageSnapshot) {
        self.records_read += other.records_read;
        self.records_written += other.records_written;
        self.bytes_read += other.bytes_read;
        self.bytes_written += other.bytes_written;
        for (k, v) in &other.source_roundtrips {
            *self.source_roundtrips.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &other.sink_roundtrips {
            *self.sink_roundtrips.entry(k.clone()).or_default() += v;
        }
        self.signals.extend(other.signals.iter().cloned());
    }
}

/// The live, thread-safe counters of one run. Cheap to share (`Arc`).
#[derive(Debug, Default)]
pub struct UsageMeter {
    records_read: AtomicU64,
    records_written: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    roundtrips: Mutex<BTreeMap<(UsageSide, &'static str), u64>>,
    signals: Mutex<Vec<CostSignal>>,
}

impl UsageMeter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count a page the source yielded.
    pub fn add_read(&self, records: u64, bytes: u64) {
        self.records_read.fetch_add(records, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Count a page the sink accepted.
    pub fn add_written(&self, records: u64, bytes: u64) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Count one backend round trip.
    pub fn add_roundtrip(&self, side: UsageSide, op: &'static str) {
        let mut map = self.roundtrips.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry((side, op)).or_default() += 1;
    }

    /// Record a backend-reported usage figure.
    pub fn add_signal(&self, signal: CostSignal) {
        self.signals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(signal);
    }

    /// Records the sink accepted so far (what a `max_records` budget checks).
    pub fn records_written(&self) -> u64 {
        self.records_written.load(Ordering::Relaxed)
    }

    /// Estimated bytes the sink accepted so far (what a `max_bytes` budget
    /// checks).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Freeze the counters.
    pub fn snapshot(&self) -> UsageSnapshot {
        let mut source_roundtrips = BTreeMap::new();
        let mut sink_roundtrips = BTreeMap::new();
        for ((side, op), n) in self
            .roundtrips
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            match side {
                UsageSide::Source => *source_roundtrips.entry((*op).to_string()).or_default() += n,
                UsageSide::Sink => *sink_roundtrips.entry((*op).to_string()).or_default() += n,
            }
        }
        UsageSnapshot {
            records_read: self.records_read.load(Ordering::Relaxed),
            records_written: self.records_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            source_roundtrips,
            sink_roundtrips,
            signals: self
                .signals
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn byte_estimate_tracks_serialized_size() {
        for v in [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(-42),
            json!(1234567890123u64),
            json!("héllo"),
            json!([]),
            json!({}),
            json!([1, 2, 3]),
            json!({"a": 1, "bb": [true, null], "c": {"d": "x"}}),
        ] {
            let exact = serde_json::to_vec(&v).unwrap().len() as u64;
            assert_eq!(estimate_json_bytes(&v), exact, "{v}");
        }
        // Floats are bounded, not exact.
        assert!(estimate_json_bytes(&json!(1.5)) >= 3);
        assert_eq!(
            estimate_page_bytes(&[json!({"a": 1}), json!({"a": 22})]),
            7 + 8
        );
    }

    #[test]
    fn meter_counts_and_snapshots() {
        let m = UsageMeter::new();
        m.add_read(3, 30);
        m.add_written(2, 20);
        m.add_roundtrip(UsageSide::Source, "page");
        m.add_roundtrip(UsageSide::Source, "page");
        m.add_roundtrip(UsageSide::Sink, "insert");
        m.add_signal(CostSignal {
            kind: "bytes_billed".into(),
            unit: "bytes".into(),
            quantity: 1024.0,
            side: UsageSide::Sink,
            connector: "bigquery".into(),
        });
        assert_eq!(m.records_written(), 2);
        assert_eq!(m.bytes_written(), 20);
        let s = m.snapshot();
        assert_eq!(s.records_read, 3);
        assert_eq!(s.bytes_read, 30);
        assert_eq!(s.source_roundtrips["page"], 2);
        assert_eq!(s.sink_roundtrips["insert"], 1);
        assert_eq!(s.roundtrips(UsageSide::Source), 2);
        assert_eq!(s.roundtrips(UsageSide::Sink), 1);
        assert_eq!(s.signals.len(), 1);
        assert_eq!(UsageSide::Sink.as_str(), "sink");

        let mut total = UsageSnapshot::default();
        total.merge(&s);
        total.merge(&s);
        assert_eq!(total.records_written, 4);
        assert_eq!(total.source_roundtrips["page"], 4);
        assert_eq!(total.signals.len(), 2);
        let round: UsageSnapshot =
            serde_json::from_value(serde_json::to_value(&s).unwrap()).unwrap();
        assert_eq!(round, s);
    }
}
