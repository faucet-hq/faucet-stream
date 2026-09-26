//! Cost and usage accounting (#704) — the CLI layer over
//! [`faucet_core::usage`].
//!
//! The executor attaches a [`UsageMeter`](faucet_core::UsageMeter) to every
//! invocation's pipeline; when the invocation ends, [`build_record`] freezes
//! the meter, prices it with the resolved [`PricingSpec`] and produces a
//! [`UsageRecord`] that is (a) reported on the invocation outcome (`faucet
//! run --output json`, the serve run record), (b) emitted as metrics, and
//! (c) recorded into the catalog store when one is active — the accumulated
//! store behind `faucet usage` and `GET /v1/usage`. Recording never fails a
//! run.
//!
//! - [`spec`] — the `usage:` block and the pricing table.
//! - [`model`] — records, cost estimation, aggregation.
//! - [`metrics`] — `faucet_usage_*` Prometheus surface.

pub mod metrics;
pub mod model;
pub mod spec;

pub use model::{
    CostEstimate, CostLine, GroupBy, UsageFilter, UsageRecord, UsageReport, UsageRow, aggregate,
    estimate,
};
pub use spec::{PricingSpec, UsageSpec};

use chrono::{DateTime, Utc};
use faucet_core::usage::UsageSnapshot;
use std::sync::Arc;

/// Records a listing returns when the caller sets no limit.
pub const DEFAULT_LIST_LIMIT: usize = 5_000;

/// The resolved pricing an executor run prices with. Cheap to share.
#[derive(Debug, Clone)]
pub struct UsageOptions {
    pub pricing: Arc<PricingSpec>,
}

impl Default for UsageOptions {
    fn default() -> Self {
        Self {
            pricing: Arc::new(PricingSpec::default()),
        }
    }
}

impl UsageOptions {
    /// Resolve from an optional `usage:` block (defaults when absent).
    pub fn from_spec(
        spec: Option<&UsageSpec>,
        base_dir: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        let pricing = match spec {
            Some(s) => s.resolve_pricing(base_dir)?,
            None => PricingSpec::default(),
        };
        Ok(Self {
            pricing: Arc::new(pricing),
        })
    }
}

/// The identity of one invocation, for the record.
pub struct RecordIdentity<'a> {
    pub run_id: &'a str,
    pub pipeline: &'a str,
    pub row: &'a str,
    pub source_kind: &'a str,
    pub sink_kind: &'a str,
    pub dataset_id: Option<String>,
    pub dataset_uri: Option<String>,
}

/// Freeze a meter into a priced record.
pub fn build_record(
    id: RecordIdentity<'_>,
    usage: UsageSnapshot,
    duration_ms: u64,
    failed: bool,
    pricing: &PricingSpec,
    now: DateTime<Utc>,
) -> UsageRecord {
    let cost = estimate(&usage, id.source_kind, id.sink_kind, pricing);
    UsageRecord {
        run_id: id.run_id.to_string(),
        pipeline: id.pipeline.to_string(),
        row: id.row.to_string(),
        source_kind: id.source_kind.to_string(),
        sink_kind: id.sink_kind.to_string(),
        dataset_id: id.dataset_id,
        dataset_uri: id.dataset_uri,
        recorded_at: now,
        duration_ms,
        failed,
        usage,
        cost,
    }
}

/// One line for `faucet run`'s text summary.
pub fn summary_line(r: &UsageRecord) -> String {
    let mut s = format!(
        "usage: {} in / {} out, {} read / {} written, est. {} {:.4}",
        r.usage.records_read,
        r.usage.records_written,
        fmt_bytes(r.usage.bytes_read),
        fmt_bytes(r.usage.bytes_written),
        r.cost.currency,
        r.cost.total
    );
    if !r.cost.not_reported.is_empty() {
        s.push_str(&format!(
            " (compute not reported by {})",
            r.cost.not_reported.join(", ")
        ));
    }
    s.push_str(&format!(
        "; hosted per-row equivalent {} {:.2}",
        r.cost.currency, r.cost.hosted_equivalent
    ));
    s
}

/// Human-readable byte count.
pub fn fmt_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// Render an aggregated report as a table.
pub fn render_report(rep: &UsageReport) -> String {
    let mut out = String::new();
    let window = match (rep.since, rep.until) {
        (Some(s), Some(u)) => format!(
            "{} → {}",
            s.format("%Y-%m-%dT%H:%M:%SZ"),
            u.format("%Y-%m-%dT%H:%M:%SZ")
        ),
        (Some(s), None) => format!("since {}", s.format("%Y-%m-%dT%H:%M:%SZ")),
        (None, Some(u)) => format!("until {}", u.format("%Y-%m-%dT%H:%M:%SZ")),
        (None, None) => "all recorded runs".to_string(),
    };
    out.push_str(&format!(
        "usage by {} ({window}; {} invocation(s); estimates in {})\n",
        rep.by.as_str(),
        rep.records,
        rep.currency
    ));
    let key_w = rep
        .rows
        .iter()
        .map(|r| r.key.len())
        .chain(std::iter::once(5))
        .max()
        .unwrap_or(5)
        .min(60);
    out.push_str(&format!(
        "  {:<key_w$}  {:>6}  {:>12}  {:>12}  {:>10}  {:>10}  {:>12}  {:>12}\n",
        rep.by.as_str(),
        "runs",
        "rows out",
        "bytes out",
        "duration",
        "requests",
        "est. cost",
        "hosted eq.",
        key_w = key_w
    ));
    let mut line = |r: &UsageRow| {
        let key = if r.key.len() > key_w {
            format!("{}…", &r.key[..key_w.saturating_sub(1)])
        } else {
            r.key.clone()
        };
        out.push_str(&format!(
            "  {:<key_w$}  {:>6}  {:>12}  {:>12}  {:>10}  {:>10}  {:>12.4}  {:>12.2}{}\n",
            key,
            r.runs,
            r.records_written,
            fmt_bytes(r.bytes_written),
            format!("{:.1}s", r.duration_ms as f64 / 1000.0),
            r.roundtrips,
            r.cost,
            r.hosted_equivalent,
            if r.not_reported.is_empty() {
                String::new()
            } else {
                format!("  (compute not reported: {})", r.not_reported.join(", "))
            },
            key_w = key_w
        ));
    };
    for r in &rep.rows {
        line(r);
    }
    line(&rep.total);
    out.push_str(
        "  estimates use the pricing table in `usage.pricing` (defaults = public list prices); \
         `hosted eq.` is what a per-row-priced hosted ELT service would charge for the same rows out\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::usage::UsageSnapshot;

    #[test]
    fn record_summary_and_table_render() {
        let usage = UsageSnapshot {
            records_read: 10,
            records_written: 10,
            bytes_read: 2048,
            bytes_written: 1_500_000,
            ..Default::default()
        };
        let r = build_record(
            RecordIdentity {
                run_id: "r1",
                pipeline: "p",
                row: "row-0",
                source_kind: "csv",
                sink_kind: "snowflake",
                dataset_id: None,
                dataset_uri: None,
            },
            usage,
            1500,
            false,
            &PricingSpec::default(),
            Utc::now(),
        );
        let line = summary_line(&r);
        assert!(line.contains("10 in / 10 out"), "{line}");
        assert!(line.contains("2.0 KiB read / 1.4 MiB written"), "{line}");
        assert!(line.contains("compute not reported by snowflake"), "{line}");
        assert_eq!(fmt_bytes(512), "512 B");
        let rep = aggregate(&[r], GroupBy::Pipeline, "USD");
        let text = render_report(&rep);
        assert!(
            text.contains("usage by pipeline (all recorded runs; 1 invocation(s)"),
            "{text}"
        );
        assert!(text.contains("compute not reported: snowflake"), "{text}");
        assert!(text.contains("total"));
        let opts = UsageOptions::from_spec(None, None).unwrap();
        assert_eq!(opts.pricing.currency, "USD");
        assert_eq!(UsageOptions::default().pricing.currency, "USD");
    }

    #[test]
    fn report_windows_and_long_keys_render() {
        let long = "p".repeat(80);
        let r = build_record(
            RecordIdentity {
                run_id: "r1",
                pipeline: &long,
                row: "row-0",
                source_kind: "csv",
                sink_kind: "jsonl",
                dataset_id: None,
                dataset_uri: None,
            },
            UsageSnapshot::default(),
            10,
            false,
            &PricingSpec::default(),
            Utc::now(),
        );
        let mut rep = aggregate(&[r], GroupBy::Pipeline, "USD");
        let at = Utc::now();
        for (since, until, needle) in [
            (Some(at), Some(at), "→"),
            (Some(at), None, "since "),
            (None, Some(at), "until "),
        ] {
            rep.since = since;
            rep.until = until;
            let text = render_report(&rep);
            assert!(text.contains(needle), "{text}");
            assert!(text.contains('…'), "a 60+ char key is truncated: {text}");
        }
    }
}
