//! Usage records (#704): one per invocation, holding the meter's snapshot,
//! the cost estimate computed from the pricing table, and the hosted-ELT
//! equivalent; plus the pure aggregation `faucet usage` / `GET /v1/usage`
//! group them with.

use super::spec::PricingSpec;
use chrono::{DateTime, Utc};
use faucet_core::JsonSchema;
use faucet_core::usage::{UsageSide, UsageSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Object-store connector kinds whose round trips are priced per request.
const OBJECT_STORE_KINDS: &[&str] = &["s3", "gcs", "azure-blob", "azure_blob"];
/// Local connector kinds: bytes moved between two of these cross no cloud.
const LOCAL_KINDS: &[&str] = &["csv", "jsonl", "parquet", "stdout", "sqlite", "duckdb"];

const GB: f64 = 1_000_000_000.0;
const GIB: f64 = 1_073_741_824.0;
const TIB: f64 = 1_099_511_627_776.0;

/// One priced component of an estimate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CostLine {
    /// What was priced: `bigquery_bytes_billed`, `s3_requests_read`, `egress`, …
    pub item: String,
    pub quantity: f64,
    pub unit: String,
    /// The rate applied, in `currency` per `unit`.
    pub rate: f64,
    pub amount: f64,
}

/// The estimate for one invocation: what faucet can price from what it
/// measured, and what it could not.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CostEstimate {
    pub currency: String,
    /// Sum of `lines[].amount`.
    pub total: f64,
    pub lines: Vec<CostLine>,
    /// Connector kinds in the run that reported no cost signal, so their
    /// backend compute is **not** in `total` (shown as "not reported", never
    /// as zero).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_reported: Vec<String>,
    /// What a per-row-priced hosted ELT service would have charged for the
    /// records written, at `pricing.hosted_elt_per_million_rows`.
    pub hosted_equivalent: f64,
}

/// Compute the estimate for a snapshot.
pub fn estimate(
    usage: &UsageSnapshot,
    source_kind: &str,
    sink_kind: &str,
    pricing: &PricingSpec,
) -> CostEstimate {
    let mut lines = Vec::new();
    let mut push = |item: String, quantity: f64, unit: &str, rate: f64| {
        if quantity > 0.0 && rate > 0.0 {
            lines.push(CostLine {
                item,
                quantity,
                unit: unit.to_string(),
                rate,
                amount: quantity * rate,
            });
        }
    };

    // Backend-reported signals.
    let mut reported_kinds: Vec<&str> = Vec::new();
    for sig in &usage.signals {
        reported_kinds.push(sig.connector.as_str());
        let item = format!("{}_{}", sig.connector, sig.kind);
        match (sig.connector.as_str(), sig.kind.as_str()) {
            ("bigquery", "bytes_billed") | ("bigquery", "bytes_processed") => push(
                item,
                sig.quantity / TIB,
                "TiB",
                pricing.warehouse.bigquery_per_tib_scanned,
            ),
            ("bigquery", "bytes_streamed") => push(
                item,
                sig.quantity / GIB,
                "GiB",
                pricing.warehouse.bigquery_streaming_per_gib,
            ),
            ("snowflake", "credits") => push(
                item,
                sig.quantity,
                "credits",
                pricing.warehouse.snowflake_per_credit,
            ),
            // A signal with no price line (e.g. BigQuery `bytes_loaded`: load
            // jobs are free on-demand) is still evidence the backend reported.
            _ => {}
        }
    }
    // Object-store requests, from the round-trip tallies.
    for (side, kind, trips) in [
        (UsageSide::Source, source_kind, &usage.source_roundtrips),
        (UsageSide::Sink, sink_kind, &usage.sink_roundtrips),
    ] {
        if !OBJECT_STORE_KINDS.contains(&kind) {
            continue;
        }
        let reads: u64 = trips
            .iter()
            .filter(|(op, _)| matches!(op.as_str(), "list" | "get" | "head"))
            .map(|(_, n)| n)
            .sum();
        let writes: u64 = trips
            .iter()
            .filter(|(op, _)| matches!(op.as_str(), "put" | "post"))
            .map(|(_, n)| n)
            .sum();
        if reads > 0 || writes > 0 {
            reported_kinds.push(kind);
        }
        push(
            format!("{kind}_requests_read_{}", side.as_str()),
            reads as f64 / 1000.0,
            "1k requests",
            pricing.object_storage.read_per_1k_requests,
        );
        push(
            format!("{kind}_requests_write_{}", side.as_str()),
            writes as f64 / 1000.0,
            "1k requests",
            pricing.object_storage.write_per_1k_requests,
        );
    }
    // Egress: bytes that leave a cloud. Only when the operator set a rate,
    // and never for a local → local move.
    let crosses_cloud = !(LOCAL_KINDS.contains(&source_kind) && LOCAL_KINDS.contains(&sink_kind));
    if crosses_cloud {
        push(
            "egress".to_string(),
            usage.bytes_read as f64 / GB,
            "GB",
            pricing.egress_per_gb,
        );
    }

    let mut not_reported: Vec<String> = [source_kind, sink_kind]
        .into_iter()
        .filter(|k| !LOCAL_KINDS.contains(k) && !reported_kinds.contains(k))
        .map(String::from)
        .collect();
    not_reported.dedup();

    CostEstimate {
        currency: pricing.currency.clone(),
        total: lines.iter().map(|l| l.amount).sum(),
        lines,
        not_reported,
        hosted_equivalent: usage.records_written as f64 / 1_000_000.0
            * pricing.hosted_elt_per_million_rows,
    }
}

/// One invocation's usage, as stored and reported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UsageRecord {
    /// The invocation's own run id (the serve run id when running under
    /// `faucet serve`).
    pub run_id: String,
    pub pipeline: String,
    pub row: String,
    pub source_kind: String,
    pub sink_kind: String,
    /// The sink dataset's catalog id, when the catalog was active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_uri: Option<String>,
    #[schemars(with = "String")]
    pub recorded_at: DateTime<Utc>,
    pub duration_ms: u64,
    /// Whether the invocation ended in an error (usage is still recorded —
    /// a failed run cost something too).
    #[serde(default)]
    pub failed: bool,
    pub usage: UsageSnapshot,
    pub cost: CostEstimate,
}

/// `GET /v1/usage` / `faucet usage` filter.
#[derive(Debug, Clone, Default)]
pub struct UsageFilter {
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub pipeline: Option<String>,
    /// Records to scan at most (newest first); `0` = backend default.
    pub limit: usize,
}

impl UsageFilter {
    pub fn matches(&self, r: &UsageRecord) -> bool {
        self.since.is_none_or(|s| r.recorded_at >= s)
            && self.until.is_none_or(|u| r.recorded_at < u)
            && self.pipeline.as_deref().is_none_or(|p| r.pipeline == p)
    }
}

/// What `faucet usage --by` groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GroupBy {
    Pipeline,
    Row,
    Dataset,
    Sink,
    Day,
}

impl GroupBy {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pipeline" => Some(Self::Pipeline),
            "row" => Some(Self::Row),
            "dataset" => Some(Self::Dataset),
            "sink" => Some(Self::Sink),
            "day" => Some(Self::Day),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pipeline => "pipeline",
            Self::Row => "row",
            Self::Dataset => "dataset",
            Self::Sink => "sink",
            Self::Day => "day",
        }
    }
}

/// One aggregated row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UsageRow {
    pub key: String,
    pub runs: u64,
    pub failed_runs: u64,
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub duration_ms: u64,
    pub roundtrips: u64,
    pub cost: f64,
    pub hosted_equivalent: f64,
    /// Connector kinds whose compute is not in `cost`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_reported: Vec<String>,
}

/// An aggregation over a window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UsageReport {
    pub by: GroupBy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub since: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub until: Option<DateTime<Utc>>,
    pub currency: String,
    /// Invocations aggregated.
    pub records: u64,
    pub rows: Vec<UsageRow>,
    pub total: UsageRow,
}

/// Group `records` by `by`, ordered by estimated cost (then records written)
/// descending. Pure.
pub fn aggregate(records: &[UsageRecord], by: GroupBy, currency: &str) -> UsageReport {
    let mut groups: BTreeMap<String, UsageRow> = BTreeMap::new();
    let mut total = empty_row("total");
    for r in records {
        let key = match by {
            GroupBy::Pipeline => r.pipeline.clone(),
            GroupBy::Row => format!("{}::{}", r.pipeline, r.row),
            GroupBy::Dataset => r
                .dataset_uri
                .clone()
                .unwrap_or_else(|| format!("(uncatalogued) {}::{}", r.pipeline, r.row)),
            GroupBy::Sink => r.sink_kind.clone(),
            GroupBy::Day => r.recorded_at.format("%Y-%m-%d").to_string(),
        };
        let row = groups.entry(key.clone()).or_insert_with(|| empty_row(&key));
        fold(row, r);
        fold(&mut total, r);
    }
    let mut rows: Vec<UsageRow> = groups.into_values().collect();
    rows.sort_by(|a, b| {
        b.cost
            .partial_cmp(&a.cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.records_written.cmp(&a.records_written))
            .then_with(|| a.key.cmp(&b.key))
    });
    UsageReport {
        by,
        since: None,
        until: None,
        currency: currency.to_string(),
        records: records.len() as u64,
        rows,
        total,
    }
}

fn empty_row(key: &str) -> UsageRow {
    UsageRow {
        key: key.to_string(),
        runs: 0,
        failed_runs: 0,
        records_read: 0,
        records_written: 0,
        bytes_read: 0,
        bytes_written: 0,
        duration_ms: 0,
        roundtrips: 0,
        cost: 0.0,
        hosted_equivalent: 0.0,
        not_reported: Vec::new(),
    }
}

fn fold(row: &mut UsageRow, r: &UsageRecord) {
    row.runs += 1;
    row.failed_runs += u64::from(r.failed);
    row.records_read += r.usage.records_read;
    row.records_written += r.usage.records_written;
    row.bytes_read += r.usage.bytes_read;
    row.bytes_written += r.usage.bytes_written;
    row.duration_ms += r.duration_ms;
    row.roundtrips += r.usage.roundtrips(UsageSide::Source) + r.usage.roundtrips(UsageSide::Sink);
    row.cost += r.cost.total;
    row.hosted_equivalent += r.cost.hosted_equivalent;
    for k in &r.cost.not_reported {
        if !row.not_reported.contains(k) {
            row.not_reported.push(k.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::usage::CostSignal;

    fn snap(written: u64, bytes: u64) -> UsageSnapshot {
        UsageSnapshot {
            records_read: written,
            records_written: written,
            bytes_read: bytes,
            bytes_written: bytes,
            ..Default::default()
        }
    }

    fn record(
        pipeline: &str,
        row: &str,
        day: &str,
        cost: CostEstimate,
        failed: bool,
    ) -> UsageRecord {
        UsageRecord {
            run_id: format!("{pipeline}-{row}-{day}"),
            pipeline: pipeline.into(),
            row: row.into(),
            source_kind: "csv".into(),
            sink_kind: "bigquery".into(),
            dataset_id: None,
            dataset_uri: Some(format!("bq://p/{pipeline}")),
            recorded_at: format!("{day}T12:00:00Z").parse().unwrap(),
            duration_ms: 1000,
            failed,
            usage: snap(1_000_000, 5000),
            cost,
        }
    }

    #[test]
    fn estimate_prices_signals_requests_and_egress() {
        let p = PricingSpec::default();
        let mut s = snap(2_000_000, 3 * GB as u64);
        s.signals.push(CostSignal {
            kind: "bytes_billed".into(),
            unit: "bytes".into(),
            quantity: TIB,
            side: UsageSide::Sink,
            connector: "bigquery".into(),
        });
        s.signals.push(CostSignal {
            kind: "bytes_streamed".into(),
            unit: "bytes".into(),
            quantity: 2.0 * GIB,
            side: UsageSide::Sink,
            connector: "bigquery".into(),
        });
        s.source_roundtrips.insert("list".into(), 500);
        s.source_roundtrips.insert("get".into(), 1500);
        let e = estimate(&s, "s3", "bigquery", &p);
        let item = |name: &str| e.lines.iter().find(|l| l.item == name).cloned();
        assert_eq!(item("bigquery_bytes_billed").unwrap().amount, 6.25);
        assert_eq!(item("bigquery_bytes_streamed").unwrap().amount, 0.1);
        let reads = item("s3_requests_read_source").unwrap();
        assert_eq!(reads.quantity, 2.0);
        assert!((reads.amount - 0.0008).abs() < 1e-9);
        assert!(item("egress").is_none(), "egress is not charged by default");
        assert!(e.not_reported.is_empty(), "{e:?}");
        assert_eq!(e.hosted_equivalent, 30.0);
        assert!((e.total - 6.3508).abs() < 1e-9, "{}", e.total);

        // An egress rate charges the bytes read across clouds, not local→local.
        let p2 = PricingSpec {
            egress_per_gb: 0.1,
            ..Default::default()
        };
        let e = estimate(&s, "s3", "bigquery", &p2);
        let egress = e.lines.iter().find(|l| l.item == "egress").unwrap();
        assert!((egress.amount - 0.3).abs() < 1e-9);
        let e = estimate(&snap(10, 3 * GB as u64), "csv", "jsonl", &p2);
        assert!(e.lines.is_empty());
        assert!(e.not_reported.is_empty());
        // A warehouse that reported nothing is called out, not priced at zero.
        let e = estimate(&snap(10, 10), "postgres", "snowflake", &p);
        assert_eq!(e.not_reported, vec!["postgres", "snowflake"]);
        assert_eq!(e.total, 0.0);
    }

    #[test]
    fn aggregate_groups_sorts_by_cost_and_totals() {
        let c = |t: f64| CostEstimate {
            currency: "USD".into(),
            total: t,
            lines: vec![],
            not_reported: vec!["postgres".into()],
            hosted_equivalent: 15.0,
        };
        let recs = vec![
            record("a", "r1", "2026-09-01", c(1.0), false),
            record("a", "r2", "2026-09-01", c(2.0), true),
            record("b", "r1", "2026-09-02", c(5.0), false),
        ];
        let rep = aggregate(&recs, GroupBy::Pipeline, "USD");
        assert_eq!(rep.records, 3);
        assert_eq!(rep.rows[0].key, "b");
        assert_eq!(rep.rows[1].key, "a");
        assert_eq!(rep.rows[1].runs, 2);
        assert_eq!(rep.rows[1].failed_runs, 1);
        assert_eq!(rep.rows[1].cost, 3.0);
        assert_eq!(rep.total.cost, 8.0);
        assert_eq!(rep.total.hosted_equivalent, 45.0);
        assert_eq!(rep.total.records_written, 3_000_000);
        assert_eq!(rep.total.not_reported, vec!["postgres"]);
        assert_eq!(aggregate(&recs, GroupBy::Day, "USD").rows.len(), 2);
        assert_eq!(aggregate(&recs, GroupBy::Row, "USD").rows.len(), 3);
        assert_eq!(
            aggregate(&recs, GroupBy::Sink, "USD").rows[0].key,
            "bigquery"
        );
        assert_eq!(
            aggregate(&recs, GroupBy::Dataset, "USD").rows[0].key,
            "bq://p/b"
        );
        let f = UsageFilter {
            since: Some("2026-09-02T00:00:00Z".parse().unwrap()),
            until: None,
            pipeline: None,
            limit: 0,
        };
        assert_eq!(recs.iter().filter(|r| f.matches(r)).count(), 1);
        let f = UsageFilter {
            pipeline: Some("a".into()),
            ..Default::default()
        };
        assert_eq!(recs.iter().filter(|r| f.matches(r)).count(), 2);
        assert_eq!(GroupBy::parse("day"), Some(GroupBy::Day));
        assert_eq!(GroupBy::parse("nope"), None);
        assert_eq!(GroupBy::Dataset.as_str(), "dataset");
    }

    #[test]
    fn snowflake_credits_are_priced_and_unpriced_signals_still_count() {
        let mut usage = snap(10, 100);
        for (connector, kind, unit, q) in [
            ("snowflake", "credits", "credits", 2.0),
            ("bigquery", "bytes_loaded", "bytes", 1e9),
        ] {
            usage.signals.push(CostSignal {
                kind: kind.into(),
                unit: unit.into(),
                quantity: q,
                side: faucet_core::usage::UsageSide::Sink,
                connector: connector.into(),
            });
        }
        let est = estimate(&usage, "csv", "snowflake", &PricingSpec::default());
        let line = est
            .lines
            .iter()
            .find(|l| l.item == "snowflake_credits")
            .expect("credits line");
        assert_eq!(line.unit, "credits");
        assert!(est.lines.iter().all(|l| l.item != "bigquery_bytes_loaded"));
        assert!(!est.not_reported.contains(&"snowflake".to_string()));
        for by in [
            GroupBy::Pipeline,
            GroupBy::Row,
            GroupBy::Dataset,
            GroupBy::Sink,
            GroupBy::Day,
        ] {
            assert_eq!(GroupBy::parse(by.as_str()), Some(by));
        }
    }
}
