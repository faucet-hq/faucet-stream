use super::*;
use crate::serve::history::memory::MemoryHistory;
use crate::serve::history::{InvocationRecord, RunHistory};
use std::time::Duration;

fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn day_query() -> OverviewQuery {
    let mut q = OverviewQuery::window(ts("2026-10-30T00:00:00Z"), ts("2026-10-31T00:00:00Z"));
    q.bucket = Bucket::Hour;
    q
}

fn invocation(ms: u64, rows: Option<faucet_core::RowAccountsSnapshot>) -> InvocationRecord {
    InvocationRecord {
        row_id: "r".into(),
        parent_record_key: None,
        run_id: None,
        records_written: 0,
        duration_ms: ms,
        error: None,
        usage: None,
        batches: None,
        source_lag: None,
        rows,
        skipped: false,
        status: None,
    }
}

fn run(id: &str, name: &str, status: RunStatus, at: &str, written: u64, ms: &[u64]) -> RunRecord {
    let mut r = RunRecord::queued(
        id.into(),
        Some(name.into()),
        Default::default(),
        None,
        ts(at),
    );
    r.status = status;
    r.records_written = written;
    r.invocations = ms.iter().map(|m| invocation(*m, None)).collect();
    r
}

fn rows(
    transform_in: u64,
    transform_out: u64,
    quarantined: u64,
    failed: u64,
) -> faucet_core::RowAccountsSnapshot {
    let mut r = faucet_core::RowAccountsSnapshot::default();
    r.transform_in = transform_in;
    r.transform_out = transform_out;
    r.quarantined = quarantined;
    r.failed = failed;
    r.coalesced = 1;
    r.skipped = 2;
    r.dropped = 3;
    r
}

#[test]
fn buckets_floor_parse_and_count() {
    let t = ts("2026-10-30T13:45:12.5Z");
    assert_eq!(Bucket::Hour.floor(t), ts("2026-10-30T13:00:00Z"));
    assert_eq!(Bucket::Day.floor(t), ts("2026-10-30T00:00:00Z"));
    assert_eq!(
        Bucket::Hour.parse_key("2026-10-30T13"),
        Some(ts("2026-10-30T13:00:00Z"))
    );
    assert_eq!(
        Bucket::Day.parse_key("2026-10-30"),
        Some(ts("2026-10-30T00:00:00Z"))
    );
    assert_eq!(Bucket::Day.parse_key("garbage"), None);
    assert_eq!(Bucket::parse("hour"), Some(Bucket::Hour));
    assert_eq!(Bucket::parse("day"), Some(Bucket::Day));
    assert_eq!(Bucket::parse("week"), None);
    assert_eq!(
        Bucket::for_range(ts("2026-10-27T00:00:00Z"), ts("2026-10-30T00:00:00Z")),
        Bucket::Hour
    );
    assert_eq!(
        Bucket::for_range(ts("2026-10-23T00:00:00Z"), ts("2026-10-30T00:00:00Z")),
        Bucket::Day
    );
    let q = day_query();
    assert_eq!(q.bucket_count(), 24);
    assert_eq!(q.bucket_starts().len(), 24);
    let mut odd = OverviewQuery::window(ts("2026-10-30T10:30:00Z"), ts("2026-10-30T12:10:00Z"));
    odd.bucket = Bucket::Hour;
    assert_eq!(
        odd.bucket_starts(),
        vec![
            ts("2026-10-30T10:00:00Z"),
            ts("2026-10-30T11:00:00Z"),
            ts("2026-10-30T12:00:00Z")
        ]
    );
    assert_eq!(odd.bucket_count(), 3);
}

#[test]
fn p95_is_nearest_rank() {
    assert_eq!(p95(&[]), 0);
    assert_eq!(p95(&[7]), 7);
    assert_eq!(p95(&[1, 2]), 2);
    let hundred: Vec<u64> = (1..=100).collect();
    assert_eq!(p95(&hundred), 95);
    let twenty: Vec<u64> = (1..=20).rev().collect();
    assert_eq!(p95(&twenty), 19);
}

#[test]
fn top_lists_order_and_cap() {
    let mut f = BTreeMap::new();
    f.insert("a".to_string(), (10, 1));
    f.insert("b".to_string(), (2, 1));
    f.insert("c".to_string(), (4, 2));
    f.insert("d".to_string(), (5, 0));
    let t = top_failures(&f, 2);
    assert_eq!(
        t.iter().map(|x| x.pipeline.as_str()).collect::<Vec<_>>(),
        ["c", "b"]
    );
    assert_eq!(t[0].rate, 0.5);
    assert_eq!(top_failures(&f, 10).len(), 3);

    let mut w = BTreeMap::new();
    w.insert("x".to_string(), 5);
    w.insert("y".to_string(), 9);
    w.insert("z".to_string(), 0);
    w.insert("a".to_string(), 9);
    let t = top_written(&w, 10);
    assert_eq!(
        t.iter().map(|x| x.pipeline.as_str()).collect::<Vec<_>>(),
        ["a", "y", "x"]
    );

    let mut d = BTreeMap::new();
    d.insert("fast".to_string(), vec![1, 2, 3]);
    d.insert("slow".to_string(), vec![100, 200]);
    assert_eq!(
        slowest(&d, 1),
        vec![PipelineLatency {
            pipeline: "slow".into(),
            p95_ms: 200,
            runs: 2
        }]
    );
}

#[test]
fn the_duration_sample_is_capped() {
    let mut agg = Aggregator::default();
    for _ in 0..DURATION_SAMPLE_CAP {
        assert!(agg.add_duration("p", 1));
    }
    assert!(!agg.add_duration("p", 1));
    let (runs, _, _) = agg.finish(&day_query());
    assert_eq!(runs.duration_sample.invocations, DURATION_SAMPLE_CAP as u64);
    assert!(runs.duration_sample.truncated);
}

#[test]
fn governance_counts_come_from_row_accounting() {
    let g = GovernanceCounts::of(&rows(10, 7, 4, 5));
    assert_eq!(
        g,
        GovernanceCounts {
            filtered: 3,
            quarantined: 4,
            dlq: 5,
            coalesced: 1,
            skipped: 2,
            dropped: 3
        }
    );
    assert_eq!(GovernanceCounts::of(&rows(1, 4, 0, 0)).filtered, 0);
    let mut sum = g;
    sum.add(&g);
    assert_eq!(sum.dlq, 10);
}

fn fleet() -> Vec<RunRecord> {
    let mut a = run(
        "a",
        "orders",
        RunStatus::Completed,
        "2026-10-30T01:10:00Z",
        100,
        &[10, 20],
    );
    a.invocations[0].rows = Some(rows(10, 8, 1, 0));
    a.invocations[1].run_id = Some("a-inv".into());
    let b = run(
        "b",
        "orders",
        RunStatus::Failed,
        "2026-10-30T01:20:00Z",
        5,
        &[30],
    );
    let mut c = run(
        "c",
        "users",
        RunStatus::Completed,
        "2026-10-30T03:00:00Z",
        50,
        &[40],
    );
    c.invocations.push(InvocationRecord {
        skipped: true,
        ..invocation(999, None)
    });
    let d = run(
        "d",
        "users",
        RunStatus::Cancelled,
        "2026-10-30T23:59:59Z",
        0,
        &[],
    );
    let e = run(
        "e",
        "users",
        RunStatus::Running,
        "2026-10-30T05:00:00Z",
        0,
        &[],
    );
    let old = run(
        "f",
        "orders",
        RunStatus::Failed,
        "2026-10-29T23:00:00Z",
        7,
        &[1],
    );
    let queued = run(
        "g",
        "orders",
        RunStatus::Queued,
        "2026-10-01T00:00:00Z",
        0,
        &[],
    );
    vec![a, b, c, d, e, old, queued]
}

#[test]
fn in_memory_aggregation_is_exact() {
    let q = day_query();
    let runs = fleet();
    let agg = aggregate_in_memory(&q, runs.iter(), std::iter::empty(), None);
    assert_eq!(agg.runs.total, 5);
    assert_eq!(agg.runs.by_status["completed"], 2);
    assert_eq!(agg.runs.by_status["failed"], 1);
    assert_eq!(agg.runs.by_status["running"], 1);
    assert_eq!(agg.runs.queued, 1);
    assert_eq!(agg.runs.running, 1);
    let one = &agg.runs.series[1];
    assert_eq!((one.succeeded, one.failed, one.cancelled), (1, 1, 0));
    assert_eq!(agg.runs.series[23].cancelled, 1);
    assert_eq!(agg.runs.series.len(), 24);
    assert_eq!(agg.records.total, 155);
    assert_eq!(agg.records.series[1].written, 105);
    assert_eq!(
        agg.records.top_pipelines[0],
        PipelineWritten {
            pipeline: "orders".into(),
            written: 105
        }
    );
    assert_eq!(
        agg.runs.failure_rate_by_pipeline,
        vec![PipelineFailure {
            pipeline: "orders".into(),
            runs: 2,
            failed: 1,
            rate: 0.5
        }]
    );
    assert_eq!(agg.runs.duration_sample.invocations, 4);
    assert_eq!(
        agg.runs.slowest[0],
        PipelineLatency {
            pipeline: "users".into(),
            p95_ms: 40,
            runs: 1
        }
    );
    assert_eq!(agg.governance.totals.filtered, 2);
    assert_eq!(agg.governance.series[1].counts.quarantined, 1);
    assert!(agg.cost.is_none());
    assert!(agg.freshness.is_none());
}

fn usage_record(
    run: &str,
    pipeline: &str,
    at: &str,
    currency: &str,
    cost: f64,
) -> crate::usage::UsageRecord {
    crate::usage::UsageRecord {
        run_id: run.into(),
        pipeline: pipeline.into(),
        row: "r".into(),
        source_kind: "csv".into(),
        sink_kind: "jsonl".into(),
        dataset_id: None,
        dataset_uri: None,
        recorded_at: ts(at),
        duration_ms: 1,
        failed: false,
        usage: Default::default(),
        cost: crate::usage::CostEstimate {
            currency: currency.into(),
            total: cost,
            lines: Vec::new(),
            not_reported: Vec::new(),
            hosted_equivalent: cost * 10.0,
        },
        tenant: None,
    }
}

#[test]
fn filters_narrow_runs_and_usage() {
    let mut runs = fleet();
    runs[0].pool = "gpu".into();
    runs[0].tenant = Some("acme".into());
    runs[0].labels.insert("team".into(), "data".into());
    let usage = vec![
        usage_record("a", "orders", "2026-10-30T02:00:00Z", "USD", 1.5),
        usage_record("a-inv", "orders", "2026-10-30T02:10:00Z", "USD", 0.25),
        usage_record("b", "orders", "2026-10-30T02:30:00Z", "USD", 0.5),
    ];
    let mut q = day_query();
    q.pool = Some("gpu".into());
    let agg = aggregate_in_memory(&q, runs.iter(), &usage, None);
    assert_eq!(agg.runs.total, 1);
    assert_eq!(agg.cost.as_ref().unwrap().invocations, 2);
    assert_eq!(agg.cost.as_ref().unwrap().estimated, 1.75);
    let mut q = day_query();
    q.labels = vec![crate::serve::history::labels::LabelFilter::parse("team:data").unwrap()];
    assert_eq!(
        aggregate_in_memory(&q, runs.iter(), &usage, None)
            .runs
            .total,
        1
    );
    let mut q = day_query();
    q.tenant = Some("acme".into());
    assert_eq!(
        aggregate_in_memory(&q, runs.iter(), &usage, None)
            .runs
            .total,
        1
    );
    let mut q = day_query();
    q.pipeline = Some("users".into());
    let agg = aggregate_in_memory(&q, runs.iter(), &usage, None);
    assert_eq!(agg.runs.total, 3);
    assert!(agg.cost.is_none());
}

#[test]
fn cost_reports_the_dominant_currency() {
    let q = day_query();
    let usage = vec![
        usage_record("a", "orders", "2026-10-30T02:00:00Z", "USD", 1.5),
        usage_record("b", "users", "2026-10-30T02:30:00Z", "USD", 2.0),
        usage_record("c", "orders", "2026-10-30T04:00:00Z", "EUR", 9.0),
        usage_record("d", "orders", "2026-10-29T04:00:00Z", "USD", 100.0),
    ];
    let cost = aggregate_in_memory(&q, std::iter::empty(), &usage, None)
        .cost
        .unwrap();
    assert_eq!(cost.currency, "USD");
    assert_eq!(cost.invocations, 2);
    assert_eq!(cost.estimated, 3.5);
    assert_eq!(cost.hosted_equivalent, 35.0);
    assert_eq!(cost.other_currencies, vec!["EUR".to_string()]);
    assert_eq!(cost.series[2].estimated, 3.5);
    assert_eq!(
        cost.by_pipeline[0],
        PipelineCost {
            pipeline: "users".into(),
            estimated: 2.0
        }
    );
    assert_eq!(CostAggregator::default().dominant(), None);
    let mut tie = CostAggregator::default();
    tie.add_bucket(q.since, "USD", 1, 1.0, 0.0);
    tie.add_bucket(q.since, "EUR", 1, 1.0, 0.0);
    assert_eq!(tie.dominant().as_deref(), Some("EUR"));
}

fn dataset(id: &str, last_success: &str) -> CatalogDataset {
    CatalogDataset {
        id: id.into(),
        uri: format!("file://{id}"),
        kind: "jsonl".into(),
        roles: vec!["sink".into()],
        first_seen: ts(last_success),
        last_seen: ts(last_success),
        last_success: ts(last_success),
        last_run_id: "r".into(),
        pipeline: "orders".into(),
        last_records: 1,
        total_records: 1,
        runs: 1,
        schema_versions: 1,
        current_schema: None,
        current_schema_hash: None,
        owners: Vec::new(),
        consumers: Vec::new(),
    }
}

fn version(id: &str, v: u32, at: &str) -> CatalogSchemaVersion {
    CatalogSchemaVersion {
        dataset_id: id.into(),
        version: v,
        recorded_at: ts(at),
        run_id: "r".into(),
        schema: serde_json::json!({}),
        schema_hash: "h".into(),
        diff: None,
    }
}

#[test]
fn freshness_lists_stale_datasets_and_schema_changes() {
    let mut q = day_query();
    assert!(
        freshness_from(
            &q,
            &[dataset("a", "2026-10-01T00:00:00Z")],
            std::iter::empty()
        )
        .is_none()
    );
    q.stale_before = Some(ts("2026-10-29T12:00:00Z"));
    assert!(freshness_from(&q, &[], std::iter::empty()).is_none());
    let ds = [
        dataset("old", "2026-10-01T00:00:00Z"),
        dataset("older", "2026-09-01T00:00:00Z"),
        dataset("fresh", "2026-10-30T06:00:00Z"),
        dataset("edge", "2026-10-29T12:00:00.5Z"),
    ];
    let vs = [
        version("fresh", 1, "2026-10-30T01:00:00Z"),
        version("fresh", 2, "2026-10-30T02:00:00Z"),
        version("fresh", 3, "2026-10-30T05:00:00Z"),
        version("old", 2, "2026-10-28T05:00:00Z"),
        version("ghost", 2, "2026-10-30T07:00:00Z"),
    ];
    let f = freshness_from(&q, &ds, &vs).unwrap();
    assert_eq!(f.datasets, 4);
    assert_eq!(f.stale_count, 2);
    assert_eq!(f.stale[0].id, "older");
    assert_eq!(f.schema_changed_count, 2);
    assert_eq!(f.schema_changed[0].id, "ghost");
    assert_eq!(f.schema_changed[0].uri, "");
    assert_eq!(
        f.schema_changed[1],
        SchemaChange {
            id: "fresh".into(),
            uri: "file://fresh".into(),
            changes: 2,
            last_changed: ts("2026-10-30T05:00:00Z"),
        }
    );
    assert!(!is_stale(
        ts("2026-10-29T12:00:00.5Z"),
        ts("2026-10-29T12:00:00.9Z")
    ));
}

#[tokio::test]
async fn the_memory_backend_and_the_listing_default_agree() {
    let h = MemoryHistory::new(Duration::from_secs(60));
    for r in fleet() {
        h.upsert(&r).await.unwrap();
    }
    h.usage_record(&usage_record(
        "a",
        "orders",
        "2026-10-30T02:00:00Z",
        "USD",
        1.5,
    ))
    .await
    .unwrap();
    let mut q = day_query();
    q.stale_before = Some(ts("2026-10-29T12:00:00Z"));
    let direct = h.overview_aggregates(&q).await.unwrap();
    let listed = aggregate_by_listing(&h, &q).await.unwrap();
    assert_eq!(direct, listed);
    assert_eq!(direct.runs.total, 5);
    assert_eq!(direct.cost.as_ref().unwrap().estimated, 1.5);
    assert!(direct.freshness.is_none());
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test]
async fn sqlite_groups_to_the_same_aggregates_as_memory() {
    let dir = tempfile::tempdir().unwrap();
    let sql = crate::serve::history::sqlite::SqliteHistory::connect(
        &format!("sqlite:{}", dir.path().join("h.db").display()),
        Duration::from_secs(60),
        Duration::from_secs(30),
        "i".into(),
    )
    .await
    .unwrap();
    let mem = MemoryHistory::new(Duration::from_secs(60));
    let mut runs = fleet();
    runs[0].pool = "gpu".into();
    runs[0].labels.insert("team".into(), "data".into());
    for r in &runs {
        sql.upsert(r).await.unwrap();
        mem.upsert(r).await.unwrap();
    }
    for u in [
        usage_record("a", "orders", "2026-10-30T02:00:00Z", "USD", 1.5),
        usage_record("a-inv", "orders", "2026-10-30T02:20:00Z", "USD", 0.25),
        usage_record("b", "users", "2026-10-30T03:00:00Z", "USD", 2.5),
        usage_record("c", "users", "2026-10-30T03:00:00Z", "EUR", 4.0),
    ] {
        sql.usage_record(&u).await.unwrap();
        mem.usage_record(&u).await.unwrap();
    }
    let mut queries = vec![day_query()];
    let mut q = day_query();
    q.pool = Some("gpu".into());
    queries.push(q);
    let mut q = day_query();
    q.labels = vec![crate::serve::history::labels::LabelFilter::parse("team").unwrap()];
    queries.push(q);
    let mut q = day_query();
    q.labels = vec![crate::serve::history::labels::LabelFilter::parse("team:data").unwrap()];
    queries.push(q);
    let mut q = day_query();
    q.pipeline = Some("users".into());
    q.bucket = Bucket::Day;
    queries.push(q);
    let mut q = day_query();
    q.top = 1;
    q.stale_before = Some(ts("2026-10-29T12:00:00Z"));
    queries.push(q);
    for q in queries {
        assert_eq!(
            sql.overview_aggregates(&q).await.unwrap(),
            mem.overview_aggregates(&q).await.unwrap(),
            "{q:?}"
        );
    }
}

#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
#[test]
fn postgres_statements_number_their_binds_in_order() {
    use crate::serve::history::sql::Dialect;
    let mut q = day_query();
    q.pipeline = Some("p".into());
    q.pool = Some("gpu".into());
    q.tenant = Some("t".into());
    q.labels = vec![
        crate::serve::history::labels::LabelFilter::parse("a:1").unwrap(),
        crate::serve::history::labels::LabelFilter::parse("b").unwrap(),
    ];
    let (text, binds) = sql::status_by_bucket(Dialect::Postgres, &q);
    assert_eq!(binds.len(), 8);
    for n in 1..=8 {
        assert!(text.contains(&format!("${n}")), "{text}");
    }
    assert!(
        text.contains("CAST(r.body AS json) #>> '{records_written}'"),
        "{text}"
    );
    let (text, binds) = sql::cost_by_pipeline(Dialect::Postgres, &q, "USD");
    assert_eq!(binds.last().map(String::as_str), Some("USD"));
    assert!(text.contains(&format!("${}", binds.len())), "{text}");
    assert!(text.contains("u.run_id IN (SELECT r.run_id"), "{text}");
    let (text, _) = sql::governance(Dialect::Postgres, &q);
    assert!(
        text.contains("json_array_elements") && text.contains("GREATEST("),
        "{text}"
    );
    let (text, _) = sql::durations(Dialect::Postgres, &q);
    assert!(text.contains("i.value ->> 'skipped'"), "{text}");
    let (text, _) = sql::current(Dialect::Postgres, &q);
    assert!(text.contains("'sharded'"), "{text}");
    let (text, _) = sql::failures(Dialect::Postgres, &q);
    assert!(text.contains("DOUBLE PRECISION"), "{text}");
    let (text, _) = sql::top_written(Dialect::Postgres, &q);
    assert!(text.contains("LIMIT 10"), "{text}");
    let (text, _) = sql::cost_by_bucket(Dialect::Postgres, &q);
    assert!(text.contains("#>> '{cost,currency}'"), "{text}");
    let (text, binds) = sql::stale_list(Dialect::Postgres, &q, ts("2026-10-29T12:00:00Z"));
    assert_eq!(binds, vec!["2026-10-29T12:00:00".to_string()]);
    assert!(text.contains("LIMIT 10"), "{text}");
    let (_, binds) = sql::stale_count(Dialect::Postgres, ts("2026-10-29T12:00:00Z"));
    assert_eq!(binds.len(), 1);
    let (text, _) = sql::schema_changed(Dialect::Postgres, &q);
    assert!(text.contains("CAST(v.version AS INTEGER) > 1"), "{text}");
    let (text, _) = sql::schema_changed_count(Dialect::Postgres, &q);
    assert!(text.contains("COUNT(DISTINCT"), "{text}");
}
