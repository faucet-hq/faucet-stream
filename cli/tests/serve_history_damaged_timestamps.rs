//! A stored timestamp that is no timestamp (#1112): the row is still read,
//! the value is the same on every read, and the damage is counted.
#![cfg(feature = "serve-history-sqlite")]

use std::time::Duration;

use chrono::{DateTime, Utc};
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::templates::{DeprecationRecord, TemplateDraft, TemplateId};
use faucet_cli::serve::history::{AuditEntry, AuditFilter, InstanceHeartbeat, RunHistory};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use sqlx::Connection as _;

const COUNTER: &str = "faucet_serve_history_unparseable_timestamps_total";

struct Db {
    _dir: tempfile::TempDir,
    url: String,
    store: SqliteHistory,
}

async fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("h.db").display());
    let store = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        "inst".to_string(),
    )
    .await
    .unwrap();
    Db {
        _dir: dir,
        url,
        store,
    }
}

impl Db {
    /// Overwrite stored text behind the store's back, as a damaged row.
    async fn damage(&self, sql: &str) {
        let mut conn = sqlx::SqliteConnection::connect(&self.url).await.unwrap();
        let done = sqlx::query(sql).execute(&mut conn).await.unwrap();
        assert!(done.rows_affected() >= 1, "{sql}");
    }
}

/// Run `read` twice under a local recorder: both reads return the same value,
/// and each read of the damaged row is counted under `table`.
fn read_twice<T: serde::Serialize>(table: &str, read: impl Fn() -> T) -> T {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (first, second) = metrics::with_local_recorder(&recorder, || (read(), read()));
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap(),
        "the value must not change between reads"
    );
    let counted: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == COUNTER
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "table" && l.value() == table)
        })
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("{other:?}"),
        })
        .sum();
    assert_eq!(counted, 2, "one count per read of the damaged {table} row");
    first
}

fn block<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

fn draft(id: &str) -> TemplateDraft {
    TemplateDraft {
        id: TemplateId::parse(id).unwrap(),
        name: Some(id.to_string()),
        description: None,
        body: format!("version: 1\nname: {id}\n"),
        format: faucet_cli::serve::load::ConfigFormat::Yaml,
        params: Default::default(),
        created_by: None,
        kind: faucet_cli::hub::TemplateKind::Pipeline,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_audit_time_reads_as_the_epoch_and_keeps_the_entry() {
    let db = db().await;
    let entry = |id: &str| AuditEntry {
        id: id.to_string(),
        timestamp: Utc::now(),
        principal: "admin".into(),
        role: "admin".into(),
        action: "run.submit".into(),
        run_id: None,
        config_fingerprint: None,
        source_ip: None,
        tenant: None,
        target: None,
        detail: None,
        result: "ok".into(),
    };
    db.store.record_audit(&entry("a-good")).await.unwrap();
    db.store.record_audit(&entry("a-bad")).await.unwrap();
    db.damage("UPDATE faucet_serve_audit SET ts = 'yesterday-ish' WHERE id = 'a-bad'")
        .await;
    let filter = AuditFilter {
        limit: 10,
        ..Default::default()
    };
    let listed = read_twice("audit", || block(db.store.list_audit(&filter)).unwrap());
    assert_eq!(listed.len(), 2, "the damaged entry is still listed");
    let bad = listed.iter().find(|e| e.id == "a-bad").unwrap();
    assert_eq!(bad.timestamp, DateTime::<Utc>::UNIX_EPOCH);
    let good = listed.iter().find(|e| e.id == "a-good").unwrap();
    assert!(good.timestamp > DateTime::<Utc>::UNIX_EPOCH);
}

#[tokio::test(flavor = "multi_thread")]
async fn damaged_template_times_read_as_the_epoch() {
    let db = db().await;
    let s = &db.store;
    for _ in 0..2 {
        s.template_register(&draft("orders")).await.unwrap();
    }
    s.template_launch("orders", 1, Some("ci"), None)
        .await
        .unwrap();
    let marker = DeprecationRecord {
        deprecated_at: Utc::now(),
        deprecated_by: Some("admin".into()),
        reason: None,
    };
    s.template_set_deprecation("orders", Some(&marker))
        .await
        .unwrap();
    s.template_set_version_deprecation("orders", 2, Some(&marker))
        .await
        .unwrap();
    db.damage("UPDATE faucet_template_launches SET launched_at = ''")
        .await;
    db.damage("UPDATE faucet_template_deprecations SET deprecated_at = '2026-13-45'")
        .await;
    db.damage("UPDATE faucet_template_version_deprecations SET deprecated_at = 'x'")
        .await;

    let launches = read_twice("template_launches", || {
        block(s.template_launches("orders")).unwrap()
    });
    assert_eq!(launches.len(), 1);
    assert_eq!(launches[0].launched_at, DateTime::<Utc>::UNIX_EPOCH);
    assert_eq!((launches[0].seq, launches[0].version), (1, 1));

    let whole = read_twice("template_deprecations", || {
        block(s.template_deprecation("orders")).unwrap()
    })
    .unwrap();
    assert_eq!(whole.deprecated_at, DateTime::<Utc>::UNIX_EPOCH);
    assert_eq!(whole.deprecated_by.as_deref(), Some("admin"));

    let versions = read_twice("template_version_deprecations", || {
        block(s.template_version_deprecations("orders")).unwrap()
    });
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version, 2);
    assert_eq!(
        versions[0].record.deprecated_at,
        DateTime::<Utc>::UNIX_EPOCH
    );

    // The state of the template derives from the launch order, not the times.
    let state = s.template_state("orders").await.unwrap();
    assert_eq!(state.stable, Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_statistics_point_and_member_start_read_as_the_epoch() {
    use faucet_cli::serve::history::catalog::{
        CatalogListFilter, CatalogUpdate, DatasetObservation, DatasetRole,
    };
    let db = db().await;
    let s = &db.store;
    let obs = |uri: &str, role| DatasetObservation {
        uri: uri.into(),
        kind: "csv".into(),
        role,
        schema: None,
        records: 7,
    };
    s.catalog_record(&CatalogUpdate {
        run_id: "r1".into(),
        pipeline: "p".into(),
        row: "default".into(),
        recorded_at: Utc::now(),
        sources: vec![obs("csv://a", DatasetRole::Source)],
        sink: obs("csv://b", DatasetRole::Sink),
        column_lineage: None,
    })
    .await
    .unwrap();
    let listed = s
        .catalog_list_datasets(&CatalogListFilter {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let id = listed
        .datasets
        .iter()
        .find(|d| d.uri == "csv://b")
        .unwrap()
        .id
        .clone();
    db.damage(&format!(
        "UPDATE faucet_catalog_stats SET recorded_at = 'no-time' WHERE dataset_id = '{id}'"
    ))
    .await;
    let detail = read_twice("catalog_stats", || {
        block(s.catalog_get_dataset(&id)).unwrap().unwrap().stats
    });
    assert_eq!(detail.len(), 1);
    assert_eq!(detail[0].recorded_at, DateTime::<Utc>::UNIX_EPOCH);
    assert_eq!(detail[0].records, 7);

    s.heartbeat_instance(&InstanceHeartbeat {
        started_at: Utc::now(),
        listen: None,
        max_concurrent: 1,
        in_flight: 0,
        state_format: faucet_core::state_version::STATE_FORMAT,
        pools: faucet_cli::serve::pools::default_only(),
        capabilities: Vec::new(),
        clock_offset_ms: None,
    })
    .await
    .unwrap();
    db.damage("UPDATE faucet_serve_instances SET started_at = 'boot'")
        .await;
    let members = read_twice("instances", || {
        block(s.live_instances(Duration::from_secs(60)))
            .unwrap()
            .into_iter()
            .map(|m| (m.instance_id, m.started_at, m.last_heartbeat))
            .collect::<Vec<_>>()
    });
    assert_eq!(members.len(), 1, "the member is still listed");
    assert_eq!(members[0].1, DateTime::<Utc>::UNIX_EPOCH);
    assert!(members[0].2 > DateTime::<Utc>::UNIX_EPOCH);
}
