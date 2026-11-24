//! Postgres run-history backend integration test (Phase 5, #127).
//!
//! Runs against `FAUCET_TEST_POSTGRES_URL` when set, otherwise against a
//! Postgres test container. Without Docker it skips locally and fails where
//! `FAUCET_REQUIRE_BACKENDS` is set (CI). It exercises the Postgres dialect of
//! the shared `history::sql` machinery (`$n` placeholders + `::text` casts)
//! that the SQLite tests cannot cover.
#![cfg(feature = "serve-history-postgres")]

use chrono::Utc;
use faucet_cli::serve::history::postgres::PostgresHistory;
use faucet_cli::serve::history::{
    Claim, DeleteOutcome, ListFilter, RunHistory, RunRecord, RunStatus,
};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// `FAUCET_TEST_POSTGRES_URL` when set, else a fresh test container.
async fn postgres() -> Option<(Option<ContainerAsync<Postgres>>, String)> {
    if let Ok(url) = std::env::var("FAUCET_TEST_POSTGRES_URL") {
        return Some((None, url));
    }
    let pg = faucet_conformance::containers::start_or_skip(
        || Postgres::default().with_tag("16-alpine"),
        &Default::default(),
    )
    .await?;
    let port = pg.get_host_port_ipv4(5432).await.expect("postgres port");
    Some((
        Some(pg),
        format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
    ))
}

fn rec(id: &str, status: RunStatus) -> RunRecord {
    let now = Utc::now();
    let mut r = RunRecord::queued(
        id.into(),
        Some("pg-test".into()),
        BTreeMap::new(),
        None,
        now,
    );
    r.status = status;
    if status.is_terminal() {
        r.finished_at = Some(now);
    }
    r
}

#[tokio::test]
async fn postgres_backend_full_lifecycle() {
    let Some((_pg, url)) = postgres().await else {
        return;
    };

    // Zero lease TTL → every run's lease is born expired, so the orphan written
    // below is recoverable by recover_orphans without waiting for a real TTL.
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::ZERO,
        "pg-test".into(),
    )
    .await
    .expect("connect postgres history (is FAUCET_TEST_POSTGRES_URL reachable?)");

    // Unique id prefix so repeated runs against a shared DB don't collide.
    let p = format!("pg-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let id = |s: &str| format!("{p}-{s}");

    // upsert → get
    h.upsert(&rec(&id("1"), RunStatus::Running)).await.unwrap();
    let got = h.get(&id("1")).await.unwrap().expect("present");
    assert_eq!(got.status, RunStatus::Running);
    assert!(h.get(&id("missing")).await.unwrap().is_none());

    // idempotency: Fresh → Replay → Conflict (atomic INSERT ON CONFLICT path)
    let key = id("k");
    let w = Duration::from_secs(3600);
    assert_eq!(
        h.claim_idempotency(&key, "fp1", &id("r1"), w)
            .await
            .unwrap(),
        Claim::Fresh
    );
    assert_eq!(
        h.claim_idempotency(&key, "fp1", &id("r2"), w)
            .await
            .unwrap(),
        Claim::Replay(id("r1"))
    );
    assert_eq!(
        h.claim_idempotency(&key, "fp2", &id("r3"), w)
            .await
            .unwrap(),
        Claim::Conflict
    );

    // list with a name filter + the keyset/cast SQL path
    let page = h
        .list(&ListFilter {
            name: Some("pg-test".into()),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(page.runs.iter().any(|r| r.run_id == id("1")));

    // Multi-status membership on the Postgres dialect (the comma-wrapped
    // `position(… in $n)` clause): exact-token matching, no substring
    // false-positives, single-status and empty-vec (= all) both correct.
    h.upsert(&rec(&id("f1"), RunStatus::Failed)).await.unwrap();
    let both = h
        .list(&ListFilter {
            name: Some("pg-test".into()),
            status: vec![RunStatus::Running, RunStatus::Failed],
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(both.runs.iter().any(|r| r.run_id == id("1")));
    assert!(both.runs.iter().any(|r| r.run_id == id("f1")));
    let only_failed = h
        .list(&ListFilter {
            name: Some("pg-test".into()),
            status: vec![RunStatus::Failed],
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(only_failed.runs.iter().all(|r| r.run_id != id("1")));
    assert!(only_failed.runs.iter().any(|r| r.run_id == id("f1")));
    let unfiltered = h
        .list(&ListFilter {
            name: Some("pg-test".into()),
            status: Vec::new(),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(unfiltered.runs.len() >= both.runs.len());

    // delete: running → 409-equivalent; terminal → deleted
    assert_eq!(
        h.delete(&id("1")).await.unwrap(),
        DeleteOutcome::StillRunning
    );
    h.upsert(&rec(&id("1"), RunStatus::Completed))
        .await
        .unwrap();
    assert_eq!(h.delete(&id("1")).await.unwrap(), DeleteOutcome::Deleted);

    // recover_orphans marks the leftover Running run failed
    h.upsert(&rec(&id("orphan"), RunStatus::Running))
        .await
        .unwrap();
    assert!(h.recover_orphans().await.unwrap() >= 1);
    assert_eq!(
        h.get(&id("orphan")).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
}

/// #697 on the Postgres dialect: a version deprecation round-trips, `newest`
/// skips it, and deleting the version clears it.
#[tokio::test]
async fn postgres_version_deprecation_round_trips() {
    use faucet_cli::serve::history::templates::{DeprecationRecord, TemplateDraft, TemplateId};
    use faucet_cli::serve::load::ConfigFormat;
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "pg-test".into(),
    )
    .await
    .expect("connect postgres history");
    let id = format!("pgv{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    for _ in 0..2 {
        h.template_register(&TemplateDraft {
            id: TemplateId::parse(&id).unwrap(),
            name: Some(id.clone()),
            description: None,
            body: format!("version: 1\nname: {id}\n"),
            format: ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
            kind: faucet_cli::hub::TemplateKind::Pipeline,
        })
        .await
        .unwrap();
    }
    let marker = DeprecationRecord {
        deprecated_at: Utc::now(),
        deprecated_by: Some("admin".into()),
        reason: Some("bad build".into()),
    };
    h.template_set_version_deprecation(&id, 2, Some(&marker))
        .await
        .unwrap();
    let st = h.template_state(&id).await.unwrap();
    assert_eq!(st.newest, Some(1));
    assert_eq!(st.deprecated_versions.len(), 1);
    let stored = &st.deprecated_versions[0].record;
    assert_eq!(stored.reason.as_deref(), Some("bad build"));
    assert_eq!(stored.deprecated_by.as_deref(), Some("admin"));
    h.template_delete(&id, None).await.unwrap();
    assert!(
        h.template_version_deprecations(&id)
            .await
            .unwrap()
            .is_empty()
    );
}

/// #856 on the Postgres dialect: test results record, read back newest
/// first, prune per version and cascade on delete; a launch keeps its
/// skipped-tests note.
#[tokio::test]
async fn postgres_template_test_results_and_launch_notes() {
    use faucet_cli::serve::history::templates::{
        RESULTS_RETAIN, TemplateDraft, TemplateId, TemplateTestResult,
    };
    use faucet_cli::serve::load::ConfigFormat;
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "pg-test".into(),
    )
    .await
    .expect("connect postgres history");
    let id = format!("pgt{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    h.template_register(&TemplateDraft {
        id: TemplateId::parse(&id).unwrap(),
        name: Some(id.clone()),
        description: None,
        body: format!("version: 1\nname: {id}\n"),
        format: ConfigFormat::Yaml,
        params: Default::default(),
        created_by: None,
        kind: faucet_cli::hub::TemplateKind::Pipeline,
    })
    .await
    .unwrap();
    let base = TemplateTestResult::new(&id, 1, "sha".into(), Default::default(), None);
    for i in 0..(RESULTS_RETAIN + 2) {
        let mut r = base.clone();
        r.result_id = format!("{}-{i}", base.result_id);
        r.recorded_at = base.recorded_at + chrono::Duration::seconds(i as i64);
        h.template_record_test(&r).await.unwrap();
    }
    let got = h.template_test_results(&id, Some(1), 100).await.unwrap();
    assert_eq!(got.len(), RESULTS_RETAIN);
    assert!(got[0].recorded_at > got[1].recorded_at);
    assert_eq!(
        h.template_test_results(&id, None, 3).await.unwrap().len(),
        3
    );
    // A label move re-checks its evidence in the same transaction (#856).
    let pass = TemplateTestResult::new(
        &id,
        1,
        "sha".into(),
        faucet_cli::template_tests::result::BundleOutcome {
            cases: vec![faucet_cli::template_tests::result::CaseResult {
                name: "c".into(),
                source: "bundle".into(),
                origin: "explicit".into(),
                passed: true,
                attempts: 1,
                duration_ms: 0,
                failure: None,
            }],
            ..Default::default()
        },
        None,
    );
    let mut pass = pass;
    pass.recorded_at = base.recorded_at + chrono::Duration::seconds(100);
    h.template_record_test(&pass).await.unwrap();
    let stale = faucet_cli::serve::history::templates::EvidenceClaim {
        result_id: base.result_id.clone(),
        body_sha256: "sha".into(),
    };
    assert!(matches!(
        h.template_launch(&id, 1, Some("root"), Some(&stale)).await,
        Err(faucet_cli::serve::history::HistoryError::StaleEvidence(_))
    ));
    let claim = faucet_cli::serve::history::templates::EvidenceClaim {
        result_id: pass.result_id.clone(),
        body_sha256: "sha".into(),
    };
    h.template_launch(&id, 1, Some("root"), Some(&claim))
        .await
        .unwrap();
    h.template_set_tag(&id, "prod", 1, Some(&claim))
        .await
        .unwrap();
    assert_eq!(
        h.template_launches(&id).await.unwrap()[0]
            .evidence
            .as_deref(),
        Some(pass.result_id.as_str())
    );
    assert_eq!(
        h.template_tag_records(&id).await.unwrap()[0]
            .evidence
            .as_deref(),
        Some(pass.result_id.as_str())
    );
    h.meta_put(&id, "1.13").await.unwrap();
    assert_eq!(h.meta_get(&id).await.unwrap().as_deref(), Some("1.13"));
    h.template_delete(&id, None).await.unwrap();
    assert!(
        h.template_test_results(&id, None, 5)
            .await
            .unwrap()
            .is_empty()
    );
}

/// #914: instances that start together all migrate a fresh database; none
/// loses the `CREATE TABLE IF NOT EXISTS` race and starts degraded.
#[tokio::test]
async fn concurrent_connects_on_a_fresh_database_all_succeed() {
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let db = format!(
        "faucet_race_{}",
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let admin = sqlx::PgPool::connect(&url).await.expect("admin connection");
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let fresh = match url.rsplit_once('/') {
        Some((base, _)) => format!("{base}/{db}"),
        None => panic!("no database in {url}"),
    };
    let connects = (0..8).map(|i| {
        let fresh = fresh.clone();
        tokio::spawn(async move {
            PostgresHistory::connect(
                &fresh,
                Duration::from_secs(3600),
                Duration::from_secs(30),
                format!("race-{i}"),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
        })
    });
    let results = futures::future::join_all(connects).await;
    let failures: Vec<String> = results
        .into_iter()
        .map(|r| r.expect("connect task"))
        .filter_map(Result::err)
        .collect();
    assert!(
        failures.is_empty(),
        "every instance migrates the fresh database: {failures:?}"
    );
}

/// The name-substring, pool and dataset filters (#998) on the Postgres
/// dialect: case-sensitive, wildcards as text, the dataset only in usage.
#[tokio::test]
async fn postgres_list_filters_match_name_pool_and_usage_dataset() {
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        "pg-filters".into(),
    )
    .await
    .expect("connect postgres history");
    let p = format!("pf{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let put = async |id: &str, name: &str, pool: &str, dataset: Option<&str>, label: bool| {
        let mut r = rec(&format!("{p}-{id}"), RunStatus::Completed);
        r.name = Some(format!("{p}-{name}"));
        r.pool = pool.into();
        if label {
            r.labels.insert("dataset_id".into(), format!("{p}-ds"));
        }
        r.invocations
            .push(faucet_cli::serve::history::InvocationRecord {
                row_id: "r".into(),
                parent_record_key: None,
                run_id: None,
                records_written: 1,
                duration_ms: 0,
                error: None,
                usage: dataset.map(|d| faucet_cli::usage::UsageRecord {
                    run_id: id.into(),
                    pipeline: "x".into(),
                    row: "r".into(),
                    source_kind: "file".into(),
                    sink_kind: "file".into(),
                    dataset_id: Some(format!("{p}-{d}")),
                    dataset_uri: None,
                    recorded_at: Utc::now(),
                    duration_ms: 0,
                    failed: false,
                    usage: Default::default(),
                    cost: Default::default(),
                    tenant: None,
                }),
                batches: None,
                source_lag: None,
                rows: None,
                skipped: false,
                status: None,
                feed_error: None,
            });
        h.upsert(&r).await.unwrap();
    };
    put("a", "orders", "default", Some("ds"), false).await;
    put("b", "Orders-50%_off", "gpu", Some("DS"), false).await;
    put("c", "customers", "gpu", None, true).await;
    let ids = async |f: ListFilter| {
        let mut ids: Vec<String> = h
            .list(&ListFilter {
                limit: 50,
                ..f.clone()
            })
            .await
            .unwrap()
            .runs
            .into_iter()
            .map(|r| r.run_id.trim_start_matches(&format!("{p}-")).to_string())
            .collect();
        ids.sort();
        let counted: u64 = h.count_by_status(&f).await.unwrap().values().sum();
        assert_eq!(counted as usize, ids.len(), "{f:?}");
        ids
    };
    let named = |n: &str| ListFilter {
        name_contains: Some(format!("{p}-{n}")),
        ..Default::default()
    };
    assert_eq!(ids(named("Orders")).await, ["b"]);
    assert_eq!(ids(named("Orders-50%_")).await, ["b"]);
    assert!(ids(named("O_ders")).await.is_empty());
    let mut f = named("");
    f.pool = Some("gpu".into());
    assert_eq!(ids(f).await, ["b", "c"]);
    let ds = ListFilter {
        dataset_id: Some(format!("{p}-ds")),
        ..Default::default()
    };
    assert_eq!(ids(ds).await, ["a"]);
}

/// Policy versions append once per (scope, version), list newest first, and
/// delete per scope — on Postgres's own statements.
#[tokio::test]
async fn postgres_policy_versions_append_once_list_and_delete() {
    use faucet_cli::serve::history::policies::PolicyVersionRecord;
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        "pg-policy".into(),
    )
    .await
    .expect("connect postgres history");
    // A scope of its own, so a shared database does not collide.
    let scope = format!("tenant:p{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let version = |version: u32| PolicyVersionRecord {
        scope: scope.clone(),
        version,
        spec: serde_json::json!({ "rules": [] }),
        created_at: Utc::now(),
        created_by: "alice".into(),
        approved_by: None,
        reason: Some(format!("v{version}")),
        change_id: None,
        restores: None,
    };
    assert!(h.policy_version_insert(&version(1)).await.unwrap());
    assert!(h.policy_version_insert(&version(2)).await.unwrap());
    assert!(!h.policy_version_insert(&version(2)).await.unwrap());
    let all: Vec<u32> = h
        .policy_versions(&scope, 0)
        .await
        .unwrap()
        .iter()
        .map(|r| r.version)
        .collect();
    assert_eq!(all, vec![2, 1]);
    assert_eq!(h.policy_versions(&scope, 1).await.unwrap().len(), 1);
    assert_eq!(
        h.policy_version_get(&scope, 1)
            .await
            .unwrap()
            .unwrap()
            .reason
            .as_deref(),
        Some("v1")
    );
    assert!(h.policy_version_get(&scope, 9).await.unwrap().is_none());
    assert_eq!(h.policy_versions_delete(&scope).await.unwrap(), 2);
    assert!(h.policy_versions(&scope, 0).await.unwrap().is_empty());
}

fn tenant_rec(id: &str) -> faucet_cli::serve::history::tenants::TenantRecord {
    let now = Utc::now();
    faucet_cli::serve::history::tenants::TenantRecord {
        id: id.into(),
        name: None,
        labels: BTreeMap::new(),
        limits: Default::default(),
        notifications: Vec::new(),
        notifications_sealed: None,
        suspended: false,
        created_at: now,
        updated_at: now,
        created_by: "t".into(),
    }
}

fn connection_rec(
    tenant: &str,
    sealed: &str,
) -> faucet_cli::serve::history::tenants::ConnectionRecord {
    use faucet_cli::serve::history::tenants::{ConnectionRecord, ConnectionStatus, new_generation};
    let now = Utc::now();
    ConnectionRecord {
        tenant: tenant.into(),
        name: "crm".into(),
        provider_type: "static".into(),
        connect_provider: None,
        sealed: sealed.into(),
        status: ConnectionStatus::Active,
        reauth_reason: None,
        created_at: now,
        updated_at: now,
        updated_by: "t".into(),
        generation: new_generation(),
    }
}

/// A connection written while its tenant is being deleted never outlives
/// the tenant, whichever of the two takes the tenant row first.
#[tokio::test]
#[allow(clippy::disallowed_methods)] // waits a fixed window to show a write is blocked
async fn a_connection_write_racing_a_tenant_delete_leaves_nothing() {
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = std::sync::Arc::new(
        PostgresHistory::connect(
            &url,
            Duration::from_secs(3600),
            Duration::from_secs(30),
            "pg-race".into(),
        )
        .await
        .expect("connect postgres history"),
    );
    let admin = sqlx::PgPool::connect(&url).await.expect("admin connection");
    let p = format!("race{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));

    assert!(
        !h.connection_insert_new(&connection_rec(&format!("{p}-none"), "x"))
            .await
            .unwrap()
    );
    let plain = format!("{p}-plain");
    h.tenant_upsert(&tenant_rec(&plain)).await.unwrap();
    assert!(
        h.connection_insert_new(&connection_rec(&plain, "a"))
            .await
            .unwrap()
    );
    assert!(
        !h.connection_insert_new(&connection_rec(&plain, "b"))
            .await
            .unwrap()
    );
    assert!(
        h.connection_upsert_in_tenant(&connection_rec(&plain, "c"))
            .await
            .unwrap()
    );
    assert_eq!(
        h.connection_get(&plain, "crm")
            .await
            .unwrap()
            .unwrap()
            .sealed,
        "c"
    );

    // The delete holds the tenant row first: the write waits, then writes nothing.
    let first = format!("{p}-delete-first");
    h.tenant_upsert(&tenant_rec(&first)).await.unwrap();
    let mut tx = admin.begin().await.unwrap();
    sqlx::query("DELETE FROM faucet_tenants WHERE id=$1")
        .bind(&first)
        .execute(&mut *tx)
        .await
        .unwrap();
    let writer = {
        let h = h.clone();
        let rec = connection_rec(&first, "late");
        tokio::spawn(async move { h.connection_insert_new(&rec).await })
    };
    let upserter = {
        let h = h.clone();
        let rec = connection_rec(&first, "late");
        tokio::spawn(async move { h.connection_upsert_in_tenant(&rec).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!writer.is_finished(), "the write waits on the tenant row");
    sqlx::query("DELETE FROM faucet_tenant_connections WHERE tenant=$1")
        .bind(&first)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(!writer.await.unwrap().unwrap());
    assert!(!upserter.await.unwrap().unwrap());
    assert!(h.connection_list(&first).await.unwrap().is_empty());

    // The write holds the tenant row first: the delete waits, then removes it.
    let second = format!("{p}-write-first");
    h.tenant_upsert(&tenant_rec(&second)).await.unwrap();
    let mut tx = admin.begin().await.unwrap();
    sqlx::query("SELECT id FROM faucet_tenants WHERE id=$1 FOR KEY SHARE")
        .bind(&second)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO faucet_tenant_connections (tenant, name, status, updated_at, body) \
         VALUES ($1, 'crm', 'active', '2026-01-01T00:00:00Z', '{}')",
    )
    .bind(&second)
    .execute(&mut *tx)
    .await
    .unwrap();
    let deleter = {
        let h = h.clone();
        let id = second.clone();
        tokio::spawn(async move { h.tenant_delete(&id).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!deleter.is_finished(), "the delete waits on the tenant row");
    tx.commit().await.unwrap();
    assert!(deleter.await.unwrap().unwrap());
    let left: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM faucet_tenant_connections WHERE tenant=$1")
            .bind(&second)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(left, 0, "no connection outlives its tenant");

    // Real calls racing each other leave either the tenant or nothing.
    for i in 0..40 {
        let id = format!("{p}-loop{i}");
        h.tenant_upsert(&tenant_rec(&id)).await.unwrap();
        let rec = connection_rec(&id, "x");
        let (_, deleted) = tokio::join!(
            async {
                if i % 2 == 0 {
                    h.connection_insert_new(&rec).await.unwrap()
                } else {
                    h.connection_upsert_in_tenant(&rec).await.unwrap()
                }
            },
            h.tenant_delete(&id)
        );
        assert!(deleted.unwrap());
        assert!(
            h.connection_list(&id).await.unwrap().is_empty(),
            "iteration {i} left a connection without a tenant"
        );
    }
}

/// The database clock, conditional launches, a fenced run start and an atomic
/// split on Postgres.
#[tokio::test]
async fn postgres_db_clock_fenced_start_and_atomic_split() {
    use faucet_cli::serve::history::{CoordinateOutcome, HistoryError, ShardInsert};
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let connect = |me: &str| {
        let url = url.clone();
        let me = me.to_string();
        async move {
            PostgresHistory::connect(&url, Duration::from_secs(60), Duration::from_secs(30), me)
                .await
                .unwrap()
        }
    };
    let a = connect("pg-a").await;
    let b = connect("pg-b").await;
    let db = a.db_now().await.unwrap();
    assert!((db - Utc::now()).num_seconds().abs() < 10, "{db}");

    let mut r = rec("pg-split", RunStatus::Running);
    a.upsert(&r).await.unwrap();
    assert!(a.start_owned(&r).await.unwrap());
    assert!(!b.start_owned(&r).await.unwrap());
    r.status = RunStatus::Sharded;
    let shard = |id: &str| ShardInsert {
        shard_id: id.into(),
        descriptor: serde_json::json!({ "id": id }),
        size_estimate: None,
    };
    assert!(
        a.coordinate_shards(&r, &[shard("0"), shard("0")])
            .await
            .is_err()
    );
    assert_eq!(
        a.get("pg-split").await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        b.coordinate_shards(&r, &[shard("0")]).await.unwrap(),
        CoordinateOutcome::NotOwner
    );
    let many: Vec<ShardInsert> = (0..700).map(|i| shard(&i.to_string())).collect();
    assert_eq!(
        a.coordinate_shards(&r, &many).await.unwrap(),
        CoordinateOutcome::Sharded { inserted: 700 }
    );
    assert_eq!(
        a.coordinate_shards(&r, &many).await.unwrap(),
        CoordinateOutcome::Sharded { inserted: 0 }
    );
    a.request_cancel("pg-split").await.unwrap();
    assert_eq!(a.shard_progress("pg-split").await.unwrap().cancelled, 700);
    assert_eq!(
        a.cancel_flagged_shards().await.unwrap(),
        0,
        "nothing left pending"
    );

    let rep = a.reclaim_orphans(3).await.unwrap();
    assert_eq!(
        (rep.requeued, rep.failed),
        (0, 0),
        "a live lease on the database clock"
    );

    let draft = faucet_cli::serve::history::templates::TemplateDraft {
        id: faucet_cli::serve::history::templates::TemplateId::parse("pg-launch").unwrap(),
        kind: faucet_cli::hub::TemplateKind::Pipeline,
        name: None,
        description: None,
        body: "version: 1\n".into(),
        format: faucet_cli::serve::load::ConfigFormat::Yaml,
        params: Default::default(),
        created_by: None,
    };
    for _ in 0..2 {
        a.template_register(&draft).await.unwrap();
    }
    a.template_launch("pg-launch", 1, None, None).await.unwrap();
    assert!(matches!(
        a.template_launch_expecting("pg-launch", 2, None, None, Some(None))
            .await,
        Err(HistoryError::LaunchRaced { found: Some(1), .. })
    ));
    assert_eq!(
        a.template_launch_expecting("pg-launch", 2, None, None, Some(Some(1)))
            .await
            .unwrap(),
        Some(2)
    );
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0))
}

/// Issue #1100 C4: the lease statements run in a transaction with its own
/// statement timeout, the lost-lease check is one statement, and a renewal
/// blocked behind another session's row lock fails instead of waiting.
#[tokio::test]
async fn postgres_lease_statements_are_bounded_and_batched() {
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let ttl = Duration::from_millis(1500);
    let (a_id, b_id) = (unique("lease-a"), unique("lease-b"));
    let a = PostgresHistory::connect(&url, Duration::from_secs(3600), ttl, a_id.clone())
        .await
        .expect("connect a");
    let b = PostgresHistory::connect(&url, Duration::from_secs(3600), ttl, b_id)
        .await
        .expect("connect b");
    let (mine, other) = (unique("run-mine"), unique("run-other"));
    a.upsert(&rec(&mine, RunStatus::Running)).await.unwrap();
    a.upsert(&rec(&other, RunStatus::Running)).await.unwrap();

    assert_eq!(a.renew_leases().await.unwrap(), 2);
    assert_eq!(
        a.renew_leases_except(std::slice::from_ref(&other))
            .await
            .unwrap(),
        1
    );
    assert_eq!(a.renew_shard_leases().await.unwrap(), 0);

    let keys = vec![
        mine.clone(),
        other.clone(),
        unique("run-missing"),
        format!("{mine}::0"),
    ];
    assert!(a.leases_lost(&keys).await.unwrap().is_empty());
    let mut lost = b.leases_lost(&keys).await.unwrap();
    lost.sort();
    let mut expected = vec![mine.clone(), other.clone()];
    expected.sort();
    assert_eq!(lost, expected);
    for key in [&mine, &other] {
        assert!(b.lease_lost(key).await.unwrap());
        assert!(!a.lease_lost(key).await.unwrap());
    }

    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("UPDATE faucet_serve_runs SET name = name WHERE run_id = $1")
        .bind(&mine)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let blocked = a.renew_leases().await;
    let waited = started.elapsed();
    assert!(blocked.is_err(), "a renewal behind a row lock is cut off");
    assert!(
        waited >= Duration::from_millis(900) && waited < Duration::from_secs(10),
        "cut at the statement timeout (1s for a 1.5s lease), waited {waited:?}"
    );
    blocker.rollback().await.unwrap();
    assert_eq!(a.renew_leases().await.unwrap(), 2, "and works again after");
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(timeout, "0", "no session keeps the lease timeout");
}

/// Issue #1100 C3: a batch of log lines stores what line-by-line inserts
/// store, on Postgres too, including when keys are already taken.
#[tokio::test]
async fn postgres_run_log_batches_match_line_by_line_inserts() {
    use faucet_cli::serve::history::{RUN_LOG_TRUNCATED_SEQ, RunLogLine};
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        unique("logs"),
    )
    .await
    .expect("connect");
    let line = |seq: u64, text: &str| RunLogLine {
        seq,
        ts: "2026-01-01T00:00:00.000Z".into(),
        level: "INFO".into(),
        line: text.into(),
        attrs: if seq.is_multiple_of(5) {
            [("row".to_string(), format!("r{seq}"))].into()
        } else {
            Default::default()
        },
    };
    let mut many: Vec<RunLogLine> = (1..=1203u64).map(|i| line(i, &format!("l{i}"))).collect();
    many.push(line(RUN_LOG_TRUNCATED_SEQ, "truncated"));
    let clash = vec![
        line(10, "mine 10"),
        line(12, "mine 12"),
        line(12, "mine 12 again"),
        line(40, "mine 40"),
    ];
    for (name, before, lines) in [
        ("plain", Vec::new(), many),
        (
            "clash",
            vec![line(10, "theirs 10"), line(11, "theirs 11")],
            clash,
        ),
    ] {
        let (batched, single) = (unique(&format!("{name}-b")), unique(&format!("{name}-s")));
        for run in [&batched, &single] {
            for l in &before {
                h.record_run_logs(run, std::slice::from_ref(l))
                    .await
                    .unwrap();
            }
        }
        h.record_run_logs(&batched, &lines).await.unwrap();
        for l in &lines {
            h.record_run_logs(&single, std::slice::from_ref(l))
                .await
                .unwrap();
        }
        let stored = |run: String| {
            let h = &h;
            async move {
                let page = h.list_run_logs(&run, None, 5000).await.unwrap();
                let rows: Vec<(u64, String, std::collections::BTreeMap<String, String>)> = page
                    .lines
                    .iter()
                    .map(|l| (l.seq, l.line.clone(), l.attrs.clone()))
                    .collect();
                (rows, page.truncated)
            }
        };
        let (b_rows, b_trunc) = stored(batched).await;
        let (s_rows, s_trunc) = stored(single).await;
        assert_eq!(b_rows, s_rows, "{name}");
        assert_eq!(b_trunc, s_trunc, "{name}");
        if name == "clash" {
            let texts: Vec<&str> = b_rows.iter().map(|r| r.1.as_str()).collect();
            assert_eq!(
                texts,
                vec![
                    "theirs 10",
                    "theirs 11",
                    "mine 10",
                    "mine 12",
                    "mine 12 again",
                    "mine 40"
                ]
            );
        } else {
            assert_eq!(b_rows.len(), 1203);
            assert!(b_trunc);
        }
    }
}

/// Issue #1100 C5: each rewritten delete parses on Postgres and removes
/// exactly the rows its `NOT IN` form removed.
#[tokio::test]
async fn postgres_exists_deletes_match_the_not_in_deletes() {
    use faucet_cli::serve::history::sql::{Dialect, Stmts};
    let Some((_pg, url)) = postgres().await else {
        return;
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        unique("purge"),
    )
    .await
    .expect("connect");
    let tag = unique("c5");
    let (live, done, gone) = (
        format!("{tag}-live"),
        format!("{tag}-done"),
        format!("{tag}-gone"),
    );
    h.upsert(&rec(&live, RunStatus::Running)).await.unwrap();
    h.upsert(&rec(&done, RunStatus::Completed)).await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let s = Stmts::new(Dialect::Postgres);
    let cases: [(&str, &str, &String); 7] = [
        (
            "faucet_serve_instance_caps",
            "DELETE FROM faucet_serve_instance_caps WHERE instance_id NOT IN \
             (SELECT instance_id FROM faucet_serve_instances)",
            &s.prune_instance_caps,
        ),
        (
            "faucet_serve_shards",
            "DELETE FROM faucet_serve_shards WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
            &s.purge_orphan_shards,
        ),
        (
            "faucet_serve_audit_tenants",
            "DELETE FROM faucet_serve_audit_tenants WHERE id NOT IN (SELECT id FROM faucet_serve_audit)",
            &s.purge_audit_tenants,
        ),
        (
            "faucet_serve_run_budgets",
            "DELETE FROM faucet_serve_run_budgets WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
            &s.purge_orphan_budgets,
        ),
        (
            "faucet_serve_run_secrets",
            "DELETE FROM faucet_serve_run_secrets WHERE run_id IN \
             (SELECT run_id FROM faucet_serve_runs WHERE status IN ('completed','failed','cancelled')) \
             OR (stored_at < $1 AND run_id NOT IN (SELECT run_id FROM faucet_serve_runs))",
            &s.run_secrets_purge,
        ),
        (
            "faucet_tenant_runs",
            "DELETE FROM faucet_tenant_runs WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
            &s.purge_orphan_tenant_runs,
        ),
        (
            "faucet_serve_run_labels",
            "DELETE FROM faucet_serve_run_labels WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
            &s.purge_orphan_run_labels,
        ),
    ];
    let seed = [
        format!(
            "INSERT INTO faucet_serve_instance_caps (instance_id, state_format) VALUES ('{gone}','1')"
        ),
        format!(
            "INSERT INTO faucet_serve_shards (run_id, shard_id, descriptor, status, attempt) VALUES \
             ('{live}','0','{{}}','pending','0'),('{gone}','0','{{}}','pending','0')"
        ),
        format!("INSERT INTO faucet_serve_audit_tenants (id, tenant) VALUES ('{gone}','t1')"),
        format!(
            "INSERT INTO faucet_serve_run_budgets (run_id, records, bytes, last_records, last_bytes, updated_at) \
             VALUES ('{live}',0,0,0,0,'t'),('{gone}',0,0,0,0,'t')"
        ),
        format!(
            "INSERT INTO faucet_serve_run_secrets (run_id, stored_at, sealed) VALUES \
             ('{live}','2020','s'),('{done}','2999','s'),('{gone}','2020','s'),('{gone}-new','2999','s')"
        ),
        format!(
            "INSERT INTO faucet_tenant_runs (run_id, tenant) VALUES ('{live}','t1'),('{gone}','t1')"
        ),
        format!(
            "INSERT INTO faucet_serve_run_labels (run_id, key, value) VALUES ('{live}','k','v'),('{gone}','k','v')"
        ),
    ];
    for (table, old, new) in cases {
        let mut left = Vec::new();
        for stmt in [old, new.as_str()] {
            let mut tx = pool.begin().await.unwrap();
            for insert in &seed {
                if insert.contains(&format!("INTO {table} ")) {
                    sqlx::query(insert).execute(&mut *tx).await.unwrap();
                }
            }
            let mut q = sqlx::query(stmt);
            if stmt.contains("$1") {
                q = q.bind("2500");
            }
            let deleted = q
                .execute(&mut *tx)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {e}"))
                .rows_affected();
            let key = match table {
                "faucet_serve_instance_caps" => "instance_id",
                "faucet_serve_audit_tenants" => "id",
                _ => "run_id",
            };
            let survivors: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {key} FROM {table} WHERE {key} LIKE $1 ORDER BY 1"
            ))
            .bind(format!("{tag}%"))
            .fetch_all(&mut *tx)
            .await
            .unwrap();
            tx.rollback().await.unwrap();
            left.push((deleted >= 1, survivors));
        }
        assert_eq!(left[0], left[1], "{table}");
        assert!(left[0].0, "{table}: the seed has an orphan to delete");
        assert!(
            !left[0].1.iter().any(|k| k.starts_with(&gone)) || table == "faucet_serve_run_secrets",
            "{table}: {:?}",
            left[0].1
        );
    }
}
