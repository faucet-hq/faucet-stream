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
