//! Integration test against a running Marquez (set FAUCET_MARQUEZ_URL to run).
//! e.g. `docker run -p 5000:5000 marquezproject/marquez` then
//! `FAUCET_MARQUEZ_URL=http://localhost:5000/api/v1/lineage cargo test -p faucet-lineage --test marquez_integration`.

#[tokio::test]
async fn emits_to_marquez() {
    let Ok(url) = std::env::var("FAUCET_MARQUEZ_URL") else {
        eprintln!("skipping: set FAUCET_MARQUEZ_URL to run");
        return;
    };
    use faucet_lineage::*;
    let cfg = LineageConfig {
        kind: Default::default(),
        namespace: "faucet-it".into(),
        transport: Transport::Http {
            url,
            timeout_secs: std::time::Duration::from_secs(10),
            auth: None,
        },
        job_name: "it-job".into(),
        parent_job: None,
        include_column_lineage: false,
        include_schema_facet: true,
        include_source_code_facet: false,
        emit_on: Default::default(),
        sample_records: 10,
        heartbeat_interval: std::time::Duration::from_secs(30),
    };
    let em = LineageEmitter::new(cfg).unwrap();
    let ctx = RunLifecycle {
        job_namespace: "faucet-it".into(),
        job_name: "it-job".into(),
        run_id: uuid::Uuid::now_v7().to_string(),
        parent: None,
        inputs: vec![DatasetRef {
            namespace: "faucet-it".into(),
            name: "postgres://h/db?table=t".into(),
        }],
        output: DatasetRef {
            namespace: "faucet-it".into(),
            name: "bigquery://p.d.t".into(),
        },
        started_at: chrono::Utc::now(),
        finished_at: None,
        records: 0,
        error: None,
        input_schemas: Vec::new(),
        output_schema: None,
        column_lineage: None,
        source_code: None,
    };
    em.emit(EventType::Start, &ctx).await;
    let mut done = ctx.clone();
    done.finished_at = Some(chrono::Utc::now());
    done.records = 42;
    em.emit(EventType::Complete, &done).await;
    // Marquez ingests async; assert the job is queryable.
    let base = std::env::var("FAUCET_MARQUEZ_URL")
        .unwrap()
        .replace("/api/v1/lineage", "");
    let jobs: serde_json::Value = reqwest::get(format!("{base}/api/v1/namespaces/faucet-it/jobs"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(jobs.to_string().contains("it-job"));
}
