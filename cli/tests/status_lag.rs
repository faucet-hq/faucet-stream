//! #733 end to end against a real Postgres: a CDC run records the slot's lag on
//! its status marker, `faucet status --probe` asks the slot again after
//! changes pile up behind it, a `max_lag_bytes` SLA marks the row degraded, and
//! `faucet doctor` reports a `lag` probe. Requires Docker.
#![cfg(all(feature = "source-postgres-cdc", feature = "sink-jsonl"))]

use clap::Parser;
use faucet_cli::cli::{Cli, StateLoadArgs, StatusArgs};
use faucet_cli::error::CliError;
use faucet_cli::status::Health;
use std::path::Path;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = faucet_conformance::containers::start(|| {
        Postgres::default()
            .with_host_auth()
            .with_tag("16-alpine")
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=4",
                "-c",
                "max_replication_slots=4",
            ])
    })
    .await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (
        container,
        format!("postgres://postgres@127.0.0.1:{port}/postgres"),
    )
}

async fn sql(url: &str, stmt: &str) {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute(stmt).await.expect("exec");
}

async fn run(args: &[&str]) -> Result<(), CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn status_args(cfg: &Path, probe: bool) -> StatusArgs {
    StatusArgs {
        config: Some(cfg.to_path_buf()),
        row: None,
        probe,
        load: StateLoadArgs {
            json: true,
            env_file: None,
            no_env_file: true,
            profile: None,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_lag_reaches_status_sla_and_doctor() {
    let (_pg, url) = start_postgres().await;
    sql(
        &url,
        "CREATE TABLE public.orders (id int4 PRIMARY KEY, pad text); \
         CREATE PUBLICATION orders_pub FOR TABLE public.orders;",
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("orders.yaml");
    std::fs::write(
        &cfg,
        format!(
            r#"version: 1
name: orders
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: "{url}"
      slot_name: lag_slot
      publication_name: orders_pub
      idle_timeout: 3
      status_update_interval: 1
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
sla:
  max_lag_bytes: 65536
"#,
            out = dir.path().join("out.jsonl").display(),
            state = dir.path().join("state").display(),
        ),
    )
    .unwrap();
    let cfgs = cfg.display().to_string();

    run(&["run", &cfgs])
        .await
        .expect("first run creates the slot");
    sql(
        &url,
        "INSERT INTO public.orders SELECT g, repeat('x', 256) FROM generate_series(1, 20) g;",
    )
    .await;
    run(&["run", &cfgs])
        .await
        .expect("second run reads the changes");

    let r = faucet_cli::commands::status::build(&status_args(&cfg, false))
        .await
        .unwrap();
    let lag = r.rows[0].lag.as_ref().expect("the run recorded its lag");
    assert_eq!(lag.measured, "last_run");
    assert!(lag.lag.bytes.is_some(), "{lag:?}");

    sql(
        &url,
        "INSERT INTO public.orders SELECT g, repeat('y', 512) FROM generate_series(21, 1020) g;",
    )
    .await;
    let r = faucet_cli::commands::status::build(&status_args(&cfg, true))
        .await
        .unwrap();
    let row = &r.rows[0];
    let lag = row.lag.as_ref().expect("the probe asked the slot");
    assert_eq!(lag.measured, "probe");
    assert!(lag.lag.bytes.unwrap() > 1000 * 512, "{lag:?}");
    assert_eq!(row.health, Health::Degraded, "{:?}", row.reasons);
    assert!(row.sla.iter().any(|v| v.kind == "lag"), "{:?}", row.sla);

    let err = run(&["doctor", &cfgs, "--json"]).await.unwrap_err();
    assert!(
        matches!(err, CliError::DoctorFailed { .. }),
        "the lag probe fails over max_lag_bytes: {err}"
    );
}
