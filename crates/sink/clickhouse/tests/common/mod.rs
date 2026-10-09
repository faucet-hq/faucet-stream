//! Shared ClickHouse container harness for the integration and conformance
//! suites.

#![allow(dead_code)]

use faucet_conformance::containers::{self, ReadyProbe, StartError, StartOptions};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;

/// Start a ClickHouse server and wait until its HTTP interface answers
/// `/ping`; returns the container and its base URL.
pub async fn start_clickhouse() -> Result<(ContainerAsync<ClickHouse>, String), StartError> {
    let opts = StartOptions::default().ready(ReadyProbe::http(8123, "/ping"));
    let container = containers::start_container(ClickHouse::default, &opts).await?;
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("clickhouse host port");
    Ok((container, format!("http://127.0.0.1:{port}")))
}
