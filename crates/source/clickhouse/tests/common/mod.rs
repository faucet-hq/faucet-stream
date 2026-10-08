//! Shared ClickHouse container harness for the integration and conformance
//! suites.

#![allow(dead_code)]

use std::time::Duration;

use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Budget for pulling and starting the server on a loaded CI runner; the
/// testcontainers default (60 s) is not enough there.
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

/// How long the server gets to answer `/ping` once the container is up.
pub const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Start a ClickHouse server and wait until its HTTP interface answers
/// `/ping`; returns the container and its base URL.
pub async fn start_clickhouse() -> Result<(ContainerAsync<ClickHouse>, String), String> {
    let container = ClickHouse::default()
        .with_startup_timeout(STARTUP_TIMEOUT)
        .start()
        .await
        .map_err(|e| format!("start clickhouse container: {e}"))?;
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .map_err(|e| format!("clickhouse host port: {e}"))?;
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, READY_TIMEOUT).await?;
    Ok((container, base))
}

/// Poll `GET {base}/ping` until it returns `200 Ok.` or `timeout` passes.
pub async fn wait_ready(base: &str, timeout: Duration) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let last = match client.get(format!("{base}/ping")).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().await.unwrap_or_default();
                if body.trim() == "Ok." {
                    return Ok(());
                }
                format!("unexpected body {body:?}")
            }
            Ok(resp) => format!("status {}", resp.status()),
            Err(e) => e.to_string(),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "clickhouse at {base} not ready after {timeout:?}: {last}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn wait_ready_reports_an_unreachable_server() {
    let err = wait_ready("http://127.0.0.1:1", Duration::from_millis(10))
        .await
        .unwrap_err();
    assert!(err.contains("not ready"), "{err}");
}
