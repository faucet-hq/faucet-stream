//! A Prometheus recorder with an idle timeout, installed inside a runtime:
//! `/metrics` serves the series, and a second install is reported as already
//! installed. Its own test binary: the recorder is process-global.
#![cfg(feature = "observability-install")]
#![allow(clippy::disallowed_methods)] // waits for the listener with a blocking sleep

use faucet_core::{ObservabilityConfig, PrometheusConfig, install_observability};
use std::io::{Read, Write};
use std::time::Duration;

fn free_listen() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}

fn scrape(listen: &str) -> String {
    for _ in 0..50 {
        if let Ok(mut s) = std::net::TcpStream::connect(listen) {
            s.write_all(b"GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n")
                .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            return out;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("no /metrics listener at {listen}");
}

fn config(listen: &str) -> ObservabilityConfig {
    ObservabilityConfig {
        prometheus: Some(PrometheusConfig {
            listen: listen.to_owned(),
            buckets: None,
            idle_timeout: Some(Duration::from_secs(3600)),
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_timeout_recorder_serves_metrics_and_installs_once() {
    let listen = free_listen();
    let report = install_observability(&config(&listen)).unwrap();
    assert_eq!(report.prometheus_listen.as_deref(), Some(listen.as_str()));
    metrics::counter!("faucet_idle_timeout_probe_total").increment(1);
    let body = tokio::task::spawn_blocking(move || scrape(&listen))
        .await
        .unwrap();
    assert!(body.contains("faucet_idle_timeout_probe_total"), "{body}");

    let again = install_observability(&config(&free_listen())).unwrap();
    assert!(again.prometheus_already_installed);
    assert!(again.prometheus_listen.is_none());
}
