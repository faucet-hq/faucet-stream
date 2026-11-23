//! A Prometheus recorder with an idle timeout installed with no tokio runtime
//! around: it serves `/metrics` from a runtime of its own. Its own test
//! binary: the recorder is process-global.
#![cfg(feature = "observability-install")]
#![allow(clippy::disallowed_methods)] // waits for the listener with a blocking sleep

use faucet_core::{ObservabilityConfig, PrometheusConfig, install_observability};
use std::io::{Read, Write};
use std::time::Duration;

#[test]
fn an_idle_timeout_recorder_runs_its_own_runtime_when_there_is_none() {
    let listen = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let cfg = ObservabilityConfig {
        prometheus: Some(PrometheusConfig {
            listen: listen.clone(),
            buckets: None,
            idle_timeout: Some(Duration::from_secs(3600)),
        }),
        ..Default::default()
    };
    let report = install_observability(&cfg).unwrap();
    assert_eq!(report.prometheus_listen.as_deref(), Some(listen.as_str()));
    metrics::counter!("faucet_idle_timeout_own_runtime_total").increment(1);
    let mut body = String::new();
    for _ in 0..50 {
        if let Ok(mut s) = std::net::TcpStream::connect(&listen) {
            s.write_all(b"GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n")
                .unwrap();
            s.read_to_string(&mut body).unwrap();
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        body.contains("faucet_idle_timeout_own_runtime_total"),
        "{body}"
    );
}
