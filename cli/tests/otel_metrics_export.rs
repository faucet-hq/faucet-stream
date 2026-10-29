#![cfg(feature = "otel")]
//! The `metrics`-facade → OpenTelemetry bridge (#980), end to end: counters,
//! gauges and histograms recorded through `metrics::*!` arrive at an OTLP
//! collector (gRPC and HTTP/protobuf) with the faucet metric name, labels as
//! attributes, the expected instrument kind and value, and the SDK's default
//! histogram buckets. A bridge release that changes any of that fails here.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

#[path = "support/otlp_mock.rs"]
mod otlp_mock;

use faucet_core::observability::otel::{OtelConfig, OtelProtocol, build_meter_provider};
use otlp_mock::{MetricsCollector, Point, start_metrics_grpc, start_metrics_http, wait_for};

const DEFAULT_BOUNDS: [f64; 15] = [
    0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0,
    10000.0,
];

const SOURCE: [(&str, &str); 3] = [("pipeline", "p"), ("row", "r1"), ("connector", "rest")];

async fn export_and_check(protocol: OtelProtocol, endpoint: String, collector: MetricsCollector) {
    let cfg = OtelConfig {
        endpoint,
        protocol,
        service_name: "faucet-test".into(),
        metric_interval_secs: 3600,
        ..Default::default()
    };
    let (provider, recorder) = build_meter_provider(&cfg).expect("meter provider builds");

    metrics::with_local_recorder(&recorder, || {
        metrics::describe_counter!(
            "faucet_source_records_total",
            "Records the source produced."
        );
        let c = metrics::counter!(
            "faucet_source_records_total",
            "pipeline" => "p", "row" => "r1", "connector" => "rest"
        );
        c.increment(2);
        c.increment(3);
        metrics::counter!(
            "faucet_source_records_total",
            "pipeline" => "p", "row" => "r2", "connector" => "rest"
        )
        .increment(7);

        let a = metrics::counter!("faucet_pipeline_pages_skipped_total", "pipeline" => "p");
        a.absolute(10);
        // A lower absolute value never moves a monotonic counter back.
        a.absolute(4);

        let g = metrics::gauge!("faucet_pipeline_in_flight", "pipeline" => "p", "row" => "r1");
        g.set(3.0);
        g.increment(2.0);
        g.decrement(1.0);

        let h = metrics::histogram!(
            "faucet_sink_write_duration_seconds",
            "pipeline" => "p", "row" => "r1", "connector" => "file"
        );
        h.record(0.5);
        h.record(7.0);
        h.record(30.0);
    });
    provider.force_flush().expect("flush reaches the collector");

    assert!(
        wait_for(10, || collector.request_count() > 0).await,
        "no metrics export reached the collector"
    );

    let rec = collector
        .last("faucet_source_records_total", &SOURCE)
        .expect("counter exported with its labels");
    assert_eq!(rec.service, "faucet-test");
    assert_eq!(rec.scope, "faucet-test", "meter scope is the service name");
    assert_eq!(
        rec.point,
        Point::Sum {
            value: 5.0,
            monotonic: true
        }
    );
    let r2 = collector
        .last(
            "faucet_source_records_total",
            &[("pipeline", "p"), ("row", "r2"), ("connector", "rest")],
        )
        .expect("second label set is its own series");
    assert_eq!(
        r2.point,
        Point::Sum {
            value: 7.0,
            monotonic: true
        }
    );

    let skipped = collector
        .last("faucet_pipeline_pages_skipped_total", &[("pipeline", "p")])
        .expect("absolute counter exported");
    assert_eq!(
        skipped.point,
        Point::Sum {
            value: 10.0,
            monotonic: true
        }
    );

    let gauge = collector
        .last(
            "faucet_pipeline_in_flight",
            &[("pipeline", "p"), ("row", "r1")],
        )
        .expect("gauge exported");
    assert_eq!(gauge.point, Point::Gauge(4.0));

    let hist = collector
        .last(
            "faucet_sink_write_duration_seconds",
            &[("pipeline", "p"), ("row", "r1"), ("connector", "file")],
        )
        .expect("histogram exported");
    match hist.point {
        Point::Histogram {
            count,
            sum,
            bounds,
            bucket_counts,
        } => {
            assert_eq!(count, 3);
            assert!((sum - 37.5).abs() < 1e-9, "sum {sum}");
            assert_eq!(bounds, DEFAULT_BOUNDS.to_vec());
            assert_eq!(bucket_counts.len(), DEFAULT_BOUNDS.len() + 1);
            // 0.5 → (0, 5], 7 → (5, 10], 30 → (25, 50].
            assert_eq!(&bucket_counts[..5], &[0, 1, 1, 0, 1]);
            assert_eq!(bucket_counts.iter().sum::<u64>(), 3);
        }
        other => panic!("expected a histogram, got {other:?}"),
    }

    let _ = provider.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_facade_exports_over_grpc() {
    let (collector, endpoint) = start_metrics_grpc().await;
    export_and_check(OtelProtocol::Grpc, endpoint, collector).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_facade_exports_over_http() {
    let (collector, endpoint, _server) = start_metrics_http().await;
    export_and_check(OtelProtocol::Http, endpoint, collector).await;
}
