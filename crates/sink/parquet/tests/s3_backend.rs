#![allow(deprecated)]
//! An S3 destination is the shared file sink over the S3 backend (#886): its
//! objects go up with the S3 API and read back as Parquet. Driven against a
//! mock S3 endpoint, so no container is needed.

use faucet_core::Sink;
use faucet_sink_parquet::{
    ParquetCompression, ParquetDestination, ParquetS3Destination, ParquetSink, ParquetSinkConfig,
    SchemaSource,
};
use serde_json::json;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(endpoint: &str) -> ParquetSinkConfig {
    ParquetSinkConfig::new(ParquetDestination::S3(ParquetS3Destination {
        bucket: "bkt".into(),
        prefix: "out/".into(),
        region: Some("us-east-1".into()),
        endpoint_url: Some(endpoint.into()),
        allow_http: true,
    }))
    .compression(ParquetCompression::Zstd)
}

fn credentials() {
    // SAFETY: constants; concurrent writes converge to the same value.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_s3_destination_publishes_one_parquet_object_per_flush() {
    credentials();
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/bkt/out/[0-9a-f-]+-00001\.parquet$"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let sink = ParquetSink::new(config(&server.uri())).await.unwrap();
    assert_eq!(sink.connector_name(), "parquet");
    assert_eq!(sink.dataset_uri(), "s3://bkt/out/");
    assert!(sink.config_schema().is_object());
    assert_eq!(
        sink.batch_atomicity(),
        faucet_core::BatchAtomicity::Atomic,
        "one object per flush, nothing visible before it"
    );
    assert_eq!(
        sink.capabilities().schema_report(),
        faucet_core::SchemaReport::Undeclared
    );
    sink.set_call_context(faucet_core::CallContext::new());
    sink.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Sink,
            "p",
            "r",
            "parquet",
        ),
    ));
    let rows: Vec<_> = (0..3)
        .map(|i| json!({"id": i, "name": format!("n{i}")}))
        .collect();
    assert_eq!(sink.write_batch(&rows).await.unwrap(), 3);
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    sink.flush().await.unwrap();
    sink.complete_run().await.unwrap();
    assert!(sink.local_outputs().await.is_empty());

    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .find(|r| r.method.as_str() == "PUT")
        .expect("one PUT");
    let (_, batches) = faucet_core::file_format::parquet_io::read_bytes(
        bytes::Bytes::from(put.body.clone()),
        &Default::default(),
        0,
        "o",
    )
    .unwrap();
    let back: Vec<_> = batches
        .iter()
        .flat_map(|b| faucet_core::columnar::record_batch_to_values(b).unwrap())
        .collect();
    assert_eq!(back.len(), 3);
    assert_eq!(back[2]["name"], "n2");
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread")]
async fn an_s3_destination_takes_arrow_batches() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    credentials();
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let sink = ParquetSink::new(config(&server.uri())).await.unwrap();
    assert!(sink.supports_columnar());
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    assert_eq!(sink.write_batch_columnar(&batch).await.unwrap(), 2);
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn an_explicit_schema_is_refused_for_an_s3_destination() {
    credentials();
    let cfg = config("http://127.0.0.1:9").schema(SchemaSource::Explicit {});
    let err = match ParquetSink::new(cfg).await {
        Ok(_) => panic!("an explicit schema must be refused"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("explicit parquet schemas"), "{err}");
}
