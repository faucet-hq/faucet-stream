//! A URI the driver's own parser cannot read is refused before it sees it.

/// The driver panics on a `%` followed by a non-ASCII character; the sink
/// answers with an error instead.
#[tokio::test]
async fn a_uri_the_driver_cannot_parse_is_an_error() {
    let config: faucet_sink_mongodb::MongoSinkConfig =
        faucet_core::serde_json::from_value(faucet_core::serde_json::json!({
            "connection_uri": "mongodb://u%a\u{E9}@db.example.com/",
            "database": "app",
            "collection": "c"
        }))
        .unwrap();
    let err = faucet_sink_mongodb::MongoSink::new(config)
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("hexadecimal"), "{err}");
}
