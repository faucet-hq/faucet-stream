//! The PostgreSQL state store over TLS (#967) against a server that rejects
//! every non-TLS connection.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::FaucetError;
use faucet_core::state::StateStore;
use faucet_state_postgres::PostgresStateStore;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn stores_bookmarks_over_verified_tls_and_refuses_an_unknown_ca() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    let (user, password) = images::POSTGRES_USER;
    let url =
        |query: String| format!("postgres://{user}:{password}@localhost:{port}/postgres?{query}");

    let store = PostgresStateStore::connect(&url(format!(
        "sslmode=verify-full&sslrootcert={}",
        tls.ca_path().display()
    )))
    .await
    .expect("verified TLS connect");
    store.ensure_table().await.unwrap();
    store.put("p::r", &json!({"cursor": 7})).await.unwrap();
    assert_eq!(store.get("p::r").await.unwrap(), Some(json!({"cursor": 7})));

    for query in [
        "sslmode=disable".to_string(),
        format!(
            "sslmode=verify-full&sslrootcert={}",
            tls.other_ca_path().display()
        ),
    ] {
        match PostgresStateStore::connect(&url(query.clone())).await {
            Err(FaucetError::State(msg)) => assert!(msg.contains("connection failed"), "{msg}"),
            Err(other) => panic!("{query}: unexpected error {other:?}"),
            Ok(_) => panic!("{query} must be refused"),
        }
    }
}
