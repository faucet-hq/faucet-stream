
use super::*;

#[test]
fn side_maps_variants() {
    assert!(matches!(Side::Source.err("x"), FaucetError::Source(_)));
    assert!(matches!(Side::Sink.err("x"), FaucetError::Sink(_)));
}

#[test]
fn client_missing_detection_and_timeouts() {
    assert!(is_client_missing(
        "DPI-1047: Cannot locate a 64-bit Oracle Client library"
    ));
    assert!(!is_client_missing("ORA-01017: invalid username/password"));
    assert!(CLIENT_HINT.contains("faucet-stream:full)"));
    assert_eq!(call_timeout(0), None);
    assert_eq!(call_timeout(5), Some(Duration::from_secs(5)));
}

#[test]
fn only_a_worker_thread_pool_start_failure_is_retried() {
    assert!(is_transient_pool_init(
        "OCI Error: ORA-24447: OCI worker thread pool failed to initialize"
    ));
    assert!(!is_transient_pool_init(
        "ORA-01017: invalid username/password"
    ));
    assert!(!is_transient_pool_init(
        "DPI-1047: Cannot locate a 64-bit Oracle Client library"
    ));
    assert_eq!(pool_init_backoff(1), POOL_INIT_BACKOFF);
    assert_eq!(pool_init_backoff(3), POOL_INIT_BACKOFF * 4);
    let total: Duration = (1..POOL_INIT_ATTEMPTS).map(pool_init_backoff).sum();
    assert!(total < Duration::from_secs(10), "{total:?}");
}

#[test]
fn build_pool_rejects_invalid_config_before_loading_the_client() {
    let err = build_pool_blocking(&OracleConnectionConfig::default(), 2).unwrap_err();
    assert!(matches!(err, FaucetError::Config(_)), "{err}");
}

#[tokio::test]
async fn blocking_propagates_results_and_panics() {
    assert_eq!(blocking(|| Ok(7)).await.unwrap(), 7);
    let err = blocking::<(), _>(|| Err(FaucetError::Source("boom".into())))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("boom"));
    let err = blocking::<(), _>(|| panic!("kaboom")).await.unwrap_err();
    assert!(err.to_string().contains("blocking task failed"), "{err}");
}

#[tokio::test]
async fn connect_pool_fails_cleanly_without_a_server() {
    let cfg = OracleConnectionConfig::new("127.0.0.1", 1, "NOPE", "u", "p");
    let Err(err) = connect_pool(&cfg, 1).await else {
        panic!("must fail");
    };
    assert!(matches!(err, FaucetError::Config(_)), "{err}");
}
