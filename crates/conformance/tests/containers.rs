//! The container start helper against a real Docker daemon (#841): a probe
//! that never passes and a wait strategy that never matches are retried and
//! reported, and a ready container is handed over.
#![cfg(feature = "containers")]

use std::time::Duration;

use faucet_conformance::containers::{self, ReadyProbe, StartErrorKind, StartOptions};
use testcontainers::GenericImage;
use testcontainers::core::{IntoContainerPort, WaitFor};

const NEVER_SET: &str = "FAUCET_CONFORMANCE_NEVER_SET_841";

fn redis() -> GenericImage {
    GenericImage::new("redis", "5.0")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
}

#[tokio::test(flavor = "multi_thread")]
async fn starts_a_ready_container_and_retries_start_up_failures() {
    let opts = StartOptions::default().ready(ReadyProbe::tcp(6379));
    let Some(container) = containers::start_or_skip(redis, &opts).await else {
        return;
    };
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
    );
    drop(container);

    let never_ready = StartOptions::default()
        .attempts(2)
        .backoff(Duration::ZERO)
        .ready(ReadyProbe::http(6379, "/"))
        .ready_timeout(Duration::from_secs(1));
    let err = containers::start_container(redis, &never_ready)
        .await
        .unwrap_err();
    assert_eq!((err.kind, err.attempts), (StartErrorKind::Exhausted, 2));
    assert!(err.message.contains("not ready"), "{err}");
    assert!(err.image.starts_with("redis:5.0"), "{err}");

    let never_logs = StartOptions::default()
        .attempts(1)
        .startup_timeout(Duration::from_secs(2))
        .require_env(NEVER_SET);
    let make = || redis().with_wait_for(WaitFor::message_on_stdout("never printed"));
    assert!(containers::start_or_skip(make, &never_logs).await.is_none());
    let err = containers::start_container(make, &never_logs)
        .await
        .unwrap_err();
    assert_eq!(err.kind, StartErrorKind::Exhausted);
    assert!(err.message.contains("timeout"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "container did not start")]
async fn start_with_panics_when_the_container_does_not_start() {
    if containers::start_or_skip(redis, &StartOptions::default())
        .await
        .is_none()
    {
        panic!("container did not start: no Docker");
    }
    let opts = StartOptions::default()
        .attempts(1)
        .startup_timeout(Duration::from_secs(2));
    containers::start_with(
        || redis().with_wait_for(WaitFor::message_on_stdout("never printed")),
        &opts,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn start_uses_the_default_options() {
    if containers::start_or_skip(redis, &StartOptions::default())
        .await
        .is_none()
    {
        return;
    }
    let container = containers::start(redis).await;
    assert!(container.get_host_port_ipv4(6379).await.is_ok());
}
