//! LocalStack for the Kinesis integration tests (#999).
//!
//! - One LocalStack per test binary at a time: tests in a binary run in
//!   parallel, and several emulators starting together on a CI runner is
//!   where `GetRecords` stalled.
//! - Readiness is the emulator's health endpoint, then a `ListStreams` that
//!   answers.
//! - Clients made here time out a call after 5 s and give up after 15 s, so a
//!   stalled emulator fails with the SDK error instead of hanging.
//! - A failing test prints the emulator's log (`docker logs`) before the
//!   container is removed.
//!
//! The other Kinesis crate keeps an identical copy in its `tests/support/`.
#![allow(dead_code)] // each test binary uses a different subset

use std::time::Duration;

use aws_sdk_kinesis::config::timeout::TimeoutConfig;
use aws_sdk_kinesis::types::StreamStatus;
use faucet_conformance::containers::{self, ReadyProbe, StartOptions};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::localstack::LocalStack;
use tokio::sync::{Semaphore, SemaphorePermit};

/// Bound on one SDK call attempt.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on one SDK call, retries included.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Bound on one test phase (seed, read, write): well past a healthy run.
pub const PHASE_TIMEOUT: Duration = Duration::from_secs(30);

static GATE: Semaphore = Semaphore::const_new(1);

/// A running LocalStack; prints its log when dropped during a panic.
pub struct Emulator {
    pub container: ContainerAsync<LocalStack>,
    pub endpoint: String,
    _turn: SemaphorePermit<'static>,
}

impl Drop for Emulator {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", emulator_log(self.container.id()));
        }
    }
}

/// The last lines of a container's log, through the Docker CLI (sync, so it
/// works from `Drop` while unwinding).
pub fn emulator_log(id: &str) -> String {
    match std::process::Command::new("docker")
        .args(["logs", "--tail", "300", id])
        .output()
    {
        Ok(out) => format!(
            "---- LocalStack log ({id}) ----\n{}{}---- end of LocalStack log ----",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(e) => format!("---- LocalStack log ({id}) unavailable: {e} ----"),
    }
}

/// Start LocalStack with `services` (e.g. `"kinesis"` or `"kinesis,sts"`)
/// once no other test in this binary holds one, and wait until Kinesis answers.
pub async fn start(services: &'static str) -> Emulator {
    let turn = GATE.acquire().await.expect("LocalStack gate closed");
    let opts = StartOptions::default().ready(ReadyProbe::http(4566, "/_localstack/health"));
    let container = containers::start_with(
        || LocalStack::default().with_env_var("SERVICES", services),
        &opts,
    )
    .await;
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    let emulator = Emulator {
        container,
        endpoint: format!("http://127.0.0.1:{port}"),
        _turn: turn,
    };
    let probe = bounded(
        faucet_common_kinesis::build_client(
            Some("us-east-1"),
            Some(&emulator.endpoint),
            &test_credentials(),
        )
        .await
        .expect("client"),
    );
    await_ready(&probe).await;
    emulator
}

pub fn test_credentials() -> faucet_common_kinesis::KinesisCredentials {
    faucet_common_kinesis::KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

/// `client` with per-attempt and per-call timeouts.
pub fn bounded(client: aws_sdk_kinesis::Client) -> aws_sdk_kinesis::Client {
    let conf = client
        .config()
        .to_builder()
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(ATTEMPT_TIMEOUT)
                .operation_timeout(CALL_TIMEOUT)
                .build(),
        )
        .build();
    aws_sdk_kinesis::Client::from_conf(conf)
}

/// Wait until the endpoint accepts Kinesis calls (the port maps first).
pub async fn await_ready(client: &aws_sdk_kinesis::Client) {
    let mut last = String::new();
    for _ in 0..120 {
        match client.list_streams().send().await {
            Ok(_) => return,
            Err(e) => last = format!("{e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("localstack kinesis never became ready: {last}");
}

/// Create a stream and wait until it is ACTIVE.
pub async fn create_stream(client: &aws_sdk_kinesis::Client, name: &str, shards: i32) {
    await_ready(client).await;
    client
        .create_stream()
        .stream_name(name)
        .shard_count(shards)
        .send()
        .await
        .expect("create stream");
    await_active(client, name).await;
}

/// Wait until `name` is ACTIVE.
pub async fn await_active(client: &aws_sdk_kinesis::Client, name: &str) {
    for _ in 0..60 {
        let out = client
            .describe_stream_summary()
            .stream_name(name)
            .send()
            .await
            .expect("describe stream");
        if out
            .stream_description_summary()
            .is_some_and(|d| d.stream_status() == &StreamStatus::Active)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("stream {name} never became ACTIVE");
}

/// One `GetRecords` round trip on every shard of `stream`, so a phase about to
/// read through the connector (whose own retry loop never gives up) first
/// fails with the SDK error when the emulator has stopped serving reads.
pub async fn probe_reads(client: &aws_sdk_kinesis::Client, stream: &str) {
    let shards = client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .unwrap_or_else(|e| panic!("ListShards on {stream}: {e:?}"));
    for shard in shards.shards() {
        let it = client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard.shard_id())
            .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
            .send()
            .await
            .unwrap_or_else(|e| panic!("GetShardIterator on {stream}: {e:?}"));
        if let Some(it) = it.shard_iterator() {
            client
                .get_records()
                .shard_iterator(it)
                .send()
                .await
                .unwrap_or_else(|e| panic!("GetRecords on {stream}: {e:?}"));
        }
    }
}

/// Run one test phase under [`PHASE_TIMEOUT`] (or `limit`), naming it on a stall.
pub async fn phase<T>(name: &str, fut: impl std::future::Future<Output = T>) -> T {
    phase_within(name, PHASE_TIMEOUT, fut).await
}

/// [`phase`] with its own bound.
pub async fn phase_within<T>(
    name: &str,
    limit: Duration,
    fut: impl std::future::Future<Output = T>,
) -> T {
    tokio::time::timeout(limit, fut)
        .await
        .unwrap_or_else(|_| panic!("{name} did not finish within {limit:?}"))
}
