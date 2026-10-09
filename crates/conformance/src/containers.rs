//! One way to start a test container (#841).
//!
//! Every connector integration test that needs a backend starts it through
//! [`start`], [`start_with`] or [`start_or_skip`]. They share one start-up
//! budget ([`DEFAULT_STARTUP_TIMEOUT`]), retry a failed start a bounded number
//! of times ([`DEFAULT_ATTEMPTS`], logged, with a growing backoff) and can poll
//! a readiness probe ([`ReadyProbe`]) before handing the container over.
//!
//! Only start-up failures are retried: a wait strategy that timed out or whose
//! log stream ended, a container Docker could not create, start, inspect or
//! map ports for, an image pull that failed, or a readiness probe that never
//! succeeded. A failed attempt's container is dropped (and so removed) before
//! the next attempt. A Docker daemon that cannot be reached is not retried, and
//! nothing the test does after the container is handed over is ever retried.
//!
//! A backend that does not come up is a skip on a developer machine and a
//! failure where the environment asks for the backends: [`start_or_skip`]
//! panics when [`REQUIRE_BACKENDS`] (or the variable set by
//! [`StartOptions::require_env`], e.g. [`REQUIRE_ORACLE`]) is set, and prints a
//! skip line and returns `None` otherwise. [`backend_missing`] applies the same
//! rule to a backend a test finds missing some other way.
//!
//! ```no_run
//! # async fn ex() {
//! use faucet_conformance::containers::{self, ReadyProbe, StartOptions};
//! use testcontainers::{GenericImage, ImageExt, core::IntoContainerPort};
//!
//! let opts = StartOptions::default().ready(ReadyProbe::http(8123, "/ping"));
//! let Some(container) = containers::start_or_skip(
//!     || GenericImage::new("clickhouse/clickhouse-server", "24.8").with_exposed_port(8123.tcp()),
//!     &opts,
//! )
//! .await
//! else {
//!     return;
//! };
//! # drop(container);
//! # }
//! ```
//!
//! The image is built by a closure because a start consumes its request and a
//! retry needs a fresh one.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use testcontainers::core::client::ClientError;
use testcontainers::core::error::TestcontainersError;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, Image, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Time a container gets to satisfy its wait strategy on one attempt.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);
/// Start attempts before giving up.
pub const DEFAULT_ATTEMPTS: u32 = 3;
/// Wait before the second attempt; doubled before each later one.
pub const DEFAULT_BACKOFF: Duration = Duration::from_secs(2);
/// Time a readiness probe gets to succeed once the container is up.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(60);
/// Upper bound on one probe call, so a hung probe cannot use the whole budget.
pub const PROBE_CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause between probe calls.
pub const PROBE_INTERVAL: Duration = Duration::from_millis(250);
/// When set, a backend that does not start fails the test instead of skipping it.
pub const REQUIRE_BACKENDS: &str = "FAUCET_REQUIRE_BACKENDS";
/// The Oracle Free image only starts where this is set (the nightly job), so
/// Oracle tests use it in place of [`REQUIRE_BACKENDS`].
pub const REQUIRE_ORACLE: &str = "FAUCET_REQUIRE_ORACLE";

/// The address a container port is reachable at from the test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// Host the port is mapped on.
    pub host: String,
    /// Mapped host port.
    pub port: u16,
}

impl Endpoint {
    /// `host:port`.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

type CheckFn = dyn Fn(Endpoint) -> BoxFuture<'static, Result<(), String>> + Send + Sync;

/// A check that a started container is ready to serve, polled until it
/// succeeds or [`StartOptions::ready_timeout`] passes.
#[derive(Clone)]
pub enum ReadyProbe {
    /// A TCP connection to the container port succeeds.
    Tcp {
        /// Container port.
        port: u16,
    },
    /// `GET path` on the container port answers with a 2xx status.
    Http {
        /// Container port.
        port: u16,
        /// Request path, starting with `/`.
        path: String,
    },
    /// A caller-supplied check against the mapped container port.
    Custom {
        /// Container port.
        port: u16,
        /// The check; an `Err` is retried until the readiness timeout.
        check: Arc<CheckFn>,
    },
}

impl fmt::Debug for ReadyProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadyProbe::Tcp { port } => write!(f, "tcp :{port}"),
            ReadyProbe::Http { port, path } => write!(f, "http :{port}{path}"),
            ReadyProbe::Custom { port, .. } => write!(f, "custom :{port}"),
        }
    }
}

impl ReadyProbe {
    /// A TCP connect to `port`.
    pub fn tcp(port: u16) -> Self {
        ReadyProbe::Tcp { port }
    }

    /// An HTTP `GET path` on `port` expecting a 2xx status.
    pub fn http(port: u16, path: impl Into<String>) -> Self {
        ReadyProbe::Http {
            port,
            path: path.into(),
        }
    }

    /// A custom async check against the endpoint `port` is mapped to.
    pub fn custom<F, Fut>(port: u16, check: F) -> Self
    where
        F: Fn(Endpoint) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        ReadyProbe::Custom {
            port,
            check: Arc::new(move |ep| Box::pin(check(ep))),
        }
    }

    /// The container port the probe targets.
    pub fn port(&self) -> u16 {
        match self {
            ReadyProbe::Tcp { port }
            | ReadyProbe::Http { port, .. }
            | ReadyProbe::Custom { port, .. } => *port,
        }
    }

    /// Run the probe once.
    pub async fn check(&self, ep: &Endpoint) -> Result<(), String> {
        match self {
            ReadyProbe::Tcp { .. } => tokio::net::TcpStream::connect(ep.addr())
                .await
                .map(drop)
                .map_err(|e| format!("connect {}: {e}", ep.addr())),
            ReadyProbe::Http { path, .. } => http_status(ep, path).await.and_then(|status| {
                if (200..300).contains(&status) {
                    Ok(())
                } else {
                    Err(format!("GET {path} on {} answered {status}", ep.addr()))
                }
            }),
            ReadyProbe::Custom { check, .. } => check(ep.clone()).await,
        }
    }
}

async fn http_status(ep: &Endpoint, path: &str) -> Result<u16, String> {
    let addr = ep.addr();
    let mut stream = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    let request = format!(
        "GET {path} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        ep.host
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write to {addr}: {e}"))?;
    let mut head = Vec::new();
    let mut buf = [0u8; 256];
    while !head.contains(&b'\n') {
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| format!("read from {addr}: {e}"))?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    parse_status_line(&head).ok_or_else(|| {
        format!(
            "no HTTP status line from {addr}: {:?}",
            String::from_utf8_lossy(&head)
        )
    })
}

fn parse_status_line(head: &[u8]) -> Option<u16> {
    let line = head.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split_whitespace();
    parts.next().filter(|v| v.starts_with("HTTP/"))?;
    parts.next()?.parse().ok()
}

/// Poll `probe` against `ep` until it succeeds or `timeout` passes. Each call
/// is bounded by [`PROBE_CALL_TIMEOUT`]; the last failure is returned.
pub async fn wait_ready(
    probe: &ReadyProbe,
    ep: &Endpoint,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let budget = PROBE_CALL_TIMEOUT.min(remaining);
        let last = match tokio::time::timeout(budget, probe.check(ep)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => e,
            Err(_) => format!("probe call exceeded {budget:?}"),
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining < PROBE_INTERVAL {
            return Err(format!(
                "{probe:?} on {} not ready after {timeout:?}: {last}",
                ep.addr()
            ));
        }
        tokio::time::sleep(PROBE_INTERVAL).await;
    }
}

/// How the helper starts a container.
#[derive(Debug, Clone)]
pub struct StartOptions {
    /// Wait-strategy budget per attempt (overrides the image's own).
    pub startup_timeout: Duration,
    /// Attempts before giving up; at least one is always made.
    pub attempts: u32,
    /// Wait before the second attempt, doubled before each later one.
    pub backoff: Duration,
    /// Optional readiness probe run after the wait strategy.
    pub ready: Option<ReadyProbe>,
    /// Budget for the readiness probe per attempt.
    pub ready_timeout: Duration,
    /// Variable that turns a backend that does not start into a failure.
    pub require_env: &'static str,
}

impl Default for StartOptions {
    fn default() -> Self {
        StartOptions {
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            attempts: DEFAULT_ATTEMPTS,
            backoff: DEFAULT_BACKOFF,
            ready: None,
            ready_timeout: DEFAULT_READY_TIMEOUT,
            require_env: REQUIRE_BACKENDS,
        }
    }
}

impl StartOptions {
    /// Set the per-attempt start-up budget.
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Set the number of attempts.
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Set the first backoff.
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff;
        self
    }

    /// Poll `probe` after each start.
    pub fn ready(mut self, probe: ReadyProbe) -> Self {
        self.ready = Some(probe);
        self
    }

    /// Set the readiness budget.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Use `var` instead of [`REQUIRE_BACKENDS`] to decide skip vs failure.
    pub fn require_env(mut self, var: &'static str) -> Self {
        self.require_env = var;
        self
    }
}

/// What one failed attempt means for the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptFailure {
    /// A start-up failure worth another attempt.
    Retry(String),
    /// Docker itself cannot be reached; another attempt will not help.
    Unavailable(String),
    /// Not a start-up failure (e.g. a bad request); never retried.
    Fatal(String),
}

/// Why a container could not be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartErrorKind {
    /// Docker could not be reached.
    Unavailable,
    /// Every attempt failed to start.
    Exhausted,
    /// A failure that is not retried.
    Fatal,
}

/// A container that did not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartError {
    /// Image the start was for.
    pub image: String,
    /// Attempts made.
    pub attempts: u32,
    /// Why it failed.
    pub kind: StartErrorKind,
    /// The last failure.
    pub message: String,
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.kind {
            StartErrorKind::Unavailable => "Docker is not available",
            StartErrorKind::Exhausted => "container did not start",
            StartErrorKind::Fatal => "container start failed",
        };
        write!(
            f,
            "{}: {what} after {} attempt(s): {}",
            self.image, self.attempts, self.message
        )
    }
}

impl std::error::Error for StartError {}

/// Classify a testcontainers error from `start()`.
pub fn classify(err: &TestcontainersError) -> AttemptFailure {
    let msg = err.to_string();
    match err {
        TestcontainersError::Client(
            ClientError::Init(_)
            | ClientError::Configuration(_)
            | ClientError::InvalidDockerHost(_),
        ) => AttemptFailure::Unavailable(msg),
        TestcontainersError::Client(_)
        | TestcontainersError::WaitContainer(_)
        | TestcontainersError::MissingInfo(_)
        | TestcontainersError::Io(_) => AttemptFailure::Retry(msg),
        _ => AttemptFailure::Fatal(msg),
    }
}

/// The backoff before attempt `attempt` (1-based); zero before the first.
pub fn backoff_before(attempt: u32, first: Duration) -> Duration {
    match attempt {
        0 | 1 => Duration::ZERO,
        n => first.saturating_mul(1u32 << (n - 2).min(16)),
    }
}

/// Run `op` up to `attempts` times, retrying only [`AttemptFailure::Retry`],
/// sleeping [`backoff_before`] between attempts and logging each failure.
pub async fn retry_start<T, F, Fut>(
    label: &str,
    attempts: u32,
    backoff: Duration,
    mut op: F,
) -> Result<T, StartError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, AttemptFailure>>,
{
    let attempts = attempts.max(1);
    let mut attempt = 0;
    loop {
        attempt += 1;
        tokio::time::sleep(backoff_before(attempt, backoff)).await;
        let error = |kind, message| StartError {
            image: label.to_string(),
            attempts: attempt,
            kind,
            message,
        };
        match op(attempt).await {
            Ok(value) => return Ok(value),
            Err(AttemptFailure::Unavailable(m)) => {
                return Err(error(StartErrorKind::Unavailable, m));
            }
            Err(AttemptFailure::Fatal(m)) => return Err(error(StartErrorKind::Fatal, m)),
            Err(AttemptFailure::Retry(m)) if attempt >= attempts => {
                return Err(error(StartErrorKind::Exhausted, m));
            }
            Err(AttemptFailure::Retry(m)) => {
                eprintln!("{label}: start attempt {attempt}/{attempts} failed, retrying: {m}");
            }
        }
    }
}

async fn endpoint<I: Image>(container: &ContainerAsync<I>, port: u16) -> Result<Endpoint, String> {
    let host = container.get_host().await.map_err(|e| e.to_string())?;
    let port = container
        .get_host_port_ipv4(port)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Endpoint {
        host: host.to_string(),
        port,
    })
}

/// Start the container `make` builds, per `opts`.
pub async fn start_container<I, R, F>(
    mut make: F,
    opts: &StartOptions,
) -> Result<ContainerAsync<I>, StartError>
where
    I: Image,
    R: Into<ContainerRequest<I>>,
    F: FnMut() -> R,
{
    let label = make().into().descriptor();
    retry_start(&label, opts.attempts, opts.backoff, |_| {
        let request = make().with_startup_timeout(opts.startup_timeout);
        async move {
            let container = request.start().await.map_err(|e| classify(&e))?;
            if let Some(probe) = &opts.ready {
                let ep = endpoint(&container, probe.port())
                    .await
                    .map_err(AttemptFailure::Retry)?;
                if let Err(e) = wait_ready(probe, &ep, opts.ready_timeout).await {
                    drop(container);
                    return Err(AttemptFailure::Retry(e));
                }
            }
            Ok(container)
        }
    })
    .await
}

/// Start with the default options; panics when the container does not start.
pub async fn start<I, R, F>(make: F) -> ContainerAsync<I>
where
    I: Image,
    R: Into<ContainerRequest<I>>,
    F: FnMut() -> R,
{
    start_with(make, &StartOptions::default()).await
}

/// Start per `opts`; panics when the container does not start.
pub async fn start_with<I, R, F>(make: F, opts: &StartOptions) -> ContainerAsync<I>
where
    I: Image,
    R: Into<ContainerRequest<I>>,
    F: FnMut() -> R,
{
    match start_container(make, opts).await {
        Ok(container) => container,
        Err(e) => panic!("{e}"),
    }
}

/// Start per `opts`; when it does not start, [`backend_missing_for`]
/// `opts.require_env` decides between a failure and a skip (`None`).
pub async fn start_or_skip<I, R, F>(make: F, opts: &StartOptions) -> Option<ContainerAsync<I>>
where
    I: Image,
    R: Into<ContainerRequest<I>>,
    F: FnMut() -> R,
{
    match start_container(make, opts).await {
        Ok(container) => Some(container),
        Err(e) => {
            backend_missing_for(opts.require_env, &e.to_string());
            None
        }
    }
}

/// A missing test backend: a skip locally, a failure when [`REQUIRE_BACKENDS`]
/// is set.
pub fn backend_missing(why: &str) {
    backend_missing_for(REQUIRE_BACKENDS, why);
}

/// A missing test backend: a skip locally, a failure when `var` is set.
pub fn backend_missing_for(var: &str, why: &str) {
    missing(std::env::var_os(var).is_some(), var, why);
}

/// Whether `var` asks for backends to be present.
pub fn required(var: &str) -> bool {
    std::env::var_os(var).is_some()
}

fn missing(required: bool, var: &str, why: &str) {
    if required {
        panic!("{why} ({var} is set)");
    }
    eprintln!("skipping: {why}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use testcontainers::core::error::WaitContainerError;
    use testcontainers::core::ports::PortMappingError;
    use tokio::net::TcpListener;

    fn ep(port: u16) -> Endpoint {
        Endpoint {
            host: "127.0.0.1".into(),
            port,
        }
    }

    async fn closed_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    async fn http_server(response: &'static [u8]) -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let mut buf = [0u8; 512];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(response).await;
            }
        });
        port
    }

    #[test]
    fn classifies_start_up_failures_as_retryable() {
        let timeout = TestcontainersError::WaitContainer(WaitContainerError::StartupTimeout);
        assert!(matches!(classify(&timeout), AttemptFailure::Retry(_)));
        let eos = TestcontainersError::WaitContainer(WaitContainerError::WaitLog(
            testcontainers::core::logs::WaitLogError::EndOfStream(vec![]),
        ));
        assert!(matches!(classify(&eos), AttemptFailure::Retry(_)));
        let ports = TestcontainersError::Client(ClientError::PortMapping(
            PortMappingError::FailedToParseHostPort("x".parse::<u16>().unwrap_err()),
        ));
        assert!(matches!(classify(&ports), AttemptFailure::Retry(_)));
        let io = TestcontainersError::Io(std::io::Error::other("reset"));
        assert!(matches!(classify(&io), AttemptFailure::Retry(_)));
    }

    #[test]
    fn classifies_unreachable_docker_and_other_errors() {
        let host = TestcontainersError::Client(ClientError::InvalidDockerHost("nope".into()));
        assert!(matches!(classify(&host), AttemptFailure::Unavailable(_)));
        let other = TestcontainersError::other("bad request");
        assert!(matches!(classify(&other), AttemptFailure::Fatal(m) if m.contains("bad request")));
    }

    #[test]
    fn backoff_doubles_after_the_first_retry() {
        let first = Duration::from_secs(2);
        assert_eq!(backoff_before(0, first), Duration::ZERO);
        assert_eq!(backoff_before(1, first), Duration::ZERO);
        assert_eq!(backoff_before(2, first), first);
        assert_eq!(backoff_before(3, first), first * 2);
        assert_eq!(backoff_before(4, first), first * 4);
        assert_eq!(backoff_before(u32::MAX, first), first * 65536);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_success_with_backoff() {
        let calls = AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        let got = retry_start("img", 3, Duration::from_secs(2), |attempt| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 3 {
                    Err(AttemptFailure::Retry(format!("try {attempt}")))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(got, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(started.elapsed(), Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_last_attempt() {
        let err = retry_start("img", 2, Duration::from_secs(1), |a| async move {
            Err::<(), _>(AttemptFailure::Retry(format!("try {a}")))
        })
        .await
        .unwrap_err();
        assert_eq!(err.kind, StartErrorKind::Exhausted);
        assert_eq!(err.attempts, 2);
        assert_eq!(err.message, "try 2");
        assert_eq!(
            err.to_string(),
            "img: container did not start after 2 attempt(s): try 2"
        );
    }

    #[tokio::test]
    async fn never_retries_unavailable_or_fatal_and_always_attempts_once() {
        for (failure, kind, text) in [
            (
                AttemptFailure::Unavailable("no daemon".into()),
                StartErrorKind::Unavailable,
                "Docker is not available",
            ),
            (
                AttemptFailure::Fatal("bad".into()),
                StartErrorKind::Fatal,
                "container start failed",
            ),
        ] {
            let calls = AtomicU32::new(0);
            let err = retry_start("img", 5, Duration::ZERO, |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                let failure = failure.clone();
                async move { Err::<(), _>(failure) }
            })
            .await
            .unwrap_err();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(err.kind, kind);
            assert!(err.to_string().contains(text), "{err}");
        }
        let calls = AtomicU32::new(0);
        let err = retry_start("img", 0, Duration::ZERO, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(AttemptFailure::Retry("x".into())) }
        })
        .await
        .unwrap_err();
        assert_eq!((calls.load(Ordering::SeqCst), err.attempts), (1, 1));
    }

    #[tokio::test]
    async fn tcp_probe_reports_a_closed_port_then_an_open_one() {
        let probe = ReadyProbe::tcp(1);
        assert_eq!(probe.port(), 1);
        assert_eq!(format!("{probe:?}"), "tcp :1");
        let closed = closed_port().await;
        assert!(
            probe
                .check(&ep(closed))
                .await
                .unwrap_err()
                .contains("connect")
        );
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let open = l.local_addr().unwrap().port();
        probe.check(&ep(open)).await.unwrap();
        assert_eq!(ep(open).addr(), format!("127.0.0.1:{open}"));
    }

    #[tokio::test]
    async fn http_probe_needs_a_2xx_status() {
        let probe = ReadyProbe::http(8123, "/ping");
        assert_eq!(probe.port(), 8123);
        assert_eq!(format!("{probe:?}"), "http :8123/ping");
        let ok = http_server(b"HTTP/1.1 200 OK\r\n\r\nOk.").await;
        probe.check(&ep(ok)).await.unwrap();
        let busy = http_server(b"HTTP/1.1 503 Service Unavailable\r\n\r\n").await;
        let err = probe.check(&ep(busy)).await.unwrap_err();
        assert!(err.contains("503"), "{err}");
        let garbage = http_server(b"nonsense").await;
        let err = probe.check(&ep(garbage)).await.unwrap_err();
        assert!(err.contains("no HTTP status line"), "{err}");
        let silent = http_server(b"").await;
        assert!(probe.check(&ep(silent)).await.is_err());
        let closed = closed_port().await;
        assert!(
            probe
                .check(&ep(closed))
                .await
                .unwrap_err()
                .contains("connect")
        );
    }

    #[test]
    fn parses_status_lines() {
        assert_eq!(parse_status_line(b"HTTP/1.0 204 No Content\r\n"), Some(204));
        assert_eq!(parse_status_line(b"SSH-2.0 x\r\n"), None);
        assert_eq!(parse_status_line(b"HTTP/1.1\r\n"), None);
        assert_eq!(parse_status_line(b"HTTP/1.1 abc\r\n"), None);
        assert_eq!(parse_status_line(&[0xff, 0xfe]), None);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_polls_a_custom_probe_until_it_passes() {
        let calls = Arc::new(AtomicU32::new(0));
        let seen = calls.clone();
        let probe = ReadyProbe::custom(9000, move |e: Endpoint| {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(e.port, 1);
                if n < 2 {
                    Err("warming".to_string())
                } else {
                    Ok(())
                }
            }
        });
        assert_eq!(format!("{probe:?}"), "custom :9000");
        assert_eq!(probe.port(), 9000);
        wait_ready(&probe, &ep(1), Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_times_out_and_bounds_a_hung_probe() {
        let failing = ReadyProbe::custom(1, |_| async { Err("refused".to_string()) });
        let err = wait_ready(&failing, &ep(1), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.contains("not ready after 1s: refused"), "{err}");

        let hung = ReadyProbe::custom(1, |_| std::future::pending::<Result<(), String>>());
        let started = tokio::time::Instant::now();
        let err = wait_ready(&hung, &ep(1), Duration::from_secs(12))
            .await
            .unwrap_err();
        assert!(err.contains("probe call exceeded"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(13));
    }

    #[test]
    fn options_builders_set_every_field() {
        let d = StartOptions::default();
        assert_eq!(d.startup_timeout, DEFAULT_STARTUP_TIMEOUT);
        assert_eq!(d.attempts, DEFAULT_ATTEMPTS);
        assert_eq!(d.backoff, DEFAULT_BACKOFF);
        assert_eq!(d.ready_timeout, DEFAULT_READY_TIMEOUT);
        assert_eq!(d.require_env, REQUIRE_BACKENDS);
        assert!(d.ready.is_none());
        let o = d
            .startup_timeout(Duration::from_secs(1))
            .attempts(7)
            .backoff(Duration::from_secs(3))
            .ready(ReadyProbe::tcp(5))
            .ready_timeout(Duration::from_secs(4))
            .require_env(REQUIRE_ORACLE);
        assert_eq!(
            (
                o.startup_timeout,
                o.attempts,
                o.backoff,
                o.ready_timeout,
                o.require_env
            ),
            (
                Duration::from_secs(1),
                7,
                Duration::from_secs(3),
                Duration::from_secs(4),
                REQUIRE_ORACLE
            )
        );
        assert_eq!(o.ready.map(|p| p.port()), Some(5));
    }

    #[test]
    fn a_missing_backend_skips_unless_required() {
        missing(false, REQUIRE_BACKENDS, "no docker");
        backend_missing_for("FAUCET_CONFORMANCE_NEVER_SET_841", "no docker");
        assert!(!required("FAUCET_CONFORMANCE_NEVER_SET_841"));
        let outcome = std::panic::catch_unwind(|| backend_missing("no docker"));
        assert_eq!(outcome.is_err(), required(REQUIRE_BACKENDS));
    }

    #[test]
    #[should_panic(expected = "no docker (FAUCET_REQUIRE_BACKENDS is set)")]
    fn a_missing_backend_fails_when_required() {
        missing(true, REQUIRE_BACKENDS, "no docker");
    }
}
