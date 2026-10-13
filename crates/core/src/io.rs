//! The I/O call wrapper every connector routes its backend calls through
//! (RFC 0013, #880).
//!
//! A connector's backend call needs the same handful of guarantees no matter
//! what the backend is: a per-call timeout, an overall deadline, classified
//! retry that honours a server's stated wait (`Retry-After` →
//! [`FaucetError::RateLimited`]), prompt cancellation, round-trip metering and
//! redaction of what an error carries. Before this module each connector wrote
//! its own copy of some of them. [`CallContext::call`] applies all of them in
//! one place:
//!
//! ```
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! use faucet_core::io::CallContext;
//! use std::time::Duration;
//!
//! let ctx = CallContext::new()
//!     .with_timeout(Duration::from_secs(30))
//!     .with_retry(faucet_core::RetryPolicy::default());
//! let n: u32 = ctx.call("query", || async { Ok(42) }).await?;
//! # assert_eq!(n, 42);
//! # Ok(())
//! # }
//! ```
//!
//! ## Who sets what
//!
//! A context has two owners. The **connector** owns the per-call behaviour it
//! documents in its config — the attempt timeout, the [`RetryPolicy`], a
//! rate-limit budget — and keeps it in a [`CallSlot`] built in `new()`. The
//! **pipeline** owns the run: it installs a context carrying the cancel token,
//! the round-trip recorder and any deadline through
//! [`Source::set_call_context`](crate::Source::set_call_context) /
//! [`Sink::set_call_context`](crate::Sink::set_call_context). [`CallSlot::get`]
//! layers the two: a field the connector set wins, every other field comes
//! from the installed context, and a field neither set takes its default
//! (a [`DEFAULT_CALL_TIMEOUT`] attempt timeout, no retry).
//!
//! The pipeline never hands a sink its cancel token: a cancelled run still
//! flushes the sink (#146 H16), and a flush that failed on the token would
//! discard the very output the cancellation is meant to keep. Nor does it hand
//! either side a retry policy — sink writes are retried page by page under the
//! duplication-safety rule (ADR 0007), and a source's policy is its own.

use crate::error::FaucetError;
use crate::observability::RoundtripRecorder;
use crate::resilience::RetryPolicy;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// The attempt timeout a call gets when neither the connector nor the caller
/// set one. Long enough for any single request a healthy backend answers;
/// short enough that a hung connection cannot stall a run for good.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// A redaction function: takes text, returns it with every known secret
/// replaced. See [`crate::redact`].
pub type CallRedactor = Arc<dyn Fn(&str) -> String + Send + Sync>;

#[derive(Clone, Default)]
struct Inner {
    /// `None` inherits; `Some(None)` means no attempt timeout.
    timeout: Option<Option<Duration>>,
    deadline: Option<Instant>,
    retry: Option<RetryPolicy>,
    rate_limit_retries: Option<u32>,
    cancel: Option<CancellationToken>,
    recorder: Option<Arc<RoundtripRecorder>>,
    metered: Option<bool>,
    redactor: Option<CallRedactor>,
}

/// Everything one backend call needs: timeout, deadline, retry policy, cancel
/// token, round-trip recorder and redactor. Cheap to clone.
///
/// Build one with [`CallContext::new`] and the `with_*` methods; run a call
/// with [`call`](Self::call). See the [module docs](self).
#[derive(Clone, Default)]
pub struct CallContext {
    inner: Arc<Inner>,
}

// Immutable once built; a panic cannot leave it half-updated.
impl std::panic::UnwindSafe for CallContext {}
impl std::panic::RefUnwindSafe for CallContext {}

impl fmt::Debug for CallContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallContext")
            .field("timeout", &self.timeout())
            .field("deadline", &self.inner.deadline)
            .field("retry", &self.inner.retry)
            .field("rate_limit_retries", &self.inner.rate_limit_retries)
            .field("cancel", &self.inner.cancel.is_some())
            .field("recorder", &self.inner.recorder.is_some())
            .field("redactor", &self.inner.redactor.is_some())
            .finish()
    }
}

impl CallContext {
    /// A context with nothing set: a [`DEFAULT_CALL_TIMEOUT`] attempt timeout,
    /// one attempt, no cancel token, no deadline, no metering.
    pub fn new() -> Self {
        Self::default()
    }

    fn edit(mut self, f: impl FnOnce(&mut Inner)) -> Self {
        f(Arc::make_mut(&mut self.inner));
        self
    }

    /// Bound each attempt by `timeout`. An attempt that runs out fails with a
    /// retriable [`FaucetError::Timeout`].
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.edit(|i| i.timeout = Some(Some(timeout)))
    }

    /// Let each attempt run as long as it takes. For a call that legitimately
    /// outlives any fixed bound (a long poll, a streamed download); a deadline
    /// still applies.
    pub fn without_timeout(self) -> Self {
        self.edit(|i| i.timeout = Some(None))
    }

    /// End the whole call — every attempt and every backoff — at `deadline`,
    /// with a [`FaucetError::Timeout`].
    pub fn with_deadline(self, deadline: Instant) -> Self {
        self.edit(|i| i.deadline = Some(deadline))
    }

    /// Retry failed attempts under `policy`: classified by
    /// [`RetryClass`](crate::RetryClass), backed off with jitter, and a
    /// [`FaucetError::RateLimited`] waits the time it states.
    pub fn with_retry(self, policy: RetryPolicy) -> Self {
        self.edit(|i| i.retry = Some(policy))
    }

    /// Make exactly one attempt, whatever a layered context says.
    pub fn without_retry(self) -> Self {
        self.with_retry(RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        })
    }

    /// Give rate limits their own budget: a [`FaucetError::RateLimited`]
    /// outcome waits its stated time and is retried up to `n` consecutive
    /// times without using one of the retry policy's attempts (any other
    /// outcome resets the count). Without this, a rate limit is an ordinary
    /// retriable attempt.
    pub fn with_rate_limit_retries(self, n: u32) -> Self {
        self.edit(|i| i.rate_limit_retries = Some(n))
    }

    /// Stop the call, and any backoff, as soon as `cancel` fires, with
    /// [`FaucetError::Cancelled`].
    pub fn with_cancel(self, cancel: CancellationToken) -> Self {
        self.edit(|i| i.cancel = Some(cancel))
    }

    /// Count each attempt as a round trip, and report throttling and retries,
    /// on `recorder`.
    pub fn with_recorder(self, recorder: Arc<RoundtripRecorder>) -> Self {
        self.edit(|i| i.recorder = Some(recorder))
    }

    /// Whether [`call`](Self::call) counts each attempt as a round trip
    /// (default `true`). Throttling and retries are reported either way; turn
    /// this off for a call whose round trips the connector counts itself.
    pub fn with_roundtrip_metering(self, on: bool) -> Self {
        self.edit(|i| i.metered = Some(on))
    }

    /// Scrub error text with `redactor` instead of the process-wide hook
    /// ([`crate::redact::redact`]).
    pub fn with_redactor(self, redactor: CallRedactor) -> Self {
        self.edit(|i| i.redactor = Some(redactor))
    }

    /// This context with every field it leaves unset taken from `base`.
    pub fn inherit(&self, base: &CallContext) -> CallContext {
        let (mine, b) = (&*self.inner, &*base.inner);
        CallContext {
            inner: Arc::new(Inner {
                timeout: mine.timeout.or(b.timeout),
                deadline: match (mine.deadline, b.deadline) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (x, y) => x.or(y),
                },
                retry: mine.retry.clone().or_else(|| b.retry.clone()),
                rate_limit_retries: mine.rate_limit_retries.or(b.rate_limit_retries),
                cancel: mine.cancel.clone().or_else(|| b.cancel.clone()),
                recorder: mine.recorder.clone().or_else(|| b.recorder.clone()),
                metered: mine.metered.or(b.metered),
                redactor: mine.redactor.clone().or_else(|| b.redactor.clone()),
            }),
        }
    }

    /// The attempt timeout, `None` when attempts are unbounded.
    pub fn timeout(&self) -> Option<Duration> {
        self.inner.timeout.unwrap_or(Some(DEFAULT_CALL_TIMEOUT))
    }

    /// The overall deadline, when one is set.
    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    /// The retry policy, when one is set (`None` = a single attempt).
    pub fn retry(&self) -> Option<&RetryPolicy> {
        self.inner.retry.as_ref()
    }

    /// The rate-limit budget set by
    /// [`with_rate_limit_retries`](Self::with_rate_limit_retries).
    pub fn rate_limit_retries(&self) -> Option<u32> {
        self.inner.rate_limit_retries
    }

    /// The cancel token, when one is set.
    pub fn cancel(&self) -> Option<&CancellationToken> {
        self.inner.cancel.as_ref()
    }

    /// Whether the cancel token has fired.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancel.as_ref().is_some_and(|c| c.is_cancelled())
    }

    /// The round-trip recorder, when one is set.
    pub fn recorder(&self) -> Option<&Arc<RoundtripRecorder>> {
        self.inner.recorder.as_ref()
    }

    /// Count one round trip the connector made outside [`call`](Self::call).
    pub fn record(&self, op: &'static str) {
        if let Some(r) = &self.inner.recorder {
            r.record(op);
        }
    }

    /// Report a backend-measured usage figure (see
    /// [`RoundtripRecorder::signal`]).
    pub fn signal(&self, kind: &'static str, unit: &'static str, quantity: f64) {
        if let Some(r) = &self.inner.recorder {
            r.signal(kind, unit, quantity);
        }
    }

    /// Scrub `text` with this context's redactor, or the process-wide one.
    pub fn redact(&self, text: &str) -> String {
        match &self.inner.redactor {
            Some(f) => f(text),
            None => crate::redact::redact(text),
        }
    }

    /// `err` with the text it carries scrubbed: the URL and body of an HTTP
    /// status error, and the URL of a transport error (credentials in it are
    /// removed too).
    pub fn redact_error(&self, err: FaucetError) -> FaucetError {
        match err {
            FaucetError::HttpStatus { status, url, body } => FaucetError::HttpStatus {
                status,
                url: self.redact(&url),
                body: self.redact(&body),
            },
            FaucetError::Http(e) => FaucetError::Http(redact_reqwest_url(e, |u| self.redact(u))),
            other => other,
        }
    }

    /// Wait `duration` the way a call waits: ended early with
    /// [`FaucetError::Cancelled`] when the token fires, and with
    /// [`FaucetError::Timeout`] when the deadline comes first. For a pause that
    /// is not a retry — a poll interval, a server-requested pacing delay.
    #[allow(clippy::disallowed_methods)] // the sanctioned connector wait
    pub async fn wait(&self, duration: Duration) -> Result<(), FaucetError> {
        if self.is_cancelled() {
            return Err(cancelled_error("wait"));
        }
        let cancelled = async {
            match &self.inner.cancel {
                Some(c) => c.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let end = Instant::now() + duration;
        match self.inner.deadline {
            Some(deadline) if deadline < end => {
                tokio::select! {
                    biased;
                    _ = cancelled => Err(cancelled_error("wait")),
                    _ = tokio::time::sleep_until(deadline) => Err(deadline_error("wait")),
                }
            }
            _ => {
                tokio::select! {
                    biased;
                    _ = cancelled => Err(cancelled_error("wait")),
                    _ = tokio::time::sleep_until(end) => Ok(()),
                }
            }
        }
    }

    /// The bound for the next attempt: the attempt timeout, cut to what is
    /// left of the deadline. `Err` once the deadline has passed.
    fn attempt_bound(&self, op: &'static str) -> Result<Option<Duration>, FaucetError> {
        let timeout = self.timeout();
        match self.inner.deadline {
            None => Ok(timeout),
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(deadline_error(op));
                }
                Ok(Some(timeout.map_or(left, |t| t.min(left))))
            }
        }
    }

    /// Run `f` as one backend call named `op` (a closed, connector-defined
    /// label such as `query`, `page`, `put`).
    ///
    /// Each invocation of `f` is one attempt. The call:
    /// - fails fast with [`FaucetError::Cancelled`] when the token has fired,
    ///   and stops an attempt or a backoff the moment it fires;
    /// - bounds each attempt by the attempt timeout and the whole call by the
    ///   deadline ([`FaucetError::Timeout`], retriable while the deadline
    ///   allows);
    /// - retries under the retry policy: classified errors only, jittered
    ///   backoff, a [`FaucetError::RateLimited`] waiting its stated time;
    /// - counts every attempt as a round trip on the recorder (with its
    ///   duration), and every throttle, throttle wait and retry;
    /// - scrubs the final error ([`redact_error`](Self::redact_error)).
    #[allow(clippy::disallowed_methods)] // the sanctioned deadline wait
    pub async fn call<T, F, Fut>(&self, op: &'static str, mut f: F) -> Result<T, FaucetError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, FaucetError>>,
    {
        if self.is_cancelled() {
            return Err(cancelled_error(op));
        }
        let single = RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        };
        let policy = self.inner.retry.as_ref().unwrap_or(&single);
        let recorder = self.inner.recorder.as_ref();
        let metered = self.inner.metered.unwrap_or(true);
        let attempt = || {
            let bound = self.attempt_bound(op);
            let fut = f();
            async move {
                let bound = bound?;
                let started = std::time::Instant::now();
                let outcome = match bound {
                    Some(t) => match tokio::time::timeout(t, fut).await {
                        Ok(r) => r,
                        Err(_) => Err(FaucetError::Timeout {
                            operation: format!("{op} call"),
                            after: t,
                        }),
                    },
                    None => fut.await,
                };
                if metered && let Some(r) = recorder {
                    r.record_timed(op, started.elapsed());
                }
                outcome
            }
        };
        let run = crate::resilience::run_call(
            policy,
            self.inner.cancel.as_ref(),
            recorder,
            self.inner.rate_limit_retries,
            attempt,
        );
        let cancelled = async {
            match &self.inner.cancel {
                Some(c) => c.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let deadline = async {
            match self.inner.deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending().await,
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = cancelled => Err(cancelled_error(op)),
            _ = deadline => Err(deadline_error(op)),
            r = run => r,
        };
        outcome.map_err(|e| self.redact_error(e))
    }

    /// Run `fut` as one backend call named `op`, in a single attempt: the
    /// form for a call that cannot be rebuilt — a statement on a borrowed
    /// transaction, a step of a cursor or stream, a non-idempotent write.
    /// Timeout, deadline, cancel, metering and redaction apply as in
    /// [`call`](Self::call); any retry policy is ignored.
    pub async fn call_once<T, Fut>(&self, op: &'static str, fut: Fut) -> Result<T, FaucetError>
    where
        Fut: Future<Output = Result<T, FaucetError>>,
    {
        let reset;
        let single = match &self.inner.retry {
            Some(p) if p.max_attempts > 1 => {
                reset = self.clone().without_retry();
                &reset
            }
            _ => self,
        };
        let mut fut = Some(fut);
        single
            .call(op, || {
                let f = fut.take();
                async move { f.expect("call_once makes one attempt").await }
            })
            .await
    }
}

fn cancelled_error(op: &'static str) -> FaucetError {
    FaucetError::Cancelled {
        operation: format!("{op} call"),
    }
}

fn deadline_error(op: &'static str) -> FaucetError {
    FaucetError::Timeout {
        operation: format!("{op} call (deadline)"),
        after: Duration::ZERO,
    }
}

/// `e` with its URL scrubbed by `redact` and stripped of credentials; the URL
/// is dropped when the scrubbed text no longer parses.
pub(crate) fn redact_reqwest_url(
    e: reqwest::Error,
    redact: impl Fn(&str) -> String,
) -> reqwest::Error {
    let Some(url) = e.url() else {
        return e;
    };
    let scrubbed = crate::util::redact_uri_credentials(&redact(url.as_str()));
    if scrubbed == url.as_str() {
        return e;
    }
    match reqwest::Url::parse(&scrubbed) {
        Ok(u) => e.with_url(u),
        Err(_) => e.without_url(),
    }
}

/// Where a connector keeps its [`CallContext`]: its own settings, plus the
/// context the pipeline installs.
///
/// Build it in `new()` from the connector's config, forward
/// [`Source::set_call_context`](crate::Source::set_call_context) /
/// [`Sink::set_call_context`](crate::Sink::set_call_context) to
/// [`install`](Self::install), and run calls through [`call`](Self::call).
/// Before the pipeline installs anything (a library caller driving the
/// connector directly) the connector's own settings and the defaults apply.
#[derive(Debug, Default)]
pub struct CallSlot {
    local: CallContext,
    installed: OnceLock<CallContext>,
    recorder: OnceLock<Arc<RoundtripRecorder>>,
}

// Set-once cells over immutable contexts; a panic cannot leave a torn value.
impl std::panic::UnwindSafe for CallSlot {}
impl std::panic::RefUnwindSafe for CallSlot {}

impl CallSlot {
    /// A slot whose connector-owned settings are `local`.
    pub fn new(local: CallContext) -> Self {
        Self {
            local,
            installed: OnceLock::new(),
            recorder: OnceLock::new(),
        }
    }

    /// The connector-owned settings.
    pub fn local(&self) -> &CallContext {
        &self.local
    }

    /// Replace the connector-owned settings (from a builder method).
    pub fn set_local(&mut self, local: CallContext) {
        self.local = local;
    }

    /// Install the pipeline's context. The first call wins.
    pub fn install(&self, ctx: CallContext) {
        let _ = self.installed.set(ctx);
    }

    /// Install a round-trip recorder, used when the installed context carries
    /// none (a caller that only calls `set_roundtrip_recorder`). The first
    /// call wins.
    pub fn install_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        let _ = self.recorder.set(recorder);
    }

    /// The effective context: the connector's settings over the installed
    /// context, with the separately installed recorder as a fallback.
    pub fn get(&self) -> CallContext {
        let mut ctx = match self.installed.get() {
            Some(base) => self.local.inherit(base),
            None => self.local.clone(),
        };
        if ctx.inner.recorder.is_none()
            && let Some(r) = self.recorder.get()
        {
            ctx = ctx.with_recorder(Arc::clone(r));
        }
        ctx
    }

    /// The round-trip recorder in effect, if any.
    pub fn recorder(&self) -> Option<Arc<RoundtripRecorder>> {
        self.get().inner.recorder.clone()
    }

    /// Count one round trip made outside [`call`](Self::call).
    pub fn record(&self, op: &'static str) {
        self.get().record(op);
    }

    /// Wait through the effective context ([`CallContext::wait`]).
    pub async fn wait(&self, duration: Duration) -> Result<(), FaucetError> {
        self.get().wait(duration).await
    }

    /// Run `f` through the effective context ([`CallContext::call`]).
    pub async fn call<T, F, Fut>(&self, op: &'static str, f: F) -> Result<T, FaucetError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, FaucetError>>,
    {
        self.get().call(op, f).await
    }

    /// Run `fut` once through the effective context
    /// ([`CallContext::call_once`]).
    pub async fn call_once<T, Fut>(&self, op: &'static str, fut: Fut) -> Result<T, FaucetError>
    where
        Fut: Future<Output = Result<T, FaucetError>>,
    {
        self.get().call_once(op, fut).await
    }
}

/// Run any future as a one-shot backend call: `client.send().within(&slot,
/// "put").await?` is [`CallSlot::call_once`] for a future whose output is not
/// a [`FaucetError`] result (an SDK response), handing that output back
/// untouched. The outer `Result` is the call context running out — the
/// deadline or the cancel token.
pub trait CallExt: Future + Send + Sized {
    /// `self` as the call `op` through `slot`'s effective context.
    fn within<'a>(
        self,
        slot: &'a CallSlot,
        op: &'static str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Self::Output, FaucetError>> + Send + 'a>>
    where
        Self: 'a,
        Self::Output: Send,
    {
        Box::pin(slot.call_once(op, async move { Ok(self.await) }))
    }

    /// [`within`](Self::within) through a [`CallContext`].
    fn within_ctx<'a>(
        self,
        ctx: &'a CallContext,
        op: &'static str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Self::Output, FaucetError>> + Send + 'a>>
    where
        Self: 'a,
        Self::Output: Send,
    {
        Box::pin(ctx.call_once(op, async move { Ok(self.await) }))
    }
}

impl<F: Future + Send> CallExt for F {}

/// A streamed response body read under a [`CallContext`]: every read fails
/// once the context's cancel token fires or its deadline passes, so a body
/// that stops arriving mid-stream ends like any other call. Built with
/// [`CallContext::reader`].
pub struct CallReader<R> {
    inner: R,
    cancel: Option<std::pin::Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>>,
    deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl<R> fmt::Debug for CallReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallReader")
            .field("cancel", &self.cancel.is_some())
            .field("deadline", &self.deadline.is_some())
            .finish_non_exhaustive()
    }
}

impl CallContext {
    /// `inner` read under this context ([`CallReader`]).
    #[allow(clippy::disallowed_methods)] // the deadline timer of a read, not a wait
    pub fn reader<R>(&self, inner: R) -> CallReader<R> {
        CallReader {
            inner,
            cancel: self
                .inner
                .cancel
                .clone()
                .map(|c| Box::pin(c.cancelled_owned())),
            deadline: self
                .inner
                .deadline
                .map(|d| Box::pin(tokio::time::sleep_until(d))),
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CallReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        if let Some(c) = self.cancel.as_mut()
            && c.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                cancelled_error("read"),
            )));
        }
        if let Some(d) = self.deadline.as_mut()
            && d.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                deadline_error("read"),
            )));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;
    use crate::observability::RoundtripSide;
    use crate::resilience::BackoffKind;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fast(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            backoff: BackoffKind::None,
            base: Duration::ZERO,
            max: Duration::ZERO,
            jitter: false,
            ..RetryPolicy::default()
        }
    }

    fn status(code: u16) -> FaucetError {
        FaucetError::HttpStatus {
            status: code,
            url: "http://h/x".into(),
            body: String::new(),
        }
    }

    #[test]
    fn call_types_are_unwind_safe() {
        fn assert_unwind_safe<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert_unwind_safe::<CallContext>();
        assert_unwind_safe::<CallSlot>();
    }

    #[tokio::test]
    async fn call_once_makes_one_metered_attempt_whatever_the_policy() {
        let meter = Arc::new(crate::usage::UsageMeter::new());
        let rec = Arc::new(
            RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "c").with_meter(meter.clone()),
        );
        let slot = CallSlot::new(CallContext::new().with_retry(fast(5)));
        slot.install_recorder(rec);
        let err = slot
            .call_once("query", async { Err::<(), _>(status(503)) })
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::HttpStatus { status: 503, .. }));
        assert_eq!(slot.call_once("query", async { Ok(7) }).await.unwrap(), 7);
        assert_eq!(meter.snapshot().source_roundtrips["query"], 2);
    }

    #[tokio::test]
    async fn call_once_ends_at_the_deadline_and_on_cancel() {
        let ctx = CallContext::new().with_deadline(Instant::now() + Duration::from_millis(20));
        let err = ctx
            .call_once("query", std::future::pending::<Result<(), FaucetError>>())
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Timeout { .. }));
        let token = CancellationToken::new();
        token.cancel();
        let err = CallContext::new()
            .with_cancel(token)
            .call_once("query", async { Ok(()) })
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Cancelled { .. }));
    }

    #[tokio::test]
    async fn defaults_make_one_attempt_with_the_default_timeout() {
        let ctx = CallContext::new();
        assert_eq!(ctx.timeout(), Some(DEFAULT_CALL_TIMEOUT));
        assert!(ctx.retry().is_none());
        assert!(ctx.deadline().is_none());
        let calls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call("op", || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(status(503)) }
            })
            .await;
        assert!(matches!(
            r,
            Err(FaucetError::HttpStatus { status: 503, .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn call_once_bounds_a_one_shot_future_and_never_retries() {
        let ctx = CallContext::new()
            .with_timeout(Duration::from_secs(1))
            .with_retry(fast(3));
        let polls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call_once("read", async {
                polls.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            })
            .await;
        assert!(matches!(r, Err(FaucetError::Timeout { .. })), "{r:?}");
        assert_eq!(polls.load(Ordering::SeqCst), 1, "never retried");

        let r = ctx
            .call_once("read", async { Err::<(), _>(status(503)) })
            .await;
        assert!(matches!(
            r,
            Err(FaucetError::HttpStatus { status: 503, .. })
        ));

        let slot = CallSlot::new(CallContext::new());
        let cancel = CancellationToken::new();
        slot.install(CallContext::new().with_cancel(cancel.clone()));
        assert_eq!(slot.call_once("read", async { Ok(5) }).await.unwrap(), 5);
        cancel.cancel();
        let r = slot.call_once("read", async { Ok(5) }).await;
        assert!(matches!(r, Err(FaucetError::Cancelled { .. })), "{r:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn within_hands_back_the_output_or_the_spent_context() {
        let slot = CallSlot::new(CallContext::new().with_timeout(Duration::from_secs(1)));
        let out: Result<u8, &str> = async { Err("sdk") }.within(&slot, "put").await.unwrap();
        assert_eq!(out, Err("sdk"));
        let hung = std::future::pending::<u8>().within(&slot, "put").await;
        assert!(matches!(hung, Err(FaucetError::Timeout { .. })), "{hung:?}");
        let ctx = CallContext::new();
        assert_eq!(async { 3 }.within_ctx(&ctx, "get").await.unwrap(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reader_fails_at_the_deadline_and_on_cancel() {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut plain = CallContext::new().reader(&b"abc"[..]);
        plain.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"abc");
        assert!(format!("{plain:?}").contains("CallReader"));

        let (_tx, rx) = tokio::io::duplex(8);
        let ctx = CallContext::new().with_deadline(Instant::now() + Duration::from_secs(1));
        let mut stalled = ctx.reader(rx);
        let err = stalled.read_to_end(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);

        let (_tx, rx) = tokio::io::duplex(8);
        let cancel = CancellationToken::new();
        let mut stalled = CallContext::new().with_cancel(cancel.clone()).reader(rx);
        cancel.cancel();
        let err = stalled.read_to_end(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
        assert!(err.to_string().contains("cancelled"), "{err}");
    }

    #[tokio::test]
    async fn retries_classified_errors_under_the_policy() {
        let ctx = CallContext::new().with_retry(fast(3));
        let calls = AtomicU32::new(0);
        let r = ctx
            .call("op", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move { if n < 2 { Err(status(502)) } else { Ok(n) } }
            })
            .await;
        assert_eq!(r.unwrap(), 2);
        let calls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call("op", || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(status(404)) }
            })
            .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "a 404 is not retried");
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_attempt_times_out_and_is_retried() {
        let ctx = CallContext::new()
            .with_timeout(Duration::from_secs(1))
            .with_retry(fast(2));
        let calls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call("query", || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::pending()
            })
            .await;
        match r {
            Err(FaucetError::Timeout { operation, after }) => {
                assert_eq!(operation, "query call");
                assert_eq!(after, Duration::from_secs(1));
            }
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_ends_the_whole_call() {
        let start = Instant::now();
        let ctx = CallContext::new()
            .without_timeout()
            .with_deadline(start + Duration::from_secs(5))
            .with_retry(RetryPolicy {
                max_attempts: 100,
                backoff: BackoffKind::Fixed,
                base: Duration::from_secs(2),
                max: Duration::from_secs(2),
                jitter: false,
                ..RetryPolicy::default()
            });
        let r: Result<(), _> = ctx.call("op", || async { Err(status(503)) }).await;
        assert!(matches!(r, Err(FaucetError::Timeout { .. })), "{r:?}");
        assert!(Instant::now() - start <= Duration::from_secs(5));
        let r: Result<(), _> = ctx.call("op", || async { Ok(()) }).await;
        assert!(
            matches!(r, Err(FaucetError::Timeout { .. })),
            "past deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_cuts_an_attempt_short() {
        let ctx = CallContext::new()
            .with_timeout(Duration::from_secs(60))
            .with_deadline(Instant::now() + Duration::from_secs(3));
        let r: Result<(), _> = ctx.call("op", std::future::pending).await;
        match r {
            Err(FaucetError::Timeout { after, .. }) => assert!(after <= Duration::from_secs(3)),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_an_attempt_and_fails_fast_afterwards() {
        let token = CancellationToken::new();
        let ctx = CallContext::new().with_cancel(token.clone());
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            t.cancel();
        });
        let r: Result<(), _> = ctx.call("op", std::future::pending).await;
        assert!(matches!(r, Err(FaucetError::Cancelled { .. })), "{r:?}");
        assert!(ctx.is_cancelled());
        let calls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call("op", || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await;
        assert!(matches!(r, Err(FaucetError::Cancelled { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_honours_cancel_and_deadline() {
        let start = Instant::now();
        CallContext::new()
            .wait(Duration::from_secs(4))
            .await
            .unwrap();
        assert!(Instant::now() - start >= Duration::from_secs(4));
        let ctx = CallContext::new().with_deadline(Instant::now() + Duration::from_secs(1));
        assert!(matches!(
            ctx.wait(Duration::from_secs(10)).await,
            Err(FaucetError::Timeout { .. })
        ));
        let token = CancellationToken::new();
        let ctx = CallContext::new().with_cancel(token.clone());
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            t.cancel();
        });
        assert!(matches!(
            ctx.wait(Duration::from_secs(10)).await,
            Err(FaucetError::Cancelled { .. })
        ));
        assert!(matches!(
            ctx.wait(Duration::ZERO).await,
            Err(FaucetError::Cancelled { .. })
        ));
        let slot = CallSlot::default();
        slot.wait(Duration::from_millis(1)).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limit_waits_its_stated_time() {
        let ctx = CallContext::new().with_retry(fast(2));
        let calls = AtomicU32::new(0);
        let start = Instant::now();
        let r = ctx
            .call("op", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(FaucetError::RateLimited(Duration::from_secs(7)))
                    } else {
                        Ok(n)
                    }
                }
            })
            .await;
        assert_eq!(r.unwrap(), 1);
        assert!(Instant::now() - start >= Duration::from_secs(7));
    }

    #[tokio::test(start_paused = true)]
    async fn the_rate_limit_budget_does_not_use_attempts() {
        let ctx = CallContext::new()
            .with_retry(fast(1))
            .with_rate_limit_retries(3);
        let calls = AtomicU32::new(0);
        let r = ctx
            .call("op", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 3 {
                        Err(FaucetError::RateLimited(Duration::from_millis(10)))
                    } else {
                        Ok(n)
                    }
                }
            })
            .await;
        assert_eq!(r.unwrap(), 3, "three rate limits retried with one attempt");
        let calls = AtomicU32::new(0);
        let r: Result<(), _> = ctx
            .call("op", || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(FaucetError::RateLimited(Duration::from_millis(1))) }
            })
            .await;
        assert!(matches!(r, Err(FaucetError::RateLimited(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 4, "1 + 3 consecutive retries");
    }

    #[tokio::test]
    async fn every_attempt_is_metered() {
        let meter = Arc::new(crate::usage::UsageMeter::new());
        let rec = Arc::new(
            RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "c").with_meter(meter.clone()),
        );
        let ctx = CallContext::new()
            .with_recorder(rec.clone())
            .with_retry(fast(3));
        let calls = AtomicU32::new(0);
        let _ = ctx
            .call("get", || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(FaucetError::RateLimited(Duration::ZERO))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        ctx.record("list");
        ctx.signal("bytes_billed", "bytes", 5.0);
        let unmetered = ctx.clone().with_roundtrip_metering(false);
        unmetered.call("get", || async { Ok(()) }).await.unwrap();
        let snap = meter.snapshot();
        assert_eq!(snap.source_roundtrips["get"], 2);
        assert_eq!(snap.source_roundtrips["list"], 1);
        assert_eq!(snap.throttled, 1);
        assert_eq!(snap.signals.len(), 1);
    }

    #[tokio::test]
    async fn the_final_error_is_redacted() {
        let ctx = CallContext::new().with_redactor(Arc::new(|s: &str| s.replace("SECRET", "***")));
        let r: Result<(), _> = ctx
            .call("op", || async {
                Err(FaucetError::HttpStatus {
                    status: 400,
                    url: "http://h/?k=SECRET".into(),
                    body: "bad SECRET".into(),
                })
            })
            .await;
        match r {
            Err(FaucetError::HttpStatus { url, body, .. }) => {
                assert_eq!(url, "http://h/?k=***");
                assert_eq!(body, "bad ***");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            ctx.redact_error(FaucetError::Config("SECRET".into())),
            FaucetError::Config(s) if s == "SECRET"
        ));
        assert_eq!(CallContext::new().redact("plain"), "plain");
    }

    async fn refused(url: &str) -> FaucetError {
        let client = crate::http::Client::new().unwrap();
        client.send(client.get(url)).await.unwrap_err()
    }

    #[tokio::test]
    async fn transport_error_urls_lose_credentials() {
        let e = refused("http://user:pw@127.0.0.1:1/x?token=SECRET").await;
        let text = CallContext::new().redact_error(e).to_string();
        assert!(!text.contains("pw@"), "{text}");
        assert!(!text.contains("SECRET"), "{text}");
        let FaucetError::Http(e) = refused("http://127.0.0.1:1/x").await else {
            panic!("expected a transport error");
        };
        assert!(e.url().is_some());
        let kept = redact_reqwest_url(e, |u| u.to_string());
        assert!(kept.url().is_some(), "an unchanged URL is kept");
        let dropped = redact_reqwest_url(kept, |_| "not a url".into());
        assert!(dropped.url().is_none());
        assert!(
            redact_reqwest_url(dropped, |_| String::new())
                .url()
                .is_none()
        );
    }

    #[test]
    fn inherit_layers_connector_settings_over_the_installed_context() {
        let token = CancellationToken::new();
        let now = Instant::now();
        let rec = Arc::new(RoundtripRecorder::new(RoundtripSide::Sink, "p", "r", "c"));
        let installed = CallContext::new()
            .with_cancel(token.clone())
            .with_recorder(rec)
            .with_timeout(Duration::from_secs(9))
            .with_deadline(now + Duration::from_secs(50))
            .with_redactor(Arc::new(|_| "X".into()));
        let local = CallContext::new()
            .with_retry(fast(4))
            .with_rate_limit_retries(2)
            .with_deadline(now + Duration::from_secs(10))
            .with_roundtrip_metering(false);
        let eff = local.inherit(&installed);
        assert_eq!(eff.timeout(), Some(Duration::from_secs(9)));
        assert_eq!(eff.retry().unwrap().max_attempts, 4);
        assert_eq!(eff.rate_limit_retries(), Some(2));
        assert!(eff.cancel().is_some() && eff.recorder().is_some());
        assert_eq!(eff.deadline(), Some(now + Duration::from_secs(10)));
        assert_eq!(eff.redact("y"), "X");
        let eff = CallContext::new().without_timeout().inherit(&installed);
        assert_eq!(eff.timeout(), None);
        let eff = CallContext::new().without_retry().inherit(&installed);
        assert_eq!(eff.retry().unwrap().max_attempts, 1);
        assert!(format!("{eff:?}").contains("CallContext"));
    }

    #[tokio::test]
    async fn a_slot_uses_its_own_settings_until_installed() {
        let mut slot = CallSlot::new(CallContext::new().with_timeout(Duration::from_secs(3)));
        assert_eq!(slot.get().timeout(), Some(Duration::from_secs(3)));
        assert!(slot.recorder().is_none());
        slot.record("noop");
        slot.set_local(CallContext::new().with_retry(fast(2)));
        assert_eq!(slot.local().retry().unwrap().max_attempts, 2);
        let rec = Arc::new(RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "c"));
        slot.install_recorder(rec);
        assert!(slot.recorder().is_some());
        let token = CancellationToken::new();
        slot.install(CallContext::new().with_cancel(token.clone()));
        slot.install(CallContext::new());
        let eff = slot.get();
        assert!(eff.cancel().is_some(), "first install wins");
        assert!(eff.recorder().is_some(), "recorder fallback");
        assert_eq!(eff.retry().unwrap().max_attempts, 2);
        assert_eq!(slot.call("op", || async { Ok(1) }).await.unwrap(), 1);
        token.cancel();
        let r: Result<(), _> = slot.call("op", || async { Ok(()) }).await;
        assert!(matches!(r, Err(FaucetError::Cancelled { .. })));
    }
}
