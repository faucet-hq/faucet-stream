//! The family-neutral source driver contract and its [`Source`] adapter.

use crate::capabilities::Capabilities;
use crate::check::{CheckContext, CheckReport, Probe};
use crate::error::FaucetError;
use crate::io::{CallContext, CallSlot};
use crate::observability::RoundtripRecorder;
use crate::pipeline::StreamPage;
use crate::traits::Source;
use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// A stream of pages a [`SourceDriver`] reads.
pub type PageStream<'a> = Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>;

/// A dataset's read primitives (RFC 0014). Wrap it in a [`DriverSource`] to
/// get a [`Source`].
///
/// [`read`](Self::read) streams the dataset in pages, each with the bookmark
/// to resume after it (when there is one); every backend request goes through
/// the [`CallSlot`] it is handed.
///
/// ```
/// use faucet_core::driver::{DriverSource, PageStream, SourceDriver};
/// use faucet_core::{CallSlot, Source, StreamPage, Value, async_trait, json};
/// use std::collections::HashMap;
///
/// struct Numbers;
///
/// #[async_trait]
/// impl SourceDriver for Numbers {
///     fn connector_name(&self) -> &'static str {
///         "numbers"
///     }
///     fn read<'a>(
///         &'a self,
///         _calls: &'a CallSlot,
///         _context: &'a HashMap<String, Value>,
///         _batch_size: usize,
///     ) -> PageStream<'a> {
///         Box::pin(faucet_core::async_stream::stream! {
///             yield Ok(StreamPage { records: vec![json!(1), json!(2)], bookmark: Some(json!(2)) });
///         })
///     }
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let source = DriverSource::new(Numbers);
/// let (rows, bookmark) = source.fetch_all_incremental().await.unwrap();
/// assert_eq!(rows.len(), 2);
/// assert_eq!(bookmark, Some(json!(2)));
/// # });
/// ```
#[async_trait]
pub trait SourceDriver: Send + Sync {
    /// The connector's name, the `connector` metric label.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// The dataset this source reads, for lineage (credential-free).
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The connector's own call defaults. The run's context is layered over
    /// it.
    fn call_context(&self) -> CallContext {
        CallContext::new()
    }

    /// Stream the dataset in pages of about `batch_size` records (`0`: one
    /// page). `context` holds the parent record of a fan-out row.
    fn read<'a>(
        &'a self,
        calls: &'a CallSlot,
        context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> PageStream<'a>;

    /// The state key bookmarks are stored under, when the source resumes.
    fn state_key(&self) -> Option<String> {
        None
    }

    /// Resume after `bookmark` on the next [`read`](Self::read).
    async fn resume_from(&self, bookmark: Value) -> Result<(), FaucetError> {
        let _ = bookmark;
        Ok(())
    }

    /// Whether a resumed read replays exactly what follows the bookmark.
    fn deterministic_replay(&self) -> bool {
        false
    }

    /// Whether reading removes what was read (a queue).
    fn consumes_destructively(&self) -> bool {
        false
    }

    /// Take the run's cancellation token. Return `true` when the source then
    /// yields what it buffered and ends, so the pipeline polls it to its end.
    fn set_cancel(&self, cancel: crate::CancellationToken) -> bool {
        let _ = cancel;
        false
    }

    /// A non-mutating preflight probe. `None` (the default) reads one page.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<Option<CheckReport>, FaucetError> {
        let _ = (calls, ctx);
        Ok(None)
    }

    /// Adjust the capabilities [`DriverSource`] derived. Default: unchanged.
    fn capabilities(&self, derived: Capabilities) -> Capabilities {
        derived
    }
}

/// A [`SourceDriver`] as a [`Source`] (RFC 0014): every fetch is the driver's
/// page stream, and the run's call context reaches every read.
pub struct DriverSource<D> {
    driver: D,
    calls: CallSlot,
}

impl<D: SourceDriver> DriverSource<D> {
    /// The source over `driver`.
    pub fn new(driver: D) -> Self {
        let calls = CallSlot::new(driver.call_context());
        Self { driver, calls }
    }

    /// The driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// The driver, mutably.
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }

    /// The call slot the reads receive.
    pub fn calls(&self) -> &CallSlot {
        &self.calls
    }

    /// Unwrap the driver.
    pub fn into_driver(self) -> D {
        self.driver
    }

    async fn drain(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let mut pages = self.driver.read(&self.calls, context, 0);
        let mut rows = Vec::new();
        let mut bookmark = None;
        while let Some(page) = pages.next().await {
            let page = page?;
            rows.extend(page.records);
            if page.bookmark.is_some() {
                bookmark = page.bookmark;
            }
        }
        Ok((rows, bookmark))
    }
}

#[async_trait]
impl<D: SourceDriver> Source for DriverSource<D> {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.drain(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.drain(context).await
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> PageStream<'a> {
        self.driver.read(&self.calls, context, batch_size)
    }

    fn config_schema(&self) -> Value {
        self.driver.config_schema()
    }

    fn state_key(&self) -> Option<String> {
        self.driver.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.driver.resume_from(bookmark).await
    }

    fn supports_exactly_once(&self) -> bool {
        self.driver.deterministic_replay()
    }

    fn consumes_destructively(&self) -> bool {
        self.driver.consumes_destructively()
    }

    fn capabilities(&self) -> Capabilities {
        self.driver.capabilities(Capabilities::of_source(self))
    }

    fn connector_name(&self) -> &'static str {
        self.driver.connector_name()
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        self.calls.install_recorder(recorder);
    }

    fn set_call_context(&self, ctx: CallContext) {
        self.calls.install(ctx);
    }

    fn set_cancel(&self, cancel: crate::CancellationToken) -> bool {
        self.driver.set_cancel(cancel)
    }

    fn dataset_uri(&self) -> String {
        self.driver.dataset_uri()
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        if let Some(report) = self.driver.check(&self.calls, ctx).await? {
            return Ok(report);
        }
        let empty = HashMap::new();
        let start = std::time::Instant::now();
        let mut pages = self.driver.read(&self.calls, &empty, 1);
        let probe = match tokio::time::timeout(ctx.timeout, pages.next()).await {
            Err(_) => Probe::fail("read", start.elapsed(), "timed out fetching first page"),
            Ok(None) | Ok(Some(Ok(_))) => Probe::pass("read", start.elapsed()),
            Ok(Some(Err(e))) => Probe::fail("read", start.elapsed(), e.to_string()),
        };
        Ok(CheckReport::single(probe))
    }
}
