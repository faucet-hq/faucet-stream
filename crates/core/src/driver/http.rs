//! The HTTP family (RFC 0014): send a record, or an array of records, to an
//! endpoint.
//!
//! An [`HttpDriver`] knows how to authenticate a page and send one request
//! body; [`HttpSinkDriver`] drives batching, the in-flight limit and which
//! rows a failed request leaves undelivered. Requests are made through the
//! [`CallSlot`] the driver is handed, so the call context's retry policy,
//! deadline and metering apply to each one.

use super::sink::{PageWrite, RowPolicy, SinkDriver};
use crate::check::{CheckContext, CheckReport};
use crate::dlq::BatchAtomicity;
use crate::error::FaucetError;
use crate::io::{CallContext, CallSlot};
use crate::traits::RowOutcome;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;

/// How a page becomes requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpBatching {
    /// One request per record, at most `concurrency` in flight.
    PerRecord {
        /// Requests in flight at once (at least 1).
        concurrency: usize,
    },
    /// One request per `batch_size` records, the body a JSON array (`0`: one
    /// request for the page).
    Array {
        /// Records per request.
        batch_size: usize,
    },
}

/// An HTTP destination's primitives (RFC 0014). Wrap it in an
/// [`HttpSinkDriver`], then a [`DriverSink`](super::DriverSink).
#[async_trait]
pub trait HttpDriver: Send + Sync {
    /// What one page's requests share — resolved credentials, typically.
    type Session: Send + Sync;

    /// The connector's name.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// The endpoint, for lineage (credential-free).
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The connector's own call defaults (its retry policy).
    fn call_context(&self) -> CallContext {
        CallContext::new()
    }

    /// A non-mutating preflight probe.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        let _ = (calls, ctx);
        Ok(CheckReport::not_implemented())
    }

    /// What a failed write leaves behind.
    fn batch_atomicity(&self) -> BatchAtomicity {
        BatchAtomicity::BestEffort
    }

    /// How pages become requests.
    fn batching(&self) -> HttpBatching;

    /// Prepare what one page's requests share.
    async fn session(&self, calls: &CallSlot) -> Result<Self::Session, FaucetError>;

    /// Send one body. `Ok` once the endpoint accepted it.
    async fn send(
        &self,
        calls: &CallSlot,
        session: &Self::Session,
        body: &Value,
    ) -> Result<(), FaucetError>;
}

/// An [`HttpDriver`] as a [`SinkDriver`].
///
/// Per record: the whole-page write stops at the first failed request; the
/// per-row write sends every record and reports each. As arrays: requests go
/// out in order; the whole-page write stops at the first failure, and the
/// per-row write reports the rows of every request after a delivered one as
/// failed, so a delivered row is never reported failed — and a page none of
/// whose requests was delivered fails as a whole.
pub struct HttpSinkDriver<D> {
    driver: D,
}

impl<D: HttpDriver> HttpSinkDriver<D> {
    /// The sink driver over `driver`.
    pub fn new(driver: D) -> Self {
        Self { driver }
    }

    /// The HTTP driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// The HTTP driver, mutably.
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }

    async fn per_record(
        &self,
        calls: &CallSlot,
        session: &D::Session,
        rows: &[Value],
        concurrency: usize,
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        let concurrency = concurrency.max(1);
        if policy == RowPolicy::WholePage {
            let mut in_flight = FuturesUnordered::new();
            let mut iter = rows.iter();
            for record in iter.by_ref().take(concurrency) {
                in_flight.push(self.driver.send(calls, session, record));
            }
            while let Some(result) = in_flight.next().await {
                result?;
                if let Some(record) = iter.next() {
                    in_flight.push(self.driver.send(calls, session, record));
                }
            }
            return Ok(PageWrite::accepted(rows.len()));
        }
        let pending: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(i, record)| async move { (i, self.driver.send(calls, session, record).await) })
            .collect();
        let mut indexed: Vec<(usize, RowOutcome)> = futures::stream::iter(pending)
            .buffer_unordered(concurrency)
            .collect()
            .await;
        indexed.sort_by_key(|(i, _)| *i);
        Ok(PageWrite::from_outcomes(
            indexed.into_iter().map(|(_, o)| o).collect(),
        ))
    }

    async fn arrays(
        &self,
        calls: &CallSlot,
        session: &D::Session,
        rows: &[Value],
        batch_size: usize,
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        let mut delivered = 0usize;
        let mut failed = Vec::new();
        let mut cause: Option<String> = None;
        for chunk in crate::batching::chunks(rows, batch_size) {
            let start = delivered + failed.len();
            if let Some(msg) = &cause {
                failed.extend((start..start + chunk.len()).map(|i| {
                    (
                        i,
                        FaucetError::Sink(format!(
                            "array-mode chunk not sent after earlier failure: {msg}"
                        )),
                    )
                }));
                continue;
            }
            match self
                .driver
                .send(calls, session, &Value::Array(chunk.to_vec()))
                .await
            {
                Ok(()) => delivered += chunk.len(),
                Err(e) if policy == RowPolicy::WholePage || delivered == 0 => return Err(e),
                Err(e) => {
                    failed.extend((start..start + chunk.len()).map(|i| {
                        (
                            i,
                            FaucetError::Sink(
                                "array-mode chunk POST failed; rows not delivered".into(),
                            ),
                        )
                    }));
                    cause = Some(e.to_string());
                }
            }
        }
        Ok(PageWrite::accepted(delivered).with_failures(failed))
    }
}

#[async_trait]
impl<D: HttpDriver> SinkDriver for HttpSinkDriver<D> {
    fn connector_name(&self) -> &'static str {
        self.driver.connector_name()
    }

    fn config_schema(&self) -> Value {
        self.driver.config_schema()
    }

    fn dataset_uri(&self) -> String {
        self.driver.dataset_uri()
    }

    fn call_context(&self) -> CallContext {
        self.driver.call_context()
    }

    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        self.driver.check(calls, ctx).await
    }

    fn batch_atomicity(&self) -> BatchAtomicity {
        self.driver.batch_atomicity()
    }

    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        if rows.is_empty() {
            return Ok(PageWrite::accepted(0));
        }
        let session = self.driver.session(calls).await?;
        match self.driver.batching() {
            HttpBatching::PerRecord { concurrency } => {
                self.per_record(calls, &session, rows, concurrency, policy)
                    .await
            }
            HttpBatching::Array { batch_size } => {
                self.arrays(calls, &session, rows, batch_size, policy).await
            }
        }
    }
}
