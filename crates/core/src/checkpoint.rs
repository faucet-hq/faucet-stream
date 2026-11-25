//! The checkpoint core shared by the row, Arrow and native page loops.
//!
//! Every loop makes a page durable the same way ([ADR 0002]): main-sink flush,
//! then DLQ flush, then the state write, then the bookmark-lag gauges. The end
//! of a run flushes the DLQ before the main sink, and an error exit flushes
//! what it can before returning the original error. Keeping this in one place
//! is what keeps the three loops from drifting apart.
//!
//! [ADR 0002]: https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0002-checkpoint-ordering.md

use std::sync::Arc;

use metrics::{Label, SharedString, counter};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::FaucetError;
use crate::resilience::{RetryMetrics, RetryPolicy, execute_with_policy_metered};
use crate::state::StateStore;
use crate::traits::Sink;

/// Whether the main sink's output is held until the run commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hold {
    /// Flush and persist at every checkpoint.
    None,
    /// Overwrite: no mid-run sink flush and no state write; an error exit and a
    /// cancelled end skip the sink flush so the prior destination is untouched.
    Overwrite,
}

/// What a checkpoint stores for its bookmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stored {
    /// The bookmark as it is.
    Bare,
    /// The exactly-once wrapper around the bookmark and this sequence number.
    ExactlyOnce(u64),
}

/// The retry policy the per-page flushes and state writes run under.
pub(crate) struct Retry<'a> {
    pub(crate) policy: &'a RetryPolicy,
    pub(crate) cancel: Option<&'a CancellationToken>,
}

/// One run's checkpoint core.
pub(crate) struct Checkpointer<'a, Si: Sink + ?Sized> {
    sink: &'a Si,
    dlq: Option<&'a Arc<dyn Sink>>,
    state: Option<(&'a dyn StateStore, &'a str)>,
    hold: Hold,
    retry: Option<Retry<'a>>,
    flush_sink_on_unwind: bool,
    pipeline: &'a str,
    row: &'a str,
    run_id: &'a str,
    dlq_labels: Vec<Label>,
    accounts: Option<&'a crate::accounting::RowAccounts>,
}

impl<'a, Si: Sink + ?Sized> Checkpointer<'a, Si> {
    pub(crate) fn new(
        sink: &'a Si,
        dlq: Option<&'a Arc<dyn Sink>>,
        state: Option<(&'a dyn StateStore, &'a str)>,
        hold: Hold,
        (pipeline, row, run_id): (&'a str, &'a str, &'a str),
    ) -> Self {
        let dlq_labels = match dlq {
            Some(d) => vec![
                Label::new("pipeline", SharedString::from(pipeline.to_string())),
                Label::new("row", SharedString::from(row.to_string())),
                Label::new(
                    "connector",
                    SharedString::from(sink.connector_name().to_string()),
                ),
                Label::new(
                    "dlq_connector",
                    SharedString::from(d.connector_name().to_string()),
                ),
            ],
            None => Vec::new(),
        };
        Self {
            sink,
            dlq,
            state,
            hold,
            retry: None,
            flush_sink_on_unwind: true,
            pipeline,
            row,
            run_id,
            dlq_labels,
            accounts: None,
        }
    }

    /// Mark the run's accepted rows confirmed at each completed checkpoint.
    pub(crate) fn with_row_accounts(
        mut self,
        accounts: Option<&'a crate::accounting::RowAccounts>,
    ) -> Self {
        self.accounts = accounts;
        self
    }

    fn confirm(&self) {
        if self.hold == Hold::None
            && let Some(a) = self.accounts
        {
            a.confirm();
        }
    }

    /// Retry the per-page flushes and state writes under `retry`.
    pub(crate) fn with_retry(mut self, retry: Option<Retry<'a>>) -> Self {
        self.retry = retry;
        self
    }

    /// Leave the main sink unflushed on an error exit (the native byte path,
    /// whose load session can hold a partly fed object).
    pub(crate) fn without_unwind_sink_flush(mut self) -> Self {
        self.flush_sink_on_unwind = false;
        self
    }

    async fn retried<T, F, Fut>(&self, op: &'static str, f: F) -> Result<T, FaucetError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, FaucetError>>,
    {
        let mut f = f;
        match &self.retry {
            Some(r) => {
                let m = RetryMetrics {
                    pipeline: self.pipeline.to_string(),
                    row: self.row.to_string(),
                    op,
                };
                execute_with_policy_metered(r.policy, r.cancel, &m, f).await
            }
            None => f().await,
        }
    }

    fn dlq_flush_error(&self, e: FaucetError) -> FaucetError {
        let mut lbl = self.dlq_labels.clone();
        lbl.push(Label::new(
            "kind",
            SharedString::const_str(crate::observability::decorator::error_kind(&e)),
        ));
        counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
        FaucetError::Sink(format!("DLQ sink flush failed: {e}"))
    }

    /// Make one page durable: sink flush (unless held) → DLQ flush → state
    /// write and bookmark-lag gauges (unless held).
    pub(crate) async fn page(&self, bookmark: &Value, stored: Stored) -> Result<(), FaucetError> {
        if self.hold == Hold::None {
            self.retried("flush", || self.sink.flush()).await?;
        }
        if let Some(dlq) = self.dlq {
            let _timer = crate::observability::DurationGuard::new(
                "faucet_sink_dlq_flush_duration_seconds",
                self.dlq_labels.clone(),
            );
            self.retried("flush", || dlq.flush())
                .await
                .map_err(|e| self.dlq_flush_error(e))?;
        }
        if self.hold == Hold::None {
            if let Some((store, key)) = self.state {
                let value = match stored {
                    Stored::Bare => bookmark.clone(),
                    Stored::ExactlyOnce(seq) => crate::idempotency::wrap_state(Some(bookmark), seq),
                };
                self.retried("state_put", || store.put(key, &value)).await?;
            }
            self.update_lag(bookmark);
        }
        self.confirm();
        Ok(())
    }

    /// Persist one bookmark with no flush: a held bookmark after the run's
    /// output was committed.
    pub(crate) async fn persist(&self, bookmark: &Value) -> Result<(), FaucetError> {
        if let Some((store, key)) = self.state {
            store.put(key, bookmark).await?;
        }
        self.update_lag(bookmark);
        Ok(())
    }

    fn update_lag(&self, bookmark: &Value) {
        let labels = crate::observability::Labels::new(self.pipeline, self.row, self.run_id);
        crate::observability::update_bookmark_lag(bookmark, &labels);
    }

    /// End of stream or cancel: DLQ flush first (its failure is returned before
    /// the main sink is touched), then the main sink unless a cancelled run
    /// holds its output.
    pub(crate) async fn finish(&self, cancelled: bool) -> Result<(), FaucetError> {
        if let Some(dlq) = self.dlq {
            let _timer = crate::observability::DurationGuard::new(
                "faucet_sink_dlq_flush_duration_seconds",
                self.dlq_labels.clone(),
            );
            dlq.flush().await.map_err(|e| self.dlq_flush_error(e))?;
        }
        if cancelled && self.hold == Hold::Overwrite {
            return Ok(());
        }
        self.sink.flush().await?;
        self.confirm();
        Ok(())
    }

    /// Error exit: best-effort DLQ flush, then the main sink unless held; a
    /// flush failure is logged and the original error returned.
    pub(crate) async fn unwind(&self, err: FaucetError) -> FaucetError {
        if let Some(dlq) = self.dlq
            && let Err(flush_err) = dlq.flush().await
        {
            tracing::warn!(
                error = %flush_err,
                "DLQ sink flush failed during error unwind; original error preserved"
            );
        }
        if self.hold == Hold::Overwrite {
            tracing::info!(
                "pipeline run failed during overwrite; sink flush skipped so the \
                 destination is unchanged"
            );
        } else if !self.flush_sink_on_unwind {
            tracing::debug!(
                "pipeline run failed on the native path; the load session is left unfinished"
            );
        } else if let Err(flush_err) = self.sink.flush().await {
            tracing::warn!(
                error = %flush_err,
                "sink flush failed during error unwind; original error preserved"
            );
        }
        err
    }
}

#[cfg(test)]
mod tests;
