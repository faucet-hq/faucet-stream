//! Log shipping for one `faucet run` process (#806): the whole process is one
//! run in the spool, shipped in the background and flushed (bounded) at exit.

use crate::config::PipelineConfig;
use crate::logship::LogExportView;

#[cfg(feature = "otel")]
static TICKS: std::sync::Mutex<Option<std::sync::Arc<crate::logship::session::LogShipSession>>> =
    std::sync::Mutex::new(None);

/// Run-log shipping for a `faucet schedule` process: each tick is a run in
/// the spool; the shipper drains in the background between ticks.
pub struct ScheduleLogs;

impl ScheduleLogs {
    /// Start shipping when `cfg` exports logs; inert otherwise.
    pub fn start(cfg: &PipelineConfig, pipeline: &str) -> Self {
        #[cfg(feature = "otel")]
        if let Some(s) = crate::logship::session::LogShipSession::start(cfg, pipeline)
            && let Ok(mut t) = TICKS.lock()
        {
            *t = Some(std::sync::Arc::new(s));
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = (cfg, pipeline);
        }
        Self
    }

    /// Flush (bounded) and stop shipping.
    pub async fn shutdown(self) {
        #[cfg(feature = "otel")]
        {
            let s = TICKS.lock().ok().and_then(|mut t| t.take());
            if let Some(s) = s.and_then(|s| std::sync::Arc::try_unwrap(s).ok()) {
                s.shutdown().await;
            }
        }
    }
}

/// Open a spool run for one schedule tick and tag `span` with it. Returns the
/// run id to pass to [`end_tick`].
pub fn begin_tick(span: &tracing::Span) -> Option<String> {
    #[cfg(feature = "otel")]
    {
        let s = TICKS.lock().ok()?.clone()?;
        let id = crate::logship::session::new_run_id();
        s.begin_run(&id, Default::default());
        span.record("log_run_id", id.as_str());
        Some(id)
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = span;
        None
    }
}

/// Close a tick's spool run; its lines keep shipping in the background.
pub fn end_tick(id: &str) {
    #[cfg(feature = "otel")]
    if let Some(s) = TICKS.lock().ok().and_then(|t| t.clone()) {
        s.end_run(id);
    }
    #[cfg(not(feature = "otel"))]
    let _ = id;
}

/// The run-log shipping of one `faucet run` invocation.
pub struct RunLogs {
    #[cfg(feature = "otel")]
    session: Option<(crate::logship::session::LogShipSession, String)>,
}

impl RunLogs {
    /// Start shipping when `cfg` exports logs; inert otherwise.
    pub fn start(cfg: &PipelineConfig, pipeline: &str) -> Self {
        #[cfg(feature = "otel")]
        {
            let session = crate::logship::session::LogShipSession::start(cfg, pipeline).map(|s| {
                let run_id = crate::logship::session::new_run_id();
                s.begin_run(&run_id, Default::default());
                s.set_default_run(Some(&run_id));
                (s, run_id)
            });
            Self { session }
        }
        #[cfg(not(feature = "otel"))]
        {
            if crate::logship::session_wanted(cfg) {
                tracing::warn!(
                    "observability.otel.export lists `logs` but this binary was built without \
                     --features otel; run logs are not shipped"
                );
            }
            let _ = pipeline;
            Self {}
        }
    }

    /// Close the run, ship what is pending within `flush_timeout_secs`, stop
    /// the shipper, and report the run's export status. `None` when shipping
    /// is off.
    pub async fn finish(self) -> Option<LogExportView> {
        #[cfg(feature = "otel")]
        {
            let (session, run_id) = self.session?;
            let view = session.finish_run(&run_id).await;
            session.shutdown().await;
            Some(view)
        }
        #[cfg(not(feature = "otel"))]
        {
            None
        }
    }
}
