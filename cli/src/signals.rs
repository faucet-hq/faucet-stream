//! Process termination signals, per platform (#565).
//!
//! The long-running verbs (`schedule`, `serve`, `mirror`, `backfill`) drain
//! gracefully when asked to stop. What "asked to stop" means differs by
//! platform: Ctrl-C everywhere, plus `SIGTERM` on Unix, plus Ctrl-Break, a
//! console-window close, and a system shutdown / logoff on Windows.

use std::future::Future;

/// A registered set of stop signals. Register once and keep it, so a signal
/// that arrives between two `recv` calls is not lost.
pub struct Terminate {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_break: tokio::signal::windows::CtrlBreak,
    #[cfg(windows)]
    ctrl_close: tokio::signal::windows::CtrlClose,
    #[cfg(windows)]
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

impl Terminate {
    /// Install the platform's stop-signal handlers.
    pub fn new() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                term: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows;
            Ok(Self {
                ctrl_break: windows::ctrl_break()?,
                ctrl_close: windows::ctrl_close()?,
                ctrl_shutdown: windows::ctrl_shutdown()?,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            Ok(Self {})
        }
    }

    /// Resolve when any stop signal arrives.
    pub async fn recv(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = self.term.recv() => {}
            }
        }
        #[cfg(windows)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = self.ctrl_break.recv() => {}
                _ = self.ctrl_close.recv() => {}
                _ = self.ctrl_shutdown.recv() => {}
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Wait for a stop signal; if the handlers cannot be installed, fall back to
/// Ctrl-C alone.
pub fn wait_for_termination() -> impl Future<Output = ()> {
    let registered = Terminate::new();
    async move {
        match registered {
            Ok(mut t) => t.recv().await,
            Err(e) => {
                tracing::warn!("stop-signal handlers unavailable ({e}); only Ctrl-C will stop");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
}

/// The stop signals of a one-shot run: the first SIGTERM / Ctrl-C cancels the
/// run cooperatively (each row finishes its page and flushes), a second one
/// exits at once.
pub struct StopSignal {
    token: faucet_core::CancellationToken,
    watcher: tokio::task::JoinHandle<()>,
}

impl StopSignal {
    /// Install the handlers; the watcher stops when this is dropped.
    pub fn install() -> Self {
        let token = faucet_core::CancellationToken::new();
        let first = wait_for_termination();
        let cancel = token.clone();
        let watcher = tokio::spawn(async move {
            first.await;
            tracing::warn!(
                "stop signal received: finishing the current page and flushing (signal again to \
                 exit at once)"
            );
            cancel.cancel();
            wait_for_termination().await;
            std::process::exit(130);
        });
        Self { token, watcher }
    }

    /// The token the first signal cancels.
    pub fn token(&self) -> faucet_core::CancellationToken {
        self.token.clone()
    }

    /// The run's result, or [`CliError::Cancelled`](crate::error::CliError::Cancelled) when it was stopped.
    pub fn finish(self, outcome: crate::error::CliResult<()>) -> crate::error::CliResult<()> {
        cancelled_outcome(self.token.is_cancelled(), outcome)
    }
}

impl Drop for StopSignal {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

pub(crate) fn cancelled_outcome(
    cancelled: bool,
    outcome: crate::error::CliResult<()>,
) -> crate::error::CliResult<()> {
    if cancelled {
        return Err(crate::error::CliError::Cancelled);
    }
    outcome
}
