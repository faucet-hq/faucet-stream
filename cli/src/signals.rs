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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn sigterm_resolves_recv_and_the_helper() {
        let mut t = Terminate::new().expect("install handlers");
        let wait = wait_for_termination();
        // SAFETY: signalling our own pid; tokio's handler is installed above,
        // so SIGTERM is delivered to it instead of terminating the process.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGTERM);
        }
        tokio::time::timeout(Duration::from_secs(5), t.recv())
            .await
            .expect("recv resolves on SIGTERM");
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("helper resolves on SIGTERM");
    }
}
