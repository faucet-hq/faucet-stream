//! Live terminal UI for `faucet run --tui` (#203, `cli-tui` feature).
//!
//! The pipeline is untouched: it emits the same `metrics` series it always
//! does, and the TUI samples the in-process Prometheus recorder's rendered
//! text a few times per second (read-only — zero hot-path impact) and draws
//! a full-screen [ratatui] view: per-invocation throughput, errors, DLQ
//! counts, bookmark age, and a live log pane. `q` (or `Ctrl-C`, which raw
//! mode delivers as a key event) cancels cooperatively via the executor's
//! [`CancellationToken`] — in-flight invocations stop at their next page
//! boundary and flush their sinks.
//!
//! Log handling: the normal stdout subscriber would corrupt the alternate
//! screen, so when a TUI session is detected at startup (`--tui` on a real
//! TTY), `run_main` routes the fmt subscriber into an in-memory ring
//! ([`log_buffer`]) that the TUI renders as its log pane. Lines are redacted
//! at capture with the same registry the stdout writer uses. On a non-TTY
//! (CI, pipes) the flag degrades to a plain run with a one-line notice.

pub mod view;

// The Prometheus recorder installer and the pure text sampler now live in the
// shared `livemetrics` module (reused by the `cli-progress` line); re-export
// them here so the TUI's `crate::tui::{metrics, setup_observability, …}` call
// sites are unchanged.
pub use crate::livemetrics::{self as metrics, install_metrics_recorder, setup_observability};

use faucet_core::CancellationToken;
use metrics_exporter_prometheus::PrometheusHandle;
use std::collections::VecDeque;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex, OnceLock};

/// Ring-buffered log lines shared between the tracing subscriber (writer
/// side, installed in `run_main`) and the TUI (render side).
#[derive(Clone, Default)]
pub struct LogBuffer {
    inner: Arc<Mutex<VecDeque<String>>>,
}

const LOG_BUFFER_CAP: usize = 200;

impl LogBuffer {
    /// Append one already-formatted subscriber line (redacted at capture).
    pub fn push_line(&self, line: &str) {
        let line = crate::secrets::registry::redact(line).into_owned();
        let mut q = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if q.len() == LOG_BUFFER_CAP {
            q.pop_front();
        }
        q.push_back(line);
    }

    /// Snapshot the buffered lines (oldest first).
    pub fn snapshot(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

/// Line-splitting `io::Write` adapter for the tracing subscriber.
pub struct LogBufferWriter {
    buffer: LogBuffer,
    partial: String,
}

impl std::io::Write for LogBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.partial.push_str(&String::from_utf8_lossy(buf));
        while let Some(at) = self.partial.find('\n') {
            let line: String = self.partial.drain(..=at).collect();
            let line = line.trim_end();
            if !line.is_empty() {
                self.buffer.push_line(line);
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBufferWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogBufferWriter {
            buffer: self.clone(),
            partial: String::new(),
        }
    }
}

/// The process-wide TUI log ring. Populated by `run_main` when it detects a
/// TUI session before installing the subscriber; read by the render loop.
static TUI_LOGS: OnceLock<LogBuffer> = OnceLock::new();

/// `--tui` was passed *and* stdout is a real terminal — the full-screen UI
/// applies. On a non-TTY the caller degrades to a plain run.
pub fn is_tui_session(tui_flag: bool) -> bool {
    tui_flag && std::io::stdout().is_terminal()
}

/// Install the ring-buffered tracing subscriber for a TUI session. Called by
/// `run_main` *instead of* the stdout subscriber, before any log line is
/// emitted. Returns the buffer for the render loop.
pub fn install_tui_tracing(level: &str) -> LogBuffer {
    use tracing_subscriber::EnvFilter;
    let buffer = TUI_LOGS.get_or_init(LogBuffer::default).clone();
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let _ = tracing_subscriber::registry()
        .with(crate::trace_layer_slot())
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(buffer.clone()),
        )
        .try_init();
    buffer
}

/// The TUI log ring, if `run_main` installed one this process.
pub fn log_buffer() -> Option<LogBuffer> {
    TUI_LOGS.get().cloned()
}

/// Source of user cancel requests — abstracted from crossterm so
/// [`drive_loop`] is testable against a scripted sequence.
pub trait CancelEvents {
    /// Drain any pending input; `true` when the user asked to cancel
    /// (`q` / `Ctrl-C`).
    fn cancel_requested(&mut self) -> bool;
}

/// The real, non-blocking crossterm key poll.
struct CrosstermEvents;

impl CancelEvents for CrosstermEvents {
    fn cancel_requested(&mut self) -> bool {
        use ratatui::crossterm::event::{Event, KeyCode, KeyModifiers};
        let mut requested = false;
        while ratatui::crossterm::event::poll(std::time::Duration::ZERO).unwrap_or(false) {
            match ratatui::crossterm::event::read() {
                Ok(Event::Key(key)) => {
                    let ctrl_c = key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL);
                    if key.code == KeyCode::Char('q') || ctrl_c {
                        requested = true;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        requested
    }
}

/// Drive the run future under the full-screen TUI. Returns the run's result
/// after the terminal is restored. `cancel` is the token wired into
/// `ExecuteOptions.cancel`; `q` / `Ctrl-C` trigger it.
pub async fn drive<T>(
    run: impl Future<Output = T>,
    pipeline: &str,
    handle: PrometheusHandle,
    cancel: CancellationToken,
) -> T {
    // `ratatui::init` enters the alternate screen + raw mode and installs a
    // panic hook that restores the terminal before the default hook runs.
    let mut terminal = ratatui::init();
    let result = drive_loop(
        &mut terminal,
        CrosstermEvents,
        run,
        pipeline,
        handle,
        cancel,
        std::time::Duration::from_millis(250),
    )
    .await;
    ratatui::restore();
    result
}

/// The render/cancel loop behind [`drive`], generic over the terminal
/// backend and the event source so it runs headless under test.
pub async fn drive_loop<B, E, T>(
    terminal: &mut ratatui::Terminal<B>,
    mut events: E,
    run: impl Future<Output = T>,
    pipeline: &str,
    handle: PrometheusHandle,
    cancel: CancellationToken,
    tick: std::time::Duration,
) -> T
where
    B: ratatui::backend::Backend,
    E: CancelEvents,
{
    let started = std::time::Instant::now();
    let mut sampler = metrics::Sampler::new(pipeline);
    let logs = log_buffer().unwrap_or_default();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tokio::pin!(run);
    loop {
        tokio::select! {
            biased;
            result = &mut run => break result,
            _ = interval.tick() => {
                if events.cancel_requested() {
                    cancel.cancel();
                }
                handle.run_upkeep();
                let model = sampler.observe(&handle.render(), started.elapsed());
                let log_lines = logs.snapshot();
                let cancelling = cancel.is_cancelled();
                let _ = terminal.draw(|frame| {
                    view::draw(frame, pipeline, &model, started.elapsed(), &log_lines, cancelling);
                });
            }
        }
    }
}

/// Flush the tail of the buffered log ring to stderr — called after terminal
/// restore when the run failed, so the operator keeps the context that was
/// on screen.
pub fn flush_logs_to_stderr(max_lines: usize) {
    if let Some(buffer) = log_buffer() {
        let lines = buffer.snapshot();
        let start = lines.len().saturating_sub(max_lines);
        for line in &lines[start..] {
            eprintln!("{line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn log_buffer_caps_and_orders_lines() {
        let buffer = LogBuffer::default();
        for i in 0..(LOG_BUFFER_CAP + 10) {
            buffer.push_line(&format!("line {i}"));
        }
        let snap = buffer.snapshot();
        assert_eq!(snap.len(), LOG_BUFFER_CAP);
        assert_eq!(snap.first().unwrap(), "line 10");
        assert_eq!(
            snap.last().unwrap(),
            &format!("line {}", LOG_BUFFER_CAP + 9)
        );
    }

    #[test]
    fn writer_splits_lines_and_keeps_partials() {
        let buffer = LogBuffer::default();
        let mut writer = LogBufferWriter {
            buffer: buffer.clone(),
            partial: String::new(),
        };
        writer.write_all(b"first line\nsecond ").unwrap();
        writer.write_all(b"half\ntrailing").unwrap();
        let snap = buffer.snapshot();
        assert_eq!(
            snap,
            vec!["first line".to_string(), "second half".to_string()]
        );
    }

    #[test]
    fn log_lines_are_redacted_at_capture() {
        crate::secrets::registry::register("tui-secret-value");
        let buffer = LogBuffer::default();
        buffer.push_line("token is tui-secret-value here");
        let snap = buffer.snapshot();
        assert!(!snap[0].contains("tui-secret-value"), "got: {}", snap[0]);
    }

    #[test]
    fn non_tty_is_not_a_tui_session() {
        // Test harnesses never run on a TTY stdout, so the flag alone must
        // not start a session — this is the CI/pipe fallback contract.
        assert!(!is_tui_session(true) || std::io::stdout().is_terminal());
        assert!(!is_tui_session(false));
    }

    /// Scripted event source: yields `true` once at the configured tick.
    struct ScriptedEvents {
        cancel_on_call: usize,
        calls: usize,
    }

    impl CancelEvents for ScriptedEvents {
        fn cancel_requested(&mut self) -> bool {
            self.calls += 1;
            self.calls == self.cancel_on_call
        }
    }

    /// Never cancels.
    struct NoEvents;
    impl CancelEvents for NoEvents {
        fn cancel_requested(&mut self) -> bool {
            false
        }
    }

    fn test_terminal() -> ratatui::Terminal<ratatui::backend::TestBackend> {
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 24)).expect("terminal")
    }

    fn recorder_handle() -> PrometheusHandle {
        // The process-global recorder slot may be taken by any other test in
        // this binary; install_metrics_recorder tolerates that and hands back
        // a usable handle either way.
        install_metrics_recorder(None).expect("recorder")
    }

    #[tokio::test(start_paused = true)]
    async fn drive_loop_ticks_render_and_exit_on_run_completion() {
        let mut terminal = test_terminal();
        let cancel = CancellationToken::new();
        let result = drive_loop(
            &mut terminal,
            NoEvents,
            async {
                tokio::time::sleep(std::time::Duration::from_millis(320)).await;
                42
            },
            "loop-pipeline",
            recorder_handle(),
            cancel.clone(),
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(result, 42);
        assert!(!cancel.is_cancelled());
        // At least one tick rendered the header into the test buffer.
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(text.contains("faucet run · loop-pipeline"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn drive_loop_fires_the_cancel_token_on_user_request() {
        let mut terminal = test_terminal();
        let cancel = CancellationToken::new();
        let run_cancel = cancel.clone();
        let result = drive_loop(
            &mut terminal,
            ScriptedEvents {
                cancel_on_call: 2,
                calls: 0,
            },
            async move {
                // A cooperative pipeline: winds down when cancelled.
                run_cancel.cancelled().await;
                "cancelled"
            },
            "loop-pipeline",
            recorder_handle(),
            cancel.clone(),
            std::time::Duration::from_millis(50),
        )
        .await;
        assert_eq!(result, "cancelled");
        assert!(cancel.is_cancelled());
        // The frame after the cancel shows the banner.
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(text.contains("cancelling…"), "{text}");
    }

    #[test]
    fn flush_logs_to_stderr_replays_the_tail() {
        let buffer = install_tui_tracing("info");
        buffer.push_line("tail line A");
        buffer.push_line("tail line B");
        // Covers the ring lookup + tail slicing; output goes to stderr.
        flush_logs_to_stderr(1);
        flush_logs_to_stderr(1000);
    }
}
