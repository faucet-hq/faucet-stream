//! Tap subprocess runner: spawn, stream stdout as parsed messages over a
//! **bounded** channel (backpressure), drain stderr into `tracing`, and always
//! reap the child (SIGTERM → grace → SIGKILL on the paths we control;
//! `kill_on_drop` is the backstop for an abrupt drop/cancel).

use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use faucet_core::{FaucetError, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::SingerSourceConfig;
use crate::message::{SingerMessage, parse_line};

/// Bounded channel capacity between the stdout reader task and the stream. This
/// is the backpressure knob: the reader blocks on a full channel, which in turn
/// blocks reading the tap's stdout, so memory stays bounded regardless of how
/// fast the tap produces.
const CHANNEL_CAPACITY: usize = 256;

/// How many trailing stderr lines to retain for the failure message.
const STDERR_TAIL_LINES: usize = 20;

/// Grace period between SIGTERM and SIGKILL on a controlled shutdown.
const SIGTERM_GRACE: Duration = Duration::from_secs(5);

/// One item read from the tap's stdout: a parsed message, or a malformed line
/// (the caller applies the [`MalformedPolicy`](crate::config::MalformedPolicy)).
pub enum Line {
    Parsed(SingerMessage),
    Malformed(String),
}

/// Outcome of a single [`TapProcess::recv`] call.
pub enum Recv {
    /// A line was read.
    Line(Line),
    /// The tap closed stdout (end of stream).
    Eof,
    /// No line arrived within the configured idle timeout.
    IdleTimeout,
}

/// Conservative secret redactor, re-exported from [`faucet_common_singer`].
pub use faucet_common_singer::Redactor;

/// A running tap process and its stdout message channel.
pub struct TapProcess {
    child: Child,
    pid: Option<u32>,
    rx: mpsc::Receiver<Line>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    // Temp files kept alive for the process lifetime; deleted on drop.
    _temp_files: Vec<tempfile::NamedTempFile>,
    _reader: JoinHandle<()>,
    _stderr: JoinHandle<()>,
}

impl TapProcess {
    /// Spawn the tap with `--config` (and optionally `--catalog` / `--state`).
    ///
    /// `start_state` is the resume bookmark loaded from the state store (the
    /// STATE `value` blob), materialized to a `state.json` and passed as
    /// `--state` when present.
    pub fn spawn(
        cfg: &SingerSourceConfig,
        start_state: Option<&Value>,
    ) -> Result<Self, FaucetError> {
        let redactor = Redactor::from_config(&cfg.tap_config);
        let mut temp_files = Vec::new();

        let config_path = write_temp("config", &cfg.tap_config)?;
        let mut command = Command::new(&cfg.executable);
        command.arg("--config").arg(config_path.path());
        temp_files.push(config_path);

        if let Some(catalog) = &cfg.catalog {
            let catalog_path = write_temp("catalog", catalog)?;
            command.arg("--catalog").arg(catalog_path.path());
            temp_files.push(catalog_path);
        }

        if let Some(state) = start_state {
            let state_path = write_temp("state", state)?;
            command.arg("--state").arg(state_path.path());
            temp_files.push(state_path);
        }

        command.args(&cfg.args);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        tracing::debug!(
            executable = %cfg.executable,
            stream = %cfg.stream,
            "spawning singer tap"
        );

        let mut child = command.spawn().map_err(|e| {
            FaucetError::Source(format!(
                "failed to spawn tap '{}': {}",
                redactor.redact(&cfg.executable),
                e
            ))
        })?;
        let pid = child.id();

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| FaucetError::Source("tap stdout not captured".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| FaucetError::Source("tap stderr not captured".into()))?;

        let (tx, rx) = mpsc::channel::<Line>(CHANNEL_CAPACITY);
        let policy_reader = {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            let item = match parse_line(&line) {
                                Ok(msg) => Line::Parsed(msg),
                                Err(reason) => Line::Malformed(reason),
                            };
                            // `send().await` blocks on a full channel — this is
                            // the backpressure that bounds memory.
                            if tx.send(item).await.is_err() {
                                break; // consumer gone
                            }
                        }
                        Ok(None) => break, // EOF
                        Err(e) => {
                            let _ = tx
                                .send(Line::Malformed(format!("stdout read error: {e}")))
                                .await;
                            break;
                        }
                    }
                }
            })
        };
        drop(tx);

        let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
        let stderr_task = {
            let stderr_tail = Arc::clone(&stderr_tail);
            let redactor = redactor.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let redacted = redactor.redact(&line);
                    tracing::debug!(target: "faucet_source_singer::tap", "{redacted}");
                    let mut tail = stderr_tail.lock().unwrap();
                    if tail.len() == STDERR_TAIL_LINES {
                        tail.pop_front();
                    }
                    tail.push_back(redacted);
                }
            })
        };

        Ok(Self {
            child,
            pid,
            rx,
            stderr_tail,
            _temp_files: temp_files,
            _reader: policy_reader,
            _stderr: stderr_task,
        })
    }

    /// Receive the next stdout line, honoring the optional idle timeout.
    pub async fn recv(&mut self, idle_timeout: Option<Duration>) -> Recv {
        match idle_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, self.rx.recv()).await {
                Ok(Some(line)) => Recv::Line(line),
                Ok(None) => Recv::Eof,
                Err(_) => Recv::IdleTimeout,
            },
            None => match self.rx.recv().await {
                Some(line) => Recv::Line(line),
                None => Recv::Eof,
            },
        }
    }

    /// The trailing stderr lines (already redacted), newest last.
    pub fn stderr_tail(&self) -> String {
        self.stderr_tail
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Gracefully stop the tap (SIGTERM → grace → SIGKILL) and reap it, then
    /// return an error if it exited non-zero. Idempotent-ish: safe to call once
    /// on the normal EOF path or the idle-timeout path.
    pub async fn shutdown_and_check(&mut self) -> Result<(), FaucetError> {
        let status = self.terminate().await;
        match status {
            Ok(status) if status.success() => Ok(()),
            Ok(status) => Err(FaucetError::Source(format!(
                "tap exited with status {}; last stderr:\n{}",
                status,
                self.stderr_tail()
            ))),
            Err(e) => Err(FaucetError::Source(format!(
                "failed to reap tap: {e}; last stderr:\n{}",
                self.stderr_tail()
            ))),
        }
    }

    /// Terminate and reap the tap, ignoring its exit status. Used on the error
    /// paths (idle timeout, malformed-fail) where the failure reason is already
    /// known and the exit code is irrelevant.
    pub async fn shutdown(&mut self) {
        let _ = self.terminate().await;
    }

    /// SIGTERM → grace → SIGKILL, then reap. Returns the final exit status.
    async fn terminate(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Ok(Some(status)) = self.child.try_wait() {
            return Ok(status); // already exited
        }
        #[cfg(unix)]
        if let Some(pid) = self.pid {
            // SAFETY: pid is this child's pid; SIGTERM is a defined signal.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        #[cfg(not(unix))]
        let _ = self.pid; // silence unused on non-unix
        match tokio::time::timeout(SIGTERM_GRACE, self.child.wait()).await {
            Ok(status) => status,
            Err(_) => {
                tracing::warn!("tap did not exit within grace period; sending SIGKILL");
                let _ = self.child.start_kill();
                self.child.wait().await
            }
        }
    }
}

/// Write `value` as JSON to a private (0600) temp file.
pub(crate) fn write_temp(
    kind: &str,
    value: &Value,
) -> Result<tempfile::NamedTempFile, FaucetError> {
    faucet_common_singer::write_private_json(kind, value).map_err(FaucetError::Source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn write_temp_is_private_and_valid_json() {
        let f = write_temp("config", &json!({"a": 1})).unwrap();
        let contents = std::fs::read_to_string(f.path()).unwrap();
        assert_eq!(contents, r#"{"a":1}"#);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(f.path()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
