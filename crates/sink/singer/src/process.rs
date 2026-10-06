//! Target subprocess runner: spawn, write Singer lines to stdin with
//! back-pressure, watch stdout for echoed `STATE` messages, keep a redacted
//! stderr tail, and always reap the child.
//!
//! Back-pressure: stdin is a fixed-size [`BufWriter`] over the OS pipe, and
//! every write awaits the pipe — a slow target stalls the writer instead of
//! letting records pile up in memory. stdout is drained continuously into a
//! [`watch`] channel that only keeps the newest echo, so it is bounded too.

use std::collections::VecDeque;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use faucet_common_singer::{Redactor, SingerMessage, parse_line, write_private_json};
use faucet_core::{FaucetError, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::SingerSinkConfig;

/// Capacity of the stdin write buffer; the pipe itself adds the OS buffer.
const STDIN_BUFFER: usize = 64 * 1024;

/// Trailing stderr lines kept for failure messages.
const STDERR_TAIL_LINES: usize = 20;

/// Grace between closing stdin / SIGTERM and SIGKILL on teardown.
const TERMINATE_GRACE: Duration = Duration::from_secs(5);

/// Grace for a target to exit after its stdin broke mid-write.
const EXIT_AFTER_BROKEN_PIPE: Duration = Duration::from_secs(5);

/// Key under which the sink's flush marker sits in the `STATE` value it sends.
pub const FLUSH_MARKER_KEY: &str = "faucet_flush";

/// The flush-marker `STATE` value for sequence `seq` on `stream`.
pub fn flush_marker(stream: &str, seq: u64) -> Value {
    serde_json::json!({ FLUSH_MARKER_KEY: { "stream": stream, "seq": seq } })
}

/// The flush sequence carried by an echoed `STATE` value, if it is one of ours.
pub fn marker_seq(value: &Value) -> Option<u64> {
    value.get(FLUSH_MARKER_KEY)?.get("seq")?.as_u64()
}

/// What the stdout reader has seen so far.
#[derive(Debug, Clone, Default)]
pub struct Echo {
    /// Highest flush-marker sequence echoed back by the target.
    pub confirmed: u64,
    /// `STATE` messages seen (ours or not).
    pub states: u64,
    /// stdout reached end of stream.
    pub eof: bool,
}

/// A running target process.
pub struct TargetProcess {
    child: Option<Child>,
    pid: Option<u32>,
    stdin: Option<BufWriter<ChildStdin>>,
    echo: watch::Receiver<Echo>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    stderr_task: Option<JoinHandle<()>>,
    redactor: Redactor,
    command: String,
    _config_file: tempfile::NamedTempFile,
    stdout_task: Option<JoinHandle<()>>,
}

impl TargetProcess {
    /// Spawn the target with `--config <0600 temp file>` plus `args` and `env`.
    pub fn spawn(cfg: &SingerSinkConfig, redactor: &Redactor) -> Result<Self, FaucetError> {
        let config_file =
            write_private_json("config", &cfg.target_config).map_err(FaucetError::Sink)?;
        let mut command = Command::new(&cfg.target_command);
        if let Some(env) = cfg.inherit_env.from_process() {
            command.env_clear().envs(env);
        }
        command
            .arg("--config")
            .arg(config_file.path())
            .args(&cfg.args)
            .envs(&cfg.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        tracing::debug!(target_command = %cfg.target_command, stream = %cfg.stream_name(), "spawning singer target");
        let mut child = command.spawn().map_err(|e| {
            FaucetError::Sink(format!(
                "failed to spawn singer target '{}': {e}",
                redactor.redact(&cfg.target_command)
            ))
        })?;
        let pid = child.id();
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");

        let (tx, rx) = watch::channel(Echo::default());
        let stdout_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match parse_line(&line) {
                    Ok(SingerMessage::State { value }) => tx.send_modify(|e| {
                        e.states += 1;
                        if let Some(seq) = marker_seq(&value) {
                            e.confirmed = e.confirmed.max(seq);
                        }
                    }),
                    _ => tracing::trace!(target: "faucet_sink_singer::target", "stdout: {line}"),
                }
            }
            tx.send_modify(|e| e.eof = true);
        });

        let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
        let stderr_task = {
            let tail = Arc::clone(&stderr_tail);
            let redactor = redactor.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let redacted = faucet_core::redact::redact(&redactor.redact(&line));
                    tracing::debug!(target: "faucet_sink_singer::target", "{redacted}");
                    let mut tail = tail.lock().unwrap();
                    if tail.len() == STDERR_TAIL_LINES {
                        tail.pop_front();
                    }
                    tail.push_back(redacted);
                }
            })
        };

        Ok(Self {
            child: Some(child),
            pid,
            stdin: Some(BufWriter::with_capacity(STDIN_BUFFER, stdin)),
            echo: rx,
            stderr_tail,
            stderr_task: Some(stderr_task),
            redactor: redactor.clone(),
            command: cfg.target_command.clone(),
            _config_file: config_file,
            stdout_task: Some(stdout_task),
        })
    }

    /// Write already-encoded Singer lines. Awaits the pipe (back-pressure); a
    /// broken pipe means the target died, reported with its stderr tail.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<(), FaucetError> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| FaucetError::Sink("singer target stdin is already closed".into()))?;
        if let Err(e) = stdin.write_all(bytes).await {
            return Err(self.failure(&format!("closed its stdin ({e})")).await);
        }
        Ok(())
    }

    /// Push buffered lines through to the target.
    pub async fn flush_stdin(&mut self) -> Result<(), FaucetError> {
        if let Some(stdin) = self.stdin.as_mut()
            && let Err(e) = stdin.flush().await
        {
            return Err(self.failure(&format!("closed its stdin ({e})")).await);
        }
        Ok(())
    }

    /// Wait until the target echoes flush marker `seq`, it closes stdout, or
    /// `timeout` elapses.
    pub async fn wait_for_echo(&mut self, seq: u64, timeout: Duration) -> Result<(), FaucetError> {
        let waited = tokio::time::timeout(timeout, async {
            loop {
                let echo = self.echo.borrow_and_update().clone();
                if echo.confirmed >= seq {
                    return Ok(());
                }
                if echo.eof || self.echo.changed().await.is_err() {
                    return Err(());
                }
            }
        })
        .await;
        match waited {
            Ok(Ok(())) => Ok(()),
            Ok(Err(())) => Err(self.failure("exited before echoing the flush STATE").await),
            Err(_) => {
                let err = FaucetError::Sink(format!(
                    "singer target '{}' did not echo the flush STATE within {}s \
                     (a target that never echoes STATE needs `flush_on: exit`); last stderr:\n{}",
                    self.redactor.redact(&self.command),
                    timeout.as_secs(),
                    self.stderr_tail()
                ));
                self.terminate().await;
                Err(err)
            }
        }
    }

    /// Close stdin, wait (up to `timeout`) for a successful exit, and confirm
    /// flush marker `seq`: a target that reports `STATE` at all must have
    /// echoed that last marker — otherwise it exited without reading all of
    /// its input. A target that never emits `STATE` is confirmed by its clean
    /// exit alone.
    pub async fn finish_confirmed(
        mut self,
        seq: u64,
        timeout: Duration,
    ) -> Result<(), FaucetError> {
        self.finish(timeout).await?;
        if let Some(task) = self.stdout_task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
        let echo = self.echo.borrow().clone();
        if echo.states > 0 && echo.confirmed < seq {
            return Err(self.status_error(&format!(
                "exited without echoing the final flush STATE (it reported STATE {} time(s), \
                 but not the last one sent), so it may not have read all of its input",
                echo.states
            )));
        }
        Ok(())
    }

    /// Close stdin and wait (up to `timeout`) for a successful exit.
    pub async fn finish(&mut self, timeout: Duration) -> Result<(), FaucetError> {
        if let Some(mut stdin) = self.stdin.take() {
            if let Err(e) = stdin.flush().await {
                return Err(self.failure(&format!("closed its stdin ({e})")).await);
            }
            let _ = stdin.shutdown().await;
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) if status.success() => {
                self.child = None;
                Ok(())
            }
            Ok(Ok(status)) => {
                self.child = None;
                self.drain_stderr().await;
                Err(self.status_error(&format!("exited with {status}")))
            }
            Ok(Err(e)) => Err(self.status_error(&format!("could not be reaped: {e}"))),
            Err(_) => {
                self.terminate().await;
                Err(self.status_error(&format!(
                    "did not exit within {}s of its input closing",
                    timeout.as_secs()
                )))
            }
        }
    }

    /// Stop the target without waiting for it to finish its work (SIGTERM →
    /// grace → SIGKILL) and reap it.
    pub async fn terminate(&mut self) {
        self.stdin = None;
        let Some(child) = self.child.as_mut() else {
            return;
        };
        if matches!(child.try_wait(), Ok(Some(_))) {
            self.child = None;
            return;
        }
        #[cfg(unix)]
        if let Some(pid) = self.pid {
            // SAFETY: pid is this child's pid; SIGTERM is a defined signal.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        #[cfg(not(unix))]
        let _ = self.pid;
        if tokio::time::timeout(TERMINATE_GRACE, child.wait())
            .await
            .is_err()
        {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.child = None;
    }

    /// The trailing stderr lines (redacted), newest last.
    pub fn stderr_tail(&self) -> String {
        self.stderr_tail
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn drain_stderr(&mut self) {
        if let Some(task) = self.stderr_task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }

    /// Build the error for a target that stopped taking input: wait briefly
    /// for its exit so the status and the complete stderr tail are reported.
    async fn failure(&mut self, what: &str) -> FaucetError {
        self.stdin = None;
        let status: Option<ExitStatus> = match self.child.as_mut() {
            Some(child) => match tokio::time::timeout(EXIT_AFTER_BROKEN_PIPE, child.wait()).await {
                Ok(Ok(status)) => Some(status),
                _ => None,
            },
            None => None,
        };
        if status.is_some() {
            self.child = None;
            self.drain_stderr().await;
        } else {
            self.terminate().await;
        }
        let what = match status {
            Some(s) => format!("{what}; exited with {s}"),
            None => what.to_string(),
        };
        self.status_error(&what)
    }

    fn status_error(&self, what: &str) -> FaucetError {
        FaucetError::Sink(format!(
            "singer target '{}' {what}; last stderr:\n{}",
            self.redactor.redact(&self.command),
            self.stderr_tail()
        ))
    }
}

impl Drop for TargetProcess {
    /// A target still running when the sink is dropped has had every flushed
    /// record confirmed; close its input and let it exit on its own, killing
    /// it only if it outlives the grace period.
    fn drop(&mut self) {
        let stdin = self.stdin.take();
        let Some(mut child) = self.child.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    drop(stdin);
                    if tokio::time::timeout(TERMINATE_GRACE * 6, child.wait())
                        .await
                        .is_err()
                    {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                });
            }
            Err(_) => {
                drop(stdin);
                let _ = child.start_kill();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flush_marker_round_trips() {
        let m = flush_marker("orders", 3);
        assert_eq!(m, json!({"faucet_flush": {"stream": "orders", "seq": 3}}));
        assert_eq!(marker_seq(&m), Some(3));
        assert_eq!(marker_seq(&json!({"bookmarks": {}})), None);
        assert_eq!(marker_seq(&json!({"faucet_flush": {"seq": "x"}})), None);
    }
}
