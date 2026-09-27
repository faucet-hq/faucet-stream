//! [`SingerSink`] — the `Sink` implementation that bridges a Singer target.
//!
//! The only place that drives the target subprocess. Per written page:
//! a `SCHEMA` (on a fresh process, or when an inferred schema widened), then
//! one `RECORD` per record. On flush: a `STATE` carrying a flush marker, then
//! either wait for the target to echo it (`flush_on: state`) or close its
//! input and wait for a clean exit (`flush_on: exit`). The pipeline persists a
//! bookmark only after `flush` returns, so bookmarks never run ahead of what
//! the target has persisted.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use faucet_common_singer::message::{
    write_activate_version, write_record, write_schema, write_state,
};
use faucet_common_singer::{Redactor, write_private_json};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::{FaucetError, Sink, Value, WriteMode, async_trait, schema_for};
use tokio::sync::Mutex;

use crate::config::{FlushOn, SingerSinkConfig};
use crate::process::{TargetProcess, flush_marker};
use crate::schema::widen;

/// Encoded lines are handed to the pipe in chunks of about this size.
const WRITE_CHUNK: usize = 64 * 1024;

/// A sink that runs a Singer target and feeds it faucet records.
///
/// **Tier-2 / experimental.** This reintroduces a runtime (usually Python)
/// dependency for pipelines that use it, and throughput is Singer-class
/// rather than faucet-class. See the crate README.
pub struct SingerSink {
    config: SingerSinkConfig,
    redactor: Redactor,
    inner: Mutex<Inner>,
    version: OnceLock<i64>,
}

#[derive(Default)]
struct Inner {
    process: Option<TargetProcess>,
    sent_schema: Option<Value>,
    inferred: Value,
    flush_seq: u64,
    unconfirmed_pages: usize,
    interrupted: bool,
    poisoned: Option<String>,
}

impl Inner {
    /// Once the target has failed while holding unconfirmed pages, those
    /// records are gone; every later write or flush must fail so no bookmark
    /// can advance past them.
    fn poison(&mut self, cause: &FaucetError) {
        self.poisoned = Some(format!(
            "singer sink: the target failed while holding unconfirmed records, which cannot be \
             recovered in this run: {cause}"
        ));
    }

    fn check_poisoned(&self) -> Result<(), FaucetError> {
        match &self.poisoned {
            Some(msg) => Err(FaucetError::Sink(msg.clone())),
            None => Ok(()),
        }
    }
}

impl SingerSink {
    /// Build a sink from its configuration (validated here).
    pub fn new(config: SingerSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            redactor: Redactor::from_config(&config.target_config),
            config,
            inner: Mutex::new(Inner::default()),
            version: OnceLock::new(),
        })
    }

    /// The configuration this sink was built from.
    pub fn config(&self) -> &SingerSinkConfig {
        &self.config
    }

    /// The table version records carry under `write_mode: overwrite`.
    pub fn activate_version(&self) -> i64 {
        *self.version.get_or_init(|| {
            self.config.activate_version.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or_default()
            })
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(self.config.flush_timeout_secs)
    }

    fn fixed_or_inferred_schema(&self, inner: &Inner) -> Value {
        self.config
            .schema
            .clone()
            .or_else(|| inner.inferred.is_object().then(|| inner.inferred.clone()))
            .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}))
    }

    async fn write_page(&self, inner: &mut Inner, records: &[Value]) -> Result<(), FaucetError> {
        if inner.process.is_none() {
            inner.process = Some(TargetProcess::spawn(&self.config, &self.redactor)?);
            inner.sent_schema = None;
        }
        let stream = self.config.stream_name();
        let schema = match &self.config.schema {
            Some(fixed) => inner.sent_schema.is_none().then(|| fixed.clone()),
            None => {
                let page = faucet_core::schema::infer_schema(records);
                let widened = widen(&mut inner.inferred, &page);
                (widened || inner.sent_schema.is_none()).then(|| inner.inferred.clone())
            }
        };
        let mut buf = Vec::with_capacity(WRITE_CHUNK + 4096);
        if let Some(schema) = schema {
            write_schema(
                &mut buf,
                stream,
                &schema,
                &self.config.effective_key_properties(),
            );
            inner.sent_schema = Some(schema);
        }
        let version = self
            .config
            .write
            .is_overwrite()
            .then(|| self.activate_version());
        let process = inner.process.as_mut().expect("spawned above");
        for record in records {
            write_record(&mut buf, stream, record, version);
            if buf.len() >= WRITE_CHUNK {
                process.write(&buf).await?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            process.write(&buf).await?;
        }
        Ok(())
    }

    async fn confirm(&self, inner: &mut Inner) -> Result<(), FaucetError> {
        if inner.unconfirmed_pages == 0 {
            return Ok(());
        }
        let mut process = inner
            .process
            .take()
            .expect("unconfirmed pages imply a running target");
        inner.flush_seq += 1;
        let seq = inner.flush_seq;
        let mut buf = Vec::new();
        write_state(&mut buf, &flush_marker(self.config.stream_name(), seq));
        process.write(&buf).await?;
        process.flush_stdin().await?;
        match self.config.flush_on {
            FlushOn::State => {
                process.wait_for_echo(seq, self.timeout()).await?;
                inner.process = Some(process);
            }
            FlushOn::Exit => {
                process.finish_confirmed(seq, self.timeout()).await?;
                inner.sent_schema = None;
            }
        }
        inner.unconfirmed_pages = 0;
        Ok(())
    }

    async fn stop(&self, inner: &mut Inner) {
        if let Some(mut process) = inner.process.take() {
            process.terminate().await;
        }
        inner.sent_schema = None;
        inner.unconfirmed_pages = 0;
    }
}

/// Whether `command` resolves to an existing file (directly, or on `PATH`).
pub(crate) fn resolve_command(command: &str) -> Option<std::path::PathBuf> {
    let direct = std::path::Path::new(command);
    if command.contains(std::path::MAIN_SEPARATOR) {
        return direct.is_file().then(|| direct.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(command))
        .find(|candidate| candidate.is_file())
}

#[async_trait]
impl Sink for SingerSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        if let Some(i) = records.iter().position(|r| !r.is_object()) {
            return Err(FaucetError::Sink(format!(
                "singer sink: record {i} is not a JSON object (Singer RECORD messages carry objects)"
            )));
        }
        let mut inner = self.inner.lock().await;
        inner.check_poisoned()?;
        if inner.interrupted {
            let earlier = inner.unconfirmed_pages.saturating_sub(1);
            self.stop(&mut inner).await;
            inner.interrupted = false;
            let err = FaucetError::Sink(
                "singer sink: a previous write to the target was interrupted mid-page; \
                 the target was stopped"
                    .into(),
            );
            if earlier > 0 {
                inner.poison(&err);
            }
            return Err(err);
        }
        let earlier = inner.unconfirmed_pages;
        inner.interrupted = true;
        inner.unconfirmed_pages += 1;
        let result = self.write_page(&mut inner, records).await;
        inner.interrupted = false;
        if let Err(e) = result {
            self.stop(&mut inner).await;
            if earlier > 0 {
                inner.poison(&e);
            }
            return Err(e);
        }
        Ok(records.len())
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let mut inner = self.inner.lock().await;
        inner.check_poisoned()?;
        let result = self.confirm(&mut inner).await;
        if let Err(e) = &result {
            inner.sent_schema = None;
            inner.poison(e);
        }
        result
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite]
    }

    fn dedups_by_key(&self) -> bool {
        false
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.activate_version();
        Ok(())
    }

    /// Confirm this instance's own records, then run the target once more to
    /// send `ACTIVATE_VERSION`: the target makes this run's version live and
    /// discards rows from earlier versions.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let mut inner = self.inner.lock().await;
        inner.check_poisoned()?;
        if let Err(e) = self.confirm(&mut inner).await {
            inner.poison(&e);
            return Err(e);
        }
        let stream = self.config.stream_name();
        let schema = self.fixed_or_inferred_schema(&inner);
        let mut process = TargetProcess::spawn(&self.config, &self.redactor)?;
        let mut buf = Vec::new();
        write_schema(
            &mut buf,
            stream,
            &schema,
            &self.config.effective_key_properties(),
        );
        write_activate_version(&mut buf, stream, self.activate_version());
        inner.flush_seq += 1;
        write_state(&mut buf, &flush_marker(stream, inner.flush_seq));
        process.write(&buf).await?;
        process
            .finish_confirmed(inner.flush_seq, self.timeout())
            .await
    }

    /// Stop the target without activating: the previous version stays live,
    /// and a later successful overwrite discards this run's rows.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        let mut inner = self.inner.lock().await;
        self.stop(&mut inner).await;
        Ok(())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schema_for!(SingerSinkConfig)).unwrap_or_default()
    }

    fn connector_name(&self) -> &'static str {
        "singer"
    }

    fn dataset_uri(&self) -> String {
        let target = std::path::Path::new(&self.config.target_command)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.config.target_command.clone());
        format!("singer://{target}/{}", self.config.stream_name())
    }

    /// Non-mutating preflight: the target executable resolves, and its config
    /// can be materialized to a private temp file. The target is not run — a
    /// Singer target has no side-effect-free probe mode.
    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let start = Instant::now();
        let command = self.redactor.redact(&self.config.target_command);
        let executable = match resolve_command(&self.config.target_command) {
            Some(_) => Probe::pass("executable", start.elapsed()),
            None => Probe::fail_hint(
                "executable",
                start.elapsed(),
                format!("singer target '{command}' was not found"),
                "install the target (e.g. `pipx install target-jsonl`) or set `target_command` to its absolute path",
            ),
        };
        let start = Instant::now();
        let config = match write_private_json("config", &self.config.target_config) {
            Ok(_) => Probe::pass("config", start.elapsed()),
            Err(reason) => Probe::fail("config", start.elapsed(), reason),
        };
        Ok(CheckReport {
            probes: vec![executable, config],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink(v: Value) -> SingerSink {
        SingerSink::new(serde_json::from_value(v).unwrap()).unwrap()
    }

    #[test]
    fn new_validates() {
        let cfg: SingerSinkConfig = serde_json::from_value(json!({"target_command": ""})).unwrap();
        assert!(SingerSink::new(cfg).is_err());
    }

    #[test]
    fn identity_and_capabilities() {
        let s = sink(json!({"target_command": "/opt/t/target-jsonl", "stream": "orders"}));
        assert_eq!(s.connector_name(), "singer");
        assert_eq!(s.dataset_uri(), "singer://target-jsonl/orders");
        assert!(!s.dedups_by_key());
        assert!(!s.is_overwrite());
        assert!(!s.supports_idempotent_writes());
        assert_eq!(s.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
        assert_eq!(
            s.supported_write_modes(),
            &[WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite]
        );
        assert_eq!(s.config().stream_name(), "orders");
        let schema = s.config_schema();
        assert!(schema["properties"]["target_command"].is_object());
        assert!(schema["properties"]["_activate_version"].is_object());
    }

    #[test]
    fn activate_version_prefers_config_and_is_stable() {
        let s =
            sink(json!({"target_command": "t", "write_mode": "overwrite", "_activate_version": 9}));
        assert!(s.is_overwrite());
        assert_eq!(s.activate_version(), 9);
        let s = sink(json!({"target_command": "t"}));
        let v = s.activate_version();
        assert!(v > 0);
        assert_eq!(s.activate_version(), v);
    }

    #[test]
    fn fixed_or_inferred_schema_precedence() {
        let s = sink(json!({"target_command": "t"}));
        let mut inner = Inner::default();
        assert_eq!(
            s.fixed_or_inferred_schema(&inner),
            json!({"type": "object", "properties": {}})
        );
        inner.inferred = json!({"type": "object", "properties": {"a": {}}});
        assert_eq!(s.fixed_or_inferred_schema(&inner), inner.inferred);
        let s = sink(json!({"target_command": "t", "schema": {"type": "object"}}));
        assert_eq!(
            s.fixed_or_inferred_schema(&inner),
            json!({"type": "object"})
        );
    }

    #[test]
    fn resolve_command_paths() {
        assert!(resolve_command("sh").is_some());
        assert!(resolve_command("/bin/sh").is_some());
        assert!(resolve_command("definitely-not-a-singer-target-xyz").is_none());
        assert!(resolve_command("/no/such/target").is_none());
    }

    #[tokio::test]
    async fn rejects_non_object_records_without_spawning() {
        let s = sink(json!({"target_command": "/no/such/target"}));
        let err = s
            .write_batch(&[json!({"a": 1}), json!(5)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("record 1"), "{err}");
        assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
        s.flush().await.unwrap();
    }

    #[tokio::test]
    async fn spawn_failure_is_a_sink_error() {
        let s = sink(json!({"target_command": "/no/such/target"}));
        let err = s.write_batch(&[json!({"a": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("failed to spawn"), "{err}");
    }

    #[tokio::test]
    async fn check_reports_missing_executable() {
        let s = sink(json!({"target_command": "/no/such/target"}));
        let report = s.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.failed_count(), 1);
        let s = sink(json!({"target_command": "sh"}));
        let report = s.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.probes.len(), 2);
    }
}
