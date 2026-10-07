//! [`SingerSource`] — the `Source` implementation that bridges a Singer tap.
//!
//! This is the only place that drives the tap subprocess. Page assembly rules
//! (v0, single-stream):
//!
//! - RECORD for the configured stream → buffered.
//! - STATE → (if `flush_on_state`) flush the buffer as a page whose `bookmark`
//!   is the STATE `value`; otherwise remember it as the pending checkpoint.
//! - buffer reaches the `batch_size` hint → flush a page with `bookmark: None`
//!   (no STATE covers these rows yet; they re-emit from the last checkpoint on
//!   resume and an idempotent sink dedups them).
//! - clean EOF → flush any trailing buffer + pending checkpoint.
//! - non-zero exit / idle timeout / malformed-fail → terminate and error
//!   **without** committing the trailing un-checkpointed buffer.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use faucet_core::{FaucetError, Source, StreamPage, Value, async_trait, schema_for};
use futures::StreamExt;
use futures_core::Stream;

use crate::assemble::PageAssembler;
use crate::config::{MalformedPolicy, SingerSourceConfig};
use crate::message::SingerMessage;
use crate::process::{Line, Recv, TapProcess};

/// A source that runs a Singer tap and adapts its output into faucet records.
///
/// **Tier-2 / experimental.** This reintroduces a runtime (usually Python)
/// dependency for pipelines that use it, throughput is Singer-class rather than
/// faucet-class, and STATE-based resume granularity depends on the individual
/// tap. See the crate README.
pub struct SingerSource {
    config: SingerSourceConfig,
    /// Resume bookmark (the last persisted STATE `value`), injected by the
    /// pipeline via [`apply_start_bookmark`](Source::apply_start_bookmark).
    start_bookmark: Mutex<Option<Value>>,
}

impl SingerSource {
    /// Build a source from its configuration.
    pub fn new(config: SingerSourceConfig) -> Self {
        Self {
            config,
            start_bookmark: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Source for SingerSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        tracing::warn!(
            "SingerSource::fetch_with_context buffers the entire stream in memory, \
             defeating bounded-memory streaming; prefer driving stream_pages via the pipeline"
        );
        // batch_size 0 = single logical batch; we concatenate every page.
        let mut stream = self.stream_pages(context, 0);
        let mut all = Vec::new();
        while let Some(page) = stream.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // Take the resume bookmark once at stream construction.
        let start = self.start_bookmark.lock().unwrap().clone();
        let idle = self.config.idle_timeout_secs.map(Duration::from_secs);
        let target_stream = self.config.stream.clone();
        let flush_on_state = self.config.flush_on_state;
        let on_malformed = self.config.on_malformed;

        Box::pin(async_stream::try_stream! {
            let mut process = TapProcess::spawn(&self.config, start.as_ref())?;
            let mut assembler = PageAssembler::new(target_stream.clone(), batch_size, flush_on_state);
            if let Some(alias) = self.config.catalog.as_ref().and_then(|c| stream_alias(c, &target_stream)) {
                assembler = assembler.with_alias(alias);
            }

            loop {
                match process.recv(idle).await {
                    Recv::Line(Line::Parsed(msg)) => match msg {
                        SingerMessage::Record { stream, record } => {
                            if let Some(page) = assembler.on_record(&stream, record) {
                                yield page;
                            }
                        }
                        SingerMessage::State { value } => {
                            if let Some(why) = assembler.stream_mismatch() {
                                process.shutdown().await;
                                Err(FaucetError::Source(why))?;
                            }
                            if let Some(page) = assembler.on_state(value) {
                                yield page;
                            }
                        }
                        SingerMessage::Schema { stream, .. } => {
                            // v0 pass-through: faucet's sinks infer schema from
                            // records, so the SCHEMA declaration is logged and
                            // skipped.
                            // FUTURE (core RFC): a Source::record_schema() hook
                            // would let us forward this to schema-aware sinks.
                            tracing::debug!(stream = %stream, "singer SCHEMA (v0: pass-through)");
                        }
                        SingerMessage::Other { message_type } => {
                            tracing::debug!(message_type = %message_type, "singer message ignored (v0)");
                        }
                    },
                    Recv::Line(Line::Malformed(reason)) => match on_malformed {
                        MalformedPolicy::Skip => {
                            tracing::warn!(reason = %reason, "skipping malformed tap output line");
                        }
                        MalformedPolicy::Fail => {
                            process.shutdown().await;
                            Err(FaucetError::Source(format!(
                                "malformed tap output line: {reason}"
                            )))?;
                        }
                    },
                    Recv::Line(Line::Overlong(max)) => {
                        let tail = process.stderr_tail();
                        process.shutdown().await;
                        Err(FaucetError::Source(format!(
                            "tap wrote a line longer than max_line_bytes ({max}); last stderr:\n{tail}"
                        )))?;
                    }
                    Recv::IdleTimeout => {
                        let tail = process.stderr_tail();
                        process.shutdown().await;
                        Err(FaucetError::Source(format!(
                            "tap produced no output within idle timeout; last stderr:\n{tail}"
                        )))?;
                    }
                    Recv::Eof => {
                        // Determine exit status BEFORE committing the trailing
                        // buffer, so a failed run never writes an
                        // un-checkpointed final page.
                        process.shutdown_and_check().await?;
                        if let Some(page) = assembler.on_eof() {
                            yield page;
                        }
                        break;
                    }
                }
            }
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schema_for!(SingerSourceConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "singer"
    }

    fn state_key(&self) -> Option<String> {
        Some(self.config.effective_state_key())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start_bookmark.lock().unwrap() = Some(bookmark);
        Ok(())
    }

    /// Non-spawning preflight for `faucet doctor`: verifies the tap executable
    /// resolves (on `PATH` or as a file) and, when a catalog is configured, that
    /// the selected `stream` exists in it. We deliberately do **not** run the
    /// tap here — that would have side effects and could block.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        use std::time::Instant;

        let mut probes = Vec::new();

        let start = Instant::now();
        if resolve_executable(&self.config.executable).is_some() {
            probes.push(Probe::pass("tap_executable", start.elapsed()));
        } else {
            probes.push(Probe::fail_hint(
                "tap_executable",
                start.elapsed(),
                format!(
                    "tap '{}' not found on PATH or as a file",
                    self.config.executable
                ),
                "install the tap (e.g. `pipx install tap-foo`) and ensure it is on PATH, \
                 or set `executable` to an absolute path",
            ));
        }

        let start = Instant::now();
        match &self.config.catalog {
            Some(catalog) => {
                if catalog_has_stream(catalog, &self.config.stream) {
                    probes.push(Probe::pass("stream_in_catalog", start.elapsed()));
                } else {
                    probes.push(Probe::fail_hint(
                        "stream_in_catalog",
                        start.elapsed(),
                        format!(
                            "stream '{}' is not present in the configured catalog",
                            self.config.stream
                        ),
                        "check the `stream` name against the catalog's stream list",
                    ));
                }
            }
            None => probes.push(Probe::skip(
                "stream_in_catalog",
                "no catalog configured; run `faucet init --source singer --discover` to generate one",
            )),
        }

        Ok(CheckReport { probes })
    }
}

/// Resolve a tap executable to a file path: an absolute/relative path is checked
/// directly, a bare name is searched on `PATH`. Returns `None` if not found.
fn resolve_executable(exe: &str) -> Option<std::path::PathBuf> {
    use std::path::Path;
    let p = Path::new(exe);
    if exe.contains('/') || p.is_absolute() {
        return if p.is_file() {
            Some(p.to_path_buf())
        } else {
            None
        };
    }
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(exe))
        .find(|candidate| candidate.is_file())
}

/// The other name of the catalog entry `stream` matches: its `tap_stream_id`
/// when `stream` is the entry's `stream`, and vice versa.
fn stream_alias(catalog: &Value, stream: &str) -> Option<String> {
    catalog.get("streams")?.as_array()?.iter().find_map(|s| {
        let name = s.get("stream").and_then(Value::as_str);
        let id = s.get("tap_stream_id").and_then(Value::as_str);
        match (name, id) {
            (Some(n), Some(i)) if n == stream && i != stream => Some(i.to_string()),
            (Some(n), Some(i)) if i == stream && n != stream => Some(n.to_string()),
            _ => None,
        }
    })
}

/// Whether a Singer catalog lists `stream` (by `stream` or `tap_stream_id`).
fn catalog_has_stream(catalog: &Value, stream: &str) -> bool {
    catalog
        .get("streams")
        .and_then(Value::as_array)
        .is_some_and(|streams| {
            streams.iter().any(|s| {
                s.get("stream").and_then(Value::as_str) == Some(stream)
                    || s.get("tap_stream_id").and_then(Value::as_str) == Some(stream)
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn catalog_has_stream_matches_stream_or_tap_stream_id() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "users", "stream": "users" },
                { "tap_stream_id": "orders-v2", "stream": "orders" }
            ]
        });
        assert!(catalog_has_stream(&catalog, "users"));
        assert!(catalog_has_stream(&catalog, "orders")); // by `stream`
        assert!(catalog_has_stream(&catalog, "orders-v2")); // by `tap_stream_id`
        assert!(!catalog_has_stream(&catalog, "missing"));
        assert!(!catalog_has_stream(&json!({}), "users"));
    }

    #[test]
    fn stream_alias_maps_between_the_two_names() {
        let catalog = json!({
            "streams": [
                { "tap_stream_id": "users", "stream": "users" },
                { "tap_stream_id": "public-orders", "stream": "orders" }
            ]
        });
        assert_eq!(
            stream_alias(&catalog, "orders").as_deref(),
            Some("public-orders")
        );
        assert_eq!(
            stream_alias(&catalog, "public-orders").as_deref(),
            Some("orders")
        );
        assert_eq!(stream_alias(&catalog, "users"), None);
        assert_eq!(stream_alias(&json!({}), "users"), None);
    }

    #[test]
    fn resolve_executable_finds_absolute_file_and_rejects_missing() {
        // A file that definitely exists.
        assert!(
            resolve_executable("/bin/sh").is_some() || resolve_executable("/bin/cat").is_some()
        );
        assert!(resolve_executable("/definitely/not/a/real/tap-xyz").is_none());
        // A bare name that will not be on PATH.
        assert!(resolve_executable("tap-this-does-not-exist-42").is_none());
    }
}
