//! Redis sink executor.

use crate::config::{RedisSinkConfig, RedisSinkType};
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;

/// A configured Redis sink that writes records to Redis data structures.
///
/// The connection is established once during construction and reused across
/// all `write_batch()` calls.
pub struct RedisSink {
    config: RedisSinkConfig,
    conn: redis::aio::MultiplexedConnection,
}

impl RedisSink {
    /// Create a new Redis sink from the given configuration.
    ///
    /// This opens a multiplexed async connection to Redis immediately.
    pub async fn new(config: RedisSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| FaucetError::Sink(format!("Redis connection failed: {e}")))?;

        Ok(Self { config, conn })
    }
}

#[async_trait]
impl faucet_core::Sink for RedisSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn connector_name(&self) -> &'static str {
        "redis"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RedisSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        use crate::config::RedisSinkType;
        let key = match &self.config.sink_type {
            RedisSinkType::List { key } | RedisSinkType::Stream { key } => format!("?key={key}"),
            RedisSinkType::KeyValue { key_field } => format!("?key_field={key_field}"),
        };
        format!(
            "{}{}",
            faucet_core::redact_uri_credentials(&self.config.url),
            key
        )
    }

    /// Non-mutating preflight probe: issue a Redis `PING` over the existing
    /// multiplexed connection (probe name `"ping"`).
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // MultiplexedConnection is cheaply cloneable; clone to satisfy &self.
        let mut conn = self.conn.clone();
        let started = std::time::Instant::now();
        let hint = "check the Redis url / that the server is reachable and accepting connections";

        let probe = match tokio::time::timeout(
            ctx.timeout,
            redis::cmd("PING").query_async::<String>(&mut conn),
        )
        .await
        {
            Ok(Ok(_)) => Probe::pass("ping", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint("ping", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("ping", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // MultiplexedConnection is cheaply cloneable (it shares the
        // underlying connection), so we clone to satisfy the &self receiver.
        let mut conn = self.conn.clone();
        let mut written = 0usize;

        // `batch_size = 0` is the "no batching" sentinel: pack the entire
        // upstream slice into a single Redis pipeline, preserving
        // `StreamPage` framing. Otherwise re-chunk into `batch_size`-sized
        // slices so each Redis pipeline stays near the recommended
        // ~1000-command working set.
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        // Process in chunks of batch_size using redis pipelines.
        for chunk in records.chunks(effective_chunk) {
            let mut pipe = redis::pipe();

            for record in chunk {
                append_record_command(&mut pipe, &self.config.sink_type, record)?;
            }

            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("Redis pipeline execution failed: {e}")))?;

            written += chunk.len();
        }

        tracing::debug!(records = written, "Redis batch written");
        Ok(written)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Write `records` AND durably record `token` for `scope` in one atomic
    /// Redis transaction (`MULTI`/`EXEC`).
    ///
    /// The whole page ships as a single atomic pipeline: every record's
    /// command for the configured [`RedisSinkType`] plus a final
    /// `SET _faucet_commit_token:{scope} {token}`. Either all of it commits
    /// or none of it does, so a crash between "sink wrote" and "state
    /// persisted" is resolved on resume by `last_committed_token` — zero
    /// duplicates on replay.
    ///
    /// **`batch_size` re-chunking does NOT apply on this path.** Splitting the
    /// page across multiple `MULTI`/`EXEC` blocks would break atomicity (a
    /// crash between chunks would commit rows without the watermark), so the
    /// entire page is one transaction regardless of `batch_size`.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let mut conn = self.conn.clone();

        let mut pipe = redis::pipe();
        pipe.atomic();
        for record in records {
            append_record_command(&mut pipe, &self.config.sink_type, record)?;
        }
        // The watermark commits in the same MULTI/EXEC as the data. Even an
        // empty page still advances the token so resume skips it.
        pipe.cmd("SET").arg(commit_token_key(scope)).arg(token);

        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            FaucetError::Sink(format!(
                "Redis atomic pipeline (MULTI/EXEC) execution failed: {e}"
            ))
        })?;

        tracing::debug!(
            records = records.len(),
            scope,
            "Redis atomic batch + commit token written"
        );
        Ok(records.len())
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let mut conn = self.conn.clone();
        // The token is opaque to the sink (it may carry an embedded resume
        // bookmark after a '#'); never parse it here — just hand it back.
        redis::cmd("GET")
            .arg(commit_token_key(scope))
            .query_async::<Option<String>>(&mut conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("Redis commit-token read failed: {e}")))
    }
}

/// The Redis key holding the last committed watermark for a pipeline `scope`
/// (the per-row state key, e.g. `"{name}::{row_id}"`).
///
/// Mirrors the SQL sinks' `_faucet_commit_token` watermark table: one plain
/// string key per scope, namespaced under the same `_faucet_commit_token`
/// prefix.
fn commit_token_key(scope: &str) -> String {
    format!("{}:{scope}", faucet_core::idempotency::COMMIT_TOKEN_TABLE)
}

/// Render the full Redis command (name first, then arguments) that writes one
/// record under the given sink mode. Pure — shared by [`append_record_command`]
/// so `write_batch` and `write_batch_idempotent` build identical commands.
fn record_command_args(
    sink_type: &RedisSinkType,
    record: &Value,
) -> Result<Vec<String>, FaucetError> {
    match sink_type {
        RedisSinkType::List { key } => {
            let serialized = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            Ok(vec!["RPUSH".into(), key.clone(), serialized])
        }
        RedisSinkType::Stream { key } => {
            let fields = flatten_record_to_fields(record);
            let mut args = vec!["XADD".into(), key.clone(), "*".into()];
            if fields.is_empty() {
                // XADD requires at least one field.
                let serialized = serde_json::to_string(record)
                    .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
                args.push("_data".into());
                args.push(serialized);
            } else {
                for (field_name, field_value) in fields {
                    args.push(field_name);
                    args.push(field_value);
                }
            }
            Ok(args)
        }
        RedisSinkType::KeyValue { key_field } => {
            let key = record
                .get(key_field)
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .ok_or_else(|| {
                    FaucetError::Sink(format!("record missing key field '{key_field}'"))
                })?;
            let serialized = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            Ok(vec!["SET".into(), key, serialized])
        }
    }
}

/// Append the command that writes `record` to `pipe`. Thin I/O-free shim over
/// the pure [`record_command_args`].
fn append_record_command(
    pipe: &mut redis::Pipeline,
    sink_type: &RedisSinkType,
    record: &Value,
) -> Result<(), FaucetError> {
    let args = record_command_args(sink_type, record)?;
    // The command name is just the first protocol argument.
    let mut cmd = redis::Cmd::new();
    for arg in &args {
        cmd.arg(arg);
    }
    pipe.add_command(cmd);
    Ok(())
}

/// Flatten a JSON record's top-level fields into string key-value pairs
/// suitable for Redis stream entries.
fn flatten_record_to_fields(record: &Value) -> Vec<(String, String)> {
    match record.as_object() {
        Some(map) => map
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (k.clone(), val)
            })
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisSinkConfig;
    use serde_json::json;

    // dataset_uri test is skipped: RedisSink::new() requires a live Redis
    // connection (opens a multiplexed connection in new()), and no offline
    // constructor exists.

    #[test]
    fn config_fields_accessible() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::List { key: "test".into() },
        );
        // RedisSink::new() is async and requires a live Redis connection,
        // so we only verify the config here.
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn flatten_object_record() {
        let record = json!({"name": "Alice", "age": 30});
        let fields = flatten_record_to_fields(&record);
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|(k, v)| k == "name" && v == "Alice"));
        assert!(fields.iter().any(|(k, v)| k == "age" && v == "30"));
    }

    #[test]
    fn flatten_non_object_returns_empty() {
        let record = json!("just a string");
        let fields = flatten_record_to_fields(&record);
        assert!(fields.is_empty());
    }

    #[test]
    fn flatten_nested_value_serializes_as_json() {
        let record = json!({"data": {"nested": true}});
        let fields = flatten_record_to_fields(&record);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "data");
        assert_eq!(fields[0].1, r#"{"nested":true}"#);
    }

    #[test]
    fn commit_token_key_namespaces_scope_under_watermark_prefix() {
        assert_eq!(
            commit_token_key("orders::row1"),
            "_faucet_commit_token:orders::row1"
        );
        assert_eq!(commit_token_key(""), "_faucet_commit_token:");
    }

    #[test]
    fn list_record_command_is_rpush_key_json() {
        let args = record_command_args(&RedisSinkType::List { key: "q".into() }, &json!({"id": 1}))
            .unwrap();
        assert_eq!(args, vec!["RPUSH", "q", r#"{"id":1}"#]);
    }

    #[test]
    fn stream_record_command_is_xadd_with_flattened_fields() {
        let args = record_command_args(
            &RedisSinkType::Stream { key: "ev".into() },
            &json!({"name": "Alice", "age": 30}),
        )
        .unwrap();
        assert_eq!(&args[..3], ["XADD", "ev", "*"]);
        // Field order depends on serde_json's map backing (preserve_order can
        // flip it under --all-features), so assert the pair set, not sequence.
        let pairs: Vec<(&str, &str)> = args[3..]
            .chunks(2)
            .map(|p| (p[0].as_str(), p[1].as_str()))
            .collect();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.contains(&("name", "Alice")));
        assert!(pairs.contains(&("age", "30")));
    }

    #[test]
    fn stream_record_command_empty_object_falls_back_to_data_field() {
        let args =
            record_command_args(&RedisSinkType::Stream { key: "ev".into() }, &json!({})).unwrap();
        assert_eq!(args, vec!["XADD", "ev", "*", "_data", "{}"]);
    }

    #[test]
    fn stream_record_command_non_object_falls_back_to_data_field() {
        let args = record_command_args(&RedisSinkType::Stream { key: "ev".into() }, &json!("bare"))
            .unwrap();
        assert_eq!(args, vec!["XADD", "ev", "*", "_data", r#""bare""#]);
    }

    #[test]
    fn key_value_record_command_is_set_key_json() {
        let args = record_command_args(
            &RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
            &json!({"id": "u1", "plan": "pro"}),
        )
        .unwrap();
        assert_eq!(args[0], "SET");
        assert_eq!(args[1], "u1");
        let parsed: Value = serde_json::from_str(&args[2]).unwrap();
        assert_eq!(parsed, json!({"id": "u1", "plan": "pro"}));
    }

    #[test]
    fn key_value_record_command_stringifies_non_string_key() {
        let args = record_command_args(
            &RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
            &json!({"id": 42}),
        )
        .unwrap();
        assert_eq!(args[1], "42");
    }

    #[test]
    fn key_value_record_command_missing_key_field_is_typed_sink_error() {
        let err = record_command_args(
            &RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
            &json!({"other": 1}),
        )
        .unwrap_err();
        match err {
            FaucetError::Sink(m) => assert!(m.contains("missing key field 'id'"), "got: {m}"),
            other => panic!("expected Sink error, got: {other:?}"),
        }
    }

    /// Collect a pipe's commands as flat arg vectors for assertion.
    fn pipe_commands(pipe: &redis::Pipeline) -> Vec<Vec<String>> {
        pipe.cmd_iter()
            .map(|cmd| {
                cmd.args_iter()
                    .map(|a| match a {
                        redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        redis::Arg::Cursor => "<cursor>".to_string(),
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn append_record_command_appends_exactly_the_pure_args() {
        let sink_type = RedisSinkType::List { key: "q".into() };
        let records = [json!({"id": 1}), json!({"id": 2})];
        let mut pipe = redis::pipe();
        for r in &records {
            append_record_command(&mut pipe, &sink_type, r).unwrap();
        }
        let cmds = pipe_commands(&pipe);
        assert_eq!(cmds.len(), 2);
        for (cmd, record) in cmds.iter().zip(&records) {
            assert_eq!(cmd, &record_command_args(&sink_type, record).unwrap());
        }
    }

    #[test]
    fn append_record_command_propagates_builder_errors() {
        let sink_type = RedisSinkType::KeyValue {
            key_field: "id".into(),
        };
        let mut pipe = redis::pipe();
        let err = append_record_command(&mut pipe, &sink_type, &json!({"no": "id"})).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
        assert_eq!(pipe.cmd_iter().count(), 0, "no command must be appended");
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config =
            RedisSinkConfig::new("redis://localhost", RedisSinkType::List { key: "k".into() });
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match RedisSink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }
}
