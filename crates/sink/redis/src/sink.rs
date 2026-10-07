//! Redis sink executor.

use crate::config::{RedisSinkConfig, RedisSinkType};
use async_trait::async_trait;
use faucet_core::{FaucetError, RowOutcome};
use redis::aio::ConnectionManager;
use serde_json::Value;

/// A configured Redis sink that writes records to Redis data structures.
///
/// The connection is established once during construction and reused across
/// all `write_batch()` calls.
pub struct RedisSink {
    config: RedisSinkConfig,
    /// Reconnects after the server connection drops (a failover), where a
    /// bare multiplexed connection failed every later command (#789 MSG-78).
    conn: ConnectionManager,
}

/// The exactly-once write: every data command and the watermark in one Lua
/// script, which refuses before writing anything when a target key holds
/// another Redis type. `MULTI`/`EXEC` applied the watermark even when a data
/// command failed at `EXEC` time (`WRONGTYPE`), so the next run skipped the
/// page and its records were lost (#789 MSG-19).
///
/// `KEYS[1]` is the watermark key, `KEYS[2..]` the data keys; `ARGV[1]` the
/// type every data key must have (`""` to skip the check), `ARGV[2]` the
/// token, then each command as its argument count followed by its arguments.
const IDEMPOTENT_SCRIPT: &str = r#"
local want = ARGV[1]
if want ~= '' then
  for i = 2, #KEYS do
    local t = redis.call('TYPE', KEYS[i])['ok']
    if t ~= 'none' and t ~= want then
      return redis.error_reply('WRONGTYPE ' .. KEYS[i] .. ' holds a ' .. t .. ', not a ' .. want)
    end
  end
end
local i = 3
while i <= #ARGV do
  local n = tonumber(ARGV[i])
  redis.call(unpack(ARGV, i + 1, i + n))
  i = i + n + 1
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
"#;

impl RedisSink {
    /// Create a new Redis sink from the given configuration.
    ///
    /// This opens the connection to Redis immediately.
    pub async fn new(config: RedisSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
        let manager = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(3)
            .set_factor(2)
            .set_max_delay(1000)
            .set_connection_timeout(std::time::Duration::from_secs(10));
        let conn = ConnectionManager::new_with_config(client, manager)
            .await
            .map_err(|e| FaucetError::Sink(format!("Redis connection failed: {e}")))?;

        Ok(Self { config, conn })
    }

    fn command(&self, record: &Value) -> Result<Vec<String>, FaucetError> {
        record_command_args_with(&self.config, record)
    }

    /// Pipeline `commands` in `batch_size` chunks.
    async fn run_commands(&self, commands: &[Vec<String>]) -> Result<(), FaucetError> {
        let mut conn = self.conn.clone();
        let chunk = if self.config.batch_size == 0 {
            commands.len().max(1)
        } else {
            self.config.batch_size
        };
        for group in commands.chunks(chunk) {
            let mut pipe = redis::pipe();
            for args in group {
                let mut cmd = redis::Cmd::new();
                for a in args {
                    cmd.arg(a);
                }
                pipe.add_command(cmd);
            }
            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("Redis pipeline execution failed: {e}")))?;
        }
        Ok(())
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

        // The connection manager is cheaply cloneable; clone to satisfy &self.
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
        // Build every command before sending any, so a bad record fails the
        // batch without having written the rows before it.
        let commands = records
            .iter()
            .map(|r| self.command(r))
            .collect::<Result<Vec<_>, _>>()?;
        self.run_commands(&commands).await?;
        tracing::debug!(records = records.len(), "Redis batch written");
        Ok(records.len())
    }

    /// Rows whose command cannot be built (a `KeyValue` record with a
    /// missing, null, object or array key) fail on their own and are DLQ'd;
    /// the rest are written (#789 MSG-20).
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(records.len());
        let mut commands = Vec::with_capacity(records.len());
        for r in records {
            match self.command(r) {
                Ok(c) => {
                    commands.push(c);
                    outcomes.push(Ok(()));
                }
                Err(e) => outcomes.push(Err(e)),
            }
        }
        if !commands.is_empty() {
            self.run_commands(&commands).await?;
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Write `records` AND durably record `token` for `scope` in one atomic
    /// Lua script ([`IDEMPOTENT_SCRIPT`]).
    ///
    /// Every record's command for the configured [`RedisSinkType`] plus a
    /// final `SET _faucet_commit_token:{scope} {token}`, after a type check
    /// that refuses before anything is written. A crash between "sink wrote"
    /// and "state persisted" is resolved on resume by `last_committed_token`.
    ///
    /// **`batch_size` re-chunking does NOT apply on this path.** Splitting the
    /// page across several scripts would break atomicity, so the entire page
    /// is one script regardless of `batch_size`.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let mut conn = self.conn.clone();
        let commands = records
            .iter()
            .map(|r| self.command(r))
            .collect::<Result<Vec<_>, _>>()?;
        let lua = redis::Script::new(IDEMPOTENT_SCRIPT);
        let mut script = lua.prepare_invoke();
        script.key(commit_token_key(scope));
        for key in data_keys(&commands) {
            script.key(key);
        }
        script.arg(expected_type(&self.config.sink_type)).arg(token);
        for args in &commands {
            script.arg(args.len());
            for a in args {
                script.arg(a);
            }
        }
        script.invoke_async::<i64>(&mut conn).await.map_err(|e| {
            FaucetError::Sink(format!(
                "Redis exactly-once write failed (nothing was written): {e}"
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

/// The Redis type every data key of a page must hold for the exactly-once
/// script (`""` = no check: `SET` replaces a key of any type).
fn expected_type(sink_type: &RedisSinkType) -> &'static str {
    match sink_type {
        RedisSinkType::List { .. } => "list",
        RedisSinkType::Stream { .. } => "stream",
        RedisSinkType::KeyValue { .. } => "",
    }
}

/// The distinct keys a set of commands writes (argument 1 of each).
fn data_keys(commands: &[Vec<String>]) -> Vec<&str> {
    let mut seen = std::collections::HashSet::new();
    commands
        .iter()
        .filter_map(|c| c.get(1).map(String::as_str))
        .filter(|k| seen.insert(*k))
        .collect()
}

/// [`record_command_args`] plus the config's write options (`SET … EX`,
/// `XADD … MAXLEN ~`).
fn record_command_args_with(
    config: &RedisSinkConfig,
    record: &Value,
) -> Result<Vec<String>, FaucetError> {
    let mut args = record_command_args(&config.sink_type, record)?;
    if let (RedisSinkType::KeyValue { .. }, Some(ttl)) = (&config.sink_type, config.ttl_secs) {
        args.push("EX".into());
        args.push(ttl.to_string());
    }
    if let (RedisSinkType::Stream { .. }, Some(max)) = (&config.sink_type, config.stream_max_len) {
        args.splice(
            2..2,
            ["MAXLEN".to_string(), "~".to_string(), max.to_string()],
        );
    }
    Ok(args)
}

/// Render the full Redis command (name first, then arguments) that writes one
/// record under the given sink mode. Pure — shared by every write path so
/// `write_batch` and `write_batch_idempotent` build identical commands.
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
            // A null, object or array key would collapse onto one rendered key
            // (`"null"`), silently overwriting records (#789 MSG-20).
            let key = match record.get(key_field) {
                None => {
                    return Err(FaucetError::Sink(format!(
                        "record missing key field '{key_field}'"
                    )));
                }
                Some(Value::String(s)) => s.clone(),
                Some(v @ (Value::Number(_) | Value::Bool(_))) => v.to_string(),
                Some(other) => {
                    return Err(FaucetError::Sink(format!(
                        "key field '{key_field}' is {}, not a string, number or boolean",
                        match other {
                            Value::Null => "null",
                            Value::Array(_) => "an array",
                            _ => "an object",
                        }
                    )));
                }
            };
            let serialized = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            Ok(vec!["SET".into(), key, serialized])
        }
    }
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

    #[test]
    fn null_object_and_array_keys_fail_per_row() {
        let t = RedisSinkType::KeyValue {
            key_field: "id".into(),
        };
        for bad in [
            json!({"id": null}),
            json!({"id": [1]}),
            json!({"id": {"a": 1}}),
        ] {
            let err = record_command_args(&t, &bad).unwrap_err();
            assert!(err.to_string().contains("not a string"), "{err}");
        }
        assert_eq!(
            record_command_args(&t, &json!({"id": true})).unwrap()[1],
            "true"
        );
    }

    #[test]
    fn write_options_extend_the_commands() {
        let mut c = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        );
        c.ttl_secs = Some(30);
        let args = record_command_args_with(&c, &json!({"id": "a"})).unwrap();
        assert_eq!(&args[args.len() - 2..], ["EX", "30"]);
        let mut c = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::Stream { key: "s".into() },
        );
        c.stream_max_len = Some(100);
        let args = record_command_args_with(&c, &json!({"f": 1})).unwrap();
        assert_eq!(&args[..6], ["XADD", "s", "MAXLEN", "~", "100", "*"]);
    }

    #[test]
    fn data_keys_are_distinct_and_typed_per_sink_type() {
        let cmds = vec![
            vec!["RPUSH".to_string(), "a".into(), "1".into()],
            vec!["RPUSH".to_string(), "a".into(), "2".into()],
            vec!["RPUSH".to_string(), "b".into(), "3".into()],
        ];
        assert_eq!(data_keys(&cmds), vec!["a", "b"]);
        assert_eq!(
            expected_type(&RedisSinkType::List { key: "a".into() }),
            "list"
        );
        assert_eq!(
            expected_type(&RedisSinkType::Stream { key: "a".into() }),
            "stream"
        );
        assert_eq!(
            expected_type(&RedisSinkType::KeyValue {
                key_field: "a".into()
            }),
            ""
        );
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
