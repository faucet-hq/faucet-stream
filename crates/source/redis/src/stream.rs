//! Redis source stream executor.

use crate::config::{RedisBinary, RedisJsonParsing, RedisSourceConfig, RedisSourceType};
use async_trait::async_trait;
use base64::Engine as _;
use faucet_core::{FaucetError, Stream, StreamPage};
use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::pin::Pin;

/// How the source turns raw Redis bytes into JSON values.
#[derive(Debug, Clone, Copy)]
struct Decoding {
    parse_json: RedisJsonParsing,
    binary: RedisBinary,
}

impl Decoding {
    fn of(config: &RedisSourceConfig) -> Self {
        Self {
            parse_json: config.parse_json,
            binary: config.binary,
        }
    }

    /// Decode one value read from `key`. Invalid UTF-8 follows `binary` (a
    /// whole reply used to fail on one binary value, #789 MSG-54); valid text
    /// is parsed as JSON only as `parse_json` allows (#789 MSG-85).
    fn value(self, bytes: &[u8], key: &str) -> Result<Value, FaucetError> {
        let text = match std::str::from_utf8(bytes) {
            Ok(t) => std::borrow::Cow::Borrowed(t),
            Err(_) => match self.binary {
                RedisBinary::Base64 => {
                    return Ok(Value::String(
                        base64::engine::general_purpose::STANDARD.encode(bytes),
                    ));
                }
                RedisBinary::Lossy => String::from_utf8_lossy(bytes),
                RedisBinary::Error => {
                    return Err(FaucetError::Source(format!(
                        "Redis value at '{key}' is not valid UTF-8 (set `binary: base64` or \
                         `binary: lossy` to read binary values)"
                    )));
                }
            },
        };
        Ok(self.text(&text))
    }

    fn text(self, text: &str) -> Value {
        let parse = match self.parse_json {
            RedisJsonParsing::None => false,
            RedisJsonParsing::All => true,
            RedisJsonParsing::Containers => {
                matches!(text.trim_start().as_bytes().first(), Some(b'{' | b'['))
            }
        };
        if parse && let Ok(v) = serde_json::from_str::<Value>(text) {
            return v;
        }
        Value::String(text.to_string())
    }
}

/// A configured Redis source that reads records from Redis data structures.
pub struct RedisSource {
    config: RedisSourceConfig,
    /// Lazily-opened connection, reused across every read. A
    /// [`ConnectionManager`] reconnects after the server connection drops (a
    /// failover), where a bare multiplexed connection failed every later
    /// command until it was rebuilt (#789 MSG-78). Cheap to clone.
    conn: std::panic::AssertUnwindSafe<tokio::sync::OnceCell<ConnectionManager>>,
}

impl RedisSource {
    /// Create a new Redis source from the given configuration. The connection
    /// is opened lazily on first use, so construction stays synchronous and does
    /// no I/O; it fails only on an invalid config (an out-of-range `batch_size`).
    pub fn new(config: RedisSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            conn: std::panic::AssertUnwindSafe(tokio::sync::OnceCell::new()),
        })
    }

    /// Return a clone of the shared connection, opening it once on first call.
    async fn connection(&self) -> Result<ConnectionManager, FaucetError> {
        let conn = self
            .conn
            .get_or_try_init(|| async {
                let client = redis::Client::open(self.config.url.as_str())
                    .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
                let manager = redis::aio::ConnectionManagerConfig::new()
                    .set_number_of_retries(3)
                    .set_factor(2)
                    .set_max_delay(1000)
                    .set_connection_timeout(std::time::Duration::from_secs(10));
                ConnectionManager::new_with_config(client, manager)
                    .await
                    .map_err(|e| FaucetError::Source(format!("Redis connection failed: {e}")))
            })
            .await?;
        Ok(conn.clone())
    }

    /// Fetch all records from the configured Redis source.
    ///
    /// The same reads as [`stream_pages`](faucet_core::Source::stream_pages),
    /// collected: a `Stream` source uses `XRANGE` here too, so a preview never
    /// claims entries into a consumer group's pending list (#789 MSG-18).
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.collect(&std::collections::HashMap::new()).await
    }

    async fn collect(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use faucet_core::Source as _;
        let mut records = Vec::new();
        let pages = self.stream_pages(context, self.config.batch_size);
        futures::pin_mut!(pages);
        while let Some(page) = futures::StreamExt::next(&mut pages).await {
            records.extend(page?.records);
        }
        tracing::info!(records = records.len(), "Redis fetch complete");
        Ok(records)
    }
}

/// Convert a single XRANGE stream entry into the JSON record shape.
fn stream_entry_to_json(
    id: &str,
    map: &std::collections::HashMap<String, redis::Value>,
    decoding: Decoding,
) -> Result<Value, FaucetError> {
    let mut fields = serde_json::Map::new();
    for (field_name, field_value) in map {
        let val = match field_value {
            redis::Value::BulkString(bytes) => decoding.value(bytes, field_name)?,
            redis::Value::SimpleString(s) => decoding.text(s),
            redis::Value::Int(n) => json!(n),
            redis::Value::Double(n) => json!(n),
            redis::Value::Boolean(b) => json!(b),
            redis::Value::Nil => Value::Null,
            other => Value::String(format!("{other:?}")),
        };
        fields.insert(field_name.clone(), val);
    }
    Ok(json!({
        "id": id,
        "fields": Value::Object(fields),
    }))
}

/// Escape the glob metacharacters `SCAN MATCH` interprets, so a value
/// substituted into a `Keys` pattern matches literally instead of widening the
/// match (#789 MSG-94).
fn escape_glob(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\' | '^') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Substitute `context` into a `Keys` pattern, escaping each value.
fn substitute_pattern(pattern: &str, context: &std::collections::HashMap<String, Value>) -> String {
    let escaped: std::collections::HashMap<String, Value> = context
        .iter()
        .map(|(k, v)| {
            let text = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (k.clone(), Value::String(escape_glob(&text)))
        })
        .collect();
    faucet_core::util::substitute_context(pattern, &escaped)
}

/// Parse a Redis stream entry ID (`ms-seq`) and return the immediate
/// successor ID, used to advance the `start` argument of the next `XRANGE`
/// call without re-emitting the last entry of the previous page.
fn next_stream_id(id: &str) -> String {
    // Stream IDs are `<ms>-<seq>`. The "next" ID after `a-b` is `a-(b+1)`,
    // wrapping to `(a+1)-0` on `u64::MAX` (which we treat as terminal).
    if let Some((ms, seq)) = id.split_once('-')
        && let (Ok(ms), Ok(seq)) = (ms.parse::<u64>(), seq.parse::<u64>())
    {
        return match seq.checked_add(1) {
            Some(next_seq) => format!("{ms}-{next_seq}"),
            None => format!("{}-0", ms.saturating_add(1)),
        };
    }
    // Fall back to appending `\x00` — XRANGE treats this as "just after".
    // Reachable only if Redis ever returns a malformed ID, which it does not
    // in practice, but we degrade safely.
    format!("{id}\u{0}")
}

#[async_trait]
impl faucet_core::Source for RedisSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.collect(context).await
    }

    /// Stream records page-by-page so the pipeline can write to the sink as
    /// pages arrive instead of buffering the full result set. Each mode maps
    /// [`RedisSourceConfig::batch_size`] onto its native paging primitive
    /// (see the type-level doc on [`RedisSourceConfig::batch_size`]).
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the underlying primitive into a single page.
    /// The Redis source has no incremental-replication mode today, so every
    /// emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let max_records = self.config.max_records;
        let decoding = Decoding::of(&self.config);

        Box::pin(async_stream::try_stream! {
            let mut conn = self.connection().await?;

            let mut emitted: usize = 0;

            match &self.config.source_type {
                RedisSourceType::List { key } => {
                    let resolved = if context.is_empty() {
                        key.clone()
                    } else {
                        faucet_core::util::substitute_context(key, context)
                    };
                    let pages = stream_list(&mut conn, &resolved, batch_size, max_records, decoding);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
                RedisSourceType::Stream { key, group, consumer, .. } => {
                    // Every path uses XRANGE: reading through a consumer group
                    // would move entries into the group's pending list and
                    // withhold them from its other consumers (#789 MSG-18). A
                    // configured group is ignored — warn so the full re-read
                    // every run is not a surprise (F56).
                    if stream_ignores_consumer_group(group.as_deref(), consumer.as_deref()) {
                        tracing::warn!(
                            stream = %key,
                            "Redis Stream source has a consumer group/consumer configured; it is \
                             ignored — the source reads with XRANGE and re-reads the entire \
                             stream every run. Drop group/consumer to silence this warning."
                        );
                    }
                    let resolved = if context.is_empty() {
                        key.clone()
                    } else {
                        faucet_core::util::substitute_context(key, context)
                    };
                    let pages = stream_xrange(&mut conn, &resolved, batch_size, max_records, decoding);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
                RedisSourceType::Keys { pattern } => {
                    let resolved = if context.is_empty() {
                        pattern.clone()
                    } else {
                        substitute_pattern(pattern, context)
                    };
                    let pages = stream_keys(&mut conn, &resolved, batch_size, max_records, decoding);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
            }

            tracing::info!(
                records = emitted,
                batch_size,
                "Redis source stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "redis"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RedisSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        use crate::config::RedisSourceType;
        let base = faucet_core::redact_uri_credentials(&self.config.url);
        match &self.config.source_type {
            RedisSourceType::List { key } => format!("{base}?key={key}"),
            RedisSourceType::Stream { key, .. } => format!("{base}?stream={key}"),
            RedisSourceType::Keys { pattern } => format!("{base}?key={pattern}"),
        }
    }
}

/// Stream a Redis list via `LRANGE start stop`, sliding the window by
/// `batch_size`. With `batch_size == 0`, drains the list in a single
/// `LRANGE 0 -1` round-trip.
///
/// **Consistency caveat (#78 LOW):** index-based `LRANGE` paging is only
/// stable if the list is not mutated mid-scan. A concurrent `LPUSH` / `LPOP`
/// shifts every element's index, so a writer pushing/popping while this drains
/// can make the source skip or duplicate elements across page boundaries. For
/// a queue-style workload where the list is being consumed concurrently,
/// prefer a Redis Stream (`XRANGE`/consumer groups) over a list.
fn stream_list<'a>(
    conn: &'a mut ConnectionManager,
    key: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
    decoding: Decoding,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    async_stream::try_stream! {
        if batch_size == 0 {
            let values = lrange(conn, key, 0, -1).await?;
            let mut records = decode_all(&values, key, decoding)?;
            if let Some(max) = max_records {
                records.truncate(max);
            }
            yield StreamPage { records, bookmark: None };
            return;
        }

        let mut start: isize = 0;
        let mut emitted: usize = 0;
        loop {
            let stop: isize = start + batch_size as isize - 1;
            let values = lrange(conn, key, start, stop).await?;
            if values.is_empty() {
                break;
            }
            let mut records = decode_all(&values, key, decoding)?;
            let returned = records.len();
            // Respect max_records — truncate the final page and stop.
            let mut stop_after_yield = false;
            if let Some(max) = max_records
                && emitted + records.len() >= max
            {
                records.truncate(max - emitted);
                stop_after_yield = true;
            }
            emitted += records.len();
            yield StreamPage { records, bookmark: None };
            if stop_after_yield || returned < batch_size {
                break;
            }
            start += batch_size as isize;
        }
    }
}

/// `LRANGE` as raw bytes, so one binary element cannot fail the whole reply.
async fn lrange(
    conn: &mut ConnectionManager,
    key: &str,
    start: isize,
    stop: isize,
) -> Result<Vec<Vec<u8>>, FaucetError> {
    redis::cmd("LRANGE")
        .arg(key)
        .arg(start)
        .arg(stop)
        .query_async(conn)
        .await
        .map_err(|e| FaucetError::Source(format!("LRANGE failed on '{key}': {e}")))
}

fn decode_all(
    values: &[Vec<u8>],
    key: &str,
    decoding: Decoding,
) -> Result<Vec<Value>, FaucetError> {
    values.iter().map(|v| decoding.value(v, key)).collect()
}

/// Stream a Redis stream via `XRANGE start + COUNT batch_size`, advancing the
/// start ID on each page. With `batch_size == 0`, drains via a single
/// `XRANGE - +` round-trip.
fn stream_xrange<'a>(
    conn: &'a mut ConnectionManager,
    key: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
    decoding: Decoding,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    use redis::AsyncCommands as _;
    async_stream::try_stream! {
        if batch_size == 0 {
            let reply: redis::streams::StreamRangeReply = conn
                .xrange_all(key)
                .await
                .map_err(|e| FaucetError::Source(format!("XRANGE failed on '{key}': {e}")))?;
            let mut records: Vec<Value> = reply
                .ids
                .iter()
                .map(|entry| stream_entry_to_json(&entry.id, &entry.map, decoding))
                .collect::<Result<_, _>>()?;
            if let Some(max) = max_records {
                records.truncate(max);
            }
            yield StreamPage { records, bookmark: None };
            return;
        }

        let mut start: String = "-".to_string();
        let mut emitted: usize = 0;
        loop {
            let reply: redis::streams::StreamRangeReply = conn
                .xrange_count(key, &start, "+", batch_size)
                .await
                .map_err(|e| FaucetError::Source(format!("XRANGE failed on '{key}': {e}")))?;

            if reply.ids.is_empty() {
                break;
            }

            // Capture the last returned ID before consuming the reply so we
            // can advance the cursor (`next_stream_id`) without re-emitting it.
            let last_id = reply
                .ids
                .last()
                .expect("non-empty checked above")
                .id
                .clone();
            let returned = reply.ids.len();
            let mut records: Vec<Value> = reply
                .ids
                .into_iter()
                .map(|entry| stream_entry_to_json(&entry.id, &entry.map, decoding))
                .collect::<Result<_, _>>()?;

            let mut stop_after_yield = false;
            if let Some(max) = max_records
                && emitted + records.len() >= max
            {
                records.truncate(max - emitted);
                stop_after_yield = true;
            }
            emitted += records.len();
            yield StreamPage { records, bookmark: None };

            if stop_after_yield || returned < batch_size {
                break;
            }
            start = next_stream_id(&last_id);
        }
    }
}

/// Stream keys matching `pattern`. The `SCAN` cursor is iterated server-side
/// (with `COUNT` set to a sensible hint), keys are buffered up to
/// `batch_size`, then `MGET`'d in one round-trip per page. With
/// `batch_size == 0`, drains the entire scan and emits one page after a
/// single `MGET`.
fn stream_keys<'a>(
    conn: &'a mut ConnectionManager,
    pattern: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
    decoding: Decoding,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    use faucet_core::DEFAULT_BATCH_SIZE;
    async_stream::try_stream! {
        // Drive the SCAN cursor manually (one `SCAN cursor MATCH .. COUNT ..`
        // round-trip at a time) rather than via the buffering `AsyncIter`, so
        // we can MGET + yield a page as soon as `batch_size` keys accumulate
        // instead of materialising the entire matched keyset first (#78 LOW).
        // SCAN COUNT is only a per-round-trip hint; a call may return more or
        // fewer keys than the hint, so we still buffer until a full page.
        let scan_hint = if batch_size == 0 { DEFAULT_BATCH_SIZE } else { batch_size };
        // `batch_size == 0` is the "no batching" sentinel — accumulate the
        // whole scan and emit one page (still one MGET).
        let chunk_size = if batch_size == 0 { usize::MAX } else { batch_size };
        let cap = max_records.unwrap_or(usize::MAX);

        let mut cursor: u64 = 0;
        let mut buffer: Vec<String> = Vec::new();
        let mut emitted: usize = 0;
        // SCAN may return a key more than once; emit each once (#789 MSG-85).
        let mut seen: HashSet<String> = HashSet::new();
        let mut skipped: usize = 0;

        'scan: loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(scan_hint)
                .query_async(conn)
                .await
                .map_err(|e| FaucetError::Source(format!("SCAN failed with pattern '{pattern}': {e}")))?;
            cursor = next_cursor;
            buffer.extend(keys.into_iter().filter(|k| seen.insert(k.clone())));

            // Flush as many full pages as the buffer now holds.
            while emitted < cap && buffer.len() >= chunk_size {
                let take = chunk_size.min(cap - emitted);
                let page_keys: Vec<String> = buffer.drain(..take).collect();
                let (records, other) = mget_records(conn, &page_keys, decoding).await?;
                skipped += other;
                emitted += records.len();
                yield StreamPage { records, bookmark: None };
            }

            if cursor == 0 || emitted >= cap {
                break 'scan;
            }
        }

        // Trailing partial page (and the single page in the batch_size==0 case).
        if emitted < cap && !buffer.is_empty() {
            let take = (cap - emitted).min(buffer.len());
            let page_keys: Vec<String> = buffer.drain(..take).collect();
            let (records, other) = mget_records(conn, &page_keys, decoding).await?;
            skipped += other;
            yield StreamPage { records, bookmark: None };
        }
        if skipped > 0 {
            tracing::warn!(
                pattern,
                skipped,
                "Redis Keys source skipped matched keys that are not strings (hash, set, list, \
                 zset or stream); narrow the pattern to string keys"
            );
        }
    }
}

/// `MGET` a slice of keys (as raw bytes) and pair them with their values via
/// [`collect_kv_records`]. A key `MGET` returns nil for was either deleted
/// since the `SCAN` or is not a string; the second count is the non-strings,
/// found with one pipelined `TYPE` round trip over the nil keys only (#789
/// MSG-30).
async fn mget_records(
    conn: &mut ConnectionManager,
    keys: &[String],
    decoding: Decoding,
) -> Result<(Vec<Value>, usize), FaucetError> {
    let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(keys)
        .query_async(conn)
        .await
        .map_err(|e| FaucetError::Source(format!("MGET failed: {e}")))?;
    let missing: Vec<&String> = keys
        .iter()
        .zip(&values)
        .filter(|(_, v)| v.is_none())
        .map(|(k, _)| k)
        .collect();
    let mut non_strings = 0;
    if !missing.is_empty() {
        let mut pipe = redis::pipe();
        for k in &missing {
            pipe.cmd("TYPE").arg(*k);
        }
        let types: Vec<String> = pipe
            .query_async(conn)
            .await
            .map_err(|e| FaucetError::Source(format!("TYPE failed: {e}")))?;
        non_strings = count_non_strings(&types);
    }
    Ok((collect_kv_records(keys, values, decoding)?, non_strings))
}

/// Keys whose `TYPE` is a non-string data type (`none` = deleted meanwhile).
fn count_non_strings(types: &[String]) -> usize {
    types
        .iter()
        .filter(|t| !matches!(t.as_str(), "none" | "string"))
        .count()
}

/// Pair `keys` with their `MGET`-returned values into `{ "key", "value" }`
/// records. Missing values (deleted between `SCAN` and `MGET`, or not
/// strings) are dropped.
fn collect_kv_records(
    keys: &[String],
    values: Vec<Option<Vec<u8>>>,
    decoding: Decoding,
) -> Result<Vec<Value>, FaucetError> {
    let mut out = Vec::new();
    for (key, value) in keys.iter().zip(values) {
        if let Some(v) = value {
            out.push(json!({ "key": key, "value": decoding.value(&v, key)? }));
        }
    }
    Ok(out)
}

/// `true` when a Redis Stream source has a consumer group/consumer configured
/// but is driven through the streaming (`stream_pages`) path, which uses
/// `XRANGE` and ignores the group — re-reading the whole stream every run. Pure
/// predicate so the load-time warning's condition is unit-testable (F56).
fn stream_ignores_consumer_group(group: Option<&str>, consumer: Option<&str>) -> bool {
    group.is_some() || consumer.is_some()
}

#[cfg(test)]
mod tests {
    #[test]
    fn keeps_its_auto_traits() {
        fn assert<T: Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert::<super::RedisSource>();
    }

    use super::*;
    use crate::config::RedisSourceConfig;

    fn cfg() -> RedisSourceConfig {
        RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        )
    }

    #[test]
    fn values_parse_as_json_only_as_configured() {
        let mut c = cfg();
        let d = Decoding::of(&c);
        assert_eq!(d.value(b"1.10", "k").unwrap(), json!("1.10"));
        assert_eq!(d.value(b"true", "k").unwrap(), json!("true"));
        assert_eq!(
            d.value(b"123456789012345678901234", "k").unwrap(),
            json!("123456789012345678901234")
        );
        assert_eq!(d.value(b" {\"a\":1}", "k").unwrap(), json!({"a": 1}));
        assert_eq!(d.value(b"[1,2]", "k").unwrap(), json!([1, 2]));
        assert_eq!(d.value(b"{oops", "k").unwrap(), json!("{oops"));
        c.parse_json = RedisJsonParsing::All;
        assert_eq!(Decoding::of(&c).value(b"true", "k").unwrap(), json!(true));
        c.parse_json = RedisJsonParsing::None;
        assert_eq!(Decoding::of(&c).value(b"[1]", "k").unwrap(), json!("[1]"));
    }

    #[test]
    fn binary_values_follow_the_binary_policy() {
        let bytes = [0xff_u8, 0x00, b'a'];
        let mut c = cfg();
        assert_eq!(Decoding::of(&c).value(&bytes, "k").unwrap(), json!("/wBh"));
        c.binary = RedisBinary::Lossy;
        assert_eq!(
            Decoding::of(&c).value(&bytes, "k").unwrap(),
            json!("\u{fffd}\u{0}a")
        );
        c.binary = RedisBinary::Error;
        let err = Decoding::of(&c).value(&bytes, "bin:1").unwrap_err();
        assert!(err.to_string().contains("bin:1"), "{err}");
        assert_eq!(Decoding::of(&c).value(b"ok", "k").unwrap(), json!("ok"));
    }

    #[test]
    fn keys_pattern_substitution_escapes_glob_metacharacters() {
        let ctx: std::collections::HashMap<String, Value> = [
            ("p.id".to_string(), json!("a*b?[c]\\^")),
            ("n".to_string(), json!(7)),
        ]
        .into();
        assert_eq!(
            substitute_pattern("user:{p.id}:{n}:*", &ctx),
            "user:a\\*b\\?\\[c\\]\\\\\\^:7:*"
        );
    }

    #[test]
    fn only_non_string_types_count_as_skipped() {
        let types: Vec<String> = ["string", "none", "hash", "zset"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(count_non_strings(&types), 2);
    }

    #[test]
    fn kv_records_drop_missing_values() {
        let keys = vec!["a".to_string(), "b".to_string()];
        let out = collect_kv_records(
            &keys,
            vec![Some(b"{\"x\":1}".to_vec()), None],
            Decoding::of(&cfg()),
        )
        .unwrap();
        assert_eq!(out, vec![json!({"key": "a", "value": {"x": 1}})]);
    }

    #[test]
    fn stream_ignores_consumer_group_flags_configured_group() {
        // F56: a configured group/consumer on the streaming path is ignored.
        assert!(stream_ignores_consumer_group(Some("g"), Some("c")));
        assert!(stream_ignores_consumer_group(Some("g"), None));
        assert!(stream_ignores_consumer_group(None, Some("c")));
        // No group/consumer → plain XRANGE drain, no warning.
        assert!(!stream_ignores_consumer_group(None, None));
    }

    #[test]
    fn creates_source() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "test".into() },
        );
        let _source = RedisSource::new(config).unwrap();
    }

    #[test]
    fn dataset_uri_list_source() {
        use faucet_core::Source;
        let source = RedisSource::new(RedisSourceConfig::new(
            "redis://u:p@localhost:6379/0",
            RedisSourceType::List {
                key: "my-list".into(),
            },
        ))
        .unwrap();
        assert_eq!(source.dataset_uri(), "redis://localhost:6379/0?key=my-list");
    }

    #[test]
    fn dataset_uri_stream_source() {
        use faucet_core::Source;
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::Stream {
                key: "events".into(),
                group: None,
                consumer: None,
                count: None,
            },
        );
        let source = RedisSource::new(config).unwrap();
        assert_eq!(source.dataset_uri(), "redis://localhost?stream=events");
    }

    #[test]
    fn dataset_uri_keys_source() {
        use faucet_core::Source;
        let source = RedisSource::new(RedisSourceConfig::new(
            "redis://u:p@localhost:6379/0",
            RedisSourceType::Keys {
                pattern: "user:*".into(),
            },
        ))
        .unwrap();
        assert_eq!(
            source.dataset_uri(),
            "redis://localhost:6379/0?key=user:*",
            "keys variant renders the glob pattern as the key, with credentials redacted"
        );
    }

    #[test]
    fn config_schema_describes_redis_source_config() {
        use faucet_core::Source;
        let source = RedisSource::new(RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        ))
        .unwrap();
        let schema = source.config_schema();
        // The schema must expose the user-facing config fields.
        let props = &schema["properties"];
        assert!(props.get("url").is_some(), "schema exposes 'url'");
        assert!(
            props.get("source_type").is_some(),
            "schema exposes 'source_type'"
        );
        assert!(
            props.get("batch_size").is_some(),
            "schema exposes 'batch_size'"
        );
    }

    #[test]
    fn new_rejects_out_of_range_batch_size() {
        let mut config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "test".into() },
        );
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match RedisSource::new(config) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            other => panic!(
                "expected a batch_size Config error, got {:?}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn next_stream_id_increments_sequence() {
        assert_eq!(next_stream_id("1234-0"), "1234-1");
        assert_eq!(next_stream_id("1234-99"), "1234-100");
    }

    #[test]
    fn next_stream_id_wraps_seq_overflow() {
        let id = format!("5-{}", u64::MAX);
        assert_eq!(next_stream_id(&id), "6-0");
    }

    #[test]
    fn next_stream_id_falls_back_on_malformed_id() {
        // Not a real Redis ID — fallback path appends NUL.
        let next = next_stream_id("not-a-real-id");
        assert!(next.starts_with("not-a-real-id"));
        assert!(next.ends_with('\u{0}'));
    }

    #[test]
    fn stream_entry_to_json_extracts_id_and_fields() {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "field1".to_string(),
            redis::Value::BulkString(b"value1".to_vec()),
        );
        map.insert("field2".to_string(), redis::Value::Int(42));
        let json = stream_entry_to_json("100-0", &map, Decoding::of(&cfg())).unwrap();
        assert_eq!(json["id"], "100-0");
        assert_eq!(json["fields"]["field1"], "value1");
        assert_eq!(json["fields"]["field2"], 42);
    }
}
