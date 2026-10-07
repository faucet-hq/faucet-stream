//! Redis-backed [`StateStore`].

use async_trait::async_trait;
use faucet_core::state::{DOCTOR_SENTINEL_KEY, StateStore, validate_state_key};
use faucet_core::{FaucetError, Value};
use redis::AsyncCommands;

/// Longest a (re)connect may take before the operation fails.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Longest one command may wait for Redis's reply before it fails.
pub const RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A `StateStore` that persists each entry as a single Redis string under
/// `{namespace}:{key}`.
///
/// [`RedisStateStore::connect`] holds a `redis::aio::ConnectionManager`: a
/// dropped connection (a Redis restart, failover or idle cut) is re-established
/// instead of failing every later call, and connects and replies are bounded
/// by [`CONNECT_TIMEOUT`] / [`RESPONSE_TIMEOUT`] so a black-holed server cannot
/// stall a run.
pub struct RedisStateStore {
    namespace: String,
    conn: Conn,
}

/// The store's connection: a self-healing manager, or a caller-supplied
/// multiplexed connection ([`RedisStateStore::from_connection`]).
#[derive(Clone)]
enum Conn {
    Managed(redis::aio::ConnectionManager),
    Shared(redis::aio::MultiplexedConnection),
}

impl redis::aio::ConnectionLike for Conn {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Conn::Managed(c) => c.req_packed_command(cmd),
            Conn::Shared(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Conn::Managed(c) => c.req_packed_commands(cmd, offset, count),
            Conn::Shared(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Conn::Managed(c) => c.get_db(),
            Conn::Shared(c) => c.get_db(),
        }
    }
}

/// Whether `e` means the connection itself failed (the manager reconnects,
/// so the operation is worth one more try).
fn is_connection_error(e: &redis::RedisError) -> bool {
    e.is_connection_dropped() || e.is_connection_refusal() || e.is_io_error() || e.is_timeout()
}

/// Run `op` on a fresh clone of `conn`, retrying once after a connection
/// failure (by then the manager has reconnected).
async fn with_retry<T, F, Fut>(conn: &Conn, mut op: F) -> redis::RedisResult<T>
where
    F: FnMut(Conn) -> Fut,
    Fut: std::future::Future<Output = redis::RedisResult<T>>,
{
    match op(conn.clone()).await {
        Err(e) if is_connection_error(&e) && matches!(conn, Conn::Managed(_)) => {
            tracing::warn!(error = %e, "Redis state connection failed; retrying once");
            op(conn.clone()).await
        }
        other => other,
    }
}

impl RedisStateStore {
    /// Connect to a Redis server and namespace all keys under `namespace:`.
    ///
    /// `url` follows the standard Redis URL form (`redis://[:pass@]host:port/db`).
    pub async fn connect(
        url: impl AsRef<str>,
        namespace: impl Into<String>,
    ) -> Result<Self, FaucetError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        // `rediss://` builds a rustls config from the process-wide provider; a
        // build that links two providers has no default, so pick one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = redis::Client::open(url.as_ref())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(CONNECT_TIMEOUT)
            .set_response_timeout(RESPONSE_TIMEOUT)
            .set_number_of_retries(3);
        let conn = tokio::time::timeout(
            CONNECT_TIMEOUT,
            redis::aio::ConnectionManager::new_with_config(client, config),
        )
        .await
        .map_err(|_| {
            FaucetError::State(format!(
                "Redis connection timed out after {CONNECT_TIMEOUT:?}"
            ))
        })?
        .map_err(|e| FaucetError::State(format!("Redis connection failed: {e}")))?;
        Ok(Self {
            namespace,
            conn: Conn::Managed(conn),
        })
    }

    /// Construct from an existing async connection. Useful for tests and for
    /// integrators who want to share a connection across multiple stores.
    pub fn from_connection(
        conn: redis::aio::MultiplexedConnection,
        namespace: impl Into<String>,
    ) -> Result<Self, FaucetError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        Ok(Self {
            namespace,
            conn: Conn::Shared(conn),
        })
    }

    /// Returns the fully-qualified Redis key for a given state key.
    pub fn redis_key(&self, key: &str) -> String {
        build_redis_key(&self.namespace, key)
    }
}

/// Keys asked of each `SCAN` page.
const SCAN_COUNT: usize = 500;

/// Format the namespaced Redis key. Exposed as a free function so it can be
/// unit-tested without constructing a `RedisStateStore` (which needs a real
/// Redis connection).
pub(crate) fn build_redis_key(namespace: &str, key: &str) -> String {
    format!("{namespace}:{key}")
}

/// Sets `KEYS[1]` to `ARGV[3]` when it holds exactly `ARGV[2]` (`ARGV[1]` =
/// `1`) or is absent (`ARGV[1]` = `0`); returns 1 when it wrote.
pub(crate) const COMPARE_AND_SET_LUA: &str = r"
local cur = redis.call('GET', KEYS[1])
if ARGV[1] == '0' then
  if cur then return 0 end
elseif cur ~= ARGV[2] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[3])
return 1
";

/// Retries when the stored string changed between the read and the script but
/// still decodes to the expected value (an equal value re-written).
const CAS_ATTEMPTS: usize = 8;

/// The `SCAN MATCH` pattern for keys starting with `prefix`. Neither the
/// namespace nor a valid key can hold a glob metacharacter, so no escaping.
pub(crate) fn scan_pattern(namespace: &str, prefix: &str) -> String {
    format!("{namespace}:{prefix}*")
}

/// The state key behind a namespaced Redis key.
pub(crate) fn strip_namespace<'a>(namespace: &str, raw: &'a str) -> Option<&'a str> {
    raw.strip_prefix(namespace)?.strip_prefix(':')
}

pub(crate) fn validate_namespace(namespace: &str) -> Result<(), FaucetError> {
    if namespace.is_empty() {
        return Err(FaucetError::Config(
            "Redis state namespace must not be empty".into(),
        ));
    }
    for (i, c) in namespace.char_indices() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
        if !ok {
            return Err(FaucetError::Config(format!(
                "Redis state namespace contains illegal character {c:?} at byte {i}"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl StateStore for RedisStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        let rkey = self.redis_key(key);
        let raw: Option<String> = with_retry(&self.conn, |mut c| {
            let rkey = rkey.clone();
            async move { c.get(rkey).await }
        })
        .await
        .map_err(|e| FaucetError::State(format!("Redis GET for key '{key}' failed: {e}")))?;
        match raw {
            None => Ok(None),
            Some(s) => {
                let value: Value = serde_json::from_str(&s).map_err(|e| {
                    FaucetError::State(format!(
                        "stored value for key '{key}' is not valid JSON: {e}"
                    ))
                })?;
                Ok(Some(value))
            }
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let serialized = serde_json::to_string(value).map_err(|e| {
            FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
        })?;
        let rkey = self.redis_key(key);
        let _: () = with_retry(&self.conn, |mut c| {
            let (rkey, serialized) = (rkey.clone(), serialized.clone());
            async move { c.set(rkey, serialized).await }
        })
        .await
        .map_err(|e| FaucetError::State(format!("Redis SET for key '{key}' failed: {e}")))?;
        tracing::debug!(key, namespace = %self.namespace, "state written to Redis");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let rkey = self.redis_key(key);
        let _: i64 = with_retry(&self.conn, |mut c| {
            let rkey = rkey.clone();
            async move { c.del(rkey).await }
        })
        .await
        .map_err(|e| FaucetError::State(format!("Redis DEL for key '{key}' failed: {e}")))?;
        Ok(())
    }

    fn supports_list(&self) -> bool {
        true
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        let pattern = scan_pattern(&self.namespace, prefix);
        // Drive the cursor by hand: the iterator form ends silently when a
        // later page fails, which would return a truncated key set.
        let mut keys = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, batch): (u64, Vec<String>) = with_retry(&self.conn, |mut c| {
                let pattern = pattern.clone();
                async move {
                    redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(pattern)
                        .arg("COUNT")
                        .arg(SCAN_COUNT)
                        .query_async(&mut c)
                        .await
                }
            })
            .await
            .map_err(|e| FaucetError::State(format!("Redis SCAN for '{pattern}' failed: {e}")))?;
            keys.extend(
                batch
                    .iter()
                    .filter_map(|raw| strip_namespace(&self.namespace, raw))
                    .filter(|key| key.starts_with(prefix))
                    .map(str::to_owned),
            );
            if next == 0 {
                break;
            }
            cursor = next;
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    fn supports_compare_and_put(&self) -> bool {
        true
    }

    /// Atomic: a Lua script swaps the value only while the stored string is
    /// still the one this call compared (or the key is still absent).
    async fn compare_and_put(
        &self,
        key: &str,
        expected: Option<&Value>,
        value: &Value,
    ) -> Result<bool, FaucetError> {
        validate_state_key(key)?;
        let serialized = serde_json::to_string(value).map_err(|e| {
            FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
        })?;
        let redis_key = self.redis_key(key);
        let script = redis::Script::new(COMPARE_AND_SET_LUA);
        let mut conn = self.conn.clone();
        for _ in 0..CAS_ATTEMPTS {
            let raw: Option<String> = with_retry(&self.conn, |mut c| {
                let rkey = redis_key.clone();
                async move { c.get(rkey).await }
            })
            .await
            .map_err(|e| FaucetError::State(format!("Redis GET for key '{key}' failed: {e}")))?;
            let current = raw
                .as_deref()
                .map(|s| {
                    serde_json::from_str::<Value>(s).map_err(|e| {
                        FaucetError::State(format!(
                            "stored value for key '{key}' is not valid JSON: {e}"
                        ))
                    })
                })
                .transpose()?;
            if current.as_ref() != expected {
                return Ok(false);
            }
            let mut call = script.key(&redis_key);
            match &raw {
                Some(guard) => call.arg("1").arg(guard),
                None => call.arg("0").arg(""),
            };
            // Not retried: a lost reply after a successful swap would read
            // back as a mismatch.
            let swapped: i64 = call
                .arg(&serialized)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| {
                    FaucetError::State(format!("Redis compare-and-set for key '{key}' failed: {e}"))
                })?;
            if swapped == 1 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn supports_atomic_batch(&self) -> bool {
        true
    }

    async fn put_batch(&self, entries: &[(String, Value)]) -> Result<(), FaucetError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut pairs = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            validate_state_key(key)?;
            let serialized = serde_json::to_string(value).map_err(|e| {
                FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
            })?;
            pairs.push((self.redis_key(key), serialized));
        }
        let _: () = with_retry(&self.conn, |mut c| {
            let pairs = pairs.clone();
            async move { c.mset(&pairs).await }
        })
        .await
        .map_err(|e| FaucetError::State(format!("Redis MSET failed: {e}")))?;
        Ok(())
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Exercise the real put → get → delete cycle on a sentinel key. This
        // validates connectivity, auth, and read/write permissions through the
        // actual code path and leaves no residue.
        let start = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.sentinel_roundtrip()).await {
            Ok(Ok(())) => Probe::pass("sentinel", start.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                e.to_string(),
                "verify the Redis server is reachable and the credentials grant read/write access",
            ),
            Err(_) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                format!(
                    "round-trip timed out after {:?}; Redis did not respond",
                    ctx.timeout
                ),
                "verify the Redis server is reachable or raise the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

impl RedisStateStore {
    /// Write, read back, and delete a sentinel key — the body of the `check()`
    /// probe, factored out so the happy path stays linear. Reuses the store's
    /// own `put`/`get`/`delete`, which already namespace the key.
    async fn sentinel_roundtrip(&self) -> Result<(), FaucetError> {
        let probe = serde_json::json!({ "faucet_doctor": true });
        self.put(DOCTOR_SENTINEL_KEY, &probe).await?;
        let got = self.get(DOCTOR_SENTINEL_KEY).await?;
        // Best-effort cleanup regardless of the read result.
        let _ = self.delete(DOCTOR_SENTINEL_KEY).await;
        match got {
            Some(v) if v == probe => Ok(()),
            _ => Err(FaucetError::State(
                "sentinel readback did not match what was written".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_redis_key_namespaces_consistently() {
        assert_eq!(
            build_redis_key("faucet", "github_issues"),
            "faucet:github_issues"
        );
        assert_eq!(build_redis_key("a", "b"), "a:b");
    }

    #[test]
    fn scan_pattern_and_strip_namespace_round_trip() {
        assert_eq!(scan_pattern("faucet", "orders::"), "faucet:orders::*");
        assert_eq!(
            strip_namespace("faucet", "faucet:orders::a"),
            Some("orders::a")
        );
        assert_eq!(strip_namespace("faucet", "other:orders::a"), None);
        assert_eq!(strip_namespace("faucet", "faucetx"), None);
    }

    #[test]
    fn validate_namespace_accepts_typical_values() {
        for ns in ["faucet", "team-1.prod", "a_b", "ABC.123"] {
            validate_namespace(ns).unwrap_or_else(|e| panic!("expected ok for {ns:?}: {e}"));
        }
    }

    #[test]
    fn validate_namespace_rejects_empty() {
        let err = validate_namespace("").unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_namespace_rejects_illegal_chars() {
        for ns in ["a:b", "a/b", "a b", "hello world"] {
            let err = validate_namespace(ns).expect_err(&format!("expected error for {ns:?}"));
            assert!(matches!(err, FaucetError::Config(_)));
        }
    }

    #[tokio::test]
    async fn connect_rejects_invalid_url() {
        let result = RedisStateStore::connect("not a url", "faucet").await;
        match result {
            Err(FaucetError::Config(msg)) => assert!(msg.contains("invalid Redis URL")),
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn connect_rejects_invalid_namespace() {
        let result = RedisStateStore::connect("redis://127.0.0.1:6379", "bad:namespace").await;
        match result {
            Err(FaucetError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
