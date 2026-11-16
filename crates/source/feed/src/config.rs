//! Feed source configuration.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default [`FeedSourceConfig::claim_ttl_secs`] of a delivery run under
/// `faucet serve`: 5 minutes.
pub const DEFAULT_CLAIM_TTL_SECS: u64 = 300;

/// Default [`FeedSourceConfig::claim_ttl_secs`] of a `faucet run` drain: one
/// minute, so a drain that died leaves its claim no longer than that.
pub const DEFAULT_STANDALONE_CLAIM_TTL_SECS: u64 = 60;

/// Largest accepted [`FeedSourceConfig::claim_ttl_secs`]: one day.
pub const MAX_CLAIM_TTL_SECS: u64 = 86_400;

/// Default [`FeedSourceConfig::max_events`] of a `faucet run` drain.
pub const DEFAULT_MAX_EVENTS: u64 = 10_000;

/// Default [`FeedSourceConfig::max_bytes`] of a `faucet run` drain: 64 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

fn default_true() -> bool {
    true
}

/// Configuration of the `feed` source.
///
/// Every field is optional. Under `faucet serve` the feed, the delivery and
/// the store come from the delivery run itself, and setting any of `feed`,
/// `delivery` or `store` is refused. Under `faucet run` all three are
/// required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedSourceConfig {
    /// The feed's uid in the store (`faucet run` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed: Option<String>,
    /// The delivery's id within the feed (`faucet run` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    /// The store that holds the feed (`faucet run` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<FeedStoreSpec>,
    /// Split a run's events into pages of this many. Each page is written and
    /// committed on its own. Unset: one page per run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_events: Option<u64>,
    /// Add `_feed: {seq, id, received_at, headers}` to every record.
    #[serde(default = "default_true")]
    pub include_metadata: bool,
    /// How long the range claim lasts without a renewal, in seconds. The
    /// source renews it every tenth of this while it runs. Default: 60 under
    /// `faucet run`, 300 for a delivery run under `faucet serve`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_ttl_secs: Option<u64>,
    /// Most events one `faucet run` drain reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u64>,
    /// Most payload bytes one `faucet run` drain reads; a single larger event
    /// is still read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

impl Default for FeedSourceConfig {
    fn default() -> Self {
        Self {
            feed: None,
            delivery: None,
            store: None,
            page_events: None,
            include_metadata: true,
            claim_ttl_secs: None,
            max_events: None,
            max_bytes: None,
        }
    }
}

impl FeedSourceConfig {
    /// A config with nothing set: the shape a delivery template uses.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read `delivery` of `feed` from `store` (the `faucet run` shape).
    pub fn standalone(
        feed: impl Into<String>,
        delivery: impl Into<String>,
        store: FeedStoreSpec,
    ) -> Self {
        Self {
            feed: Some(feed.into()),
            delivery: Some(delivery.into()),
            store: Some(store),
            ..Self::default()
        }
    }

    /// Split a run into pages of `events`.
    pub fn page_events(mut self, events: u64) -> Self {
        self.page_events = Some(events);
        self
    }

    /// Whether records carry the `_feed` metadata.
    pub fn include_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }

    /// Set the claim's time to live.
    pub fn claim_ttl_secs(mut self, secs: u64) -> Self {
        self.claim_ttl_secs = Some(secs);
        self
    }

    /// Set the most events one `faucet run` drain reads.
    pub fn max_events(mut self, events: u64) -> Self {
        self.max_events = Some(events);
        self
    }

    /// Set the most payload bytes one `faucet run` drain reads.
    pub fn max_bytes(mut self, bytes: u64) -> Self {
        self.max_bytes = Some(bytes);
        self
    }

    /// The claim's time to live for a delivery run under `faucet serve`.
    pub fn claim_ttl(&self) -> Duration {
        Duration::from_secs(self.claim_ttl_secs.unwrap_or(DEFAULT_CLAIM_TTL_SECS))
    }

    /// The claim's time to live for a `faucet run` drain.
    pub fn standalone_claim_ttl(&self) -> Duration {
        Duration::from_secs(
            self.claim_ttl_secs
                .unwrap_or(DEFAULT_STANDALONE_CLAIM_TTL_SECS),
        )
    }

    /// Refuse values no run could use.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let positive = |name: &str, value: Option<u64>| match value {
            Some(0) => Err(FaucetError::Config(format!(
                "feed source: `{name}` must be at least 1"
            ))),
            _ => Ok(()),
        };
        positive("page_events", self.page_events)?;
        positive("claim_ttl_secs", self.claim_ttl_secs)?;
        positive("max_events", self.max_events)?;
        positive("max_bytes", self.max_bytes)?;
        if self.claim_ttl_secs.is_some_and(|s| s > MAX_CLAIM_TTL_SECS) {
            return Err(FaucetError::Config(format!(
                "feed source: `claim_ttl_secs` must be at most {MAX_CLAIM_TTL_SECS}"
            )));
        }
        for (name, value) in [("feed", &self.feed), ("delivery", &self.delivery)] {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                return Err(FaucetError::Config(format!(
                    "feed source: `{name}` must not be empty"
                )));
            }
        }
        Ok(())
    }
}

/// The store a `faucet run` drain opens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum FeedStoreSpec {
    /// A SQLite feed store file.
    Sqlite(SqliteStoreConfig),
    /// A PostgreSQL feed store.
    Postgres(PostgresStoreConfig),
}

impl FeedStoreSpec {
    /// The SQLite store at `path`.
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self::Sqlite(SqliteStoreConfig { path: path.into() })
    }

    /// The PostgreSQL store at `url`.
    pub fn postgres(url: impl Into<String>) -> Self {
        Self::Postgres(PostgresStoreConfig { url: url.into() })
    }
}

/// A SQLite feed store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteStoreConfig {
    /// Path of the store's database file. It must exist.
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub path: String,
}

/// A PostgreSQL feed store.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresStoreConfig {
    /// Connection URL of the database that holds the store.
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub url: String,
}

impl std::fmt::Debug for PostgresStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStoreConfig")
            .field("url", &faucet_core::util::redact_uri_credentials(&self.url))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_empty_config_is_valid_and_keeps_metadata_on() {
        let cfg: FeedSourceConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg, FeedSourceConfig::new());
        assert!(cfg.include_metadata);
        assert_eq!(cfg.claim_ttl(), Duration::from_secs(DEFAULT_CLAIM_TTL_SECS));
        assert_eq!(cfg.standalone_claim_ttl(), Duration::from_secs(60));
        assert_eq!(
            FeedSourceConfig::new()
                .claim_ttl_secs(7)
                .standalone_claim_ttl(),
            Duration::from_secs(7)
        );
        cfg.validate().unwrap();
    }

    #[test]
    fn the_standalone_shape_round_trips() {
        let value = json!({
            "feed": "orders",
            "delivery": "warehouse",
            "store": {"type": "sqlite", "config": {"path": "./feeds.db"}},
            "page_events": 500,
            "include_metadata": false,
            "claim_ttl_secs": 60,
            "max_events": 100,
            "max_bytes": 4096
        });
        let cfg: FeedSourceConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            cfg,
            FeedSourceConfig::standalone(
                "orders",
                "warehouse",
                FeedStoreSpec::sqlite("./feeds.db")
            )
            .page_events(500)
            .include_metadata(false)
            .claim_ttl_secs(60)
            .max_events(100)
            .max_bytes(4096)
        );
        assert_eq!(serde_json::to_value(&cfg).unwrap(), value);
        cfg.validate().unwrap();
    }

    #[test]
    fn unknown_keys_are_refused() {
        let err = serde_json::from_value::<FeedSourceConfig>(json!({"feeds": "x"})).unwrap_err();
        assert!(err.to_string().contains("feeds"), "{err}");
        let err = serde_json::from_value::<FeedSourceConfig>(
            json!({"store": {"type": "sqlite", "config": {"file": "x"}}}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("file"), "{err}");
    }

    #[test]
    fn zero_and_out_of_range_values_are_refused() {
        for (cfg, name) in [
            (FeedSourceConfig::new().page_events(0), "page_events"),
            (FeedSourceConfig::new().claim_ttl_secs(0), "claim_ttl_secs"),
            (FeedSourceConfig::new().max_events(0), "max_events"),
            (FeedSourceConfig::new().max_bytes(0), "max_bytes"),
            (
                FeedSourceConfig::new().claim_ttl_secs(MAX_CLAIM_TTL_SECS + 1),
                "claim_ttl_secs",
            ),
            (
                FeedSourceConfig::standalone(" ", "d", FeedStoreSpec::sqlite("x")),
                "feed",
            ),
            (
                FeedSourceConfig::standalone("f", "", FeedStoreSpec::sqlite("x")),
                "delivery",
            ),
        ] {
            let err = cfg.validate().unwrap_err();
            assert!(matches!(err, FaucetError::Config(_)), "{err}");
            assert!(err.to_string().contains(name), "{err}");
        }
    }

    #[test]
    fn a_postgres_url_is_never_printed_with_its_password() {
        let spec = FeedStoreSpec::postgres("postgres://app:hunter2@db.internal/feeds");
        let shown = format!("{spec:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("db.internal"), "{shown}");
    }

    #[test]
    fn the_store_fields_carry_the_resource_annotation() {
        let schema = serde_json::to_value(schemars::schema_for!(FeedSourceConfig)).unwrap();
        let text = schema.to_string();
        assert!(text.contains(r#""x-faucet-resource":"path""#), "{text}");
        assert!(text.contains(r#""x-faucet-resource":"url""#), "{text}");
        assert!(schema.get("required").is_none(), "every field is optional");
    }
}
