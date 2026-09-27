//! Configuration for the NATS sink.

use faucet_common_nats::NatsConnectionConfig;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for [`NatsSink`](crate::NatsSink).
///
/// The [`NatsConnectionConfig`] surface (`servers` / `auth` / `tls` / `name`)
/// is flattened in.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NatsSinkConfig {
    /// Connection settings (servers, auth, tls, name).
    #[serde(flatten)]
    pub connection: NatsConnectionConfig,

    /// Default subject every record is published to.
    pub subject: String,

    /// Optional top-level record field whose (string) value overrides
    /// [`subject`](Self::subject) on a per-record basis — the subject-per-record
    /// pattern. When the field is missing or is not a string the sink errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_field: Option<String>,

    /// Records per publish batch. Records are published one NATS message each;
    /// after every `batch_size` records the client is flushed so nothing is
    /// lost. Defaults to [`DEFAULT_BATCH_SIZE`]. `0` publishes the whole batch
    /// handed by the pipeline, flushing once at the end.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl NatsSinkConfig {
    /// Convenience constructor for a fixed-subject sink.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            connection: NatsConnectionConfig::default(),
            subject: subject.into(),
            subject_field: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Validate the config at construction time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        if self.subject.trim().is_empty() {
            return Err(FaucetError::Config(
                "nats sink: `subject` must not be empty".into(),
            ));
        }
        if let Some(field) = &self.subject_field
            && field.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "nats sink: `subject_field` must not be empty when set".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

impl NatsSinkConfig {
    /// What a failed batch write leaves behind (#737): each record is its own publish.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        faucet_core::BatchAtomicity::BestEffort
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_accepts_minimal() {
        assert!(NatsSinkConfig::new("events.out").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_subject() {
        let mut c = NatsSinkConfig::new("x");
        c.subject = " ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_subject_field() {
        let mut c = NatsSinkConfig::new("x");
        c.subject_field = Some("".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_batch_size_over_max() {
        let mut c = NatsSinkConfig::new("x");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn deserialize_flattened() {
        let c: NatsSinkConfig = serde_json::from_value(json!({
            "servers": ["nats://a:4222"],
            "subject": "events.out",
            "subject_field": "topic"
        }))
        .unwrap();
        assert_eq!(c.subject, "events.out");
        assert_eq!(c.subject_field.as_deref(), Some("topic"));
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(NatsSinkConfig);
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: NatsSinkConfig = serde_json::from_value(
            serde_json::json!({"servers": ["nats://a:4222"], "subject": "events.out"}),
        )
        .unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
