//! [`BodyResume`]: the bounded, counted resume policy every pipeline
//! `read_object` installs.
//!
//! The storage client re-requests a body that drops mid-stream from the last
//! byte it received, pinned to the generation of the first response. Its
//! default policy does so without an attempt limit and without telling
//! anyone, so a body that keeps failing would loop until a run deadline that
//! may not exist. This policy keeps the client's own rule for which errors
//! are worth a resume, stops after a fixed number of resumes, counts each one
//! as a source retry and a `get` round trip, and logs it.

use faucet_core::RetryClass;
use faucet_core::observability::RoundtripRecorder;
use google_cloud_storage::Error;
use google_cloud_storage::read_resume_policy::{
    ReadResumePolicy, Recommended, ResumeQuery, ResumeResult,
};
use std::sync::Arc;

/// Resumes of one object body before the read fails, when a connector does
/// not configure its own.
pub const DEFAULT_BODY_RESUME_ATTEMPTS: u32 = 3;

/// A read-resume policy that resumes a dropped body at most `attempts` times.
///
/// Install it per request with `with_read_resume_policy`. `attempts: 0` never
/// resumes: the first body error fails the read.
#[derive(Debug)]
pub struct BodyResume {
    attempts: u32,
    bucket: String,
    key: String,
    recorder: Option<Arc<RoundtripRecorder>>,
}

impl BodyResume {
    /// A policy for one read of `key` in `bucket`.
    pub fn new(attempts: u32, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            attempts,
            bucket: bucket.into(),
            key: key.into(),
            recorder: None,
        }
    }

    /// Count each resume on `recorder` (a retry of class `connection` and a
    /// `get` round trip).
    #[must_use]
    pub fn with_recorder(mut self, recorder: Option<Arc<RoundtripRecorder>>) -> Self {
        self.recorder = recorder;
        self
    }
}

impl ReadResumePolicy for BodyResume {
    fn on_error(&self, status: &ResumeQuery, error: Error) -> ResumeResult {
        match Recommended.on_error(status, error) {
            ResumeResult::Continue(e) if status.attempt_count > self.attempts => {
                ResumeResult::Exhausted(e)
            }
            ResumeResult::Continue(e) => {
                if let Some(recorder) = &self.recorder {
                    recorder.retry(RetryClass::Connection);
                    recorder.record("get");
                }
                tracing::warn!(
                    bucket = %self.bucket,
                    key = %self.key,
                    attempt = status.attempt_count,
                    max_attempts = self.attempts,
                    error = %faucet_core::redact::redact(&e.to_string()),
                    "GCS object body dropped mid-read; resuming from the last byte received"
                );
                ResumeResult::Continue(e)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::UsageMeter;
    use faucet_core::observability::RoundtripSide;

    fn dropped() -> Error {
        Error::io("connection reset")
    }

    fn metered() -> (Arc<UsageMeter>, Option<Arc<RoundtripRecorder>>) {
        let meter = Arc::new(UsageMeter::new());
        let recorder = RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "gcs")
            .with_meter(meter.clone());
        (meter, Some(Arc::new(recorder)))
    }

    #[test]
    fn resumes_up_to_the_limit_then_is_exhausted() {
        let policy = BodyResume::new(2, "b", "k");
        for attempt in 1..=2 {
            let r = policy.on_error(&ResumeQuery::new(attempt), dropped());
            assert!(matches!(r, ResumeResult::Continue(_)), "{attempt}: {r:?}");
        }
        let r = policy.on_error(&ResumeQuery::new(3), dropped());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");
    }

    #[test]
    fn zero_never_resumes() {
        let (meter, recorder) = metered();
        let policy = BodyResume::new(0, "b", "k").with_recorder(recorder);
        let r = policy.on_error(&ResumeQuery::new(1), dropped());
        assert!(matches!(r, ResumeResult::Exhausted(_)), "{r:?}");
        let usage = meter.snapshot();
        assert!(usage.source_retries.is_empty(), "{usage:?}");
    }

    #[test]
    fn an_error_the_client_does_not_resume_stays_permanent() {
        let (meter, recorder) = metered();
        let policy = BodyResume::new(3, "b", "k").with_recorder(recorder);
        let r = policy.on_error(&ResumeQuery::new(1), Error::deser("checksum mismatch"));
        assert!(matches!(r, ResumeResult::Permanent(_)), "{r:?}");
        assert!(meter.snapshot().source_retries.is_empty());
    }

    #[test]
    fn each_resume_is_a_connection_retry_and_a_get_round_trip() {
        let (meter, recorder) = metered();
        let policy = BodyResume::new(2, "b", "k").with_recorder(recorder);
        for attempt in 1..=3 {
            let _ = policy.on_error(&ResumeQuery::new(attempt), dropped());
        }
        let usage = meter.snapshot();
        assert_eq!(
            usage.source_retries.get("connection"),
            Some(&2),
            "{usage:?}"
        );
        let value = serde_json::to_value(&usage).unwrap();
        assert!(value.to_string().contains("\"get\":2"), "{value}");
    }

    #[test]
    fn default_is_three() {
        assert_eq!(DEFAULT_BODY_RESUME_ATTEMPTS, 3);
    }
}
