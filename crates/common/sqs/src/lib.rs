#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-sqs
//!
//! Shared configuration types for the faucet-stream AWS SQS source
//! (`faucet-source-sqs`) and sink (`faucet-sink-sqs`) connectors: the
//! [`SqsCredentials`] auth enum and the [`build_client`] helper that assembles
//! an `aws_sdk_sqs::Client` from region / endpoint / credential settings. Both
//! connector crates re-export these so end-user imports do not change.

mod auth;

pub use auth::{SqsCredentials, build_client};

/// Whether `queue_url` names a FIFO queue (SQS FIFO queue names end in
/// `.fifo`). FIFO queues need per-group ordering care on both sides.
pub fn is_fifo(queue_url: &str) -> bool {
    queue_url.trim_end_matches('/').ends_with(".fifo")
}

#[cfg(test)]
mod tests {
    #[test]
    fn fifo_queues_are_recognised_by_their_suffix() {
        assert!(super::is_fifo(
            "https://sqs.us-east-1.amazonaws.com/1/orders.fifo"
        ));
        assert!(super::is_fifo("https://h/1/orders.fifo/"));
        assert!(!super::is_fifo("https://h/1/orders"));
    }
}
