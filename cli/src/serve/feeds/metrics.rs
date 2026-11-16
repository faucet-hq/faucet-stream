//! `faucet_feed_*` metrics the scheduler emits (#1034). The `feed` label is
//! `<tenant>/<name>` for a tenant feed, else the feed's name; `delivery` is
//! the delivery's name; `outcome` is a closed set.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// The closed set of delivery-run outcomes.
pub const OUTCOMES: &[&str] = &[
    "delivered",
    "failed",
    "refused",
    "cancelled",
    "claim_lost",
    "held",
    "needs_approval",
];

pub fn describe() {
    describe_gauge!(
        "faucet_feed_depth",
        "Events in the feed that its slowest delivery has not delivered yet"
    );
    describe_gauge!(
        "faucet_feed_delivery_lag",
        "Events in the feed that this delivery has not delivered yet"
    );
    describe_gauge!(
        "faucet_feed_oldest_undelivered_seconds",
        "Age of the oldest event this delivery has not delivered yet"
    );
    describe_counter!(
        "faucet_feed_runs_total",
        "Delivery runs by outcome: delivered, failed, refused (a submit that could not start), \
         cancelled, claim_lost, held, needs_approval"
    );
    describe_gauge!(
        "faucet_feed_delivery_stalled",
        "1 while the delivery is stalled on one event, else 0"
    );
    describe_gauge!(
        "faucet_feed_undelivered_ratio",
        "Undelivered bytes or age of the feed as a fraction of its undelivered limits (the larger)"
    );
    describe_counter!(
        "faucet_feed_trimmed_events_total",
        "Events removed by retention"
    );
    describe_counter!(
        "faucet_feed_gaps_total",
        "Ranges of events a delivery skipped (an admin skip or a recover)"
    );
    describe_counter!(
        "faucet_feed_orphan_deliveries_total",
        "Delivery positions deleted from the feed store because no definition listed them"
    );
    describe_gauge!(
        "faucet_feed_held",
        "Deliveries of the feed that are held until an admin recovers them"
    );
    describe_counter!(
        "faucet_feed_ingest_requests_total",
        "Ingest requests by HTTP status (`feed` is empty for a request no feed admitted)"
    );
    describe_counter!(
        "faucet_feed_ingest_refused_total",
        "Refused ingest requests by reason: unauthorized, malformed, too_large, content_type, \
         content_encoding, rate_limited, tenant_rate_limited, full, store, seal, busy, timeout, \
         draining, definitions_stale"
    );
    describe_counter!("faucet_feed_events_total", "Events stored");
    describe_counter!("faucet_feed_bytes_total", "Payload bytes stored");
    describe_histogram!(
        "faucet_feed_append_duration_seconds",
        "How long one store transaction of ingested events took"
    );
    describe_histogram!(
        "faucet_feed_append_group_requests",
        "Ingest requests committed in one store transaction"
    );
    describe_counter!(
        "faucet_feed_store_errors_total",
        "Ingest requests that failed because the feed's store was unavailable"
    );
    describe_counter!(
        "faucet_feed_dedupe_hits_total",
        "Events not stored again because their dedupe key was already seen"
    );
    describe_counter!(
        "faucet_feed_scheduler_restarts_total",
        "Times the feed scheduler loop panicked and was restarted"
    );
}

pub fn depth(feed: &str, events: u64) {
    gauge!("faucet_feed_depth", "feed" => feed.to_string()).set(events as f64);
}

pub fn lag(feed: &str, delivery: &str, events: u64) {
    gauge!("faucet_feed_delivery_lag", "feed" => feed.to_string(), "delivery" => delivery.to_string())
        .set(events as f64);
}

pub fn oldest_undelivered(feed: &str, delivery: &str, secs: f64) {
    gauge!(
        "faucet_feed_oldest_undelivered_seconds",
        "feed" => feed.to_string(),
        "delivery" => delivery.to_string()
    )
    .set(secs);
}

pub fn run(feed: &str, delivery: &str, outcome: &'static str) {
    counter!(
        "faucet_feed_runs_total",
        "feed" => feed.to_string(),
        "delivery" => delivery.to_string(),
        "outcome" => outcome
    )
    .increment(1);
}

pub fn stalled(feed: &str, delivery: &str, stalled: bool) {
    gauge!(
        "faucet_feed_delivery_stalled",
        "feed" => feed.to_string(),
        "delivery" => delivery.to_string()
    )
    .set(if stalled { 1.0 } else { 0.0 });
}

pub fn undelivered_ratio(feed: &str, ratio: f64) {
    gauge!("faucet_feed_undelivered_ratio", "feed" => feed.to_string()).set(ratio);
}

pub fn trimmed(feed: &str, events: u64) {
    counter!("faucet_feed_trimmed_events_total", "feed" => feed.to_string()).increment(events);
}

pub fn gap(feed: &str) {
    counter!("faucet_feed_gaps_total", "feed" => feed.to_string()).increment(1);
}

pub fn held(feed: &str, deliveries: usize) {
    gauge!("faucet_feed_held", "feed" => feed.to_string()).set(deliveries as f64);
}

pub fn ingest_request(feed: &str, status: u16) {
    counter!(
        "faucet_feed_ingest_requests_total",
        "feed" => feed.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn ingest_refused(feed: &str, reason: &'static str) {
    counter!(
        "faucet_feed_ingest_refused_total",
        "feed" => feed.to_string(),
        "reason" => reason
    )
    .increment(1);
}

pub fn ingested(feed: &str, events: u64, bytes: u64) {
    counter!("faucet_feed_events_total", "feed" => feed.to_string()).increment(events);
    counter!("faucet_feed_bytes_total", "feed" => feed.to_string()).increment(bytes);
}

pub fn append_duration(feed: &str, took: std::time::Duration) {
    histogram!("faucet_feed_append_duration_seconds", "feed" => feed.to_string())
        .record(took.as_secs_f64());
}

pub fn append_group(feed: &str, requests: usize) {
    histogram!("faucet_feed_append_group_requests", "feed" => feed.to_string())
        .record(requests as f64);
}

pub fn store_error(feed: &str) {
    counter!("faucet_feed_store_errors_total", "feed" => feed.to_string()).increment(1);
}

pub fn dedupe_hits(feed: &str, events: u64) {
    counter!("faucet_feed_dedupe_hits_total", "feed" => feed.to_string()).increment(events);
}

pub fn orphan_delivery(feed: &str) {
    counter!("faucet_feed_orphan_deliveries_total", "feed" => feed.to_string()).increment(1);
}

pub fn scheduler_restarted() {
    counter!("faucet_feed_scheduler_restarts_total").increment(1);
}

/// A deleted delivery: its gauges go back to zero, so its alerts clear.
pub fn forget_delivery(feed: &str, delivery: &str) {
    lag(feed, delivery, 0);
    oldest_undelivered(feed, delivery, 0.0);
    stalled(feed, delivery, false);
}

/// A deleted feed: its gauges go back to zero, so its alerts clear.
pub fn forget_feed(feed: &str, deliveries: &[String]) {
    depth(feed, 0);
    undelivered_ratio(feed, 0.0);
    held(feed, 0);
    for d in deliveries {
        forget_delivery(feed, d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_series_can_be_described_emitted_and_forgotten() {
        describe();
        depth("acme/orders", 10);
        lag("acme/orders", "lake", 4);
        oldest_undelivered("acme/orders", "lake", 2.5);
        for outcome in OUTCOMES {
            run("acme/orders", "lake", outcome);
        }
        stalled("acme/orders", "lake", true);
        undelivered_ratio("acme/orders", 0.6);
        trimmed("acme/orders", 3);
        gap("acme/orders");
        held("acme/orders", 1);
        scheduler_restarted();
        ingest_request("acme/orders", 202);
        ingest_request("", 401);
        ingest_refused("", "unauthorized");
        ingested("acme/orders", 3, 120);
        append_duration("acme/orders", std::time::Duration::from_millis(4));
        append_group("acme/orders", 2);
        store_error("acme/orders");
        dedupe_hits("acme/orders", 1);
        forget_feed("acme/orders", &["lake".to_string()]);
        assert_eq!(OUTCOMES.len(), 7);
    }
}
