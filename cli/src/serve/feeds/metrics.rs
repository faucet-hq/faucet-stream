//! `faucet_feed_*` metrics the scheduler emits (#1034). The `feed` label is
//! `<tenant>/<name>` for a tenant feed, else the feed's name; `delivery` is
//! the delivery's name; `outcome` is a closed set.

use metrics::{counter, describe_counter, describe_gauge, gauge};

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
    describe_gauge!(
        "faucet_feed_held",
        "Deliveries of the feed that are held until an admin recovers them"
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
        forget_feed("acme/orders", &["lake".to_string()]);
        assert_eq!(OUTCOMES.len(), 7);
    }
}
