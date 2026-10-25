//! Prometheus surface for notifications (#280).
//!
//! - `faucet_notifications_sent_total{channel,event,outcome}` — deliveries
//!   attempted; `outcome` ∈ `ok` | `error`.
//! - `faucet_notifications_dropped_total{channel,reason}` — events not
//!   delivered; `reason` ∈ `coalesced` | `channel_error`.
//! - `faucet_notification_dispatch_duration_seconds{channel}` — per-delivery
//!   latency.
//!
//! Follows the CLI-side convention (`faucet_schedule_*`, `faucet_serve_*`,
//! `faucet_pipeline_sla_*`): plain `metrics` macros, low-cardinality labels
//! only (never pipeline/row/record values as labels — those stay in logs).

use metrics::{counter, describe_counter, describe_histogram, histogram};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_notifications_sent_total",
            "Notification deliveries attempted, by channel/event/outcome"
        );
        describe_counter!(
            "faucet_notifications_dropped_total",
            "Notifications dropped before/at delivery, by channel/reason"
        );
        describe_histogram!(
            "faucet_notification_dispatch_duration_seconds",
            "Per-delivery notification dispatch latency in seconds"
        );
    });
}

/// Record a delivery attempt outcome.
pub fn record_sent(channel: &'static str, event: &'static str, ok: bool) {
    describe();
    counter!(
        "faucet_notifications_sent_total",
        "channel" => channel,
        "event" => event,
        "outcome" => if ok { "ok" } else { "error" },
    )
    .increment(1);
}

/// Record a dropped notification.
pub fn record_dropped(channel: &'static str, reason: &'static str) {
    describe();
    counter!(
        "faucet_notifications_dropped_total",
        "channel" => channel,
        "reason" => reason,
    )
    .increment(1);
}

/// Record per-delivery dispatch latency.
pub fn record_duration(channel: &'static str, secs: f64) {
    describe();
    histogram!("faucet_notification_dispatch_duration_seconds", "channel" => channel).record(secs);
}

#[cfg(test)]
mod tests {
    // Emit helpers must be callable without an installed recorder (the `metrics`
    // macros no-op) — a panic here would take down every run in a build without
    // observability installed.
    #[test]
    fn emitting_without_recorder_is_a_noop() {
        super::record_sent("slack", "run_failure", true);
        super::record_dropped("slack", "coalesced");
        super::record_duration("slack", 0.01);
    }
}
