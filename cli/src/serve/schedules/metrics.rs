//! `faucet_serve_schedule_*` metrics (#1044). Labels are the schedule name,
//! a closed `outcome` set and, for a tenant fan-out, the tenant id.

use metrics::{counter, describe_counter, describe_gauge, gauge};

/// The closed set of tick outcomes.
pub const OUTCOMES: &[&str] = &["started", "skipped", "failed", "retried"];

pub fn describe() {
    describe_gauge!(
        "faucet_serve_schedules_active",
        "Schedules this server ticks (not paused)"
    );
    describe_gauge!(
        "faucet_serve_schedule_healthy",
        "1 if the schedule's last tick started its runs, else 0"
    );
    describe_gauge!(
        "faucet_serve_schedule_last_tick_unix_seconds",
        "Unix time of the schedule's last tick that started its runs"
    );
    describe_counter!(
        "faucet_serve_schedule_ticks_total",
        "Schedule ticks by outcome: started, skipped (overlap), failed (refused), retried"
    );
    describe_counter!(
        "faucet_serve_schedule_missed_ticks_total",
        "Schedule ticks that never ran (no instance was up, or collapsed into one catch-up)"
    );
    describe_counter!(
        "faucet_serve_schedule_tenant_failures_total",
        "Fan-out ticks refused for one tenant while the others ran"
    );
}

/// Pre-emit a schedule's series at zero so they exist from startup.
pub fn preinit(schedule: &str) {
    for outcome in OUTCOMES {
        counter!("faucet_serve_schedule_ticks_total", "schedule" => schedule.to_string(), "outcome" => *outcome)
            .increment(0);
    }
    counter!("faucet_serve_schedule_missed_ticks_total", "schedule" => schedule.to_string())
        .increment(0);
    healthy(schedule, true);
}

pub fn active(n: usize) {
    gauge!("faucet_serve_schedules_active").set(n as f64);
}

pub fn tick(schedule: &str, outcome: &'static str) {
    counter!("faucet_serve_schedule_ticks_total", "schedule" => schedule.to_string(), "outcome" => outcome)
        .increment(1);
}

pub fn missed(schedule: &str, n: u64) {
    counter!("faucet_serve_schedule_missed_ticks_total", "schedule" => schedule.to_string())
        .increment(n);
}

pub fn tenant_failed(schedule: &str, tenant: &str) {
    counter!(
        "faucet_serve_schedule_tenant_failures_total",
        "schedule" => schedule.to_string(),
        "tenant" => tenant.to_string()
    )
    .increment(1);
}

pub fn healthy(schedule: &str, ok: bool) {
    gauge!("faucet_serve_schedule_healthy", "schedule" => schedule.to_string()).set(if ok {
        1.0
    } else {
        0.0
    });
}

pub fn last_tick(schedule: &str, unix_secs: i64) {
    gauge!("faucet_serve_schedule_last_tick_unix_seconds", "schedule" => schedule.to_string())
        .set(unix_secs as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;

    #[test]
    fn every_series_is_emitted_with_its_labels() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            describe();
            preinit("nightly");
            active(2);
            tick("nightly", "started");
            missed("nightly", 3);
            tenant_failed("nightly", "acme");
            healthy("nightly", false);
            last_tick("nightly", 1_700_000_000);
        });
        let names: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(k, _, _, _)| k.key().name().to_string())
            .collect();
        for want in [
            "faucet_serve_schedules_active",
            "faucet_serve_schedule_healthy",
            "faucet_serve_schedule_last_tick_unix_seconds",
            "faucet_serve_schedule_ticks_total",
            "faucet_serve_schedule_missed_ticks_total",
            "faucet_serve_schedule_tenant_failures_total",
        ] {
            assert!(names.iter().any(|n| n == want), "missing {want}: {names:?}");
        }
    }
}
