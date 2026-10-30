use super::*;

#[test]
fn synth_log_export_failed_carries_the_counts() {
    let e = synth_event("log_export_failed", "p").unwrap();
    assert_eq!(e.kind.as_str(), "log_export_failed");
    assert_eq!(e.details["pending_lines"], 1204);
    assert_eq!(e.details["run_id"], "run-synthetic");
    assert!(e.message.contains("1204 line(s) still buffered"));
}

#[test]
fn synth_connection_needs_reauth_names_the_connection() {
    let e = synth_event("connection_needs_reauth", "acme").unwrap();
    assert_eq!(e.kind.as_str(), "connection_needs_reauth");
    assert_eq!(e.details["tenant"], "acme");
    assert_eq!(e.details["connection"], "synthetic-connection");
    assert!(e.run.is_none());
}

#[test]
fn synth_event_maps_known_kinds() {
    use crate::notify::EventKind;
    assert_eq!(
        synth_event("run_failure", "p").unwrap().kind,
        EventKind::RunFailure
    );
    assert_eq!(
        synth_event("scheduler_stuck", "p").unwrap().kind,
        EventKind::SchedulerStuck
    );
    let drift = synth_event("profile_drift", "p").unwrap();
    assert_eq!(drift.kind, EventKind::ProfileDrift);
    assert_eq!(drift.details["column"], "amount");
    assert!(drift.title.contains("amount.null_rate"), "{}", drift.title);
    // DLQ synthetic count clears any reasonable threshold.
    assert_eq!(
        synth_event("dlq_threshold", "p")
            .unwrap()
            .details
            .get("records_dlq")
            .and_then(|v| v.as_u64()),
        Some(1_000_000)
    );
}

#[test]
fn synthetic_events_never_share_a_real_runs_incident_key() {
    let e = synth_event("run_failure", "orders").unwrap();
    assert_eq!(e.incident_key(), "orders:faucet-notify-test");
    assert_ne!(
        e.incident_key(),
        NotifyEvent::run_failure("orders", "", "s", "m").incident_key()
    );
}

#[test]
fn the_verdict_fails_on_no_match_or_a_failed_delivery() {
    let o = |action: &'static str, error: Option<&str>| DeliveryOutcome {
        rule: "r".into(),
        channel: "slack",
        action,
        error: error.map(str::to_string),
    };
    assert!(test_verdict("run_failure", &[o("trigger", None)]).is_ok());
    let none = test_verdict("run_failure", &[]).unwrap_err().to_string();
    assert!(none.contains("no notification rule"), "{none}");
    assert!(test_verdict("run_failure", &[o("coalesced", None)]).is_err());
    let failed = test_verdict("x", &[o("trigger", None), o("resolve", Some("boom"))])
        .unwrap_err()
        .to_string();
    assert!(
        failed.contains("delivery failed for rule(s): r"),
        "{failed}"
    );
}

#[tokio::test]
async fn test_command_exits_non_zero_when_delivery_fails() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("faucet.yaml");
    std::fs::write(
            &cfg,
            "version: 1\nname: n\nnotifications:\n  - name: s\n    channel:\n      type: slack\n      config:\n        webhook_url: http://127.0.0.1:1/hook\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: out.jsonl\n",
        )
        .unwrap();
    let args = |event: &str| NotifyTestArgs {
        config: Some(cfg.clone()),
        event: event.into(),
        keep_open: false,
        profile: None,
        env_file: None,
        no_env_file: true,
    };
    let err = test(args("run_failure")).await.unwrap_err().to_string();
    assert!(err.contains("delivery failed"), "{err}");
}

#[test]
fn synth_event_rejects_unknown_kind() {
    assert!(synth_event("nope", "p").is_err());
}

#[test]
fn synth_events_carry_run_identity_except_scheduler_stuck() {
    // #480: `faucet notify test` must exercise the same payload shape a real
    // run sends, so a receiver can be validated against populated fields.
    for kind in [
        "run_failure",
        "run_success",
        "sla_breach",
        "circuit_open",
        "contract_abort",
        "dlq_threshold",
        "profile_drift",
        "budget_exceeded",
    ] {
        let e = synth_event(kind, "p").unwrap();
        let run = e
            .run
            .as_ref()
            .unwrap_or_else(|| panic!("{kind} needs a run context"));
        assert!(run.run_id.is_some(), "{kind} run_id");
        assert!(run.invocation_id.is_some(), "{kind} invocation_id");
        assert_ne!(run.run_id, run.invocation_id, "{kind} ids must differ");
        assert!(run.finished_at.is_some(), "{kind} finished_at");
        assert!(run.duration.is_some(), "{kind} duration");
    }
    // No owning invocation in production either — faithful null shape.
    assert!(synth_event("scheduler_stuck", "p").unwrap().run.is_none());
}

#[test]
fn synth_change_requested_names_the_synthetic_request() {
    let e = synth_event("change_requested", "p").unwrap();
    assert_eq!(e.kind, crate::notify::EventKind::ChangeRequested);
    assert!(e.message.contains("synthetic change request"));
}
