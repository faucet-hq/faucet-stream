use super::*;

#[test]
fn constructors_fix_severity_and_kind() {
    assert_eq!(
        NotifyEvent::run_failure("p", "", "sink", "boom").severity,
        Severity::Error
    );
    assert_eq!(
        NotifyEvent::circuit_open("p", "", 5, 30).severity,
        Severity::Critical
    );
    assert_eq!(
        NotifyEvent::run_success("p", "", 10).severity,
        Severity::Info
    );
    assert_eq!(
        NotifyEvent::sla_breach("p", "", "staleness", "old").severity,
        Severity::Warning
    );
    assert_eq!(
        NotifyEvent::scheduler_stuck("p", "no beat").kind,
        EventKind::SchedulerStuck
    );
}

#[test]
fn incident_and_dedupe_keys() {
    let f = NotifyEvent::run_failure("p", "r1", "sink", "boom");
    assert_eq!(f.incident_key(), "p:r1");
    assert_eq!(f.dedupe_key(), "run_failure:p:r1");
    assert!(f.opens_incident());
    assert!(!f.closes_incident());

    let s = NotifyEvent::run_success("p", "r1", 3);
    assert_eq!(s.incident_key(), "p:r1"); // matches the failure it resolves
    assert!(s.closes_incident());
    assert!(!s.opens_incident());
}

#[test]
fn details_carry_structured_context() {
    let e = NotifyEvent::dlq_threshold("p", "", 42);
    assert_eq!(e.details.get("records_dlq").unwrap(), &Value::from(42u64));
}

/// #456 H5: a notification is delivered to Slack / PagerDuty / a customer
/// webhook, i.e. outside the trust boundary, so a resolved secret that landed
/// in the error text must not ride along.
#[test]
fn secrets_are_scrubbed_from_every_outbound_field() {
    // A realistic leak: the API key is a query parameter, and reqwest's error
    // Display embeds the whole URL.
    let secret = "sk-live-456-audit-secret";
    crate::secrets::registry::register(secret);

    let ev = NotifyEvent::run_failure(
        "p",
        "row",
        "http",
        format!("HTTP error for url (https://api.example.com/v1?api_key={secret})"),
    );
    assert!(!ev.message.contains(secret), "the message kept the secret");
    assert!(ev.message.contains("***"), "the message was not masked");

    // Titles and string details go into the same payload.
    let ev = NotifyEvent::sla_breach("p", "row", "staleness", format!("token {secret} stale"));
    assert!(!ev.message.contains(secret));

    let ev = NotifyEvent::run_failure("p", "row", "cfg", "boom")
        .with("detail", Value::String(format!("url={secret}")));
    assert!(
        !ev.details["detail"].as_str().unwrap().contains(secret),
        "the detail kept the secret"
    );
    // Non-string details pass through untouched.
    let ev = NotifyEvent::run_success("p", "row", 7);
    assert_eq!(ev.details["records_written"], Value::from(7u64));
}

#[test]
fn change_requested_names_the_request_with_or_without_a_reason() {
    let with = NotifyEvent::change_requested("p", "c1", "run", "bob", "nightly load");
    assert_eq!(with.kind, EventKind::ChangeRequested);
    assert!(with.message.contains("nightly load"));
    let without = NotifyEvent::change_requested("p", "c1", "run", "bob", "");
    assert!(without.message.contains("/v1/changes/c1/approve"));
    assert_eq!(EventKind::ChangeRequested.as_str(), "change_requested");
}
