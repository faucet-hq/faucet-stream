
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
