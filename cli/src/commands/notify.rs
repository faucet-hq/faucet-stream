//! `faucet notify test` — fire one synthetic event through a config's
//! `notifications:` rules to validate channel setup end-to-end (no pipeline
//! runs). Uses the real delivery path, so a Slack/PagerDuty/webhook that is
//! reachable will actually receive the test message.

use crate::cli::{NotifyArgs, NotifyCommand, NotifyTestArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::notify::{Notifier, NotifyEvent};

/// Execute the `notify` subcommand.
pub async fn run(args: NotifyArgs) -> CliResult<()> {
    match args.command {
        NotifyCommand::Test(a) => test(a).await,
    }
}

async fn test(args: NotifyTestArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    // Real load (resolves secrets) so channel credentials are live for delivery.
    let cfg = PipelineConfig::from_path_async(&path, None).await?;
    if cfg.notifications.is_empty() {
        return Err(CliError::Config(
            "no `notifications:` block in this config — add one, or run \
             `faucet schema notifications` to see the block's JSON Schema"
                .to_string(),
        ));
    }

    let notifier = Notifier::from_specs(&cfg.notifications)?
        .expect("a non-empty notifications list yields Some(notifier)");
    let pipeline = cfg
        .name
        .clone()
        .unwrap_or_else(|| "faucet-notify-test".to_string());
    let event = synth_event(&args.event, &pipeline)?;

    println!(
        "Firing synthetic `{}` event through {} notification rule(s)…",
        args.event,
        cfg.notifications.len()
    );
    notifier.emit(event).await;
    println!("Done — check your channels. Any delivery failure was logged above.");
    Ok(())
}

/// Build a synthetic event for the requested kind. DLQ uses a large count so it
/// clears any configured `dlq_threshold`.
///
/// Every kind except `scheduler_stuck` carries a synthetic [`RunContext`], so a
/// receiver being tested sees the same populated `run_id` / `invocation_id` /
/// timing fields a real run would send (#480). `scheduler_stuck` deliberately
/// does not — it has no owning invocation in production either, so leaving it
/// null is the faithful shape.
fn synth_event(kind: &str, pipeline: &str) -> CliResult<NotifyEvent> {
    let run = || {
        crate::notify::RunContext::start(
            Some(format!("test-run-{}", uuid::Uuid::now_v7())),
            Some(format!("test-invocation-{}", uuid::Uuid::now_v7())),
        )
        .finish(std::time::Instant::now())
    };
    Ok(match kind {
        "run_failure" => NotifyEvent::run_failure(
            pipeline,
            "",
            "test",
            "synthetic test failure from `faucet notify test`",
        )
        .with_run(run()),
        "run_success" => NotifyEvent::run_success(pipeline, "", 0).with_run(run()),
        "sla_breach" => NotifyEvent::sla_breach(pipeline, "", "staleness", "synthetic SLA breach")
            .with_run(run()),
        "circuit_open" => NotifyEvent::circuit_open(pipeline, "", 5, 30).with_run(run()),
        "contract_abort" => {
            NotifyEvent::contract_abort(pipeline, "", "synthetic contract breach").with_run(run())
        }
        "dlq_threshold" => NotifyEvent::dlq_threshold(pipeline, "", 1_000_000).with_run(run()),
        "scheduler_stuck" => NotifyEvent::scheduler_stuck(pipeline, "synthetic scheduler-stuck"),
        "profile_drift" => NotifyEvent::profile_drift(
            pipeline,
            "",
            "amount",
            "null_rate",
            "synthetic profile drift: null_rate 0.4 vs baseline mean 0",
        )
        .with_run(run()),
        "change_requested" => NotifyEvent::change_requested(
            pipeline,
            "change-synthetic",
            "run",
            "synthetic-requester",
            "synthetic change request",
        ),
        "budget_exceeded" => {
            NotifyEvent::budget_exceeded(pipeline, "", "max_records", 1_000_000, 1_000_500)
                .with_run(run())
        }
        other => {
            return Err(CliError::Config(format!(
                "unknown --event `{other}` (expected one of: run_failure, run_success, \
                 sla_breach, circuit_open, contract_abort, dlq_threshold, scheduler_stuck, \
                 profile_drift, change_requested, budget_exceeded)"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
