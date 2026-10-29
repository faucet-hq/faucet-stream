//! `faucet notify test` — fire one synthetic event through a config's
//! `notifications:` rules to validate channel setup end-to-end (no pipeline
//! runs). Uses the real delivery path, so a Slack/PagerDuty/webhook that is
//! reachable will actually receive the test message.

use crate::cli::{NotifyArgs, NotifyCommand, NotifyTestArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::notify::{DeliveryOutcome, Notifier, NotifyEvent};

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
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
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
    let mut outcomes = notifier.emit_report(event.clone()).await;
    if event.opens_incident() && !args.keep_open {
        // Close the incident the test opened, so it neither stays open on
        // call nor swallows a real failure.
        outcomes.extend(notifier.resolve(&event).await);
    }
    for o in &outcomes {
        match &o.error {
            None => println!("  {} ({}): {} ok", o.rule, o.channel, o.action),
            Some(e) => println!("  {} ({}): {} FAILED — {e}", o.rule, o.channel, o.action),
        }
    }
    test_verdict(&args.event, &outcomes)
}

/// Fail when no rule took the event or any delivery failed.
fn test_verdict(kind: &str, outcomes: &[DeliveryOutcome]) -> CliResult<()> {
    if !outcomes.iter().any(|o| o.action == "trigger") {
        return Err(CliError::Config(format!(
            "no notification rule delivered a `{kind}` event — check the rules' `on:`, \
             `min_severity` and `dlq_threshold`"
        )));
    }
    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| o.error.is_some())
        .map(|o| o.rule.as_str())
        .collect();
    if !failed.is_empty() {
        return Err(CliError::Config(format!(
            "notification delivery failed for rule(s): {}",
            failed.join(", ")
        )));
    }
    println!("Done — every matching rule delivered.");
    Ok(())
}

/// Build a synthetic event for the requested kind. DLQ uses a large count so it
/// clears any configured `dlq_threshold`.
///
/// Every kind except `scheduler_stuck` carries a synthetic [`RunContext`](crate::notify::RunContext), so a
/// receiver being tested sees the same populated `run_id` / `invocation_id` /
/// timing fields a real run would send (#480). `scheduler_stuck` deliberately
/// does not — it has no owning invocation in production either, so leaving it
/// null is the faithful shape.
/// The row every synthetic event carries, so its incident / dedup key
/// (`{pipeline}:faucet-notify-test`) never matches a real run's.
pub const TEST_ROW: &str = "faucet-notify-test";

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
            TEST_ROW,
            "test",
            "synthetic test failure from `faucet notify test`",
        )
        .with_run(run()),
        "run_success" => NotifyEvent::run_success(pipeline, TEST_ROW, 0).with_run(run()),
        "sla_breach" => {
            NotifyEvent::sla_breach(pipeline, TEST_ROW, "staleness", "synthetic SLA breach")
                .with_run(run())
        }
        "circuit_open" => NotifyEvent::circuit_open(pipeline, TEST_ROW, 5, 30).with_run(run()),
        "contract_abort" => {
            NotifyEvent::contract_abort(pipeline, TEST_ROW, "synthetic contract breach")
                .with_run(run())
        }
        "dlq_threshold" => {
            NotifyEvent::dlq_threshold(pipeline, TEST_ROW, 1_000_000).with_run(run())
        }
        "scheduler_stuck" => NotifyEvent::scheduler_stuck(pipeline, "synthetic scheduler-stuck"),
        "profile_drift" => NotifyEvent::profile_drift(
            pipeline,
            TEST_ROW,
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
            NotifyEvent::budget_exceeded(pipeline, TEST_ROW, "max_records", 1_000_000, 1_000_500)
                .with_run(run())
        }
        "connection_needs_reauth" => NotifyEvent::connection_needs_reauth(
            pipeline,
            "synthetic-connection",
            "the provider rejected the refresh token (HTTP 400 invalid_grant)",
        ),
        "log_export_failed" => NotifyEvent::log_export_failed(
            pipeline,
            TEST_ROW,
            "run-synthetic",
            1204,
            0,
            "synthetic: OTLP log export to the collector failed",
        )
        .with_run(run()),
        other => {
            return Err(CliError::Config(format!(
                "unknown --event `{other}` (expected one of: run_failure, run_success, \
                 sla_breach, circuit_open, contract_abort, dlq_threshold, scheduler_stuck, \
                 profile_drift, change_requested, budget_exceeded, \
                 connection_needs_reauth, log_export_failed)"
            )));
        }
    })
}

#[cfg(test)]
mod tests;
