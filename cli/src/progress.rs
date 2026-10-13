//! Inline live progress for `faucet run` (#385, `cli-progress` feature).
//!
//! A lightweight alternative to the full-screen `--tui`: one [`indicatif`]
//! line per active matrix-row invocation showing records in/out, rows/sec,
//! pages, and elapsed time, updated a few times a second while the run
//! streams. Numbers come from the same in-process Prometheus recorder the TUI
//! samples ([`crate::livemetrics`]) — no new measurement plumbing on the hot
//! path.
//!
//! Rendered on **stderr** so a piped stdout stays clean for records; logs also
//! go to stderr, and indicatif suspends its lines while a log line prints so
//! the two don't interleave. The live line is only drawn on an interactive
//! terminal — a non-TTY stdout (CI, pipes) or `--quiet` disables it and the
//! run falls back to the periodic `tracing` progress logs. `--tui` takes
//! precedence when both are requested.

use crate::livemetrics::{RowStats, Sampler};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use metrics_exporter_prometheus::PrometheusHandle;
use std::collections::HashMap;
use std::io::IsTerminal;
use std::time::{Duration, Instant};

/// Should the inline progress line render? Requires an interactive terminal on
/// **both** stdout and stderr (progress draws to stderr; the piped-stdout
/// fallback is keyed on stdout per the issue), and neither `--quiet` nor
/// `--tui`. On any non-TTY or when suppressed the caller keeps the periodic
/// log output instead.
pub fn is_progress_session(quiet: bool, tui: bool) -> bool {
    !quiet && !tui && std::io::stdout().is_terminal() && std::io::stderr().is_terminal()
}

/// Max redraws per second — keeps the render throttled well under the hot path.
const RENDER_HZ: u8 = 10;
/// Sampling cadence (100ms → 10 Hz, matching `RENDER_HZ`).
const TICK: Duration = Duration::from_millis(100);

/// Drive the run future while rendering one inline progress line per active
/// row. Returns the run's result after the lines are finalized. The recorder
/// `handle` is the same one `livemetrics::setup_observability` installed.
pub async fn drive<T>(run: impl Future<Output = T>, pipeline: &str, handle: PrometheusHandle) -> T {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(RENDER_HZ));
    drive_with(run, pipeline, handle, multi, TICK).await
}

/// The render/sample loop behind [`drive`], generic over the [`MultiProgress`]
/// draw target and tick so it runs headless (hidden target) under test.
async fn drive_with<T>(
    run: impl Future<Output = T>,
    pipeline: &str,
    handle: PrometheusHandle,
    multi: MultiProgress,
    tick: Duration,
) -> T {
    let mut bars: HashMap<String, ProgressBar> = HashMap::new();
    let mut sampler = Sampler::new(pipeline);
    let started = Instant::now();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tokio::pin!(run);
    let result = loop {
        tokio::select! {
            biased;
            result = &mut run => break result,
            _ = interval.tick() => {
                handle.run_upkeep();
                let elapsed = started.elapsed();
                let model = sampler.observe(&handle.render(), elapsed);
                for (row_id, row) in &model.rows {
                    let bar = bars.entry(row_id.clone()).or_insert_with(|| {
                        let pb = multi.add(ProgressBar::new_spinner());
                        pb.set_style(spinner_style());
                        pb.enable_steady_tick(Duration::from_millis(120));
                        pb
                    });
                    bar.set_message(format_row_line(row_id, row, elapsed));
                }
            }
        }
    };

    // Finalize: one last sample so the closing line reflects the true totals,
    // then stop every spinner (leaving its final line on screen).
    handle.run_upkeep();
    let elapsed = started.elapsed();
    let model = sampler.observe(&handle.render(), elapsed);
    for (row_id, bar) in &bars {
        if let Some(row) = model.rows.get(row_id) {
            bar.set_message(format_row_line(row_id, row, elapsed));
        }
        bar.finish();
    }
    result
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✓")
}

/// Format one row's live line:
/// `<row>  <src>→<sink>  <in> in / <out> out  <rate>/s  page <p>  <elapsed>`.
/// Pure so it can be unit-tested without a terminal.
pub fn format_row_line(row_id: &str, row: &RowStats, elapsed: Duration) -> String {
    let label = if row_id.is_empty() { "run" } else { row_id };
    let src = if row.source.is_empty() {
        "?"
    } else {
        &row.source
    };
    let sink = if row.sink.is_empty() { "?" } else { &row.sink };
    let mut line = format!(
        "{label:<16} {src}→{sink}  {} in / {} out  {}/s  page {}  {}",
        format_count(row.records_in),
        format_count(row.records_out),
        format_rate(row.rate),
        row.pages,
        format_elapsed(elapsed),
    );
    if row.dlq_records > 0 {
        line.push_str(&format!("  {} dlq", format_count(row.dlq_records)));
    }
    match row.finished {
        Some(true) => line.push_str("  done"),
        Some(false) => line.push_str("  FAILED"),
        None => {}
    }
    line
}

/// Compact human count: `1234` → `1.2k`, `2_500_000` → `2.5M`.
fn format_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    }
}

fn format_rate(r: f64) -> String {
    if r >= 100.0 {
        format!("{r:.0}")
    } else if r >= 10.0 {
        format!("{r:.1}")
    } else {
        format!("{r:.2}")
    }
}

fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;

    #[test]
    fn quiet_disables_progress() {
        assert!(!is_progress_session(true, false));
    }

    #[test]
    fn tui_takes_precedence_over_progress() {
        assert!(!is_progress_session(false, true));
    }

    #[test]
    fn non_tty_stdout_disables_progress() {
        // Test harnesses run with a non-TTY stdout, so a plain session must
        // resolve to `false` here — the CI/pipe fallback contract.
        assert!(!is_progress_session(false, false) || std::io::stdout().is_terminal());
    }

    fn row(src: &str, sink: &str, rin: u64, rout: u64, pages: u64, rate: f64) -> RowStats {
        RowStats {
            source: src.into(),
            sink: sink.into(),
            records_in: rin,
            records_out: rout,
            pages,
            rate,
            ..Default::default()
        }
    }

    #[test]
    fn format_row_line_renders_all_fields() {
        let r = row("rest", "jsonl", 1500, 1499, 3, 250.0);
        let line = format_row_line("orders", &r, Duration::from_secs(65));
        assert!(line.contains("orders"), "{line}");
        assert!(line.contains("rest→jsonl"), "{line}");
        assert!(line.contains("1.5k in"), "{line}");
        assert!(line.contains("1.5k out"), "{line}");
        assert!(line.contains("250/s"), "{line}");
        assert!(line.contains("page 3"), "{line}");
        assert!(line.contains("1m05s"), "{line}");
    }

    #[test]
    fn empty_row_id_falls_back_to_run_label() {
        let r = row("csv", "stdout", 10, 10, 1, 0.0);
        let line = format_row_line("", &r, Duration::from_secs(1));
        assert!(line.starts_with("run"), "{line}");
    }

    #[test]
    fn unknown_connectors_render_placeholders() {
        let r = row("", "", 0, 0, 0, 0.0);
        let line = format_row_line("x", &r, Duration::ZERO);
        assert!(line.contains("?→?"), "{line}");
    }

    #[test]
    fn finished_and_dlq_annotations() {
        let mut r = row("rest", "bq", 100, 90, 1, 0.0);
        r.dlq_records = 10;
        r.finished = Some(false);
        let line = format_row_line("r", &r, Duration::from_secs(2));
        assert!(line.contains("10 dlq"), "{line}");
        assert!(line.contains("FAILED"), "{line}");
    }

    #[tokio::test(start_paused = true)]
    async fn drive_loop_samples_and_finalizes() {
        // Install the shared recorder and emit a couple of series for pipeline
        // "p" so the sampler creates a bar and the render loop runs.
        let handle = crate::livemetrics::install_metrics_recorder(None).expect("recorder");
        metrics::counter!(
            "faucet_source_records_total",
            "pipeline" => "p", "row" => "r", "connector" => "rest"
        )
        .increment(5);
        metrics::counter!(
            "faucet_sink_records_total",
            "pipeline" => "p", "row" => "r", "connector" => "jsonl"
        )
        .increment(5);

        // Hidden draw target → no terminal needed. The run future outlives a
        // few ticks so the loop samples at least once, then completes.
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let out = drive_with(
            async {
                tokio::time::sleep(Duration::from_millis(350)).await;
                "done"
            },
            "p",
            handle,
            multi,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(out, "done");
    }

    #[tokio::test(start_paused = true)]
    async fn public_drive_wrapper_runs() {
        // Exercise the public `drive` entry point (stderr draw target). In the
        // non-TTY test env indicatif renders nothing; the future still resolves.
        let handle = crate::livemetrics::install_metrics_recorder(None).expect("recorder");
        let out = drive(
            async {
                tokio::time::sleep(Duration::from_millis(120)).await;
                99u8
            },
            "wrap",
            handle,
        )
        .await;
        assert_eq!(out, 99);
    }

    #[test]
    fn count_and_elapsed_formatting() {
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_500), "1.5k");
        assert_eq!(format_count(2_500_000), "2.5M");
        assert_eq!(format_elapsed(Duration::from_secs(5)), "5s");
        assert_eq!(format_elapsed(Duration::from_secs(125)), "2m05s");
        assert_eq!(format_elapsed(Duration::from_secs(3_665)), "1h01m05s");
    }
}
