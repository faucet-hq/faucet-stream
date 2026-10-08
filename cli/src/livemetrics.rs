//! Shared live-metrics plumbing for `faucet run` — the in-process Prometheus
//! recorder installer plus a pure Prometheus-text parser/aggregator.
//!
//! Both the full-screen TUI (`--tui`, `cli-tui`) and the lightweight inline
//! progress line (`cli-progress`) sample the same in-process Prometheus
//! recorder's rendered text a few times a second (read-only — zero hot-path
//! impact) and reduce the handful of `faucet_*` series they care about into a
//! `TuiModel` per tick. Everything below the recorder installer is pure and
//! unit-tested; the render loops just call `Sampler::observe` with the
//! rendered text.
//!
//! This module is compiled whenever *either* live-view feature is on, so the
//! recorder + parser are shared rather than duplicated.

use crate::error::{CliError, CliResult};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::collections::BTreeMap;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Recorder install (moved from `tui` so `cli-progress` can reuse it without
// pulling ratatui).
// ---------------------------------------------------------------------------

/// Default histogram buckets — mirrors `faucet-core`'s installer so a
/// CLI-owned recorder behaves identically for any `/metrics` scraper.
const DEFAULT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// The handle of the recorder this module installed, if any — makes
/// [`install_metrics_recorder`] idempotent (a second call returns the same
/// handle instead of racing on the process-global recorder slot).
static RECORDER_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the Prometheus recorder for a live-view session and return its
/// render handle. When the config carries an `observability.prometheus` block
/// the `/metrics` HTTP endpoint is preserved (recorder + listener, exactly
/// what `install_observability` would have set up); otherwise a listener-less
/// recorder is installed purely as the live view's data source. Idempotent: a
/// second call returns the first call's handle.
pub fn install_metrics_recorder(
    prom: Option<&faucet_core::PrometheusConfig>,
) -> CliResult<PrometheusHandle> {
    // Validate the listener address up front so a config error surfaces even
    // when the recorder slot is already occupied.
    let listen: Option<std::net::SocketAddr> = match prom {
        Some(p) => Some(
            p.listen
                .parse()
                .map_err(|e| CliError::Observability(format!("prometheus listen: {e}")))?,
        ),
        None => None,
    };
    if let Some(handle) = RECORDER_HANDLE.get() {
        return Ok(handle.clone());
    }
    let handle = match (prom, listen) {
        (Some(p), Some(listen)) => {
            let (recorder, exporter) = PrometheusBuilder::new()
                .with_http_listener(listen)
                .set_buckets(p.buckets.as_deref().unwrap_or(DEFAULT_BUCKETS))
                .map_err(|e| CliError::Observability(e.to_string()))?
                .build()
                .map_err(|e| CliError::Observability(e.to_string()))?;
            let handle = recorder.handle();
            if ::metrics::set_global_recorder(recorder).is_ok() {
                tokio::spawn(exporter);
                tracing::info!("Prometheus /metrics listening on {}", p.listen);
            } else {
                warn_recorder_occupied();
            }
            handle
        }
        _ => {
            let recorder = PrometheusBuilder::new()
                .set_buckets(DEFAULT_BUCKETS)
                .map_err(|e| CliError::Observability(e.to_string()))?
                .build_recorder();
            let handle = recorder.handle();
            if ::metrics::set_global_recorder(recorder).is_err() {
                warn_recorder_occupied();
            }
            handle
        }
    };
    Ok(RECORDER_HANDLE.get_or_init(|| handle).clone())
}

fn warn_recorder_occupied() {
    tracing::warn!(
        "metrics recorder already installed; the live view may show no data (was a recorder installed before this `faucet run`?)"
    );
}

/// Install a live-view session's observability: the CLI owns the metrics
/// recorder (keeping the `/metrics` endpoint when one is configured) so it can
/// render the recorder's output; the rest of the observability config (OTLP
/// traces) installs as usual with the prometheus block taken out.
pub fn setup_observability(cfg: &crate::config::PipelineConfig) -> CliResult<PrometheusHandle> {
    crate::set_build_version();
    let mut obs_cfg = crate::obs::build_observability_config(cfg);
    let prom = obs_cfg.prometheus.take();
    let handle = install_metrics_recorder(prom.as_ref())?;
    faucet_core::install_observability(&obs_cfg)?;
    Ok(handle)
}

// ---------------------------------------------------------------------------
// Pure Prometheus-text parsing + per-row aggregation.
// ---------------------------------------------------------------------------

/// One parsed sample line: `name{label="v",…} 1.5`.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parse the Prometheus text exposition format, skipping `# HELP` / `# TYPE`
/// comments and lines that don't parse (robustness over strictness — a live
/// view must never crash on exporter output).
pub fn parse_samples(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    // Split into name[{labels}] and value. The value is the last
    // whitespace-separated token (an optional timestamp would follow it, but
    // metrics-exporter-prometheus does not emit timestamps).
    let (head, value_str) = match line.find('}') {
        Some(close) => {
            let (h, rest) = line.split_at(close + 1);
            (h, rest.trim())
        }
        None => {
            let mut parts = line.split_whitespace();
            let h = parts.next()?;
            (h, line[h.len()..].trim())
        }
    };
    let value: f64 = value_str.split_whitespace().next()?.parse().ok()?;

    let (name, labels) = match head.find('{') {
        None => (head.trim().to_string(), BTreeMap::new()),
        Some(open) => {
            let name = head[..open].trim().to_string();
            let body = head[open + 1..head.len() - 1].trim_end_matches(',');
            (name, parse_labels(body)?)
        }
    };
    if name.is_empty() {
        return None;
    }
    Some(Sample {
        name,
        labels,
        value,
    })
}

/// Parse `k="v",k2="v2"` with Prometheus escaping (`\\`, `\"`, `\n`) inside
/// label values.
fn parse_labels(body: &str) -> Option<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    let mut chars = body.chars().peekable();
    loop {
        // Skip separators / whitespace.
        while matches!(chars.peek(), Some(',') | Some(' ')) {
            chars.next();
        }
        if chars.peek().is_none() {
            return Some(labels);
        }
        let mut key = String::new();
        for c in chars.by_ref() {
            if c == '=' {
                break;
            }
            key.push(c);
        }
        if chars.next()? != '"' {
            return None;
        }
        let mut value = String::new();
        loop {
            match chars.next()? {
                '\\' => match chars.next()? {
                    'n' => value.push('\n'),
                    '\\' => value.push('\\'),
                    '"' => value.push('"'),
                    other => {
                        value.push('\\');
                        value.push(other);
                    }
                },
                '"' => break,
                c => value.push(c),
            }
        }
        labels.insert(key.trim().to_string(), value);
    }
}

/// Live view of one matrix row / invocation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowStats {
    /// `connector` label observed on the source-side series.
    pub source: String,
    /// `connector` label observed on the sink-side series.
    pub sink: String,
    pub records_in: u64,
    pub records_out: u64,
    /// Source pages fetched so far (`faucet_source_pages_total`).
    pub pages: u64,
    /// Sink records/second over the last sampling window.
    pub rate: f64,
    pub source_errors: u64,
    pub sink_errors: u64,
    pub dlq_records: u64,
    /// `faucet_pipeline_last_bookmark_unix_seconds` gauge (0 = never).
    pub last_bookmark_unix: f64,
    /// From `faucet_pipeline_runs_total{status}`: Some(true)=ok, Some(false)=err.
    pub finished: Option<bool>,
    pub in_flight: bool,
}

/// The reduced model the renderers draw.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TuiModel {
    /// Row-id → stats, sorted by row id (BTreeMap keeps render order stable).
    pub rows: BTreeMap<String, RowStats>,
    pub total_in: u64,
    pub total_out: u64,
    pub total_rate: f64,
}

/// Turns successive Prometheus renders into `TuiModel`s, computing
/// records/s from consecutive sink-records totals.
#[derive(Debug, Default)]
pub struct Sampler {
    pipeline: String,
    prev_out: BTreeMap<String, (u64, std::time::Duration)>,
}

impl Sampler {
    pub fn new(pipeline: impl Into<String>) -> Self {
        Self {
            pipeline: pipeline.into(),
            prev_out: BTreeMap::new(),
        }
    }

    /// Reduce one rendered scrape into a model. `elapsed` is time since the
    /// live view started (monotonic), used for rate windows.
    pub fn observe(&mut self, text: &str, elapsed: std::time::Duration) -> TuiModel {
        let mut model = TuiModel::default();
        for s in parse_samples(text) {
            if s.labels.get("pipeline").map(String::as_str) != Some(self.pipeline.as_str()) {
                continue;
            }
            let row_id = s.labels.get("row").cloned().unwrap_or_default();
            let row = model.rows.entry(row_id).or_default();
            let connector = s.labels.get("connector").cloned().unwrap_or_default();
            match s.name.as_str() {
                "faucet_source_records_total" => {
                    row.records_in += s.value as u64;
                    if !connector.is_empty() {
                        row.source = connector;
                    }
                }
                "faucet_source_pages_total" => row.pages += s.value as u64,
                "faucet_sink_records_total" => {
                    row.records_out += s.value as u64;
                    if !connector.is_empty() {
                        row.sink = connector;
                    }
                }
                "faucet_source_errors_total" => row.source_errors += s.value as u64,
                "faucet_sink_errors_total" => row.sink_errors += s.value as u64,
                "faucet_sink_dlq_records_total" => row.dlq_records += s.value as u64,
                "faucet_pipeline_last_bookmark_unix_seconds" => {
                    row.last_bookmark_unix = s.value;
                }
                "faucet_pipeline_in_flight" => {
                    if s.value > 0.0 {
                        row.in_flight = true;
                    }
                }
                "faucet_pipeline_runs_total" => {
                    // Fill connector names even before data flows.
                    if row.source.is_empty()
                        && let Some(src) = s.labels.get("source")
                    {
                        row.source = src.clone();
                    }
                    if row.sink.is_empty()
                        && let Some(dst) = s.labels.get("sink")
                    {
                        row.sink = dst.clone();
                    }
                    if s.value > 0.0 {
                        match s.labels.get("status").map(String::as_str) {
                            Some("ok") => row.finished = Some(row.finished.unwrap_or(true)),
                            Some("err") => row.finished = Some(false),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        // Rates from the previous observation of the same row.
        let mut prev = std::mem::take(&mut self.prev_out);
        for (row_id, row) in &mut model.rows {
            if let Some((prev_count, prev_at)) = prev.remove(row_id) {
                let dt = elapsed.saturating_sub(prev_at).as_secs_f64();
                // Counter reset (shouldn't happen in-process) → rate 0.
                if dt > 0.0 && row.records_out >= prev_count {
                    row.rate = (row.records_out - prev_count) as f64 / dt;
                }
            }
            self.prev_out
                .insert(row_id.clone(), (row.records_out, elapsed));
            model.total_in += row.records_in;
            model.total_out += row.records_out;
            model.total_rate += row.rate;
        }
        model
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parses_bare_and_labelled_samples() {
        let text = "\
# HELP faucet_x helper
# TYPE faucet_x counter
faucet_up 1
faucet_source_records_total{pipeline=\"p\",row=\"r1\",connector=\"csv\"} 42
";
        let samples = parse_samples(text);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].name, "faucet_up");
        assert_eq!(samples[0].value, 1.0);
        assert!(samples[0].labels.is_empty());
        assert_eq!(samples[1].labels["connector"], "csv");
        assert_eq!(samples[1].value, 42.0);
    }

    #[test]
    fn parses_escaped_label_values() {
        let text = r#"m{a="quo\"te",b="back\\slash",c="new\nline"} 7"#;
        let s = &parse_samples(text)[0];
        assert_eq!(s.labels["a"], "quo\"te");
        assert_eq!(s.labels["b"], "back\\slash");
        assert_eq!(s.labels["c"], "new\nline");
    }

    #[test]
    fn garbage_lines_are_skipped_not_fatal() {
        let text = "not a metric\nname_only\n{}} 3\nok_metric 5\n";
        let samples = parse_samples(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "ok_metric");
    }

    fn render(pipeline: &str, row: &str, out: u64) -> String {
        format!(
            "faucet_sink_records_total{{pipeline=\"{pipeline}\",row=\"{row}\",connector=\"jsonl\"}} {out}\n"
        )
    }

    #[test]
    fn sampler_filters_by_pipeline_and_computes_rates() {
        let mut s = Sampler::new("mine");
        let t0 = Duration::from_secs(1);
        let m = s.observe(&(render("mine", "a", 100) + &render("other", "a", 999)), t0);
        assert_eq!(m.rows.len(), 1);
        assert_eq!(m.rows["a"].records_out, 100);
        assert_eq!(m.rows["a"].rate, 0.0, "no window yet");

        let m = s.observe(&render("mine", "a", 300), Duration::from_secs(3));
        assert!((m.rows["a"].rate - 100.0).abs() < 1e-9, "200 records / 2s");
        assert_eq!(m.total_out, 300);
    }

    #[test]
    fn sampler_counter_reset_yields_zero_rate() {
        let mut s = Sampler::new("p");
        s.observe(&render("p", "a", 500), Duration::from_secs(1));
        let m = s.observe(&render("p", "a", 10), Duration::from_secs(2));
        assert_eq!(m.rows["a"].rate, 0.0);
    }

    #[test]
    fn sampler_aggregates_row_fields() {
        let text = "\
faucet_source_records_total{pipeline=\"p\",row=\"r\",connector=\"spanner\"} 10
faucet_source_pages_total{pipeline=\"p\",row=\"r\",connector=\"spanner\"} 4
faucet_sink_records_total{pipeline=\"p\",row=\"r\",connector=\"jsonl\"} 8
faucet_source_errors_total{pipeline=\"p\",row=\"r\",connector=\"spanner\",kind=\"http\"} 1
faucet_sink_errors_total{pipeline=\"p\",row=\"r\",connector=\"jsonl\",kind=\"io\"} 2
faucet_sink_dlq_records_total{pipeline=\"p\",row=\"r\",connector=\"jsonl\"} 3
faucet_pipeline_last_bookmark_unix_seconds{pipeline=\"p\",row=\"r\"} 1700000000
faucet_pipeline_in_flight{pipeline=\"p\",row=\"r\"} 1
";
        let mut s = Sampler::new("p");
        let m = s.observe(text, Duration::from_secs(1));
        let row = &m.rows["r"];
        assert_eq!(row.source, "spanner");
        assert_eq!(row.sink, "jsonl");
        assert_eq!(row.records_in, 10);
        assert_eq!(row.records_out, 8);
        assert_eq!(row.pages, 4);
        assert_eq!(row.source_errors, 1);
        assert_eq!(row.sink_errors, 2);
        assert_eq!(row.dlq_records, 3);
        assert_eq!(row.last_bookmark_unix, 1_700_000_000.0);
        assert!(row.in_flight);
        assert_eq!(row.finished, None);
    }

    #[test]
    fn run_status_ok_and_err_map_to_finished() {
        let ok = "faucet_pipeline_runs_total{pipeline=\"p\",row=\"r\",source=\"csv\",sink=\"stdout\",status=\"ok\"} 1\n";
        let err = "faucet_pipeline_runs_total{pipeline=\"p\",row=\"r\",source=\"csv\",sink=\"stdout\",status=\"err\",kind=\"sink\"} 1\n";
        let mut s = Sampler::new("p");
        let m = s.observe(ok, Duration::from_secs(1));
        assert_eq!(m.rows["r"].finished, Some(true));
        assert_eq!(m.rows["r"].source, "csv");
        assert_eq!(m.rows["r"].sink, "stdout");
        let m = s.observe(&(ok.to_string() + err), Duration::from_secs(2));
        // Any err marks the row failed even alongside an ok retry count.
        assert_eq!(m.rows["r"].finished, Some(false));
    }

    #[test]
    fn install_metrics_recorder_is_idempotent() {
        let first = install_metrics_recorder(None).expect("first install");
        let second = install_metrics_recorder(None).expect("second install");
        let _ = first.render();
        let _ = second.render();
    }

    #[test]
    fn install_metrics_recorder_rejects_a_bad_listen_address() {
        let prom = faucet_core::PrometheusConfig {
            listen: "not-an-address".into(),
            buckets: None,
        };
        let err = install_metrics_recorder(Some(&prom)).expect_err("bad listen");
        assert!(matches!(err, CliError::Observability(_)), "{err:?}");
    }

    #[test]
    fn setup_observability_installs_from_a_config_without_prometheus() {
        // No `observability.prometheus` block → listener-less recorder + the
        // rest of observability install (both idempotent). Covers the
        // `setup_observability` wiring end to end.
        let yaml = "version: 1\n\
            pipeline:\n\
            \x20 source: { type: rest, config: { base_url: \"http://x\" } }\n\
            \x20 sink: { type: stdout, config: {} }\n";
        let cfg = crate::config::parse_with_extension(yaml, "yaml").expect("parse cfg");
        let handle = setup_observability(&cfg).expect("setup observability");
        let _ = handle.render();
    }

    #[tokio::test]
    async fn install_metrics_recorder_accepts_a_listener_config() {
        // Ephemeral port; whichever install wins the process-global slot, the
        // call must succeed and hand back a handle.
        let prom = faucet_core::PrometheusConfig {
            listen: "127.0.0.1:0".into(),
            buckets: None,
        };
        let handle = install_metrics_recorder(Some(&prom)).expect("listener install");
        let _ = handle.render();
    }
}
