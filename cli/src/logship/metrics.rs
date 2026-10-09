//! `faucet_logs_*` metrics (#806).

/// Why buffered lines were dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Undelivered past `buffer_max_age_secs`.
    MaxAge,
    /// The buffer grew past `buffer_max_bytes`.
    MaxBytes,
    /// The run hit its per-run line cap.
    MaxLines,
    /// The capture queue was full (the writer could not keep up).
    QueueFull,
}

impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxAge => "max_age",
            Self::MaxBytes => "max_bytes",
            Self::MaxLines => "max_lines",
            Self::QueueFull => "queue_full",
        }
    }
}

/// Register HELP text.
pub fn describe() {
    metrics::describe_gauge!(
        "faucet_logs_buffered_lines",
        "Run-log lines buffered locally and not yet acknowledged by the OTLP collector."
    );
    metrics::describe_gauge!(
        "faucet_logs_buffered_bytes",
        "Bytes of undelivered run-log lines in the local buffer."
    );
    metrics::describe_gauge!(
        "faucet_logs_oldest_undelivered_seconds",
        "Age of the oldest undelivered run-log line (0 when everything is delivered)."
    );
    metrics::describe_counter!(
        "faucet_logs_shipped_lines_total",
        "Run-log lines the OTLP collector acknowledged."
    );
    metrics::describe_counter!(
        "faucet_logs_dropped_lines_total",
        "Run-log lines dropped before delivery, by reason (max_age|max_bytes|max_lines|queue_full)."
    );
    metrics::describe_counter!(
        "faucet_logs_export_errors_total",
        "Failed OTLP log export attempts."
    );
    metrics::describe_counter!(
        "faucet_logs_local_write_failures_total",
        "Run-log lines that could not be written to the local buffer."
    );
}

pub fn shipped(n: u64) {
    metrics::counter!("faucet_logs_shipped_lines_total").increment(n);
}

pub fn dropped(reason: DropReason, n: u64) {
    if n > 0 {
        metrics::counter!("faucet_logs_dropped_lines_total", "reason" => reason.as_str())
            .increment(n);
    }
}

pub fn export_error() {
    metrics::counter!("faucet_logs_export_errors_total").increment(1);
    metrics::counter!("faucet_otel_export_failures_total", "signal" => "logs").increment(1);
}

pub fn local_write_failure(n: u64) {
    metrics::counter!("faucet_logs_local_write_failures_total").increment(n);
}

/// Buffer gauges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferGauges {
    pub lines: u64,
    pub bytes: u64,
    pub oldest_secs: u64,
}

pub fn set_buffer(g: BufferGauges) {
    metrics::gauge!("faucet_logs_buffered_lines").set(g.lines as f64);
    metrics::gauge!("faucet_logs_buffered_bytes").set(g.bytes as f64);
    metrics::gauge!("faucet_logs_oldest_undelivered_seconds").set(g.oldest_secs as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_and_emitters() {
        assert_eq!(DropReason::MaxAge.as_str(), "max_age");
        assert_eq!(DropReason::MaxBytes.as_str(), "max_bytes");
        assert_eq!(DropReason::MaxLines.as_str(), "max_lines");
        assert_eq!(DropReason::QueueFull.as_str(), "queue_full");
        describe();
        shipped(1);
        dropped(DropReason::MaxAge, 0);
        dropped(DropReason::MaxAge, 2);
        export_error();
        local_write_failure(1);
        set_buffer(BufferGauges::default());
    }
}
