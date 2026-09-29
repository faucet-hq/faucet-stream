#![allow(deprecated)]
//! Proves `faucet-source-csv` upholds the shared connector contract by invoking
//! the reusable `faucet-conformance` battery (checks 1, 2, 6, 9, 10 & 11).
//!
//! CSV is a full-table source (no bookmark) and not a sink, so check 3
//! (bookmark round-trip) and checks 4/5/7/8 (sink capabilities) do not apply.

use std::io::Write;

use faucet_core::Source;
use faucet_source_csv::{CsvSource, CsvSourceConfig};

/// Write a small CSV with `total` `(id, name)` rows and return the temp file.
fn small_fixture(total: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "id,name").unwrap();
    for i in 0..total {
        writeln!(file, "{i},row-{i}").unwrap();
    }
    file.flush().unwrap();
    file
}

#[test]
fn conformance_config_schema_valid() {
    let source = CsvSource::new(CsvSourceConfig::new("/tmp/does-not-matter.csv"));
    faucet_conformance::assert_config_schema_valid(&source);
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[test]
fn conformance_connector_name_nonempty() {
    let source = CsvSource::new(CsvSourceConfig::new("/tmp/does-not-matter.csv"));
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "csv");
}

// ── Check 9: batch_size=0 yields a single page ────────────────────────────────
#[tokio::test]
async fn conformance_batch_size_zero_single_page() {
    // `batch_size = 0` is the CSV source's "no batching" sentinel — it drains the
    // whole file into one page. The config value is authoritative.
    let file = small_fixture(6);
    let config = CsvSourceConfig {
        batch_size: 0,
        ..CsvSourceConfig::new(file.path().to_string_lossy().to_string())
    };
    let source = CsvSource::new(config);
    faucet_conformance::assert_batch_size_zero_single_page(&source).await;
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // A valid file makes the default page-pull probe pass; the check must return
    // Ok(report) with a well-formed probe regardless.
    let file = small_fixture(6);
    let source = CsvSource::new(CsvSourceConfig::new(
        file.path().to_string_lossy().to_string(),
    ));
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[tokio::test]
async fn conformance_bounded_memory() {
    // Generate a CSV large enough that one-big-page buffering would be caught.
    let total = 5_000;
    let batch = 250;
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "id,name").unwrap();
    for i in 0..total {
        writeln!(file, "{i},row-{i}").unwrap();
    }
    file.flush().unwrap();

    let config = CsvSourceConfig {
        batch_size: batch,
        ..CsvSourceConfig::new(file.path().to_string_lossy().to_string())
    };
    let source = CsvSource::new(config);
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // A missing file must surface a typed FaucetError, never a panic.
    let source = CsvSource::new(CsvSourceConfig::new(
        "/nonexistent/faucet-conformance/missing.csv",
    ));
    faucet_conformance::assert_errors_not_panics(&source).await;
}
