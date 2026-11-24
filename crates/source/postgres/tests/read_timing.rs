//! Wall-clock of reading a wide table, for before/after comparisons. Ignored:
//! run with `FAUCET_BENCH_POSTGRES_URL` and `FAUCET_BENCH_POSTGRES_QUERY` set.

use faucet_core::Source;
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use futures::StreamExt;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing run against an external database"]
async fn read_timing() {
    let (Ok(url), Ok(query)) = (
        std::env::var("FAUCET_BENCH_POSTGRES_URL"),
        std::env::var("FAUCET_BENCH_POSTGRES_QUERY"),
    ) else {
        eprintln!("read_timing: FAUCET_BENCH_POSTGRES_URL / _QUERY not set, skipping");
        return;
    };
    let source = PostgresSource::new(PostgresSourceConfig::new(&url, &query))
        .await
        .expect("source");
    let ctx = std::collections::HashMap::new();
    for round in 0..3 {
        let started = std::time::Instant::now();
        let mut pages = source.stream_pages(&ctx, 1000);
        let mut rows = 0usize;
        while let Some(page) = pages.next().await {
            rows += page.expect("page").records.len();
        }
        eprintln!(
            "read_timing round {round}: {rows} rows in {:?}",
            started.elapsed()
        );
    }
}
