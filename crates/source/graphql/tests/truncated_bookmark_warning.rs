//! The warning a truncated unordered incremental run logs (#751). Its own test
//! binary: it captures logs with a thread-local subscriber, and sibling tests
//! hitting the same `warn!` callsite on other threads made the capture miss it.

mod support;

use faucet_core::Source;
use faucet_source_graphql::GraphqlStream;
use futures::StreamExt;
use serde_json::json;
use std::sync::{Arc, Mutex};
use support::{Api, config};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer};

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Holding the bookmark back on a truncated unordered run says why.
#[tokio::test(flavor = "current_thread")]
async fn a_truncated_unordered_run_warns_that_the_bookmark_is_held() {
    let logs = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let server = MockServer::start().await;
    let api = Api::default();
    *api.rows.lock().unwrap() = vec![
        (1, "2026-01-09T00:00:00Z".into()),
        (2, "2026-01-02T00:00:00Z".into()),
        (3, "2026-01-03T00:00:00Z".into()),
    ];
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;
    let start = json!("2026-01-01T00:00:00Z");
    let source = GraphqlStream::new(config(&server).max_pages(1));
    source.apply_start_bookmark(start.clone()).await.unwrap();
    let ctx = std::collections::HashMap::new();
    let pages: Vec<_> = source.stream_pages(&ctx, 2).collect().await;
    let last = pages.into_iter().filter_map(|p| p.unwrap().bookmark).last();
    assert_eq!(last, Some(start));
    let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(text.contains("bookmark is not advanced"), "{text}");
}
