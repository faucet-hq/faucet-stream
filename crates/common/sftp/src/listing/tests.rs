use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

fn attrs(mode: u32) -> SftpMetadata {
    SftpMetadata {
        permissions: Some(mode),
        ..SftpMetadata::default()
    }
}

fn file(name: &str) -> (String, SftpMetadata) {
    (name.to_string(), attrs(0o100644))
}

fn dir(name: &str) -> (String, SftpMetadata) {
    (name.to_string(), attrs(0o040755))
}

fn status(code: StatusCode) -> SftpError {
    SftpError::Status(russh_sftp::protocol::Status {
        id: 1,
        status_code: code,
        error_message: "dummy".into(),
        language_tag: "en-US".into(),
    })
}

/// Serves `replies` one per `READDIR`, then either the end of the directory
/// or, when `endless`, the last reply again forever.
#[derive(Default)]
struct FakeDir {
    replies: Mutex<Vec<Vec<(String, SftpMetadata)>>>,
    endless: Option<Vec<(String, SftpMetadata)>>,
    open_error: Option<StatusCode>,
    read_error_after: Option<usize>,
    trickle: Option<std::time::Duration>,
    reads: AtomicUsize,
    closes: AtomicUsize,
}

impl FakeDir {
    fn with(replies: Vec<Vec<(String, SftpMetadata)>>) -> Self {
        Self {
            replies: Mutex::new(replies.into_iter().rev().collect()),
            ..Self::default()
        }
    }
}

#[async_trait]
impl DirReader for FakeDir {
    async fn open_dir(&self, _path: &str) -> Result<String, SftpError> {
        match self.open_error {
            Some(code) => Err(status(code)),
            None => Ok("h1".into()),
        }
    }

    async fn read_dir(
        &self,
        handle: &str,
    ) -> Result<Option<Vec<(String, SftpMetadata)>>, SftpError> {
        assert_eq!(handle, "h1");
        let n = self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(delay) = self.trickle {
            tokio::time::sleep(delay).await;
        }
        if self.read_error_after == Some(n) {
            return Err(status(StatusCode::Failure));
        }
        if let Some(reply) = self.replies.lock().unwrap().pop() {
            return Ok(Some(reply));
        }
        Ok(self.endless.clone())
    }

    async fn close_dir(&self, handle: String) {
        assert_eq!(handle, "h1");
        self.closes.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn listing_keeps_entries_across_replies() {
    let fake = FakeDir::with(vec![vec![file("a"), file("b")], vec![], vec![file("c")]]);
    let listed = list_with(&fake, "/data", 10, |_, _| true).await.unwrap();
    let paths: Vec<_> = listed.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, ["/data/a", "/data/b", "/data/c"]);
    assert_eq!(listed[2].name, "c");
    assert!(listed[0].metadata.file_type().is_file());
    assert_eq!(fake.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn listing_keeps_only_matching_entries() {
    let fake = FakeDir::with(vec![vec![
        dir("."),
        dir(".."),
        file("keep.jsonl"),
        dir("sub"),
        file("skip.txt"),
    ]]);
    let mut seen = Vec::new();
    let listed = list_with(&fake, "/data/", 3, |name, meta| {
        seen.push(name.to_string());
        meta.file_type().is_file() && name.ends_with(".jsonl")
    })
    .await
    .unwrap();
    assert_eq!(
        seen,
        ["keep.jsonl", "sub", "skip.txt"],
        "`.` and `..` are not offered"
    );
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, "/data/keep.jsonl");
}

#[tokio::test]
async fn listing_fails_past_the_entry_limit() {
    let fake = FakeDir::with(vec![vec![file("a"), file("b")], vec![file("c")]]);
    assert_eq!(
        list_with(&fake, "d", 3, |_, _| true).await.unwrap().len(),
        3,
        "exactly the limit is fine"
    );
    let fake = FakeDir::with(vec![vec![file("a"), file("b")], vec![file("c")]]);
    let err = list_with(&fake, "d", 2, |_, _| false).await.unwrap_err();
    assert!(
        matches!(err, ListError::TooManyEntries { ref path, limit: 2 } if path == "d"),
        "{err:?}"
    );
    let text = err.to_string();
    assert!(
        text.contains("'d'") && text.contains("max_listed_entries"),
        "{text}"
    );
    assert_eq!(
        fake.closes.load(Ordering::SeqCst),
        1,
        "the handle is closed"
    );
}

#[tokio::test]
async fn a_listing_that_never_ends_stops_at_the_limit() {
    let fake = FakeDir {
        endless: Some(vec![file("again"); 100]),
        ..FakeDir::default()
    };
    let err = list_with(&fake, "/loop", 1_000, |_, _| true)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ListError::TooManyEntries { limit: 1_000, .. }
    ));
    assert_eq!(fake.reads.load(Ordering::SeqCst), 11);
    assert_eq!(fake.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_trickling_listing_hits_its_timeout() {
    let fake = FakeDir {
        endless: Some(vec![file("slow")]),
        trickle: Some(std::time::Duration::from_secs(5)),
        ..FakeDir::default()
    };
    let calls = faucet_core::CallContext::new().with_timeout(std::time::Duration::from_secs(60));
    let err = calls
        .call_once("list", async {
            list_with(&fake, "/slow", 1_000_000, |_, _| true)
                .await
                .map_err(|e| faucet_core::FaucetError::Source(e.to_string()))
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::Timeout { .. }),
        "{err:?}"
    );
    assert!(fake.reads.load(Ordering::SeqCst) <= 13);
}

#[tokio::test]
async fn open_and_read_failures_name_the_path() {
    let fake = FakeDir {
        open_error: Some(StatusCode::NoSuchFile),
        ..FakeDir::default()
    };
    let err = list_with(&fake, "/gone", 5, |_, _| true).await.unwrap_err();
    assert!(err.is_no_such_file());
    assert!(
        err.to_string().contains("SFTP list '/gone' failed"),
        "{err}"
    );
    assert_eq!(fake.closes.load(Ordering::SeqCst), 0, "nothing was opened");

    let fake = FakeDir {
        read_error_after: Some(1),
        endless: Some(vec![file("x")]),
        ..FakeDir::default()
    };
    let err = list_with(&fake, "/flaky", 5, |_, _| true)
        .await
        .unwrap_err();
    assert!(!err.is_no_such_file());
    assert!(err.to_string().contains("'/flaky'"), "{err}");
    assert_eq!(fake.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn errors_after_a_refused_packet_report_the_refusal() {
    let guard = InboundGuard::default();
    let err = ListError::sftp("stat", "/p", SftpError::Timeout);
    assert!(matches!(err.explained(&guard), ListError::Sftp { .. }));

    guard.record(MAX_INBOUND_PACKET + 9);
    let err = ListError::sftp("stat", "/p", SftpError::Timeout).explained(&guard);
    assert!(matches!(err, ListError::PacketTooLarge(_)));
    assert!(err.to_string().contains("packet"), "{err}");
    let err = ListError::Channel("dummy".into()).explained(&guard);
    assert!(matches!(err, ListError::PacketTooLarge(_)));
    let err = ListError::TooManyEntries {
        path: "/p".into(),
        limit: 1,
    }
    .explained(&guard);
    assert!(matches!(err, ListError::TooManyEntries { .. }));
}

#[test]
fn every_error_names_what_to_change() {
    let refused = ListError::ChannelRefused {
        server: "sftp.example:22".into(),
        reason: "dummy".into(),
    };
    let text = refused.to_string();
    assert!(
        text.contains("sftp.example:22") && text.contains("MaxSessions"),
        "{text}"
    );
    assert_eq!(ListError::Channel("dummy".into()).to_string(), "dummy");
    let as_error: &dyn std::error::Error = &refused;
    assert!(as_error.source().is_none());
}

#[test]
fn joined_paths_follow_the_listed_path() {
    assert_eq!(join("", "a"), "a");
    assert_eq!(join("/", "a"), "/a");
    assert_eq!(join("/d", "a"), "/d/a");
    assert_eq!(join("d/", "a"), "d/a");
}

#[test]
fn a_failed_second_connection_names_both_limits() {
    let e = ListError::SecondConnectionRefused {
        server: "h:22".into(),
        reason: "connection refused".into(),
    }
    .to_string();
    assert!(
        e.contains("h:22") && e.contains("connection refused"),
        "{e}"
    );
    assert!(e.contains("MaxSessions"), "{e}");
    assert!(e.contains("two connections per user"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lister_refusal_stays_with_its_lister() {
    let Some((_c, port)) = crate::test_support::server().await else {
        return;
    };
    let handle = crate::authenticate(&crate::test_support::config(port))
        .await
        .unwrap();
    let first = handle.lister().await.unwrap();
    first.guard.record(crate::MAX_INBOUND_PACKET + 1);
    assert!(handle.refusal().is_none());
    let e = ListError::Channel("closed".into()).explained(&first.guard);
    assert!(matches!(e, ListError::PacketTooLarge(_)), "{e}");

    let second = handle.lister().await.unwrap();
    assert!(second.guard.refusal().is_none());
    second.list_dir("/data", 10, |_, _| true).await.unwrap();
    first.close().await;
    second.close().await;
    let session = handle.open_session().await.unwrap();
    session.canonicalize(".").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_a_lister_waits_only_for_the_servers_close() {
    let Some((_c, port)) = crate::test_support::server().await else {
        return;
    };
    let handle = crate::authenticate(&crate::test_support::config(port))
        .await
        .unwrap();
    for _ in 0..3 {
        let lister = handle.lister().await.unwrap();
        lister.list_dir("/data", 10, |_, _| true).await.unwrap();
        let started = std::time::Instant::now();
        lister.close().await;
        assert!(
            started.elapsed() < CLOSE_WAIT / 2,
            "{:?}",
            started.elapsed()
        );
    }
}
