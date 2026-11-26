//! When a long read takes fresh storage credentials (#1117), and the task
//! feed that lets a reader finish before it does.
//!
//! Pure: no catalog, no table. The stream module asks [`Credentials`] whether
//! a refresh is due and tells it what happened.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iceberg::scan::{FileScanTask, FileScanTaskStream};
use tokio::time::Instant;

/// The least time between two refresh attempts, whatever the first came to:
/// a catalog that is down, or one that vends credentials about to expire,
/// must not turn the read into one catalog call per data file.
pub const MIN_BETWEEN_ATTEMPTS: Duration = Duration::from_secs(30);

/// How long before a known expiry the credentials are replaced.
pub const EXPIRY_MARGIN: Duration = Duration::from_secs(300);

/// The upper bound on one refresh call. The source's other calls have no
/// attempt bound, and a catalog that never answers must not stall a read
/// whose credentials still work.
pub const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// The age of the storage credentials a read is working with.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// The handle holds credentials the catalog vended (they expire).
    vended: bool,
    /// Replace them at this age; zero replaces them before every data file.
    every: Duration,
    taken_at: Instant,
    last_attempt: Instant,
    /// When the catalog said they expire, on the wall clock (epoch ms).
    expires_at_ms: Option<i64>,
}

impl Credentials {
    pub fn new(vended: bool, every: Duration, expires_at_ms: Option<i64>, now: Instant) -> Self {
        Self {
            vended,
            every,
            taken_at: now,
            last_attempt: now,
            expires_at_ms,
        }
    }

    /// The instant from which a refresh is due, or `None` when it never is.
    /// `wall_ms` is the wall clock at `now`.
    pub fn due_at(&self, now: Instant, wall_ms: i64) -> Option<Instant> {
        if !self.vended {
            return None;
        }
        if self.every.is_zero() {
            return Some(now);
        }
        let by_age = self.taken_at.checked_add(self.every);
        let by_expiry = self.expires_at_ms.and_then(|at| {
            let left = u64::try_from(at.saturating_sub(wall_ms)).unwrap_or(0);
            now.checked_add(Duration::from_millis(left).saturating_sub(EXPIRY_MARGIN))
        });
        let due = match (by_age, by_expiry) {
            (Some(a), Some(e)) => a.min(e),
            (a, e) => a.or(e)?,
        };
        Some(due.max(self.last_attempt + MIN_BETWEEN_ATTEMPTS))
    }

    pub fn due(&self, now: Instant, wall_ms: i64) -> bool {
        self.due_at(now, wall_ms).is_some_and(|at| now >= at)
    }

    /// A refresh was tried and failed; the credentials are the old ones.
    pub fn failed(&mut self, now: Instant) {
        self.last_attempt = now;
    }

    /// A refresh brought new credentials.
    pub fn refreshed(&mut self, vended: bool, expires_at_ms: Option<i64>, now: Instant) {
        self.vended = vended;
        self.expires_at_ms = expires_at_ms;
        self.taken_at = now;
        self.last_attempt = now;
    }
}

/// The planned data files of one scan step, shared by the readers that work
/// through them one after another.
pub type TaskQueue = Arc<Mutex<VecDeque<FileScanTask>>>;

pub fn queue(tasks: Vec<FileScanTask>) -> TaskQueue {
    Arc::new(Mutex::new(tasks.into()))
}

pub fn is_empty(queue: &TaskQueue) -> bool {
    queue.lock().expect("task queue lock").is_empty()
}

/// The tasks one reader gets: from the front of `queue` until `until` has
/// passed, and always at least one, so every reader makes progress. A reader
/// only takes a task it starts, so a task left in the queue is read by the
/// next reader and none is read twice.
pub fn feed(queue: TaskQueue, until: Option<Instant>) -> FileScanTaskStream {
    let mut first = true;
    Box::pin(futures::stream::poll_fn(move |_| {
        if !std::mem::take(&mut first) && until.is_some_and(|at| Instant::now() >= at) {
            return std::task::Poll::Ready(None);
        }
        let next = queue.lock().expect("task queue lock").pop_front();
        std::task::Poll::Ready(next.map(Ok))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    const WALL: i64 = 1_800_000_000_000;

    fn tasks(n: usize) -> Vec<FileScanTask> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );
        (0..n)
            .map(|i| {
                FileScanTask::builder()
                    .with_file_size_in_bytes(1)
                    .with_start(0)
                    .with_length(1)
                    .with_data_file_path(format!("f{i}.parquet"))
                    .with_data_file_format(DataFileFormat::Parquet)
                    .with_schema(schema.clone())
                    .with_project_field_ids(vec![1])
                    .with_case_sensitive(true)
                    .build()
                    .unwrap()
            })
            .collect()
    }

    async fn drain(stream: FileScanTaskStream) -> Vec<String> {
        stream
            .map(|t| t.unwrap().data_file_path().to_string())
            .collect()
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn credentials_that_were_not_vended_are_never_due() {
        let c = Credentials::new(false, Duration::ZERO, Some(WALL), Instant::now());
        assert_eq!(c.due_at(Instant::now(), WALL), None);
        tokio::time::advance(Duration::from_secs(86_400)).await;
        assert!(!c.due(Instant::now(), WALL + 86_400_000));
    }

    #[tokio::test(start_paused = true)]
    async fn vended_credentials_are_due_at_their_age() {
        let c = Credentials::new(true, Duration::from_secs(300), None, Instant::now());
        tokio::time::advance(Duration::from_secs(299)).await;
        assert!(!c.due(Instant::now(), WALL));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(c.due(Instant::now(), WALL));
    }

    #[tokio::test(start_paused = true)]
    async fn a_known_expiry_brings_the_refresh_forward_but_not_under_the_floor() {
        let start = Instant::now();
        // Expires in 6 minutes: due 5 minutes before that, in 1 minute.
        let c = Credentials::new(true, Duration::from_secs(3600), Some(WALL + 360_000), start);
        assert_eq!(c.due_at(start, WALL), Some(start + Duration::from_secs(60)));
        // Already inside the margin when vended: due after the floor, not at once.
        let c = Credentials::new(true, Duration::from_secs(3600), Some(WALL + 10_000), start);
        assert_eq!(c.due_at(start, WALL), Some(start + MIN_BETWEEN_ATTEMPTS));
        // An expiry in the past is the same case.
        let c = Credentials::new(true, Duration::from_secs(3600), Some(WALL - 1), start);
        assert_eq!(c.due_at(start, WALL), Some(start + MIN_BETWEEN_ATTEMPTS));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_attempt_is_tried_again_after_the_floor_and_a_success_starts_over() {
        let mut c = Credentials::new(true, Duration::from_secs(300), None, Instant::now());
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(c.due(Instant::now(), WALL));
        c.failed(Instant::now());
        assert!(!c.due(Instant::now(), WALL), "not on the very next file");
        tokio::time::advance(MIN_BETWEEN_ATTEMPTS - Duration::from_secs(1)).await;
        assert!(!c.due(Instant::now(), WALL));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(c.due(Instant::now(), WALL));
        c.refreshed(true, None, Instant::now());
        assert!(!c.due(Instant::now(), WALL));
        tokio::time::advance(Duration::from_secs(300)).await;
        assert!(c.due(Instant::now(), WALL));
        // A reload that brought no vended credentials ends the refreshing.
        c.refreshed(false, None, Instant::now());
        assert_eq!(c.due_at(Instant::now(), WALL), None);
    }

    #[tokio::test(start_paused = true)]
    async fn an_age_or_expiry_too_far_away_is_not_due_and_does_not_overflow() {
        let now = Instant::now();
        let c = Credentials::new(true, Duration::from_secs(u64::MAX), None, now);
        assert_eq!(c.due_at(now, WALL), None);
        let c = Credentials::new(true, Duration::from_secs(u64::MAX), Some(i64::MAX), now);
        assert!(!c.due(now, WALL));
        // A countable expiry still applies when the age does not.
        let c = Credentials::new(
            true,
            Duration::from_secs(u64::MAX),
            Some(WALL + 360_000),
            now,
        );
        assert_eq!(c.due_at(now, WALL), Some(now + Duration::from_secs(60)));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_refreshes_before_every_data_file() {
        let mut c = Credentials::new(true, Duration::ZERO, None, Instant::now());
        assert!(c.due(Instant::now(), WALL));
        c.refreshed(true, None, Instant::now());
        assert!(c.due(Instant::now(), WALL));
        c.failed(Instant::now());
        assert!(c.due(Instant::now(), WALL));
    }

    #[tokio::test(start_paused = true)]
    async fn a_feed_without_a_deadline_hands_out_every_task_in_order() {
        let q = queue(tasks(4));
        assert_eq!(
            drain(feed(q.clone(), None)).await,
            ["f0.parquet", "f1.parquet", "f2.parquet", "f3.parquet"]
        );
        assert!(is_empty(&q));
        assert!(drain(feed(q, None)).await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_feed_past_its_deadline_still_hands_out_one_task_and_keeps_the_rest() {
        let q = queue(tasks(3));
        let past = Some(Instant::now());
        assert_eq!(drain(feed(q.clone(), past)).await, ["f0.parquet"]);
        assert_eq!(drain(feed(q.clone(), past)).await, ["f1.parquet"]);
        assert!(!is_empty(&q));
        assert_eq!(drain(feed(q.clone(), past)).await, ["f2.parquet"]);
        assert!(is_empty(&q));
    }

    #[tokio::test(start_paused = true)]
    async fn a_feed_stops_when_its_deadline_passes_between_two_tasks() {
        let q = queue(tasks(5));
        let mut stream = feed(q.clone(), Some(Instant::now() + Duration::from_secs(10)));
        assert!(stream.next().await.is_some());
        assert!(stream.next().await.is_some());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none(), "it stays ended");
        assert_eq!(q.lock().unwrap().len(), 3);
    }
}
