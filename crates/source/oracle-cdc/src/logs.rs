//! Redo log selection and gap detection for a mining window. Pure.
//!
//! A window `[start, end]` needs every log (per redo thread) whose SCN range
//! overlaps it, with contiguous sequence numbers, the oldest one starting at
//! or before `start`, and the newest one reaching past `end` unless the thread
//! produced no later redo at all (it was closed). Anything less means redo was recycled or archive logs
//! were deleted, and the run fails naming the missing SCN range — changes are
//! never skipped silently.

use std::collections::BTreeMap;

/// One online or archived redo log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFile {
    /// File path (`MEMBER` / `NAME`).
    pub name: String,
    /// Redo thread (`THREAD#`).
    pub thread: i64,
    /// Log sequence (`SEQUENCE#`).
    pub sequence: i64,
    /// `FIRST_CHANGE#`.
    pub first_change: u64,
    /// `NEXT_CHANGE#` (a huge sentinel for the current online log).
    pub next_change: u64,
    /// `true` for an archived copy.
    pub archived: bool,
}

/// Pick the logs covering `[start, end]`, preferring archived copies (an
/// online log can be overwritten mid-read), or explain what is missing.
pub fn plan_log_files(all: &[LogFile], start: u64, end: u64) -> Result<Vec<LogFile>, String> {
    let mut chosen: BTreeMap<(i64, i64), LogFile> = BTreeMap::new();
    for log in all
        .iter()
        .filter(|l| l.first_change <= end && l.next_change > start)
    {
        let key = (log.thread, log.sequence);
        match chosen.get(&key) {
            Some(existing) if existing.archived || !log.archived => {}
            _ => {
                chosen.insert(key, log.clone());
            }
        }
    }
    if chosen.is_empty() {
        return Err(format!(
            "no online or archived redo log covers SCN {start}..={end}; the redo was recycled \
             or the archived logs were deleted — the changes in that range cannot be recovered \
             (re-snapshot the tables, then restart capture)"
        ));
    }
    let mut by_thread: BTreeMap<i64, Vec<&LogFile>> = BTreeMap::new();
    for log in chosen.values() {
        by_thread.entry(log.thread).or_default().push(log);
    }
    for (thread, logs) in &by_thread {
        for pair in logs.windows(2) {
            if pair[1].sequence != pair[0].sequence + 1 {
                return Err(format!(
                    "redo thread {thread} is missing log sequence(s) {}..{} (SCN {}..{}); an \
                     archived log was deleted before it was mined — those changes cannot be \
                     recovered",
                    pair[0].sequence + 1,
                    pair[1].sequence - 1,
                    pair[0].next_change,
                    pair[1].first_change
                ));
            }
        }
        let oldest = logs[0].first_change;
        if oldest > start {
            return Err(format!(
                "redo for SCN {start}..{oldest} on thread {thread} is no longer available (the \
                 online logs were overwritten or the archived logs deleted); those changes \
                 cannot be recovered — raise archive-log retention, then re-snapshot"
            ));
        }
        let newest = logs[logs.len() - 1];
        if newest.next_change <= end
            && let Some(later) = next_log_after(all, *thread, newest.sequence)
        {
            return Err(missing_tail(
                *thread,
                newest.sequence,
                newest.next_change,
                later,
                end,
            ));
        }
    }
    // A thread with redo on both sides of the window but none inside it lost
    // every log that covered the window.
    let threads: std::collections::BTreeSet<i64> = all.iter().map(|l| l.thread).collect();
    for thread in threads.difference(&by_thread.keys().copied().collect()) {
        let before = all
            .iter()
            .filter(|l| l.thread == *thread && l.next_change <= start)
            .max_by_key(|l| l.sequence);
        if let Some(before) = before
            && let Some(later) = next_log_after(all, *thread, before.sequence)
        {
            return Err(missing_tail(
                *thread,
                before.sequence,
                before.next_change,
                later,
                end,
            ));
        }
    }
    Ok(chosen.into_values().collect())
}

/// The lowest-sequence log of `thread` after `sequence`.
fn next_log_after(all: &[LogFile], thread: i64, sequence: i64) -> Option<&LogFile> {
    all.iter()
        .filter(|l| l.thread == thread && l.sequence > sequence)
        .min_by_key(|l| l.sequence)
}

fn missing_tail(thread: i64, last_seq: i64, from: u64, later: &LogFile, end: u64) -> String {
    format!(
        "redo thread {thread} is missing log sequence(s) {}..{} (SCN {from}..{}); the redo \
         covering the end of the mining window (SCN {end}) was deleted or overwritten before \
         it was mined — those changes cannot be recovered",
        last_seq + 1,
        later.sequence - 1,
        later.first_change
    )
}

/// The oldest SCN any available log still holds.
pub fn oldest_available_scn(logs: &[LogFile]) -> Option<u64> {
    logs.iter().map(|l| l.first_change).min()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(seq: i64, first: u64, next: u64, archived: bool) -> LogFile {
        LogFile {
            name: format!("{}{seq}", if archived { "arch" } else { "redo" }),
            thread: 1,
            sequence: seq,
            first_change: first,
            next_change: next,
            archived,
        }
    }

    #[test]
    fn prefers_archived_and_filters_by_range() {
        let logs = vec![
            log(1, 0, 100, true),
            log(2, 100, 200, false),
            log(2, 100, 200, true),
            log(3, 200, u64::MAX, false),
        ];
        let chosen = plan_log_files(&logs, 150, 250).unwrap();
        let names: Vec<&str> = chosen.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, vec!["arch2", "redo3"]);
        assert_eq!(oldest_available_scn(&logs), Some(0));
    }

    #[test]
    fn detects_recycled_redo() {
        let logs = vec![log(5, 500, 600, false), log(6, 600, u64::MAX, false)];
        let err = plan_log_files(&logs, 400, 650).unwrap_err();
        assert!(err.contains("400..500"), "{err}");
        let err = plan_log_files(&[], 1, 2).unwrap_err();
        assert!(err.contains("no online or archived redo log"), "{err}");
        assert_eq!(oldest_available_scn(&[]), None);
    }

    #[test]
    fn detects_sequence_gaps() {
        let logs = vec![log(1, 0, 100, true), log(3, 200, 300, true)];
        let err = plan_log_files(&logs, 50, 250).unwrap_err();
        assert!(err.contains("sequence(s) 2..2"), "{err}");
    }

    #[test]
    fn detects_a_deleted_log_at_the_end_of_the_window() {
        // seq 11 [300000, 700000) was deleted; seq 12 starts after the window.
        let logs = vec![log(10, 0, 300_000, true), log(12, 700_000, u64::MAX, false)];
        let err = plan_log_files(&logs, 1000, 501_000).unwrap_err();
        assert!(err.contains("sequence(s) 11..11"), "{err}");
        assert!(err.contains("SCN 300000..700000"), "{err}");
        // A thread closed before the end of the window produced no later redo.
        assert_eq!(
            plan_log_files(&[log(10, 0, 300_000, true)], 1000, 501_000)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn detects_a_thread_whose_logs_inside_the_window_are_all_gone() {
        let mut before = log(4, 0, 100, true);
        before.thread = 2;
        let mut after = log(7, 900, u64::MAX, false);
        after.thread = 2;
        let logs = vec![log(1, 0, u64::MAX, false), before.clone(), after];
        let err = plan_log_files(&logs, 200, 500).unwrap_err();
        assert!(err.contains("redo thread 2"), "{err}");
        assert!(err.contains("sequence(s) 5..6"), "{err}");
        // Closed before the window: nothing missing.
        assert_eq!(
            plan_log_files(&[log(1, 0, u64::MAX, false), before], 200, 500)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn multiple_threads_are_checked_independently() {
        let mut t2 = log(9, 0, u64::MAX, false);
        t2.thread = 2;
        let logs = vec![log(1, 0, u64::MAX, false), t2];
        assert_eq!(plan_log_files(&logs, 10, 20).unwrap().len(), 2);
    }
}
