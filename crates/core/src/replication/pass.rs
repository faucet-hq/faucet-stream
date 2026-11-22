//! How a paginated pass ended, and the bookmark it may persist.
//!
//! An HTTP source reads a result set page by page. When the page loop stops
//! before the server said "no more pages" — a `max_pages` cap, a server that
//! repeats a cursor — rows may be unread, and a bookmark derived from the
//! rows that *were* read can skip them. [`bookmark_after_pass`] is the one
//! rule every paginated source uses to decide what to keep.

use serde_json::Value;

/// How a pass's page loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PassEnd {
    /// The server reported the last page.
    Complete,
    /// The loop stopped with pages possibly unread.
    Truncated(Truncation),
}

/// Why a pass stopped before the last page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Truncation {
    /// The configured page cap stopped the pass.
    MaxPages {
        /// The cap.
        max: usize,
    },
    /// The server sent back the cursor just used without saying whether more
    /// pages follow.
    RepeatedCursor,
    /// A cursor, link or page repeated an earlier, non-adjacent one: the pass
    /// was going round a loop, so the pages behind it were never reached.
    Cycle,
}

/// What a pass's bookmark means, for [`bookmark_after_pass`].
///
/// Build it with [`ResumeRules::new`] and the setters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResumeRules {
    incremental: bool,
    cursor_bookmark: bool,
    windowed: bool,
    ordered: bool,
    pushes_bookmark: bool,
}

impl ResumeRules {
    /// A full-refresh pass with no bookmark.
    pub fn new() -> Self {
        Self::default()
    }

    /// The bookmark is the largest replication key read.
    pub fn incremental(mut self, on: bool) -> Self {
        self.incremental = on;
        self
    }

    /// The bookmark is the server's cursor for the next unread page.
    pub fn cursor_bookmark(mut self, on: bool) -> Self {
        self.cursor_bookmark = on;
        self
    }

    /// The bookmark is the end of a time window.
    pub fn windowed(mut self, on: bool) -> Self {
        self.windowed = on;
        self
    }

    /// The feed is sorted ascending by the replication key.
    pub fn ordered(mut self, on: bool) -> Self {
        self.ordered = on;
        self
    }

    /// The next run sends the bookmark to the server, which starts there.
    pub fn pushes_bookmark(mut self, on: bool) -> Self {
        self.pushes_bookmark = on;
        self
    }

    /// Unread rows sort after the largest key read and the next request
    /// starts at it, so that key is a safe resume point.
    fn resumes_past_unread(self) -> bool {
        self.incremental && self.ordered && self.pushes_bookmark && !self.windowed
    }
}

/// The bookmark a pass may persist once its page loop ended.
///
/// - A complete pass keeps its bookmark.
/// - A pass cut short by the page cap or a repeated cursor keeps a cursor
///   bookmark (it names the next unread page) and the largest key of an
///   ordered feed whose bookmark the server honours; an incremental or
///   windowed pass otherwise cannot advance safely, and a full refresh keeps
///   none.
/// - A pass that went round a loop keeps only the largest key of an ordered,
///   pushed feed; every other pass refuses, because the pages behind the loop
///   were never read.
///
/// `Err` carries the truncation so each connector words its own error.
pub fn bookmark_after_pass(
    end: PassEnd,
    rules: ResumeRules,
    bookmark: Option<Value>,
) -> Result<Option<Value>, Truncation> {
    let truncation = match end {
        PassEnd::Complete => return Ok(bookmark),
        PassEnd::Truncated(t) => t,
    };
    if rules.resumes_past_unread() {
        return Ok(bookmark);
    }
    match truncation {
        Truncation::Cycle => Err(truncation),
        _ if rules.cursor_bookmark => Ok(bookmark),
        _ if rules.incremental || rules.windowed => Err(truncation),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_resume_table_holds_for_every_end() {
        let bm = Some(json!(5));
        let max = PassEnd::Truncated(Truncation::MaxPages { max: 3 });
        let echo = PassEnd::Truncated(Truncation::RepeatedCursor);
        let cycle = PassEnd::Truncated(Truncation::Cycle);
        let full = ResumeRules::new();
        let inc = ResumeRules::new().incremental(true);
        let ordered = inc.ordered(true).pushes_bookmark(true);
        let cursor = ResumeRules::new().cursor_bookmark(true);

        for rules in [full, inc, ordered, cursor] {
            assert_eq!(
                bookmark_after_pass(PassEnd::Complete, rules, bm.clone()),
                Ok(bm.clone())
            );
        }
        for end in [max, echo] {
            assert_eq!(bookmark_after_pass(end, full, bm.clone()), Ok(None));
            assert!(bookmark_after_pass(end, inc, bm.clone()).is_err());
            assert!(bookmark_after_pass(end, inc.ordered(true), bm.clone()).is_err());
            assert_eq!(
                bookmark_after_pass(end, ordered, bm.clone()),
                Ok(bm.clone())
            );
            assert!(bookmark_after_pass(end, ordered.windowed(true), bm.clone()).is_err());
            assert!(bookmark_after_pass(end, full.windowed(true), bm.clone()).is_err());
            assert_eq!(
                bookmark_after_pass(end, cursor.windowed(true), bm.clone()),
                Ok(bm.clone())
            );
        }
        assert_eq!(
            bookmark_after_pass(max, inc, bm.clone()),
            Err(Truncation::MaxPages { max: 3 })
        );
        assert_eq!(
            bookmark_after_pass(cycle, full, bm.clone()),
            Err(Truncation::Cycle)
        );
        assert!(bookmark_after_pass(cycle, inc, bm.clone()).is_err());
        assert!(bookmark_after_pass(cycle, cursor, bm.clone()).is_err());
        assert!(bookmark_after_pass(cycle, ordered.windowed(true), bm.clone()).is_err());
        assert_eq!(bookmark_after_pass(cycle, ordered, bm.clone()), Ok(bm));
    }
}
