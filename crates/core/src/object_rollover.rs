//! Cross-page accumulation and rollover for object-store sinks (#618).
//!
//! An object-store sink used to write **one object per `write_batch` page**
//! with nothing retained between calls, so a small `batch_size` produced a
//! swarm of tiny objects — the small-files problem that dominates read time on
//! S3/Athena/Spark, where per-object overhead outweighs the bytes. The Parquet
//! sink already accumulates across pages and rolls over on a row *or* byte
//! threshold; this is that logic, extracted so every object-store sink shares
//! one definition rather than four near-copies.
//!
//! ## What this owns and what it does not
//!
//! This is the **decision**, not the I/O: it says when the open object is full
//! and hands back the bytes to upload. Each sink keeps its own upload — the
//! vendor SDKs differ too much to unify here, and pushing the upload behind a
//! trait would buy an abstraction whose only implementors are four call sites.
//!
//! ## Why a byte threshold and not only rows
//!
//! Rows are a poor proxy for object size: 10k rows of a wide table and 10k
//! rows of `{"id":1}` differ by orders of magnitude, so a rows-only cap either
//! writes tiny objects for narrow data or unbounded ones for wide data. The
//! byte threshold is what actually bounds peak memory, since the open object's
//! body is buffered until it rolls.

use serde_json::Value;

/// Accumulates encoded rows for one open object, and says when to roll.
///
/// The sink pushes each page's rows in, takes whatever completed objects come
/// back, and calls [`finish`](Self::finish) at flush time for the partial
/// remainder.
#[derive(Debug)]
pub struct ObjectAccumulator {
    buf: Vec<u8>,
    rows: usize,
    /// Bytes already handed out as parts for the open object, so the byte cap
    /// measures the whole object rather than just the unflushed tail.
    parted_bytes: usize,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    part_bytes: Option<usize>,
}

/// One object's worth of accumulated bytes, ready to upload.
#[derive(Debug, PartialEq, Eq)]
pub struct CompletedObject {
    /// Encoded body (for JSONL: one record per line, trailing newline).
    pub body: Vec<u8>,
    /// Records the body holds — for logging and metrics, and so a caller can
    /// assert no row was lost across a rollover.
    pub rows: usize,
}

/// What a push produced.
///
/// Three outcomes rather than an `Option`, because "a part is ready to upload"
/// and "the object is finished" are different instructions to the sink: the
/// first frees memory mid-object, the second closes it.
#[derive(Debug, PartialEq, Eq)]
pub enum Emit {
    /// Keep accumulating.
    Nothing,
    /// A multipart part is full. Upload it and drop it — this is what keeps
    /// peak memory at O(part size) instead of O(object size) when the object
    /// cap is large or unset.
    Part(Vec<u8>),
    /// The object reached its record/byte cap.
    Object(CompletedObject),
}

impl ObjectAccumulator {
    /// Build an accumulator. `max_rows`/`max_bytes` of `None` or `0` mean "no
    /// limit on this axis"; with neither set, nothing ever rolls and the whole
    /// run lands in one object at `finish`.
    pub fn new(max_rows: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            buf: Vec::new(),
            rows: 0,
            parted_bytes: 0,
            max_rows: max_rows.filter(|n| *n > 0),
            max_bytes: max_bytes.filter(|n| *n > 0),
            part_bytes: None,
        }
    }

    /// Emit [`Emit::Part`]s once the unflushed tail reaches `bytes`, so the
    /// sink can stream them into a multipart upload and free the memory.
    ///
    /// Without this, an object with a large (or absent) byte cap is held whole
    /// in RAM before its single-shot upload — which is what caps output size
    /// by available memory. `0` disables parting.
    pub fn with_part_size(mut self, bytes: usize) -> Self {
        self.part_bytes = (bytes > 0).then_some(bytes);
        self
    }

    /// Bytes of the open object already uploaded as parts.
    pub fn parted_bytes(&self) -> usize {
        self.parted_bytes
    }

    /// Whether any part of the open object has already been uploaded — the
    /// sink needs this to know whether to complete a multipart upload or do a
    /// single-shot put.
    pub fn has_parts(&self) -> bool {
        self.parted_bytes > 0
    }

    /// Rows currently held in the open object.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Bytes currently held in the open object.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing is buffered.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one encoded record (caller-encoded, so the accumulator stays
    /// format-agnostic) and return any object this completed.
    ///
    /// The threshold is checked **after** appending, so a single record larger
    /// than `max_bytes` still lands in its own object rather than being
    /// refused or split — splitting would corrupt it, and refusing would drop
    /// data the source really produced.
    pub fn push_encoded(&mut self, encoded: &[u8]) -> Emit {
        self.buf.extend_from_slice(encoded);
        self.rows += 1;
        // The object cap is checked first: an object that is finished should
        // be closed, not parted one push before closing.
        if self.max_rows.is_some_and(|m| self.rows >= m)
            || self
                .max_bytes
                .is_some_and(|m| self.parted_bytes + self.buf.len() >= m)
        {
            return Emit::Object(self.take());
        }
        if self.part_bytes.is_some_and(|m| self.buf.len() >= m) {
            let part = std::mem::take(&mut self.buf);
            self.parted_bytes += part.len();
            return Emit::Part(part);
        }
        Emit::Nothing
    }

    /// Append one JSON record as an NDJSON line.
    pub fn push_record(&mut self, record: &Value) -> Result<Emit, crate::FaucetError> {
        let mut line = serde_json::to_vec(record)
            .map_err(|e| crate::FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
        line.push(b'\n');
        Ok(self.push_encoded(&line))
    }

    /// Close the open object, if any. Called at `flush`, and on drop-time
    /// finalisation — an object left unfinished is data loss, so a sink must
    /// never skip it.
    ///
    /// Returns `Some` whenever the object holds rows, **including when the
    /// unflushed tail is empty but parts were already uploaded**: a multipart
    /// upload still has to be completed.
    pub fn finish(&mut self) -> Option<CompletedObject> {
        (!self.is_empty()).then(|| self.take())
    }

    fn take(&mut self) -> CompletedObject {
        self.parted_bytes = 0;
        CompletedObject {
            body: std::mem::take(&mut self.buf),
            rows: std::mem::replace(&mut self.rows, 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(i: u64) -> Value {
        json!({ "id": i })
    }

    /// Push `n` records and collect whatever the accumulator emitted.
    fn push_all(acc: &mut ObjectAccumulator, n: u64) -> (Vec<CompletedObject>, Vec<Vec<u8>>) {
        let (mut objects, mut parts) = (Vec::new(), Vec::new());
        for i in 0..n {
            match acc.push_record(&rec(i)).unwrap() {
                Emit::Nothing => {}
                Emit::Part(p) => parts.push(p),
                Emit::Object(o) => objects.push(o),
            }
        }
        (objects, parts)
    }

    #[test]
    fn nothing_rolls_without_a_threshold() {
        // The "one object for the whole run" configuration — what an operator
        // who wants the fewest possible files asks for.
        let mut acc = ObjectAccumulator::new(None, None);
        let (objects, parts) = push_all(&mut acc, 1000);
        assert!(objects.is_empty() && parts.is_empty());
        let done = acc.finish().expect("the remainder must be written");
        assert_eq!(done.rows, 1000);
        assert!(acc.finish().is_none(), "finish must not double-emit");
    }

    #[test]
    fn rows_across_several_pages_land_in_one_object() {
        // The headline fix: a small `batch_size` used to mean one object per
        // page. Three pages of 4 under a 10-row cap must produce one full
        // object plus a remainder, not three objects.
        let mut acc = ObjectAccumulator::new(Some(10), None);
        let mut completed = Vec::new();
        for page in 0..3 {
            for i in 0..4 {
                if let Emit::Object(o) = acc.push_record(&rec(page * 4 + i)).unwrap() {
                    completed.push(o);
                }
            }
        }
        assert_eq!(completed.len(), 1, "one rollover at 10 rows");
        assert_eq!(completed[0].rows, 10);
        let rest = acc.finish().expect("2 rows remain");
        assert_eq!(rest.rows, 2);
        assert_eq!(
            completed[0].rows + rest.rows,
            12,
            "no row may be lost across a rollover"
        );
    }

    #[test]
    fn the_byte_threshold_rolls_independently_of_rows() {
        // Rows are a poor proxy for size; this is the axis that actually
        // bounds peak memory.
        let mut acc = ObjectAccumulator::new(None, Some(32));
        let (objects, _) = push_all(&mut acc, 20);
        assert!(!objects.is_empty(), "a byte cap must roll");
        for obj in &objects {
            assert!(
                obj.body.len() >= 32,
                "an object rolls at or past the threshold, not before: {}",
                obj.body.len()
            );
            assert!(obj.body.len() < 32 + 64, "overshoot bounded by one record");
        }
    }

    #[test]
    fn whichever_threshold_hits_first_wins() {
        let mut acc = ObjectAccumulator::new(Some(1000), Some(24));
        let (objects, _) = push_all(&mut acc, 10);
        assert!(
            !objects.is_empty(),
            "the byte cap must roll even though the row cap is far away"
        );
    }

    #[test]
    fn a_single_oversized_record_gets_its_own_object() {
        // Splitting it would corrupt it and refusing it would drop data the
        // source really produced, so the only correct answer is one object
        // over the threshold.
        let mut acc = ObjectAccumulator::new(None, Some(8));
        let big = json!({ "blob": "x".repeat(500) });
        let Emit::Object(obj) = acc.push_record(&big).unwrap() else {
            panic!("an oversized record must complete an object immediately");
        };
        assert_eq!(obj.rows, 1);
        assert!(obj.body.len() > 8);
    }

    #[test]
    fn a_zero_threshold_means_no_limit_not_roll_every_record() {
        // `0` is the house "no limit" sentinel; reading it as "roll always"
        // would turn an existing config into one object per record.
        let mut acc = ObjectAccumulator::new(Some(0), Some(0));
        let (objects, parts) = push_all(&mut acc, 50);
        assert!(objects.is_empty() && parts.is_empty());
        assert_eq!(acc.finish().expect("remainder").rows, 50);
    }

    #[test]
    fn bodies_are_ndjson_with_a_trailing_newline() {
        let mut acc = ObjectAccumulator::new(Some(2), None);
        acc.push_record(&rec(1)).unwrap();
        let Emit::Object(obj) = acc.push_record(&rec(2)).unwrap() else {
            panic!("rolled");
        };
        let text = String::from_utf8(obj.body).unwrap();
        assert_eq!(text, "{\"id\":1}\n{\"id\":2}\n");
    }

    #[test]
    fn counters_track_the_open_object() {
        let mut acc = ObjectAccumulator::new(Some(10), None);
        assert!(acc.is_empty());
        acc.push_record(&rec(1)).unwrap();
        assert_eq!(acc.rows(), 1);
        assert!(acc.len() > 0);
        assert!(!acc.is_empty());
        acc.finish();
        assert!(acc.is_empty(), "finish resets the open object");
        assert_eq!(acc.rows(), 0);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn parts_bound_peak_memory_for_an_uncapped_object() {
        // The case the byte cap cannot help with: "one object for the whole
        // run". Without parting, the entire object sits in RAM before a
        // single-shot upload, which is what caps output size by memory.
        let mut acc = ObjectAccumulator::new(None, None).with_part_size(64);
        let (objects, parts) = push_all(&mut acc, 200);
        assert!(objects.is_empty(), "no object cap, so nothing rolls");
        assert!(!parts.is_empty(), "parts must be emitted");
        for p in &parts {
            assert!(p.len() >= 64, "a part fills before it is emitted");
            assert!(p.len() < 64 + 64, "and is not held far past the threshold");
        }
        assert!(
            acc.len() < 64,
            "the unflushed tail stays under one part: {}",
            acc.len()
        );
        assert!(acc.has_parts());
        // Every byte is accounted for: parts + tail = the whole object.
        let tail = acc.finish().expect("tail");
        assert_eq!(tail.rows, 200, "rows count the whole object, not the tail");
    }

    #[test]
    fn the_byte_cap_measures_the_whole_object_not_just_the_tail() {
        // A part'd object must still roll at `max_bytes`; measuring only the
        // unflushed tail would make the cap unreachable and produce one
        // unbounded object.
        let mut acc = ObjectAccumulator::new(None, Some(200)).with_part_size(64);
        let (objects, parts) = push_all(&mut acc, 200);
        assert!(!parts.is_empty(), "parts still stream");
        assert!(
            !objects.is_empty(),
            "the object cap must still be reached once parts are counted"
        );
    }

    #[test]
    fn taking_an_object_resets_the_part_counter() {
        // Otherwise the next object would inherit the previous one's parted
        // bytes and roll early — and, worse, `has_parts` would tell the sink
        // to complete a multipart upload that was never started.
        let mut acc = ObjectAccumulator::new(Some(2), None).with_part_size(8);
        acc.push_record(&rec(1)).unwrap();
        let Emit::Object(_) = acc.push_record(&rec(2)).unwrap() else {
            panic!("rolled at 2 rows");
        };
        assert_eq!(acc.parted_bytes(), 0);
        assert!(!acc.has_parts());
    }

    #[test]
    fn an_object_cap_closes_rather_than_parting_on_the_same_push() {
        // A push that both fills a part and finishes the object must close it:
        // emitting a part first would leave a completed object needing a
        // second, empty finish.
        let mut acc = ObjectAccumulator::new(Some(1), None).with_part_size(1);
        assert!(matches!(acc.push_record(&rec(1)).unwrap(), Emit::Object(_)));
    }
}

/// Cross-page record accumulation for **warehouse** sinks (#617).
///
/// The object-store sinks above accumulate encoded bytes; a warehouse sink
/// needs the records themselves, because its commit is a statement it builds
/// from them (`INSERT … SELECT FROM UNNEST`, `COPY`, `INSERT … FORMAT
/// JSONEachRow`). The problem is the same one: the commit unit was the *page*
/// unit, so a small `batch_size` meant one expensive warehouse operation per
/// small page — slow and costly everywhere, and a hard "too many parts"
/// failure on ClickHouse.
///
/// ## Why this is append-only
///
/// Accumulation is applied to the plain `write_batch` path and **not** to
/// `write_batch_idempotent` or `write_batch_partial`:
///
/// - a commit token must land atomically with *its own* page, so deferring the
///   write past the page the token names would make the watermark a lie;
/// - a DLQ needs to know which rows of *this* page failed, which a deferred,
///   merged commit cannot report.
///
/// This is the same carve-out the BigQuery sink makes for `write_batch_partial`
/// (#612), for the same reasons.
#[derive(Debug)]
pub struct PageAccumulator {
    rows: Vec<Value>,
    bytes: usize,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
}

impl PageAccumulator {
    /// `max_rows`/`max_bytes` of `None` or `0` mean "no limit on this axis".
    /// With neither set, the whole run commits once at `flush`.
    pub fn new(max_rows: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            max_rows: max_rows.filter(|n| *n > 0),
            max_bytes: max_bytes.filter(|n| *n > 0),
        }
    }

    /// Records held for the next commit.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether nothing is buffered.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Estimated serialized size of the buffered records.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Add a page and return the group to commit, if one is now full.
    ///
    /// The whole page is added before the check, so a group can overshoot by
    /// at most one page — splitting a page across two commits would break the
    /// DLQ's per-page row indices for any caller that later wants them, and
    /// buys nothing when the threshold is a soft target.
    pub fn push_page(&mut self, page: &[Value]) -> Option<Vec<Value>> {
        for r in page {
            // An estimate, not a measurement: serializing every record twice
            // (once to size it, once to commit it) would cost more than the
            // precision is worth for a rollover threshold.
            self.bytes += estimate_size(r);
            self.rows.push(r.clone());
        }
        let full = self.max_rows.is_some_and(|m| self.rows.len() >= m)
            || self.max_bytes.is_some_and(|m| self.bytes >= m);
        full.then(|| self.take())
    }

    /// Take whatever is buffered, for the `flush`-time commit.
    pub fn finish(&mut self) -> Option<Vec<Value>> {
        (!self.is_empty()).then(|| self.take())
    }

    fn take(&mut self) -> Vec<Value> {
        self.bytes = 0;
        std::mem::take(&mut self.rows)
    }
}

/// Rough serialized size of a JSON value, without serializing it.
///
/// Used only to decide when a commit group is big enough, so it trades
/// accuracy for not walking every value twice. Numbers and booleans are
/// charged a flat width; strings their length plus quoting.
fn estimate_size(v: &Value) -> usize {
    match v {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => 2 + a.iter().map(estimate_size).sum::<usize>() + a.len(),
        Value::Object(m) => {
            2 + m
                .iter()
                .map(|(k, v)| k.len() + 3 + estimate_size(v))
                .sum::<usize>()
        }
    }
}

#[cfg(test)]
mod page_accumulator_tests {
    use super::*;
    use serde_json::json;

    fn page(n: usize) -> Vec<Value> {
        (0..n).map(|i| json!({ "id": i })).collect()
    }

    #[test]
    fn small_pages_merge_into_one_commit_group() {
        // The headline fix: `batch_size` could only ever *split* a page, so
        // ten 10-row pages meant ten warehouse operations.
        let mut acc = PageAccumulator::new(Some(100), None);
        let mut commits = Vec::new();
        for _ in 0..10 {
            if let Some(g) = acc.push_page(&page(10)) {
                commits.push(g);
            }
        }
        assert_eq!(commits.len(), 1, "ten small pages → one commit");
        assert_eq!(commits[0].len(), 100);
        assert!(acc.finish().is_none(), "nothing left over");
    }

    #[test]
    fn a_group_overshoots_by_at_most_one_page() {
        // Pages are never split across commits, so the threshold is a soft
        // target — pinned so a future "exact" rewrite has to justify itself.
        let mut acc = PageAccumulator::new(Some(10), None);
        let g = acc
            .push_page(&page(25))
            .expect("one page over the cap commits");
        assert_eq!(g.len(), 25);
    }

    #[test]
    fn the_byte_cap_rolls_independently_of_rows() {
        let mut acc = PageAccumulator::new(None, Some(200));
        let mut commits = 0;
        for _ in 0..20 {
            if acc.push_page(&page(5)).is_some() {
                commits += 1;
            }
        }
        assert!(commits > 0, "a byte cap must roll");
    }

    #[test]
    fn no_cap_means_one_commit_for_the_whole_run() {
        let mut acc = PageAccumulator::new(None, None);
        for _ in 0..50 {
            assert!(acc.push_page(&page(10)).is_none());
        }
        assert_eq!(acc.finish().expect("flush commits").len(), 500);
    }

    #[test]
    fn a_zero_cap_means_no_limit() {
        // `0` is the house "no limit" sentinel; reading it as "commit always"
        // would restore the per-page behaviour this exists to remove.
        let mut acc = PageAccumulator::new(Some(0), Some(0));
        assert!(acc.push_page(&page(10)).is_none());
        assert_eq!(acc.finish().expect("remainder").len(), 10);
    }

    #[test]
    fn counters_track_the_open_group() {
        let mut acc = PageAccumulator::new(Some(100), None);
        assert!(acc.is_empty());
        acc.push_page(&page(3));
        assert_eq!(acc.len(), 3);
        assert!(acc.bytes() > 0);
        acc.finish();
        assert!(acc.is_empty());
        assert_eq!(acc.bytes(), 0, "finish resets the size estimate too");
    }

    #[test]
    fn size_estimate_grows_with_real_content() {
        // It only has to be monotonic in the data to be useful as a threshold.
        let small = estimate_size(&json!({ "a": 1 }));
        let big = estimate_size(&json!({ "a": 1, "b": "x".repeat(1000) }));
        assert!(big > small + 900, "{small} vs {big}");
        assert!(estimate_size(&json!([1, 2, 3])) > estimate_size(&json!([])));
    }
}
