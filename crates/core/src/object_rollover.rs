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
