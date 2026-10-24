//! The generic half of the preview engine (#586).
//!
//! One operation, no knowledge of *why* it was asked for:
//!
//! ```text
//! PreviewRequest { kind, config, rows }  →  PreviewPage { rows, columns, capped_by }
//! ```
//!
//! It builds the connector through the same registry `faucet run` uses, drives
//! [`Source::stream_pages`], and stops as soon as one of its bounds is reached.
//! Nothing here knows about local files, the local-output ledger, or the dev
//! flag — that is the caller's policy (see [`super`]).
//!
//! ## Why the row cap is enforced by *stopping*, not by truncating
//!
//! `rows + 1` records are read and then the stream is dropped. Every file source
//! in the workspace streams lazily (`async_stream::try_stream!` over a
//! `BufReader`, or an Arrow `ParquetRecordBatchStream`), so dropping the stream
//! stops the read: a 500-row preview of a 4 GiB file touches the first few
//! kilobytes. Reading the *whole* result set and slicing it would be the same
//! API with none of the point — and it is what makes "show me more rows" cheap
//! enough that this engine needs no offset or cursor (see [`super`]).
//!
//! The extra record is what makes truncation an observation instead of a guess:
//! "there is at least one more row" is knowable for free, and "exactly `rows`
//! rows" is otherwise indistinguishable from "the file happens to have `rows`
//! rows".
//!
//! ## Three bounds, because "unlimited" must still terminate
//!
//! A [`RowCap::Unlimited`] read — the whole-dataset preview — has no row bound,
//! so the other two do the work:
//!
//! | bound | limit | on hit |
//! |---|---|---|
//! | rows | [`RowCap`] | [`Capped::Rows`] |
//! | response size | [`Bounds::max_bytes`] | [`Capped::Bytes`] |
//! | wall clock | [`Bounds::deadline`] | [`Capped::Time`] |
//!
//! All three produce a **partial answer that says it is partial**, never an
//! error: asking for a whole dataset that does not fit gets you as much of it as
//! fits, plus the reason it stopped. Only [`Bounds::hard_timeout`] — a single
//! page that never returns at all — fails the request outright, because there is
//! nothing partial to hand back.
//!
//! ### The bounds only work if every page is bounded
//!
//! Both are checked *as pages arrive*, so they can only interrupt a read that
//! arrives in pieces. Ask a source for one unbounded page and it will hand back
//! the whole file in a single `Vec` before either bound is ever consulted —
//! which is precisely what this engine used to do on the unlimited path
//! (`batch_size: 0`, every source's "drain into one page" sentinel), turning a
//! 40 GiB `out.jsonl` into an OOM while the docs promised a truncated answer.
//!
//! So the engine **never requests an unbounded page**: an unlimited read is
//! unlimited in *rows*, paged at [`PREVIEW_UNLIMITED_PAGE_ROWS`]. The residual,
//! stated plainly because it is not something this layer can fix: a source that
//! ignores the page-size hint and materializes its whole result set anyway (the
//! default [`Source::stream_pages`] does exactly that, via `fetch_with_context`)
//! is bounded by that source, not by us. It matters for #591's remote sources,
//! not for the three file readers here, all of which page lazily.

use crate::auth_catalog::AuthCatalog;
use crate::error::{CliError, CliResult};
use faucet_core::Source;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::RowCap;

/// Wall-clock budget for one preview read. On expiry the rows already read are
/// returned as a [`Capped::Time`] page.
///
/// A preview is an interactive request: it must answer, and 30 seconds is long
/// past the point where a person is still waiting for a table.
pub const PREVIEW_DEADLINE: Duration = Duration::from_secs(30);

/// How long the engine waits, past the deadline, to learn whether the stream
/// has ended (a complete answer) or has more (a time-capped one).
const END_PROBE: Duration = Duration::from_millis(100);

/// Absolute ceiling on one read, after which the request fails.
///
/// [`PREVIEW_DEADLINE`] is checked between pages, so it cannot interrupt a
/// single page that never completes (a stalled remote source, for #591). This
/// backstop can. It is deliberately generous — reaching it means something is
/// wrong, not slow.
pub const PREVIEW_HARD_TIMEOUT: Duration = Duration::from_secs(60);

/// Approximate response-size budget for the rows of one preview.
///
/// The real bound on an unlimited read: 64 MiB of JSON is already far past what
/// a browser will render usefully, and it keeps a "preview the whole dataset" on
/// a 40 GiB file from trying to buffer 40 GiB. Measured with a structural size
/// estimate rather than by serializing each record, so the intent is a bound of
/// the right order of magnitude, not an exact content-length.
pub const PREVIEW_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Page size for an [unlimited](RowCap::Unlimited) read.
///
/// An unlimited preview is unlimited in **rows**, not in page size. Requesting
/// one unbounded page (`batch_size: 0`) would hand the engine the entire file in
/// a single `Vec` before the byte budget or the deadline could look at it, so
/// "unlimited" has to mean "paged, without a row ceiling".
/// [`faucet_core::DEFAULT_BATCH_SIZE`] is the same cadence the pipeline itself
/// streams at.
pub const PREVIEW_UNLIMITED_PAGE_ROWS: usize = faucet_core::DEFAULT_BATCH_SIZE;

/// The non-row bounds on one read.
///
/// Separated from the constants above, and passed in rather than read from them,
/// so every bound has a test that exercises the **real** loop: a 30-second
/// deadline and a 64 MiB budget are not things a unit test can reach otherwise,
/// and an untested bound is a bound that quietly stops working.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// Approximate response-size budget for the returned rows.
    pub max_bytes: usize,
    /// Wall-clock budget. On expiry the read returns what it has, as
    /// [`Capped::Time`].
    pub deadline: Duration,
    /// Ceiling on the whole read, after which it fails rather than returning a
    /// partial answer. Guards a single page that never completes.
    pub hard_timeout: Duration,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            max_bytes: PREVIEW_MAX_BYTES,
            deadline: PREVIEW_DEADLINE,
            hard_timeout: PREVIEW_HARD_TIMEOUT,
        }
    }
}

/// What to read: a source spec plus the bound, already resolved against the
/// server's caps by the caller ([`super::PreviewConfig::resolve_rows`]).
#[derive(Debug, Clone)]
pub struct PreviewRequest {
    /// Source connector kind (`"csv"`, `"parquet"`, `"jsonl"`, and — for #591 —
    /// any other registered source).
    pub kind: String,
    /// That connector's own config, as it would appear under `source.config:`.
    pub config: Value,
    /// Maximum rows to return, or [`RowCap::Unlimited`] for the whole dataset.
    pub rows: RowCap,
}

/// Why a read stopped early. `None` on the response means "this is the whole
/// dataset".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capped {
    /// The row cap was reached and at least one more row exists.
    Rows,
    /// The response-size budget was reached ([`PREVIEW_MAX_BYTES`]).
    Bytes,
    /// The deadline was reached ([`PREVIEW_DEADLINE`]).
    Time,
}

impl Capped {
    /// One clause a client can render verbatim.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::Bytes => "bytes",
            Self::Time => "time",
        }
    }
}

/// One page of rows, plus what the caller needs to render a table honestly.
#[derive(Debug, Clone)]
pub struct PreviewPage {
    /// The records, at most [`PreviewRequest::rows`] of them.
    pub rows: Vec<Value>,
    /// Column names across `rows`, in the order the records present them — the
    /// table header. Empty when the records are not JSON objects (a scalar or
    /// array per line is legal), in which case a caller should render the raw
    /// value.
    pub columns: Vec<String>,
    /// Why the read stopped short of the end of the dataset, or `None` when this
    /// *is* the whole dataset. Observed, not inferred.
    pub capped_by: Option<Capped>,
    /// Pages pulled from the source before a bound was reached — a cheap signal
    /// that the read really did stop early.
    pub pages_read: usize,
    pub elapsed_ms: u64,
}

impl PreviewPage {
    /// Whether more of the dataset exists beyond what was returned.
    pub fn truncated(&self) -> bool {
        self.capped_by.is_some()
    }
}

/// Read through the source `req.kind` describes, under `req.rows` and the
/// engine's default [`Bounds`].
pub async fn read_capped(req: &PreviewRequest, auth: &AuthCatalog) -> CliResult<PreviewPage> {
    read_capped_with(req, auth, Bounds::default()).await
}

/// [`read_capped`] with explicit bounds — the form the bound tests drive, so
/// each of the three is exercised in the real loop rather than by inspection.
pub async fn read_capped_with(
    req: &PreviewRequest,
    auth: &AuthCatalog,
    bounds: Bounds,
) -> CliResult<PreviewPage> {
    let started = Instant::now();
    let page = tokio::time::timeout(bounds.hard_timeout, read_inner(req, auth, started, bounds))
        .await
        .map_err(|_| {
            CliError::Serve(format!(
                "preview abandoned after {:?} reading a `{}` source — a single page never \
                 returned",
                bounds.hard_timeout, req.kind
            ))
        })??;
    Ok(PreviewPage {
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        ..page
    })
}

async fn read_inner(
    req: &PreviewRequest,
    auth: &AuthCatalog,
    started: Instant,
    bounds: Bounds,
) -> CliResult<PreviewPage> {
    // One past the cap: enough to *know* whether more rows exist. An unlimited
    // read has no such number — the byte and time bounds are what stop it.
    let want = match req.rows {
        RowCap::Rows(n) => Some(n.max(1).saturating_add(1)),
        RowCap::Unlimited => None,
    };
    let source = build_source(&req.kind, req.config.clone(), auth).await?;

    let mut rows: Vec<Value> = Vec::new();
    if let Some(want) = want {
        // Bounded ask: pre-size. Unbounded reads grow as they go rather than
        // reserving for a file whose size is not yet known.
        rows.reserve(want.min(4096));
    }
    let mut pages_read = 0usize;
    let mut bytes = 0usize;
    let mut capped_by = None;
    {
        let context = HashMap::new();
        // The per-page hint. Never `0` ("drain into one page"): a bound that is
        // checked as pages arrive cannot interrupt a read that arrives all at
        // once. An unlimited read is unlimited in rows, paged all the same.
        let hint = want.unwrap_or(PREVIEW_UNLIMITED_PAGE_ROWS);
        let mut pages = source.stream_pages(&context, hint);
        'outer: while let Some(page) = pages.next().await {
            let page = page?;
            pages_read += 1;
            for record in page.records {
                // Enough to answer *and* to prove there is more. Whether that
                // proof arrived is decided after the loop, not here: a source
                // that stops exactly at `want` (because it was told to read no
                // further) has still handed over the surplus record.
                if want.is_some_and(|want| rows.len() >= want) {
                    break 'outer;
                }
                if bytes >= bounds.max_bytes {
                    capped_by = Some(Capped::Bytes);
                    break 'outer;
                }
                bytes += approx_bytes(&record);
                rows.push(record);
            }
            // Checked between pages: a bound that can only be observed after a
            // page completes is still a bound, and it keeps the check off the
            // per-record path.
            if started.elapsed() >= bounds.deadline {
                // Only a capped read if more was actually coming: a stream that
                // ends right after its last page returned the whole dataset.
                match tokio::time::timeout(END_PROBE, pages.next()).await {
                    Ok(None) => {}
                    Ok(Some(Err(e))) => return Err(e.into()),
                    Ok(Some(Ok(_))) | Err(_) => capped_by = Some(Capped::Time),
                }
                break;
            }
        }
        // `pages` is dropped here — that is what stops the underlying read.
    }

    // The read-one-extra trick, resolved. Holding `want` rows means the surplus
    // record exists, which is the *observation* that more of the dataset is
    // there; it is not part of the answer. This is deliberately decided on the
    // row count rather than on how the loop exited, because the two are not the
    // same event: the source may end its own stream at `want` without the engine
    // ever seeing a further record. Deciding it there is what made a file of
    // exactly `cap + 1` rows report `cap + 1` rows and `truncated: false`.
    if let Some(want) = want.filter(|want| rows.len() >= *want) {
        capped_by = Some(Capped::Rows);
        rows.truncate(want.saturating_sub(1));
    }
    Ok(PreviewPage {
        columns: columns_of(&rows),
        rows,
        capped_by,
        pages_read,
        elapsed_ms: 0,
    })
}

/// Resolve a source kind to a connector.
///
/// Everything goes through the shared registry (so plugin-registered sources and
/// the `auth:` catalog work exactly as they do in a run) except `jsonl`, which
/// has no connector crate — see [`super::jsonl`].
async fn build_source(kind: &str, config: Value, auth: &AuthCatalog) -> CliResult<Box<dyn Source>> {
    if kind == super::jsonl::KIND {
        let cfg: super::jsonl::JsonLinesConfig = serde_json::from_value(config)
            .map_err(|e| CliError::Config(format!("invalid jsonl preview config: {e}")))?;
        return Ok(Box::new(super::jsonl::JsonLinesSource::new(cfg)));
    }
    crate::registry::build_source(kind, config, auth, None).await
}

/// Rough serialized size of one record, without allocating.
///
/// An estimate on purpose: the byte budget wants a bound of the right order of
/// magnitude, and serializing every record to measure it would double the work
/// of the read it is protecting.
fn approx_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        // Plus the quotes; escaping is ignored (it can only make the real size
        // larger, so the budget stays conservative in the safe direction).
        Value::String(s) => s.len() + 2,
        Value::Array(items) => 2 + items.len() + items.iter().map(approx_bytes).sum::<usize>(),
        Value::Object(map) => {
            2 + map
                .iter()
                .map(|(k, v)| k.len() + 4 + approx_bytes(v))
                .sum::<usize>()
        }
    }
}

/// Column names across `rows`, de-duplicated, in the order the records present
/// them.
///
/// "The order the records present them" is `serde_json::Map`'s iteration order,
/// which is alphabetical unless something in the dependency tree turns on
/// `preserve_order`. Nothing here depends on which: the union is stable and
/// complete either way, and the header always matches the keys the rows actually
/// carry, because it is computed from those rows.
///
/// Ragged records (a field only some rows carry) contribute their extra keys at
/// the end rather than being dropped — a preview that hid a column would be
/// actively misleading about what was written.
fn columns_of(rows: &[Value]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::new();
    for row in rows {
        if let Value::Object(map) = row {
            for key in map.keys() {
                if seen.insert(key.as_str()) {
                    columns.push(key.clone());
                }
            }
        }
    }
    columns
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_jsonl(dir: &std::path::Path, rows: usize) -> String {
        let mut body = String::new();
        for i in 0..rows {
            body.push_str(&format!("{{\"i\":{i},\"name\":\"r{i}\"}}\n"));
        }
        let p = dir.join("out.jsonl");
        std::fs::write(&p, body).unwrap();
        p.to_string_lossy().to_string()
    }

    fn request(path: &str, rows: RowCap) -> PreviewRequest {
        PreviewRequest {
            kind: "jsonl".into(),
            config: serde_json::json!({ "path": path, "batch_size": 2 }),
            rows,
        }
    }

    #[tokio::test]
    async fn reads_up_to_the_cap_and_reports_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 50);
        let page = read_capped(&request(&path, RowCap::Rows(10)), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 10);
        assert!(page.truncated(), "50 rows behind a cap of 10");
        assert_eq!(page.capped_by, Some(Capped::Rows));
        assert_eq!(page.columns, vec!["i".to_string(), "name".to_string()]);
        // 11 records at 2 per page = 6 pages; nowhere near the 25 the whole
        // file would take.
        assert!(page.pages_read <= 6, "read did not stop early: {page:?}");
    }

    #[tokio::test]
    async fn a_file_shorter_than_the_cap_is_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 3);
        let page = read_capped(&request(&path, RowCap::Rows(100)), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 3);
        assert!(!page.truncated());
        assert_eq!(page.capped_by, None);
    }

    #[tokio::test]
    async fn a_file_exactly_the_cap_is_not_truncated() {
        // The off-by-one that a naive `rows.len() == cap` check gets wrong.
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 10);
        let page = read_capped(&request(&path, RowCap::Rows(10)), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 10);
        assert_eq!(
            page.capped_by, None,
            "exactly `cap` rows is a complete read"
        );
    }

    #[tokio::test]
    async fn a_file_of_exactly_one_more_row_than_the_cap_is_truncated() {
        // The regression that hid behind "the loop broke on a surplus record":
        // when the source is told to read `cap + 1` and the file has exactly
        // that, the stream ends without the engine seeing a further record — and
        // the surplus row must still be recognised as proof, not returned as an
        // extra row alongside `truncated: false`.
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 11);
        let req = PreviewRequest {
            kind: "jsonl".into(),
            // `limit` is what the real handler sets: the cap plus the surplus.
            config: serde_json::json!({ "path": path, "batch_size": 11, "limit": 11 }),
            rows: RowCap::Rows(10),
        };
        let page = read_capped(&req, &AuthCatalog::new()).await.unwrap();
        assert_eq!(page.rows.len(), 10, "the surplus row is evidence, not data");
        assert_eq!(page.capped_by, Some(Capped::Rows));
        assert!(page.truncated());
    }

    #[tokio::test]
    async fn an_unlimited_read_returns_the_whole_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 2_500);
        let page = read_capped(&request(&path, RowCap::Unlimited), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 2_500);
        assert_eq!(
            page.capped_by, None,
            "nothing was left behind, so nothing should claim it was"
        );
        assert_eq!(page.rows[2_499]["i"], 2_499);
    }

    #[tokio::test]
    async fn an_empty_file_previews_as_zero_rows_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 0);
        let page = read_capped(&request(&path, RowCap::Rows(10)), &AuthCatalog::new())
            .await
            .unwrap();
        assert!(page.rows.is_empty());
        assert!(page.columns.is_empty());
        assert!(!page.truncated());
    }

    #[tokio::test]
    async fn a_one_row_cap_still_returns_a_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 5);
        let page = read_capped(&request(&path, RowCap::Rows(1)), &AuthCatalog::new())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.capped_by, Some(Capped::Rows));
    }

    #[tokio::test]
    async fn a_source_error_propagates_rather_than_returning_an_empty_page() {
        let req = request("/definitely/not/here.jsonl", RowCap::Rows(10));
        let err = read_capped(&req, &AuthCatalog::new()).await.unwrap_err();
        assert!(err.to_string().contains("failed to open"), "{err}");
    }

    #[tokio::test]
    async fn an_unknown_kind_is_an_error_not_a_panic() {
        let req = PreviewRequest {
            kind: "not-a-connector".into(),
            config: serde_json::json!({}),
            rows: RowCap::Rows(10),
        };
        assert!(read_capped(&req, &AuthCatalog::new()).await.is_err());
    }

    #[tokio::test]
    async fn a_bad_jsonl_config_is_a_config_error() {
        let req = PreviewRequest {
            kind: "jsonl".into(),
            config: serde_json::json!({ "nope": 1 }),
            rows: RowCap::Rows(10),
        };
        let err = read_capped(&req, &AuthCatalog::new()).await.unwrap_err();
        assert!(matches!(err, CliError::Config(_)), "{err:?}");
    }

    #[test]
    fn columns_union_keeps_first_seen_order_and_ragged_keys() {
        let rows = vec![
            serde_json::json!({"a": 1, "b": 2}),
            serde_json::json!({"a": 3, "z": 4}),
        ];
        assert_eq!(columns_of(&rows), vec!["a", "b", "z"]);
    }

    #[test]
    fn columns_are_empty_for_non_object_records() {
        let rows = vec![serde_json::json!(1), serde_json::json!([1, 2])];
        assert!(columns_of(&rows).is_empty());
    }

    #[tokio::test]
    async fn non_object_records_survive_with_no_columns() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("scalars.jsonl");
        std::fs::write(&p, "1\n2\n").unwrap();
        let req = PreviewRequest {
            kind: "jsonl".into(),
            config: serde_json::json!({ "path": p.to_string_lossy() }),
            rows: RowCap::Rows(10),
        };
        let page = read_capped(&req, &AuthCatalog::new()).await.unwrap();
        assert_eq!(page.rows.len(), 2);
        assert!(page.columns.is_empty());
    }

    #[test]
    fn approx_bytes_scales_with_the_record() {
        // Not exact — but it must grow with the payload, or the byte budget is
        // decoration.
        let small = serde_json::json!({"a": 1});
        let big = serde_json::json!({"a": "x".repeat(10_000)});
        assert!(approx_bytes(&small) < 40);
        assert!(approx_bytes(&big) > 10_000);
        // Nesting is counted, not skipped.
        let nested = serde_json::json!({"a": [{"b": "yy"}, {"b": "zz"}]});
        assert!(approx_bytes(&nested) > approx_bytes(&small));
    }

    /// The spec the **handler** builds for an unlimited read, so these tests
    /// exercise the page size production actually uses rather than a convenient
    /// one. Mirrors `handlers::preview::source_spec`.
    fn unlimited_request(path: &str) -> PreviewRequest {
        PreviewRequest {
            kind: "jsonl".into(),
            config: serde_json::json!({
                "path": path,
                "batch_size": PREVIEW_UNLIMITED_PAGE_ROWS,
                "limit": 0,
            }),
            rows: RowCap::Unlimited,
        }
    }

    #[tokio::test]
    async fn the_byte_budget_bounds_an_unlimited_read() {
        // The property that makes "preview the whole dataset" safe to offer: a
        // dataset past the budget comes back as a partial answer that says so.
        //
        // Driven through the *handler's* page size and a small budget, rather
        // than through `batch_size: 1` and the real 64 MiB — the previous version
        // of this test did the latter and so passed green while production, which
        // sent `batch_size: 0`, materialized the whole file before the budget was
        // ever consulted.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fat.jsonl");
        let blob = "x".repeat(4096);
        let body: String = (0..500)
            .map(|i| format!("{{\"i\":{i},\"blob\":\"{blob}\"}}\n"))
            .collect();
        std::fs::write(&p, body).unwrap();

        let bounds = Bounds {
            max_bytes: 64 * 1024,
            ..Bounds::default()
        };
        let page = read_capped_with(
            &unlimited_request(&p.to_string_lossy()),
            &AuthCatalog::new(),
            bounds,
        )
        .await
        .unwrap();
        assert_eq!(page.capped_by, Some(Capped::Bytes), "{:?}", page.capped_by);
        assert!(page.rows.len() < 500, "the read must stop before EOF");
        assert!(!page.rows.is_empty(), "…but still return what it read");
        assert!(page.truncated());
    }

    #[tokio::test]
    async fn an_unbounded_page_would_defeat_the_byte_budget() {
        // Why `PREVIEW_UNLIMITED_PAGE_ROWS` exists, pinned as a test rather than
        // as a comment. `batch_size: 0` makes the jsonl reader hand over the
        // whole file in one page; the budget is checked as pages arrive, so it
        // cannot stop what has already arrived. Nothing in production sends 0 —
        // `every_kind_is_paged_under_an_unlimited_read` holds that line — and
        // this documents the consequence if it ever did.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("all-at-once.jsonl");
        let body: String = (0..500).map(|i| format!("{{\"i\":{i}}}\n")).collect();
        std::fs::write(&p, body).unwrap();

        let req = PreviewRequest {
            kind: "jsonl".into(),
            // The sentinel the handler must never send.
            config: serde_json::json!({ "path": p.to_string_lossy(), "batch_size": 0 }),
            rows: RowCap::Unlimited,
        };
        let page = read_capped_with(
            &req,
            &AuthCatalog::new(),
            Bounds {
                max_bytes: 64,
                ..Bounds::default()
            },
        )
        .await
        .unwrap();
        // The budget did stop *accumulation* mid-page, but the source had already
        // built all 500 records — the memory the budget was meant to bound.
        assert_eq!(page.pages_read, 1, "one page: the whole file");
        assert!(page.rows.len() < 500);
    }

    #[tokio::test]
    async fn the_deadline_returns_a_partial_answer_rather_than_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 5_000);
        let bounds = Bounds {
            deadline: Duration::ZERO,
            ..Bounds::default()
        };
        let page = read_capped_with(&unlimited_request(&path), &AuthCatalog::new(), bounds)
            .await
            .unwrap();
        assert_eq!(page.capped_by, Some(Capped::Time));
        assert!(page.truncated());
        // One page's worth, not zero rows and not the file: the deadline is
        // checked *after* a page, so the caller always gets something back.
        assert_eq!(page.rows.len(), PREVIEW_UNLIMITED_PAGE_ROWS);
    }

    #[tokio::test]
    async fn a_dataset_that_ends_on_the_deadline_page_is_not_reported_capped() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), 10);
        let bounds = Bounds {
            deadline: Duration::ZERO,
            ..Bounds::default()
        };
        let page = read_capped_with(&unlimited_request(&path), &AuthCatalog::new(), bounds)
            .await
            .unwrap();
        assert_eq!(page.capped_by, None);
        assert!(!page.truncated());
        assert_eq!(page.rows.len(), 10);
    }

    // The hard timeout itself (the `tokio::time::timeout` wrapper) has no unit
    // test here: making it fire deterministically needs a source that never
    // yields, and the only injection point — `PluginRegistry::install` — is a
    // process-wide `OnceLock` that a lib-test binary cannot claim without
    // poisoning it for every other test. Racing it against a real file read
    // (`hard_timeout: ZERO`) passes or fails on timer granularity, which is worse
    // than no test. What it maps to over HTTP *is* covered, deterministically:
    // see `handlers::preview::tests::an_abandoned_read_is_unavailable_not_a_500`.

    #[test]
    fn the_unlimited_page_size_is_never_the_drain_everything_sentinel() {
        assert_ne!(PREVIEW_UNLIMITED_PAGE_ROWS, 0);
    }

    #[test]
    fn capped_by_serializes_as_a_client_readable_word() {
        assert_eq!(serde_json::to_string(&Capped::Rows).unwrap(), "\"rows\"");
        assert_eq!(serde_json::to_string(&Capped::Bytes).unwrap(), "\"bytes\"");
        assert_eq!(serde_json::to_string(&Capped::Time).unwrap(), "\"time\"");
        assert_eq!(Capped::Time.as_str(), "time");
    }
}
