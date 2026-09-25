//! `GET /v1/local-outputs/{id}/preview` — read back the rows a local sink wrote
//! (#586).
//!
//! This is the **trust / input policy** layer over the generic
//! [preview engine](crate::serve::preview). The engine reads any source; this
//! handler decides which reads are allowed on *this* surface, and there are only
//! four things it does:
//!
//! 1. **Refuse unless the operator opted in.** Off by default
//!    (`--preview-local-outputs`), because returning file contents over HTTP is a
//!    local-testing convenience, not something a normally-exposed control plane
//!    should offer.
//! 2. **Never take a path from the caller.** The request names a *ledger id*.
//!    The path comes from the ledger row — i.e. from the sink that opened it —
//!    so there is no string in the request that could point the read anywhere
//!    else. Path traversal is not defended against here; it is *unrepresentable*,
//!    which is the only defence worth having.
//! 3. **Refuse a file faucet does not own.** The retention GC will not *delete*
//!    a `pre_existing` output — one faucet wrote to but did not create — because
//!    that is somebody else's data. Serving its contents back over HTTP
//!    discloses the same data the delete-side guard exists to protect, so the
//!    read side refuses it too. A guardrail on one verb and not the other is not
//!    a guardrail.
//! 4. **Bound the read.** `row_count_to_load` is resolved through
//!    [`PreviewConfig`](crate::serve::preview::PreviewConfig): omitted → the soft
//!    cap, present → clamped to the hard cap. `row_count_to_load=all` asks for
//!    the whole dataset and gets it only if the operator lifted the ceiling
//!    (`--preview-max-rows 0`); otherwise it is the ceiling, which is the point
//!    of having one.
//!
//! Everything else — opening the file, streaming, stopping early, collecting
//! columns — is the engine's, unchanged, and is what #591 (non-local preview)
//! inherits.
//!
//! ## Mapping a ledger row to a source spec
//!
//! | sink `kind` | source used | why |
//! |---|---|---|
//! | `jsonl` | [`preview::jsonl`](crate::serve::preview::jsonl) | no `faucet-source-jsonl` crate exists; this is the reader |
//! | `csv` | `faucet-source-csv` | round-trips the csv sink's output, embedded newlines included |
//! | `parquet` | `faucet-source-parquet` | reads the first row group(s) only |
//!
//! Any other kind is a `400` naming it, rather than a best-effort guess. `stdout`
//! is not a readable artifact and there is no XML sink, so the three above are
//! the complete set of local file sinks that report their outputs.

use crate::local_outputs::{LocalOutputRecord, LocalOutputState};
use crate::serve::error::ServeError;
use crate::serve::preview::engine::PREVIEW_UNLIMITED_PAGE_ROWS;
use crate::serve::preview::{Capped, PreviewPage, PreviewRequest, RowCap, RowRequest, read_capped};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// `GET /v1/local-outputs/{id}/preview` query string.
///
/// `row_count_to_load` is a string rather than a number so `all` is sayable.
/// Parsing is deferred to [`RowRequest::parse`] so a nonsense value is a `400`
/// naming the parameter rather than axum's generic rejection — and never a
/// silent fall back to the default, which would let a capped read pass for a
/// whole file.
#[derive(Debug, Default, Deserialize)]
pub struct PreviewQuery {
    /// Rows to load: a count, or `all` / `0` for the whole dataset. Omitted → the
    /// server's soft cap. Always clamped by the hard cap, if the server has one.
    pub row_count_to_load: Option<String>,
}

impl PreviewQuery {
    fn requested(&self) -> Result<Option<RowRequest>, ServeError> {
        self.row_count_to_load
            .as_deref()
            .map(RowRequest::parse)
            .transpose()
            .map_err(ServeError::BadConfig)
    }
}

/// `GET /v1/local-outputs/{id}/preview` response body.
#[derive(Debug, Serialize)]
pub struct PreviewResponse {
    /// The ledger id that was previewed.
    pub output_id: String,
    /// The file that was read — echoed so the console can label the panel with
    /// the artifact, not just the id.
    pub path: String,
    /// Connector kind of the writing sink (`jsonl` / `csv` / `parquet`).
    pub kind: String,
    pub dataset_id: String,
    pub pipeline: String,
    /// The run that most recently wrote this file.
    pub run_id: String,
    /// Rows actually read.
    pub rows: Vec<Value>,
    /// Column names across `rows`, in source order. Empty when the records are
    /// not JSON objects.
    pub columns: Vec<String>,
    /// Rows returned. Explicit so a client never has to decide whether
    /// `rows.length` is the answer or an artifact of its own parsing.
    pub row_count: usize,
    /// The bound this request resolved to, after clamping. `null` = unlimited
    /// (the caller asked for everything and the server has no ceiling).
    pub row_limit: Option<usize>,
    /// The server's hard cap, so a client can label its own input honestly.
    /// `null` = no ceiling.
    pub max_rows: Option<usize>,
    /// More of the dataset exists beyond what was returned.
    pub truncated: bool,
    /// Why the read stopped short: `rows` (the row bound), `bytes` (the response
    /// budget), or `time` (the deadline). `null` = this is the whole dataset.
    ///
    /// A whole-dataset request that came back partial *must* say why, or the
    /// caller has no way to tell a complete answer from a clipped one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capped_by: Option<Capped>,
    pub elapsed_ms: u64,
}

/// `GET /v1/local-outputs/{id}/preview` → 200 with the capped page.
///
/// - `403` when previews are disabled on this server.
/// - `404` when the ledger has no such output.
/// - `409` when the file is gone (collected by the retention GC, or removed
///   out-of-band) — an expected end state, reported as such rather than as a
///   500 from a failed `open`.
/// - `400` for a kind that has no reader, or an unparseable `row_count_to_load`.
/// - `422` when the file is there but cannot be parsed (a partial last line from
///   a run that died mid-flush).
pub async fn preview_output(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Query(query): Query<PreviewQuery>,
) -> Result<Json<PreviewResponse>, ServeError> {
    let policy = *state.preview();
    if !policy.enabled {
        return Err(ServeError::Forbidden(DISABLED.into()));
    }
    let record = state
        .history()
        .local_output_get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;

    let rows = policy.resolve_rows(query.requested()?);
    let request = source_spec(&record, rows)?;
    let page = read_capped(&request, &crate::auth_catalog::AuthCatalog::new())
        .await
        .map_err(|e| read_error(&record, e))?;
    audit(&state, &actor, &record, page.rows.len()).await;
    Ok(Json(response(&record, page, rows, policy.max_rows())))
}

/// Record a served preview in the audit log.
///
/// Reads are not audited elsewhere in this control plane, and this one is the
/// exception on purpose: every other read returns *metadata about* a pipeline,
/// while this one returns the pipeline's **data**. "Who read the contents of this
/// file, and how much of it" is the question an audit log exists to answer, and
/// it cannot be reconstructed afterwards from anything else.
///
/// Only successful reads are logged here — a refusal already writes its own
/// entry through the RBAC middleware.
async fn audit(state: &ServerState, actor: &AuthContext, record: &LocalOutputRecord, rows: usize) {
    crate::serve::audit::write(
        state,
        actor,
        "local_output.preview",
        Some(record.run_id.clone()),
        None,
        &format!("output={} kind={} rows={rows}", record.id, record.kind),
    )
    .await;
}

/// The refusal text for a server that did not opt in. Names the flag, because
/// "forbidden" with no next step is the least useful possible answer.
const DISABLED: &str = "local-output preview is disabled on this server — start \
                        `faucet serve` with `--preview-local-outputs` (or set \
                        FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS=true) to enable it. It reads \
                        the contents of files on the server's disk, so it is opt-in and \
                        intended for local testing.";

/// Build the source spec for a ledger row — the *only* place a path enters the
/// engine, and it comes from the ledger, never from the request.
///
/// `batch_size` is set to `rows + 1` on both file sources: `CsvSource` and
/// `ParquetSource` deliberately ignore the trait-level `batch_size` hint in
/// favour of their config field, so setting it here is what actually keeps the
/// first page small.
fn source_spec(record: &LocalOutputRecord, rows: RowCap) -> Result<PreviewRequest, ServeError> {
    // A collected (or externally removed) file has nothing to read. Answering
    // "expired" is the useful answer; letting the source fail to open it would
    // surface as a 500 about a missing path.
    if record.state() == LocalOutputState::Expired {
        return Err(ServeError::Conflict(format!(
            "`{}` was cleaned up by local-output retention — the run record is kept, \
             but the file is gone. Re-run the pipeline to regenerate it.",
            record.path
        )));
    }
    if !record.fs_path().exists() {
        return Err(ServeError::Conflict(format!(
            "`{}` is no longer on disk (removed outside faucet). The ledger row is \
             kept; re-run the pipeline to regenerate the file.",
            record.path
        )));
    }
    // The read-side twin of the GC's `pre_existing` refusal. faucet wrote to this
    // file but did not create it, so its contents are not faucet's to hand out —
    // and the part of it that predates faucet is exactly what the delete-side
    // guard protects. Refusing to *unlink* somebody's export while streaming it
    // back over HTTP would be a guardrail in name only. A `replaced` file —
    // pre-existing, but truncated by faucet — holds only faucet's output, so it
    // is previewed.
    if record.state() == LocalOutputState::External {
        return Err(ServeError::Forbidden(format!(
            "`{}` already existed when faucet first opened it, so faucet wrote to a file \
             it does not own. Its contents are not previewed, for the same reason the \
             retention GC will not delete it. The output is still listed, with state \
             `external`.",
            record.path
        )));
    }
    // The per-page size handed to the source. For a bounded read it is the cap
    // plus the one surplus record the engine uses to detect truncation, so the
    // first page is already enough and nothing further is decoded.
    //
    // For an unlimited read it must **not** be `0`. That is every source's
    // "drain into one page" sentinel, and it is how this handler used to ask
    // jsonl and csv to materialize an entire file into a single `Vec` before the
    // engine's byte budget or deadline — both checked as pages arrive — could
    // look at any of it. Unlimited means unlimited in *rows*; the read is still
    // paged. (`limit: 0` below is the jsonl reader's own "no row limit", which is
    // the part that should be unbounded.)
    let page = match rows {
        RowCap::Rows(n) => n.saturating_add(1),
        RowCap::Unlimited => PREVIEW_UNLIMITED_PAGE_ROWS,
    };
    let row_limit = match rows {
        RowCap::Rows(n) => n.saturating_add(1),
        RowCap::Unlimited => 0,
    };
    let config = match record.kind.as_str() {
        "jsonl" => json!({ "path": record.path, "batch_size": page, "limit": row_limit }),
        "csv" => json!({ "path": record.path, "batch_size": page }),
        // `local_path` is `ParquetLocation`'s snake_case tag; a rolled parquet
        // run records each part as its own ledger row, so this is always one
        // concrete file, never a glob.
        "parquet" => json!({
            "source": { "type": "local_path", "path": record.path },
            "batch_size": page,
        }),
        other => {
            return Err(ServeError::BadConfig(format!(
                "preview is not supported for `{other}` outputs — only the local file \
                 sinks faucet can read back (jsonl, csv, parquet)"
            )));
        }
    };
    Ok(PreviewRequest {
        kind: record.kind.clone(),
        config,
        rows,
    })
}

/// Classify an engine failure for HTTP.
///
/// A read that fails on a file that *is* there is a problem with the file's
/// contents (a partial last line, a truncated parquet footer), which is the
/// caller's data rather than a server fault — `422`, with the connector's own
/// message, which names the line or byte offset. A missing connector is a build
/// choice, so it is a `400` that says so, and the engine's own timeout is a
/// `503` (the read was abandoned, not judged) rather than a verdict on the file.
fn read_error(record: &LocalOutputRecord, err: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match err {
        CliError::UnknownConnector { .. } => ServeError::BadConfig(format!(
            "this build of faucet cannot read `{}` outputs: {err}",
            record.kind
        )),
        // The engine reports its timeout as `Serve` — see
        // `preview::engine::PREVIEW_TIMEOUT`.
        CliError::Serve(m) => ServeError::Unavailable(m),
        other => ServeError::Unprocessable {
            message: format!("could not read `{}`: {other}", record.path),
            details: None,
        },
    }
}

fn response(
    record: &LocalOutputRecord,
    page: PreviewPage,
    rows: RowCap,
    max_rows: Option<usize>,
) -> PreviewResponse {
    PreviewResponse {
        output_id: record.id.clone(),
        path: record.path.clone(),
        kind: record.kind.clone(),
        dataset_id: record.dataset_id.clone(),
        pipeline: record.pipeline.clone(),
        run_id: record.run_id.clone(),
        row_count: page.rows.len(),
        truncated: page.truncated(),
        capped_by: page.capped_by,
        rows: page.rows,
        columns: page.columns,
        row_limit: rows.rows(),
        max_rows,
        elapsed_ms: page.elapsed_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::{LocalOutputObservation, LocalOutputRecord};
    use chrono::Utc;
    use std::path::Path as FsPath;

    fn record(path: &FsPath, kind: &str) -> LocalOutputRecord {
        LocalOutputRecord::new(&LocalOutputObservation {
            path: path.to_path_buf(),
            dataset_uri: format!("file://{}", path.display()),
            dataset_id: "ds-1".into(),
            kind: kind.into(),
            pipeline: "demo".into(),
            row: "default".into(),
            run_id: "run-1".into(),
            pre_existing: false,
            replaced: false,
            retention_days: None,
            observed_at: Utc::now(),
        })
    }

    /// A `viewer` — the lowest role that holds `LocalOutputRead`, so the handler
    /// tests run as the least-privileged principal that can reach this route.
    fn actor() -> AuthContext {
        AuthContext {
            principal: "bob".into(),
            role: crate::serve::rbac::Role::Viewer,
            source_ip: None,
        }
    }

    fn touch(dir: &FsPath, name: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, "{\"a\":1}\n").unwrap();
        p
    }

    #[test]
    fn jsonl_spec_carries_the_ledger_path_and_a_bounded_page() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "out.jsonl");
        let spec = source_spec(&record(&p, "jsonl"), RowCap::Rows(10)).unwrap();
        assert_eq!(spec.kind, "jsonl");
        assert_eq!(spec.config["path"], p.to_string_lossy().as_ref());
        assert_eq!(spec.config["batch_size"], 11);
        assert_eq!(spec.config["limit"], 11, "the reader stops on its own too");
        assert_eq!(spec.rows, RowCap::Rows(10));
    }

    #[test]
    fn csv_spec_targets_the_csv_source() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "rows.csv");
        let spec = source_spec(&record(&p, "csv"), RowCap::Rows(5)).unwrap();
        assert_eq!(spec.kind, "csv");
        assert_eq!(spec.config["path"], p.to_string_lossy().as_ref());
        // CsvSource ignores the trait-level hint, so this field is the cap.
        assert_eq!(spec.config["batch_size"], 6);
    }

    #[test]
    fn parquet_spec_uses_a_single_local_path_never_a_glob() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "part.parquet");
        let spec = source_spec(&record(&p, "parquet"), RowCap::Rows(7)).unwrap();
        assert_eq!(spec.kind, "parquet");
        assert_eq!(spec.config["source"]["type"], "local_path");
        assert_eq!(spec.config["source"]["path"], p.to_string_lossy().as_ref());
        assert!(
            spec.config["source"].get("pattern").is_none(),
            "a glob would let one row expand to many files"
        );
    }

    #[test]
    fn an_unreadable_kind_is_rejected_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "thing.out");
        let err = source_spec(&record(&p, "stdout"), RowCap::Rows(10)).unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("stdout"), "{m}"),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[test]
    fn a_collected_output_is_a_conflict_not_a_failed_open() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "out.jsonl");
        let mut r = record(&p, "jsonl");
        r.deleted_at = Some(Utc::now());
        let err = source_spec(&r, RowCap::Rows(10)).unwrap_err();
        match err {
            ServeError::Conflict(m) => assert!(m.contains("cleaned up"), "{m}"),
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn a_file_removed_out_of_band_is_a_conflict_too() {
        let dir = tempfile::tempdir().unwrap();
        let r = record(&dir.path().join("never-written.jsonl"), "jsonl");
        let err = source_spec(&r, RowCap::Rows(10)).unwrap_err();
        match err {
            ServeError::Conflict(m) => assert!(m.contains("no longer on disk"), "{m}"),
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn an_external_file_is_not_previewable() {
        // `pre_existing` means faucet wrote to a file it did not create. The GC
        // refuses to delete it; serving its contents back over HTTP would
        // disclose the very data that refusal protects, so the read side refuses
        // too — and says why, since the output stays visible in the list.
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "theirs.jsonl");
        let mut r = record(&p, "jsonl");
        r.pre_existing = true;
        assert_eq!(r.state(), LocalOutputState::External);
        match source_spec(&r, RowCap::Rows(10)).unwrap_err() {
            ServeError::Forbidden(m) => {
                assert!(m.contains("does not own"), "{m}");
                assert!(m.contains("external"), "the state stays discoverable: {m}");
            }
            other => panic!("expected Forbidden, got {other:?}"),
        }
    }

    #[test]
    fn a_replaced_file_is_previewable() {
        // faucet truncated a file it did not create: every byte in it is
        // faucet's output, so reading it back discloses nothing that predates
        // faucet. It stays uncollectable (see the sweep tests).
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "overwritten.jsonl");
        let mut r = record(&p, "jsonl");
        r.pre_existing = true;
        r.replaced = true;
        assert_eq!(r.state(), LocalOutputState::Replaced);
        assert!(source_spec(&r, RowCap::Rows(10)).is_ok());
    }

    #[test]
    fn an_abandoned_read_is_unavailable_not_a_500() {
        // The 503 arm: the engine reports its hard timeout as `CliError::Serve`,
        // and abandoning a read is not a verdict on the file — so it must not
        // become a 422 ("your data is malformed") or a 500.
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "out.jsonl");
        let err = read_error(
            &record(&p, "jsonl"),
            crate::error::CliError::Serve("preview abandoned after 60s".into()),
        );
        match err {
            ServeError::Unavailable(m) => assert!(m.contains("abandoned"), "{m}"),
            other => panic!("expected Unavailable, got {other:?}"),
        }
        assert_eq!(
            ServeError::Unavailable(String::new()).status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn an_unreadable_file_is_unprocessable_and_keeps_the_connectors_diagnostic() {
        // A file that is present but cannot be parsed is a fact about the
        // caller's data, and the connector's own message (which names the line or
        // byte offset) is the useful part — so it must survive the mapping.
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "rows.csv");
        let err = read_error(
            &record(&p, "csv"),
            crate::error::CliError::Config("ragged CSV row at line 7".into()),
        );
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("line 7"), "{message}");
                assert!(message.contains("rows.csv"), "{message}");
            }
            other => panic!("expected Unprocessable, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_connector_says_the_build_lacks_it() {
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "part.parquet");
        let err = read_error(
            &record(&p, "parquet"),
            crate::error::CliError::UnknownConnector {
                kind: "source",
                name: "parquet".into(),
                available: "csv".into(),
            },
        );
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("parquet"), "{m}"),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_disabled_server_refuses_before_touching_the_ledger() {
        let state = crate::serve::test_support::test_state();
        assert!(!state.preview().enabled, "off by default");
        let err = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path("whatever".into()),
            axum::extract::Query(PreviewQuery::default()),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Forbidden(m) => {
                assert!(m.contains("--preview-local-outputs"), "{m}")
            }
            other => panic!("expected Forbidden, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unknown_id_is_not_found() {
        let mut config = crate::serve::test_support::test_config();
        config.preview = crate::serve::preview::PreviewConfig::new(true, 100, 1000);
        let state = crate::serve::test_support::state_from(&config);
        let err = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path("no-such-output".into()),
            axum::extract::Query(PreviewQuery::default()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::NotFound), "{err:?}");
    }

    #[tokio::test]
    async fn previews_a_recorded_jsonl_output_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n").unwrap();

        let mut config = crate::serve::test_support::test_config();
        config.preview = crate::serve::preview::PreviewConfig::new(true, 100, 1000);
        let state = crate::serve::test_support::state_from(&config);
        let obs = LocalOutputObservation {
            path: path.clone(),
            dataset_uri: "file:///out.jsonl".into(),
            dataset_id: "ds-1".into(),
            kind: "jsonl".into(),
            pipeline: "demo".into(),
            row: "default".into(),
            run_id: "run-1".into(),
            pre_existing: false,
            replaced: false,
            retention_days: None,
            observed_at: Utc::now(),
        };
        state.history().local_output_record(&obs).await.unwrap();
        let id = crate::local_outputs::ledger::output_id(&path);

        let axum::Json(body) = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(id.clone()),
            axum::extract::Query(PreviewQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(body.output_id, id);
        assert_eq!(body.rows.len(), 2);
        assert_eq!(body.row_count, 2);
        assert_eq!(body.columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(body.row_limit, Some(100));
        assert_eq!(body.max_rows, Some(1000));
        assert!(!body.truncated);
        assert_eq!(body.capped_by, None);
        assert_eq!(body.kind, "jsonl");
        assert_eq!(body.run_id, "run-1");
    }

    #[tokio::test]
    async fn row_count_to_load_is_clamped_to_the_hard_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let body: String = (0..20).map(|i| format!("{{\"i\":{i}}}\n")).collect();
        std::fs::write(&path, body).unwrap();

        let mut config = crate::serve::test_support::test_config();
        config.preview = crate::serve::preview::PreviewConfig::new(true, 5, 3);
        let state = crate::serve::test_support::state_from(&config);
        state
            .history()
            .local_output_record(&LocalOutputObservation {
                path: path.clone(),
                dataset_uri: "file:///out.jsonl".into(),
                dataset_id: "ds-1".into(),
                kind: "jsonl".into(),
                pipeline: "demo".into(),
                row: "default".into(),
                run_id: "run-1".into(),
                pre_existing: false,
                replaced: false,
                retention_days: None,
                observed_at: Utc::now(),
            })
            .await
            .unwrap();

        let axum::Json(body) = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(crate::local_outputs::ledger::output_id(&path)),
            axum::extract::Query(PreviewQuery {
                row_count_to_load: Some("1000".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(body.row_limit, Some(3), "clamped to the hard cap");
        assert_eq!(body.rows.len(), 3);
        assert!(body.truncated, "17 rows were left unread");
        assert_eq!(body.capped_by, Some(Capped::Rows));
    }

    /// Record `path` in the ledger of a preview-enabled server and return both.
    async fn enabled_state_with(
        path: &std::path::Path,
        default_rows: usize,
        max_rows: usize,
    ) -> (crate::serve::state::ServerState, String) {
        let mut config = crate::serve::test_support::test_config();
        config.preview = crate::serve::preview::PreviewConfig::new(true, default_rows, max_rows);
        let state = crate::serve::test_support::state_from(&config);
        state
            .history()
            .local_output_record(&LocalOutputObservation {
                path: path.to_path_buf(),
                dataset_uri: "file:///out.jsonl".into(),
                dataset_id: "ds-1".into(),
                kind: "jsonl".into(),
                pipeline: "demo".into(),
                row: "default".into(),
                run_id: "run-1".into(),
                pre_existing: false,
                replaced: false,
                retention_days: None,
                observed_at: Utc::now(),
            })
            .await
            .unwrap();
        (state, crate::local_outputs::ledger::output_id(path))
    }

    fn write_rows(dir: &FsPath, rows: usize) -> std::path::PathBuf {
        let p = dir.join("out.jsonl");
        let body: String = (0..rows).map(|i| format!("{{\"i\":{i}}}\n")).collect();
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn an_unlimited_read_is_unlimited_in_rows_but_still_paged() {
        // The regression that made the unlimited path OOM: `batch_size: 0` is
        // every source's "drain into one page" sentinel, so the engine's byte and
        // time bounds — both checked as pages arrive — never got a chance to act
        // on a 40 GiB file. The row limit is the part that must be unbounded.
        let dir = tempfile::tempdir().unwrap();
        let p = touch(dir.path(), "out.jsonl");
        let spec = source_spec(&record(&p, "jsonl"), RowCap::Unlimited).unwrap();
        assert_eq!(
            spec.config["batch_size"], PREVIEW_UNLIMITED_PAGE_ROWS,
            "an unbounded page defeats every bound the engine has"
        );
        assert_ne!(spec.config["batch_size"], 0);
        assert_eq!(spec.config["limit"], 0, "no *row* limit, though");
        assert_eq!(spec.rows, RowCap::Unlimited);
    }

    #[test]
    fn every_kind_is_paged_under_an_unlimited_read() {
        // csv takes its page size from the same field, and parquet from its own
        // `batch_size`; none of them may be handed the drain-everything sentinel.
        let dir = tempfile::tempdir().unwrap();
        for kind in ["jsonl", "csv", "parquet"] {
            let p = touch(dir.path(), &format!("out.{kind}"));
            let spec = source_spec(&record(&p, kind), RowCap::Unlimited).unwrap();
            assert_eq!(
                spec.config["batch_size"], PREVIEW_UNLIMITED_PAGE_ROWS,
                "{kind} was handed an unbounded page"
            );
        }
    }

    #[tokio::test]
    async fn row_count_to_load_all_returns_the_whole_dataset_when_no_ceiling_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_rows(dir.path(), 1_200);
        // `max_rows: 0` — the operator lifted the ceiling.
        let (state, id) = enabled_state_with(&path, 500, 0).await;

        let axum::Json(body) = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(id),
            axum::extract::Query(PreviewQuery {
                row_count_to_load: Some("all".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(body.rows.len(), 1_200, "every row, not the soft cap");
        assert_eq!(body.row_count, 1_200);
        assert_eq!(body.row_limit, None, "null = unlimited");
        assert_eq!(body.max_rows, None);
        assert!(!body.truncated);
        assert_eq!(body.capped_by, None);
    }

    #[tokio::test]
    async fn row_count_to_load_all_is_still_clamped_by_a_configured_ceiling() {
        // The hard cap is not negotiable from the request side — that is the
        // whole reason it exists.
        let dir = tempfile::tempdir().unwrap();
        let path = write_rows(dir.path(), 1_200);
        let (state, id) = enabled_state_with(&path, 100, 250).await;

        let axum::Json(body) = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(id),
            axum::extract::Query(PreviewQuery {
                row_count_to_load: Some("all".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(body.rows.len(), 250);
        assert_eq!(body.row_limit, Some(250));
        assert!(body.truncated);
        assert_eq!(body.capped_by, Some(Capped::Rows));
    }

    #[tokio::test]
    async fn zero_is_the_same_request_as_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_rows(dir.path(), 40);
        let (state, id) = enabled_state_with(&path, 5, 0).await;

        let axum::Json(body) = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(id),
            axum::extract::Query(PreviewQuery {
                row_count_to_load: Some("0".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(body.rows.len(), 40);
        assert_eq!(body.row_limit, None);
    }

    #[tokio::test]
    async fn an_unparseable_row_count_is_a_400_naming_the_parameter() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_rows(dir.path(), 3);
        let (state, id) = enabled_state_with(&path, 100, 1000).await;

        let err = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(id),
            axum::extract::Query(PreviewQuery {
                row_count_to_load: Some("lots".into()),
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("row_count_to_load"), "{m}"),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_malformed_file_is_unprocessable_not_internal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, "{\"a\":1}\n{oops\n").unwrap();

        let mut config = crate::serve::test_support::test_config();
        config.preview = crate::serve::preview::PreviewConfig::new(true, 100, 1000);
        let state = crate::serve::test_support::state_from(&config);
        state
            .history()
            .local_output_record(&LocalOutputObservation {
                path: path.clone(),
                dataset_uri: "file:///out.jsonl".into(),
                dataset_id: "ds-1".into(),
                kind: "jsonl".into(),
                pipeline: "demo".into(),
                row: "default".into(),
                run_id: "run-1".into(),
                pre_existing: false,
                replaced: false,
                retention_days: None,
                observed_at: Utc::now(),
            })
            .await
            .unwrap();

        let err = preview_output(
            axum::extract::State(state),
            axum::extract::Extension(actor()),
            axum::extract::Path(crate::local_outputs::ledger::output_id(&path)),
            axum::extract::Query(PreviewQuery::default()),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("line 2"), "{message}")
            }
            other => panic!("expected Unprocessable, got {other:?}"),
        }
    }
}
