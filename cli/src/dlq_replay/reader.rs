//! Read a DLQ location (a local JSONL file, a directory of `*.jsonl`, or a
//! glob) back into DLQ envelopes, and expose it as a [`Source`] so `faucet
//! dlq replay` can feed the unwrapped original payloads through the normal
//! pipeline path.
//!
//! The line-parsing core ([`classify_line`]) is pure and unit-tested; file
//! IO ([`scan_files`]) is a thin shim over it. A DLQ location may contain
//! arbitrary lines (blank lines, non-faucet output), so parsing is
//! tolerant: unparseable and non-envelope lines are **skipped and counted**,
//! never fatal.

use async_trait::async_trait;
use faucet_core::{FaucetError, Source, UnwrappedEnvelope, unwrap_envelope};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Base64 of the sealed-payload magic `FCT` — every line the jsonl sink
/// writes under an `encryption:` block starts with this, so sealed lines are
/// detectable even in builds without the `encryption` feature.
const SEALED_LINE_PREFIX: &str = "RkNU";

/// Optional decryption for DLQ lines sealed at rest by the jsonl sink's
/// `encryption:` block (#207). The default carries no keys: sealed lines are
/// still *detected* (and counted as [`LineOutcome::Undecryptable`]) so an
/// encrypted DLQ inspected without a key reports what it is instead of "all
/// lines malformed".
#[derive(Clone, Default)]
pub struct DlqDecryptor {
    #[cfg(feature = "encryption")]
    inner: Option<Arc<faucet_core::CompiledEncryption>>,
    #[cfg(feature = "encryption")]
    sealing: Option<Value>,
}

impl std::fmt::Debug for DlqDecryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DlqDecryptor(..)")
    }
}

/// How one raw line decodes before classification.
enum LineDecode {
    /// Not a sealed line — classify the raw text.
    Plain,
    /// Sealed and successfully decrypted — classify the plaintext.
    #[cfg(feature = "encryption")]
    Decrypted(String),
    /// Sealed but no key / wrong key / tampered — counted, never fatal.
    Undecryptable,
}

impl DlqDecryptor {
    /// Build from user-supplied keys (`--encryption-key`, repeatable): the
    /// first entry is the current key, the rest are rotation candidates.
    /// An empty slice builds the inert default.
    pub fn from_keys(keys: &[String]) -> Result<Self, FaucetError> {
        if keys.is_empty() {
            return Ok(Self::default());
        }
        #[cfg(feature = "encryption")]
        {
            let spec = faucet_core::EncryptionSpec {
                key: keys[0].clone(),
                previous_keys: keys[1..].to_vec(),
                algorithm: Default::default(),
            };
            Ok(Self {
                inner: Some(Arc::new(faucet_core::CompiledEncryption::compile(&spec)?)),
                sealing: Some(serde_json::json!({
                    "key": keys[0],
                    "previous_keys": &keys[1..],
                })),
            })
        }
        #[cfg(not(feature = "encryption"))]
        Err(FaucetError::Config(
            "--encryption-key requires a faucet build with the `encryption` feature \
             (cargo install faucet-cli --features encryption)"
                .into(),
        ))
    }

    /// Build from a jsonl sink config's raw `encryption` block (as found in a
    /// config's `dlq:` sink), if present.
    pub fn from_config_value(value: Option<&Value>) -> Result<Self, FaucetError> {
        // The binding is only consumed under the `encryption` feature.
        #[cfg_attr(not(feature = "encryption"), allow(unused_variables))]
        let Some(value) = value else {
            return Ok(Self::default());
        };
        #[cfg(feature = "encryption")]
        {
            let spec: faucet_core::EncryptionSpec = serde_json::from_value(value.clone())
                .map_err(|e| FaucetError::Config(format!("dlq sink `encryption` block: {e}")))?;
            Ok(Self {
                inner: Some(Arc::new(faucet_core::CompiledEncryption::compile(&spec)?)),
                sealing: Some(value.clone()),
            })
        }
        #[cfg(not(feature = "encryption"))]
        Err(FaucetError::Config(
            "the config's dlq sink has an `encryption` block, but this faucet build has no \
             `encryption` feature"
                .into(),
        ))
    }

    /// The `encryption` block that seals a file with the same keys this
    /// decryptor reads, so a replay's re-failed rows stay encrypted.
    pub fn sealing_value(&self) -> Option<Value> {
        #[cfg(feature = "encryption")]
        {
            self.sealing.clone()
        }
        #[cfg(not(feature = "encryption"))]
        None
    }

    /// Whether any key is loaded.
    pub fn is_active(&self) -> bool {
        #[cfg(feature = "encryption")]
        {
            self.inner.is_some()
        }
        #[cfg(not(feature = "encryption"))]
        false
    }

    fn decode(&self, line: &str) -> LineDecode {
        let trimmed = line.trim();
        if !trimmed.starts_with(SEALED_LINE_PREFIX) {
            return LineDecode::Plain;
        }
        #[cfg(feature = "encryption")]
        if let Some(enc) = &self.inner {
            use base64::Engine as _;
            let Ok(sealed) = base64::engine::general_purpose::STANDARD.decode(trimmed) else {
                // Starts like a sealed line but is not base64 — let the JSON
                // classifier call it malformed.
                return LineDecode::Plain;
            };
            if !faucet_core::encryption::is_encrypted(&sealed) {
                return LineDecode::Plain;
            }
            return match enc.decrypt(&sealed) {
                Ok(plain) => match String::from_utf8(plain) {
                    Ok(text) => LineDecode::Decrypted(text),
                    Err(_) => LineDecode::Undecryptable,
                },
                Err(_) => LineDecode::Undecryptable,
            };
        }
        // Sealed-looking line with no key available (or an encryption-less
        // build): report it as encrypted rather than malformed.
        LineDecode::Undecryptable
    }
}

/// A pre-built source attached to a single [`ExpandedNode`](crate::expand::ExpandedNode)
/// so the executor runs it instead of building one from the connector
/// registry. Used only by `faucet dlq replay`, which runs exactly one
/// invocation, so the source is taken once. `Clone` shares the same cell
/// (cloning the node does not duplicate the source).
#[derive(Clone)]
pub struct SourceOverride(Arc<Mutex<Option<Box<dyn Source>>>>);

impl SourceOverride {
    /// Wrap a pre-built source.
    pub fn new(source: Box<dyn Source>) -> Self {
        Self(Arc::new(Mutex::new(Some(source))))
    }

    /// Take the source out of the cell. Returns `None` if it was already
    /// taken (a second invocation would build from the registry instead).
    pub fn take(&self) -> Option<Box<dyn Source>> {
        self.0.lock().ok().and_then(|mut g| g.take())
    }
}

impl std::fmt::Debug for SourceOverride {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SourceOverride(..)")
    }
}

/// Outcome of classifying a single line of a DLQ location.
#[derive(Debug, Clone, PartialEq)]
pub enum LineOutcome {
    /// A blank / whitespace-only line — ignored, not counted as a skip.
    Blank,
    /// A line that is not valid JSON — skipped and counted.
    Malformed,
    /// Valid JSON that is not a DLQ envelope (no `payload`) — skipped and
    /// counted.
    NonEnvelope,
    /// A line sealed by the jsonl sink's `encryption:` block that could not
    /// be decrypted (no key, wrong key, or tampering) — skipped and counted.
    Undecryptable,
    /// A parsed DLQ envelope.
    Envelope(Box<UnwrappedEnvelope>),
}

/// Classify one raw line. Pure — no IO. Blank lines are ignored; anything
/// else is either an envelope, malformed JSON, valid-but-not-an-envelope, or
/// an (un)decryptable sealed line.
pub fn classify_line(line: &str) -> LineOutcome {
    classify_line_with(line, &DlqDecryptor::default())
}

/// [`classify_line`] with decryption support: sealed lines are decrypted
/// through `dec` before classification.
pub fn classify_line_with(line: &str, dec: &DlqDecryptor) -> LineOutcome {
    if line.trim().is_empty() {
        return LineOutcome::Blank;
    }
    fn classify_text(text: &str) -> LineOutcome {
        match serde_json::from_str::<Value>(text) {
            Ok(value) => match unwrap_envelope(&value) {
                Ok(env) => LineOutcome::Envelope(Box::new(env)),
                Err(_) => LineOutcome::NonEnvelope,
            },
            Err(_) => LineOutcome::Malformed,
        }
    }
    match dec.decode(line) {
        LineDecode::Plain => classify_text(line),
        #[cfg(feature = "encryption")]
        LineDecode::Decrypted(plain) => classify_text(&plain),
        LineDecode::Undecryptable => LineOutcome::Undecryptable,
    }
}

/// Envelopes read from a DLQ location plus the tolerant-parse tallies.
#[derive(Debug, Default, Clone)]
pub struct ScanResult {
    /// Every parsed envelope, in file order.
    pub envelopes: Vec<UnwrappedEnvelope>,
    /// Non-blank lines that were not valid JSON.
    pub malformed: usize,
    /// Valid-JSON lines that were not DLQ envelopes (no `payload`).
    pub non_envelope: usize,
    /// Sealed (encrypted) lines that could not be decrypted with the
    /// available keys.
    pub undecryptable: usize,
    /// Files that were read.
    pub files_read: usize,
}

/// Suffix `faucet dlq discard` gives the file it archives discarded envelopes
/// into (`dlq.jsonl` → `dlq.jsonl.archived`).
pub const ARCHIVE_SUFFIX: &str = ".archived";

/// Whether `path` is a discard archive — the current `<file>.archived` or the
/// `<stem>.archived.jsonl` written by older versions. Directory and glob
/// locations skip them, so a replay never re-reads discarded envelopes; an
/// archive named explicitly as the location is still read.
pub fn is_archive(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.ends_with(ARCHIVE_SUFFIX) || n.ends_with(".archived.jsonl"))
}

/// Expand a DLQ location into the concrete local files to read.
///
/// * a file path → just that file,
/// * a directory → every `*.jsonl` entry directly inside it (sorted),
/// * anything containing a glob metacharacter (`*?[`) → glob matches,
///
/// in both of the last two cases without discard archives ([`is_archive`]).
///
/// Returns an error only when the location resolves to nothing (a clear
/// signal the path is wrong), so callers never silently report an empty DLQ
/// for a typo'd path.
pub fn expand_location(location: &str) -> Result<Vec<PathBuf>, FaucetError> {
    let has_glob = location.contains(['*', '?', '[']);
    let mut files: Vec<PathBuf> = if has_glob {
        glob::glob(location)
            .map_err(|e| FaucetError::Config(format!("invalid DLQ glob '{location}': {e}")))?
            .filter_map(Result::ok)
            .filter(|p| p.is_file() && !is_archive(p))
            .collect()
    } else {
        let path = Path::new(location);
        if path.is_dir() {
            std::fs::read_dir(path)
                .map_err(|e| FaucetError::Source(format!("reading DLQ dir '{location}': {e}")))?
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.is_file() && p.extension().is_some_and(|x| x == "jsonl") && !is_archive(p)
                })
                .collect()
        } else if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            Vec::new()
        }
    };
    files.sort();
    if files.is_empty() {
        return Err(FaucetError::Source(format!(
            "DLQ location '{location}' matched no files (expected a .jsonl file, a directory of \
             .jsonl files, or a glob)"
        )));
    }
    Ok(files)
}

/// Read and classify every line of every file, collecting envelopes and
/// tallies. Blank lines are ignored; malformed / non-envelope lines are
/// counted but never abort the scan.
pub fn scan_files(files: &[PathBuf], dec: &DlqDecryptor) -> Result<ScanResult, FaucetError> {
    let mut out = ScanResult::default();
    for file in files {
        let text = std::fs::read_to_string(file).map_err(|e| {
            FaucetError::Source(format!("reading DLQ file '{}': {e}", file.display()))
        })?;
        out.files_read += 1;
        for line in text.lines() {
            match classify_line_with(line, dec) {
                LineOutcome::Blank => {}
                LineOutcome::Malformed => out.malformed += 1,
                LineOutcome::NonEnvelope => out.non_envelope += 1,
                LineOutcome::Undecryptable => out.undecryptable += 1,
                LineOutcome::Envelope(env) => out.envelopes.push(*env),
            }
        }
    }
    Ok(out)
}

/// Whether an envelope matches an optional reason filter. `None` matches
/// everything; a legacy envelope with no `reason` field never matches an
/// explicit filter.
pub fn reason_matches(env: &UnwrappedEnvelope, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(want) => env.reason.as_deref() == Some(want),
    }
}

/// A [`Source`] over a DLQ location that yields the **unwrapped original
/// payloads** (optionally filtered by reason), so a replay run feeds them
/// through the referenced config's transforms / quality / contract / sink.
///
/// It has no `state_key`, so the executor never wraps it for bookmarking —
/// a replay is a fresh, whole-location read.
pub struct DlqReaderSource {
    files: Vec<PathBuf>,
    reason: Option<String>,
    dec: DlqDecryptor,
}

impl DlqReaderSource {
    /// Build a reader over the already-expanded `files`, keeping only
    /// envelopes whose reason matches `reason` (if set). Sealed lines are
    /// decrypted through `dec`.
    pub fn new(files: Vec<PathBuf>, reason: Option<String>, dec: DlqDecryptor) -> Self {
        Self { files, reason, dec }
    }
}

#[async_trait]
impl Source for DlqReaderSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let files = self.files.clone();
        let reason = self.reason.clone();
        let dec = self.dec.clone();
        // Blocking file IO off the async runtime.
        let scan = tokio::task::spawn_blocking(move || scan_files(&files, &dec))
            .await
            .map_err(|e| FaucetError::Source(format!("DLQ reader task panicked: {e}")))??;
        Ok(scan
            .envelopes
            .into_iter()
            .filter(|env| reason_matches(env, reason.as_deref()))
            .map(|env| env.payload)
            .collect())
    }

    fn connector_name(&self) -> &'static str {
        "dlq-reader"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    fn envelope_line(reason: &str, payload: Value) -> String {
        json!({
            "error": { "kind": "Sink", "message": "boom" },
            "reason": reason,
            "payload": payload,
            "ts_ms": 1,
            "sink": "pg",
            "pipeline": "etl",
            "row": "",
            "record_index": 0,
        })
        .to_string()
    }

    #[test]
    fn classify_line_blank_is_ignored() {
        assert_eq!(classify_line(""), LineOutcome::Blank);
        assert_eq!(classify_line("   \t "), LineOutcome::Blank);
    }

    #[test]
    fn classify_line_malformed_json() {
        assert_eq!(classify_line("{not json"), LineOutcome::Malformed);
        assert_eq!(classify_line("just text"), LineOutcome::Malformed);
    }

    #[test]
    fn classify_line_valid_json_but_not_envelope() {
        assert_eq!(classify_line(r#"{"a":1}"#), LineOutcome::NonEnvelope);
        assert_eq!(classify_line("[1,2,3]"), LineOutcome::NonEnvelope);
    }

    #[test]
    fn classify_line_parses_envelope() {
        let line = envelope_line("quality", json!({"id": 7}));
        match classify_line(&line) {
            LineOutcome::Envelope(env) => {
                assert_eq!(env.payload, json!({"id": 7}));
                assert_eq!(env.reason.as_deref(), Some("quality"));
            }
            other => panic!("expected envelope, got {other:?}"),
        }
    }

    #[test]
    fn reason_matches_filter() {
        let env = UnwrappedEnvelope {
            payload: json!({}),
            reason: Some("contract".into()),
            error_kind: None,
            error_message: None,
            record_index: None,
            pipeline: None,
            row: None,
            sink: None,
            ts_ms: None,
        };
        assert!(reason_matches(&env, None));
        assert!(reason_matches(&env, Some("contract")));
        assert!(!reason_matches(&env, Some("quality")));
        // A legacy envelope with no reason never matches an explicit filter.
        let legacy = UnwrappedEnvelope {
            reason: None,
            ..env
        };
        assert!(reason_matches(&legacy, None));
        assert!(!reason_matches(&legacy, Some("quality")));
    }

    fn write_tmp(name: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        (dir, path)
    }

    #[test]
    fn scan_files_counts_skips_and_collects_envelopes() {
        let body = format!(
            "{}\n\n{}\nnot json\n{{\"a\":1}}\n",
            envelope_line("quality", json!({"id": 1})),
            envelope_line("contract", json!({"id": 2})),
        );
        let (_dir, path) = write_tmp("dlq.jsonl", &body);
        let scan = scan_files(&[path], &DlqDecryptor::default()).unwrap();
        assert_eq!(scan.envelopes.len(), 2);
        assert_eq!(scan.malformed, 1);
        assert_eq!(scan.non_envelope, 1);
        assert_eq!(scan.files_read, 1);
    }

    #[test]
    fn expand_location_glob_matches_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.jsonl", "b.jsonl"] {
            std::fs::write(dir.path().join(name), "\n").unwrap();
        }
        std::fs::write(dir.path().join("skip.txt"), "\n").unwrap();
        let pattern = format!("{}/*.jsonl", dir.path().display());
        let got = expand_location(&pattern).unwrap();
        assert_eq!(got.len(), 2, "glob matches both .jsonl files, not the .txt");
        // A glob that matches nothing errors.
        assert!(expand_location(&format!("{}/*.none", dir.path().display())).is_err());
    }

    #[test]
    fn expand_location_file_dir_and_missing() {
        let (dir, path) = write_tmp("dlq.jsonl", "\n");
        // single file
        assert_eq!(
            expand_location(path.to_str().unwrap()).unwrap(),
            vec![path.clone()]
        );
        // directory → the .jsonl inside
        let got = expand_location(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(got, vec![path]);
        // missing path → error
        assert!(expand_location(dir.path().join("nope.jsonl").to_str().unwrap()).is_err());
    }

    #[test]
    fn directory_and_glob_locations_skip_discard_archives() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "dlq.jsonl",
            "dlq.jsonl.archived",
            "old.archived.jsonl",
            "other.jsonl",
        ] {
            std::fs::write(dir.path().join(name), "\n").unwrap();
        }
        let names = |files: Vec<PathBuf>| -> Vec<String> {
            files
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
                .collect()
        };
        assert_eq!(
            names(expand_location(dir.path().to_str().unwrap()).unwrap()),
            ["dlq.jsonl", "other.jsonl"]
        );
        assert_eq!(
            names(expand_location(&format!("{}/*", dir.path().display())).unwrap()),
            ["dlq.jsonl", "other.jsonl"]
        );
        let explicit = dir.path().join("dlq.jsonl.archived");
        assert_eq!(
            expand_location(explicit.to_str().unwrap()).unwrap(),
            vec![explicit],
            "an archive named explicitly is still read"
        );
        assert!(is_archive(Path::new("a/old.archived.jsonl")));
        assert!(!is_archive(Path::new("a/archived.jsonl.d/x.jsonl")));
    }

    #[tokio::test]
    async fn dlq_reader_source_yields_filtered_payloads() {
        let body = format!(
            "{}\n{}\n",
            envelope_line("quality", json!({"id": 1})),
            envelope_line("contract", json!({"id": 2})),
        );
        let (_dir, path) = write_tmp("dlq.jsonl", &body);
        // No filter → both payloads.
        let src = DlqReaderSource::new(vec![path.clone()], None, DlqDecryptor::default());
        let all = src.fetch_all().await.unwrap();
        assert_eq!(all, vec![json!({"id": 1}), json!({"id": 2})]);
        // Reason filter → only matching payloads.
        let src =
            DlqReaderSource::new(vec![path], Some("contract".into()), DlqDecryptor::default());
        let filtered = src.fetch_all().await.unwrap();
        assert_eq!(filtered, vec![json!({"id": 2})]);
    }

    #[test]
    fn source_override_takes_once() {
        struct Dummy;
        #[async_trait]
        impl Source for Dummy {
            async fn fetch_with_context(
                &self,
                _c: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![])
            }
        }
        let ov = SourceOverride::new(Box::new(Dummy));
        assert!(ov.take().is_some());
        assert!(ov.take().is_none());
    }

    #[cfg(feature = "encryption")]
    mod sealed_lines {
        use super::*;
        use base64::Engine as _;

        fn seal(key: &str, text: &str) -> String {
            let enc = faucet_core::CompiledEncryption::compile(&faucet_core::EncryptionSpec {
                key: key.into(),
                previous_keys: vec![],
                algorithm: Default::default(),
            })
            .unwrap();
            base64::engine::general_purpose::STANDARD.encode(enc.encrypt(text.as_bytes()))
        }

        #[test]
        fn sealed_envelope_classifies_with_the_right_key() {
            let line = seal("k", &envelope_line("quality", serde_json::json!({"id": 1})));
            let dec = DlqDecryptor::from_keys(&["k".to_string()]).unwrap();
            assert!(matches!(
                classify_line_with(&line, &dec),
                LineOutcome::Envelope(_)
            ));
            // Rotation: the sealing key found among the later candidates.
            let rotated = DlqDecryptor::from_keys(&["new".to_string(), "k".to_string()]).unwrap();
            assert!(matches!(
                classify_line_with(&line, &rotated),
                LineOutcome::Envelope(_)
            ));
        }

        #[test]
        fn sealed_line_without_or_with_wrong_key_is_undecryptable_not_malformed() {
            let line = seal("k", "{\"payload\": {}}");
            assert_eq!(
                classify_line_with(&line, &DlqDecryptor::default()),
                LineOutcome::Undecryptable
            );
            let wrong = DlqDecryptor::from_keys(&["other".to_string()]).unwrap();
            assert_eq!(
                classify_line_with(&line, &wrong),
                LineOutcome::Undecryptable
            );
        }

        #[test]
        fn plain_lines_pass_through_a_keyed_decryptor() {
            let dec = DlqDecryptor::from_keys(&["k".to_string()]).unwrap();
            assert!(matches!(
                classify_line_with(&envelope_line("quality", serde_json::json!({"a": 1})), &dec),
                LineOutcome::Envelope(_)
            ));
            assert_eq!(
                classify_line_with("{not json", &dec),
                LineOutcome::Malformed
            );
            // A line that merely starts with the sealed prefix but is not
            // base64/sealed falls back to normal classification.
            assert_eq!(
                classify_line_with("RkNU-not-really-sealed!!!", &dec),
                LineOutcome::Malformed
            );
        }

        #[test]
        fn from_keys_empty_is_inert_and_from_config_value_none_is_inert() {
            assert!(!DlqDecryptor::from_keys(&[]).unwrap().is_active());
            assert!(!DlqDecryptor::from_config_value(None).unwrap().is_active());
            let v = serde_json::json!({"key": "k"});
            assert!(
                DlqDecryptor::from_config_value(Some(&v))
                    .unwrap()
                    .is_active()
            );
            assert!(
                DlqDecryptor::from_config_value(Some(&serde_json::json!({"nope": 1}))).is_err()
            );
        }
    }
}
