//! Process-global registry of resolved secret values + a redaction scrubber.
//!
//! Interpolation resolves secrets on raw config strings, so by the time the
//! config is a typed structure a secret value is an ordinary `String`. Rather
//! than tag fields, we track the resolved *values* and scrub any occurrence
//! from output the CLI emits (the [`RedactingWriter`]).

use std::borrow::Cow;
use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::{OnceLock, RwLock};

/// Values shorter than this are not registered — masking 1–3 char strings
/// would over-redact unrelated output.
const MIN_REDACT_LEN: usize = 4;

fn registry() -> &'static RwLock<HashSet<String>> {
    static REG: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Shortest line of a multi-line secret registered on its own.
const MIN_LINE_LEN: usize = 8;

/// Register a resolved secret value so it is scrubbed from future output —
/// along with the forms it takes once escaped (JSON logs, `{:?}` fields) and,
/// for a multi-line secret such as a PEM key, each line on its own.
pub fn register(secret: &str) {
    if secret.len() < MIN_REDACT_LEN {
        return;
    }
    let mut forms = vec![secret.to_owned()];
    let quoted = |s: String| s[1..s.len() - 1].to_owned();
    if let Ok(json) = serde_json::to_string(secret) {
        forms.push(quoted(json));
    }
    forms.push(quoted(format!("{secret:?}")));
    if secret.contains(['\n', '\r']) {
        forms.extend(
            secret
                .lines()
                .map(str::trim)
                .filter(|l| l.len() >= MIN_LINE_LEN)
                .map(str::to_owned),
        );
    }
    let mut reg = registry().write().expect("secret registry lock poisoned");
    reg.extend(forms);
}

/// Replace every registered secret value in `input` with `***`.
pub fn redact(input: &str) -> Cow<'_, str> {
    redact_with(input, |_| "***".to_owned())
}

/// Replace every registered secret value in `input` with a caller-supplied
/// token. `token(secret)` receives the raw secret and returns its replacement;
/// it is called only for secrets actually present in `input`. Used by the
/// config-snapshot writer (#374) to swap secrets for stable `<secret:hmac:…>`
/// tokens instead of `***`, so a rotation surfaces as a changed hash without
/// ever persisting the secret. Same longest-first ordering as [`redact`].
pub fn redact_with(input: &str, token: impl Fn(&str) -> String) -> Cow<'_, str> {
    let reg = registry().read().expect("secret registry lock poisoned");
    if reg.is_empty() {
        return Cow::Borrowed(input);
    }
    // Process **longest secret first**: a secret that is a substring of another
    // (e.g. `abcd` inside `abcdXYZW`) must be replaced *after* the longer one,
    // otherwise replacing the shorter one first destroys the longer match and
    // leaves its extra tail (`XYZW`) exposed. `HashSet` iteration order is
    // randomized, so without this sort redaction is nondeterministic.
    let mut secrets: Vec<&str> = reg.iter().map(String::as_str).collect();
    secrets.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    let mut out: Option<String> = None;
    for secret in secrets {
        let current = out.as_deref().unwrap_or(input);
        if current.contains(secret) {
            out = Some(current.replace(secret, &token(secret)));
        }
    }
    match out {
        Some(s) => Cow::Owned(s),
        None => Cow::Borrowed(input),
    }
}

/// Longest registered secret in bytes (0 if none). Sizes the [`RedactingWriter`]
/// hold-back window so a secret split across two `write()` calls is still caught.
fn max_secret_len() -> usize {
    registry()
        .read()
        .expect("secret registry lock poisoned")
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0)
}

/// An `io::Write` adapter that runs [`redact`] over every chunk before
/// forwarding it to the inner writer. Wrapping the tracing subscriber's
/// writer in this scrubs secret values out of *all* CLI log/diagnostic output
/// at the I/O boundary, regardless of which field carried the value.
pub struct RedactingWriter<W: Write> {
    inner: W,
    /// Trailing bytes withheld from the previous `write` — the window that might
    /// be the *start* of a secret completing in a later write. Bounded by the
    /// longest registered secret; flushed on `flush`/drop.
    pending: Vec<u8>,
}

impl<W: Write> RedactingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            pending: Vec::new(),
        }
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);
        // Withhold the last `max_secret_len - 1` bytes so a secret straddling
        // this write and the next is scrubbed once the two are joined. Everything
        // before that window is safe to emit: any *complete* secret in it has
        // already been masked, and an *incomplete* prefix can only sit in the
        // withheld tail.
        let keep = max_secret_len().saturating_sub(1);
        if self.pending.len() > keep {
            // `into_owned` drops the borrow of `self.pending` so we can mutate it.
            let scrubbed = redact(&String::from_utf8_lossy(&self.pending)).into_owned();
            let mut split = scrubbed.len().saturating_sub(keep);
            while split > 0 && !scrubbed.is_char_boundary(split) {
                split -= 1;
            }
            // Snapped to a char boundary above, so the byte split is also a char
            // boundary — emit the prefix, retain the suffix.
            let bytes = scrubbed.as_bytes();
            self.inner.write_all(&bytes[..split])?;
            self.pending.clear();
            self.pending.extend_from_slice(&bytes[split..]);
        }
        // Report the original length consumed — the tracing fmt layer treats a
        // short write as an error, and the withheld bytes are an internal detail.
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            let scrubbed = redact(&String::from_utf8_lossy(&self.pending)).into_owned();
            self.inner.write_all(scrubbed.as_bytes())?;
            self.pending.clear();
        }
        self.inner.flush()
    }
}

impl<W: Write> Drop for RedactingWriter<W> {
    fn drop(&mut self) {
        // Emit any withheld tail so the final bytes of a stream (e.g. a log event
        // formatted then dropped without an explicit flush) are never lost.
        let _ = self.flush();
    }
}

/// `MakeWriter` that produces a [`RedactingWriter`] over stderr, for the
/// tracing fmt subscriber. Only needed when the `observability` feature wires
/// a subscriber (the sole place the CLI formats tracing output).
#[cfg(feature = "observability")]
pub struct RedactingMakeWriter;

#[cfg(feature = "observability")]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RedactingMakeWriter {
    type Writer = RedactingWriter<std::io::Stderr>;
    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(std::io::stderr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear() {
        registry().write().unwrap().clear();
    }

    #[test]
    #[serial]
    fn redacts_registered_value() {
        clear();
        register("supersecrettoken");
        assert_eq!(
            redact("Authorization: supersecrettoken"),
            "Authorization: ***"
        );
    }

    #[test]
    #[serial]
    fn escaped_and_per_line_forms_are_redacted() {
        clear();
        register("pa\"ss\\word-1");
        let json = serde_json::json!({ "msg": "pw pa\"ss\\word-1" }).to_string();
        assert!(!redact(&json).contains("word-1"), "{}", redact(&json));
        let dbg = format!("{:?}", "pa\"ss\\word-1");
        assert_eq!(redact(&dbg), "\"***\"");
        let pem = "-----BEGIN KEY-----\nMIIBVgIBADANBgkqhkiG9w0BAQEF\nshort\n-----END KEY-----";
        register(pem);
        assert!(!redact("line: MIIBVgIBADANBgkqhkiG9w0BAQEF").contains("MIIBVg"));
        let escaped = serde_json::to_string(pem).unwrap();
        assert!(!redact(&escaped).contains("MIIBVg"), "{}", redact(&escaped));
        assert_eq!(redact("short"), "short");
        clear();
    }

    #[test]
    #[serial]
    fn leaves_unregistered_text_untouched() {
        clear();
        register("supersecrettoken");
        assert_eq!(redact("nothing to see"), "nothing to see");
    }

    #[test]
    #[serial]
    fn does_not_register_short_values() {
        clear();
        register("abc"); // < MIN_REDACT_LEN
        assert_eq!(redact("abc def"), "abc def");
    }

    #[test]
    #[serial]
    fn redact_handles_overlapping_secrets_longest_first() {
        clear();
        // A shorter secret that is a prefix of a longer one. Redacting the
        // shorter first (unordered iteration) leaves the longer secret's tail
        // ("XYZW") exposed; longest-first redaction must mask the whole thing.
        register("abcd");
        register("abcdXYZW");
        let out = redact("value=abcdXYZW end");
        assert!(
            !out.contains("XYZW"),
            "longer secret partially leaked: {out}"
        );
        assert_eq!(out, "value=*** end");
    }

    #[test]
    #[serial]
    fn writer_scrubs_secret_split_across_writes() {
        clear();
        register("supersecretvalue");
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = RedactingWriter::new(&mut buf);
            // The secret straddles two separate write() calls.
            w.write_all(b"token=supersec").unwrap();
            w.write_all(b"retvalue done").unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(
            !out.contains("supersecretvalue"),
            "secret leaked across write boundary: {out}"
        );
        assert_eq!(out, "token=*** done");
    }

    #[test]
    #[serial]
    fn writer_scrubs_secret_on_write() {
        clear();
        let secret = "hunter2pass";
        register(secret);
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = RedactingWriter::new(&mut buf);
            write!(w, "token={secret} done").unwrap();
            w.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "token=*** done");
    }
}
