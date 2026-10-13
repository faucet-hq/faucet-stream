//! Process-global registry of resolved secret values + a redaction scrubber.
//!
//! Interpolation resolves secrets on raw config strings, so by the time the
//! config is a typed structure a secret value is an ordinary `String`. Rather
//! than tag fields, we track the resolved *values* and scrub any occurrence
//! from output the CLI emits (the [`RedactingWriter`]).

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, OnceLock, RwLock};

/// Values shorter than this are not registered — masking 1–3 char strings
/// would over-redact unrelated output.
const MIN_REDACT_LEN: usize = 4;

/// Shortest line of a multi-line secret registered on its own.
const MIN_LINE_LEN: usize = 8;

/// Most forms the registry holds (#789 SERVE-24). A long-running server
/// registers every rotated token; past this the least recently registered
/// secrets are dropped so redaction cost stays bounded.
pub const MAX_FORMS: usize = 16_384;

/// The registered forms, each with the sequence number of its last
/// registration, plus the matcher compiled from them (rebuilt on change, not
/// per call).
struct Registry {
    forms: HashMap<String, u64>,
    /// Process-lifetime credentials (the server's own tokens and keys):
    /// never evicted, whatever the churn in `forms`.
    pinned: HashSet<String>,
    seq: u64,
    max_forms: usize,
    compiled: Option<Arc<Compiled>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            forms: HashMap::new(),
            pinned: HashSet::new(),
            seq: 0,
            max_forms: MAX_FORMS,
            compiled: None,
        }
    }
}

struct Compiled {
    /// Every form, longest first (ties by text): a secret that is a
    /// substring of another is replaced after it, so the longer one's tail
    /// is never left exposed.
    patterns: Vec<String>,
    max_len: usize,
}

impl Registry {
    fn insert(&mut self, forms: Vec<String>) {
        self.seq += 1;
        for f in forms {
            if !self.pinned.contains(&f) {
                self.forms.insert(f, self.seq);
            }
        }
        if self.forms.len() > self.max_forms {
            let mut by_age: Vec<(u64, String)> =
                self.forms.iter().map(|(f, s)| (*s, f.clone())).collect();
            by_age.sort();
            let excess = self.forms.len() - self.max_forms;
            let cutoff = by_age[excess - 1].0;
            self.forms.retain(|_, s| *s > cutoff);
        }
        self.compiled = None;
    }

    fn pin(&mut self, forms: Vec<String>) {
        for f in forms {
            self.forms.remove(&f);
            self.pinned.insert(f);
        }
        self.compiled = None;
    }

    fn compile(&self) -> Compiled {
        let mut patterns: Vec<String> = self
            .forms
            .keys()
            .chain(self.pinned.iter())
            .cloned()
            .collect();
        patterns.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        Compiled {
            max_len: patterns.first().map(String::len).unwrap_or(0),
            patterns,
        }
    }
}

fn registry() -> &'static RwLock<Registry> {
    static REG: OnceLock<RwLock<Registry>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(Registry::default()))
}

fn compiled() -> Arc<Compiled> {
    if let Some(c) = &registry()
        .read()
        .expect("secret registry lock poisoned")
        .compiled
    {
        return Arc::clone(c);
    }
    let mut reg = registry().write().expect("secret registry lock poisoned");
    if let Some(c) = &reg.compiled {
        return Arc::clone(c);
    }
    let c = Arc::new(reg.compile());
    reg.compiled = Some(Arc::clone(&c));
    c
}

/// The forms a secret takes in output: itself, escaped as JSON and `{:?}`,
/// and for a multi-line secret each line on its own.
fn forms_of(secret: &str) -> Vec<String> {
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
    forms
}

/// Register a resolved secret value so it is scrubbed from future output —
/// along with the forms it takes once escaped (JSON logs, `{:?}` fields) and,
/// for a multi-line secret such as a PEM key, each line on its own.
pub fn register(secret: &str) {
    if secret.len() < MIN_REDACT_LEN {
        return;
    }
    registry()
        .write()
        .expect("secret registry lock poisoned")
        .insert(forms_of(secret));
}

/// [`register`] for a credential the process holds for its whole life (a
/// server's bearer tokens, vault keys, provider client secrets): it is never
/// evicted when the registry reaches [`MAX_FORMS`], and never unregistered.
pub fn register_pinned(secret: &str) {
    if secret.len() < MIN_REDACT_LEN {
        return;
    }
    registry()
        .write()
        .expect("secret registry lock poisoned")
        .pin(forms_of(secret));
}

/// Stop scrubbing `secret`: a refresh token rotated away (#789 SERVE-24).
pub fn unregister(secret: &str) {
    let mut reg = registry().write().expect("secret registry lock poisoned");
    let before = reg.forms.len();
    for f in forms_of(secret) {
        reg.forms.remove(&f);
    }
    if reg.forms.len() != before {
        reg.compiled = None;
    }
}

/// Replace every registered secret value in `input` with `***`.
pub fn redact(input: &str) -> Cow<'_, str> {
    redact_with(input, |_| "***".to_owned())
}

/// Characters that end a URL embedded in a message.
fn url_boundary(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '\'' | '`' | '<' | '>' | '(' | ')' | '[' | ']')
}

/// Redact a message that leaves the process (#874): every registered secret,
/// and the credentials of every URL in it (`scheme://user:pass@host`,
/// credential query parameters), so text persisted to a shared store is clean
/// whichever instance later reads it.
pub fn redact_message(input: &str) -> String {
    let secrets = redact(input);
    let text = secrets.as_ref();
    let plain = faucet_core::util::redact_uri_credentials;
    if !text.contains("://") {
        return plain(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("://") {
        let start = rest[..at]
            .char_indices()
            .rev()
            .find(|&(_, c)| url_boundary(c) || c == '=' || c == ',')
            .map_or(0, |(i, c)| i + c.len_utf8());
        let end = rest[at..]
            .char_indices()
            .find(|&(_, c)| url_boundary(c))
            .map_or(rest.len(), |(i, _)| at + i);
        out.push_str(&plain(&rest[..start]));
        out.push_str(&plain(&rest[start..end]));
        rest = &rest[end..];
    }
    out.push_str(&plain(rest));
    out
}

/// Replace every registered secret value in `input` with a caller-supplied
/// token. `token(secret)` receives the raw secret and returns its replacement;
/// it is called only for secrets actually present in `input`. Used by the
/// config-snapshot writer (#374) to swap secrets for stable `<secret:hmac:…>`
/// tokens instead of `***`, so a rotation surfaces as a changed hash without
/// ever persisting the secret. Longer secrets are replaced first.
pub fn redact_with(input: &str, token: impl Fn(&str) -> String) -> Cow<'_, str> {
    let c = compiled();
    let mut out: Option<String> = None;
    for secret in &c.patterns {
        let current = out.as_deref().unwrap_or(input);
        if current.contains(secret.as_str()) {
            out = Some(current.replace(secret.as_str(), &token(secret)));
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
    compiled().max_len
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

    const PEM: &str = "-----BEGIN KEY-----\nMIIBVgIBADANBgkqhkiG9w0BAQEF\nshort\n-----END KEY-----";

    /// Drop only the values these tests register: the registry is global,
    /// and wiping it would remove a secret a concurrent test elsewhere just
    /// registered.
    fn clear() {
        for s in [
            "supersecrettoken",
            "pa\"ss\\word-1",
            PEM,
            "abcd",
            "abcdXYZW",
            "supersecretvalue",
            "rotated-away-token",
            "still-live-token",
            "alpha-secret",
            "hunter2pass",
        ] {
            unregister(s);
        }
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
        let pem = PEM;
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
    fn unregister_drops_every_form_of_a_rotated_secret() {
        clear();
        register("rotated-away-token");
        register("still-live-token");
        assert_eq!(redact("rotated-away-token"), "***");
        unregister("rotated-away-token");
        unregister("never-registered");
        assert_eq!(redact("rotated-away-token"), "rotated-away-token");
        assert_eq!(redact("still-live-token"), "***");
        clear();
    }

    #[test]
    fn the_registry_is_bounded_and_evicts_the_oldest() {
        let mut reg = Registry {
            max_forms: 4,
            ..Registry::default()
        };
        reg.insert(forms_of("the-very-first-secret"));
        for i in 0..6 {
            reg.insert(forms_of(&format!("bounded-secret-{i}")));
        }
        assert!(reg.forms.len() <= 4, "{}", reg.forms.len());
        assert!(!reg.forms.contains_key("the-very-first-secret"));
        assert!(reg.forms.contains_key("bounded-secret-5"));
        let compiled = reg.compile();
        assert_eq!(compiled.max_len, "bounded-secret-5".len());
        assert!(Registry::default().compile().patterns.is_empty());
    }

    #[test]
    fn pinned_secrets_survive_eviction_and_are_still_redacted() {
        let mut reg = Registry {
            max_forms: 4,
            ..Registry::default()
        };
        reg.pin(forms_of("server-bearer-token"));
        for i in 0..8 {
            reg.insert(forms_of(&format!("tenant-secret-{i}")));
        }
        assert!(reg.forms.len() <= 4);
        assert!(reg.pinned.contains("server-bearer-token"));
        assert!(
            reg.compile()
                .patterns
                .iter()
                .any(|p| p == "server-bearer-token")
        );
    }

    #[test]
    #[serial]
    fn register_pinned_redacts_and_ignores_short_values() {
        register_pinned("pinned-cred-value");
        register_pinned("abc");
        assert_eq!(redact("token=pinned-cred-value abc"), "token=*** abc");
    }

    #[test]
    #[serial]
    fn redact_with_hands_the_token_the_matched_secret() {
        clear();
        assert_eq!(redact_with("plain", |_| unreachable!()), "plain");
        register("alpha-secret");
        let out = redact_with("a alpha-secret b alpha-secret", |s| {
            format!("<{}>", s.len())
        });
        assert_eq!(out, "a <12> b <12>");
        clear();
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

    #[test]
    #[serial]
    fn redact_message_masks_secrets_and_url_credentials() {
        register("s3cr3t-874-value");
        let msg = "open 'postgres://app:pw@db:5432/x' failed (s3cr3t-874-value); \
                   retry https://h/api?api_key=k1&x=1 and mysql://u:p2@m/y";
        let out = redact_message(msg);
        assert!(!out.contains("s3cr3t-874-value"), "{out}");
        assert!(!out.contains("app:pw"), "{out}");
        assert!(!out.contains("k1"), "{out}");
        assert!(!out.contains("u:p2"), "{out}");
        assert!(out.contains("postgres://db:5432/x"), "{out}");
        assert!(out.contains("mysql://m/y"), "{out}");
        assert!(!redact_message("Password=hunter;Server=x").contains("hunter"));
        assert_eq!(redact_message("plain"), "plain");
        assert_eq!(redact_message("at end url=s3://b/k"), "at end url=s3://b/k");
        unregister("s3cr3t-874-value");
    }
}
