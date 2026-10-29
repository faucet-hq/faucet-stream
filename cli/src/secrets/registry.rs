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
    /// Every form, longest first (ties by text).
    patterns: Vec<String>,
    /// One automaton over every form, scanned once per input however many
    /// forms are registered. Every (overlapping) occurrence is found and
    /// overlapping occurrences are masked as one run, so no part of any
    /// secret — the tail of a longer one, the head of one overlapping
    /// another — is left exposed.
    matcher: Option<aho_corasick::AhoCorasick>,
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
        let matcher = (!patterns.is_empty())
            .then(|| {
                aho_corasick::AhoCorasick::builder()
                    .match_kind(aho_corasick::MatchKind::Standard)
                    // A DFA over thousands of forms costs far more to build
                    // (on every registry change) than it saves per line.
                    .kind(Some(aho_corasick::AhoCorasickKind::ContiguousNFA))
                    .build(&patterns)
                    .ok()
            })
            .flatten();
        Compiled {
            max_len: patterns.first().map(String::len).unwrap_or(0),
            matcher,
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
    let Some(matcher) = c.matcher.as_ref() else {
        return redact_each(input, &c.patterns, token);
    };
    // Merge overlapping occurrences into runs; each run is replaced by the
    // token of the longest secret in it.
    // Overlapping matches arrive in end order, so sort them by start first.
    let mut runs: Vec<(usize, usize, usize)> = matcher
        .find_overlapping_iter(input)
        .map(|m| (m.start(), m.end(), m.pattern().as_usize()))
        .collect();
    if runs.is_empty() {
        return Cow::Borrowed(input);
    }
    runs.sort_unstable();
    let mut merged: Vec<(usize, usize, usize)> = Vec::with_capacity(runs.len());
    for r in runs {
        match merged.last_mut() {
            Some(m) if r.0 < m.1 => {
                m.1 = m.1.max(r.1);
                if c.patterns[r.2].len() > c.patterns[m.2].len() {
                    m.2 = r.2;
                }
            }
            _ => merged.push(r),
        }
    }
    let mut out = String::with_capacity(input.len());
    let mut last = 0;
    for (start, end, p) in merged {
        out.push_str(&input[last..start]);
        out.push_str(&token(&c.patterns[p]));
        last = end;
    }
    out.push_str(&input[last..]);
    Cow::Owned(out)
}

/// The pattern-by-pattern fallback for a registry the automaton could not be
/// built for (never expected; kept so redaction never silently stops).
fn redact_each<'a>(
    input: &'a str,
    patterns: &[String],
    token: impl Fn(&str) -> String,
) -> Cow<'a, str> {
    let mut out: Option<String> = None;
    for secret in patterns {
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
mod tests;
