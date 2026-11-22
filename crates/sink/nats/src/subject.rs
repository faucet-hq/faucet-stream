//! Checks on subjects a record names through `subject_field`.

use faucet_core::FaucetError;

/// Whether `subject` is in the server's reserved space: a first token that
/// starts with `$` (`$JS.API…`, `$KV…`, `$SYS…`) or a reply inbox (`_INBOX…`).
fn reserved(subject: &str) -> bool {
    let first = subject.split('.').next().unwrap_or_default();
    first.starts_with('$') || first == "_INBOX"
}

/// What makes `subject` unpublishable, if anything: whitespace or control
/// characters, an empty token, or a wildcard token.
fn syntax_problem(subject: &str) -> Option<&'static str> {
    if subject.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Some("it contains whitespace or a control character");
    }
    let mut tokens = subject.split('.');
    if tokens.any(str::is_empty) {
        return Some("it has an empty token");
    }
    if subject.split('.').any(|t| t == "*" || t == ">") {
        return Some("a published subject cannot hold the wildcard tokens `*` or `>`");
    }
    None
}

/// What makes `pattern` an invalid `allowed_subjects` entry, if anything.
pub(crate) fn pattern_problem(pattern: &str) -> Option<&'static str> {
    if pattern.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Some("it contains whitespace or a control character");
    }
    let tokens: Vec<&str> = pattern.split('.').collect();
    if tokens.iter().any(|t| t.is_empty()) {
        return Some("it has an empty token");
    }
    if tokens[..tokens.len() - 1].contains(&">") {
        return Some("`>` may only be the last token");
    }
    None
}

/// Whether `subject` matches the NATS subject `pattern` (`*` = one token,
/// `>` = one or more trailing tokens).
pub(crate) fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut subject = subject.split('.');
    for p in pattern.split('.') {
        match p {
            ">" => return subject.next().is_some(),
            "*" => {
                if subject.next().is_none() {
                    return false;
                }
            }
            literal => {
                if subject.next() != Some(literal) {
                    return false;
                }
            }
        }
    }
    subject.next().is_none()
}

/// Whether a pattern opens the reserved space a `subject` sits in: its first
/// token must name that space literally, so `>` or `*` alone never does.
fn opens_reserved(pattern: &str, subject: &str) -> bool {
    let first = pattern.split('.').next().unwrap_or_default();
    first != "*" && first != ">" && reserved(first) && subject_matches(pattern, subject)
}

/// Refuse a subject taken from record field `field` that is malformed, in the
/// reserved space without an `allowed_subjects` entry naming that space, or
/// outside a non-empty `allowed_subjects` list. A per-row error.
pub(crate) fn check_record_subject(
    subject: &str,
    field: &str,
    allowed: &[String],
) -> Result<(), FaucetError> {
    let reason = if let Some(problem) = syntax_problem(subject) {
        Some(problem)
    } else if reserved(subject) && !allowed.iter().any(|p| opens_reserved(p, subject)) {
        Some("subjects starting with `$` or `_INBOX.` belong to the server")
    } else if !allowed.is_empty() && !allowed.iter().any(|p| subject_matches(p, subject)) {
        Some("it matches no `allowed_subjects` pattern")
    } else {
        None
    };
    match reason {
        None => Ok(()),
        Some(reason) => Err(FaucetError::Sink(format!(
            "nats sink: subject '{subject}' from subject_field '{field}' is not allowed ({reason}); \
             list it in allowed_subjects to publish there"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn patterns_match_token_wise() {
        assert!(subject_matches("orders.*", "orders.a"));
        assert!(!subject_matches("orders.*", "orders.a.b"));
        assert!(subject_matches("orders.>", "orders.a.b"));
        assert!(!subject_matches("orders.>", "orders"));
        assert!(subject_matches("orders.new", "orders.new"));
        assert!(!subject_matches("orders.new", "orders.old"));
        assert!(subject_matches(">", "a.b"));
    }

    #[test]
    fn reserved_subjects_are_refused_by_default() {
        for s in [
            "$JS.API.STREAM.PURGE.X",
            "$KV.cache.k",
            "$SYS.REQ",
            "_INBOX.abc",
        ] {
            let err = check_record_subject(s, "topic", &[])
                .unwrap_err()
                .to_string();
            assert!(err.contains("belong to the server"), "{err}");
        }
        assert!(check_record_subject("orders.a", "topic", &[]).is_ok());
        assert!(check_record_subject("INBOX.a", "topic", &[]).is_ok());
    }

    #[test]
    fn only_a_literal_reserved_pattern_opens_the_reserved_space() {
        let kv = list(&["$KV.cache.>"]);
        assert!(check_record_subject("$KV.cache.k", "f", &kv).is_ok());
        assert!(check_record_subject("$KV.other.k", "f", &kv).is_err());
        assert!(check_record_subject("$JS.API.INFO", "f", &kv).is_err());
        assert!(check_record_subject("$JS.API.INFO", "f", &list(&[">"])).is_err());
        assert!(check_record_subject("$JS.API", "f", &list(&["*.API"])).is_err());
        assert!(check_record_subject("_INBOX.x", "f", &list(&["_INBOX.*"])).is_ok());
    }

    #[test]
    fn a_list_confines_ordinary_subjects() {
        let only = list(&["orders.*"]);
        assert!(check_record_subject("orders.a", "f", &only).is_ok());
        let err = check_record_subject("billing.a", "f", &only)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no `allowed_subjects` pattern"), "{err}");
    }

    #[test]
    fn malformed_subjects_are_refused() {
        for s in ["a b", "a..b", ".a", "a.", "a.*", "a.>", "a\tb"] {
            assert!(check_record_subject(s, "f", &list(&[">"])).is_err(), "{s}");
        }
        assert!(check_record_subject("a*b", "f", &[]).is_ok());
    }

    #[test]
    fn pattern_problems() {
        assert!(pattern_problem("orders.>").is_none());
        assert!(pattern_problem("*.x").is_none());
        assert!(pattern_problem("").is_some());
        assert!(pattern_problem("a..b").is_some());
        assert!(pattern_problem("a.>.b").is_some());
        assert!(pattern_problem("a b").is_some());
    }
}
