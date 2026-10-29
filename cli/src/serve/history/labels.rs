//! Run labels as a list filter (#958): the `?label=` grammar, the per-query
//! cap, and the submit-time key/value limits.

use std::collections::BTreeMap;

/// Most label filters one `GET /v1/runs` may carry.
pub const MAX_LABEL_FILTERS: usize = 10;
/// Longest label key a run may carry, in characters.
pub const MAX_LABEL_KEY_LEN: usize = 63;
/// Longest label value a run may carry, in characters.
pub const MAX_LABEL_VALUE_LEN: usize = 256;

/// Provenance labels only the server stamps (a template-triggered run's
/// template, version, overlay, selection): filterable, never settable.
pub const RESERVED_LABELS: &[&str] = &[
    "template",
    "template_version",
    "allow_untested",
    "sink_template",
    "sink_template_version",
    "overlay",
    "overlay_version",
    crate::select::LABEL_SELECTION,
];

/// Refuse a caller-supplied label set: a reserved provenance key (unless the
/// server wrote the request) or a key/value past the limits.
pub fn check_submitted(labels: &BTreeMap<String, String>, trusted: bool) -> Result<(), String> {
    if !trusted
        && let Some(k) = labels
            .keys()
            .find(|k| RESERVED_LABELS.contains(&k.as_str()))
    {
        return Err(format!(
            "run label '{k}' is set by the server (provenance); choose another key"
        ));
    }
    validate(labels, RESERVED_LABELS)
}

/// One label condition: `key` present with exactly `value`, or just present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelFilter {
    pub key: String,
    pub value: Option<String>,
}

impl LabelFilter {
    /// Parse `key:value` (split on the first `:`) or a bare `key`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let (key, value) = match raw.split_once(':') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (raw, None),
        };
        if key.is_empty() {
            return Err(format!(
                "label filter '{raw}' has an empty key — use `label=key:value` or `label=key`"
            ));
        }
        Ok(Self {
            key: key.to_string(),
            value,
        })
    }

    /// Whether `labels` satisfies this condition.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        match (labels.get(&self.key), &self.value) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(have), Some(want)) => have == want,
        }
    }
}

/// Whether a run's labels satisfy every filter (AND).
pub fn matches_all(filters: &[LabelFilter], labels: &BTreeMap<String, String>) -> bool {
    filters.iter().all(|f| f.matches(labels))
}

/// Parse the repeatable `label=` values and the comma-separated `labels=`
/// alias into filters, refusing more than [`MAX_LABEL_FILTERS`].
pub fn parse_filters(label: &[String], labels: &[String]) -> Result<Vec<LabelFilter>, String> {
    let mut out = Vec::new();
    for raw in label {
        out.push(LabelFilter::parse(raw)?);
    }
    for raw in labels {
        for part in raw.split(',').filter(|p| !p.is_empty()) {
            out.push(LabelFilter::parse(part)?);
        }
    }
    if out.len() > MAX_LABEL_FILTERS {
        return Err(format!(
            "{} label filters given; at most {MAX_LABEL_FILTERS} are allowed per query",
            out.len()
        ));
    }
    Ok(out)
}

/// Parse CLI `--label` values: `key=value` (split on the first `=`), else the
/// API's `key:value` / `key` grammar, capped like the API.
pub fn parse_cli_filters(raw: &[String]) -> Result<Vec<LabelFilter>, String> {
    let normalized: Vec<String> = raw
        .iter()
        .map(|r| match r.split_once('=') {
            Some((k, v)) => format!("{k}:{v}"),
            None => r.clone(),
        })
        .collect();
    parse_filters(&normalized, &[])
}

/// Refuse a label set a run may not carry: an empty, over-long or `:`-holding
/// key, or an over-long value. Keys in `exempt` (server-stamped provenance) are skipped.
pub fn validate(labels: &BTreeMap<String, String>, exempt: &[&str]) -> Result<(), String> {
    for (k, v) in labels {
        if exempt.contains(&k.as_str()) {
            continue;
        }
        if k.is_empty() {
            return Err("a run label has an empty key".into());
        }
        if k.contains(':') {
            return Err(format!(
                "run label key '{k}' contains ':' — the `label=key:value` filter splits on the \
                 first ':', so the run could never be found by it"
            ));
        }
        if k.chars().count() > MAX_LABEL_KEY_LEN {
            return Err(format!(
                "run label key '{k}' is longer than {MAX_LABEL_KEY_LEN} characters"
            ));
        }
        if v.chars().count() > MAX_LABEL_VALUE_LEN {
            return Err(format!(
                "run label '{k}' has a value longer than {MAX_LABEL_VALUE_LEN} characters"
            ));
        }
    }
    Ok(())
}

/// Cut `value` to [`MAX_LABEL_VALUE_LEN`] characters, for labels the server
/// derives from data it does not control (an object key).
pub fn clamp_value(value: &str) -> String {
    value.chars().take(MAX_LABEL_VALUE_LEN).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parse_splits_on_the_first_colon() {
        assert_eq!(
            LabelFilter::parse("url:https://x:8080").unwrap(),
            LabelFilter {
                key: "url".into(),
                value: Some("https://x:8080".into())
            }
        );
        assert_eq!(
            LabelFilter::parse("team").unwrap(),
            LabelFilter {
                key: "team".into(),
                value: None
            }
        );
        assert_eq!(
            LabelFilter::parse("team:").unwrap().value,
            Some(String::new())
        );
        assert!(LabelFilter::parse(":x").unwrap_err().contains("empty key"));
        assert!(LabelFilter::parse("").is_err());
    }

    #[test]
    fn matching_is_exact_and_anded() {
        let l = labels(&[("team", "data"), ("env", "prod")]);
        let f = |s: &str| LabelFilter::parse(s).unwrap();
        assert!(f("team:data").matches(&l));
        assert!(!f("team:dat").matches(&l));
        assert!(f("env").matches(&l));
        assert!(!f("region").matches(&l));
        assert!(matches_all(&[f("team:data"), f("env:prod")], &l));
        assert!(!matches_all(&[f("team:data"), f("env:dev")], &l));
        assert!(matches_all(&[], &l));
    }

    #[test]
    fn filters_combine_both_forms_and_are_capped() {
        let got = parse_filters(&["a:1".into()], &["b:2,c".into(), "".into()]).unwrap();
        assert_eq!(
            got.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        let many: Vec<String> = (0..=MAX_LABEL_FILTERS).map(|i| format!("k{i}")).collect();
        assert!(
            parse_filters(&many, &[])
                .unwrap_err()
                .contains("at most 10")
        );
        let ten: Vec<String> = (0..MAX_LABEL_FILTERS).map(|i| format!("k{i}")).collect();
        assert_eq!(parse_filters(&ten, &[]).unwrap().len(), 10);
        assert!(parse_filters(&[], &[":v".into()]).is_err());
    }

    #[test]
    fn cli_filters_take_equals_or_colon() {
        let f = parse_cli_filters(&["team=data".into(), "env:prod".into(), "x".into()]).unwrap();
        assert_eq!(f[0].key, "team");
        assert_eq!(f[0].value.as_deref(), Some("data"));
        assert_eq!(f[1].value.as_deref(), Some("prod"));
        assert_eq!(f[2].value, None);
        assert_eq!(
            parse_cli_filters(&["url=a:b".into()]).unwrap()[0]
                .value
                .as_deref(),
            Some("a:b")
        );
        assert!(parse_cli_filters(&["=v".into()]).is_err());
    }

    #[test]
    fn validate_enforces_the_limits() {
        assert!(validate(&labels(&[("team", "data")]), &[]).is_ok());
        let long_key = "k".repeat(MAX_LABEL_KEY_LEN + 1);
        assert!(
            validate(&labels(&[(&long_key, "v")]), &[])
                .unwrap_err()
                .contains("longer than 63")
        );
        assert!(validate(&labels(&[(&"k".repeat(63), "v")]), &[]).is_ok());
        let long_val = "v".repeat(MAX_LABEL_VALUE_LEN + 1);
        assert!(
            validate(&labels(&[("k", &long_val)]), &[])
                .unwrap_err()
                .contains("longer than 256")
        );
        assert!(validate(&labels(&[("k", &long_val)]), &["k"]).is_ok());
        assert!(
            validate(&labels(&[("", "v")]), &[])
                .unwrap_err()
                .contains("empty key")
        );
        assert!(
            validate(&labels(&[("a:b", "v")]), &[])
                .unwrap_err()
                .contains("contains ':'")
        );
        assert!(validate(&labels(&[("url", "https://x:8080")]), &[]).is_ok());
    }

    #[test]
    fn check_submitted_refuses_reserved_keys_from_callers_only() {
        let l = labels(&[("template", "t")]);
        assert!(
            check_submitted(&l, false)
                .unwrap_err()
                .contains("set by the server")
        );
        assert!(check_submitted(&l, true).is_ok());
        let long = labels(&[("selection", &"x".repeat(MAX_LABEL_VALUE_LEN + 1))]);
        assert!(check_submitted(&long, true).is_ok());
        assert!(check_submitted(&labels(&[("k", &"x".repeat(300))]), true).is_err());
        assert!(check_submitted(&labels(&[("team", "data")]), false).is_ok());
    }

    #[test]
    fn clamp_value_cuts_at_the_cap_on_a_char_boundary() {
        assert_eq!(clamp_value("abc"), "abc");
        let s = "é".repeat(MAX_LABEL_VALUE_LEN + 5);
        assert_eq!(clamp_value(&s).chars().count(), MAX_LABEL_VALUE_LEN);
    }
}
