//! Positional (`?`) bind ordering for SQL dialects that bind by position.

use serde_json::Value;

/// Delimits a context token's index in a query marked for
/// [`order_positional_bindings`]: a token becomes
/// `CONTEXT_MARK<index>CONTEXT_MARK`. U+0001 never appears in a real query;
/// a query that holds it is refused by [`check_no_context_mark`].
pub const CONTEXT_MARK: char = '\u{1}';

/// The quoting and comment rules a dialect uses, so a `?` inside a literal,
/// a quoted identifier or a comment is not taken for a bind marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SqlLexing {
    double_quote_escapes: bool,
    triple_quotes: bool,
    dollar_quotes: bool,
    backticks: bool,
    slash_comments: bool,
    hash_comments: bool,
}

impl SqlLexing {
    /// Snowflake: `'…'` with backslash escapes, `"…"` identifiers, `$$…$$`,
    /// `--`, `//` and `/* */` comments.
    pub const fn snowflake() -> Self {
        Self {
            double_quote_escapes: false,
            triple_quotes: false,
            dollar_quotes: true,
            backticks: false,
            slash_comments: true,
            hash_comments: false,
        }
    }

    /// BigQuery (GoogleSQL): `'…'`, `"…"`, `'''…'''` and `"""…"""` with
    /// backslash escapes (a raw `r'…'` literal is the same span), backtick
    /// identifiers, `--`, `#` and `/* */` comments.
    pub const fn bigquery() -> Self {
        Self {
            double_quote_escapes: true,
            triple_quotes: true,
            dollar_quotes: false,
            backticks: true,
            slash_comments: false,
            hash_comments: true,
        }
    }
}

/// Refuse a query that already holds [`CONTEXT_MARK`]: it would be taken for
/// a context token's position and bind the wrong value.
pub fn check_no_context_mark(connector: &str, query: &str) -> Result<(), crate::FaucetError> {
    if query.contains(CONTEXT_MARK) {
        return Err(crate::FaucetError::Config(format!(
            "{connector}: the query contains the control character U+0001, which is \
             reserved for placing context values; remove it"
        )));
    }
    Ok(())
}

/// Turn `marked` — a query whose context tokens were replaced by
/// `CONTEXT_MARK<i>CONTEXT_MARK` (see
/// [`substitute_context_bind_params`](super::substitute_context_bind_params)) —
/// into SQL with `?` markers and the bind values in the markers' textual
/// order: each `params` value goes to the next `?` the query itself holds,
/// each context value to the marker that replaced its token. A `?` inside a
/// literal, a quoted identifier or a comment (per `lexing`) is not a marker.
/// Params left over when the query holds fewer `?` than params are appended,
/// so the backend reports the count mismatch.
pub fn order_positional_bindings(
    marked: &str,
    params: &[Value],
    context_values: Vec<Value>,
    lexing: SqlLexing,
) -> (String, Vec<Value>) {
    let mut out = BindOrder {
        sql: String::with_capacity(marked.len()),
        bindings: Vec::with_capacity(params.len() + context_values.len()),
        context_values,
    };
    let mut params = params.iter();
    let mut chars = marked.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            CONTEXT_MARK => out.context(&mut chars),
            '?' => {
                out.sql.push('?');
                if let Some(p) = params.next() {
                    out.bindings.push(p.clone());
                }
            }
            '\'' | '"' => {
                let escapes = c == '\'' || lexing.double_quote_escapes;
                if lexing.triple_quotes && out.triple(&mut chars, c) {
                    let close: String = [c, c, c].iter().collect();
                    out.skip(&mut chars, &close, escapes);
                } else {
                    out.sql.push(c);
                    out.skip(&mut chars, &c.to_string(), escapes);
                }
            }
            '`' if lexing.backticks => {
                out.sql.push(c);
                out.skip(&mut chars, "`", true);
            }
            '$' if lexing.dollar_quotes && chars.peek() == Some(&'$') => {
                chars.next();
                out.sql.push_str("$$");
                out.skip(&mut chars, "$$", false);
            }
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                out.sql.push_str("--");
                out.skip(&mut chars, "\n", false);
            }
            '/' if lexing.slash_comments && chars.peek() == Some(&'/') => {
                chars.next();
                out.sql.push_str("//");
                out.skip(&mut chars, "\n", false);
            }
            '#' if lexing.hash_comments => {
                out.sql.push('#');
                out.skip(&mut chars, "\n", false);
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                out.sql.push_str("/*");
                out.skip(&mut chars, "*/", false);
            }
            _ => out.sql.push(c),
        }
    }
    out.bindings.extend(params.cloned());
    (out.sql, out.bindings)
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

struct BindOrder {
    sql: String,
    bindings: Vec<Value>,
    context_values: Vec<Value>,
}

impl BindOrder {
    /// A context mark becomes `?` bound to its value wherever it sits, as the
    /// token it replaced always did.
    fn context(&mut self, chars: &mut Chars<'_>) {
        let index: String = chars.by_ref().take_while(|&d| d != CONTEXT_MARK).collect();
        let value = index
            .parse::<usize>()
            .ok()
            .and_then(|i| self.context_values.get_mut(i))
            .map(std::mem::take)
            .unwrap_or(Value::Null);
        self.sql.push('?');
        self.bindings.push(value);
    }

    /// After the opening `quote`, whether the next two characters make it a
    /// triple quote; if so they are consumed and the opener written.
    fn triple(&mut self, chars: &mut Chars<'_>, quote: char) -> bool {
        let mut ahead = chars.clone();
        if ahead.next() == Some(quote) && ahead.next() == Some(quote) {
            chars.next();
            chars.next();
            for _ in 0..3 {
                self.sql.push(quote);
            }
            true
        } else {
            false
        }
    }

    /// Copy through the end of a quoted or commented run closed by `close`.
    /// With `escapes`, a backslash carries the next character; a doubled
    /// single-character delimiter (`''`, `""`) stays inside the run.
    fn skip(&mut self, chars: &mut Chars<'_>, close: &str, escapes: bool) {
        let mut run = String::new();
        while let Some(d) = chars.next() {
            if d == CONTEXT_MARK {
                self.context(chars);
                run.clear();
                continue;
            }
            self.sql.push(d);
            if escapes && d == '\\' {
                if let Some(e) = chars.next() {
                    self.sql.push(e);
                }
                run.clear();
                continue;
            }
            run.push(d);
            if run.ends_with(close) {
                let mut delim = close.chars();
                if let (Some(only), None) = (delim.next(), delim.next())
                    && chars.peek() == Some(&only)
                {
                    chars.next();
                    self.sql.push(only);
                    run.clear();
                    continue;
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mark(i: usize) -> String {
        format!("{CONTEXT_MARK}{i}{CONTEXT_MARK}")
    }

    #[test]
    fn snowflake_markers_interleave_and_quoted_question_marks_are_not_markers() {
        let params = [json!("p1"), json!("p2")];
        let marked = format!(
            "SELECT '?', 'it''s ?', 'a\\'?', \"col?\", $$?$$ -- ?\n\
             , ? /* ? */ , {} // ?\n , ? , {}",
            mark(0),
            mark(1)
        );
        let (sql, binds) = order_positional_bindings(
            &marked,
            &params,
            vec![json!("c0"), json!("c1")],
            SqlLexing::snowflake(),
        );
        assert_eq!(
            sql,
            "SELECT '?', 'it''s ?', 'a\\'?', \"col?\", $$?$$ -- ?\n\
             , ? /* ? */ , ? // ?\n , ? , ?"
        );
        assert_eq!(
            binds,
            vec![json!("p1"), json!("c0"), json!("p2"), json!("c1")]
        );
    }

    #[test]
    fn bigquery_skips_every_quote_and_comment_form() {
        let params = [json!("p1"), json!("p2")];
        let marked = format!(
            "SELECT '?', \"a\\\"?\", '''x ' ? ''', \"\"\"?\"\"\", r'\\'?', `c?` # ?\n\
             , {} -- ?\n , ? /* ? */ , ?",
            mark(0)
        );
        let (sql, binds) =
            order_positional_bindings(&marked, &params, vec![json!("c0")], SqlLexing::bigquery());
        assert_eq!(
            sql,
            "SELECT '?', \"a\\\"?\", '''x ' ? ''', \"\"\"?\"\"\", r'\\'?', `c?` # ?\n\
             , ? -- ?\n , ? /* ? */ , ?"
        );
        assert_eq!(binds, vec![json!("c0"), json!("p1"), json!("p2")]);
    }

    #[test]
    fn a_context_token_before_a_static_marker_binds_first() {
        let marked = format!("SELECT * FROM t WHERE a = {} AND b = ?", mark(0));
        let (sql, binds) = order_positional_bindings(
            &marked,
            &[json!("static")],
            vec![json!(42)],
            SqlLexing::bigquery(),
        );
        assert_eq!(sql, "SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(binds, vec![json!(42), json!("static")]);
    }

    #[test]
    fn left_over_params_trail_and_slash_and_hash_follow_the_profile() {
        let (sql, binds) =
            order_positional_bindings("SELECT 1 // ?", &[json!(1)], vec![], SqlLexing::bigquery());
        assert_eq!(sql, "SELECT 1 // ?");
        assert_eq!(binds, vec![json!(1)]);
        let (_, binds) = order_positional_bindings(
            "SELECT 1 # ?",
            &[json!(1), json!(2)],
            vec![],
            SqlLexing::snowflake(),
        );
        assert_eq!(binds, vec![json!(1), json!(2)]);
        let (sql, binds) = order_positional_bindings(
            "SELECT `a`, $$?$$",
            &[json!(1)],
            vec![],
            SqlLexing::bigquery(),
        );
        assert_eq!(sql, "SELECT `a`, $$?$$");
        assert_eq!(binds, vec![json!(1)]);
    }

    #[test]
    fn a_query_holding_the_mark_is_refused() {
        assert!(check_no_context_mark("bigquery", "SELECT 1").is_ok());
        let err = check_no_context_mark("bigquery", "SELECT '\u{1}'").unwrap_err();
        assert!(err.to_string().contains("U+0001"), "{err}");
    }
}
