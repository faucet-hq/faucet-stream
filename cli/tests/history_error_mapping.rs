//! A run-history failure reaches a caller as an HTTP status decided by the
//! error's type, in one place (`impl From<HistoryError> for ServeError`,
//! #1051): no `ServeError` is built from a `HistoryError`'s text, which would
//! turn a transient outage into a 500 and a retryable caller into a failed one.
#![allow(clippy::disallowed_methods)]

use syn::visit::Visit;

/// Whether a method chain is a call on the run history (`….history().x()`,
/// `history.x()`), following the receivers only, never the arguments.
fn reads_history(e: &syn::Expr) -> bool {
    match e {
        syn::Expr::MethodCall(m) => m.method == "history" || reads_history(&m.receiver),
        syn::Expr::Await(a) => reads_history(&a.base),
        syn::Expr::Try(t) => reads_history(&t.expr),
        syn::Expr::Field(f) => reads_history(&f.base),
        syn::Expr::Paren(p) => reads_history(&p.expr),
        syn::Expr::Path(p) => p.path.is_ident("history"),
        _ => false,
    }
}

/// Whether a `ServeError::…(…)` constructor in an expression is built from
/// the text of `param` (`param.to_string()`, or `param` inside `format!`).
struct BuildsFromText<'a> {
    param: &'a str,
    found: bool,
}

impl BuildsFromText<'_> {
    fn reads_text(&self, e: &syn::Expr) -> bool {
        match e {
            syn::Expr::MethodCall(m) if m.method == "to_string" => {
                matches!(&*m.receiver, syn::Expr::Path(p) if p.path.is_ident(self.param))
            }
            syn::Expr::Macro(m) if m.mac.path.is_ident("format") => {
                let tokens = m.mac.tokens.to_string();
                tokens.contains(&format!("{{{}}}", self.param))
                    || tokens.contains(&format!("{{{}:", self.param))
                    || tokens
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .any(|t| t == self.param)
            }
            _ => false,
        }
    }
}

impl<'ast> Visit<'ast> for BuildsFromText<'_> {
    fn visit_expr_call(&mut self, c: &'ast syn::ExprCall) {
        if let syn::Expr::Path(p) = &*c.func
            && p.path.segments.iter().any(|s| s.ident == "ServeError")
            && c.args.iter().any(|a| self.reads_text(a))
        {
            self.found = true;
        }
        syn::visit::visit_expr_call(self, c);
    }
}

fn closure_param(c: &syn::ExprClosure) -> Option<(String, bool)> {
    match c.inputs.first()? {
        syn::Pat::Ident(i) => Some((i.ident.to_string(), false)),
        syn::Pat::Type(t) => {
            let syn::Pat::Ident(i) = &*t.pat else {
                return None;
            };
            let typed = matches!(&*t.ty, syn::Type::Path(p)
                if p.path.segments.last().is_some_and(|s| s.ident == "HistoryError"));
            Some((i.ident.to_string(), typed))
        }
        _ => None,
    }
}

fn from_text(body: &syn::Expr, param: &str) -> bool {
    let mut v = BuildsFromText {
        param,
        found: false,
    };
    v.visit_expr(body);
    v.found
}

/// Every place that builds a `ServeError` from a history error's text.
#[derive(Default)]
struct Finder {
    hits: Vec<String>,
}

impl<'ast> Visit<'ast> for Finder {
    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        if m.method == "map_err"
            && let Some(syn::Expr::Closure(c)) = m.args.first()
            && let Some((param, _)) = closure_param(c)
            && reads_history(&m.receiver)
            && from_text(&c.body, &param)
        {
            self.hits.push("a map_err on a history call".into());
        }
        syn::visit::visit_expr_method_call(self, m);
    }

    fn visit_expr_closure(&mut self, c: &'ast syn::ExprClosure) {
        if let Some((param, true)) = closure_param(c)
            && from_text(&c.body, &param)
        {
            self.hits.push("a closure over a HistoryError".into());
        }
        syn::visit::visit_expr_closure(self, c);
    }

    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        for input in &f.sig.inputs {
            if let syn::FnArg::Typed(t) = input
                && let syn::Pat::Ident(i) = &*t.pat
                && matches!(&*t.ty, syn::Type::Path(p)
                    if p.path.segments.last().is_some_and(|s| s.ident == "HistoryError"))
            {
                let block = syn::Expr::Block(syn::ExprBlock {
                    attrs: Vec::new(),
                    label: None,
                    block: (*f.block).clone(),
                });
                if from_text(&block, &i.ident.to_string()) {
                    self.hits
                        .push(format!("fn {} over a HistoryError", f.sig.ident));
                }
            }
        }
        syn::visit::visit_item_fn(self, f);
    }
}

fn scan(source: &str) -> Vec<String> {
    let file = syn::parse_file(source).expect("parses");
    let mut f = Finder::default();
    f.visit_file(&file);
    f.hits
}

fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_serve_error_is_built_from_a_history_errors_text() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut found = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path).unwrap();
        for hit in scan(&source) {
            found.push(format!("{}: {hit}", path.display()));
        }
    }
    assert!(
        found.is_empty(),
        "map a HistoryError with `ServeError::from` (or `?`), never from its text:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_scan_finds_each_shape_it_refuses() {
    let hits = scan(
        r#"
        async fn a(state: &S) -> Result<(), ServeError> {
            state.history().get("x").await.map_err(|e| ServeError::Internal(e.to_string()))?;
            state.history().get("x").await.map_err(|e| ServeError::Internal(format!("read: {e}")))?;
            Ok(())
        }
        fn b() {
            let _ = |e: crate::serve::history::HistoryError| ServeError::Internal(e.to_string());
        }
        fn c(e: HistoryError) -> ServeError {
            ServeError::Internal(format!("store: {}", e))
        }
        fn fine(state: &S) {
            let _ = state.history().get("x").map_err(ServeError::from);
            let _ = plan(Opts { store: history.as_ref() }).await.map_err(|e| ServeError::BadConfig(e.to_string()));
            let _ = serde_json::to_value(1).map_err(|e| ServeError::Internal(e.to_string()));
        }
        "#,
    );
    assert_eq!(hits.len(), 4, "{hits:?}");
}
