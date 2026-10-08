//! Recursive report-tree / matrix flatten transform (`tree_flatten`, #530).
//!
//! Financial-report APIs (profit-and-loss, balance-sheet and similar report
//! endpoints) return a self-referential nested-`Rows` matrix — a tree of
//! section → subsection → line, where the tabular output is one row per **leaf**
//! carrying the section labels it sits under plus the period columns. Flattening
//! it is the one reshape that kept those taps on the embedded-DuckDB SQL path;
//! `tree_flatten` moves them back to an inbuilt transform.
//!
//! Pure + recursive: a depth-first walk carrying an ancestor-label stack; at each
//! leaf it emits `{ <ancestor columns…>, <header→value columns…>, [path] }`. It
//! routes through [`TransformStage::Custom`](crate::stage::TransformStage) (1→0..N),
//! so no new stage-enum variant is needed (the exhaustive-enum rule).

// The whole module is gated by `#[cfg(feature = "transform-tree-flatten")]` at
// its `pub mod tree;` declaration in `lib.rs`.
use crate::FaucetError;
use crate::stage::TransformStage;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Default [`TreeFlattenSpec::max_depth`] — a stack-overflow backstop for a
/// malformed or cyclic tree, far above any real report nesting.
pub const DEFAULT_MAX_DEPTH: usize = 64;

fn default_max_depth() -> usize {
    DEFAULT_MAX_DEPTH
}
fn default_leaf() -> String {
    "has_no_children".to_owned()
}
fn default_value_field() -> String {
    "value".to_owned()
}
fn default_path_sep() -> String {
    " > ".to_owned()
}

/// How the value columns are read from a leaf node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnsSpec {
    /// Path (within a node) to the leaf's cell array — e.g. `ColData`.
    pub from: String,
    /// Path (within the whole record) to the header definitions, paired
    /// positionally with the cells to name the value columns. Absent → cells are
    /// named `col_0`, `col_1`, ….
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// Field within each header element holding its label (e.g. `ColTitle`).
    /// Absent → the header element is used as a scalar string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_label: Option<String>,
    /// Field within each cell holding the value (e.g. `value`). Absent-in-cell →
    /// the whole cell is used.
    #[serde(default = "default_value_field")]
    pub value: String,
}

impl Default for ColumnsSpec {
    fn default() -> Self {
        Self {
            from: String::new(),
            header: None,
            header_label: None,
            value: default_value_field(),
        }
    }
}

impl ColumnsSpec {
    fn is_unset(&self) -> bool {
        self.from.trim().is_empty()
    }
}

/// Which ancestor labels to carry down onto each emitted row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AncestorsSpec {
    /// Path (within a node) to that node's group label — e.g.
    /// `Header.ColData[0].value`.
    pub field: String,
    /// Column names for depth 1, 2, …; extra depth is appended as
    /// `ancestor_<n>`, missing levels are null.
    #[serde(default, rename = "as")]
    pub as_names: Vec<String>,
}

/// Spec for the `tree_flatten` transform — recursive tree/matrix → leaf rows
/// (1→0..N). Compile with [`TreeFlattenSpec::compile`]; attach via
/// [`TreeFlattenSpec::into_stage`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TreeFlattenSpec {
    /// Path to the top-level node array within the record — e.g. `Rows.Row`.
    /// Absent → the record itself is the single root node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// Path (within a node) to its child-node array — the recursion key, e.g.
    /// `Rows.Row`.
    pub children: String,
    /// Leaf detection: `has_no_children` (default) or `has_field:<name>` (a node
    /// carrying `<name>` is a leaf even if it also has children).
    #[serde(default = "default_leaf")]
    pub leaf: String,
    /// How the value columns are read from a leaf. Set exactly one of
    /// `columns` or `groups`.
    #[serde(default, skip_serializing_if = "ColumnsSpec::is_unset")]
    pub columns: ColumnsSpec,
    /// Several positional cell arrays per leaf, each named by its own header
    /// list (#746) — the `zip_columns` `groups` shape. Each group needs a
    /// `header`; the groups' columns are merged into one row, and a width
    /// mismatch, a missing cell array or a column two groups both name fails
    /// the page.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<ColumnsSpec>,
    /// Ancestor/group labels carried onto every row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ancestors: Option<AncestorsSpec>,
    /// Emit the joined ancestor path under this column (e.g. `Income > Sales`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_as: Option<String>,
    /// Separator for `path_as`.
    #[serde(default = "default_path_sep")]
    pub path_sep: String,
    /// Skip leaves whose value cells are all empty (null or `""`).
    #[serde(default)]
    pub drop_empty: bool,
    /// Also emit a row for a group node that carries its own cells (subtotals).
    #[serde(default)]
    pub emit_group_rows: bool,
    /// Stack-overflow backstop; a tree deeper than this fails the record rather
    /// than silently dropping the deeper rows.
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
}

impl TreeFlattenSpec {
    /// Validate the spec, returning a reusable [`CompiledTreeFlatten`].
    pub fn compile(&self) -> Result<CompiledTreeFlatten, FaucetError> {
        CompiledTreeFlatten::compile(self)
    }

    /// Compile and wrap as a [`TransformStage::Custom`] (1→0..N). With
    /// `groups` the stage is a [`TransformStage::PageFn`], so a record that
    /// cannot be zipped fails the page instead of passing through.
    pub fn into_stage(&self) -> Result<TransformStage, FaucetError> {
        let compiled = self.compile()?;
        if compiled.spec.groups.is_empty() {
            return Ok(TransformStage::Custom(Arc::new(move |rec| {
                compiled.apply(rec)
            })));
        }
        Ok(TransformStage::PageFn(Arc::new(move |page: Vec<Value>| {
            let mut out = Vec::with_capacity(page.len());
            for rec in page {
                out.extend(compiled.try_apply(rec)?);
            }
            Ok(out)
        })))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum LeafMode {
    NoChildren,
    HasField(String),
}

/// Validated [`TreeFlattenSpec`] — apply per record with [`CompiledTreeFlatten::apply`].
#[derive(Debug, Clone)]
pub struct CompiledTreeFlatten {
    spec: TreeFlattenSpec,
    leaf_mode: LeafMode,
}

impl CompiledTreeFlatten {
    fn compile(spec: &TreeFlattenSpec) -> Result<Self, FaucetError> {
        if spec.children.trim().is_empty() {
            return Err(FaucetError::Transform(
                "tree_flatten: `children` must be non-empty".to_owned(),
            ));
        }
        if spec.groups.is_empty() && spec.columns.is_unset() {
            return Err(FaucetError::Transform(
                "tree_flatten: `columns.from` must be non-empty".to_owned(),
            ));
        }
        if !spec.groups.is_empty() && !spec.columns.is_unset() {
            return Err(FaucetError::Transform(
                "tree_flatten: set exactly one of `columns` or `groups`".to_owned(),
            ));
        }
        for (i, g) in spec.groups.iter().enumerate() {
            if g.is_unset() || g.header.as_deref().is_none_or(|h| h.trim().is_empty()) {
                return Err(FaucetError::Transform(format!(
                    "tree_flatten: group {i} needs a non-empty `from` and `header`"
                )));
            }
        }
        if spec.max_depth == 0 {
            return Err(FaucetError::Transform(
                "tree_flatten: `max_depth` must be greater than zero".to_owned(),
            ));
        }
        let leaf_mode = if spec.leaf == "has_no_children" {
            LeafMode::NoChildren
        } else if let Some(field) = spec.leaf.strip_prefix("has_field:") {
            if field.trim().is_empty() {
                return Err(FaucetError::Transform(
                    "tree_flatten: `leaf: has_field:<name>` requires a field name".to_owned(),
                ));
            }
            LeafMode::HasField(field.to_owned())
        } else {
            return Err(FaucetError::Transform(format!(
                "tree_flatten: `leaf` must be `has_no_children` or `has_field:<name>`, got '{}'",
                spec.leaf
            )));
        };
        Ok(Self {
            spec: spec.clone(),
            leaf_mode,
        })
    }

    /// Flatten one record (a report) into 0..N leaf rows. Non-object records and
    /// records with no resolvable root pass through unchanged (never silently
    /// dropped). With `groups`, a record that cannot be zipped (see
    /// [`try_apply`](Self::try_apply)) is also passed through, with an error
    /// logged; the pipeline stage uses `try_apply` and fails instead.
    pub fn apply(&self, rec: Value) -> Vec<Value> {
        match self.flatten(&rec) {
            Ok(Some(rows)) => rows,
            Ok(None) => vec![rec],
            Err(e) => {
                tracing::error!(error = %e, "tree_flatten: record passed through unflattened");
                vec![rec]
            }
        }
    }

    /// [`apply`](Self::apply), failing on a record that cannot be flattened
    /// without losing data: a leaf missing a group's cell array, a group whose
    /// width differs from its header count, a column named twice, or a tree
    /// deeper than `max_depth`.
    pub fn try_apply(&self, rec: Value) -> Result<Vec<Value>, FaucetError> {
        Ok(self.flatten(&rec)?.unwrap_or_else(|| vec![rec]))
    }

    /// `Ok(None)` = pass the record through unchanged.
    fn flatten(&self, rec: &Value) -> Result<Option<Vec<Value>>, FaucetError> {
        if !rec.is_object() {
            return Ok(None);
        }
        let sources = self.column_sources(rec)?;

        // Resolve the root node list. A root that resolves to an (even empty)
        // array/node is used as-is — an empty report yields zero rows. Only a
        // *missing* root path passes the record through (never silently dropped).
        let roots: Vec<&Value> = match &self.spec.root {
            Some(path) => match path_get(rec, path) {
                Some(Value::Array(a)) => a.iter().collect(),
                Some(v) => vec![v],
                None => return Ok(None),
            },
            None => vec![rec],
        };

        let mut out: Vec<Value> = Vec::new();
        let mut ancestors: Vec<Value> = Vec::new();
        for node in roots {
            self.walk(node, &mut ancestors, 0, &sources, &mut out)?;
        }
        Ok(Some(out))
    }

    /// Each column source with its header labels. `columns` is lenient
    /// (unnamed cells become `col_<i>`); every group is strict.
    fn column_sources<'s>(
        &'s self,
        rec: &Value,
    ) -> Result<Vec<(&'s ColumnsSpec, Vec<String>)>, FaucetError> {
        if self.spec.groups.is_empty() {
            let labels = self
                .spec
                .columns
                .header
                .as_deref()
                .and_then(|h| path_get(rec, h))
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .map(|el| header_label(&self.spec.columns, el))
                        .collect()
                })
                .unwrap_or_default();
            return Ok(vec![(&self.spec.columns, labels)]);
        }
        let mut owner: Map<String, Value> = Map::new();
        let mut out = Vec::with_capacity(self.spec.groups.len());
        for g in &self.spec.groups {
            let header = g.header.as_deref().unwrap_or_default();
            let labels: Vec<String> = path_get(rec, header)
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    FaucetError::Transform(format!(
                        "tree_flatten: group '{}': header `{header}` is not an array",
                        g.from
                    ))
                })?
                .iter()
                .map(|el| header_label(g, el))
                .collect();
            for n in &labels {
                if let Some(Value::String(prev)) =
                    owner.insert(n.clone(), Value::String(g.from.clone()))
                {
                    let which = if prev == g.from {
                        format!("group '{prev}' names it twice")
                    } else {
                        format!("groups '{prev}' and '{}' both name it", g.from)
                    };
                    return Err(FaucetError::Transform(format!(
                        "tree_flatten: duplicate column '{n}': {which}"
                    )));
                }
            }
            out.push((g, labels));
        }
        Ok(out)
    }

    fn is_leaf(&self, node: &Value, has_children: bool) -> bool {
        match &self.leaf_mode {
            LeafMode::NoChildren => !has_children,
            LeafMode::HasField(f) => node.get(f).is_some(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        &self,
        node: &Value,
        ancestors: &mut Vec<Value>,
        depth: usize,
        sources: &[(&ColumnsSpec, Vec<String>)],
        out: &mut Vec<Value>,
    ) -> Result<(), FaucetError> {
        if depth >= self.spec.max_depth {
            // Truncating would silently drop every deeper row (CORE-32).
            return Err(FaucetError::Transform(format!(
                "tree_flatten: the tree is nested deeper than max_depth ({}) — raise \
                 `max_depth` if the report is genuinely that deep",
                self.spec.max_depth
            )));
        }
        let children = path_get(node, &self.spec.children).and_then(Value::as_array);
        let has_children = children.is_some_and(|c| !c.is_empty());
        let leaf = self.is_leaf(node, has_children);
        let group_row =
            self.spec.emit_group_rows && sources.iter().any(|(c, _)| node_has_cells(node, &c.from));

        if (leaf || group_row)
            && let Some(row) = self.emit_row(node, ancestors, sources, out.len())?
        {
            out.push(row);
        }

        if let Some(children) = children.filter(|c| !c.is_empty()) {
            // Push this node's label, recurse, pop.
            let label = self
                .spec
                .ancestors
                .as_ref()
                .and_then(|a| path_get(node, &a.field).cloned())
                .unwrap_or(Value::Null);
            ancestors.push(label);
            for child in children {
                self.walk(child, ancestors, depth + 1, sources, out)?;
            }
            ancestors.pop();
        }
        Ok(())
    }

    fn emit_row(
        &self,
        node: &Value,
        ancestors: &[Value],
        sources: &[(&ColumnsSpec, Vec<String>)],
        row_index: usize,
    ) -> Result<Option<Value>, FaucetError> {
        let mut row = Map::new();

        // Ancestor columns.
        if let Some(anc) = &self.spec.ancestors {
            for (i, label) in ancestors.iter().enumerate() {
                let name = anc
                    .as_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("ancestor_{}", i + 1));
                row.insert(name, label.clone());
            }
        }
        // Joined ancestor path.
        if let Some(path_col) = &self.spec.path_as {
            let joined = ancestors
                .iter()
                .map(scalar_string)
                .collect::<Vec<_>>()
                .join(&self.spec.path_sep);
            row.insert(path_col.clone(), Value::String(joined));
        }

        // Value columns from the node's cell array(s).
        let strict = !self.spec.groups.is_empty();
        let mut all_empty = true;
        for (spec, labels) in sources {
            let cells = path_get(node, &spec.from).and_then(Value::as_array);
            if strict {
                let Some(cells) = cells else {
                    return Err(FaucetError::Transform(format!(
                        "tree_flatten: row {row_index} has no `{}` array (group '{}')",
                        spec.from, spec.from
                    )));
                };
                if cells.len() != labels.len() {
                    return Err(FaucetError::Transform(format!(
                        "tree_flatten: row {row_index}, group '{}': {} cell(s) but {} header(s)",
                        spec.from,
                        cells.len(),
                        labels.len()
                    )));
                }
            }
            for (i, cell) in cells.into_iter().flatten().enumerate() {
                let value = path_get(cell, &spec.value)
                    .cloned()
                    .unwrap_or_else(|| cell.clone());
                if !is_empty_value(&value) {
                    all_empty = false;
                }
                let name = labels
                    .get(i)
                    .cloned()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("col_{i}"));
                if row.contains_key(&name) {
                    return Err(FaucetError::Transform(format!(
                        "tree_flatten: row {row_index}: duplicate column '{name}' (header \
                         labels repeat, or collide with an ancestor/path column)"
                    )));
                }
                row.insert(name, value);
            }
        }

        if self.spec.drop_empty && all_empty {
            return Ok(None);
        }
        Ok(Some(Value::Object(row)))
    }
}

fn header_label(spec: &ColumnsSpec, el: &Value) -> String {
    if let Some(field) = &spec.header_label
        && let Some(v) = path_get(el, field)
    {
        return scalar_string(v);
    }
    scalar_string(el)
}

fn node_has_cells(node: &Value, from: &str) -> bool {
    path_get(node, from)
        .and_then(Value::as_array)
        .is_some_and(|a| !a.is_empty())
}

fn is_empty_value(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

/// Render a JSON scalar as a plain string (objects/arrays → compact JSON).
fn scalar_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// Resolve a dot/bracket path against a value. Supports a leading `$`/`$.`, `.key`
/// segments, and `[n]` array indices (e.g. `Header.ColData[0].value`,
/// `$.Rows.Row`). Returns `None` on any miss. Purpose-built here because
/// `CompiledPath` (stage.rs) does not support array indexing.
fn path_get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    let mut rest = path.trim();
    rest = rest.strip_prefix('$').unwrap_or(rest);
    rest = rest.strip_prefix('.').unwrap_or(rest);
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('[') {
            // [n] index
            let close = after.find(']')?;
            let idx: usize = after[..close].trim().parse().ok()?;
            cur = cur.as_array()?.get(idx)?;
            rest = &after[close + 1..];
            rest = rest.strip_prefix('.').unwrap_or(rest);
        } else {
            // .key up to the next '.' or '['
            let end = rest.find(['.', '[']).unwrap_or(rest.len());
            let key = &rest[..end];
            if key.is_empty() {
                return None;
            }
            cur = cur.get(key)?;
            rest = &rest[end..];
            rest = rest.strip_prefix('.').unwrap_or(rest);
        }
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> TreeFlattenSpec {
        TreeFlattenSpec {
            root: Some("Rows.Row".to_owned()),
            children: "Rows.Row".to_owned(),
            leaf: "has_no_children".to_owned(),
            columns: ColumnsSpec {
                from: "ColData".to_owned(),
                header: Some("Columns.Column".to_owned()),
                header_label: Some("ColTitle".to_owned()),
                value: "value".to_owned(),
            },
            groups: vec![],
            ancestors: Some(AncestorsSpec {
                field: "Header.ColData[0].value".to_owned(),
                as_names: vec!["section".to_owned(), "subsection".to_owned()],
            }),
            path_as: Some("group_path".to_owned()),
            path_sep: " > ".to_owned(),
            drop_empty: false,
            emit_group_rows: false,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// A nested-`Rows` P&L: Income → {Sales, Services}, one leaf each, two
    /// period columns.
    fn nested_rows_report() -> Value {
        json!({
            "Columns": { "Column": [ {"ColTitle": ""}, {"ColTitle": "Jan 2024"}, {"ColTitle": "Feb 2024"} ] },
            "Rows": { "Row": [
                {
                    "Header": { "ColData": [ {"value": "Income"} ] },
                    "Rows": { "Row": [
                        { "ColData": [ {"value": "Sales"}, {"value": "100"}, {"value": "120"} ] },
                        { "ColData": [ {"value": "Services"}, {"value": "50"}, {"value": "60"} ] }
                    ] }
                }
            ] }
        })
    }

    #[test]
    fn flattens_nested_rows_report_to_leaf_rows() {
        let out = spec().compile().unwrap().apply(nested_rows_report());
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["section"], json!("Income"));
        assert_eq!(out[0]["group_path"], json!("Income"));
        // Header pairing: the first column's header is empty, so it falls back to
        // `col_0` (an empty column name is unusable downstream); the two periods
        // take their header titles.
        assert_eq!(out[0]["col_0"], json!("Sales"));
        assert_eq!(out[0]["Jan 2024"], json!("100"));
        assert_eq!(out[0]["Feb 2024"], json!("120"));
        assert_eq!(out[1]["col_0"], json!("Services"));
        assert_eq!(out[1]["Feb 2024"], json!("60"));
    }

    #[test]
    fn uneven_depth_names_extra_levels_and_leaves_missing_null() {
        // Income has a nested subsection; a sibling leaf sits at depth 1.
        let report = json!({
            "Rows": { "Row": [
                {
                    "Header": { "ColData": [ {"value": "Income"} ] },
                    "Rows": { "Row": [
                        {
                            "Header": { "ColData": [ {"value": "Domestic"} ] },
                            "Rows": { "Row": [
                                { "ColData": [ {"value": "Sales"}, {"value": "100"} ] }
                            ] }
                        }
                    ] }
                },
                { "ColData": [ {"value": "Other"}, {"value": "5"} ] }
            ] }
        });
        let mut s = spec();
        s.columns.header = None;
        let out = s.compile().unwrap().apply(report);
        assert_eq!(out.len(), 2);
        // Deep leaf: section=Income, subsection=Domestic.
        assert_eq!(out[0]["section"], json!("Income"));
        assert_eq!(out[0]["subsection"], json!("Domestic"));
        assert_eq!(out[0]["col_0"], json!("Sales"));
        // Shallow leaf: no ancestors at all.
        assert!(out[1].get("section").is_none());
        assert_eq!(out[1]["col_0"], json!("Other"));
    }

    #[test]
    fn header_cell_length_mismatch_zips_to_shorter() {
        let mut s = spec();
        s.ancestors = None;
        s.root = None;
        s.children = "children".to_owned();
        let report = json!({
            "Columns": { "Column": [ {"ColTitle": "A"}, {"ColTitle": "B"} ] },
            "ColData": [ {"value": "x"}, {"value": "y"}, {"value": "z"} ]
        });
        let out = s.compile().unwrap().apply(report);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["A"], json!("x"));
        assert_eq!(out[0]["B"], json!("y"));
        // Third cell has no header → col_2.
        assert_eq!(out[0]["col_2"], json!("z"));
    }

    #[test]
    fn leaf_has_field_mode() {
        let mut s = spec();
        s.leaf = "has_field:ColData".to_owned();
        s.emit_group_rows = false;
        // A node that has BOTH children and ColData is a leaf under has_field.
        let report = json!({
            "Rows": { "Row": [
                {
                    "Header": { "ColData": [ {"value": "Total"} ] },
                    "ColData": [ {"value": "Total"}, {"value": "9"} ],
                    "Rows": { "Row": [ { "ColData": [ {"value": "x"}, {"value": "1"} ] } ] }
                }
            ] }
        });
        s.columns.header = None;
        let out = s.compile().unwrap().apply(report);
        // Parent (has ColData) emits, plus its child leaf → 2 rows.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["col_0"], json!("Total"));
    }

    #[test]
    fn emit_group_rows_includes_subtotals() {
        let mut s = spec();
        s.emit_group_rows = true;
        s.columns.header = None;
        let report = json!({
            "Rows": { "Row": [
                {
                    "Header": { "ColData": [ {"value": "Income"} ] },
                    "ColData": [ {"value": "Income total"}, {"value": "150"} ],
                    "Rows": { "Row": [
                        { "ColData": [ {"value": "Sales"}, {"value": "100"} ] }
                    ] }
                }
            ] }
        });
        let out = s.compile().unwrap().apply(report);
        // The group row (subtotal) + the leaf.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["col_0"], json!("Income total"));
        assert_eq!(out[1]["col_0"], json!("Sales"));
    }

    #[test]
    fn drop_empty_skips_all_empty_leaves() {
        let mut s = spec();
        s.drop_empty = true;
        s.columns.header = None;
        s.ancestors = None;
        s.root = None;
        s.children = "children".to_owned();
        let report = json!({ "ColData": [ {"value": ""}, {"value": null} ] });
        let out = s.compile().unwrap().apply(report);
        assert!(out.is_empty(), "an all-empty leaf is dropped");
    }

    #[test]
    fn max_depth_guard_fails_instead_of_truncating() {
        // Build a chain deeper than max_depth.
        let mut node = json!({ "ColData": [ {"value": "leaf"} ] });
        for _ in 0..10 {
            node = json!({ "Header": {"ColData":[{"value":"g"}]}, "children": [node] });
        }
        let mut s = spec();
        s.root = None;
        s.children = "children".to_owned();
        s.columns.header = None;
        s.ancestors = None;
        s.max_depth = 3;
        let err = s.compile().unwrap().try_apply(node).unwrap_err();
        assert!(
            err.to_string().contains("deeper than max_depth (3)"),
            "{err}"
        );
    }

    #[test]
    fn duplicate_column_labels_fail_the_record() {
        let node = json!({
            "Header": {"ColData": [{"value": "a"}, {"value": "a"}]},
            "ColData": [{"value": "1"}, {"value": "2"}]
        });
        let mut s = spec();
        s.root = None;
        s.children = "children".to_owned();
        s.ancestors = None;
        s.columns.header = Some("Header.ColData".to_owned());
        s.columns.header_label = Some("value".to_owned());
        let c = s.compile().unwrap();
        let err = c.try_apply(node);
        assert!(
            err.as_ref()
                .is_err_and(|e| e.to_string().contains("duplicate column")),
            "{err:?}"
        );
    }

    #[test]
    fn empty_report_yields_nothing() {
        let mut s = spec();
        let out = s
            .clone()
            .compile()
            .unwrap()
            .apply(json!({ "Rows": { "Row": [] } }));
        assert!(out.is_empty());
        // Non-object passes through.
        s.root = None;
        let passed = s.compile().unwrap().apply(json!("scalar"));
        assert_eq!(passed, vec![json!("scalar")]);
    }

    #[test]
    fn compile_rejects_bad_config() {
        let mut s = spec();
        s.children = " ".to_owned();
        assert!(s.compile().is_err());
        let mut s = spec();
        s.columns.from = "".to_owned();
        assert!(s.compile().is_err());
        let mut s = spec();
        s.leaf = "bogus".to_owned();
        assert!(s.compile().is_err());
        let mut s = spec();
        s.leaf = "has_field:".to_owned();
        assert!(s.compile().is_err());
        let mut s = spec();
        s.max_depth = 0;
        assert!(s.compile().is_err());
    }

    #[test]
    fn path_get_supports_dots_and_indices() {
        let v = json!({ "Header": { "ColData": [ {"value": "hi"} ] } });
        assert_eq!(path_get(&v, "Header.ColData[0].value"), Some(&json!("hi")));
        assert_eq!(
            path_get(&v, "$.Header.ColData[0].value"),
            Some(&json!("hi"))
        );
        assert_eq!(path_get(&v, "Header.missing"), None);
        assert_eq!(path_get(&v, "Header.ColData[9].value"), None);
    }

    #[test]
    fn into_stage_produces_a_custom_stage() {
        let stage = spec().into_stage().unwrap();
        assert!(matches!(stage, TransformStage::Custom(_)));
    }

    /// #746: a leaf carrying two positional cell arrays, each named by its own
    /// header list.
    fn grouped() -> TreeFlattenSpec {
        serde_json::from_value(json!({
            "root": "rows",
            "children": "rows",
            "groups": [
                {"from": "dims", "header": "dimHeaders", "header_label": "name"},
                {"from": "mets", "header": "metHeaders", "header_label": "name"}
            ],
            "ancestors": {"field": "label", "as": ["section"]}
        }))
        .unwrap()
    }

    fn grouped_report() -> Value {
        json!({
            "dimHeaders": [{"name": "date"}, {"name": "country"}],
            "metHeaders": [{"name": "sessions"}],
            "rows": [
                {"label": "Web", "rows": [
                    {"dims": [{"value": "20260901"}, {"value": "DE"}], "mets": [{"value": "12"}]},
                    {"dims": [{"value": "20260902"}, {}], "mets": [{"value": "7"}]}
                ]}
            ]
        })
    }

    #[test]
    fn groups_merge_into_one_row_per_leaf() {
        let c = grouped().compile().unwrap();
        let out = c.try_apply(grouped_report()).unwrap();
        assert_eq!(
            out,
            vec![
                json!({"section": "Web", "date": "20260901", "country": "DE", "sessions": "12"}),
                json!({"section": "Web", "date": "20260902", "country": {}, "sessions": "7"}),
            ]
        );
        assert_eq!(c.apply(grouped_report()), out);
    }

    #[test]
    fn groups_fail_loudly_and_apply_passes_through() {
        let c = grouped().compile().unwrap();
        let err = |r: Value| c.try_apply(r).unwrap_err().to_string();

        let mut r = grouped_report();
        r["rows"][0]["rows"][1]["mets"] = json!([]);
        let e = err(r.clone());
        assert!(
            e.contains("row 1, group 'mets': 0 cell(s) but 1 header(s)"),
            "{e}"
        );
        assert_eq!(c.apply(r.clone()), vec![r]);

        let mut r = grouped_report();
        r["rows"][0]["rows"][0]
            .as_object_mut()
            .unwrap()
            .remove("dims");
        assert!(err(r).contains("row 0 has no `dims` array"));

        let mut r = grouped_report();
        r["metHeaders"][0]["name"] = json!("date");
        let e = err(r);
        assert!(
            e.contains("duplicate column 'date'") && e.contains("'dims' and 'mets'"),
            "{e}"
        );

        let mut r = grouped_report();
        r["dimHeaders"][1]["name"] = json!("date");
        assert!(err(r).contains("group 'dims' names it twice"));

        let mut r = grouped_report();
        r["metHeaders"] = json!("sessions");
        assert!(err(r).contains("group 'mets': header `metHeaders` is not an array"));

        // Non-objects and a missing root still pass through.
        assert_eq!(c.try_apply(json!(7)).unwrap(), vec![json!(7)]);
        let bare = json!({"dimHeaders": [], "metHeaders": []});
        assert_eq!(c.try_apply(bare.clone()).unwrap(), vec![bare]);
    }

    #[test]
    fn groups_compile_validation_and_stage() {
        let bad = |v: Value| {
            serde_json::from_value::<TreeFlattenSpec>(v)
                .unwrap()
                .compile()
                .unwrap_err()
                .to_string()
        };
        let g = json!({"from": "a", "header": "h"});
        assert!(bad(json!({"children": "rows"})).contains("`columns.from`"));
        assert!(
            bad(json!({"children": "rows", "columns": {"from": "c"}, "groups": [g]}))
                .contains("exactly one")
        );
        assert!(bad(json!({"children": "rows", "groups": [{"from": "a"}]})).contains("group 0"));
        assert!(
            bad(json!({"children": "rows", "groups": [{"from": "a", "header": " "}]}))
                .contains("group 0")
        );
        let s = grouped();
        let back: TreeFlattenSpec =
            serde_json::from_value(serde_json::to_value(&s).unwrap()).unwrap();
        assert_eq!(back, s);
        assert!(serde_json::to_value(&s).unwrap().get("columns").is_none());

        match s.into_stage().unwrap() {
            TransformStage::PageFn(f) => {
                assert_eq!(f(vec![grouped_report()]).unwrap().len(), 2);
                let mut r = grouped_report();
                r["rows"][0]["rows"][0]["mets"] = json!([]);
                assert!(f(vec![r]).is_err());
            }
            other => panic!("expected PageFn, got {other:?}"),
        }
    }

    #[test]
    fn group_rows_count_any_groups_cells() {
        let mut s = grouped();
        s.emit_group_rows = true;
        let c = s.compile().unwrap();
        let mut r = grouped_report();
        r["rows"][0]["dims"] = json!([{"value": "total"}, {"value": "*"}]);
        r["rows"][0]["mets"] = json!([{"value": "19"}]);
        let out = c.try_apply(r).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["sessions"], json!("19"));
    }
}
