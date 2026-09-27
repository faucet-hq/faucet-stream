//! Runtime matrix-row selection — the composable selection model spanning
//! four issues that resolve through **one** eligibility → narrowing → parents
//! → skip formula:
//!
//! ```text
//! 1. eligible  = status gate ({mandatory, active} ∪ --status)          # #371
//! 2. narrowed  = (eligible ∩ --tag) ∪ (--select / --only by id)        # #376 / #370
//! 3. parents   = apply include_parents policy to narrowed              # #377
//! 4. run set   = parents − (--skip)                                    # #370
//! ```
//!
//! - **#370 — identity.** `--select <id>` (exact) / `--only <glob>` force-include
//!   a row *by name*, bypassing the status gate; `--skip <id|glob>` removes last.
//! - **#371 — readiness (`status`).** Each row's source carries a
//!   [`SourceStatus`] ladder. The status gate decides *eligibility*;
//!   `--status <tier>` additively widens the eligible set.
//! - **#376 — classification (`tags`).** `--tag <t>` narrows *within* the
//!   eligible set. A tag can only shrink the eligible set, never resurrect a
//!   non-ready (`available`/`draft`/`archived`) row.
//! - **#377 — `include_parents`.** The single, explicit policy that decides
//!   whether a selected row's `parent:` / `depends_on:` ancestors are pulled in.
//!   Default `off` (strict): a required ancestor missing from the run set is a
//!   hard, fail-fast error.
//!
//! Selection runs on the **expanded node list** (after `expand()`), so it never
//! alters `{name}::{row_id}` state-key derivation — bookmarks stay identical
//! across a full run and any selected subset.

use crate::config::{IncludeParents, SelectionSpec, SourceStatus};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// A fully-resolved selection request, built from CLI flags + config.
#[derive(Debug, Clone, Default)]
pub struct RunSelection {
    /// Exact row ids to force-include (bypass status/tags).
    pub select: Vec<String>,
    /// Glob patterns to force-include by id (bypass status/tags).
    pub only: Vec<String>,
    /// Row ids / globs to remove from the run set (applied last).
    pub skip: Vec<String>,
    /// Status tiers to add to the default `{mandatory, active}` eligible set.
    pub status: Vec<SourceStatus>,
    /// Tags to narrow the eligible set by (union within the list).
    pub tags: Vec<String>,
    /// Parent/dependency inclusion policy.
    pub include_parents: IncludeParents,
}

impl RunSelection {
    /// Resolve raw CLI strings + the config's `selection:` block into a typed
    /// [`RunSelection`]. Parses `--status` tiers and `--include-parents`,
    /// surfacing typed errors on unknown values. Precedence for the policy:
    /// `--include-parents` flag/env > `selection.include_parents` in config >
    /// built-in default (`off`).
    #[allow(clippy::too_many_arguments)]
    pub fn resolve(
        select: &[String],
        only: &[String],
        skip: &[String],
        status: &[String],
        tags: &[String],
        include_parents_flag: Option<&str>,
        cfg_selection: Option<&SelectionSpec>,
    ) -> CliResult<Self> {
        let status = status
            .iter()
            .map(|s| {
                SourceStatus::parse(s).ok_or_else(|| CliError::UnknownStatus {
                    value: s.clone(),
                    available: SourceStatus::ALL
                        .iter()
                        .map(|v| v.as_str().to_owned())
                        .collect(),
                })
            })
            .collect::<CliResult<Vec<_>>>()?;

        let include_parents = match include_parents_flag {
            Some(s) => IncludeParents::parse(s).ok_or_else(|| CliError::UnknownIncludeParents {
                value: s.to_owned(),
            })?,
            None => cfg_selection.map(|s| s.include_parents).unwrap_or_default(),
        };

        Ok(Self {
            select: dedup(select),
            only: dedup(only),
            skip: dedup(skip),
            status,
            tags: dedup(tags),
            include_parents,
        })
    }

    /// Build from the shared CLI [`SelectionArgs`](crate::cli::SelectionArgs)
    /// plus the config's `selection:` block.
    pub fn from_args(
        args: &crate::cli::SelectionArgs,
        cfg_selection: Option<&SelectionSpec>,
    ) -> CliResult<Self> {
        Self::resolve(
            &args.select,
            &args.only,
            &args.skip,
            &args.status,
            &args.tags,
            args.include_parents.as_deref(),
            cfg_selection,
        )
    }

    /// Whether any selector actively narrows/widens the run set (so callers
    /// like `validate` know to print a selection report). `--include-parents`
    /// alone does not count — it only governs ancestor inclusion.
    pub fn narrows(&self) -> bool {
        self.has_matrix_only_selector() || !self.status.is_empty()
    }

    /// Any row-narrowing selector present (matrix-only flags). `--status` and
    /// `--include-parents` are excluded because they are meaningful even on a
    /// single anonymous row.
    fn has_matrix_only_selector(&self) -> bool {
        !self.select.is_empty()
            || !self.only.is_empty()
            || !self.skip.is_empty()
            || !self.tags.is_empty()
    }
}

/// Run label carrying a run's canonical selection (#741), so a subset run is
/// distinguishable in `GET /v1/runs`.
pub const LABEL_SELECTION: &str = "selection";

/// The wire form of a row selection (#741) — the `selection` object every
/// run-starting surface accepts (HTTP bodies, MCP arguments, trigger files,
/// suites). Mirrors the CLI flags one for one; every field is optional, and an
/// empty object applies only the status gate.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SelectionRequest {
    /// Row ids to run, exactly (bypasses the status gate).
    #[serde(default)]
    pub select: Vec<String>,
    /// Row-id globs to run (`act*`; bypasses the status gate).
    #[serde(default)]
    pub only: Vec<String>,
    /// Row ids / globs removed last. A `mandatory` row needs an exact id.
    #[serde(default)]
    pub skip: Vec<String>,
    /// Narrow the eligible rows to those carrying any of these tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Status tiers added to the default eligible set (`mandatory`, `active`).
    #[serde(default)]
    pub status: Vec<SourceStatus>,
    /// Whether a selected row's missing `parent:` / `depends_on:` ancestors
    /// are pulled in: `off` (default — refuse), `eligible`, or `all`.
    #[serde(default)]
    pub include_parents: Option<IncludeParents>,
}

impl SelectionRequest {
    /// Parse the CLI flags (typed errors on an unknown status tier or policy).
    pub fn from_args(args: &crate::cli::SelectionArgs) -> CliResult<Self> {
        let sel = RunSelection::from_args(args, None)?;
        let mut req = Self::from_run_selection(&sel);
        req.include_parents = args.include_parents.as_ref().map(|_| sel.include_parents);
        Ok(req)
    }

    /// The flags as a selection, `None` when no selection flag is set.
    pub fn from_flags(args: &crate::cli::SelectionArgs) -> CliResult<Option<Self>> {
        let any = !args.select.is_empty()
            || !args.only.is_empty()
            || !args.skip.is_empty()
            || !args.tags.is_empty()
            || !args.status.is_empty()
            || args.include_parents.is_some();
        if any {
            Self::from_args(args).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Back to CLI flags (for handing a resolved selection to `faucet run`'s
    /// execution path).
    pub fn to_args(&self) -> crate::cli::SelectionArgs {
        crate::cli::SelectionArgs {
            select: self.select.clone(),
            only: self.only.clone(),
            skip: self.skip.clone(),
            status: self.status.iter().map(|s| s.as_str().to_string()).collect(),
            tags: self.tags.clone(),
            include_parents: self.include_parents.map(|p| p.as_str().to_string()),
        }
    }

    /// The wire form of a resolved selection (policy always explicit).
    pub fn from_run_selection(sel: &RunSelection) -> Self {
        Self {
            select: sel.select.clone(),
            only: sel.only.clone(),
            skip: sel.skip.clone(),
            tags: sel.tags.clone(),
            status: sel.status.clone(),
            include_parents: Some(sel.include_parents),
        }
    }

    /// The typed selection, with the config's `selection.include_parents` as
    /// the policy when the request does not name one.
    pub fn to_run_selection(&self, cfg: Option<&SelectionSpec>) -> RunSelection {
        let mut status = Vec::new();
        for s in &self.status {
            if !status.contains(s) {
                status.push(*s);
            }
        }
        RunSelection {
            select: dedup(&self.select),
            only: dedup(&self.only),
            skip: dedup(&self.skip),
            status,
            tags: dedup(&self.tags),
            include_parents: self
                .include_parents
                .or_else(|| cfg.map(|c| c.include_parents))
                .unwrap_or_default(),
        }
    }

    /// Whether no field is set (the status gate alone).
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// A stable one-line form — sorted, deduplicated, fields in a fixed order:
    /// `select=contacts,deals;include_parents=eligible`, or `default` for an
    /// empty selection. The `selection` run label and the change-request
    /// fingerprint use it.
    pub fn canonical(&self) -> String {
        fn list(name: &str, items: &[String], out: &mut Vec<String>) {
            if items.is_empty() {
                return;
            }
            let set: BTreeSet<&str> = items.iter().map(String::as_str).collect();
            out.push(format!(
                "{name}={}",
                set.into_iter().collect::<Vec<_>>().join(",")
            ));
        }
        let mut parts = Vec::new();
        list("select", &self.select, &mut parts);
        list("only", &self.only, &mut parts);
        list("skip", &self.skip, &mut parts);
        list("tags", &self.tags, &mut parts);
        let status: Vec<String> = self.status.iter().map(|s| s.as_str().to_string()).collect();
        list("status", &status, &mut parts);
        if let Some(p) = self.include_parents {
            parts.push(format!("include_parents={}", p.as_str()));
        }
        if parts.is_empty() {
            "default".to_string()
        } else {
            parts.join(";")
        }
    }

    /// Apply to a loaded config's expanded rows. A topology-mode config
    /// (`pipeline.nodes`) has no rows to select, so any selection is refused
    /// rather than ignored.
    pub fn apply(
        &self,
        cfg: &crate::config::PipelineConfig,
        nodes: Vec<ExpandedNode>,
    ) -> CliResult<Vec<ExpandedNode>> {
        refuse_topology(cfg)?;
        select_nodes(
            nodes,
            &self.to_run_selection(cfg.selection.as_ref()),
            !cfg.matrix.is_empty(),
        )
    }
}

/// Why a selection on a topology-mode config is refused.
pub const TOPOLOGY_REFUSAL: &str = "this pipeline is a topology (`pipeline.nodes`) — it has no \
     matrix rows to select; run it without a selection";

/// Whether `e` is a refused selection (an unknown row / tag / status tier, an
/// empty run set, a missing ancestor, a topology) — a caller error, which the
/// control plane answers with 400.
pub fn is_selection_error(e: &CliError) -> bool {
    matches!(
        e,
        CliError::NoMatchForSelector { .. }
            | CliError::UnknownTag { .. }
            | CliError::EmptyRunSet { .. }
            | CliError::RunSetMissingAncestors { .. }
            | CliError::SelectorsWithoutMatrix { .. }
            | CliError::UnknownStatus { .. }
            | CliError::UnknownIncludeParents { .. }
    ) || matches!(e, CliError::Config(m) if m == TOPOLOGY_REFUSAL)
}

/// The refusal for a row selection on a topology-mode config.
pub fn refuse_topology(cfg: &crate::config::PipelineConfig) -> CliResult<()> {
    if crate::topology::is_topology(cfg) {
        return Err(CliError::Config(TOPOLOGY_REFUSAL.into()));
    }
    Ok(())
}

/// Apply `sel` to `nodes` (expanded, in BFS order) and return the running
/// subset, order-preserved. Errors on unknown tokens, an empty run set, or a
/// dependency violation under the active `include_parents` policy.
///
/// `has_matrix` is `false` for the single anonymous invocation (no `matrix:`);
/// matrix-only selectors (`--select`/`--only`/`--skip`/`--tag`) are then a hard
/// error. The status gate still applies to the lone row.
pub fn select_nodes(
    nodes: Vec<ExpandedNode>,
    sel: &RunSelection,
    has_matrix: bool,
) -> CliResult<Vec<ExpandedNode>> {
    let resolution = resolve(&nodes, sel, has_matrix);
    if let Some(e) = resolution.error {
        return Err(e);
    }
    let run: HashSet<&str> = resolution.run_set.iter().map(String::as_str).collect();
    Ok(nodes
        .into_iter()
        .filter(|n| run.contains(n.id.as_str()))
        .collect())
}

/// Why one row is — or is not — in a resolved run set (#741).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum RowDecision {
    /// Picked by the selection itself: `by` is `default` (the status gate with
    /// no narrowing selector), `select`, `only`, or `tag`.
    Selected { by: &'static str },
    /// Added as a required ancestor of `because` under `include_parents`.
    PulledIn { because: String },
    /// Not in the run set, and nothing needs it.
    Excluded { reason: String },
    /// Needed by a row in the run set but not allowed into it — the reason the
    /// selection is refused.
    Blocked { reason: String },
}

impl RowDecision {
    /// Whether the row runs.
    pub fn runs(&self) -> bool {
        matches!(self, Self::Selected { .. } | Self::PulledIn { .. })
    }
}

/// A selection resolved against a node list without running anything: the run
/// set in execution order, each row's decision, and the error a trigger with
/// this selection would return.
#[derive(Debug)]
pub struct Resolution {
    /// Row ids that run, in execution order (dependency level, then declared
    /// order). Empty when `error` is set.
    pub run_set: Vec<String>,
    /// One decision per row. Empty when the selection names an unknown row or
    /// tag (nothing is decided before the typo is fixed).
    pub decisions: HashMap<String, RowDecision>,
    pub error: Option<CliError>,
}

/// Resolve `sel` against `nodes` — the one implementation behind
/// [`select_nodes`] and the rows API's dry-run resolve.
pub fn resolve(nodes: &[ExpandedNode], sel: &RunSelection, has_matrix: bool) -> Resolution {
    let fail = |error: CliError, decisions: HashMap<String, RowDecision>| Resolution {
        run_set: Vec::new(),
        decisions,
        error: Some(error),
    };
    if !has_matrix && sel.has_matrix_only_selector() {
        let mut flags = Vec::new();
        if !sel.select.is_empty() {
            flags.push("--select");
        }
        if !sel.only.is_empty() {
            flags.push("--only");
        }
        if !sel.skip.is_empty() {
            flags.push("--skip");
        }
        if !sel.tags.is_empty() {
            flags.push("--tag");
        }
        return fail(
            CliError::SelectorsWithoutMatrix {
                flags: flags.join(", "),
            },
            HashMap::new(),
        );
    }

    // Typo protection: every identity/skip token must match ≥1 row id, and
    // every requested tag must be present on some row. Checked against the
    // full node set (before any gating), so a typo is caught regardless of
    // status.
    for token in &sel.select {
        if !nodes.iter().any(|n| &n.id == token) {
            return fail(
                CliError::NoMatchForSelector {
                    flag: "--select",
                    token: token.clone(),
                    available: all_ids(nodes),
                },
                HashMap::new(),
            );
        }
    }
    for token in sel.only.iter().chain(sel.skip.iter()) {
        let flag = if sel.only.contains(token) {
            "--only"
        } else {
            "--skip"
        };
        if !nodes.iter().any(|n| token_matches(token, &n.id)) {
            return fail(
                CliError::NoMatchForSelector {
                    flag,
                    token: token.clone(),
                    available: all_ids(nodes),
                },
                HashMap::new(),
            );
        }
    }
    if !sel.tags.is_empty() {
        let present: BTreeSet<&str> = nodes
            .iter()
            .flat_map(|n| n.tags.iter().map(String::as_str))
            .collect();
        for tag in &sel.tags {
            if !present.contains(tag.as_str()) {
                return fail(
                    CliError::UnknownTag {
                        tag: tag.clone(),
                        available: present.iter().map(|s| (*s).to_owned()).collect(),
                    },
                    HashMap::new(),
                );
            }
        }
    }

    // Effective status set = {mandatory, active} ∪ --status.
    let mut active_status: HashSet<SourceStatus> =
        HashSet::from([SourceStatus::Mandatory, SourceStatus::Active]);
    active_status.extend(sel.status.iter().copied());

    let has_identity = !sel.select.is_empty() || !sel.only.is_empty();
    let has_tag = !sel.tags.is_empty();

    let is_eligible = |n: &ExpandedNode| active_status.contains(&n.status);
    let by_select = |n: &ExpandedNode| sel.select.iter().any(|id| id == &n.id);
    let by_only = |n: &ExpandedNode| sel.only.iter().any(|g| token_matches(g, &n.id));
    let matches_tag = |n: &ExpandedNode| sel.tags.iter().any(|t| n.tags.iter().any(|nt| nt == t));

    // Stage 1 + 2: eligibility → narrowing.
    let mut decisions: HashMap<String, RowDecision> = HashMap::new();
    let mut run: HashSet<String> = HashSet::new();
    for n in nodes {
        let decision = if !has_identity && !has_tag {
            if is_eligible(n) {
                RowDecision::Selected { by: "default" }
            } else {
                RowDecision::Excluded {
                    reason: parked_reason(n.status),
                }
            }
        } else if has_identity && by_select(n) {
            RowDecision::Selected { by: "select" }
        } else if has_identity && by_only(n) {
            RowDecision::Selected { by: "only" }
        } else if has_tag && matches_tag(n) && is_eligible(n) {
            RowDecision::Selected { by: "tag" }
        } else if has_tag && matches_tag(n) {
            RowDecision::Excluded {
                reason: format!(
                    "tagged, but {} — a tag never resurrects a parked row",
                    parked_reason(n.status)
                ),
            }
        } else {
            RowDecision::Excluded {
                reason: "not selected".to_string(),
            }
        };
        if decision.runs() {
            run.insert(n.id.clone());
        }
        decisions.insert(n.id.clone(), decision);
    }

    // Stage 3: parent / dependency closure under the include_parents policy.
    let node_by_id: HashMap<&str, &ExpandedNode> =
        nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    if let Err(e) = apply_parent_policy(
        &node_by_id,
        &active_status,
        sel.include_parents,
        &mut run,
        &mut decisions,
    ) {
        return fail(e, decisions);
    }

    // Stage 4: skip (applied last). A `mandatory` row is removable only by an
    // exact `--skip <id>`, never by a glob.
    for n in nodes {
        if !run.contains(&n.id) {
            continue;
        }
        let mandatory = n.status == SourceStatus::Mandatory;
        if let Some(tok) = sel
            .skip
            .iter()
            .find(|tok| skip_matches(tok, &n.id, mandatory))
        {
            run.remove(&n.id);
            decisions.insert(
                n.id.clone(),
                RowDecision::Excluded {
                    reason: format!("removed by skip `{tok}`"),
                },
            );
        }
    }

    // Post-skip integrity: skipping a row that a surviving row structurally
    // depends on would orphan the dependent (a child can't fan out without its
    // parent). Fail fast rather than run a broken graph.
    let mut orphans: Vec<String> = Vec::new();
    for n in nodes {
        if !run.contains(&n.id) {
            continue;
        }
        for (anc, kind) in required_ancestors(n) {
            if !run.contains(&anc) {
                orphans.push(format!("{} → {anc} ({kind})", n.id));
                decisions.insert(
                    n.id.clone(),
                    RowDecision::Blocked {
                        reason: format!("its {kind} `{anc}` was removed from the run set"),
                    },
                );
            }
        }
    }
    if !orphans.is_empty() {
        orphans.sort();
        orphans.dedup();
        return fail(
            CliError::RunSetMissingAncestors {
                pairs: orphans,
                policy: sel.include_parents.as_str(),
            },
            decisions,
        );
    }

    if run.is_empty() {
        let rows = nodes
            .iter()
            .map(|n| format!("{} [{}]", n.id, n.status.as_str()))
            .collect();
        return fail(CliError::EmptyRunSet { rows }, decisions);
    }

    let depths = execution_depths(nodes);
    let mut run_set: Vec<&ExpandedNode> = nodes.iter().filter(|n| run.contains(&n.id)).collect();
    run_set.sort_by_key(|n| depths.get(n.id.as_str()).copied().unwrap_or(0));
    Resolution {
        run_set: run_set.into_iter().map(|n| n.id.clone()).collect(),
        decisions,
        error: None,
    }
}

fn parked_reason(status: SourceStatus) -> String {
    format!(
        "status `{}` is not in the default eligible set (mandatory, active)",
        status.as_str()
    )
}

/// Each row's execution level: `0` for a row with no `parent:` / `depends_on:`
/// edge, else one more than its deepest ancestor. Rows at the same level may
/// run together.
pub fn execution_depths(nodes: &[ExpandedNode]) -> HashMap<&str, usize> {
    let by_id: HashMap<&str, &ExpandedNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    fn depth<'a>(
        id: &'a str,
        by_id: &HashMap<&'a str, &'a ExpandedNode>,
        memo: &mut HashMap<&'a str, usize>,
        guard: usize,
    ) -> usize {
        if let Some(d) = memo.get(id) {
            return *d;
        }
        let d = match by_id.get(id) {
            Some(n) if guard < by_id.len() => required_ancestors(n)
                .iter()
                .filter_map(|(a, _)| by_id.get_key_value(a.as_str()).map(|(k, _)| *k))
                .map(|a| depth(a, by_id, memo, guard + 1) + 1)
                .max()
                .unwrap_or(0),
            _ => 0,
        };
        memo.insert(id, d);
        d
    }
    let mut memo = HashMap::new();
    for n in nodes {
        depth(n.id.as_str(), &by_id, &mut memo, 0);
    }
    memo
}

/// Walk the `parent:` / `depends_on:` ancestor closure of the current run set,
/// adding or rejecting ancestors per the policy. Collects **every** offending
/// pair (transitively) before erroring.
fn apply_parent_policy(
    node_by_id: &HashMap<&str, &ExpandedNode>,
    active_status: &HashSet<SourceStatus>,
    policy: IncludeParents,
    run: &mut HashSet<String>,
    decisions: &mut HashMap<String, RowDecision>,
) -> CliResult<()> {
    let mut violations: Vec<String> = Vec::new();
    let mut queue: VecDeque<String> = run.iter().cloned().collect();
    while let Some(id) = queue.pop_front() {
        // `id` is always a real node (run set only ever holds known ids).
        let node = match node_by_id.get(id.as_str()) {
            Some(n) => *n,
            None => continue,
        };
        for (anc, kind) in required_ancestors(node) {
            if run.contains(&anc) {
                continue;
            }
            // Ancestor id validity was proven at expand time.
            let anc_status = node_by_id.get(anc.as_str()).map(|n| n.status);
            let eligible = anc_status
                .map(|s| active_status.contains(&s))
                .unwrap_or(false);
            let pulled = |decisions: &mut HashMap<String, RowDecision>| {
                decisions.insert(
                    anc.clone(),
                    RowDecision::PulledIn {
                        because: id.clone(),
                    },
                );
            };
            match policy {
                IncludeParents::Off => {
                    violations.push(format!("{id} → {anc} ({kind})"));
                    decisions.insert(
                        anc.clone(),
                        RowDecision::Blocked {
                            reason: format!(
                                "required by `{id}` ({kind}) but not selected — include_parents is \
                                 off; select it, or use include_parents: eligible / all"
                            ),
                        },
                    );
                }
                IncludeParents::Eligible => {
                    if eligible {
                        if run.insert(anc.clone()) {
                            tracing::info!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=eligible: auto-included required ancestor"
                            );
                            pulled(decisions);
                            queue.push_back(anc);
                        }
                    } else {
                        violations.push(format!("{id} → {anc} ({kind}, parked)"));
                        decisions.insert(
                            anc.clone(),
                            RowDecision::Blocked {
                                reason: format!(
                                    "required by `{id}` ({kind}) but parked (status `{}`) — \
                                     include_parents: eligible never pulls in a parked row; use \
                                     all, or select it by id",
                                    anc_status.map(SourceStatus::as_str).unwrap_or("unknown")
                                ),
                            },
                        );
                    }
                }
                IncludeParents::All => {
                    if run.insert(anc.clone()) {
                        if eligible {
                            tracing::info!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=all: auto-included required ancestor"
                            );
                        } else {
                            tracing::warn!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=all: pulling a parked ancestor into the run set"
                            );
                        }
                        pulled(decisions);
                        queue.push_back(anc);
                    }
                }
            }
        }
    }
    if !violations.is_empty() {
        violations.sort();
        violations.dedup();
        return Err(CliError::RunSetMissingAncestors {
            pairs: violations,
            policy: policy.as_str(),
        });
    }
    Ok(())
}

/// The `parent:` + `depends_on:` edges of `node` — the "required ancestors" a
/// run-set row cannot execute correctly without.
fn required_ancestors(node: &ExpandedNode) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    if let NodeRole::Child { parent_id, .. } = &node.role {
        out.push((parent_id.clone(), "parent"));
    }
    for d in &node.depends_on {
        out.push((d.clone(), "depends_on"));
    }
    out
}

fn all_ids(nodes: &[ExpandedNode]) -> Vec<String> {
    nodes.iter().map(|n| n.id.clone()).collect()
}

/// Dedup a token list, preserving first-seen order.
fn dedup(items: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for it in items {
        if seen.insert(it.clone()) {
            out.push(it.clone());
        }
    }
    out
}

/// Whether a `--skip` token removes `id`. A glob token never removes a
/// `mandatory` row; an exact-id token removes any row (including mandatory).
fn skip_matches(token: &str, id: &str, mandatory: bool) -> bool {
    if has_glob(token) {
        !mandatory && glob_match(token, id)
    } else {
        token == id
    }
}

/// Whether a token (exact id or glob) matches `id`.
fn token_matches(token: &str, id: &str) -> bool {
    if has_glob(token) {
        glob_match(token, id)
    } else {
        token == id
    }
}

fn has_glob(s: &str) -> bool {
    s.contains('*') || s.contains('?')
}

/// Minimal `*` (any run, incl. empty) / `?` (exactly one char) glob matcher.
/// Sufficient for row-id selection; no character classes or escaping.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    // Iterative backtracking match.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_with_extension;
    use crate::expand::expand;

    /// Build expanded nodes from YAML for selection tests.
    fn nodes(yaml: &str) -> Vec<ExpandedNode> {
        expand(&parse_with_extension(yaml, "yaml").unwrap()).unwrap()
    }

    fn ids(nodes: &[ExpandedNode]) -> Vec<String> {
        let mut v: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
        v.sort();
        v
    }

    fn sel() -> RunSelection {
        RunSelection::default()
    }

    /// A HiBob-style multi-endpoint matrix used by most tests.
    const HIBOB: &str = r#"
version: 1
pipeline:
  sources:
    hibob: { type: rest, config: { base_url: https://api.hibob.com } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: people
    source: { ref: hibob, status: active, config: { path: /people } }
    sink: { ref: wh }
    tags: [core, daily]
  - id: payroll
    source: { ref: hibob, status: mandatory, config: { path: /payroll } }
    sink: { ref: wh }
    tags: [finance]
  - id: audit
    source: { ref: hibob, status: available, config: { path: /audit } }
    sink: { ref: wh }
    tags: [finance]
  - id: beta
    source: { ref: hibob, status: draft, config: { path: /beta } }
    sink: { ref: wh }
"#;

    #[test]
    fn glob_matches_star_and_question() {
        assert!(glob_match("timeoff_*", "timeoff_requests"));
        assert!(glob_match("timeoff_*", "timeoff_"));
        assert!(!glob_match("timeoff_*", "people"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("p*e", "people"));
        assert!(!glob_match("p*e", "payroll"));
    }

    #[test]
    fn bare_run_includes_mandatory_and_active_only() {
        let out = select_nodes(nodes(HIBOB), &sel(), true).unwrap();
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn status_widens_eligible_set_additively() {
        let s = RunSelection {
            status: vec![SourceStatus::Available],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["audit", "payroll", "people"]);
    }

    #[test]
    fn select_by_id_bypasses_status_gate() {
        // `beta` is draft (parked) but explicitly selected → runs anyway.
        let s = RunSelection {
            select: vec!["beta".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["beta"]);
    }

    #[test]
    fn only_glob_selects_subset() {
        let s = RunSelection {
            only: vec!["p*".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        // p* matches people + payroll (both identity-selected, status bypassed).
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn tag_narrows_within_eligible_only() {
        // `finance` tags payroll (mandatory, eligible) + audit (available, NOT
        // eligible). Bare --tag finance keeps only the eligible one.
        let s = RunSelection {
            tags: vec!["finance".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll"]);
    }

    #[test]
    fn tag_plus_status_resurrects_parked_row() {
        let s = RunSelection {
            tags: vec!["finance".into()],
            status: vec![SourceStatus::Available],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["audit", "payroll"]);
    }

    #[test]
    fn skip_removes_after_selection() {
        let s = RunSelection {
            status: vec![SourceStatus::Available],
            skip: vec!["audit".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn mandatory_survives_glob_skip_but_not_exact_skip() {
        // A glob skip cannot drop the mandatory `payroll` row…
        let s = RunSelection {
            skip: vec!["p*".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll"]);
        // …but an exact-id skip can.
        let s = RunSelection {
            select: vec!["payroll".into()],
            skip: vec!["payroll".into()],
            ..sel()
        };
        let err = select_nodes(nodes(HIBOB), &s, true).unwrap_err();
        assert!(matches!(err, CliError::EmptyRunSet { .. }), "got {err:?}");
    }

    #[test]
    fn unknown_select_token_errors_with_available() {
        let s = RunSelection {
            select: vec!["peeple".into()],
            ..sel()
        };
        match select_nodes(nodes(HIBOB), &s, true).unwrap_err() {
            CliError::NoMatchForSelector {
                flag,
                token,
                available,
            } => {
                assert_eq!(flag, "--select");
                assert_eq!(token, "peeple");
                assert!(available.contains(&"people".to_string()));
            }
            other => panic!("expected NoMatchForSelector, got {other:?}"),
        }
    }

    #[test]
    fn unknown_tag_errors() {
        let s = RunSelection {
            tags: vec!["nope".into()],
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(HIBOB), &s, true).unwrap_err(),
            CliError::UnknownTag { .. }
        ));
    }

    #[test]
    fn empty_run_set_errors_when_all_parked() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: a
    source: { ref: api, status: available }
    sink: { ref: wh }
  - id: b
    source: { ref: api, status: draft }
    sink: { ref: wh }
"#;
        assert!(matches!(
            select_nodes(nodes(yaml), &sel(), true).unwrap_err(),
            CliError::EmptyRunSet { .. }
        ));
    }

    const DEPS: &str = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: active }
    sink: { ref: wh }
    tags: [core]
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    tags: [finance]
    depends_on: [dims]
"#;

    #[test]
    fn include_parents_off_errors_on_missing_ancestor() {
        // Selecting only `facts` (via tag) drops its `depends_on: dims`.
        let s = RunSelection {
            tags: vec!["finance".into()],
            include_parents: IncludeParents::Off,
            ..sel()
        };
        match select_nodes(nodes(DEPS), &s, true).unwrap_err() {
            CliError::RunSetMissingAncestors { pairs, policy } => {
                assert_eq!(policy, "off");
                assert!(
                    pairs
                        .iter()
                        .any(|p| p.contains("facts") && p.contains("dims"))
                );
            }
            other => panic!("expected RunSetMissingAncestors, got {other:?}"),
        }
    }

    #[test]
    fn include_parents_eligible_pulls_in_active_ancestor() {
        let s = RunSelection {
            tags: vec!["finance".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        let out = select_nodes(nodes(DEPS), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn include_parents_eligible_errors_on_parked_ancestor() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: available }
    sink: { ref: wh }
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    depends_on: [dims]
"#;
        let s = RunSelection {
            select: vec!["facts".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(yaml), &s, true).unwrap_err(),
            CliError::RunSetMissingAncestors { .. }
        ));
    }

    #[test]
    fn include_parents_all_pulls_in_parked_ancestor() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: draft }
    sink: { ref: wh }
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    depends_on: [dims]
"#;
        let s = RunSelection {
            select: vec!["facts".into()],
            include_parents: IncludeParents::All,
            ..sel()
        };
        let out = select_nodes(nodes(yaml), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn select_ancestor_by_id_satisfies_dependency() {
        let s = RunSelection {
            select: vec!["facts".into(), "dims".into()],
            include_parents: IncludeParents::Off,
            ..sel()
        };
        let out = select_nodes(nodes(DEPS), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn parent_edge_closure_respected() {
        // `posts` is a per-record child of `users`; selecting only `posts` must
        // pull in `users` under `eligible`.
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { ref: api, status: active }
    sink: { ref: wh }
  - id: posts
    parent: users
    source: { ref: api, status: active, config: { path: "/u/${users.id}/posts" } }
    sink: { ref: wh }
"#;
        let s = RunSelection {
            select: vec!["posts".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        let out = select_nodes(nodes(yaml), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["posts", "users"]);
    }

    #[test]
    fn matrix_only_selectors_rejected_without_matrix() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#;
        let s = RunSelection {
            select: vec!["row-0".into()],
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(yaml), &s, false).unwrap_err(),
            CliError::SelectorsWithoutMatrix { .. }
        ));
    }

    #[test]
    fn no_selectors_keeps_plain_config_unchanged() {
        // A matrix with no status/tags anywhere must run every row (no
        // behaviour change for pre-selection configs).
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - { id: a }
  - { id: b }
  - { id: c }
"#;
        let out = select_nodes(nodes(yaml), &sel(), true).unwrap();
        assert_eq!(ids(&out), vec!["a", "b", "c"]);
    }

    #[test]
    fn selection_request_canonical_is_stable() {
        let a = SelectionRequest {
            select: vec!["deals".into(), "contacts".into(), "deals".into()],
            status: vec![SourceStatus::Available],
            tags: vec!["b".into(), "a".into()],
            include_parents: Some(IncludeParents::Eligible),
            ..Default::default()
        };
        assert_eq!(
            a.canonical(),
            "select=contacts,deals;tags=a,b;status=available;include_parents=eligible"
        );
        assert_eq!(SelectionRequest::default().canonical(), "default");
        assert!(SelectionRequest::default().is_empty());
        let b = SelectionRequest {
            only: vec!["a*".into()],
            skip: vec!["x".into()],
            ..Default::default()
        };
        assert_eq!(b.canonical(), "only=a*;skip=x");
    }

    #[test]
    fn selection_request_policy_falls_back_to_config() {
        let req = SelectionRequest {
            select: vec!["a".into(), "a".into()],
            status: vec![SourceStatus::Draft, SourceStatus::Draft],
            ..Default::default()
        };
        let cfg = SelectionSpec {
            include_parents: IncludeParents::All,
        };
        let sel = req.to_run_selection(Some(&cfg));
        assert_eq!(sel.select, vec!["a"]);
        assert_eq!(sel.status, vec![SourceStatus::Draft]);
        assert_eq!(sel.include_parents, IncludeParents::All);
        assert_eq!(
            req.to_run_selection(None).include_parents,
            IncludeParents::Off
        );
        let explicit = SelectionRequest {
            include_parents: Some(IncludeParents::Eligible),
            ..Default::default()
        };
        assert_eq!(
            explicit.to_run_selection(Some(&cfg)).include_parents,
            IncludeParents::Eligible
        );
    }

    #[test]
    fn selection_request_round_trips_cli_flags() {
        assert!(
            SelectionRequest::from_flags(&crate::cli::SelectionArgs::default())
                .unwrap()
                .is_none()
        );
        let args = crate::cli::SelectionArgs {
            select: vec!["a".into()],
            status: vec!["draft".into()],
            include_parents: Some("all".into()),
            ..Default::default()
        };
        let req = SelectionRequest::from_flags(&args).unwrap().unwrap();
        assert_eq!(req.include_parents, Some(IncludeParents::All));
        let back = req.to_args();
        assert_eq!(back.status, vec!["draft"]);
        assert_eq!(back.include_parents.as_deref(), Some("all"));
        let no_policy = crate::cli::SelectionArgs {
            tags: vec!["t".into()],
            ..Default::default()
        };
        let req = SelectionRequest::from_flags(&no_policy).unwrap().unwrap();
        assert_eq!(req.include_parents, None);
        let bad = crate::cli::SelectionArgs {
            status: vec!["nope".into()],
            ..Default::default()
        };
        assert!(SelectionRequest::from_flags(&bad).is_err());
        let json: SelectionRequest = serde_json::from_value(
            serde_json::json!({"select": ["a"], "include_parents": "eligible"}),
        )
        .unwrap();
        assert_eq!(json.include_parents, Some(IncludeParents::Eligible));
        assert!(
            serde_json::from_value::<SelectionRequest>(serde_json::json!({"rows": ["a"]})).is_err()
        );
    }

    #[test]
    fn apply_refuses_topology_and_selects_matrix_rows() {
        let topo = parse_with_extension(
            "version: 1\nname: t\npipeline:\n  sources:\n    s: { type: csv, config: { path: a.csv } }\n  sinks:\n    o: { type: jsonl, config: { path: o.jsonl } }\n  nodes:\n    src: { kind: source, ref: s }\n    w: { kind: sink, ref: o }\n  edges:\n    - { from: src, to: w }\n",
            "yaml",
        )
        .unwrap();
        let req = SelectionRequest {
            select: vec!["src".into()],
            ..Default::default()
        };
        let err = req.apply(&topo, Vec::new()).unwrap_err();
        assert!(is_selection_error(&err));
        let cfg = parse_with_extension(HIBOB, "yaml").unwrap();
        let req = SelectionRequest {
            select: vec!["beta".into()],
            ..Default::default()
        };
        let out = req.apply(&cfg, expand(&cfg).unwrap()).unwrap();
        assert_eq!(ids(&out), vec!["beta"]);
        assert!(!is_selection_error(&CliError::Config("x".into())));
        assert!(is_selection_error(&CliError::EmptyRunSet { rows: vec![] }));
    }

    #[test]
    fn resolve_explains_every_row() {
        let s = RunSelection {
            tags: vec!["finance".into()],
            skip: vec!["payroll".into()],
            select: vec!["people".into()],
            ..sel()
        };
        let r = resolve(&nodes(HIBOB), &s, true);
        assert!(r.error.is_none());
        assert_eq!(r.run_set, vec!["people"]);
        assert_eq!(
            r.decisions["people"],
            RowDecision::Selected { by: "select" }
        );
        assert!(
            matches!(&r.decisions["payroll"], RowDecision::Excluded { reason } if reason.contains("skip"))
        );
        assert!(
            matches!(&r.decisions["audit"], RowDecision::Excluded { reason } if reason.contains("tagged"))
        );
        assert!(
            matches!(&r.decisions["beta"], RowDecision::Excluded { reason } if reason == "not selected")
        );
        let bare = resolve(&nodes(HIBOB), &sel(), true);
        assert!(
            matches!(&bare.decisions["beta"], RowDecision::Excluded { reason } if reason.contains("draft"))
        );
        assert_eq!(
            bare.decisions["people"],
            RowDecision::Selected { by: "default" }
        );
        let by_only = resolve(
            &nodes(HIBOB),
            &RunSelection {
                only: vec!["pay*".into()],
                ..sel()
            },
            true,
        );
        assert_eq!(
            by_only.decisions["payroll"],
            RowDecision::Selected { by: "only" }
        );
        assert!(
            RowDecision::PulledIn {
                because: "x".into()
            }
            .runs()
        );
        assert!(!RowDecision::Blocked { reason: "x".into() }.runs());
    }

    #[test]
    fn resolve_orders_the_run_set_by_dependency_level() {
        let s = RunSelection {
            select: vec!["facts".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        let n = nodes(DEPS);
        let r = resolve(&n, &s, true);
        assert_eq!(r.run_set, vec!["dims", "facts"]);
        assert_eq!(
            r.decisions["dims"],
            RowDecision::PulledIn {
                because: "facts".into()
            }
        );
        let depths = execution_depths(&n);
        assert_eq!((depths["dims"], depths["facts"]), (0, 1));
        let orphan = resolve(
            &n,
            &RunSelection {
                select: vec!["facts".into(), "dims".into()],
                skip: vec!["dims".into()],
                ..sel()
            },
            true,
        );
        assert!(orphan.error.is_some());
        assert!(matches!(
            &orphan.decisions["facts"],
            RowDecision::Blocked { .. }
        ));
    }
}
