# RFC 0011 — Change approvals, run budgets, and cost & usage accounting

*Let humans and agents propose changes that an owner approves before anything moves (#703), bound every approved run by what was agreed, and report what each run moved and roughly what it cost (#704).*

| | |
|---|---|
| **RFC** | 0011 |
| **Title** | Change approvals, run budgets, and cost & usage accounting |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #703 (plan → approve → run) · #704 (cost & usage) · #698 (roles) · #283 / #374 (plan) · #702 (policies) · #707 (impact) · #420 (MCP) · #205 (audit) · #280 (notifications) · epic #38 |
| **Related ADRs** | — |

## Summary

Three pieces that compose:

1. **Usage accounting** — every invocation is metered (records, estimated
   bytes, backend round trips, connector-reported cost signals) and priced
   against a rate table the operator controls. Always on; stored in the
   catalog / serve history; reported per run, per group, and over HTTP.
2. **Run budgets** — hard ceilings on one invocation (`max_records`,
   `max_bytes`, `max_duration_secs`, `allowed_sinks`), enforced by the engine.
3. **Change requests** — a proposed run / template registration / template
   launch, stored with its plan and executed only after the approvers the
   server's policy names approve it. The approved budget travels with it.

## Motivation

Agents now write and trigger pipelines over MCP, and faucet already splits
operators from admins (#698). What is missing is the review step teams use for
code: a proposal with readable review material, an approver, and limits on
what an approved change may do. Without it the choice is "let the agent run
anything" or "let it run nothing". The review material exists already —
`faucet plan` (#283), the policy verdict (#702), the impact report (#707) —
the workflow around it does not. And "limits on what it may do" needs a
measure of what a run does, which is the usage meter.

## Design

### Usage (core: `faucet_core::usage`; CLI: `cli/src/usage/`)

- `UsageMeter` — atomic counters attached with `Pipeline::with_usage_meter`
  and fed by the decorators every source and sink is already wrapped in, plus
  the `RoundtripRecorder` connectors already use for
  `faucet_*_roundtrips_total`. A connector that knows its backend's own figure
  (BigQuery `totalBytesBilled`) reports a `CostSignal` through the same
  recorder. No connector code is needed to be metered.
- Bytes are a JSON-shaped estimate of each page — identical across
  connectors, so pipelines compare; explicitly not wire bytes.
- `estimate()` prices a snapshot against `PricingSpec` (shipped defaults are
  public list prices, every rate overridable inline or by file). A connector
  kind with no signal is listed as *compute not reported*, never priced at
  zero. `hosted_equivalent` prices the rows written at a per-row hosted-ELT
  rate for comparison.
- The executor creates the meter per invocation *outside* the pipeline call,
  so a failed invocation still gets a record.
- Storage rides the run-history backends (`faucet_usage`, idempotent per
  `(run_id, row_id)`, never purged by run retention).

### Budgets (core: `faucet_core::budget`)

`BudgetSink` wraps the destination innermost. Records and bytes are checked
**before** a page is written; a crossing page is refused whole, the
cooperative token is cancelled, and the bookmark never advances past the
refused page. Refusing rather than truncating is what keeps the bookmark
honest. Duration is a timer that cancels the token; the verdict turns the
partial `Ok` into `FaucetError::BudgetExceeded`. `allowed_sinks` is a
plan-time check in `run_expanded`. The config's `budget:`, the run flags, and
a change request's budget merge by taking the stricter ceiling.

A decorator rather than a `RunStreamOptions` field because that struct is
externally constructible and therefore frozen under the semver contract.

### Change requests (`cli/src/serve/changes/`)

- `ChangeRequest { kind, status, requester, reason, payload, plan, budget,
  required_approvals, approvals, rejection, expires_at, run_id | template,
  error }`, stored in `faucet_serve_changes` (shared, so any cluster instance
  may approve or execute).
- **Plan at proposal, re-plan at execution.** The plan stores a *material*
  fingerprint — per row: source, sink, write mode, delivery guarantee,
  transform chain, quality/contract/masking/drift settings; for a launch, the
  target and the version it replaces. Execution recomputes it; a difference
  marks the request `invalidated` and nothing runs. Probes, impact and policy
  output are derived from the world and are not material.
- **Approval policy** lives in `--auth-config` (`approvals:`): ordered rules
  by kind with roles, named principals, a quorum and a self-approval switch.
  Default for an unmatched kind: admins, one approval, no self-approval.
  Single-principal servers use the same rule with self-approval allowed.
- **Gate.** `--require-approval run,template_register,template_launch`
  makes the gate mandatory; `require_approval: true` on a run makes it
  opt-in per request. An executing request marks its submission internally
  (`#[serde(skip)]`), so the gate cannot be bypassed from the wire.
- **Surfaces.** `/v1/changes*` (with new permissions `ChangeRead` viewer+,
  `ChangeRequest` / `ChangeApprove` operator+), MCP `propose_run` /
  `propose_template`, the console Changes page, `change_requested` /
  `budget_exceeded` notifications, and the `change.*` audit actions.

## Alternatives considered

- **Git-hosted review** (open a PR instead): out of scope; a later
  integration can create a change request from a merged PR.
- **Truncating at a budget** instead of refusing the page: leaves the
  bookmark ahead of what was written. Rejected.
- **A wire-level flag to skip the gate for executing requests**: exploitable.
  Rejected in favour of a non-serialized field set in-process.

## Out of scope

- Real billing: faucet reports estimates with their inputs, never invoices.
- Per-tenant budgets and quotas — #709.
