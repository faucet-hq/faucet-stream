---
title: "Change approvals (plan → approve → run)"
description: "A change request is a proposed action — run a pipeline, register a template version, launch a template version — that faucet serve stores with its plan and executes only after…"
---

A **change request** is a proposed action — run a pipeline, register a
template version, launch a template version — that `faucet serve` stores with
its plan and executes only after an approver approves it. It is the review
step teams use for code, applied to data movement, and it is what makes the
control plane safe to hand to automation: an agent proposes over MCP, a human
approves in the console.

<pre class="mermaid">sequenceDiagram
    participant R as Requester (human or agent)
    participant S as faucet serve
    participant A as Approver
    R-&gt;&gt;S: POST /v1/changes {kind: run, payload, reason, budget}
    S-&gt;&gt;S: plan (rows, delivery, policy, impact) + material fingerprint
    S--&gt;&gt;R: 201 pending
    A-&gt;&gt;S: POST /v1/changes/{id}/approve
    S-&gt;&gt;S: re-plan, compare fingerprint
    alt plan unchanged
        S-&gt;&gt;S: submit run with the approved budget
        S--&gt;&gt;A: executed (run_id)
    else plan changed
        S--&gt;&gt;A: invalidated (nothing ran)
    end</pre>

## Proposing

```bash
curl -X POST http://127.0.0.1:8080/v1/changes \
  -H "Authorization: Bearer $OPERATOR_TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "kind": "run",
    "payload": { "config": "version: 1\nname: orders\n...", "name": "orders-backfix" },
    "reason": "re-sync orders after the upstream fix",
    "budget": { "max_records": 5000000, "max_duration_secs": 1800 }
  }'
```

| `kind` | `payload` | What executes |
|---|---|---|
| `run` | a `POST /v1/runs` body | the run, as the requester, with the budget merged in |
| `template_register` | a `POST /v1/templates` body | the registration (and launch, when the body asks) |
| `template_launch` | `{ "id": "...", "version": 3 }` (version optional, default `newest`) | the launch |
| `template_delete` | `{ "id": "...", "version": 3 }` (no `version` = every version) | the delete |
| `template_deprecate` | `{ "id": "...", "version": 3, "reason": "...", "undo": false }` | retiring (or with `undo`, reviving) the version |
| `policy_update` | made by `POST /v1/policy` / `/v1/policy/rollback`, never by `POST /v1/changes` | storing the next version of the server-managed [data-flow policy](/faucet-stream/cookbook/policies/#under-faucet-serve) |
| `tenant_policy_update` | made by `POST /v1/tenants/{tenant}/policy` (and `/rollback`) | storing the next version of that tenant's add-only policy |

**Policy changes always need approval**, whatever `--require-approval` says:
by default an admin other than the requester approves a `policy_update`, and
another admin of the tenant — or a server admin — a `tenant_policy_update`. A
tenant-scoped principal never approves the server's policy, nor another
tenant's. The plan holds the version the draft was based on, so a request
approved after another policy change landed is `invalidated` rather than
overwriting it. (On a server with a single principal — `--auth-token`,
`--no-auth` — that principal may approve its own request, as for every kind.)

The response is the stored request: `status: pending`, the plan (one
[`faucet plan`](/faucet-stream/reference/cli/#plan) report per root row — source, sink,
write mode, delivery guarantee, transforms, the [policy](/faucet-stream/cookbook/policies/)
verdict and the [impact](/faucet-stream/cookbook/impact/) severity — plus a summary), the budget,
how many approvals it needs and when it expires. A config that violates a
data-flow policy is refused here, before anyone is asked to approve it.

Two other ways in:

- **`POST /v1/runs` with `"require_approval": true`** (and an optional
  `reason` / `budget`) — the 202 body is `{ status: "pending_approval",
  change_id, change }` instead of a run. The console's Submit page has a
  *request approval* checkbox for this.
- **The MCP `propose_run` / `propose_template` tools** (on `faucet serve
  --mcp`) — an agent that holds `ChangeRequest` files a request as its
  principal and gets back the id and the plan summary to tell the human.

## Approving

```bash
curl -X POST http://127.0.0.1:8080/v1/changes/$ID/approve \
  -H "Authorization: Bearer $APPROVER_TOKEN" \
  -d '{ "comment": "matches the ticket" }'
```

Below the quorum the request stays `pending` with the approval recorded. At
the quorum faucet **re-plans** the payload against the world as it is now and
compares the material fingerprint — per row: source, sink, write mode,
delivery guarantee, transform chain, quality / contract / masking / drift
settings, plus the run's row `selection` (a different subset is a different
change); for a launch, the target version and the version it replaces. If
anything material moved (a template version was launched in the meantime, the
default config now points somewhere else), the request becomes `invalidated`
with the difference spelled out and **nothing runs**: the approver reviewed
something else. A rotated secret or a live probe result is not material.

Under `--auth-config` the requester is also checked again: if it is no longer
a principal in the file, now holds a lower role than when it asked, or is
scoped to a different tenant, the request becomes `failed` and nothing runs.

Otherwise the request executes and becomes `executed` (with `run_id` or the
template id and version) or, if execution itself errors, `failed` with the
reason. A refusal that passes on its own — the run history is unreachable,
the run queue is full, a tenant is at its limit, the server is draining —
leaves the request `approved` with the reason in `error`; the sweep below
runs it again later. A policy change that another landed change has made
stale is `invalidated`. A run carries the label `change: <id>` and, unless the requester set
one, the idempotency key `change:<id>`.

`POST /v1/changes/{id}/reject` with `{ "reason": "..." }` rejects; the
requester may reject (withdraw) their own. A pending request lapses to
`expired` after its window (a sweep runs every minute, and reads apply it too).

Every transition out of `pending` is a compare-and-set on the stored status,
so two approvals that reach the quorum together, or an approval racing a
rejection or the expiry sweep, cannot both win: the loser gets `409` and
nothing runs twice. If an execution is interrupted after approval (a crash, a
store error before `executed` was written), the same sweep finds the request
still `approved` five minutes later and finishes it: a run is submitted again
under its `change:<id>` key (so the original run is replayed, never doubled),
a launch is re-applied (re-launching the live version is a no-op), and a
registration is marked `failed` — its outcome is unknown, so check the
template's versions before proposing it again.

A `change_requested` notification is built from the proposed config as it
loads — `${env:…}` webhook URLs resolve and the `--default-config` base
applies — exactly as the run would see it.

## Who may approve

Reaching the approve / reject routes needs `ChangeApprove` (operator and up).
Which of those principals **count** is the `approvals:` block of
`--auth-config`:

```yaml
principals:
  - { name: alice, token: "${env:ALICE_TOKEN}", role: admin }
  - { name: bob,   token: "${env:BOB_TOKEN}",   role: operator }
  - { name: dave,  token: "${env:DAVE_TOKEN}",  role: operator }
approvals:
  expire_secs: 86400
  rules:
    - kinds: [run]
      roles: [operator, admin]
      min_approvers: 1
      self_approve: false
    - kinds: [template_register, template_launch]
      principals: [alice]
      roles: [admin]
      min_approvers: 2
```

The first rule whose `kinds` covers the request applies (an empty `kinds`
covers everything). `roles` and `principals` together say who counts; a named
principal counts whatever their role. `min_approvers` distinct principals must
approve. `self_approve: false` (the default) means the requester's own
approval is refused. With no rule for a kind, the default is **admins only,
one approval, no self-approval**. A refusal is a `403` that names the rule.

A server with a single principal (`--auth-token`, `--no-auth`) uses admin-only
with self-approval allowed — otherwise nothing could ever be approved there.

A [schedule](/faucet-stream/cookbook/schedules/) change under `--require-approval run` is governed
by two rules, `schedule` and `run`: an approver counts when one of them allows
them, and the change executes once each rule has its own `min_approvers`.
Whenever schedule changes are gated (`run`, `schedule` or both), the server
refuses to start when the principals in `--auth-config` could not meet one of
the rules they need.

## Making it mandatory

```bash
faucet serve --auth-config auth.yaml --require-approval run,template_launch
```

- `run`: `POST /v1/runs` and template triggers answer with a pending change
  request instead of starting a run; `POST /v1/backfill`, a `POST /v1/verify`
  with `repair`, a `POST /v1/dlq/replay` and a `POST /v1/runs/{id}/rollback`
  (each writes to the destination outside the run queue; their `dry_run` forms
  still answer) are refused with `403` — no change-request kind expresses them,
  so they are disabled on such a server (run them from the CLI or a server
  without `--require-approval run`); the MCP `run_pipeline` tool points the agent at
  `propose_run`, and the MCP `run_template` tool answers with the pending
  change request, as `POST /v1/templates/{id}/runs` does.
- A trigger declared in the server's `--triggers` file is a **standing
  approval**: whoever controls that file (the server operator) approved the
  pipeline it names, so its fires — object arrivals, queue edges, schedule
  ticks and `POST /v1/triggers/{name}` — run without a change request. A
  fire per event would otherwise file one request per file or tick, and a
  rejected one would lose the event. Leave a pipeline out of `--triggers` if
  every run of it needs review.
- `template_register` / `template_launch`: those kinds must come through
  `POST /v1/changes`. `POST /v1/templates` (both kinds when it also launches or
  assigns channels), `/launch`, `/rollback`, `/tags` and a non-dry-run
  `/v1/templates/sync` answer `409`. Under `template_launch` so do deleting a
  version that was ever launched, the `newest` version or a version a channel
  points at (or the whole template), and retiring or reviving a version,
  since each moves what a request resolves to: propose those as
  `template_delete` / `template_deprecate` changes instead. They take the
  `template_launch` approval rule unless a rule names them, and can also be
  gated on their own (`--require-approval template_delete`). So do the MCP `register_template` /
  `launch_template` / `rollback_template` tools. `--templates-sync` registers,
  launches and retires versions on its own schedule, so it cannot be combined
  with `template_register`, `template_launch` or `template_deprecate`.

A template trigger that waits for approval stores the template body with its
`${env:}` / `${file:}` / secret references still unresolved — they resolve
when the approved run executes — so a trigger carrying a `secret: true` param
or `env` overrides is refused with `422`. Change-request responses hide
callback `headers` from everyone but admins and the proposed `config` from
principals that cannot approve. The body a `template_register` change proposes
is shown only to unscoped admins, the same rule `GET /v1/templates/{id}`
applies to a registered body (it can hold inline credentials); its plan stays
visible to every reviewer.

`--approval-expiry-secs` sets the default window when the auth config does not.

## Budgets

The `budget` on a request (`max_records`, `max_bytes`, `max_duration_secs`,
`allowed_sinks`) is merged into the run with the config's own
[`budget:`](/faucet-stream/cookbook/usage/#run-budgets) block — the stricter of each ceiling wins —
so an approved change can never move more than was agreed. The page that
would cross a record or byte ceiling is refused whole (the bookmark stays
put) and the run fails with `budget_exceeded`; an overwrite aborts cleanly
rather than leaving a half-written table.

## Notifications and audit

A run request whose config declares [`notifications:`](/faucet-stream/cookbook/notifications/)
emits a `change_requested` event through those channels when it is proposed,
so the approvers hear about it where they already listen. A run stopped by a
budget emits `budget_exceeded`.

Every step is in the audit log: `change.requested`, `change.approved`,
`change.rejected`, `change.executed` (linked to the run), `change.invalidated`,
`change.expired`, `change.failed`.

## Console

The **Changes** page lists requests by status. Selecting one shows the plan
row by row, the budget, the approvals so far and — for principals holding
`change_approve` — the Approve and Reject controls. The server's policy still
decides; a refused approval shows its reason.

## Metrics

| Metric | Meaning |
|---|---|
| `faucet_serve_changes_total{kind,outcome}` | `requested`, `approved`, `denied`, `rejected`, `executed`, `invalidated`, `expired`, `failed`. |
| `faucet_serve_changes_pending` | Requests awaiting approval. |
| `faucet_serve_policy_versions_total{layer}` | Policy versions stored by approved changes (`layer` = `server` / `tenant`). |
| `faucet_budget_exceeded_total{pipeline,row,budget}` | Runs stopped by a budget ceiling. |
