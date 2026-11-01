# Web console: functionality inventory walk (#998)

Every capability of the console before the redesign, and the new ones, checked against the rebuilt pages. Legend: ✅ kept (where) · ⚠️ changed (how) · ❌ dropped (why) · 🆕 new. The design rules are in [console-design.md](console-design.md).

## Inventory walk: Shell, Submit, schema form, Triggers, Settings (#998 build)

### Shell (shell.js, app.js, access.js, tenant.js, tz.js, page.js, palette.js)
- ✅ Grouped sidebar Operate / Data / Build / Govern / Admin, groups collapsible (`faucet.nav.collapsed`), shell.js `renderSide`
- ⚠️ Nav badges: active runs (running + sharded, from `counts` on `GET /v1/runs`, falls back to the page length on an older server), pending changes (warn, `GET /v1/changes/counts`), 🆕 datasets and templates counts, tenants
- ⚠️ "soon" entries (Schedules, Policy) now link to designed placeholder pages instead of being dead
- ✅ Breadcrumb group / page / item; `setCrumbs` + `setCrumbTitle`
- ✅ Context chip slot (`setContext`); filled by pages (Runs: run id; Submit: "from run …")
- ⚠️ Tenant switcher: a select in the top bar; a tenant-scoped principal sees a fixed chip instead of a disabled select
- ✅ Display timezone select (labels shortened to "Local", "UTC", zone names); also in Settings
- ✅ Theme toggle (auto → light/dark); 🆕 Light / Dark / Auto segmented control in Settings
- ⚠️ Access token: the modal is now a design-system dialog (app.js `askToken`, opens on 401); the top-bar key button moved to Settings → Access token and the ⌘K "Access token" action
- ✅ Copy link to this page (top bar)
- ✅ Expand (hide sidebar) `faucet.shell.expanded`; also a toggle in Settings
- ✅ Status line: history healthy/degraded, run queue full, draining, cluster N live, version (`/readyz`, `/metrics`)
- ⚠️ Who-am-I: moved to the sidebar foot (avatar initials, principal, role tag; viewer shown "read-only", tenant shown "tenant X")
- ✅ Permission gating `data-perm` + 403 re-read (access.js)
- ✅ Narrow ≤1100px: icon sidebar; split views show detail with a back link (verified on Triggers and Submit at 1000px)
- ✅ Toasts ok / bad / info / warn with an optional link (page.js `toast`)
- 🆕 Command palette ⌘K / "Go to…" (palette.js)
- 🆕 Keyboard j/k, Enter, Esc, `/` (page.js `installGlobalBehaviours`)
- 🆕 Copy buttons (`data-copy`), relative time with absolute on hover (`ago`)
- 🆕 Triggers and Settings real pages; Schedules and Policy designed "soon" pages

### Submit (views/submit.js)
- ✅ Catalogue load `GET /v1/schemas`; empty catalogue has its own empty state
- ⚠️ Guided / Editor as a segmented control; Editor is seeded from the form whenever the editor still holds the last seed (not only when empty). Editor → Guided does not parse YAML back (no YAML parser in the console): the form keeps its own values and the lede says so
- ✅ Read-only notice without `config_submit` (alert, mock "viewer" state)
- ✅ Source and sink pickers with schema forms (`GET /v1/schemas/{source|sink}/{name}`), each with a "schema" link to the Schemas page
- ⚠️ Transforms: "Add transform" menu listing every compiled transform with a short description; 🆕 remove and reorder (move up) per transform
- ✅ Optional blocks: Reliability (state, dlq, delivery, resilience, sla) as toggles with hints; Data governance (quality, contract, masking, schema) as adders; only blocks the server lists; placement honoured
- ✅ DLQ block: sink picker (defaults to `file`) + that sink's form + the DLQ's other fields
- ✅ Config assembly + dependency-free YAML emitter; empty values dropped; defaults left out of the config (fields show the default but `read()` omits it)
- ⚠️ Run options: name, timeout (as a duration "2h"), doctor first, idempotency key, request approval + reason (`change_request`); format select lives in Editor (YAML/JSON)
- 🆕 Run options: pool (from `/readyz` pools), labels (key/value rows), budget max records / max duration
- ✅ Validate (`doctor`) → Review step "Validated" / "Validation failed" probe table (per-probe detail on failure)
- 🆕 Field-level validation before any request: required, whole numbers, min/max, JSON fields, durations; an error summary alert whose links jump to the step and focus the field
- 🆕 Plan (`plan`, `POST /v1/plan`) → Plan card: row, write mode, delivery guarantee, policy verdict, governance passes
- ✅ Submit run → toast with "open run" and navigate to the run; pending approval → toast "open request" and navigate to the change (verified end to end on :8899: change request created and shown)
- ✅ Submit errors shown as an alert with the server message and Retry-After
- 🆕 Live YAML preview with copy, line count, error count, "approval required" note
- 🆕 Stepper (Source · Sink · Transforms · Options · Review), clickable, `?step=` in the URL (back button works)
- 🆕 Prefill `?source=` / `?sink=` (Schemas "Use in Submit")
- 🆕 Prefill from a run `?run=<id>` (or `?from=`): `GET /v1/runs/{id}/config` opens in Editor with an info alert, a "from run" chip and a line diff against the original; a run without a stored config shows a warning alert (verified both: 404 on :8899, stored body via a stubbed response)
- ❌ Unused run options `clock`, `concurrency`, `callback`, `selection`, `depends_on`: not in the mocks
- ⚠️ Editor errors: the server's message with its line number in the editor footnote (no wavy underline on the line: a textarea cannot style one line)

### Schema form (now inside views/submit.js; schema-form.js left untouched for the templates page)
- ✅ Object properties with required dots; `$ref` resolution
- ✅ `Option<T>`: nested object behind a toggle sub-panel ("optional · …"); scalar renders as T
- ⚠️ Unit enums (consts, and const + enum mixes) → select; per-option tooltips dropped (the field's help line carries the description)
- ✅ Discriminated unions → segmented picker of variants + that variant's fields; a lone `config` object is flattened
- ✅ enum → select; boolean → checkbox (short label + help); integer/number → input with validation; string → input (SQL fields and `query` → mono textarea)
- ⚠️ Secret-looking fields: password input with a secret-reference placeholder and help line (`secret-field.js` export kept); autocomplete is `off` rather than `new-password`
- ✅ Defaults shown; description first sentence as help (markdown rendered)
- ⚠️ Arrays of scalars: add/remove rows; arrays of objects fall back to a JSON textarea (as before for anything unknown)
- ✅ `read()` drops empty values

### Triggers (views/triggers.js)
- ✅ List from `GET /v1/triggers` (name, type, enabled, health, last fire, fired/errors, methods, debounce, cron, timezone, poll interval, threshold, template, config, tenants); falls back to `/readyz` triggers (name, type, health, last fire) on an older server, with "not reported by this server" facts
- ✅ Fire a webhook (`trigger_fire`): dialog with JSON body (validated) and, when the trigger lists one, a dedupe key sent as that header; 202 → toast "open run", 200 coalesced → info toast (verified end to end on a throwaway `--triggers` server)
- 🆕 Sections by type, tabs All / Unhealthy, stat strip, unhealthy alert + last error, per-type facts card, curl snippet with copy, runs started by the trigger (`label=faucet.trigger.name:<name>`), Runs tab, empty state with a sample triggers file
- ⚠️ Dedupe header is not in `GET /v1/triggers` (the API omits it); shown only when present

### Settings (views/settings.js)
- ✅ Identity: principal, role, permission chips, "Audit log for you" link (`audit_read`)
- ✅ Server: history, run queue, draining, cluster replicas + pools, replica table when clustered, version
- ✅ Reload default config (`reload`): result shown in the card + toast; a server without `--default-config` says so
- ⚠️ Default config File / Loaded: the server reports the path only in the reload response and no load time, so File reads "—, shown after a reload" and Loaded "at startup" until a reload in this session (drop of the always-visible path/time from the mock)
- ✅ Preferences: theme, timezone, tenant scope, sidebar
- ✅ Access token: replace (dialog) / clear (confirm); `?token=1` opens the dialog (⌘K action)
- 🆕 Keyboard shortcuts card

## Inventory walk — Runs + Overview (#998 build, commits 8ba52db5, 3aa7259e)

### Runs — list (views/runs.js)
- ✅ Load runs page `GET /v1/runs?limit=100[&status][&name][&since][&until][&label…][&tenant]` (+ `mine=true`, `submitted_by=` when used)
- ✅ Tabs All runs / Active / Failed (+ "Mine" only when the principal is not anonymous, as before)
- ⚠️ Filter panel: name, status multi-select (menu of checkboxes), pool, since/until (native date-time picker in the display timezone, shown as "Since · 28 Oct, 00:00"), label key:value (Enter adds), Clear / Apply. Pool is filtered client-side over loaded rows (GET /v1/runs has no pool param).
- ⚠️ Label filters: max 10, leading ":" refused, removable chips — now every filter (status, q, since, until, pool, by, mine, label, panel open, detail tab) is in the URL; `update(params)` re-reads them; Back works
- ✅ Tenant scope via `withTenant`
- ✅ Refresh button re-reads every shown row (`limit=min(500,max(100,shown))`)
- ✅ Load more (`cursor=next_cursor`)
- ✅ Polling 3 s while any shown run is live, else 15 s, 3 s after an error; scroll + selection kept (patch)
- ✅ Stat strip Running / Succeeded / Failed / Records (exact `counts` when the server sends them, else loaded rows with "+")
- 🆕 Stat cells are filters (click Failed → status=failed; active cell selected)
- ✅ Sections Running / Queued / Failed / Succeeded / Cancelled, collapsible, remembered (`faucet.runs.sections`)
- ⚠️ Row: ring by status, title name ‖ pipeline ‖ id, age with absolute on hover, meta first fact bold: failed → error code/first line; queued → "queued · waiting for a slot in pool X"; template@vN / backfill unit / DLQ replay / kinds; rows, invocations, duration, pool, tenant, attempt
- ✅ Empty states: filtered (+ Clear filters button) vs none (+ Submit a run (config_submit) / Browse templates)
- ✅ Submit shortcut `+` (config_submit)
- ⚠️ Detail placeholder → 🆕 summary page when nothing is selected: last-24h card (success rate, median/p95 duration, queued now, estimated cost, runs-per-hour sparkline; from GET /v1/overview, falls back to the run list), Needs attention table (failed + queued runs with Open / Inspect DLQ), keyboard hints; filtered → "Pick a run · N runs match"; Failed-only → "N failed runs · most common: …"
- ✅ Exports `runTitle`, `runKinds` (+ `errorCode`, `money`, `fmtSecs` for Overview)
- 🆕 Error state: quiet alert with Retry above the last list loaded
- 🆕 Top-bar chips: "N filters", cluster · N live, "run 01a1…c64f" with copy
- 🆕 j/k/Enter/Esc and `/` (global, page.js)

### Runs — detail
- ✅ `GET /v1/runs/{id}`, polls 3 s while live; 404 → "Run not found" (+ All runs), other errors → alert + Retry; keeps polling through 5xx
- ✅ Crumb shows the run name; context chip short id (+ copy)
- ⚠️ Tabs Overview / Invocations (n) / Logs / Config 🆕 / Dead-letter queue / Compare 🆕 — tab in the URL (`?tab=`), kept when stepping through runs. DLQ tab shows when the run has DLQ/quarantined rows, a `dlq_location` or `replay_of`; otherwise reachable from the More menu ("Inspect a dead-letter queue"). Compare shows for finished runs.
- ✅ Header sub-line: status pill, kinds, started/failed/finished age (abs on hover) 🆕 "by <submitted_by>" (trigger:… links to the trigger), attempt
- ✅ Error alert: failing invocation error (copy), run error when not the generic summary, failed invocations list; 🆕 "Open logs at the error", "Compare with last good run"
- ⚠️ Output card: "N rows moved [so far / before the failure]"; live → Throughput, Lag behind source, Running for, Estimated cost; finished → Submitted / Started (queued Xs) / Finished / Duration
- ✅ Row accounting stacked bar + legend + balanced / N unaccounted (footnote of the output card)
- ✅ Invocations summary: 6 slowest, link to the tab
- 🆕 Governance card (from row accounting: quarantined, dead-lettered, filtered, unaccounted). ⚠️ The mock's masking/contract/drift/budget figures are not in the run record; shown what is stored.
- 🆕 What this run touches: datasets written (invocation usage) + read (catalog `last_run_id`), rows, schema version, links to dataset + lineage
- ✅ Provenance: template + version link (sink template), tenant link + pool, submitted by, change label, idempotency key (copy), timeout, replayed from, clock, depends_on, 🆕 run id (copy)
- ✅ Labels as chips → click adds a label filter (caller labels first, provenance muted)
- ✅ Log export card (status, pending, dropped, last error, log service link)
- ✅ Live log tail (last 6 lines, level colouring, connecting/streaming/ended/expired/in the log service/stream error)
- ✅ Invocations tab: slowest first, row + invocation id (copy), records, row accounting bar per invocation (tooltip offered/filtered/non-finite), batches (committed/partial/whole to DLQ/failed), source lag, duration + bar; 🆕 status filter, matrix fan-out card for parent-keyed rows, Errors card
- ✅ Logs tab: full SSE stream, truncated/end/expired/link events, auto-scroll; 🆕 search, level filter All/Info/Warn/Error, Follow toggle, Download, Log service button
- 🆕 Config tab: `GET /v1/runs/{id}/config` (falls back to `config_body`; empty state when neither), redacted tag, fingerprint, copy, Re-run (POST /v1/runs, config_submit), Open in Submit (`#/submit?from=<run id>` — Submit page must read `from`)
- ✅ Cancel (run_write) — ⚠️ now a confirm dialog (rows moved, invocations finished)
- ✅ Delete (run_write, terminal) — confirm dialog; list refreshes; navigates back
- ✅ Roll back… (rollback perm, terminal + invocation ids): invocation select, dry run (default), force, config textarea (opens on 422) → report card (mode, deleted / restored / conflicts / bookmark, verdict) 🆕 "Roll back for real" after a clean dry run (confirm)
- ⚠️ DLQ: location prefilled from `RunRecord.dlq_location` when present (else typed), reason select, Inspect (auto on open) → totals by reason / error kind / files + 5 samples (payload copy); Discard… (dlq_manage) with "delete instead of archiving"; Replay card (dlq_manage) through this run's config (or a pasted one), rows reason, dry run
- 🆕 Compare: previous finished runs of the same pipeline (select), records / duration / rows offered / DLQ+quarantined / cost / failed invocations with deltas, config line diff when both configs are stored
- 🆕 More menu: copy run id, copy link, copy config, filter by pipeline (or name), open written dataset, open in lineage, inspect a DLQ, roll back, delete
- ✅ Narrow (≤1100px): detail replaces the list, back link "Runs" keeps the filters
- ✅ onChange: list + badges refresh when the run's status changes
- ⚠️ Viewer: actions hidden by data-perm; a "read-only" chip replaces them when the caller has none of run_write / rollback / config_submit

### Overview (views/overview.js)
- ✅ `GET /v1/overview` (range 24h / 7d, bucket hour / day, tenant)
- ⚠️ Range tabs Today / This week (`?range=7d`) — the old 24h/7d/30d picker dropped 30d (mock has two ranges)
- ✅ Runs total + per-bucket sparkline, success rate, records written + sparkline, estimated cost (+ the other range's cost)
- ⚠️ Failure rate by pipeline / slowest pipelines / governance series charts → ❌ not on the home page (the mock trades them for the attention queue; the figures stay in the API and Usage)
- 🆕 Needs attention: failed runs in range, failing template tests, stale datasets, pending changes, unhealthy triggers, connections needing re-auth — each with its page link and Open / Review
- 🆕 Running now (running + queued, link to Runs filtered to active)
- ✅ Data freshness (fresh / stale / schema changed with links; schema-change footnote)
- 🆕 Change requests (3 latest with status, approvals, requester)
- 🆕 Server health (history, run queue, cluster + pools, triggers) from /readyz
- 🆕 Templates (launched / draft / failing tests, ready to launch)
- 🆕 First-run checklist when the server has no runs, templates or datasets
- ✅ Loading skeleton; error alert + Retry
- ❌ Grafana links (`grafana` in the response) — not in the mock; left out

## Inventory walk — Changes, Tenants, Usage, Audit (#998 build)

### Changes (views/changes.js)
- ✅ List up to 500, tenant-scoped (GET /v1/changes?limit=500&tenant=)
- ✅ Auto-refresh 15 s (poll, list + open pending detail, patch keeps scroll/selection)
- ✅ Refresh button (list + detail)
- ✅ Needs me tab (change_approve; not mine, not already approved by me) / ✅ All tab — now URL state (?view=)
- ✅ Stat strip from GET /v1/changes/counts, "+" fallback when truncated
- ✅ Sections Pending / Approved · executing / Recently decided, collapsible + remembered
- ✅ Row: ring, summary, age (absolute on hover), meta (kind, write_mode, n of m, requester / reason / error / approvers)
- ✅ Empty states (nothing needs you → Show all changes; no change requests yet)
- ✅ Deep link #/changes/:id; 404 vs error (Retry)
- ✅ Detail head: summary title + crumb, status pill, requester+role, age, expires-in / decided-ago, tenant link
- ✅ Outcome alert (run link by pipeline name + copy id; template link), error / invalidated alert, rejection alert
- ✅ Requester reason quote
- ✅ Plan diff (launch/delete/deprecate/register/rows/budget), fingerprint + 🆕 copy, re-planned tag, verdict footnote with 🆕 dataset links + "See impact"
- ✅ Approvals card (avatars, comment, waiting slots) · ✅ Budget card
- ⚠️ Decision card replaced by Approve / Reject dialogs (mock): comment optional, reason required (validated in place)
- ✅ Approve label "Approve and run/apply" at quorum; toast (🆕 with "open run" link); 403 text surfaces in a toast
- ✅ After decision: reload detail + list, nav badges, switch to All when no longer needs me
- ✅ Impact tab (severity, planned-from, column delta, affected datasets 🆕 linked, owners, notes); policy violations; empty explanation
- ✅ History tab timeline (requested → approvals → rejection → final + run link → expiry), id + copy
- 🆕 Kind / requester filters (filter ibtn, removable chips, URL state)
- 🆕 Delta "awaiting N approvals" + "expires in" chip in the top bar; detail tab in URL (?tab=)
- ⚠️ "Propose again" (invalidated runs) opens Submit without a prefill
- ⚠️ Approvals card right shows "n of m needed" (no approval-rule name is in the API)

### Tenants (views/tenants.js)
- ✅ List (GET /v1/tenants); 404 → "built without tenants" canvas
- ✅ Refresh · ✅ Create form (#/tenants?new=1, tenant_admin) then opens it
- ✅ Stat strip tenants / running / need re-auth (connections count moved into rows)
- ✅ Sections Active / Suspended; ring ok / warn-half (re-auth) / hollow (suspended); meta
- ✅ Deep link, 404 vs error
- ✅ Connect providers list
- ✅ Detail head (name, status pill, id+copy, created by + when, active runs)
- ⚠️ "Runs for this tenant" is a deep link #/runs?tenant=<id> (🆕) instead of mutating the global switcher
- ✅ Suspend (🆕 confirmation dialog) / Resume
- ✅ Limits form (blank = none), read-only kv grid without tenant_admin / for scoped principals
- ✅ Labels chips · ✅ Recent runs (5, linked) · ✅ Delete with typed id (+ 🆕 final confirm dialog), toast with counts, clears switcher
- ✅ Connections table (type, via provider, status + reason, updated by/when); ✅ Reconnect; ✅ Delete connection (🆕 confirm)
- ✅ Connect-through-provider form ("no providers" hint); ✅ Store credentials (🆕 live JSON validation)
- 🆕 Usage card (last 30 days, sparkline), pending changes card, recent audit card (admin), Activity tab (audit + change requests), search filter
- 🆕 Tenant-scoped principal: canvas, own tenant only, read-only limits, alert

### Usage (views/usage.js)
- ✅ Group by pipeline/row/dataset/sink/day/(tenant) — seg control
- ✅ Filters pipeline / since / until / tenant switcher; Enter / change reloads; refresh
- ✅ 404 → "built without catalog"
- ✅ Stat strip (invocations, rows, bytes, cost, hosted eq.)
- ✅ Table + totals, failed runs, compute-not-reported tag, window label, explanatory note, empty state
- 🆕 Filters + group + sort in the URL; sortable columns; CSV download; rows link (pipeline → drill-down, row → runs, dataset → catalog search, sink → schema, day → that day by pipeline, tenant → tenant)
- 🆕 Drill-down: summary card with rows/cost sparklines (by=day&pipeline=), by-day table, all pipelines with the drilled one selected
- 🆕 Day view bar chart over every day of the window
- ⚠️ Default window = last 30 days (was all time); "Last 90 days" in the empty state; clearing since shows all time
- ⚠️ Pipeline filter is an exact name (the API matches exactly); the mock said "contains"
- ⚠️ Drill-down "cost lines" card (mock) not built: /v1/usage rows carry no per-line breakdown — shows a by-day table instead

### Audit log (views/audit.js, 🆕 page)
- 🆕 List GET /v1/audit (limit 100), newest first, sections by day (Today / Yesterday / weekday / date)
- 🆕 Tabs All / Denials (result=denied, client fallback on older servers), stat strip (Denied cell toggles it)
- 🆕 Filters principal / action / tenant / since / until in URL, removable chips; every principal/action/tenant in the detail links to a filter
- 🆕 Load older (before=<id> cursor, falls back to until=<timestamp>)
- 🆕 Detail: every AuditEntry field; target parsed into template/connection/run/change/tenant/trigger links; run + copy fingerprint; denied alert
- 🆕 "The run it started" card (GET /v1/runs/{id}); "More from <principal>"
- 🆕 Deep link to an entry not loaded → GET /v1/audit/{id}; 404 → not found
- 🆕 Viewer (no audit_read) → designed admin-only page

## Templates — inventory walk (build ebf5e8c2 + follow-up)

### List pane
- ✅ Load `GET /v1/templates` (+ sync.origins); feature-missing 404 → "Templates are not available" empty state.
- ✅ Tabs All / Launched / Draft — now URL state (`?lt=`).
- ✅ Search by id/description, ⚠️ moved into the mock's filter panel (`?f=1`), URL state `?q=` (replaceState while typing).
- ✅ Sort name / last updated (`?sort=updated`). ✅ Include deprecated (`?dep=1`).
- ✅ Stat strip Templates/Launched/Draft/Failing; 🆕 becomes "Matching" when filtered; Launched/Draft cells are filters.
- 🆕 Owner quick filter (`?owner=`), active-filter chips with remove + Clear.
- ✅ Kind sections (collapsible, remembered in `faucet.templates.sections`).
- ✅ Row ring/lead/age `vN`/channel facts/newest/test word; 🆕 stream count (source) and write modes (sink) from the matrix; ⚠️ param count shown only for deployments/pipelines; tooltip = id + description.
- ⚠️ Refresh button replaced by 30 s polling + reload after every action (mock has filter/matrix/sync/register icons).
- ✅ Matrix button → `#/templates?tab=matrix`; ✅ Register `+` (template_admin) → `?tab=register`; 🆕 Sync icon → `?tab=sync`.
- ✅ Empty states: unavailable / no match / none registered.

### Registry (nothing selected)
- 🆕 Overview tab: lifecycle funnel (draft/tested/launched/on channels), needs-attention table (failing → see failures, ready → Launch vN deep link `?launch=1`, draft → Run tests, deprecated → triggers), recent launches (from every template's detail, ≤60 templates, 6 parallel).
- ✅ Matrix (`GET /v1/templates/matrix`): rows/cols, ✓ n/m, partial warn, ✗, —; per-cell tooltip; facets source/sink owner+type (⚠️ single-select, URL state); count "x of y sources × …"; Clear filters. 🆕 selected cell (`?cell=src→sink`) with per-stream requested/resolved/reason, CLI command + copy, Run N streams.
- ✅ Register: YAML/JSON seg, id, description, document, launch toggle, error with details; toast incl. inherited tests / failing tests; navigates to new template. ⚠️ No line highlight in the editor (a textarea); the alert title names "Line N" when the server message has one. ❌ mock's "Run its tests after registering" toggle: the server always tests a version with a tests: block on register, so the toggle would do nothing — stated in the help text instead.
- ✅ Sync: origins table, origin picker, Dry run / Pull now, per-origin report (register/launch/revive/deprecate/orphaned/skipped/unchanged/warnings/failed), origin errors, reload after pull. ⚠️ "Last pull" column dropped (no API field). 🆕 empty state when no origins (`--templates-sync`).

### Template page
- ✅ `GET …?version=newest`; not found / unavailable (Retry) states.
- ✅ Crumbs, 🆕 kind as top-bar delta, 🆕 "N triggers" chip (GET /v1/triggers); status pill; kind facts; params count; registered by X (registrations → created_by fallback; links to audit for people); config name when ≠ id; description lede.
- ✅ Draft alert; ✅ deprecated alert with reason; 🆕 "Still in use" triggers card.
- ✅ New version (template_admin): editor seeded with newest body, id locked, description prefilled, body_withheld handled; ⚠️ now a page state (`?tab=versions&newver=1`) with Cancel/Register vN in the header.
- ✅ Roll back (template_admin) — 🆕 confirmation dialog; ❌ mock's Reason field (rollback API takes no body).
- ✅ Deprecate/Revive template — 🆕 dialog with reason field instead of prompt().
- ✅ Launch vN gating (newest ≠ stable, not retired, not deprecated, passing/untested) — 🆕 confirmation dialog (stable a→b, tests, triggers following, undo) and toast with Undo (→ rollback dialog).
- 🆕 More menu: new version, copy id/link, deprecate/revive, delete template (DELETE /v1/templates/{id}).
- ✅ Tabs: Overview 🆕 / Versions / Tests 🆕 / Run (run_write; source + pipeline) / Pairings (source+sink) · Use (deployment) / Activity 🆕 (health moved here, status_read).

### Overview 🆕
- Lifecycle stepper of the newest version with evidence and the next action (launch / run tests / see failures).
- Channels card (stable/assigned/newest/previous, since from launch log, unassigned list).
- Pairs-with card (source: sinks; sink: sources) from the matrix; params card for pipelines/deployments.

### Versions
- ✅ Versions table: version, channel chips (+deprecated), tests status (reason tooltip), registered (age · by), runs 7d (`/v1/templates/{id}/runs` when present, else `/v1/runs?label=template:<id>`, "+" when capped). ⚠️ "Last tested" column moved into the menu hint / Tests tab.
- ✅ Run button (run_write) → Run tab with that version (`?version=N`).
- ✅ Per-version menu: run, run tests (template_admin), compare 🆕, show config / clean config (⚠️ open to every reader — the server withholds bodies it must), assign channel dev…prod (disabled unless tests pass/untested), launch, deprecate/revive version (🆕 reason dialog), delete version (confirm).
- ✅ Launch history (age, vN, by, tests skipped / passed). ⚠️ `#seq` not shown.
- 🆕 Compare two versions (`?cmp=a..b`, clean toggle, +/− counts, copy, "what changed" streams added/removed).
- ✅ Config card under the table (`?cfg=N[&clean=1]`) with copy.
- ⚠️ Per-version "why it can't take a label" + test history moved to the Tests tab.

### Tests 🆕 tab
- ✅ Gate card for the newest version: passed/failed X of Y, sha, faucet, tested age, cases by origin, shared suites, companions; verdict (stable / gate open / untested on this faucet / closed). ⚠️ "Retried" count lives in the runs table.
- ✅ Every recorded test run across versions (when, version, result, cases, retried, faucet, by); failed run expands its failing cases / error (`?tr=`).
- ✅ Run tests (template_admin).

### Run
- ✅ Source: sinks / deployments (deprecated excluded); none → explain.
- ✅ Version pickers (channels then pinned, deprecated marked), sink + sink version, deployment + version, run name.
- ✅ Params = union of source/sink/overlay for picked versions, cached; computed excluded; typed inputs (bool toggle, closed values as select, secret password); default placeholder; required dot; secret / trusted tags; md description; "from sink X"; values preserved; int/float/values validation with per-field errors and "N fields need attention".
- 🆕 Live request preview (secrets masked) and Copy as API call (curl, secrets masked).
- ✅ Streams/rows picker (≥2 rows): checkbox rows with write mode/key/parent/deps/health, unsupported disabled with reason; Defaults/All/None; include parents off/eligible/all; resolved run set (selected / pulled in / blocked, errors). ⚠️ ancestor highlighting dropped (pulled-in rows show in the resolved set).
- ✅ allow_untested gate ("Run vN anyway", labelled + audited).
- ✅ Submit body (selection only when non-default); deprecated/warnings toasts; navigates to the run; 🆕 pending-approval toast with link; error alert with details.
- ✅ Sink "Pairings" → sources with Run into this sink (`?sink=` preselect); ✅ deployment "Use" → sources with how-to.

### Pairings 🆕
- Per-sink plan per stream (`append ← overwrite` aliases, cannot-write chips with reason), fit, Run into this sink / Run N streams.

### Activity 🆕
- Recent runs (status, version, sink, rows, when) → Runs filtered by label.
- ✅ Health (`/v1/status?template=`, status_read): row, health, last success, bookmark, lag, DLQ, next resume, reasons. ⚠️ moved from a tab into this card; shown only when the server can compose it.
- Datasets its runs wrote (→ dataset page), pending changes for it (→ change), triggers using it.

### Permission gates
- ✅ template_admin on every write control (data-perm + `can()` for menu items); run_write on Run tab/buttons; status_read on health. Viewer sees no actions (verified by gating; no viewer token on the demo to render).

### Not verified live
- Viewer / tenant-scoped renders (no such principal on the demo server).
- Sync with real origins (demo has none) — empty state only.
- Launch / roll back / delete / assign actually executed (dialogs rendered; avoided mutating the shared demo); run tests and a real run submit were executed successfully.

### Datasets + Lineage — inventory walk (build commits 168bdaea, f9072606)

#### Datasets list (`#/catalog`)
- ✅ Role tabs All / Destinations / Sources (`?role=`).
- ⚠️ Stat strip: Datasets (with "N of M" when filtered) / Fresh / Stale / Evolved; each cell filters the list (`?fresh=`). "Last 24h" became Fresh; "Owned" moved to the catalogue's Ownership gaps card. The mock's "Drifted" cell is "Evolved": the list API carries no profile, so drift can't be counted without fetching every dataset.
- ✅ Filter panel toggle (icon button, dot when a filter is active), now in the URL (`?filters=1`) with removable chips and "clear all".
- ✅ Search (`?q=`, now also matches pipeline and owner).
- ✅ Kind multi-select, options only for kinds the other filters reach, with counts (`?kind=a,b`).
- ⚠️ Last-success date range: two text fields (`YYYY-MM-DD[ HH:MM]`, display timezone) instead of the old picker (the mock draws plain fields); stored in the URL as UTC.
- ✅ Refresh (reloads list + open dataset), link to the lineage graph, Local outputs (server-wide, `?outputs=1`).
- ✅ Sections Destinations / Sources, collapse remembered (`faucet.datasets.sections`).
- ✅ Paging: 200 per section, "Show N more".
- ✅ Row: ring (fresh / stale half / never hollow), short name, age (absolute on hover), meta kind · rows · schema vN · consumers · owner; stale rows say "stale · last success X ago".
- ✅ Empty states: catalog missing, no match, nothing catalogued (Submit link).
- ✅ Selection keeps the list (`update(params)`), narrow back link, j/k/Enter.
- 🆕 Catalogue page with nothing selected: freshness card, needs attention, ownership gaps (readers from lineage), by kind.
- 🆕 List polls every 30 s.

#### Dataset detail (`#/catalog/:id?tab=`)
- ✅ Crumb title = short name; owner chip in the top bar.
- ✅ Header: freshness pill, kind · "written/read by <pipeline>" (links to its runs), first seen, full URI with copy.
- ✅ Open in lineage (`#/lineage/:id`).
- ⚠️ Tabs now in the URL: Overview / Schema (versions) / Profile (drift) / Lineage (edges) / Activity 🆕 / Outputs (count).
- ✅ Volume card: "N rows · M columns", schema tag, last change; rows per run with sparkline; worst null-rate column with sparkline (else total rows); consumers with kinds; footnote = latest schema diff.
- ⚠️ "Runs + last run id" KV replaced by "Usual cadence" (median gap between runs, last success); the last run is in the lineage panel, Activity and the schema timeline. The mock's "Freshness SLA" needs SLA data the catalogue does not carry.
- 🆕 Cadence / stale alert on the overview.
- ✅ Volume bars per recorded run (tooltip rows · time · run id).
- ✅ Owners & consumers card (owners chips, consumers name / kind / columns, contact + registered_by on hover).
- ✅ Annotate form (`data-perm="catalog_annotate"`, `?annotate=1`): owners, clear owners, consumer name/kind/contact/columns; "nothing to save" guard; toast + reload. 🆕 inline validation (name required, contact shape, unknown column with a suggestion).
- ✅ Lineage card: mini graph of up/downstream with links; "open graph".
- ✅ Schema tab: timeline newest first with diff badges, columns, run link, age. ⚠️ Per-version JSON toggle replaced by 🆕 compare any two versions (line diff, `?cmp=1&a=&b=`).
- 🆕 Columns table (type, nullable, null rate, since) and field detail (added in, null rate, distinct/top, type history, readers, column lineage) via `?field=`.
- ✅ Profile tab: latest run, rows, columns profiled (+ skipped), baseline, drift alert, per-column table with drift flags, types, null rate + sparkline (drift dot), distinct, range, top values / high cardinality; empty state pointing to `profiling:`. ⚠️ Mock's "Compare with baseline" button dropped (no per-run comparison API).
- ✅ Lineage tab: neighbours graph, upstream/downstream tables (pipeline, runs, last rows). 🆕 column lineage per incoming edge with an edge picker.
- 🆕 Activity tab: runs that wrote it (`GET /v1/runs?dataset_id=`, falls back to filtering the newest 200 runs on a server without it), runs that read it, templates that wrote it, pending changes naming it.
- ✅ Outputs tab: local outputs scoped to the dataset.
- ✅ Errors: not found vs could not load (Retry, Copy error) vs catalog missing.

#### Local outputs (dataset tab and server-wide)
- ✅ Summary "N tracked · cleaned after N days / automatic cleanup off".
- ✅ Show cleaned toggle, refresh.
- ✅ Row: path (short, full on hover, copy), state with explanation, written age, run (or dataset when server-wide).
- ⚠️ The mock's Size column is a Run column: the API reports no file size.
- ✅ Preview (preview_enabled, previewable kinds, present|replaced): rows field capped by `preview_max_rows`, Load / All, status "N rows · whole output / output has more · ms", — vs null, 240-char cut with tooltip, non-object rows span the row, inline errors.
- ✅ Manage menu (`can_manage`, `data-perm="local_output_manage"`): purge older than N days (dialog; dataset-scoped without confirm, 0 days server-wide asks), clean this dataset, clean every local output (confirm).
- ✅ Delete one now (present) — ⚠️ now asks to confirm.
- ✅ Sweep report toasts (deleted + bytes, nothing to clean, up to 3 skip reasons).

#### Lineage (`#/lineage`, `#/lineage/:root`)
- ✅ Whole graph, or focus on a dataset; ⚠️ depth is a 1–8 stepper (default 2, `?depth=`) plus direction (`?dir=both|up|down`) instead of fixed depth 8; leaving focus via the chip's ×.
- ✅ Layered left→right layout, SVG curves; ⚠️ names are short (full URI on hover) instead of scrolling inside the box.
- ✅ Click a node selects it and highlights neighbours + edges; click empty space clears; double-click opens the dataset.
- ✅ Focus root styled distinctly; re-fit on resize.
- ✅ "Datasets" button.
- 🆕 Pan (drag), zoom (wheel, buttons, +/−/0), fit, zoom %, minimap (click/drag to move), search (dims non-matches, Enter centres + selects), kind filter, show-sources toggle, freshness dots + legend, edge thickness by runs.
- 🆕 Side panel: dataset (rows, schema, last success + run, owners, upstream/downstream, open dataset / last run / trace a column / focus here) or edge (pipeline, matrix row, runs, last rows + run, column lineage table, pipeline runs / last run); Esc closes.
- 🆕 Column trace (`?col=`): expanded column lists along the traced hops, edges between column rows, panel "comes from / flows into / other columns".
- ⚠️ Mock legend's "failing or drifted" (red) dropped: the lineage and dataset list APIs carry no run failure or drift per dataset.
- Not verifiable on this demo server (no-auth admin, no stale/never datasets): viewer-role hiding (relies on `data-perm`), stale and never-succeeded styling, empty catalogue and empty lineage states (code paths written to the mock).

## Schemas, Schedules, Policy

- ✅ Catalogue of compiled sources / sinks / transforms / state stores (`GET /v1/schemas`) — Schemas list, sections with counts
- ✅ "Showing N sources and M sinks built into this server; rebuild with `--features full`" — catalogue footnote
- ✅ One connector's JSON Schema as a field table (field, type, default, description) + raw JSON — detail, raw schema in a collapsible card with copy
- 🆕 Split pane with tabs (All / Sources / Sinks / Transforms), search (`/`), deep link `#/schemas/<kind>/<name>`
- 🆕 Stat strip (fields / required / with defaults / secret); required dot; type pill; default as code
- 🆕 Example config (required fields and defaults, like `faucet init`) with copy; "Use in Submit" (`#/submit?source=<name>`)
- 🆕 Selecting a field shows its full description, values, constraints and resource / SQL markers (`?field=`); filter fields
- 🆕 "Most used here" from the connector kinds of recent runs; configuration blocks listed
- 🆕 Schedules and Policy: designed "soon" pages (what will live there, how to do it today) instead of disabled nav entries
