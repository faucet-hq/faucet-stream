# Console functionality inventory (#998)

Every capability of the old console, one line each with the API behind it, then the 🆕 ideas for the redesign.
Mockups cover each item; the "Mockup screens" section at the end maps screens to items.
Legend once the build lands: ✅ kept (where) · ⚠️ changed (how) · ❌ dropped (why) · 🆕 new.

## Shell (index.html, shell.js, app.js, access.js, tenant.js, tz.js)
- Grouped sidebar Operate / Data / Build / Govern / Admin, groups collapsible (localStorage `faucet.nav.collapsed`) → client
- Nav badges: running runs, pending changes (warn), tenants → `GET /v1/runs?status=running&limit=100`, `GET /v1/changes?status=pending&limit=100`, `GET /v1/tenants`
- Disabled "soon" entries for pages without an API (Schedules, Policy) → client
- Breadcrumb group / page / item; item replaced by the view's own title (`setCrumbTitle`) → client
- Context chip (short run id on a run page) → client
- Tenant switcher (hidden without tenants API; pinned for a tenant-scoped principal) → `GET /v1/tenants`, `GET /v1/whoami`
- Display timezone select (all times + date picker follow it) → client (`faucet.tz`)
- Theme toggle light/dark (starts on auto = OS) → client (`faucet.theme`)
- Access token modal (save / clear; opens on 401) → client; every `/v1` call sends `Authorization: Bearer`
- Copy link to this page → client
- Expand (hide sidebar) toggle → client (`faucet.shell.expanded`)
- Status line: history healthy/degraded, run queue full, draining, cluster N live, faucet version → `GET /readyz`, `GET /metrics` (faucet_build_info)
- Who-am-I badge: avatar initials, principal, role (viewer shown read-only) → `GET /v1/whoami`
- Permission gating: `data-perm` on controls hidden by one injected CSS rule per missing permission; 403 re-reads whoami → `GET /v1/whoami`
- Narrow (≤1100px): sidebar icons only; split views show the detail in place of the list with a back link → client
- Toasts (info / success / warn / error) → client
- 🆕 Command palette ⌘K / Ctrl+K: pages, recent runs, templates, datasets, changes, tenants, console actions → existing list endpoints
- 🆕 Keyboard: j/k move in any list, Enter opens, Esc back to the list / closes menus, `/` focuses the list search → client
- 🆕 Every id/URI/config with a copy button; relative times with the absolute time on hover; every name links to its page → client
- 🆕 Audit log, Triggers and Settings become real pages (APIs exist); Schedules and Policy stay "soon" with a designed placeholder

## Schemas (old views/schemas.js)
- Catalogue of compiled sources / sinks / transforms / state stores → `GET /v1/schemas`
- Note: "showing N sources and M sinks built into this server; rebuild with more Cargo features (`--features full`)" → `GET /v1/schemas` counts
- One connector's JSON Schema rendered as a field table (field, type, default, description) + raw JSON → `GET /v1/schemas/{kind}/{name}`
- 🆕 Split pane with search and sections with counts; deep link `#/schemas/<kind>/<name>`
- 🆕 Stat strip: fields / required / with defaults; required dot; type as pill; default as code chip
- 🆕 Example config (YAML like `faucet init`: required fields + defaults) with copy; "Use in Submit" (`#/submit?source=<name>`)
- 🆕 Raw JSON Schema collapsed at the bottom, with copy; selecting a field shows its full description, enum values, constraints


### Runs area: functionality inventory (old views/runs.js + views/detail.js)

Routes: `#/runs` (list), `#/runs/:id` (list + selected run). One view: selecting a run keeps the list, its filters and scroll.

### List (runs.js)
- Load runs page: `GET /v1/runs?limit=100[&status=a,b][&name=][&since=][&until=][&label=k:v …][&tenant=]` → `{runs, next_cursor}`.
- Tabs: All runs / Active (queued,pending,running,sharded) / Failed — set the status filter.
- Filter panel (toggle icon, dot when any filter is on): status multi-select (queued,pending,running,sharded,completed,failed,cancelled), name, since/until date-time pickers (display-timezone aware, sent as UTC ISO), label filter `key:value` or `key` (Enter adds), Apply, Clear.
- Label filters: up to 10 (API cap), refused leading ":"; shown as removable chips; mirrored in the URL `#/runs?label=…` (repeatable) via replaceState; `update(params)` re-reads them on hash change (deep link).
- Tenant scope: `withTenant` adds `tenant=` from the topbar tenant switcher.
- Refresh button: re-reads every row already shown (`limit=min(500,max(100,shown))`).
- Load more: `cursor=next_cursor` appends the next page.
- Polling: every 3 s while any shown run is non-terminal, else every 15 s; 3 s retry after an error (toast on error).
- Stat strip: Running / Succeeded / Failed / Records written over the loaded rows ("+" suffix when more pages exist).
- Sections (collapsible, remembered in localStorage `faucet.runs.sections`): Running (running,sharded), Queued (queued,pending), Failed, Succeeded, Cancelled.
- Row: status ring by status; title = name ‖ labels.pipeline ‖ run_id; tooltip name + id; age since submitted (tooltip absolute); meta facts: failed → first line of first invocation error ‖ run error; else template@version ‖ backfill unit ‖ "DLQ replay" ‖ source→sink kinds (from invocation usage) ‖ records; then kinds, records, N invocations, elapsed, pool (non-default), tenant name (when not scoped), attempt N.
- Empty states: filtered ("No runs match these filters") vs none ("No runs yet" + link to Submit / POST /v1/runs).
- Submit shortcut: `+` link to `#/submit`, gated `data-perm="config_submit"`.
- Detail placeholder when nothing selected ("Select a run" / "Nothing to show yet").
- Exports used elsewhere: `runTitle`, `runKinds`, `fmtTime` (only by detail.js in this tree).

### Detail (detail.js)
- Load: `GET /v1/runs/{id}`; polls every 3 s while non-terminal; on 404/4xx shows "Run not found"/"Could not load", keeps polling on network/5xx.
- Breadcrumb shows run name (`setCrumbTitle`); topbar context chip shows short run id (shell).
- Tabs: Overview / Invocations / Logs / Dead-letter queue; last tab remembered across runs (module var, not URL).
- Header sub-line: status pill, source→sink kinds, full run id (mono), started/submitted age (abs on hover), attempt N.
- Error alert: run error + each failed invocation `row (parent key): error` (generic "N invocation(s) failed" summary suppressed).
- Output card: records written (so far while live), invocation count; kv Submitted/Started/Finished (time + date in display tz), Duration (live: since start; else total, "+ Xs queued" when queued ≥1 s).
- Row accounting (#879): sums invocation `rows` → stacked bar + legend (written/merged/skipped/DLQ/quarantined/dropped/unaccounted), "balanced" or "N unaccounted".
- Invocations summary card: 6 slowest rows (row id + parent key, status dot, records, duration); link "all N rows →" switches tab.
- Provenance card: template link + version (labels.template/template_version), pool, tenant link, depends_on (row ← after…), replay_of, idempotency key, clock, timeout_secs.
- Labels card: sorted (caller first, provenance muted) clickable chips → add label filter to the list.
- Log export card (#806): status pill, pending lines, dropped lines, last error, "View logs ↗" link (`log_export.link`).
- Live log tail card: last 10 SSE lines with level colouring, state (connecting/streaming/ended/expired/in the log service/stream error).
- Invocations tab: all rows slowest first: matrix row, parent key, invocation run id (short), records, rows cell (ok/merged/skipped/dlq/quarantined/dropped/unaccounted, tooltip offered/filtered/non-finite), batches cell (committed/partial/dlq_all/failed), source lag (bytes/events/seconds), duration + proportional bar, error.
- Logs tab: full SSE stream `GET /v1/runs/{id}/logs` (text/event-stream; events log/truncated/end/expired(404)/link), auto-scroll when at bottom, link to the log service.
- Cancel run: `POST /v1/runs/{id}/cancel`, shown while live, `data-perm="run_write"`.
- Delete run: `DELETE /v1/runs/{id}` with confirm, shown when terminal, `data-perm="run_write"`; list refreshes and navigates back.
- Roll back… (#706): shown for a terminal run with invocation ids, `data-perm="rollback"`; panel with invocation select, dry-run (default on), force, optional config textarea (opened automatically on 422); `POST /v1/runs/{id}/rollback {invocation_id, dry_run, force, config?}` → report (mode append/upsert/overwrite, deleted/restored/conflicts, note, bookmark/token rewound, status dry-run/rolled back/blocked).
- DLQ tab (not run-scoped): location + reason (partial/dlq_all/quality/schema_drift/contract); Inspect `POST /v1/dlq/inspect {location, reason, limit:5}` → totals, by_reason, by_error_kind, 5 samples with payload; Discard `POST /v1/dlq/discard {location, reason, delete}` with confirm + "delete (no archive)" (`dlq_manage`); Replay through a pasted YAML config `POST /v1/dlq/replay {config, config_format:"yaml", from, reason, dry_run}` (`dlq_manage`).
- Narrow window: back link "Runs" to the list (keeps label query).
- `onChange` refreshes the list when the run's status changes; `onDeleted` drops it.

### 🆕 Ideas for the redesign (no code written; owner switched to mockups-first)
- 🆕 URL state for everything: `?status=`, `?q=`, `?since=`, `?until=`, `?pool=`, `?tenant=`, `?label=` and `?tab=` on the detail, so filtered lists and a run's Logs tab are linkable and Back works (old: only labels in the URL, tab in a module var).
- 🆕 Stat strip cells are filters (click Failed → failed runs), with true totals once `GET /v1/runs` `counts` (unmerged branch) is present; fall back to loaded-row counts.
- 🆕 Quick filters from any clicked value: status pill, pipeline label, pool, tenant, template, any label chip.
- 🆕 Cross-links: template + version → template page at that version; each invocation's `usage.dataset_id` → dataset page (written dataset) and its lineage focus; tenant → tenant page; `approved_change`/change label → change request; `replay_of` → DLQ tab prefilled with that location.
- 🆕 "What this run touched" card: datasets written (from invocation usage dataset_id/dataset_uri), source/sink kinds, estimated cost + bytes (from usage.cost/usage.usage), linking to Datasets and Usage.
- 🆕 Copy buttons on run id, invocation ids, idempotency key, dataset URIs, error text, and the run's stored config.
- 🆕 Config tab: the stored config body (`config_body` when the server keeps it) as YAML with copy + "Re-run with this config" (POST /v1/runs, `config_submit`) and "Open in Submit" prefilled.
- 🆕 Compare with previous run of the same pipeline/template: records, duration, row accounting delta.
- 🆕 Logs tab: level filter (INFO/WARN/ERROR), text search, follow toggle, download; DLQ tab prefilled with the run's DLQ location when its config names one.
- 🆕 Relative times everywhere with the absolute time (display zone) on hover; keyboard j/k/Enter/Esc in the list (global).
- 🆕 Long lists: virtualise rows or page by cursor with "Load more"; keep scroll + selection across polling.

### Templates — functionality inventory (old views/templates.js, ~1880 lines)

Routes: `#/templates` (list + registry tools), `#/templates/:id` (template page); `?sink=<id>` on a template opens its Run tab with that sink preselected. Views share `{teardown, update}`; update re-selects without re-render.

### List pane
- Load all templates — `GET /v1/templates` (also returns `sync.origins[]`).
- Feature-missing detection: bare 404 → "templates feature not compiled / no --template-store" empty state.
- Status tabs All (launched+draft) / Launched / Draft (client-side).
- Search by id or description (client-side, filter panel toggle with active-filter dot).
- Sort by name / last updated (`created_at`).
- "Include deprecated templates" toggle.
- Stat strip: Templates / Launched / Draft / Failing (newest version's tests failing|unknown).
- Sections by kind: Source templates / Sink templates / Deployments / Pipelines (collapsible, persisted in localStorage `faucet.templates.sections`).
- Row: ring tone (ok launched, warn hollow draft, bad tests failing, muted hollow deprecated); title id; age = newest version `vN`; meta lead (stable vN / draft / vN tests failing / deprecated) + channel tags (`tag vN`), "newest vN" when ≠ stable, test word (passed / not run / no tests), param count; tooltip = id + description + registered age.
- Refresh button (reload list + current detail).
- Matrix button (opens registry overview → Compatibility tab).
- Register button `+` — data-perm `template_admin` (opens overview → Register tab).
- Empty states: unavailable / no match / none registered (explains source/sink/pipeline).

### Registry overview (detail pane with nothing selected)
- Tabs Compatibility / Register (`template_admin`) / Sync (`template_admin`, only when sync origins exist).
- Compatibility matrix — `GET /v1/templates/matrix` (only when ≥1 source and ≥1 sink template): sources as rows (name, @owner, source_type, stream count, link), sinks as columns (name, @owner, sink_type, link); cell ✓ links to `#/templates/<src>?sink=<sink>`; partial cell `ok/total` (not linked), ✗ none, — no cell; tooltip lists per-stream write mode / satisfies / incompatible reasons.
- Matrix facet filters: source owner, source type, sink owner, sink type (multi-select), count "x of y sources × …", Clear filters.
- Register form — `POST /v1/templates {config, config_format yaml|json, id?, description?, launch?}`: placeholder example source-template; kind explanation; toast with version, inherited tests, tests failing; navigates to the new template; error output with `details` JSON.
- Sync panel — origins list (name, kind, prefix, launch policy, prune policy, interval); origin picker (all / one); Dry run / Pull now — `POST /v1/templates/sync {origin?, dry_run}`; per-origin report: register (after vN, → launch), launch vN, revive, deprecate, orphaned, unchanged, skipped (reason), warnings, failed; origin errors; mutation-count toast; reload after pull.

### Template page header
- `GET /v1/templates/{id}?version=newest` (draft templates open too); not-found / unavailable states.
- Crumb title = id; status pill Launched · vN / Draft · never launched / Deprecated; kind label with explanation; versions + params count; "vN registered by X" or registered age; config `name` when ≠ id; description quote.
- Draft alert (unpinned runs refused); Deprecated alert with reason.
- New version (`template_admin`): opens register form locked to id, seeded with newest body (`GET …?version=N`, `body_withheld` refusal), description prefilled; stays on page.
- Roll back (`template_admin`) — `POST /v1/templates/{id}/rollback`; disabled with reason when no previous.
- Deprecate / Revive template (`template_admin`) — `POST /v1/templates/{id}/deprecate {undo, reason?}` (prompt for reason).
- Launch vN (`template_admin`) shown when newest ≠ stable, not retired, template not deprecated, tests passing/untested — `POST /v1/templates/{id}/launch {version}` (toast already_launched / replaced).
- Tabs Versions / Run (`run_write`; labelled "Pairings" for sink, "Use" for deployment) / Health (`status_read`, pipelines with a stable version only).

### Versions tab
- Launch-gate card for newest: "vN passed/failed X of Y cases" / no bundle / no result; body sha tag; tested age; kv: cases by origin, retried count, shared suites (name@release / vN), faucet version + recorded_by; failing case list; footnote verdict (stable / gate open / untested runs tests first / blocked + why).
- Versions table, newest first: version, channel chips (stable teal, previous, newest, assigned tags) + deprecated chip w/ reason, tests status dot (passed/failing/not run/no tests/unreadable, reason tooltip), last tested age, runs in last 7 days (`GET /v1/runs?label=template:<id>&since=…&limit=500`, count by `template_version` label, "+" when capped).
- Per version: Run (`run_write`) → opens Run tab with that version picked; Launch (`template_admin`, disabled when live / retired / tests blocking, reason tooltip).
- Per-version More menu: assign channel dev/test/staging/pre-prod/canary/prod (`template_admin`, disabled unless tests pass/untested) — `POST /v1/templates/{id}/tags {tag, version}`; Test history toggle; Run tests (`template_admin`, has_tests) — `POST /v1/templates/{id}/versions/{v}/test` (toast pass/fail counts); Show config / Show clean config (`template_admin`) — `GET …?version=v` / `&clean=true`, withheld notice, copy button; Deprecate/Revive version (`template_admin`, reason prompt) — `POST /v1/templates/{id}/versions/{v}/deprecate {undo, reason?}`; Delete vN (`template_admin`, confirm) — `DELETE /v1/templates/{id}?version=v` (last version → back to list).
- Under a version: "why it can't take a label" (failing cases / unreadable reason / untested note); recorded test runs table (time, pass/fail, cases passed/total, retried, error, faucet version, by).
- Launch history card: age, #seq, vN, launched_by, tests skipped reason / tests passed.

### Run tab (pipeline / source template) — `POST /v1/templates/{id}/runs`
- Source-template: sinks `GET /v1/templates?kind=sink-template` and deployments `?kind=deployment` (deprecated excluded); none → explain registering a sink.
- Sections: Source/Template (id, version picker newest-first with channel notes + deprecated, channel chips of pick), Sink (template + version picker: channels then pinned), Deployment (none / template + version).
- Param fields = union of source + sink + overlay params for the picked versions (`GET /v1/templates/{t}?version=v`, cached), first declaring section wins; computed params excluded; typed inputs (bool select, int/float number, secret password), default placeholder, tags type/required/secret/trusted, md description; typed values preserved across re-render; int/float validation; numbers sent as exact text, bool as boolean.
- Row/stream picker (when ≥2 selectable rows) — `GET /v1/templates/{id}/rows?version&sink&sink_version&overlay&overlay_version`: checkbox per row (id, status pill, tags, child-of, after deps, resolved write mode / alias / requested, health), unsupported rows disabled with reason; defaults/all/none quick picks; include parents off/eligible/all; ancestor highlighting; live preview of resolved run set — same endpoint with `state=false&select=…&include_parents=…` (pulled-in / blocked marks, error).
- Run name (optional); allow_untested gate: unlabelled version whose tests don't pass after launch → "run anyway" checkbox (labelled + audited).
- Submit body {version, sink, sink_version, overlay, overlay_version, params, selection{select, include_parents} only when non-default, name, allow_untested}; toasts deprecated warning, warnings, run id + via; navigates to `#/runs/<id>`; error output with details.
- Sink template "Pairings": list of source templates (`?kind=source-template`) linking to `#/templates/<src>?sink=<this>`. Deployment "Use": list of source templates with how-to.

### Health tab — `GET /v1/status?template=<id>` (`status_read`)
- Overall health pill, state kinds, generated_at, state note; per row: row id, health, last success, bookmark (k=v), lag (human, probe vs last-run tooltip), DLQ count (—/?/n), next resume; reasons/errors/failed-batches line.

### Permission gates
`template_admin`: register, new version, launch, rollback, deprecate/revive, channel assign, run tests, show config, version deprecate, delete. `run_write`: Run tab/buttons. `status_read`: Health.

### 🆕 ideas (design proposal — not built; owner switched to mockups-first)
- Restructure the page around the lifecycle: a stepper Draft → Tested → Launched → Channels at the top of an Overview tab, each step showing its evidence (newest version's test verdict + gate, stable version + launched by/when, channel map dev…prod with their versions); the primary action is always the next step (Run tests / Launch vN / Assign channel).
- Source × sink pairing as a first-class tab: for a source template, its row of the matrix (each sink with ✓/partial/✗ and per-stream write plan) with "Run into this sink"; for a sink, its column; for a deployment, the sources it applies to. Full matrix stays a list-pane tool (`#/templates?view=matrix`, facets in URL).
- Register / sync as list-pane tools with their own URL (`?view=register`, `?view=sync`); New version opens the same editor seeded and locked.
- Versions tab: compare any two versions' bodies (textDiff of `GET …?version=a` vs `b`, clean form), per-version menu with all actions; tests + run history inline under a row.
- Cross-links: recent runs of this template (`/v1/runs?label=template:<id>`) with status and version, link each to `#/runs/<id>`; datasets those runs wrote (invocations' `usage.dataset_id` → `#/catalog/<id>`); pending changes mentioning it (`/v1/changes`, kind template_*); "runs: last 7 days" becomes a link to the filtered Runs list.
- URL state for list tab/search/sort/kind and detail `?tab=`/`?version=`; j/k list keys; copy buttons on ids, bodies, sha; relative times with absolute on hover; quick filters by owner (`acme/…`) and kind from chips.

### Datasets + Lineage — functionality inventory (old console)

Status column is filled after the rebuild: ✅ kept (where) · ⚠️ changed · ❌ dropped · 🆕 new. Rebuild not started (mockups-first), so every item below is still "to verify".

### Datasets list (`#/catalog`) — `GET /v1/catalog/datasets?limit=200&cursor=` (walks every page, ≤100 pages, then filters client-side)
- Role tabs: All / Destinations (`roles` contains sink) / Sources.
- Stat strip: Datasets total, Last 24h (last_success < 24h), Evolved (schema_versions > 1), Owned (owners non-empty).
- Filter panel toggle (icon button with a dot when any filter is active).
- Filter: free-text search over the URI.
- Filter: kind multi-select; options are the kinds still reachable under the other filters; pruned when they vanish.
- Filter: last-success date range from / to (themed date-time picker, tz-aware).
- Refresh button: reloads the list and the open detail.
- Link to the lineage graph (`#/lineage`).
- "Local outputs" button: opens the server-wide local-outputs panel in the detail pane.
- Sections Destinations / Sources, collapsible, collapse state persisted (localStorage `faucet.datasets.sections`).
- Paging: 200 rows per section, "Show N more" footer.
- Row: fresh (<24h) ring vs hollow, title = URI without scheme, age = last success, meta = kind · rows last run · schema vN · consumers · first owner · "by <pipeline>"; tooltip full URI + last success time.
- Empty states: catalog feature missing (404 without code), no match, nothing catalogued (link to Submit).
- Selection stays in the list without re-render on route update (`update(params)`); narrow-window back link.

### Dataset detail (`#/catalog/:id`) — `GET /v1/catalog/datasets/{id}`
- Breadcrumb title = short URI.
- Header pill: last success age (ok tone when fresh); kind · roles; "written by <pipeline>"; first seen age.
- "Lineage graph" button → `#/lineage/:id`.
- Tabs (in-memory, not in URL): Overview / Schema (count) / Profile (drift count) / Lineage (edge count) / Outputs.
- Overview volume card: header "N rows last run · M columns", schema vN tag, last schema change age.
- KV: Rows per run (latest, sparkline over `stats[].records`, average over N runs).
- KV: highest null-rate column with sparkline over profile history (else Total rows).
- KV: Runs count + last run id (short) + last success time.
- KV: Consumers count + first 3 kinds.
- Footnote: latest schema diff summary (+added ~widened !changed −removed, warn tone on removed/changed) or "schema vN since …".
- Volume bars: one bar per recorded run (≥3 runs), tooltip rows · time · run id.
- Owners & consumers card: owners pills; declared consumers (name, kind, columns read or "every column", contact, registered_by).
- Annotate form (`data-perm="catalog_annotate"`): owners (comma list), clear-all-owners checkbox, consumer name/kind/contact/columns → `POST /v1/catalog/datasets/{id}/consumers` `{owners?, consumers?:[{name,kind,contact,columns}]}`; "nothing to save" guard; toast + reload list and detail.
- Lineage card: upstream edges / this dataset / downstream edges; each edge links to the neighbour dataset with runs + last rows; "open the graph".
- Schema tab: timeline newest first, version, diff badges per change kind, column count, age, per-version JSON toggle showing the JSON Schema.
- Profile tab (`d.profile`): latest run id + time, rows, columns (+skipped beyond max_columns), baseline runs, drift count / stable; drift findings list (column.metric + detail); per-column table: name + drift metric flags, types, null rate + null-rate sparkline with drift dots, distinct, numeric min/mean/max or string length range, top values with share (or "high cardinality"); empty state pointing to `profiling:`.
- Outputs tab: local outputs panel scoped to `dataset_id`.
- Errors: not found vs could not load vs catalog missing.

### Local outputs panel — `GET /v1/local-outputs?dataset_id=&include_expired=true`
- Summary: N tracked · auto-cleaned after N days / cleanup disabled (`retention_days`, `gc_enabled`).
- "Show cleaned" toggle (include_expired).
- Refresh.
- Row: kind, path, state (present / expired / external / replaced, each with an explanation tooltip), age, last written.
- Preview (only `preview_enabled`, kinds jsonl/csv/parquet/file, state present|replaced) → `GET /v1/local-outputs/{id}/preview?row_count_to_load=N|all`; rows input capped by `preview_max_rows`, default `preview_default_rows`; Load / Max(all) buttons; status "N rows · ms · output has more / whole output"; table distinguishes missing field (—) vs null; long cells truncated at 240 chars with tooltip; non-object records span the row; errors inline.
- Manage (only `can_manage`): purge older than N days → `POST /v1/local-outputs/cleanup {older_than_days, confirm: days==0}` (dataset-scoped adds `dataset_id`, no confirm); confirm dialog for 0 days.
- Manage: clean all / clean this dataset → `POST /v1/local-outputs/cleanup {all:true, confirm:true}` or `{dataset_id}` after a confirm dialog.
- Delete one now (state present) → `DELETE /v1/local-outputs/{id}`.
- Sweep report toasts: deleted count + bytes, "nothing to clean", up to 3 skipped reasons (pre_existing, already_deleted, not_on_disk, in_flight, delete_failed).

### Lineage (`#/lineage`, `#/lineage/:root`) — `GET /v1/catalog/lineage[?root=&depth=8]`
- Whole graph, or rooted at a dataset with depth 8 ("Whole graph" button to leave root mode).
- Layered left→right layout (sources → sinks), SVG edges, node boxes with the URI scrolling inside the box.
- Click a node: highlight it, its neighbours and their edges; others recede. Click empty space clears.
- Double-click a node: open the dataset page.
- Root node styled distinctly; re-layout on window resize.
- "Datasets" button back to the list.
- Edge data available but unused by the old graph: pipeline, row, runs, last_records, last_run_id, first/last_seen, `column_lineage.fields` (dst column → src columns).

### 🆕 ideas (not built — mockups first)
- Dataset titles: short name (`datasetName(uri)`), full URI only in detail with a copy button (local paths contain usernames).
- Tabs and filters in the URL (`?tab=schema`, `?role=`, `?kind=`, `?q=`) so every state is linkable and back works.
- Related panel on a dataset: recent runs that wrote or read it (`/v1/runs`, match `invocations[].usage.dataset_id` / pipeline), templates whose runs write it (run labels `template`), quick links to each.
- Schema compare: pick two versions, line diff of their JSON Schemas (components `textDiff`).
- Freshness against the run cadence: highlight a dataset whose last success is older than 2× its usual gap between runs (from `stats[]`).
- Lineage as a real graph canvas: hand-written SVG with wheel zoom, drag pan, zoom in/out/fit buttons; focus mode `#/lineage/:root?depth=N` (1–8 hops up and downstream, a stepper); nodes show short name, kind tag, status ring coloured by freshness (ok <24h, warn stale, muted never succeeded), rows of the last run; edges labelled by pipeline on hover, thickness by runs; selecting a node opens a side panel (split-pane style): kv grid (kind, roles, last success, rows, schema version, owners), upstream/downstream lists, links to the dataset page, its last run and lineage focus; selecting an edge shows pipeline, row, runs, last run link and the column lineage table (dst column ← src columns) on demand; search box to find and centre a node; keyboard: +/− zoom, 0 fit, Esc clear.
- Column-lineage focus: pick a column on the selected dataset and trace it through `column_lineage` hops, dimming unrelated edges.

### Functionality inventory — Changes, Tenants, Usage, Audit (fork D)

Source: old views `changes.js`, `tenants.js`, `usage.js` (logic only) + `GET /v1/audit` (no old page).
One line per capability: what it does → API / perm.

### Changes (`#/changes`, `#/changes/:id`)
- List change requests (up to 500, tenant-scoped) → `GET /v1/changes?limit=500&tenant=` (perm change_read, viewer+)
- Auto-refresh the list every 15 s → same
- Manual refresh button (list + open detail) → same + `GET /v1/changes/{id}`
- "Needs me" tab (only for `change_approve`): pending, not requested by me, not already approved by me → client filter on list + `/v1/whoami`
- "All" tab → client
- Stat strip: pending / executed / rejected / failed+invalidated counts ("+" when list truncated at 500) → client
- Sections: Pending / Approved, executing / Decided (executed, rejected, failed, invalidated, expired), collapsible, persisted in localStorage → client
- Row: status ring, one-line summary of what the change proposes (run pipeline·rows→sinks; template register/launch/delete/deprecate), age, meta (kind, approvals n of m, requester / rejection reason / error / approvers) → client
- Empty states: "nothing needs your approval" (link to All) and "no change requests yet" (how to propose: Submit page option, POST /v1/changes, agent propose_run, --require-approval run) → client
- Deep link to one request; 404 vs error message → `GET /v1/changes/{id}`
- Detail head: title = summary (breadcrumb title too), status pill (pending n/m approvals or final status), requester + role, age, expires-in (pending) / decided-ago, tenant → client
- Outcome alert: started run (link `#/runs/{run_id}`) or template id + version (link `#/templates/{id}`) → client
- Error alert (execution failed / approval invalidated) and rejection alert (by, when, reason) → client
- Requester's reason shown as a quote → client
- Plan card: plan as a diff (template launch stable before→after; delete; deprecate/revive; register vN (+launch); per pipeline row source→sink, write_mode, delivery, transforms, schema drift, quality/contract/masking passes; budget lines); material fingerprint (first 8 chars); "re-planned on approve" tag → client over `plan`
- Plan verdict footnote: policy violation count / breaking downstream datasets / impact unknown / additive only / no schema change → client over `plan.rows[].impact|policy`
- Approvals card: each approver (avatar initials, principal, role, when, comment) + empty slots "waiting for an approver" → client
- Budget card: ceilings attached to the request → client over `budget`
- Decision card (perm change_approve, pending only): optional approval comment, rejection reason (required) → client
- Approve (label "Approve and run/apply" when it completes the quorum) → `POST /v1/changes/{id}/approve {comment?}` (change_approve); toast result; 403 names the policy rule
- Reject (reason required, else toast + focus) → `POST /v1/changes/{id}/reject {reason}` (change_approve; requester may withdraw)
- After a decision: reload detail + list, refresh nav badges, switch to All if no longer "needs me" → APIs above + `/v1/changes?status=pending` (shell badge)
- Impact tab: per row severity, planned-from, column delta counts, affected datasets table (uri, depth, severity, opaque, owners), notes; policy violations table (rule, column, label, sink) → client over `plan.rows[]`
- Impact empty state explaining catalog:/policy: requirements → client
- History tab: timeline requested → approvals → rejection → final status (+run link) → expiry (future); change id shown → client

### Tenants (`#/tenants`, `#/tenants/:id`)
- List tenants (404 → "built without the tenants feature") → `GET /v1/tenants` (tenant_read)
- Refresh button → same
- Create tenant form (id, optional name; perm tenant_admin), then open it → `POST /v1/tenants {id, name?}`
- Stat strip: tenants, running (sum active_runs), connections (+ "n re-auth" in red) → client
- Sections: Active / Suspended; row ring ok / warn (needs re-auth) / hollow (suspended); meta: active runs or re-auth count, id, connection count, limits text → client
- Deep link to one tenant; 404 vs error → `GET /v1/tenants/{id}`
- Connect providers list → `GET /v1/connect/providers`
- Detail head: name, status pill (active / suspended / n need re-auth), id, created by + when, active runs → client
- "Runs for this tenant": sets the tenant switcher and opens Runs → client (`setTenant`) + `#/runs`
- Suspend / Resume (tenant_admin) → `PATCH /v1/tenants/{id} {suspended}`
- Limits form: concurrent runs, records/bytes/seconds per run; blank = none (tenant_admin) → `PATCH /v1/tenants/{id} {limits}`
- Labels shown as k=v chips (read-only) → client
- Recent runs card: latest 10 (status ring, name, age, records) linking to the run → `GET /v1/runs?tenant=&limit=10`
- Delete tenant with typed-id confirmation (tenant_admin); toast reports runs + state keys deleted; clears the tenant switcher → `DELETE /v1/tenants/{id}`
- Connections tab: table name, type (+ via provider), status (active / needs re-auth + reason), updated by + when → from tenant record
- Reconnect a provider-backed connection (opens authorize URL in a new tab) (connection_manage) → `POST /v1/tenants/{id}/connect/{provider} {connection, redirect}`
- Delete connection (connection_manage) → `DELETE /v1/tenants/{id}/connections/{name}`
- Connect through a provider form: provider, connection name, redirect URL (defaults to this page) → `POST …/connect/{provider}`; "no providers configured" hint (--connect-providers)
- Store credentials form: name, type (static / oauth2 / oauth2_refresh / token_endpoint / flow), config JSON (validated client-side) → `PUT /v1/tenants/{id}/connections/{name} {provider:{type,config}}`
- Credentials never displayed (names, types, status only) → server guarantees

### Usage (`#/usage`)
- Report grouped by pipeline / row / dataset / sink / day (+ tenant when the server has tenants) → `GET /v1/usage?by=` (usage_read)
- Filters: pipeline, since, until (YYYY-MM-DD), tenant from the global switcher; reload on change/Enter; refresh button → same params
- 404 → "built without the catalog feature" message → client
- Stat strip: invocations, rows written (compact), bytes written, estimated cost, hosted equivalent → `report.total`
- Table per group: key, runs (+failed), rows in, rows out, bytes out, duration, requests (round trips), est. cost, hosted eq.; "compute not reported" marker per connector; totals row → `report.rows[]`, `report.total`
- Window label (group count, since → until) → client
- Explanatory note: estimates from `usage.pricing`, hosted eq. meaning, compute-not-reported is never zero → static text
- Empty: "No usage recorded for this window — run a pipeline first" → client
- Exports used elsewhere: none (fmtBytes is defined here; no other module imports it)

### Audit log (new page `#/audit`, `#/audit/:id`)
- API: `GET /v1/audit?principal&action&tenant&since&until&limit(≤1000, default 100)` → `{entries:[{id (UUIDv7, sort key), timestamp, principal, role, action, tenant?, target?, detail?, run_id?, config_fingerprint?, source_ip?, result}]}`, newest first; perm audit_read (admin); nav entry gated `data-perm="audit_read"`
- No single-entry endpoint: `#/audit/:id` must resolve from the loaded page (or a widened query)
- No cursor/pagination: deeper history needs a narrower `until`

### 🆕 ideas (not built — owner switched to mockups-first)
- Changes: requester / kind / tenant quick filters from clicked values (URL state); link the template version in the plan and its dataset(s); "approved by me" filter; copy id
- Changes: link plan rows' sinks to their catalog datasets and affected datasets to `#/catalog/{id}`
- Tenants: usage card for the tenant (`GET /v1/usage?tenant=&by=pipeline`), pending changes for the tenant (`GET /v1/changes?tenant=`), recent audit for the tenant when admin (`GET /v1/audit?tenant=&limit=20`); runs link as `#/runs?tenant=` deep link instead of mutating the global switcher
- Usage: filters + group + sort in the URL; sortable columns; rows drill into runs (`#/runs?pipeline=` / dataset page / tenant page / day window); trend sparkline per group needs per-day data (client can derive with a second `by=day&pipeline=` call)
- Audit: sections by day; quick filters from clicked principal / action / result / tenant; detail kv grid with links to run, template (`target: template:<id>@<v>`), tenant, connection; "more from this principal" (same API, `principal=`); denials-only filter (`result=denied` is client-side)

### Demo data created
- None.

### Build area inventory: Submit, schema form, Triggers, Settings (fork E)

Source: old-ui/views/submit.js, schema-form.js, secret-field.js. One line per capability, followed by the API behind it.

### Submit (`#/submit`, nav perm `config_submit`)
- Loads the connector catalog (sources, sinks, transforms, state, blocks) on open: `GET /v1/schemas`
- Two modes: Guided (wizard) and Editor (raw textarea); switching to Editor seeds it with YAML built from the wizard, but only when the editor is empty: client-side
- Read-only notice shown when the caller lacks `run_write`: `GET /v1/whoami` (via access.js `can`)
- Source kind picker; the selected kind's config form renders from its schema: `GET /v1/schemas/source/{name}`
- Sink kind picker with its schema form: `GET /v1/schemas/sink/{name}`
- Transforms: "+ add transform" appends a picker + schema form per transform; order is kept; there is no remove control (old gap): `GET /v1/schemas/transform/{name}`
- Optional pipeline blocks grouped Reliability (state, dlq, delivery, resilience, sla) and Data governance (quality, contract, masking, schema), shown only if the server lists them in `catalog.blocks`; adder buttons carry the block description as a tooltip: `GET /v1/schemas` (blocks)
- Adding a block renders its schema form; ✕ removes it and restores the adder: `GET /v1/schemas/block/{name}`
- Block placement honoured: `placement: "pipeline"` → `pipeline.<name>`, else top level: client-side
- DLQ block special case: its `sink` is a full connector picker (defaults to `file`, else `jsonl`) plus that sink's schema form, then the DLQ's other fields: `GET /v1/schemas/sink/{name}` + `/block/dlq`
- Config assembly: `{version:1, name?, pipeline:{source:{type,config}, sink:{type,config}, transforms:[{type,config}], <blocks>}}`; empty block values are dropped: client-side
- Minimal dependency-free YAML emitter (quotes strings YAML would misread, `[]`/`{}` for empty collections, nested list items aligned): client-side
- Run options: name, format (yaml|json), timeout_secs, doctor_first, idempotency_key: fields of `POST /v1/runs`
- Request approval checkbox + reason (perm `change_request`): `require_approval`, `reason` on `POST /v1/runs`
- "Check (doctor)" button (perm `doctor`): validates + probes and prints the JSON report, or ✗ + error details: `POST /v1/doctor` {config, config_format}
- "Run" button (perm `config_submit`): submits and navigates to the new run (`#/runs/{run_id}`), toast with status: `POST /v1/runs`
- Pending approval response (`status: pending_approval`) → toast + navigate to `#/changes/{change_id}`: `POST /v1/runs` 202 body
- Submit errors: message, Retry-After hint (429), JSON `details` in an output block + error toast: `ApiError`
- Not in the old page (available in the API, unused): `labels`, `clock`, `concurrency`, `callback`, `budget`, `selection`, `depends_on`, `pool` on `POST /v1/runs`; `POST /v1/plan` (Plan, operator) with `sample`, `impact`, `depth`

### Schema form (`schema-form.js`, only imported by submit.js; `renderSchemaForm(schema, value) -> {el, read()}`)
- Object properties with required markers; `$ref` → `$defs` resolution
- `Option<T>` (oneOf/anyOf T|null): a nested object sits behind an enable checkbox (left off = feature not configured); a scalar renders as T
- Unit enums documented per variant (oneOf of `const`s) → select with per-option tooltips
- Discriminated unions (`{type, config}` auth pattern) → type select + that variant's fields; a lone `config` object is flattened
- `enum` → select (default preselected); boolean → checkbox; integer/number → number input; string → text input
- Secret-looking string fields render as password inputs with `autocomplete=new-password` (`secret-field.js` `isSecretField`, tested by `scripts/test_console_secret_field.py`; must keep that export)
- Defaults prefilled as values; first sentence of the description as placeholder + inline help (markdown `**bold**`/`` `code` `` rendered)
- Arrays: add/remove items of the item schema
- Object/array defaults seed nested controls; anything else falls back to a raw-JSON textarea
- `read()` drops empty strings/empty objects so unset fields are omitted

### Triggers (new page; no old view)
- From `GET /readyz` → `triggers: [{name, healthy}]` (only when the server runs with `--triggers`); the demo returns `[]`
- Fire a webhook trigger: `POST`/`PUT /v1/triggers/{name}` (optional JSON body → `${trigger.body}`); 202 run enqueued, 200 `{status: coalesced}`, 400 method not allowed, 404 unknown/no triggers, 422/500 config problems; perm `trigger_fire` (operator+)
- Not available: trigger type (webhook / object_arrival / queue_depth / schedule), methods, last fire time, runs per trigger → the page can only show name + health, so "fire" must be offered with a caveat that non-webhook triggers return 404/400. Runs started by a trigger are attributed `trigger:<name>` in audit and carry the trigger's labels/idempotency key prefix `trig:<name>:` (can be searched in runs).
- Metrics exist (`faucet_serve_trigger_*` on `/metrics`: healthy, last fire unix, fired total, errors) and could be parsed from `/metrics` text without new APIs.

### Settings (new page; no old view)
- Identity: `GET /v1/whoami` → principal, role, permissions, tenant
- Server: `GET /readyz` → status, history_ok, queue_ok, draining, cluster {enabled, instances, pools, members[{instance_id, listen, pools, in_flight, max_concurrent}]}, triggers
- Version: `GET /metrics` → `faucet_build_info{version}`
- Admin: hot-reload the `--default-config` merge base: `POST /v1/reload` → {reloaded, path, reason}; perm `reload` (admin)
- Console preferences (client-side, localStorage): theme (`faucet.theme`), display timezone (tz.js), tenant scope (`faucet.tenant`), sidebar hidden (`faucet.shell.expanded`), nav groups collapsed; access token (api.js `token`)
- Keyboard reference: ⌘K/Ctrl+K palette, j/k, Enter, Esc, `/` (page.js)

### 🆕 ideas
- 🆕 Submit "Plan" action next to Validate: `POST /v1/plan` (resolved row, write mode, delivery guarantee, transform chain, policy result; optional sample records + impact)
- 🆕 Live YAML preview of the assembled config with copy, beside the form
- 🆕 Prefill from URL: `#/submit?source=<kind>&sink=<kind>` (links from Schemas "use this connector")
- 🆕 Re-run / edit a past run's config: `#/submit?from=<run id>` when `GET /v1/runs/{id}` carries `config_body` (stored for clustered/caller configs); fall back to an explanatory note when absent
- 🆕 Remove a transform, reorder transforms
- 🆕 Expose the unused run options: labels (key/value rows), pool, timeout, concurrency, budget ceilings
- 🆕 Triggers page: health list + curl snippet with copy + Fire button (perm `trigger_fire`) + "runs from this trigger" link (runs filtered by `trig:` idempotency/labels where present)
- 🆕 Settings page: identity, permissions chips, server/cluster health, version, preferences, shortcuts reference, reload button

### Verification checklist
Not built: the coordinator switched to mockups-first before any page code was written. No file under cli/src/serve/ui was changed by this fork.
