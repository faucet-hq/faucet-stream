# Web console design

The `faucet serve` web console (`cli/src/serve/ui/`) is built to approved
mockups. This page is the design system those mockups define and the patterns
the code uses to follow them. Read it before changing any page.

## How UI work is done: mockups first

1. **Inventory what the existing UI can do**: capabilities, actions, data
   shown, permission gates, deep links. This is a list of functionality, never
   a reference for design.
2. **Mock every page and every state after a click**: tabs, menus, forms with
   validation, dialogs, selected and filtered states, empty / loading skeleton /
   error, viewer- and tenant-limited views, narrow widths. Mocks are static HTML
   with sample data, in both themes, with one PNG per view × state and a
   `system` page showing every token and component. The console mocks live on
   the `design/console-redesign-mockups` branch (`console-redesign/`).
3. **Review the mocks against these rules, then get the owner's approval**.
4. **Build to match the approved mocks exactly.** Compare rendered screens to
   the mock PNGs in both themes and walk the inventory (✅ kept, ⚠️ changed, ❌
   dropped with a reason, 🆕 new) so nothing useful is lost.

The mock helpers (`console-redesign/mock.js`) and the build's
`components.js` have the same names and anatomy, so a page's code reads like
its mock.

## Principles

- **Quiet surfaces, one accent.** Teal means *selected*, *active* or *the
  primary action*, nothing else. Status colours (ok, warn, bad, info) mean
  status, nothing else.
- **Sans for language, mono for code.** Labels, values, names and numbers are
  sans. Mono only for literal code (config keys, ids, SQL) at 12px.
- **Every name goes somewhere.** A run, dataset, template, change or tenant
  shown anywhere links to its page; every id, URI and config has a copy button.
- **Every state is designed.** Empty states say what to do next; loading shows
  a skeleton shaped like the content; errors are a quiet alert with Retry.
- **The URL is the state.** Filters, tabs and selection live in the hash, so
  every screen is linkable and Back works.

## Tokens

All colours, sizes, spaces and radii are custom properties on `:root` in
`styles.css`. Dark is the default; light (`data-theme="light"`, or `auto` with
a light OS) redefines the same names. Nothing outside the token blocks uses a
literal colour.

| Group | Tokens |
|---|---|
| Surfaces | `--bg` → `--chrome` → `--panel` → `--panel-2`, `--hover`, `--pill` |
| Lines | `--line-soft`, `--line`, `--line-strong` |
| Text | `--text`, `--text-2`, `--muted`, `--faint` |
| Status | `--ok`, `--warn`, `--bad`, `--info` (+ `--tint-ok`, `--tint-bad` for diffs) |
| Selection | `--accent`, `--accent-hover`, `--accent-ink`, `--teal-fill`, `--teal-line`, `--teal-ink`, `--teal-badge` |
| Type | `--fs-title` 21 · `--fs-stat` 16 · `--fs-kv` 14 · `--fs` 13 (base) · `--fs-sm` 12 · `--fs-xs` 11 |
| Space | `--sp-1` 4 · `--sp-2` 8 · `--sp-3` 12 · `--gap` 14 · `--sp-4` 16 · `--sp-5` 22 |
| Radius | `--r-xs` 4 · `--r-sm` 7 · `--r` 9 · `--r-card` 11 · `--r-lg` 12 |
| Size | `--ctl-h` 30 (buttons, inputs, selects, tabs) · `--side-w` 236 · `--list-w` 372 |

## Components

Each is a function in `components.js` returning markup. Arguments are text and
are escaped, unless they are markup from `html\`…\`` or another component. Never
build markup by concatenating strings that contain data.

| Component | Use it for |
|---|---|
| `html`, `raw`, `join`, `esc` | Writing markup safely: `html\`<b>${name}</b>\`` escapes `name`. |
| `ic(name)` | A stroke icon from the fixed set. |
| `btn(label, {kind, icon, href, perm})` | One button height. `primary` is the only teal button; `danger`, `quiet`, `sm`. |
| `ibtn(icon, {on, dot, title})` | Icon buttons in tab bars and tools; `dot` marks an active filter. |
| `input`, `select`, `check`, `toggle`, `field` | Form controls; `field` adds a label (`code: true` for a config key), required dot, help or error. |
| `tabs`, `dtabs`, `dhead` | List tabs; detail-header tabs with actions on the right and a back link on narrow screens. |
| `menu(summary, items)` | A `<details>` popover: items, headings, separators, danger items. |
| `stats([[label, value, tone, filter]])` | The stat strip. A cell with a filter value is clickable; the active one is selected. |
| `sect(label, count)` + `.sect-rows` | Collapsible list sections. |
| `row(ring, title, age, meta, {on, href})` | The one list-row anatomy: ring · title · age · meta (first fact bold, via `meta()`). |
| `ring(tone, shape)` | Status ring: full, `half` (running), `hollow` (queued/draft). |
| `card({icon, title, tag, aside, right, body, note})` | All content lives in cards; tables go inside cards; never a card in a card. |
| `kv`, `grid(n, items)` | Label-over-value facts in 2, 3 or 4 columns. |
| `tbl(cols, rows, foot)` | Tables: muted sentence-case headers, numbers right-aligned (`[label, "num"]`), rows can link (`href`). |
| `pill`, `chip`, `tag`, `ch` | Status pill · filter/fact chip · small tag · release channel (`stable` is teal). |
| `lnk`, `id`, `cp`, `cpi` | Links to pages; ids in mono with copy; copy buttons on blocks and inline. |
| `code`, `diff`, `logl` | Code (yaml/json) with copy; line diffs; log lines. |
| `spark`, `bar`, `legend`, `meter` | Inline charts in token colours. |
| `alert`, `empty`, `skelRows`, `skelCards` | Error and notice alerts, empty states, loading skeletons. |
| `steps` | The Submit stepper. |
| `head(title, sub, lede)` | Page title, a dot-separated fact line and an optional lede. |

`page.js` adds the overlays: `dialog()` / `confirmAction()` (modal, Esc and
scrim close) and `toast(text, tone, link)`; and times: `ago(iso)` (relative,
absolute on hover, display timezone) and `at(iso)`.

## Page shapes

There are two.

- **Split pane** (`layout(host, "split")`) for anything you browse: Runs,
  Changes, Templates, Datasets, Tenants, Schemas, Audit, Triggers. A 372px list
  (tabs · stat strip · search · sections · rows) beside a detail (dhead · head ·
  cards). At ≤1100px the sidebar becomes icons and the detail replaces the list;
  a back link returns.
- **Canvas** (`layout(host, "canvas")`, or `"canvas-panel"` with a side panel)
  for forms and reports: Overview, Submit, Usage, Lineage, Settings. Same
  gutters as the split pane; never a narrow centred column.

## Page pattern

`views/schemas.js` is the reference split page; `views/schedules.js` the
reference canvas.

- `render(host, params)` calls `layout()`, renders a skeleton, loads, renders.
  It returns `{ update(params), teardown() }`; routes sharing a `group` call
  `update()` on navigation inside the page, so the list keeps its scroll and
  only the detail changes.
- **URL state**: read `params.query`; write with `setQuery({...})` (use
  `{ replace: true }` while typing). Build links with `links.js` (`href.run(id)`)
  and `hashFor()`.
- **Data**: `load()` for one-shot loads with an error alert and Retry; `poll(fn,
  ms)` for live pages (pauses while the tab is hidden); `patch(el, markup)`
  replaces markup only when it changed, keeping scroll and focus.
- **Permissions**: give a control `data-perm="<permission>"` (or `perm:` on
  `btn`/`menu` items). `access.js` hides every control the caller lacks with one
  stylesheet rule per permission; the server stays the authority.
- **Shell**: `setCrumbs([...])`, `setCrumbTitle()`, `setDelta()`,
  `setContext()` (top-bar chips).
- **Keyboard** (free on every split page): `j`/`k` move, `Enter` opens, `Esc`
  goes back or closes a menu, `/` focuses the list search, `⌘K` opens the
  palette.

## Dos and don'ts

Do: sans for every label, value and description; mono only for literal code at
12px; sentence-case muted 12px headers; content in cards with tables inside;
teal only for selection and the primary button; status colours only for
status; link every name and id; copy buttons on ids, URIs and configs; empty
states that say what to do; skeletons while loading; a quiet alert with Retry
on error.

Don't: uppercase or letter-spaced headers; mono for prose, numbers or names;
a card inside a card; hard-coded colours outside the token blocks; a narrow
centred column; teal links, chips, filters, focus rings or tags; ad-hoc font
sizes; "Loading…" text.

## Adding a page

1. Mock it first (every state, both themes) and get it approved.
2. Add `views/<page>.js` exporting `render<Page>(host, params)` following the
   page pattern, and register its routes in `app.js` (with a `group` for split
   pages).
3. Add it to `NAV` in `shell.js` (and its permission), and to `links.js`.
4. Page-level CSS goes in the area file (`views/operate.css`, `templates.css`,
   `data.css`, `govern.css`, `build.css`), tokens only. Anything reusable
   becomes a component in `components.js` + `styles.css`.
5. Render every state in both themes at 1440 and 1000px against the mock PNGs,
   click everything, and check the browser console is clean.
