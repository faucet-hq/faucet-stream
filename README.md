# faucet design mockups

Static design mockups for faucet's user interfaces, kept on this `designs` branch so every mockup lives in one place. Each interface has its own top-level folder: `console-redesign/` for the web console and `docs/` for the docs site (a `website/` folder will hold marketing-site mockups when that work starts). They use sample data only and are not product code: the shipped console is `cli/src/serve/ui/` on `main`, built to match the approved screens here.

UI work follows "mockups first, then build": every page and every state after a click is drawn here in both themes, the owner approves it, and only then is the console code written.

## What is here

| Path | What it is |
|---|---|
| `console-redesign/gallery.html` | Every view and state, in both themes. Start here. |
| `docs/gallery.html` | The docs site on Astro Starlight (#1073): every page and state, both themes, three widths. See `docs/README.md`. |
| `console-redesign/viewer.html` | One screen at a time, chosen in the URL. |
| `console-redesign/index.html` | The console shell the views render into. |
| `console-redesign/views/` | One file per page (`runs.js`, `schedules.js`, `policy.js`, …) with its states. |
| `console-redesign/mock.js`, `console.css` | Shared components and design tokens. |
| `console-redesign/shots/` | One PNG per view × state × theme (`<view>-<state>-<theme>[-1000].png`; the `-1000` ones are 1000 px wide). |
| `console-redesign/spec/` | The functionality inventory and the API each new screen needs. |
| `console-redesign/approved/` | Snapshots of earlier approved screens. |
| `console-redesign/deprecated/` | Earlier versions, kept for reference. |

## Run it locally

The pages load their scripts as ES modules, which browsers refuse to load from `file://`, so serve the folder over HTTP. Python 3 is all you need:

```bash
git clone --branch designs --single-branch https://github.com/faucet-hq/faucet-stream.git faucet-design
cd faucet-design/console-redesign
python3 -m http.server 8765 --bind 127.0.0.1
```

Then open:

- the gallery: <http://127.0.0.1:8765/gallery.html>
- one screen: <http://127.0.0.1:8765/viewer.html?view=schedules&state=selected&theme=dark>

`view` is a file name in `views/` without `.js`, `state` is one of that view's states (the gallery lists them), and `theme` is `dark` or `light`. Any free port works in place of `8765`; stop the server with Ctrl+C.

If you already have the repository checked out, a worktree avoids switching branches:

```bash
git fetch origin designs
git worktree add ../faucet-design origin/designs
cd ../faucet-design/console-redesign
python3 -m http.server 8765 --bind 127.0.0.1
```

## Regenerate the screenshots

`render.mjs` opens every view × state × theme in headless Chrome and writes the PNGs to `shots/`. It needs Node 22 or newer and Google Chrome:

```bash
cd console-redesign
node render.mjs                              # everything
node render.mjs --views schedules,policy     # some views only
node render.mjs --themes light               # one theme only
```

Re-render every view when the shared shell changes (the navigation, `mock.js`, `console.css`), since it appears on every screen.

## Adding a mockup

1. Add or edit `console-redesign/views/<page>.js`. Give each state a label and the endpoints or features it covers, so the gallery and `spec/` stay complete.
2. Reuse the components in `mock.js` and the tokens in `console.css`. Teal marks only selected and active items (the current nav item, the selected row, active tabs).
3. Draw every state a click can reach: tabs, menus, forms with validation, dialogs, empty, loading, error, permission-limited views (viewer, tenant), and narrow widths.
4. Render, look at the PNGs in both themes, fix what is off, then ask for approval.
