# Docs site redesign (faucet-hq/faucet-stream#1073)

Static mockups for moving the docs site from mdBook to **Astro Starlight**, in the calm style of the Apache Magpie docs: a quiet sidebar, task cards, a short header and generous spacing. They use sample data only. Nothing here is product code; the Starlight build will be made to match these screens once they are approved.

| Path | What it is |
|---|---|
| `gallery.html` | Every page and state, light or dark, at 1440 / 1000 / 390 px. Start here. |
| `index.html?view=…&state=…&theme=…` | One screen, live. |
| `views/*.js` | One file per page type, with its states. |
| `mock.js`, `docs.css` | The shell (header, topics, sidebar, "On this page"), shared components and design tokens. |
| `views/system.js` | The design-system page: every token and component. |
| `shots/` | One PNG per view × state × theme × width (`<view>-<state>-<theme>-<width>.png`) and `manifest.js` for the gallery. |
| `spec/functionality-inventory.md` | What the current docs site can do, and what each mock keeps, changes, drops or adds. |
| `render.mjs` | Regenerates `shots/` with headless Chrome: `node render.mjs` (or `--views guide,catalog`). |

## Run it locally

All pages use plain scripts, so they open straight from disk: open `gallery.html`. To serve them instead:

From the root of the `designs` branch:

```bash
python3 -m http.server 8766 --bind 127.0.0.1
# http://127.0.0.1:8766/docs/gallery.html
```

## What is drawn

- **Pages:** docs home, guide page, tutorial (steps, file tree, diff lines), cookbook recipe (diagram, tables), Learn the architecture (Beginner / Architect toggle), topic home (Cookbook), benchmarks (charts), 404.
- **Reference:** connector catalog (all / filtered / no match), capability matrix (wide table, scrolled sideways), CLI reference.
- **Overlays:** search ⌘K (empty, loading, results, no results), Ask AI menu, theme menu (Light / Dark / Auto), phone menu.
- **States:** scrolled (sticky header, "On this page" follows), code tab switched, code copied, "On this page" dropdown open (tablet and phone), sidebar groups collapsed and expanded.
- **System:** colour tokens for each theme, type scale, buttons, chips, badges, inputs, cards, asides, code blocks, steps, flow diagram, segmented control, table, page footer.
