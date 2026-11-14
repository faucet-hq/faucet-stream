# faucet-stream docs site (Astro Starlight)

The new docs site for faucet-stream, built with [Astro Starlight](https://starlight.astro.build/) to the approved mockups on the `designs` branch (`docs/`). Tracking issue: #1073.

For now the pages are **converted** from the mdBook sources in `docs/book/src` on every build, so `docs/book` stays the one place to edit docs. The mdBook site keeps deploying until a switch-over PR moves the deploy here, makes these pages the source, and removes mdBook.

## Run it

```bash
cd docs/site
npm ci
npm run dev       # convert + dev server at http://localhost:4321/faucet-stream/
npm run build     # convert + static build into dist/
node scripts/check-redirects.mjs   # every old mdBook .html URL resolves
node scripts/check-links.mjs       # every internal link and #anchor resolves
```

## Layout

| Path | What it is |
|---|---|
| `nav.mjs` | Maps the mdBook `SUMMARY.md` parts onto the header topics (Learn · Cookbook · Reference · Operate · Extend · Compare) and their sidebar groups. A page with no group mapping is still shown, and the converter warns. |
| `scripts/convert.mjs` | Converts `docs/book/src` into `src/content/docs` (titles, descriptions, typed asides from blockquotes, Mermaid blocks, hidden Rust lines, `{{#include}}`, links), writes the topic sidebar, the topic home pages, a Markdown copy of each page for "Copy page as Markdown", and a redirect page for every old `.html` URL. |
| `authored/` | Pages written for this site only: the docs home and the 404 page. |
| `src/components/` | Starlight overrides: header with topics, search, Ask AI, GitHub and the theme menu; sidebar (topic grid on phones); breadcrumbs; footers; and the page scripts (flow diagrams, the Beginner / Architect toggle, Mermaid, the connector catalog filter). |
| `src/styles/theme.css` | The mockup tokens mapped onto Starlight's variables, plus every component style, light and dark. |

Generated folders (`src/content/docs`, `src/generated`, `public`) are git-ignored.
