# faucet-stream docs site (Astro Starlight)

The faucet-stream docs, built with [Astro Starlight](https://starlight.astro.build/) and published at <https://faucet-hq.github.io/faucet-stream/>. The design follows the approved mockups on the `designs` branch (`docs/`); see #1073.

## Edit a page

- Pages are Markdown in `src/content/docs/<path>.md`; the URL is `/faucet-stream/<path>/`. Each page starts with frontmatter (`title`, `description`); the title is the page's H1, so the body starts at `##`.
- Link to other pages by their site URL: `[State](/faucet-stream/cookbook/state/)`. `npm run check` fails on a link or `#anchor` that does not resolve.
- Notes use asides: `:::note`, `:::tip`, `:::caution`, `:::danger` (optionally `:::note[Title]`), closed by `:::`.
- A new page also needs a sidebar entry in `src/topics.json` (topic → group → `{ label, slug }`).
- The connector protocol page is built from `docs/spec/faucet-connector-spec-v0.md`; edit it there.
- Three reference pages are **generated** and must not be edited here: `generated/reference/capability-matrix.md` (`faucet conformance --matrix`), `compatibility.md` and `template-hub-matrix.md` (their `cli/tests` regenerate commands). `scripts/prebuild.mjs` wraps them into pages at build time.

## Run it

```bash
cd docs/site
npm ci
npm run dev       # http://localhost:4321/faucet-stream/
npm run build     # static site into dist/ (prebuild + astro build + postbuild)
npm run check     # every old mdBook .html URL redirects; every internal link and #anchor resolves
```

## Layout

| Path | What it is |
|---|---|
| `src/content/docs/` | Every page, including the docs home (`index.md`), the topic homes (`cookbook/index.md`, …) and the 404 page. |
| `src/topics.json` | The header topics (Learn · Cookbook · Reference · Operate · Extend · Compare) and each topic's sidebar groups. |
| `generated/reference/` | The generators' exact output; `cli/tests` compare against these files byte for byte. |
| `redirects.json` | Old mdBook `.html` URL → new URL. `scripts/postbuild.mjs` writes a redirect page for each one. |
| `scripts/` | `prebuild.mjs` (wraps the generated pages and the protocol spec), `postbuild.mjs` (redirects, a Markdown copy of every page for "Copy page as Markdown", `sitemap.xml`), `check-redirects.mjs`, `check-links.mjs`. |
| `src/components/` | Starlight overrides: header with topics, search, Ask AI, GitHub and the theme menu; sidebar (topic grid on phones); breadcrumbs; footers; and the page scripts (flow diagrams, the Beginner / Architect toggle, Mermaid, the connector catalog filter). |
| `src/styles/theme.css` | The mockup tokens mapped onto Starlight's variables, plus every component style, light and dark. |
| `public/assets/` | Images, fonts, favicons and the vendored `mermaid.min.js`. |

## Dependencies

- Mermaid loads from the vendored, MIT-licensed `public/assets/mermaid.min.js`, so the `mermaid` npm package and its GPL/EPL `elkjs` dependency stay out.
- `sharp` (an optional Astro dependency with LGPL libvips binaries) is replaced by the empty stub in `stubs/sharp`. The site uses Astro's passthrough image service, so sharp is never loaded.
