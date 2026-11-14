# Docs site: functionality inventory

What the current mdBook docs site (`docs/book/`, published at faucet-hq.github.io/faucet-stream) can do. This lists **capabilities only**, not looks. The redesign must keep each one, or drop it for a stated reason. Status is filled in when the build is compared with the mocks: ✅ kept · ⚠️ changed · ❌ dropped (with reason) · 🆕 new.

## Site shell

| # | Capability | Today | In the mocks |
|---|---|---|---|
| S1 | Brand logo links to the docs home | Big hero logo on the landing page; the sidebar has no logo | ⚠️ Logo mark and wordmark sit in the header on every page |
| S2 | Full-text search over every page | mdBook search box, moved into the header as a "Search ⌘K" pill | ✅ Header pill, ⌘K / Ctrl-K, results in a modal (Pagefind) |
| S3 | Keyboard shortcut for search | ⌘K / Ctrl-K focuses the box | ✅ ⌘K / Ctrl-K opens the modal; `/` also opens it |
| S4 | Light / dark theme | One toggle (light ↔ navy), remembered | ⚠️ Menu with Auto / Light / Dark; Auto follows the OS (🆕 Auto) |
| S5 | "Ask AI" about the current page | Header menu: opens the page in Claude, ChatGPT, Perplexity, Grok or Gemini with a prompt that names the page URL and `llms.txt` | ✅ Same menu and providers, plus "Copy page as Markdown" 🆕 |
| S6 | Link to the GitHub repo | Header icon | ✅ Header icon |
| S7 | Link to LinkedIn | Header icon | ⚠️ Moves to the site footer |
| S8 | Print the page | Header print icon (mdBook print-all page) | ❌ The browser's own print stays, with print styles. The single "print the whole book" page is dropped; nobody prints 97 pages, and `llms-full.txt` covers "the whole docs in one file" |
| S9 | Edit this page on GitHub | Header pencil icon (`edit-url-template`) | ⚠️ "Edit this page" link in the page footer |
| S10 | Sidebar with every page | Numbered tree (1. … 15.6.), groups collapsible (`fold.level = 1`), sidebar resizable and hideable | ⚠️ Pages split into header topics (Learn · Cookbook · Reference · Operate · Extend · Compare); the sidebar shows one topic's groups, with no numbers. Groups collapse. The sidebar can no longer be resized or hidden on desktop |
| S11 | Current page highlighted in the sidebar | Yes | ✅ |
| S12 | Previous / next page | Big side arrows plus bottom arrows | ⚠️ Previous / next cards at the page bottom |
| S13 | "On this page" heading list | Right column built by `page-toc.js`, follows scroll, hidden on short pages and the landing page | ✅ Right column on wide screens; an "On this page" dropdown at the top below 1152 px |
| S14 | Phone layout | mdBook's hamburger sidebar | ✅ Hamburger opens a full-height menu with the topics and the sidebar |
| S15 | 404 page | mdBook's default 404 | ⚠️ Branded 404 with search and links to the main topics |
| S16 | Favicon | `favicon.svg` / `.png` | ✅ |
| S17 | Sitemap and stable base URL | `site-url = /faucet-stream/`, sitemap written in `docs.yml` | ✅ Same base path; Astro sitemap integration |
| S18 | `llms.txt` / `llms-full.txt` for AI tools | Generated in `docs.yml` | ✅ Kept (Ask AI prompt still names it) |
| S19 | Old deep links keep working | n/a (current URLs) | 🆕 Redirects from every mdBook `.html` path |

## Page content

| # | Capability | Today | In the mocks |
|---|---|---|---|
| C1 | Landing page: tagline, get-started buttons, install snippet, "why" points, map of the book, "where else to look" | `introduction.md` with hero HTML | ⚠️ Calm hero, task cards ("What do you want to do?"), install snippet, foundations list, "where else to look" |
| C2 | Live connector counts on the landing page and catalog (`<!--COUNT:…-->`, checked by `check-doc-counts.sh`) | Yes | ✅ Same markers in the new pages |
| C3 | Code blocks with syntax highlighting | highlight.js | ✅ Expressive Code: file names, highlighted lines, copy button with "Copied" state |
| C4 | Copy-code button | mdBook copy icon | ✅ |
| C5 | Hidden Rust lines in examples (`# ` prefix) | mdBook feature | ⚠️ Converted at migration time (the hidden lines are removed or shown) |
| C6 | `{{#include}}` of repo files | One page uses it | ⚠️ Replaced by an import of the file in MDX |
| C7 | Callouts ("> **Tip:**", plain blockquotes used as notes) | Blockquotes | ⚠️ Typed asides: note, tip, caution, danger |
| C8 | Tables, including very wide ones (capability matrix, connector catalog) | Plain tables, page scrolls sideways | ⚠️ Tables scroll inside their own box with a sticky first column and header |
| C9 | Mermaid diagrams, themed for light and dark | `mdbook-mermaid` + `mermaid-init.js` | ✅ Rendered in both themes |
| C10 | "Live" pipeline flow diagrams (`fs-flow`), matching the marketing site | `fs-flow.js` | ✅ Same component, restyled to the new tokens |
| C11 | Beginner / Architect reading-mode toggle on "Learn the architecture", remembered across pages | `learn-toggle.js` | ✅ Same toggle, as a segmented control |
| C12 | Benchmark bar charts and stat tiles (comparison pages) | Inline HTML + CSS | ✅ Restyled to the new tokens |
| C13 | Console screenshots | `assets/console/*.png` | ✅ Framed images in both themes |
| C14 | Footnote-style legends under tables (¹ ² …) | Plain text | ✅ |
| C15 | Generated reference pages (compatibility, capability matrix) | Generated Markdown checked in CI | ✅ Same generators, new path |

## New in the mocks (needs build work)

| # | What | Why |
|---|---|---|
| N1 | Header topics (Learn · Cookbook · Reference · Operate · Extend · Compare) | Makes a 97-page sidebar calm. Needs the community `starlight-sidebar-topics` plugin or a small custom sidebar component |
| N2 | Task cards on the landing page and on each topic's index | Gives a "pick your task" entry point |
| N3 | Connector catalog filter (text filter + Source / Sink / Tier chips) | The catalog has 78 rows; filtering beats scrolling. Pure client-side, no new API |
| N4 | Steps component in tutorials | Numbered steps read better than headings |
| N5 | Theme "Auto" | Follows the OS |
| N6 | "Copy page as Markdown" in the Ask AI menu | Same goal as Ask AI, for any other tool |
| N7 | "Last updated" date in the page footer | From git, built in to Starlight |
