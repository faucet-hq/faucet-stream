// After `astro build`: a redirect page for every old mdBook URL, a Markdown copy of every page, and sitemap.xml.
import { readFileSync, writeFileSync, mkdirSync, readdirSync, statSync, copyFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dist = path.join(site, 'dist');
const content = path.join(site, 'src/content/docs');

const redirects = JSON.parse(readFileSync(path.join(site, 'redirects.json'), 'utf8'));
for (const [from, to] of Object.entries(redirects)) {
  if (from === 'index.html' || from.endsWith('/index.html')) continue;
  const dest = path.join(dist, from);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, `<!doctype html><meta charset="utf-8"><title>Moved</title><link rel="canonical" href="${to}"><meta name="robots" content="noindex"><meta http-equiv="refresh" content="0; url=${to}"><script>location.replace(${JSON.stringify(to)} + location.hash)</script><a href="${to}">This page moved</a>\n`);
}

let pages = 0;
(function walk(dir) {
  for (const f of readdirSync(dir)) {
    const p = path.join(dir, f);
    if (statSync(p).isDirectory()) { walk(p); continue; }
    if (!/\.mdx?$/.test(f) || f === '404.md') continue;
    const rel = path.relative(content, p).replace(/\.mdx?$/, '').replace(/(^|\/)index$/, '');
    const md = readFileSync(p, 'utf8');
    const title = (md.match(/^title: (".*"|.*)$/m) || [])[1];
    const body = md.replace(/^---\n[\s\S]*?\n---\n/, '');
    const dest = path.join(dist, (rel || 'index') + '.md');
    mkdirSync(path.dirname(dest), { recursive: true });
    writeFileSync(dest, `# ${title ? (title.startsWith('"') ? JSON.parse(title) : title) : rel}\n\n${body.replace(/^\s+/, '')}`);
    pages++;
  }
})(content);

if (existsSync(path.join(dist, 'sitemap-index.xml'))) copyFileSync(path.join(dist, 'sitemap-index.xml'), path.join(dist, 'sitemap.xml'));
console.log(`postbuild: ${Object.keys(redirects).length} redirects, ${pages} Markdown copies, sitemap.xml`);
