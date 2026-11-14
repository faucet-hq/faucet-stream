// Fails when a built page links to a missing page or a missing #anchor inside the site.
import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dist = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../dist');
const BASE = '/faucet-stream/';
const pages = [];
(function walk(d) { for (const f of readdirSync(d)) { const p = path.join(d, f); if (statSync(p).isDirectory()) walk(p); else if (f === 'index.html') pages.push(p); } })(dist);
const ids = new Map();
const idsOf = (file) => {
  if (!ids.has(file)) ids.set(file, new Set([...readFileSync(file, 'utf8').matchAll(/\sid="([^"]+)"/g)].map((m) => m[1])));
  return ids.get(file);
};
const problems = [];
for (const page of pages) {
  const html = readFileSync(page, 'utf8');
  const main = html.slice(html.indexOf('<main'), html.lastIndexOf('</main>'));
  const from = '/' + path.relative(dist, path.dirname(page));
  for (const [, href] of main.matchAll(/<a [^>]*href="([^"]+)"/g)) {
    if (!href.startsWith(BASE) && !href.startsWith('#')) continue;
    const [p, hash] = href.split('#');
    const target = p ? path.join(dist, p.slice(BASE.length), p.endsWith('/') ? 'index.html' : '') : page;
    if (p && !p.endsWith('/') && !existsSync(path.join(dist, p.slice(BASE.length)))) { problems.push(`${from}: ${href} (no such file)`); continue; }
    if (p && p.endsWith('/') && !existsSync(target)) { problems.push(`${from}: ${href} (no such page)`); continue; }
    if (hash && target.endsWith('index.html') && !idsOf(target).has(decodeURIComponent(hash))) problems.push(`${from}: ${href} (no such anchor)`);
  }
}
if (problems.length) { problems.forEach((p) => console.error(p)); console.error(`check-links: ${problems.length} broken link(s)`); process.exit(1); }
console.log(`check-links: ${pages.length} pages, every internal link resolves`);
