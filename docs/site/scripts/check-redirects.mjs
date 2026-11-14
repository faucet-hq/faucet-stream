// Fails when an old mdBook URL (redirects.json) has neither a built page nor a redirect, or a redirect points nowhere.
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dist = path.join(site, 'dist');
const redirects = JSON.parse(readFileSync(path.join(site, 'redirects.json'), 'utf8'));
const files = Object.keys(redirects).filter((f) => f !== 'print.html');
const missing = files.filter((f) => !existsSync(path.join(dist, f)));
const badTargets = Object.values(redirects).filter((to) => !existsSync(path.join(dist, to.replace(/^\/faucet-stream\//, ''), 'index.html')));
if (missing.length || badTargets.length) {
  missing.forEach((f) => console.error(`missing page or redirect: ${f}`));
  badTargets.forEach((t) => console.error(`redirect target not built: ${t}`));
  process.exit(1);
}
console.log(`check-redirects: all ${files.length} old URLs resolve`);
