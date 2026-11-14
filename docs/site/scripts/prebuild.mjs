// Wraps pages whose source lives elsewhere into Starlight pages (frontmatter from the H1 and first paragraph):
// the generators' exact output (generated/) and the connector protocol spec (docs/spec/).
import { readFileSync, writeFileSync, readdirSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const src = path.join(site, 'generated');
const out = path.join(site, 'src/content/docs');

export function wrap(md, editUrl) {
  const h1 = md.match(/^# (.+)$/m);
  if (!h1) throw new Error('generated page has no "# " title');
  const body = md.replace(h1[0] + '\n', '').replace(/^\s+/, '');
  const para = body.split(/\n\s*\n/).map((b) => b.trim()).find((b) => b && !/^(<|:::|```|[#|>!-]|\d+\.)/.test(b));
  let description = (para || '').replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1').replace(/[`*_]/g, '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
  if (description.length > 180) description = description.slice(0, 177).replace(/\s+\S*$/, '') + '…';
  const fm = [`title: ${JSON.stringify(h1[1].trim())}`, description && `description: ${JSON.stringify(description)}`, `editUrl: ${editUrl ? JSON.stringify(editUrl) : 'false'}`].filter(Boolean).join('\n');
  return `---\n${fm}\n---\n\n${body}`;
}

for (const dir of readdirSync(src)) {
  for (const f of readdirSync(path.join(src, dir)).filter((n) => n.endsWith('.md'))) {
    mkdirSync(path.join(out, dir), { recursive: true });
    writeFileSync(path.join(out, dir, f), wrap(readFileSync(path.join(src, dir, f), 'utf8')));
  }
}

const SPEC = 'docs/spec/faucet-connector-spec-v0.md';
mkdirSync(path.join(out, 'spec'), { recursive: true });
writeFileSync(
  path.join(out, 'spec/faucet-connector-spec-v0.md'),
  wrap(readFileSync(path.join(site, '../..', SPEC), 'utf8'), `https://github.com/faucet-hq/faucet-stream/edit/main/${SPEC}`),
);
