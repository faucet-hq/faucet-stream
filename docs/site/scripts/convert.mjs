// Converts the mdBook sources (docs/book/src) into Starlight content, the topic sidebar and old-URL redirects.
import { readFileSync, writeFileSync, mkdirSync, rmSync, cpSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TOPICS, GROUP_LEADS, BASE } from '../nav.mjs';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repo = path.resolve(site, '../..');
const book = path.join(repo, 'docs/book/src');
const out = path.join(site, 'src/content/docs');
const pub = path.join(site, 'public');
const gen = path.join(site, 'src/generated');
const authored = path.join(site, 'authored');
const GH = 'https://github.com/faucet-hq/faucet-stream/blob/main';
const warnings = [];
const warn = (m) => warnings.push(m);

// ---------- SUMMARY.md ----------
function parseSummary() {
  const lines = readFileSync(path.join(book, 'SUMMARY.md'), 'utf8').split('\n');
  const pages = [];
  let section = '';
  let group = null;
  for (const line of lines) {
    const part = line.match(/^# (.+)$/);
    if (part) { if (part[1] !== 'Summary') { section = part[1]; group = null; } continue; }
    const item = line.match(/^(\s*)(?:- )?\[([^\]]+)\]\(([^)]*)\)\s*$/);
    if (!item) continue;
    const depth = item[1].length / 2;
    const label = item[2].replace(/`/g, '');
    const file = item[3].replace(/^\.\//, '');
    if (!file) { group = label; continue; }
    if (depth === 0) group = null;
    pages.push({ section, group, label, file, slug: file.replace(/\.md$/, '') });
  }
  return pages;
}

// ---------- markdown transforms ----------
const slugOf = (file) => (file === 'introduction.md' ? '' : file.replace(/\.md$/, '').replace(/\/index$/, ''));
const urlOf = (slug) => `${BASE}/${slug ? slug + '/' : ''}`;

function rewriteLinks(md, file) {
  const dir = path.posix.dirname(file);
  const fix = (href) => {
    if (/^(https?:|mailto:|#|\/)/.test(href)) return href;
    const [p, hash = ''] = href.split('#');
    const target = path.posix.normalize(path.posix.join(dir, p));
    const anchor = hash ? '#' + hash : '';
    if (target.startsWith('../')) return `${GH}/${path.posix.normalize(path.posix.join('docs/book/src', target))}${anchor}`;
    if (p.endsWith('.md')) {
      if (!existsSync(path.join(book, target))) warn(`${file}: broken link ${href}`);
      return urlOf(slugOf(target)) + anchor;
    }
    if (target.startsWith('assets/')) return `${BASE}/${target}${anchor}`;
    return href;
  };
  return md
    .replace(/(\]\()([^)\s]+)((?:\s+"[^"]*")?\))/g, (_, a, h, b) => a + fix(h) + b)
    .replace(/(\b(?:href|src)=")([^"]+)(")/g, (_, a, h, b) => a + fix(h) + b);
}

function transformFences(md) {
  const lines = md.split('\n');
  const outL = [];
  let fence = null;
  let buf = [];
  for (const line of lines) {
    const m = line.match(/^(\s*)(```+|~~~+)(.*)$/);
    if (!fence && m) { fence = { indent: m[1], mark: m[2], info: m[3].trim() }; buf = []; continue; }
    if (fence && m && m[2].startsWith(fence.mark[0]) && m[2].length >= fence.mark.length && !m[3].trim()) {
      const lang = fence.info.split(/[\s,]/)[0];
      if (lang === 'mermaid') {
        outL.push(`<pre class="mermaid">${buf.join('\n').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</pre>`);
      } else {
        let body = buf;
        let info = fence.info;
        if (lang === 'rust') {
          body = buf.filter((l) => !/^\s*#(\s|$)/.test(l));
          info = 'rust';
        } else if (/^(text|console|plain|output)$/.test(lang) || /^(sh|shell|bash|zsh)$/.test(lang)) {
          info = lang === 'console' ? 'shell' : lang;
        }
        outL.push(`${fence.indent}${fence.mark}${info}`, ...body, `${fence.indent}${fence.mark}`);
      }
      fence = null;
      continue;
    }
    if (fence) buf.push(line); else outL.push(line);
  }
  if (fence) outL.push(`${fence.indent}${fence.mark}${fence.info}`, ...buf);
  return outL.join('\n');
}

const asideKind = (title) => {
  if (/^tip\b/i.test(title)) return 'tip';
  if (/security|warning|caveat|rejected|not dropped|danger|careful|breaking/i.test(title)) return 'caution';
  return 'note';
};
function transformBlockquotes(md) {
  const lines = md.split('\n');
  const outL = [];
  let inFence = false;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*(```|~~~)/.test(lines[i])) inFence = !inFence;
    if (inFence || !/^> ?/.test(lines[i])) { outL.push(lines[i]); continue; }
    const block = [];
    while (i < lines.length && /^>/.test(lines[i])) { block.push(lines[i].replace(/^> ?/, '')); i++; }
    i--;
    const first = block[0] || '';
    if (/^\s*["“]/.test(first)) { block.forEach((l) => outL.push('> ' + l)); continue; }
    const t = first.match(/^\*\*([^*]+?)[:.]?\*\*\s*(.*)$/);
    if (t) {
      const title = t[1].trim();
      const kind = asideKind(title);
      const plainTitle = /^(tip|note|warning|caution|important)$/i.test(title);
      outL.push(plainTitle ? `:::${kind}` : `:::${kind}[${title.replace(/[[\]]/g, '')}]`);
      if (t[2]) outL.push(t[2]);
      outL.push(...block.slice(1), ':::');
    } else {
      outL.push(':::note', ...block, ':::');
    }
  }
  return outL.join('\n');
}

function includes(md, file) {
  return md.replace(/\{\{#include ([^}]+)\}\}/g, (_, rel) => {
    const p = path.resolve(path.join(book, path.dirname(file)), rel.trim());
    if (!existsSync(p)) { warn(`${file}: include not found ${rel}`); return ''; }
    return readFileSync(p, 'utf8');
  });
}

function themeCss(md) {
  return md.replace(/html\.(navy|coal|ayu)\s+([^,{]+)/g, ':root[data-theme="dark"] $2').replace(/(:root\[data-theme="dark"\] [^,{]+)(,\s*\1)+/g, '$1');
}

function frontmatter(md, fallbackTitle) {
  let title = fallbackTitle;
  const h1 = md.match(/^# (.+)$/m);
  if (h1) { title = h1[1].trim(); md = md.replace(h1[0] + '\n', '').replace(h1[0], ''); }
  const para = md.split(/\n\s*\n/).map((b) => b.trim()).find((b) => b && !/^(<|:::|```|[#|>!-]|\d+\.)/.test(b));
  let description = (para || '').replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1').replace(/[`*_]/g, '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
  if (description.length > 180) description = description.slice(0, 177).replace(/\s+\S*$/, '') + '…';
  const q = (s) => JSON.stringify(s.replace(/`/g, ''));
  return `---\ntitle: ${q(title)}\n${description ? `description: ${q(description)}\n` : ''}---\n\n${md.replace(/^\s+/, '')}`;
}

// ---------- topics & sidebar ----------
function topicOf(page) {
  return TOPICS.find((t) => t.sections.includes(page.section));
}
function groupOf(t, page) {
  return (t.regroup && t.regroup[page.slug]) || (t.sectionGroup && t.sectionGroup[page.section]) || page.group || page.section || t.label;
}
function buildTopics(pages) {
  return TOPICS.map((t) => {
    const mine = pages.filter((p) => topicOf(p) === t);
    const order = [...(t.groups || [])];
    mine.forEach((p) => { const g = groupOf(t, p); if (!order.includes(g)) { order.push(g); if (t.groups) warn(`${p.file}: no group mapping in topic ${t.id}, placed in "${g}"`); } });
    const items = order.map((g) => ({
      label: g,
      items: mine.filter((p) => groupOf(t, p) === g).map((p) => ({ label: p.label, slug: slugOf(p.file) })),
    })).filter((g) => g.items.length);
    return { id: t.id, label: t.label, icon: t.icon, link: t.link, items };
  });
}

function topicIndex(t, topic) {
  const cards = topic.items.map((g) => {
    const first = g.items[0];
    return `<a class="fs-card fs-card-left" href="${urlOf(first.slug)}"><span class="fs-card-title">${g.label}</span><span class="fs-card-text">${GROUP_LEADS[g.label] || g.items.slice(0, 3).map((i) => i.label).join(', ')}</span><span class="fs-card-more">${g.items.length} ${g.items.length === 1 ? 'page' : 'pages'}</span></a>`;
  }).join('\n');
  const list = topic.items.flatMap((g) => g.items).map((i) => `<a href="${urlOf(i.slug)}"><b>${i.label}</b></a>`).join('\n');
  return `---\ntitle: ${JSON.stringify(t.label)}\ndescription: ${JSON.stringify(t.lead || '')}\neditUrl: false\n---\n\n<p class="fs-lead">${t.lead || ''}</p>\n\n<div class="fs-cards fs-cards-3">\n${cards}\n</div>\n\n## Every page\n\n<div class="fs-dlist">\n${list}\n</div>\n`;
}

// ---------- run ----------
const pages = parseSummary();
const rawPages = [];
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
for (const page of pages) {
  if (page.file === 'introduction.md') continue;
  let md = readFileSync(path.join(book, page.file), 'utf8');
  md = includes(md, page.file);
  md = transformFences(md);
  md = transformBlockquotes(md);
  md = rewriteLinks(md, page.file);
  md = themeCss(md);
  if (page.file === 'reference/connectors.md') md = md.replace(/^## Sources$/m, '<div class="fs-filter not-content" data-fs-catalog></div>\n\n## Sources');
  md = frontmatter(md, page.label);
  const dest = path.join(out, page.file);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, md);
  rawPages.push([slugOf(page.file), md]);
}
const topics = buildTopics(pages);
for (const t of TOPICS) {
  const dest = path.join(out, t.link.replace(/^\//, ''), 'index.md');
  if (t.link === '/' ) continue;
  if (existsSync(dest)) continue;
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, topicIndex(t, topics.find((x) => x.id === t.id)));
}
cpSync(authored, out, { recursive: true });
mkdirSync(gen, { recursive: true });
writeFileSync(path.join(gen, 'topics.json'), JSON.stringify(topics, null, 2) + '\n');

// assets + old-URL redirects
rmSync(path.join(pub, 'assets'), { recursive: true, force: true });
cpSync(path.join(book, 'assets'), path.join(pub, 'assets'), { recursive: true });
const redirects = {};
for (const page of pages) redirects[page.file.replace(/\.md$/, '.html')] = urlOf(slugOf(page.file));
redirects['print.html'] = urlOf('');
for (const [from, to] of Object.entries(redirects)) {
  if (from === 'index.html' || from.endsWith('/index.html')) continue;
  const dest = path.join(pub, from);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, `<!doctype html><meta charset="utf-8"><title>Moved</title><link rel="canonical" href="${to}"><meta name="robots" content="noindex"><meta http-equiv="refresh" content="0; url=${to}"><script>location.replace(${JSON.stringify(to)} + location.hash)</script><a href="${to}">This page moved</a>\n`);
}
for (const [slug, md] of rawPages) {
  const dest = path.join(pub, (slug || 'index') + '.md');
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, md.replace(/^---\ntitle: ("[^\n]*")\n(?:description: [^\n]*\n)?---\n/, (_, t) => `# ${JSON.parse(t)}\n`));
}
writeFileSync(path.join(pub, 'index.md'), readFileSync(path.join(authored, 'index.md'), 'utf8').replace(/^---[\s\S]*?---\n/, '# faucet-stream documentation\n').replace(/<[^>]+>/g, ''));
writeFileSync(path.join(gen, 'redirects.json'), JSON.stringify(redirects, null, 2) + '\n');

console.log(`converted ${pages.length} pages into ${topics.length} topics, ${Object.keys(redirects).length} redirects`);
if (warnings.length) { console.log(`${warnings.length} warning(s):`); warnings.forEach((w) => console.log('  ' + w)); }
