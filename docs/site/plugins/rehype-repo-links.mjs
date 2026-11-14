// Links inline code that names a path in this repository (e.g. `cli/examples/x.yaml`) to it on GitHub.
// Only paths that exist at build time are linked; code already inside a link is left alone.
import { existsSync, statSync } from 'node:fs';
import path from 'node:path';

const PATH = /^(?:\.\/)?([A-Za-z0-9_.@-]+(?:\/[A-Za-z0-9_.@-]+)+)\/?$/;

export default function rehypeRepoLinks({ root, repo }) {
  const seen = new Map();
  const target = (text) => {
    const m = text.trim().match(PATH);
    if (!m) return null;
    const rel = m[1];
    if (rel.includes('..') || rel.startsWith('faucet-stream/')) return null;
    if (!seen.has(rel)) {
      const abs = path.join(root, rel);
      seen.set(rel, existsSync(abs) ? `${repo}/${statSync(abs).isDirectory() ? 'tree' : 'blob'}/main/${rel}` : null);
    }
    return seen.get(rel);
  };
  const walk = (node, inLink, inPre) => {
    if (!node.children) return;
    node.children = node.children.map((child) => {
      if (child.type !== 'element') return child;
      const link = inLink || child.tagName === 'a';
      const pre = inPre || child.tagName === 'pre';
      if (child.tagName === 'code' && !link && !pre) {
        const text = (child.children || []).map((c) => c.value || '').join('');
        const href = target(text);
        if (href) return { type: 'element', tagName: 'a', properties: { href, className: ['fs-repo-link'] }, children: [child] };
      }
      walk(child, link, pre);
      return child;
    });
  };
  return (tree) => walk(tree, false, false);
}
