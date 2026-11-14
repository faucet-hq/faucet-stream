/* faucet-stream docs redesign mock: shell, icons, nav, helpers, view registry. */
(function () {
  const P = {
    arrow: '<path d="M5 12h14"/><path d="m12 5 7 7-7 7"/>',
    arrowL: '<path d="M19 12H5"/><path d="m12 19-7-7 7-7"/>',
    search: '<circle cx="11" cy="11" r="7.5"/><path d="m20.5 20.5-4.2-4.2"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
    moon: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    monitor: '<rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8M12 17v4"/>',
    menu: '<path d="M4 7h16M4 12h16M4 17h16"/>',
    x: '<path d="M18 6 6 18M6 6l12 12"/>',
    down: '<path d="m6 9 6 6 6-6"/>',
    right: '<path d="m9 18 6-6-6-6"/>',
    left: '<path d="m15 18-6-6 6-6"/>',
    copy: '<rect width="13" height="13" x="9" y="9" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>',
    check: '<path d="M20 6 9 17l-5-5"/>',
    sparkles: '<path d="M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9z"/><path d="M19 15l.7 1.8 1.8.7-1.8.7L19 20l-.7-1.8-1.8-.7 1.8-.7z"/>',
    github: '<path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.4 5.4 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/>',
    linkedin: '<path d="M16 8a6 6 0 0 1 6 6v7h-4v-7a2 2 0 0 0-4 0v7h-4v-7a6 6 0 0 1 6-6z"/><rect width="4" height="12" x="2" y="9"/><circle cx="4" cy="4" r="2"/>',
    zap: '<path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/>',
    sync: '<path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15 6.7L3 16"/><path d="M8 16H3v5"/>',
    server: '<rect width="20" height="8" x="2" y="2" rx="2"/><rect width="20" height="8" x="2" y="14" rx="2"/><path d="M6 6h.01M6 18h.01"/>',
    plug: '<path d="M12 22v-5M9 8V2M15 8V2M18 8v5a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4V8Z"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>',
    bulb: '<path d="M15 14c.2-1 .7-1.7 1.5-2.5 1-.9 1.5-2.2 1.5-3.5A6 6 0 0 0 6 8c0 1 .2 2.2 1.5 3.5.7.7 1.3 1.5 1.5 2.5"/><path d="M9 18h6M10 22h4"/>',
    alert: '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4M12 17h.01"/>',
    octagon: '<path d="M7.86 2h8.28L22 7.86v8.28L16.14 22H7.86L2 16.14V7.86L7.86 2z"/><path d="M12 8v4M12 16h.01"/>',
    ext: '<path d="M15 3h6v6M10 14 21 3M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
    pencil: '<path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/>',
    terminal: '<path d="m4 17 6-6-6-6M12 19h8"/>',
    shield: '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>',
    db: '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>',
    layers: '<path d="m12 2 10 5-10 5L2 7z"/><path d="m2 17 10 5 10-5M2 12l10 5 10-5"/>',
    file: '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/>',
    folder: '<path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z"/>',
    wand: '<path d="m21.64 3.64-1.28-1.28a1.21 1.21 0 0 0-1.72 0L2.36 18.64a1.21 1.21 0 0 0 0 1.72l1.28 1.28a1.2 1.2 0 0 0 1.72 0L21.64 5.36a1.2 1.2 0 0 0 0-1.72"/><path d="m14 7 3 3M5 6v4M19 14v4M10 2v2M7 8H3M21 16h-4M11 3H9"/>',
    gauge: '<path d="m12 14 4-4"/><path d="M3.34 19a10 10 0 1 1 17.32 0"/>',
    book: '<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>',
    clock: '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    enter: '<path d="M20 4v7a4 4 0 0 1-4 4H4"/><path d="m9 10-5 5 5 5"/>',
    hash: '<path d="M4 9h16M4 15h16M10 3 8 21M16 3l-2 18"/>',
    merge: '<circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M6 21V9a9 9 0 0 0 9 9"/>',
    inbox: '<path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
    pkg: '<path d="m7.5 4.27 9 5.15"/><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5M12 22V12"/>',
    scale: '<path d="m16 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="m2 16 3-8 3 8c-.87.65-1.92 1-3 1s-2.13-.35-3-1Z"/><path d="M7 21h10M12 3v18M3 7h2c2 0 5-1 7-2 2 1 5 2 7 2h2"/>',
    cap: '<path d="M22 10 12 5 2 10l10 5 10-5Z"/><path d="M6 12v5c3 3 9 3 12 0v-5"/>',
    columns: '<rect width="18" height="18" x="3" y="3" rx="2"/><path d="M12 3v18"/>',
    mdcopy: '<rect width="8" height="4" x="8" y="2" rx="1"/><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><path d="M9 14l2 2 4-4"/>',
    filter: '<path d="M22 3H2l8 9.46V19l4 2v-8.54z"/>',
  };
  const icon = (n, cls = '') => `<svg class="i ${cls}" viewBox="0 0 24 24" aria-hidden="true">${P[n] || ''}</svg>`;
  const LOGO = `<svg viewBox="6 11 112 112" aria-hidden="true"><g fill="none" stroke="var(--mark)" stroke-width="13" stroke-linecap="round" stroke-linejoin="round"><path d="M55 40 V24"/><path d="M43 24 H67"/><path d="M32 40 H78 a15 15 0 0 1 15 15 V66"/></g><g fill="var(--mark)"><circle cx="93" cy="82" r="6"/><circle cx="93" cy="99" r="5" opacity=".7"/><circle cx="93" cy="112" r="4" opacity=".45"/></g></svg>`;
  const brand = () => `<a class="brand" href="#">${LOGO}<span class="wm"><b>faucet</b><span>stream</span></span></a>`;

  const TOPICS = [
    ['learn', 'Learn', 'cap'], ['cookbook', 'Cookbook', 'book'], ['reference', 'Reference', 'hash'],
    ['operate', 'Operate', 'server'], ['extend', 'Extend', 'plug'], ['compare', 'Compare', 'scale'],
  ];
  const NAV = {
    learn: [
      ['Start here', ['Introduction', 'Installation', 'Your first pipeline', 'Core concepts', 'Try it locally', 'Agent skills']],
      ['Understand', ['Learn the architecture']],
      ['Tutorials', ['REST API → BigQuery', 'PostgreSQL CDC → JSONL', 'Multi-pipeline DAGs', 'Embedding as a Rust library']],
    ],
    cookbook: [
      ['Shaping the data', ['Record transforms', 'SQL transform', 'WASM transform', 'Topology (tee / merge / join)']],
      ['Reading from sources', ['Pagination styles', 'Authentication', 'Source discovery', 'Bearer-token REST API']],
      ['Moving data reliably', ['Incremental replication & state', 'State & status', 'Upsert / mirror tables', 'Mirror (snapshot → CDC)', 'Backfill', 'Parallel range partitioning', 'Dead-letter queues', 'Content verification', 'Undoing a run', 'Resilience', 'File formats', 'Compression']],
      ['Data quality & governance', ['Data-quality checks', 'Data contracts', 'PII detection & masking', 'Data-flow policies', 'Schema drift']],
      ['Config & reuse', ['Config composition', 'Parameters & templates', 'Template Hub', 'Secrets interpolation', 'Adaptive batching', 'Throughput tuning', 'Testing pipelines']],
      ['Observability & lineage', ['SLA monitoring', 'Column profiling', 'Notifications', 'Lineage (OpenLineage)', 'Dashboards & alerts', 'Data Movement Catalog', 'Change impact analysis', 'Cost & usage', 'Change approvals', 'Embedded integrations']],
    ],
    reference: [
      ['Connectors', ['Connector catalog', 'Choosing a connector', 'Conformance & tiers', 'Capability matrix', 'Feature compatibility', 'Value fidelity', 'Template Hub matrix']],
      ['CLI & config', ['CLI commands', 'Configuration file', 'Editor setup', 'HTTP API (faucet serve)', 'Triggers']],
    ],
    operate: [
      ['Deploy & run', ['Deploying faucet', 'Running as a service', 'Web console', 'MCP server', 'Running a cluster']],
      ['Schedule & trigger', ['Scheduling', 'Schedules', 'Event-driven triggers', 'Orchestration (Airflow / Dagster)']],
      ['Keep it healthy', ['Observability', 'Reliability testing', 'Chaos testing', 'Performance tuning', 'faucet doctor', 'Troubleshooting & FAQ']],
      ['Upgrade & maintain', ['Pinning the version', 'Upgrading safely', 'Migrating from Meltano / Airbyte', 'Backing up the server DB']],
    ],
    extend: [
      ['Build on faucet', ['Authoring a connector', 'Connector protocol (FCP v0)', 'Connector marketplace', 'Embedding faucet']],
    ],
    compare: [
      ['Comparisons', ['How faucet compares', 'Benchmarks', 'vs. Meltano', 'vs. Airbyte', 'vs. Singer', 'vs. Redpanda Connect', 'vs. Vector', 'vs. Fivetran']],
    ],
  };

  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  function hlVal(v) {
    let com = '';
    const ci = v.search(/\s#\s/);
    if (ci >= 0) { com = `<span class="c">${v.slice(ci)}</span>`; v = v.slice(0, ci); }
    v = v.replace(/("[^"]*"|'[^']*')/g, '<span class="s">$1</span>')
      .replace(/(^|\s)(-?\d[\d_.]*|true|false|null)(?=\s*$)/g, '$1<span class="n">$2</span>');
    return v + com;
  }
  function yaml(src) {
    return src.split('\n').map((raw) => {
      const l = esc(raw);
      if (/^\s*#/.test(l)) return `<span class="c">${l}</span>`;
      const m = l.match(/^(\s*-?\s*)([\w.$-]+)(:)(.*)$/);
      if (m) return `${m[1]}<span class="k">${m[2]}</span>${m[3]}${hlVal(m[4])}`;
      const li = l.match(/^(\s*-\s+)(.*)$/);
      if (li) return li[1] + hlVal(li[2]);
      return l;
    });
  }
  function sh(src) {
    return src.split('\n').map((raw) => {
      const l = esc(raw);
      if (/^\s*#/.test(l)) return `<span class="c">${l}</span>`;
      const m = l.match(/^\$ (\S+)(.*)$/);
      if (m) return `<span class="p">$ </span><span class="cmd">${m[1]}</span>${m[2].replace(/(\s)(--?[\w-]+)/g, '$1<span class="k">$2</span>')}`;
      return `<span style="color:var(--muted)">${l}</span>`;
    });
  }
  function code({ file, lang = 'yaml', src, hl = [], add = [], copied = false, term = false, tabs = null, tab = 0 }) {
    const lines = lang === 'sh' ? sh(src) : lang === 'raw' ? src.split('\n').map(esc) : yaml(src);
    const body = lines.map((l, i) => `<span class="ln${hl.includes(i + 1) ? ' hl' : ''}${add.includes(i + 1) ? ' add' : ''}">${l || ' '}</span>`).join('');
    const cp = `<button class="copy${copied ? ' done' : ''}">${copied ? icon('check') + 'Copied' : icon('copy') + 'Copy'}</button>`;
    let head;
    if (tabs) head = `<div class="tabs">${tabs.map((t, i) => `<button class="${i === tab ? 'on' : ''}">${t}</button>`).join('')}${cp}</div>`;
    else if (term) head = `<div class="code-h term"><span class="dot"></span><span class="dot"></span><span class="dot"></span><span style="margin-left:8px">${file || 'Terminal'}</span>${cp}</div>`;
    else if (file) head = `<div class="code-h">${icon('file')}<span class="fname">${file}</span>${cp}</div>`;
    else head = '';
    return `<div class="code">${head}<pre>${body}</pre></div>`;
  }
  const ASIDE_IC = { note: 'info', tip: 'bulb', caution: 'alert', danger: 'octagon' };
  const ASIDE_T = { note: 'Note', tip: 'Tip', caution: 'Caution', danger: 'Danger' };
  const aside = (kind, html, title) => `<div class="aside ${kind}">${icon(ASIDE_IC[kind])}<div><b class="t">${title || ASIDE_T[kind]}</b><p>${html}</p></div></div>`;
  const flow = (nodes) => `<div class="flow">${nodes.map((n, i) => {
    const [a, b, alt] = Array.isArray(n) ? n : [n];
    return `${i ? '<span class="wire"></span>' : ''}<span class="node${alt ? ' alt' : ''}">${a}${b ? `<small>${b}</small>` : ''}</span>`;
  }).join('')}</div>`;
  const card = (ic, title, text, opts = {}) => `<a class="card${opts.left ? ' left' : ''}" href="#"><span class="ic">${icon(ic)}</span><div><h3>${title}${opts.left ? '' : icon('arrow')}</h3><p>${text}</p>${opts.more ? `<div class="more">${opts.more}</div>` : ''}</div><span class="go">${icon('right')}</span></a>`;
  const crumbs = (items) => `<div class="crumbs">${items.map((c, i) => (i < items.length - 1 ? `<a href="#">${c}</a><span class="sep">/</span>` : `<span>${c}</span>`)).join('')}</div>`;
  const pn = (prev, next) => `<div class="pn-row">${prev ? `<a class="pn" href="#"><small>${icon('left')}Previous</small><b>${prev}</b></a>` : '<span></span>'}${next ? `<a class="pn next" href="#"><small>Next${icon('right')}</small><b>${next}</b></a>` : ''}</div>`;
  const pfoot = (updated = 'Nov 12, 2026') => `<div class="pfoot"><a href="#">${icon('pencil')}Edit this page</a><span>Last updated: ${updated}</span></div>`;

  function sidebar(topic, active, closed = []) {
    return NAV[topic].map(([g, items]) => `<div class="nav-g${closed.includes(g) ? ' closed' : ''}"><button class="nav-gh">${icon('down')}${g}</button><div class="nav-items">${items.map((it) => `<a class="nav-a${it === active ? ' on' : ''}" href="#">${it}</a>`).join('')}</div></div>`).join('');
  }
  function toc(items, on) {
    if (!items || !items.length) return '';
    return `<aside class="toc"><div class="toc-h">On this page</div>${items.map(([t, l]) => `<a href="#" class="${l === 3 ? 'l3' : ''}${t === on ? ' on' : ''}">${t}</a>`).join('')}<div class="toc-foot"><a href="#">${icon('pencil')}Edit this page</a><a href="#">${icon('github')}View source</a></div></aside>`;
  }
  function tocDropdown(items, on, open) {
    if (!items || !items.length) return '';
    const cur = on || items[0][0];
    return `<div class="toc-dd"><span class="lbl">On this page</span><b>${cur}</b>${icon(open ? 'x' : 'down')}${open ? `<div class="toc-dd-pop">${[['Overview', 2], ...items].map(([t, l]) => `<a href="#" class="${l === 3 ? 'l3' : ''}${t === cur ? ' on' : ''}">${t}</a>`).join('')}</div>` : ''}</div>`;
  }

  function askAiMenu() {
    const pv = [['Claude', '#c96442', 'C'], ['ChatGPT', '#10a37f', 'G'], ['Perplexity', '#1f6f78', 'P'], ['Grok', '#111827', 'X'], ['Gemini', '#3b6fe0', 'G']];
    return `<div class="pop" style="width:300px"><div class="pop-h">Ask about this page</div><div style="padding:0 10px 8px;font-size:12.5px;color:var(--muted);line-height:1.4">Opens a chat that already links this page and llms.txt.</div>${pv.map(([n, c, l], i) => `<div class="pop-item${i === 0 ? ' hl' : ''}"><span class="prov" style="background:${c}">${l}</span><span>Open in ${n}</span>${icon('ext', 'ext')}</div>`).join('')}<div class="pop-sep"></div><div class="pop-item">${icon('mdcopy')}<span>Copy page as Markdown<span class="sub">Paste it into any AI tool</span></span></div><div class="pop-item">${icon('file')}<span>Open llms.txt</span>${icon('ext', 'ext')}</div></div>`;
  }
  function themeMenu(cur) {
    return `<div class="pop" style="min-width:180px">${[['auto', 'Auto', 'monitor'], ['light', 'Light', 'sun'], ['dark', 'Dark', 'moon']].map(([k, l, ic]) => `<div class="pop-item${k === cur ? ' hl' : ''}">${icon(ic)}<span>${l}</span>${k === cur ? icon('check', 'tick') : ''}</div>`).join('')}</div>`;
  }

  function header(o) {
    const theme = document.documentElement.dataset.theme;
    const topics = TOPICS.map(([k, l]) => `<a href="#" class="${k === o.topic ? 'on' : ''}">${l}</a>`).join('');
    return `<header class="hdr"><div class="hdr-in">${brand()}<nav class="topics">${topics}</nav>
      <div class="hdr-r">
        <div class="search-pill${o.searchFocus ? ' focus' : ''}">${icon('search')}<span>Search docs</span><kbd>⌘ K</kbd></div>
        <div class="pop-anchor askai"><button class="icon-btn${o.askOpen ? ' open' : ''}">${icon('sparkles')}<span class="lbl">Ask AI</span>${icon('down', 'chev')}</button>${o.askOpen ? askAiMenu() : ''}</div>
        <a class="icon-btn gh" href="#" title="GitHub">${icon('github')}</a>
        <div class="pop-anchor theme-btn"><button class="icon-btn${o.themeOpen ? ' open' : ''}" title="Theme">${icon(o.themeChoice === 'auto' ? 'monitor' : theme === 'dark' ? 'moon' : 'sun')}</button>${o.themeOpen ? themeMenu(o.themeChoice || theme) : ''}</div>
      </div>
      <div class="hdr-mobile"><button class="icon-btn">${icon('search')}</button><button class="icon-btn${o.mobileMenu ? ' open' : ''}">${icon(o.mobileMenu ? 'x' : 'menu')}</button></div>
    </div></header>`;
  }
  function mobileMenu(o) {
    return `<div class="mmenu"><div class="topics-m">${TOPICS.map(([k, l, ic]) => `<a href="#" class="${k === o.topic ? 'on' : ''}">${icon(ic)}${l}</a>`).join('')}</div>${sidebar(o.topic, o.active)}<div class="mm-foot"><a class="btn btn-ghost btn-sm" href="#">${icon('sparkles')}Ask AI</a><a class="btn btn-ghost btn-sm" href="#">${icon('github')}GitHub</a><a class="btn btn-ghost btn-sm" href="#">${icon('sun')}Theme</a></div></div>`;
  }
  const siteFooter = () => `<footer class="sfoot"><div class="sfoot-in">${brand()}<span>Apache-2.0 · Move data, reliably.</span><nav><a href="#">${icon('github')}GitHub</a><a href="#">${icon('linkedin')}LinkedIn</a><a href="#">docs.rs</a><a href="#">Template Hub</a><a href="#">Connector Hub</a></nav></div></footer>`;

  function page(o) {
    const layout = o.layout || 'doc';
    const cls = ['page', layout === 'splash' ? 'splash' : '', !o.toc || !o.toc.length ? 'no-toc' : '', o.wide ? 'wide' : ''].join(' ');
    const side = layout === 'splash' ? '' : `<nav class="side">${sidebar(o.topic, o.active, o.closed)}</nav>`;
    const main = `<main class="main">${o.toc && o.toc.length ? tocDropdown(o.toc, o.tocOn, o.tocOpen) : ''}<div class="main-in prose">${o.body}</div></main>`;
    return header(o) + `<div class="${cls}">${side}${main}${layout === 'splash' ? '' : toc(o.toc, o.tocOn)}</div>` + siteFooter() + (o.mobileMenu ? mobileMenu(o) : '') + (o.overlay || '');
  }

  const VIEWS = {};
  function view(name, def) { VIEWS[name] = { name, ...def }; }

  function boot() {
    const q = new URLSearchParams(location.search);
    const theme = q.get('theme') || 'light';
    document.documentElement.dataset.theme = theme;
    const v = VIEWS[q.get('view') || 'landing'];
    const stKey = q.get('state') || Object.keys(v.states)[0];
    const st = v.states[stKey];
    document.title = `${v.title} · ${stKey} — faucet docs mock`;
    document.body.innerHTML = page(st.render());
    if (st.after) requestAnimationFrame(() => st.after());
  }

  window.M = { icon, LOGO, brand, TOPICS, NAV, code, aside, flow, card, crumbs, pn, pfoot, page, view, VIEWS, boot, esc, askAiMenu, themeMenu };
})();
