// faucet console mockups (#998): the component helpers (one per design-system
// component, mirroring cli/src/serve/ui/components.js), the shell, and the page
// registry. Pages live in views/<page>.js and register with M.view().
// URL: index.html?view=<page>&state=<state>&theme=light|dark[&role=viewer][&scope=tenant][&overlay=palette|toast]
(function () {
  const P = {
    home: '<path d="M3 11l9-7 9 7v9a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1z"/>',
    runs: '<path d="M5 4l14 8-14 8z"/>',
    changes: '<path d="M6 3v12"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="6" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/>',
    trig: '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>',
    sched: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    db: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
    lineage: '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.5 6h3a4 4 0 0 1 4 4v0M7.5 18h3a4 4 0 0 0 4-4"/>',
    tpl: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><path d="M17.5 14v7M14 17.5h7"/>',
    schema: '<path d="M8 3H6a2 2 0 0 0-2 2v4l-2 3 2 3v4a2 2 0 0 0 2 2h2M16 3h2a2 2 0 0 1 2 2v4l2 3-2 3v4a2 2 0 0 1-2 2h-2"/>',
    submit: '<path d="M12 19V5M5 12l7-7 7 7"/>',
    policy: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/>',
    usage: '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
    tenants: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20a6.5 6.5 0 0 1 13 0"/><path d="M16 4.5a3.5 3.5 0 0 1 0 7M21.5 20a6.5 6.5 0 0 0-4-6"/>',
    audit: '<path d="M9 4h10v16H5V8z"/><path d="M9 4v4H5M9 13h6M9 17h6"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 0 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 0 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 0 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 0 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/>',
    search: '<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>',
    filter: '<path d="M3 5h18M6 12h12M10 19h4"/>',
    sliders: '<path d="M4 6h10M18 6h2M4 18h4M12 18h8M14 4v4M8 16v4"/>',
    link: '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
    expand: '<path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/>',
    chev: '<path d="M6 9l6 6 6-6"/>',
    right: '<path d="M9 6l6 6-6 6"/>',
    back: '<path d="M15 6l-6 6 6 6"/>',
    updown: '<path d="M8 9l4-4 4 4M8 15l4 4 4-4"/>',
    bell: '<path d="M6 8a6 6 0 1 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.9 1.9 0 0 0 3.4 0"/>',
    logs: '<path d="M4 6h16M4 12h16M4 18h10"/>',
    check: '<path d="M20 6L9 17l-5-5"/>',
    x: '<path d="M18 6L6 18M6 6l12 12"/>',
    play: '<path d="M6 4l14 8-14 8z"/>',
    pause: '<path d="M8 5v14M16 5v14"/>',
    undo: '<path d="M9 14L4 9l5-5"/><path d="M4 9h11a5 5 0 0 1 0 10h-3"/>',
    flask: '<path d="M9 3h6M10 3v6L4 19a1.5 1.5 0 0 0 1.3 2h13.4A1.5 1.5 0 0 0 20 19l-6-10V3"/>',
    rocket: '<path d="M12 15l-3-3a22 22 0 0 1 2-4A12.9 12.9 0 0 1 22 2c0 2.7-.8 7.5-6 11a22.4 22.4 0 0 1-4 2z"/><path d="M9 12H5s.6-3 2-4c1.6-1.1 4 0 4 0"/><path d="M12 15v4s3-.6 4-2c1.1-1.6 0-4 0-4"/><circle cx="15.5" cy="8.5" r="1.5"/><path d="M7 17l-3 3M5 14.5l-1.5 1.5M9.5 19.5L8 21"/>',
    copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>',
    refresh: '<path d="M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    trash: '<path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"/>',
    more: '<circle cx="5" cy="12" r="1.2"/><circle cx="12" cy="12" r="1.2"/><circle cx="19" cy="12" r="1.2"/>',
    matrix: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18M15 3v18"/>',
    sync: '<path d="M4 12a8 8 0 0 1 13.7-5.7M20 12a8 8 0 0 1-13.7 5.7"/><path d="M18 3v4h-4M6 21v-4h4"/>',
    info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7.5v.5"/>',
    alert: '<path d="M12 3l10 18H2z"/><path d="M12 10v5M12 18v.5"/>',
    diff: '<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M12 7v6M9 10h6M9 17h6"/>',
    code: '<path d="M8 6l-6 6 6 6M16 6l6 6-6 6"/>',
    zoomin: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M8 11h6M11 8v6"/>',
    zoomout: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4M8 11h6"/>',
    fit: '<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
    focus: '<circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/>',
    columns: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16M15 4v16"/>',
    download: '<path d="M12 4v11M7 10l5 5 5-5M5 20h14"/>',
    edit: '<path d="M4 20h4L19 9l-4-4L4 16z"/>',
    key: '<circle cx="7.5" cy="15.5" r="4.5"/><path d="M10.7 12.3L21 2M17 6l3 3M14 9l2 2"/>',
    theme: '<circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor"/>',
    server: '<rect x="3" y="4" width="18" height="7" rx="1.5"/><rect x="3" y="13" width="18" height="7" rx="1.5"/><path d="M7 7.5h.01M7 16.5h.01"/>',
    shield: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/>',
    user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
    layers: '<path d="M12 3l9 5-9 5-9-5z"/><path d="M3 13l9 5 9-5"/>',
    clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  };
  const ic = (n, cls = 'i') => (P[n] ? `<svg class="${cls}" viewBox="0 0 24 24">${P[n]}</svg>` : '');

  // ---- components (same names and anatomy as the build's components.js) ----
  const H = {
    ic,
    ring: (tone, shape = '') => `<span class="ring ${shape} ${tone}"></span>`,
    row: (lead, title, age, meta, opts = {}) => `<div class="row ${opts.on ? 'on' : ''} ${opts.kb ? 'kb' : ''} ${opts.hov ? 'hov' : ''}">${lead}<span class="t ${opts.mono ? 'mono' : ''}">${title}</span><span class="age">${age}</span><span class="meta">${meta}</span></div>`,
    sect: (label, count, closed) => `<div class="sect ${closed ? 'closed' : ''}">${ic('chev')}${label}<span class="c">${count}</span></div>`,
    stats: (items, on) => `<div class="stats" style="--n:${items.length}">${items.map(([k, v, cls], i) => `<div class="${on === i ? 'on' : ''}"><small>${k}</small><b class="${cls || ''}">${v}</b></div>`).join('')}</div>`,
    tabs: (items, on, tools = '') => `<div class="tabs">${items.map((t) => { const [l, n] = Array.isArray(t) ? t : [t]; return `<span class="tab ${l === on ? 'on' : ''}">${l}${n != null ? `<span class="n">${n}</span>` : ''}</span>`; }).join('')}${tools ? `<span class="tools">${tools}</span>` : ''}</div>`,
    kv: (k, v, s) => `<div class="kv"><small>${k}</small><b>${v}</b>${s ? `<div class="s">${s}</div>` : ''}</div>`,
    grid: (n, items) => `<div class="grid${n}">${items.join('')}</div>`,
    card: ({ icon, title, tag, aside, right, body = '', note, cls = '' }) => `<div class="card ${cls}">${title || icon ? `<div class="h">${icon ? ic(icon) : ''}${title ? `<b>${title}</b>` : ''}${tag ? `<span class="tag">${tag}</span>` : ''}${aside ? `<span class="muted">${aside}</span>` : ''}${right ? `<span class="r">${right}</span>` : ''}</div>` : ''}${body}${note ? `<div class="note">${note}</div>` : ''}</div>`,
    tbl: (cols, rows, foot) => `<table><tr>${cols.map((c) => { const [l, cl] = Array.isArray(c) ? c : [c]; return `<th class="${cl || ''}">${l}</th>`; }).join('')}</tr>${rows.map((r) => { const cells = r.cells || r; return `<tr class="${r.cls || ''}">${cells.map((c, i) => { const cl = Array.isArray(cols[i]) ? cols[i][1] : ''; return `<td class="${cl || ''}">${c}</td>`; }).join('')}</tr>`; }).join('')}${foot ? `<tfoot><tr>${foot.map((c, i) => `<td class="${Array.isArray(cols[i]) ? cols[i][1] : ''}">${c}</td>`).join('')}</tr></tfoot>` : ''}</table>`,
    pill: (text, tone = '', icon = '') => `<span class="pill ${tone}">${icon ? ic(icon) : ''}${text}</span>`,
    chip: (text, cls = '') => `<span class="chip ${cls}">${text}</span>`,
    ch: (text, stable) => `<span class="ch ${stable ? 'stable' : ''}">${text}</span>`,
    tag: (text, tone = '') => `<span class="tag ${tone}">${text}</span>`,
    btn: (label, { kind = '', icon = '', act = false, cls = '' } = {}) => `<span class="btn ${kind} ${act ? 'act' : ''} ${cls}">${icon ? ic(icon) : ''}${label}</span>`,
    ibtn: (icon, { on = false, dot = false, act = false } = {}) => `<span class="ibtn ${on ? 'on' : ''} ${act ? 'act' : ''}">${ic(icon)}${dot ? '<span class="bd"></span>' : ''}</span>`,
    input: (text, { ph = false, icon = '', cls = '', mono = false } = {}) => `<div class="input ${ph ? 'ph' : ''} ${mono ? 'mono' : ''} ${cls}">${icon ? ic(icon) : ''}${text}</div>`,
    select: (text, cls = '') => `<div class="select ${cls}">${text}</div>`,
    check: (label, on) => `<span class="check ${on ? 'on' : ''}"><i></i>${label}</span>`,
    toggle: (label, on) => `<span class="toggle ${on ? 'on' : ''}"><i></i>${label}</span>`,
    field: (label, control, { help = '', req = false, code = false, cls = '', err = '' } = {}) => `<div class="field ${cls}"><label>${code ? `<code>${label}</code>` : label}${req ? '<span class="req"></span>' : ''}</label>${control}${err ? `<div class="help bad">${err}</div>` : help ? `<div class="help">${help}</div>` : ''}</div>`,
    alert: (title, text = '', tone = '', btns = '') => `<div class="alert ${tone}">${ic(tone === 'ok' ? 'check' : tone ? 'alert' : 'info')}<div><b>${title}</b>${text ? ` ${text}` : ''}${btns ? `<div class="btns">${btns}</div>` : ''}</div></div>`,
    empty: (title, text = '', { icon = '', btns = '' } = {}) => `<div class="empty">${icon ? ic(icon) : ''}<b>${title}</b>${text ? `<span>${text}</span>` : ''}${btns ? `<div class="btns">${btns}</div>` : ''}</div>`,
    skelRows: (n = 6) => Array.from({ length: n }, () => '<div class="skel-row"><span class="skel"></span><span class="skel w80"></span><span class="skel"></span><span class="skel"></span></div>').join(''),
    skelCards: (n = 2) => Array.from({ length: n }, () => '<div class="skel-card"><span class="skel w30"></span><span class="skel w80"></span><span class="skel w60"></span></div>').join(''),
    cp: () => `<span class="cp">${ic('copy')}</span>`,
    cpi: () => `<span class="cpi">${ic('copy')}</span>`,
    lnk: (text, mono) => `<span class="lnk ${mono ? 'mono' : ''}">${text}</span>`,
    id: (text, copy = true) => `<span class="mono">${text}</span>${copy ? `<span class="cpi">${ic('copy')}</span>` : ''}`,
    code: (text, { box = true, copy = true, lang = 'yaml' } = {}) => `<div class="code ${box ? 'box' : ''}">${lang === 'yaml' ? yaml(text) : lang === 'json' ? json(text) : text}${copy ? `<span class="cp">${ic('copy')}</span>` : ''}</div>`,
    diff: (lines) => `<div class="diff">${lines.map(([op, t]) => `<span class="${{ '+': 'a', '-': 'd', '@': 'h' }[op] || 'k'}">${op === '@' ? '' : (op || ' ') + ' '}${t}</span>`).join('')}</div>`,
    logl: (ts, lv, msg) => `<div><span class="muted">${ts}</span> <span class="lv ${{ INFO: 'info', WARN: 'warn', ERROR: 'bad' }[lv] || ''}">${lv}</span> ${msg}</div>`,
    spark: (pts, color = 'var(--text-2)', w = 150, h = 30, mark, lo, hi) => {
      const max = hi ?? Math.max(...pts), min = lo ?? Math.min(...pts);
      const xy = pts.map((p, i) => [+(i * (w / (pts.length - 1))).toFixed(1), +(h - 4 - ((p - min) / (max - min || 1)) * (h - 8)).toFixed(1)]);
      return `<svg class="spark" width="${w}" height="${h}"><polyline fill="none" stroke="${color}" stroke-width="1.6" points="${xy.map((p) => p.join(',')).join(' ')}"/>${mark != null ? `<circle cx="${xy[mark][0]}" cy="${xy[mark][1]}" r="3" fill="var(--warn)"/>` : ''}</svg>`;
    },
    bar: (parts) => `<div class="bar">${parts.map(([w, c]) => `<i style="width:${w}%;background:${c}"></i>`).join('')}</div>`,
    legend: (parts) => `<div class="legend">${parts.map(([l, c]) => `<span style="--c:${c}">${l}</span>`).join('')}</div>`,
    meter: (pct) => `<div class="meter"><i style="width:${pct}%"></i></div>`,
    dhead: (tabsHtml, right = '', back = '') => `<div class="dhead">${back ? `<span class="back">${ic('back')}${back}</span>` : ''}${tabsHtml}${right ? `<span class="r">${right}</span>` : ''}</div>`,
    dtabs: (items, on) => items.map((t) => { const [l, n] = Array.isArray(t) ? t : [t]; return `<span class="tab ${l === on ? 'on' : ''}">${l}${n != null ? `<span class="n">${n}</span>` : ''}</span>`; }).join(''),
    head: (title, sub = [], lede = '') => `<div><h1>${title}</h1>${sub.length ? `<div class="sub">${sub.join('<span class="faint">·</span>')}</div>` : ''}${lede ? `<p class="lede">${lede}</p>` : ''}</div>`,
    pop: (items, style = '') => `<div class="pop" style="${style}">${items.map((it) => (it === '-' ? '<div class="msep"></div>' : typeof it === 'string' ? it : it.label != null ? `<div class="mi ${it.cls || ''}">${it.icon ? ic(it.icon) : ''}${it.label}${it.hint ? `<span class="muted">${it.hint}</span>` : ''}</div>` : `<div class="mlab">${it.heading}</div>`)).join('')}</div>`,
    dialog: (title, body, actions, icon = '') => `<div class="scrim"><div class="dialog"><div class="dh">${icon ? ic(icon) : ''}${title}</div><div class="db">${body}</div><div class="actions">${actions}</div></div></div>`,
    toast: (text, tone = 'ok') => `<div class="toasts"><div class="toast ${tone}">${text}</div></div>`,
    steps: (items) => `<div class="steps">${items.map(([label, st, n], i) => `${i ? `<span class="step-line ${st === 'done' || st === 'cur' ? 'done' : ''}"></span>` : ''}<span class="step ${st}"><span class="sn">${st === 'done' ? ic('check') : st === 'bad' ? ic('x') : n ?? i + 1}</span>${label}</span>`).join('')}</div>`,
  };

  function yaml(text) {
    return text.split('\n').map((line) => {
      if (/^\s*#/.test(line)) return `<span class="c">${line}</span>`;
      const m = line.match(/^(\s*-?\s*)([A-Za-z0-9_.$/-]+)(:)(.*)$/);
      if (!m) return line;
      const v = m[4];
      const tv = v.trim();
      const cls = /^-?\d+(\.\d+)?$|^(true|false|null)$/.test(tv) ? 'n' : /^["']/.test(tv) ? 's' : '';
      const cm = v.match(/^(.*?)(\s+#.*)$/);
      const val = cm ? cm[1] : v;
      return `${m[1]}<span class="k">${m[2]}</span>${m[3]}${cls ? `<span class="${cls}">${val}</span>` : val}${cm ? `<span class="c">${cm[2]}</span>` : ''}`;
    }).join('\n');
  }
  function json(text) {
    return text.replace(/("(?:[^"\\]|\\.)*")(\s*:)?|\b(-?\d+(?:\.\d+)?|true|false|null)\b/g, (m, s, c, lit) => (s ? (c ? `<span class="k">${s}</span>${c}` : `<span class="s">${s}</span>`) : `<span class="n">${lit}</span>`));
  }

  // ---- shell ----
  const NAV = [
    ['Operate', [['home', 'Overview', 'overview'], ['runs', 'Runs', 'runs', '3'], ['changes', 'Changes', 'changes', '2', true], ['trig', 'Triggers', 'triggers'], ['sched', 'Schedules', 'schedules', null, false, true]]],
    ['Data', [['db', 'Datasets', 'datasets', '48'], ['lineage', 'Lineage', 'lineage']]],
    ['Build', [['tpl', 'Templates', 'templates', '17'], ['schema', 'Schemas', 'schemas'], ['submit', 'Submit', 'submit']]],
    ['Govern', [['policy', 'Policy', 'policy', null, false, true], ['usage', 'Usage', 'usage'], ['tenants', 'Tenants', 'tenants', '6']]],
    ['Admin', [['audit', 'Audit log', 'audit'], ['settings', 'Settings', 'settings']]],
  ];
  const VIEWS = {};
  const params = new URLSearchParams(location.search);
  const ctx = {
    view: params.get('view') || 'runs',
    state: params.get('state') || '',
    role: params.get('role') || 'admin',
    scope: params.get('scope') || '',
  };

  function shell(navKey, st = {}) {
    const viewer = ctx.role === 'viewer';
    const tenantScoped = ctx.scope === 'tenant';
    const hiddenForTenant = new Set(['tenants', 'audit', 'submit', 'schemas']);
    document.getElementById('side').innerHTML = `
      <div class="ws"><svg class="logo" viewBox="6 6 116 116" width="30" height="30"><rect class="tile" x="6" y="6" width="116" height="116" rx="28"/><g class="mk" fill="none" stroke-width="11" stroke-linecap="round" stroke-linejoin="round" transform="translate(-2,2)"><path d="M55 40 V26"/><path d="M44 26 H66"/><path d="M34 40 H76 a14 14 0 0 1 14 14 V64"/></g><g class="dr" transform="translate(-2,2)"><circle cx="90" cy="79" r="5.5"/><circle cx="90" cy="95" r="4.6" opacity="0.85"/><circle cx="90" cy="108" r="3.6" opacity="0.6"/></g></svg>
        <div><b>faucet</b><span>${tenantScoped ? 'acme · tenant console' : 'prod-eu · 3 replicas'}</span></div>${ic('updown', 'i chev')}</div>
      <div class="find">${ic('search')}<span>Go to…</span><kbd>⌘K</kbd></div>
      ${NAV.map(([g, items]) => {
        const shown = items.filter(([, , key]) => !(tenantScoped && hiddenForTenant.has(key)) && !(viewer && key === 'submit'));
        return shown.length ? `<div class="grp"><h6>${g} ${ic('chev')}</h6>${shown.map(([k, l, key, n, hot, soon]) =>
          { const bn = st.badges === false ? null : st.badges && key in st.badges ? st.badges[key] : tenantScoped && key !== 'runs' && key !== 'changes' ? null : n; return `<div class="nav ${key === navKey ? 'on' : ''} ${soon ? 'soon' : ''}">${ic(k)}<span class="lbl">${l}</span>${soon ? '<span class="tag">soon</span>' : bn ? `<span class="n ${hot ? 'hot' : ''}">${bn}</span>` : ''}</div>`; }).join('')}</div>` : '';
      }).join('')}
      <div class="foot"><div class="who"><span class="avatar">${tenantScoped ? 'AC' : viewer ? 'VI' : 'OA'}</span><span class="nm">${tenantScoped ? 'acme-portal' : viewer ? 'analyst-vi' : 'ops-admin'}</span></div><span class="tag">${tenantScoped ? 'tenant acme' : viewer ? 'read-only' : 'admin'}</span></div>`;
    document.getElementById('status').innerHTML = `<span><span class="dot"></span>history postgres · healthy</span><span>cluster · 3 live</span><span>faucet serve 2.4.0</span><span>Design mockup · sample data</span>`;
  }

  /** The top bar: crumbs (last is bold), an optional delta, and right-side chips. */
  function top(crumbs, { delta = '', right = '' } = {}) {
    const tenantScoped = ctx.scope === 'tenant';
    const tenantSel = tenantScoped ? `<span class="chip">${ic('tenants')}acme</span>` : `<span class="select" style="height:28px">All tenants</span>`;
    document.getElementById('top').innerHTML = `<div class="crumb">${crumbs.map((c, i) => (i === crumbs.length - 1 ? `<b>${c}</b>` : `${c}<span class="faint">/</span>`)).join(' ')}</div>${delta ? `<span class="delta">${delta}</span>` : ''}<div class="right">${right}${tenantSel}<span class="select" style="height:28px">UTC</span>${ic('theme')}${ic('link')}${ic('expand')}</div>`;
  }
  const list = (h) => (document.getElementById('list').innerHTML = h);
  const detail = (h) => (document.getElementById('detail').innerHTML = h);
  const panel = (h) => {
    let p = document.getElementById('panel');
    if (!p) { p = document.createElement('div'); p.id = 'panel'; p.className = 'panel'; document.getElementById('body').appendChild(p); }
    p.innerHTML = h;
  };
  const overlay = (h) => document.getElementById('main').insertAdjacentHTML('beforeend', h);

  function palette() {
    return `<div class="scrim"><div class="pal"><div class="pin">${ic('search')}<span>orders</span><span class="caret"></span></div><div class="pl">
      <div class="pg">Runs</div>
      <div class="pi on">${ic('runs')}<span><mark>orders</mark>_cdc_mirror</span><span class="m">running · 7f3c…a91e · 14m ago</span></div>
      <div class="pi">${ic('runs')}<span><mark>orders</mark>_nightly_rollup</span><span class="m">completed · 2e81…05bd · 3h ago</span></div>
      <div class="pg">Datasets</div>
      <div class="pi">${ic('db')}<span>analytics.<mark>orders</mark></span><span class="m">bigquery · fresh 6s</span></div>
      <div class="pi">${ic('db')}<span><mark>orders</mark>-db.public.<mark>orders</mark></span><span class="m">postgres · source</span></div>
      <div class="pg">Templates</div>
      <div class="pi">${ic('tpl')}<span>faucet-hq/rest-<mark>orders</mark></span><span class="m">source · stable v11</span></div>
      <div class="pg">Actions</div>
      <div class="pi">${ic('submit')}<span>Submit a run from <mark>orders</mark>_cdc_mirror's config</span><span class="m">opens Submit</span></div>
    </div><div class="pf"><span><kbd>↑</kbd> <kbd>↓</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>⌘↵</kbd> new tab</span><span><kbd>esc</kbd> close</span></div></div></div>`;
  }

  function boot() {
    const t = params.get('theme');
    if (t === 'light') document.documentElement.dataset.theme = 'light';
    document.documentElement.dataset.role = ctx.role;
    const v = VIEWS[ctx.view];
    if (!v) { document.body.innerHTML = `<p style="padding:20px">Unknown view ${ctx.view}</p>`; return; }
    const stateKey = ctx.state || Object.keys(v.states)[0];
    const st = v.states[stateKey] || {};
    const body = document.getElementById('body');
    const shape = st.shape || v.shape || 'split';
    if (shape !== 'split') body.classList.add('canvas');
    if (shape === 'canvas-panel') body.classList.add('side-panel');
    if (st.sel !== false && shape === 'split') body.classList.add('has-sel');
    shell(v.nav || ctx.view, st);
    v.render(stateKey, ctx);
    const ov = params.get('overlay');
    if (ov === 'palette') overlay(palette());
    if (ov === 'toast') overlay(H.toast(`${ic('check')}Run <b>orders_cdc_mirror</b> submitted · <span class="lnk">open</span>`, 'ok'));
    document.title = `${v.label} · ${st.label || stateKey} — faucet console mockup`;
  }

  window.M = { ...H, VIEWS, ctx, top, list, detail, panel, overlay, palette, boot, NAV, view: (name, def) => (VIEWS[name] = { name, ...def }) };
  Object.assign(window, H);
})();
