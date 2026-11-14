(function () {
  const { icon, code, aside, crumbs, pn, pfoot, card } = M;

  // ---- benchmarks (compare topic) ----
  const bars = (title, sub, rows, unit) => {
    const max = Math.max(...rows.map((r) => r[1]));
    return `<div class="bars"><h4>${title}</h4><div class="sub">${sub}</div>${rows.map(([n, v, us]) => `<div class="bar"><span class="nm${us ? ' us' : ''}">${n}</span><span class="track"><span class="fill${us ? ' us' : ''}" style="width:${(v / max) * 100}%"></span></span><span class="val${us ? ' us' : ''}">${v.toLocaleString()} ${unit}</span></div>`).join('')}</div>`;
  };
  const bench = () => `
    ${crumbs(['Compare', 'Benchmarks'])}
    <h1 class="title">Benchmarks</h1>
    <p class="lead">Same data, same machine, same destination. Reproduce every number with <code>scripts/bench</code>.</p>
    ${aside('note', 'Sample numbers for the mockup only. The real page keeps its measured results.')}
    <div class="stats">
      <div class="stat"><div class="v">9.4<small>×</small></div><div class="l">faster end to end</div></div>
      <div class="stat"><div class="v">62<small>MB</small></div><div class="l">peak memory, flat with size</div></div>
      <div class="stat"><div class="v">1</div><div class="l">binary, no Python runtime</div></div>
      <div class="stat"><div class="v">0</div><div class="l">rows lost after kill -9</div></div>
    </div>
    ${bars('Postgres → JSONL, 10 M rows', 'Records per second, higher is better', [['faucet', 412000, 1], ['Tool A', 61000], ['Tool B', 44000]], 'rec/s')}
    ${bars('Peak memory', 'MB, lower is better', [['faucet', 62, 1], ['Tool A', 410], ['Tool B', 980]], 'MB')}
    ${pfoot()}${pn('How faucet compares', 'vs. Meltano')}`;
  M.view('benchmarks', {
    title: 'Benchmarks (charts)',
    group: 'Pages',
    states: { default: { label: 'Default', render: () => ({ topic: 'compare', active: 'Benchmarks', toc: [['Throughput', 2], ['Memory', 2], ['Method', 2]], tocOn: 'Throughput', body: bench() }) } },
  });

  // ---- topic index (Cookbook home) ----
  const topicIndex = () => `
    ${crumbs(['Cookbook'])}
    <h1 class="title">Cookbook</h1>
    <p class="lead">Short recipes for one job each. Find the task, copy the config, adjust the names.</p>
    <div class="cards three">
      ${card('wand', 'Shaping the data', 'Transforms, SQL, WASM and topology.', { left: true, more: '4 recipes' })}
      ${card('plug', 'Reading from sources', 'Pagination, auth and discovery.', { left: true, more: '4 recipes' })}
      ${card('shield', 'Moving data reliably', 'State, upserts, CDC, DLQ, rollback.', { left: true, more: '12 recipes' })}
      ${card('alert', 'Quality & governance', 'Checks, contracts, masking, policies.', { left: true, more: '5 recipes' })}
      ${card('layers', 'Config & reuse', 'Composition, templates, secrets, tuning.', { left: true, more: '7 recipes' })}
      ${card('gauge', 'Observability & lineage', 'SLAs, profiling, lineage, costs.', { left: true, more: '10 recipes' })}
    </div>
    <h2>Most read</h2>
    <div class="dlist">
      <a href="#"><b>Incremental replication & state${icon('arrow')}</b><span>Move only what changed since the last run.</span></a>
      <a href="#"><b>Upsert / mirror tables${icon('arrow')}</b><span>Keep a destination table equal to the source.</span></a>
      <a href="#"><b>PII detection & masking${icon('arrow')}</b><span>Find and mask personal data before it leaves.</span></a>
    </div>
    ${pfoot()}`;
  M.view('topic-index', {
    title: 'Topic home (Cookbook)',
    group: 'Pages',
    states: { default: { label: 'Default', render: () => ({ topic: 'cookbook', active: '', closed: ['Moving data reliably', 'Data quality & governance', 'Config & reuse', 'Observability & lineage'], body: topicIndex() }) } },
  });

  // ---- search modal ----
  const modal = (q, inner, opts = {}) => `<div class="scrim"><div class="modal"><div class="modal-in">${icon('search')}${q ? `<span>${q}</span><span class="caret" style="margin-left:-8px"></span>` : '<span class="ph">Search the docs</span><span class="caret"></span>'}<kbd>Esc</kbd></div><div class="modal-body">${inner}</div><div class="modal-foot"><span><kbd>↵</kbd>open</span><span><kbd>↑</kbd><kbd>↓</kbd>move</span><span class="hide-m"><kbd>Esc</kbd>close</span><span class="pf">Search by Pagefind</span></div></div></div>`;
  const res = (title, path, ex, hl, sub) => `<div class="res${hl ? ' hl' : ''}${sub ? ' sub' : ''}">${icon(sub ? 'hash' : 'file')}<div><b>${title}</b>${path ? `<span class="path">${path}</span>` : ''}${ex ? `<span class="ex">${ex}</span>` : ''}</div><span class="enter">${icon('enter')}</span></div>`;
  const recent = () => `<div class="res-h">Recent</div>${res('Incremental replication & state', 'Cookbook › Moving data reliably', '', true)}${res('Connector catalog', 'Reference › Connectors')}${res('Deploying faucet', 'Operate › Deploy & run')}<div class="res-h">Popular</div>${res('Your first pipeline', 'Learn › Start here')}${res('CLI commands', 'Reference › CLI & config')}`;
  const results = () => `<div class="res-h">Pages</div>
    ${res('Upsert / mirror tables', 'Cookbook › Moving data reliably', 'Set <code>write_mode: <mark>upsert</mark></code> and a <code>key</code> so re-runs update rows instead of adding them…', true)}
    ${res('Choosing a key', '', 'The <mark>upsert</mark> key must be unique in the source…', false, true)}
    ${res('Deletes and tombstones', '', 'CDC deletes turn into <mark>upsert</mark> tombstones when…', false, true)}
    ${res('Mirror (snapshot → CDC)', 'Cookbook › Moving data reliably', 'A snapshot first, then CDC changes applied as <mark>upserts</mark>…')}
    ${res('Feature compatibility', 'Reference › Connectors', 'Which sinks support <mark>upsert</mark> together with exactly-once…')}`;
  const none = () => `<div class="empty" style="margin:16px 8px;border:0">${icon('search')}<b>No results for “kafak”</b>Did you mean <a href="#">kafka</a>? Or ask the docs AI about it.<div style="margin-top:16px"><a class="btn btn-soft btn-sm" href="#">${icon('sparkles')}Ask AI about “kafak”</a></div></div>`;
  const loading = () => `<div style="padding:14px 12px">${[80, 60, 90, 50, 70].map((w) => `<div class="skel" style="width:${w}%"></div>`).join('')}</div>`;
  const behind = () => M.guideBase();
  M.view('search', {
    title: 'Search (⌘K)',
    group: 'Overlays',
    states: {
      empty: { label: 'Opened, nothing typed', render: () => ({ ...behind(), overlay: modal('', recent()) }) },
      loading: { label: 'Loading the index', widths: [1440], render: () => ({ ...behind(), overlay: modal('ups', loading()) }) },
      results: { label: 'Results for “upsert”', render: () => ({ ...behind(), overlay: modal('upsert', results()) }) },
      'no-results': { label: 'No results', render: () => ({ ...behind(), overlay: modal('kafak', none()) }) },
    },
  });

  // ---- header menus ----
  M.view('menus', {
    title: 'Header menus',
    group: 'Overlays',
    states: {
      'ask-ai': { label: 'Ask AI menu open', widths: [1440], render: () => ({ ...behind(), askOpen: true }) },
      theme: { label: 'Theme menu open', widths: [1440], render: () => ({ ...behind(), themeOpen: true }) },
      'theme-auto': { label: 'Theme set to Auto', widths: [1440], render: () => ({ ...behind(), themeOpen: true, themeChoice: 'auto' }) },
    },
  });

  // ---- mobile menu ----
  M.view('mobile-menu', {
    title: 'Phone menu',
    group: 'Overlays',
    states: {
      learn: { label: 'Open on a Learn page', widths: [390], render: () => ({ ...behind(), mobileMenu: true }) },
      cookbook: { label: 'Open on a Cookbook page', widths: [390], render: () => ({ topic: 'cookbook', active: 'Dead-letter queues', closed: ['Shaping the data', 'Reading from sources', 'Data quality & governance', 'Config & reuse', 'Observability & lineage'], body: '', mobileMenu: true }) },
    },
  });

  // ---- 404 ----
  const nf = () => `<div class="nf"><div class="drip">${M.LOGO}</div><div class="code404">404 · PAGE NOT FOUND</div><h1 class="title" style="font-size:40px">This page has run dry</h1><p class="lead" style="margin:0 auto">It may have moved when the docs were reorganised. Search for it, or start from one of these.</p><div class="search-pill">${icon('search')}<span>Search docs</span><kbd>⌘ K</kbd></div>
    <div class="cards three" style="text-align:left">${card('cap', 'Learn', 'Install and run a first pipeline.', { left: true })}${card('book', 'Cookbook', 'Recipes for one job each.', { left: true })}${card('hash', 'Reference', 'Connectors, CLI and config.', { left: true })}</div></div>`;
  M.view('not-found', {
    title: '404 page',
    group: 'Pages',
    states: { default: { label: 'Default', render: () => ({ topic: '', layout: 'splash', body: nf() }) } },
  });
})();
