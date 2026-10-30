// Lineage — a graph tool: full-width canvas (pan, zoom, minimap, search, focus
// mode with hop depth) and a side panel for the selected node or edge.
(function () {
  const W = 210, H = 46;
  // [id, col, y, name, kind, freshness tone, meta]
  const ALL = [
    ['src_orders', 0, 40, 'orders-db.public.orders', 'postgres', 'ok', 'source · CDC'],
    ['src_items', 0, 120, 'orders-db.public.order_items', 'postgres', 'ok', 'source · CDC'],
    ['src_billing', 0, 220, 'billing /v2/invoices', 'rest', 'ok', 'source · 5m'],
    ['src_clicks', 0, 320, 'clickstream', 'kafka', 'ok', 'source · live'],
    ['src_crm', 0, 420, 'crm /contacts', 'rest', 'bad', 'source · failing'],
    ['orders', 1, 80, 'analytics.orders', 'bigquery', 'ok', '41.2M rows · 6s'],
    ['items', 1, 160, 'analytics.order_items', 'bigquery', 'ok', '9.8M rows · 6s'],
    ['invoices', 1, 240, 'lake/invoices/dt=*', 's3', 'ok', '84k/day · 5m'],
    ['clicks', 1, 320, 'events.clicks', 'iceberg', 'ok', '6.8M rows · 2h'],
    ['crm', 1, 420, 'crm.contacts', 'postgres', 'warn', '212k rows · 26h'],
    ['daily', 2, 60, 'bi.orders_daily', 'bigquery', 'ok', '1,204 rows · 1h'],
    ['cash', 2, 180, 'ml.cashflow_features', 'iceberg', 'bad', 'drift · 2h'],
    ['ledger', 2, 300, 'finance.ledger', 'postgres', 'warn', 'stale · 26h'],
    ['ledger_daily', 3, 240, 'bi.ledger_daily', 'bigquery', 'warn', 'behind · 26h'],
  ];
  const EDGES = [
    ['src_orders', 'orders'], ['src_items', 'items'], ['src_billing', 'orders'], ['src_billing', 'invoices'], ['src_clicks', 'clicks'], ['src_crm', 'crm'],
    ['orders', 'daily'], ['orders', 'cash'], ['items', 'daily'], ['invoices', 'ledger'], ['crm', 'ledger'], ['ledger', 'ledger_daily'], ['cash', 'ledger_daily'],
  ];
  let COLX = [40, 330, 620, 910];
  let NW = W;
  const compact = () => { COLX = [10, 245, 480, 715]; NW = 200; };

  function nodeSvg(n, cls = '', extra = '') {
    const [, col, y, name, kind, tone, meta] = n;
    const x = COLX[col];
    return `<g class="gn ${cls}"><rect x="${x}" y="${y}" width="${NW}" height="${H}" rx="8"/><circle class="gdot ${tone}" cx="${x + 14}" cy="${y + 16}" r="4"/><text class="gt" x="${x + 26}" y="${y + 20}">${name}</text><text class="gs" x="${x + 26}" y="${y + 35}">${kind} · ${meta}</text>${extra}</g>`;
  }
  const P = (id) => ALL.find((n) => n[0] === id);
  function edgeSvg(a, b, cls = '') {
    const A = P(a), B = P(b);
    const x1 = COLX[A[1]] + NW, y1 = A[2] + H / 2, x2 = COLX[B[1]], y2 = B[2] + H / 2, m = (x1 + x2) / 2;
    return `<path class="ge ${cls}" d="M${x1} ${y1} C${m} ${y1} ${m} ${y2} ${x2} ${y2}"/>`;
  }
  function minimap(sel = '', vp = [4, 4, 142, 82]) {
    const s = 150 / 1180;
    return `<div class="minimap"><svg viewBox="0 0 150 90">${ALL.map((n) => `<rect class="mm-n ${n[0] === sel ? 'sel' : ''}" x="${(COLX[n[1]] * s).toFixed(1)}" y="${(n[2] * s * 1.25 + 4).toFixed(1)}" width="${(W * s).toFixed(1)}" height="5" rx="1.5"/>`).join('')}<rect class="mm-vp" x="${vp[0]}" y="${vp[1]}" width="${vp[2]}" height="${vp[3]}" rx="3"/></svg></div>`;
  }
  const tools = `<div class="gtools">${ibtn('zoomin')}${ibtn('zoomout')}${ibtn('fit')}${ibtn('focus')}</div>`;
  const legendHtml = `<div class="lg-legend"><span><span class="dot"></span>fresh</span><span><span class="dot warn"></span>stale</span><span><span class="dot bad"></span>failing or drifted</span><span><span class="dot muted"></span>never succeeded</span></div>`;

  function bar({ search = '', focus = '', depth = 2 } = {}) {
    return `<div class="lg-bar">${input(search || 'Find a dataset', { icon: 'search', ph: !search })}
      ${focus ? `${chip(`focus: ${focus}`, 'x')}<span class="lg-depth"><span>−</span><b>${depth} hops</b><span>+</span></span>${select('Both ways')}<span class="r">6 of 14</span>` : `${select('Every kind')}${toggle('Show sources', true)}<span class="r">14 datasets · 13 edges${btn('Datasets', { kind: 'sm', icon: 'db' })}</span>`}</div>`;
  }

  function canvas(svgInner, { view = '0 0 1160 500', search, focus, depth, extra = '', zoom = '100%', sel = '', vp } = {}) {
    return `<div class="card lg">${bar({ search, focus, depth })}<div class="graph"><svg viewBox="${view}" preserveAspectRatio="xMidYMid meet">${svgInner}</svg>${tools}<span class="lg-zoom">${zoom}</span>${minimap(sel, vp)}${legendHtml}${extra}</div></div>`;
  }

  const head0 = (sub) => `${dhead('', btn('Copy link', { icon: 'link', kind: 'quiet' }))}${head('Lineage', sub)}`;

  function nodePanel() {
    return `${dhead(`<span class="tab on">Dataset</span><span class="tab">Columns</span>`, ibtn('x'))}
      ${head('analytics.orders', [pill('Fresh · 6s', 'ok', 'check'), '<span>bigquery</span>'])}
      ${card({ body: grid(2, [kv('Rows last run', '1.24M'), kv('Schema', 'v14', '+1 column today'), kv('Last success', '6s ago', `run ${lnk('7f3c…a91e', true)}`), kv('Owners', 'team-commerce')]) })}
      ${card({ title: 'Upstream', right: '2', body: `<div class="lg-list"><div>${ring('ok')}${lnk('orders-db.public.orders')}<span class="r">orders_cdc_mirror</span></div><div>${ring('ok')}${lnk('billing /v2/invoices')}<span class="r">invoices_enrich</span></div></div>` })}
      ${card({ title: 'Downstream', right: '2', body: `<div class="lg-list"><div>${ring('ok')}${lnk('bi.orders_daily')}<span class="r">orders_rollup</span></div><div>${ring('bad')}${lnk('ml.cashflow_features')}<span class="r">cashflow_features_build</span></div></div>` })}
      <div style="display:flex;gap:8px;flex-wrap:wrap">${btn('Open dataset', { icon: 'db' })}${btn('Last run', { icon: 'runs' })}${btn('Trace a column', { icon: 'columns' })}</div>`;
  }

  M.view('lineage', {
    label: 'Lineage', nav: 'lineage', icon: 'lineage', summary: 'how data flows: graph with focus, freshness and column lineage',
    shape: 'canvas',
    states: {
      overview: { label: 'Whole graph · search, controls, minimap', shape: 'canvas', covers: ['whole graph', 'layered layout', 'datasets button'], new: ['pan / zoom / fit controls', 'minimap', 'freshness on every node', 'search to find and centre'] },
      focus: { label: 'Focus mode · node selected', shape: 'canvas-panel', covers: ['rooted at a dataset', 'click to highlight neighbours', 'open the dataset'], new: ['hop depth stepper (up/downstream)', 'side panel: kv, upstream/downstream, links to dataset and last run'] },
      edge: { label: 'Edge selected · column lineage', shape: 'canvas-panel', covers: ['edge data: pipeline, row, runs, last rows, last run'], new: ['column lineage table per edge'] },
      column: { label: 'Column traced across hops', shape: 'canvas-panel', new: ['trace one column through every hop'] },
      zoomed: { label: 'Zoomed in · 180%', shape: 'canvas', new: ['zoom level and viewport on the minimap'] },
      empty: { label: 'Empty graph', shape: 'canvas', covers: ['empty state'] },
      loading: { label: 'Loading', shape: 'canvas' },
    },
    render(state) {
      if (state === 'overview') {
        M.top(['Data', 'Lineage']);
        const svg = EDGES.map(([a, b]) => edgeSvg(a, b, a === 'src_billing' && b === 'orders' ? 'on' : '')).join('') + ALL.map((n) => nodeSvg(n, n[0] === 'orders' ? 'nb' : '')).join('')
          + `<g class="gn"><text class="gs" x="40" y="22">Sources</text><text class="gs" x="330" y="22">Landed</text><text class="gs" x="620" y="22">Derived</text><text class="gs" x="910" y="22">Reports</text></g>`;
        M.detail(`${head0(['<span>14 datasets</span>', '<span>13 edges from the last 30 days of runs</span>', '<span>drag to pan · scroll to zoom · click a dataset to focus</span>'])}${canvas(svg)}`);
        return;
      }
      if (state === 'focus' || state === 'edge') {
        M.top(['Data', 'Lineage', 'analytics.orders']);
        compact();
        const keep = new Set(['src_orders', 'src_billing', 'orders', 'daily', 'cash', 'ledger_daily']);
        const selEdge = state === 'edge' ? ['orders', 'cash'] : null;
        const svg = EDGES.filter(([a, b]) => keep.has(a) && keep.has(b)).map(([a, b]) => {
          const on = true;
          const cls = selEdge && a === selEdge[0] && b === selEdge[1] ? 'sel' : on ? (state === 'focus' && (a === 'orders' || b === 'orders') ? 'on' : '') : 'dim';
          return edgeSvg(a, b, cls);
        }).join('') + ALL.filter((n) => keep.has(n[0])).map((n) => nodeSvg(n, n[0] === 'orders' && state === 'focus' ? 'sel' : state === 'focus' && ['src_orders', 'src_billing', 'daily', 'cash'].includes(n[0]) ? 'nb' : '')).join('')
          + (selEdge ? `<g><rect class="ge-lbl-bg" x="384" y="146" width="150" height="20" rx="10"/><text class="ge-lbl" x="398" y="160">cashflow_features_build</text></g>` : '');
        M.detail(`${head0([`<span>focused on ${lnk('analytics.orders')}</span>`, '<span>2 hops up and down</span>'])}${canvas(svg, { focus: 'analytics.orders', depth: 2, sel: 'orders', view: '0 20 925 300' })}`);
        if (state === 'focus') { M.panel(nodePanel()); return; }
        M.panel(`${dhead('<span class="tab on">Edge</span>', ibtn('x'))}
          ${head('analytics.orders → ml.cashflow_features', ['<span>written by a pipeline</span>'])}
          ${card({ body: grid(2, [kv('Pipeline', lnk('cashflow_features_build')), kv('Matrix row', '<span class="mono">features</span>'), kv('Runs', '64', 'first 64d ago'), kv('Last rows', '412,880', `run ${lnk('c19d…77a0', true)} · 2h`)]) })}
          ${card({ icon: 'columns', title: 'Column lineage', right: '6 columns', body: tbl(['Into', 'From', 'How'], [['<span class="mono">amount</span>', '<span class="mono">total</span>', 'identity'], ['<span class="mono">discount_ratio</span>', '<span class="mono">discount_code, total</span>', 'sql'], ['<span class="mono">account_id</span>', '<span class="mono">customer_id</span>', 'rename'], ['<span class="mono">region</span>', '<span class="mono">ship_country</span>', 'sql']]), note: 'Recorded from the pipeline’s transforms; opaque steps are marked, never guessed.' })}
          <div style="display:flex;gap:8px;flex-wrap:wrap">${btn('Open pipeline runs', { icon: 'runs' })}${btn('Last run', { icon: 'play' })}</div>`);
        return;
      }
      if (state === 'column') {
        M.top(['Data', 'Lineage', 'analytics.orders']);
        // expanded nodes with column lists; trace `total`
 const BW = 200;
        const big = (x, y, name, kind, tone, cols, hl, cls = '') => {
          const h = 46 + cols.length * 20 + 6;
          return `<g class="gn ${cls}"><rect x="${x}" y="${y}" width="${BW}" height="${h}" rx="8"/><circle class="gdot ${tone}" cx="${x + 14}" cy="${y + 16}" r="4"/><text class="gt" x="${x + 26}" y="${y + 20}">${name}</text><text class="gs" x="${x + 26}" y="${y + 35}">${kind}</text><line class="sep" x1="${x}" x2="${x + BW}" y1="${y + 44}" y2="${y + 44}"/>${cols.map((c, i) => `${c === hl ? `<rect class="colhl" x="${x + 4}" y="${y + 48 + i * 20}" width="${BW - 8}" height="18" rx="4"/>` : ''}<text class="gcol ${c === hl ? 'hl' : ''}" x="${x + 14}" y="${y + 61 + i * 20}">${c}</text>`).join('')}</g>`;
        };
        const yc = (y, i) => y + 57 + i * 20;
        const e = (x1, y1, x2, y2, cls) => { const m = (x1 + x2) / 2; return `<path class="ge ${cls}" d="M${x1} ${y1} C${m} ${y1} ${m} ${y2} ${x2} ${y2}"/>`; };
        const svg = e(210, yc(40, 2), 245, yc(60, 1), 'on') + e(445, yc(60, 1), 480, yc(20, 1), 'on') + e(445, yc(60, 1), 480, yc(220, 0), 'on') + e(210, yc(260, 0), 245, yc(60, 3), 'dim') + e(680, yc(220, 0), 715, yc(260, 1), 'on')
          + big(10, 40, 'orders-db.public.orders', 'postgres', 'ok', ['id', 'customer_id', 'amount_cents', 'discount_code'], 'amount_cents')
          + big(10, 260, 'billing /v2/invoices', 'rest', 'ok', ['invoice_id', 'paid_at'], '', 'dim')
          + big(245, 60, 'analytics.orders', 'bigquery', 'ok', ['order_id', 'total', 'email_hash', 'discount_code'], 'total')
          + big(480, 20, 'bi.orders_daily', 'bigquery', 'ok', ['day', 'revenue', 'orders'], 'revenue')
          + big(480, 220, 'ml.cashflow_features', 'iceberg', 'bad', ['amount', 'discount_ratio', 'region'], 'amount')
          + big(715, 260, 'finance.forecast', 'postgres', 'ok', ['month', 'expected_cash'], 'expected_cash');
        M.detail(`${head0([`<span>tracing column ${chip('analytics.orders.total', 'mono')}</span>`, '<span>4 hops · 5 columns</span>'])}${canvas(svg, { focus: 'analytics.orders', depth: 3, sel: 'orders', view: '0 10 925 380' })}`);
        M.panel(`${dhead('<span class="tab">Dataset</span><span class="tab on">Columns</span>', ibtn('x'))}
          ${head('total', ['<span>analytics.orders</span>', '<span>number</span>'])}
          ${card({ title: 'Comes from', body: `<div class="lg-list"><div><span class="mono">orders-db.public.orders.amount_cents</span><span class="r">cast · divide by 100</span></div></div>` })}
          ${card({ title: 'Flows into', right: '3', body: `<div class="lg-list"><div><span class="mono">bi.orders_daily.revenue</span><span class="r">sum</span></div><div><span class="mono">ml.cashflow_features.amount</span><span class="r">identity</span></div><div><span class="mono">finance.forecast.expected_cash</span><span class="r">sql</span></div></div>`, note: 'A planned change to <b>total</b> reaches 3 datasets and 1 dashboard owner.' })}
          ${card({ title: 'Other columns', body: `<div class="pad" style="display:flex;flex-wrap:wrap;gap:6px">${['order_id', 'email_hash', 'discount_code', 'channel', 'created_at'].map((c) => chip(c, 'mono')).join('')}</div>` })}`);
        return;
      }
      if (state === 'zoomed') {
        M.top(['Data', 'Lineage']);
        const svg = EDGES.map(([a, b]) => edgeSvg(a, b, a === 'orders' || b === 'orders' ? 'on' : '')).join('') + ALL.map((n) => nodeSvg(n, n[0] === 'orders' ? 'nb' : '')).join('');
        M.detail(`${head0(['<span>14 datasets</span>', '<span>hover a dataset for its last run</span>'])}${canvas(svg, { view: '280 40 600 260', zoom: '180%', vp: [36, 6, 72, 34], search: 'orders' })}`);
        return;
      }
      if (state === 'empty') {
        M.top(['Data', 'Lineage']);
        M.detail(`${head0(['<span>no edges yet</span>'])}<div class="card lg">${bar()}<div class="graph" style="display:grid;place-items:center">${empty('No lineage recorded yet', 'An edge appears when a run reads one dataset and writes another. Run a pipeline, or turn on the catalogue with a <code>catalog:</code> block on a CLI run.', { icon: 'lineage', btns: btn('Submit a run', { kind: 'primary', icon: 'submit', act: true }) + btn('Datasets', { icon: 'db' }) })}</div></div>`);
        return;
      }
      if (state === 'loading') {
        M.top(['Data', 'Lineage']);
        const sk = [[40, 60], [40, 160], [40, 260], [330, 110], [330, 210], [620, 80], [620, 200], [910, 150]].map(([x, y]) => `<rect x="${x}" y="${y}" width="${W}" height="${H}" rx="8" style="fill:var(--pill)"/>`).join('');
        M.detail(`${head0(['<span class="skel w30" style="width:180px"></span>'])}<div class="card lg">${bar()}<div class="graph"><svg viewBox="0 0 1160 360" preserveAspectRatio="xMidYMid meet">${sk}</svg></div></div>`);
      }
    },
  });
})();
