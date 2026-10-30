// Datasets — split pane: the catalogue on the left, one dataset on the right.
(function () {
  const ROWS = {
    orders: ['ok', '', 'analytics.orders', '6s', '<b>bigquery</b> · 41.2M rows · schema v14 · 3 consumers'],
    ledger: ['warn', 'half', 'finance.ledger', '26h', '<b>postgres</b> · stale · SLA 24h breached'],
    cash: ['bad', '', 'ml.cashflow_features', '2h', '<b>iceberg</b> · profile drift · null rate 0.4% → 11%'],
    inv: ['ok', '', 'lake/invoices/dt=*', '5m', '<b>s3 parquet</b> · 84k rows/day · no owner'],
    daily: ['ok', '', 'bi.orders_daily', '1h', '<b>bigquery</b> · 1,204 rows · schema v3'],
    src1: ['ok', '', 'orders-db.public.orders', '6s', '<b>postgres</b> · CDC slot faucet_orders'],
    src2: ['ok', '', 'billing /v2/invoices', '5m', '<b>rest</b> · incremental on updated_at'],
  };
  const r = (k, on) => { const [tone, shape, t, age, meta] = ROWS[k]; return row(ring(tone, shape), t, age, meta, { on }); };

  function listHtml({ on = 'orders', filters = false, kindOpen = false, loading = false, emptyList = false } = {}) {
    const tools = ibtn('filter', { on: filters, dot: filters }) + ibtn('lineage') + ibtn('download') + ibtn('refresh');
    if (loading) return `${tabs(['All', 'Destinations', 'Sources'], 'All', tools)}<div class="skel-card" style="padding:12px"><span class="skel w80"></span></div>${skelRows(8)}`;
    if (emptyList) return `${tabs(['All', 'Destinations', 'Sources'], 'All', tools)}${stats([['Datasets', '0'], ['Fresh', '0'], ['Stale', '0'], ['Drifted', '0']])}`;
    const f = filters ? `<div class="filters ds-filters">
        ${input('ledger', { icon: 'search' })}
        <div class="r2">${select('postgres, bigquery', 'open')}${select('Destinations')}</div>
        <div class="r2">${input('2026-10-01 00:00', { icon: 'clock' })}${input('to · now', { icon: 'clock', ph: true })}</div>
        <div class="active-f">${chip('search: ledger', 'x')}${chip('kind: postgres, bigquery', 'x')}${chip('role: destination', 'x')}${chip('since Oct 1', 'x')}<span class="lnk" style="font-size:12px">clear all</span></div>
      ${kindOpen ? pop([{ heading: 'Kind · 6 in view' }, `<div class="mi">${check('postgres', true)}<span class="muted">9</span></div>`, `<div class="mi hl">${check('bigquery', true)}<span class="muted">14</span></div>`, `<div class="mi">${check('iceberg')}<span class="muted">3</span></div>`, `<div class="mi">${check('s3')}<span class="muted">6</span></div>`, `<div class="mi">${check('rest')}<span class="muted">11</span></div>`, `<div class="mi">${check('kafka')}<span class="muted">5</span></div>`], 'left:10px;top:84px;width:200px;min-width:0') : ''}</div>` : '';
    const body = filters
      ? `${sect('Destinations', 2)}${r('ledger', on === 'ledger')}${row(ring('ok'), 'finance.ledger_daily', '3h', '<b>bigquery</b> · 1,180 rows · schema v5', {})}`
      : `${sect('Destinations', 31)}${r('orders', on === 'orders')}${r('ledger', on === 'ledger')}${r('cash', on === 'cash')}${r('inv', on === 'inv')}${r('daily')}${sect('Sources', 17)}${r('src1')}${r('src2')}`;
    return `${tabs(['All', 'Destinations', 'Sources'], 'All', tools)}
      ${stats([['Datasets', filters ? '2 <em>of 48</em>' : '48'], ['Fresh', filters ? '1' : '45', 'ok'], ['Stale', filters ? '1' : '2', 'warn'], ['Drifted', filters ? '0' : '1', 'bad']])}
      ${f}${body}`;
  }

  const TABS = [['Overview'], ['Schema', 14], ['Profile', 1], ['Lineage', 5], ['Activity'], ['Outputs', 2]];
  function dHead(tab, { title = 'analytics.orders', status, sub, viewer = false } = {}) {
    const actions = btn('Annotate', { icon: 'edit', act: true }) + btn('Open in lineage', { icon: 'lineage' });
    return `${dhead(dtabs(TABS, tab), actions, 'Datasets')}
      ${head(title, sub || [status || pill('Fresh · 6s', 'ok', 'check'), `<span>bigquery · written by ${lnk('orders_cdc_mirror')}</span>`, '<span>first seen 112d ago</span>', `<span class="mono">bigquery://analytics.orders</span>${cpi()}`])}`;
  }

  const VOL = [38, 40, 39, 42, 41, 44, 43, 45, 47, 46, 48, 50, 49, 52];
  const NUL = [0.3, 0.4, 0.3, 0.5, 0.4, 0.3, 0.4, 0.4, 0.3, 0.5, 0.4, 0.4, 0.3, 0.4];

  function miniGraph() {
    const n = (x, y, name, kind, cls = '', dot = 'ok') => `<g class="gn ${cls}"><rect x="${x}" y="${y}" width="180" height="38" rx="8"/><circle class="gdot ${dot}" cx="${x + 13}" cy="${y + 13}" r="3.5"/><text class="gt" x="${x + 24}" y="${y + 17}">${name}</text><text class="gs" x="${x + 24}" y="${y + 31}">${kind}</text></g>`;
    return `<svg class="ds-mini" viewBox="0 0 760 150" height="170" preserveAspectRatio="xMidYMid meet">
      <path class="ge" d="M200 39 C245 39 245 75 290 75"/><path class="ge" d="M200 111 C245 111 245 75 290 75"/><path class="ge" d="M470 75 C515 75 515 39 560 39"/><path class="ge" d="M470 75 C515 75 515 111 560 111"/>
      ${n(20, 20, 'orders-db.public.orders', 'postgres · source')}${n(20, 92, 'billing /v2/invoices', 'rest · source')}${n(290, 56, 'analytics.orders', 'bigquery · 41.2M rows', 'sel')}${n(560, 20, 'bi.orders_daily', 'bigquery · 1h ago')}${n(560, 92, 'ml.cashflow_features', 'iceberg · drift', '', 'bad')}
    </svg>`;
  }

  const overviewCards = () => `
    ${card({ icon: 'db', title: '41.2M rows · 23 columns', tag: 'schema v14', right: 'last change today',
      body: grid(4, [kv('Rows per run', '1.24M', spark(VOL, 'var(--text-2)', 150, 30)), kv('Null rate · <span class="mono">discount_code</span>', '0.4%', spark(NUL, 'var(--text-2)', 150, 30, null, 0, 5)), kv('Freshness SLA', '15 min', 'met in 30 of 30 runs'), kv('Consumers', '3', '2 dashboards · 1 model')]),
      note: `<span class="ok">Schema v14</span> · added <span class="mono">discount_code STRING</span> (additive, evolved automatically). No downstream reader uses it yet.` })}
    ${card({ icon: 'lineage', title: 'Lineage', right: `2 up · 2 down · ${lnk('open graph')}`, body: miniGraph() })}
    <div class="two">
      ${card({ icon: 'user', title: 'Owners & consumers', right: btn('Annotate', { kind: 'sm', icon: 'edit', act: true }), body: `<div class="pad">${chip('team-commerce')} ${chip('data-platform')}</div>${tbl(['Consumer', 'Kind', 'Reads'], [[lnk('bi.orders_daily'), 'dashboard', '<span class="mono">order_id, total</span>'], ['cashflow model', 'model', '<span class="mono">total, discount_code</span>'], ['finance export', 'export', 'every column']])}` })}
      ${card({ title: 'Rows per run', aside: 'last 30 runs', right: lnk('runs that wrote it'), body: `<div class="ds-vol" style="padding-top:14px">${[30, 34, 31, 36, 35, 38, 37, 40, 41, 39, 42, 44, 43, 46, 45, 47, 49, 48, 50, 52, 51, 53, 55, 54, 56, 58, 57, 59, 61, 60].map((h, i) => `<i class="${i === 29 ? 'last' : ''}" style="height:${h * 0.85}%"></i>`).join('')}</div>` })}`;

  M.view('datasets', {
    label: 'Datasets', nav: 'datasets', icon: 'db', summary: 'every dataset a pipeline touched: freshness, schema, profile, lineage',
    states: {
      catalog: { label: 'Nothing selected · catalogue health', sel: false, covers: ['list tabs/stats/sections', 'catalogue summary'], new: ['catalogue health summary (stale, drift, no owner)', 'stat cells filter the list'] },
      filters: { label: 'Filters applied · kind menu open', sel: false, covers: ['search', 'kind multi-select', 'last-success date range', 'role tabs'], new: ['filters in the URL with removable chips'] },
      overview: { label: 'Overview tab', covers: ['volume card', 'sparklines', 'schema footnote', 'owners & consumers', 'lineage card', 'volume bars'], new: ['short dataset names; full URI with copy', 'mini lineage graph'] },
      schema: { label: 'Schema · timeline + field selected', covers: ['schema timeline', 'diff badges', 'JSON schema'], new: ['field detail: type history, null rate, who reads it'] },
      compare: { label: 'Schema · compare two versions', covers: ['per-version JSON'], new: ['compare any two schema versions'] },
      profile: { label: 'Profile · drift finding', covers: ['profile summary', 'drift findings', 'per-column table', 'null-rate sparklines', 'top values'] },
      lineage: { label: 'Lineage tab', covers: ['upstream/downstream edges', 'open the graph'], new: ['column lineage per edge on demand'] },
      activity: { label: 'Activity · related runs, templates, changes', new: ['recent runs that wrote/read it', 'templates that write it', 'pending changes that affect it'] },
      outputs: { label: 'Outputs · preview + manage menu', covers: ['local outputs list', 'states present/expired/external/replaced', 'preview rows', 'purge/clean/delete'] },
      annotate: { label: 'Annotate owners & consumers · validation', covers: ['annotate form', 'clear owners'] },
      stale: { label: 'Stale dataset · SLA breached', covers: ['freshness'], new: ['freshness against the usual run cadence'] },
      empty: { label: 'Empty catalogue', sel: false, badges: { datasets: null }, covers: ['empty states'] },
      loading: { label: 'Loading skeleton', covers: [] },
      error: { label: 'Error loading', covers: ['not found / could not load'] },
      viewer: { label: 'Viewer role (read-only)', params: { role: 'viewer' }, covers: ['catalog_annotate / local_output_manage gates'] },
    },
    render(state) {
      const crumbTitle = state === 'stale' ? 'finance.ledger' : state === 'profile' ? 'ml.cashflow_features' : state === 'outputs' ? 'lake/invoices/dt=*' : ['catalog', 'filters', 'empty'].includes(state) ? null : 'analytics.orders';
      M.top(crumbTitle ? ['Data', 'Datasets', crumbTitle] : ['Data', 'Datasets'], { right: crumbTitle === 'analytics.orders' ? chip('owner team-commerce') : crumbTitle === 'lake/invoices/dt=*' ? chip('no owner') : '' });

      if (state === 'catalog' || state === 'filters') {
        M.list(listHtml({ on: null, filters: state === 'filters', kindOpen: state === 'filters' }));
        M.detail(`${dhead('', btn('Lineage graph', { icon: 'lineage' }) + btn('Local outputs', { icon: 'download' }))}
          ${head('Catalogue', ['<span>48 datasets</span>', '<span>31 destinations · 17 sources</span>', '<span>recorded by every successful run</span>'])}
          ${card({ icon: 'clock', title: 'Freshness', right: 'against each dataset’s SLA or usual cadence', body: grid(4, [kv('Fresh', '<span class="ok">45</span>', 'within SLA'), kv('Stale', '<span class="warn">2</span>', 'finance.ledger · 26h'), kv('Never succeeded', '1', 'staging.events_v2'), kv('Median age', '7 min', 'last success')]) })}
          <div class="pair">
            ${card({ icon: 'alert', title: 'Needs attention', right: '4', body: tbl(['Dataset', 'Why', ['Since', 'num']], [[lnk('finance.ledger'), '<span class="warn">SLA 24h breached</span>', '26h'], [lnk('ml.cashflow_features'), '<span class="bad">null rate 0.4% → 11%</span>', '2h'], [lnk('analytics.orders'), 'schema v14 · added column', 'today'], [lnk('staging.events_v2'), 'never succeeded', '3d']]) })}
            ${card({ icon: 'user', title: 'Ownership gaps', right: '9 without an owner', body: tbl(['Dataset', 'Kind', ['Readers', 'num']], [[lnk('lake/invoices/dt=*'), 's3', '2'], [lnk('staging.events_v2'), 'bigquery', '0'], [lnk('ops.audit_export'), 'postgres', '1']]), note: `Impact analysis can only name owners it knows. ${lnk('Annotate')} a dataset to add one.` })}
          </div>
          ${card({ icon: 'layers', title: 'By kind', body: tbl(['Kind', ['Datasets', 'num'], ['Rows last 24h', 'num'], ['Schema changes 7d', 'num']], [['bigquery', '14', '52.1M', '3'], ['postgres', '9', '1.8M', '0'], ['s3', '6', '840k', '1'], ['rest', '11', '96k', '0']]) })}`);
        return;
      }
      if (state === 'empty') {
        M.list(listHtml({ emptyList: true }));
        M.detail(empty('Nothing catalogued yet', 'A dataset appears here after the first successful run that reads or writes it. Every run records its source, destination, schema and row counts.', { icon: 'db', btns: btn('Submit a run', { kind: 'primary', icon: 'submit', act: true }) + btn('How the catalogue works') }));
        return;
      }
      if (state === 'loading') {
        M.list(listHtml({ loading: true }));
        M.detail(`${dhead(dtabs(TABS, 'Overview'))}<div><span class="skel t"></span><span class="skel w30" style="margin-top:10px"></span></div>${skelCards(3)}`);
        return;
      }
      const on = { stale: 'ledger', profile: 'cash', outputs: 'inv' }[state] || 'orders';
      M.list(listHtml({ on }));

      if (state === 'error') {
        M.detail(`${dhead(dtabs(TABS, 'Overview'), '', 'Datasets')}${head('analytics.orders')}
          ${alert('Couldn’t load this dataset.', 'The history store did not answer within 10 s (<span class="mono">GET /v1/catalog/datasets/7c41e0a29b3f…</span>). The list keeps its last good state.', 'bad', btn('Retry', { icon: 'refresh' }) + btn('Copy error', { icon: 'copy', kind: 'quiet' }))}`);
        return;
      }
      if (state === 'overview' || state === 'viewer') {
        M.detail(`${dHead('Overview')}${overviewCards()}`);
        return;
      }
      if (state === 'stale') {
        M.detail(`${dhead(dtabs(TABS, 'Overview'), btn('Annotate', { icon: 'edit', act: true }) + btn('Open in lineage', { icon: 'lineage' }), 'Datasets')}
          ${head('finance.ledger', [pill('Stale · 26h', 'warn', 'clock'), `<span>postgres · written by ${lnk('ledger_nightly')}</span>`, '<span>first seen 210d ago</span>'])}
          ${alert('Freshness SLA breached.', `Last success 26h ago; the SLA is 24h and the usual gap between runs is 23–25h. The last 2 runs of ${lnk('ledger_nightly')} failed with <span class="mono">rate_limited</span>.`, 'warn', btn('Open failed run', { icon: 'runs' }) + btn('Runs that write it', { kind: 'quiet' }))}
          ${card({ icon: 'db', title: '2.1M rows · 18 columns', tag: 'schema v7', right: 'last change 34d ago', body: grid(4, [kv('Rows per run', '18,402', spark([18, 18.2, 18.1, 18.3, 18.4, 18.4], 'var(--text-2)', 150, 30)), kv('Last success', '26h ago', 'Oct 29, 01:12 UTC'), kv('Freshness SLA', '24h', '<span class="warn">missed 1 of last 30</span>'), kv('Consumers', '2', 'bi.ledger_daily · ml model')]), note: '<span class="warn">Downstream at risk</span> · <b>bi.ledger_daily</b> and <b>ml.cashflow_features</b> read this table and are now 26h behind.' })}`);
        return;
      }
      if (state === 'schema' || state === 'compare') {
        const timeline = tbl(['Version', 'Change', 'Columns', 'Run', ['When', 'num']], [
          ['<span class="mono">v14</span>', '<span class="ok">+ <span class="mono">discount_code</span></span> string', '23', `<span class="mono">7f3c…a91e</span>`, 'today'],
          ['<span class="mono">v13</span>', '<span class="info">~ <span class="mono">amount</span></span> integer → number', '22', '<span class="mono">2e81…05bd</span>', '9d ago'],
          ['<span class="mono">v12</span>', '<span class="ok">+ <span class="mono">channel</span></span> string', '22', '<span class="mono">b04c…11fe</span>', '31d ago'],
          ['<span class="mono">v11</span>', '<span class="bad">− <span class="mono">legacy_flag</span></span> removed', '21', '<span class="mono">90aa…3c27</span>', '48d ago'],
        ]);
        if (state === 'schema') {
          M.detail(`${dHead('Schema')}
            <div class="two">
              <div class="stack">
                ${card({ title: 'Schema timeline', right: `14 versions · ${btn('Compare versions', { kind: 'sm', icon: 'diff' })}`, body: timeline })}
                ${card({ title: 'Columns', aside: 'v14 · 23 columns', right: input('Filter columns', { ph: true, icon: 'search' }), body: tbl(['Column', 'Type', 'Nullable', ['Null rate', 'num'], ['Since', 'num']], [
                  ['<span class="mono">order_id</span>', chip('string'), 'no', '0%', 'v1'],
                  { cells: ['<span class="mono">discount_code</span>', chip('string'), 'yes', '0.4%', 'v14'], cls: 'sel' },
                  ['<span class="mono">amount</span>', chip('number'), 'no', '0%', 'v13'],
                  ['<span class="mono">channel</span>', chip('string'), 'yes', '2.1%', 'v12'],
                  ['<span class="mono">created_at</span>', chip('timestamp'), 'no', '0%', 'v1'],
                ]) })}
              </div>
              ${card({ icon: 'columns', title: 'discount_code', tag: 'string · nullable', body: grid(2, [kv('Added in', 'v14', 'today, run 7f3c…a91e'), kv('Null rate', '0.4%', spark(NUL, 'var(--text-2)', 140, 26, null, 0, 5)), kv('Distinct', '1,284', 'top: SPRING10 (8%)'), kv('Masking', 'none', 'no rule matches')]) + `<div class="note">Read by <b>cashflow model</b>. Column lineage: ← ${lnk('orders-db.public.orders', true)}<span class="mono">.discount_code</span> (identity).</div>` })}
            </div>`);
          return;
        }
        M.detail(`${dHead('Schema')}
          ${card({ icon: 'diff', title: 'Compare schema versions', right: `<span class="ds-sel-pair">${select('v11 · 48d ago')}<span class="muted">→</span>${select('v14 · today')}</span>`, body: diff([
            ['@', 'properties'],
            [' ', '  order_id: { type: string }'],
            ['-', '  amount: { type: integer }'],
            ['+', '  amount: { type: number }'],
            ['-', '  legacy_flag: { type: boolean }'],
            ['+', '  channel: { type: [string, "null"] }'],
            ['+', '  discount_code: { type: [string, "null"] }'],
            [' ', '  created_at: { type: string, format: date-time }'],
          ]), note: '<b>3 versions apart</b> · 2 added, 1 widened (integer → number), 1 removed. The removal was breaking for <b>bi.orders_daily</b> at the time.' })}
          ${card({ title: 'Schema timeline', right: '14 versions', body: timeline })}`);
        return;
      }
      if (state === 'profile') {
        const prow = (col, types, nr, nrPts, drift, dist, stat, top) => [`<span class="mono">${col}</span>${drift ? ` ${tag(drift, 'bad')}` : ''}`, types, `${nr} ${spark(nrPts, 'var(--text-2)', 90, 20, drift ? nrPts.length - 1 : null)}`, dist, stat, top];
        M.detail(`${dHead('Profile', { title: 'ml.cashflow_features', sub: [pill('Profile drift', 'bad', 'alert'), `<span>iceberg · written by ${lnk('cashflow_features_build')}</span>`, '<span>first seen 64d ago</span>'] })}
          ${alert('Drift in 1 column.', `<span class="mono">discount_ratio</span> null rate 0.4% → 11.2% (baseline of 20 runs, z = 9.4). Started with run ${lnk('c19d…77a0', true)} 2h ago.`, 'bad', btn('Open that run', { icon: 'runs' }) + btn('Compare with baseline', { kind: 'quiet' }))}
          ${card({ icon: 'columns', title: 'Latest profile', aside: 'run c19d…77a0 · 2h ago', right: 'baseline 20 runs', body: grid(4, [kv('Rows', '412,880'), kv('Columns profiled', '31', '+ 4 skipped beyond max_columns'), kv('Drifting', '<span class="bad">1</span>', 'null_rate'), kv('Stable for', '19 runs', 'before this one')]) })}
          ${card({ title: 'Columns', right: input('Filter columns', { ph: true, icon: 'search' }), body: tbl(['Column', 'Types', 'Null rate', ['Distinct', 'num'], 'Range', 'Top values'], [
            prow('discount_ratio', 'number', '<span class="bad">11.2%</span>', [0.4, 0.3, 0.5, 0.4, 0.4, 0.3, 11.2], 'null rate', '913', '0 – 0.6', '0 (61%) · 0.1 (12%)'),
            prow('account_id', 'string', '0%', [0, 0, 0, 0, 0, 0, 0], '', '84,102', 'len 12', 'high cardinality'),
            prow('amount', 'number', '0%', [0, 0, 0, 0, 0, 0, 0], '', '201,433', '0.01 – 98,410', 'mean 412.6'),
            prow('region', 'string', '0.1%', [0.1, 0.2, 0.1, 0.1, 0.1, 0.2, 0.1], '', '5', 'len 2–4', 'eu (48%) · us (37%)'),
          ]) })}`);
        return;
      }
      if (state === 'lineage') {
        M.detail(`${dHead('Lineage')}
          ${card({ icon: 'lineage', title: 'Neighbours', right: `${btn('Open in lineage', { kind: 'sm', icon: 'focus' })}`, body: miniGraph() })}
          <div class="pair">
            ${card({ title: 'Upstream', right: '2', body: tbl(['Dataset', 'Pipeline', ['Runs', 'num'], ['Last rows', 'num']], [[lnk('orders-db.public.orders'), lnk('orders_cdc_mirror'), '412', '1.24M'], [lnk('billing /v2/invoices'), lnk('invoices_enrich'), '96', '8,210']]) })}
            ${card({ title: 'Downstream', right: '2', body: tbl(['Dataset', 'Pipeline', ['Runs', 'num'], ['Last rows', 'num']], [[lnk('bi.orders_daily'), lnk('orders_rollup'), '30', '1,204'], [lnk('ml.cashflow_features'), lnk('cashflow_features_build'), '64', '412,880']]) })}
          </div>
          ${card({ icon: 'columns', title: 'Column lineage', aside: 'orders_cdc_mirror · orders-db.public.orders → analytics.orders', right: select('orders-db.public.orders'), body: tbl(['Column here', 'From', 'Transform'], [['<span class="mono">order_id</span>', '<span class="mono">id</span>', 'rename'], ['<span class="mono">total</span>', '<span class="mono">amount_cents</span>', 'cast · divide'], ['<span class="mono">email_hash</span>', '<span class="mono">email</span>', 'mask · hash'], ['<span class="mono">discount_code</span>', '<span class="mono">discount_code</span>', 'identity']]) })}`);
        return;
      }
      if (state === 'activity') {
        M.detail(`${dHead('Activity')}
          ${card({ icon: 'runs', title: 'Recent runs that wrote it', right: lnk('all in Runs'), body: tbl(['Run', 'Status', 'Template', ['Rows', 'num'], ['When', 'num']], [
            [lnk('orders_cdc_mirror'), '<span class="info">● running</span>', lnk('faucet-hq/rest-orders@v11'), '1,240,512', '14m'],
            [lnk('orders_cdc_mirror'), '<span class="ok">● completed</span>', lnk('faucet-hq/rest-orders@v11'), '1,198,004', '1h'],
            [lnk('orders_backfill_2026_09'), '<span class="ok">● completed</span>', '—', '18.2M', '3d'],
            [lnk('orders_cdc_mirror'), '<span class="bad">● failed</span>', lnk('faucet-hq/rest-orders@v10'), '0', '6d'],
          ]) })}
          <div class="pair">
            ${card({ icon: 'runs', title: 'Recent runs that read it', body: tbl(['Run', 'Status', ['When', 'num']], [[lnk('orders_rollup'), '<span class="ok">● completed</span>', '1h'], [lnk('cashflow_features_build'), '<span class="ok">● completed</span>', '2h']]) })}
            ${card({ icon: 'tpl', title: 'Templates that write it', body: tbl(['Template', 'Stable', ['Runs 7d', 'num']], [[lnk('faucet-hq/rest-orders'), ch('v11', true), '1,204'], [lnk('acme/orders-backfill'), ch('v3'), '2']]) })}
          </div>
          ${card({ icon: 'changes', title: 'Pending changes that affect it', right: '1', body: tbl(['Change', 'Impact', 'Requested'], [[lnk('Overwrite finance.ledger nightly'), '<span class="warn">breaking · reads entry_id</span>', 'analyst-ana · 3h']]) })}`);
        return;
      }
      if (state === 'outputs') {
        M.detail(`${dHead('Outputs', { title: 'lake/invoices/dt=*', sub: [pill('Fresh · 5m', 'ok', 'check'), `<span>s3 parquet · written by ${lnk('invoices_s3_export')}</span>`, '<span class="mono">s3://lake/invoices/dt=*</span>' + cpi()] })}
          <div class="card">
            <div class="h">${ic('download')}<b>Local outputs</b><span class="muted">2 tracked · cleaned after 7 days</span><span class="r">${toggle('Show cleaned', false)}${btn('Manage', { kind: 'sm open', icon: 'more', act: true })}</span></div>
            ${pop([{ heading: 'Manage local outputs' }, { label: 'Purge older than…', icon: 'clock', hint: '7 days' }, { label: 'Clean this dataset now', icon: 'trash' }, '-', { label: 'Clean every local output', icon: 'trash', cls: 'danger' }], 'right:16px;top:44px')}
            ${tbl(['Path', 'State', 'Written', ['Size', 'num'], ''], [
              { cells: ['<span class="mono">out/invoices/dt=2026-10-30/part-0.parquet</span>', dotLabel('present', 'ok'), '5m ago', '4.1 MiB', btn('Preview', { kind: 'sm' })], cls: 'sel' },
              ['<span class="mono">out/invoices/dt=2026-10-29/part-0.parquet</span>', dotLabel('replaced', ''), '1d ago', '3.9 MiB', btn('Preview', { kind: 'sm' })],
            ])}
          </div>
          ${card({ icon: 'columns', title: 'Preview', aside: 'part-0.parquet · first 5 of 84,210 rows · 38 ms', right: `${input('5', { cls: '' })}${btn('Load', { kind: 'sm' })}${btn('All', { kind: 'sm quiet' })}`, body: `<div style="overflow:hidden">${tbl(['invoice_id', 'customer', 'amount', 'currency', 'issued_at', 'note'], [
            ['INV-20931', 'cust_0412', '1,204.50', 'EUR', '2026-10-30T06:02:11Z', '<span class="null">null</span>'],
            ['INV-20932', 'cust_0098', '88.00', 'USD', '2026-10-30T06:02:14Z', 'expedite'],
            ['INV-20933', 'cust_1170', '412.10', 'EUR', '2026-10-30T06:03:40Z', '<span class="null">—</span>'],
            ['INV-20934', 'cust_0412', '9.99', 'EUR', '2026-10-30T06:04:02Z', '<span class="null">null</span>'],
          ]).replace('<table>', '<table class="ds-pre">')}</div>`, note: '<b>—</b> is a missing field; <b>null</b> is an explicit null. Long values are cut at 240 characters.' })}`);
        return;
      }
      if (state === 'annotate') {
        M.detail(`${dHead('Overview')}
          ${card({ icon: 'edit', title: 'Annotate analytics.orders', aside: 'owners and declared consumers are told when a change to this dataset is planned', body: `<div class="form">
              ${field('Owners', input('team-commerce, data-platform'), { help: 'Comma-separated teams or people.' })}
              ${field('&nbsp;', check('Clear every owner', false))}
              <div class="wide" style="font-weight:600">Add a consumer</div>
              ${field('Name', input('Finance export', {}), { req: true })}
              ${field('Kind', select('export'))}
              ${field('Contact', input('finance@example', { cls: 'err' }), { err: 'Use a team name or a URL; this looks like an incomplete address.' })}
              ${field('Columns read', input('order_id, total, discount', { cls: 'err', mono: true }), { err: '<span class="mono">discount</span> is not a column of v14. Did you mean <span class="mono">discount_code</span>?' })}
            </div>`, note: '' }).replace('<div class="note"></div>', '')}
          <div class="card"><div class="actions" style="border-top:0"><span class="muted">2 fields need attention</span>${btn('Cancel')}${btn('Save annotations', { kind: 'primary' })}</div></div>
          ${overviewCards().split('<div class="two">')[0]}`);
        return;
      }
    },
  });

  function dotLabel(text, tone) { return `<span style="white-space:nowrap"><span class="dot ${tone || 'muted'}"></span>${text}</span>`; }
})();
