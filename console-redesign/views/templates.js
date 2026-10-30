// Templates — split pane organised around the lifecycle (draft → tested →
// launched → channels) and the source × sink pairing. Registry tools (matrix,
// register, sync) live on the registry page shown when nothing is selected.
(function () {
  const ID = 'faucet-hq/rest-orders';
  const S = {
    registry: { label: 'Registry overview · nothing selected', sel: false, covers: ['list pane', 'registry overview', 'stat strip'], new: ['lifecycle funnel', 'needs-attention list'] },
    filtered: { label: 'List filtered · owner, kind, sort', covers: ['search', 'tabs', 'sort', 'include deprecated', 'kind sections'], new: ['owner + kind quick filters', 'filters in the URL'] },
    overview: { label: 'Template · overview (lifecycle)', covers: ['header', 'launch gate', 'channels', 'launch / roll back'], new: ['lifecycle stepper with next action', 'channel map'] },
    versions: { label: 'Versions · menu open', covers: ['versions table', 'channel assign', 'run tests', 'show config', 'deprecate / delete version', 'launch history'], new: ['compare with…'] },
    diff: { label: 'Versions · compare two', covers: ['show config / clean config'], new: ['version diff'] },
    tests: { label: 'Tests · results + history', covers: ['launch gate card', 'test runs', 'failing cases', 'run tests'] },
    run: { label: 'Run · versions, sink, params (error)', covers: ['run form', 'version pickers', 'sink + deployment', 'typed params', 'int validation', 'secret param'] },
    rows: { label: 'Run · streams + resolved set', covers: ['row picker', 'unsupported rows', 'include parents', 'preview', 'allow untested'] },
    pairings: { label: 'Pairings · this template × sinks', covers: ['matrix cell links', 'per-stream write plan'], new: ['pairing tab', 'run into this sink'] },
    matrix: { label: 'Registry · compatibility matrix', covers: ['matrix', 'facet filters', 'cell tooltip'], new: ['selected cell detail'] },
    register: { label: 'Registry · register (YAML error)', covers: ['register form', 'error details'], new: ['line-level error'] },
    newversion: { label: 'New version · seeded editor', covers: ['new version (locked id)'] },
    sync: { label: 'Registry · sync origins + dry run', covers: ['sync panel', 'dry run report'] },
    health: { label: 'Activity · production health per row', covers: ['health tab'] },
    activity: { label: 'Activity · runs, datasets, changes', covers: ['runs (7d)', 'launch history'], new: ['recent runs', 'datasets it produced', 'pending changes'] },
    draft: { label: 'Draft template', covers: ['draft alert'], new: ['next step: run tests'] },
    deprecated: { label: 'Deprecated template', covers: ['deprecated alert', 'revive'] },
    launch: { label: 'Launch · confirmation dialog', covers: ['launch vN'], new: ['impact of a launch'] },
    rollback: { label: 'Roll back · dialog', covers: ['roll back'] },
    toast: { label: 'Launched · toast with undo', covers: ['launch toast'] },
    viewer: { label: 'Viewer role · read-only', params: { role: 'viewer' }, covers: ['permission gates'] },
    loading: { label: 'Loading skeleton', covers: [] },
    empty: { label: 'Empty registry', sel: false, badges: { templates: null }, covers: ['empty states'] },
    error: { label: 'Error loading a template', covers: ['not found / unavailable'] },
  };

  const LIST = [
    ['Source templates', 9, [
      ['ok', '', ID, 'v12', '<b>stable v11</b> · canary v12 · 4 streams · tests passed'],
      ['ok', '', 'acme/billing-api', 'v7', '<b>stable v7</b> · 6 streams · 12 tenants'],
      ['warn', 'hollow', 'acme/ticketing', 'v3', '<b>draft</b> · never launched · tests not run'],
      ['bad', '', 'faucet-hq/graphql-catalog', 'v5', '<b>tests failing</b> · 2 of 31 cases · launch blocked'],
      ['', 'hollow', 'faucet-hq/legacy-csv', 'v3', '<b>deprecated</b> · replaced by faucet-hq/csv'],
    ]],
    ['Sink templates', 5, [
      ['ok', '', 'faucet-hq/bigquery', 'v4', '<b>stable v4</b> · upsert · overwrite → append'],
      ['ok', '', 'faucet-hq/postgres', 'v6', '<b>stable v6</b> · upsert · delete · overwrite'],
      ['ok', '', 'acme/warehouse', 'v2', '<b>stable v2</b> · append · upsert'],
    ]],
    ['Deployments', 3, [
      ['ok', '', 'prod-eu', 'v9', '<b>state postgres</b> · DLQ s3 · SLA 2h'],
    ]],
  ];

  const tools = () => ibtn('filter') + ibtn('matrix') + ibtn('sync', { act: true }) + ibtn('plus', { act: true });

  function listPane({ on = ID, filters = false, loading = false, none = false } = {}) {
    if (loading) return `${tabs(['All', 'Launched', 'Draft'], 'All', tools())}${stats([['Templates', '—'], ['Launched', '—'], ['Draft', '—'], ['Failing', '—']])}${skelRows(8)}`;
    if (none) return `${tabs(['All', 'Launched', 'Draft'], 'All', tools())}${stats([['Templates', '0'], ['Launched', '0'], ['Draft', '0'], ['Failing', '0']])}${empty('No templates yet', 'Register one, or pull the catalogue from a template origin.', { icon: 'tpl' })}`;
    if (filters) {
      return `${tabs(['All', 'Launched', 'Draft'], 'Launched', ibtn('filter', { on: true }) + ibtn('matrix') + ibtn('sync', { act: true }) + ibtn('plus', { act: true }))}
        <div class="filters">${input('rest', { icon: 'search' })}<div class="r2">${select('Owner · faucet-hq')}${select('Sort · last updated')}</div>${check('Include deprecated', false)}</div>
        <div class="active-f"><span class="chip x">launched</span><span class="chip x">owner faucet-hq</span><span class="chip x">“rest”</span><span class="lnk" style="font-size:12px;margin-left:4px">Clear</span></div>
        ${stats([['Matching', '3'], ['Launched', '3', 'ok'], ['Draft', '0'], ['Failing', '0']])}
        ${sect('Source templates', 2)}
        ${row(ring('ok'), ID, 'v12', '<b>stable v11</b> · canary v12 · 4 streams · tests passed', { on: on === ID })}
        ${row(ring('ok'), 'faucet-hq/rest-invoices', 'v4', '<b>stable v4</b> · 2 streams · tests passed')}
        ${sect('Sink templates', 1)}
        ${row(ring('ok'), 'faucet-hq/rest-webhook', 'v2', '<b>stable v2</b> · append')}`;
    }
    return `${tabs(['All', 'Launched', 'Draft'], 'All', tools())}
      ${stats([['Templates', '17'], ['Launched', '13', 'ok'], ['Draft', '3'], ['Failing', '1', 'bad']])}
      ${LIST.map(([s, n, rows]) => sect(s, n) + rows.map(([tone, shape, t, age, meta]) => row(ring(tone, shape), t, age, meta, { on: t === on })).join('')).join('')}`;
  }

  function top(name = ID, kind = 'source-template') {
    M.top(['Build', 'Templates', name], { delta: kind ? `<span class="muted">${kind}</span>` : '', right: name === ID ? chip(`${ic('trig')}3 triggers`) : '' });
  }

  const TABS = ['Overview', 'Versions', 'Tests', 'Run', 'Pairings', 'Activity'];
  const ACT = () => btn('Roll back', { icon: 'undo', act: true }) + btn('Launch v12', { kind: 'primary', icon: 'rocket', act: true }) +
    `<span class="act" style="position:relative">${btn('', { icon: 'more' })}</span>`;
  const headRO = (status = pill('Launched', 'ok', 'check')) =>
    head(ID, [status, '<span>REST source · 4 streams · 3 params</span>', `<span>registered by ${lnk('sync:template-hub')}</span>`]);
  const dh = (tab, right = ACT()) => dhead(dtabs(TABS, tab), right, 'Templates');

  function lifecycle() {
    return card({ icon: 'layers', title: 'Lifecycle of v12', tag: 'newest', aside: 'what it takes to make v12 the stable release', right: 'registered 2h ago',
      body: `${steps([['Registered · v12', 'done'], ['Tested · 31 of 31', 'done'], ['Launch gate · open', 'cur', 3], ['Channels · prod on v11', '', 4]])}
        <div class="tpl-ev">${kv('Registered', 'v12', `2h ago by ${lnk('sync:template-hub')}`)}${kv('Tests', '31 / 31 passed', 'same body sha · faucet 2.4')}${kv('Launch gate', '<span class="ok">open</span>', 'tests pass on this version')}${kv('Stable today', 'v11', 'launched 6d ago by ops-admin')}</div>`,
      note: `<div class="tpl-next"><span><span class="ok">Next step: launch v12.</span> Unpinned triggers keep resolving <b>v11</b> until you do.</span>${btn('Launch v12', { kind: 'primary', icon: 'rocket', act: true })}</div>` });
  }
  function channels() {
    return card({ title: 'Channels', aside: 'what each name resolves to', right: `<span class="act">${btn('Assign', { cls: 'sm', icon: 'plus' })}</span>`,
      body: tbl(['Channel', 'Version', 'Since'], [
        ['stable', ch('v11', true), '6d ago'], ['prod', ch('v11'), '6d ago'], ['canary', ch('v12'), '2h ago'],
        ['staging', ch('v12'), '2h ago'], ['previous', ch('v10'), '19d ago'], ['dev · test · pre-prod', '<span class="faint">unassigned</span>', ''],
      ]) });
  }
  function pairingSummary() {
    return card({ icon: 'matrix', title: 'Pairs with', aside: 'source × sink', right: lnk('all pairings'),
      body: tbl(['Sink', 'Streams', ''], [
        [lnk('faucet-hq/bigquery'), '<span class="ok">4 / 4</span>', 'upsert'],
        [lnk('faucet-hq/postgres'), '<span class="ok">4 / 4</span>', 'upsert · delete'],
        [lnk('acme/warehouse'), '<span class="warn">3 / 4</span>', '1 needs keys'],
        [lnk('faucet-hq/jsonl'), '<span class="ok">4 / 4</span>', 'append'],
      ]) });
  }

  function overview() {
    return `${dh('Overview')}${headRO()}${lifecycle()}<div class="pair">${channels()}${pairingSummary()}</div>`;
  }

  function launched() {
    return overview()
      .replace(/\['Launch gate · open', 'cur', 3\], \['Channels · prod on v11', '', 4\]/, '')
      .replace('Launch gate · open', 'Launched · v12').replace('Channels · prod on v11', 'Channels · prod on v11')
      .split(btn('Launch v12', { kind: 'primary', icon: 'rocket', act: true })).join('')
      .replace('<span class="ok">Next step: launch v12.</span> Unpinned triggers keep resolving <b>v11</b> until you do.', '<span class="ok">v12 is stable.</span> Unpinned triggers resolve <b>v12</b> from their next start. Next: move <b>prod</b> to v12 when canary looks good.')
      .replace(`${ch('v11', true)}</td><td class="">6d ago`, `${ch('v12', true)}</td><td class="">just now`)
      .replace('launched 6d ago by ops-admin', 'launched just now by ops-admin').replace('<b>v11</b><div class="s">launched just now', '<b>v12</b><div class="s">launched just now').replace('<span class="step cur"><span class="sn">3</span>', `<span class="step done"><span class="sn">${ic('check')}</span>`).replace('<span class="step-line "></span><span class="step "><span class="sn">4</span>', '<span class="step-line done"></span><span class="step cur"><span class="sn">4</span>');
  }

  function versions(menuOpen = true) {
    const menu = menuOpen ? pop([
      { heading: 'v12 · canary · newest' },
      { label: 'Run v12', icon: 'play' }, { label: 'Run tests', icon: 'flask', hint: 'last passed 2h ago' }, { label: 'Compare with stable v11', icon: 'diff', cls: 'hl' },
      { label: 'Show config', icon: 'code' }, { label: 'Show clean config', icon: 'code' }, '-',
      { heading: 'Assign channel' },
      `<div style="display:flex;gap:4px;flex-wrap:wrap;padding:4px 10px 6px">${['dev', 'test', 'staging', 'pre-prod', 'prod'].map((c) => ch(c)).join('')}</div>`, '-',
      { label: 'Deprecate v12', icon: 'pause' }, { label: 'Delete v12', icon: 'trash', cls: 'danger' },
    ], 'right:16px;top:124px;width:260px') : '';
    const rowsV = [
      ['<span class="mono">v12</span>', `<div class="tpl-chips">${ch('canary')}${ch('staging')}${ch('newest')}</div>`, '<span class="ok">● passed</span>', '2h ago · sync', '38', `<span class="act">${btn('Run', { cls: 'sm', icon: 'play' })} ${btn('', { cls: 'sm open', icon: 'more' })}</span>`],
      ['<span class="mono">v11</span>', `<div class="tpl-chips">${ch('stable', true)}${ch('prod')}</div>`, '<span class="ok">● passed</span>', '6d ago · sync', '1,204', `<span class="act">${btn('Run', { cls: 'sm', icon: 'play' })} ${btn('', { cls: 'sm', icon: 'more' })}</span>`],
      ['<span class="mono">v10</span>', ch('previous'), '<span class="muted">● stale</span>', '19d ago · ops-admin', '0', `<span class="act">${btn('Run', { cls: 'sm', icon: 'play' })} ${btn('', { cls: 'sm', icon: 'more' })}</span>`],
      ['<span class="mono faint">v9</span>', tag('deprecated'), '<span class="muted">—</span>', '34d ago · sync', '0', `<span class="act">${btn('', { cls: 'sm', icon: 'more' })}</span>`],
    ];
    return `${dh('Versions')}${headRO()}
      ${card({ title: 'Versions', aside: 'newest first', right: `${btn('Compare', { cls: 'sm', icon: 'diff' })}<span class="act">${btn('New version', { cls: 'sm', icon: 'plus' })}</span>`,
        body: tbl(['Version', 'Channels', 'Tests', 'Registered', ['Runs (7d)', 'num'], ['', 'num']], rowsV) + menu })}
      ${card({ title: 'Launch history', right: '4 launches', body: `<div class="log">${['<span class="muted">6d ago</span>&nbsp; v11 launched by <b>ops-admin</b> · tests passed', '<span class="muted">19d ago</span>&nbsp; v10 launched by <b>sync:template-hub</b> · tests passed', '<span class="muted">26d ago</span>&nbsp; v9 rolled back to v8 by <b>ops-admin</b> · reason “pagination regression”'].map((l) => `<div>${l}</div>`).join('')}</div>` })}`;
  }

  function diffView() {
    return `${dh('Versions')}${headRO()}
      ${card({ icon: 'diff', title: 'Compare versions', right: lnk('back to versions'),
        body: `<div class="tpl-bar">${select('v11 · stable')}<span class="muted">→</span>${select('v12 · canary')}${toggle('Clean config (comments and order ignored)', true)}<span class="r"><span class="muted">+5 −2 lines</span>${btn('Copy v12', { cls: 'sm', icon: 'copy' })}</span></div>
        ${diff([['@', 'streams:'], [' ', '  - name: orders'], [' ', '    write: [upsert, append]'], [' ', '    primary_keys: [order_id]'], ['-', '    page_size: 100'], ['+', '    page_size: ${param.page_size}'], [' ', '  - name: order_items'], ['@', '…'], ['+', '  - name: refunds'], ['+', '    source: { ref: default, config: { path: /v2/refunds } }'], ['+', '    write: [append]'], ['@', 'params:'], ['-', '  page_size: { type: int, default: 100 }'], ['+', '  page_size: { type: int, default: 250, description: rows per request }']])}`,
        note: '<b>What changed:</b> one stream added (refunds), page size became a parameter with a higher default. Parameters otherwise unchanged; no sink changes needed.' })}`;
  }

  function testsView() {
    return `${dh('Tests')}${headRO()}
      ${card({ icon: 'flask', title: 'v12 passed 31 of 31 cases', tag: 'same body sha', aside: 'faucet 2.4', right: `tested 2h ago<span class="act">${btn('Run tests', { cls: 'sm', icon: 'play' })}</span>`,
        body: grid(4, [kv('Fixture cases', '18 / 18'), kv('Parameter sweep', '9 / 9', 'all-pairs, 3 params'), kv('Shared suites', 'rest-contract@1.4'), kv('Pairings checked', '4 sinks', 'bigquery · postgres · warehouse · jsonl')]),
        note: '<span class="ok">Launch gate open</span> · v12 can become stable. A result counts only for the same body and the running faucet major.' })}
      ${card({ title: 'Test runs', aside: 'all versions', right: '6 runs',
        body: `<table><tr><th>When</th><th>Version</th><th>Result</th><th class="num">Cases</th><th class="num">Retried</th><th>Faucet</th><th>By</th></tr>
          <tr><td>2h ago</td><td class="mono">v12</td><td><span class="ok">● passed</span></td><td class="num">31 / 31</td><td class="num">0</td><td>2.4.0</td><td>sync:template-hub</td></tr>
          <tr class="sel"><td>3h ago</td><td class="mono">v12</td><td><span class="bad">● failed</span></td><td class="num">29 / 31</td><td class="num">1</td><td>2.4.0</td><td>ops-admin</td></tr>
          <tr class="tpl-sub"><td></td><td colspan="6"><ul><li><span class="mono">fixtures/refunds_pagination</span>: expected 3 pages, got 2 (cursor dropped on the last page)</li><li><span class="mono">sweep page_size=1000</span>: HTTP 400 limit must be ≤ 500</li></ul></td></tr>
          <tr><td>6d ago</td><td class="mono">v11</td><td><span class="ok">● passed</span></td><td class="num">27 / 27</td><td class="num">0</td><td>2.4.0</td><td>sync:template-hub</td></tr>
          <tr><td>19d ago</td><td class="mono">v10</td><td><span class="ok">● passed</span></td><td class="num">27 / 27</td><td class="num">2</td><td>2.3.1</td><td>ops-admin</td></tr></table>` })}`;
  }

  function runForm() {
    return `${dh('Run', btn('Copy as API call', { icon: 'code' }) + btn('Run v11', { kind: 'primary', icon: 'play', act: true }))}${headRO()}
      ${card({ icon: 'tpl', title: 'What to run', aside: 'source, sink and deployment', body: `<div class="form tpl-five">${field('Source version', select('stable · v11'), { help: 'resolved when the run starts' })}${field('Sink template', select('faucet-hq/bigquery'), { req: true })}${field('Sink version', select('stable · v4'))}${field('Deployment overlay', select('prod-eu · v9'), { help: 'state postgres · DLQ s3' })}${field('Run name', input('rest-orders check'), { help: 'optional' })}</div>` })}
      <div class="two">
        ${card({ title: 'Parameters', aside: 'from the source, sink and overlay', right: '6 params',
          body: `<div class="form">${field('api_root', input('https://api.example.com/v2'), { code: true, req: true, help: 'base URL of the orders API' })}${field('api_token', input('••••••••••••'), { code: true, req: true, help: '<span class="tag">secret</span> never stored in the run record' })}${field('page_size', input('two hundred', { cls: 'err' }), { code: true, err: 'must be an integer between 1 and 500' })}${field('start_date', input('2026-01-01', { ph: true }), { code: true, help: 'default 2026-01-01' })}${field('dataset', input('analytics'), { code: true, help: 'from sink faucet-hq/bigquery' })}${field('include_archived', toggle('Include archived orders', false), { code: true })}</div>`,
          note: '<span class="bad">1 field needs attention</span> · page_size' })}
        ${card({ icon: 'code', title: 'Request', aside: 'POST …/runs', body: code(`version: stable\nsink: faucet-hq/bigquery\nsink_version: stable\noverlay: prod-eu\nname: rest-orders check\nparams:\n  api_root: "https://api.example.com/v2"\n  api_token: "••••"\n  page_size: two hundred   # invalid\n  dataset: analytics`, { box: false }) })}
      </div>`;
  }

  function rowsView() {
    const pick = (on, name, meta, why = '', cls = '') => `<div class="tpl-pick ${cls}">${check('', on)}<span><span class="mono">${name}</span> <span class="muted" style="font-size:12px">${meta}</span></span><span>${why ? '' : ''}</span>${why ? `<span class="why">${why}</span>` : ''}</div>`;
    return `${dh('Run', btn('Run v10 anyway', { kind: 'primary', icon: 'play', act: true }))}${headRO()}
      <div class="two">
        ${card({ title: 'Streams to run', aside: 'into faucet-hq/bigquery', right: `${btn('Defaults', { cls: 'sm' })}${btn('All', { cls: 'sm' })}${btn('None', { cls: 'sm' })}`,
          body: `<div class="tpl-bar">${select('Include parents · when eligible')}<span class="r muted">3 of 5 selected</span></div>
            ${pick(true, 'orders', 'upsert · key order_id · healthy')}
            ${pick(true, 'order_items', 'upsert · child of orders', '', 'child')}
            ${pick(true, 'refunds', 'append · new in v12')}
            ${pick(false, 'customers', 'upsert · after orders')}
            ${pick(false, 'audit_events', 'unsupported', 'bigquery cannot upsert audit_events: the stream declares no primary_keys', 'dis')}` })}
        <div class="stack">
          ${card({ title: 'Resolved run set', right: '4 rows', body: tbl(['Row', 'Why', 'Write'], [['<span class="mono">orders</span>', 'selected', 'upsert'], ['<span class="mono">order_items</span>', 'selected', 'upsert'], ['<span class="mono">refunds</span>', 'selected', 'append'], ['<span class="mono">customers</span>', '<span class="info">pulled in</span>', 'upsert']]) })}
          ${alert('v10 has no passing test result on faucet 2.4.', 'Its tests were recorded on 2.3. You can run it anyway; the run is labelled <code>allow_untested</code> and audited.', 'warn', check('Run anyway', true))}
        </div>
      </div>`;
  }

  function pairings() {
    const plan = (items) => `<div class="tpl-chips">${items.map(([s, m, t]) => `<span class="chip ${t || ''}"><span class="mono">${s}</span> ${m}</span>`).join('')}</div>`;
    return `${dh('Pairings')}${headRO()}
      ${card({ icon: 'matrix', title: 'Sinks this template can write to', aside: 'per-stream write plan', right: lnk('open the full matrix'),
        body: tbl(['Sink template', 'Plan per stream', 'Fit', ''], [
          [`${lnk('faucet-hq/bigquery')}<div class="muted" style="font-size:12px">stable v4 · bigquery</div>`, plan([['orders', 'upsert'], ['order_items', 'upsert'], ['refunds', 'append'], ['customers', 'upsert']]), '<span class="ok">● 4 / 4</span>', `<span class="act">${btn('Run into this sink', { cls: 'sm', icon: 'play' })}</span>`],
          [`${lnk('faucet-hq/postgres')}<div class="muted" style="font-size:12px">stable v6 · postgres</div>`, plan([['orders', 'upsert'], ['order_items', 'upsert'], ['refunds', 'append'], ['customers', 'upsert']]), '<span class="ok">● 4 / 4</span>', `<span class="act">${btn('Run into this sink', { cls: 'sm', icon: 'play' })}</span>`],
          [`${lnk('acme/warehouse')}<div class="muted" style="font-size:12px">stable v2 · snowflake</div>`, plan([['orders', 'upsert'], ['order_items', 'upsert'], ['refunds', 'append'], ['customers', '<span class="warn">no keys</span>']]), '<span class="warn">● 3 / 4</span>', `<span class="act">${btn('Run 3 streams', { cls: 'sm', icon: 'play' })}</span>`],
          [`${lnk('faucet-hq/jsonl')}<div class="muted" style="font-size:12px">stable v3 · file</div>`, plan([['orders', 'append ← upsert'], ['order_items', 'append ← upsert'], ['refunds', 'append'], ['customers', 'append ← upsert']]), '<span class="ok">● 4 / 4</span>', `<span class="act">${btn('Run into this sink', { cls: 'sm', icon: 'play' })}</span>`],
          [`${lnk('acme/lake-parquet')}<div class="muted" style="font-size:12px">draft v1 · s3</div>`, '<span class="muted">sink has no stable version</span>', '<span class="faint">● —</span>', ''],
        ]),
        note: '<b>append ← upsert</b> means the sink aliases the requested mode. A stream the sink cannot write is left out of the run, never written wrongly.' })}`;
  }

  function registryDetail(tab, body, actions = '', sub = ['<span>17 templates · 9 source · 5 sink · 3 deployments</span>', `<span>synced from ${lnk('template-hub')} 12m ago</span>`]) {
    return `${dhead(dtabs(['Overview', 'Matrix', 'Register', 'Sync'], tab), actions)}${head('Template registry', sub)}${body}`;
  }

  function registry() {
    const funnel = [['Draft', '3', 'registered, never launched'], ['Tested', '2', 'gate open, waiting to launch'], ['Launched', '13', 'stable version live'], ['On channels', '9', 'prod or canary assigned']];
    return registryDetail('Overview', `
      ${card({ icon: 'layers', title: 'Lifecycle', aside: 'where every template stands', body: `<div class="tpl-funnel">${funnel.map(([k, v, s], i) => `<div><small>${k}</small><b>${v}</b>${meter([18, 12, 76, 53][i])}<span class="muted" style="font-size:12px">${s}</span></div>`).join('')}</div>` })}
      ${card({ icon: 'alert', title: 'Needs attention', right: '4', body: tbl(['Template', 'What', ''], [
        [lnk('faucet-hq/graphql-catalog'), '<span class="bad">● tests failing</span> · 2 of 31 cases · launch blocked', lnk('see failures')],
        [lnk(ID), '<span class="ok">● ready to launch</span> · v12 passed 31 of 31', `<span class="act">${btn('Launch v12', { cls: 'sm', icon: 'rocket' })}</span>`],
        [lnk('acme/ticketing'), '<span class="warn">● draft</span> · never launched, tests not run', `<span class="act">${btn('Run tests', { cls: 'sm', icon: 'flask' })}</span>`],
        [lnk('faucet-hq/legacy-csv'), '<span class="muted">● deprecated</span> · still used by 2 triggers', lnk('show triggers')],
      ]) })}
      ${card({ title: 'Recent launches', body: `<div class="log"><div><span class="muted">2h ago</span>&nbsp; acme/billing-api v7 launched by <b>ops-raj</b></div><div><span class="muted">6d ago</span>&nbsp; faucet-hq/rest-orders v11 launched by <b>ops-admin</b></div><div><span class="muted">9d ago</span>&nbsp; faucet-hq/postgres v6 launched by <b>sync:template-hub</b></div></div>` })}`,
      btn('Register', { icon: 'plus', act: true }) + btn('Pull from origins', { icon: 'sync', act: true }));
  }

  function matrix() {
    const c = (t, cls = '') => `<span class="cell ${cls}">${t}</span>`;
    return registryDetail('Matrix', `
      ${card({ icon: 'matrix', title: 'Source × sink compatibility', aside: 'streams a sink can write', right: '7 of 9 sources × 4 of 5 sinks',
        body: `<div class="tpl-facets">${select('Source owner · any')}${select('Source type · rest, graphql')}${select('Sink owner · any')}${select('Sink type · any')}<span class="chip x">source type: rest, graphql</span><span class="r">${lnk('Clear filters')}</span></div>
          <div class="mx">${tbl(['Source ↓ · sink →', 'faucet-hq/bigquery', 'faucet-hq/postgres', 'acme/warehouse', 'faucet-hq/jsonl'], [
            [`${lnk(ID)} <span class="muted" style="font-size:12px">rest · 4 streams</span>`, c('✓ 4 / 4'), c('✓ 4 / 4'), c('3 / 4', 'warn'), c('✓ 4 / 4')],
            [`${lnk('acme/billing-api')} <span class="muted" style="font-size:12px">rest · 6</span>`, c('✓ 6 / 6'), c('✓ 6 / 6'), c('✓ 6 / 6'), c('✓ 6 / 6')],
            [`${lnk('acme/ticketing')} <span class="muted" style="font-size:12px">rest · 3</span>`, c('2 / 3', 'sel'), c('✓ 3 / 3'), c('✗', 'no'), c('✓ 3 / 3')],
            [`${lnk('faucet-hq/graphql-catalog')} <span class="muted" style="font-size:12px">graphql · 5</span>`, c('✓ 5 / 5'), c('✓ 5 / 5'), c('✓ 5 / 5'), c('✓ 5 / 5')],
            [`${lnk('faucet-hq/rest-invoices')} <span class="muted" style="font-size:12px">rest · 2</span>`, c('✓ 2 / 2'), c('✓ 2 / 2'), c('—', 'no'), c('✓ 2 / 2')],
          ])}</div>` })}
      ${card({ title: 'acme/ticketing → faucet-hq/bigquery', tag: '2 of 3 streams', right: `<span class="act">${btn('Run 2 streams', { cls: 'sm', icon: 'play' })}</span>`,
        body: tbl(['Stream', 'Requested', 'Resolved', ''], [['<span class="mono">tickets</span>', 'upsert', 'upsert', '<span class="ok">● ok</span>'], ['<span class="mono">comments</span>', 'append', 'append', '<span class="ok">● ok</span>'], ['<span class="mono">sla_events</span>', 'upsert', '—', '<span class="bad">● no primary_keys declared</span>']]) })}`);
  }

  function register() {
    return registryDetail('Register', `
      ${card({ icon: 'plus', title: 'Register a template', aside: 'source-template, sink-template, deployment or pipeline', right: `<span class="seg"><span class="on">YAML</span><span>JSON</span></span>`,
        body: `<div class="form">${field('Id', input('acme/orders-v2', {}), { help: 'optional · defaults to <code>owner/name</code> from the document' })}${field('Description', input('Orders, items and refunds from the v2 API'))}
          <div class="field wide tpl-ed"><label>Document</label><div class="area">${['<span class="k">kind</span>: source-template', '<span class="k">name</span>: orders-v2', '<span class="k">owner</span>: acme', '<span class="k">params</span>:', '  <span class="k">api_root</span>: { type: string, required: true }', '<span class="k">source</span>:', '  <span class="k">type</span>: rest', '  <span class="k">config</span>: { base_url: "${param.api_root}" }', '<span class="k">streams</span>:', '  - <span class="k">name</span>: orders', '    <span class="k">write</span>: [upsert, append]', '    <span class="k">primary_keys</span>: [order_id]', '  - <span class="k">name</span>: refunds', '<span class="errl">    <span class="k">write</span>: [merge]                 <span class="c"># line 14</span></span>'].join('\n')}</div></div></div>
          <div style="padding:0 16px 14px">${alert('Line 14 · streams[1].write', '<code>merge</code> is not a write mode. Use one of <code>append</code>, <code>upsert</code>, <code>delete</code>, <code>overwrite</code>.', 'bad')}</div>`,
        note: `<div class="tpl-next">${toggle('Run its tests after registering', true)}${toggle('Launch if the tests pass', false)}${btn('Register', { kind: 'primary', icon: 'plus' })}</div>` })}`);
  }

  function newVersion() {
    return `${dh('Versions', btn('Cancel') + btn('Register v13', { kind: 'primary', icon: 'plus' }))}${head('New version of faucet-hq/rest-orders', [pill('v13 · draft'), '<span>seeded from newest v12</span>'])}
      ${card({ icon: 'edit', title: 'Document', tag: 'id locked', right: `${lnk('compare with v12')}`,
        body: `<div class="form">${field('Id', input('<span class="mono">faucet-hq/rest-orders</span>', { cls: 'dis' }), { help: 'a new version keeps the id; register a new template to change it' })}${field('Description', input('Orders, items, refunds and customers from the REST API'))}
          <div class="field wide tpl-ed"><label>Body</label><div class="area">${['<span class="k">kind</span>: source-template', '<span class="k">name</span>: rest-orders', '<span class="k">owner</span>: faucet-hq', '<span class="k">params</span>:', '  <span class="k">api_root</span>: { type: string, required: true }', '  <span class="k">api_token</span>: { type: string, secret: true }', '  <span class="k">page_size</span>: { type: int, default: 250 }', '<span class="k">streams</span>:', '  - <span class="k">name</span>: orders', '    <span class="k">write</span>: [upsert, append]', '    <span class="k">primary_keys</span>: [order_id]', '  - <span class="k">name</span>: refunds', '    <span class="k">write</span>: [append]', '<span class="c">  # …</span>'].join('\n')}</div></div></div>`,
        note: '<span class="muted">Registering appends v13 and moves nobody: stable stays v11 until you launch.</span>' })}`;
  }

  function sync() {
    return registryDetail('Sync', `
      ${card({ icon: 'sync', title: 'Origins', aside: 'where templates are pulled from', right: `${select('All origins')}<span class="act">${btn('Dry run', { cls: 'sm' })}${btn('Pull now', { cls: 'sm', icon: 'sync' })}</span>`,
        body: tbl(['Origin', 'Kind', 'Prefix', 'Launch', 'Prune', 'Every', 'Last pull'], [
          ['template-hub', 'github', '<span class="mono">faucet-hq/</span>', 'follow', 'deprecate', '15m', '12m ago · <span class="ok">ok</span>'],
          ['acme-bucket', 's3', '<span class="mono">acme/</span>', 'ignore', 'keep', '1h', '41m ago · <span class="warn">1 warning</span>'],
        ]) })}
      ${card({ title: 'Dry run · template-hub', tag: 'nothing changed', right: 'just now',
        body: tbl(['Action', 'Template', 'Detail'], [
          ['<span class="ok">register</span>', lnk('faucet-hq/rest-orders'), 'after v12 → v13, then launch (policy follow)'],
          ['<span class="info">launch</span>', lnk('faucet-hq/postgres'), 'v7 (sidecar launch: true)'],
          ['<span class="warn">deprecate</span>', lnk('faucet-hq/legacy-csv'), 'removed from the origin'],
          ['skipped', '<span class="mono">faucet-hq/broken.yaml</span>', '<span class="bad">invalid YAML at line 3</span>'],
          ['unchanged', '<span class="muted">12 templates</span>', ''],
        ]),
        note: `<div class="tpl-next"><span>Pulling would make <b>3 changes</b>. One file would be skipped.</span>${btn('Pull now', { kind: 'primary', icon: 'sync', act: true })}</div>` })}`);
  }

  function health() {
    return `${dh('Activity')}${headRO()}
      ${card({ icon: 'shield', title: 'Stable v11 in production', tag: 'status read', right: 'checked 30s ago',
        body: tbl(['Row', 'Health', 'Last success', 'Bookmark', ['Lag', 'num'], ['DLQ', 'num'], 'Next resume'], [
          ['<span class="mono">orders</span>', '<span class="ok">● healthy</span>', '6m ago', '<span class="mono">updated_at 10:12</span>', '6m', '0', 'bookmark'],
          ['<span class="mono">order_items</span>', '<span class="ok">● healthy</span>', '6m ago', '<span class="mono">parent cursor</span>', '6m', '0', 'bookmark'],
          ['<span class="mono">refunds</span>', '<span class="warn">● lagging</span>', '3h ago', '<span class="mono">updated_at 07:02</span>', '3h 10m', '14', 'bookmark'],
          ['<span class="mono">customers</span>', '<span class="faint">● never ran</span>', '—', '—', '—', '—', 'full read'],
        ]),
        note: '<span class="warn">refunds is 3h behind</span> · 14 rows in its dead-letter queue. ' + lnk('Open the last run') })}`;
  }

  function activity() {
    return `${dh('Activity')}${headRO()}
      ${card({ icon: 'runs', title: 'Recent runs', aside: 'every version, newest first', right: lnk('all 1,242 in Runs'), body: tbl(['Run', 'Version', 'Status', 'Sink', ['Rows', 'num'], 'When'], [
        [lnk('rest-orders · acme'), '<span class="mono">v11</span>', '<span class="ok">● succeeded</span>', 'faucet-hq/bigquery', '84,210', '6m ago'],
        [lnk('rest-orders canary'), '<span class="mono">v12</span>', '<span class="info">● running</span>', 'faucet-hq/bigquery', '3,118', '2m ago'],
        [lnk('rest-orders · globex'), '<span class="mono">v11</span>', '<span class="ok">● succeeded</span>', 'faucet-hq/postgres', '12,904', '21m ago'],
        [lnk('rest-orders · initech'), '<span class="mono">v11</span>', '<span class="bad">● failed</span>', 'acme/warehouse', '0', '1h ago'],
      ]) })}
      <div class="pair">
        ${card({ icon: 'db', title: 'Datasets it produces', right: '4', body: tbl(['Dataset', 'Last success'], [[lnk('analytics.orders'), '6m ago'], [lnk('analytics.order_items'), '6m ago'], [lnk('analytics.refunds'), '<span class="warn">3h ago</span>'], [lnk('analytics.customers'), '<span class="faint">never</span>']]) })}
        ${card({ icon: 'changes', title: 'Pending changes', right: '1', body: tbl(['Change', 'Approvals'], [[`${lnk('Launch v12')}<div class="muted" style="font-size:12px">ops-raj · 1h ago</div>`, '1 of 2']]),
          note: `Triggers using it: ${lnk('orders-hourly')}, ${lnk('orders-webhook')} on stable · ${lnk('orders-canary')} on canary` })}
      </div>`;
  }

  function draft() {
    return `${dhead(dtabs(TABS, 'Overview'), btn('Run tests', { kind: 'primary', icon: 'flask', act: true }) + `<span class="act">${btn('', { icon: 'more' })}</span>`, 'Templates')}
      ${head('acme/ticketing', [pill('Draft · never launched', 'warn'), '<span>REST source · 3 streams · 2 params</span>', `<span>registered by ${lnk('ops-raj')} 2d ago</span>`])}
      ${alert('Unpinned runs and triggers are refused until a version is launched.', 'Pin a version (for example <code>v3</code>) to try it, or run its tests and launch it.', 'warn')}
      ${card({ icon: 'layers', title: 'Lifecycle of v3', tag: 'newest', right: 'registered 2d ago',
        body: `${steps([['Registered · v3', 'done'], ['Tests · not run', 'cur', 2], ['Launch gate', '', 3], ['Channels', '', 4]])}<div class="tpl-ev">${kv('Registered', 'v3', '2d ago by ops-raj')}${kv('Tests', '<span class="muted">not run</span>', '14 fixture cases, 1 suite')}${kv('Launch gate', '<span class="muted">waiting</span>', 'needs a passing result')}${kv('Stable today', '—', 'none launched')}</div>`,
        note: `<div class="tpl-next"><span><b>Next step: run the tests.</b> A passing result opens the launch gate.</span>${btn('Run tests', { kind: 'primary', icon: 'flask', act: true })}</div>` })}
      ${card({ title: 'Versions', right: '3', body: tbl(['Version', 'Channels', 'Tests', 'Registered'], [['<span class="mono">v3</span>', ch('newest'), '<span class="muted">● not run</span>', '2d ago · ops-raj'], ['<span class="mono">v2</span>', '', '<span class="bad">● failed</span>', '5d ago · ops-raj'], ['<span class="mono">v1</span>', '', '<span class="bad">● failed</span>', '6d ago · ops-raj']]) })}`;
  }

  function deprecated() {
    return `${dhead(dtabs(TABS, 'Overview'), btn('Revive', { icon: 'undo', act: true }), 'Templates')}
      ${head('faucet-hq/legacy-csv', [pill('Deprecated', '', 'pause'), '<span>file source · 1 stream</span>', '<span>stable v3</span>'])}
      ${alert('Deprecated 5d ago by ops-admin: replaced by faucet-hq/csv.', 'Unpinned runs still resolve stable <b>v3</b> and warn. Deleting the template stops them.', '')}
      ${card({ title: 'Still in use', right: '2 triggers', body: tbl(['Trigger', 'Resolves', 'Last fire'], [[lnk('legacy-import'), 'stable → v3', '3h ago'], [lnk('vendor-drop'), 'pinned v2', '1d ago']]),
        note: `Move them to ${lnk('faucet-hq/csv')} before deleting.` })}
      ${card({ title: 'Versions', right: '3', body: tbl(['Version', 'Channels', 'Tests', 'Registered'], [['<span class="mono">v3</span>', ch('stable', true), '<span class="ok">● passed</span>', '81d ago'], ['<span class="mono">v2</span>', ch('previous'), '<span class="ok">● passed</span>', '120d ago'], ['<span class="mono faint">v1</span>', tag('deprecated'), '—', '200d ago']]) })}`;
  }

  M.view('templates', {
    label: 'Templates', nav: 'templates', icon: 'tpl', summary: 'lifecycle, pairing, registry tools',
    states: S,
    render(state) {
      const name = { draft: 'acme/ticketing', deprecated: 'faucet-hq/legacy-csv', registry: '', empty: '', matrix: '', register: '', sync: '' }[state];
      const on = name === undefined ? ID : name;
      const regLike = ['registry', 'empty', 'matrix', 'register', 'sync'].includes(state);
      if (regLike) M.top(['Build', 'Templates', ...(state === 'registry' || state === 'empty' ? [] : [{ matrix: 'Matrix', register: 'Register', sync: 'Sync' }[state]])]);
      else if (state === 'loading' || state === 'error') M.top(['Build', 'Templates', ID]);
      else top(on, state === 'draft' || state === 'deprecated' ? 'source-template' : 'source-template');
      M.list(listPane({ on, filters: state === 'filtered', loading: state === 'loading', none: state === 'empty' }));
      const body = {
        registry, matrix, register, sync, filtered: overview, overview, versions, diff: diffView, tests: testsView, run: runForm, rows: rowsView,
        pairings, newversion: newVersion, health, activity, draft, deprecated, launch: overview, rollback: overview, toast: launched, viewer: overview,
        loading: () => `${dhead('<span class="skel" style="width:280px;height:26px"></span>')}<div><span class="skel t"></span><div style="height:10px"></div><span class="skel w30"></span></div>${skelCards(3)}`,
        empty: () => registryDetail('Overview', empty('Register your first template', 'A template is a reusable source, sink, deployment or pipeline with typed parameters. Register one here, or pull a catalogue from an origin with <code>faucet serve --templates-sync</code>.', { icon: 'tpl', btns: btn('Register a template', { kind: 'primary', icon: 'plus' }) + btn('Browse the hub', { icon: 'link' }) }), '', ['<span>no templates</span>', '<span>no sync origins configured</span>']),
        error: () => `${dh('Overview', '')}${head(ID, [pill('Unknown', '')])}${alert('Couldn’t load this template.', 'The server answered <code>503</code>: the template store is unavailable. The list on the left is from the last good load.', 'bad', btn('Retry', { icon: 'refresh' }))}`,
      }[state];
      M.detail(body());
      if (state === 'launch') M.overlay(dialog('Launch v12 of faucet-hq/rest-orders?', `${grid(2, [kv('Stable', 'v11 → v12'), kv('Tests', '<span class="ok">31 / 31 passed</span>'), kv('Follows stable', '3 triggers', '12 tenants run it unpinned'), kv('Undo', 'Roll back', 'returns stable to v11')]).replace('class="grid2"', 'class="grid2" style="padding:0"')}<span class="muted">Runs already started keep their version. New runs resolve v12 from the next start.</span>`, btn('Cancel') + btn('Launch v12', { kind: 'primary', icon: 'rocket' }), 'rocket'));
      if (state === 'rollback') M.overlay(dialog('Roll back faucet-hq/rest-orders to v10?', `<span>Stable moves from <b>v11</b> to the previous launch, <b>v10</b>. The launch log keeps both entries.</span>${field('Reason', input('pagination regression in v11', {}), { help: 'shown in the launch history and the audit log' })}`, btn('Cancel') + btn('Roll back to v10', { kind: 'danger', icon: 'undo' }), 'undo'));
      if (state === 'toast') M.overlay(toast(`${ic('check')}<span><b>v12 launched.</b> Stable is now v12 for 3 triggers.</span> <span class="lnk">Undo</span>`, 'ok'));
    },
  });
})();
