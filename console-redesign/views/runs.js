// Runs — split pane: list of runs | run detail.
(function () {
  const RUN = 'orders_cdc_mirror';
  const TOOLS = ibtn('filter') + ibtn('refresh') + ibtn('plus', { act: true });

  // ---- list pane ------------------------------------------------------------
  const R = {
    cdc: (o) => row(ring('info', 'half'), RUN, '14m', '<b>postgres-cdc → bigquery</b> · 1.24M rows · pool cdc', o),
    backfill: (o) => row(ring('info', 'half'), 'payments_events_backfill', '41m', '<b>backfill 2026-09</b> · unit 18 / 30 · rest → snowflake', o),
    kafka: (o) => row(ring('info', 'half'), 'clickstream_kafka', '2h', '<b>kafka → iceberg</b> · exactly-once · 6.8M rows', o),
    queued: (o) => row(ring('muted', 'hollow'), 'ledger_nightly_overwrite', '1m', '<b>queued</b> · waiting for a slot in pool default', o),
    crm: (o) => row(ring('bad'), 'crm_contacts_upsert', '22m', '<b>rate_limited</b> · rest → postgres · gave up after 6 tries', o),
    rollup: (o) => row(ring('bad'), 'warehouse_daily_rollup', '3h', '<b>contract_violation</b> · 12 rows quarantined', o),
    invoices: (o) => row(ring('ok'), 'invoices_s3_to_duckdb', '5m', '<b>s3 → duckdb</b> · 84,210 rows · 38s', o),
    hr: (o) => row(ring('ok'), 'tenant-acme · hr_sync', '9m', '<b>template hr-sync@v7</b> · 3 streams · 2,914 rows', o),
    nightly: (o) => row(ring('ok'), 'orders_nightly_rollup', '3h', '<b>bigquery → bigquery</b> · 412,006 rows · 4m 12s', o),
    cancelled: (o) => row(ring('muted'), 'events_replay_dryrun', '5h', '<b>cancelled</b> by ops-admin · 0 rows', o),
  };
  const STATS = (on, failedOnly) => stats([['Running', '3', 'info'], ['Succeeded', '412', 'ok'], ['Failed', '7', 'bad'], ['Records', '18.4M']], on);

  function listAll({ sel = 'cdc', kb = '', tenant = false } = {}) {
    const o = (k) => ({ on: k === sel, kb: k === kb });
    if (tenant) {
      return `${tabs(['All runs', 'Active', 'Failed'], 'All runs', ibtn('filter') + ibtn('refresh'))}
        ${stats([['Running', '1', 'info'], ['Succeeded', '86', 'ok'], ['Failed', '2', 'bad'], ['Records', '1.2M']])}
        ${sect('Running', 1)}
        ${row(ring('info', 'half'), 'hr_sync', '3m', '<b>template hr-sync@v7</b> · 3 streams · 1,104 rows so far', { on: sel === 'cdc' })}
        ${sect('Failed', 2)}
        ${row(ring('bad'), 'crm_contacts', '1d', '<b>auth_expired</b> · connection crm needs re-auth')}
        ${sect('Succeeded', 86)}
        ${row(ring('ok'), 'hr_sync', '1h', '<b>template hr-sync@v7</b> · 3 streams · 2,914 rows')}
        ${row(ring('ok'), 'billing_invoices', '2h', '<b>template billing-api@v7</b> · 6 streams · 18,330 rows')}`;
    }
    return `${tabs(['All runs', 'Active', 'Failed'], 'All runs', TOOLS)}
      ${STATS()}
      ${sect('Running', 3)}${R.cdc(o('cdc'))}${R.backfill(o('backfill'))}${R.kafka(o('kafka'))}
      ${sect('Queued', 1)}${R.queued(o('queued'))}
      ${sect('Failed', 7)}${R.crm(o('crm'))}${R.rollup(o('rollup'))}
      ${sect('Succeeded', 412)}${R.invoices(o('invoices'))}${R.hr(o('hr'))}`;
  }

  // ---- detail building blocks ------------------------------------------------
  const DTABS = (on) => dtabs(['Overview', ['Invocations', 2], 'Logs', 'Config', 'Dead-letter queue'], on);
  const MORE_BTN = (open) => `<span class="btn ${open ? 'open' : ''} act">${ic('more')}</span>`;
  const RUN_ACTIONS = (open) => btn('Cancel run', { icon: 'x', act: true }) + MORE_BTN(open);
  const runHead = (extra = []) => head(RUN, [pill('Running', 'info', 'play'), `<span>postgres-cdc → bigquery</span>`, `<span>started 14m ago by ${lnk('trigger:orders-wal')}</span>`, ...extra]);
  const TOP = () => M.top(['Operate', 'Runs', RUN], { right: chip('<span class="dot"></span>cluster · 3 live') + chip(`run 7f3c…a91e${cpi()}`, 'mono') });

  const moved = () => card({ icon: 'runs', title: '1,240,512 rows moved', tag: 'exactly-once', aside: 'atomic watermark · token #00000000001842', right: 'updated 4s ago',
    body: grid(4, [kv('Throughput', '1,476 rows/s', 'peak 2,310'), kv('Lag behind source', '3.2s', 'LSN 2F/A81C4E10'), kv('Last bookmark', '6s ago', 'flushed with page 1,842'), kv('Estimated cost', '$0.84', 'bytes streamed 4.1 GiB')]),
    note: `<div style="display:flex;justify-content:space-between"><span>Row accounting · every row offered is accounted for</span><span class="ok">balanced</span></div>${bar([[96.7, 'var(--ok)'], [2.6, 'var(--info)'], [0.5, 'var(--warn)'], [0.2, 'var(--bad)']])}${legend([['accepted 1,199,830', 'var(--ok)'], ['coalesced 32,410', 'var(--info)'], ['filtered 6,210', 'var(--warn)'], ['DLQ 2,062', 'var(--bad)']])}` });

  const touched = () => card({ icon: 'layers', title: 'What this run touches', right: lnk('Lineage'),
    body: tbl(['Dataset', 'Role', ['Rows', 'num'], 'Schema'], [
      [`${lnk('orders-db.public.orders')}`, 'read', '1,024,880', 'v9'],
      [`${lnk('orders-db.public.order_items')}`, 'read', '215,632', 'v4'],
      [`${lnk('analytics.orders')}`, 'written', '1,024,880', '<span class="ok">v14 · +1 column</span>'],
      [`${lnk('analytics.order_items')}`, 'written', '215,632', 'v6'],
    ]) });

  const provenance = () => card({ title: 'Provenance', body: grid(4, [
    kv('Template', `${lnk('faucet-hq/postgres-cdc')} <span class="mono">v12</span>`, 'channel canary'),
    kv('Tenant', lnk('acme'), 'pool cdc'),
    kv('Approved change', lnk('CR-0139'), 'by ops-admin · 2d ago'),
    kv('Idempotency key', `<span class="mono">trig:orders-wal:edge:48</span>${cpi()}`, 'timeout 6h'),
  ]),
    note: `<div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center">${['pipeline=orders', 'team=commerce', 'tier=gold', 'template=faucet-hq/postgres-cdc'].map((l) => chip(l, 'mono')).join('')}<span class="muted" style="margin-left:auto">click a label to filter runs</span></div>` });

  const liveLog = () => card({ icon: 'logs', title: 'Live log', right: `streaming · ${lnk('open Logs')}`, body: `<div class="log">${logl('10:42:07', 'INFO', 'page 1842 written · 680 rows · bookmark committed')}${logl('10:42:05', 'WARN', 'schema drift: added column <b>discount_code</b> (string) — evolved')}${logl('10:42:01', 'INFO', 'page 1841 written · 702 rows · bookmark committed')}</div>` });

  const invSummary = () => card({ title: 'Invocations', right: lnk('all 2 rows'), body: tbl(['Row', 'Status', ['Rows', 'num'], ['Duration', 'num']], [[`<span class="mono">orders</span>`, '<span class="ok">● running</span>', '1,024,880', '14m 02s'], [`<span class="mono">order_items</span>`, '<span class="ok">● running</span>', '215,632', '14m 02s']]) });
  const governance = () => card({ title: 'Governance', right: '5 passes', body: grid(2, [kv('Masking', '2 rules', 'email · card hashed'), kv('Contract', 'v3', '0 breaches'), kv('Schema drift', 'evolve', '+1 column today'), kv('Budget', '41% used', 'max 5M rows')]) });

  function overview({ actions = RUN_ACTIONS(false), viewer = false } = {}) {
    return `${dhead(DTABS('Overview'), actions, 'Runs')}
      ${runHead()}
      ${moved()}
      <div class="two">${invSummary()}${governance()}</div>
      ${touched()}
      ${provenance()}
      ${liveLog()}`;
  }

  // ---- states -------------------------------------------------------------------
  const S = {};
  S.overview = { label: 'Run selected · overview', covers: ['run header', 'output + row accounting', 'invocations summary', 'provenance', 'labels → filter', 'live log tail', 'cancel (run_write)'], new: ['what this run touches (datasets read/written)', 'cross-links: template version, tenant, change, datasets', 'labels as quick filters', 'copy run id'],
    render() { TOP(); M.list(listAll()); M.detail(overview()); } };

  S.summary = { label: 'Nothing selected · summary', sel: false, covers: ['list: tabs, stats, sections, rows', 'detail placeholder'], new: ['useful empty detail: last 24h, needs attention, shortcuts'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(listAll({ sel: '' }));
      M.detail(`${head('Runs', [`<span>last 24 hours</span>`, `<span>${lnk('all time')}</span>`])}
        ${card({ icon: 'runs', title: '418 runs · 18.4M records', right: 'updated 4s ago', body: grid(4, [kv('Success rate', '98.3%', '412 of 419 finished'), kv('Median duration', '1m 48s', 'p95 22m'), kv('Queued now', '1', 'pool default is full'), kv('Estimated cost', '$41.20', lnk('Usage'))]),
          note: `${spark([22, 31, 28, 35, 40, 37, 44, 52, 49, 58, 61, 55, 63, 70], 'var(--text-2)', 640, 40)}` })}
        ${card({ icon: 'alert', title: 'Needs attention', right: '3', body: tbl(['Run', 'Problem', ['Since', 'num'], ''], [
          [lnk('crm_contacts_upsert'), '<span class="bad">● rate_limited</span> · gave up after 6 tries', '22m', btn('Open', { kind: 'sm' })],
          [lnk('warehouse_daily_rollup'), '<span class="bad">● contract_violation</span> · 12 rows in DLQ', '3h', btn('Inspect DLQ', { kind: 'sm' })],
          [lnk('ledger_nightly_overwrite'), '<span class="warn">● queued</span> 1m · pool default full', '1m', btn('Open', { kind: 'sm' })],
        ]) })}
        <div class="muted" style="display:flex;gap:16px;font-size:12px"><span><kbd>j</kbd> <kbd>k</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>/</kbd> search</span><span><kbd>esc</kbd> back to the list</span><span><kbd>⌘K</kbd> go to anything</span></div>`);
    } };

  S.filters = { label: 'Filters open + applied', sel: false, covers: ['status multi-select', 'name', 'since/until', 'label key:value filter', 'apply / clear', 'filters in URL'], new: ['active filter chips', 'pool + tenant filters', 'filters shareable via URL'],
    render() {
      M.top(['Operate', 'Runs'], { right: chip('3 filters') });
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'All runs', `<span class="ibtn on">${ic('filter')}<span class="bd"></span></span>` + ibtn('refresh') + ibtn('plus', { act: true }))}
        <div class="filters">
          ${input('orders', { icon: 'search' })}
          <div class="r2">${select('Status · failed, running')}${select('Pool · any')}</div>
          <div class="r2">${input('Since · 28 Oct, 00:00')}${input('Until', { ph: true })}</div>
          ${input('label key:value — Enter to add', { ph: true, mono: true })}
          <div style="display:flex;gap:8px;justify-content:flex-end">${btn('Clear', { kind: 'quiet' })}${btn('Apply', { kind: 'primary' })}</div>
        </div>
        <div class="active-f">${chip('status: failed, running', 'x')}${chip('name: orders', 'x')}${chip('since 28 Oct', 'x')}${chip('team=commerce', 'mono x')}</div>
        ${sect('Running', 1)}${R.cdc({})}
        ${sect('Failed', 1)}${row(ring('bad'), 'orders_backfill_q3', '2d', '<b>timeout</b> · rest → snowflake · after 6h')}`);
      M.detail(empty('Pick a run', 'Two runs match these filters. Open one to see its invocations, row accounting and live logs.', { icon: 'runs' }));
    } };

  S['stat-filter'] = { label: 'Stat cell used as a filter', sel: false, covers: ['stat strip', 'Failed tab'], new: ['stat cells filter the list'],
    render() {
      M.top(['Operate', 'Runs'], { right: chip('status: failed') });
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'Failed', TOOLS)}
        ${stats([['Running', '3', 'info'], ['Succeeded', '412', 'ok'], ['Failed', '7', 'bad'], ['Records', '18.4M']], 2)}
        <div class="active-f">${chip('status: failed', 'x')}</div>
        ${sect('Failed', 7)}${R.crm({ kb: true })}${R.rollup({})}
        ${row(ring('bad'), 'orders_backfill_q3', '2d', '<b>timeout</b> · rest → snowflake · after 6h')}
        ${row(ring('bad'), 'clickstream_kafka', '3d', '<b>broker_unreachable</b> · kafka → iceberg')}
        ${row(ring('bad'), 'tenant-globex · crm_contacts', '4d', '<b>auth_expired</b> · connection needs re-auth')}
        ${row(ring('bad'), 'ledger_nightly_overwrite', '5d', '<b>budget_exceeded</b> · max_records 2,000,000')}
        ${row(ring('bad'), 'events_dedupe', '6d', '<b>policy_violation</b> · eu-pii denies sink s3')}`);
      M.detail(empty('7 failed runs in the last 7 days', 'Most common: <b>rate_limited</b> (2) and <b>auth_expired</b> (2). Open one to see the error and the failed invocation.', { icon: 'alert' }));
    } };

  S.kb = { label: 'Keyboard focus in the list', sel: false, covers: ['list rows'], new: ['j/k focus ring, Enter opens'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(listAll({ sel: '', kb: 'crm' }));
      M.detail(empty('crm_contacts_upsert', 'Press <kbd>↵</kbd> to open the focused run, <kbd>j</kbd> / <kbd>k</kbd> to move, <kbd>/</kbd> to search.', { icon: 'runs' }));
    } };

  S.empty = { label: 'Empty · no runs yet', sel: false, badges: { runs: null }, covers: ['empty state (none)', 'submit shortcut (config_submit)'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'All runs', TOOLS)}${stats([['Running', '0'], ['Succeeded', '0'], ['Failed', '0'], ['Records', '0']])}
        ${empty('No runs yet', 'Submit a pipeline config, trigger a template, or POST it to <code>/v1/runs</code>.', { icon: 'runs', btns: btn('Submit a run', { kind: 'primary', icon: 'submit', act: true }) + btn('Browse templates') })}`);
      M.detail(empty('Nothing to show yet', 'Runs appear here as soon as one is submitted. Each shows its invocations, row accounting and live logs.', { icon: 'logs' }));
    } };

  S['filtered-empty'] = { label: 'Filters match nothing', sel: false, covers: ['empty state (filtered)'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'All runs', `<span class="ibtn on">${ic('filter')}<span class="bd"></span></span>` + ibtn('refresh'))}
        ${STATS()}
        <div class="active-f">${chip('status: cancelled', 'x')}${chip('pool: cdc', 'x')}${chip('since 29 Oct', 'x')}</div>
        ${empty('No runs match these filters', 'Nothing was cancelled in pool <b>cdc</b> since 29 Oct.', { btns: btn('Clear filters') })}`);
      M.detail(empty('No run selected', '', { icon: 'runs' }));
    } };

  S.loading = { label: 'Loading skeleton', sel: false, covers: ['loading state'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'All runs', TOOLS)}<div class="stats" style="--n:4">${'<div><span class="skel w60"></span><span class="skel" style="margin-top:8px;height:16px;width:40%"></span></div>'.repeat(4)}</div>${skelRows(8)}`);
      M.detail(`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`);
    } };

  S.error = { label: 'Error loading runs', sel: false, covers: ['error handling (toast + retry)'],
    render() {
      M.top(['Operate', 'Runs']);
      M.list(`${tabs(['All runs', 'Active', 'Failed'], 'All runs', TOOLS)}
        ${alert("Couldn't load runs.", 'The server answered 503: run history is degraded (falling back to memory). Showing the last list loaded 2m ago.', 'bad', btn('Retry', { icon: 'refresh' }))}
        ${STATS()}${sect('Running', 3)}${R.cdc({})}${R.backfill({})}${R.kafka({})}`);
      M.detail(empty('Pick a run', 'The list may be stale until the server recovers.', { icon: 'runs' }));
    } };

  S.viewer = { label: 'Viewer role (read-only)', params: { role: 'viewer' }, covers: ['data-perm gating: no cancel/delete/rollback/DLQ actions, no submit'],
    render() { TOP(); M.list(listAll()); M.detail(overview({ actions: chip('read-only') })); } };

  S.tenant = { label: 'Tenant-scoped principal', params: { scope: 'tenant' }, covers: ['tenant switcher pinned', 'tenant-filtered list'],
    render() {
      M.top(['Operate', 'Runs', 'hr_sync']);
      M.list(listAll({ tenant: true }));
      M.detail(`${dhead(dtabs(['Overview', ['Invocations', 3], 'Logs'], 'Overview'), btn('Cancel run', { icon: 'x', act: true }), 'Runs')}
        ${head('hr_sync', [pill('Running', 'info', 'play'), `<span>template ${lnk('hr-sync')} <span class="mono">v7</span></span>`, '<span>started 3m ago by acme-portal</span>'])}
        ${card({ icon: 'runs', title: '1,104 rows moved', aside: '3 streams', right: 'updated 2s ago', body: grid(4, [kv('Streams', '3 of 3 running'), kv('Connection', 'hris', 'active'), kv('Tenant limits', '1 of 3 runs', 'max 1M rows per run'), kv('Estimated cost', '$0.02')]) })}
        ${card({ title: 'Invocations', right: '3 rows', body: tbl(['Stream', 'Status', ['Rows', 'num'], ['Duration', 'num']], [['<span class="mono">employees</span>', '<span class="ok">● running</span>', '804', '3m 01s'], ['<span class="mono">departments</span>', '<span class="ok">● succeeded</span>', '42', '12s'], ['<span class="mono">time_off</span>', '<span class="ok">● running</span>', '258', '3m 01s']]) })}`);
    } };

  S.invocations = { label: 'Invocations tab', covers: ['invocations table: row, parent key, invocation id, records, row accounting, batches, source lag, duration bar, error', 'slowest first'], new: ['copy invocation id', 'row accounting as a bar per invocation'],
    render() {
      TOP(); M.list(listAll());
      const accounting = (ok, extra = '') => `<span class="ok">${ok}</span>${extra}`;
      M.detail(`${dhead(DTABS('Invocations'), RUN_ACTIONS(false), 'Runs')}
        ${runHead()}
        ${card({ title: 'Invocations', aside: 'slowest first', right: select('All statuses'), body: tbl(['Row', ['Records', 'num'], 'Row accounting', 'Batches', ['Lag', 'num'], 'Duration'], [
          [`<span class="mono">orders</span><div class="muted" style="font-size:12px;white-space:nowrap"><span class="mono">01a1…7c2e</span>${cpi()}</div>`, '1,024,880', `<div style="width:150px">${bar([[97.7, 'var(--ok)'], [2.1, 'var(--info)'], [0.2, 'var(--bad)']])}</div><span class="muted" style="font-size:12px">21,506 merged · <span class="bad">2,062 DLQ</span></span>`, '<span style="white-space:nowrap">1,842 · <span class="bad">3 partial</span></span>', '3.2s', `<div style="width:90px">14m 02s${meter(100)}</div>`],
          [`<span class="mono">order_items</span><div class="muted" style="font-size:12px;white-space:nowrap"><span class="mono">01a1…9d04</span>${cpi()}</div>`, '215,632', `<div style="width:150px">${bar([[95, 'var(--ok)'], [5, 'var(--info)']])}</div><span class="muted" style="font-size:12px">10,904 merged</span>`, '402', '1.1s', `<div style="width:90px">14m 02s${meter(100)}</div>`],
        ]), note: 'Row accounting per invocation: offered = ok + merged + skipped + DLQ + quarantined + dropped. Hover a bar for offered, filtered and non-finite counts.' })}
        ${card({ title: 'Matrix fan-out', aside: 'child invocations of row stores', right: '3 of 48', body: tbl(['Row', 'Parent key', ['Records', 'num'], 'Status', 'Duration'], [
          ['<span class="mono">store_orders</span>', '<span class="mono">store=eu-01</span>', '12,402', '<span class="ok">● succeeded</span>', `<div style="width:90px">48s${meter(60)}</div>`],
          ['<span class="mono">store_orders</span>', '<span class="mono">store=eu-02</span>', '9,881', '<span class="ok">● succeeded</span>', `<div style="width:90px">41s${meter(52)}</div>`],
          { cells: ['<span class="mono">store_orders</span>', '<span class="mono">store=us-07</span>', '0', '<span class="bad">● failed</span>', `<div style="width:90px">2s${meter(3)}</div>`], cls: 'hov' },
        ]), note: `<span class="bad">store=us-07</span> · <span class="mono">HTTP 429 after 6 retries (rate_limited)</span> ${lnk('open logs at this invocation')}` })}`);
    } };

  S.logs = { label: 'Logs tab · live', covers: ['SSE log stream', 'auto-scroll', 'truncated/ended/expired states', 'link to log service', 'log export status'], new: ['level filter', 'search', 'follow toggle', 'download'],
    render() {
      TOP(); M.list(listAll());
      const lines = [
        ['10:41:52', 'INFO', 'invocation <span class="mono">orders</span> resumed from bookmark LSN 2F/A81B9910'],
        ['10:41:55', 'INFO', 'page 1839 written · 655 rows · bookmark committed'],
        ['10:41:58', 'INFO', 'page 1840 written · 698 rows · bookmark committed'],
        ['10:42:01', 'INFO', 'page 1841 written · 702 rows · bookmark committed'],
        ['10:42:03', 'WARN', 'sink retry 1/5 after 503 from bigquery insertAll (class http_5xx), backing off 400ms'],
        ['10:42:04', 'INFO', 'sink retry succeeded'],
        ['10:42:05', 'WARN', 'schema drift: added column <b>discount_code</b> (string) — evolved'],
        ['10:42:06', 'ERROR', '3 rows failed contract v3 (amount &lt; 0) → dead-letter queue'],
        ['10:42:07', 'INFO', 'page 1842 written · 680 rows · bookmark committed'],
      ];
      M.detail(`${dhead(DTABS('Logs'), RUN_ACTIONS(false), 'Runs')}
        ${runHead()}
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          ${input('bookmark', { icon: 'search' }).replace('class="input', 'style="width:220px" class="input')}
          <span class="seg"><span class="on">All</span><span>Info</span><span>Warn</span><span>Error</span></span>
          ${toggle('Follow', true)}
          <span style="margin-left:auto;display:flex;gap:8px">${btn('Download', { icon: 'download' })}${btn('Log service', { icon: 'link' })}</span>
        </div>
        ${card({ icon: 'logs', title: 'Live log', tag: 'streaming', aside: '1,842 lines · 2 match', right: 'export · 0 pending · shipped to OTLP', body: `<div class="log">${lines.map((l) => logl(...l)).join('')}<div class="end">following · new lines appear here</div></div>` })}`);
    } };

  S.config = { label: 'Config tab', covers: ['(new) stored config body'], new: ['stored config as YAML with copy', 'Re-run with this config', 'Open in Submit prefilled', 'needs API proposal #1'],
    render() {
      TOP(); M.list(listAll());
      M.detail(`${dhead(DTABS('Config'), btn('Open in Submit', { icon: 'edit', act: true }) + btn('Re-run', { kind: 'primary', icon: 'play', act: true }), 'Runs')}
        ${runHead()}
        ${card({ icon: 'code', title: 'Config this run used', tag: 'redacted', aside: 'secrets and sealed params stay masked', right: `fingerprint <span class="mono">9b1f…c07a</span>${cpi()}`,
          body: code(`version: 1
name: orders_cdc_mirror
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: \${secret:orders-db-url}   # redacted
      slot: faucet_orders
      tables: [public.orders, public.order_items]
  sink:
    type: bigquery
    config:
      project: analytics-prod
      dataset: analytics
      write_mode: upsert
      key: [order_id]
  schema:
    on_drift: evolve
  contract: { ref: orders@v3 }
delivery: exactly_once
budget:
  max_records: 5000000`, { box: false }),
          note: 'Re-run submits this exact config as a new run (needs <b>config_submit</b>). Open in Submit loads it into the guided form so you can change it first.' })}`);
    } };

  S.dlq = { label: 'Dead-letter queue · inspect + replay', covers: ['DLQ location + reason', 'inspect totals / by reason / by error kind / samples', 'discard (dlq_manage)', 'replay through a config (dlq_manage)'], new: ['prefilled with the run\'s own DLQ (API proposal #2)', 'replay targets the same pipeline by default'],
    render() {
      TOP(); M.list(listAll());
      M.detail(`${dhead(DTABS('Dead-letter queue'), btn('Discard…', { kind: 'danger', icon: 'trash', act: true }), 'Runs')}
        ${runHead()}
        ${card({ icon: 'alert', title: '2,062 rows in the dead-letter queue', aside: `<span class="mono">s3://dlq-bucket/orders/</span>`, right: 'inspected 10s ago',
          body: grid(4, [kv('Contract breaches', '2,040', 'amount &lt; 0 · 1,980'), kv('Quality', '22', 'email format'), kv('Partial batches', '0'), kv('Oldest', '14m ago', 'page 1,207')]),
          note: `<div class="pair" style="gap:10px">${field('Location', input('s3://dlq-bucket/orders/', { mono: true }))}${field('Reason', select('all reasons'))}</div>` })}
        ${card({ title: 'Samples', right: '5 of 2,062', body: tbl(['#', 'Reason', 'Error', 'Payload'], [
          ['1', 'contract', '<span class="mono">amount: -12.50 &lt; min 0</span>', `<span class="mono">{"order_id": "o-88213", "amount": -12.5, …}</span>`],
          ['2', 'contract', '<span class="mono">amount: -4.00 &lt; min 0</span>', `<span class="mono">{"order_id": "o-88219", "amount": -4, …}</span>`],
          ['3', 'quality', '<span class="mono">email: not an email</span>', `<span class="mono">{"order_id": "o-88301", "email": "n/a", …}</span>`],
        ]) })}
        ${card({ icon: 'refresh', title: 'Replay', aside: 'send the rows through a pipeline again', cls: 'act',
          body: `<div class="form">${field('Replay through', select('orders_cdc_mirror · this run\'s config'))}${field('Rows', select('contract breaches · 2,040'))}<div class="wide">${check('Dry run first — show what would be written', true)}</div></div>`,
          note: `<div style="display:flex;align-items:center;gap:8px"><span>Rows that fail again go to <span class="mono">s3://dlq-bucket/orders/replay-01/</span></span><span style="margin-left:auto;display:flex;gap:8px">${btn('Dry run')}${btn('Replay 2,040 rows', { kind: 'primary' })}</span></div>` })}`);
    } };

  S.rollback = { label: 'Rollback panel · dry-run report', covers: ['rollback: invocation select, dry run, force, config on 422', 'report: mode, deleted/restored/conflicts, bookmark rewound', 'perm rollback'],
    render() {
      M.top(['Operate', 'Runs', 'orders_nightly_rollup']);
      M.list(listAll({ sel: '' }).replace('orders_nightly_rollup', 'orders_nightly_rollup'));
      M.detail(`${dhead(dtabs(['Overview', ['Invocations', 1], 'Logs', 'Config'], 'Overview'), btn('Roll back…', { icon: 'undo', cls: 'open', act: true }) + btn('Delete', { kind: 'danger', icon: 'trash', act: true }), 'Runs')}
        ${head('orders_nightly_rollup', [pill('Completed', 'ok', 'check'), '<span>bigquery → bigquery</span>', '<span>finished 3h ago · 4m 12s</span>'])}
        ${card({ icon: 'undo', title: 'Roll back this run', aside: 'undo what the run wrote, then rewind the bookmark', cls: 'act',
          body: `<div class="form three">${field('Invocation', select('rollup · 01a1…44f0'))}<div class="field"><label>Options</label><div style="display:flex;gap:16px;height:30px;align-items:center">${check('Dry run', true)}${check('Force past conflicts')}</div></div><div class="field"><label>&nbsp;</label><div style="display:flex;justify-content:flex-end;gap:8px">${btn('Cancel', { kind: 'quiet' })}${btn('Dry run', { kind: 'primary' })}</div></div></div>` })}
        ${card({ icon: 'check', title: 'Dry run · nothing changed yet', tag: 'upsert', right: 'just now',
          body: grid(4, [kv('Rows deleted', '1,204', 'appended by this run'), kv('Rows restored', '410,802', 'from the run journal'), kv('Conflicts', '0', 'no later run touched these keys'), kv('Bookmark', 'rewound', 'to 28 Oct 02:00')]),
          note: `<div style="display:flex;align-items:center"><span><span class="ok">Safe to roll back.</span> The journal holds every key this run wrote; the commit token rewinds to <span class="mono">…1841</span>.</span><span style="margin-left:auto">${btn('Roll back for real', { kind: 'danger' })}</span></div>` })}`);
    } };

  S.failed = { label: 'Failed run', covers: ['error alert: run error + failed invocations', 'retry/attempt', 'delete (terminal)', 'rollback (terminal)'], new: ['copy error', 'jump to the failed invocation\'s logs', 'compare with the last good run'],
    render() {
      M.top(['Operate', 'Runs', 'crm_contacts_upsert']);
      M.list(listAll({ sel: 'crm' }));
      M.detail(`${dhead(dtabs(['Overview', ['Invocations', 4], 'Logs', 'Config'], 'Overview'), btn('Re-run', { icon: 'play', act: true }) + MORE_BTN(false), 'Runs')}
        ${head('crm_contacts_upsert', [pill('Failed', 'bad', 'x'), '<span>rest → postgres</span>', '<span>failed 22m ago after 4m 10s</span>', '<span>attempt 2</span>'])}
        ${alert('rate_limited: gave up after 6 tries', `<div style="margin-top:4px"><span class="mono">HTTP 429 Too Many Requests · retry-after 120s</span>${cpi()}</div><div style="margin-top:4px">Failed invocation: <span class="mono">contacts</span> (page 41). 3 other invocations succeeded and kept their bookmarks.</div>`, 'bad', btn('Open logs at the error', { icon: 'logs' }) + btn('Compare with last good run', { icon: 'diff' }))}
        ${card({ icon: 'runs', title: '18,240 rows moved before the failure', right: 'finished 22m ago', body: grid(4, [kv('Submitted', '10:18:02', '28 Oct 2026'), kv('Started', '10:18:03', 'queued 1s'), kv('Finished', '10:22:13', 'UTC'), kv('Duration', '4m 10s')]),
          note: `<div style="display:flex;justify-content:space-between"><span>Row accounting</span><span class="ok">balanced</span></div>${bar([[100, 'var(--ok)']])}${legend([['written 18,240', 'var(--ok)']])}` })}
        ${card({ title: 'Invocations', right: lnk('all 4 rows'), body: tbl(['Row', 'Status', ['Records', 'num'], ['Duration', 'num']], [
          { cells: ['<span class="mono">contacts</span>', '<span class="bad">● failed</span>', '6,400', '4m 10s'], cls: 'hov' },
          ['<span class="mono">companies</span>', '<span class="ok">● succeeded</span>', '8,812', '2m 01s'],
          ['<span class="mono">deals</span>', '<span class="ok">● succeeded</span>', '2,410', '58s'],
          ['<span class="mono">owners</span>', '<span class="ok">● succeeded</span>', '618', '9s'],
        ]) })}`);
    } };

  S.compare = { label: 'Compare with previous run', covers: ['(new)'], new: ['compare two runs of the same pipeline: records, duration, row accounting, config diff'],
    render() {
      M.top(['Operate', 'Runs', 'crm_contacts_upsert']);
      M.list(listAll({ sel: 'crm' }));
      M.detail(`${dhead(dtabs(['Overview', ['Invocations', 4], 'Logs', 'Config', 'Compare'], 'Compare'), select('vs. 28 Oct 06:00 · succeeded'), 'Runs')}
        ${head('crm_contacts_upsert', [pill('Failed', 'bad', 'x'), `<span>compared with ${lnk('the run 4h earlier')} · succeeded</span>`])}
        ${card({ icon: 'diff', title: 'What changed between the two runs', body: tbl(['', ['This run', 'num'], ['Previous', 'num'], ['Change', 'num']], [
          ['Records written', '18,240', '24,915', '<span class="bad">−27%</span>'],
          ['Duration', '4m 10s', '3m 52s', '+18s'],
          ['Requests (round trips)', '1,204', '911', '<span class="warn">+32%</span>'],
          ['Rate-limited responses', '41', '0', '<span class="bad">+41</span>'],
          ['Invocations failed', '1 of 4', '0 of 4', ''],
        ]) })}
        ${card({ icon: 'code', title: 'Config difference', aside: 'only lines that changed', body: diff([['@', 'pipeline.source.config'], [' ', '  base_url: https://crm.example.test/api'], ['-', '  page_size: 100'], ['+', '  page_size: 500'], ['-', '  request_concurrency: 2'], ['+', '  request_concurrency: 8'], ['@', 'resilience.retry'], [' ', '  max_attempts: 6']]),
          note: 'The page size and concurrency went up 4×; the API started answering 429 at the same time. Lowering <span class="mono">request_concurrency</span> is the likely fix.' })}`);
    } };

  S.menu = { label: '"More" menu open', covers: ['delete (run_write)', 'rollback (rollback)', 'copy link', 'labels filter'], new: ['copy run id / config', 'open lineage of the written dataset'],
    render() {
      TOP(); M.list(listAll());
      const more = `${btn('Cancel run', { icon: 'x', act: true })}<span class="btn open act">${ic('more')}</span>${pop([
        { label: 'Copy run id', icon: 'copy', hint: '7f3c…a91e' }, { label: 'Copy link to this run', icon: 'link' }, { label: 'Copy config as YAML', icon: 'code' }, '-',
        { label: 'Filter runs by this pipeline', icon: 'filter', cls: 'hl' }, { label: 'Open written dataset', icon: 'db' }, { label: 'Open in lineage', icon: 'lineage' }, '-',
        { label: 'Roll back…', icon: 'undo', cls: 'dis', hint: 'when finished' }, { label: 'Delete run', icon: 'trash', cls: 'danger dis', hint: 'when finished' },
      ], 'top:36px;right:0')}`;
      M.detail(overview({ actions: more }));
    } };

  S.cancel = { label: 'Cancel confirmation', covers: ['cancel run (run_write)', 'flush-completing cancel'],
    render() {
      TOP(); M.list(listAll()); M.detail(overview());
      M.overlay(dialog('Cancel orders_cdc_mirror?', `<p>The run stops at the next page boundary and flushes what it has written, so the sink and the bookmark stay consistent. Nothing written so far is undone.</p>${card({ body: grid(2, [kv('Rows moved', '1,240,512'), kv('Last bookmark', '6s ago', 'resumes from here on the next run')]) })}`, btn('Keep running', { kind: 'quiet' }) + btn('Cancel run', { kind: 'danger', icon: 'x' }), 'alert'));
    } };

  S.delete = { label: 'Delete confirmation', covers: ['delete run (run_write, terminal only)'],
    render() {
      M.top(['Operate', 'Runs', 'invoices_s3_to_duckdb']);
      M.list(listAll({ sel: 'invoices' }));
      M.detail(`${dhead(dtabs(['Overview', ['Invocations', 1], 'Logs', 'Config'], 'Overview'), btn('Roll back…', { icon: 'undo', act: true }) + btn('Delete', { kind: 'danger', icon: 'trash', act: true }), 'Runs')}
        ${head('invoices_s3_to_duckdb', [pill('Completed', 'ok', 'check'), '<span>s3 → duckdb</span>', '<span>finished 5m ago · 38s</span>'])}
        ${card({ icon: 'runs', title: '84,210 rows moved', body: grid(4, [kv('Submitted', '10:37:01'), kv('Started', '10:37:01'), kv('Finished', '10:37:39'), kv('Duration', '38s')]) })}`);
      M.overlay(dialog('Delete this run from history?', '<p>Removes the run record, its invocations and its log tail from the server. The data it wrote stays where it is; to undo the data use <b>Roll back</b>.</p>', btn('Keep', { kind: 'quiet' }) + btn('Delete run', { kind: 'danger', icon: 'trash' }), 'trash'));
    } };

  S.toast = { label: 'Toast after an action', params: { overlay: 'toast' }, covers: ['toasts'],
    render() { TOP(); M.list(listAll()); M.detail(overview()); } };

  S.palette = { label: '⌘K palette over a run', params: { overlay: 'palette' }, covers: ['(new) command palette'], new: ['⌘K: runs, datasets, templates, actions'],
    render() { TOP(); M.list(listAll()); M.detail(overview()); } };

  M.view('runs', {
    label: 'Runs', nav: 'runs', icon: 'runs', summary: 'what ran, what is running, what failed',
    states: Object.fromEntries(Object.entries(S).map(([k, v]) => [k, v])),
    render(state) { S[state].render(); },
  });
})();
