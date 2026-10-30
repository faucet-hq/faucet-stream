// Overview — the home page (canvas): what needs an operator's attention now.
(function () {
  const S = {};

  const attention = () => card({ icon: 'alert', title: 'Needs attention', right: '5', body: tbl(['', 'What', 'Where', ['Since', 'num'], ''], [
    ['<span class="bad">●</span>', `${lnk('crm_contacts_upsert')} failed · rate_limited`, 'run', '22m', btn('Open', { kind: 'sm' })],
    ['<span class="bad">●</span>', `${lnk('ml.cashflow_features')} null rate 0.4% → 11%`, 'dataset', '2h', btn('Open', { kind: 'sm' })],
    ['<span class="warn">●</span>', `${lnk('finance.ledger')} stale · SLA 24h breached`, 'dataset', '26h', btn('Open', { kind: 'sm' })],
    ['<span class="warn">●</span>', `${lnk('Overwrite finance.ledger')} awaits you`, 'change', '3h', btn('Review', { kind: 'sm' })],
    ['<span class="warn">●</span>', `${lnk('globex')} connection <span class="mono">crm</span> needs re-auth`, 'tenant', '1d', btn('Open', { kind: 'sm' })],
  ]) });

  const running = () => card({ icon: 'runs', title: 'Running now', right: lnk('All runs'), body: `<div style="padding:6px 6px 8px">
    ${row(ring('info', 'half'), 'orders_cdc_mirror', '14m', '<b>postgres-cdc → bigquery</b> · 1,476 rows/s · lag 3.2s')}
    ${row(ring('info', 'half'), 'payments_events_backfill', '41m', '<b>backfill 2026-09</b> · unit 18 / 30')}
    ${row(ring('info', 'half'), 'clickstream_kafka', '2h', '<b>kafka → iceberg</b> · 6.8M rows')}
    ${row(ring('muted', 'hollow'), 'ledger_nightly_overwrite', '1m', '<b>queued</b> · pool default full')}</div>` });

  const activity = () => card({ icon: 'usage', title: 'Last 24 hours', right: lnk('Usage'), body: grid(4, [
    kv('Runs', '418', spark([12, 18, 15, 22, 19, 24, 21, 26, 28, 25, 30, 27], 'var(--text-2)', 140, 26)),
    kv('Success rate', '98.3%', '412 of 419 finished'),
    kv('Records written', '18.4M', spark([1.1, 1.4, 1.2, 1.6, 1.5, 1.8, 1.7, 1.9, 2.2, 2.0, 2.4, 2.3], 'var(--text-2)', 140, 26)),
    kv('Estimated cost', '$41.20', 'this week $212.85'),
  ]) });

  const freshness = () => card({ icon: 'db', title: 'Data freshness', right: lnk('Datasets'), body: grid(3, [
    kv('Fresh', '<span class="ok">45</span>', 'within their SLA'),
    kv('Stale', '<span class="warn">2</span>', `${lnk('finance.ledger')} · ${lnk('crm.accounts')}`),
    kv('Drifted', '<span class="bad">1</span>', lnk('ml.cashflow_features')),
  ]), note: 'Schema changes today: <b>analytics.orders v14</b> (+discount_code, additive).' });

  const changes = () => card({ icon: 'changes', title: 'Change requests', right: lnk('Changes'), body: `<div style="padding:6px 6px 8px">
    ${row(ring('warn', 'hollow'), 'Overwrite finance.ledger nightly', '3h', '<b>needs you</b> · 1 of 2 approvals · by analyst-ana')}
    ${row(ring('warn', 'hollow'), 'Launch acme/billing-api v8', '1d', '<b>template launch</b> · tests passed · by ops-raj')}
    ${row(ring('ok'), 'Register faucet-hq/bigquery v4', '2d', '<b>executed</b> · approved by ops-admin')}</div>` });

  const health = () => card({ icon: 'server', title: 'Server health', right: lnk('Settings'), body: tbl(['Part', 'State', 'Detail'], [
    ['History', '<span class="ok">● healthy</span>', 'postgres'],
    ['Run queue', '<span class="ok">● accepting</span>', '3 of 24 slots busy'],
    ['Cluster', '<span class="ok">● 3 live</span>', 'pools default · cdc · backfill'],
    ['Triggers', '<span class="warn">● 1 of 4 degraded</span>', `${lnk('orders-arrival')} · listing failed 3×`],
    ['Log shipping', '<span class="ok">● shipping</span>', '0 lines pending'],
  ]) });

  const templates = () => card({ icon: 'tpl', title: 'Templates', right: lnk('Templates'), body: grid(2, [
    kv('Launched', '13', '3 draft · 1 failing tests'),
    kv('Ready to launch', '2', `${lnk('faucet-hq/rest-orders')} v12 · ${lnk('acme/ticketing')} v3`),
  ]) });

  S.home = { label: 'Home · needs attention first', covers: ['shell status (readyz)', 'runs running/failed', 'pending changes', 'dataset freshness/drift', 'trigger health', 'usage', 'GET /v1/overview (#1006) or the existing list endpoints'], new: ['Overview home page', 'one "needs attention" queue across runs, datasets, changes, tenants', 'every item links to its page'],
    render() {
      M.top(['Operate', 'Overview'], { right: chip('<span class="dot"></span>cluster · 3 live') });
      M.detail(`${dhead(dtabs(['Today', 'This week'], 'Today'), btn('Submit a run', { icon: 'submit', act: true }))}
        ${head('Good morning, ops-admin', ['<span>prod-eu · 3 replicas</span>', '<span>5 things need attention</span>'])}
        ${activity()}
        <div class="two">${attention()}${running()}</div>
        <div class="two">${freshness()}${changes()}</div>
        <div class="two">${health()}${templates()}</div>`);
    } };

  S['first-run'] = { label: 'First run · empty server', badges: false, covers: ['empty states'], new: ['getting-started checklist'],
    render() {
      M.top(['Operate', 'Overview']);
      M.detail(`${head('Welcome to faucet', ['<span>this server has not run anything yet</span>'])}
        ${card({ icon: 'rocket', title: 'Get your first pipeline running', right: '0 of 4 done', body: steps([['Pick a source and sink', 'cur'], ['Validate the config', ''], ['Run it', ''], ['Watch it in Runs', '']]),
          note: `<div style="display:flex;gap:8px">${btn('Submit a run', { kind: 'primary', icon: 'submit' })}${btn('Start from a template', { icon: 'tpl' })}${btn('Browse connectors', { icon: 'schema' })}</div>` })}
        <div class="pair">
          ${card({ icon: 'tpl', title: 'Templates', body: empty('No templates registered', 'Register one, or point <code>faucet serve --templates-sync</code> at a repo.', { btns: btn('Register a template') }) })}
          ${card({ icon: 'db', title: 'Datasets', body: empty('Nothing catalogued yet', 'Every dataset a run reads or writes shows up here with its schema, freshness and lineage.') })}
        </div>`);
    } };

  S.loading = { label: 'Loading skeleton', covers: ['loading state'],
    render() {
      M.top(['Operate', 'Overview']);
      M.detail(`<span class="skel t"></span><span class="skel w30"></span><div class="skel-card"><span class="skel w30"></span><div class="grid4" style="padding:0">${'<div><span class="skel w60"></span><span class="skel" style="margin-top:8px;height:16px;width:50%"></span></div>'.repeat(4)}</div></div><div class="two">${skelCards(1)}${skelCards(1)}</div><div class="two">${skelCards(1)}${skelCards(1)}</div>`);
    } };

  M.view('overview', {
    label: 'Overview', nav: 'overview', icon: 'home', summary: 'the home page: what needs attention now', shape: 'canvas',
    states: S,
    render(state) { S[state].render(); },
  });
})();
