// Tenants — split pane: customers the server runs pipelines for, their limits and connections.
(function () {
  const ROWS = {
    active: [
      ['acme', ring('ok'), 'Acme Corp', '2m', '<b>2 running</b> · 3 connections · 3 concurrent · 1M rows/run'],
      ['globex', ring('warn', 'half'), 'Globex', '1h', '<b>1 needs re-auth</b> · 2 connections · no limits'],
      ['initech', ring('ok'), 'Initech', '3h', '<b>idle</b> · 1 connection · 2 concurrent'],
      ['umbrella', ring('ok'), 'Umbrella Labs', '1d', '<b>idle</b> · 4 connections · 5 concurrent · 50 GiB/run'],
    ],
    suspended: [['hooli', ring('', 'hollow'), 'Hooli', '12d', '<b>suspended</b> · 1 connection · runs refused']],
  };
  const listPane = (sel) => {
    const r = ([id, ...a]) => row(...a.slice(0, 4), { on: id === sel });
    return `${tabs(['Tenants'], 'Tenants', ibtn('filter') + ibtn('refresh') + ibtn('plus', { act: true }))}
      ${stats([['Tenants', '5'], ['Running', '3', 'info'], ['Need re-auth', '1', 'bad']])}
      ${sect('Active', 4)}${ROWS.active.map(r).join('')}${sect('Suspended', 1)}${ROWS.suspended.map(r).join('')}`;
  };
  const acmeActions = `${btn('Runs for this tenant', { icon: 'runs' })}${btn('Suspend', { kind: 'danger', icon: 'pause', act: true })}`;
  const acmeHead = (on, right = acmeActions) => `${dhead(dtabs(['Overview', ['Connections', 3], 'Activity'], on), right, 'Tenants')}
    ${head('Acme Corp', [pill('Active', 'ok', 'check'), `<span>id <span class="mono">acme</span>${cpi()}</span>`, `<span>created by ${lnk('ops-admin')} · 112d ago</span>`, '<span>2 active runs</span>'])}`;

  const limitsCard = (scoped) => scoped ? card({ title: 'Limits', aside: 'set by the operator', body: grid(4, [kv('Concurrent runs', '3', '2 in use'), kv('Records per run', '1,000,000'), kv('Bytes per run', 'none'), kv('Seconds per run', 'none')]) }) : card({
    title: 'Limits', aside: 'ceilings every run of this tenant honours',
    body: `<div class="form three" style="grid-template-columns:repeat(4,minmax(0,1fr))">
      ${field('Concurrent runs', input(scoped ? '3' : '3'), { help: '2 in use' })}${field('Records per run', input('1,000,000'))}${field('Bytes per run', input('none', { ph: true }))}${field('Seconds per run', input('none', { ph: true }))}</div>`
      + (scoped ? '' : `<div class="actions act"><span class="muted">Blank means no limit. Changes apply to the next run.</span>${btn('Save limits', { kind: 'primary' })}</div>`),
  });
  const usageCard = card({
    icon: 'usage', title: 'Usage', aside: 'last 30 days', right: lnk('open in Usage'),
    body: grid(4, [kv('Runs', '412', '7 failed'), kv('Rows written', '18.4M', spark([3, 4, 3, 5, 6, 5, 7, 6, 8, 7, 9, 8], 'var(--text-2)', 130, 26)), kv('Bytes written', '41.2 GiB'), kv('Estimated cost', '$38.10', 'hosted equivalent $276')]),
  });
  const runsCard = card({
    icon: 'runs', title: 'Recent runs', right: lnk('all runs for acme'),
    body: tbl(['Run', 'Status', ['Rows', 'num'], ['When', 'num']], [
      [lnk('hr_sync'), '<span class="info">● running</span>', '2,914', '2m ago'],
      [lnk('billing_invoices'), '<span class="ok">● completed</span>', '84,210', '1h ago'],
      [lnk('crm_contacts'), '<span class="bad">● failed</span> <span class="muted">rate_limited</span>', '0', '3h ago'],
      [lnk('billing_invoices'), '<span class="ok">● completed</span>', '83,977', '1d ago'],
    ]),
  });
  const changesCard = card({ icon: 'changes', title: 'Pending changes', right: '1', body: `<div class="list" style="border:0;padding:6px 8px 8px">${row(ring('warn', 'hollow'), 'Launch acme/billing-api v8 for acme', '1d', '<b>template launch</b> · 0 of 1 approvals')}</div>` });
  const auditCard = card({ icon: 'audit', title: 'Recent audit', right: lnk('open in Audit log'), body: tbl(['Who and what', ['When', 'num']], [
    [`${lnk('trigger:acme-hourly')}<div class="muted mono">run.submit</div>`, '2m ago'],
    [`${lnk('acme-portal')}<div class="muted mono">connection.needs_reauth</div>`, '3h ago'],
    [`${lnk('ops-admin')}<div class="muted mono">tenant.update</div>`, '2d ago'],
  ]) });
  const labels = card({ title: 'Labels', right: 'set through the API', body: `<div class="pad" style="display:flex;gap:6px;flex-wrap:wrap">${chip('region=eu', 'mono')}${chip('plan=enterprise', 'mono')}${chip('csm=team-north', 'mono')}</div>` });
  const overview = (scoped) => `${usageCard}<div class="two"><div class="stack">${runsCard}${scoped ? '' : changesCard}</div><div class="stack">${labels}${scoped ? '' : auditCard}</div></div>${limitsCard(scoped)}
    ${scoped ? '' : card({ title: 'Delete tenant', aside: 'removes its runs, usage, change requests, state and connections', cls: 'act', body: `<div class="form one" style="grid-template-columns:1fr auto;align-items:end">${field('Type the tenant id to confirm', input('type acme to delete', { ph: true }))}${btn('Delete tenant', { kind: 'danger', cls: 'dis' })}</div>` })}`;

  const connections = (err) => `${card({
    icon: 'key', title: 'Connections', aside: 'credentials are sealed; only names, types and status are shown', right: '3',
    body: tbl(['Name', 'Type', 'Status', 'Updated', ''], [
      ['<span class="mono">crm</span>', 'oauth2_refresh · via provider <span class="mono">crm-cloud</span>', `<span class="warn">● needs re-auth</span><div class="muted" style="font-size:12px">token endpoint returned invalid_grant 3h ago</div>`, `${lnk('acme-portal')} · 3h ago`, `<span class="act" style="display:inline-flex;gap:6px">${btn('Reconnect', { size: '', cls: 'sm', icon: 'refresh' })}${btn('', { cls: 'sm quiet', icon: 'trash' })}</span>`],
      ['<span class="mono">billing</span>', 'static · bearer', '<span class="ok">● active</span>', `${lnk('ops-admin')} · 20d ago`, `<span class="act">${btn('', { cls: 'sm quiet', icon: 'trash' })}</span>`],
      ['<span class="mono">warehouse</span>', 'token_endpoint', '<span class="ok">● active</span>', `${lnk('ops-admin')} · 41d ago`, `<span class="act">${btn('', { cls: 'sm quiet', icon: 'trash' })}</span>`],
    ]),
  })}
  <div class="pair act">
    ${card({ title: 'Connect through a provider', aside: 'opens the provider’s sign-in', body: `<div class="form one">${field('Provider', select('crm-cloud'))}${field('Connection name', input('crm', { mono: true }), { req: true })}${field('Return to', input('this page', { ph: true }), { help: 'Where the provider sends the browser back' })}</div><div class="actions">${btn('Connect', { kind: 'primary', icon: 'link' })}</div>` })}
    ${card({ title: 'Store credentials', aside: 'sealed at rest', body: `<div class="form one">${field('Name', input('erp', { mono: true }), { req: true })}${field('Type', select('static'))}${field('Config', `<div class="area" style="${err ? 'border-color:var(--bad)' : ''}">{\n  "token": "\${secret:ERP_TOKEN}"${err ? '' : ','}\n  "header": "Authorization"\n}</div>`, err ? { err: 'Not valid JSON: expected “,” after line 2.' } : { help: 'JSON · use ${secret:…} references, never raw secrets' })}</div><div class="actions">${btn('Save', { kind: 'primary', cls: err ? 'dis' : '' })}</div>` })}
  </div>`;

  M.view('tenants', {
    label: 'Tenants', nav: 'tenants', icon: 'tenants', summary: 'customers the server runs pipelines for',
    states: {
      overview: { label: 'Tenant selected · overview', covers: ['list', 'stat strip', 'detail head', 'limits form', 'labels', 'recent runs', 'delete with typed id', 'runs for this tenant'], new: ['usage card', 'pending changes', 'recent audit', 'runs link as #/runs?tenant= deep link', 'copy id'] },
      connections: { label: 'Connections · re-auth + forms', covers: ['connections table', 'needs re-auth', 'reconnect', 'delete connection', 'connect provider form', 'store credentials form'] },
      jsonerr: { label: 'Store credentials · JSON error', covers: ['config JSON validated client-side'] },
      create: { label: 'Create tenant', sel: true, covers: ['create tenant form (id, name)'] },
      suspend: { label: 'Suspend confirmation', covers: ['suspend / resume'] },
      del: { label: 'Delete · typed confirmation', covers: ['delete tenant (typed id)', 'deletion report toast'] },
      none: { label: 'No tenants yet (empty)', sel: false, badges: { tenants: null }, covers: ['empty state'] },
      off: { label: 'Tenants feature not built', sel: false, shape: 'canvas', covers: ['404 → feature missing'] },
      scoped: { label: 'Tenant-scoped principal', params: { scope: 'tenant' }, shape: 'canvas', covers: ['scoped principals see only their tenant'] },
    },
    render(state, ctx) {
      if (state === 'off') {
        M.top(['Govern', 'Tenants']);
        M.detail(empty('This server was built without tenants', 'Multi-tenant integrations need the <code>tenants</code> feature and a vault key (<code>--vault-key</code>). The rest of the console works without it.', { icon: 'tenants', btns: btn('Read the embedded integrations guide') }));
        return;
      }
      if (state === 'none') {
        M.top(['Govern', 'Tenants']);
        M.list(`${tabs(['Tenants'], 'Tenants', ibtn('refresh') + ibtn('plus', { act: true }))}${stats([['Tenants', '0'], ['Running', '0'], ['Need re-auth', '0']])}${empty('No tenants yet', 'Create one to scope runs, limits and connections to a customer.', { btns: btn('Create tenant', { kind: 'primary', icon: 'plus', act: true }) })}`);
        M.detail(empty('Run pipelines for your customers', 'A tenant owns sealed connections, per-run limits and its own state namespace. Template runs and triggers can fan out to every tenant.', { icon: 'tenants' }));
        return;
      }
      if (state === 'scoped') {
        M.top(['Govern', 'Tenants', 'Acme Corp']);
        M.detail(`${dhead(dtabs(['Overview', ['Connections', 3]], 'Overview'), btn('Runs', { icon: 'runs' }))}
          ${head('Acme Corp', [pill('Active', 'ok', 'check'), '<span>you are signed in as <b>acme-portal</b> · tenant acme</span>'])}
          ${alert('You see only this tenant.', 'Limits are set by the operator; other tenants, audit and server settings are not shown.')}
          ${overview(true)}`);
        return;
      }
      if (state === 'create') {
        M.top(['Govern', 'Tenants', 'New tenant']);
        M.list(listPane(''));
        M.detail(`${dhead('', '', 'Tenants')}${head('New tenant', ['<span>an id is permanent; the name can change later</span>'])}
          ${card({ title: 'Tenant', body: `<div class="form">${field('Id', input('initech-eu', { mono: true, cls: 'focus' }), { req: true, help: 'Lowercase letters, digits and dashes · used in state keys and URLs' })}${field('Display name', input('Initech Europe'))}</div>`, note: 'Limits, labels and connections can be added after it exists.' })}
          <div style="display:flex;gap:8px;justify-content:flex-end">${btn('Cancel')}${btn('Create tenant', { kind: 'primary', icon: 'plus' })}</div>`);
        return;
      }
      M.top(['Govern', 'Tenants', 'Acme Corp'], { right: chip('2 running') });
      M.list(listPane('acme'));
      if (state === 'connections' || state === 'jsonerr') {
        M.detail(`${acmeHead('Connections')}${connections(state === 'jsonerr')}`);
        return;
      }
      M.detail(`${acmeHead('Overview')}${overview(false)}`);
      if (state === 'suspend') M.overlay(dialog('Suspend Acme Corp?', '<span>New runs, triggers and fan-outs for <b>acme</b> are refused until you resume it. The <b>2 running</b> runs finish; nothing is deleted.</span>', `${btn('Cancel')}${btn('Suspend tenant', { kind: 'danger', icon: 'pause' })}`, 'pause'));
      if (state === 'del') M.overlay(dialog('Delete Acme Corp permanently?', `<span>This removes <b>412 runs</b>, their usage records, <b>1 change request</b>, <b>18 state keys</b> and <b>3 connections</b>. It cannot be undone.</span>${field('Type <code>acme</code> to confirm', input('acme', { mono: true, cls: 'focus' }))}`, `${btn('Cancel')}${btn('Delete tenant', { kind: 'danger', icon: 'trash' })}`, 'trash'));
    },
  });
})();
