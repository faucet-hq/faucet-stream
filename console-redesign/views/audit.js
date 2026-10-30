// Audit log — split pane: who did what on the control plane, newest first.
(function () {
  const E = {
    e1: [ring('ok'), 'run.submit', '2m', '<b>trigger:orders-wal</b> · run orders_cdc_mirror · ok'],
    e2: [ring('bad'), 'template.launch', '9m', '<b>analyst-ana</b> · denied · role operator lacks template_admin'],
    e3: [ring('ok'), 'change.approved', '1h', '<b>ops-admin</b> · CR-0142 · “staging verified”'],
    e4: [ring('ok'), 'template.register', '2h', '<b>sync:template-hub</b> · faucet-hq/rest-orders v12'],
    e5: [ring('warn'), 'connection.needs_reauth', '3h', '<b>runtime</b> · tenant acme · connection crm'],
    e6: [ring('ok'), 'run.cancel', '5h', '<b>ops-admin</b> · run events_replay_dryrun'],
    e7: [ring('ok'), 'tenant.update', 'Wed', '<b>ops-admin</b> · tenant acme · limits'],
    e8: [ring('bad'), 'POST /v1/runs', 'Wed', '<b>token-ci</b> · denied · policy eu-pii refuses sink'],
    e9: [ring('ok'), 'template.rollback', 'Wed', '<b>ops-raj</b> · acme/billing-api v7 → v6'],
  };
  const listPane = ({ sel = 'e2', filtered = false, denials = false } = {}) => {
    const r = (k) => row(...E[k], { on: k === sel, mono: true });
    const active = filtered
      ? `<div class="active-f">${chip('principal: analyst-ana', 'x')}${chip('result: denied', 'x')}<span class="lnk" style="font-size:12px;margin-left:4px">clear</span></div>`
      : '';
    const body = filtered
      ? `${sect('Today', 1)}${r('e2')}${sect('This week', 1)}${row(ring('bad'), 'template.deprecate', 'Mon', '<b>analyst-ana</b> · denied · role operator lacks template_admin', { mono: true })}`
      : denials
        ? `${sect('Today', 1)}${r('e2')}${sect('Wednesday', 1)}${r('e8')}`
        : `${sect('Today', 6)}${['e1', 'e2', 'e3', 'e4', 'e5', 'e6'].map(r).join('')}${sect('Wednesday', 3)}${['e7', 'e8', 'e9'].map(r).join('')}`;
    return `${tabs(['All', 'Denials'], denials ? 'Denials' : 'All', ibtn('filter', { on: filtered }) + ibtn('refresh'))}
      ${stats([['Entries', '1,204'], ['Principals', '14'], ['Denied', '9', 'bad'], ['Mutations', '311']], denials ? 2 : undefined)}
      ${active}${body}<span class="btn quiet more">Load older</span>`;
  };
  const detail = (actions = '') => `${dhead('', `${btn('Copy link', { icon: 'link' })}${actions}`, 'Audit log')}
    ${head('template.launch', [pill('Denied', 'bad', 'x'), `<span>by ${lnk('analyst-ana')} · operator</span>`, '<span>9m ago · 30 Oct 2026, 10:33:51 UTC</span>'])}
    ${alert('Refused before anything changed.', 'The principal’s role <b>operator</b> lacks the <code>template_admin</code> permission this route needs. The request never reached the template registry.', 'bad')}
    ${card({ title: 'Entry', right: `<span class="mono">01a18a61-d075…7d704</span>${cpi()}`, body: grid(3, [
      kv('Principal', lnk('analyst-ana'), 'role operator'),
      kv('Action', '<span class="mono">template.launch</span>', 'route POST /v1/templates/{id}/launch'),
      kv('Result', '<span class="bad">denied</span>'),
      kv('Target', `${lnk('acme/billing-api')} <span class="mono">v8</span>`, 'template version'),
      kv('Tenant', lnk('acme')),
      kv('Run', '<span class="muted">—</span>'),
      kv('Source IP', '<span class="mono">10.0.4.17</span>'),
      kv('Config fingerprint', '<span class="muted">—</span>', 'set on run submits'),
      kv('Detail', '<span class="muted">—</span>'),
    ]) })}
    ${card({ icon: 'user', title: 'More from analyst-ana', right: lnk('filter the list'), body: tbl(['Action', 'Target', 'Result', ['When', 'num']], [
      ['<span class="mono">change.request</span>', lnk('Overwrite finance.ledger nightly'), '<span class="ok">● ok</span>', '3h ago'],
      ['<span class="mono">run.submit</span>', lnk('ledger_staging_check'), '<span class="ok">● ok</span>', '5h ago'],
      ['<span class="mono">template.deprecate</span>', lnk('octo/scratch-sqlite'), '<span class="bad">● denied</span>', 'Mon'],
    ]) })}`;

  M.view('audit', {
    label: 'Audit log', nav: 'audit', icon: 'audit', summary: 'every mutating or denied control-plane action',
    states: {
      entry: { label: 'Denied entry selected', covers: ['GET /v1/audit list', 'every AuditEntry field', 'links to run / template / tenant'], new: ['page did not exist', 'sections by day', 'more from this principal', 'copy id and link'] },
      filtered: { label: 'Quick filters applied', covers: ['principal / action / tenant / since / until filters'], new: ['filters from any clicked value, shown as removable chips'] },
      denials: { label: 'Denials only', covers: ['result filter'], new: ['denials tab (needs API proposal #4: result filter)'] },
      run: { label: 'Run submit entry · links its run', covers: ['run_id', 'config_fingerprint'] },
      none: { label: 'Empty window', sel: false, covers: ['empty state'] },
      forbidden: { label: 'Viewer · admin only (403)', sel: false, shape: 'canvas', params: { role: 'viewer' }, covers: ['audit_read permission'] },
    },
    render(state) {
      if (state === 'forbidden') {
        M.top(['Admin', 'Audit log']);
        M.detail(empty('The audit log is for admins', 'Reading it needs the <code>audit_read</code> permission, which only the admin role has. Ask an admin, or use the Runs and Changes pages for what you started.', { icon: 'shield', btns: btn('Open Runs', { icon: 'runs' }) }));
        return;
      }
      if (state === 'none') {
        M.top(['Admin', 'Audit log']);
        M.list(`${tabs(['All', 'Denials'], 'All', ibtn('filter', { on: true }) + ibtn('refresh'))}${stats([['Entries', '0'], ['Principals', '0'], ['Denied', '0'], ['Mutations', '0']])}<div class="active-f">${chip('since: 30 Oct 2026', 'x')}${chip('action: tenant.delete', 'x')}</div>${empty('Nothing in this window', 'No entry matches. Clear a filter, or widen the dates; entries older than the run-retention window are purged.')}`);
        M.detail(empty('Pick an entry', 'Each entry is one mutating or denied control-plane action, attributed to a principal.', { icon: 'audit' }));
        return;
      }
      M.top(['Admin', 'Audit log', state === 'run' ? 'run.submit' : 'template.launch']);
      if (state === 'run') {
        M.list(listPane({ sel: 'e1' }));
        M.detail(`${dhead('', btn('Copy link', { icon: 'link' }), 'Audit log')}
          ${head('run.submit', [pill('ok', 'ok', 'check'), `<span>by ${lnk('trigger:orders-wal')} · operator</span>`, '<span>2m ago</span>'])}
          ${card({ title: 'Entry', right: `<span class="mono">01a18a72-11b0…c2e9</span>${cpi()}`, body: grid(3, [kv('Principal', lnk('trigger:orders-wal'), 'trigger'), kv('Action', '<span class="mono">run.submit</span>'), kv('Result', '<span class="ok">ok</span>'), kv('Run', lnk('orders_cdc_mirror'), '<span class="mono">7f3c…a91e</span>'), kv('Tenant', '<span class="muted">—</span>'), kv('Config fingerprint', `<span class="mono">sha256:9b1f…44a0</span>${cpi()}`), kv('Source IP', '<span class="mono">10.0.2.4</span>'), kv('Target', '<span class="muted">—</span>'), kv('Detail', '<span class="muted">—</span>')]) })}
          ${card({ icon: 'runs', title: 'The run it started', right: lnk('open run'), body: grid(4, [kv('Status', '<span class="info">running</span>'), kv('Rows', '1.24M'), kv('Pipeline', 'postgres-cdc → bigquery'), kv('Started', '14m ago')]) })}`);
        return;
      }
      M.list(listPane({ sel: 'e2', filtered: state === 'filtered', denials: state === 'denials' }));
      M.detail(detail());
    },
  });
})();
