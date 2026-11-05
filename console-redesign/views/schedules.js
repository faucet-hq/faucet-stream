// Schedules (#1044) — split pane: time-driven runs, separate from event triggers.
// A schedule runs a registered template (version or channel + params) on a cron.
// Console-created schedules live in run history (every replica sees them);
// `--schedules <file>` ones are read-only here. Tenant principals manage their
// own tenant's schedules within max_schedules and a minimum tick interval.
(function () {
  const S = {
    broken: ['broken', ring('bad'), 'invoice-digest', '1d', '<b>template unresolved</b> · acme/invoices@prod · 3 failed ticks'],
    hourly: ['hourly', ring('ok'), 'hourly-crm-sync', 'in 14m', '<b>15 * * * *</b> · UTC · tenant acme'],
    nightly: ['nightly', ring('ok'), 'nightly-rollup', 'in 6h', '<b>0 2 * * *</b> · Europe/Berlin · 6 tenants'],
    usage: ['usage', ring('ok'), 'usage-snapshot', 'in 41m', '<b>*/30 * * * *</b> · UTC · this server'],
    acmeorders: ['acmeorders', ring('ok'), 'acme-daily-orders', 'in 3h', '<b>0 5 * * *</b> · America/New_York · tenant acme'],
    weekly: ['weekly', ring('ok'), 'weekly-ledger-export', '4d', '<b>0 6 * * MON</b> · Europe/Berlin · from schedules file'],
    partner: ['partner', ring('', 'hollow'), 'partner-sftp-pull', '—', '<b>paused</b> · */30 * * * * · by ops-raj 2d ago'],
  };
  const TEN = {
    hourly: ['hourly', ring('ok'), 'hourly-crm-sync', 'in 14m', '<b>15 * * * *</b> · UTC · by acme-portal'],
    acmeorders: ['acmeorders', ring('ok'), 'acme-daily-orders', 'in 3h', '<b>0 5 * * *</b> · America/New_York · by acme-portal'],
    acmeinv: ['acmeinv', ring('ok'), 'acme-invoice-pull', 'in 52m', '<b>0 * * * *</b> · UTC · by acme-portal'],
    acmeweekly: ['acmeweekly', ring('', 'hollow'), 'acme-weekly-report', '—', '<b>paused</b> · 0 7 * * MON · by acme-portal'],
    acmecrm: ['acmecrm', ring('ok'), 'acme-crm-contacts', 'in 2h', '<b>0 */4 * * *</b> · UTC · by acme-portal'],
  };
  const r = (sel) => ([id, ...a]) => row(...a, { on: id === sel });
  const SH = {
    billing: ['billing', ring('ok'), 'billing-sync', 'in 2h', '<b>shared</b> · runs for 3 tenants · 0 */6 * * *'],
    nightly: ['nightly', ring('ok'), 'nightly-rollup', 'in 6h', '<b>shared</b> · runs for 6 tenants · 0 2 * * *'],
  };
  const SHT = {
    billing: ['billing', ring('ok'), 'billing-sync', 'in 2h', '<b>shared</b> · by the operator · read only'],
    nightly: ['nightly', ring('ok'), 'nightly-rollup', 'in 6h', '<b>shared</b> · by the operator · read only'],
  };
  const scopedPane = (sel) => `${tabs([['All', 4], ['Paused', 0]], 'All', ibtn('refresh') + btn('New', { icon: 'plus', kind: 'primary', act: true }))}
      ${stats([['For acme', '4'], ['Ticks today', '19'], ['Missed', '0'], ['Next tick', 'in 14m']])}
      ${sect('acme’s own', 2)}${[S.hourly, S.acmeorders].map(r(sel)).join('')}
      ${sect('Shared · include acme', 2)}${[SH.billing, SH.nightly].map(r(sel)).join('')}
      <div class="muted" style="font-size:12px;padding:10px 14px">Showing schedules that run for acme. Pick “All tenants” above to see all 8.</div>`;
  const newBtn = (dis) => btn('New', { icon: 'plus', kind: 'primary', act: true, cls: dis ? 'dis' : '' });
  const listPane = (sel, { tenant = false, limit = false } = {}) => {
    if (tenant) {
      const rows = limit ? [TEN.hourly, TEN.acmeinv, TEN.acmecrm, TEN.acmeorders, TEN.acmeweekly] : [TEN.hourly, TEN.acmeinv, TEN.acmeorders, TEN.acmeweekly];
      const active = rows.filter((x) => x !== TEN.acmeweekly);
      return `${tabs([['All', rows.length], ['Paused', 1]], 'All', ibtn('refresh') + newBtn(limit))}
        ${stats([['Schedules', `${rows.length} of 5`, limit ? 'warn' : ''], ['Ticks today', '31'], ['Missed', '0'], ['Next tick', 'in 14m']])}
        ${sect('Active', active.length)}${active.map(r(sel)).join('')}${sect('Paused', 1)}${r(sel)(TEN.acmeweekly)}
        ${sect('From the operator', 2)}${[SHT.billing, SHT.nightly].map(r(sel)).join('')}
        <div class="muted" style="font-size:12px;padding:10px 14px">Shared schedules are managed by the server operator and do not count toward acme’s limit.</div>`;
    }
    return `${tabs([['All', 8], ['Paused', 1]], 'All', ibtn('refresh') + newBtn())}
      ${stats([['Schedules', '8'], ['Ticks today', '128'], ['Missed', '3', 'bad'], ['Next tick', 'in 14m']])}
      ${sect('Needs attention', 1)}${r(sel)(S.broken)}
      ${sect('Active', 6)}${[S.hourly, S.usage, SH.billing, S.acmeorders, S.nightly, S.weekly].map(r(sel)).join('')}
      ${sect('Paused', 1)}${r(sel)(S.partner)}`;
  };

  const actions = ({ paused = false, file = false } = {}) => file
    ? btn('Run now', { icon: 'play', act: true })
    : `${btn('Run now', { icon: 'play', act: true })}${paused ? btn('Resume', { icon: 'play', kind: 'primary', act: true }) : btn('Pause', { icon: 'pause', act: true })}${btn('Edit', { icon: 'edit', act: true })}${ibtn('more', { act: true })}`;
  const dtab = (on, { runs = 186, missed = 0 } = {}) => dtabs(['Overview', ['Runs', runs], ['Missed ticks', missed], 'History'], on);

  const nextRows = (tz = 'Europe/Berlin', local = '02:00', utc = '01:00', days = ['Fri 7 Nov', 'Sat 8 Nov', 'Sun 9 Nov', 'Mon 10 Nov', 'Tue 11 Nov']) =>
    tbl(['Next 5 ticks', tz, ['UTC', 'num']], days.map((d) => [d, local, utc]));
  const ticksCard = ({ cron = '0 2 * * *', words = 'every day at 02:00', next = 'tomorrow 02:00', nextS = 'in 6h 12m · Europe/Berlin', last = 'today 02:00', lastS = '6 runs · all completed', missed = '0', overlap = 'skip', rows = nextRows() } = {}) => card({
    icon: 'clock', title: 'When it runs', aside: `<span class="mono">${cron}</span> · ${words}`,
    body: `${grid(2, [kv('Next tick', next, nextS), kv('Last tick', last, lastS), kv('Missed (30d)', missed, missed === '0' ? 'none' : 'not run later'), kv('Overlap', overlap, 'while the last tick still runs')])}
      <div class="pad" style="padding-top:0">${rows}</div>`,
  });
  const targetCard = ({ tpl = 'acme/billing-api', ver = 'stable', verS = 'resolves to v7 now' } = {}) => card({
    icon: 'tpl', title: 'What it runs', right: lnk('open template'),
    body: `${grid(2, [kv('Template', lnk(tpl), 'pipeline'), kv('Version', `<span class="mono">${ver}</span>`, verS), kv('Pool', 'default'), kv('Timeout', '45m')])}
      <div class="pad" style="padding-top:0">${tbl(['Param', 'Value'], [['<span class="mono">region</span>', 'eu'], ['<span class="mono">lookback_days</span>', '2'], ['<span class="mono">api_token</span>', '<span class="faint">sealed · secret param</span>']])}</div>`,
  });
  const tenantsCard = () => card({
    icon: 'tenants', title: 'Tenants', tag: 'fan-out', aside: 'operators only',
    body: `<div class="pad" style="display:flex;gap:6px;flex-wrap:wrap">${['acme', 'globex', 'initech', 'umbrella', 'hooli', 'stark'].map((t) => chip(t)).join('')}</div>`,
    note: 'One run per tenant each tick, admitted against that tenant’s own limits. One tenant at its limit does not stop the others.',
  });
  const ticksTable = ({ runs = '6', ok = '6 completed', third = '<span class="bad">● 1 of 6 failed</span>', all = 'all 186 runs' } = {}) => card({
    icon: 'runs', title: 'Recent ticks', right: lnk(all),
    body: tbl(['Tick', 'Outcome', ['Took', 'num']], [
      ['today', `<span class="ok">● ${ok}</span>`, '11m'],
      ['yesterday', `<span class="ok">● ${ok}</span>`, '12m'],
      ['Tue 4 Nov', third, '14m'],
      ['Mon 3 Nov', `<span class="ok">● ${ok}</span>`, '10m'],
    ]),
  });
  const nightlyHead = () => head('nightly-rollup', [pill('Active', 'ok', 'check'), '<span>every day at 02:00 Europe/Berlin</span>', `<span>runs ${lnk('acme/billing-api')} <span class="mono">stable</span></span>`, `<span>created by ${lnk('ops-admin')}</span>`]);
  const nightly = (tab = 'Overview') => {
    const top = `${dhead(dtab(tab), actions(), 'Schedules')}${nightlyHead()}`;
    if (tab === 'Runs') return `${top}${card({ icon: 'runs', title: 'Runs started by this schedule', right: lnk('open in Runs'), body: tbl(['Run', 'Tenant', 'Status', ['Rows', 'num'], 'Started'], [
      [lnk('nightly-rollup · acme'), 'acme', '<span class="ok">● completed</span>', '41,208', 'today 02:00'],
      [lnk('nightly-rollup · globex'), 'globex', '<span class="ok">● completed</span>', '12,940', 'today 02:00'],
      [lnk('nightly-rollup · initech'), 'initech', '<span class="ok">● completed</span>', '8,115', 'today 02:00'],
      [lnk('nightly-rollup · globex'), 'globex', '<span class="bad">● failed</span>', '—', 'Tue 02:00'],
      [lnk('nightly-rollup · acme'), 'acme', '<span class="ok">● completed</span>', '40,772', 'Tue 02:00']]), note: 'Runs carry the labels <code>schedule=nightly-rollup</code> and <code>schedule_tick</code>.' })}`;
    if (tab === 'History') return `${top}${card({ title: 'Change history', right: 'from the audit log', body: `<div class="log" style="font-family:var(--sans);font-size:12.5px;line-height:2">
      <div><span class="muted" style="display:inline-block;width:120px">2 Oct 10:14</span><span class="dot muted"></span>Created by ${lnk('ops-admin')} · cron <span class="mono">0 1 * * *</span></div>
      <div><span class="muted" style="display:inline-block;width:120px">9 Oct 16:02</span><span class="dot muted"></span>Edited by ${lnk('ops-raj')} · cron <span class="mono">0 1 * * *</span> → <span class="mono">0 2 * * *</span> · tenants +hooli +stark</div>
      <div><span class="muted" style="display:inline-block;width:120px">20 Oct 08:30</span><span class="dot warn"></span>Paused by ${lnk('ops-raj')} · “warehouse maintenance”</div>
      <div><span class="muted" style="display:inline-block;width:120px">20 Oct 13:05</span><span class="dot ok"></span>Resumed by ${lnk('ops-raj')} · 4 ticks skipped while paused</div>
      <div><span class="muted" style="display:inline-block;width:120px">4 Nov 09:11</span><span class="dot muted"></span>Run now by ${lnk('ops-admin')}</div></div>` })}`;
    if (tab === 'Missed ticks') return `${top}${empty('No missed ticks', 'Every tick in the last 30 days fired on time.', { icon: 'check' })}`;
    return `${top}<div class="two"><div class="stack">${ticksCard()}${targetCard()}</div><div class="stack">${tenantsCard()}${ticksTable()}</div></div>`;
  };

  // ---- the form (create / edit / tenant) ----
  const seg = (items, on) => `<span class="seg">${items.map((x) => `<span class="${x === on ? 'on' : ''}">${x}</span>`).join('')}</span>`;
  const form = ({ mode = 'create', source = false, invalid = false, tenant = false, tenantErr = false, approval = false } = {}) => {
    const what = source
      ? `<div class="form">${field('Template', select('faucet-hq/rest-orders · source template'), { req: true, cls: 'wide' })}${field('Version', seg(['stable', 'newest', 'channel', 'pin'], 'stable'), { help: 'stable follows launches · pin never moves', cls: 'wide' })}
          ${field('Sink template', select('faucet-hq/bigquery'), { req: true, help: 'A source template runs paired with a sink' })}${field('Sink version', select('stable · v4'))}
          ${field('Deployment overlay', select('prod-eu · deployment'), { help: 'Optional · state, DLQ, notifications', cls: 'wide' })}${field('Streams', `<div style="display:flex;gap:14px;height:30px;align-items:center">${check('orders', true)}${check('refunds', true)}${check('customers', false)}</div>`, { help: '2 of 3 streams', cls: 'wide' })}</div>`
      : `<div class="form one">${field('Template', select(tenant ? 'acme/crm-contacts · pipeline template' : 'acme/billing-api · pipeline template'), { req: true, help: tenant ? 'Only templates acme may run' : 'Registered templates only' })}${field('Version', seg(['stable', 'newest', 'channel', 'pin'], 'stable'), { help: 'stable · resolves to v7 now' })}</div>`;
    const params = `<div class="form">${field('<code>region</code>', select('eu'), { help: 'One of eu, us' })}${invalid
      ? field('<code>lookback_days</code>', input('', { cls: 'err' }), { req: true, err: 'Required: this param has no default.' })
      : field('<code>lookback_days</code>', input('2'), { req: true, help: 'int · default 1' })}<div class="wide">${field('<code>api_token</code>', input('••••••••••••'), { help: 'Secret · sealed with the server vault key, never shown again' })}</div></div>`;
    const cronErr = invalid ? 'Not a valid cron: month “13” is out of range 1–12.' : tenantErr ? 'Ticks every 2 minutes; acme’s schedules may tick at most every 5 minutes.' : '';
    const cronVal = invalid ? '0 2 1 13 *' : tenantErr ? '*/2 * * * *' : mode === 'edit' ? '0 3 * * *' : '0 2 * * *';
    const cronHelp = mode === 'edit' ? 'Every day at 03:00' : 'Every day at 02:00';
    const name = mode === 'edit' ? 'nightly-rollup' : tenant ? 'acme-crm-contacts' : source ? 'orders-to-lake' : 'nightly-rollup';
    const whenCard = card({ icon: 'clock', title: 'When', aside: 'DST-correct in the timezone',
      body: `<div class="form">${field('Name', input(name, { mono: true, cls: mode === 'edit' ? 'dis' : '' }), { req: true, help: mode === 'edit' ? 'Names cannot change' : 'Lowercase, digits, dashes' })}${field('Timezone', select(tenant ? 'UTC' : 'Europe/Berlin'))}
        ${field('Cron', input(cronVal, { mono: true, cls: cronErr ? 'err' : '' }), cronErr ? { req: true, err: cronErr } : { req: true, help: cronHelp })}${field('Overlap', seg(['skip', 'queue', 'forbid'], 'skip'), { help: 'While the last tick still runs' })}</div>
        <div class="pad" style="padding-top:0">${cronErr ? empty('No preview', 'Fix the cron to see the next ticks.', {}) : mode === 'edit' ? nextRows('Europe/Berlin', '03:00', '02:00') : nextRows(tenant ? 'UTC' : 'Europe/Berlin', tenant ? '00:00' : '02:00', tenant ? '00:00' : '01:00')}</div>` });
    const tenantsCard2 = tenant ? '' : card({ icon: 'tenants', title: 'Tenants', aside: 'operators only',
      body: `<div class="form one">${field('Run for', seg(['this server', 'all tenants', 'selected'], 'selected'))}<div style="display:flex;gap:6px;flex-wrap:wrap">${['acme', 'globex', 'initech', 'umbrella', 'hooli', 'stark'].map((t) => chip(`${t} ${ic('x')}`)).join('')}${chip(`${ic('plus')}add`)}</div></div>`,
      note: 'One run per tenant each tick, each admitted against that tenant’s limits.' });
    const opts = card({ title: 'Run options', body: `<div class="form three">${field('Pool', select('default'))}${field('Timeout', input('45m'))}${field('Labels', input('team=finance', { mono: true }))}</div>` });
    const limitAlert = tenant ? alert('acme can have 5 schedules; 4 are in use.', 'Ticks fire at most every 5 minutes, and each run counts against acme’s run limits.', '') : '';
    const editNote = mode === 'edit' ? card({ icon: 'diff', title: 'What changes', body: diff([['-', 'cron: 0 2 * * *'], ['+', 'cron: 0 3 * * *'], [' ', 'template, params, tenants — unchanged']]), note: 'Takes effect from the next tick. A tick that already started keeps running.' }) : '';
    const submitLabel = approval ? 'Request approval' : mode === 'edit' ? 'Save changes' : 'Create schedule';
    const foot = `<div class="actions" style="border:0;padding:4px 0 0">${approval ? '<span class="muted">This server requires approval for schedule changes; this becomes a change request.</span>' : ''}${btn('Cancel', { kind: 'quiet' })}${btn(submitLabel, { kind: 'primary', icon: approval ? 'changes' : 'check', cls: invalid || tenantErr ? 'dis' : '' })}</div>`;
    return `${dhead('', '', 'Schedules')}${head(mode === 'edit' ? 'Edit nightly-rollup' : 'New schedule', [], mode === 'edit' ? '' : 'Runs a registered template on a cron. Every tick goes through the same admission, budgets and audit as any other run.')}
      ${limitAlert}${invalid ? alert('2 fields need attention.', 'Nothing is saved until they are fixed.', 'bad') : ''}
      <div class="pair"><div class="stack">${card({ icon: 'tpl', title: 'What it runs', body: what })}${card({ icon: 'sliders', title: 'Parameters', aside: 'from the template’s params', body: params })}${opts}</div>
      <div class="stack">${whenCard}${tenantsCard2}${editNote}</div></div>${foot}`;
  };

  M.view('schedules', {
    label: 'Schedules', nav: 'schedules', icon: 'sched', summary: 'time-driven runs of registered templates, separate from event triggers (#1044)',
    states: {
      list: { label: 'All schedules · nothing selected', sel: false, covers: ['GET /v1/schedules', 'next / last tick', 'source console | file', 'tenant'], new: ['Schedules page (#1044)'] },
      selected: { label: 'Schedule selected · overview', covers: ['GET /v1/schedules/{name}', 'next 5 ticks', 'target + params', 'tenant fan-out', 'recent ticks'], new: ['run now / pause / edit'] },
      runs: { label: 'Runs tab', covers: ['runs this schedule started (label schedule=)'] },
      missed: { label: 'Missed ticks tab', covers: ['missed ticks (no replica running) · not run later'] },
      history: { label: 'History tab', covers: ['create / edit / pause / resume / run-now from the audit log'] },
      create: { label: 'New schedule · pipeline template', covers: ['POST /v1/schedules', 'params form from params:', 'cron + timezone + next ticks', 'tenants (operators)'] },
      'create-source': { label: 'New schedule · source template', covers: ['sink template + version', 'deployment overlay', 'streams'] },
      invalid: { label: 'New schedule · validation errors', covers: ['bad cron', 'required param missing'] },
      edit: { label: 'Edit schedule', covers: ['PUT /v1/schedules/{name}', 'what changes'] },
      approval: { label: 'New schedule under --require-approval schedule', covers: ['becomes a change request'] },
      requested: { label: 'Change request created', covers: ['pending schedule, link to Changes'] },
      runnow: { label: 'Run now dialog', covers: ['POST /v1/schedules/{name}/run'] },
      pause: { label: 'Pause dialog', covers: ['POST /v1/schedules/{name}/pause'] },
      paused: { label: 'Paused schedule', covers: ['resume', 'who paused and why'] },
      delete: { label: 'Delete dialog', covers: ['DELETE /v1/schedules/{name}'] },
      file: { label: 'From the schedules file · read only', covers: ['--schedules file', 'run now only'] },
      broken: { label: 'Unhealthy · template unresolved', covers: ['failed ticks with reason, never skipped silently'] },
      empty: { label: 'No schedules', sel: false, covers: ['empty state'] },
      off: { label: 'Schedule feature not built in', sel: false, covers: ['binary without the schedule feature'] },
      loading: { label: 'Loading', sel: false, covers: ['skeleton'] },
      error: { label: 'Could not load', sel: false, covers: ['error with retry'] },
      viewer: { label: 'Viewer · read only', params: { role: 'viewer' }, covers: ['no write controls'] },
      tenant: { label: 'Tenant principal · own schedule', params: { scope: 'tenant' }, covers: ['tenant-scoped list', 'no fan-out', 'limit shown'] },
      'tenant-create': { label: 'Tenant · tick interval below the minimum', params: { scope: 'tenant' }, covers: ['min interval guardrail', 'only templates the tenant may run', 'no tenants selector'] },
      'tenant-limit': { label: 'Tenant · max_schedules reached', params: { scope: 'tenant' }, sel: false, covers: ['max_schedules guardrail'] },
      'scoped-list': { label: 'Operator · switcher set to acme', params: { tenant: 'acme' }, sel: false, covers: ['GET /v1/schedules?tenant=acme', 'acme-owned + shared schedules that include acme'], new: ['shared tag with tenant count'] },
      scoped: { label: 'Operator · shared schedule seen as acme', params: { tenant: 'acme' }, covers: ['runs, missed and recent ticks filtered to acme', 'edits apply to every tenant (warning)'] },
      'scoped-pause': { label: 'Operator · pause a shared schedule while scoped', params: { tenant: 'acme' }, covers: ['pause affects all three tenants'] },
      'tenant-shared': { label: 'Tenant principal · operator’s shared schedule', params: { scope: 'tenant' }, covers: ['read only, managed by the server operator', 'own runs only', 'other tenants not named'] },
    },
    render(state) {
      const crumbs = (t) => (t ? ['Operate', 'Schedules', t] : ['Operate', 'Schedules']);
      if (state === 'loading') {
        M.top(crumbs());
        M.list(`${tabs([['All', '…']], 'All', ibtn('refresh'))}${skelRows(8)}`);
        M.detail(skelCards(3));
        return;
      }
      if (state === 'error') {
        M.top(crumbs());
        M.list(`${tabs([['All', '—']], 'All', ibtn('refresh'))}${empty('Could not load schedules', 'The server answered 503.', { icon: 'alert', btns: btn('Retry', { icon: 'refresh' }) })}`);
        M.detail(alert('Schedules are unavailable right now.', 'They are stored in the run-history database, which this server cannot reach. Ticks resume once it reconnects.', 'bad', btn('Retry', { icon: 'refresh' })));
        return;
      }
      if (state === 'empty' || state === 'off') {
        M.top(crumbs());
        const off = state === 'off';
        M.list(`${tabs([['All', 0]], 'All', ibtn('refresh') + (off ? '' : newBtn()))}${empty(off ? 'Schedules are not built in' : 'No schedules yet', off ? 'This faucet binary was built without the schedule feature.' : 'A schedule runs a registered template on a cron.', { icon: 'sched' })}`);
        M.detail(off
          ? `${head('Schedules need the schedule feature', [], 'This server was built without it, so it cannot tick schedules. The release binaries, the Homebrew formula and the <code>full</code> container image include it.')}${card({ icon: 'code', title: 'Build it in', body: code('cargo install faucet-cli --features "serve,schedule"', { box: false, lang: 'plain' }) })}`
          : `${head('Run templates on a cron', [], 'Create a schedule here, or define schedules in a file and start the server with <code>--schedules</code>. Schedules from a file are shown read-only.')}
            <div class="two">${card({ icon: 'plus', title: 'In the console', body: `<div class="pad stack" style="gap:10px"><span>Pick a template, set its params and a cron. Every replica picks it up without a restart.</span><div>${btn('New schedule', { icon: 'plus', kind: 'primary', act: true })}</div></div>` })}
            ${card({ icon: 'code', title: 'schedules.yaml', body: code(`version: 1
schedules:
  - name: nightly-rollup
    cron: "0 2 * * *"
    timezone: Europe/Berlin
    template: { id: acme/billing-api, version: stable }
    params: { region: eu }`, { box: false }), note: 'Then start the server with <code>faucet serve --schedules schedules.yaml</code>.' })}</div>`);
        return;
      }
      const billingTicks = (lastS) => ticksCard({ cron: '0 */6 * * *', words: 'every 6 hours', next: 'today 18:00', nextS: 'in 2h · UTC', last: 'today 12:00', lastS,
        rows: tbl(['Next 5 ticks', 'UTC'], [['today', '18:00'], ['tomorrow', '00:00'], ['tomorrow', '06:00'], ['tomorrow', '12:00'], ['tomorrow', '18:00']]) });
      const acmeTicks = (title) => card({ icon: 'runs', title, right: lnk('all 31 in Runs'), body: tbl(['Tick', 'Outcome', ['Took', 'num']], [
        ['today 12:00', '<span class="ok">● 1 of 1 completed</span>', '4m'], ['today 06:00', '<span class="ok">● 1 of 1 completed</span>', '4m'], ['today 00:00', '<span class="ok">● 1 of 1 completed</span>', '5m'], ['yesterday 18:00', '<span class="ok">● 1 of 1 completed</span>', '4m']]) });
      if (state === 'scoped-list') {
        M.top(crumbs());
        M.list(scopedPane(''));
        M.detail(`${head('4 schedules run for acme', ['<span>2 of its own · 2 shared</span>', '<span>next tick in 14m</span>'])}
          ${card({ title: 'Coming up for acme', right: 'next 12 hours · UTC', body: tbl(['When', 'Schedule', 'Owner', 'Template'], [
            ['in 14m · 14:15', lnk('hourly-crm-sync'), 'acme', 'acme/crm-sync'], ['in 2h · 18:00', lnk('billing-sync'), `shared ${tag('3 tenants')}`, 'faucet-hq/billing-export'],
            ['in 3h · 17:00', lnk('acme-daily-orders'), 'acme', 'acme/orders'], ['in 6h · 01:00', lnk('nightly-rollup'), `shared ${tag('6 tenants')}`, 'acme/billing-api']]) })}`);
        return;
      }
      if (state === 'scoped' || state === 'scoped-pause') {
        M.top(crumbs('billing-sync'));
        M.list(scopedPane('billing'));
        M.detail(`${dhead(dtabs(['Overview', ['Runs · acme', 31], ['Missed · acme', 0], 'History'], 'Overview'), actions(), 'Schedules')}
          ${head('billing-sync', [pill('Active', 'ok', 'check'), chip('shared · runs for 3 tenants'), '<span>every 6 hours · UTC</span>', `<span>runs ${lnk('faucet-hq/billing-export')} <span class="mono">stable</span></span>`])}
          ${alert('Runs for acme, globex and initech; changes apply to all three.', 'Runs, missed ticks and recent ticks below are acme’s only, because the switcher is set to acme. Edit, Pause and Delete change the schedule for every tenant it runs for.', 'warn')}
          <div class="two"><div class="stack">${billingTicks('acme: 1 of 1 completed')}</div>
          <div class="stack">${card({ icon: 'tenants', title: 'Tenants', tag: 'fan-out', aside: 'operators only', body: `<div class="pad" style="display:flex;gap:6px;flex-wrap:wrap">${chip(`${ic('check')}acme`, 'on')}${chip('globex')}${chip('initech')}</div>`, note: 'acme is the tenant you are looking at.' })}${acmeTicks('Recent ticks · acme')}</div></div>`);
        if (state === 'scoped-pause') M.overlay(dialog('Pause billing-sync for every tenant?', `<span>This schedule runs for <b>acme, globex and initech</b>. Pausing stops ticks for all three, not only acme. Skipped ticks are not run later.</span>
          ${alert('To stop it for acme only, edit the schedule and remove acme from its tenants.', '', '')}
          ${field('Reason', '<div class="area sans">Billing API maintenance window</div>', { help: 'Optional · shown on the schedule and in the audit log' })}`, `${btn('Cancel')}${btn('Pause for all 3 tenants', { kind: 'primary', icon: 'pause' })}`, 'pause'));
        return;
      }
      if (state === 'tenant-shared') {
        M.top(crumbs('billing-sync'));
        M.list(listPane('billing', { tenant: true }));
        M.detail(`${dhead(dtabs(['Overview', ['Your runs', 31]], 'Overview'), '', 'Schedules')}
          ${head('billing-sync', [pill('Active', 'ok', 'check'), chip('managed by the server operator'), '<span>every 6 hours · UTC</span>', `<span>runs ${lnk('faucet-hq/billing-export')} <span class="mono">stable</span></span>`])}
          ${alert('Managed by the server operator.', 'It runs for acme and other tenants. You can see when it runs and acme’s runs from it; ask the operator to change or pause it.', '')}
          <div class="two"><div class="stack">${billingTicks('1 run for acme · completed')}</div><div class="stack">${acmeTicks('acme’s runs')}</div></div>`);
        return;
      }
      if (state === 'tenant-limit') {
        M.top(crumbs());
        M.list(listPane('', { tenant: true, limit: true }));
        M.detail(`${head('acme’s schedules', ['<span>5 of 5 in use</span>', '<span>ticks at most every 5 minutes</span>'])}
          ${alert('acme has reached its limit of 5 schedules.', 'Delete one to create another, or ask an operator to raise <code>max_schedules</code>. Paused schedules count toward the limit.', 'warn')}
          ${card({ title: 'Coming up', body: tbl(['When', 'Schedule', 'Template'], [['in 14m', lnk('hourly-crm-sync'), 'acme/crm-sync'], ['in 52m', lnk('acme-invoice-pull'), 'acme/invoices'], ['in 2h', lnk('acme-crm-contacts'), 'acme/crm-contacts'], ['in 3h', lnk('acme-daily-orders'), 'acme/orders']]) })}`);
        return;
      }
      if (state === 'tenant' || state === 'tenant-create') {
        M.top(crumbs(state === 'tenant' ? 'hourly-crm-sync' : 'New schedule'));
        M.list(listPane(state === 'tenant' ? 'hourly' : '', { tenant: true }));
        if (state === 'tenant-create') { M.detail(form({ tenant: true, tenantErr: true })); return; }
        M.detail(`${dhead(dtab('Overview', { runs: 412 }), actions(), 'Schedules')}
          ${head('hourly-crm-sync', [pill('Active', 'ok', 'check'), '<span>every hour at :15 UTC</span>', `<span>runs ${lnk('acme/crm-sync')} <span class="mono">stable</span></span>`, `<span>created by ${lnk('acme-portal')}</span>`])}
          <div class="two"><div class="stack">${ticksCard({ cron: '15 * * * *', words: 'every hour at :15', next: 'today 14:15', nextS: 'in 14m · UTC', last: 'today 13:15', lastS: '1 run · completed', rows: tbl(['Next 5 ticks', 'UTC'], [['today', '14:15'], ['today', '15:15'], ['today', '16:15'], ['today', '17:15'], ['today', '18:15']]) })}</div>
          <div class="stack">${card({ icon: 'tenants', title: 'Runs for', body: `<div class="pad">${chip('acme')}</div>`, note: 'A tenant’s schedules run only for that tenant, within its limits.' })}${ticksTable({ runs: '1', ok: '1 completed', third: '<span class="ok">● 1 completed</span>', all: 'all 412 runs' })}</div></div>`);
        return;
      }

      const sel = { file: 'weekly', broken: 'broken', paused: 'partner', missed: 'hourly', create: '', 'create-source': '', invalid: '', approval: '', requested: '' }[state];
      const selected = sel === undefined ? 'nightly' : sel;
      const title = { weekly: 'weekly-ledger-export', broken: 'invoice-digest', partner: 'partner-sftp-pull', hourly: 'hourly-crm-sync', nightly: 'nightly-rollup', '': 'New schedule' }[selected];
      M.top(crumbs(state === 'list' ? '' : state === 'edit' ? 'nightly-rollup' : title));
      M.list(listPane(state === 'list' ? '' : selected));

      if (state === 'list') {
        M.detail(`${head('8 schedules', ['<span>next tick in 14m</span>', '<span class="bad">3 missed in the last 24h</span>'])}
          ${alert('invoice-digest failed its last 3 ticks.', 'Its template channel no longer resolves.', 'bad', btn('Open', { icon: 'right' }))}
          ${card({ title: 'Coming up', right: 'next 12 hours · UTC', body: tbl(['When', 'Schedule', 'Runs', 'Template'], [
            ['in 14m · 14:15', lnk('hourly-crm-sync'), '1 · acme', 'acme/crm-sync'], ['in 41m · 14:30', lnk('usage-snapshot'), '1', 'faucet-hq/usage-export'],
            ['in 1h 14m · 15:15', lnk('hourly-crm-sync'), '1 · acme', 'acme/crm-sync'], ['in 3h · 17:00', lnk('acme-daily-orders'), '1 · acme', 'acme/orders'], ['in 6h · 01:00', lnk('nightly-rollup'), '6 tenants', 'acme/billing-api']]) })}`);
        return;
      }
      if (['create', 'create-source', 'invalid', 'approval', 'edit'].includes(state)) {
        M.detail(form({ mode: state === 'edit' ? 'edit' : 'create', source: state === 'create-source', invalid: state === 'invalid', approval: state === 'approval' }));
        return;
      }
      if (state === 'requested') {
        M.top(crumbs('weekly-export'));
        M.detail(`${dhead('', btn('Open in Changes', { icon: 'changes', kind: 'primary' }), 'Schedules')}${head('weekly-export', [pill('Pending approval', 'warn'), `<span>requested by ${lnk('ops-raj')} · just now</span>`, `<span>change ${lnk('cr-0145', true)}</span>`])}
          ${alert('Change request cr-0145 created.', 'The schedule starts ticking once an approver accepts it on the Changes page. Nothing runs until then.', 'warn', btn('Open in Changes', { icon: 'changes' }))}
          ${card({ icon: 'diff', title: 'What was requested', body: diff([['+', 'name: weekly-export'], ['+', 'cron: 0 6 * * MON · Europe/Berlin'], ['+', 'template: acme/billing-api @ stable'], ['+', 'params: region=eu, lookback_days=7'], ['+', 'tenants: acme, globex']]) })}`);
        return;
      }
      if (state === 'file') {
        M.detail(`${dhead(dtab('Overview', { runs: 44 }), actions({ file: true }), 'Schedules')}
          ${head('weekly-ledger-export', [pill('Active', 'ok', 'check'), chip('from schedules file'), '<span>Mondays at 06:00 Europe/Berlin</span>', `<span>runs ${lnk('finance/ledger-export')} <span class="mono">pin v3</span></span>`])}
          ${alert('Defined in the server’s schedules file.', 'Change it in <code>schedules.yaml</code> and restart the server. The console can run it now but cannot edit, pause or delete it.', '')}
          <div class="two"><div class="stack">${ticksCard({ cron: '0 6 * * MON', words: 'Mondays at 06:00', next: 'Mon 10 Nov 06:00', nextS: 'in 4d · Europe/Berlin', last: 'Mon 3 Nov 06:00', lastS: '1 run · completed', rows: nextRows('Europe/Berlin', '06:00', '05:00', ['Mon 10 Nov', 'Mon 17 Nov', 'Mon 24 Nov', 'Mon 1 Dec', 'Mon 8 Dec']) })}</div>
          <div class="stack">${card({ icon: 'code', title: 'schedules.yaml', aside: 'as loaded at start', body: code(`- name: weekly-ledger-export
  cron: "0 6 * * MON"
  timezone: Europe/Berlin
  template: { id: finance/ledger-export, version: 3 }`, { box: false }) })}</div></div>`);
        return;
      }
      if (state === 'broken') {
        M.detail(`${dhead(dtab('Overview', { runs: 31, missed: 0 }), actions(), 'Schedules')}
          ${head('invoice-digest', [pill('Unhealthy', 'bad', 'alert'), '<span>every day at 07:00 UTC</span>', `<span>runs ${lnk('acme/invoices')} <span class="mono">channel prod</span></span>`, '<span>tenant acme</span>'])}
          ${alert('The template channel does not resolve.', '<code>acme/invoices</code> has no version on channel <code>prod</code> since v4 was deleted yesterday. Each tick is recorded as a failed run with this reason; none are skipped silently.', 'bad', btn('Edit target', { icon: 'edit', act: true }) + btn('Pause', { icon: 'pause', act: true }))}
          ${card({ icon: 'runs', title: 'Recent ticks', body: tbl(['Tick', 'Outcome', 'Reason'], [
            ['today 07:00', '<span class="bad">● failed</span>', 'channel prod of acme/invoices does not resolve'],
            ['yesterday 07:00', '<span class="bad">● failed</span>', 'channel prod of acme/invoices does not resolve'],
            ['Tue 4 Nov 07:00', '<span class="bad">● failed</span>', 'channel prod of acme/invoices does not resolve'],
            ['Mon 3 Nov 07:00', '<span class="ok">● completed</span>', '—']]) })}`);
        return;
      }
      if (state === 'paused') {
        M.detail(`${dhead(dtab('Overview', { runs: 960 }), actions({ paused: true }), 'Schedules')}
          ${head('partner-sftp-pull', [pill('Paused', '', 'pause'), '<span>every 30 minutes · UTC</span>', `<span>runs ${lnk('partner/sftp-drop')} <span class="mono">stable</span></span>`])}
          ${alert(`Paused by ${lnk('ops-raj')} 2 days ago.`, '“Partner SFTP is down for a migration until Friday.” 96 ticks were skipped; they are not run later.', '')}
          <div class="two"><div class="stack">${ticksCard({ cron: '*/30 * * * *', words: 'every 30 minutes', next: '—', nextS: 'paused', last: 'Mon 3 Nov 09:30', lastS: '1 run · completed', rows: empty('No upcoming ticks', 'Resume to see the next ticks.', {}) })}</div><div class="stack">${targetCard({ tpl: 'partner/sftp-drop', verS: 'resolves to v2 now' })}</div></div>`);
        return;
      }
      if (state === 'missed') {
        M.detail(`${dhead(dtab('Missed ticks', { runs: 412, missed: 3 }), actions(), 'Schedules')}
          ${head('hourly-crm-sync', [pill('Active', 'ok', 'check'), '<span>every hour at :15 UTC</span>', `<span>runs ${lnk('acme/crm-sync')} <span class="mono">stable</span></span>`, '<span>tenant acme</span>'])}
          ${card({ icon: 'alert', title: '3 missed ticks', aside: 'last 30 days', body: tbl(['Tick', 'Why', 'Next tick that ran'], [
            ['Wed 5 Nov 03:15', 'no replica was running (03:02–05:40 UTC)', '06:15'], ['Wed 5 Nov 04:15', 'no replica was running', '06:15'], ['Wed 5 Nov 05:15', 'no replica was running', '06:15']]),
            note: 'Missed ticks are counted, not run later. Use Run now to catch up by hand.' })}`);
        return;
      }
      const tab = { runs: 'Runs', history: 'History' }[state] || 'Overview';
      M.detail(nightly(tab));
      if (state === 'runnow') M.overlay(dialog('Run nightly-rollup now?', `<span>Starts one tick now for its <b>6 tenants</b>, exactly as the 02:00 tick would. The next scheduled tick is unchanged.</span>
        ${alert('The last tick finished at 02:11, so nothing overlaps.', '', 'ok')}`, `${btn('Cancel')}${btn('Run now', { kind: 'primary', icon: 'play' })}`, 'play'));
      if (state === 'pause') M.overlay(dialog('Pause nightly-rollup?', `<span>No ticks fire while it is paused, and skipped ticks are not run later. Resume it at any time.</span>
        ${field('Reason', '<div class="area sans">Warehouse maintenance tonight</div>', { help: 'Optional · shown on the schedule and in the audit log' })}`, `${btn('Cancel')}${btn('Pause', { kind: 'primary', icon: 'pause' })}`, 'pause'));
      if (state === 'delete') M.overlay(dialog('Delete nightly-rollup?', `<span>It stops ticking and leaves the list. Runs it already started stay in history.</span>${field('Type <code>nightly-rollup</code> to confirm', input('nightly-rollup', { mono: true, cls: 'focus' }))}`, `${btn('Cancel')}${btn('Delete schedule', { kind: 'danger', icon: 'trash' })}`, 'trash'));
    },
  });
})();
