// Feeds (#1034) — split pane: durable inboxes that senders push events into over
// HTTP. A feed stores every event before it answers, and each of its deliveries
// runs a registered template over the new events on a flush policy. Feeds never
// write to a sink themselves; runs do. Console-made feeds live in run history;
// feeds from the `--feeds` file are read-only here. Tenant principals manage
// their own tenant's feeds within max_feeds and their ingest quotas.
(function () {
  const HOST = 'https://ingest.faucet.example.com';
  const F = {
    partner: ['partner', ring('bad'), 'partner-callbacks', '4s', '<b>delivery stalled</b> · to-warehouse · 18,204 pending'],
    pings: ['pings', ring('bad'), 'device-pings', '—', '<b>refusing events</b> · undelivered limit reached'],
    tickets: ['tickets', ring('ok'), 'support-tickets', '2s', '<b>312 pending</b> · 2 deliveries · 41,208 today'],
    billing: ['billing', ring('ok'), 'billing-events', '11s', '<b>0 pending</b> · 1 delivery · 9,870 today'],
    app: ['app', ring('ok'), 'app-events', '1s', '<b>1,940 pending</b> · 1 delivery · tenant acme'],
    audit: ['audit', ring('ok'), 'audit-trail', '3m', '<b>0 pending</b> · 1 delivery · from feeds file'],
    legacy: ['legacy', ring('', 'hollow'), 'legacy-import', '2h', '<b>delivery paused</b> · 6,410 pending · by ops-raj'],
  };
  const TEN = {
    app: ['app', ring('ok'), 'app-events', '1s', '<b>1,940 pending</b> · 1 delivery · by acme-portal'],
    orders: ['orders', ring('ok'), 'order-updates', '9s', '<b>0 pending</b> · 2 deliveries · by acme-portal'],
    forms: ['forms', ring('ok'), 'signup-forms', '4m', '<b>12 pending</b> · 1 delivery · by acme-portal'],
    crm: ['crm', ring('', 'hollow'), 'crm-changes', '1d', '<b>delivery paused</b> · 88 pending · by acme-portal'],
    chat: ['chat', ring('ok'), 'chat-transcripts', '40s', '<b>3 pending</b> · 1 delivery · by acme-portal'],
  };
  const r = (sel) => ([id, ...a]) => row(...a, { on: id === sel });
  const newBtn = (dis) => btn('New', { icon: 'plus', kind: 'primary', act: true, cls: dis ? 'dis' : '' });
  const listPane = (sel, { tenant = false, limit = false, scoped = false } = {}) => {
    if (tenant) {
      const rows = limit ? [TEN.app, TEN.orders, TEN.forms, TEN.chat, TEN.crm] : [TEN.app, TEN.orders, TEN.forms, TEN.crm];
      const live = rows.filter((x) => x !== TEN.crm);
      return `${tabs([['All', rows.length], ['Needs attention', 0]], 'All', ibtn('refresh') + newBtn(limit))}
        ${stats([['Feeds', `${rows.length} of 5`, limit ? 'warn' : ''], ['Today', '63,402'], ['Pending', '2,040'], ['Stored', '1.1 GiB']])}
        ${sect('Receiving', live.length)}${live.map(r(sel)).join('')}${sect('Delivery paused', 1)}${r(sel)(TEN.crm)}`;
    }
    if (scoped) return `${tabs([['All', 1], ['Needs attention', 0]], 'All', ibtn('refresh') + newBtn())}
        ${stats([['For acme', '1'], ['Today', '63,402'], ['Pending', '1,940'], ['Stored', '1.1 GiB']])}
        ${sect('acme’s feeds', 1)}${r(sel)(F.app)}
        <div class="muted" style="font-size:12px;padding:10px 14px">Showing feeds that belong to acme. Pick “All tenants” above to see all 7.</div>`;
    return `${tabs([['All', 7], ['Needs attention', 2]], 'All', ibtn('refresh') + newBtn())}
      ${stats([['Feeds', '7'], ['Today', '212,430'], ['Pending', '26,866', 'warn'], ['Refused', '1.2%']])}
      ${sect('Needs attention', 2)}${[F.partner, F.pings].map(r(sel)).join('')}
      ${sect('Receiving', 4)}${[F.tickets, F.app, F.billing, F.audit].map(r(sel)).join('')}
      ${sect('Delivery paused', 1)}${r(sel)(F.legacy)}`;
  };

  // ---- shared detail pieces ----
  const url = (name, tenant) => `${HOST}/v1/${tenant ? `tenants/${tenant}/` : ''}feeds/${name}/events`;
  const dtab = (on, { deliveries = 2, refused = 96, tokens = 2 } = {}) => dtabs(['Overview', ['Deliveries', deliveries], 'Events', ['Refused', refused], ['Tokens', tokens], 'History'], on);
  const actions = ({ file = false } = {}) => file
    ? btn('Send test event', { icon: 'play', act: true })
    : `${btn('Send test event', { icon: 'play', act: true })}${btn('Edit', { icon: 'edit', act: true })}${ibtn('more', { act: true })}`;
  const sendCard = ({ name = 'support-tickets', tenant = '', auth = 'token + signature', sig = 'X-Signature-256' } = {}) => card({
    icon: 'link', title: 'Send events here', right: lnk('how senders connect'),
    body: `<div class="pad" style="padding-bottom:6px">${input(`<span class="faint">POST</span>&nbsp; ${url(name, tenant)}${cpi()}`, { mono: true })}</div>
      ${grid(4, [kv('Sign-in', auth, sig ? `signature header <span class="mono">${sig}</span>` : 'write-only feed token'), kv('Accepts', 'JSON, NDJSON', 'up to 1 MiB per event'), kv('Duplicates', '<span class="mono">X-Event-Id</span>', 'same id within 24h is stored once'), kv('Answers', '202 once stored', 'never before the event is durable')])}`,
    note: 'A feed token can only add events to this feed. It cannot read events, start runs or reach any other part of the server.',
  });
  const flowCard = ({ rec = '41,208', del = '40,896', pend = '312', ref = '96', pts = [22, 30, 28, 41, 36, 52, 47, 60, 44, 38, 51, 49, 63, 58, 40, 46], pendCls = '' } = {}) => card({
    icon: 'usage', title: 'Last 24 hours', right: spark(pts, 'var(--text-2)', 170, 30),
    body: grid(4, [kv('Received', rec, 'stored and answered'), kv('Delivered', del, 'written by a run'), kv('Pending', `<span class="${pendCls}">${pend}</span>`, 'waiting for the next flush'), kv('Refused', ref, 'never stored')]),
  });
  const storeCard = ({ und = '3.1 MiB of 50 GiB', undPct = 1, oldest = '38s', oldPct = 1, kept = '1.2 GiB', tone = '' } = {}) => card({
    icon: 'db', title: 'Storage and retention', aside: 'store <span class="mono">default</span>',
    body: `<div class="pad stack" style="gap:12px">
      <div><div style="display:flex;justify-content:space-between"><span>Not delivered yet</span><b class="${tone}">${und}</b></div>${meter(undPct)}</div>
      <div><div style="display:flex;justify-content:space-between"><span>Oldest waiting</span><b class="${tone}">${oldest} <span class="muted">of 30 days</span></b></div>${meter(oldPct)}</div>
      <div style="display:flex;justify-content:space-between"><span>Delivered, kept for replay</span><b>${kept} <span class="muted">· 7 days</span></b></div></div>`,
    note: 'Events are removed only after 7 days and once every delivery has them. At the limit the feed refuses new events; it never drops stored ones.',
  });
  const delivRows = (stalled) => [
    [lnk('to-warehouse'), lnk('acme/tickets-to-lake'), '5,000 events or 60s', stalled ? '<span class="bad">18,204 · 3h 12m</span>' : '312 · 38s', stalled ? '<span class="bad">● failed</span> 2m ago' : '<span class="ok">● completed</span> 22s ago'],
    [lnk('to-search'), lnk('faucet-hq/events-to-index'), '500 events or 10s', '0 · up to date', '<span class="ok">● completed</span> 6s ago'],
  ];
  const delivCard = (stalled) => card({ icon: 'runs', title: 'Deliveries', right: btn('Add delivery', { icon: 'plus', act: true }),
    body: tbl(['Delivery', 'Runs', 'Flushes at', 'Behind', 'Last run'], delivRows(stalled)) });
  const ticketsHead = () => head('support-tickets', [pill('Receiving', 'ok', 'check'), '<span>last event 2s ago</span>', '<span>2 deliveries</span>', `<span>created by ${lnk('ops-admin')}</span>`]);
  const overview = () => `${dhead(dtab('Overview'), actions(), 'Feeds')}${ticketsHead()}
    ${sendCard()}<div class="two"><div class="stack">${flowCard()}</div><div class="stack">${storeCard()}</div></div>${delivCard()}`;

  const delivery = ({ paused = false, stalled = false } = {}) => `${dhead(dtab('Deliveries'), `${btn('Deliver now', { icon: 'play', act: true })}${paused ? btn('Resume', { icon: 'play', kind: 'primary', act: true }) : btn('Pause', { icon: 'pause', act: true })}${btn('Edit', { icon: 'edit', act: true })}${ibtn('more', { act: true })}`, 'support-tickets')}
    ${head('to-warehouse', [stalled ? pill('Stalled', 'bad', 'alert') : paused ? pill('Paused', '', 'pause') : pill('Delivering', 'ok', 'check'), `<span>runs ${lnk('acme/tickets-to-lake')} <span class="mono">pin v4</span></span>`, '<span>exactly once</span>'])}
    <div class="two"><div class="stack">
      ${card({ icon: 'feed', title: 'Position', body: grid(4, [kv('Delivered to', '<span class="mono">#1,204,881</span>', 'the cursor'), kv('Newest event', '<span class="mono">#1,205,193</span>'), kv('Behind', '312 events', '38s · 1.1 MiB'), kv('Replay from', '<span class="mono">#1,163,400</span>', '7 days ago')]) })}
      ${card({ icon: 'tpl', title: 'What it runs', right: lnk('open template'), body: `${grid(2, [kv('Template', lnk('acme/tickets-to-lake'), 'pipeline'), kv('Version', '<span class="mono">pin v4</span>', 'never moves'), kv('Pool', 'default'), kv('After 5 failed runs', 'stall and alert')])}
        <div class="pad" style="padding-top:0">${tbl(['Param', 'Value'], [['<span class="mono">table</span>', 'support.tickets'], ['<span class="mono">warehouse_token</span>', '<span class="faint">sealed · secret param</span>']])}</div>` })}
    </div><div class="stack">
      ${card({ icon: 'clock', title: 'When it flushes', body: grid(2, [kv('Events', '5,000', 'or'), kv('Size', '64 MiB', 'or'), kv('Oldest waiting', '60s', 'whichever comes first'), kv('At most every', '10s')]), note: 'One run per flush, never one per event. A backlog is delivered in runs of up to 50,000 events.' })}
    </div></div>
      ${card({ icon: 'runs', title: 'Recent runs', right: lnk('all 1,412 in Runs'), body: tbl(['Run', 'Events', 'Status', ['Rows', 'num'], 'Started'], [
        [lnk('to-warehouse · 8f21'), '<span class="mono">#1,204,570–1,204,881</span>', '<span class="ok">● completed</span>', '312', '22s ago'],
        [lnk('to-warehouse · 8f20'), '<span class="mono">#1,199,570–1,204,569</span>', '<span class="ok">● completed</span>', '5,000', '1m ago'],
        [lnk('to-warehouse · 8f1f'), '<span class="mono">#1,199,102–1,199,569</span>', '<span class="ok">● completed</span>', '468', '2m ago'],
        [lnk('to-warehouse · 8f1e'), '<span class="mono">#1,199,102–1,199,569</span>', '<span class="bad">● failed</span>', '—', '3m ago']]),
        note: 'A run that fails is tried again over the same events. Runs carry the labels <code>feed=support-tickets</code> and <code>feed_delivery=to-warehouse</code>.' })}`;

  const events = ({ open = false, sealed = false } = {}) => {
    const payload = sealed
      ? alert('This feed seals its events.', 'Payloads are encrypted with the server’s vault key and are opened only inside a delivery run. The console shows the envelope.', '')
      : code(`{
  "ticket_id": 88214,
  "status": "open",
  "priority": "high",
  "updated_at": "2026-11-16T09:41:07Z"
}`, { lang: 'json' });
    const rows = [
      ['<span class="mono">#1,205,193</span>', '<span class="mono">01JCX9…7Q2M</span>', '2s ago', 'JSON', '412 B', '<span class="muted">pending</span>'],
      { cls: open ? 'on' : '', cells: ['<span class="mono">#1,205,192</span>', '<span class="mono">01JCX9…7Q1D</span>', '2s ago', 'JSON', '388 B', '<span class="muted">pending</span>'] },
      ['<span class="mono">#1,204,881</span>', '<span class="mono">01JCX8…2KAF</span>', '24s ago', 'JSON', '1.2 KiB', 'to-warehouse · to-search'],
      ['<span class="mono">#1,204,880</span>', '<span class="mono">01JCX8…2K9T</span>', '24s ago', 'NDJSON line', '402 B', 'to-warehouse · to-search'],
      ['<span class="mono">#1,204,879</span>', '<span class="mono">01JCX8…2K8B</span>', '25s ago', 'JSON', '377 B', 'to-warehouse · to-search'],
    ];
    return `${dhead(dtab('Events'), actions(), 'Feeds')}${ticketsHead()}
      ${card({ icon: 'logs', title: 'Newest events', right: `${input('Go to event number or id', { ph: true, icon: 'search' })}`, body: tbl(['Event', 'Id', 'Received', 'Type', ['Size', 'num'], 'Delivered by'], rows),
        note: 'Event numbers only go up and are assigned when the event is stored. Events older than the replay window are gone.' })}
      ${open ? card({ icon: 'code', title: 'Event #1,205,192', aside: 'received 2s ago · 388 B · JSON', right: sealed ? '' : tag('admins only'), body: `${grid(3, [kv('Id', '<span class="mono">01JCX9…7Q1D</span>'), kv('Duplicate key', '<span class="mono">evt_5f02a1</span>'), kv('Sender', '203.0.113.24', 'token helpdesk-prod')])}<div class="pad" style="padding-top:0">${payload}</div>`,
        note: sealed ? '' : 'Opening a payload is recorded in the audit log.' }) : ''}`;
  };

  const refused = () => `${dhead(dtab('Refused'), actions(), 'Feeds')}${ticketsHead()}
    ${card({ title: 'Refused in the last 24 hours', aside: '96 of 41,304 requests · 0.2%', body: `<div class="pad">${bar([[52, 'var(--bad)'], [27, 'var(--warn)'], [14, 'var(--info)'], [7, 'var(--text-2)']])}${legend([['Bad signature · 50', 'var(--bad)'], ['Rate limited · 26', 'var(--warn)'], ['Not valid JSON · 13', 'var(--info)'], ['Too large · 7', 'var(--text-2)']])}</div>`,
      note: 'A refused request stores nothing and the sender gets the status shown. 212 repeats of an event already stored were answered 202 and are not counted here.' })}
    ${card({ icon: 'alert', title: 'Most recent', body: tbl(['When', 'Why', ['Answer', 'num'], 'Sender', 'Detail'], [
      ['1m ago', 'Bad signature', '401', '198.51.100.7', 'X-Signature-256 does not match the body'],
      ['4m ago', 'Rate limited', '429', '203.0.113.24', 'over 200 events/s · retry after 2s'],
      ['4m ago', 'Rate limited', '429', '203.0.113.24', 'over 200 events/s · retry after 2s'],
      ['31m ago', 'Not valid JSON', '400', '203.0.113.24', 'line 3 of 40: unexpected end of input'],
      ['2h ago', 'Too large', '413', '192.0.2.61', 'event of 1.8 MiB · limit 1 MiB']]),
      note: 'Payloads of refused requests are never kept.' })}`;

  const tokens = () => `${dhead(dtab('Tokens'), actions(), 'Feeds')}${ticketsHead()}
    ${card({ icon: 'key', title: 'Feed tokens', right: btn('New token', { icon: 'plus', act: true }), body: tbl(['Name', 'Starts with', 'Created', 'Last used', 'Expires', ''], [
      ['helpdesk-prod', '<span class="mono">fft_9k2…</span>', `12 Oct by ${lnk('ops-admin')}`, '2s ago', 'never', lnk('Revoke')],
      ['helpdesk-staging', '<span class="mono">fft_c41…</span>', `3 Nov by ${lnk('ops-raj')}`, '4d ago', 'in 26 days', lnk('Revoke')]]),
      note: 'A token is shown once, when it is made. Senders put it in <code>Authorization: Bearer …</code>. Revoking takes effect within 5 seconds.' })}
    ${card({ icon: 'shield', title: 'Signature check', right: btn('Change secret', { icon: 'edit', act: true }), body: grid(4, [kv('Header', '<span class="mono">X-Signature-256</span>'), kv('Algorithm', 'HMAC-SHA256', 'hex, prefix <span class="mono">sha256=</span>'), kv('Secret', '<span class="faint">sealed</span>', 'set 12 Oct'), kv('Timestamp', 'none', 'no replay window')]),
      note: 'With both set, a request needs a valid token and a valid signature.' })}`;

  const history = () => `${dhead(dtab('History'), actions(), 'Feeds')}${ticketsHead()}
    ${card({ title: 'Change history', right: 'from the audit log', body: `<div class="log" style="font-family:var(--sans);font-size:12.5px;line-height:2">
      <div><span class="muted" style="display:inline-block;width:120px">12 Oct 10:14</span><span class="dot muted"></span>Created by ${lnk('ops-admin')} · delivery to-warehouse · token helpdesk-prod</div>
      <div><span class="muted" style="display:inline-block;width:120px">21 Oct 15:40</span><span class="dot muted"></span>Delivery to-search added by ${lnk('ops-raj')}</div>
      <div><span class="muted" style="display:inline-block;width:120px">3 Nov 09:02</span><span class="dot muted"></span>Token helpdesk-staging made by ${lnk('ops-raj')} · expires in 30 days</div>
      <div><span class="muted" style="display:inline-block;width:120px">9 Nov 22:17</span><span class="dot warn"></span>to-warehouse paused by ${lnk('ops-raj')} · “warehouse maintenance”</div>
      <div><span class="muted" style="display:inline-block;width:120px">10 Nov 01:05</span><span class="dot ok"></span>to-warehouse resumed by ${lnk('ops-raj')} · 9,412 waiting events delivered in 2 runs</div>
      <div><span class="muted" style="display:inline-block;width:120px">14 Nov 11:30</span><span class="dot muted"></span>to-search replayed from 13 Nov 00:00 by ${lnk('ops-admin')} · 58,120 events</div>
      <div><span class="muted" style="display:inline-block;width:120px">15 Nov 08:12</span><span class="dot bad"></span>to-search skipped <span class="mono">#1,180,004–1,180,019</span> by ${lnk('ops-admin')} · 16 events · “test payloads sent to production by mistake”</div></div>` })}`;

  // ---- forms ----
  const seg = (items, on) => `<span class="seg">${items.map((x) => `<span class="${x === on ? 'on' : ''}">${x}</span>`).join('')}</span>`;
  const deliveryFields = ({ invalid = false, tenant = false } = {}) => `<div class="form">${field('Delivery name', input('to-warehouse', { mono: true }), { req: true })}${field('Start from', select('new events only'), { help: 'Or the oldest stored event' })}
      ${field('Template', select(tenant ? 'acme/events-to-lake · pipeline template' : 'acme/tickets-to-lake · pipeline template'), { req: true, help: tenant ? 'Only templates acme may run' : 'Its source must be <code>type: feed</code>', cls: 'wide' })}
      ${field('Version', seg(['stable', 'newest', 'channel', 'pin'], 'pin'), { help: 'pin v4 · never moves', cls: 'wide' })}${field('<code>table</code>', invalid ? input('', { cls: 'err' }) : input('support.tickets'), invalid ? { req: true, err: 'Required: this param has no default.', cls: 'wide' } : { req: true, help: 'string', cls: 'wide' })}</div>
    <div class="form three">${field('Flush at events', input('5,000'))}${field('or size', input('64 MiB'))}${field('or oldest waiting', input('60s'))}</div>`;
  const form = ({ mode = 'create', invalid = false, tenant = false, approval = false, rate = false } = {}) => {
    const name = mode === 'edit' ? 'support-tickets' : tenant ? 'signup-forms' : 'support-tickets';
    const nameField = invalid
      ? field('Name', input('Support Tickets', { mono: true, cls: 'err' }), { req: true, err: 'Lowercase letters, digits and dashes only.' })
      : field('Name', input(name, { mono: true, cls: mode === 'edit' ? 'dis' : '' }), { req: true, help: mode === 'edit' ? 'Names cannot change' : 'Becomes part of the URL' });
    const who = card({ icon: 'key', title: 'Who can send', body: `<div class="form">${nameField}${field('Sign-in', seg(['token', 'signature', 'both'], 'both'), { help: 'At least one is required', cls: 'wide' })}
        ${field('Signature header', input('X-Signature-256', { mono: true }))}${field('Signing secret', input('••••••••••••••••'), { help: 'Sealed with the server vault key, never shown again' })}
        ${field('Only from addresses', input('any address', { ph: true }), { help: 'Optional · one range per line', cls: 'wide' })}</div>`,
      note: mode === 'edit' ? 'Tokens are managed on the Tokens tab.' : 'A first token is made when you save and shown once.' });
    const accepts = card({ icon: 'filter', title: 'What it accepts', body: `<div class="form">${field('Content', `<div style="display:flex;gap:14px;height:30px;align-items:center">${check('JSON', true)}${check('NDJSON', true)}${check('Form', false)}${check('Text', false)}</div>`, { cls: 'wide' })}
        ${field('Largest event', input('1 MiB'))}${rate ? field('Events per second', input('2,000', { cls: 'err' }), { err: 'acme’s feeds may take at most 500 events per second in total; 380 are in use.' }) : field('Events per second', input('200'), { help: 'Above it senders get 429' })}
        ${field('Duplicate key', input('X-Event-Id', { mono: true }), { help: 'A header or a JSON path' })}${field('A repeat is answered', seg(['202', '409'], '202'), { help: 'Within 24 hours' })}</div>` });
    const keep = card({ icon: 'db', title: 'Keep and protect', body: `<div class="form">${field('Keep delivered events', input('7 days'), { help: 'For replay' })}${field('Refuse new events above', input(tenant ? '2 GiB or 30 days' : '50 GiB or 30 days'), { help: 'Of events not delivered yet' })}
        ${field('Store', select('default'), { help: 'Defined by the server' })}${field('Seal payloads', toggle(tenant ? 'On for tenant feeds' : 'Off', tenant), { help: 'Encrypted with the vault key' })}</div>` });
    const deliv = mode === 'edit' ? '' : card({ icon: 'runs', title: 'First delivery', aside: 'more can be added later', body: deliveryFields({ invalid, tenant }) });
    const editNote = mode === 'edit' ? card({ icon: 'diff', title: 'What changes', body: diff([['-', 'events per second: 200'], ['+', 'events per second: 500'], [' ', 'sign-in, retention, deliveries — unchanged']]), note: 'Takes effect within 5 seconds on every replica. Stored events are not touched.' }) : '';
    const limitAlert = tenant ? alert('acme can have 5 feeds; 4 are in use.', 'Its feeds share 500 events per second and 5 GiB of stored events.', '') : '';
    const bad = invalid || rate;
    const foot = `<div class="actions" style="border:0;padding:4px 0 0">${approval ? '<span class="muted">This server requires approval for runs; a feed with a delivery becomes a change request.</span>' : ''}${btn('Cancel', { kind: 'quiet' })}${btn(approval ? 'Request approval' : mode === 'edit' ? 'Save changes' : 'Create feed', { kind: 'primary', icon: approval ? 'changes' : 'check', cls: bad ? 'dis' : '' })}</div>`;
    return `${dhead('', '', 'Feeds')}${head(mode === 'edit' ? 'Edit support-tickets' : 'New feed', [], mode === 'edit' ? '' : 'A feed gives senders a URL to push events to. Every event is stored before it is answered, and each delivery runs a template over the new events.')}
      ${limitAlert}${invalid ? alert('2 fields need attention.', 'Nothing is saved until they are fixed.', 'bad') : ''}
      <div class="pair"><div class="stack">${who}${accepts}</div><div class="stack">${keep}${deliv}${editNote}</div></div>${foot}`;
  };
  const CURL = `curl -X POST ${url('support-tickets')} \\
  -H "Authorization: Bearer fft_9k2R…b7Qe" \\
  -H "X-Event-Id: evt_5f02a1" \\
  -H "Content-Type: application/json" \\
  -d '{"ticket_id": 88214, "status": "open"}'`;

  M.view('feeds', {
    label: 'Feeds', nav: 'feeds', icon: 'feed', summary: 'durable inboxes for pushed events; deliveries run templates over them (#1034)',
    states: {
      list: { label: 'All feeds · nothing selected', sel: false, covers: ['GET /v1/feeds', 'pending / refused / stalled at a glance', 'source console | file', 'tenant'], new: ['Feeds page (#1034)'] },
      selected: { label: 'Feed selected · overview', covers: ['GET /v1/feeds/{name}', 'ingest URL + sign-in', 'received / delivered / pending / refused', 'storage against retention limits', 'deliveries'] },
      delivery: { label: 'Delivery · position, flush policy, runs', covers: ['cursor, newest event, behind', 'flush policy', 'template + version + params', 'runs it started with their event ranges'] },
      events: { label: 'Events tab', covers: ['GET /v1/feeds/{name}/events (envelopes)', 'which deliveries have each event'] },
      'event-open': { label: 'Event opened · payload', covers: ['payload for admins (FeedAdmin), audited'] },
      sealed: { label: 'Event opened · sealed feed', covers: ['sealed payloads are never shown'] },
      refused: { label: 'Refused tab', covers: ['refusals by reason', 'recent refusals with status and sender', 'duplicates counted apart'] },
      tokens: { label: 'Tokens tab', covers: ['GET /v1/feeds/{name}/tokens', 'signature check settings'] },
      'token-new': { label: 'New token · shown once', covers: ['POST /v1/feeds/{name}/tokens', 'optional expiry'] },
      'token-revoke': { label: 'Revoke token dialog', covers: ['DELETE /v1/feeds/{name}/tokens/{id}'] },
      history: { label: 'History tab', covers: ['create / edit / tokens / pause / resume / replay from the audit log', 'skipped ranges (gaps)'] },
      create: { label: 'New feed', covers: ['POST /v1/feeds', 'sign-in, accepted content, limits, duplicates', 'retention + store + sealing', 'first delivery'] },
      invalid: { label: 'New feed · validation errors', covers: ['bad name', 'required param missing'] },
      created: { label: 'Feed created · URL and token', covers: ['token shown once', 'copyable curl'] },
      edit: { label: 'Edit feed', covers: ['PUT /v1/feeds/{name}', 'what changes'] },
      'delivery-new': { label: 'Add a delivery', covers: ['POST /v1/feeds/{name}/deliveries', 'start from now or the oldest event'] },
      test: { label: 'Send test event dialog', covers: ['POST …/events from the console', 'copy as curl'] },
      'deliver-now': { label: 'Deliver now dialog', covers: ['POST …/deliveries/{d}/flush'] },
      pause: { label: 'Pause delivery dialog', covers: ['POST …/deliveries/{d}/pause'] },
      paused: { label: 'Paused delivery', covers: ['events keep arriving', 'who paused and why', 'resume'] },
      stalled: { label: 'Delivery stalled after failed runs', covers: ['failing event range', 'retry', 'never skipped silently'] },
      skip: { label: 'Skip events dialog · admins', covers: ['POST …/deliveries/{d}/skip', 'gap recorded, reason required'] },
      full: { label: 'Feed full · refusing new events', covers: ['undelivered limit reached', '503 to senders, nothing dropped'] },
      held: { label: 'Feed held after a store restore', covers: ['POST /v1/feeds/{name}/recover'] },
      replay: { label: 'Replay dialog', covers: ['POST …/deliveries/{d}/replay', 'event count before confirming'] },
      delete: { label: 'Delete dialog', covers: ['DELETE /v1/feeds/{name}', 'stored events are removed'] },
      file: { label: 'From the feeds file · read only', covers: ['--feeds file', 'test event only'] },
      approval: { label: 'New feed under --require-approval run', covers: ['becomes a change request'] },
      requested: { label: 'Change request created', covers: ['pending feed, link to Changes'] },
      empty: { label: 'No feeds', sel: false, covers: ['empty state'] },
      off: { label: 'Feeds not built in', sel: false, covers: ['binary without the feeds feature'] },
      loading: { label: 'Loading', sel: false, covers: ['skeleton'] },
      error: { label: 'Could not load', sel: false, covers: ['error with retry'] },
      'store-down': { label: 'Feed store unreachable', covers: ['ingest answers 503', 'deliveries hold'] },
      viewer: { label: 'Viewer · read only', params: { role: 'viewer' }, covers: ['no write controls', 'no payloads'] },
      tenant: { label: 'Tenant principal · own feed', params: { scope: 'tenant' }, covers: ['tenant-scoped list and URL', 'quotas shown', 'sealed by default'] },
      'tenant-create': { label: 'Tenant · rate above the tenant quota', params: { scope: 'tenant' }, covers: ['events/s quota guardrail', 'only templates the tenant may run'] },
      'tenant-limit': { label: 'Tenant · max_feeds reached', params: { scope: 'tenant' }, sel: false, covers: ['max_feeds guardrail'] },
      'scoped-list': { label: 'Operator · switcher set to acme', params: { tenant: 'acme' }, sel: false, covers: ['GET /v1/feeds?tenant=acme'] },
    },
    render(state, ctx) {
      const crumbs = (...t) => ['Operate', 'Feeds', ...t.filter(Boolean)];
      if (state === 'loading') {
        M.top(crumbs());
        M.list(`${tabs([['All', '…']], 'All', ibtn('refresh'))}${skelRows(8)}`);
        M.detail(skelCards(3));
        return;
      }
      if (state === 'error') {
        M.top(crumbs());
        M.list(`${tabs([['All', '—']], 'All', ibtn('refresh'))}${empty('Could not load feeds', 'The server answered 503.', { icon: 'alert', btns: btn('Retry', { icon: 'refresh' }) })}`);
        M.detail(alert('Feeds cannot be listed right now.', 'Their settings are kept in the run-history database, which this server cannot reach. Senders are still answered for up to a minute, and stored events are safe.', 'bad', btn('Retry', { icon: 'refresh' })));
        return;
      }
      if (state === 'empty' || state === 'off') {
        M.top(crumbs());
        const off = state === 'off';
        M.list(`${tabs([['All', 0]], 'All', ibtn('refresh') + (off ? '' : newBtn()))}${empty(off ? 'Feeds are not built in' : 'No feeds yet', off ? 'This faucet binary was built without the feeds feature.' : 'A feed gives senders a URL to push events to.', { icon: 'feed' })}`);
        M.detail(off
          ? `${head('Feeds need the feeds feature', [], 'This server was built without it, so it cannot take pushed events. The release binaries, the Homebrew formula and the <code>full</code> container image include it.')}${card({ icon: 'code', title: 'Build it in', body: code('cargo install faucet-cli --features "serve,feeds"', { box: false, lang: 'plain' }) })}`
          : `${head('Take pushed events without losing any', [], 'Senders post to a feed’s URL. The server stores each event before it answers, then runs your template over the new events every few seconds or minutes.')}
            <div class="two">${card({ icon: 'plus', title: 'In the console', body: `<div class="pad stack" style="gap:10px"><span>Name the feed, choose how senders sign in, and pick the template that delivers its events.</span><div>${btn('New feed', { icon: 'plus', kind: 'primary', act: true })}</div></div>` })}
            ${card({ icon: 'code', title: 'feeds.yaml', body: code(`version: 1
feeds:
  - name: support-tickets
    ingest:
      auth: { tokens: managed }
    deliveries:
      - name: to-warehouse
        template: { id: acme/tickets-to-lake, version: 4 }
        flush: { max_events: 5000, max_age: 60s }`, { box: false }), note: 'Then start the server with <code>faucet serve --feeds feeds.yaml</code>.' })}</div>`);
        return;
      }
      if (state === 'scoped-list') {
        M.top(crumbs());
        M.list(listPane('', { scoped: true }));
        M.detail(`${head('1 feed belongs to acme', ['<span>63,402 events today</span>', '<span>1,940 pending</span>'])}
          ${card({ title: 'acme’s quotas', body: grid(3, [kv('Feeds', '1 of 5'), kv('Events per second', '380 of 500', 'across its feeds'), kv('Stored events', '1.1 of 5 GiB')]) })}
          ${card({ title: 'Deliveries', body: tbl(['Feed', 'Delivery', 'Behind', 'Last run'], [[lnk('app-events'), lnk('to-lake'), '1,940 · 12s', '<span class="ok">● completed</span> 8s ago']]) })}`);
        return;
      }
      if (state.startsWith('tenant')) {
        const t = 'acme';
        if (state === 'tenant-limit') {
          M.top(crumbs());
          M.list(listPane('', { tenant: true, limit: true }));
          M.detail(`${head('acme’s feeds', ['<span>5 of 5 in use</span>', '<span>380 of 500 events per second</span>'])}
            ${alert('acme has reached its limit of 5 feeds.', 'Delete one to create another, or ask an operator to raise <code>max_feeds</code>.', 'warn')}
            ${card({ title: 'acme’s quotas', body: grid(3, [kv('Feeds', '5 of 5', 'at the limit'), kv('Events per second', '380 of 500', 'across its feeds'), kv('Stored events', '1.1 of 5 GiB')]) })}`);
          return;
        }
        M.top(crumbs(state === 'tenant' ? 'app-events' : 'New feed'));
        M.list(listPane(state === 'tenant' ? 'app' : '', { tenant: true }));
        if (state === 'tenant-create') { M.detail(form({ tenant: true, rate: true })); return; }
        M.detail(`${dhead(dtab('Overview', { deliveries: 1, refused: 4, tokens: 1 }), actions(), 'Feeds')}
          ${head('app-events', [pill('Receiving', 'ok', 'check'), '<span>last event 1s ago</span>', chip('payloads sealed'), `<span>created by ${lnk('acme-portal')}</span>`])}
          <div class="two"><div class="stack">${sendCard({ name: 'app-events', tenant: t, auth: 'token', sig: '' })}${flowCard({ rec: '63,402', del: '61,462', pend: '1,940', ref: '4' })}</div>
          <div class="stack">${card({ icon: 'tenants', title: 'acme’s quotas', body: grid(3, [kv('Feeds', '4 of 5'), kv('Events / second', '380 of 500'), kv('Stored', '1.1 of 5 GiB')]), note: 'Set by the server operator. Above the rate, senders get 429.' })}${storeCard({ und: '6.4 MiB of 5 GiB', kept: '1.1 GiB' })}</div></div>
          ${card({ icon: 'runs', title: 'Deliveries', right: btn('Add delivery', { icon: 'plus', act: true }), body: tbl(['Delivery', 'Runs', 'Flushes at', 'Behind', 'Last run'], [[lnk('to-lake'), lnk('acme/events-to-lake'), '10,000 events or 30s', '1,940 · 12s', '<span class="ok">● completed</span> 8s ago']]) })}`);
        return;
      }

      const sel = { stalled: 'partner', skip: 'partner', full: 'pings', held: 'billing', file: 'audit', paused: 'legacy', create: '', invalid: '', approval: '', requested: '', created: 'tickets' }[state];
      const selected = sel === undefined ? 'tickets' : sel;
      const title = { partner: 'partner-callbacks', pings: 'device-pings', billing: 'billing-events', audit: 'audit-trail', legacy: 'legacy-import', tickets: 'support-tickets', '': 'New feed' }[selected];
      const sub = { delivery: 'to-warehouse', 'deliver-now': 'to-warehouse', pause: 'to-warehouse', replay: 'to-warehouse', 'delivery-new': 'New delivery', paused: 'to-lake', stalled: 'to-warehouse', skip: 'to-warehouse' }[state];
      M.top(crumbs(state === 'list' ? '' : title, sub));
      M.list(listPane(state === 'list' ? '' : selected));

      if (state === 'list') {
        M.detail(`${head('7 feeds', ['<span>212,430 events today</span>', '<span class="warn">26,866 pending</span>', '<span>1.2% refused</span>'])}
          ${alert('partner-callbacks has not delivered for 3 hours.', 'Its delivery to-warehouse stalled after 5 failed runs. Events are still being stored.', 'bad', btn('Open', { icon: 'right' }))}
          ${alert('device-pings is refusing new events.', 'It holds 50 GiB of events that are not delivered yet, which is its limit.', 'bad', btn('Open', { icon: 'right' }))}
          ${card({ title: 'Deliveries', right: 'most behind first', body: tbl(['Feed', 'Delivery', 'Behind', 'Last run'], [
            [lnk('partner-callbacks'), lnk('to-warehouse'), '<span class="bad">18,204 · 3h 12m</span>', '<span class="bad">● failed</span> 2m ago'],
            [lnk('legacy-import'), lnk('to-lake'), '6,410 · paused', '—'],
            [lnk('app-events'), lnk('to-lake'), '1,940 · 12s', '<span class="ok">● completed</span> 8s ago'],
            [lnk('support-tickets'), lnk('to-warehouse'), '312 · 38s', '<span class="ok">● completed</span> 22s ago'],
            [lnk('support-tickets'), lnk('to-search'), '0 · up to date', '<span class="ok">● completed</span> 6s ago']]) })}`);
        return;
      }
      if (['create', 'invalid', 'approval', 'edit'].includes(state)) {
        M.detail(form({ mode: state === 'edit' ? 'edit' : 'create', invalid: state === 'invalid', approval: state === 'approval' }));
        return;
      }
      if (state === 'requested') {
        M.detail(`${dhead('', btn('Open in Changes', { icon: 'changes', kind: 'primary' }), 'Feeds')}${head('support-tickets', [pill('Pending approval', 'warn'), `<span>requested by ${lnk('ops-raj')} · just now</span>`, `<span>change ${lnk('cr-0152', true)}</span>`])}
          ${alert('Change request cr-0152 created.', 'The feed and its URL exist once an approver accepts it on the Changes page. Nothing is received or run until then.', 'warn', btn('Open in Changes', { icon: 'changes' }))}
          ${card({ icon: 'diff', title: 'What was requested', body: diff([['+', 'feed: support-tickets · token + signature'], ['+', 'accepts: JSON, NDJSON · 1 MiB · 200 events/s'], ['+', 'retention: 7 days · refuse above 50 GiB or 30 days'], ['+', 'delivery to-warehouse: acme/tickets-to-lake @ pin v4'], ['+', 'flush: 5,000 events or 64 MiB or 60s']]) })}`);
        return;
      }
      if (state === 'created') {
        M.detail(`${dhead(dtab('Overview', { deliveries: 1, refused: 0, tokens: 1 }), actions(), 'Feeds')}${head('support-tickets', [pill('Waiting for the first event', '', 'clock'), '<span>created just now</span>'])}
          ${alert('Copy the token now. It is not shown again.', '', 'warn')}
          <div class="two"><div class="stack">${card({ icon: 'key', title: 'Token helpdesk-prod', body: `<div class="pad">${input(`fft_9k2RmV0aXN0aGVmZWVkdG9rZW5leGFtcGxlb7Qe${cpi()}`, { mono: true })}</div>`, note: 'Stored only as a hash. Lost it? Make a new token and revoke this one.' })}
            ${card({ icon: 'code', title: 'Send the first event', body: code(CURL, { box: false, lang: 'plain' }) })}</div>
          <div class="stack">${card({ icon: 'check', title: 'What happens next', body: `<div class="pad stack" style="gap:8px"><span>1. The sender posts to the URL and gets <b>202</b> once the event is stored.</span><span>2. Within 60 seconds, to-warehouse runs <b>acme/tickets-to-lake</b> over the new events.</span><span>3. The delivery moves forward only after that run has written them.</span></div>` })}</div></div>`);
        return;
      }
      if (state === 'delivery-new') {
        M.detail(`${dhead('', '', 'support-tickets')}${head('Add a delivery', [], 'Each delivery reads the same events at its own pace and runs its own template.')}
          <div class="pair"><div class="stack">${card({ icon: 'runs', title: 'Delivery', body: deliveryFields() })}</div>
          <div class="stack">${card({ icon: 'info', title: 'Good to know', body: `<div class="pad stack" style="gap:8px"><span>Starting from the oldest stored event delivers <b>41,480 events</b> from the last 7 days first.</span><span>A delivery that falls behind keeps its events from being removed, up to the feed’s limit.</span></div>` })}</div></div>
          <div class="actions" style="border:0;padding:4px 0 0">${btn('Cancel', { kind: 'quiet' })}${btn('Add delivery', { kind: 'primary', icon: 'check' })}</div>`);
        return;
      }
      if (state === 'file') {
        M.detail(`${dhead(dtab('Overview', { deliveries: 1, refused: 0, tokens: 0 }), actions({ file: true }), 'Feeds')}
          ${head('audit-trail', [pill('Receiving', 'ok', 'check'), chip('from feeds file'), '<span>last event 3m ago</span>'])}
          ${alert('Defined in the server’s feeds file.', 'Change it in <code>feeds.yaml</code> and restart the server. The console can send a test event but cannot edit, pause or delete it.', '')}
          <div class="two"><div class="stack">${sendCard({ name: 'audit-trail', auth: 'signature', sig: 'X-Signature-256' })}${flowCard({ rec: '1,204', del: '1,204', pend: '0', ref: '0', pts: [3, 4, 2, 5, 4, 6, 3, 2, 4, 5, 3, 4] })}</div>
          <div class="stack">${card({ icon: 'code', title: 'feeds.yaml', aside: 'as loaded at start', body: code(`- name: audit-trail
  ingest:
    auth:
      signature: { header: X-Signature-256, secret: "\${secret:vault:feeds/audit#hmac}" }
  deliveries:
    - name: to-archive
      pipeline: ./pipelines/audit_to_archive.yaml
      flush: { max_events: 1000, max_age: 5m }`, { box: false }) })}</div></div>`);
        return;
      }
      if (state === 'stalled' || state === 'skip') {
        M.detail(`${dhead(dtab('Deliveries', { deliveries: 1, refused: 12 }), `${btn('Try again now', { icon: 'refresh', kind: 'primary', act: true })}${btn('Pause', { icon: 'pause', act: true })}${ibtn('more', { act: true })}`, 'partner-callbacks')}
          ${head('to-warehouse', [pill('Stalled', 'bad', 'alert'), `<span>runs ${lnk('partners/callbacks-to-warehouse')} <span class="mono">stable</span></span>`, '<span>at least once</span>'])}
          ${alert('Stopped after 5 failed runs. No events were skipped.', 'Every run failed on the same events with <code>sink: connection refused (warehouse.internal:5439)</code>. New events are still stored, and delivery continues from the same place once a run succeeds.', 'bad', btn('Open the last run', { icon: 'right' }) + btn('Skip events…', { icon: 'right', act: true }))}
          <div class="two"><div class="stack">${card({ icon: 'feed', title: 'Position', body: grid(4, [kv('Delivered to', '<span class="mono">#402,117</span>', '3h 12m ago'), kv('Newest event', '<span class="mono">#420,321</span>'), kv('Behind', '<span class="bad">18,204 events</span>', '71 MiB'), kv('Feed limit', '0.1% used', 'of 50 GiB')]) })}</div>
          <div class="stack">${storeCard({ und: '71 MiB of 50 GiB', oldest: '3 hours', kept: '310 MiB' })}</div></div>
${card({ icon: 'runs', title: 'Failed runs', body: tbl(['Run', 'Events', 'Status', 'Why'], [
              [lnk('to-warehouse · 2c91'), '<span class="mono">#402,118–407,117</span>', '<span class="bad">● failed</span>', 'sink: connection refused'],
              [lnk('to-warehouse · 2c90'), '<span class="mono">#402,118–407,117</span>', '<span class="bad">● failed</span>', 'sink: connection refused'],
              [lnk('to-warehouse · 2c8f'), '<span class="mono">#402,118–407,117</span>', '<span class="bad">● failed</span>', 'sink: connection refused']]) })}`);
        if (state === 'skip') M.overlay(dialog('Skip events for to-warehouse?', `<span>Moves this delivery past events it cannot deliver. The skipped events stay stored until they age out, and the gap is recorded on the feed for good.</span>
          ${field('Skip up to and including', input('#407,117', { mono: true }), { help: 'The range every failed run stopped on: #402,118–407,117 · 5,000 events' })}
          ${field('Reason', '<div class="area sans">Malformed batch from the partner, re-sent by them as #418,002 onward</div>', { req: true, help: 'Shown on the feed and in the audit log' })}
          ${alert('5,000 events will never reach this delivery’s destination.', 'Other deliveries of this feed are not affected.', 'warn')}`, `${btn('Cancel')}${btn('Skip 5,000 events', { kind: 'danger', icon: 'right' })}`, 'alert'));
        return;
      }
      if (state === 'full') {
        M.detail(`${dhead(dtab('Overview', { deliveries: 1, refused: 9412 }), actions(), 'Feeds')}
          ${head('device-pings', [pill('Refusing events', 'bad', 'alert'), '<span>last stored event 41m ago</span>', '<span>1 delivery</span>'])}
          ${alert('This feed holds as much as it is allowed to.', 'Its delivery to-lake has been paused for 2 days, so 50 GiB of events are waiting. Senders get <b>503</b> with a retry time and nothing already stored is dropped. Resume or fix the delivery, or raise the limit.', 'bad', btn('Open to-lake', { icon: 'right' }) + btn('Raise the limit', { icon: 'edit', act: true }))}
          <div class="two"><div class="stack">${sendCard({ name: 'device-pings', auth: 'token', sig: '' })}${flowCard({ rec: '1,920,400', del: '0', pend: '48,112,900', ref: '9,412', pendCls: 'bad', pts: [60, 61, 59, 62, 60, 61, 58, 60, 12, 0, 0, 0] })}</div>
          <div class="stack">${storeCard({ und: '50 GiB of 50 GiB', undPct: 100, oldest: '2 days', oldPct: 7, kept: '0', tone: 'bad' })}</div></div>`);
        return;
      }
      if (state === 'held') {
        M.detail(`${dhead(dtab('Overview', { deliveries: 1, refused: 0, tokens: 1 }), btn('Recover…', { icon: 'refresh', kind: 'primary', act: true }), 'Feeds')}
          ${head('billing-events', [pill('Held', 'warn', 'alert'), '<span>not receiving</span>', '<span>1 delivery waiting</span>'])}
          ${alert('The feed store looks restored from a backup.', 'The delivery to-ledger says it delivered up to <span class="mono">#88,410</span>, but the store’s newest event is <span class="mono">#86,902</span>. Receiving and delivering are stopped so nothing is skipped or repeated by accident.', 'warn')}
          ${card({ icon: 'info', title: 'Recover', body: `<div class="pad stack" style="gap:8px"><span><b>Continue from the store’s newest event.</b> Events <span class="mono">#86,903–88,410</span> were lost with the restore and are recorded as a gap in History.</span><span>Senders get 503 until an admin recovers the feed.</span></div>`, note: 'Recovering is admin only and is written to the audit log.' })}`);
        return;
      }
      if (state === 'store-down') {
        M.detail(`${dhead(dtab('Overview'), actions(), 'Feeds')}${head('support-tickets', [pill('Store unreachable', 'bad', 'alert'), '<span>last event 2m ago</span>', '<span>2 deliveries holding</span>'])}
          ${alert('The feed store has not answered for 2 minutes.', 'Senders get <b>503</b> with a retry time, so no event is accepted without being stored. Deliveries wait and continue from the same place when the store is back.', 'bad', btn('Retry', { icon: 'refresh' }))}
          <div class="two"><div class="stack">${sendCard()}${flowCard({ rec: '40,112', del: '39,800', pend: '312', ref: '1,908', pts: [22, 30, 28, 41, 36, 52, 47, 60, 44, 38, 4, 0] })}</div><div class="stack">${card({ icon: 'db', title: 'Store', body: grid(2, [kv('Name', '<span class="mono">default</span>', 'postgres'), kv('Last answer', '2m ago', 'connection timed out')]) })}</div></div>`);
        return;
      }
      if (state === 'paused') {
        M.detail(`${dhead(dtab('Deliveries', { deliveries: 1, refused: 0, tokens: 1 }), `${btn('Resume', { icon: 'play', kind: 'primary', act: true })}${btn('Edit', { icon: 'edit', act: true })}${ibtn('more', { act: true })}`, 'legacy-import')}
          ${head('to-lake', [pill('Paused', '', 'pause'), `<span>runs ${lnk('finance/import-to-lake')} <span class="mono">stable</span></span>`, '<span>at least once</span>'])}
          ${alert(`Paused by ${lnk('ops-raj')} 2 hours ago.`, '“Lake maintenance until 18:00.” Events keep arriving and are stored; 6,410 are waiting. They are delivered in order when it resumes.', '')}
          <div class="two"><div class="stack">${card({ icon: 'feed', title: 'Position', body: grid(4, [kv('Delivered to', '<span class="mono">#90,022</span>', '2h ago'), kv('Newest event', '<span class="mono">#96,432</span>'), kv('Behind', '6,410 events', '24 MiB'), kv('Feed limit', '0.05% used', 'of 50 GiB')]) })}</div>
          <div class="stack">${storeCard({ und: '24 MiB of 50 GiB', oldest: '2 hours', kept: '120 MiB' })}</div></div>`);
        return;
      }
      const viewerNote = ctx.role === 'viewer';
      if (state === 'delivery' || state === 'deliver-now' || state === 'pause' || state === 'replay') M.detail(delivery());
      else if (state === 'events') M.detail(events());
      else if (state === 'event-open') M.detail(events({ open: true }));
      else if (state === 'sealed') M.detail(events({ open: true, sealed: true }));
      else if (state === 'refused') M.detail(refused());
      else if (state === 'tokens' || state === 'token-new' || state === 'token-revoke') M.detail(tokens());
      else if (state === 'history') M.detail(history());
      else M.detail(viewerNote ? `${dhead(dtab('Overview'), '', 'Feeds')}${ticketsHead()}<div class="two"><div class="stack">${sendCard()}${flowCard()}</div><div class="stack">${storeCard()}</div></div>${card({ icon: 'runs', title: 'Deliveries', body: tbl(['Delivery', 'Runs', 'Flushes at', 'Behind', 'Last run'], delivRows()) })}` : overview());

      if (state === 'test') M.overlay(dialog('Send a test event to support-tickets', `<span>Stores one event exactly as a sender would, signed with a console-only credential. Its deliveries will run it like any other event.</span>
        ${field('Body', `<div class="area mono">{"ticket_id": 0, "status": "test", "note": "sent from the console"}</div>`, { help: 'JSON · 68 B' })}
        ${alert('Stored as event #1,205,194 · answered 202 in 9 ms.', '', 'ok')}`, `${btn('Copy as curl', { icon: 'copy' })}${btn('Close')}${btn('Send again', { kind: 'primary', icon: 'play' })}`, 'play'));
      if (state === 'token-new') M.overlay(dialog('New token for support-tickets', `${field('Name', input('helpdesk-eu', { mono: true }), { req: true })}${field('Expires', select('in 90 days'), { help: 'Or never' })}
        ${alert('Copy it now. It is not shown again.', '', 'warn')}${input(`fft_Qm8xZXhhbXBsZXRva2VudmFsdWVub3RyZWFs4tT2${cpi()}`, { mono: true })}`, `${btn('Done', { kind: 'primary', icon: 'check' })}`, 'key'));
      if (state === 'token-revoke') M.overlay(dialog('Revoke helpdesk-staging?', `<span>Requests that use this token are refused with <b>401</b> within 5 seconds. Events it already sent stay stored. This cannot be undone.</span>`, `${btn('Cancel')}${btn('Revoke token', { kind: 'danger', icon: 'trash' })}`, 'key'));
      if (state === 'deliver-now') M.overlay(dialog('Deliver to-warehouse now?', `<span>Starts one run over the <b>312 events</b> that are waiting, without waiting for the flush policy.</span>
        ${alert('No run of this delivery is in progress.', '', 'ok')}`, `${btn('Cancel')}${btn('Deliver now', { kind: 'primary', icon: 'play' })}`, 'play'));
      if (state === 'pause') M.overlay(dialog('Pause to-warehouse?', `<span>No runs start while it is paused. Events keep arriving and are stored, and they count toward the feed’s limit of 50 GiB.</span>
        ${field('Reason', '<div class="area sans">Warehouse maintenance tonight</div>', { help: 'Optional · shown on the delivery and in the audit log' })}`, `${btn('Cancel')}${btn('Pause', { kind: 'primary', icon: 'pause' })}`, 'pause'));
      if (state === 'replay') M.overlay(dialog('Replay to-warehouse', `<span>Moves the delivery back so it delivers stored events again. Events from before the replay window are gone and cannot be replayed.</span>
        ${field('Replay from', select('13 Nov 2026 00:00 UTC'), { help: 'Earliest possible: 9 Nov 09:41 · event #1,163,400' })}
        ${alert('58,120 events will be delivered again, in 2 runs.', 'This sink is exactly once only for new events. Rows already written by earlier runs will be written again unless the template upserts by key.', 'warn')}`, `${btn('Cancel')}${btn('Replay 58,120 events', { kind: 'primary', icon: 'undo' })}`, 'undo'));
      if (state === 'delete') M.overlay(dialog('Delete support-tickets?', `<span>Its URL stops accepting events at once, and its <b>1.2 GiB</b> of stored events are removed, including <b>312 that are not delivered yet</b>. Runs it already started stay in history.</span>${field('Type <code>support-tickets</code> to confirm', input('support-tickets', { mono: true, cls: 'focus' }))}`, `${btn('Cancel')}${btn('Delete feed and its events', { kind: 'danger', icon: 'trash' })}`, 'trash'));
    },
  });
})();
