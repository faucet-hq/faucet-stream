// Triggers — split pane: event triggers the server watches (webhooks, object
// arrival, queue depth, schedules), their health and the runs they started.
// Type, schedule and last fire come from the proposed GET /v1/triggers (#6);
// today the server reports only each trigger's name and health.
(function () {
  const L = {
    ingest: (o) => row(ring('ok'), 'orders-ingest', '2m', '<b>webhook</b> · POST, PUT · 38 runs today', o),
    partner: (o) => row(ring('ok'), 'partner-files', '11m', '<b>object arrival</b> · s3 prefix landing/partner/', o),
    lake: (o) => row(ring('ok'), 'lake-invoices', '1h', '<b>object arrival</b> · batch mode · gcs', o),
    queue: (o) => row(ring('bad'), 'events-backlog', '3h', '<b>queue depth</b> · redis stream · error backoff', o),
    nightly: (o) => row(ring('ok'), 'nightly-rollup', '9h', '<b>schedule</b> · 02:00 Europe/Berlin · 6 tenants', o),
    hourly: (o) => row(ring('ok'), 'hourly-crm', '42m', '<b>schedule</b> · every hour at :15', o),
  };
  const listPane = (sel) => {
    const o = (k) => ({ on: k === sel });
    return `${tabs([['All', 6], ['Unhealthy', 1]], 'All', ibtn('refresh'))}
      ${stats([['Triggers', '6'], ['Healthy', '5', 'ok'], ['Unhealthy', '1', 'bad'], ['Fired today', '214']])}
      ${sect('Webhooks', 1)}${L.ingest(o('ingest'))}
      ${sect('Object arrival', 2)}${L.partner(o('partner'))}${L.lake(o('lake'))}
      ${sect('Queue depth', 1)}${L.queue(o('queue'))}
      ${sect('Schedules', 2)}${L.nightly(o('nightly'))}${L.hourly(o('hourly'))}`;
  };
  const CURL = `curl -X POST https://faucet.example.internal/v1/triggers/orders-ingest \\
  -H "Authorization: Bearer $FAUCET_TOKEN" \\
  -H "X-Delivery-Id: 7c1e-0412" \\
  -H "Content-Type: application/json" \\
  -d '{"order_ids": [1042, 1043]}'`;
  const runsCard = () => card({ icon: 'runs', title: 'Runs started by this trigger', right: lnk('all 38 in Runs'),
    body: tbl(['Run', 'Status', ['Rows', 'num'], 'Started'], [
      [lnk('orders-ingest · 0412'), '<span class="info">● running</span>', '1,204', '2m ago'],
      [lnk('orders-ingest · 0411'), '<span class="ok">● completed</span>', '880', '9m ago'],
      [lnk('orders-ingest · 0410'), '<span class="ok">● completed</span>', '2,310', '21m ago'],
      [lnk('orders-ingest · 0409'), '<span class="faint">● coalesced</span>', '—', '22m ago'],
    ]) });
  const webhook = () => `${dhead(dtabs(['Overview', ['Runs', 38]], 'Overview'), btn('Copy curl', { icon: 'copy' }) + btn('Fire', { icon: 'play', kind: 'primary', act: true }), 'Triggers')}
    ${head('orders-ingest', [pill('Healthy', 'ok', 'check'), '<span>webhook</span>', `<span>runs template ${lnk('acme/billing-api')} <span class="mono">stable</span></span>`, '<span>last fired 2m ago</span>'])}
    ${card({ icon: 'trig', title: 'Webhook', aside: 'fires on POST or PUT to its URL', right: 'from triggers.yaml',
      body: grid(4, [kv('Methods', 'POST, PUT'), kv('Dedupe header', '<span class="mono">X-Delivery-Id</span>', 'same value → same run'), kv('Debounce', '30s', 'leading edge'), kv('Fired today', '38', '2 coalesced · 0 errors')]),
      note: 'Runs are labelled <code>trigger=orders-ingest</code> and audited as <b>trigger:orders-ingest</b>. The request body is available to the config as <code>${trigger.body}</code>.' })}
    ${card({ icon: 'code', title: 'Fire it from anywhere', aside: 'needs a token with trigger_fire', body: code(CURL, { box: false, lang: 'plain' }) })}
    ${runsCard()}`;

  M.view('triggers', {
    label: 'Triggers', nav: 'triggers', icon: 'trig', summary: 'event triggers, their health and the runs they start',
    states: {
      webhook: { label: 'Webhook selected', covers: ['trigger name + health (/readyz)', 'fire POST/PUT /v1/triggers/{name} (trigger_fire)'], new: ['Triggers page', 'type / methods / last fire (api-proposal #6)', 'curl snippet with copy', 'runs started by this trigger'] },
      fire: { label: 'Fire dialog with body editor', covers: ['fire a webhook with an optional JSON body'], new: ['body editor + dedupe key'] },
      fired: { label: 'Fired · toast', covers: ['202 enqueued / 200 coalesced outcomes'], new: ['toast linking to the run'] },
      unhealthy: { label: 'Unhealthy trigger', covers: ['healthy: false from /readyz'], new: ['error, backoff and last good poll (api-proposal #6)'] },
      empty: { label: 'No triggers configured', sel: false, covers: ['server without --triggers'], new: ['empty state that says how to start'] },
    },
    render(state) {
      if (state === 'empty') {
        M.top(['Operate', 'Triggers']);
        M.list(`${tabs([['All', 0]], 'All', ibtn('refresh'))}${empty('No triggers on this server', 'Triggers start runs when a webhook is called, a file lands, a queue backs up or a schedule ticks.', { icon: 'trig' })}`);
        M.detail(`${head('Start runs from events', [], 'This server was started without a triggers file. Add one and restart <code>faucet serve</code> with <code>--triggers</code>.')}
          ${card({ icon: 'code', title: 'triggers.yaml', aside: 'one webhook, one schedule', body: code(`version: 1
triggers:
  - name: orders-ingest
    type: webhook
    methods: [POST]
    dedupe_header: X-Delivery-Id
    config: pipelines/orders.yaml
  - name: nightly-rollup
    type: schedule
    cron: "0 2 * * *"
    timezone: Europe/Berlin
    template: { id: acme/billing-api }`, { box: false }) })}
          ${card({ title: 'Then', body: code('faucet serve --triggers triggers.yaml', { box: false, lang: 'plain' }), note: `Read the ${lnk('triggers reference')} for object arrival and queue depth.` })}`);
        return;
      }
      if (state === 'unhealthy') {
        M.top(['Operate', 'Triggers', 'events-backlog']);
        M.list(listPane('queue'));
        M.detail(`${dhead(dtabs(['Overview', ['Runs', 12]], 'Overview'), btn('Copy name', { icon: 'copy' }), 'Triggers')}
          ${head('events-backlog', [pill('Unhealthy', 'bad', 'alert'), '<span>queue depth</span>', `<span>runs ${lnk('clickstream_kafka')} config</span>`])}
          ${alert('The watcher cannot reach its queue.', 'Redis refused the connection 7 times in a row; it retries every 40s and has not fired since. Runs already queued are unaffected.', 'bad')}
          ${card({ icon: 'trig', title: 'Queue depth', aside: 'fires once each time the depth rises past the threshold', body: grid(4, [kv('Queue', '<span class="mono">events:ingest</span>', 'redis stream'), kv('Threshold', '10,000 entries'), kv('Last good poll', '3h ago', 'depth 2,140'), kv('Errors', '7 in a row', 'last 40s ago')]),
            note: '<span class="mono">connection refused (os error 61)</span>' })}
          ${card({ icon: 'runs', title: 'Recent runs from this trigger', right: lnk('all 12 in Runs'), body: tbl(['Run', 'Status', ['Rows', 'num'], 'Started'], [[lnk('events-backlog · 0012'), '<span class="ok">● completed</span>', '48,220', '3h ago'], [lnk('events-backlog · 0011'), '<span class="ok">● completed</span>', '51,006', '9h ago']]) })}`);
        return;
      }
      M.top(['Operate', 'Triggers', 'orders-ingest']);
      M.list(listPane('ingest'));
      M.detail(webhook());
      if (state === 'fire') {
        M.overlay(dialog('Fire orders-ingest', `<span>Starts one run of ${lnk('acme/billing-api')} now, exactly as a caller’s POST would.</span>
          ${field('Request body', `<div class="area mono" style="height:110px">{\n  "order_ids": [1042, 1043],\n  "source": "console"\n}</div>`, { help: 'Optional JSON, available to the config as <code>${trigger.body}</code>' })}
          ${field('Dedupe key', input('console-2026-10-30-1', { mono: true }), { help: 'Sent as X-Delivery-Id; firing again with the same key does nothing' })}`,
          `${btn('Cancel', { kind: 'quiet' })}${btn('Fire', { icon: 'play', kind: 'primary' })}`, 'trig'));
      }
      if (state === 'fired') M.overlay(toast(`${ic('check')}<span>Fired <b>orders-ingest</b> · run queued · ${lnk('open run')}</span>`, 'ok'));
    },
  });
})();
