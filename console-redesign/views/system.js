// Design system reference: tokens, every component, both page shapes, the rules.
// Source of truth for the build (cli/src/serve/ui) and docs/contributing/console-design.md.
M.view('system', {
  label: 'Design system', nav: 'system', icon: 'layers', summary: 'tokens, components, page shapes and rules',
  shape: 'canvas',
  states: {
    tokens: { label: 'Tokens: colour, type, space, radius', covers: ['every colour, size, space and radius is a token'] },
    controls: { label: 'Controls: buttons, inputs, menus, tabs', covers: ['one set of controls used everywhere'] },
    content: { label: 'Content: cards, kv grid, stats, tables, rows', covers: ['card anatomy', 'list-row anatomy', 'tables inside cards'] },
    feedback: { label: 'Feedback: alerts, empty, skeleton, dialog, toast, code', covers: ['states: empty, loading, error', 'overlays'] },
    shapes: { label: 'Page shapes, graph primitives and rules', covers: ['two page shapes', 'graph primitives', 'dos and don’ts'] },
  },
  render(state) {
    M.top(['Design system', { tokens: 'Tokens', controls: 'Controls', content: 'Content', feedback: 'Feedback', shapes: 'Shapes and rules' }[state]]);
    const nav = dhead(dtabs(['Tokens', 'Controls', 'Content', 'Feedback', 'Shapes and rules'], { tokens: 'Tokens', controls: 'Controls', content: 'Content', feedback: 'Feedback', shapes: 'Shapes and rules' }[state]), chip('dark and light: same design, different values'));
    const sw = (name, note = '') => `<div><i style="--c:var(--${name})"></i><span>--${name}</span>${note ? `<small>${note}</small>` : ''}</div>`;
    const specRow = (label, html) => `<div class="spec"><small>${label}</small>${html}</div>`;
    let body = '';
    if (state === 'tokens') {
      body = `
      ${card({ icon: 'layers', title: 'Surfaces and lines', aside: 'page → chrome → panel → panel-2; hairlines in three weights', body: `<div class="sw" style="grid-template-columns:repeat(9,minmax(0,1fr))">${['bg', 'chrome', 'panel', 'panel-2', 'hover', 'pill', 'line-soft', 'line', 'line-strong'].map((n) => sw(n)).join('')}</div>` })}
      <div style="display:grid;grid-template-columns:1fr 1fr 1.5fr;gap:var(--gap)">
        ${card({ title: 'Text', aside: 'four steps', body: `<div class="sw" style="grid-template-columns:repeat(4,minmax(0,1fr))">${['text', 'text-2', 'muted', 'faint'].map((n) => sw(n)).join('')}</div>` })}
        ${card({ title: 'Status', aside: 'only for status', body: `<div class="sw" style="grid-template-columns:repeat(4,minmax(0,1fr))">${['ok', 'warn', 'bad', 'info'].map((n) => sw(n)).join('')}</div>` })}
        ${card({ title: 'Selection', aside: 'selected, active, primary', body: `<div class="sw" style="grid-template-columns:repeat(3,minmax(0,1fr))">${['accent', 'accent-ink', 'teal-fill', 'teal-line', 'teal-ink', 'teal-badge'].map((n) => sw(n)).join('')}</div>`, note: 'Never teal for links, chips, filters, focus rings or tags.' })}
      </div>
      <div class="pair">
        ${card({ title: 'Type', aside: 'one base size, one title size', body: `
          ${specRow('--fs-title 21', '<span style="font-size:var(--fs-title);font-weight:600;letter-spacing:-.015em">orders_cdc_mirror</span>')}
          ${specRow('--fs-stat 16', '<b style="font-size:var(--fs-stat)">18.4M</b>')}
          ${specRow('--fs-kv 14', '<b style="font-size:var(--fs-kv);font-weight:500">1,476 rows/s</b>')}
          ${specRow('--fs 13 base', '<span>Every row offered is accounted for</span>')}
          ${specRow('--fs-sm 12', '<span class="muted" style="font-size:var(--fs-sm)">Header, meta, help text</span>')}
          ${specRow('mono 12', '<span class="mono">write_mode: upsert</span><span class="muted">only literal code</span>')}` })}
        ${card({ title: 'Space, radius, size', body: `
          ${specRow('space', ['sp-1 4', 'sp-2 8', 'sp-3 12', 'gap 14', 'sp-4 16', 'sp-5 22'].map((s) => `<span class="chip">${s}</span>`).join(''))}
          ${specRow('radius', ['r-xs 4', 'r-sm 7', 'r 9', 'r-card 11', 'r-lg 12'].map((s) => `<span class="chip">${s}</span>`).join(''))}
          ${specRow('control height', '<span class="chip">ctl-h 30</span><span class="muted">buttons, inputs, selects, tabs share it</span>')}
          ${specRow('layout', '<span class="chip">side-w 236</span><span class="chip">list-w 372</span><span class="chip">narrow ≤ 1100</span>')}` })}
      </div>`;
    } else if (state === 'controls') {
      body = `
      ${card({ title: 'Buttons', aside: 'one height; primary is the only teal button', body: `
        ${specRow('kinds', `${btn('Launch v12', { kind: 'primary', icon: 'rocket' })}${btn('Roll back', { icon: 'undo' })}${btn('Delete', { kind: 'danger', icon: 'trash' })}${btn('Cancel', { kind: 'quiet' })}${btn('Disabled', { cls: 'dis' })}`)}
        ${specRow('small', `${btn('Run', { cls: 'sm', icon: 'play' })}${btn('Launch', { cls: 'sm' })}${btn('More', { cls: 'sm', icon: 'more' })}`)}
        ${specRow('icon buttons', `${ibtn('filter')}${ibtn('filter', { on: true, dot: true })}${ibtn('refresh')}${ibtn('plus')}${ibtn('more')}<span class="muted">pressed state and a filter-active dot</span>`)}` })}
      ${card({ title: 'Inputs', aside: 'quiet: token colours, same radius and height as buttons', body: `<div class="form three">
        ${field('Pipeline', input('orders_nightly'), { help: 'Sans for values a person types.' })}
        ${field('Search', input('Search runs', { ph: true, icon: 'search' }))}
        ${field('Group by', select('pipeline'))}
        ${field('key', input('entry_id', { mono: true }), { code: true, req: true, help: 'A config key label is mono; its help is sans.' })}
        ${field('Records per run', input('none', { ph: true, cls: 'err' }), { err: 'Must be a whole number above 0.' })}
        ${field('Options', `<div style="display:flex;gap:16px;align-items:center;height:30px">${check('Dry run', true)}${check('Force')}${toggle('Doctor first', true)}</div>`)}
      </div>` })}
      <div class="pair">
        ${card({ title: 'Tabs and segmented control', body: `${specRow('tabs', tabs(['Overview', ['Invocations', 2], 'Logs'], 'Overview'))}${specRow('segmented', '<span class="seg"><span class="on">Guided</span><span>Editor</span></span>')}${specRow('section', sect('Running', 3))}` })}
        ${card({ title: 'Menu', aside: 'open state', body: `<div style="position:relative;height:210px;padding:14px 16px">${btn('More', { icon: 'more', cls: 'open' })}${pop(['<div class="mlab">Version v12</div>', { label: 'Assign channel', icon: 'layers', hint: 'canary' }, { label: 'Run tests', icon: 'flask', cls: 'hl' }, { label: 'Show config', icon: 'code' }, '-', { label: 'Delete v12', icon: 'trash', cls: 'danger' }], 'top:52px;left:16px')}</div>` })}
      </div>`;
    } else if (state === 'content') {
      body = `
      <div class="two">
        <div class="stack">
          ${card({ icon: 'runs', title: '1,240,512 rows moved', tag: 'exactly-once', aside: 'card header: icon · bold title · tag · muted aside', right: 'right meta',
            body: grid(4, [kv('Throughput', '1,476 rows/s', 'sub line'), kv('Lag', '3.2s', 'LSN 2F/A81C4E10'), kv('Cost', '$0.84'), kv('Bookmark', '6s ago')]),
            note: '<span class="ok">Footnote verdict</span> · one sentence that says what the numbers mean.' })}
          ${card({ title: 'Table inside a card', right: '3 rows', body: tbl(['Row', 'Status', ['Rows', 'num'], ['Duration', 'num']], [[`<span class="mono">orders</span>`, '<span class="ok">● running</span>', '1,024,880', '14m 02s'], { cls: 'hov', cells: [`<span class="mono">order_items</span>`, '<span class="ok">● succeeded</span>', '215,632', '9m 41s'] }, [`<span class="mono">refunds</span>`, '<span class="bad">● failed</span>', '0', '12s']], ['Total', '', '1,240,512', '']), note: 'Sentence-case muted headers, hairlines, numbers right-aligned in tabular figures, row hover.' })}
        </div>
        <div class="stack">
          ${card({ title: 'Stat strip', body: `<div class="pad">${stats([['Running', '3', 'info'], ['Succeeded', '412', 'ok'], ['Failed', '7', 'bad'], ['Records', '18.4M']], 2)}</div>`, note: 'A cell can be a filter; the active one is selected (teal fill).' })}
          ${card({ title: 'List rows', aside: 'one anatomy everywhere', body: `<div class="pad" style="display:flex;flex-direction:column;gap:2px">
            ${row(ring('info', 'half'), 'orders_cdc_mirror', '14m', '<b>postgres-cdc → bigquery</b> · 1.24M rows', { on: true })}
            ${row(ring('bad'), 'crm_contacts_upsert', '22m', '<b>rate_limited</b> · gave up after 6 tries', { kb: true })}
            ${row(ring('ok'), 'invoices_s3_to_duckdb', '5m', '<b>s3 → duckdb</b> · 84,210 rows', { hov: true })}
            ${row(ring('warn', 'hollow'), 'acme/ticketing', 'v3', '<b>draft</b> · never launched')}
          </div>`, note: 'Ring · title · age · meta with the first fact bold. Selected, keyboard-focused, hover.' })}
          ${card({ title: 'Pills, chips, tags, channels', body: `${specRow('status pill', `${pill('Running', 'info', 'play')}${pill('Launched', 'ok', 'check')}${pill('Pending', 'warn')}${pill('Failed', 'bad', 'x')}`)}${specRow('chip · filter', `${chip('pool cdc')}${chip('pipeline: orders', 'x')}${chip('7f3c…a91e', 'mono')}`)}${specRow('tag · channel', `${tag('exactly-once')}${tag('deprecated')}${ch('stable', true)}${ch('canary')}`)}${specRow('links', `${lnk('analytics.orders')} ${lnk('7f3c…a91e', true)}${cpi()}`)}` })}
        </div>
      </div>`;
    } else if (state === 'feedback') {
      body = `
      <div class="pair">
        <div class="stack">
          ${alert('Couldn’t load this run.', 'The server answered 503: history backend degraded.', 'bad', btn('Retry', { icon: 'refresh', cls: 'sm' }))}
          ${alert('Draft template.', 'Unpinned runs are refused until a version is launched.', 'warn')}
          ${alert('Rolled back.', 'Deleted 12 rows and rewound the bookmark.', 'ok')}
          ${card({ title: 'Empty state', body: empty('No runs yet', 'Submit a config or trigger a template; runs appear here as they start.', { icon: 'runs', btns: btn('Submit a run', { kind: 'primary', icon: 'submit' }) + btn('Browse templates') }) })}
        </div>
        <div class="stack">
          ${card({ title: 'Loading skeleton', aside: 'same shape as the content it replaces', body: `<div class="pad">${skelRows(3)}</div><div class="pad" style="padding-top:0">${skelCards(1)}</div>` })}
          ${card({ title: 'Code block', aside: 'mono 12, copy button', body: code('pipeline:\n  source:\n    type: postgres\n    config:\n      table: public.orders   # required\n      batch_size: 1000\n  sink:\n    type: bigquery', { box: false }) })}
          ${card({ title: 'Diff', body: diff([['', 'row ledger · sink postgres.finance.ledger'], ['-', 'write_mode: upsert'], ['+', 'write_mode: overwrite'], ['@', '⋯']]) })}
        </div>
      </div>`;
      M.overlay(toast(`${ic('check')}Toast · Run <b>orders_cdc_mirror</b> submitted · <span class="lnk">open</span>`, 'ok'));
    } else {
      body = `
      <div class="pair">
        ${card({ title: 'Split pane', aside: 'anything you browse', body: `<div class="pad"><div class="shape" style="grid-template-columns:1fr 2.2fr"><div>list<br>tabs · stats · sections · rows</div><div>detail<br>tabs · title · cards</div></div></div>`, note: 'Runs, Changes, Templates, Datasets, Tenants, Schemas, Audit, Triggers. At ≤1100px the detail replaces the list and a back link appears.' })}
        ${card({ title: 'Canvas', aside: 'forms and reports', body: `<div class="pad"><div class="shape" style="grid-template-columns:1fr 0.6fr"><div>full-width canvas<br>title · cards</div><div>optional side panel</div></div></div>`, note: 'Overview, Submit, Usage, Lineage (with side panel), Settings. Same gutters as the split pane; never a narrow centred column.' })}
      </div>
      ${card({ icon: 'lineage', title: 'Graph primitives', aside: 'nodes, edges, freshness dots, tools; all token classes', body: `<div class="graph" style="height:150px"><svg width="100%" height="150" viewBox="0 0 900 150">
        <path class="ge on" d="M210 50 C260 50 260 75 310 75"/><path class="ge dim" d="M210 115 C260 115 260 75 310 75"/><path class="ge col" d="M510 75 C560 75 560 40 610 40"/><path class="ge" d="M510 75 C560 75 560 115 610 115"/>
        <g class="gn nb"><rect x="30" y="32" width="180" height="38" rx="8"/><circle class="gdot ok" cx="44" cy="51" r="4"/><text class="gt" x="56" y="48">orders-db.orders</text><text class="gs" x="56" y="62">postgres · 6s</text></g>
        <g class="gn dim"><rect x="30" y="96" width="180" height="38" rx="8"/><circle class="gdot warn" cx="44" cy="115" r="4"/><text class="gt" x="56" y="112">billing /v2/invoices</text><text class="gs" x="56" y="126">rest · 26h</text></g>
        <g class="gn sel"><rect x="310" y="56" width="200" height="38" rx="8"/><circle class="gdot ok" cx="324" cy="75" r="4"/><text class="gt" x="336" y="72">analytics.orders</text><text class="gs" x="336" y="86">bigquery · selected</text></g>
        <g class="gn"><rect x="610" y="21" width="250" height="38" rx="8"/><circle class="gdot ok" cx="624" cy="40" r="4"/><text class="gt" x="636" y="37">bi.orders_daily</text><text class="gm" x="636" y="51">discount_code ← discount_code</text></g>
        <g class="gn"><rect x="610" y="96" width="180" height="38" rx="8"/><circle class="gdot bad" cx="624" cy="115" r="4"/><text class="gt" x="636" y="112">ml.cashflow_features</text><text class="gs" x="636" y="126">iceberg · drifted</text></g>
      </svg><div class="gtools" style="right:10px;top:10px">${ibtn('zoomin')}${ibtn('zoomout')}${ibtn('fit')}</div></div>` })}
      <div class="pair">
        ${card({ icon: 'check', title: 'Do', body: `<div class="pad lede" style="margin:0">Sans for every label, value and description. Mono only for literal code at 12px. Sentence-case headers, muted, 12px. Content in cards; tables inside cards. Teal only for selection and the primary button. Status colours only for status. Every name or id links to its page and has a copy button. Empty states say what to do next; loading shows a skeleton; errors are a quiet alert with Retry.</div>` })}
        ${card({ icon: 'x', title: 'Don’t', body: `<div class="pad lede" style="margin:0">No uppercase or letter-spaced headers. No mono for prose, numbers or names. No nested card-in-card. No hard-coded colours outside the token blocks. No narrow centred column. No teal links, chips, filters, focus rings or tags. No ad-hoc font sizes. No "Loading…" text.</div>` })}
      </div>`;
    }
    M.detail(nav + body);
  },
});
