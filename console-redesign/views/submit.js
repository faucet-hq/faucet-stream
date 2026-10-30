// Submit — canvas: build a pipeline config from the connector schemas, check
// it, plan it and run it (or request approval). A live YAML preview sits beside.
(function () {
  const mode = (on) => `<span class="seg"><span class="${on === 'guided' ? 'on' : ''}">Guided</span><span class="${on === 'editor' ? 'on' : ''}">Editor</span></span>`;
  const ACTIONS = btn('Validate', { icon: 'check' }) + btn('Plan', { icon: 'diff' }) + btn('Submit run', { icon: 'submit', kind: 'primary', act: true });
  const STEPS = (cur) => {
    const order = ['Source', 'Sink', 'Transforms', 'Options', 'Review'];
    const at = order.indexOf(cur);
    return card({ body: steps(order.map((l, i) => [l, i < at ? 'done' : i === at ? 'cur' : ''])) });
  };
  const sub = (title, body, right = '') => `<div class="sb-sub"><div class="h"><b>${title}</b>${right ? `<span class="r">${right}</span>` : ''}</div>${body}</div>`;
  const kfield = (key, control, o = {}) => field(key, control, { code: true, ...o });

  const YAML = {
    source: `version: 1
name: orders_to_warehouse
pipeline:
  source:
    type: postgres
    config:
      connection_url: \${env:PG_URL}
      query: select * from public.orders
        where updated_at >= \${bookmark}
      max_connections: 10
      batch_size: 1000
      replication:
        type: incremental
        column: updated_at`,
    full: `version: 1
name: orders_to_warehouse
pipeline:
  source:
    type: postgres
    config:
      connection_url: \${env:PG_URL}
      query: select * from public.orders
        where updated_at >= \${bookmark}
  sink:
    type: bigquery
    config:
      project_id: analytics-prod
      dataset_id: commerce
      table_id: orders
      write_mode: upsert
      key: [order_id]
  transforms:
    - type: cast
      config: { field: amount, to: number }
  state:
    type: postgres
    config: { url: \${env:STATE_URL} }
  dlq:
    sink: { type: s3, config: { bucket: dlq } }`,
  };
  const preview = (yaml, note = '<span class="ok">valid YAML</span> · 14 lines · secrets stay as references') => card({ icon: 'code', title: 'Config', aside: 'live preview', right: btn('Copy', { icon: 'copy', kind: 'sm' }), body: code(yaml, { box: false, copy: false }), note });

  const SOURCE_FORM = (err = false) => card({ icon: 'db', title: 'Source', right: `${select('<span class="mono">postgres</span>').replace('class="select', 'style="height:28px;width:170px" class="select')}${lnk('schema')}`,
    body: `<div class="form">
      ${kfield('connection_url', input(err ? '' : '••••••••••••  ${env:PG_URL}', { cls: err ? 'err' : '', mono: !err }), { req: true, help: 'Postgres connection URL. Use a secret reference, never a literal password.', err: err ? 'Required: the source has no database to read.' : '' })}
      ${kfield('max_connections', input('10'), { help: 'Pool size' })}
      ${kfield('query', `<div class="area mono" style="height:58px">select * from public.orders\nwhere updated_at >= \${bookmark}</div>`, { req: true, cls: 'wide', help: 'One SQL statement. <code>${bookmark}</code> binds the incremental cursor.' })}
      ${kfield('batch_size', input(err ? '0.5' : '1000', { cls: err ? 'err' : '' }), { help: 'Rows per page; 0 means one page', err: err ? 'Must be a whole number between 0 and 1,000,000.' : '' })}
      ${kfield('tls', select('disable'), { help: 'disable, require, verify_ca or verify_full' })}
    </div>
    ${sub(toggle('<code>replication</code>', true), `<div class="form">${kfield('type', select('incremental'))}${kfield('column', input('updated_at'), { req: true })}${kfield('initial_value', input('2026-01-01T00:00:00Z', { mono: true }), { help: 'Where the first run starts' })}</div>`, 'optional')}
    ${sub(toggle('<code>shard</code>', false), '', 'optional · split the read across replicas')}` });

  M.view('submit', {
    label: 'Submit', nav: 'submit', icon: 'submit', shape: 'canvas', summary: 'build, check, plan and run a pipeline config',
    states: {
      source: { label: 'Guided · source step', covers: ['source kind picker + schema form', 'Option<T> behind a toggle', 'help text from the schema', 'secret field'], new: ['stepper', 'live YAML preview with copy', 'connector schema link'] },
      sink: { label: 'Guided · sink step (union, array, nested)', covers: ['sink kind picker', 'auth union type picker', 'enum select', 'array add/remove', 'nested object'], new: ['write mode explained inline'] },
      transforms: { label: 'Guided · transforms with add menu', covers: ['add transform', 'transform schema form'], new: ['reorder and remove transforms'] },
      options: { label: 'Guided · pipeline + run options', covers: ['optional blocks state/dlq/delivery/resilience/sla/quality/contract/masking/schema', 'DLQ sink picker', 'run name, timeout, doctor_first, idempotency key', 'request approval + reason'], new: ['labels, pool, budget ceilings exposed'] },
      review: { label: 'Review · validate passed + plan', covers: ['Check (doctor) report'], new: ['Plan result card (POST /v1/plan)', 'full config with copy'] },
      errors: { label: 'Validation errors + doctor failed', covers: ['submit/doctor errors with details'], new: ['field-level errors', 'error summary linking to fields'] },
      editor: { label: 'Editor mode', covers: ['raw YAML editor', 'editor seeded from the wizard'], new: ['line-numbered errors'] },
      fromrun: { label: 'Prefilled from a past run', covers: [], new: ['#/submit?from=<run id> (api-proposal #1)', 'diff against the original'] },
      submitted: { label: 'Submitted · toast', covers: ['navigate to the new run', 'pending approval → change request'], new: ['toast with link'] },
      viewer: { label: 'Viewer · read only', params: { role: 'viewer' }, covers: ['read-only notice without run_write'] },
    },
    render(state, ctx) {
      M.top(['Build', 'Submit'], { right: state === 'fromrun' ? chip(`from run 2e81…05bd${cpi()}`, 'mono') : '' });
      const headBlock = (m = 'guided', extra = '') => `${dhead(mode(m), ACTIONS)}${head('Submit a run', [], extra || 'Pick a source and a sink; the form comes from each connector’s schema. Validate probes every connection without moving data.')}`;

      if (state === 'viewer') {
        M.detail(`${dhead(mode('guided'), btn('Validate', { icon: 'check' }))}${head('Submit a run')}
          ${alert('Read-only access.', 'Your role can browse schemas and validate a config, but not submit runs. Ask an operator, or open a change request from an operator account.', '')}
          <div class="sb-grid"><div class="stack">${STEPS('Source')}${SOURCE_FORM()}</div>${preview(YAML.source)}</div>`);
        return;
      }
      if (state === 'editor') {
        M.detail(`${headBlock('editor', 'Paste or write a full config. Switching back to Guided keeps what both modes understand.')}
          <div class="sb-grid">
            <div class="stack">${card({ icon: 'code', title: 'orders_to_warehouse.yaml', right: select('YAML').replace('class="select', 'style="height:26px;width:90px" class="select'),
              body: `<div class="area mono" style="border:0;border-radius:0;height:430px;line-height:1.75">${YAML.full.split('\n').map((l, i) => `<span class="faint">${String(i + 1).padStart(2, ' ')}</span>  ${l.replace('write_mode: upsert', 'write_mode: <span style="text-decoration:underline wavy var(--bad)">upsrt</span>')}`).join('\n')}</div>`,
              note: `<span class="bad">Line 16</span> · <code>write_mode</code> must be one of append, upsert, delete, overwrite (got <code>upsrt</code>).` })}</div>
            <div class="stack">${card({ title: 'Run options', body: `<div class="form one">${field('Run name', input('orders_to_warehouse · manual', {}))}${field('Timeout', input('6h'), { help: 'Cancel the run after this long' })}${check('Probe connections before running (doctor first)', true)}</div>` })}
              ${alert('Not checked yet.', 'Validate parses the config and probes each source, sink and state store.', '')}</div>
          </div>`);
        return;
      }
      if (state === 'sink') {
        M.detail(`${headBlock()}
          <div class="sb-grid"><div class="stack">${STEPS('Sink')}
          ${card({ icon: 'db', title: 'Sink', right: `${select('<span class="mono">bigquery</span>').replace('class="select', 'style="height:28px;width:170px" class="select')}${lnk('schema')}`,
            body: `<div class="form">
              ${kfield('project_id', input('analytics-prod'), { req: true })}${kfield('dataset_id', input('commerce'), { req: true })}
              ${kfield('table_id', input('orders'), { req: true })}${kfield('write_mode', select('upsert'), { help: 'Upsert MERGEs by <code>key</code>; rows with a missing key fail into the DLQ.' })}
              ${kfield('key', `<div class="sb-arr"><div>${input('order_id')}${ibtn('x')}</div><div class="add">${btn('Add column', { icon: 'plus', kind: 'sm' })}</div></div>`, { help: 'Columns that identify a row', req: true })}
              ${kfield('create_table', check('Create the table on first write', true))}
            </div>
            ${sub('<code>credentials</code>', `<div class="form"><div class="wide"><span class="seg"><span>service_account_key</span><span>service_account_file</span><span class="on">application_default</span></span></div><div class="wide help muted" style="font-size:12px;color:var(--muted)">Uses the server’s ambient Google credentials. Pick a key to pass one explicitly; it stays a secret reference.</div></div>`, 'one of three')}
            ${sub(toggle('<code>partitioning</code>', true), `<div class="form">${kfield('field', input('created_at'))}${kfield('type', select('DAY'))}</div>`, 'optional')}` })}
          </div>${preview(YAML.full.split('\n').slice(0, 19).join('\n'), '<span class="ok">valid YAML</span> · 19 lines')}</div>`);
        return;
      }
      if (state === 'transforms') {
        M.detail(`${headBlock()}
          <div class="sb-grid"><div class="stack">${STEPS('Transforms')}
            ${card({ title: '1 · <span class="mono" style="font-weight:400">cast</span>', right: ibtn('updown') + ibtn('trash'), body: `<div class="form three">${kfield('field', input('amount'), { req: true })}${kfield('to', select('number'), { req: true })}${kfield('on_error', select('fail'))}</div>` })}
            ${card({ title: '2 · <span class="mono" style="font-weight:400">redact</span>', right: ibtn('updown') + ibtn('trash'), body: `<div class="form">${kfield('fields', `<div class="sb-arr"><div>${input('email')}${ibtn('x')}</div><div>${input('phone')}${ibtn('x')}</div></div>`, { req: true })}${kfield('mask', input('***'), { help: 'Replacement text' })}</div>` })}
            <div style="position:relative">${btn('Add transform', { icon: 'plus', cls: 'open' })}
              ${pop([{ heading: 'Reshape' }, { label: '<span class="mono">select</span>', hint: 'keep listed fields' }, { label: '<span class="mono">flatten</span>', hint: 'nest → dotted keys', cls: 'hl' }, { label: '<span class="mono">rename_field</span>', hint: 'one field' }, '-', { heading: 'Values' }, { label: '<span class="mono">value_case</span>', hint: 'upper / lower' }, { label: '<span class="mono">set</span>', hint: 'constant field' }], 'top:36px;left:0;width:300px')}</div>
          </div>${preview(YAML.full.split('\n').slice(0, 22).join('\n'), '<span class="ok">valid YAML</span> · 22 lines')}</div>`);
        return;
      }
      if (state === 'options') {
        const block = (name, on, body, hint) => sub(toggle(`<code>${name}</code>`, on), on ? body : '', hint);
        M.detail(`${headBlock()}
          <div class="sb-grid"><div class="stack">${STEPS('Options')}
            ${card({ icon: 'shield', title: 'Reliability', aside: 'what happens on a crash, a bad row, a slow source', body: `<div style="padding-top:12px">
              ${block('state', true, `<div class="form">${kfield('type', select('postgres'))}${kfield('url', input('${env:STATE_URL}', { mono: true }), { req: true })}</div>`, 'bookmarks survive restarts')}
              ${block('dlq', true, `<div class="form">${kfield('sink', select('<span class="mono">s3</span>'))}${kfield('bucket', input('pipeline-dlq'), { req: true })}${kfield('on_batch_error', select('dlq_all'), { cls: 'wide', help: 'Route every row of a failed page to the DLQ instead of failing the run' })}</div>`, 'failed rows land here')}
              ${block('delivery', false, '', 'exactly-once needs a CDC source')}${block('resilience', false, '', 'retries and a circuit breaker')}${block('sla', false, '', 'freshness and volume alerts')}</div>` })}
            ${card({ icon: 'policy', title: 'Data governance', body: `<div class="sb-adders" style="border-top:0">${['quality', 'contract', 'masking', 'schema drift'].map((b) => btn(b, { icon: 'plus', kind: 'sm' })).join('')}</div>` })}
            ${card({ icon: 'runs', title: 'Run options', body: `<div class="form three">
              ${field('Run name', input('orders_to_warehouse · backfill check'))}${field('Pool', select('default'))}${field('Timeout', input('2h'))}
              ${field('Labels', `<div class="sb-kv">${input('team', { mono: true })}${input('commerce', { mono: true })}${ibtn('x')}</div>`, { cls: 'wide', help: 'Searchable on the Runs page' })}
              ${field('Max records', input('5,000,000'), { help: 'Budget: the page that would cross is refused' })}${field('Max duration', input('3h'))}${field('Idempotency key', input('Optional', { ph: true }))}
              <div class="wide" style="display:flex;gap:18px;flex-wrap:wrap">${check('Probe connections first (doctor)', true)}${check('Request approval before it runs', true)}</div>
              ${field('Reason for approvers', `<div class="area sans" style="height:46px">Backfills the September gap after the source outage.</div>`, { cls: 'wide' })}</div>` })}
          </div>${preview(YAML.full, '<span class="ok">valid YAML</span> · 28 lines · approval required: <b>finance writes · 2 admins</b>')}</div>`);
        return;
      }
      if (state === 'review' || state === 'submitted') {
        M.detail(`${headBlock()}
          <div class="sb-grid"><div class="stack">${STEPS('Review')}
            ${card({ icon: 'check', title: 'Validated', tag: 'doctor', aside: '4 probes passed in 1.8s', right: 'just now', body: tbl(['Probe', 'Result', ['Time', 'num']], [
              ['source <span class="mono">postgres</span> · connect', '<span class="ok">● pass</span>', '220 ms'], ['source <span class="mono">postgres</span> · read one page', '<span class="ok">● pass</span>', '610 ms'], ['sink <span class="mono">bigquery</span> · table metadata', '<span class="ok">● pass</span>', '740 ms'], ['state <span class="mono">postgres</span> · read/write', '<span class="ok">● pass</span>', '190 ms']]) })}
            ${card({ icon: 'diff', title: 'Plan', aside: 'what this run will do', body: grid(4, [kv('Rows', '1', 'default'), kv('Write', 'upsert', 'key order_id'), kv('Delivery', 'at least once', 'keyed upsert dedups replays'), kv('Policy', '<span class="ok">allowed</span>', 'eu-pii · 0 violations')]),
              note: `Schema impact · <span class="ok">additive</span> · adds <code>discount_code</code> to ${lnk('analytics.orders')}; 2 downstream readers unaffected.` })}
          </div>${preview(YAML.full, '<span class="ok">valid YAML</span> · 28 lines')}</div>`);
        if (state === 'submitted') M.overlay(toast(`${ic('check')}<span>Run <b>orders_to_warehouse</b> queued · ${lnk('open run')}</span>`, 'ok'));
        return;
      }
      if (state === 'errors') {
        M.detail(`${headBlock()}
          ${alert('2 fields need attention before this can run.', `${lnk('connection_url')} is required · ${lnk('batch_size')} must be a whole number.`, 'bad')}
          <div class="sb-grid"><div class="stack">            ${card({ icon: 'x', title: 'Validation failed', tag: 'doctor', aside: '1 of 3 probes failed', right: 'just now', body: tbl(['Probe', 'Result', 'Detail'], [
              ['sink <span class="mono">bigquery</span> · table metadata', '<span class="bad">● fail</span>', 'dataset <span class="mono">commerce</span> not found in project <span class="mono">analytics-prod</span>'],
              ['state <span class="mono">file</span> · read/write', '<span class="ok">● pass</span>', ''], ['source <span class="mono">postgres</span> · connect', '<span class="faint">● skipped</span>', 'no connection_url']]) })}
            ${SOURCE_FORM(true)}
          </div>${preview(YAML.source, '<span class="bad">2 errors</span> · fix them in the form')}</div>`);
        return;
      }
      if (state === 'fromrun') {
        M.detail(`${headBlock('guided', ' ')}
          ${alert('Prefilled from run orders_nightly_rollup.', `The config stored with ${lnk('2e81…05bd', true)} (3h ago, completed). Secrets stay as references. ${lnk('Show changes from the original')}`, '')}
          <div class="sb-grid"><div class="stack">${STEPS('Review')}
            ${card({ icon: 'diff', title: 'Changes from the original run', right: '2 lines', body: diff([[' ', 'name: orders_nightly_rollup'], ['-', 'name: orders_nightly_rollup'], ['+', 'name: orders_nightly_rollup · rerun'], [' ', '  sink:'], ['-', '      write_mode: append'], ['+', '      write_mode: upsert'], ['+', '      key: [order_id]']].slice(1)) })}
          </div>${preview(YAML.full)}</div>`);
        return;
      }
      M.detail(`${headBlock()}<div class="sb-grid"><div class="stack">${STEPS('Source')}${SOURCE_FORM()}</div>${preview(YAML.source)}</div>`);
    },
  });
})();
