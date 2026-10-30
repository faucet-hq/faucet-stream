// Schemas — split pane: every compiled connector, transform and state store,
// and the config schema of the one selected.
(function () {
  const ri = (n) => `<span class="ri">${ic(n)}</span>`;
  const SOURCES = [['postgres', 14, 'Run a SQL query; incremental by a cursor column'], ['postgres-cdc', 12, 'Logical replication from a slot'], ['rest', 31, 'Any HTTP API: auth, pagination, extraction'], ['kafka', 18, 'Consume topics; exactly-once offsets'], ['s3', 16, 'Objects under a prefix: JSONL, CSV, Parquet'], ['file', 15, 'Local files, directories, globs, one URL']];
  const SINKS = [['bigquery', 19, 'Streaming insert, MERGE upsert, overwrite'], ['postgres', 17, 'Auto-mapped columns or one JSONB column'], ['s3', 14, 'Files under a prefix with rollover'], ['stdout', 4, 'JSON Lines to the terminal']];
  const TRANSFORMS = [['cast', 3, 'Convert a field to another type'], ['flatten', 2, 'Nest objects into dotted keys'], ['select', 1, 'Keep only the listed fields'], ['redact', 2, 'Mask values by field name']];
  const STATES = [['file', 2, 'One JSON file per key'], ['postgres', 4, 'faucet_state table'], ['redis', 4, 'Keys under a namespace']];

  function listPane(sel, q = '') {
    const section = (label, items, kind, on) => `${sect(label, items.length)}${items.map(([n, f, d]) => row(ri(kind === 'transform' ? 'code' : kind === 'state' ? 'key' : 'db'), n, `${f}`, d, { mono: true, on: on === `${kind}:${n}` })).join('')}`;
    if (q === 'kafkaa') {
      return `${tabs([['All', 47], 'Sources', 'Sinks', 'Transforms'], 'All', ibtn('refresh'))}${input('kafkaa', { icon: 'search', cls: 'focus' })}
        ${empty('No connector matches “kafkaa”', 'Check the spelling, or the connector may not be compiled into this server. <span class="lnk">Clear the search</span>', { icon: 'search' })}`;
    }
    return `${tabs([['All', 47], 'Sources', 'Sinks', 'Transforms'], 'All', ibtn('refresh'))}
      ${input(q || 'Search connectors and fields', { ph: !q, icon: 'search' })}
      ${section('Sources', SOURCES, 'source', sel)}${section('Sinks', SINKS, 'sink', sel)}${section('Transforms', TRANSFORMS, 'transform', sel)}${section('State stores', STATES, 'state', sel)}`;
  }

  const req = '<span class="req" title="required"></span>';
  const f = (name, required, type, def, desc, cls = '') => ({ cells: [`<span class="sc-name"><span class="mono">${name}</span> ${required ? req : ''}</span>`, pill(type), def ? `<code>${def}</code>` : '<span class="faint">—</span>', desc], cls });

  const ftable = (rows) => `<table><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr>${rows.map((r) => (r.span ? `<tr class="${r.cls}"><td></td><td colspan="3">${r.span}</td></tr>` : `<tr class="${r.cls || ''}">${r.cells.map((c) => `<td>${c}</td>`).join('')}</tr>`)).join('')}</table>`;

  function connector({ name, kind, crate, lede, stat, fields, yaml, open = '' }) {
    return `${dhead('', btn('Copy link', { icon: 'link', kind: 'quiet' }) + btn(`Use in Submit`, { icon: 'submit', kind: 'primary', act: true }), 'Schemas')}
      ${head(`${name} · ${kind}`, [pill('Built in', 'ok', 'check'), `<span class="mono">${crate}</span>`, `<span>${stat[0]} fields · ${stat[1]} required</span>`], lede)}
      ${stats([['Fields', stat[0]], ['Required', stat[1]], ['With defaults', stat[2]], ['Secret', stat[3]]])}
      ${card({ title: 'Fields', aside: 'click a field for its full description', right: input('Filter fields', { ph: true, icon: 'search' }).replace('class="input', 'style="height:26px;width:190px" class="input'),
        body: ftable(fields) })}
      ${open}
      ${card({ icon: 'code', title: 'Example config', aside: 'required fields and defaults, like faucet init', body: code(yaml, { box: false }) })}
      ${card({ icon: 'chev', title: 'Raw JSON Schema', aside: 'draft 2020-12 · 4.1 KB', right: btn('Copy', { icon: 'copy', kind: 'sm' }), cls: 'closed' })}`;
  }

  const PG_FIELDS = (open) => [
    f('connection_url', true, 'string', '', 'Postgres connection URL. A secret: use <code>${env:PG_URL}</code> or a secrets reference.'),
    f('query', true, 'string', '', 'SQL to run; <code>${bookmark}</code> binds the incremental cursor.', open ? 'sel' : ''),
    ...(open ? [{ span: `<div class="grid4" style="padding:2px 0 6px">${kv('Required', 'yes', 'no default')}${kv('Accepts', 'one SQL statement', 'streamed with a server-side cursor')}${kv('Placeholders', '<span class="mono">${bookmark}</span>', 'only with replication: incremental')}${kv('Marked as SQL', '<span class="mono">x-faucet-sql</span>', 'serve triggers refuse a whole sender-chosen value here')}</div>`, cls: 'sc-open sel' }] : []),
    f('max_connections', false, 'integer', '10', 'Pool size. Each shard holds one connection.'),
    f('batch_size', false, 'integer', '1000', 'Rows per page; <code>0</code> sends everything as one page.'),
    f('tls', false, 'enum', 'disable', 'disable, require, verify_ca or verify_full.'),
    f('replication', false, 'object', '', 'Optional. Incremental reads by a cursor column.'),
  ];
  const PG_YAML = `source:
  type: postgres
  config:
    connection_url: \${env:PG_URL}      # required, secret
    query: "select * from public.orders where updated_at >= \${bookmark}"
    max_connections: 10
    batch_size: 1000
    tls: disable
    # replication:
    #   type: incremental
    #   column: updated_at`;

  M.view('schemas', {
    label: 'Schemas', nav: 'schemas', icon: 'schema', summary: 'config schema of every compiled connector',
    states: {
      catalogue: { label: 'Nothing selected · catalogue', sel: false, covers: ['catalogue of sources/sinks/transforms/state', 'built-into-this-server note'], new: ['split pane with sections + search', 'deep link #/schemas/<kind>/<name>'] },
      source: { label: 'Source selected · fields', covers: ['field table', 'example config', 'raw JSON Schema'], new: ['stat strip', 'type pill / default chip', 'example YAML with copy', 'Use in Submit'] },
      field: { label: 'Field selected · full description', covers: ['field description, enum, constraints'], new: ['field detail inline', 'filter fields'] },
      sink: { label: 'Sink selected', covers: ['sink schema'], new: ['union shown as variants', 'enum values listed'] },
      transform: { label: 'Transform selected', covers: ['transform schema'] },
      search: { label: 'Search with no match', sel: false, covers: ['search'], new: ['empty state that says what to do'] },
      loading: { label: 'Loading', sel: false },
    },
    render(state) {
      const crumbs = { catalogue: ['Build', 'Schemas'], search: ['Build', 'Schemas'], loading: ['Build', 'Schemas'], source: ['Build', 'Schemas', 'postgres · source'], field: ['Build', 'Schemas', 'postgres · source'], sink: ['Build', 'Schemas', 'bigquery · sink'], transform: ['Build', 'Schemas', 'cast · transform'] }[state];
      M.top(crumbs);
      if (state === 'loading') {
        M.list(`${tabs([['All'], 'Sources', 'Sinks', 'Transforms'], 'All')}${input('Search connectors and fields', { ph: true, icon: 'search' })}${skelRows(9)}`);
        M.detail(`<span class="skel t"></span><span class="skel w30"></span>${skelCards(3)}`);
        return;
      }
      const sel = { source: 'source:postgres', field: 'source:postgres', sink: 'sink:bigquery', transform: 'transform:cast' }[state] || '';
      M.list(listPane(sel, state === 'search' ? 'kafkaa' : ''));
      if (state === 'catalogue' || state === 'search') {
        M.detail(`${head('Connector schemas', [], 'Every source, sink, transform and state store compiled into this server, with the config each one accepts. Pick one to see its fields, an example config and the raw JSON Schema.')}
          ${stats([['Sources', '22'], ['Sinks', '19'], ['Transforms', '14'], ['State stores', '4']])}
          <div class="two">
            ${card({ title: 'Most used here', aside: 'by pipelines in the last 7 days', body: tbl(['Connector', 'Kind', ['Runs', 'num']], [[`<span class="lnk mono">postgres-cdc</span>`, 'source', '1,204'], [`<span class="lnk mono">bigquery</span>`, 'sink', '1,180'], [`<span class="lnk mono">rest</span>`, 'source', '402'], [`<span class="lnk mono">s3</span>`, 'sink', '96']]) })}
            ${card({ title: 'Configuration blocks', aside: 'pipeline-level', body: `<div class="pad" style="display:flex;flex-wrap:wrap;gap:6px">${['state', 'dlq', 'delivery', 'resilience', 'sla', 'quality', 'contract', 'masking', 'schema'].map((b) => chip(b, 'mono')).join('')}</div>` })}
          </div>
          <div class="note" style="border:0;padding:0 2px">Showing the <b>22 sources</b> and <b>19 sinks</b> built into this server. Rebuild with more Cargo features (<code>--features full</code>) to enable the rest.</div>`);
        return;
      }
      if (state === 'source' || state === 'field') {
        M.detail(connector({ name: 'postgres', kind: 'source', crate: 'faucet-source-postgres 2.4', lede: 'Runs a SQL query and streams the rows as JSON. Incremental reads bind the last cursor value, so a restart resumes where the last page committed.', stat: [14, 2, 9, 1], fields: PG_FIELDS(state === 'field'), yaml: PG_YAML }));
        return;
      }
      if (state === 'sink') {
        M.detail(connector({ name: 'bigquery', kind: 'sink', crate: 'faucet-sink-bigquery 2.4', lede: 'Streams rows into a table, or MERGEs them by key. Overwrite loads into a temporary table and swaps it in one transaction.', stat: [19, 3, 11, 1],
          fields: [
            f('project_id', true, 'string', '', 'Google Cloud project that owns the dataset.'),
            f('dataset_id', true, 'string', '', 'Dataset holding the table.'),
            f('table_id', true, 'string', '', 'Table to write. Created on first write when <code>create_table</code> is on.'),
            f('credentials', false, 'one of', 'application_default', 'service_account_key, service_account_file or application_default.'),
            f('write_mode', false, 'enum', 'append', 'append, upsert, delete or overwrite. Upsert and delete need <code>key</code>.'),
            f('key', false, 'array', '', 'Columns that identify a row for upsert and delete.'),
          ],
          yaml: `sink:
  type: bigquery
  config:
    project_id: analytics-prod        # required
    dataset_id: commerce              # required
    table_id: orders                  # required
    credentials:
      type: application_default
    write_mode: append
    # key: [order_id]`,
        }));
        return;
      }
      M.detail(connector({ name: 'cast', kind: 'transform', crate: 'transform-cast', lede: 'Converts one field to another type. Values that cannot convert follow on_error.', stat: [3, 2, 1, 0],
        fields: [f('field', true, 'string', '', 'Dot path of the field to convert.'), f('to', true, 'enum', '', 'string, integer, number, boolean or timestamp.'), f('on_error', false, 'enum', 'fail', 'fail, null or keep.')],
        yaml: `transforms:
  - type: cast
    config:
      field: amount     # required
      to: number        # required
      on_error: fail`,
      }));
    },
  });
})();
