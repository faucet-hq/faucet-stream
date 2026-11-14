(function () {
  const { icon, code, aside, crumbs, pn, pfoot } = M;
  const Y = '<span class="yes">✓</span>', N = '<span class="no">—</span>';
  const ROWS = [
    ['bigquery', 'Source', 'T1', 'stable', 1, 1, 1, 1],
    ['bigquery', 'Sink', 'T1', 'stable', 1, 1, 1, 0],
    ['clickhouse', 'Sink', 'T1', 'stable', 1, 1, 0, 0],
    ['csv', 'Source', 'T1', 'stable', 1, 0, 1, 1],
    ['delta', 'Sink', 'T1', 'beta', 1, 1, 1, 0],
    ['kafka', 'Source', 'T1', 'stable', 1, 1, 0, 1],
    ['mongodb', 'Source', 'T2', 'beta', 1, 0, 0, 1],
    ['mysql-cdc', 'Source', 'T1', 'stable', 1, 1, 0, 1],
    ['postgres', 'Source', 'T1', 'stable', 1, 1, 0, 1],
    ['postgres', 'Sink', 'T1', 'stable', 1, 1, 0, 0],
    ['postgres-cdc', 'Source', 'T1', 'stable', 1, 1, 0, 1],
    ['rest', 'Source', 'T1', 'stable', 1, 0, 1, 1],
    ['s3', 'Sink', 'T1', 'stable', 1, 1, 1, 0],
    ['snowflake', 'Sink', 'T1', 'stable', 1, 1, 1, 0],
  ];
  const tier = { stable: '<span class="badge b-teal"><span class="dot"></span>Stable</span>', beta: '<span class="badge b-amber"><span class="dot"></span>Beta</span>' };
  const table = (rows) => `<div class="tbl-wrap" style="max-height:620px"><table class="t sticky1"><thead><tr><th>Connector</th><th>Kind</th><th>Maturity</th><th class="c-mid">Conformance</th><th class="c-mid">Resumable</th><th class="c-mid">Effectively-once</th><th class="c-mid">Compression</th><th class="c-mid">Discover</th></tr></thead><tbody>${rows.map((r) => `<tr><td><a href="#"><code>${r[0]}</code></a></td><td>${r[1]}</td><td>${tier[r[3]]}</td><td class="c-mid"><span class="badge b-grey">${r[2]}</span></td>${r.slice(4).map((v) => `<td class="c-mid">${v ? Y : N}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  const filter = (q, kind) => `<div class="filter"><div class="input${q ? ' focus' : ''}">${icon('search')}${q ? `<span>${q}</span><span class="caret"></span>` : '<span class="ph">Filter connectors…</span>'}</div>
    ${[['All', 78], ['Sources', 43], ['Sinks', 35]].map(([l, c]) => `<span class="chip${l === kind ? ' on' : ''}">${l}<span class="ct">${c}</span></span>`).join('')}
    <span class="chip">${icon('filter')}Stable only</span></div>`;
  const catalog = (q = '', kind = 'All') => {
    let rows = ROWS.filter((r) => (!q || r[0].includes(q)) && (kind === 'All' || r[1] + 's' === kind));
    return `
    ${crumbs(['Reference', 'Connectors', 'Connector catalog'])}
    <h1 class="title">Connector catalog</h1>
    <p class="lead">faucet ships <b><!--COUNT:sources-->43<!--/COUNT--> sources</b> and <b><!--COUNT:sinks-->35<!--/COUNT--> sinks</b>. Each is a Cargo feature and its own crate; <code>faucet list</code> shows what your binary has.</p>
    ${filter(q, kind)}
    ${rows.length ? `<p class="count-line">Showing ${q || kind !== 'All' ? rows.length : 78} of 78${q ? ` · matching “${q}”` : ''}</p>${table(rows)}<p class="legend">✓ supported · — not applicable. Conformance: T1 passes the faucet-conformance battery in CI; T2 not yet wired in.</p>` : `<div class="empty">${icon('search')}<b>No connector matches “${q}”</b>Try a shorter name, or check the <a href="#">Connector Hub</a> for community crates.<div style="margin-top:16px"><a class="btn btn-ghost btn-sm" href="#">Clear filter</a></div></div>`}
    ${aside('note', 'Two different “tier” signals: <b>Conformance</b> says whether the connector runs in the CI test battery; <b>Maturity</b> is computed by <code>faucet conformance</code> from its schema and capabilities.')}
    ${pfoot()}
    ${pn(null, 'Choosing a connector')}`;
  };
  const TOC = [['Sources and sinks', 2], ['Legend', 2]];
  M.view('catalog', {
    title: 'Connector catalog',
    group: 'Reference',
    states: {
      default: { label: 'All connectors', render: () => ({ topic: 'reference', active: 'Connector catalog', toc: TOC, tocOn: TOC[0][0], body: catalog() }) },
      filtered: { label: 'Filtered (“post”, Sources)', render: () => ({ topic: 'reference', active: 'Connector catalog', toc: TOC, tocOn: TOC[0][0], body: catalog('post', 'Sources') }) },
      empty: { label: 'No match', render: () => ({ topic: 'reference', active: 'Connector catalog', toc: TOC, tocOn: TOC[0][0], body: catalog('zendeskk') }) },
    },
  });

  const FEAT = ['Streams', 'Resumable', 'Effectively-once', 'Upsert', 'Delete', 'Overwrite', 'Schema evolve', 'Compression', 'Partitioned', 'Discover', 'CDC', 'Exactly-once'];
  const MX = ['bigquery', 'clickhouse', 'delta', 'duckdb', 'elasticsearch', 'iceberg', 'jsonl', 'kafka', 'mysql', 'postgres', 's3', 'snowflake', 'sqlite'];
  const matrix = () => `
    ${crumbs(['Reference', 'Connectors', 'Capability matrix'])}
    <h1 class="title">Capability matrix</h1>
    <p class="lead">What every sink can do. Generated from each connector's <code>DESCRIPTOR</code>, so it is never out of date.</p>
    <p class="count-line">${icon('columns')} Scroll sideways inside the table; the connector column stays put.</p>
    <div class="tbl-wrap" style="max-height:560px"><table class="t sticky1"><thead><tr><th>Sink</th>${FEAT.map((f) => `<th class="c-mid">${f}</th>`).join('')}</tr></thead><tbody>
    ${MX.map((m, i) => `<tr><td><code>${m}</code></td>${FEAT.map((_, j) => `<td class="c-mid">${(i * 7 + j * 3) % 5 ? Y : N}</td>`).join('')}</tr>`).join('')}
    </tbody></table></div>
    <p class="legend">✓ declared and covered by a conformance assertion · — not supported.</p>
    ${pfoot()}${pn('Conformance & tiers', 'Feature compatibility')}`;
  M.view('matrix', {
    title: 'Capability matrix (wide table)',
    group: 'Reference',
    states: {
      default: { label: 'Default', render: () => ({ topic: 'reference', active: 'Capability matrix', wide: true, body: matrix() }) },
      'scrolled-x': { label: 'Table scrolled sideways', render: () => ({ topic: 'reference', active: 'Capability matrix', wide: true, body: matrix() }), after: () => { const w = document.querySelector('.tbl-wrap'); w.scrollLeft = 420; } },
    },
  });

  const OPTS = [
    ['<code>--dry-run</code>', 'Read from the source and run transforms, but write nothing.'],
    ['<code>--limit &lt;N&gt;</code>', 'Stop after N records.'],
    ['<code>--state &lt;URI&gt;</code>', 'Override the state backend (file, redis, postgres).'],
    ['<code>--set &lt;key=value&gt;</code>', 'Override one config value; repeatable.'],
    ['<code>--log-format &lt;fmt&gt;</code>', '<code>text</code> (default) or <code>json</code>.'],
  ];
  const cli = () => `
    ${crumbs(['Reference', 'CLI & config', 'CLI commands'])}
    <h1 class="title">CLI commands</h1>
    <p class="lead">Every <code>faucet</code> subcommand. Run <code>faucet &lt;command&gt; --help</code> for the same text in your terminal.</p>
    <div class="row-wrap">${['init', 'validate', 'run', 'schedule', 'serve', 'state', 'status', 'verify', 'rollback', 'doctor', 'list', 'schema'].map((c, i) => `<a class="chip${i === 2 ? ' on' : ''}" href="#"><code style="background:none;border:0;padding:0;color:inherit">${c}</code></a>`).join('')}</div>
    <h2><code style="font-size:.85em">faucet run</code></h2>
    <p>Run one pipeline to completion, then exit with a status code you can script on.</p>
    ${code({ term: true, lang: 'sh', src: '$ faucet run <CONFIG> [OPTIONS]' })}
    <h3>Options</h3>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Option</th><th>Description</th></tr></thead><tbody>${OPTS.map(([a, b]) => `<tr><td style="white-space:nowrap">${a}</td><td>${b}</td></tr>`).join('')}</tbody></table></div>
    <h3>Exit codes</h3>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Code</th><th>Meaning</th></tr></thead><tbody><tr><td><code>0</code></td><td>Success</td></tr><tr><td><code>2</code></td><td>Config invalid</td></tr><tr><td><code>3</code></td><td>Data-quality check failed</td></tr><tr><td><code>4</code></td><td>Source or sink error after retries</td></tr></tbody></table></div>
    ${pfoot()}${pn('Template Hub matrix', 'Configuration file')}`;
  const TOC_CLI = [['faucet run', 2], ['Options', 3], ['Exit codes', 3]];
  M.view('cli', {
    title: 'CLI reference',
    group: 'Reference',
    states: { default: { label: 'Default', render: () => ({ topic: 'reference', active: 'CLI commands', toc: TOC_CLI, tocOn: 'faucet run', body: cli() }) } },
  });
})();
