(function () {
  const { icon, code, aside, crumbs, pn, pfoot } = M;
  const TOC = [['When a record fails', 2], ['Turn it on', 2], ['What lands in the queue', 2], ['Replay', 2], ['Options', 2]];
  const diagram = `<div class="diagram"><svg style="width:100%;max-width:600px;height:auto" viewBox="0 0 600 210">
    <defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path class="head" d="M0 0 10 5 0 10z"/></marker></defs>
    <rect class="box" x="10" y="80" width="110" height="46" rx="9"/><text x="65" y="108" text-anchor="middle">source</text>
    <rect class="box" x="190" y="80" width="130" height="46" rx="9"/><text x="255" y="108" text-anchor="middle">transforms</text>
    <rect class="box" x="400" y="20" width="180" height="46" rx="9"/><text x="490" y="48" text-anchor="middle">sink</text>
    <rect class="box warn" x="400" y="140" width="180" height="46" rx="9"/><text x="490" y="168" text-anchor="middle">dead-letter queue</text>
    <path class="edge" d="M120 103 H186" marker-end="url(#ah)"/>
    <path class="edge" d="M320 96 C 360 96 360 43 396 43" marker-end="url(#ah)"/>
    <path class="edge" d="M320 110 C 360 110 360 163 396 163" marker-end="url(#ah)" stroke-dasharray="5 4"/>
    <text class="lab" x="352" y="60">valid</text><text class="lab" x="340" y="150">failed + reason</text>
  </svg></div><p class="cap">Mermaid diagrams render in both themes.</p>`;
  const body = () => `
    ${crumbs(['Cookbook', 'Moving data reliably', 'Dead-letter queues'])}
    <h1 class="title">Dead-letter queues</h1>
    <p class="lead">Send records that fail a transform, a check or the sink to a side destination with the reason, and keep the rest of the run going.</p>
    <h2>When a record fails</h2>
    ${diagram}
    <h2>Turn it on</h2>
    ${code({ file: 'pipeline.yaml', src: `dlq:
  sink:
    type: jsonl
    config: { path: "./dlq/orders.jsonl" }
  max_ratio: 0.01   # stop if more than 1% fail`, hl: [5] })}
    ${aside('note', 'The dead-letter sink goes through the same governance as the main sink: policies, masking and budgets all apply.')}
    <h2>What lands in the queue</h2>
    <p>Each failed record keeps its original payload and gains an envelope:</p>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Field</th><th>Type</th><th>Meaning</th></tr></thead><tbody>
      <tr><td><code>_faucet.error</code></td><td>string</td><td>Why the record failed, already redacted.</td></tr>
      <tr><td><code>_faucet.stage</code></td><td>string</td><td><code>transform</code>, <code>quality</code> or <code>sink</code>.</td></tr>
      <tr><td><code>_faucet.run_id</code></td><td>string</td><td>The run that produced it.</td></tr>
      <tr><td><code>_faucet.at</code></td><td>timestamp</td><td>RFC 3339, with offset.</td></tr>
    </tbody></table></div>
    <h2>Replay</h2>
    ${code({ term: true, lang: 'sh', src: '$ faucet dlq replay pipeline.yaml --since 1h\n✓ replayed 14 records · 14 written · 0 failed again' })}
    <h2>Options</h2>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Option</th><th>Default</th><th>What it does</th></tr></thead><tbody>
      <tr><td><code>max_ratio</code></td><td><code>1.0</code></td><td>Stop the run when the share of failed records passes this.</td></tr>
      <tr><td><code>max_records</code></td><td>none</td><td>Stop after this many failures.</td></tr>
      <tr><td><code>include_payload</code></td><td><code>true</code></td><td>Keep the original record (masked fields stay masked).</td></tr>
    </tbody></table></div>
    ${pfoot()}
    ${pn('Parallel range partitioning', 'Content verification')}`;
  M.view('recipe', {
    title: 'Cookbook recipe',
    group: 'Pages',
    states: {
      default: { label: 'Default (diagram, tables)', render: () => ({ topic: 'cookbook', active: 'Dead-letter queues', closed: ['Shaping the data', 'Reading from sources', 'Data quality & governance', 'Config & reuse', 'Observability & lineage'], toc: TOC, tocOn: 'When a record fails', body: body() }) },
      'sidebar-open': { label: 'All sidebar groups expanded', widths: [1440], render: () => ({ topic: 'cookbook', active: 'Dead-letter queues', toc: TOC, tocOn: 'When a record fails', body: body() }) },
    },
  });
})();
