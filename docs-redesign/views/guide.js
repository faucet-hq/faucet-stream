(function () {
  const { icon, code, aside, crumbs, pn, pfoot } = M;
  const TOC = [['Before you start', 2], ['Create the config', 2], ['Check it', 2], ['Run it', 2], ['Run it again', 3], ['What just happened', 2], ['Next steps', 2]];
  const YAML = `# Read a CSV file and write JSON lines.
source:
  type: csv
  config:
    path: "./data/orders.csv"
    has_header: true

transforms:
  - type: rename
    fields: { order_id: id }

sink:
  type: jsonl
  config:
    path: "./out/orders.jsonl"`;
  const JSON_SRC = `{
  "source": { "type": "csv", "config": { "path": "./data/orders.csv" } },
  "sink":   { "type": "jsonl", "config": { "path": "./out/orders.jsonl" } }
}`;
  const body = (o = {}) => `
    ${crumbs(['Learn', 'Start here', 'Your first pipeline'])}
    <h1 class="title">Your first pipeline</h1>
    <p class="lead">Move rows from a CSV file into JSON lines with one small YAML file. It takes about five minutes and needs no database or cloud account.</p>
    ${aside('tip', 'Using Claude Code or Codex? Install the <a href="#">faucet agent skills</a> and ask it to “write a pipeline from orders.csv to JSON lines”.')}
    <h2>Before you start</h2>
    <p>You need the <code>faucet</code> binary. Check it is on your <code>PATH</code>:</p>
    ${code({ term: true, lang: 'sh', src: '$ faucet --version\nfaucet 1.18.2' })}
    <h2>Create the config</h2>
    <p>Save this as <code>pipeline.yaml</code>. Every pipeline has a <strong>source</strong>, an optional list of <strong>transforms</strong>, and a <strong>sink</strong>.</p>
    ${o.tabJson ? code({ tabs: ['pipeline.yaml', 'pipeline.json'], tab: 1, lang: 'raw', src: JSON_SRC, copied: o.copied }) : code({ tabs: ['pipeline.yaml', 'pipeline.json'], tab: 0, src: YAML, hl: [3, 4, 13, 14], copied: o.copied })}
    <h2>Check it</h2>
    <p><code>faucet validate</code> checks the file against each connector's schema without moving any data.</p>
    ${code({ term: true, lang: 'sh', src: '$ faucet validate pipeline.yaml\n✓ pipeline.yaml is valid (source: csv, sink: jsonl)' })}
    <h2>Run it</h2>
    ${code({ term: true, lang: 'sh', src: '$ faucet run pipeline.yaml\n✓ read 1,204 records · wrote 1,204 records · 0.18 s' })}
    <h3>Run it again</h3>
    <p>Running the same file again rewrites the output. To read only new rows, add <a href="#">state</a> so faucet remembers where it stopped.</p>
    ${aside('caution', 'A sink in <code>append</code> mode adds rows on every run. Use <code>overwrite</code> or <code>upsert</code> when you re-run a full load.')}
    <h2>What just happened</h2>
    <p>faucet streamed the file page by page: each page was read, renamed, and written before the next one was read, so memory stays flat however big the file is.</p>
    ${M.flow([['csv', 'source'], ['rename', 'transform'], ['jsonl', 'sink']])}
    <h2>Next steps</h2>
    <ul><li><a href="#">Core concepts</a>: sources, sinks, streams and state.</li><li><a href="#">REST API → BigQuery</a>: a real incremental pipeline.</li><li><a href="#">Connector catalog</a>: every source and sink.</li></ul>
    ${pfoot()}
    ${pn('Agent skills', 'Core concepts')}`;
  const base = (o = {}) => ({ topic: 'learn', active: 'Your first pipeline', toc: TOC, tocOn: o.tocOn || 'Before you start', tocOpen: o.tocOpen, body: body(o) });
  M.guideBase = base;
  M.view('guide', {
    title: 'Guide page',
    group: 'Pages',
    states: {
      default: { label: 'Default', widths: [1440, 1000, 390], render: () => base() },
      scrolled: { label: 'Scrolled (sticky header, TOC follows)', widths: [1440, 1000, 390], render: () => base({ tocOn: 'Run it' }), after: () => { const h = [...document.querySelectorAll('.prose h2')].find((x) => x.textContent === 'Run it'); window.scrollTo(0, h.getBoundingClientRect().top + window.scrollY - 120); } },
      'tab-json': { label: 'Code tab switched to JSON', widths: [1440], render: () => base({ tabJson: true }), after: () => { const h = [...document.querySelectorAll('.prose h2')].find((x) => x.textContent === 'Create the config'); window.scrollTo(0, h.getBoundingClientRect().top + window.scrollY - 100); } },
      copied: { label: 'Code copied', widths: [1440, 390], render: () => base({ copied: true, tocOn: 'Create the config' }), after: () => { const h = [...document.querySelectorAll('.prose h2')].find((x) => x.textContent === 'Create the config'); window.scrollTo(0, h.getBoundingClientRect().top + window.scrollY - 100); } },
      'toc-open': { label: '"On this page" dropdown open', widths: [1000, 390], render: () => base({ tocOn: 'Run it', tocOpen: true }), after: () => { const h = [...document.querySelectorAll('.prose h2')].find((x) => x.textContent === 'Run it'); window.scrollTo(0, h.getBoundingClientRect().top + window.scrollY - 160); } },
    },
  });
})();
