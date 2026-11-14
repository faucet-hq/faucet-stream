(function () {
  const { icon, code, aside, crumbs, pn, pfoot } = M;
  const TOC = [['What you will build', 2], ['Steps', 2], ['Set up the project', 3], ['Describe the API', 3], ['Add incremental state', 3], ['Point it at BigQuery', 3], ['Run and re-run', 3], ['Troubleshooting', 2]];
  const tree = `<div class="tree">
    <div class="d">${icon('folder')}rest-to-bq/</div>
    <div style="padding-left:22px">${icon('file')}<span class="hl">pipeline.yaml</span><span class="note">the pipeline</span></div>
    <div style="padding-left:22px">${icon('file')}.env<span class="note">API token and GCP key path</span></div>
    <div style="padding-left:22px" class="d">${icon('folder')}state/<span class="note">bookmarks (created on the first run)</span></div>
  </div>`;
  const body = () => `
    ${crumbs(['Learn', 'Tutorials', 'REST API → BigQuery'])}
    <h1 class="title">REST API → BigQuery</h1>
    <p class="lead">Pull a paginated JSON API into a BigQuery table, then re-run it so only new records move.</p>
    <div class="row-wrap" style="margin:-8px 0 30px"><span class="badge b-grey">${icon('clock')}20 minutes</span><span class="badge b-teal"><span class="dot"></span>source: rest</span><span class="badge b-blue"><span class="dot"></span>sink: bigquery</span></div>
    <h2>What you will build</h2>
    ${M.flow([['rest', 'cursor pages'], ['state', 'bookmark'], ['bigquery', 'upsert by id']])}
    <h2>Steps</h2>
    <ol class="steps">
      <li><h3>Set up the project</h3><p>Make a folder with these files:</p>${tree}</li>
      <li><h3>Describe the API</h3><p>Tell faucet where the records are and how to get the next page.</p>
        ${code({ file: 'pipeline.yaml', src: `source:
  type: rest
  config:
    base_url: "https://api.example.com/v2"
    path: /orders
    records_path: "$.data[*]"
    pagination:
      style: cursor
      next_path: "$.meta.next_cursor"`, hl: [6, 7, 8, 9] })}</li>
      <li><h3>Add incremental state</h3><p>Remember the newest <code>updated_at</code> so the next run asks only for newer rows.</p>
        ${code({ file: 'pipeline.yaml', src: `    replication:
      mode: incremental
      key: updated_at
state:
  backend: file
  path: ./state`, add: [1, 2, 3, 4, 5, 6] })}
        ${aside('note', 'State is saved only after the sink confirms the write, so a crash never skips rows.')}</li>
      <li><h3>Point it at BigQuery</h3>${code({ file: 'pipeline.yaml', src: `sink:
  type: bigquery
  config:
    project: my-project
    dataset: raw
    table: orders
    write_mode: upsert
    key: [id]` })}</li>
      <li><h3>Run and re-run</h3>${code({ term: true, lang: 'sh', src: '$ faucet run pipeline.yaml\n✓ 3 pages · 2,450 records · bookmark updated_at=2026-11-12T09:14:03Z\n$ faucet run pipeline.yaml\n✓ 1 page · 12 records (new since the bookmark)' })}</li>
    </ol>
    <h2>Troubleshooting</h2>
    ${aside('danger', 'If the API returns the same cursor twice, faucet stops with <code>PaginationLoop</code> rather than loop forever. Check <code>next_path</code>.', 'Pagination loop')}
    ${pfoot()}
    ${pn('Learn the architecture', 'PostgreSQL CDC → JSONL')}`;
  M.view('tutorial', {
    title: 'Tutorial page',
    group: 'Pages',
    states: {
      default: { label: 'Default (steps, file tree, diff lines)', render: () => ({ topic: 'learn', active: 'REST API → BigQuery', toc: TOC, tocOn: 'What you will build', body: body() }) },
      steps: { label: 'Scrolled to the steps', widths: [1440, 390], render: () => ({ topic: 'learn', active: 'REST API → BigQuery', toc: TOC, tocOn: 'Add incremental state', body: body() }), after: () => { const h = [...document.querySelectorAll('.steps h3')][1]; window.scrollTo(0, h.getBoundingClientRect().top + window.scrollY - 110); } },
    },
  });
})();
