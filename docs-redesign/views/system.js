(function () {
  const { icon, code, aside, card, flow } = M;
  const TOK = ['--bg', '--bg-raised', '--bg-soft', '--card', '--card-line', '--ink', '--ink-2', '--muted', '--faint', '--line', '--accent', '--accent-fill', '--accent-soft', '--mark', '--code-bg', '--code-hl', '--note', '--tip', '--caution', '--danger'];
  const sw = () => `<div class="swatches">${TOK.map((t) => `<div class="sw"><i style="background:var(${t})"></i><div><b>${t}</b><span data-tok="${t}"></span></div></div>`).join('')}</div>`;
  const body = () => `
    <div class="sys">
    <h1 class="title">Design system</h1>
    <p class="lead">Every token and component the docs pages use. Starlight's theme variables are mapped onto these in the build.</p>
    <h2>Colour tokens</h2>${sw()}
    <h2>Type</h2>
    <div class="type-row"><code>title · 46/600</code><span style="font-size:46px;font-weight:600;letter-spacing:-.03em;color:var(--ink);line-height:1.1">Documentation</span></div>
    <div class="type-row"><code>lead · 20/380</code><span class="lead" style="margin:0">Calm, grey, generous.</span></div>
    <div class="type-row"><code>h2 · 26/600</code><span style="font-size:26px;font-weight:600;color:var(--ink)">Section heading</span></div>
    <div class="type-row"><code>h3 · 19/600</code><span style="font-size:19px;font-weight:600;color:var(--ink)">Sub-heading</span></div>
    <div class="type-row"><code>body · 16/400</code><span>Body text in Geist, 1.7 line height. <a href="#">A link</a> and <code>inline code</code>.</span></div>
    <div class="type-row"><code>label · 12/600</code><span style="font-size:12px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--muted)">Sidebar group</span></div>
    <h2>Buttons, chips, badges</h2>
    <div class="row-wrap"><a class="btn btn-primary" href="#">Primary${icon('arrow')}</a><a class="btn btn-soft" href="#">Soft</a><a class="btn btn-ghost" href="#">Ghost</a><a class="btn btn-ghost btn-sm" href="#">Small</a></div>
    <div class="row-wrap"><span class="chip on">Selected<span class="ct">43</span></span><span class="chip">Chip<span class="ct">35</span></span><span class="badge b-teal"><span class="dot"></span>Stable</span><span class="badge b-amber"><span class="dot"></span>Beta</span><span class="badge b-blue">Sink</span><span class="badge b-grey">T1</span><kbd>⌘ K</kbd></div>
    <div class="row-wrap"><div class="input" style="max-width:320px"><svg class="i" viewBox="0 0 24 24"></svg><span class="ph">Input</span></div><div class="input focus" style="max-width:320px">${icon('search')}<span>Focused</span><span class="caret"></span></div></div>
    <h2>Cards</h2>
    <div class="cards">${card('zap', 'Task card', 'Centered, with icon and arrow.')}${card('book', 'Left card', 'Compact, for lists of places.', { left: true, more: '12 recipes' })}</div>
    <div class="dlist"><a href="#"><b>Divided list item${icon('arrow')}</b><span>One line of description.</span></a></div>
    <h2>Asides</h2>
    ${aside('note', 'Background a reader may need.')}${aside('tip', 'A shortcut or better way.')}${aside('caution', 'Something that can surprise you.')}${aside('danger', 'Something that can lose data.')}
    <h2>Code</h2>
    ${code({ file: 'pipeline.yaml', src: 'source:\n  type: csv   # a comment\n  config: { path: "./a.csv" }', hl: [2] })}
    ${code({ tabs: ['YAML', 'JSON'], src: 'sink:\n  type: jsonl', copied: true })}
    ${code({ term: true, lang: 'sh', src: '$ faucet run pipeline.yaml --dry-run\n✓ read 10 records' })}
    <h2>Steps & file tree</h2>
    <ol class="steps"><li><h3>First step</h3><p>Text.</p></li><li><h3>Second step</h3><p>Text.</p></li></ol>
    <h2>Pipeline flow</h2>${flow([['source', 'reads'], ['transform'], ['sink', 'writes'], ['dlq', 'failed', 1]])}
    <h2>Segmented control</h2><div class="seg"><button class="on">${icon('cap')}One</button><button>${icon('layers')}Two</button></div>
    <h2>Table</h2>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Name</th><th class="c-mid">Supported</th><th>Notes</th></tr></thead><tbody><tr><td><code>upsert</code></td><td class="c-mid"><span class="yes">✓</span></td><td>Needs a key.</td></tr><tr><td><code>delete</code></td><td class="c-mid"><span class="no">—</span></td><td>Not applicable.</td></tr></tbody></table></div>
    <h2>Page footer</h2>${M.pfoot()}${M.pn('Previous page', 'Next page')}
    </div>`;
  M.view('system', {
    title: 'Design system',
    group: 'System',
    states: { default: { label: 'Tokens & components', widths: [1440], render: () => ({ topic: '', layout: 'splash', body: body() }), after: () => { document.querySelectorAll('[data-tok]').forEach((el) => { el.textContent = getComputedStyle(document.documentElement).getPropertyValue(el.dataset.tok).trim(); }); } } },
  });
})();
