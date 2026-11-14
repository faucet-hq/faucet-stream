(function () {
  const { icon, code, aside, crumbs, pn, pfoot } = M;
  const seg = (mode) => `<div class="seg" role="group" aria-label="Reading mode"><button class="${mode === 'beginner' ? 'on' : ''}">${icon('cap')}Beginner's guide</button><button class="${mode === 'architect' ? 'on' : ''}">${icon('layers')}Architect reference</button></div>`;
  const beginner = () => `
    <h2>A pipeline is a conveyor belt</h2>
    <p>Think of faucet as a conveyor belt between two places. The <strong>source</strong> puts boxes of records on the belt, the <strong>sink</strong> takes them off, and faucet makes sure no box falls off on the way.</p>
    ${M.flow([['Source', 'reads'], ['faucet pipeline', 'moves'], ['Sink', 'writes']])}
    <h2>Pages, not files</h2>
    <p>Records travel in <strong>pages</strong>. faucet writes one page before it reads the next, so memory stays small even for a billion rows.</p>
    ${M.flow(['page 1', 'write', 'page 2', 'write', ['page 3', '+ bookmark'], 'save bookmark'])}
    ${aside('tip', 'The bookmark is saved only after the sink says the page is safe. That is why a crash never loses or skips rows.')}
    <h2>Where to next</h2>
    <p>Switch to <strong>Architect reference</strong> above to see the traits and guarantees behind this picture.</p>`;
  const architect = () => `
    <h2>Runtime layers</h2>
    <div class="tbl-wrap"><table class="t"><thead><tr><th>Layer</th><th>Crate</th><th>Responsibility</th></tr></thead><tbody>
      <tr><td>Connectors</td><td><code>faucet-source-*</code> / <code>faucet-sink-*</code></td><td>Family drivers wrapped in <code>DriverSource</code> / <code>DriverSink</code>.</td></tr>
      <tr><td>Governance</td><td><code>faucet-core::governance</code></td><td>Budget, policy, profiling and metadata passes, assembled once.</td></tr>
      <tr><td>Engine</td><td><code>faucet-core::pipeline</code></td><td>Streams pages, writes only <code>GovernedPage</code>s, commits state.</td></tr>
      <tr><td>Runtimes</td><td><code>faucet-cli</code></td><td><code>run</code>, <code>schedule</code> and <code>serve</code>.</td></tr>
    </tbody></table></div>
    <h2>The write path</h2>
    ${code({ file: 'crates/core/src/traits.rs', lang: 'raw', src: `#[async_trait]
pub trait Sink: Send + Sync {
    async fn write_governed(&mut self, page: GovernedPage) -> Result<WriteReport>;
    async fn flush(&mut self) -> Result<()>;
}` })}
    ${aside('note', 'A sink that writes fewer rows than offered must say why in its <code>WriteReport</code>; otherwise the page fails with <code>RowsUnaccounted</code>.')}`;
  const body = (mode) => `
    ${crumbs(['Learn', 'Understand', 'Learn the architecture'])}
    <h1 class="title">Learn the architecture</h1>
    <p class="lead">One story, two depths. Pick how deep you want to go; the page remembers your choice.</p>
    ${seg(mode)}
    ${mode === 'beginner' ? beginner() : architect()}
    ${pfoot()}
    ${pn('Agent skills', 'REST API → BigQuery')}`;
  const TOC_B = [['A pipeline is a conveyor belt', 2], ['Pages, not files', 2], ['Where to next', 2]];
  const TOC_A = [['Runtime layers', 2], ['The write path', 2]];
  M.view('learn', {
    title: 'Learn the architecture',
    group: 'Pages',
    states: {
      beginner: { label: "Beginner's guide", render: () => ({ topic: 'learn', active: 'Learn the architecture', toc: TOC_B, tocOn: TOC_B[0][0], body: body('beginner') }) },
      architect: { label: 'Architect reference', render: () => ({ topic: 'learn', active: 'Learn the architecture', toc: TOC_A, tocOn: TOC_A[0][0], body: body('architect') }) },
    },
  });
})();
