(function () {
  const { icon, code, card, crumbs } = M;
  const TOC = [['What do you want to do?', 2], ['Understand the foundations', 2], ['Where else to look', 2]];
  const body = () => `
    ${crumbs(['Docs', 'Introduction'])}
    <h1 class="title">Documentation</h1>
    <p class="lead">faucet-stream is a data-movement platform. One <code>faucet</code> binary moves data between <b><!--COUNT:sources-->43<!--/COUNT--> sources</b> and <b><!--COUNT:sinks-->35<!--/COUNT--> sinks</b> from a YAML file, with incremental state, CDC and exactly-once delivery built in.</p>
    <div class="btns"><a class="btn btn-primary" href="#">Get started${icon('arrow')}</a><a class="btn btn-ghost" href="#">${icon('cap')}Learn the architecture</a></div>
    ${code({ term: true, lang: 'sh', src: '$ cargo install faucet-cli\n$ faucet init my_pipeline --source postgres --sink bigquery\n$ faucet run my_pipeline.yaml' })}
    <h2>What do you want to do?</h2>
    <div class="cards">
      ${card('zap', 'Move your first data', 'Install faucet and run a pipeline in five minutes.')}
      ${card('sync', 'Keep tables in sync', 'Incremental replication, upserts and CDC mirrors.')}
      ${card('server', 'Run it in production', 'Deploy, schedule, monitor and upgrade safely.')}
      ${card('plug', 'Find a connector', 'Browse every source and sink and what each supports.')}
    </div>
    <ul class="links2">
      <li><a href="#">Shape data with transforms</a></li><li><a href="#">Check data quality</a></li>
      <li><a href="#">Use Template Hub templates</a></li><li><a href="#">Embed faucet in Rust</a></li>
      <li><a href="#">Write your own connector</a></li><li><a href="#">Compare with other tools</a></li>
    </ul>
    <h2>Understand the foundations</h2>
    <p>Four ideas explain almost everything faucet does.</p>
    <div class="dlist">
      <a href="#"><b>How a pipeline runs${icon('arrow')}</b><span>Source, transforms and sink, streamed page by page in bounded memory.</span></a>
      <a href="#"><b>State and resuming${icon('arrow')}</b><span>Bookmarks that let every run pick up exactly where the last one stopped.</span></a>
      <a href="#"><b>Delivery guarantees${icon('arrow')}</b><span>At-least-once by default, exactly-once where the sink can commit atomically.</span></a>
      <a href="#"><b>Where your data may go${icon('arrow')}</b><span>Data-flow policies, PII masking and contracts, checked before a row moves.</span></a>
    </div>
    <h2>Where else to look</h2>
    <div class="cards three">
      ${card('book', 'API docs', 'Every crate on docs.rs, with all features on.', { left: true })}
      ${card('pkg', 'Template Hub', 'Ready-made source and sink templates.', { left: true })}
      ${card('terminal', 'Runnable examples', 'A config for nearly every connector pair.', { left: true })}
    </div>
    ${M.pfoot()}`;
  M.view('landing', {
    title: 'Docs home',
    group: 'Pages',
    states: {
      default: { label: 'Default', render: () => ({ topic: 'learn', active: 'Introduction', toc: TOC, body: body() }) },
    },
  });
})();
