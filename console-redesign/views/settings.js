// Settings — canvas: who you are, how the server is, how this console behaves.
(function () {
  const ADMIN_PERMS = ['run_read', 'run_write', 'schema_read', 'doctor', 'trigger_fire', 'dlq_read', 'dlq_manage', 'catalog_read', 'catalog_annotate', 'template_read', 'template_admin', 'change_request', 'change_approve', 'tenant_admin', 'audit_read', 'reload'];
  const VIEWER_PERMS = ['run_read', 'schema_read', 'catalog_read', 'template_read', 'change_read'];
  const identity = (viewer) => card({ icon: 'user', title: 'You', right: viewer ? '' : lnk('Audit log for you'),
    body: `<div class="set-who"><span class="avatar">${viewer ? 'VI' : 'OA'}</span><div><b>${viewer ? 'analyst-vi' : 'ops-admin'}</b><div class="muted">${viewer ? 'viewer · read-only' : 'admin'} · signed in with a bearer token</div></div></div>
      <div class="set-perms">${(viewer ? VIEWER_PERMS : ADMIN_PERMS).map((p) => chip(p, 'mono')).join('')}</div>`,
    note: viewer ? 'Controls you cannot use are hidden across the console. An admin can change your role in the auth config.' : `${ADMIN_PERMS.length} permissions · every route still checks its own.` });
  const server = () => card({ icon: 'server', title: 'Server', right: 'faucet serve 2.4.0',
    body: `${grid(4, [kv('History', '<span class="ok">healthy</span>', 'postgres'), kv('Run queue', '<span class="ok">accepting</span>', '3 of 32 slots busy'), kv('Draining', 'no'), kv('Cluster', '3 replicas', 'pools default, cdc')])}
      ${tbl(['Replica', 'Listens on', 'Pools', ['Running', 'num'], ['Max', 'num']], [
        ['<span class="mono">replica-a</span>', '<span class="mono">10.0.4.11:8080</span>', 'default', '2', '16'],
        ['<span class="mono">replica-b</span>', '<span class="mono">10.0.4.12:8080</span>', 'default', '1', '16'],
        ['<span class="mono">replica-c</span>', '<span class="mono">10.0.4.13:8080</span>', 'cdc', '1', '4'],
      ])}` });
  const prefs = () => card({ icon: 'theme', title: 'This console', aside: 'saved in this browser',
    body: `<div class="form">${field('Theme', document.documentElement.dataset.theme === 'light' ? '<span class="seg"><span class="on">Light</span><span>Dark</span><span>Auto</span></span>' : '<span class="seg"><span>Light</span><span class="on">Dark</span><span>Auto</span></span>')}${field('Times shown in', select('Europe/Berlin'), { help: 'Every time and the date picker follow this' })}
      ${field('Tenant scope', select('All tenants'), { help: 'Scopes Runs, Usage and Changes' })}${field('Sidebar', toggle('Show the sidebar', true))}</div>` });
  const keys = () => card({ icon: 'key', title: 'Keyboard', body: `<table class="set-kbd">${[
    ['<kbd>⌘</kbd> <kbd>K</kbd>', 'Go to any page, run, template, dataset or tenant'],
    ['<kbd>j</kbd> <kbd>k</kbd>', 'Move down and up a list'],
    ['<kbd>↵</kbd>', 'Open the highlighted item'],
    ['<kbd>esc</kbd>', 'Back to the list, or close a menu'],
    ['<kbd>/</kbd>', 'Search the current list'],
  ].map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`).join('')}</table>` });
  const token = () => card({ icon: 'key', title: 'Access token', aside: 'sent as a bearer token on every API call',
    body: `<div class="form one">${field('Token', `<div style="display:flex;gap:8px">${input('••••••••••••••••••••••••', { cls: '' }).replace('class="input', 'style="flex:1" class="input')}${btn('Replace')}${btn('Clear', { kind: 'quiet' })}</div>`, { help: 'Kept in this browser only. Clearing it signs you out.' })}</div>` });
  const admin = (done) => card({ icon: 'refresh', title: 'Default config', aside: 'the merge base every submitted config starts from', right: btn('Reload', { icon: 'refresh', act: true }),
    body: grid(3, [kv('File', '<span class="mono">config/defaults.yaml</span>'), kv('Loaded', done ? 'just now' : '6d ago', done ? 'by ops-admin' : 'at startup'), kv('Runs in flight', '4', 'keep the config they started with')]),
    note: done ? '<span class="ok">Reloaded.</span> The new defaults apply to runs submitted from now on.' : 'Reload re-reads and validates the file; an invalid file is refused and the current one stays.' });

  M.view('settings', {
    label: 'Settings', nav: 'settings', icon: 'settings', shape: 'canvas', summary: 'identity, server health and console preferences',
    states: {
      overview: { label: 'Settings', covers: ['whoami principal/role/permissions', '/readyz history, queue, draining, cluster members + pools', 'version from /metrics', 'theme, timezone, tenant scope, sidebar', 'access token', 'POST /v1/reload (reload)'], new: ['Settings page', 'keyboard shortcuts reference'] },
      reloaded: { label: 'Default config reloaded', covers: ['reload result'], new: ['toast + result note'] },
      viewer: { label: 'Viewer', params: { role: 'viewer' }, covers: ['permission-gated admin controls hidden'] },
    },
    render(state, ctx) {
      const viewer = ctx.role === 'viewer';
      M.top(['Admin', 'Settings']);
      M.detail(`
        ${head('Settings', [], 'Who you are on this server, how the server is doing, and how this console behaves for you.')}
        <div class="set-grid">
          <div class="stack">${identity(viewer)}${prefs()}${token()}</div>
          <div class="stack">${server()}${viewer ? '' : admin(state === 'reloaded')}${keys()}</div>
        </div>`);
      if (state === 'reloaded') M.overlay(toast(`${ic('check')}<span>Reloaded <span class="mono">config/defaults.yaml</span></span>`, 'ok'));
    },
  });
})();
