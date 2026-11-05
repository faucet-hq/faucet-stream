// Policy (#1043) — the data-flow policy the server enforces: classifications,
// rules, refusals and a dry run (viewer+), and for admins on a server-managed
// policy an editor whose changes go through an approved change request.
// A policy loaded with `--policy <file>` is read-only here.
(function () {
  const SRC = { file: 'file', server: 'server' };
  const subFor = (src, ver = 7) => src === SRC.file
    ? [pill('Enforced', 'ok', 'shield'), '<span>eu-pii</span>', `<span>loaded from <span class="mono">--policy</span> file · sha <span class="mono">3f9c…a1</span></span>`, '<span>since server start 2d ago</span>']
    : [pill('Enforced', 'ok', 'shield'), `<span>eu-pii · version ${ver}</span>`, `<span>approved by ${lnk('sec-lead')} 3d ago</span>`, '<span>managed by the server</span>'];
  const tabsFor = (on, { history = true, refusals = 14 } = {}) => dtabs(['Rules', ['Refusals', refusals], 'Dry run', ...(history ? [['History', 7]] : [])], on);
  const editBtn = (src) => (src === SRC.server ? btn('Edit policy', { icon: 'edit', kind: 'primary', act: true }) : '');

  const classCard = () => card({
    icon: 'layers', title: 'Classifications', aside: 'label a column by name, pattern or value', right: '4 labels',
    body: tbl(['Label', 'Matches', 'Value detector', ['Columns seen (30d)', 'num']], [
      ['<span class="mono">pii.email</span>', 'fields <span class="mono">email</span>, pattern <span class="mono">.*_email$</span>', 'email', '12 in 9 datasets'],
      ['<span class="mono">pii.card</span>', 'pattern <span class="mono">^card_.*</span>', 'credit_card', '3 in 2 datasets'],
      ['<span class="mono">pii.national_id</span>', 'fields <span class="mono">ssn</span>, <span class="mono">tax_id</span>', 'ssn', '2 in 2 datasets'],
      ['<span class="mono">location.precise</span>', 'fields <span class="mono">lat</span>, <span class="mono">lon</span>', '—', '4 in 1 dataset'],
    ]),
  });
  const rulesCard = () => card({
    icon: 'shield', title: 'Rules', aside: 'which sinks a label may reach', right: '3 rules',
    body: tbl(['Rule', 'When', 'Allowed if', 'At run time'], [
      ['<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', '<span class="mono">pii.*</span> · any sink', 'sink <span class="mono">residency</span> ∈ eu, or masked by hash · tokenize', 'quarantine'],
      ['<span class="mono" style="white-space:nowrap">no-raw-cards</span>', '<span class="mono">pii.card</span> · any sink', 'masked by partial', 'fail'],
      ['<span class="mono" style="white-space:nowrap">no-pii-to-stdout</span>', '<span class="mono">pii.*</span> · sink kind stdout', '<span class="bad">never (deny)</span>', 'fail'],
    ]),
    note: 'Refused at submit when a plan shows a violation; checked again on every page at run time.',
  });
  const factsCard = () => card({ title: 'In effect', body: grid(4, [kv('Rules', '3'), kv('Labels', '4'), kv('Sinks with attributes', '11', 'residency, tier'), kv('Refusals (7d)', '14', '9 at submit · 5 at run time')]) });
  const rulesBody = () => `${factsCard()}<div class="pair">${classCard()}${rulesCard()}</div>`;

  const refusals = (tenant = false) => {
    const rows = [
      ['today 09:12', lnk('orders-export · 7f3c'), 'acme', '<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', '<span class="mono">customer_email</span>', 's3 · us-backup', 'run time · quarantined 412 rows'],
      ['today 08:40', lnk('change cr-0137'), 'globex', '<span class="mono" style="white-space:nowrap">no-pii-to-stdout</span>', '<span class="mono">email</span>', 'stdout', 'submit · refused'],
      ['yesterday 17:03', lnk('crm-debug · 2e81'), 'acme', '<span class="mono" style="white-space:nowrap">no-raw-cards</span>', '<span class="mono">card_number</span>', 'postgres · analytics', 'run time · failed'],
      ['yesterday 11:20', lnk('template acme/leads v3'), 'acme', '<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', '<span class="mono">work_email</span>', 'bigquery · us-central', 'submit · refused'],
    ].filter((x) => !tenant || x[2] === 'acme');
    return `${stats(tenant ? [['Refusals (7d)', '3'], ['At submit', '1'], ['At run time', '2', 'bad']] : [['Refusals (7d)', '14'], ['At submit', '9'], ['At run time', '5', 'bad'], ['Rules involved', '3']])}
      ${card({ icon: 'alert', title: tenant ? 'Your refused runs' : 'Refusals', right: tenant ? '' : `${select('All tenants')}`, body: tbl(['When', 'Run or change', ...(tenant ? [] : ['Tenant']), 'Rule', 'Column', 'Sink', 'Where'], rows.map((x) => (tenant ? [x[0], x[1], x[3], x[4], x[5], x[6]] : x))), note: tenant ? 'Ask an operator if a rule blocks something you need; they can see why.' : `From the audit log (<code>policy.denied</code>) and runs that failed with error kind <code>policy</code>. ${lnk('Open in Audit log')}` })}`;
  };

  const DRY_CFG = `pipeline:
  source:
    type: postgres
    config: { query: "select * from contacts" }
  sink:
    type: s3
    attributes: { residency: us }
    config: { bucket: backups-us, prefix: contacts/ }`;
  const dryRun = (clean) => `<div class="pair">
      ${card({ icon: 'code', title: 'Config to check', right: `<span class="seg"><span class="on">Paste</span><span>Template</span></span>`, body: `<div class="pad stack" style="gap:10px"><div class="area" style="height:190px">${clean ? DRY_CFG.replace('residency: us', 'residency: eu').replace('backups-us', 'backups-eu') : DRY_CFG}</div><div style="display:flex;gap:8px;align-items:center">${select('row: all rows')}${check('Use a sample of real rows (plan --sample)', true)}<span style="margin-left:auto">${btn('Check', { icon: 'shield', kind: 'primary' })}</span></div></div>`, note: 'Nothing runs. The config is checked the way a submit would be.' })}
      ${clean
        ? card({ icon: 'check', title: 'No violations', body: `<div class="pad">${alert('This config passes every rule.', '<code>customer_email</code> carries <code>pii.email</code> and reaches a sink with <code>residency: eu</code>, which <code>pii-to-eu-only</code> allows.', 'ok')}</div>` })
        : card({ icon: 'alert', title: '2 violations', tag: 'would be refused', body: tbl(['Rule', 'Column', 'Label', 'Why', 'To fix'], [
          ['<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', '<span class="mono">email</span>', 'pii.email', 'residency is us', 'hash or tokenize it, or use an eu sink'],
          ['<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', '<span class="mono">phone</span>', 'pii.phone (by value)', 'residency is us', 'same'],
        ]), note: 'Labels came from the sample (12 rows). Columns the sample did not show are checked again at run time.' })}
    </div>`;

  const POLICY_V7 = `version: 1
description: EU personal data stays in the EU
classifications:
  - label: pii.email
    fields: [email]
    field_pattern: ".*_email$"
    value_detector: email
  - label: pii.card
    field_pattern: "^card_.*"
    value_detector: credit_card
rules:
  - name: pii-to-eu-only
    when: { label: "pii.*" }
    require: { residency: [eu] }
    mask: [hash, tokenize]
    on_runtime: quarantine`;
  const POLICY_BAD = POLICY_V7.replace('require: { residency: [eu] }', 'require: { residency: eu }').replace('value_detector: credit_card', 'value_detector: iban');

  const history = () => card({
    icon: 'clock', title: 'Versions', right: 'every change was approved by a second admin',
    body: tbl(['Version', 'Changed by', 'Approved by', 'Reason', 'When', ''], [
      ['<b>v7</b> ' + tag('active'), lnk('sec-raj'), lnk('sec-lead'), 'Add pii.national_id; deny stdout for pii.*', '3d ago', ''],
      ['v6', lnk('sec-lead'), lnk('ops-admin'), 'Allow tokenize for pii.email', '12d ago', btn('Roll back to v6', { icon: 'undo', act: true })],
      ['v5', lnk('sec-raj'), lnk('sec-lead'), 'Card numbers: partial mask only', '1mo ago', btn('Roll back to v5', { icon: 'undo', act: true })],
      ['v4', lnk('ops-admin'), lnk('sec-lead'), 'Seeded from eu-pii.yaml', '2mo ago', btn('Roll back to v4', { icon: 'undo', act: true })],
    ]),
    note: 'A rollback is a change request too: it takes effect when another admin approves it.',
  });

  M.view('policy', {
    label: 'Policy', nav: 'policy', icon: 'policy', shape: 'canvas', summary: 'the data-flow policy the server enforces, and approved edits (#1043)',
    states: {
      file: { label: 'Rules · loaded from the --policy file (read only)', covers: ['GET /v1/policy', 'classifications', 'rules', 'source file + sha'], new: ['Policy page (#1043)'] },
      server: { label: 'Rules · server-managed (admin can edit)', covers: ['version + approver', 'Edit policy (admin)'] },
      refusals: { label: 'Refusals', covers: ['GET /v1/policy/refusals', 'submit + run-time refusals', 'tenant filter'] },
      dryrun: { label: 'Dry run · violations', covers: ['POST /v1/policy/check', 'how to fix'] },
      'dryrun-clean': { label: 'Dry run · clean', covers: ['passes every rule'] },
      history: { label: 'History · versions + rollback', covers: ['version history', 'approver + reason', 'rollback as a change request'] },
      rollback: { label: 'Roll back dialog', covers: ['rollback creates a change request'] },
      edit: { label: 'Edit · validation errors', covers: ['YAML editor', 'PolicySpec::validate errors with lines'] },
      preview: { label: 'Preview · diff + impact', covers: ['POST /v1/policy/preview', 'YAML diff', 'newly refused / allowed templates and recent runs', 'reason'] },
      submitted: { label: 'Change requested · pending approval', covers: ['POST /v1/policy → ChangeKind::PolicyUpdate', 'link to Changes'] },
      none: { label: 'No policy loaded', covers: ['empty state', 'write a policy (server-managed)'] },
      viewer: { label: 'Viewer · read only', params: { role: 'viewer' }, covers: ['no Edit / rollback'] },
      tenant: { label: 'Tenant principal', params: { scope: 'tenant' }, covers: ['policy hidden', 'own refusals only'] },
      loading: { label: 'Loading', covers: ['skeleton'] },
      error: { label: 'Could not load', covers: ['error with retry'] },
    },
    render(state) {
      M.top(['Govern', 'Policy']);
      if (state === 'loading') { M.detail(`${head('Policy', [])}${skelCards(3)}`); return; }
      if (state === 'error') { M.detail(`${head('Policy', [])}${alert('Could not load the policy.', 'The server answered 500. Enforcement is unaffected; only this page failed.', 'bad', btn('Retry', { icon: 'refresh' }))}`); return; }
      if (state === 'none') {
        M.detail(`${head('Policy', [pill('None loaded', '')], 'No data-flow policy is enforced: any column may reach any sink.')}
          <div class="two">${card({ icon: 'edit', title: 'Write one here', body: `<div class="pad stack" style="gap:10px"><span>Start from a template. Like every policy change, the first version needs a second admin’s approval before it is enforced.</span><div>${btn('Write a policy', { icon: 'plus', kind: 'primary', act: true })}</div></div>` })}
          ${card({ icon: 'code', title: 'Or load a file at start', body: code('faucet serve --policy policies/eu-pii.yaml', { box: false, lang: 'plain' }), note: 'A file-loaded policy is shown here read-only.' })}</div>`);
        return;
      }
      if (state === 'tenant') {
        M.detail(`${head('Policy', [pill('Enforced', 'ok', 'shield'), '<span>set by the server operator</span>'], 'The server’s data-flow policy decides which sinks labelled columns may reach. Its rules are not shown to tenants; refusals of acme’s runs are listed here.')}
          ${dhead(dtabs([['Refusals', 3]], 'Refusals'))}${refusals(true)}`);
        return;
      }
      if (state === 'edit' || state === 'preview') {
        const err = state === 'edit';
        M.top(['Govern', 'Policy', 'Edit']);
        const right = `${btn('Discard', { kind: 'quiet' })}${err ? btn('Preview impact', { icon: 'diff', kind: 'primary', cls: 'dis' }) : btn('Back to editor', { icon: 'edit' })}`;
        const top = `${dhead('', right, 'Policy')}${head(err ? 'Edit eu-pii' : 'Review your change to eu-pii', [`<span>draft of version 8 · based on v7</span>`, '<span>needs a second admin’s approval</span>'])}`;
        if (err) {
          M.detail(`${top}${alert('2 problems in the draft.', 'Fix them to preview the impact.', 'bad')}
            <div class="pair">${card({ icon: 'code', title: 'eu-pii.yaml', aside: 'draft', body: `<div class="pad"><div class="area" style="border-color:var(--bad)">${POLICY_BAD}</div></div>` })}
            ${card({ icon: 'alert', title: 'Problems', body: tbl(['Line', 'Problem'], [['8', '<span class="bad">unknown value detector “iban”</span> · one of email, credit_card, ssn, phone, ipv4'], ['13', '<span class="bad"><code>require.residency</code> must be a list</span> · write <code>[eu]</code>']]), note: 'Checked as you type, with the same validation the server runs on submit.' })}</div>`);
          return;
        }
        M.detail(`${top}
          <div class="pair">${card({ icon: 'diff', title: 'What changes', aside: 'v7 → draft', body: diff([['@', 'rules · pii-to-eu-only'], [' ', 'require: { residency: [eu] }'], ['-', 'mask: [hash, tokenize]'], ['+', 'mask: [hash]'], ['@', 'classifications'], ['+', '- label: pii.phone'], ['+', '  value_detector: phone']]) })}
          ${card({ icon: 'lineage', title: 'Impact', aside: 'every registered template and the last 7 days of runs', body: `${grid(3, [kv('Newly refused', '<span class="bad">2 templates</span>', '5 recent runs'), kv('Newly allowed', '0'), kv('Unchanged', '41 templates')])}<div class="pad" style="padding-top:0">${tbl(['Would be refused', 'Rule', 'Why'], [[lnk('acme/leads') + ' <span class="mono">stable v3</span>', '<span class="mono" style="white-space:nowrap">pii-to-eu-only</span>', 'tokenizes <span class="mono">work_email</span> into a us sink'], [lnk('globex/support-tickets') + ' <span class="mono">v9</span>', '<span class="mono" style="white-space:nowrap">no-pii-to-stdout</span>', '<span class="mono">phone</span> (newly labelled) reaches stdout'], [lnk('5 runs in the last 7 days'), '', 'same two templates']])}</div>` })}</div>
          ${card({ title: 'Request the change', body: `<div class="form one">${field('Reason', '<div class="area sans">Tokens are reversible with the vault key; legal asked for hashing only. Adds phone numbers as personal data.</div>', { req: true, help: 'Shown to the approver and kept with the version' })}</div><div class="actions"><span class="muted">Policy changes always need another admin’s approval. Runs already admitted keep the version they were checked against.</span>${btn('Request change', { icon: 'changes', kind: 'primary' })}</div>` })}`);
        return;
      }
      const src = state === 'file' ? SRC.file : SRC.server;
      const tab = { refusals: 'Refusals', dryrun: 'Dry run', 'dryrun-clean': 'Dry run', history: 'History', rollback: 'History' }[state] || 'Rules';
      const banner = src === SRC.file
        ? alert('Managed by the --policy file.', 'This server loaded <code>policies/eu-pii.yaml</code> at start. Change the file and restart to update it; the console shows it read-only.', '')
        : state === 'submitted'
          ? alert('Your change to version 8 waits for approval.', `It takes effect on every replica once another admin approves ${lnk('change cr-0144')}. Version 7 stays enforced until then.`, 'warn', btn('Open in Changes', { icon: 'changes' }))
          : '';
      const body = tab === 'Refusals' ? refusals() : tab === 'Dry run' ? dryRun(state === 'dryrun-clean') : tab === 'History' ? history() : rulesBody();
      M.detail(`${head('Policy', subFor(src))}${banner}${dhead(tabsFor(tab, { history: src === SRC.server }), state === 'submitted' ? btn('Edit policy', { icon: 'edit', kind: 'primary', act: true, cls: 'dis' }) : editBtn(src))}${body}`);
      if (state === 'rollback') {
        M.overlay(dialog('Roll back to version 6?', `<span>This creates a change request. Version 7 stays enforced until another admin approves it.</span>
          ${diff([['-', 'mask: [hash]  (v7)'], ['+', 'mask: [hash, tokenize]  (v6)'], ['-', 'label pii.national_id']])}
          ${field('Reason', '<div class="input err"></div>', { req: true, err: 'A reason is required.' })}`, `${btn('Cancel')}${btn('Request rollback', { kind: 'primary', icon: 'undo' })}`, 'undo'));
      }
    },
  });
})();
