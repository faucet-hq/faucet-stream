// Changes — split pane: change requests that wait for approval, and what they would do.
(function () {
  const LIST = {
    pending: [
      ['cr142', ring('warn', 'hollow'), 'Overwrite finance.ledger nightly', '3h', '<b>run</b> · write_mode overwrite · 1 of 2 approvals · by analyst-ana'],
      ['cr141', ring('warn', 'hollow'), 'Launch acme/billing-api v8', '1d', '<b>template launch</b> · tests passed · 0 of 1 · by ops-raj'],
    ],
    approved: [['cr139', ring('info', 'half'), 'Backfill payments 2026-10', '6m', '<b>executing</b> · approved by ops-admin · 30 units']],
    decided: [
      ['cr138', ring('ok'), 'Register faucet-hq/bigquery v4', '2d', '<b>executed</b> · approved by ops-admin'],
      ['cr137', ring('bad'), 'Run ad-hoc PII export to s3', '4d', '<b>rejected</b> · policy eu-pii denies sink'],
      ['cr136', ring('bad'), 'Upsert crm.contacts from billing', '5d', '<b>failed</b> · re-plan changed the material'],
      ['cr135', ring('ok'), 'Backfill payments 2026-09', '5d', '<b>executed</b> · 30 units'],
      ['cr134', ring('', 'hollow'), 'Deprecate octo/scratch-sqlite', '9d', '<b>expired</b> · nobody approved in 72h'],
    ],
  };
  const listPane = (sel, { tab = 'Needs me', empty: none = false } = {}) => {
    const r = ([id, ...a]) => row(...a.slice(0, 4), { on: id === sel });
    const body = none
      ? `${sect('Pending', 0)}${empty('Nothing needs you', 'Every pending change already has your vote, or you requested it yourself. <span class="lnk">Show all changes</span>', { icon: 'check' })}${sect('Recently decided', 22)}${LIST.decided.slice(0, 3).map(r).join('')}`
      : `${sect('Pending', 2)}${LIST.pending.map(r).join('')}${sect('Approved · executing', 1)}${LIST.approved.map(r).join('')}${sect('Recently decided', 22)}${LIST.decided.map(r).join('')}`;
    return `${tabs([['Needs me', none ? 0 : 2], 'All'], tab, ibtn('filter') + ibtn('refresh'))}
      ${stats([['Pending', none ? '0' : '2', 'warn'], ['Executed', '18', 'ok'], ['Rejected', '3'], ['Failed', '2', 'bad']])}
      ${body}`;
  };
  const crumbs = (t) => (t ? ['Operate', 'Changes', t] : ['Operate', 'Changes']);
  const subPending = [pill('Pending · 1 of 2 approvals', 'warn'), `<span>requested by ${lnk('analyst-ana')} · operator</span>`, '<span>3h ago</span>', '<span>expires in 21h</span>'];
  const tabsFor = (on) => dtabs(['Plan', 'Impact', 'History'], on);
  const actions = `${btn('Reject', { kind: 'danger', icon: 'x', act: true })}${btn('Approve and run', { kind: 'primary', icon: 'check', act: true })}`;

  const planCard = card({
    icon: 'changes', title: 'What changes if this runs', tag: 're-planned on approve', right: `material fingerprint <span class="mono">9b1f…</span>${cpi()}`,
    body: diff([[' ', 'row ledger · sink postgres · finance.ledger'], ['-', 'write_mode: upsert   key: [entry_id]'], ['+', 'write_mode: overwrite'], ['+', 'budget.max_records: 2000000'], [' ', 'transforms · quality · contract v5 · masking — unchanged']]),
    note: `<span class="warn">Breaking for 2 downstream datasets</span> · ${lnk('bi.ledger_daily')} and ${lnk('ml.cashflow_features')} read <code>entry_id</code> history that an overwrite replaces. <span class="lnk">See impact</span>`,
  });
  const quote = `<div class="card"><div class="pad" style="color:var(--text-2)"><small class="muted" style="display:block;margin-bottom:4px">Reason from the requester</small>Finance wants a clean nightly reload after the ledger re-key; staging was verified against last month's close.</div></div>`;
  const approvals = card({
    title: 'Approvals', right: 'rule finance-writes · 2 admins',
    body: `<div class="vote" style="border-top:0"><span class="avatar">OA</span>${lnk('ops-admin')}<span class="ok">${ic('check')}</span><span class="r">1h ago · “staging verified”</span></div>
      <div class="vote"><span class="avatar vacant"></span><span class="muted">waiting for a second admin</span><span class="r">self-approval off for the requester</span></div>`,
  });
  const budgetCard = card({ title: 'Budget attached', right: 'stricter wins', body: grid(2, [kv('Max records', '2,000,000'), kv('Max duration', '45m'), kv('Allowed sinks', '<span class="mono">postgres</span>'), kv('Max bytes', '—')]) });
  const planBody = () => `${planCard}${quote}<div class="two">${approvals}${budgetCard}</div>`;
  const detailHead = (on, pill0 = subPending, right = actions, title = 'Overwrite finance.ledger nightly') =>
    `${dhead(tabsFor(on), right, 'Changes')}${head(title, pill0)}`;

  const impactBody = () => `${card({
    icon: 'lineage', title: '2 downstream datasets affected', tag: 'breaking', right: 'planned from the plan sample · depth 2',
    body: tbl(['Dataset', 'Depth', 'Severity', 'Columns read', 'Owners'], [
      [lnk('bi.ledger_daily'), '1', `<span class="bad">● breaking</span>`, '<span class="mono">entry_id</span> · <span class="mono">amount</span>', lnk('team-bi')],
      [lnk('ml.cashflow_features'), '2', `<span class="bad">● breaking</span>`, '<span class="mono">entry_id</span>', lnk('team-ml')],
      [lnk('finance.ledger_archive'), '1', `<span class="muted">● unknown</span>`, '<span class="muted">no column lineage recorded</span>', '<span class="muted">—</span>'],
    ]),
    note: 'Column delta: 0 added · 0 removed · history replaced. Owners are notified when the change is approved.',
  })}
  ${card({ icon: 'policy', title: 'Policy check', right: 'policy eu-pii v3', body: tbl(['Rule', 'Column', 'Label', 'Sink', 'Result'], [['pii-to-eu-only', '<span class="mono">customer_email</span>', 'pii.email', 'postgres · finance', '<span class="ok">● allowed</span> · masked by hash'], ['no-raw-cards', '<span class="mono">card_last4</span>', 'pii.card', 'postgres · finance', '<span class="ok">● allowed</span> · partial mask']]), note: '<span class="ok">No violations.</span> A violation here would block approval until the plan changes.' })}`;

  const historyBody = () => card({
    title: 'Timeline', right: `change <span class="mono">cr-0142</span>${cpi()}`,
    body: `<div class="log" style="font-family:var(--sans);font-size:12.5px;line-height:2">
      <div><span class="muted" style="display:inline-block;width:110px">today 07:12</span><span class="dot muted"></span>Requested by ${lnk('analyst-ana')} · run · plan fingerprint <span class="mono">9b1f…</span></div>
      <div><span class="muted" style="display:inline-block;width:110px">today 09:40</span><span class="dot ok"></span>Approved by ${lnk('ops-admin')} · “staging verified”</div>
      <div><span class="muted" style="display:inline-block;width:110px">pending</span><span class="dot warn"></span>Waiting for a second admin approval</div>
      <div><span class="muted" style="display:inline-block;width:110px">tomorrow 07:12</span><span class="dot muted"></span><span class="muted">Expires if not approved (72h policy)</span></div></div>`,
  });

  M.view('changes', {
    label: 'Changes', nav: 'changes', icon: 'changes', summary: 'proposed runs and template actions waiting for approval',
    states: {
      needs: { label: 'Needs me · nothing selected', sel: false, covers: ['Needs me tab', 'stat strip', 'sections', 'row summary'], new: ['keyboard j/k in the list'] },
      plan: { label: 'Pending change · plan', covers: ['plan diff', 'fingerprint', 'verdict footnote', 'approvals', 'budget', 'requester reason', 'approve/reject'], new: ['dataset cross-links', 'copy fingerprint'] },
      impact: { label: 'Pending change · impact', covers: ['impact tab', 'affected datasets', 'policy violations'], new: ['owners link to their datasets'] },
      history: { label: 'Pending change · history', covers: ['history timeline', 'expiry'] },
      approve: { label: 'Approve dialog (comment)', covers: ['approve with comment', 'quorum label'] },
      reject: { label: 'Reject dialog · reason required', covers: ['reject reason required'] },
      executed: { label: 'Executed · links its run', covers: ['outcome alert with run link'], new: ['run + dataset cross-links'] },
      failed: { label: 'Failed · invalidated by re-plan', covers: ['error alert', 'invalidated'] },
      none: { label: 'Nothing needs you (empty)', sel: false, covers: ['empty “nothing needs your approval”'] },
      viewer: { label: 'Viewer · read only', params: { role: 'viewer' }, covers: ['permission gate change_approve'] },
      toast: { label: 'After approving · toast', covers: ['toast result', 'badge refresh'] },
    },
    render(state) {
      const right = { right: chip('expires in 21h') };
      if (state === 'needs' || state === 'none') {
        M.top(crumbs(), { delta: state === 'needs' ? '<span class="warn">2 wait for you</span>' : '' });
        M.list(listPane('', { none: state === 'none' }));
        M.detail(state === 'none'
          ? empty('No change requests wait for you', 'When someone proposes a run or a template launch under <code>--require-approval</code>, it appears here for a vote. Propose one from Submit with “Request approval”.', { icon: 'changes', btns: btn('Open Submit', { icon: 'submit' }) + btn('Show all changes') })
          : `${head('2 changes wait for your vote', ['<span>oldest 1d ago</span>', '<span>approval rules from the auth config</span>'])}
            ${card({ title: 'Waiting for you', right: 'oldest first', body: tbl(['Change', 'Kind', 'Approvals', 'Requested', ['Expires', 'num']], [[lnk('Launch acme/billing-api v8'), 'template launch', '0 of 1', `${lnk('ops-raj')} · 1d ago`, '47h'], [lnk('Overwrite finance.ledger nightly'), 'run', '1 of 2', `${lnk('analyst-ana')} · 3h ago`, '21h']]) })}
            <div class="muted" style="font-size:12px">Pick a change to see its plan. <kbd>j</kbd> <kbd>k</kbd> move · <kbd>↵</kbd> open</div>`);
        return;
      }
      if (state === 'executed' || state === 'toast') {
        M.top(crumbs('Backfill payments 2026-09'));
        M.list(listPane('cr135', { tab: 'All' }));
        M.detail(`${dhead(tabsFor('Plan'), '', 'Changes')}${head('Backfill payments 2026-09', [pill('Executed', 'ok', 'check'), `<span>requested by ${lnk('ops-raj')}</span>`, '<span>approved 5d ago by ops-admin</span>'])}
          ${alert('Started run', `${lnk('payments_events_backfill', true)} · 30 units · ${lnk('all 30 unit runs')}. It wrote ${lnk('finance.payments')}.`, 'ok')}
          ${card({ icon: 'changes', title: 'What it ran', tag: 'material matched on approve', right: 'fingerprint <span class="mono">4c02…</span>', body: diff([[' ', 'backfill · pipeline payments_events'], ['+', 'window: 2026-09-01 → 2026-10-01 · unit 1d · 30 units'], ['+', 'into: sink postgres · finance.payments'], [' ', 'delivery at-least-once · state namespaced']]), note: '<span class="ok">Additive only</span> · no schema change downstream.' })}
          ${approvals.replace('waiting for a second admin', 'second approval by finance-lead').replace('<span class="avatar vacant"></span><span class="muted">', '<span class="avatar">FL</span><span>').replace('self-approval off for the requester', '5d ago')}`);
        if (state === 'toast') M.overlay(toast(`${ic('check')}<span>Approved · run <span class="mono">payments_events_backfill</span> started · <span class="lnk">open run</span></span>`, 'done'));
        return;
      }
      if (state === 'failed') {
        M.top(crumbs('Upsert crm.contacts from billing'));
        M.list(listPane('cr136', { tab: 'All' }));
        M.detail(`${dhead(tabsFor('Plan'), '', 'Changes')}${head('Upsert crm.contacts from billing', [pill('Failed', 'bad', 'x'), `<span>requested by ${lnk('analyst-ana')}</span>`, '<span>5d ago</span>'])}
          ${alert('Approval invalidated.', 'The plan was re-built on approve and its material no longer matched what was reviewed: the sink key changed from <code>contact_id</code> to <code>email</code>. Nothing ran; propose it again to review the new plan.', 'bad', btn('Propose again', { icon: 'refresh', act: true }))}
          ${card({ icon: 'changes', title: 'Reviewed plan vs re-plan', right: 'fingerprints <span class="mono">71aa…</span> → <span class="mono">e09c…</span>', body: diff([[' ', 'row contacts · sink postgres · crm.contacts'], ['-', 'key: [contact_id]'], ['+', 'key: [email]'], [' ', 'write_mode: upsert — unchanged']]) })}
          ${historyBody().replace('Waiting for a second admin approval', 'Approved by ops-admin · re-plan differs').replace('dot warn', 'dot bad')}`);
        return;
      }
      M.top(crumbs('Overwrite finance.ledger nightly'), { delta: '<span class="warn">awaiting 1 approval</span>', ...right });
      M.list(listPane('cr142'));
      const tab = { impact: 'Impact', history: 'History' }[state] || 'Plan';
      const body = state === 'impact' ? impactBody() : state === 'history' ? historyBody() : planBody();
      M.detail(`${detailHead(tab)}${body}`);
      if (state === 'approve') {
        M.overlay(dialog('Approve this change?', `<span>Yours is the <b>second</b> approval, so the run starts right away after a fresh re-plan. If the material differs from what you reviewed, it stops instead.</span>
          ${field('Comment', '<div class="area sans">Checked the impact with team-bi; they re-snapshot after the run.</div>', { help: 'Optional · shown in the history and the audit log' })}`,
          `${btn('Cancel')}${btn('Approve and run', { kind: 'primary', icon: 'check' })}`, 'check'));
      }
      if (state === 'reject') {
        M.overlay(dialog('Reject this change?', `<span>The requester sees your reason. They can propose it again.</span>
          ${field('Reason', '<div class="input err"></div>', { req: true, err: 'A reason is required to reject.' })}`,
          `${btn('Cancel')}${btn('Reject', { kind: 'danger', icon: 'x' })}`, 'x'));
      }
    },
  });
})();
