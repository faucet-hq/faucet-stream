// Policy — designed "soon" page: data-flow policies are enforced today, but the
// server exposes no endpoint to read the loaded policy.
(function () {
  const planned = (icon, title, text) => row(`<span class="ri">${ic(icon)}</span>`, title, '', text);
  M.view('policy', {
    label: 'Policy', nav: 'policy', icon: 'policy', shape: 'canvas', summary: 'soon — the data-flow policy the server enforces',
    states: {
      soon: { label: 'Soon · what will live here', covers: ['nav slot reserved'], new: ['designed placeholder instead of a dead link'] },
    },
    render() {
      M.top(['Govern', 'Policy']);
      M.detail(`
        ${head('Policy', [tag('soon'), '<span>enforced today, not yet browsable here</span>'], 'The server already refuses runs that send labelled columns to a sink the policy forbids. This page will show the loaded policy, which rules matched recent runs, and the refusals they caused.')}
        <div class="two">
          ${card({ icon: 'policy', title: 'What will live here', body: `<div class="st-ul">${planned('layers', 'Classifications', 'which columns carry each label, by name or value detector')}${planned('shield', 'Rules', 'which sinks a label may reach: require, mask or deny')}${planned('alert', 'Refusals', 'runs and change requests the policy stopped, with the rule')}${planned('diff', 'Dry run a config', 'check a pipeline against the policy before submitting')}</div>` })}
          ${card({ icon: 'code', title: 'Use it today', body: `<div class="pad stack" style="gap:10px"><span>Check a pipeline against a policy from the CLI:</span>${code('faucet policy pipeline.yaml --policy eu-pii.yaml', { lang: 'plain' })}<span>Refusals on this server appear in the ${lnk('Audit log')} as <code>policy.denied</code>, and Submit’s ${lnk('Plan')} shows violations before a run starts.</span></div>` })}
        </div>`);
    },
  });
})();
