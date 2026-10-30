// Schedules — designed "soon" page: the server has no schedules API yet.
// The slot is ready: same canvas shape, an honest explanation, what to use today.
(function () {
  const planned = (icon, title, text) => row(`<span class="ri">${ic(icon)}</span>`, title, '', text);
  M.view('schedules', {
    label: 'Schedules', nav: 'schedules', icon: 'sched', shape: 'canvas', summary: 'soon — cron schedules across the cluster',
    states: {
      soon: { label: 'Soon · what will live here', covers: ['nav slot reserved'], new: ['designed placeholder instead of a dead link'] },
    },
    render() {
      M.top(['Operate', 'Schedules']);
      M.detail(`
        ${head('Schedules', [tag('soon'), '<span>the server has no schedules API yet</span>'], 'This page will list every cron schedule the server runs, its next and last tick, and what each tick started. Until then, schedules run either as a separate process or as a schedule trigger on this server.')}
        <div class="two">
          ${card({ icon: 'sched', title: 'What will live here', body: `<div class="st-ul">${planned('clock', 'Next and last tick per schedule', 'in the display timezone, with overlap policy and lateness')}${planned('runs', 'Runs each tick started', 'linked to the Runs page')}${planned('pause', 'Pause and resume', 'with an audit entry')}${planned('alert', 'Missed ticks', 'from faucet_schedule_* metrics')}</div>` })}
          ${card({ icon: 'code', title: 'Use it today', body: `<div class="pad stack" style="gap:10px"><span>Run one pipeline on a cron in its own process:</span>${code(`schedule:
  cron: "*/15 * * * *"
  timezone: Europe/Berlin
  overlap: skip`, {})}${code('faucet schedule pipeline.yaml', { lang: 'plain' })}<span>Or let this server tick it: add a <code>schedule</code> trigger, then find it on ${lnk('Triggers')}.</span></div>` })}
        </div>`);
    },
  });
})();
