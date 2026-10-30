// Usage — full-width canvas report: what runs moved and what it is estimated to cost.
(function () {
  const filters = (by = 'Pipeline', extra = '') => `<div class="card"><div class="pad" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
    <span class="muted">Group by</span><span class="seg">${['Pipeline', 'Row', 'Dataset', 'Sink', 'Day', 'Tenant'].map((x) => `<span class="${x === by ? 'on' : ''}">${x}</span>`).join('')}</span>
    ${input(extra ? 'orders_cdc_mirror' : 'Pipeline contains…', { ph: !extra, icon: 'search' }).replace('class="input', 'style="flex:1;min-width:180px" class="input')}
    ${input('1 Oct 2026', {})}<span class="muted">→</span>${input('30 Oct 2026', {})}
    ${ibtn('refresh')}${ibtn('download')}</div>${extra}</div>`;
  const statsRow = stats([['Invocations', '1,284'], ['Rows written', '412.8M'], ['Bytes written', '1.21 TiB'], ['Estimated cost', '$1,846.20'], ['Hosted equivalent', '$6,192.00']]);
  const nr = '<span class="tag" title="the connector reports no compute signal; never priced as zero">compute not reported</span>';
  const rows = [
    ['orders_cdc_mirror', '412', '1', '214.6M', '214.1M', '612.0 GiB', '21h 04m', '18,240', '$842.10', '$3,211.50', ''],
    ['clickstream_kafka', '96', '0', '120.2M', '118.9M', '402.5 GiB', '9h 12m', '—', '$611.40', '$1,783.50', ''],
    ['payments_events_backfill', '30', '2', '31.0M', '30.4M', '88.1 GiB', '3h 41m', '6,120', '$214.90', '$456.00', ''],
    ['invoices_s3_to_duckdb', '720', '3', '40.1M', '40.1M', '96.2 GiB', '6h 30m', '21,600', '$96.60', '$601.50', nr],
    ['crm_contacts_upsert', '26', '7', '9.6M', '9.3M', '14.0 GiB', '1h 02m', '3,412', '$81.20', '$139.50', ''],
  ];
  const table = (sorted = 'Est. cost', sel = -1) => {
    const cols = [['Pipeline', ''], ['Runs', 'num'], ['Rows in', 'num'], ['Rows out', 'num'], ['Bytes out', 'num'], ['Duration', 'num'], ['Requests', 'num'], ['Est. cost', 'num'], ['Hosted eq.', 'num']];
    const head = `<tr>${cols.map(([l, c]) => `<th class="${c} ${l === sorted ? 'sorted' : ''}">${l}${l === sorted ? ' ↓' : ''}</th>`).join('')}</tr>`;
    const body = rows.map((r, i) => `<tr class="${i === sel ? 'sel' : ''}"><td>${lnk(r[0])} ${r[10]}</td><td class="num">${r[1]}${r[2] !== '0' ? ` <span class="bad">· ${r[2]} failed</span>` : ''}</td><td class="num">${r[3]}</td><td class="num">${r[4]}</td><td class="num">${r[5]}</td><td class="num">${r[6]}</td><td class="num">${r[7]}</td><td class="num">${r[8]}</td><td class="num">${r[9]}</td></tr>`).join('');
    const foot = `<tfoot><tr><td>Total · 5 pipelines</td><td class="num">1,284</td><td class="num">415.5M</td><td class="num">412.8M</td><td class="num">1.21 TiB</td><td class="num">41h 29m</td><td class="num">49,372</td><td class="num">$1,846.20</td><td class="num">$6,192.00</td></tr></tfoot>`;
    return `<table>${head}${body}${foot}</table>`;
  };
  const note = 'Estimates use each run’s <code>usage.pricing</code> table (public list prices by default). <b>Hosted eq.</b> is what a per-row-priced hosted ELT service would charge for the same rows. A connector that reports no compute signal is marked, never priced as zero.';

  M.view('usage', {
    label: 'Usage', nav: 'usage', icon: 'usage', summary: 'what runs moved and what it is estimated to cost', shape: 'canvas',
    states: {
      report: { label: 'Report by pipeline', covers: ['group by (pipeline/row/dataset/sink/day/tenant)', 'filters in the URL', 'stat strip', 'grouped table + totals', 'compute not reported', 'explanatory note'], new: ['sortable columns', 'rows link to their runs', 'CSV download'] },
      drill: { label: 'Drill-down · one pipeline', covers: ['per-group detail'], new: ['per-day trend', 'cost lines', 'runs of this pipeline'] },
      day: { label: 'Grouped by day', covers: ['by=day'], new: ['daily trend bars'] },
      loading: { label: 'Loading', covers: ['skeleton'] },
      none: { label: 'Empty window', covers: ['empty “no usage recorded”'] },
    },
    render(state) {
      M.top(['Govern', 'Usage'], { right: chip('1 Oct → 30 Oct 2026') });
      const headHtml = head('Usage', ['<span>every figure is an estimate</span>', '<span>pricing: list prices, USD</span>']);
      if (state === 'loading') {
        M.detail(`${headHtml}${filters()}<div class="stats" style="--n:5">${Array.from({ length: 5 }, () => '<div><span class="skel w60"></span><span class="skel w30" style="margin-top:8px;height:16px"></span></div>').join('')}</div>${skelCards(2)}`);
        return;
      }
      if (state === 'none') {
        M.detail(`${headHtml}${filters('Pipeline')}${card({ body: empty('No usage recorded for this window', 'Nothing ran between 1 Oct and 30 Oct, or this server keeps no usage history. Widen the dates or run a pipeline first.', { icon: 'usage', btns: btn('Last 90 days') + btn('Open Runs', { icon: 'runs' }) }) })}`);
        return;
      }
      if (state === 'day') {
        const days = [62, 58, 71, 66, 69, 40, 38, 64, 73, 70, 68, 75, 44, 41, 72, 78, 74, 70, 79, 47, 45, 76, 80, 77, 82, 85, 51, 49, 83, 88];
        const bars = `<svg width="100%" height="120" viewBox="0 0 900 120" preserveAspectRatio="none" style="display:block">${days.map((d, i) => `<rect x="${i * 30 + 4}" y="${110 - d}" width="22" height="${d}" rx="2" fill="${i === 29 ? 'var(--text-2)' : 'var(--line-strong)'}"/>`).join('')}</svg>`;
        M.detail(`${headHtml}${filters('Day')}${statsRow}
          ${card({ icon: 'usage', title: 'Rows written per day', right: 'peak 18.9M on 30 Oct', body: `<div class="pad">${bars}<div style="display:flex;justify-content:space-between;color:var(--muted);font-size:12px;margin-top:6px"><span>1 Oct</span><span>15 Oct</span><span>30 Oct</span></div></div>` })}
          ${card({ title: 'By day', right: '30 days', body: tbl([['Day', ''], ['Runs', 'num'], ['Rows out', 'num'], ['Bytes out', 'num'], ['Est. cost', 'num'], ['Hosted eq.', 'num']], [['30 Oct 2026', '48', '18.9M', '55.2 GiB', '$84.10', '$283.50'], ['29 Oct 2026', '44', '17.6M', '51.0 GiB', '$79.40', '$264.00'], ['28 Oct 2026', '43', '16.9M', '49.8 GiB', '$76.20', '$253.50'], ['27 Oct 2026', '22', '9.4M', '27.3 GiB', '$40.10', '$141.00']].map((r) => [lnk(r[0]), ...r.slice(1)])), note })}`);
        return;
      }
      if (state === 'drill') {
        M.detail(`${headHtml}${filters('Pipeline', `<div class="note" style="display:flex;gap:6px;align-items:center">Showing ${chip('pipeline: orders_cdc_mirror', 'x')} <span class="lnk" style="margin-left:6px">clear</span></div>`)}
          <div class="two">${card({ icon: 'runs', title: 'orders_cdc_mirror', tag: 'postgres-cdc → bigquery', right: lnk('412 runs'),
            body: grid(2, [kv('Rows written', '214.1M', spark([5, 6, 6, 7, 8, 7, 8, 9, 8, 9, 10, 9, 11], 'var(--text-2)', 170, 30)), kv('Estimated cost', '$842.10', spark([28, 30, 29, 31, 33, 30, 32, 34, 31, 33, 36, 35, 37], 'var(--text-2)', 170, 30)), kv('Bytes streamed', '612.0 GiB', 'bigquery reported'), kv('Failed runs', '1', '<span class="lnk">open it</span>')]) })}
            ${card({ title: 'Cost lines', right: 'from the pricing table', body: tbl([['Item', ''], ['Quantity', 'num'], ['Rate', 'num'], ['Amount', 'num']], [['BigQuery streaming', '612.0 GiB', '$0.05 / GiB', '$30.60'], ['BigQuery bytes billed', '129.8 TiB', '$6.25 / TiB', '$811.50'], ['Egress', '0 GB', '$0.00', '$0.00']], ['Total', '', '', '$842.10']) })}</div>
          ${card({ title: 'All pipelines', right: 'sorted by estimated cost', body: table('Est. cost', 0), note })}`);
        return;
      }
      M.detail(`${headHtml}${filters()}${statsRow}${card({ icon: 'usage', title: 'By pipeline', aside: '5 groups · 1 Oct → 30 Oct 2026', right: 'click a column to sort', body: table('Est. cost'), note })}`);
    },
  });
})();
