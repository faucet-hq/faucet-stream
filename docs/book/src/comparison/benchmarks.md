# Benchmarks (vs Meltano)

Honest, reproducible evidence for the "built for throughput" claim. Every number
below comes from [`BENCHMARKS.md`](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)
— identical workloads, one machine (Apple M3 Pro, 12 cores, 18 GiB RAM), 1M rows,
seed 42, median of 5 timed runs. faucet-stream is compared against
[Meltano](https://meltano.com/) (the most common [Singer](https://www.singer.io/)
runtime).

> **Read the caveats first.** This measures single-machine *batch* throughput of
> three specific moves. It does **not** measure distributed throughput, connector
> breadth, or correctness. The CSV→JSONL figure is a **best case** (upper bound),
> not the typical case — see [vs. Meltano](./meltano.md) and
> [`BENCHMARKS.md`](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)
> for the full methodology, hardware capture, and the Postgres-row measurement
> caveat.

<style>
.viz-root {
  --surface-1: #fcfcfb;
  --text-primary: #0b0b0b;
  --text-secondary: #52514e;
  --muted: #898781;
  --track: #eceae4;
  --series-faucet: #2a78d6;   /* categorical slot 1 — blue (the hero) */
  --series-meltano: #eb6834;  /* categorical slot 8 — orange */
  --gap-bar: #256abf;         /* derived-ratio bars: one muted blue */
  --hairline: rgba(11,11,11,0.10);
}
html.navy .viz-root, html.coal .viz-root, html.ayu .viz-root {
  --surface-1: #1a1a19;
  --text-primary: #ffffff;
  --text-secondary: #c3c2b7;
  --muted: #898781;
  --track: #2c2c2a;
  --series-faucet: #3987e5;
  --series-meltano: #d95926;
  --gap-bar: #5598e7;
  --hairline: rgba(255,255,255,0.10);
}
.viz-root { margin: 1.25rem 0 1.75rem; }
.viz-hero { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0.5rem 0 1.5rem; }
.viz-tile {
  flex: 1 1 150px; background: var(--surface-1); border: 1px solid var(--hairline);
  border-radius: 10px; padding: 0.85rem 1rem;
}
.viz-tile .num {
  font: 700 1.9rem/1.05 system-ui, -apple-system, "Segoe UI", sans-serif;
  color: var(--series-faucet); letter-spacing: -0.01em;
}
.viz-tile .cap { color: var(--text-secondary); font-size: 0.8rem; margin-top: 0.25rem; }
.viz-title {
  font: 600 0.95rem/1.3 system-ui, sans-serif; color: var(--text-primary);
  margin: 1.1rem 0 0.15rem;
}
.viz-sub { color: var(--muted); font-size: 0.78rem; margin: 0 0 0.7rem; }
.viz-legend { display: flex; gap: 1.1rem; margin: 0.2rem 0 0.9rem; font-size: 0.82rem; color: var(--text-secondary); }
.viz-legend span { display: inline-flex; align-items: center; gap: 0.4rem; }
.viz-legend i { width: 12px; height: 12px; border-radius: 3px; display: inline-block; }
.viz-group { margin: 0.55rem 0 0.9rem; }
.viz-group > .g-label { font-size: 0.8rem; color: var(--text-secondary); margin-bottom: 0.35rem; }
.bar-row { display: flex; align-items: center; gap: 0.6rem; margin: 3px 0; }
.bar-name { flex: 0 0 118px; text-align: right; font-size: 0.78rem; color: var(--text-secondary); }
.bar-track { flex: 1 1 auto; background: var(--track); border-radius: 4px; height: 20px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 4px; min-width: 3px; }
.bar-fill.faucet { background: var(--series-faucet); }
.bar-fill.meltano { background: var(--series-meltano); }
.bar-fill.gap { background: var(--gap-bar); }
.bar-val {
  flex: 0 0 96px; font-size: 0.8rem; color: var(--text-primary);
  font-variant-numeric: tabular-nums;
}
.viz-root details { margin-top: 0.6rem; font-size: 0.85rem; }
.viz-root details summary { cursor: pointer; color: var(--text-secondary); }
@media (max-width: 520px) {
  .bar-name { flex-basis: 84px; }
  .bar-val { flex-basis: 78px; font-size: 0.74rem; }
}
</style>

<div class="viz-root">

<div class="viz-hero">
  <div class="viz-tile"><div class="num">~80×</div><div class="cap">faster on CSV → JSONL (best case, parse-bound)</div></div>
  <div class="viz-tile"><div class="num">~10×</div><div class="cap">faster on a realistic DB → DB move (sink-bound)</div></div>
  <div class="viz-tile"><div class="num">~40×</div><div class="cap">less peak memory (20 vs 819 MiB)</div></div>
  <div class="viz-tile"><div class="num">1:1</div><div class="cap">exact row-count parity, every scenario</div></div>
</div>

<div class="viz-legend">
  <span><i style="background:var(--series-faucet)"></i> faucet-stream</span>
  <span><i style="background:var(--series-meltano)"></i> Meltano (Singer)</span>
</div>

<div class="viz-title">Throughput — rows/second (higher is better)</div>
<div class="viz-sub">1,000,000 rows. Meltano's bar is a sliver on purpose — that is the result.</div>

<div class="viz-group">
  <div class="g-label">A — CSV → JSONL · parse-bound (best case)</div>
  <div class="bar-row"><div class="bar-name">faucet</div><div class="bar-track"><div class="bar-fill faucet" style="width:100%"></div></div><div class="bar-val">700,574</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:1.24%"></div></div><div class="bar-val">8,698</div></div>
</div>

<div class="viz-group">
  <div class="g-label">B — Postgres → JSONL · typed row decode</div>
  <div class="bar-row"><div class="bar-name">faucet</div><div class="bar-track"><div class="bar-fill faucet" style="width:27.3%"></div></div><div class="bar-val">191,124</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:1.13%"></div></div><div class="bar-val">7,931</div></div>
</div>

<div class="viz-group">
  <div class="g-label">C — Postgres → Postgres · sink-bound (the realistic move)</div>
  <div class="bar-row"><div class="bar-name">faucet (COPY)</div><div class="bar-track"><div class="bar-fill faucet" style="width:16.7%"></div></div><div class="bar-val">117,147</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:1.74%"></div></div><div class="bar-val">12,186</div></div>
</div>

<div class="viz-title">Peak memory — MiB (lower is better)</div>
<div class="viz-sub">Here faucet is the sliver: bounded-memory streaming holds flat while Meltano buffers.</div>

<div class="viz-group">
  <div class="g-label">A — CSV → JSONL</div>
  <div class="bar-row"><div class="bar-name">faucet</div><div class="bar-track"><div class="bar-fill faucet" style="width:2.45%"></div></div><div class="bar-val">20.1</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:100%"></div></div><div class="bar-val">818.8</div></div>
</div>

<div class="viz-group">
  <div class="g-label">B — Postgres → JSONL</div>
  <div class="bar-row"><div class="bar-name">faucet</div><div class="bar-track"><div class="bar-fill faucet" style="width:2.65%"></div></div><div class="bar-val">21.7</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:75.8%"></div></div><div class="bar-val">621.0</div></div>
</div>

<div class="viz-group">
  <div class="g-label">C — Postgres → Postgres</div>
  <div class="bar-row"><div class="bar-name">faucet</div><div class="bar-track"><div class="bar-fill faucet" style="width:4.13%"></div></div><div class="bar-val">33.8</div></div>
  <div class="bar-row"><div class="bar-name">Meltano</div><div class="bar-track"><div class="bar-fill meltano" style="width:84.3%"></div></div><div class="bar-val">690.3</div></div>
</div>

<div class="viz-title">The gap collapses as the workload gets more I/O-bound</div>
<div class="viz-sub">Speed-up multiplier (faucet ÷ Meltano). The best case is not the typical case.</div>

<div class="viz-group">
  <div class="bar-row"><div class="bar-name">A · parse-bound</div><div class="bar-track"><div class="bar-fill gap" style="width:100%"></div></div><div class="bar-val">~80×</div></div>
  <div class="bar-row"><div class="bar-name">B · row decode</div><div class="bar-track"><div class="bar-fill gap" style="width:30%"></div></div><div class="bar-val">~24×</div></div>
  <div class="bar-row"><div class="bar-name">C · sink-bound</div><div class="bar-track"><div class="bar-fill gap" style="width:12.5%"></div></div><div class="bar-val">~10×</div></div>
</div>

<details>
<summary>Show the raw numbers as a table</summary>

| Scenario | Bottleneck | faucet (rows/s) | Meltano (rows/s) | Gap | faucet RSS (MiB) | Meltano RSS (MiB) |
|---|---|--:|--:|--:|--:|--:|
| A — CSV → JSONL | parse/serialize (best case) | 700,574 | 8,698 | ~80× | 20.1 | 818.8 |
| B — Postgres → JSONL | typed row decode | 191,124 | 7,931 | ~24× | 21.7 | 621.0 |
| C — Postgres → Postgres | destination write (sink-bound) | 117,147 (COPY) | 12,186 | ~10× | 33.8 | 690.3 |

All runs: 1,000,000 rows, seed 42, exact row-count parity (1,000,000 = 1,000,000). Measured 2026-10-16 against Meltano 4.4.1; earlier runs are in the History section of [`BENCHMARKS.md`](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md#history).

</details>

</div>

## How to reproduce

The harness never fabricates a number — a tool that won't install/run is recorded
as such, not faked. Regenerate everything on your own hardware:

```bash
make bench            # Scenario A (CSV → JSONL, 1M rows) — no infra
make bench-smoke      # 100k-row smoke run
make bench-postgres   # adds Scenarios B & C (needs Docker)
```

Results land in `benchmarks/results/`. One independent confirmation on your own
hardware is worth more to this project than a new connector — open an issue or PR
with your output, **especially if faucet does not win.** See
[`benchmarks/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/benchmarks/README.md)
and [Performance tuning](../operations/tuning.md) for the levers behind these
numbers.
