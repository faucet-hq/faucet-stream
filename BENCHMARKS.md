# Benchmarks — faucet-stream vs Meltano (Singer)

Honest, reproducible evidence for the "built for throughput" claim. This compares
`faucet-stream` against [Meltano](https://meltano.com/) (the most common
[Singer](https://www.singer.io/) runtime) on **identical** workloads, on one
machine.

**Headline: on single-machine batch throughput, faucet-stream is roughly
10–80× faster than a Python Singer runtime** — and the size of the gap depends
heavily on how much of the run is per-row work vs I/O:

- **CSV → JSONL is a *best case*** that maximally exposes Python's per-row
  interpreter + Singer-over-pipe overhead (faucet was ~80× faster here). Quote
  this as the upper bound, not the typical case.
- **Sink-bound moves narrow the gap.** When the destination write dominates
  (Postgres → Postgres, Scenario C), both tools are bounded by the *same* database
  write path, so the ratio shrinks toward the low end of that range. Network- or
  API-bound moves narrow it further.

**Regenerate everything** with `make bench` (CSV, 1M rows), `make bench-smoke`
(100k), or `make bench-postgres` (adds the Docker Postgres scenarios B & C). The
harness lives in [`benchmarks/`](benchmarks/README.md) and never fabricates a
number — a tool that won't install/run is recorded as such, not faked.

> **Read the caveats section before quoting any number.** This measures
> single-machine batch throughput of specific moves. It does **not** measure
> distributed throughput, connector breadth, or correctness.
>
> **Reproduce and report.** One independent confirmation of these numbers on your
> own hardware is worth more to this project than a new connector — run
> `make bench` (and `make bench-postgres` if you have Docker) and open an issue or
> PR with your `benchmarks/results/` output, especially if faucet does *not* win.

## Methodology

### Dataset (deterministic)

`scripts/gen_bench_data.py` emits a seeded CSV (`--seed 42`), so both tools consume
byte-identical input and re-runs are comparable. Columns exercise mixed types — an
`int` id, two strings, a float, an RFC3339 timestamp, a bool, and a small nested
JSON column (`attributes`):

```
id,first_name,country,amount,created_at,active,attributes
1,Eve,GB,6767.32,2020-01-01T00:01:22Z,false,"{""tier"":""free"",""score"":2.501,""tags"":[""beta"",""epsilon""]}"
```

Sizes: **1,000,000 rows** (primary) and **100,000 rows** (smoke). On-disk sizes are
recorded per run in `benchmarks/results/versions.txt` (the 100k CSV is ~11.5 MiB;
the 1M CSV is ~10× that).

### Scenarios

| Scenario | Bottleneck | faucet | Meltano |
|---|---|---|---|
| **A — CSV → JSONL** (no infra, always run) | parse/serialize (best case) | `source-file` → `sink-file` (recorded numbers: `source-csv` → `sink-jsonl`) | `tap-csv` → `target-jsonl` |
| **B — Postgres → JSONL** (needs Docker) | typed row decode | `source-postgres` → `sink-file` (recorded numbers: `sink-jsonl`) | `tap-postgres` → `target-jsonl` |
| **C — Postgres → Postgres** (needs Docker) | **destination write (sink-bound)** | `source-postgres` → `sink-postgres` | `tap-postgres` → `target-postgres` |

### Measurement

- **Wall-clock**: [`hyperfine`](https://github.com/sharkdp/hyperfine), 1 warmup run
  discarded + **5 timed runs**, median ± stddev.
- **Peak RSS**: `/usr/bin/time` (`-l` on macOS, `-v` on Linux), one sampled run.
- **Throughput**: `rows ÷ median wall-clock`.
- **Parity**: both tools must emit the same row count; record shapes are
  spot-checked (see caveats re: Singer metadata).

### Exact reproduction

```bash
# 1. Build the binary the harness uses (`release-lto`: fat LTO, one codegen
#    unit, as in the core / analytics / cdc images)
cargo build -p faucet-cli --profile release-lto \
  --no-default-features --features "source-file,sink-file,file-format-csv,source-postgres,sink-postgres"

# 2. Run (installs an isolated Meltano venv on first run)
scripts/run-bench.sh            # 1,000,000 rows, 5 runs (Scenario A)
scripts/run-bench.sh --smoke    # 100,000 rows
scripts/run-bench.sh --postgres # also Scenarios B & C (needs Docker)

# Results land in benchmarks/results/{versions.txt,results.md,raw/*.json}
```

Tool versions and hardware are captured per run in
`benchmarks/results/versions.txt`, including the full package list of Meltano and
of every plugin environment. Meltano itself is pinned in the harness
(`BENCH_MELTANO_VERSION`, default `4.4.1`). The run reported below used:

| | |
|---|---|
| Hardware | Apple M3 Pro, 12 cores, 18 GiB RAM |
| OS | macOS 26.7.1 (arm64) |
| Rust | rustc 1.96.0 |
| faucet | 1.14.0 (release: `source-file,sink-file,file-format-csv,source-postgres,sink-postgres`) |
| hyperfine | 1.20.0 |
| Meltano | 4.4.1 (Python 3.12.14) |
| Meltano plugins | `tap-csv` v1.3.2, `tap-postgres` 0.9.0, `target-jsonl` 0.1.4, `target-postgres` 0.8.0 (`singer-sdk` 0.53.7–0.54.8, SQLAlchemy 2.0–2.1) |
| Date | 2026-10-16 |

The harness now defaults to the newest Python Meltano supports (3.14 at the
time of writing); this run used 3.12.14. A 100k-row check of all three scenarios
on Python 3.14.7 gave the same faucet-to-Meltano ratios within noise (Scenario A:
73× on both).

The host was not idle: managed endpoint-security agents ran throughout (load
average ~5–7), which slows both tools. Absolute times on a quiet machine will be
lower; the ratios are the useful part.

## Results

### Scenario A — CSV → JSONL

**Primary run — 1,000,000 rows** (116 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **1.43** | 0.052 | **700,574** | **20.1** | 1,000,000 |
| Meltano (Singer) | 114.97 | 4.51 | 8,698 | 818.8 | 1,000,000 |

On this workload and hardware, faucet-stream was **~80× faster** (701k vs 8.7k
rows/s) and used **~40× less peak memory** (20.1 vs 819 MiB), with **exact
row-count parity** (1,000,000 = 1,000,000).

**Smoke run — 100,000 rows** (11.5 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **0.18** | 0.010 | **552,792** | **19.5** | 100,000 |
| Meltano (Singer) | 13.20 | 0.19 | 7,578 | 360.3 | 100,000 |

Note how faucet's throughput *rises* from the 100k to the 1M run (553k → 701k
rows/s) as fixed startup cost amortizes, while Meltano's stays flat (~7.5–8.7k rows/s)
— consistent with a fixed Python/subprocess startup cost plus a per-record
Singer-over-pipe overhead. This is why the 1M run is the meaningful one.

### Scenario B — Postgres → JSONL

**1,000,000 rows** (`bench` table, seed 42, 1 warmup + 5 timed runs). Postgres 16
in Docker (colima); the same machine as Scenario A. faucet `source-postgres` →
`sink-file` (streamed via `sqlx`) vs Meltano `tap-postgres` → `target-jsonl`.

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **5.23** | 0.053 | **191,124** | **21.7** | 1,000,000 |
| Meltano (Singer) | 126.08 | 3.45 | 7,931 | 621.0 | 1,000,000 |

On this workload/hardware, faucet-stream was **~24× faster** and used **~29× less
peak memory**, with **exact row-count parity** (1,000,000 = 1,000,000). The gap is
narrower than Scenario A because both tools are now bounded by real typed row
decoding from Postgres (Scenario A was all-string CSV) — faucet still streams
rows via `sqlx` while `tap-postgres` marshals each row through SQLAlchemy and the
Singer JSON pipe.

### Scenario C — Postgres → Postgres (sink-bound)

**The honest, realistic move: data from one database into another on one machine,
where the destination write dominates.** faucet `source-postgres` →
`sink-postgres` (multi-row `INSERT`s via `sqlx`) vs Meltano `tap-postgres` →
`target-postgres`. Because both tools are now bounded by the *same* Postgres write
path, this is where the CSV→JSONL best-case gap shrinks toward the low end of the
10–80× range — and it is the number to quote for "move a table between two
Postgres databases."

**1,000,000 rows** (`bench` → `bench_dest`, seed 42, 1 warmup + 5 timed runs).
Postgres 16 in Docker (colima) on an Apple M3 Pro; faucet `source-postgres` →
`sink-postgres` (AutoMap columns) vs Meltano `tap-postgres` →
`target-postgres` 0.8.0. faucet uses its `write_method: copy` bulk-load
fast-path (#308, `COPY … FROM STDIN`), the benchmark config's default since it is
the best append path.

**Batch parity (both sides write 5,000-row batches).** So the gap below cannot be
dismissed as a Meltano batch-size misconfiguration: both loaders are pinned to the
same write batch. faucet emits 5,000-row source pages (`batch_size: 5000`) and
writes each via `COPY … FROM STDIN` (its `copy` row) or multi-row `INSERT` (its
`insert` row), and Meltano's `target-postgres` is pinned to `batch_size_rows: 5000`
and writes multi-row `INSERT`s. `target-postgres` also ships an opt-in COPY loader
(`use_copy: true`, default off) that this run leaves off — so the faucet-`copy` vs
Meltano row additionally differs in write *method* (COPY vs INSERT), while the
faucet-`insert` vs Meltano row is method-matched. See
[`benchmarks/faucet/postgres_to_postgres.yaml`](benchmarks/faucet/postgres_to_postgres.yaml)
and [`benchmarks/meltano/meltano.yml`](benchmarks/meltano/meltano.yml).

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream (`write_method: copy`)** | **8.54** | 0.246 | **117,147** | **33.8** | 1,000,000 |
| Meltano (Singer) | 82.06 | 1.18 | 12,186 | 690.3 | 1,000,000 |

On this workload/hardware faucet's COPY path was **~10× faster** than Meltano,
with **~20× less peak memory** (33.8 vs 690 MiB) and **exact row-count parity**
(1,000,000 = 1,000,000). Both rows are from the same session, with both loaders
pinned to 5,000-row batches. (An earlier run measured faucet's multi-row
`INSERT` path at ~1.24× slower than its COPY path; the harness times COPY only.)

This is the whole point of the scenario: **the gap
collapses from ~80× to ~10× as the workload moves from parse-bound to
sink-bound**, because both tools become bounded by the same Postgres write
path. The narrowing, all at 1M rows on the same machine:

| Scenario | Bottleneck | faucet (rows/s) | Meltano (rows/s) | Gap |
|---|---|---|---|---|
| A — CSV → JSONL | parse/serialize (best case) | 700,574 | 8,698 | **~80×** |
| B — Postgres → JSONL | typed row decode | 191,124 | 7,931 | **~24×** |
| C — Postgres → Postgres | **destination write (sink-bound)** | 117,147 (`copy`) | 12,186 | **~10×** |

So quote **~10×** for a realistic DB→DB move (with `write_method: copy`), and
treat the ~80× CSV→JSONL figure as the upper bound, not the typical case.

**Why ~10× and not more?** It is near the ceiling for this shape, and the
profile proves it: at 1M, faucet spends ~62% of its wall on CPU (decode
Postgres rows → JSON → re-encode) and the rest blocked on the destination
write. `COPY` roughly halves the write step vs multi-row `INSERT` (the 5–10×
folk number applies to the write step in isolation, and Amdahl caps the
end-to-end gain at ~1.6× when 62% of the time is decode) — measured end-to-end
it bought **1.24×** (10.10s → 8.12s in the July 2026 run). This benchmark runs Meltano's
`target-postgres` on its default `INSERT` loader (its opt-in `use_copy: true`
COPY path is left off), so only the faucet-`insert` row is a method-matched
comparison; against faucet's COPY row the difference is partly write-method, not
just runtime. The remaining headroom is the decode side
(skipping the JSON intermediate for same-engine moves), which is niche and
high-complexity — see issue #308. Batch size is *not* the lever (throughput is
flat from 500→5000 rows/`INSERT` and worsens past the 65 535 bind-param cap).

> **Reproduce.** `make bench-postgres` (needs Docker) runs all three scenarios;
> `scripts/run-bench.sh --postgres --rows 1000000` scales Scenario C to the 1M
> headline size. The harness spins Postgres 16 in Docker, `COPY`-loads the seeded
> `bench` table, times faucet `postgres_to_postgres.yaml` (TRUNCATE `bench_dest`
> before each run) against Meltano `tap-postgres → target-postgres`, and writes
> the table into `benchmarks/results/results.md`. All three scenarios above are
> 1M rows. (Meltano's loader is pinned to `meltanolabs-target-postgres==0.8.0` —
> older `0.0.x` pins bundle a `singer_sdk` that needs `pkg_resources`, removed
> from modern setuptools on Python 3.12.)

Reproduce with Docker: see
[`benchmarks/README.md`](benchmarks/README.md#scenario-b--postgres--jsonl-manual-needs-docker).

## Caveats — what this does and does NOT prove

**What it measures:** single-machine, single-process **batch throughput** and peak
memory for CSV→JSONL, Postgres→JSONL, and Postgres→Postgres moves on the hardware
in `versions.txt`.

0. **CSV → JSONL is a best case, not the typical case.** It is almost pure
   parse-and-reserialize, which maximally exposes Python's per-row interpreter and
   Singer-over-pipe overhead — so the ~80× there is the *upper bound*. The
   sink-bound Postgres→Postgres move (Scenario C) is the realistic "move a table
   between two databases" number, and the gap there is smaller because both tools
   share the same database write path. Quote the range (10–80×),
   and prefer the scenario closest to your actual workload.

**What it does not prove / honest boundaries:**

1. **Not distributed.** This is one box, one process. It says nothing about
   horizontal scaling, multi-worker throughput, or cluster behavior.
2. **Python interpreter startup is part of Meltano's cost — and it's real, but
   call it out separately.** A meaningful slice of Meltano's wall-clock on smaller
   datasets is Python + plugin subprocess startup and the Singer JSON-over-pipe
   handoff between tap and target. That cost is *fixed*, so it dominates small runs
   and amortizes on large ones — which is exactly why the **1M-row run is the one
   that matters** and the 100k run is only a smoke test. faucet's compiled-binary
   startup advantage is genuine but is partly a startup effect; the 1M row is where
   steady-state streaming throughput shows through.
3. **Singer may add metadata.** Singer/target-jsonl can emit `_sdc_*` metadata
   columns, changing record shape and output size. In *this* configuration the
   record counts matched exactly and no `_sdc_*` columns were added, but a
   different tap/target pairing may differ — so treat row-count parity, not
   byte-size parity, as the correctness check.
4. **Both emit string-typed values here.** `tap-csv` emits all-string fields and
   faucet's CSV source likewise passes through string cells, so neither tool is
   doing type inference in Scenario A — this is a fair, apples-to-apples parse +
   reserialize. Scenario B (Postgres) exercises real typed decoding on both sides.
5. **Hardware-dependent.** Absolute numbers only mean something relative to the
   `versions.txt` machine. Re-run on your own hardware before quoting.
6. **Peak RSS includes faucet's own code.** `maximum resident set size` counts the
   binary's resident code pages, so faucet's ~20 MiB is mostly fixed startup cost
   (on the same runs, macOS's private "peak memory footprint" is ~7 MiB). Per-row
   memory is flat: a 1-row run and a 1M-row run differ by ~2 MiB.
7. **One workload.** A different shape (wide rows, deeply nested JSON, many small
   files, network-bound APIs) could move the ratio. This is not a general
   "faucet is Nx faster" claim — it is one reproducible data point.

**Bottom line:** the comparison is designed to be *fair and falsifiable*, not
flattering. If faucet does not win on the 1M-row run on your hardware, the harness
will say so.

## History

Results move as both tools change. Earlier published runs, for comparison:

| Run | faucet | Meltano | A: CSV → JSONL | B: Postgres → JSONL | C: Postgres → Postgres |
|---|---|---|---|---|---|
| 2026-10-16 (current) | 1.14.0 | 4.4.1 | ~80× (1.43 s vs 115.0 s) | ~24× (5.23 s vs 126.1 s) | ~10× (8.54 s vs 82.1 s) |
| 2026-07-09 | 1.2.0 | 4.2.1 | ~96× (1.40 s vs 135.5 s) | ~25× (5.56 s vs 139.2 s) | ~16× (8.12 s vs 129.8 s†) |

The gap narrowed mainly because **Meltano got faster** (Scenario C: 130 s → 82 s),
not because faucet slowed: on the same machine and load, faucet's times are within
noise of July. † The July Scenario C Meltano row came from a separate session
before the 5,000-row batch pin and had a ±34 s stddev; the October row is
same-session and pinned (±1.2 s). July's peak RSS (11.8 MiB) was a smaller binary;
the October figure (20.1 MiB) is fixed startup overhead from a larger build — see
[#910](https://github.com/faucet-hq/faucet-stream/issues/910).
