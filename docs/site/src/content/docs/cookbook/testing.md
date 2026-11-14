---
title: "Testing pipelines (faucet test)"
description: "faucet test runs fixture-based, fully-offline tests for your pipeline logic. A spec file declares sample input records, the pipeline under test, and the expected outcome; the…"
---

`faucet test` runs **fixture-based, fully-offline tests** for your pipeline
logic. A spec file declares sample input records, the pipeline under test, and
the expected outcome; the runner streams the fixtures through the real
transform → quality → contract path with an in-memory source, sink, and DLQ —
no database, API, broker, or credentials required. That makes pipeline logic
CI-testable: assert "this config + these records produce exactly this output"
on every pull request.

```bash
faucet test tests/orders_tests.yaml       # one spec file
faucet test tests/*.yaml                  # shell glob — any number of specs
faucet test tests/*.yaml --json           # machine-readable report
faucet test tests/*.yaml --filter orders  # run only matching case names
```

The exit code is the number of failed cases (0 = all passed), so CI gates on
it directly.

## Spec file format

```yaml
version: 1
tests:
  - name: null order ids quarantined      # unique per spec file
    config: ../pipeline.yaml              # pipeline config to test (relative to the spec)
    input:                                # fixture records (inline…)
      - { OrderId: 1, Amount: 9.5 }
      - { OrderId: null, Amount: 3.0 }
    expect:
      records: [ { order_id: 1, amount: 9.5 } ]   # what the sink must receive
      dlq: [ { order_id: null, amount: 3.0 } ]    # what quarantine must route
```

Each case needs `name`, `input`, `expect`, and exactly one of:

- **`config:`** — a pipeline config file path (resolved relative to the spec
  file). The case runs that config's transform chain, `quality:` checks, and
  `contract:` against the fixtures. The configured source and sink are **never
  built or contacted** — fixtures replace the source and an in-memory capture
  replaces the sink (a `dlq:` block's sink is likewise replaced by an
  in-memory capture, and quarantine works in tests even without one).
- **`pipeline:`** — the same logic inline, for testing a transform chain or
  contract in isolation:

  ```yaml
  - name: flatten then stamp
    pipeline:
      transforms:
        - type: flatten
          config: { separator: "_" }
        - type: set
          config: { values: { day: "${now.date}" } }
      quality: { … }        # optional, same shape as pipeline.quality
      contract: { … }       # optional, same shape as pipeline.contract
    clock: 2026-02-01T00:00:00Z
    input: [ { user: { name: Ada } } ]
    expect:
      records: [ { user_name: Ada, day: "2026-02-01" } ]
  ```

### Case fields

| Field | Purpose |
|-------|---------|
| `name` | Unique case name (also the `--filter` target). |
| `config` / `pipeline` | What to test — a config file or inline logic (exactly one). |
| `row` | Matrix row id to test when `config` expands to several invocations. The error lists available ids when omitted ambiguously. Row-level transform overrides apply, exactly as in `faucet run`. |
| `input` | Inline record array, **or** a path (relative to the spec) to a `.jsonl` / `.ndjson` (one record per line), `.json`, `.yaml` / `.yml` (top-level array) fixture file. |
| `page_size` | Chunk fixtures into pages of N records. Default `0` = one page (like `batch_size: 0`). Set it to exercise per-page semantics — batch quality checks and aggregating SQL transforms operate per page. |
| `clock` | Fixed `${now.*}` clock for the case (RFC 3339 or `YYYY-MM-DD`). Overrides `--clock`; default is process start. Pin it whenever the pipeline stamps `${now.*}` so the case is deterministic. |
| `expect` | The assertions — see below. |

### Expectations

All fields are optional, at least one is required; every set field is asserted:

| Field | Asserts |
|-------|---------|
| `records` | The sink received exactly these records, in order. |
| `dlq` | These record payloads were routed to the DLQ (quality / contract quarantine), in order. Envelope metadata (timestamp, error message) is not compared — only the quarantined payload. |
| `records_written` | Count-only alternative to `records`. |
| `dlq_count` | Count-only alternative to `dlq`. |
| `error` | The run must **fail** and the error message must contain this substring — for quality `abort` and contract `on_breach: fail` paths. Without it, a failing run fails the case. |
| `unordered: true` | Compare `records` / `dlq` as multisets instead of ordered lists. |
| `match: subset` | Each expected record only names the fields it cares about; extra actual fields are allowed (recursively). Default `match: exact` also flags unexpected fields. Arrays always compare element-wise with equal length. |

Failures print a structured, path-based diff:

```
spec.yaml
  ✗ null order ids quarantined
      - records[0].amount: expected 9.5, got 3.0
      - dlq: expected 1 record(s), got 0

2 tests, 1 passed, 1 failed
```

## What runs, what doesn't

`faucet test` executes the genuine `faucet-core` pipeline loop per page, so
what a test observes is what production does for the same records:

- **Runs:** the full transform chain (including layered pipeline + source
  template + matrix-row transforms, resolved exactly as `faucet run` does),
  `quality:` record + batch checks with real quarantine/abort routing,
  `contract:` enforcement with real quarantine/fail semantics, and DLQ
  envelope routing (unwrapped to payloads for matching).
- **Replaced:** the source (fixtures), the sink, and the DLQ sink (in-memory
  captures). `state:` bookmarks and `delivery:` guarantees don't apply — every
  case is a fresh, single run.
- **Inert:** the `schema:` (drift) block — there is no destination schema
  offline; a warning notes this when the config declares one.
- **Offline config loading:** referenced configs load without contacting
  secrets managers (`${vault:…}`-style directives stay unresolved — safe,
  because the source/sink configs holding them are never used). Pass
  `--resolve-secrets` for the rare secret inside a transform / quality /
  contract block. `${env:VAR}` / `${file:…}` interpolation and `--profile`
  overlays work as usual.

Note: `${now.*}` tokens resolve in source/sink configs (untested here) and in
**inline** test transforms; a config file's transform chain cannot contain
them (`faucet run` rejects that too).

## CI recipe

Pipeline tests need only the `faucet` binary — no services, no Docker:

```yaml
# .github/workflows/pipelines.yml
name: pipeline-tests
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install faucet
        run: cargo install faucet-cli
      - name: Validate configs
        run: faucet validate pipeline.yaml --no-secrets
      - name: Run pipeline tests
        run: faucet test tests/*.yaml
```

`--json` emits a report for tooling:

```json
{
  "total": 5,
  "passed": 5,
  "failed": 0,
  "tests": [
    { "name": "clean orders pass through", "spec": "tests/orders.yaml", "status": "pass", "failures": [] }
  ]
}
```

## Complete example

A runnable pipeline + spec pair ships in
[`cli/examples/tests/`](https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples/tests)
— quality quarantine, contract breach, fixture files, subset/unordered
matching, and an inline case with a pinned clock:

```bash
faucet test cli/examples/tests/pipeline_tests.yaml
```

`faucet schema test` prints the spec file's JSON Schema for editor validation.
