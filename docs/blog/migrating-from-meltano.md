# Migrating from Meltano/Singer to faucet-stream

*A concept-by-concept, config-by-config guide for teams running Singer taps on
Meltano who want a single fast binary instead of a Python plugin runtime — with
an honest note on when you shouldn't switch.*

> Grounded in [`cli/examples/rest_to_postgres.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_postgres.yaml).
> Reflects both tools as of 2026-07. See the [full comparison](https://faucet-hq.github.io/faucet-stream/comparison/meltano.html)
> and [our benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md).

## Should you migrate?

Straight answer first, because it makes the rest credible.

**Stay on Meltano if** your first requirement is **connector breadth** — 600+
Singer taps vs faucet's ~58 built-in connectors. If you depend on a long-tail
SaaS tap that faucet doesn't have, Meltano wins today, full stop. (That said, the
experimental `singer` source lets faucet run those taps unchanged — see the
gotcha below — so "no native connector" isn't necessarily a dealbreaker.)

**Move to faucet if** you feel the cost of the Python plugin runtime and want:

- **Throughput.** On a reproducible 1M-row CSV→JSONL move, faucet does 712k
  rows/s in 11.8 MiB vs Meltano's 7.4k rows/s in 724 MiB — output identical
  row-for-row. Sink-bound moves narrow the gap (the benchmarks show that too,
  honestly), but the structural win — no per-row Python overhead, bounded
  streaming memory — is real.
- **Operational simplicity.** faucet is one static binary. No virtualenv, no
  plugin resolution, no Python-version matrix to keep green in CI and prod.
- **Governance in the movement path.** Data quality, versioned data contracts,
  PII masking (applied before any sink sees a row), schema-drift policy, and
  lineage are native and zero-config — not assembled from mappers + dbt tests +
  external tooling.

If you're doing EL→dbt today, note faucet doesn't replace dbt either — see the
[orchestration recipe](https://faucet-hq.github.io/faucet-stream/cookbook/orchestration.html)
(faucet for EL, dbt for T, Airflow/Dagster to schedule).

## The mental model maps cleanly

| Meltano / Singer | faucet-stream |
|---|---|
| extractor (tap) | a `source` |
| loader (target) | a `sink` |
| `meltano.yml` | a `faucet.yaml` `pipeline:` block |
| Singer `STATE` message | a resumable `state:` bookmark |
| `replication-method: INCREMENTAL` | `replication_method: { type: Incremental }` |
| `replication-key` | `replication_key` |
| stream maps / mappers | `transforms:` (incl. the embedded-DuckDB `sql` transform) |
| plugin config / env | `${env:VAR}`, `${file:...}`, `${secret:...}` interpolation |
| `meltano run tap target` | `faucet run pipeline.yaml` |

## Before / after: Stripe charges → Postgres

### Before — Meltano

```yaml
# meltano.yml
plugins:
  extractors:
    - name: tap-stripe
      config:
        client_secret: ${STRIPE_TOKEN}
      select:
        - charges.*
      metadata:
        charges:
          replication-method: INCREMENTAL
          replication-key: created
  loaders:
    - name: target-postgres
      config:
        host: localhost
        database: app
        default_target_schema: public
```

```bash
# plus: a virtualenv, `meltano install`, plugin resolution, then:
meltano run tap-stripe target-postgres
```

### After — faucet

```yaml
# faucet.yaml
version: 1
name: stripe_charges_to_postgres

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.stripe.com/v1
      path: /charges
      auth:
        type: bearer
        config: { token: ${env:STRIPE_TOKEN} }
      pagination:
        type: Cursor
        next_token_path: $.next_page
        param_name: starting_after
      replication_method: { type: Incremental }
      replication_key: created
      primary_keys: ["id"]
      state_key: stripe:charges

  transforms:
    - type: keys_case
      config: { mode: snake }

  sink:
    type: postgres
    config:
      connection_url: ${env:PG_URL}
      table_name: stripe_charges
      column_mapping: { type: jsonb, column: data }

  state:
    type: file
    config: { path: ./.faucet-state }
```

```bash
cargo install faucet-cli
faucet run faucet.yaml            # no install/resolve step — one binary
```

This is a real, runnable config —
[`cli/examples/rest_to_postgres.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_postgres.yaml).

## Migration steps

1. **Inventory your taps and targets.** For each, check the
   [connector catalog](https://faucet-hq.github.io/faucet-stream/reference/connectors.html).
   Native SaaS taps (Stripe, Shopify, …) usually map onto faucet's generic
   `rest` / `graphql` source pointed at the same API. Databases, warehouses,
   files, and streaming systems map to dedicated connectors.
2. **Translate one pipeline** using the table above. Keep the raw-JSONB landing
   pattern (`column_mapping: { type: jsonb }`) if you transform downstream in
   dbt — it mirrors how most Singer targets land data.
3. **Port your STATE.** faucet keeps its own bookmark in the `state:` store; you
   don't hand-migrate Singer STATE. On first run, set the initial position via
   the source's replication config (or just let it do a full initial load).
4. **`faucet validate faucet.yaml`** — checks the config without running it or
   touching infra, ideal for CI.
5. **`faucet preview faucet.yaml --limit 10`** — runs just the source and prints
   records, so you confirm extraction before wiring the sink.
6. **Run it, and diff row counts** against your Meltano output for the same
   window.
7. **Adopt the governance you were bolting on** — replace mapper-based redaction
   with native [masking](https://faucet-hq.github.io/faucet-stream/cookbook/masking.html),
   dbt data tests at the edge with in-path [quality checks](https://faucet-hq.github.io/faucet-stream/cookbook/quality.html)
   and [contracts](https://faucet-hq.github.io/faucet-stream/cookbook/contracts.html).

## Gotchas

- **No tap for your source?** If it's a REST or GraphQL API, the generic source
  usually covers it with `pagination` + `auth` config — you're configuring, not
  writing a plugin. If it's a truly bespoke SaaS with only a Singer tap, the
  experimental [`singer` source](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
  runs that tap unchanged under faucet — a zero-rewrite stepping stone (it's v0
  and single-stream, and reintroduces the tap's Python process) while you wait
  for, or file, a native connector request.
- **Mappers → transforms.** Simple renames/casts/drops map to built-in
  [record transforms](https://faucet-hq.github.io/faucet-stream/cookbook/transforms.html);
  anything SQL-shaped maps to the embedded-DuckDB
  [`sql` transform](https://faucet-hq.github.io/faucet-stream/cookbook/sql-transform.html).
- **Orchestration.** If you drove Meltano from Airflow, keep Airflow — just swap
  the `meltano run` call for `faucet run`. See the
  [orchestration recipe](https://faucet-hq.github.io/faucet-stream/cookbook/orchestration.html).

## Try it in 60 seconds

```bash
cargo install faucet-cli
faucet run cli/examples/csv_to_jsonl.yaml   # no infra needed
```

---

*faucet-stream is an MIT/Apache-2.0 Rust library + CLI for moving data between
<!--COUNT:sources-->38<!--/COUNT--> sources and <!--COUNT:sinks-->30<!--/COUNT--> sinks. [Docs](https://faucet-hq.github.io/faucet-stream/) ·
[GitHub](https://github.com/faucet-hq/faucet-stream).*
