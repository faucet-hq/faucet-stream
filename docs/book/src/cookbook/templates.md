# Parameters & pipeline templates

Most fleets run the *same* pipeline shape over and over with a handful of values
that change: a tenant id, a date window, a target table, a source URL. Copying the
config once per tenant means N files to keep in sync; re-sending the whole config
on every trigger means re-validating it every time and hoping the caller got the
shape right.

faucet splits that into two pieces:

- **`params:`** — a config declares its typed, trigger-time surface. Available
  everywhere, no build feature needed.
- **the template registry** — register a parameterized config **once**, then
  trigger runs by `{id, params}`. Needs the `templates` build feature.

## Declaring parameters

```yaml
kind: pipeline          # a complete config — see "Registering a template" for the other kinds
version: 1
name: tenant-sync

params:
  tenant_id:
    type: string
    required: true
    description: Tenant whose events to sync
  since:
    type: string
    default: "1970-01-01"
  page_size:
    type: int
    default: 500
  api_token:
    type: string
    required: true
    secret: true
  region:
    type: string
    default: us
    values: [us, eu, apac]

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: "/tenants/${param.tenant_id}/events"
      query_params: { since: "${param.since}" }
      auth: { type: bearer, config: { token: "${param.api_token}" } }
      pagination:
        type: PageNumber
        param_name: page
        start_page: 1
        page_size: "${param.page_size}"
        page_size_param: per_page
  sink:
    type: file
    config:
      path: "./out/${param.tenant_id}/events.jsonl"
```

Fields per entry:

| Field | Meaning |
|---|---|
| `type` | `string` (default) · `int` · `float` · `bool` |
| `required` | The caller must supply a value. Mutually exclusive with `default`. |
| `default` | Value when the caller supplies none. An ordinary config scalar, so `default: "${env:SINCE}"` works. On a typed param it may be text that resolves to the type (`type: int, default: "${env:PORT}"`). |
| `secret` | Registered for redaction the instant it is bound — never reaches a log, an error message, an API response, the audit log, or the registry. |
| `description` | Shown by `faucet template list` / `show`, `GET /v1/templates`, and the MCP `get_template` tool. |
| `values` | Closed set of acceptable values. Anything else is rejected at bind time, naming the set. A `default` must be one of them. |

Reference a param anywhere in the config as `${param.NAME}`. Write `$${param.x}`
for a literal `${param.x}`.

### Closed value sets

`values:` turns a param into an **enumerable axis**. Without it a typo'd value —
`region: ue` — binds happily and surfaces as a 404 halfway through a run; with it
the bind fails up front and names the three it will accept:

```
param 'region': 'ue' is not one of the allowed values: apac, eu, us
```

Being enumerable is the other half: a
[test suite](#testing-the-parameter-space) can sweep every declared value
without being told what they are, so the sweep stays correct as the set grows.

### Types are real

When `${param.NAME}` is a scalar's **entire** text, the declared type survives:
`page_size` above arrives at the connector as the JSON number `500`, not `"500"`.
Embedded in a longer string (`.../events?since=${param.since}`) it is stringified,
like every other interpolation namespace.

Values are accepted in either wire shape, so a CLI `--param page_size=500` and an
HTTP `{"page_size": 500}` behave identically — and `--param page_size=abc` is
rejected up front, naming the param.

## Running a parameterized config directly

`faucet run` and `faucet validate` take `--param`:

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param since=2026-01-01 \
  --param api_token="$TOKEN"

# Validate in CI without inventing values: required params bind to type-shaped
# placeholders, so the config's structure is still fully checked.
faucet validate tenant-sync.yaml

# Or check one concrete invocation end to end (strict binding).
faucet validate tenant-sync.yaml --param tenant_id=acme --param api_token=x
```

`--param-env NAME=VALUE` overrides an environment variable for that run's
`${env:VAR}` resolution only; bare `--param-env TOKEN` takes the value from your
own environment, so a secret never appears in the process arguments. The process
environment itself is never modified — which is what makes this safe inside a
concurrent server. A template trigger's `env` overrides (HTTP / MCP) may only
name variables the template reads (`${env:NAME}` / `${secret:NAME}`) and are
literal text — a value containing `${` is refused, and a tenant-scoped
principal may not pass `env` at all.

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param-env API_HOST=eu.example.com
```

## Registering a template

The registry stores four **kinds** of document, told apart by a `kind:` line:

| `kind:` | What it is | How it runs |
|---|---|---|
| `source-template` | One system: its connector, shared `transforms`, and **streams** with per-stream write preferences ([Template Hub](./template-hub.md)) | Composed with a registered `sink-template`: `faucet template run <source> --sink <sink>` / `POST …/runs {"sink": …}` |
| `sink-template` | One destination and how a stream is addressed (`per_stream`) | Never on its own — named as the `sink` of a source template's run |
| `deployment` | The operational blocks of a composed run — `state`, `dlq`, `notifications`, `sla`, … ([Deployment overlays](./template-hub.md#deployment-overlays)) | Never on its own — named as the `overlay` of a source template's run |
| `pipeline` | A complete config with `params:` | Alone, as below |

A source or sink template is registered under its own `name` (the hub id), is
validated as a hub template, and goes through the publishability lint — a
literal credential or a private hostname is refused, because a shared registry
is a shared place. A template's kind is fixed for its id: a later register
under the same id with a different kind is refused. A document without `kind:`
is still accepted as a pipeline but prints a deprecation notice — add
`kind: pipeline` to a complete config.

A pipeline template's `name:` is its state-key namespace (`{name}::{row}`).
One registered without a `name:` runs under its **template id** — the same on
`faucet template run`, a server trigger and MCP — so two unnamed templates
sharing a state store never read each other's bookmarks.

```bash
faucet template register hub/source-templates/acme/billing.yaml --launch    # id = acme/billing
faucet template register hub/sink-templates/faucet-hq/bigquery.yaml --launch  # id = faucet-hq/bigquery
faucet template list --kind sink-template
faucet template run acme/billing --sink faucet-hq/bigquery \
  --param api_token="$ACME_TOKEN" --param bq_project=my-project --param bq_sa_key="$BQ_SA_KEY"
# → composes the two, prints the per-stream plan (bills: overwrite, transactions: upsert[id], …), runs
faucet template register ops/prod.yaml --launch                               # kind: deployment
faucet template run acme/billing --sink faucet-hq/bigquery --overlay prod …   # + state / DLQ / SLA
```

The rest of this page uses a complete `pipeline` template; everything about
versions, channels, launching, and triggering applies to all three kinds.

```bash
faucet template register tenant-sync.yaml --store sqlite:./faucet-templates.db
# registered template 'tenant-sync' version 1
#
# params:
#   api_token            string  required  [secret]
#   page_size            int     default 500
#   since                string  default "1970-01-01"
#   tenant_id            string  required  — Tenant whose events to sync

faucet template list  --store sqlite:./faucet-templates.db
faucet template show  tenant-sync --store sqlite:./faucet-templates.db
faucet template run   tenant-sync --store sqlite:./faucet-templates.db \
  --param tenant_id=acme --param api_token="$TOKEN"
faucet template delete tenant-sync --store sqlite:./faucet-templates.db --version 1
```

`--store` accepts `sqlite:<path>`, a `postgres://…` URL, or `memory`
(process-lifetime only, for a smoke test) — the same grammar as `catalog.url` and
`faucet serve --history`, and it can be set once via `FAUCET_TEMPLATE_STORE`. SQL
stores need the matching `serve-history-sqlite` / `serve-history-postgres` build
feature.

`faucet template run` materializes the template and then runs it through the
*identical* path as `faucet run` — observability, lineage, notifications, the
catalog, SLA evaluation and row selection all behave the same.

### Versions, launching, and channels

Every `register` appends a **new numeric version**, auto-incrementing from 1 — and
that is *all* it does. **Registering never moves existing callers.** A nightly
build, a feature branch, a half-tested experiment: they all land as a new version
while everyone who did not pin one keeps running exactly what they ran yesterday.

Making a version live is a separate, deliberate step: **`launch`**.

```bash
faucet template register tenant-sync.yaml          # v4 exists; nobody is affected
faucet template launch   tenant-sync               # v4 is live — this moves callers
```

That split is the whole point. A deploy can register freely; promoting a build to
"what production runs" stays a decision somebody makes on purpose.

#### Template status

A template is in exactly one of three states, and the state is **derived** from
what has actually happened, so it can never disagree with the registry:

| Status | Meaning |
|---|---|
| `draft` | Registered but never launched — the work-in-progress state. An unpinned run is refused (there is no blessed version); explicit selectors still work, so a draft is fully testable. |
| `launched` | A version has been launched. Unpinned runs resolve to it. |
| `deprecated` | Explicitly retired. Unpinned runs **still work** — retiring must not hard-break callers — but every trigger warns and listings mark it. `delete` is the hard stop. |

```bash
faucet template register tenant-sync.yaml --launch   # skip the draft stage
faucet template deprecate tenant-sync --reason "superseded by tenant-sync-v2"
faucet template deprecate tenant-sync --undo         # revive it
```

#### Retiring one version

A single bad build can be retired without deleting it, which would erase its
history and break anything pinned to it:

```bash
faucet template deprecate tenant-sync --version 5 --reason "drops the invoices stream"
faucet template deprecate tenant-sync --version 5 --undo      # revive it
```

A retired version:

- **still runs** when pinned (`--version 5`), or when `stable` or a channel
  points at it, and every such trigger carries a `deprecated` warning
  (`"v5 is deprecated: drops the invoices stream"`);
- is **skipped by `newest`**, which resolves to the highest version that is
  not retired (when every version is retired, `newest` says so instead);
- **cannot be launched**. Revive it first, or launch another version.

Deprecating the live version does not move `stable`; roll back separately.
Deleting a version clears its marker. Over HTTP it is
`POST /v1/templates/{id}/versions/{version}/deprecate` with
`{"reason":"…"}` or `{"undo":true}`, and `GET /v1/templates/{id}` lists
`deprecated_versions`. A [sync](#hosting-templates-in-a-repo-or-bucket-sync)
from a Template Hub catalog retires a version here when the catalog deprecates
the body it holds.

#### Channels

On top of the numbers sit **named channels**: pointers at one numeric version.
Three are **derived** — computed from the launch log, never assigned:

| Derived channel | Resolves to |
|---|---|
| `stable` | The launched version. **The default when no version is given.** Moves only via `launch`. |
| `previous` | The version launched *before* the current one — the rollback target. Unset until a second launch. |
| `newest` | The highest version number, launched or not. The build tip. |

The rest are **assignable** — you point them wherever you like with `promote`:

| Assignable channel | Meaning |
|---|---|
| `dev` | Day-to-day development |
| `test` | QA / integration testing |
| `staging` | Staging |
| `pre-prod` | Pre-production / release-candidate soak |
| `canary` | Partial-traffic canary ahead of `prod` |
| `prod` | Production |

The set is **closed on purpose**: an open-ended tag namespace becomes a second,
unreviewable naming system in which a typo (`prd`) silently creates a channel
nobody watches. An unknown name is rejected with the valid list. Names are
forgiving about spelling — `pre-prod`, `pre_prod`, `PreProd`, and `preprod` are
one channel — and if you need a free-form label, put it in the run's `labels`,
not in the registry.

> **There is no `latest`.** It reads as both "the newest build" and "the current
> stable release", and those are exactly the two things this model keeps apart. Ask
> for it and faucet says so rather than guessing:
>
> ```text
> `latest` is not a version channel here because it is ambiguous. Did you mean
> `stable` (the launched version — also the default when no version is given), or
> `newest` (the highest version number, launched or not)?
> ```

#### Selecting a version

| Selector | Resolves to |
|---|---|
| *(omitted)* | `stable` — the launched version |
| a channel name (`prod`, `newest`, `previous`, …) | whatever that channel points at |
| a number (`2`) | exactly that version |

```bash
faucet template run tenant-sync --param tenant_id=acme            # stable
faucet template run tenant-sync --version prod    --param …       # whatever prod names
faucet template run tenant-sync --version newest  --param …       # the build tip
faucet template run tenant-sync --version 2       --param …       # pinned
```

Asking for an unset channel is an error phrased for *that* channel, because the
fix differs: `stable` needs a launch, `previous` needs a second launch, an
environment channel needs a promote. Silently falling back would run the wrong
code.

#### Promoting and launching

```bash
# Register v5 and point `dev` at it in one step.
faucet template register tenant-sync.yaml --tag dev

# Walk it up the channels — each promote copies another channel's current target.
faucet template promote tenant-sync --tag test     --version dev
faucet template promote tenant-sync --tag pre-prod --version test
faucet template promote tenant-sync --tag prod     --version pre-prod
# → template 'tenant-sync': prod → v5

# Bless whatever soaked in pre-prod as the new stable.
faucet template launch tenant-sync --version pre-prod
# → template 'tenant-sync': launched v5 (was v4; previous → v4)
```

`launch` defaults to `newest`, since launching what you just registered is the
common case. Re-launching the already-live version is a no-op — which is what
keeps `previous` a real rollback target rather than a copy of the current version.
A promote *from* a channel resolves to a concrete version at that moment, so a
pointer never silently follows future registrations. Derived channels cannot be
assigned: `faucet template promote … --tag stable` is rejected and tells you to
use `launch`.

#### Rolling back

```bash
faucet template rollback tenant-sync
# → template 'tenant-sync': rolled back to v4 (was v5; previous → v5)
```

Rollback re-launches `previous`, and it is an ordinary launch under the hood — so
the launch log keeps the full audit trail and `previous` becomes the version you
just rolled off (roll back twice and you are where you started).

```bash
curl -sX POST localhost:8080/v1/templates/tenant-sync/launch \
  -H "Authorization: Bearer $TOKEN" -d '{"version":"pre-prod"}'
# → 200 {"id":"tenant-sync","version":5,"replaced":4,"already_launched":false,"status":"launched"}

curl -sX POST localhost:8080/v1/templates/tenant-sync/rollback \
  -H "Authorization: Bearer $TOKEN" -d '{}'
```

#### Inspecting

`faucet template list` shows one row per id — its status, what is live, and the
build tip:

```text
ID                          STATUS       LIVE    NEWEST   PARAMS  DESCRIPTION
orders-export               launched     v2      v2            4  Nightly export of an orders table.
```

`faucet template show` reports one version in the context of the whole release
state — every version with the channels pointing at it, and who launched what:

```text
template  orders-export   [launched]
name      orders-export
about     Nightly export of an orders table.
created   2026-08-07T14:34:05Z
showing   v2  (live)

versions:
  v2    live, newest, dev, staging
  v1    previous

launch history (newest first):
  v2    2026-08-07T14:34:05Z
  v1    2026-08-07T14:34:05Z
```

A description describes the *template*, so it carries forward: re-registering
without `--description` keeps the previous one rather than blanking the listing.

`GET /v1/templates/{id}` returns the same picture as JSON, so a client can pin,
promote, launch, or roll back without a second call:

```jsonc
{
  "id": "tenant-sync", "version": 2,      // the version returned
  "status": "launched",                   // draft | launched | deprecated
  "versions": [3, 2, 1],                  // everything stored, newest first
  "stable": 2,                            // the launched version (unpinned runs)
  "previous": 1,                          // the rollback target
  "newest": 3,                            // the build tip
  "is_stable": true,                      // the returned version is the live one
  "tags": { "dev": 3, "prod": 1 },        // assignable channels only
  "launches": [ { "seq": 2, "version": 2, "launched_at": "…", "launched_by": "ci" } ],
  "body": "version: 1\nname: tenant-sync\n…"
}
```

Pass `?version=newest` to open a `draft` template — it has no `stable` version yet.

#### Suites for a source template

A suite whose `template:` is a source template names the sink it should be
tested against: `sink:` (a registered id, or a path when `template:` is a path)
and optionally `sink_select:`. Every case then materializes the **composed**
pipeline — the same document a trigger builds — and `auto:` cases sweep the
merged parameter surface, so a sink param the source never declared is still
covered. `overlay:` (and `overlay_select:`) adds a deployment overlay to that
composition — a registered id, or a path in a file-based suite — and its params
join the surface too.

```yaml
version: 1
template: acme/billing
sink: faucet-hq/bigquery
suite:
  auto: { enum_coverage: true }
  cases:
    - name: prod-shape
      params: { bq_project: analytics, api_token: placeholder }
```

#### In the console

The web console (`serve-ui`) has a **Templates** view built around exactly this:
a list showing each template's status, live version, and build tip, and a
per-template **versions page** with one row per version, the channels currently
pointing at it, an assign-channel dropdown, and Launch / Config / Deprecate /
Delete (a retired row shows a `deprecated` pill and cannot be launched) —
plus Roll back, Deprecate for the whole template, and a typed trigger form
generated from the template's `params:`. The controls follow the signed-in
role: a viewer sees no buttons that change anything, an operator can run
templates but not manage them.

#### Wire shapes and cleanup

`version` accepts a channel name (`"prod"`), a numeric string (`"2"`), or a bare
number (`2`), so a query string, a JSON body, and an MCP tool argument all mean
the same thing. `0`, `latest`, and unknown names are rejected rather than silently
falling back.

Deleting: `--version <N|channel>` removes one version;
`faucet template delete <id>` with no `--version` removes the template entirely.
Channels pointing at a deleted version — and its launch-log entries — are dropped
with it, so no pointer outlives its target. Runs already produced are untouched.

The 20 most recent versions of each id are kept, so a template re-registered on
every deploy keeps a useful rollback window without growing without bound.

**Practical pattern.** Let deploys `register --tag dev` freely — nothing moves.
Walk a version up the channels (`dev` → `test` → `pre-prod` → `prod`) as it earns
trust. Bless it with `launch` when it should become what unpinned callers get, and
keep `rollback` one command away. Point scheduled jobs at a channel
(`--version prod`) when they must be pinned to a specific promotion train, and
leave everything else unpinned so `launch` is your single release lever.

## Testing the parameter space

A template fans out across a parameter space, and a change to one version can
silently break one corner of it — a param that no longer interpolates, a value
that yields an invalid config, a required param whose failure stopped being
clean. `faucet template test` turns that sweep into a red/green artifact:

```bash
faucet template test suite.yaml                                    # template: is a path
faucet template test suite.yaml --store sqlite:./faucet-templates.db --select prod
```

```yaml
version: 1
# A registered id, or — as here — a path, so a template can be tested *before*
# it is ever registered, which is when these failures are cheapest to fix.
template: ./tenant-sync.yaml

suite:
  # Derived from the template's own `params:`, so these stay correct as the
  # template gains params instead of going stale like a hand-written list.
  auto:
    enum_coverage: true      # one case per declared value of every `values:` param
    required_omitted: true   # one per required param, omitted, expecting a named failure
    defaults_baseline: true  # the all-defaults combination

  cases:
    - name: eu-small-pages
      params: { tenant_id: acme, api_token: t0ken, region: eu, page_size: 50 }

    # `error:` implies the case must fail *and* that the message mentions the
    # substring — a real assertion rather than "it failed somehow", which would
    # also pass on an unrelated break.
    - name: rejects-an-undeclared-region
      params: { tenant_id: acme, api_token: t0ken, region: antarctica }
      expect: { error: region }

  combine:
    params:
      region: [us, eu, apac]
      page_size: [1, 500]
    exclude:
      - { region: apac, page_size: 1 }   # a genuinely-invalid pairing
    pairwise: false                      # all-pairs instead of the full product

  # Optional second tier: fixture records through the real pipeline, using
  # `faucet test`'s matchers.
  behavioral:
    - name: shapes-a-record
      params: { tenant_id: acme, api_token: t0ken }
      input: [{ "Id": "1", "Name": "Acme" }]
      expect: { records_written: 1 }
```

```
template ./tenant-sync.yaml
  ok   [explicit] eu-small-pages
  ok   [explicit] rejects-an-undeclared-region
  ok   [combine] page_size=1,region=us
  …
  ok   [auto] auto:missing-api_token
  ok   [auto] auto:missing-tenant_id

13 case(s): 13 passed, 0 failed
```

**Two tiers, both offline.** The default *validation* tier materializes the
template for a combination exactly as a real trigger would, then expands it,
deserializes each row's connector configs into their typed structs and compiles
each row's transform chain — the same checks `faucet validate` runs, so a param
whose type the connector rejects (an `int` used as a whole `query_params`
value) fails the case (in topology mode it validates the graph
instead — skipping that would let a broken graph pass). No network, no data, no
sink, which is what makes it cheap enough to run on every change. The
*behavioural* tier feeds fixture records through the real pipeline via the
[`faucet test`](./testing.md) harness, reusing its matchers rather than
reimplementing them.

**Case origins.** Every case is labelled `explicit`, `combine`, `auto`, or
`behavioral` in the report, because a red case nobody wrote is otherwise a
mystery. Required params a `combine:` sweep does not name are filled
automatically, so generated cases test the axes you listed and nothing else.

**Guard rails.**

- A cartesian product explodes quietly, so generation stops at **512 cases** with
  an error rather than a truncation — a report covering a third of the space
  would read green.
- Set `pairwise: true` to reduce to an **all-pairs** set: most param-interaction
  bugs involve two params, so this keeps the coverage that matters while turning
  a multiplicative count into roughly the product of the two largest lists.
- An `exclude:` entry naming no swept param is rejected — it can never match, and
  it silently *widens* the tested space rather than narrowing it.
- An empty suite is rejected. A suite with no cases reports green, which is worse
  than no suite.
- A sweep whose full product passes 32,768 combinations is refused before it is
  built (narrow it or use `pairwise: true`, which is generated without the
  product).
- A behavioural case on a config with several rows (a multi-stream source
  template, a `matrix:`) must name one with `row:`; fixture paths resolve
  against the suite file's directory.
- Duplicate case names are rejected (they make `--filter` ambiguous).

`--filter '<pattern>'` runs a subset (`*` wildcards; a bare name is an exact
match, so `--filter auto` does *not* match `auto:defaults`). `--json` emits the
machine-readable report. The exit code is the failed-case count, mirroring
`faucet test`, so CI gates on it without parsing output. `faucet schema
template-test` prints the suite schema.

## Test bundles and the launch gate

A suite file lives next to a template; a **test bundle** lives *inside* it. Add a
`tests:` block to any template document — `kind: pipeline`, `source-template`,
`sink-template` or `deployment` — and the tests are stored verbatim with the
version, covered by its content hash. Changing a test is a new version, exactly
like changing the config, so "v3 passed its tests" always means v3's tests.

<!-- faucet:no-validate -->
```yaml
# cli/examples/csv_to_jsonl_with_tests.yaml (abridged)
kind: pipeline
version: 1
name: orders_export
params:
  region: { type: string, default: us, values: [us, eu] }
pipeline: { … }
tests:
  suite:                       # the `faucet template test` grammar above
    auto: { enum_coverage: true, defaults_baseline: true }
    cases:
      - { name: unknown-region-is-refused, params: { region: apac }, expect: { error: region } }
  fixtures:                    # `faucet test` cases, run through this template
    - name: rows-are-stamped
      params: { region: eu }
      input: [{ id: 1 }, { id: 2 }]
      expect: { records: [{ id: 1, exported: true }, { id: 2, exported: true }] }
    - name: inline-filter      # or an inline `pipeline:` instead of the template's own
      retries: 1               # explicit and recorded — never a silent retry
      pipeline: { transforms: [{ type: filter, config: { path: status, op: eq, value: paid } }] }
      input: [{ id: 1, status: paid }, { id: 2, status: open }]
      expect: { records_written: 1 }
  requires_suites:             # shared suites, picked by semver range
    - { name: rest-conformance, version: ">=1.2,<2" }
```

| Field | Meaning |
|---|---|
| `suite` | Parameter-space cases (`cases`, `combine`, `auto`, `behavioral`), as in a suite file. |
| `fixtures` | `faucet test` cases. Without `pipeline:` a fixture runs the template's own materialized config (`params:` bind it, `row:` picks a row); with `pipeline:` it runs that inline logic. |
| `requires_suites` | Shared `kind: test-suite` documents, by `name` and semver `version` range over their `release:`. |
| `sink` / `source` / `overlay` (+ `*_select`) | The companions a hub kind composes with: a source template names its `sink` (and optionally an `overlay`); a sink template names its `source`; a deployment overlay names both. Selectors default to `stable`. A `pipeline` takes none. |
| `retries` (on any case) | Extra attempts, at most 5. A case that passed on a retry says so (`attempts`). |

Everything runs **offline**: records are inline (a fixture path is refused — the
version stores only the document) and no connector is ever opened. Live checks
against real systems are deliberately not part of the gate.

### Running a bundle

```bash
faucet template test cli/examples/csv_to_jsonl_with_tests.yaml          # a file — no registry
faucet template register orders.yaml --test --store sqlite:./faucet-templates.db
faucet template test orders_export@3 --store sqlite:./faucet-templates.db   # run + record on v3
faucet template test orders_export@3 --no-record --filter 'auto:*' --store …  # a subset, not recorded
```

A registered run is **recorded on the version**: per-case pass/fail, attempts and
duration, the faucet binary version, the time, who ran it, the sha256 of the body
it tested, and which shared-suite releases it resolved. The newest 20 runs per
version are kept; deleting a version (or the template) deletes its results. Over
HTTP: `POST /v1/templates` with `"test": true`, `POST
/v1/templates/{id}/versions/{version}/test`, and `GET …/versions/{version}/tests`;
over MCP: `register_template {test: true}` and `test_template`.

### Shared suites

A suite many templates share — a conformance suite for every REST source, say — is
its own document, registered (or kept in a hub's `test-suites/` directory) and
versioned by a semver `release`:

```yaml
kind: test-suite
name: rest-conformance
release: 1.4.0
suite:
  auto: { defaults_baseline: true, required_omitted: true }
fixtures: [ … ]
```

A `requires_suites` range resolves to the **highest registered, non-deprecated
release** in range each time the bundle runs; the result records which release ran.
A release is immutable — registering the same `release:` twice is refused. A range
nothing satisfies fails **registration** of the requiring template with a typed
error naming the suite, the range and the releases that exist. A test-suite is
never runnable on its own. `faucet schema test-suite` / `faucet schema
template-tests` print the two schemas.

### The launch gate

`faucet serve --require-template-tests` (or `FAUCET_REQUIRE_TEMPLATE_TESTS=1`)
makes the registry refuse to **launch** a version unless a passing bundle result is
recorded for that exact version body under the **running faucet major version**.
Every launch path goes through the same check — `POST /v1/templates/{id}/launch`
and `/rollback`, a register with `launch: true` (the bundle runs first), the MCP
`launch_template` / `rollback_template` / `register_template` tools, template sync
launches, and approved `template_launch` change requests. A refusal is a `422`
whose `error.details.gate` lists the failing cases, and is audited
(`template.launch`, result `refused`).

| Version | `--require-template-tests` off (default) | on |
|---|---|---|
| passing result, this major | launches | launches |
| failing result | launches (console warns) | refused, failing cases listed |
| results only from another major, or another body | launches | refused — rerun the bundle |
| bundle never run | launches | refused |
| no `tests:` block | launches (console warns) | refused |

An admin can launch past a refusal with a reason — `--skip-tests-reason "<why>"`
on `faucet template launch|rollback|register`, `skip_tests_reason` in the HTTP
body or MCP arguments. The reason is written on the launch log (`tests_skipped`)
and in the audit log (`detail: tests skipped: <why>`). The override is admin-only
however it arrives: the launch routes and tools already are, and a change request
carrying `skip_tests_reason` may only be proposed by an admin.

The CLI applies the gate with `--require-tests` on `faucet template launch`,
`rollback`, `register --launch` and `sync`.

> **Migration.** The gate is **off by default** in this release so existing
> deployments keep launching as before. To adopt it: add `tests:` blocks, run
> `faucet template test <id>@<version>` for each version you intend to launch
> (or re-register with `--test`), then start the server with
> `--require-template-tests`. After a faucet major upgrade, rerun the bundles —
> results from the previous major no longer count.

The console's versions page shows each version's test status and recent runs, a
**Run tests** button, and disables **Launch** with the failing cases listed when
the gate would refuse.

## Running a subset of streams

A template with rows — the `streams:` of a source template, the `matrix:` of a
pipeline template — does not have to run all of them. Backfill one stream,
re-run the one that failed, run the cheap streams hourly and the heavy one
nightly, or let an embedding product's tenant pick which objects to sync, all
from one registered template. Every trigger path takes the same **selection**,
the model `faucet run` already uses (`--select` / `--only` / `--skip` /
`--tag` / `--status` / `--include-parents`):

```json
{ "selection": { "select": ["deals"], "only": ["act*"], "skip": ["audit"],
                 "tags": ["finance"], "status": ["available"],
                 "include_parents": "eligible" } }
```

| surface | how |
|---|---|
| CLI | `faucet template run crm --sink files --select deals --include-parents eligible` |
| HTTP | `selection` in the body of `POST /v1/templates/{id}/runs`, `POST /v1/runs`, `POST /v1/tenants/{t}/runs`, `POST /v1/tenants/{t}/templates/{id}/runs` and `POST /v1/templates/{id}/fanout` |
| MCP | `selection` on `run_template`, `run_pipeline` and `propose_run` |
| triggers | `run: { selection: {…} }` on a schedule / webhook / object-arrival / queue-depth trigger ([reference](../reference/triggers.md)) |
| console | the **Streams** (or **Rows**) checkboxes on the trigger form |

What runs is resolved the same way everywhere: status gate
(`mandatory` / `active`, widened by `status`) → narrowed by `tags`, or picked by
id with `select` / `only` → ancestors per `include_parents` → `skip` removed
last. Omit `selection` and every row runs; an empty object applies only the
status gate. For a source template the composition then includes **only the
selected streams** — so a stream the chosen sink cannot run (say, one that
needs `upsert` on a file sink) no longer blocks the others.

**Parents and dependencies.** A child stream (`parent:`) or a row with
`depends_on:` needs its ancestors in the run:

| `include_parents` | a selected row whose ancestor is not selected |
|---|---|
| `off` (default) | refused before anything runs, naming every missing ancestor |
| `eligible` | runnable (`mandatory` / `active`) ancestors are pulled in; a parked (`available` / `draft` / `archived`) one is refused |
| `all` | every ancestor is pulled in (parked ones with a warning) |

**What a selection does not change.** State keys are per row
(`{pipeline}::{row}`), so running a subset never touches another row's
bookmark. The run is labelled `selection=<canonical form>` (for example
`select=deal_lines;include_parents=eligible`), which is also part of the
idempotency fingerprint — the same key with a different subset is a `409`, not
a replay. The selection is stored with the run, so a clustered, claimed or
sharded run executes the same subset on whichever instance picks it up, and a
[change request](./approvals.md) fingerprints it: a different subset is a
different change. A topology pipeline (`pipeline.nodes`) has no rows, so any
selection there is refused.

**Errors are `400`s** naming the problem — an unknown row or tag (with the
valid ones listed), an empty run set, a missing ancestor under
`include_parents: off`.

### Listing the rows

Selection is only usable if callers can see the valid names, so every
registered template lists its rows:

```bash
curl -s "localhost:8080/v1/templates/crm/rows?sink=files" -H "Authorization: Bearer $TOKEN"
faucet template rows crm --sink files --store sqlite:./faucet-templates.db
faucet hub rows example-csv --sink jsonl            # a catalog template, unregistered
```

Each row carries, where faucet can derive it:

| group | fields |
|---|---|
| identity | `id`, `kind` (`stream` / `row`), `description`, `status`, `tags`, `default_selected` |
| hierarchy | `parent`, `parent_key`, `children`, `depends_on`, `depth`, `per_parent_record` |
| write | `write.requested`; with a sink `write.resolved`, `write.supported`, `write.offered`, `write.alias_applied`, `write.unsupported_reason`; `primary_keys`, `delete_marker` |
| read | `read.source_kind`, `read.replication` (`full` / `incremental` + field), `read.resumable`, `read.shardable`, `read.supports_discover` |
| guarantees (with a sink) | `guarantees.delivery_guarantee`, `guarantees.cleanup_capable` |
| shape | `shape.schema` (from the contract, `schema_source: contract`), `shape.transforms` (count, kinds, whether any rename or drop fields) |
| inputs | `params_used` |
| state | `state.health`, `last_success`, `last_failure`, `last_error`, `bookmark_age_secs`, `lag` — from [`faucet status`](../reference/cli.md), when the state store is readable |

Rows are expanded with placeholder-bound params, so a required param never
blocks a listing; the state group needs the config's defaults to name its
store (a source template's comes from the sink pairing and an `overlay`). A
topology template returns `rows: []` with `selectable: false`.

**Previewing a selection.** Pass the selection as query parameters (or flags)
and the listing becomes a dry run: each row gains `selected`, `pulled_in`
(with `because`), `blocked` (with the reason) or `excluded`, and the report
gains `run_set` in execution order and the `error` a trigger would return.

```bash
curl -s "localhost:8080/v1/templates/crm/rows?select=deal_lines&include_parents=eligible"
# → {…,"run_set":["deals","deal_lines"],
#     "rows":[…,{"id":"deals","pulled_in":{"because":"deal_lines"},…},
#                {"id":"deal_lines","selected":true,…}]}
faucet template rows crm --select deal_lines --store sqlite:./faucet-templates.db
#   ✗ deals   … blocked: required by `deal_lines` (parent) but not selected — include_parents is off; …
```

The MCP `list_template_rows` tool returns the same report.

## Triggering over HTTP

Point `faucet serve --history` at the same store and the same templates become
triggerable over HTTP and MCP — one registry, not three:

```bash
faucet serve --history sqlite:./faucet-templates.db --auth-token "$TOKEN"
```

```bash
# Register (admin / TemplateAdmin)
curl -sX POST localhost:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"id":"tenant-sync","config":"'"$(sed 's/"/\\"/g;:a;N;$!ba;s/\n/\\n/g' tenant-sync.yaml)"'"}'

# Browse (viewer+ / TemplateRead)
curl -s localhost:8080/v1/templates            -H "Authorization: Bearer $TOKEN"
curl -s localhost:8080/v1/templates/tenant-sync -H "Authorization: Bearer $TOKEN"

# Trigger (operator+ / RunWrite)
curl -sX POST localhost:8080/v1/templates/tenant-sync/runs \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"params":{"tenant_id":"acme","api_token":"'"$TOKEN"'"},"env":{"API_HOST":"eu.example.com"}}'
# → 202 {"run_id":"…","status":"queued","template_id":"tenant-sync",
#        "template_version":1,"params":{"tenant_id":"acme","api_token":"***",…}}
```

```bash
# A source template names its sink; params are the union of both halves.
curl -sX POST localhost:8080/v1/templates/acme%2Fbilling/runs \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"sink":"faucet-hq/bigquery","sink_version":"stable","params":{"api_token":"…","bq_project":"my-project"}}'
# → 202 {…,"template_id":"acme/billing","template_version":1,"sink_template":"faucet-hq/bigquery",
#        "sink_template_version":1,"streams":[{"stream":"bills","chosen":"overwrite",…},…]}
curl -s "localhost:8080/v1/templates?kind=source-template" -H "Authorization: Bearer $TOKEN"
```

A trigger is submitted through the same path as `POST /v1/runs`, so idempotency
keys, `doctor_first`, queue limits, cluster dispatch, metrics, and the audit log
all behave identically. The run is labelled `template` and `template_version`
(and `sink_template` / `sink_template_version` for a composed run), so
`GET /v1/runs?…` and your dashboards can group by provenance.

See the [HTTP API reference](../reference/http-api.md) for the full endpoint list.

## Agent tools (MCP)

With `--mcp`, an agent gets `list_templates` / `get_template` read-only, plus
`register_template` / `run_template` behind `--mcp-allow-mutations` and the
caller's `RunWrite` scope:

```bash
faucet serve --history sqlite:./faucet-templates.db --mcp --mcp-allow-mutations
# or, over stdio:
faucet mcp --template-store sqlite:./faucet-templates.db --allow-mutations
```

Without a store the template tools are not advertised at all, so an agent never
sees a tool it cannot use. `run_template` takes the same `sink` / `sink_version`
pair as the HTTP trigger for a source template (and `overlay` / `overlay_version`
for a deployment overlay), and its `dry_run` output carries the per-stream
write-mode plan and what the overlay set. `list_template_rows` lists a template's rows (and
previews a `selection`); `run_template` and `run_pipeline` take a `selection`
to run a subset.

## What is and isn't stored

The config body is stored **verbatim**. `${env:…}` / `${vault:…}` / `${secret:…}`
stay unresolved tokens and are resolved *at trigger time*, on the instance that
runs the pipeline — the same privilege surface as any normally-submitted config.
That is the recommended way to get a credential into a template: reference it from
the body, don't pass it as a param.

A caller-supplied `secret: true` param value is never persisted. It lives only for
the duration of one trigger: bound into the materialized config, registered for
redaction, and echoed back as `"***"`.

One consequence is worth stating plainly: a **clustered** server persists the
materialized config so a peer can execute the run, which would put a secret param
value in the shared history database. A clustered trigger that *supplies* a value
for a `secret: true` param is therefore refused with a `422` explaining the two
safe alternatives (reference the secret from the body, or trigger on a
non-clustered server). A secret param left at a `default: "${env:VAR}"` is not
refused: the reference stays unresolved in the persisted body and resolves on
the executing instance. Non-clustered servers store no config body and are
unaffected.

## Hosting templates in a repo or bucket (sync)

*(requires the `templates-sync` build feature; S3 / GCS / Azure Blob origins
additionally need `templates-sync-object-store`)*

The registry is where templates *run from*; a repo or bucket is where they are
*authored* — reviewed in a pull request, or dropped in a partner's bucket. A
**sync file** names those **origins**, and `faucet serve --templates-sync` pulls
them into the registry: on start, on demand, and on a per-origin interval.

```yaml
# sync.yaml — see cli/examples/templates/sync.yaml
version: 1
origins:
  - name: platform
    source:
      type: github
      config: { repo: acme/data-templates, ref: main, path: templates/, token: "${env:GITHUB_TOKEN}" }
    prefix: platform-        # this origin owns every `platform-*` id
    launch: follow           # a sidecar's `launch: true` moves `stable`
    prune: deprecate         # removed upstream → deprecated here (never deleted)
    interval_secs: 300
  - name: partner
    source:
      type: s3
      config: { bucket: acme-templates, prefix: shared/, region: us-east-1 }
    prefix: partner-
```

```bash
faucet serve --history sqlite:./faucet.db --templates-sync sync.yaml
faucet template sync    --store sqlite:./faucet.db --config sync.yaml --dry-run   # plan only
faucet template sync    --store sqlite:./faucet.db --config sync.yaml --origin platform
faucet template publish platform-nightly --store sqlite:./faucet.db --config sync.yaml --origin platform
```

Over HTTP the same two verbs are `POST /v1/templates/sync` (`{origin?, dry_run?}`)
and `POST /v1/templates/{id}/publish` (`{origin, version?}`), both
`TemplateAdmin` (admin) and audited as `template.sync` / `template.publish`; the console's
Templates page grows a **Sync from origins** panel when the server has any.

**Layout at an origin.** The template id is the file **stem**, with the origin's
`prefix` prepended: `templates/nightly.yaml` under `prefix: platform-` registers
as `platform-nightly`. Only `*.yaml` / `*.yml` / `*.json` files directly in the
directory — plus one level of owner directories, whose files register as
`owner/name` — are read, on GitHub and object-store origins alike; a GitHub
origin may name several directories with `paths: [source-templates,
sink-templates]` instead of one `path` (a Template Hub catalog — stems must be
unique across them, and `publish` writes each template to the directory named
for its kind: `source-templates`, `sink-templates`, `deployments`; a pipeline
goes to the first). A GitHub pull resolves `ref` to one commit and reads every
file at that commit, so a push during the pull cannot mix two revisions; a
directory past the contents API's 1000-entry limit is read from the git tree. An optional sidecar `<stem>.faucet.yaml` beside the template
carries release intent, kept out of the config body so the body stays runnable
with `faucet run`:

```yaml
# nightly.faucet.yaml
description: Nightly account sync
launch: true          # make this version `stable` on pull (honoured under `launch: follow`)
tags: [staging]       # assignable channels to point at the pulled version
```

**What a pull does — and never does.**

| Upstream state | Registry action |
|---|---|
| New file | `register` a version (launched only if the policy says so) |
| Body changed (comments/whitespace ignored — the canonical body is hashed) | `register` the next version |
| Body unchanged | nothing — re-pulling is free and never inflates the version counter |
| Unchanged, but `stable` lags the policy (`always`, or a sidecar flipped `launch`) and that version was never launched | `launch` the existing version |
| Unchanged, but an operator rolled back off it (the launch log already holds it) or the version is retired | nothing — a pull never undoes a rollback; a new body is a new version and is launched as usual |
| File removed | `prune: keep` → reported as orphaned; `prune: deprecate` → deprecated |
| Removed file returns | under `prune: deprecate` the deprecation the origin placed is lifted; a deprecation a person placed is left alone, and nothing is launched into that template until they lift it |
| Bad id / unparseable body / bad or unreadable sidecar / ambiguous stem / a file that could not be read this pull | skipped and reported — one broken file never blocks the origin, and it still counts as upstream, so `prune: deprecate` never retires it |

A pull **only appends**: nothing is overwritten and nothing is deleted (a delete
would cascade to the launch log and silently repoint `stable`). Under the default
`launch: ignore` a pull moves nobody — exactly like a manual `register`;
`follow` lets the sidecar decide; `always` is GitOps mode, where merging upstream
is the release. Every register is attributed (`created_by: sync:<origin>`, or the
principal who called the HTTP endpoint). Pulls of one origin are serialized in a
process, and a body another pull registered since the plan was made is not
registered again, so a rollback always has a different version to land on.

`${env:…}` / `${file:…}` / `${secret:…}` in the sync file resolve inside each
value after it is parsed — an environment value can never add keys. A value that
is exactly one directive may resolve to a number (`interval_secs:
${env:SYNC_EVERY}`).

**One owner per template.** Each origin owns the id namespace named by its
`prefix`; two origins with overlapping prefixes (including an empty prefix beside
any other) are refused when the file loads, so "who wins" never has to be decided
at runtime, and an origin never touches ids outside its prefix.

**Publish is manual.** `faucet template publish <id> --origin X [--version stable]`
writes one registered version back as `<id minus prefix>.<yaml|json>` — a
deliberate operator step (audited), never automatic, so the registry can never
overwrite a reviewed file on its own. The next pull sees the identical body and
plans `unchanged`.

**Credentials.** GitHub uses the contents API — no git binary; a private repo
needs only a `token` (use `${env:…}` / `${secret:…}`; the value is registered for
log redaction). Object-store origins use the SDK default chain (`AWS_*`,
Application Default Credentials, `AZURE_*`), like the trigger watchers. A
transport failure on the initial pull is logged and counted, not fatal — the
server comes up on the registry it has; an invalid sync file *is* fatal.

Metrics: `faucet_serve_template_sync_runs_total{origin,outcome=ok|partial|error}`,
`faucet_serve_template_sync_mutations_total{origin}`,
`faucet_serve_template_sync_last_unix_seconds{origin}`. Schema:
`faucet schema templates-sync`.

## Safety properties

- **Structure safety.** Params are substituted per JSON/YAML scalar, before the
  typed parse — a value containing `:`, a newline, or `-` stays the single scalar
  it replaced and can never inject a key or an array element. SQL-bound and
  JSON-safe substitution paths downstream are untouched, so the existing
  SQL/JSON-injection guarantees hold for param-derived text too.
- **No re-interpolation of caller input.** Env/file/secret directives resolve
  *before* params bind, so a supplied value is never itself scanned for
  directives. A supplied value containing `${` is rejected outright: params are
  data, not directives.
- **Typos fail loudly.** An undeclared `--param`, an undeclared `${param.x}`
  reference, a missing `required` param, or a type mismatch is an error naming the
  param — never a silent no-op.
- **Nothing leaks by accident.** `${param.*}` binding happens pre-parse on every
  load path, and a token that somehow survived to matrix expansion is rejected
  there as a backstop rather than reaching a connector as literal text.

## Build features

| Feature | Enables |
|---|---|
| *(none)* | The `params:` block, `${param.*}`, `--param` / `--param-env`, `faucet schema params` |
| `templates` | `faucet template …`, `/v1/templates*`, the MCP template tools (implies `serve`) |
| `templates-sync` | `faucet template sync\|publish`, `faucet serve --templates-sync`, `POST /v1/templates/sync` + `/{id}/publish`, GitHub origins (implies `templates`) |
| `templates-sync-object-store` | S3 / GCS / Azure Blob origins (implies `templates-sync`; pulls `object_store`) |
| `serve-history-sqlite` / `serve-history-postgres` | A registry that survives a restart |

`templates` is in `--features full`, not in `default`.

## See also

- Runnable example: [`cli/examples/rest_to_jsonl_templated.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_jsonl_templated.yaml)
  and its suite [`cli/examples/tests/template_suite.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/tests/template_suite.yaml);
  sync file [`cli/examples/templates/sync.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/templates/sync.yaml);
  [`cli/examples/templates/hub-sync.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/templates/hub-sync.yaml)
  mirrors the [public Template Hub](./template-hub.md#the-public-hub) with one GitHub origin whose
  `paths: [source-templates, sink-templates]` reads both catalog directories
- [RFC 0006 — template hosting + sync](https://github.com/faucet-hq/faucet-stream/blob/main/rfcs/0006-template-hosting-sync.md)
- [`params:` reference](../reference/config.md#params) · [CLI reference](../reference/cli.md) · [HTTP API](../reference/http-api.md)
- [Config composition](./composition.md) — `extends` / `profiles` / `!include`, for
  variation that is *static* rather than per-run
- [Event-driven triggers](./triggers.md) — firing runs on object arrival, a
  webhook, or queue depth
