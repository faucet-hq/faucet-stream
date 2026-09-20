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
      url: "https://api.example.com/tenants/${param.tenant_id}/events?since=${param.since}"
      auth: { type: bearer, config: { token: "${param.api_token}" } }
      pagination:
        type: page_number
        config: { page_param: page, size_param: per_page, size: "${param.page_size}" }
  sink:
    type: jsonl
    config:
      path: "./out/${param.tenant_id}/events.jsonl"
```

Fields per entry:

| Field | Meaning |
|---|---|
| `type` | `string` (default) · `int` · `float` · `bool` |
| `required` | The caller must supply a value. Mutually exclusive with `default`. |
| `default` | Value when the caller supplies none. An ordinary config scalar, so `default: "${env:SINCE}"` works. |
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
concurrent server.

```bash
faucet run tenant-sync.yaml --param tenant_id=acme --param-env API_HOST=eu.example.com
```

## Registering a template

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

#### In the console

The web console (`serve-ui`) has a **Templates** view built around exactly this:
a list showing each template's status, live version, and build tip, and a
per-template **versions page** with one row per version, the channels currently
pointing at it, an assign-channel dropdown, and Launch / Config / Delete —
plus Roll back, Deprecate, and a typed trigger form generated from the template's
`params:`.

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
template for a combination exactly as a real trigger would, then expands it and
compiles each row's transform chain (in topology mode it validates the graph
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
- Duplicate case names are rejected (they make `--filter` ambiguous).

`--filter '<pattern>'` runs a subset (`*` wildcards; a bare name is an exact
match, so `--filter auto` does *not* match `auto:defaults`). `--json` emits the
machine-readable report. The exit code is the failed-case count, mirroring
`faucet test`, so CI gates on it without parsing output. `faucet schema
template-test` prints the suite schema.

## Triggering over HTTP

Point `faucet serve --history` at the same store and the same templates become
triggerable over HTTP and MCP — one registry, not three:

```bash
faucet serve --history sqlite:./faucet-templates.db --auth-token "$TOKEN"
```

```bash
# Register (operator+ / TemplateWrite)
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

A trigger is submitted through the same path as `POST /v1/runs`, so idempotency
keys, `doctor_first`, queue limits, cluster dispatch, metrics, and the audit log
all behave identically. The run is labelled `template` and `template_version`, so
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
sees a tool it cannot use.

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
value in the shared history database. A clustered trigger of a template declaring
`secret: true` params is therefore refused with a `422` explaining the two safe
alternatives (reference the secret from the body, or trigger on a non-clustered
server). Non-clustered servers store no config body and are unaffected.

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
| `serve-history-sqlite` / `serve-history-postgres` | A registry that survives a restart |

`templates` is in `--features full`, not in `default`.

## See also

- Runnable example: [`cli/examples/rest_to_jsonl_templated.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_jsonl_templated.yaml)
  and its suite [`cli/examples/tests/template_suite.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/tests/template_suite.yaml)
- [`params:` reference](../reference/config.md#params) · [CLI reference](../reference/cli.md) · [HTTP API](../reference/http-api.md)
- [Config composition](./composition.md) — `extends` / `profiles` / `!include`, for
  variation that is *static* rather than per-run
- [Event-driven triggers](./triggers.md) — firing runs on object arrival, a
  webhook, or queue depth
