---
name: faucet-templates
description: >-
  Use when working with faucet Template Hub templates: running a hub template
  with `faucet run --source <owner>/<name> --sink <owner>/<name>`, picking
  between several published templates for the same system, composing a source
  template with a sink template and checking the pairing, adding a state store
  or DLQ to a composed run with a deployment overlay, pinning a template
  version, writing a new source-template for an HTTP or GraphQL API (auth,
  pagination, incremental streams, write preferences), writing a sink-template,
  adding streams or params to a template, testing a template with a
  `faucet template test` suite or recorded replays, versioning a template
  (stable, preview, deprecating a version), or publishing a template to the
  public catalog github.com/faucet-hq/template-hub or a private catalog.
license: MIT OR Apache-2.0
---

# faucet Template Hub templates

The Template Hub splits a pipeline in two. A **source-template** describes how
to read one system once (connector, auth, pagination, shared transforms, and a
list of **streams**, each a destination table with the write modes it
accepts). A **sink-template** describes one destination and how each stream is
addressed. `faucet run --source X --sink Y` composes the two into an ordinary
pipeline config at run time, picking for each stream the first write mode the
sink supports. A third document, a `kind: deployment` overlay, adds the
operational blocks (state, DLQ, alerts) that belong to neither template.

How composition, overlays, namespaces, versions and trust signals work:
[Template Hub](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html).
Params, the template registry and suites:
[Parameters & pipeline templates](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html).

`faucet hub ...` works on template files in a catalog. `faucet template ...` is
a different tool: the registry that `faucet serve` triggers by id. Publishing
to the hub never involves `faucet template register`; the one shared piece is
`faucet template test`, the suite runner.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

## Where the facts come from

| Need | Command |
|---|---|
| Document shapes (source template, sink template, overlay, suite) | `faucet schema source-template`, `faucet schema sink-template`, `faucet schema deployment`, `faucet schema template-test` |
| A param entry's shape | `faucet schema params` |
| Connector config inside a template | `faucet schema source <type>`, `faucet schema sink <type>` |
| A transform's config | `faucet schema transform <name>` |
| Hub verbs and their flags | `faucet hub --help`, `faucet hub <verb> --help` |
| Run-time flags (hubs, overlays, params, row selection, trust) | `faucet run --help` |

`faucet template` and `faucet schema template-test` exist only in builds that
include the template registry; if they are missing, use the release binary or
the container image (Step 0).

## Workflow 1: run a template

1. **Find it.**
   ```bash
   faucet hub list
   faucet hub list --sort stars
   faucet hub matrix
   faucet hub rows faucet-hq/example-rest-api
   ```
   When several namespaces publish the same system, prefer the official
   `faucet-hq/` one, then compare the trust signals `faucet hub list` shows
   ([Choosing between variants](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#choosing-between-variants-stars-and-trust)).
   Stars measure popularity, not correctness. Read the README the catalog keeps
   beside the template (required scopes, run times, changelog) before picking
   credentials.

2. **Check the pairing** before running anything:
   ```bash
   faucet hub check --source faucet-hq/example-rest-api --sink faucet-hq/postgres
   ```
   It prints each stream's chosen mode and a ready `faucet run` line listing the
   required params, and exits non-zero when a stream has no viable mode. When a
   stream cannot run on that sink, pick another sink or run a subset of streams
   (the row-selection flags in `faucet run --help`).

3. **Add operations with an overlay.** Incremental streams need a durable state
   store or they re-read everything every run (`faucet validate` warns). Write
   a `kind: deployment` file (`faucet schema deployment` lists what it may set;
   [Deployment overlays](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#deployment-overlays)
   shows one) and pass it with `--overlay`. Keep the state DSN a `secret: true`
   param.

4. **Validate offline, then run against a local sink first.**
   ```bash
   faucet validate --source faucet-hq/example-rest-api --sink faucet-hq/sqlite --show-composed
   faucet run --source faucet-hq/example-rest-api --sink faucet-hq/jsonl \
     --param base_url=https://api.example.com/v1 --param api_token="$API_TOKEN" --limit 100
   faucet run --source faucet-hq/example-rest-api --sink faucet-hq/postgres \
     --overlay ops/prod.yaml --param base_url=https://api.example.com/v1 \
     --param api_token="$API_TOKEN" --param pg_url="$PG_URL"
   ```
   Secrets come from a secret store into environment variables, and the shell
   expands them into `--param`. Param values are literal: never pass a
   `${env:...}` string as a param value.

5. **Pin for production.** Use an exact version (`--source <owner>/<name>@<N>`)
   and move the pin on purpose after reading the template's changelog.
   Selectors need a catalog with version history (the public hub); a local
   directory hub refuses them.

To keep or review the generated config,
`faucet hub compose --source X --sink Y --out composed.yaml` writes it; it is an
ordinary config from then on.

## Workflow 2: author a source template

1. **Start from the closest existing template** with the same connector, auth
   and pagination style (`faucet hub list`, then read it in the catalog). The
   skeleton is `faucet-hq/example-rest-api`
   ([hub/source-templates/faucet-hq/example-rest-api.yaml](https://github.com/faucet-hq/faucet-stream/blob/main/hub/source-templates/faucet-hq/example-rest-api.yaml)).
   A fuller, tested example with an incremental stream, a request-side
   bookmark bind, rate-limit handling, a closed-set param, a suite and a
   recorded replay is in [examples/](examples/source-templates/acme/example-api.yaml).
   Put the file at `source-templates/<your-login>/<name>.yaml` with
   `owner: <your-login>` and `name:` equal to the file stem.

2. **Read the schemas instead of guessing field names.**
   ```bash
   faucet schema source-template
   faucet schema source rest
   faucet schema source graphql
   ```
   The patterns for each piece are documented once:
   [Authentication](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html)
   (inline auth, or a shared `auth:` catalog entry referenced with `auth: { ref }`),
   [Pagination styles](https://faucet-hq.github.io/faucet-stream/cookbook/pagination.html),
   [Incremental replication & state](https://faucet-hq.github.io/faucet-stream/cookbook/state.html)
   (replication keys, request-side bookmark binds, GraphQL variables),
   [Resilience](https://faucet-hq.github.io/faucet-stream/cookbook/resilience.html)
   (throttling signalled in a response), and the REST source's
   [README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/source/rest/README.md)
   (date-window slicing, async jobs, discovery).

3. **One stream per destination table.** Give each a stream `name`, a
   `source.config` override (usually the endpoint path), `primary_keys` and a
   `write` preference list. How to choose them, and the raw-versus-transformed
   key naming trap:
   [A source template](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#a-source-template).
   Shaping every sink needs goes in the top-level `transforms`.

4. **Make every host and every secret a param.** The API root is a param that
   defaults to the public URL (tests and replays point it at a local server).
   Every credential is `${param.NAME}` with `secret: true` and no default.
   Check each param's type against the field it fills: `faucet validate`
   rejects, for example, a number where the connector wants a string.

5. **Lint, check, validate** from the catalog root:
   ```bash
   faucet hub lint --hub .
   faucet hub check --hub . --source <your-login>/<name> --sink faucet-hq/jsonl
   faucet hub check --hub . --source <your-login>/<name> --sink faucet-hq/bigquery
   faucet validate --hub . --source <your-login>/<name> --sink faucet-hq/postgres
   ```
   Fix every lint finding; the catalog's CI runs the same lint and composes
   every pairing.

6. **Test** it (Workflow 4).

## Workflow 3: author a sink template

Copy the closest sink in the catalog (`faucet hub list` shows sinks and the
write modes each supports), then read `faucet schema sink-template` and
`faucet schema sink <type>`. Prefix params (`pg_`, `bq_`) so they cannot
collide with a source's. Address each stream through `per_stream` (its
substitution tokens are on the
[Template Hub](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#a-sink-template)
page). Declare a `write_mode_aliases` entry only when the destination satisfies
a mode by construction, and read that page's rules on aliases and child streams
first. Check it against several sources:

```bash
faucet hub lint --hub .
faucet hub check --hub . --source faucet-hq/example-rest-api --sink <your-login>/<name>
faucet hub matrix --hub .
```

## Workflow 4: test a template

1. **Suite.** Write `tests/<owner>/<name>/suite.yaml` (shape:
   `faucet schema template-test`; walkthrough:
   [Testing the parameter space](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#testing-the-parameter-space);
   example: [examples/tests/](examples/tests/acme/example-api/suite.yaml)). A
   suite for a source template names a sink. Turn on the `auto` cases so every
   required param, every closed value set and the all-defaults combination is
   covered, add explicit cases for the params that matter, and add
   `behavioral` cases that feed records through the template's transforms and
   assert the exact output. Use fake values for secret params.
   ```bash
   faucet template test tests/<owner>/<name>/suite.yaml
   ```
   The exit code is the number of failed cases.

2. **Recorded replay** (in the catalog repository). The public catalog's
   `scripts/replay.py` serves recorded HTTP exchanges and compares each
   stream's output with `expected/<stream>.jsonl`; it is the only offline test
   of pagination, auth headers and incremental binds. Record from the API
   reference or a sandbox account, run twice so the second run proves the
   bookmark advanced, and replace every id, name, email and token with
   synthetic values. Its format is in the catalog's
   [CONTRIBUTING](https://github.com/faucet-hq/template-hub/blob/main/CONTRIBUTING.md);
   [examples/tests/](examples/tests/acme/example-api/replay.yaml) has a
   passing pair.

3. **Live smoke test** once against the real API into a local sink, then a
   second run with a file state overlay, which should request only newer
   records.

## Workflow 5: version and publish

The catalog's
[CONTRIBUTING](https://github.com/faucet-hq/template-hub/blob/main/CONTRIBUTING.md)
is the source of truth for the PR flow; the versioning model (computed
versions, `stable`, previews, deprecation) is on the
[Template Hub](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#the-public-hub)
page.

1. Fork faucet-hq/template-hub and touch only your namespace: the template,
   its README (`<name>.md`: scopes, run times, changelog), an optional sidecar
   `<name>.faucet.yaml`, the suite and replay under `tests/<owner>/<name>/`,
   and, in the first PR into the namespace, its `OWNERS` file.
2. Run what the catalog's CI runs (lint, every `hub check`, the suite, the
   replay) and regenerate the index as CONTRIBUTING describes:
   ```bash
   faucet hub matrix --hub . --format json --out index.json
   ```
3. Fill in the PR template: the system and a link to its public docs, one line
   per stream (name, write preference, why), and confirm you ran it against
   the real system.

Ship a risky change as a preview (the sidecar keeps `stable` where it is),
move `stable` once it is proven, and retire bad versions in the sidecar with a
reason naming the replacement.

For a private catalog, lay a repository out the same way and point consumers
at it with `--hub` / `--source-hub` and a GitHub token
([A private source with the public sinks](https://faucet-hq.github.io/faucet-stream/cookbook/template-hub.html#a-private-source-with-the-public-sinks)).

## Hard rules

- **No secrets in templates.** Every credential is a `secret: true` param with
  no default. Never write a literal token, a password inside a URL, a private
  hostname, or placeholder text; `faucet hub lint` refuses them.
- **Param values are literal.** Get secrets from a secret store into
  environment variables and let the shell expand them into `--param`.
- **A published version is immutable.** Never rewrite history or edit a
  version in place. Fix forward (the fix is the next version); roll back by
  re-committing the older body and pointing `stable` at it.
- **A change in meaning is a new version.** Renaming or removing a stream or
  param, or changing `primary_keys` or `write`, is never a silent edit; say
  what changed in the template's changelog.
- **Keep `stable` on a proven version.** Never deprecate the stable version;
  move `stable` first.
- **Describe the system, not your company.** Public endpoints only, and a
  correct write preference per stream (keyed modes need `primary_keys`).
- **Sink templates never set `write_mode` or `key`.** The composer injects them
  per stream.
- **Read a community template before trusting it.** One that reads the host's
  environment, files or secret managers is refused unless you trust its owner
  explicitly (`faucet run --help`).
