---
name: faucet-api
description: >-
  Use when integrating an application or service with `faucet serve` over
  HTTP: discovering the server's API and auth model, choosing which role's
  token a caller needs, registering, testing and launching templates over
  HTTP, rolling out a source and sink template change together, triggering
  template runs or raw config runs programmatically with idempotency keys,
  labels and an execution pool, polling a run until it is terminal and mapping
  its status and per-row results, finding runs by label, and decoding a
  401, 403, 409, 422, 429 or 503 response from the API.
license: MIT OR Apache-2.0
---

# Driving `faucet serve` from another service

A calling service talks to `faucet serve` over its `/v1` HTTP API: it
registers templates, triggers runs, polls them and reads their results. This
skill is the procedure for building that client so it is correct, retry-safe
and never leaks a credential.

Standing the server up is the `faucet-deploy` skill; keeping it healthy is
`faucet-operate`; what may go into a request body is `faucet-secrets`.
Writing the templates themselves is `faucet-templates`.

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

The server is a faucet binary too: the API a client may use is the one that
server's release serves. Its version is the `version` label of the
`faucet_build_info` metric on its unauthenticated `/metrics` endpoint.

## Step 1: discover the API from the server's release

Never write a client from memory or from this skill.

1. Read the server's version from `/metrics` (`faucet_build_info`).
2. Read the OpenAPI document for exactly that release: `docs/openapi.yaml` in
   the faucet-stream repository at the tag `faucet-cli-v<version>`
   ([`docs/openapi.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/docs/openapi.yaml)
   on `main` is the newest). A CI test keeps it in step with the router, so
   every path, request field, response field and status code is there.
3. Read the prose for each endpoint you use in the
   [HTTP API reference](https://faucet-hq.github.io/faucet-stream/reference/http-api.html),
   and the security model in
   [Running faucet as a service](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html).
4. Generate the client from the OpenAPI document where your language allows,
   so a field rename fails at build time rather than in production.

## Step 2: pick the token the caller needs

The server has roles, and each route needs a permission
([role × route matrix](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#role--route-matrix)).

1. List the endpoints the caller uses, and look each one up in the matrix.
2. Give the caller the **least** role that covers them. A service that only
   triggers and polls runs needs the operator role, not admin; template
   registration and launch need admin, so keep them in a release pipeline
   rather than in the runtime service.
3. With the token in hand, call `GET /v1/whoami`: it returns the principal,
   role and permissions the server actually granted. Check it at the
   client's start-up and fail loudly on a mismatch.
4. Pass the token as an `Authorization: Bearer` header from a secret store,
   never from code or a log line.

## Step 3: register and launch templates (release pipeline)

Templates are registered once and triggered many times
([Registering a template](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#registering-a-template)).

1. Before registering, run the template's tests locally with
   `faucet template test <file>` and `faucet hub check` (the
   `faucet-templates` skill).
2. Register by `POST /v1/templates`. Registration runs the version's `tests:`
   bundle and records the result; read it in the response.
3. A source template's tests compose with a sink template, and the reverse.
   Each half's test cases must carry the params of the companion they run
   with. For a brand-new pair, register both and then launch both: a
   companion with no launched version is tested at its newest, and
   registering the second half re-tests the first
   ([Bootstrapping a new source + sink pair](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#bootstrapping-a-new-source--sink-pair)).
4. When a source and a sink template change together, register first the half
   the other's tests depend on, then the other, then re-run the first half's
   tests (`POST /v1/templates/{id}/versions/{version}/test`) before launching.
5. Launch moves only onto a passing version
   ([Labels need passing tests](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#labels-need-passing-tests)).
   A refusal is a `422` whose details list the failing cases: fix the
   template, do not look for an override.
6. **Roll out the template before the caller.** A trigger with a param the
   launched version does not declare is refused. Launch the template version
   that accepts the new params first, then deploy the calling code that sends
   them; remove a param in the reverse order.

## Step 4: trigger runs

Trigger a template by `POST /v1/templates/{id}/runs`, or submit a whole config
by `POST /v1/runs` ([Triggering over HTTP](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#triggering-over-http)).
Read the exact request fields from the OpenAPI document. For every trigger:

1. **Validate first.** Before triggering the first run of a batch, check every
   unit's params against the template's declared params
   (`GET /v1/templates/{id}` returns them), so a batch fails before any run
   starts instead of halfway through.
2. **Idempotency key.** Derive it from the caller's own job identity (job id
   plus attempt-independent unit id), never a random value, so a retried
   request replays the original run instead of starting a second one. A reused
   key with a different request is a `409`
   ([Idempotency](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#idempotency)).
3. **Labels.** Put the caller's own ids (tenant, job, unit) in the run's
   labels: they are how you find the run later, and they are stored on the
   run record.
4. **Pool.** On a cluster with
   [execution pools](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#execution-pools),
   name the pool when the run must execute on particular replicas (a private
   network, an allowlisted egress IP). A pool no live member serves is
   refused, so treat that `422` as a deployment problem, not a retry.
5. **Credentials.** Follow the `faucet-secrets` skill: a non-secret param is
   echoed in the response and stored with the run.

## Step 5: poll and map results

1. Poll `GET /v1/runs/{id}` with exponential backoff and a deadline, until
   the status is terminal. On a cluster a new run first shows as pending.
2. Map the server's statuses (listed in the OpenAPI document) to the caller's
   own states once, in one function.
3. Read each invocation's own status, error and row accounting
   ([`GET /v1/runs/{id}`](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#get-v1runsid--runrecord)):
   one failed stream does not hide the others, so report per stream.
4. To find runs again (a lost run id, a dashboard, a reconciliation job),
   list them with `GET /v1/runs?label=key:value`, repeated to AND several
   labels
   ([`GET /v1/runs`](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#get-v1runs)).
   An unknown query parameter is a `400`, so a typo cannot silently match
   every run.
5. Prefer a completion callback over tight polling when the caller can
   receive one
   ([Completion callbacks](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#completion-callbacks)).

## Step 6: decode an error

Every error has the same envelope
([Error envelope](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#error-envelope)).
Read `error.code`, `error.message` and `error.details`; the message names the
cause. Decide by status:

| Status | Meaning for the client | Next step |
|---|---|---|
| `401` | Token missing or wrong | Fix the secret; never retry blindly |
| `403` | Role lacks the permission, or the body reads the server's own credentials | `GET /v1/whoami`, then Step 2 |
| `409` | Idempotency key reused with a different request, a row already running, or an approval gate | Read the message; a changed request needs a new key |
| `422` | The request is wrong | Read the message (below); do not retry unchanged |
| `429` | Queue full | Retry after `Retry-After` |
| `503` | Draining, or history degraded (an idempotency-keyed submit is refused) | Retry with backoff; see `faucet-operate` |

Common `422` causes, each named in the message: a param the template does not
declare (Step 3.6), a required param missing, a value outside a param's
closed set, a literal credential or a credential-named param that is not
`secret: true` in a registered template, a `secret: true` value sent to a
clustered server without a vault key (`faucet-secrets`), a pool nobody
serves, a launch onto a failing version. Fix the request or the template,
then send a new request.

## Client checklist

- [ ] Endpoints and fields generated or checked against the server release's
      OpenAPI document
- [ ] Least-privilege token; `GET /v1/whoami` checked at start-up
- [ ] Connect and request timeouts on every call
- [ ] Retries only on `429`, `503` and transport errors, and only for calls
      that carry an idempotency key or are reads
- [ ] Idempotency key derived from the caller's job identity
- [ ] The caller's ids in run labels
- [ ] Params validated for every unit before the first trigger
- [ ] Template version launched before the caller code that needs it
- [ ] Request and response bodies never logged (`faucet-secrets`)
