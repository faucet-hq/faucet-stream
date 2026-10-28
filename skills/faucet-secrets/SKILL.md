---
name: faucet-secrets
description: >-
  Use when a faucet config, template, run request or trigger carries a
  password, key, token or connection string; choosing between a secret
  reference (`${env:…}`, `${file:…}`, a secrets manager), a `secret: true`
  template param and a tenant connection; passing a per-run credential to a
  clustered `faucet serve`; reviewing a template, request or log for leaked
  or credential-named values; a run response, run history, audit entry or log
  that shows a credential; or a credential that has already been exposed and
  needs rotating.
license: MIT OR Apache-2.0
---

# Credential hygiene with faucet

A credential can reach four places it should not: a run response, the run
history database, a log line, and the agent's own tool output. This skill is
how to choose the mechanism that keeps it out of all four, and what to do
when one slips through.

Writing the pipeline is `faucet-pipelines`; building a client for the server
is `faucet-api`; the server's deployment is `faucet-deploy`.

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

## Agent safety rules (always)

1. **Never print a credential.** Do not echo a run response, a request body,
   a config with resolved values, or a log at debug level into tool output
   when it may hold one. Read only the fields you need (`jq` the status and
   ids), or redact first.
2. **Redact before writing to a file**, not after: a file is copied, committed
   and uploaded before anyone cleans it.
3. **Never paste a credential into a config or template** to make it work.
   Use one of the mechanisms below.
4. **If a credential is exposed anyway**, stop. Tell the user what was
   exposed, where (which response, file, log or commit) and to whom, and
   recommend rotating it. Do not continue the task as if nothing happened,
   and do not try to scrub history silently.

## Step 1: know where each kind of value goes

Read these pages for the current behaviour; do not rely on memory:

- [What is and isn't stored](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#what-is-and-isnt-stored):
  a registered body is stored verbatim, references stay unresolved, a
  non-secret param value is stored with the run and echoed in the trigger
  response, a `secret: true` value is redacted and never persisted on a
  single node.
- [Per-run secret template params need a vault key](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#3-per-run-secret-template-params-need-a-vault-key):
  how a clustered server seals `secret: true` values, and what it refuses.
- [Secrets managers](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html)
  and its
  [redaction boundary](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html#redaction-guarantee-and-its-boundary):
  which output faucet scrubs and which it cannot.
- [Server credentials](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#server-credentials):
  a submitted config may not read the server's own secrets.

The rule of thumb: only a **non-secret param** value ends up in plain text in
a response and in history. Everything else is either a reference resolved at
run time or a redacted value.

## Step 2: choose the mechanism

| Situation | Mechanism |
|---|---|
| One credential per deployment (the service's own database, warehouse or API key) | A reference in the config or template body (`${env:…}`, `${file:…}` or a secrets manager), resolved on the instance that runs it |
| A credential per run or per customer, single-node server | A `secret: true` template param |
| A credential per run or per customer, clustered server | A `secret: true` template param, with the same `--vault-key` on every member; see the cluster page above for sealing and key rotation |
| The same, clustered, no vault key | Refused with a `422` naming the key. Configure the key, or reference the credential from the body, or use a non-clustered server |
| A customer's long-lived connection (OAuth refresh token, database login) on a multi-tenant server | A sealed tenant connection, referenced by the run's `auth: { ref }` ([Connections](https://faucet-hq.github.io/faucet-stream/cookbook/embedded-integrations.html#connections)) |

Confirm the flags and param fields on the binary: `faucet serve --help`,
`faucet schema params`. Never work around a refusal by renaming a secret
param to something that does not look like a credential, or by dropping
`secret: true`: that is exactly the case that leaks. If the user asks for a
workaround, explain what would be stored where, and proceed only with their
explicit sign-off.

## Step 3: review pass

Run this over every template, request body, script and log you touch:

1. **Templates.** `faucet hub lint` (for hub templates) refuses literal
   credentials under credential-named keys and credential-named params that
   are not `secret: true`; registering a source or sink template applies the
   same lint. Run it, and read every param: anything that names a key, token, password,
   secret, certificate or connection string must be `secret: true` or come
   from a reference.
2. **Request bodies and scripts.** Search for credential-shaped values:
   private-key headers, long base64 or hex strings, `user:password@` in
   URLs, bearer tokens. Each one must come from a secret store at run time.
3. **Logs.** Run with `--log-format json` at `info` or quieter; never debug
   level on a run whose connectors hold resolved credentials (the redaction
   boundary above explains why).
4. **History.** On a server, `GET /v1/runs/{id}` shows what was stored for a
   run; a non-secret param there that is a credential is a leak. Rotate it,
   then fix the template.

```bash
faucet validate pipeline.yaml --no-secrets
faucet schema params
```

`--no-secrets` checks the structure without fetching any credential, so the
agent never sees resolved values while iterating on a config.

## If a credential leaked

1. Stop and tell the user (safety rule 4).
2. Rotate the credential at its issuer; deleting the run or the log is not
   enough, copies already exist.
3. Find the cause: which mechanism carried it (Step 2) and why the review
   pass missed it (Step 3).
4. Fix the template or the caller, and delete the run record that holds the
   old value (`DELETE /v1/runs/{id}`) once it is rotated.
