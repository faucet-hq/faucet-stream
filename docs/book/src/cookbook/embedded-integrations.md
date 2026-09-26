# Embedded integrations (multi-tenant)

A product that syncs *its customers'* data — "connect your CRM", "sync your
warehouse" — needs one engine running the same pipeline for many customers,
each with their own credentials, bookmarks and destination. `faucet serve`
does that with **tenants**:

- A tenant owns **connections**: credentials sealed with AES-256-GCM under the
  server's vault key, stored by the API or created through a hosted OAuth
  connect flow.
- A run started **for a tenant** resolves `auth: { ref }` against that
  tenant's connections, binds `${tenant.*}` tokens, keeps its state under the
  tenant, and is held to the tenant's limits.
- A principal can be **confined to one tenant**, so the product's backend can
  hold a per-customer token.
- A template can be **fanned out** across every tenant, on demand or on a cron
  schedule.

Build with the `tenants` feature (included in `full`):

```bash
cargo install faucet-cli --features "tenants,serve-history-sqlite,schedule,triggers"
```

## Start a server with a vault key

```bash
export FAUCET_VAULT_KEY="$(openssl rand -hex 32)"   # from your secrets manager in production
faucet serve --auth-config auth.yaml --history sqlite:./faucet.db \
  --connect-providers providers.yaml
```

The vault key seals every stored credential; without it the server refuses to
store or open connections (`503`). To rotate, start with the new key and pass
the old one as `--vault-previous-key` until every connection has been
re-stored. Keep the history database and the key apart: the database alone
holds only ciphertext.

## Tenants

```bash
curl -X POST localhost:8080/v1/tenants -H "Authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' -d '{
    "id": "acme",
    "name": "Acme Corp",
    "labels": {"region": "eu"},
    "limits": {"max_concurrent_runs": 2, "max_records_per_run": 5000000},
    "notifications": [
      {"name": "acme-ops", "on": ["connection_needs_reauth"],
       "channel": {"type": "webhook", "config": {"url": "https://product.example/hooks/faucet"}}}
    ]
  }'
```

| Field | Meaning |
|---|---|
| `id` | A slug: `^[a-z0-9][a-z0-9_-]{0,62}$`. The first segment of the tenant's state keys. |
| `name`, `labels` | What `${tenant.name}` and `${tenant.labels.<key>}` read. |
| `limits.max_concurrent_runs` | Runs queued or running at once; the next submission is a `429`. |
| `limits.max_records_per_run`, `max_bytes_per_run`, `max_duration_secs` | Joined into every run's [budget](./usage.md#run-budgets): a noisy tenant stops at the page boundary. |
| `notifications` | The config `notifications:` shape, for tenant-level events (`connection_needs_reauth`). |

`PATCH /v1/tenants/{tenant}` updates any field; `{"suspended": true}` refuses
the tenant's runs (`409`) until it is resumed.

## Connections

A connection is an [`auth:` catalog](./auth.md) entry that belongs to a
tenant — the same `{ type, config }` shape:

```bash
curl -X POST localhost:8080/v1/tenants/acme/connections -H "Authorization: Bearer $OPERATOR" \
  -H 'content-type: application/json' -d '{
    "name": "crm",
    "provider": {"type": "oauth2_refresh", "config": {
      "token_url": "https://idp.example/oauth/token",
      "client_id": "…", "client_secret": "…", "refresh_token": "…"}}
  }'
```

`GET` returns names, types and status — never the credentials. Resolved
credentials are registered with the redaction registry, so they never reach a
log line or an error body. `PUT …/connections/{name}` replaces one (that is how
a tenant reconnects), `DELETE` removes it.

**Refresh-token rotation.** An `oauth2_refresh` connection writes every
rotated refresh token back into its sealed record, so the next run — on any
cluster instance — presents the current one.

**Re-authorization.** When the provider rejects the grant (`401`, or `400`
with `invalid_grant`) the connection is marked `needs_reauth` with the reason,
a `connection_needs_reauth` notification goes out through the tenant's
`notifications:`, and `connection.needs_reauth` is audited. Runs that
reference the connection are refused with a `409` until it is reconnected;
the tenant's other connections keep working.

## Hosted OAuth connect

Declare the providers your product offers once per deployment:

```yaml
# providers.yaml
version: 1
providers:
  - name: crm
    authorize_url: https://idp.example/oauth/authorize
    token_url: https://idp.example/oauth/token
    client_id: ${env:CRM_CLIENT_ID}
    client_secret: ${env:CRM_CLIENT_SECRET}
    scopes: [read, offline_access]
    extra_authorize_params: { prompt: consent }
    redirect_base: https://faucet.example.com        # this server's public URL
    allowed_redirects: [https://app.example.com/integrations]
```

Register `https://faucet.example.com/v1/connect/callback` as the redirect URI
with the provider. Then, from your product's **backend**:

```bash
curl -X POST localhost:8080/v1/tenants/acme/connect/crm -H "Authorization: Bearer $OPERATOR" \
  -H 'content-type: application/json' \
  -d '{"connection": "crm", "redirect": "https://app.example.com/integrations/done"}'
# → {"authorize_url": "https://idp.example/oauth/authorize?…", "expires_at": "…"}
```

Send the user's browser to `authorize_url`. The provider redirects to
`/v1/connect/callback`, which exchanges the code (PKCE `S256`), stores the
grant as an `oauth2_refresh` connection, and redirects to your `redirect` with
`?connection=crm&status=ok` — or `status=error&error=<code>` (`expired`,
`access_denied`, `exchange_failed`). The `state` parameter is random,
single-use and valid for ten minutes; a `redirect` outside `allowed_redirects`
is refused, so the callback is never an open redirector. The start call is a
bearer-authenticated `POST` precisely so a bearer token never travels in a
browser URL.

## Running for a tenant

Write the pipeline once, against connection names and `${tenant.*}`:

```yaml
version: 1
kind: pipeline
name: crm-contacts
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.crm.example
      path: /v1/contacts
      records_path: "$.results[*]"
      auth: { ref: crm }                      # the tenant's connection
  sink:
    type: bigquery
    config:
      project_id: product-warehouse
      dataset_id: "tenant_${tenant.id}"        # one dataset per tenant
      table_id: contacts
  state:
    type: postgres
    config: { url: "${env:STATE_URL}" }
```

Register it as a [template](./templates.md), then run it for a tenant:

```bash
curl -X POST localhost:8080/v1/tenants/acme/templates/crm-contacts/runs \
  -H "Authorization: Bearer $OPERATOR" -H 'content-type: application/json' -d '{}'
```

(`POST /v1/tenants/{tenant}/runs` takes a whole config instead.) The run:

- resolves `auth: { ref: crm }` against Acme's connections first (a
  connection shadows a config `auth:` entry of the same name); a reference
  to a connection the tenant does not have is a `409`;
- binds `${tenant.id}`, `${tenant.name}` (the id when unnamed) and
  `${tenant.labels.<key>}` anywhere in the document;
- keeps its bookmarks at `acme::crm-contacts::<row>` (and its SLA, profiling
  and rollback markers beside them), so tenants never share a bookmark;
- carries `tenant` on its run record, usage records, audit entries and change
  requests — `GET /v1/runs?tenant=acme`, `GET /v1/usage?by=tenant`.

A `${tenant.*}` token in a run not started for a tenant is refused rather
than handed to a connector as literal text.

## Fan-out

Run a template once per tenant:

```bash
curl -X POST localhost:8080/v1/templates/crm-contacts/fanout \
  -H "Authorization: Bearer $OPERATOR" -H 'content-type: application/json' \
  -d '{"tenants": "all", "concurrency": 8}'
```

`tenants` is `"all"` (every tenant that is not suspended) or a list. Each
tenant runs as itself; the response has one entry per tenant —
`submitted` (with `run_id`), `pending_approval`, `skipped` (a missing or
revoked connection, a suspended tenant, a tenant at its limit — with the
reason) or `failed`. An `idempotency_key` is suffixed `:<tenant>`, and every
run carries a `fanout` label with the fan-out's id.

On a schedule, add a `schedule` trigger to the `--triggers` file (needs the
`triggers` and `schedule` features):

```yaml
version: 1
triggers:
  - name: nightly-crm
    type: schedule
    cron: "0 2 * * *"
    timezone: Europe/Berlin
    template: { id: crm-contacts }
    tenants: all
```

Each tick submits one run per tenant, keyed `trig:nightly-crm:<tick>:<tenant>`,
so cluster instances running the same triggers file let exactly one run
through per tick and tenant.

## Tenant-scoped principals

Give each customer's backend its own token, confined to its tenant:

```yaml
# auth.yaml
principals:
  - { name: admin, token: "${env:ADMIN_TOKEN}", role: admin }
  - { name: acme-backend, token: "${env:ACME_TOKEN}", role: operator, tenant: acme }
```

`acme-backend` reaches `/v1/tenants/acme/…`, its own runs, change requests and
usage (lists are filtered), template reads and the schema catalog. Another
tenant's run or route is a `404` — existence does not leak — and every global
administrative route (tenant CRUD, template admin, audit, reload, fan-out) is a
`403`. Its plain `POST /v1/runs` runs for Acme.

## Deleting a tenant

```bash
curl -X DELETE localhost:8080/v1/tenants/acme -H "Authorization: Bearer $ADMIN"
# → {"runs": 42, "usage_records": 42, "change_requests": 1, "state_keys_deleted": 3}
```

Deletes the tenant's run records, usage records, change requests, every
state key its runs used (recorded as each run starts, with its store spec
sealed under the vault key) and their markers, its connections and pending
connect sessions. It is refused while a run is queued or running. Destination
data is never touched — faucet does not own it. A key the server could not
record safely (no vault key, a state store with credentials in its spec) is
listed under `state_keys_not_deleted` instead.

## The console

The web console has a **Tenants** page — tenants with their limits,
connections and their status, a **Connect** button per provider, a form for
stored credentials, suspend and delete — and a tenant switcher in the top bar
that scopes Runs, Usage and Changes. A tenant-scoped principal is pinned to
its tenant.

## Metrics

| Metric | Meaning |
|---|---|
| `faucet_serve_tenant_runs_total{tenant,outcome}` | Finished tenant runs. |
| `faucet_serve_tenant_limit_rejections_total{tenant,limit}` | Runs refused by `max_concurrent_runs` or a suspension. |
| `faucet_serve_connections{status}` | Connections by `active` / `needs_reauth`. |
| `faucet_serve_connect_flows_total{provider,outcome}` | Connect flows `started` / `completed` / `failed`. |
