# RFC 0012 — Multi-tenant embedded integrations

*Let a SaaS product run faucet as the engine behind "connect your Salesforce / HubSpot / NetSuite" for its own customers: tenants, an encrypted per-tenant connection vault, a hosted OAuth connect flow, tenant-scoped runs, state and schedules, and per-tenant isolation in the control plane (#709).*

| | |
|---|---|
| **RFC** | 0012 |
| **Title** | Multi-tenant embedded integrations |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #709 · #571 (Template Hub) · #444 (params / template registry) · #556 (refresh-token rotation) · #205 (RBAC) · #207 (encryption) · #703 (budgets / approvals) · #704 (usage) · #196 (triggers) · epic #38 |
| **Related ADRs** | — |

## Summary

`faucet serve` gains a **tenant** dimension. A tenant owns **connections**
(credentials, encrypted at rest, created directly or through a hosted OAuth
authorization-code flow), and every run started *for* a tenant resolves its
`auth: { ref }` against that tenant's connections, namespaces its state under
the tenant, carries the tenant on its run record, usage, audit and catalog
entries, and is held to the tenant's limits. Principals can be scoped to one
tenant. Templates can be triggered per tenant and fanned out across every
tenant — on demand or on a cron schedule — with bounded concurrency.

## Motivation

Product teams increasingly sync *their customers'* data. faucet already has
the engine — source templates and the hub, typed params, shared single-flight
auth providers with refresh-token rotation, serve with RBAC, triggers, budgets
and usage accounting — but it is single-tenant: one credential set per
pipeline, one state namespace, one audit stream. The gap is tenancy, not
connectors.

## Design

### Tenants

- `POST /v1/tenants` `{ id, name?, limits?, notifications?, labels? }`,
  `GET /v1/tenants`, `GET /v1/tenants/{t}`, `PATCH /v1/tenants/{t}`,
  `DELETE /v1/tenants/{t}`. The id is a slug (`^[a-z0-9][a-z0-9_-]{0,62}$`).
- `limits: { max_concurrent_runs?, max_records_per_run?, max_bytes_per_run?,
  max_duration_secs? }` — the concurrency limit is enforced at submit
  (`429` naming the tenant); the rest become a [`BudgetSpec`] merged into every
  run for the tenant (#703), so a noisy tenant is stopped at the page boundary.
- `notifications:` — the same `NotificationSpec` list a config takes; the
  server emits tenant-level events (`connection_needs_reauth`) through it.
- Storage rides the run-history backends: `faucet_tenants`.

### Tenant-scoped runs

- `POST /v1/tenants/{t}/runs` (a `POST /v1/runs` body) and
  `POST /v1/tenants/{t}/templates/{id}/runs` (a trigger body) run for the
  tenant. A tenant-scoped principal's plain `POST /v1/runs` is scoped
  implicitly.
- A tenant run:
  - resolves `auth: { ref: <name> }` against the **tenant's connections**
    first, then the config's own `auth:` catalog (a connection shadows a
    catalog entry of the same name);
  - namespaces every state key as `{tenant}::{pipeline}::{row}` (and the SLA /
    profiling / rollback markers under it), so two tenants running the same
    template never share a bookmark;
  - substitutes `${tenant.id}` / `${tenant.name}` / `${tenant.labels.K}` in
    source and sink configs (and DLQ paths), so a template can route each
    tenant to its own destination;
  - carries `tenant` on the run record (a first-class field, filterable in
    `GET /v1/runs?tenant=`), its usage records, audit entries and change
    requests;
  - is refused while one of the connections it references is `needs_reauth`
    (`409`), so a revoked token pauses exactly that tenant's work.

### Connection vault

- `POST /v1/tenants/{t}/connections` `{ name, provider: { type, config } }` —
  the same `{ type, config }` shape as the `auth:` catalog (`static`,
  `oauth2`, `oauth2_refresh`, `token_endpoint`, `flow`). `GET` lists names,
  types, status and timestamps — **never** the credentials. `DELETE` removes one.
- Credentials are sealed with AES-256-GCM (`faucet_core::encryption`) under the
  server's vault key (`--vault-key` / `FAUCET_VAULT_KEY`, with
  `--vault-previous-key` for rotation) before they reach storage
  (`faucet_tenant_connections`). A server without a vault key refuses to store
  connections. Resolved credentials are registered with the redaction registry
  for the run, so they never reach a log line or an error body.
- **Rotation persistence:** an `oauth2_refresh` connection gets a per-connection
  `StateStore` adapter that writes the rotated refresh token back into the
  sealed connection record — the next run, on any cluster instance, uses it.
- **Re-auth:** a provider failure that means the grant is gone (`invalid_grant`,
  `401`/`400` from the token endpoint) marks the connection `needs_reauth`,
  records why, emits `connection_needs_reauth` through the tenant's
  notifications and the audit log, and later runs referencing it are refused
  until the tenant reconnects (a new connect flow or a `PUT`).

### Hosted OAuth connect

- Providers are declared once per deployment in a file passed with
  `--connect-providers`: `{ name, authorize_url, token_url, client_id,
  client_secret, scopes, extra_authorize_params?, redirect_base }`.
- The embedding product's **backend** (bearer-authenticated) calls
  `POST /v1/tenants/{t}/connect/{provider}` `{ connection, redirect }` and gets
  back `{ authorize_url, expires_at }` to send its user to. Bearer tokens never
  travel in a browser URL — which is why this is a POST returning a URL rather
  than a browser-facing GET.
- The flow uses `state` (random, single-use, 10-minute expiry, stored in
  `faucet_connect_sessions`) and PKCE (`S256`).
- `GET /v1/connect/callback?code=&state=` is public (the `state` is the
  credential): it exchanges the code at the provider's token endpoint, stores
  the result as an `oauth2_refresh` connection (clearing `needs_reauth`), and
  redirects to the caller's `redirect` with `?connection=…&status=ok` (or
  `status=error&error=…`). A `redirect` must match the provider's
  `allowed_redirects` prefixes — no open redirector.

### Fan-out and schedules

- `POST /v1/templates/{id}/fanout` `{ tenants: "all" | [ids], params?,
  concurrency?, sink?, overlay? }` triggers the template once per tenant (as
  that tenant), skipping tenants whose required connection is missing or
  `needs_reauth`, with at most `concurrency` submissions in flight and each
  tenant's own concurrency limit respected. Returns one entry per tenant.
- A new trigger type `schedule` (`cron`, `timezone`) in the `--triggers` file,
  whose `run` may name a template and `tenants: all | [..]` — the cron
  fan-out the issue asks for. Idempotency key
  `trig:<name>:<tick>:<tenant>` makes a tick replay-safe across a cluster.

### Access control

- A principal in `--auth-config` may carry `tenant: <id>`. A tenant-scoped
  principal can reach only `/v1/tenants/{its id}/…`, its own runs (lists are
  filtered; another tenant's run is `404`, not `403`, so existence does not
  leak), its own change requests and usage; every global administrative route
  (tenants CRUD, templates admin, audit, reload, fan-out) is denied. New
  permissions: `TenantRead` (viewer+), `TenantAdmin` (admin),
  `ConnectionManage` (operator+).

### Deletion

`DELETE /v1/tenants/{t}` cascades: connections, pending connect sessions, run
records, usage records, change requests, and every state key a tenant run
wrote (recorded in a `faucet_tenant_state_refs` ledger as `(state spec, key)`
when the run starts, so deletion can rebuild each store and delete exactly
those keys and their markers). Destination data is never deleted — faucet
does not own it.

### Surfaces

HTTP (above; OpenAPI), the console (a tenant switcher that scopes every view,
a Tenants page with connections and a "Connect" button per provider), metrics
(`faucet_serve_tenant_runs_total{tenant,outcome}`,
`faucet_serve_tenant_limit_rejections_total{tenant,limit}`,
`faucet_serve_connections{status}`, `faucet_serve_connect_flows_total{provider,outcome}`),
audit actions `tenant.*`, `connection.*`, `connect.*`. Feature flag:
`tenants` (CLI-only, `= ["serve", "encryption", "templates", "notify"]`),
in `full`. The `schedule` trigger type additionally needs the `triggers` and
`schedule` features.

## Alternatives considered

- **A tenant per config file / per process**: pushes isolation to the
  deployment and gives up shared templates, pooled providers and one control
  plane. Rejected — that is today's workaround.
- **Browser-facing `GET /v1/connect/{provider}` with a bearer**: leaks bearer
  tokens into browser history and referrers. Rejected for the backend POST.
- **Storing credentials in the state store**: state stores are per pipeline
  and not all encrypted; the vault needs one encrypted, shared home.

## Out of scope

- A white-label end-user UI beyond the redirect-based connect flow.
- Billing per tenant (usage records carry the tenant, #704).
