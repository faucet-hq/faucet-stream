# HTTP API reference (`faucet serve`)

`faucet serve` exposes a JSON REST control plane for submitting, polling,
listing, cancelling, and streaming the logs of pipeline runs, plus
unauthenticated health and Prometheus endpoints. A machine-readable
[`docs/openapi.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/docs/openapi.yaml)
spec ships alongside this page and is kept in sync with the router by a CI test.

See the [serve cookbook](../cookbook/serve.md) for a guided quickstart, the
security model, and operational guidance. This page is the endpoint reference.

## Authentication

All `/v1/*` endpoints require `Authorization: Bearer <token>` unless the server
was started with `--no-auth`. The token is compared in constant time; the
`Authorization` header is the only accepted credential (no query-string auth).
`/healthz`, `/readyz`, and `/metrics` are always unauthenticated (probes /
scrapers). `OPTIONS` preflight bypasses auth so browsers behind a CORS policy
work.

**Failed tokens.** A request that presents an invalid bearer token gets `401`
and counts against its source address: after 20 within a minute, every request
from that address that carries a token gets `429` until the minute ends. The
first failure of each minute, and the one that reaches the limit, are written
to the audit log as `auth.failed` (`denied`, with the source address; at most
60 such records a minute across all addresses). The token itself is never
recorded. A token shorter than 16 characters draws a warning at startup.

**Host and origin.** A `--no-auth` server bound to loopback answers only the
host names `localhost`, `127.0.0.1`, `[::1]` and its listen address; any other
`Host` gets `403`, so a web page served from another name that resolves to the
loopback address cannot reach it. On any server, `--allowed-host <name>`
(repeatable, `*` for any) turns the check on with that list. `/healthz`,
`/readyz` and `/metrics` are exempt, so load balancers and kubelets can probe
by address. Independently, a `POST`, `PUT`, `PATCH` or `DELETE` that carries an
`Origin` header must come from the server's own origin (the `Host`, or the
`X-Forwarded-Host` a proxy sets), a `--cors-origin`, or an `--allowed-origin`;
anything else, including `Origin: null`, gets `403`. Clients that send no
`Origin` (scripts, other servers) are unaffected. Behind a proxy that rewrites
`Host` and sets no `X-Forwarded-Host`, list the console's public origin with
`--allowed-origin`.

Every response carries `X-Content-Type-Options: nosniff`; `/v1` and `/mcp`
responses also carry `Cross-Origin-Resource-Policy: same-origin`. `/mcp`
takes its JSON-RPC body only as `Content-Type: application/json` (else `415`).

### RBAC & the audit log (`--auth-config`)

A single `--auth-token` is one implicit **admin** principal. For a team
deployment, `--auth-config <file>` promotes the server to **role-based access
control**: a YAML/JSON file of principals, each a `{ name, token, role }`. Three
built-in roles form a ladder:

| Role | Permitted |
|------|-----------|
| `viewer` | read-only: `GET /v1/runs*`, `GET /v1/overview`, `GET /v1/schemas*`, `GET /v1/catalog/*`, `GET /v1/usage`, `GET`/`POST /v1/status` and `GET`/`POST /v1/mirror/{name}` for a registered `template` (without caller `params`), `GET /v1/changes*`, `GET /v1/templates*`, `GET /v1/local-outputs`, `GET /v1/tenants*` |
| `operator` | everything a viewer can do **plus** submit / cancel / delete runs, trigger registered pipeline templates, propose and approve / reject change requests (as far as the `approvals:` policy allows), `POST /v1/doctor`, `POST /v1/plan`, `/v1/status` and `/v1/mirror/{name}` with an inline `config` or template `params`, firing triggers, deleting local sink outputs, running for tenants and fanning templates out across them, and managing tenant connections (including hosted OAuth connect flows) |
| `admin` | everything, including the template lifecycle (register, launch, roll back, deprecate, assign channels, delete, sync, publish), the tenant lifecycle (create, update, suspend, delete), pipeline state (`/v1/state/{pipeline}/{row}`) and `GET /v1/audit` |

```yaml
# auth.yaml
principals:
  - { name: alice, token: "${env:ALICE_TOKEN}", role: admin }
  - { name: ci,    token: "${env:CI_TOKEN}",    role: operator }
  - { name: dash,  token: "${env:DASH_TOKEN}",  role: viewer }
```

```bash
faucet serve --auth-config auth.yaml
```

Tokens resolve `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` and secret-manager
references (`${vault:…}`, `${aws-sm:…}`, …) when the server starts; a variable
or file that does not resolve refuses the start, and a token that still holds
`${` after resolution is refused rather than accepted literally. The variables
and files an auth config reads are then out of reach of every config the
server loads (see [Server credentials](#server-credentials)).

Principal names must be unique and non-empty. The names the server gives its
own actors — `runtime`, and any name starting with `system:`, `trigger:` or
`sync:` — are reserved and refuse the start, so audit records and change
requests attributed to them can never be confused with a configured principal.

The same file may carry an `approvals:` block — who may approve which
[change requests](../cookbook/approvals.md), how many approvals each kind
needs, whether a requester may approve their own, and how long a request
stays approvable:

```yaml
approvals:
  expire_secs: 86400
  rules:
    - { kinds: [run], roles: [operator, admin], min_approvers: 1, self_approve: false }
    - { kinds: [template_register, template_launch], roles: [admin], min_approvers: 2 }
```

With no rule for a kind: admins only, one approval, no self-approval.

A principal may carry `tenant: <id>` to confine it to one tenant
([embedded integrations](../cookbook/embedded-integrations.md)). It reaches
only `/v1/tenants/<its id>/…`, its own runs, change requests and usage (lists
are filtered; another tenant's run or route is a `404`, so existence does not
leak), the schema catalog, template reads and `GET /v1/whoami`. Every global
administrative route is a `403`. It runs **registered templates only**
(`POST /v1/tenants/<its id>/templates/{id}/runs`): a config of its own — `POST
/v1/runs`, `POST /v1/tenants/<its id>/runs`, `POST /v1/changes` — is a `403`,
because a submitted config resolves references and builds connectors on the
server. An unscoped operator may still submit a config for a tenant.

```yaml
principals:
  - { name: acme-backend, token: "${env:ACME_TOKEN}", role: operator, tenant: acme }
```

### The read / write / admin token trio

For the common split — dashboards read, operators write, one admin — there is
no file to author. Pass any subset of three flags (or their env vars) and the
server synthesizes the equivalent RBAC config:

```bash
faucet serve \
  --read-token  "$READ_TOKEN" \   # FAUCET_SERVE_READ_TOKEN  → viewer
  --write-token "$WRITE_TOKEN" \  # FAUCET_SERVE_WRITE_TOKEN → operator
  --admin-token "$ADMIN_TOKEN"     # FAUCET_SERVE_ADMIN_TOKEN → admin
```

Prefer the env vars: a flag value is visible in `ps`. The trio is mutually
exclusive with `--auth-token` / `--auth-config` / `--no-auth`, an empty token
is rejected at startup, and reusing one token for two roles is refused (the
role would otherwise depend on scan order).

### Role × route matrix

The contract, enforced by a table-driven test over **every** registered route
(`cli/tests/serve_rbac.rs`): a read token cannot reach anything that changes
state, and a route nobody classified stays admin-only and fails the test until
someone does.

| Route | viewer | operator | admin |
|---|:--:|:--:|:--:|
| `GET /v1/runs`, `/v1/runs/{id}`, `/v1/runs/{id}/logs`, `/v1/runs/{id}/config`³ | ✓ | ✓ | ✓ |
| `GET /v1/overview` | ✓ | ✓ | ✓ |
| `POST /v1/runs`, `DELETE /v1/runs/{id}`, `POST /v1/runs/{id}/cancel` | — | ✓ | ✓ |
| `POST /v1/backfill` | — | ✓ | ✓ |
| `POST /v1/verify` | — | ✓ | ✓ |
| `POST /v1/plan` | — | ✓ | ✓ |
| `GET /v1/policy`, `/v1/policy/versions`, `/v1/policy/refusals`, `GET /v1/tenants/{tenant}/policy` | ✓ | ✓ | ✓ |
| `POST /v1/policy/check` | — | ✓ | ✓ |
| `POST /v1/policy`, `/v1/policy/preview`, `/v1/policy/rollback` (and the `/v1/tenants/{tenant}/policy` equivalents) | — | — | ✓ |
| `POST /v1/runs/{id}/rollback` | — | — | ✓ |
| `GET /v1/schemas`, `/v1/schemas/{kind}/{name}` | ✓ | ✓ | ✓ |
| `POST /v1/doctor` | — | ✓ | ✓ |
| `POST /v1/dlq/inspect` | ✓ | ✓ | ✓ |
| `POST /v1/dlq/replay`, `/v1/dlq/discard` | — | ✓ | ✓ |
| `GET /v1/triggers` | ✓ | ✓ | ✓ |
| `POST`/`PUT /v1/triggers/{name}` | — | ✓ | ✓ |
| `GET /v1/catalog/*` | ✓ | ✓ | ✓ |
| `POST /v1/catalog/datasets/{id}/consumers` | — | ✓ | ✓ |
| `GET /v1/usage` | ✓ | ✓ | ✓ |
| `GET`/`POST /v1/status` | ✓² | ✓ | ✓ |
| `GET`/`POST /v1/mirror/{name}` | ✓² | ✓ | ✓ |
| `GET`/`PUT`/`DELETE /v1/state/{pipeline}/{row}` | — | — | ✓ |
| `GET /v1/local-outputs`, `/v1/local-outputs/{id}/preview` | ✓ | ✓ | ✓ |
| `DELETE /v1/local-outputs/{id}`, `POST /v1/local-outputs/cleanup` | — | ✓ | ✓ |
| `GET /v1/templates`, `/v1/templates/{id}`, `/v1/templates/{id}/rows`, `/v1/templates/{id}/runs` | ✓ | ✓ | ✓ |
| `POST /v1/templates/{id}/runs` (trigger) | — | ✓ | ✓ |
| `POST /v1/templates`, `DELETE /v1/templates/{id}` | — | — | ✓ |
| `POST /v1/templates/{id}/{tags,launch,rollback,deprecate}` | — | — | ✓ |
| `POST /v1/templates/{id}/versions/{version}/deprecate` | — | — | ✓ |
| `GET /v1/templates/{id}/versions/{version}/tests` | ✓ | ✓ | ✓ |
| `POST /v1/templates/{id}/versions/{version}/test` | — | — | ✓ |
| `POST /v1/templates/sync`, `POST /v1/templates/{id}/publish` | — | — | ✓ |
| `GET /v1/whoami` | ✓ | ✓ | ✓ |
| `GET /v1/changes`, `GET /v1/changes/counts`, `GET /v1/changes/{id}` | ✓ | ✓ | ✓ |
| `POST /v1/changes` | — | ✓ | ✓ |
| `POST /v1/changes/{id}/approve`, `/reject` | — | ✓¹ | ✓¹ |
| `GET /v1/tenants`, `/v1/tenants/{tenant}`, `/v1/tenants/{tenant}/connections[/{name}]`, `GET /v1/connect/providers` | ✓ | ✓ | ✓ |
| `POST /v1/tenants/{tenant}/connections`, `PUT`/`DELETE …/connections/{name}`, `POST …/connect/{provider}` | — | ✓ | ✓ |
| `POST /v1/tenants/{tenant}/runs`, `…/templates/{id}/runs`, `POST /v1/templates/{id}/fanout` | — | ✓ | ✓ |
| `POST /v1/tenants`, `PATCH`/`DELETE /v1/tenants/{tenant}` | — | — | ✓ |
| `POST /mcp` | ✓ | ✓ | ✓ |
| `GET /v1/audit`, `/v1/audit/{id}` | — | — | ✓ |
| `POST /v1/reload` | — | — | ✓ |
| *any unclassified `/v1` route* | — | — | ✓ |

¹ Reaching the route; the `approvals:` policy then decides whether the approval counts.
³ Below an unscoped admin, credential-looking values come back masked.
² With a registered `template` and its declared param defaults. An inline `config` is loaded on the server, and caller `params` decide what it connects to, so either needs `operator` (`403` for a viewer).

Two entries are POSTs a **read** token can reach, because they change nothing:

- `POST /mcp` — the MCP transport's baseline is a read scope; its one mutating
  tool (`run_pipeline`) re-checks `RunWrite` inside the handler, and the tools
  that load a config (`validate_config`, `preview`) need `operator`.
- `POST /v1/dlq/inspect` — summarises a DLQ location. The location is
  caller-supplied, so a read token can ask the server to read a path on its
  filesystem. That is the same trust boundary as run logs (which carry record
  data), and the reason the control plane is not meant to face the public
  internet.

A request whose role lacks the route's required permission gets `403 forbidden`
(and a `denied` audit record). `--auth-config` is mutually exclusive with
`--auth-token` / `--no-auth`. Every token is registered for log redaction at
startup.

**Managing templates is admin-only.** Registering, launching, rolling back,
deprecating (a template or one version), assigning channels, deleting, syncing
and publishing templates need `admin`; `operator` triggers registered templates
(`POST /v1/templates/{id}/runs`) but no longer changes them. **Migration:** an
`--auth-config` file whose `operator` principals managed templates needs those
principals promoted to `role: admin`. The MCP `register_template` /
`launch_template` / `rollback_template` / `deprecate_template` tools follow the
same rule.

### Server credentials

A config the server loads — submitted, planned, probed, or a template — can
never read the server's own credentials. Refused with `403`:

- `${env:}` / `${secret:}` of `FAUCET_VAULT_KEY`, `FAUCET_VAULT_PREVIOUS_KEYS`,
  `FAUCET_SERVE_AUTH_TOKEN`, the `FAUCET_SERVE_{READ,WRITE,ADMIN}_TOKEN` trio,
  `FAUCET_SERVE_HISTORY`, `FAUCET_TEMPLATE_STORE` and `FAUCET_SNAPSHOT_KEY`;
- the server's own input files — `--auth-config`, `--connect-providers`,
  `--templates-sync`, `--otel-config`, `--triggers`, `--default-config`, the
  startup `.env`, a SQLite `--history` database and the snapshot key file —
  through `${file:}`;
- every variable, file and secret-manager reference (`${vault:…}`, …, any
  `#field` of the same secret) that `--auth-config`, `--connect-providers`,
  `--templates-sync` or `--otel-config` reads;
- anything under `/proc`, `/sys` or `/dev` (except `/dev/null`) through
  `${file:}`.

The same files and trees are refused as **data**: a `file`, `csv`, `jsonl`,
`parquet`, `sqlite`, `duckdb`, `delta` or `iceberg` source, sink, state store
or DLQ — including a template override, topology node, fan-out source,
partition bound probe, `verify.destination` and a `sql` transform's `csv` /
`jsonl` relation — that would reach one of them is a `403`, before any of the
config runs. A read is refused when its path is such a file, a directory
listing that would include one (any depth with `recursive: true`), or a glob
any of whose segments could match into a system tree or onto a protected file
(an existing match that is a symlink into one counts). A sink or state store
is refused only when it would write such a file, so it may use a directory
that also holds one. In a config the request carries, such a path may not
take a value from records (`${parent.*}` and other runtime tokens other than
`${now.*}`, `${backfill.*}` and `${partition.*}`). In a run of a caller's
config, or of a template holding values its caller supplied, a record value
in a path or remote-path field may not turn the path into a URL or leave a
leading segment empty (anchoring the path at the root), and a remote path on
the connector's own server (an SFTP `path` or `file_name`) may not take a `..`
segment from a caller-supplied value.

### SQL connectors run sandboxed

A query can read files by itself — DuckDB's `read_text('/etc/passwd')` or
`ATTACH`, SQLite's `ATTACH` — so every connector whose schema marks a confining
switch with `x-faucet-sandbox` runs with that switch on in a config the server
loads. Among the built-ins that is every `duckdb` source and sink and every
`sqlite` source (`sandbox: true`); a plugin connector that marks its own
switch is confined the same way. DuckDB
gets `enable_external_access = false` with extension autoload off and the
configuration locked (a query cannot `SET` it back), and each SQLite
connection allows no attached database (which also stops `VACUUM INTO`;
extension loading is always off). The server sets the flag on every template,
row override, topology node, fan-out source, partition bound probe,
`verify.destination` and DLQ sink, overwriting a `sandbox: false`. Such a query still reads and writes its own database; it
fails with a `Permission Error` / `too many attached databases` the moment it
reaches past it. The `sql` transform is always sandboxed this way, and in a
config the server loads one that sets no `memory_limit` / `threads` gets `1GB`
and the machine's parallelism up to 4 threads (DuckDB's own defaults take most
of the machine).

The one exception is a config in the server's own `--triggers` file: its
operator wrote it on the server, so its SQL keeps host access (the run record
shows `host_access: true`, and a clustered claim of it keeps it too). A
template a trigger runs is sandboxed like any other.

### Subprocess connectors

A connector whose schema marks a field `x-faucet-resource: command` runs a
program named in its config — among the built-ins, the `singer` source and
sink; a plugin connector is recognised the same way. A config
**the request carries** (`POST /v1/runs`, `/v1/doctor`, `/v1/plan`,
`/v1/status`, `/v1/backfill`, `/v1/verify`, `/v1/dlq/replay`, a rollback with
a `config`, MCP `validate_config` / `preview` / `run_pipeline`) may use them
only when the server was started with `--allow-subprocess-connectors`;
otherwise it is a `422`. A config submitted for a tenant is refused even then.
Registered templates and the triggers file are written by the server's
operator and may always use them.

### Listeners

A connector field annotated `x-faucet-resource: listen` opens a listener in
the server process — among the built-ins, a `webhook` source's
`listen_addr`. In a config **the request carries** it must be a literal
`address:port` inside an `--allow-listen` range (default `127.0.0.1` and
`[::1]`, ports 29100–29199): a host name, port `0`, the server's own
`--listen` port or an address outside the ranges is a `403` naming the
ranges to use. An address the config leaves out is the connector's default
(`127.0.0.1:8080` for `webhook`), and is checked the same way. A listener
off loopback also needs `auth_token` or `signature`, so not everyone who
can reach the port can add records. `POST /v1/doctor` (whose probe binds
the address) is checked too, and the webhook source checks again when it
binds inside a caller's run. A tenant's config may open none. Registered
templates, the triggers file and `faucet run` bind where they say. In a
cluster, while a member that does not check listeners is live, a caller's
config that opens one is refused with `503` until the rolling upgrade
finishes.

### Memory bounds

A config **the request carries** may not lift what a connector holds in the
server's memory: a source's `batch_size: 0` (one page holding the whole
result) is a `403`, and so is a field whose schema carries
`x-faucet-caller-max: N` set to `null`, `0` or above `N`. Among the
built-ins: `max_staged_records` on the CDC sources and `oracle-cdc`'s
`max_staged_records_total` (1 to 1,000,000), a
`websocket` source's `max_message_bytes` (up to 64 MiB) and a `webhook`
source's `timeout_secs` (up to 3,600), `max_payloads` (up to 1,000,000) and
`max_buffered_bytes` (up to 1 GiB). A field the config leaves out keeps the
connector's default. These are record and byte counts per connector, not a
memory limit for the run; registered templates, the triggers file and
`faucet run` are not limited by them.

### Keys the server sets

A sink config key the server sets for a run itself is removed from a config
**the request carries** before it loads: `_overwrite_run` (the token that
gives an overwrite run its own staging area) is always chosen by the server,
so two runs never share one.

### Reference policy

Every config the server loads from a caller is checked against one
allow-list before anything runs. A connector field that names a local path,
a URL or a program carries an `x-faucet-resource` annotation in its schema, so
the server confines connector configs it has never seen, plugin connectors
included, with no per-connector code. Host-bearing fields that are not URLs
are annotated too and read for their hosts: a broker or server list
(`brokers: b1:9092,b2:9092`, NATS `servers`), a bare `host` / `emulator_host`,
an ADO.NET / ODBC connection string (`Server=tcp:host,1433;…`) and an Azure
Storage connection string (`BlobEndpoint=…`, or the account host). A field
whose value is not a URL also declares its grammar
(`x-faucet-url-syntax: ado | oracle | azure-conn | url-list`), and the server
reads it the way the connector's client does:

- **`ado`** (MSSQL `connection_string`): the SQL Server driver's grammar —
  empty and leading `;` segments are skipped, a key may hold any character
  (`==` is a literal `=`), and a value may be `'…'` / `"…"` quoted (a doubled
  quote embeds one) or `{…}` braced. A string the driver would refuse
  (non-ASCII, an unclosed quote) is refused for a caller.
- **`oracle`** (Oracle `connect_string`): every Easy Connect address
  (`//host…`, `;`- and `,`-separated lists), every descriptor `HOST` and every
  `HTTPS_PROXY` is a host; `wallet_location`, `my_wallet_directory`,
  `token_location` and `oci_config_file` are paths; `oci_iam_url` and
  `azure_db_app_id_uri` are URLs. `token_auth` other than `OAUTH` with an
  explicit `token_location` authenticates with the server's own identity and is
  refused like `external_auth`; `PROTOCOL=BEQ` / `PROGRAM=` runs a program and
  needs `--allow-subprocess-connectors`; `PROTOCOL=IPC`, a descriptor without a
  `HOST`, a bare net service name and `ldap:` naming are refused for a caller,
  since the server's own Oracle configuration would resolve them.
- **`azure-conn`** (Azure Storage `connection_string`): read as the Azure
  client reads it — only the exact keys `AccountName`, `AccountKey`,
  `SharedAccessSignature`, `BlobEndpoint`, `EndpointSuffix`,
  `DefaultEndpointsProtocol` and `UseDevelopmentStorage` count, and the last of
  a repeated key wins. The host checked is `BlobEndpoint`, else the account
  host built from `AccountName` / `EndpointSuffix` (default
  `core.windows.net`), plus the loopback host for `UseDevelopmentStorage=true`;
  every other `…Endpoint=` key, in any case, is checked too. For an
  `azure-blob` connector the blob host comes from the whole connection
  (`account`, `endpoint`, `use_emulator` and the string, in the order the
  client applies them), so a private endpoint or the emulator needs only its
  own host allowed. `AccountName` (and the connector's `account`) must be an
  Azure storage account name — 3 to 24 lowercase letters and digits — and
  `DefaultEndpointsProtocol` must be `http` or `https`, under every policy. A
  caller's string that writes one of those keys in another case
  (`accountname=`), which the client ignores, is refused, naming the key.
- **`url-list`** (Kafka `brokers`, NATS `servers`): every element.

A `mongodb://` or `mongodb+srv://` URI, in any field, is read the way the
MongoDB driver reads it: options start at the first `?` (a `,` or `://` inside
an option does not start a new URL), user info ends at the last `@`, hosts end
at the first `/` and are split on `,` (`#` and `;` are ordinary host
characters), option keys are compared after lower-casing (Unicode, so
`tlsCertificateKeyFile` is matched however it is cased) and values are
percent-decoded with `+` kept as `+`. For a caller's config:

- every host, and `proxyHost`, is checked against the host rules; a host ending
  in `.sock` is a Unix socket path, checked against the path roots (a relative
  one is refused);
- `tlsCAFile` and `tlsCertificateKeyFile` are paths under the path roots;
- `tlsInsecure`, `tlsAllowInvalidCertificates` and `tlsAllowInvalidHostnames`
  with any value other than `false` need `--allow-unverified-tls`;
- `authMechanism=MONGODB-AWS` or `GSSAPI`, or any `ENVIRONMENT` entry in
  `authMechanismProperties` (`MONGODB-OIDC` with the server's Kubernetes,
  Azure or GCP identity), authenticates with the server's own credentials and
  is refused;
- with `--allow-url-host`, a `mongodb+srv://` name must be admitted by a
  `*.<parent domain>` entry, since its lookup may return any host under that
  domain;
- without `--allow-url-host`, and always for a tenant, the URI must keep the
  driver on the server it names: add `directConnection=true` (one server), or
  `loadBalanced=true` behind a load balancer. A replica set seed list, a
  `replicaSet` without `directConnection=true` and `mongodb+srv://` (without
  `loadBalanced=true`) are refused, because the server the driver reaches
  names the further hosts it connects to.

The same rule covers other clients that learn servers from the one they
reach. Without `--allow-url-host`, and always for a tenant, a NATS client is
built with `ignore_discovered_servers: true` (a submitted `false` is
overwritten), and a `kafka` source, sink, DLQ sink or lineage transport and an
`oracle` / `oracle-cdc` source or sink are refused (`403`): those clients
follow the brokers and listener redirects their servers advertise, which the
server cannot check. With `--allow-url-host` they run, and the advertised
addresses are not checked against the list.

Any URL value is trimmed first; a value that is, as a whole, an IP address
(`fd00:ec2::254`) is a host; a `,` list whose later elements carry their own
`scheme://` is checked element by element; a `//host/…` reference is checked by
its host; and a `unix:` / `redis+unix:` URL is checked as a path. A caller's
value from which no server can be read (`postgres:///db` without a socket
`host=`, `redis://`) is refused — name the server explicitly. A value longer
than 64 KiB is refused. A free-form
option map (Delta `storage_options`, Kafka `extra_client_config`) names the keys
that hold an endpoint, a credential file or a program (`x-faucet-resource-keys`),
and those keys are checked like the typed fields: Kafka's `bootstrap.servers`
and `metadata.broker.list` as broker lists, `*.url` keys as URLs, `*.location`,
`*.file` and `sasl.kerberos.keytab` as paths, and `plugin.library.paths`,
`ssl.engine.location`, `ssl.providers` and `*.cmd` as programs, which a caller
config may not set unless the server allows subprocess connectors (a tenant
never). A Delta Azure account option (`azure_storage_account_name`,
`account_name`) must be a storage account name. A transform's config is read the same way (a `sql` transform's
`http` relation `url` and `csv` / `jsonl` relation `path`). The same allow-list covers `${env:}`,
`${file:}`, `${secret:}` and secret-manager directives, the state, DLQ and
lineage paths, every endpoint an `auth:` provider calls (`token_url`, `url`,
a `flow` provider's `steps[].request.url` and `base_url_from`, a service
account's `token_uri` — in the config or inside `key_json`), a transform that
reads a file (`lookup`'s `jsonl`), every notification channel URL (a webhook
`url`, a Slack `webhook_url`, a PagerDuty `endpoint`), and the `reconcile:` count probe (a SQL probe
runs sandboxed like every other SQL connector). An `auth:` provider in a
caller's config may not name a local file at all: pass a service-account key
inline as `key_json` rather than `key_file`, and leave out `oauth2_refresh`'s
`persist` (the server decides where tokens are written). A tenant's
connections are held to the same rules, under the tenant's policy, when they
are stored and again whenever a run uses them.

| Reference | Default for a caller's config | Widen with |
|-----------|-------------------------------|------------|
| Local path (connector path, `${file:}`, state / DLQ / lineage file) | under the server's working directory or the system temp directory | `--allow-path-root DIR` (repeatable) |
| URL scheme | any | `--allow-url-scheme` restricts it to the listed schemes |
| URL host (including a database URL's `host=` / `hostaddr=`, the servers or `HOST=` entries of a `key=value` connection string or connect descriptor, and a state store's or lineage transport's `url` / `brokers`, read from the state store's or transport's own schema whatever connectors the build includes) | any, except link-local and cloud-metadata addresses (`169.254.0.0/16`, `fe80::/10`, `fd00:ec2::254`, `fd20:ce::254`, `fd00:c1::a9fe:a9fe`, `100.100.100.200`, `metadata.google.internal`) | `--allow-url-host` restricts it to the listed hosts (`*.example.com` matches subdomains); a metadata host is reachable only when listed |
| A Google credential key a Pub/Sub, Spanner or BigQuery connector is given (inline or as a file) | a `service_account` key only, whose `token_uri` follows the URL host and scheme rules; `authorized_user` and `external_account` credentials are refused | `--allow-url-host` / `--allow-url-scheme` for the `token_uri` |
| A URL holding a control character (tabs, line breaks and leading or trailing control characters are first removed, as URL parsers do; the scheme, host and fragment are then read from what is left) | refused | — |
| A file a connection string names (`sslkey=`, `sslrootcert=`, `socket=`, `TrustServerCertificateCA=`, MongoDB `tlsCAFile=` / `tlsCertificateKeyFile=` and `.sock` hosts, a wallet directory, Oracle `token_location` / `oci_config_file`) | under the path roots, like any local path | `--allow-path-root DIR` |
| A setting that turns off certificate or host-key verification (`insecure_skip_verify`, MSSQL `trust_server_certificate` / `TrustServerCertificate=true`, SFTP `known_hosts: {mode: insecure}`, `verify_ca=false` in a MySQL URL, `#insecure` on a `rediss://` URL, MongoDB `tlsInsecure` / `tlsAllowInvalidCertificates` / `tlsAllowInvalidHostnames`, Kafka `enable.ssl.certificate.verification=false`, Oracle `tls.server_dn_match: false`) | refused | `--allow-unverified-tls` (never for a tenant) |
| A connector that authenticates with the server's own identity: its environment, instance or workload role (IRSA, EKS Pod Identity, GKE workload identity, Azure managed identity), application-default credentials, local credential files or OS authentication (Oracle `external_auth`, Oracle `token_auth` other than `OAUTH` with a `token_location`, MongoDB `authMechanism=MONGODB-AWS` / `GSSAPI` or an `ENVIRONMENT` entry in `authMechanismProperties`, and every connector whose descriptor reports it, for example a cloud connector whose credentials are left to the default chain). An absent credentials field counts as the default it selects. A custom connector registered without an answer counts too | refused | `--allow-host-credentials` (never for a tenant), or register the pipeline as a template |
| A MongoDB URI the driver may extend with hosts its server names (a seed list, `replicaSet`, `mongodb+srv://`) | refused; add `directConnection=true` (one server) or `loadBalanced=true` | `--allow-url-host` (never for a tenant) |
| A client that follows servers its servers advertise: Kafka (source, sink, DLQ, lineage transport), Oracle (`oracle`, `oracle-cdc`) | refused; a NATS client is kept on its `servers` (`ignore_discovered_servers` forced on) | `--allow-url-host` (never for a tenant) |
| `${env:NAME}` / `${secret:NAME}` | none | `--allow-env NAME` or `--allow-env 'PREFIX*'` |
| `${vault:}` / `${aws-sm:}` / `${gcp-sm:}` / `${azure-kv:}` | none | `--allow-secret-scheme vault,aws-sm` |
| A program (`singer`) | refused | `--allow-subprocess-connectors` |
| A listener (a `webhook` source's `listen_addr`) | `127.0.0.1` and `[::1]`, ports 29100–29199 | `--allow-listen` (never for a tenant); see [Listeners](#listeners) |

A config that would use the server's own identity is checked after its matrix
rows are merged, for every row's source, sink and DLQ sink, every topology
node and a mirror's snapshot source; the `403` names each connector and field,
never a value. A value a caller supplies for a registered template's param
(or a trigger value, or a schedule param) that lands in the config of such a
connector is refused the same way, because the caller would then choose what
the server's identity reaches; registering such a template answers with a
warning. Inside a caller's run, a connector that would fall back to the
server's identity when it builds its client fails the run with the same flag
in its error. In a cluster, while a member that does not check this when it
builds a connector is live, a caller's run whose connectors check it only at
build time is refused with `503` until the rolling upgrade finishes; set
`--allow-host-credentials` the same on every replica.

What each built-in connector reports, and what a caller names instead:

| Connector | Uses the server's identity | Name instead |
|---|---|---|
| `kinesis`, `sqs`, `dynamodb`, `s3`, `parquet` (S3 location) | `credentials` other than `access_key` (and `anonymous` for S3), or absent; `region` absent when the server's environment names none | `access_key` (LocalStack / MinIO too), `anonymous` for a public S3 bucket, and a `region` |
| `redshift` | `iam` / `redshift_data_api` with `aws_credentials` other than `access_key` (or absent) or no region; the `copy` staging client's `copy.credentials` | `aws_credentials: access_key`, `copy.credentials: access_key` |
| `gcs`, `bigquery`, `spanner`, `pubsub` | `application_default` (the default); BigQuery's `bulk_load.gcs_auth` too; nothing with `emulator_host` (Spanner, Pub/Sub) | a service-account key, file or inline; `anonymous` (GCS) |
| `azure-blob` | `default` without `use_emulator`, `managed_identity`, a connection string with no key, SAS or emulator | `account_key`, `sas_token`, `service_principal`, a keyed connection string |
| `clickhouse`, `mssql` (`staging`), `databricks` (cloud `staging`), `snowflake` (`bulk_load`) | the upload store's `credentials` left to the default chain; Snowflake also a `storage_options` key naming a role, profile, metadata endpoint or workload identity | `credentials: access_key` / `service_account_json_*` / `account_key` / `sas_token`, or static keys in `storage_options` |
| `delta` | storage options naming no credential of the table's store, a role / profile / metadata / workload-identity key, or (S3) a server environment naming `AWS_IAM_ROLE_ARN` / `AWS_S3_ASSUME_ROLE_ARN` | static keys in `credentials` or `storage_options` |
| `iceberg` | `client.assume-role.arn`; a Glue client without `aws_access_key_id` / `aws_secret_access_key`, with `profile_name` or without a region; an `s3://` / `gs://` warehouse whose properties name no storage credential (Glue, SQL, HMS) | static keys in `catalog.properties`; a REST catalog's vended credentials keep working |
| `kafka` | `extra_client_config` `sasl.mechanism` / `sasl.mechanisms` = `GSSAPI`, or a `SASL_*` `security.protocol` with no other mechanism (GSSAPI is librdkafka's default) | SASL PLAIN / SCRAM, mTLS |
| `mongodb`, `mongodb-cdc`, `oracle`, `oracle-cdc` | as listed above | a user and password |

A refused reference is a `403` that names the field, the value and the flag
that would allow it; for a URL or connection string it names the field and the
offending key or host instead of the value, and credentials are removed from
any value it does show. `/dev/null` and `:memory:` are always allowed. A path is
compared after symlinks are resolved — a `..` after a symlink climbs out of the
link's target, as the kernel would — and a `..` that leaves a root, or that
follows a directory which does not exist yet, is refused. A path field that
holds a remote location (`s3://`, `s3a://`, `gs://`, `gcs://`, `az://`,
`abfss://`, `http(s)://`, `sftp://`, …) is checked as a URL, and may not
contain a `..` segment. The file source applies the roots again to every file
a directory or glob lists: a file whose resolved path leaves the roots (a
symlink below a root that points outside it) fails the run, and each file is
opened without following links once it has been checked. Anything else is a local path: a relative path whose
first segment contains `:` (`zz:/x`, `zz://x`) reads as a URL scheme to some
readers and as a directory to others, so it is refused — write `./zz:/x` or an
absolute path — and so is a path starting with `~`, which some engines expand
to a home directory. `sqlite:` and `duckdb:` locations are read the way their
drivers read them, percent-decoding included (`sqlite://%2Fdata%2Fx.db` is
`/data/x.db`), and a SQLite name that is itself a `file:` URI is refused. The
server's own files — every `.env*` file in its working directory or beside its
`--env-file` (including files created after it started), its input files,
run-history database and default-config state directory — are refused whatever
the roots allow, and so is listing the working directory as a whole (read a
subdirectory instead). A URL host is compared in
the form a URL parser reads it, so `2852039166`, `0xa9fea9fe`, `0251.0376.0251.0376`
or a trailing dot (`169.254.169.254.`, `metadata.google.internal.`) name the
metadata address they spell, and an `http(s)` URL's authority ends at a `\` as
it does for HTTP clients (`http://169.254.169.254\@allowed.example/` names
`169.254.169.254`). A URL whose host a caller's config takes from
records (`https://${parent.host}/…`, a whole `${parent.next_url}`) cannot be
checked, so it is refused; register the pipeline as a template instead.

Under `faucet serve`, database connectors never fill a setting the config
leaves out from the server's environment: `PGPASSWORD`, `~/.pgpass`,
`PGHOST`, `PGPORT`, `PGDATABASE` and `PGAPPNAME` are not read by the
PostgreSQL connectors, the PostgreSQL state store or Redshift, so a config
naming no password sends none; a config that leaves out a user, client
certificate, root certificate or server options while `PGUSER`,
`PGSSLCERT` / `PGSSLKEY`, `PGSSLROOTCERT` or `PGOPTIONS` is set is refused.
`PGSSLMODE` still applies. `faucet run` and library use keep libpq's
behaviour: those variables fill what the URL leaves out.

#### Template params a caller supplies

A registered template is the operator's, so the server loads its body under
the open policy. The values a caller supplies for the template's `params:`
are not, and neither is an `env` override (`"env": {"OUT_DIR": "…"}`), which
replaces the value of a `${env:VAR}` / `${secret:VAR}` the template reads:
wherever one lands in the materialized config — whole
(`path: ${param.out}`), spliced into longer text (`path: /data/${param.name}.jsonl`),
reached through `${vars.*}` / `${sources.*}`, carried by a `for_each` list
item, or composed in from a source × sink × overlay pairing — the field is
checked against the caller policy above after the trusted load and before
anything in the config runs: every annotated connector path, URL and
command, the state, DLQ and lineage paths, `auth:` provider endpoints and
files, every absolute URL, and a connector `type` that runs a program. A
tenant run is confined to `--tenant-path-root/<tenant>` (and may not send
`env` overrides at all). A param's `default` that an `env` override filled counts as the override's.
A refusal is a `403` naming the param (or `env override 'VAR'`) and the
field, and is audited as `policy.denied`:

```
403 a value a caller supplied for a template param reaches what the server's
reference policy does not allow a caller: param 'x' sets
`pipeline.dlq.sink.config.path` to /etc/faucet-x (outside the allowed path roots …)
```

The refusal shows only what the caller sent: a value the caller's text fills
whole is shown as is; in a value that also holds the template's own text
(including what the server's `${env:}` / `${file:}` resolved), everything but
the caller's text reads `…`, in the value and in the reason. It is redacted
like any server message, so an `env` override (registered as a possible
secret) shows as `***`, and a `secret: true` param's value is never shown. A
clustered run re-checked on the instance that claims it shows no value: the
stored record keeps where the caller's text is, never the text. A key that
holds a secret is not stored either: a value under such a key is recorded at
the key's object.

This applies to `POST /v1/templates/{id}/runs`, tenant template runs,
`POST /v1/templates/{id}/fanout`, MCP `run_template` (including a dry run),
an approval-gated template run (checked when it is proposed and again when it
executes), `/v1/status` and `/v1/mirror` with `template:` + `params`, and a
clustered run on the instance that claims it (after its sealed `secret: true`
values are unsealed). Not checked: a param's `default` (unless an `env` override wrote it), a
`${env:}` the server's own environment fills, a closed `values:`
set, an `int` / `float` / `bool` value, a `computed` param built only from
those (a `${map:…}` switch), a param the template declares `trusted: true`,
and a template trigger in the server's own `--triggers` file (the operator
wrote its params). `for_each` stream names are not checked: they are held to
`^[a-z0-9][a-z0-9_]*$`, so a sink path built from `${stream}` cannot leave
its directory.

**At run time.** A caller's run executes with the caller policy scoped over
it — on every instance that claims it, sharded or not — so its HTTP connectors
check the first request and every redirect hop against it. The network
calls a submission makes before it is queued (partition bound probes,
discovery fan-out, row selection, and the auth logins they trigger) run under
the same policy. Whatever the
origin, an HTTP connector never follows a redirect into a link-local or
cloud-metadata address, and a host *name* is judged by the addresses it
resolves to when the connection is made: the client's resolver drops
link-local / metadata addresses (so `169.254.169.254.nip.io`, or a name
re-pointed after the submit-time check, is never connected to) unless the
policy lists that name exactly. Behind an HTTP proxy (set in the
environment, the system settings or the connector) the proxy resolves the
target, so a caller-submitted run resolves the name itself first and refuses
it when any address is link-local / metadata; a name the server cannot
resolve is left to the proxy, and an answer that changes between that check
and the proxy's own lookup is not covered. A GCS `storage_host` that is not a
`googleapis.com` endpoint never receives the server's own credentials
(`application_default` or a key file); a Snowflake `account` must be an
account identifier, so its request URL always names a Snowflake host.

**DLQ locations.** The `location` of `POST /v1/dlq/inspect` and
`/v1/dlq/discard`, and the `from` / `failed_dlq` of `POST /v1/dlq/replay`, are
checked like a connector path: never a system tree or a server credential
file, and under the path roots — for a tenant, under `--tenant-path-root/<tenant>`.
An inspect or discard `location` that names nothing on disk is a `400`.

The server's own credentials stay unreadable whatever the flags say:
`FAUCET_VAULT_KEY`, the serve token variables and the files and variables
`--auth-config` read.

**Tenants.** A config submitted for a tenant never runs a program, and with
`--tenant-path-root DIR` its local paths are confined to `DIR/<tenant id>`
instead of the roots above.

**Trusted configs.** Registered templates, the `--triggers` file and the
`--default-config` base are written by the server's operator, so they are not
checked against the allow-list.

**Errors.** A run's error text is redacted before it is stored, so a resolved
secret or a URL password in a failure message never reaches the history
database, a peer instance or `GET /v1/runs/{id}`.

**Who am I.** `GET /v1/whoami` returns the caller's `principal`, `role` and
`permissions` to any authenticated caller (`--no-auth` and `--auth-token`
report an admin). The web console uses it to hide controls the role cannot use;
each route still enforces its own permission.

**Audit log.** Every mutating action (`run.submit` / `run.cancel` / `run.delete` /
`template.register` / `template.delete` / `template.run` / `template.promote` /
`template.version_deprecate` / `local_output.delete` / `local_output.cleanup` /
`catalog.annotate`), every plan (`plan`), every data-flow-policy refusal
(`policy.denied` — by the submitting principal at submit time, by `runtime`
when the backstop denied a page mid-run) and every denied attempt is recorded with principal, role, action, run id,
config fingerprint (submit), source IP, timestamp, and result. Admins read it via
`GET /v1/audit` (newest first; `result=denied` keeps the refusals; when a page
is full, `next_before` is the cursor for the next, older one: pass it as
`before`) and one entry by id via `GET /v1/audit/{id}`. Records persist in the run-history backend (`faucet_serve_audit`
for the SQL backends; an in-memory ring otherwise) and expire with the
`--retain-terminal-runs-secs` window.

### One order of checks for every operation

Every endpoint, MCP tool, trigger fire and approved change request that loads
or runs a config goes through the same runtime operations (#887), on behalf of
the calling principal (its identity, role, tenant, and whether the config came
from the request or from the server's operator). Each applies the same checks
in the same order and stops at the first refusal:

1. **load** — parse, merge `--default-config`, bind params, resolve secrets;
2. **trust** — screen the config for its origin, bind the tenant's scope, and
   check its references against the [reference policy](#reference-policy);
3. **policy** — the [data-flow policy](../cookbook/policies.md) (`422`,
   audited as `policy.denied`) — for every operation that writes;
4. **budget** — the request's `budget` (and a tenant's per-run ceilings)
   merged into the config's, the stricter winning;
5. **tenant** — a run for a tenant is admitted against its limits (`409`
   suspended, `429` at `max_concurrent_runs`);
6. **governance** — the run's governed options (budget, state namespace,
   resilience, lineage, pricing, catalog);
7. **execution**.

An operation that writes nothing (`/v1/plan`, `/v1/doctor`, a status read, a
`dry_run`) skips the policy, budget and tenant checks, never the trust check.
So a malformed config answers `400` / `422` before a tenant limit answers
`409` / `429`, and the same config gets the same answer through
`POST /v1/runs`, the MCP `run_pipeline` tool, a change request, or (for a
template) `POST /v1/templates/{id}/runs`, `run_template` and a template trigger.

## Endpoints

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `POST` | `/v1/runs` | `202` | Submit a run; config validated synchronously |
| `GET` | `/v1/runs` | `200` | List runs (filters below) |
| `GET` | `/v1/runs/{id}` | `200` | Get one run record |
| `DELETE` | `/v1/runs/{id}` | `204` | Remove a terminal run from history |
| `POST` | `/v1/runs/{id}/cancel` | `202` / `200` | Request cancel (202) or no-op if terminal (200) |
| `GET` | `/v1/overview` | `200` | Fleet trends over the run history for the console's Overview page (`since`, `until`, `bucket`, `pipeline`, `pool`, `label`, `tenant`, `stale_after`, `top`) — viewer / `RunRead` |
| `GET` | `/v1/runs/{id}/config` | `200` | The run's stored config (`{format, body, redacted}`); masked below an unscoped admin; `404` when none was stored (only clustered runs store one) — `RunRead`, audit `run.config` |
| `GET` | `/v1/runs/{id}/logs` | `200` | Stream the run's logs (`text/event-stream`), or read persisted logs with `?format=jsonl\|text` |
| `POST` | `/v1/backfill` | `202` | Submit a windowed backfill: one tracked run per window unit (operator) |
| `GET` | `/v1/audit` | `200` | Read the audit log — **admin only** (RBAC). Filters: `principal`, `action`, `result`, `tenant`, `since`, `until`, `limit`; page with `before=<next_before>`. Each entry's `target` names what was touched (`template:<id>@<version>`, `connection:<tenant>/<name>`, `origin:<name>`, `mcp:<tool>`); an MCP tool call is one entry per call, under the calling principal, `result: error` when the tool failed |
| `GET` | `/v1/audit/{id}` | `200` | One audit entry; `404` once purged — **admin only** |
| `POST` | `/v1/reload` | `200` / `422` | Hot-reload the `--default-config` merge base — **admin only** (RBAC). No-op (`reloaded:false`) if no default-config; `422` (old config kept) if the new one is invalid. A reload answers `{reloaded, path, loaded_at}` |
| `GET` | `/v1/catalog/datasets` | `200` | List catalogued datasets (`kind`, `q`, `limit`, `cursor`) — requires the `catalog` build feature |
| `GET` | `/v1/catalog/datasets/{id}` | `200` | One dataset's detail: schema timeline, volume, edges |
| `GET` | `/v1/catalog/lineage` | `200` | The lineage edge graph (`root`, `depth`) |
| `GET` | `/v1/usage` | `200` | Cost & usage report over recorded invocations (`by`, `since`, `until`, `pipeline`, `limit`, `include_records`) — viewer / `UsageRead`; requires the `catalog` build feature |
| `GET` | `/v1/local-outputs` | `200` | List tracked local sink output files with age + state (`dataset_id`, `pipeline`, `include_expired`, `limit`) — viewer / `LocalOutputRead` |
| `DELETE` | `/v1/local-outputs/{id}` | `200` | Delete one recorded output file now (operator / `LocalOutputManage`); `404` for an unknown id |
| `POST` | `/v1/local-outputs/cleanup` | `200` | Bulk clean: `older_than_days` \| `expired` \| `dataset_id` (optionally with `older_than_days`) \| `run_id` \| `all`, plus `dry_run` (operator / `LocalOutputManage`) |
| `POST` | `/v1/templates` | `201` | Register a pipeline template (admin / `TemplateAdmin`) — requires the `templates` build feature |
| `GET` | `/v1/templates` | `200` | List templates — newest version each, plus release state (viewer / `TemplateRead`) |
| `GET` | `/v1/templates/{id}` | `200` | One template version + its whole release state. `?version=stable` (default), another channel, or `?version=N` |
| `GET` | `/v1/templates/{id}/rows` | `200` | The template's selectable rows (streams / matrix rows) with their metadata; selector params (`select`, `only`, `skip`, `tags`, `status`, `include_parents`) turn it into a dry-run resolve (`TemplateRead`, audit `template.rows`) |
| `DELETE` | `/v1/templates/{id}` | `204` | Delete one version (`?version=<channel\|N>`) or all (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/runs` | `202` | Trigger a run from a template with `params` / `env` (operator / `RunWrite`) |
| `GET` | `/v1/templates/{id}/runs` | `200` | The runs started from the template (label `template: <id>`), paged like `GET /v1/runs` (`limit`, `cursor`, and `counts=true` for totals); `404` for an id never registered (viewer / `TemplateRead`) |
| `POST` | `/v1/templates/{id}/tags` | `200` | Point an assignable channel (`prod`, `dev`, …) at a version (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/launch` | `200` | Make a version live — moves `stable` and so unpinned callers (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/rollback` | `200` | Re-launch `previous` (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/deprecate` | `200` | Retire a template, or revive it with `{"undo":true}` (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/versions/{version}/deprecate` | `200` | Retire one version (`{"reason":"…"}`), or revive it with `{"undo":true}`. It still runs when pinned, with a `deprecated` warning; `newest` skips it and `launch` refuses it (admin / `TemplateAdmin`) |
| `POST` | `/v1/templates/{id}/versions/{version}/test` | `200` | Run the version's `tests:` bundle and record the result on it; `200` even when cases fail, `422` without a bundle (admin / `TemplateAdmin`) |
| `GET` | `/v1/templates/{id}/versions/{version}/tests` | `200` | The version's test status (`passing` / `failing` / `untested` / `no_tests`), `has_tests` and recent runs (`?limit=`, max 20) (viewer / `TemplateRead`) |
| `POST` | `/v1/templates/sync` | `200` | Pull the `--templates-sync` origins into the registry — `{origin?, dry_run?}`; one report per origin, appends only (admin / `TemplateAdmin`; requires the `templates-sync` feature; `422` when the server has no origins) |
| `POST` | `/v1/templates/{id}/publish` | `200` | Write one version back to an origin — `{origin, version?}` (admin / `TemplateAdmin`; `templates-sync`) |
| `GET` | `/v1/whoami` | `200` | The caller's `principal`, `role` and `permissions` (every role / `Identity`) |
| `GET` / `POST` | `/v1/status` | `200` | [Pipeline health](#pipeline-status-and-state) per row, from an inline `config` (operator) or a registered `template` (viewer / `StatusRead`) |
| `GET` / `POST` | `/v1/mirror/{name}` | `200` | [Per-table mirror status](#mirror-status) from the mirror's state store (viewer / `StatusRead` with a `template`; operator with an inline `config`) |
| `GET` | `/v1/state/{pipeline}/{row}` | `200` | A row's bookmark, envelope sequence and markers (admin / `StateAdmin`) |
| `PUT` | `/v1/state/{pipeline}/{row}` | `200` / `409` | Move the row's bookmark (admin / `StateAdmin`); `409` while a run holds the row |
| `DELETE` | `/v1/state/{pipeline}/{row}` | `200` / `409` | Reset the row so its next run re-syncs (admin / `StateAdmin`); `409` while a run holds the row |
| `POST` | `/v1/changes` | `201` | Propose a [change request](../cookbook/approvals.md) (`kind` = `run` / `template_register` / `template_launch` / `template_delete` / `template_deprecate`, `payload`, `reason`, `budget`); planned now, executed on approval — operator / `ChangeRequest` |
| `GET` | `/v1/changes` | `200` | List change requests (`status`, `kind`, `requester`, `limit`), newest first, plan rows omitted — viewer / `ChangeRead` |
| `GET` | `/v1/changes/counts` | `200` | `{"counts": {status: n}}`: exact totals over every request matching `kind`, `requester`, `tenant` (lapsed pending requests count as `expired`) — viewer / `ChangeRead` |
| `GET` | `/v1/changes/{id}` | `200` | One change request with its plan — viewer / `ChangeRead` |
| `POST` | `/v1/changes/{id}/approve` | `200` | Approve (`comment`); at quorum re-plans and executes, or marks it `invalidated` — `ChangeApprove`, then the `approvals:` policy |
| `POST` | `/v1/changes/{id}/reject` | `200` | Reject with a `reason`; the requester may withdraw their own — `ChangeApprove`, then the policy |
| `GET` / `POST` | `/v1/policy`, `/v1/policy/*`, `/v1/tenants/{tenant}/policy*` | `200` / `201` | The data-flow policy in force, its versions, refusals, dry run, preview, and proposed changes and rollbacks — see [Policy](#policy) |
| `GET`/`POST` | `/v1/tenants` | `200`/`201` | List / create [tenants](../cookbook/embedded-integrations.md) (`tenants` feature) — viewer / `TenantRead`, admin / `TenantAdmin` |
| `GET`/`PATCH`/`DELETE` | `/v1/tenants/{tenant}` | `200` | Read, update (`name`, `labels`, `limits`, `notifications`, `suspended`) or delete a tenant with everything faucet holds for it |
| `GET`/`POST` | `/v1/tenants/{tenant}/connections` | `200`/`201` | List (never credentials) / store a sealed connection `{name, provider: {type, config}}` — operator / `ConnectionManage` |
| `GET`/`PUT`/`DELETE` | `/v1/tenants/{tenant}/connections/{name}` | `200`/`204` | Read, replace (reconnect) or delete one connection |
| `POST` | `/v1/tenants/{tenant}/connect/{provider}` | `200` | Start a hosted OAuth flow — `{connection, redirect}` → `{authorize_url, expires_at}` |
| `GET` | `/v1/connect/callback` | `303` | OAuth redirect target (unauthenticated; the single-use `state` is the credential). Redirects with `status=ok`, or `status=error&error=<code>`; a provider `error` is passed on only when it is 1–64 characters of `[A-Za-z0-9_.-]`, else as `provider_error` |
| `GET` | `/v1/connect/providers` | `200` | The `--connect-providers` providers |
| `POST` | `/v1/tenants/{tenant}/runs` | `202` | A `POST /v1/runs` body, run as the tenant — operator / `RunWrite` |
| `POST` | `/v1/tenants/{tenant}/templates/{id}/runs` | `202` | A template trigger, run as the tenant — operator / `RunWrite` |
| `POST` | `/v1/templates/{id}/fanout` | `200` | Trigger a template once per tenant (`tenants: "all" \| [ids]`, `fanout_concurrency`) — operator / `RunWrite` |
| `GET` | `/v1/triggers` | `200` | The `--triggers` file's triggers with live health, `last_fire`, `fired_total` / `errors_total` since start (viewer / `StatusRead`; `triggers` feature) |
| `GET` | `/healthz` | `200` | Liveness (unauthenticated) |
| `GET` | `/readyz` | `200`/`503` | Readiness (unauthenticated) |
| `GET` | `/metrics` | `200` | Prometheus exposition (unauthenticated) |

### `POST /v1/runs`

Request body:

```json
{
  "config": "version: 1\npipeline:\n  source: {...}\n  sink: {...}\n",
  "config_format": "yaml",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600,
  "doctor_first": true,
  "idempotency_key": "airflow-task-123-attempt-2",
  "clock": "2026-05-29T00:00:00Z",
  "callback": {
    "url": "https://caller.example/jobs/abc/complete",
    "extra_fields": { "job_id": "abc" }
  }
}
```

- **`config`** (required) — the YAML or JSON pipeline body. A topology config
  (`pipeline.nodes`) runs as its node graph; it is refused (`422`) for a tenant
  run and with `doctor_first`, which probe matrix rows.
- **`config_format`** — `yaml` (default) or `json`.
- **`name`** — metadata; also drives the **state-key and metric identity** (see
  the cookbook's cardinality note). Two submissions sharing a `name` share
  replication bookmarks.
- **`labels`** — string key/value metadata stored on the run record and
  filterable with `GET /v1/runs?label=key:value` (#958). Keys are 1–63
  characters without `:` and values at most 256; anything else is a `422`
  (the filter splits on the first `:`, so a key holding one could never
  match). A value may hold `:` and `,`; filter on a `,` value with the
  repeatable `label=` form. The provenance
  keys the server stamps (`template`, `template_version`, `allow_untested`,
  `sink_template`, `sink_template_version`, `overlay`, `overlay_version`,
  `selection`, `depends_on`) are refused with a `422`; they stay filterable.
- **`timeout_secs`** — wall-clock cap; on expiry the run is marked failed.
- **`doctor_first`** — run preflight probes before executing; on any failure the
  submit returns `422` with the doctor report in `error.details`.
- **`idempotency_key`** — replay protection (see cookbook). Keys are scoped to
  the tenant a run is for, so two tenants sending the same key start two runs.
  The prefixes `tenant:`, `trig:`, `sched:`, `backfill:` and `change:` are
  reserved for the keys the server derives itself; a caller's key starting with
  one is a `400` on every route that submits a run (including tenant runs,
  template runs, fan-outs and `POST /v1/changes`).
  The request is identified by the config as submitted (before references
  resolve, merged with `--default-config`); a key claimed by the previous
  release, which identified it by the resolved config, still replays.
- **`clock`** — overrides the `${now.*}` clock for backfills (default: submit time).
- **`concurrency`** — overrides this run's **connector** concurrency: how many
  concurrent connections/fetches the source and sink may use, whatever the
  config says. This is the multi-tenant knob — one template driving a customer
  with beefy read replicas and one with a small instance, without per-customer
  config copies or the template author pre-declaring a `${param.*}`. It maps
  onto whichever knob the connector declares (`max_connections` /
  `partition_concurrency` / `shard_concurrency` / `concurrency`), so a
  connector with none ignores it. It does **not** change matrix parallelism
  (`execution.max_concurrent`) or the server's `--max-concurrent` slots, and it
  caps only the *client* side — it cannot raise what the upstream will accept.
  Per-shard for a sharded run. `0` is rejected. It is part of the idempotency
  fingerprint, so replaying a key with a different value is a 409, not a
  replay.
- **`callback`** — a per-run completion callback; see below.
- **`selection`** — run only some of the config's matrix rows (#741):
  `{ "select": [ids], "only": [globs], "skip": [ids|globs], "tags": [..],
  "status": [tiers], "include_parents": "off|eligible|all" }` — the CLI's
  selection flags. Omitted: every row runs; `{}` applies the status gate
  only. An unknown row or tag, an empty run set, a missing ancestor or a
  topology config is a `400` naming the valid rows. The run is labelled
  `selection=<canonical>` (part of the idempotency fingerprint) and the
  selection is stored with it, so a clustered or sharded run applies the same
  subset. The same field is accepted by every run-starting endpoint
  (template triggers, tenant runs, fan-out).
- **`depends_on`** — completion dependencies between the run's rows (#960):
  `{ "orders": ["customers"], "payments": ["orders"] }` starts `orders` only
  after `customers` completes. Ordering only, with no record hand-off (that
  is `parent:`); a failed or skipped prerequisite skips its dependents, as a
  config's own `depends_on:` does. The edges are **added** to the config's
  `depends_on:` — a request never removes one. Every name must be a matrix
  row of the run (a row without an `id` is `row-<n>`), and the combined
  `parent:` + `depends_on:` graph must stay acyclic; otherwise a `400` naming
  the edge (`b → ghost`, `a → c` for a cycle). A topology or a config without
  `matrix:` has nothing to order (`400`). With a `selection`, a prerequisite
  the selection leaves out follows `include_parents` like a declared one. The
  edges are folded into the stored config, so a clustered, claimed or sharded
  run applies them and they are part of the idempotency fingerprint (the same
  key with different edges is a `409`); the run record keeps them as
  `depends_on`, the run is labelled `depends_on=<row>:<prerequisites>;…`, and
  a change request shows them in its plan summary (different edges are a
  different change). Accepted by every run-starting endpoint (template
  triggers, tenant runs, fan-out, MCP `run_pipeline` / `run_template` /
  `propose_run`, a trigger's `run.depends_on`).
- **`pool`** — the execution pool to run in (#957). Wins over the config's
  `pool:`; with neither the run goes to `default`. Only a replica serving the
  pool (`faucet serve --pool`) claims the run and its shards. A pool no live
  cluster member serves — or, on a non-clustered server, one it does not serve
  — is a `422` naming the pool; so is a pool other than `default` while a
  member running a release without pools is live. Part of the idempotency
  fingerprint when it is not `default`. The same field is accepted by
  `POST /v1/templates/{id}/runs` (and tenant runs and fan-out) and
  `POST /v1/backfill`; the run record carries the resolved `pool`. On a
  template run a caller or a tenant starts, a pool other than the template's
  own `pool:` is a `422` unless the template's `pool_override` allows it
  (`true`, or a list naming the pool); the operator's triggers, schedules and
  fan-outs are not limited by it. See
  [Execution pools](../cookbook/cluster.md#execution-pools).

Response (`202`):

```json
{ "run_id": "0192…", "status": "queued", "submitted_at": "2026-05-29T12:00:00Z" }
```

A `--default-config` (if the server was started with one) is merged **under** the
submitted config (submitted values win).

### `GET /v1/runs`

Query parameters: `status`, `name`, `since`, `until` (RFC3339), `limit` (default
50, max 500), `cursor`, `tenant`, `submitted_by`, `mine`, `dataset_id`, `name_contains`, `pool`, `counts`, `label`, `labels`. Ordering is
`(submitted_at DESC, run_id DESC)`; `cursor` is the previous page's
`next_cursor` — the last row's sort key (`<submitted_at>~<run_id>`), so paging
stays correct when that run is deleted or purged between requests. A bare
`run_id` is still accepted. Every filter composes with the others and with the
cursor.

**Filtering by label (#958).** `label=key:value` keeps runs whose label `key`
is exactly `value`; `label=key` keeps runs that carry `key` at all. Repeat it
to require several (they are ANDed):

```bash
curl -s -G http://127.0.0.1:8080/v1/runs -H "Authorization: Bearer $TOKEN" \
  --data-urlencode label=team:data --data-urlencode label=env:prod
```

The value is split from the key on the **first** `:`, so `label=url:https://x`
matches the value `https://x`; URL-encode values as usual. `labels=team:data,env:prod`
is a comma-separated alias (a value containing `,` needs the repeatable form).
At most 10 label filters per request. Provenance labels filter like any other —
`label=template:orders`, `label=backfill:<range-hash>` (one backfill's units),
`label=pipeline:<name>`. A sharded run is listed once, as its parent, with the
labels it was submitted with.

**Filtering by submitter (#998).** Every run records who submitted it in
`submitted_by`: the authenticated principal (`trigger:<name>` for a trigger,
the requester for an approved change request, `anonymous` under `--no-auth`,
`token` for the single `--auth-token`). The server sets it; a request body
cannot. `submitted_by=<principal>` keeps that principal's runs, and `mine=true`
keeps the caller's own (setting both is a `400`). Runs recorded before the
field existed have no submitter and match neither.

**Filtering by dataset (#998).** `dataset_id=<catalog dataset id>` keeps the
runs one of whose invocations wrote that dataset — the `usage.dataset_id` a
run records while the catalog is active, and only that field (the same text
in a label does not match). Anything but letters, digits and `-` is a `400`.

**Filtering by title and pool (#998).** `name_contains=<text>` keeps runs whose
title — the run's `name`, or its `pipeline` label when it has none — contains
the text, case-sensitively (`%` and `_` are ordinary characters). `pool=<name>`
keeps runs in that execution pool; runs recorded before pools existed are in
`default`. An invalid pool name is a `400`.

**Exact counts (#998).** With `counts=true`, `counts` holds how many runs match
the filters, per status, ignoring `limit` and `cursor` — so a dashboard can show
totals without paging through every run. Only statuses with at least one run
appear. Without it the field is omitted: counting reads every matching run.

**Unknown parameters are a `400`.** A typo such as `?lables=team:data` is
refused rather than silently returning every run. So are an unknown `status`
token, an empty label key and more than 10 label filters.

```json
{
  "runs": [ { "run_id": "…", "status": "completed", "submitted_by": "alice", … } ],
  "next_cursor": "2026-10-01T12:00:00.000000000Z~0192…",
  "counts": { "completed": 41, "failed": 2, "running": 1 }
}
```

### `GET /v1/runs/{id}` → `RunRecord`

```json
{
  "run_id": "0192…",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "status": "completed",
  "submitted_at": "…", "started_at": "…", "finished_at": "…",
  "elapsed_secs": 12.4,
  "records_written": 4096,
  "pool": "default",
  "submitted_by": "alice",
  "dlq_location": "./dlq/orders.jsonl",
  "invocations": [
    {"row_id": "default", "parent_record_key": null, "records_written": 4096, "error": null,
     "status": "succeeded",
     "batches": {"attempted": 5, "committed": 5, "dlq_partial": 0, "dlq_all": 0, "failed": 0},
     "rows": {"transform_in": 4100, "transform_out": 4098, "offered": 4098, "accepted": 4096,
              "coalesced": 0, "skipped": 0, "failed": 2, "quarantined": 0, "dropped": 0,
              "unaccounted": 0},
     "source_lag": {"bytes": 4096}}
  ],
  "error": null,
  "idempotency_key": "airflow-task-123-attempt-2",
  "doctor_report": null
}
```

`status` is one of `queued`, `running`, `completed`, `failed`, `cancelled`.
`elapsed_secs` is filled live for running runs. A run's completion `callback`
comes back with every header value as `***` and URL credentials stripped, and
the stored `config_body` (cluster runs) is returned to admins only — it can
hold inline credentials. Each invocation carries its own `status`
(`succeeded`, `failed` or `skipped`, #952) and `error`, so a run of many rows —
one per `for_each` item, say — maps each row to its own outcome, and one
failed row never hides the others; records written before the field existed
omit it (derive it from `error` / `skipped`). Each invocation also carries
`batches` — how its sink writes ended (`committed` / `dlq_partial` / `dlq_all` /
`failed`, #737), `rows` — where every row went (#879): of the `offered` rows,
how many the sink `accepted`, merged into another row of the same page by a
keyed write (`coalesced`), declined by design (`skipped`), failed per row to the
DLQ (`failed`), a quality / contract / drift / policy pass `quarantined`, or a
poison-pill `drop` action `dropped`; `unaccounted` rows were lost by a sink
without saying so and fail the run; `transform_in` / `transform_out` count the
transform chain's input and output, the same view `faucet run --output json`
prints — and, for a source with a head, `source_lag` at the end of the
invocation (`bytes` / `events` / `seconds`, #733).

A DLQ replay that writes (`POST /v1/dlq/replay` without `dry_run`) is recorded
as a run under its `id` while it runs, with the label `dlq_replay: "true"` and
`replay_of` naming the DLQ location it read, so it appears in `GET /v1/runs`
and holds one of its tenant's `max_concurrent_runs` slots until it ends.

`dlq_location` (#998) is where the run's dead-letter queue writes: the `path`
of its `dlq:` sink as configured at submit, when that sink writes local files
(`file`, `jsonl`) — the `location` `POST /v1/dlq/inspect` takes. The server
sets it; it is absent without a file DLQ.

### `GET /v1/runs/{id}/config`

The config a run was submitted with, as stored (#998). Only clustered runs
store one (any instance must be able to run them); other runs answer `404`.

```json
{ "format": "yaml", "body": "version: 1\npipeline: …", "redacted": true }
```

An unscoped admin gets the body as stored. Anyone else gets it re-rendered in
the same format with every value under a credential-looking key (`password`,
`token`, `secret`, `key`, `credentials`, …) replaced by `***`; `${env:…}` /
`${vault:…}` references are kept, since they name a secret rather than hold
one. Secrets the server resolved are masked for everyone, and sealed template
params stay `${sealed:N}`. `redacted` says whether anything was masked. Every
read is audited as `run.config`.

> **Bookmarks:** run records carry record counts + per-row outcomes, not
> replication bookmarks. Bookmark state is per-row/per-state-key and lives in the
> configured [state backend](../cookbook/state.md), not in the run record.

### `GET /v1/runs/{id}/logs` (SSE)

`text/event-stream`. The server replays the run's bounded ring buffer, then
streams the live tail. Event types:

- `event: log` — one captured log line (subject to the server's `FAUCET_LOG`
  level; secrets are redacted).
- `event: truncated` — the reader fell behind and lines were dropped; rely on
  the centralized log sink for the full history.
- `event: end` — the run reached a terminal state; the stream closes.

The SSE buffer is **ephemeral**: it survives a short drain window after the run
finishes (independent of run-record retention), then is dropped. A known run
whose buffer has expired yields a single `end`.

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/v1/runs/0192…/logs
```

#### Persisted logs — `?format=jsonl` / `?format=text` (#529)

With a persistent `--history` backend and `--log-retention-secs > 0`, captured
(redacted) log lines are also stored durably, so they can be fetched **any time
after the run ends** — past the SSE drain window, and from any instance in a
cluster. Add a `format` query parameter to switch the same endpoint from the SSE
stream to a paginated read:

- `?format=jsonl` → `application/x-ndjson`, one `{seq, ts, level, line}` object
  per line, oldest-first. Paginate with `?after=<seq>&limit=<n>` (`limit`
  defaults to 1000, max 10000). A trailing `{"truncated":true}` record means
  earlier lines were dropped by the per-run cap.
- `?format=text` → `text/plain`, the lines concatenated.

```bash
# First page of durable logs, as NDJSON:
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/runs/0192…/logs?format=jsonl&limit=500"
# Next page: pass the last seq you saw.
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/runs/0192…/logs?format=jsonl&after=500"
```

Retention is governed by **`--log-retention-secs`** (default `auto`: 24 h when
logs are shipped, 7 days when the server is their only copy),
independent of run-record retention; `0` disables durable log persistence
(ephemeral SSE only). **`--log-max-lines-per-run`** (default `100000`) caps how
many lines are stored per run. The in-memory `--history` backend stays ephemeral
(no durable persistence) unless log shipping is on.

Each `jsonl` object also carries `attrs` (`target`, `row`, `connector`,
`invocation_id`, `trace_id`, …) when the line had them.

#### Log shipping (#806)

With `export: [logs]` in `--otel-config`, this store is the buffer the server
ships to the OTLP collector from: delivered lines are kept
`--log-retention-secs` *after delivery*, undelivered ones until delivered,
bounded by `--log-buffer-max-age-secs` (7 days) and `--log-buffer-max-bytes`
(1 GiB). `GET /v1/runs/{id}` then reports `log_export` (`status`:
`not_configured` / `pending` / `exported` / `failed` / `partially_dropped`,
`delivered_seq`, `total_seq`, `pending_lines`, `dropped_lines`, `last_error`,
`last_attempt_at`, `delivered_at`, `link`). Once a run's local copy has aged out
and `--log-link-template` is set, `?format=jsonl` ends with
`{"aged_out":true,"link":"…"}`, `?format=text` with the link, and the SSE stream
opens with an `event: link`. See [Shipping logs](../operations/observability.md#shipping-logs).

### `GET /v1/catalog/*` (Data Movement Catalog)

Read-only browsing of the [Data Movement Catalog](../cookbook/catalog.md)
accumulated in the server's `--history` backend (every serve run records into
it automatically). Viewer-readable under RBAC; requires a build with the
`catalog` feature.

- `GET /v1/catalog/datasets?kind=&q=&limit=&cursor=` — paginated dataset list,
  ordered `(last_seen DESC, id DESC)`; `q` is a case-insensitive URI substring.
  `next_cursor` is the last row's sort key (`<last_seen>~<id>`), so paging
  continues after that row even when a dataset's `last_seen` moves between
  requests (a dataset that runs again mid-listing moves ahead of the cursor and
  shows up on a fresh listing, not twice).
- `GET /v1/catalog/datasets/{id}` — the dataset plus its deduplicated schema
  timeline (each version with a `diff` vs the previous), recent per-run volume
  points, upstream/downstream lineage edges, and — once a `profiling:`
  pipeline has written it — `profile` (the latest run's per-column statistics
  + drift findings, and the recent history). `404` for an unknown id.
- `GET /v1/catalog/lineage?root=&depth=` — the source→sink edge graph; with
  `root` (a dataset id), a BFS slice bounded by `depth` hops.
- `POST /v1/catalog/datasets/{id}/consumers` — merge an owners / consumers
  annotation (#707): `{ owners?: [..], consumers: [{name, kind?, contact?,
  columns?}], replace?: bool }`. Owners replace the list when given;
  consumers upsert by `name`. `200` with the updated detail, `404` for an
  unknown id, `422` for an empty annotation. `CatalogAnnotate` (operator);
  audited `catalog.annotate`. What [impact analysis](../cookbook/impact.md)
  names.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/catalog/datasets?kind=postgres&limit=20"
```

### `/v1/changes*` (change requests)

Plan → approve → run: see the [change approvals cookbook](../cookbook/approvals.md)
for the full flow. `POST /v1/runs` accepts `require_approval` (plus `reason`
and `budget`); when it is set, or the server runs with
`--require-approval run`, the 202 body is
`{ "status": "pending_approval", "change_id": "…", "change": { … } }` instead
of a run. `budget` on a run or a change request (`max_records`, `max_bytes`,
`max_duration_secs`, `allowed_sinks`) is merged with the config's own
`budget:` block, the stricter of each ceiling winning.
Under `--require-approval run`, the writes that bypass the run queue —
`POST /v1/backfill`, `POST /v1/verify` with `repair`, `POST /v1/dlq/replay` and
`POST /v1/runs/{id}/rollback` — answer `403`: no change-request kind expresses
them, so they are disabled on that server (their `dry_run` forms still answer).

A template trigger that becomes a change request stores the template body
unresolved (`${env:}` / `${file:}` / secret references stay tokens and resolve
when the approved run executes), so a trigger that supplies a `secret: true`
param or `env` overrides is refused with `422`. Responses mask what the reader
may not see: callback `headers`, and credential-like query values in the
callback `url`, for everyone but an admin, and the proposed `config` for a
principal that cannot approve (a viewer). The body a `template_register`
change proposes, and the template body a template trigger's `run` change
carries, are masked for everyone but an unscoped admin, as on
`GET /v1/templates/{id}`; the plan is not. A `run` change's callback is
checked when the change is created, with the rules a submitted run's is
(above): one with credentials in its URL, or with `headers` on a server with a
persistent history, is refused with `422`. A `template_register` change that
also launches the template or assigns channels must satisfy the approval rule
for `template_launch` as well as its own. A tenant-scoped
principal cannot `POST /v1/changes` (`403`); its gated template triggers
become change requests on their own.

With `--require-approval template_register` / `template_launch`, the template
lifecycle routes refuse with `409` and point at `POST /v1/changes`:
`POST /v1/templates` (both kinds when it also launches or assigns channels),
`/launch`, `/rollback` and `/tags` (`template_launch`) and a non-dry-run
`/v1/templates/sync` (either kind); the MCP `register_template` /
`launch_template` / `rollback_template` tools refuse the same way. Under
`template_launch` (or `template_delete` / `template_deprecate`), a `DELETE` of
a launched version, the `newest` version, a channel's target or the whole
template, and a version `/deprecate`, refuse too: propose them as
`template_delete` / `template_deprecate` changes. An approved change executes
through the registry. `--templates-sync` registers and
launches on its own schedule, so the server refuses to start with it and
either template kind gated.

A refused approval is a `403` naming the rule (role, named principals,
self-approval); a request that is not `pending`, or a second approval by the
same principal, is a `409`. Audited as `change.requested` / `approved` /
`rejected` / `executed` / `invalidated` / `expired` / `failed`.

### Pipeline status and state

`GET /v1/status` (and `POST /v1/status` with the same fields in a JSON body)
returns the `StatusReport` behind [`faucet status`](cli.md#status): per row,
`health` (`ok` / `running` / `warming` / `unknown` / `degraded` / `failed`),
`last_success` / `last_failure` (from the state store's run-outcome marker,
the SLA history, and this server's run history — each tagged with its
`source`), `bookmark` and `bookmark_age_secs`, `exactly_once`
(`state_seq`, and with `probe=true` the sink's watermark, `agreement` and which
side the next run `trusted`), `overwrite_staging` (`state` present / absent /
unknown, `verified` only with `probe=true`), `lag` (the source's lag reported
when the last run ended, or with `probe=true` queried now — `measured`
`last_run` / `probe`, plus `human`), `batches` (the last run's sink-write
outcomes), `dlq` backlog, `sla` verdicts (including `lag`), `profiling`,
`rollback`, `children`, the `resume` sentence, and per-field `errors`. The
top-level `health` / `exit_code` are the worst row's. A run in flight on this
server shows as `running` — every submitted run carries a `pipeline` label
(the config's `name`, else `serve`, unless the caller set one) that ties it
to its pipeline whatever the run is named. Name the config with `config` (inline YAML / JSON,
`config_format`) **or** `template` (+ `version`, a number or channel,
default `stable`; `sink` for a source template; `params` in the POST body).
A viewer may name a `template`; an inline `config` is loaded on the server
(its references resolve and its connectors are built), so it needs `operator`.

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  --get --data-urlencode "template=orders" http://localhost:8080/v1/status | jq '.rows[] | {row, health, resume}'
```

`/v1/state/{pipeline}/{row}` reads (`GET`), moves (`PUT` with `bookmark`,
optional `parent_key`, `dry_run`, `force`, `skip_watermark_check`) or resets
(`DELETE` with `parent_key`, `include_markers`, `rewind_token`, `dry_run`,
`force`, `skip_watermark_check` as query parameters) one row's durable state,
with the same exactly-once rules as [`faucet state`](cli.md#state). The config
is named the same way, and its pipeline name must equal `{pipeline}`. A
mutation answers `409` while the row's run lease is live or this server has a
run of the pipeline in flight, unless `force`. Every call is admin-only and
audited (`state.get` / `state.set` / `state.reset`; status as `status`).

### Mirror status

`GET /v1/mirror/{name}` (and `POST` with the config in a JSON body) returns the
report behind [`faucet mirror status`](cli.md#mirror): `mode` (`tables` /
`single`), a `summary` (tables per phase, how many are lagging) and one entry per
table — `phase` (`pending` / `snapshotting` / `active` / `paused` / `dropped` /
`refused`), `snapshot` (`rows`, `estimated_rows`, `shards_done` /
`shards_total`, `percent`, `attempts`), `changes`, the committed `position`,
`lag_secs` / `lagging`, `consecutive_failures`, `last_error`, `key` and
`write_mode`. It is read from the mirror's state store, so it answers whether
the mirror runs in this server or in a separate `faucet mirror` process. Name
the config with `config` or `template` as for `/v1/status`; its `name:` must
equal `{name}` (else `422`), and a mirror that has not started is a `422`.
Viewer-readable (`StatusRead`) with a `template`; an inline `config` needs
`operator`. Audited as `mirror.status`.

```bash
curl -s -H "Authorization: Bearer $TOKEN" --get \
  --data-urlencode "config@shop_mirror.yaml" http://localhost:8080/v1/mirror/shop_mirror \
  | jq '.tables[] | {table, phase, lag_secs, last_error}'
```

### `/v1/schedules` (schedules)

Registered templates run on a cron (#1044); see the
[Schedules cookbook](../cookbook/schedules.md).

| Method | Path | Permission | Audit |
|---|---|---|---|
| `GET` | `/v1/schedules[?tenant=]` | `ScheduleRead` | `schedule.list` |
| `POST` | `/v1/schedules/preview` | `ScheduleRead` | `schedule.preview` |
| `GET` | `/v1/schedules/{name}[?tenant=]` | `ScheduleRead` | `schedule.get` |
| `POST` | `/v1/schedules[?reason=]` | `ScheduleWrite` | `schedule.create` |
| `PUT` | `/v1/schedules/{name}[?reason=]` | `ScheduleWrite` | `schedule.update` |
| `DELETE` | `/v1/schedules/{name}[?reason=]` | `ScheduleWrite` | `schedule.delete` |
| `POST` | `/v1/schedules/{name}/pause` (`{"reason"}`) | `ScheduleWrite` | `schedule.pause` |
| `POST` | `/v1/schedules/{name}/resume` | `ScheduleWrite` | `schedule.resume` |
| `POST` | `/v1/schedules/{name}/run` | `ScheduleWrite` | `schedule.run` |

`ScheduleRead` is granted to viewers and up, `ScheduleWrite` to operators and
up. A create, edit or delete answers `202` with a pending change request
under `--require-approval schedule`. Under `--require-approval run` a create
or edit answers `202` too, run now answers `409`, and resuming a schedule
that was not stored through an approved change answers `409`.
A schedule from the `--schedules` file answers `409` to edit, pause and
delete. Every schedule ticks at most every 5 minutes, and a cron with a
seconds field is refused (`422`). A schedule's params are its creator's:
one that names a server file or an internal URL the
[caller reference policy](#reference-policy) does not allow is refused with
`403` when the schedule is created or edited. A tenant principal sees and
changes only its own tenant's schedules (another tenant's is a `404`), never
fans out (`403`), and is held to `max_schedules` and the minimum tick
interval (`422` naming the limit); on an operator's fan-out it sees only its
own tick errors and none of the change history.
Without the `schedule` and `templates` features `GET /v1/schedules` answers
`{"available": false}` and no other route exists.

### `/v1/tenants*` (multi-tenant embedded integrations)

See the [embedded integrations cookbook](../cookbook/embedded-integrations.md).
A run for a tenant resolves `auth: { ref }` against the tenant's connections
first, binds `${tenant.id}` / `${tenant.name}` / `${tenant.labels.<key>}`,
namespaces its state keys `{tenant}::{pipeline}::{row}`, carries `tenant` on
its run record, usage, audit entries and change requests, and is held to the
tenant's `limits` — `429` (`limit_exceeded`) at `max_concurrent_runs`; the
per-run ceilings join the run's budget. A suspended tenant, or a row that
references a missing connection or one that needs re-authorization, is a
`409`. `GET /v1/runs`, `/v1/changes`, `/v1/usage` and `/v1/audit` accept
`tenant=`; `GET /v1/usage?by=tenant` groups by it. Storing connections needs
the server's `--vault-key`; without it those routes answer `503`. Audited as
`tenant.*`, `connection.*` (`connection.needs_reauth` when a grant is revoked)
and `connect.start` / `connect.complete`.

### `GET /v1/overview` (fleet trends)

The aggregates behind the console's [Overview](../cookbook/web-console.md#overview)
page, read from the run history rather than Prometheus, so they survive a
restart and cover every replica of a cluster:

- `runs`: totals `by_status`, a `series` of succeeded / failed / cancelled per
  bucket, `failure_rate_by_pipeline` (finished runs, highest rate first),
  `slowest` (p95 invocation time per pipeline), and `queued` / `running`
  now, whatever the window;
- `records`: records written per bucket and the `top_pipelines`;
- `governance`: rows not written, from each invocation's row accounting:
  `filtered` by a transform, `quarantined`, sent to the DLQ (`dlq`),
  `coalesced`, `skipped` and `dropped`, per bucket;
- `cost` (the `catalog` build, when usage records exist): the estimated cost
  and hosted equivalent per bucket and by pipeline, in the currency most
  invocations were priced in (`other_currencies` lists the rest);
- `freshness` (the `catalog` build, when datasets are catalogued; not for a
  tenant-scoped principal): datasets with no successful run within
  `stale_after` (default `24h`), and datasets whose schema changed in the
  window;
- `attention`: change requests waiting for approval, unhealthy triggers,
  templates whose newest version's tests fail, and tenant connections that
  need re-authorization. A group is present only when its feature is built
  in and in use, with a `count` and the first 20 `items`;
- `grafana` (with `--grafana-url`): a link per section to the matching
  dashboard from `observability/grafana/`, over the same range.

The window is `[since, until)`: `since` is a duration back from `until`
(`30m`, `24h`, `7d`, `2w`, default `7d`) or an RFC 3339 instant, and `until`
defaults to now. Buckets are UTC hours up to three days and days beyond
(`bucket=hour|day` overrides; at most 1,000 buckets). Runs are grouped by
pipeline: a run's `pipeline` label, else its run name. The server sets that
label to the config's `name:` (`serve` for an unnamed config) unless the
submitter set it, so it normally matches the pipeline cost is grouped by.
`pipeline` filters by that key; `pool`,
`label` / `labels` and `tenant` filter runs as on `GET /v1/runs`; a
tenant-scoped principal sees its own tenant only. The SQL history backends
group in the database; the p95 is computed over the newest 10,000 finished
invocations in the window (`runs.duration_sample.truncated` says when the
window held more), and `top` (1–50, default 10) bounds every list. `RunRead`
(viewer and up); not audited when allowed, since the console polls it (a
`403` denial is audited `overview.get`); `400` for a bad parameter.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/overview?since=24h&pipeline=orders"
```

### `GET /v1/usage` (cost & usage)

The [cost & usage report](../cookbook/usage.md) over every invocation this
server has recorded, aggregated by `by=pipeline|row|dataset|sink|day|tenant`
(default `pipeline`) within an optional half-open `since` / `until` window
(RFC 3339 or `YYYY-MM-DD`) and an optional `pipeline`. `limit` caps the
invocation records read (newest first, default 5000); `include_records=true`
returns them alongside the report. Estimates are priced with each run's
`usage:` table and labelled as such; `hosted_equivalent` is what a per-row-
priced hosted ELT service would charge for the same rows. `UsageRead`
(viewer and up); audited `usage.list`; `400` for a bad `by` / timestamp.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/usage?by=day&since=2026-09-01"
```

### Local sink outputs

Lists and reclaims the **local files** the server's sinks wrote (jsonl / csv /
parquet). The control surface behind the console's Datasets-page cleanup
controls, and the same engine as the background sweeper described under
[Local output retention](cli.md#local-output-retention). Requires a build with
the `catalog` feature.

- `GET /v1/local-outputs?dataset_id=&pipeline=&include_expired=&limit=` — the
  tracked outputs, newest write first, each with `state`, `age_secs`, and the
  retention window in force. The response also carries the server's default
  `retention_days`, whether the sweeper is running (`gc_enabled`), and whether
  the **caller** may delete (`can_manage`), so a client can hide destructive
  controls rather than offer buttons that only 403.
- `DELETE /v1/local-outputs/{id}` — delete one file now.
- `GET /v1/local-outputs/{id}/preview?row_count_to_load=N` — the **first N rows
  of the file**, with their column names. Opt-in: inert (`403`, naming the flag)
  unless the server was started with `--preview-local-outputs`. See
  [Preview](#preview) below.
- `POST /v1/local-outputs/cleanup` — bulk clean. Exactly one scope:
  `{"older_than_days": N}`, `{"expired": true}` (each output's own window),
  `{"dataset_id": "…"}`, `{"run_id": "…"}` ("clean up after that run" — its
  history record is untouched), or `{"all": true}`. Sending none or several is a
  `400` rather than a guess — except `{"dataset_id": "…", "older_than_days": N}`,
  which purges that one dataset's outputs older than N days (the dataset page's
  age purge). Add `"dry_run": true` to see what would go.

  A scope that ignores retention windows — `all`, and `older_than_days: 0`, which
  matches every output — also needs `"confirm": true`, or it is refused with a
  `400`. That is the same gate as the CLI's `--yes`, decided by the same
  predicate, so a scripted caller cannot inherit the console's confirm dialog by
  accident.

`state` is `present` (on disk), `expired` (collected — the record is kept), or
`external` (faucet wrote the file but did not create it).

**A refusal is a `200`, not an error.** The report carries `deleted: 0` and a
`skipped` reason: `pre_existing` (faucet did not create the file — never
deleted, by any scope), `in_flight` (the file may still be being written; retried
later), `not_on_disk` (already gone — a no-op, and the record is marked expired),
`already_deleted`, `delete_failed`, `relative_path` (a row recorded before paths
were stored absolute — it would resolve against the sweeper's directory, not the
writer's), or `other_host` (another host wrote the file; each row records its
writer's host, and only a sweeper there deletes it). The host identity is the
`FAUCET_HOST_ID` environment variable when set, else the host name: set it to a
stable value (e.g. the StatefulSet pod name or the volume's name) when a
container's host name changes on restart but its output volume persists, or
outputs written before a restart are never collected. `changed` means the file
at the recorded path is not the one faucet wrote (each row records the device
and inode of the file it wrote, and of its directory): somebody else's file is
never deleted, and neither is an output whose directory changed after it was
recorded (a directory replaced by a symbolic link, say). The file is deleted
through its directory held open from the root, so a directory changed after
that check cannot redirect the delete. A record written before directory
identities were recorded is checked without following any symbolic link on its
path, so such an output under a symlinked directory is reported `changed`.

`in_flight` covers two cases, because one is not enough: the output's ledger row
names a run that is currently executing, **or** the file itself was touched
within `--local-output-in-flight-grace-secs` (default 60). The second is what
protects a *new* run rewriting a path the ledger still attributes to the previous
run — a run id the ledger has not recorded yet. Only recorded paths are ever touched:
never a glob, never a directory. Run history, catalog entries, and lineage are
untouched.

```bash
# What would "clean everything" remove?
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"all": true, "dry_run": true}' \
  http://127.0.0.1:8080/v1/local-outputs/cleanup

# Reclaim anything older than 3 days.
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"older_than_days": 3}' \
  http://127.0.0.1:8080/v1/local-outputs/cleanup
```

#### Preview

`GET /v1/local-outputs/{id}/preview` reads a tracked output back and returns its
first rows — the other half of "N records written". It is a **source-backed capped
read**: the server builds the matching *source* connector for the output's kind
(`file` → `source-file`, `csv` → `source-csv`, `parquet` → `source-parquet`,
`jsonl` → its JSON Lines reader), pulls one page, and stops. A 100-row preview of a 4 GiB file reads its
first few kilobytes; nothing past the cap is decoded.

**It is off by default.** Without `--preview-local-outputs`
(`FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS`) every request is a `403` naming the flag,
for every role — it is a server capability, not a permission. Reading needs
`LocalOutputRead` (`viewer` and up), the same scope that lists these files, and a
served preview writes a `local_output.preview` **audit** entry naming the
principal, the output, and the row count: it is the one read on this control plane
that returns pipeline *data* rather than metadata about a pipeline, and "who read
this file" cannot be reconstructed after the fact.

An output in state `external` is **never** previewed (`403`). faucet wrote to that
file but did not create it, so its contents are not faucet's to hand out — the
read-side twin of the retention GC's refusal to delete it. The GC's other two
refusals apply as well (`409`): an output recorded with a relative path, and one
written on another host (in a cluster, the same path on this host is a
different file — preview it from the writing instance).

The request names a **ledger id**, never a path: the path comes from the row the
sink wrote, so a preview cannot be aimed at another file.

There is **no offset and no cursor** — these sources are sequential streams with
no row index, so `OFFSET N` could only mean "read N records and discard them",
which costs exactly what a larger limit costs. "Show me more" is spelled "raise
the limit", and the engine makes that cheap by stopping rather than truncating.

| Parameter | Behaviour |
|---|---|
| `row_count_to_load` omitted | The soft cap — `--preview-default-rows` / `FAUCET_SERVE_PREVIEW_DEFAULT_ROWS` (default 500). |
| `row_count_to_load=N` | `N`, clamped to the hard cap — `--preview-max-rows` / `FAUCET_SERVE_PREVIEW_MAX_ROWS` (default 5000). Never honoured above it. |
| `row_count_to_load=all` (or `0`) | The whole dataset — served in full only where the operator lifted the ceiling with `--preview-max-rows 0` (`preview_max_rows: null`); otherwise it resolves to the ceiling. |
| anything else | `400` naming the parameter — never a silent fall back to the default, which would let a capped read pass for a whole file. |

The response carries the rows, the `columns` across them (the table header; empty
when the records are not JSON objects), `row_count` (rows returned), the
`row_limit` the request resolved to (`null` = unlimited), the server's `max_rows`
(`null` = no ceiling), and `truncated` — which is *observed* (one row past the cap
is read) rather than inferred, so "exactly 500 rows" is distinguishable from
"capped at 500".

When `truncated` is true, **`capped_by` says which bound stopped the read**:
`rows` (the row limit), `bytes` (a 64 MiB response-size budget), or `time` (a 30s
deadline). The last two are what make an uncapped read safe to offer: a dataset
larger than the server can hold comes back as as much of it as fits, plus the
reason — never an out-of-memory, and never a clipped table that looks complete.
`capped_by` is absent when the response *is* the whole dataset.

Failure modes are all typed, and none of them is a 500:

| Status | Meaning |
|---|---|
| `403` | Previews disabled on this server; the role lacks `LocalOutputRead`; or the output is `external` — a file faucet wrote to but did not create, whose contents are not faucet's to serve (the same reason the retention GC will not delete it). |
| `404` | No such tracked output. |
| `409` | The file is gone — collected by retention, or removed out of band. The ledger row and the run record are kept; the message says so. |
| `422` | The file is there but unparseable (e.g. a half-written last line from a run that died mid-flush). The message carries the connector's own line/offset diagnostic. |
| `400` | The output's kind has no reader, or this build lacks the source connector for it. |
| `503` | The read was abandoned after the 60-second hard timeout — a single page that never returned, not a verdict on the file's contents. (The 30-second deadline is different: it yields a partial `200` with `capped_by: "time"`.) |

```bash
# The first 20 rows of a tracked output.
ID=$(curl -sH "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/v1/local-outputs | jq -r '.outputs[0].id')
curl -sH "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/local-outputs/$ID/preview?row_count_to_load=20" \
  | jq '{columns, row_count, truncated, capped_by}'

# Every row (needs a server started with --preview-max-rows 0; otherwise this
# comes back clamped to the ceiling, with capped_by: "rows").
curl -sH "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/local-outputs/$ID/preview?row_count_to_load=all" \
  | jq '{row_count, row_limit, truncated, capped_by}'
```

### `/v1/templates*` (template registry)

Register a template once, then trigger runs by `{id, params}` instead of
re-sending a config. The registry holds three **kinds** of document, told apart
by their `kind:` line: a `source-template` (one system — its connector, shared
transforms, and streams), a `sink-template` (one destination), and a complete
`pipeline`. A source template runs **composed** with a sink template named in the
trigger body; a pipeline runs alone; a sink template is never run on its own.
Storage rides the server's `--history` backend, so `faucet template …` and the
MCP template tools see the same registry. Requires a build with the `templates`
feature; see the [cookbook page](../cookbook/templates.md) and the
[Template Hub](../cookbook/template-hub.md).

```bash
# Register (the body is stored verbatim — ${env:…} / ${vault:…} stay unresolved).
curl -sX POST http://127.0.0.1:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"tenant-sync","config":"version: 1\nname: tenant-sync\n…","config_format":"yaml"}'
# → 201 {"id":"tenant-sync","version":1,"params":{…},"created_at":"…","created_by":"…"}

# Template tests and labels (#856): registering runs the version's `tests:`
# block and records the result; only a passing version takes a label.
curl -sX POST http://127.0.0.1:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"config":"kind: pipeline\n…\ntests:\n  suite: …"}'
# → 201 {…,"tests":{"result_id":"…","passed":false,"cases":[{"name":"rows","passed":false,"attempts":2,…}],…}}
curl -sX POST http://127.0.0.1:8080/v1/templates/orders/launch \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"version":2}'
# → 422 {"error":{"code":"unprocessable","message":"template 'orders' v2 is failing — launch needs a passing version: …",
#        "details":{"tests":{"id":"orders","version":2,"status":"failing","action":"launch","reason":"…","failing":["rows: …"]}}}}
curl -sX POST http://127.0.0.1:8080/v1/templates/orders/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"version":2,"allow_untested":true}'   # a pinned, unlabelled version; labelled + audited
# → 202 {…}

# Trigger a pipeline template.
curl -sX POST http://127.0.0.1:8080/v1/templates/tenant-sync/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"params":{"tenant_id":"acme"},"env":{"API_HOST":"eu.example.com"},"version":2}'
# → 202 {"run_id":"…","status":"queued","submitted_at":"…",
#        "template_id":"tenant-sync","template_version":2,
#        "params":{"tenant_id":"acme","api_token":"***"},"streams":[]}

# Register a source template and a sink template (ids are `owner/name`) …
curl -sX POST http://127.0.0.1:8080/v1/templates -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"config":"kind: source-template\nname: billing\nowner: acme\n…","launch":true}'
curl -sX POST http://127.0.0.1:8080/v1/templates -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"config":"kind: sink-template\nname: bigquery\nowner: faucet-hq\n…","launch":true}'
curl -s "http://127.0.0.1:8080/v1/templates?kind=sink-template" -H "Authorization: Bearer $TOKEN"

# … and run the pairing. An id's `/` is percent-encoded in the path (`acme%2Fbilling`);
# the trigger names the sink, and binds both halves' params.
curl -sX POST http://127.0.0.1:8080/v1/templates/acme%2Fbilling/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sink":"faucet-hq/bigquery","sink_version":"stable","params":{"api_token":"…","bq_project":"my-project"}}'
# → 202 {"run_id":"…","template_id":"acme/billing","template_version":1,
#        "sink_template":"faucet-hq/bigquery","sink_template_version":1,
#        "streams":[{"stream":"bills","requested":["overwrite","upsert"],"chosen":"overwrite","key":["id"]}, …],
#        "params":{"api_token":"***","bq_project":"my-project"}}
```

**Rows and selection (#741).** `GET /v1/templates/{id}/rows?version=&sink=&sink_version=&overlay=`
lists a source template's streams or a pipeline template's matrix rows —
identity (`status`, `tags`, `default_selected`), hierarchy (`parent`,
`children`, `depends_on`, `depth`, `per_parent_record`), `write` (resolved
against `sink` when given, with `supported` / `unsupported_reason`), `read`,
`guarantees`, `shape`, `params_used`, and the `faucet status` view as `state`
when the state store is readable (`?state=false` skips it; for a
tenant-scoped principal it is the tenant's own state — its `<tenant>::`
state keys — and only its own runs). Any selector
parameter (`select`, `only`, `skip`, `tags`, `status` comma-joined;
`include_parents`) resolves that selection without running anything: rows gain
`selected` / `pulled_in` / `blocked` / `excluded`, and the body gains `run_set`
and the `error` a trigger would return. A topology template returns
`rows: []`, `selectable: false`. The trigger body takes the same `selection`
object as `POST /v1/runs`; for a source template only the selected streams are
composed, so a stream the sink cannot run does not block the others. It also
takes `depends_on` (#960) over the template's rows or streams — for a source
template the edges join the streams' own `depends_on:` before the selection
resolves (so `include_parents` follows them), an edge on a stream the
selection leaves out is dropped with it, and a `for_each` stream is named by
its rendered row name. The response echoes the edges the run applies.
`?params=` (a URL-encoded JSON object of param values) lists a source
template's `for_each` streams as the rows a trigger with those items would
run (#952) — `?params={"tables":["orders","customers"]}` — and selectors take
the rendered names; without it a repeated stream is listed as two placeholder
items and `notes` says so.

**List params and `for_each` (#952).** A trigger supplies a `type: list`
param as a JSON array (`"params": {"tables": ["orders", "customers"]}`).
Every item is checked before the run is queued — a wrong-typed item, one
outside its `values:`, a list over `max_items` (or under `min_items`, default
1), a rendered stream name that is invalid or taken twice — and refused with
`422` naming the item (`tables[2]`). The response's `streams[]` lists every
expanded row. A `secret` list (or a `secret` field of its items) is shown as
`***` in `params`.

**New pairs (#951).** Registering one half of a new source + sink pair whose
`tests:` name each other records a red result for the first (its companion is
not registered: `missing_companions`); registering the second tests it
against the first's newest version (`companion_notes`) and re-runs the first's
tests, returned as `companions_retested` — then both launch. A trigger whose
`sink` (or `overlay`) names no registered template answers `422` naming the
kind it looked for (`no sink-template 'x' in the registry`).

```bash
curl -s "http://127.0.0.1:8080/v1/templates/crm/rows?sink=files&select=deal_lines&include_parents=eligible" \
  -H "Authorization: Bearer $TOKEN"
curl -sX POST http://127.0.0.1:8080/v1/templates/crm/runs -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sink":"files","selection":{"select":["deal_lines"],"include_parents":"eligible"}}'
# → 202 {…,"selection":{"select":["deal_lines"],…}} — the run is labelled
#   selection=select=deal_lines;include_parents=eligible
```

**Matrix.** `GET /v1/templates/matrix` composes every registered source
template with every registered sink template and returns the catalog index
shape (`sources`, `sinks`, `matrix[]` with per-stream write modes and a
`command` per compatible pairing) — the console's compatibility grid. Like
`/sync`, `matrix` is a static route, so no template can take that id.

**Kinds.** `GET /v1/templates` rows carry `kind` (`?kind=` filters); rows written
before kinds existed read as `pipeline`. A source template is registered under
its `name` (an explicit `id` must match), its document is validated as a hub
template and run through the publishability lint (a literal credential or a
private hostname is a `422`), and a re-register can never change a template's
kind under the same id. A trigger on a source template without `sink` is a `422`
naming the field; `sink` on a pipeline template is a `422`; a trigger on a sink
template is a `422` pointing at the source side. The composed run's `name` is
the pairing (`{source}.{sink}`), so its state keys are scoped to that sink; a
deployment overlay with `state_scope: source` names it after the source
template so the keys survive a sink swap. The run is labelled `sink_template` / `sink_template_version` beside
`template` / `template_version`. Registering a document with no `kind:` still
works as a pipeline but is deprecated: add `kind: pipeline`.

**Deployment overlays.** A `kind: deployment` template carries the operational
blocks a composed run gets from neither template — `state`, `dlq`,
`notifications`, `sla`, `profiling`, `policy`, `resilience`, `execution`, `delivery`, `schedule`, and
per-stream `sla` / `dlq` / `delivery` under `streams:` (see
[Deployment overlays](../cookbook/template-hub.md#deployment-overlays)). It is
registered like any template, never triggered on its own (`422`), and applied
with `overlay` on a source-template trigger — a registered id (with
`overlay_version`, default `stable`) or an inline mapping, whose `kind` / `name`
may be omitted:

```bash
curl -sX POST http://127.0.0.1:8080/v1/templates/acme%2Fbilling/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sink":"faucet-hq/bigquery","overlay":"prod","params":{"state_dsn":"…"}}'
# → 202 {…, "overlay":"prod","overlay_version":2,
#        "overlay_contributes":["pipeline.state","pipeline.dlq","matrix.bills.sla"], "warnings":[]}
```

An overlay that would change connectors or streams, names a stream the source
lacks, or declares a param differently from the templates is a `422`; `overlay`
on a `pipeline` template is a `422`. The run is labelled `overlay` (and
`overlay_version` for a registered one), and `warnings` flags incremental
streams composed with no state store.

An **inline** overlay is the request's own document, so it is screened like a
submitted config before it is merged into the (trusted) template body: its
`${env:}` / `${file:}` / `${secret:}` / secret-manager references, every local
path (a `file` state store, a file DLQ sink), every URL (a notification
webhook, an `http` DLQ sink) and any connector that runs a program are held to
the [caller reference policy](#reference-policy) — refused with `403` (`422`
for a subprocess connector without `--allow-subprocess-connectors`). It may use
literal values and the `${now.*}`, `${backfill.*}` and `${partition.*}` tokens
only: a `${param.*}`, `${vars.*}` or row token is refused (`403`), because its
value would be bound after the screen. An MCP `run_template` dry run applies the
same checks. A run for
a tenant (a tenant-scoped principal, or `POST /v1/tenants/{tenant}/…`) may not
carry an inline overlay at all (`403`); register the overlay and name it
instead. Every refusal is audited as a denied `template.run`.

**Registering never moves callers.** `POST /v1/templates` appends a version and
stops there; `POST /v1/templates/{id}/launch` is the one call that moves `stable`
and therefore every unpinned caller. So a template is `draft` until something is
launched (an unpinned trigger is a `422`), then `launched`, and `deprecated` once
retired — a deprecated template still serves pinned and `stable` callers, but the
trigger response carries a `deprecated` field. Pass `launch: true` on register to
do both in one call.

**Version selection.** Versions are numeric and auto-incrementing. On top of them
sits a closed channel set: three **derived** — `stable` (the launched version, and
what an omitted selector resolves to), `previous` (the rollback target), `newest`
(the build tip) — and six **assignable**: `dev`, `test`, `staging`, `pre-prod`,
`canary`, `prod`. There is deliberately no `latest`: it means both "newest build"
and "current release", so it is rejected with a message naming `stable` and
`newest`. `version` accepts a channel name (`"prod"`), a numeric string (`"2"`), or
a bare number (`2`), so a query string and a JSON body agree. `0` and unknown
channel names are rejected rather than silently falling back, and asking for an
*unset* channel is a `422` phrased for that channel (`stable` needs a launch,
`previous` needs a second launch, an environment channel needs a promote).

`POST /v1/templates/{id}/tags` moves an assignable channel:
`{"tag":"prod","version":"stable"}` copies whatever `stable` names today;
`{"tag":"prod","version":3}` pins one. A derived channel cannot be assigned
(`422`) — `stable` moves only via `launch`. `POST /v1/templates/{id}/launch`
defaults to `newest` and returns `{version, replaced, already_launched, status}`;
re-launching the live version is a no-op, which keeps `previous` a real rollback
target. `GET /v1/templates/{id}` returns `status`, `versions` (newest first),
`stable` / `previous` / `newest`, `is_stable`, the `tags` pointer map, the
`launches` log, and `registrations` — each stored version's `registered_at` and
`registered_by` (the principal; `sync:<origin>` for a template sync,
`local:<user>` for `faucet template register`; absent for a version recorded
without one), newest first — so a client can pin, promote, launch, or roll back
without a second request. Use `?version=newest` to read a `draft` template. The stored
`body` is returned only to an admin with no tenant scope; anyone else gets an
empty `body` and `body_withheld: true` (the parameter surface stays in
`params`), the same rule as a run's `config_body`. In `params`, the
default of a `secret: true` param always reads `"(hidden)"`, here and in
`GET /v1/templates`; runs still use the real default.

The trigger body's `params` / `env` / `version` / `sink` / `sink_version` /
`overlay` / `overlay_version` are template-specific; every other field (`name`, `labels`, `timeout_secs`,
`doctor_first`, `idempotency_key`, `clock`, `concurrency`) behaves exactly as in
`POST /v1/runs`, because the run is submitted through the same path. The run is
labelled `template` and `template_version` (plus `sink_template` /
`sink_template_version` for a composed run, `overlay` / `overlay_version`
when an overlay applied, `allow_untested` for a run of a non-`passing` version,
`selection` for a row selection and `depends_on` for request-supplied
dependencies). Only the server sets these: a caller's
`labels` entry under any of those keys is dropped (on `POST /v1/runs`,
`POST /v1/backfill` and `POST /v1/changes` it is a `422`).

Status codes: `404` for an unknown id or pinned version; `422` for a missing
`required` param or a type mismatch, naming the param; `429` when the queue is
full. On a **clustered** server the materialized config is persisted for peer
execution, so a trigger that supplies a `secret: true` param is sealed under the
server's vault key (`--vault-key` / `FAUCET_VAULT_KEY`): the stored config — and
`config_body` in `GET /v1/runs/{id}` — carries `${sealed:N}` tokens, the values
are stored encrypted (bound to the run id) apart from the record, opened only by
the member that runs it, and deleted once the run is terminal. Without a vault
key, or with `env` overrides, the trigger is refused with `422`; reference the
secret from the template body (`${env:…}` / `${vault:…}`, resolved on the
executing instance) instead. See
[clustered execution](../cookbook/cluster.md#3-per-run-secret-template-params-need-a-vault-key).

### `POST /v1/backfill`

Plans a `[from, to)` range into window units (chunked by `window`) and submits
**one tracked run per unit** — see the [backfill
cookbook](../cookbook/backfill.md) for the model.

```json
{
  "config": "version: 1\nname: orders\npipeline: {...}\n",
  "config_format": "yaml",
  "from": "2026-06-01",
  "to": "2026-07-01",
  "window": "1d",
  "timezone": "UTC",
  "name": "orders",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600,
  "resume": false,
  "restart": false
}
```

- **`config`** (required) — every root source must reference a `${backfill.*}`
  or `${now.*}` scoping token (400 otherwise). Bookmark-range backfills are
  CLI-only.
- **`from`** / **`to`** (required) — RFC3339 or `YYYY-MM-DD` (midnight in
  `timezone`), half-open.
- **`window`** / **`timezone`** — default to the config's `backfill:` block.
- **`name`** — base run name; unit runs are `{name}-backfill-{range_hash}-{unit}` (the
  pipeline `name` is rewritten per unit so state keys never touch the live
  bookmark). `delivery` is forced to `at_least_once`; `timeout_secs` applies
  per unit.
- **`resume`** / **`restart`** — with a `state:` block the submitted range is
  recorded in that state store, and a later POST of the same range is refused
  with `409`: each unit would resume its old bookmark and read nothing. Set
  `resume: true` to continue that backfill (submitted units replay, the rest
  submit) or `restart: true` to delete each unit's scoped state and submit
  every unit as a new run. Setting both is a `422`. Without a `state:` block no
  unit keeps a bookmark, so the range is not recorded.

`202` response: `{backfill, descriptor, planned, submitted, units: [{unit,
start, end, status, run_id?, error?}]}` where `backfill` is the stable range
hash carried as the `backfill` label on every unit run (plus a `backfill_unit`
label). Each unit is submitted with the deterministic idempotency key
`backfill:{hash}:{unit}`, so **re-POSTing the same body is replay-safe** —
already-submitted units replay their existing run, the rest submit (a full
queue marks the remainder `not_submitted`; re-POST with `resume: true` to
continue). A config
carrying `shard: {count}` makes each unit a sharded run tracked via shard
progress. Requires `RunWrite` (operator); audited as `backfill.submit`.

### `POST /v1/verify`

Compare one root row's destination to its source by content (#701) — see the
[verification cookbook](../cookbook/verify.md) for the digest + bisection
model.

```json
{ "config": "version: 1\n…", "config_format": "yaml", "row": "orders",
  "repair": false, "allow_delete": false, "dry_run": false, "max_differences": 1000 }
```

`200` with the report: `strategy` (`range` / `full`), `ranges_compared`,
`ranges_differing`, `server_digests`, `rows_fetched_source` /
`rows_fetched_dest`, `differences: [{key, kind, columns?}]`, `truncated`, and
`repaired_upserts` / `repaired_deletes` when a repair ran. A mismatch is a
result, not an error. Requires `RunWrite` (operator); audited as `verify`.

### `POST /v1/plan`

Plan a config without running it (#283 / #707) — the report behind
[`faucet plan`](./cli.md#plan).

```json
{ "config": "version: 1\n…", "config_format": "yaml", "row": "orders",
  "sample": [{ "id": 1, "email": "a@x.io" }], "impact": true, "depth": 5 }
```

`200` with the plan: the resolved row, the lineage ops, and — given
`sample` records — the output schema / volume / sink schema delta from the
offline harness; `policy` when the server's `--policy` (or the config's
block) applies; `impact` with `impact: true` — the downstream datasets,
contracts, owners and declared consumers the planned schema affects, each
with a severity (`breaking` / `additive` / `unknown`), walked over this
server's catalog. Nothing is written and no run starts (the sink is built
only for its non-mutating probe), but the config is loaded on the server —
its references resolve and discovery reaches its hosts — so it is `Plan`
(operator); audited as
`plan`. `impact` needs the `catalog` feature (`422` otherwise). A topology config
(`pipeline.nodes`) is refused with `422`: a plan is per matrix row, and a node
graph has none.

**Policy refusals.** Every submission (`POST /v1/runs`, template triggers,
backfills, trigger fires) is checked against the server's policy — its
`--policy` file, or the stored version in force — plus, for a tenant run, the
tenant's policy, merged with the config's own `policy:` block, before it is
queued: a violation is a `422` whose `details` carry the per-row report, and a
`policy.denied` audit entry whose `detail` names the first violation's rule,
column, label and sink. Registering a violating pipeline template warns
(`warnings[]` on the summary) instead — it may be composed with a compliant
sink later. A runtime backstop denial fails the run and is audited as
`policy.denied` by principal `runtime`, under the run's tenant.

### Policy

The data-flow policy a server enforces (#1043). See
[Data-flow policies](../cookbook/policies.md#under-faucet-serve) for the model.

| Method | Path | Answers | Who |
|---|---|---|---|
| `GET` | `/v1/policy` | The server policy in force: `source` (`file` / `server`), `store` (`--policy-store`: `server` / `file`), `persistent_history` (`false`: edits are refused), `loaded`, `file` (path, sha256), `version` (who requested and approved it), `policy` + `yaml`, `rules` (outlines), `summary` (rules, labels, sinks with attributes, catalog columns seen per label), `refusals_7d`, `editable`, `file_drift`, `pending_change`. `file_drift` is set on a server-store server started with `--policy`: `{path, differs}`, plus the file as canonical `yaml` when it `differs` from the version in force (it was not applied; propose it with `POST /v1/policy` to import it). A tenant principal gets `rules` outlines and its own counts only. | viewer / `PolicyRead` |
| `GET` | `/v1/policy/versions` | The server-managed versions, newest first, each with its YAML. | viewer / `PolicyRead` (unscoped) |
| `GET` | `/v1/policy/refusals` | `since`, `tenant`, `limit`: what the policy refused (`phase` submit / runtime, `rule`, `column`, `label`, `sink`, `layer`, `outcome`). A tenant principal sees its own. | viewer / `PolicyRead` |
| `POST` | `/v1/policy/check` | Dry run: `config` (or `template` + `version`), `row`, `sample` → the policy report, without running anything. Not for tenant principals. | operator / `Plan` |
| `POST` | `/v1/policy/preview` | `{policy}` → `valid`, `problems` (line + message), `diff`, `impact` (templates and recent runs newly refused / allowed). Stores nothing. | admin / `PolicyAdmin` |
| `POST` | `/v1/policy` | `{policy, reason, base_version}` → `201` with a `policy_update` change request. `409` on the in-memory history ("set a persistent --history to edit the policy here"), with `--policy-store file`, or with a stale `base_version`; `422` with `details.problems` for a draft that does not validate. | admin / `PolicyAdmin` |
| `POST` | `/v1/policy/rollback` | `{version, reason}` → `201` with a `policy_update` change re-enacting that version. | admin / `PolicyAdmin` |
| `GET` | `/v1/tenants/{tenant}/policy` | The server rules that apply to the tenant (outlines), its add-only policy, versions, refusal counts, pending change. | viewer / `PolicyRead` |
| `POST` | `/v1/tenants/{tenant}/policy` (`/preview`, `/rollback`) | As the server routes, for the tenant's policy: a `tenant_policy_update` change, approved by another admin of the tenant or a server admin. | the tenant's admins and server admins / `PolicyAdmin` |

A change is never applied by these routes: approving the change request
(`POST /v1/changes/{id}/approve`) re-checks it and stores the next version,
or marks it `invalidated` when another version landed first. Audited as
`policy.get`, `policy.versions`, `policy.refusals`, `policy.check`,
`policy.preview`, `policy.request`, `policy.rollback`, `policy.updated`
(by the approver, with the version) when a change is applied, and
`policy.imported` (by `system`) when a `--policy` file seeds an empty store.

### `POST /v1/runs/{id}/rollback`

Undo one invocation of a finished run (#706) — see the [rollback
cookbook](../cookbook/rollback.md).

```json
{ "invocation_id": "019…", "row": "orders", "config": "version: 1\n…",
  "config_format": "yaml", "dry_run": false, "force": false }
```

- **`invocation_id`** — one of the run's `invocations[].run_id`; optional when
  the run has exactly one invocation.
- **`config`** — the config the run was made with; optional when the server
  stored it (cluster mode), `422` otherwise.

`200` with the report: `applied`, `blocked`, `mode`, `deleted`, `restored`,
`conflicts`, `bookmark_rewound`, `token_rewound`, `note`. `blocked: true`
means a later run changed the keys and nothing was touched — pass `force`. `409` while the run is still running. A rollback that writes
(not `dry_run`) passes the data-flow policy and, for a tenant's run, the
tenant's admission (`409` suspended, `429` at its run limit), like any write.
Admin-only (`Rollback` permission); audited as `run.rollback`.

## Completion callbacks

Instead of polling, a submission can name an endpoint to be POSTed when the run
reaches a terminal state. The destination rides the *submission*, not the config,
so one registered pipeline (or template) can serve many callers each reporting to
their own endpoint.

```json
"callback": {
  "url": "https://caller.example/jobs/abc/complete",
  "method": "POST",
  "headers": { "X-Caller": "orchestrator" },
  "extra_fields": { "job_id": "abc" },
  "on": ["completed", "failed", "cancelled"]
}
```

Accepted on `POST /v1/runs` and `POST /v1/templates/{id}/runs`. The body:

```json
{
  "event": "run.completed",
  "run_id": "0192…",
  "status": "completed",
  "name": "nightly-rollup",
  "labels": { "requester": "airflow" },
  "submitted_at": "2026-05-29T12:00:00Z",
  "started_at": "2026-05-29T12:00:01Z",
  "finished_at": "2026-05-29T12:04:11Z",
  "elapsed_secs": 250.4,
  "records_written": 14203,
  "error": null,
  "attempt": 0,
  "job_id": "abc"
}
```

`run_id` is the id returned by the submission. `error` is redacted. `extra_fields`
are merged at the top level; a key colliding with any field above is refused with
`422` at submit time rather than silently dropped.

`on` defaults to **every** terminal status. Narrowing it is a footgun: a callback
subscribed only to `completed` never fires for a failed or cancelled run, and a
caller waiting on it will hang.

> **Delivery is at-most-once, and best-effort.** The callback fires from the
> in-process terminal transitions. It is **not** fired when a run is failed by
> lease-expiry orphan recovery, by cluster reclaim-poison, or by the
> sharded-parent completion sweep — those happen inside the history backend.
> So treat a missing callback as **unknown**, never as "still running", and
> reconcile against `GET /v1/runs/{id}`, which is always authoritative. A
> non-2xx response is retried a few times with backoff, then dropped with a
> warning; the run's recorded outcome is never affected.

Refusals (all `422` at submit time, so a bad destination never becomes a silent
no-op an hour later):

| Condition | Why |
|---|---|
| Scheme is not `http`/`https` | |
| The URL carries credentials (`https://user:password@…`) | The URL is stored with the run and shown to its readers. Authenticate with a header (in-memory history) or a capability token in the path. |
| Host is link-local / cloud-metadata (`169.254.0.0/16`, `fe80::/10`, `fd00:ec2::254`, `100.100.100.200`, `metadata.google.internal`, …) | Closes the instance-metadata SSRF hole. Override by naming the host in `--callback-allow-host`. |
| A **tenant-scoped** principal names a loopback, private, shared (`100.64.0.0/10`), unique-local (`fc00::/7`) or unspecified address, or `localhost` | A tenant cannot submit a config, so its callback must not reach the server's own host or network. Override by naming the host in `--callback-allow-host`. |
| `--callback-allow-host` is set and the host is not in it | Explicit allowlist mode. |
| `headers` supplied to a server with a **persistent run history** (`--history sqlite:` / `postgres://`, clustered or not) | The server stores the run record — including these values — in its history database, which would keep them in clear text. Authenticate without a request header (e.g. a capability token in a single-use URL path), or submit to a server with in-memory run history. |
| `extra_fields` key collides with a faucet-emitted field | Would let a submission spoof the `status`/`event` a receiver keys off. |
| `on` contains a non-terminal status | |
| Supplied on `POST /v1/backfill` | One backfill POST fans out into N unit runs, so a single callback has no single run to describe. Poll the unit runs by their `backfill` label instead. |

The link-local check (and, for a tenant's run, the loopback / private one)
holds at delivery too: an IPv4-mapped IPv6 literal
(`[::ffff:169.254.169.254]`) is refused, a host name is resolved and refused
when any address it resolves to is refused (the connection is then pinned
to the checked addresses, so a second DNS answer cannot redirect it), and a
redirect response is never followed — a `3xx` counts as a failed delivery.

**Egress posture.** For a caller who can submit a config this guard closes the
metadata hole; it is not a general egress control. Such a caller can already
point a `rest` source at an arbitrary address, so the deployment-level mitigations in the
[serve cookbook](../cookbook/serve.md) still apply. Use `--callback-allow-host`
(repeatable) when you want callbacks restricted to known receivers.

## Error envelope

Every error is a JSON `ApiError`:

```json
{ "error": { "code": "unprocessable", "message": "…", "details": { } } }
```

| Status | When |
|--------|------|
| `400` | Malformed body / parse / interpolation failure; a `schedule:` block in the config |
| `401` | Missing/invalid bearer token |
| `403` | Authenticated, but the principal's role lacks the required permission (RBAC); a tenant-scoped principal submitting a config; a config that reads the [server's own credentials](#server-credentials); a `Host` or `Origin` the server does not answer ([Authentication](#authentication)) |
| `404` | Unknown `run_id` |
| `409` | `DELETE` on a running run; idempotency key reused with a different payload; a pipeline-state change while a run holds the row; a template lifecycle call the server gates behind approval |
| `413` | Body exceeds `--body-limit-bytes` |
| `415` | A body sent with a `Content-Type` the route does not take (`/mcp`, a webhook trigger) |
| `422` | Expand/validation failure; `doctor_first` failed (report in `details`); a [subprocess connector](#subprocess-connectors) without `--allow-subprocess-connectors` |
| `429` | Run queue full (carries `Retry-After`); a per-tenant limit; too many invalid bearer tokens from the address |
| `500` | Internal error. The message is generic and carries a `reference`; the server log line with that reference holds the detail |

**A row that is already running.** Every run takes a lease on each row in the
config's durable `state:` store, and the server never overrides a live one
(there is no `--force` over HTTP). A second run of the same row — another
`POST /v1/runs`, a trigger or template fire, a backfill unit, or a `faucet run`
/ `faucet schedule` process on the same store — is accepted (`202`) but its
invocation **fails** with an error naming the holder's run id, pid and lease
expiry. Recovery: wait for the holder to finish, cancel it
(`POST /v1/runs/{id}/cancel`, which releases the lease as it stops), or, if its
process crashed on another host, wait for the lease to expire about a minute
after it stopped renewing.

## Metrics

`/metrics` serves the standard `faucet_*` pipeline metrics plus serve-specific
series: `faucet_serve_requests_total{method,path,status}`,
`faucet_serve_request_duration_seconds{method,path}`, `faucet_serve_runs_queued`,
`faucet_serve_runs_in_flight`, `faucet_serve_runs_total{status,reason}`
(`reason="pool_unavailable"` for a run `--pool-wait-timeout-secs` failed),
`faucet_serve_runs_pending{pool}` (cluster mode: unclaimed runs per execution
pool),
`faucet_serve_idempotency_hits_total`, `faucet_serve_history_degraded` (the
backend is unreachable and the server admits no new work),
`faucet_serve_pending_finalize` (finished runs' terminal records waiting for
the history to take them), `faucet_serve_pending_finalize_dropped_total`
(terminal records dropped after the history kept refusing them),
`faucet_serve_pending_finalize_refused_total` (cluster mode: terminal statuses
the history refused; kept and retried with a backoff),
`faucet_serve_policy_seed_pending` (the `--policy` file is not in the policy
store yet), `faucet_serve_run_reload_retries_total` (claimed runs whose
config could not be re-loaded yet and is retried in place),
`faucet_serve_policy_merge_failures_total{tenant}` (a tenant's policy no
longer merges with the server's; its runs are refused) and
`faucet_serve_history_unreachable` (the last
lease heartbeat did not reach the history database; `/readyz` is `503` while
it is `1`), and `faucet_serve_history_connections_discarded_total` (pooled
history connections dropped because a failed commit left them inside a
transaction; the pool opens a fresh one).
With the `tenants` feature: `faucet_serve_tenant_runs_total{tenant,outcome}`,
`faucet_serve_tenant_limit_rejections_total{tenant,limit}`,
`faucet_serve_connections{status}` and
`faucet_serve_connect_flows_total{provider,outcome}` (`outcome` is `started`,
`completed`, `failed` or `superseded`). See
[Observability](../operations/observability.md).
