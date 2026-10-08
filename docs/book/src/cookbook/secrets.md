# Secrets-manager interpolation

faucet can pull secret values directly from HashiCorp Vault, AWS Secrets Manager,
GCP Secret Manager, and Azure Key Vault — using `${scheme:reference}` directives
right inside your config file. Resolution happens at config-load time: values are
fetched concurrently, de-duplicated, substituted into the config tree, and
**never written to disk or logs**.

These directives join the existing load-time set: `${env:VAR}`, `${file:PATH}`,
and `${secret:VAR}` (alias for `${env:}`).

## Build features

None of the four backends are compiled in by default. Opt in per backend or take
all four with the aggregate feature:

```bash
# All four backends
cargo install faucet-cli --features secrets

# Individual backends
cargo install faucet-cli --features secrets-vault
cargo install faucet-cli --features secrets-aws-sm
cargo install faucet-cli --features secrets-gcp-sm
cargo install faucet-cli --features secrets-azure-kv
```

Using faucet-cli from source or as a library dependency:

```bash
cargo build -p faucet-cli --features secrets
```

The `full` aggregate feature includes all four backends.

## HashiCorp Vault (KV v2)

**Directive:** `${vault:<path>[#field]}`

**Auth:** set `VAULT_ADDR` and `VAULT_TOKEN` in the environment. `VAULT_NAMESPACE`
is optional (for HCP Vault or enterprise namespaces).

The `#field` selector parses the secret body as a JSON object and extracts one
key. Omit it to receive the entire secret body as a string.

```yaml
# Requires: VAULT_ADDR + VAULT_TOKEN, and a KV v2 secret at
# secret/data/faucet/api with a `token` field.
# Build with: --features secrets-vault
version: 1
name: rest-to-jsonl-with-vault
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/items
      auth:
        type: bearer
        config:
          token: "${vault:secret/data/faucet/api#token}"
  sink:
    type: file
    config:
      path: ./out/items.jsonl
```

## AWS Secrets Manager

**Directive:** `${aws-sm:<name-or-ARN>[#field]}`

**Auth:** the standard `aws-config` default credential chain — environment
variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`),
`~/.aws/credentials` profile, EC2/ECS instance credentials, web identity token,
or IAM role attached to the compute environment. No manual config needed beyond
what the AWS SDK picks up automatically.

The `#field` selector works the same as for Vault: it parses the secret as JSON
and extracts one key.

```yaml
# Build with: --features secrets-aws-sm
version: 1
name: postgres-to-bigquery-secure
pipeline:
  source:
    type: postgres
    config:
      connection_url: "${aws-sm:prod/postgres#connection_url}"
      query: "SELECT * FROM events WHERE created_at > now() - interval '1 day'"
  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: analytics
      table_id: events
      auth:
        type: application_default
```

## GCP Secret Manager

**Directive:** `${gcp-sm:projects/<project>/secrets/<secret>/versions/<version>}`

Use `versions/latest` to always fetch the current active version.

**Auth:** Application Default Credentials — run `gcloud auth application-default login`
for local development, or rely on the service account attached to GCE/Cloud Run
in production. No extra environment variables needed.

```yaml
# Build with: --features secrets-gcp-sm
version: 1
name: rest-to-gcs-secure
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.partner.com
      path: /v2/export
      auth:
        type: bearer
        config:
          token: "${gcp-sm:projects/my-project/secrets/partner-api-token/versions/latest}"
  sink:
    type: gcs
    config:
      bucket: my-export-bucket
      prefix: exports/
      auth:
        type: application_default
```

## Azure Key Vault

**Directive:** `${azure-kv:<vault>/<secret>[/<version>]}`

Omit the version segment to fetch the current (enabled) version.

**Auth:** the `azure_identity` default chain — `AZURE_TENANT_ID` /
`AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` environment variables (service
principal), managed identity (when running in Azure), or `az login` (developer
tools). These are tried in that order; the first that succeeds is used.

```yaml
# Build with: --features secrets-azure-kv
version: 1
name: rest-to-snowflake-secure
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/records
      auth:
        type: bearer
        config:
          token: "${azure-kv:my-vault/api-token}"
  sink:
    type: snowflake
    config:
      account: myaccount.us-east-1
      warehouse: LOAD_WH
      database: RAW
      schema: PUBLIC
      table: records
      auth:
        type: oauth
        config:
          token: "${azure-kv:my-vault/snowflake-token}"
```

## The `#field` JSON extractor

Both Vault and AWS Secrets Manager support storing multiple values as a JSON
object inside one secret. The `#field` selector lets you extract a single key
(GCP Secret Manager and Azure Key Vault references refuse `#`):

```yaml
# Secret at prod/db contains: {"host": "db.example.com", "password": "s3cr3t"}
connection_url: "postgresql://app:${aws-sm:prod/db#password}@${aws-sm:prod/db#host}/mydb"
```

Each reference is fetched and de-duplicated — the same `(scheme, path)` pair is
fetched exactly once even if it appears in multiple config fields.

If the field is absent, faucet surfaces a clear error listing the available keys.
If the secret body isn't valid JSON when `#field` is used, faucet errors rather
than returning raw bytes.

## Validating configs with secrets

**With resolution (real preflight):** `faucet validate` resolves all secrets
as part of config validation and prints one line per reference to confirm
which secrets were reached (never the values):

```
secret: vault:secret/data/faucet/api#token → resolved
ok: 'rest-to-jsonl-with-vault' rows=1 (roots=1, children=0) execution=(defaults)
  - default [root] source=rest sink=jsonl
```

**Offline (no network / credentials):** `faucet validate --no-secrets` validates
grammar and structure only, skipping all secret fetches. Use this in CI steps
that don't have credentials, or in local development before you have vault access:

```bash
faucet validate --no-secrets pipeline.yaml
```

**Grammar reference:** `faucet schema secrets` prints the full directive syntax
and auth requirements for all four schemes in machine-readable JSON:

```bash
faucet schema secrets
```

## Resolution order

Secret directives resolve as the **final** load-time stage, after `${env:}` /
`${file:}` / `${vars.X}` / `${sources.X}` are all settled. This means you can
use env vars to compose a secret path:

```yaml
pipeline:
  source:
    type: rest
    config:
      auth:
        type: bearer
        config:
          token: "${vault:secret/data/${env:APP_ENV}/api#token}"
```

Substitution order: `${env:APP_ENV}` resolves first (during the raw text pass);
the resulting path `secret/data/prod/api#token` is then fetched from Vault.

## Redaction guarantee and its boundary

faucet scrubs every resolved secret value from its own tracing / log / error
output. Every byte written through the CLI's tracing subscriber passes through a
`RedactingWriter` that replaces any registered secret value with `***`. Errors
that contain deserialized config fields go through the same scrubber before they
reach stderr.

Every resolution path registers its result for redaction — the secrets-manager
directives (`${vault:…}`, `${aws-sm:…}`, …) **and** the load-time
`${env:…}` / `${secret:…}` / `${file:…}` forms. A credential supplied via the
common `${env:TOKEN}` form is therefore scrubbed exactly like a `${vault:…}` one
(values shorter than 4 characters are not registered). The `faucet serve` bearer
auth token (`--auth-token` / `FAUCET_SERVE_AUTH_TOKEN`) is registered the same way.
The scrubber withholds a short trailing window between writes, so a secret split
across two separate log writes is still masked. Independently, `faucet_core::Credential`
and the built-in auth providers hand-write their `Debug` to print secrets as `***`,
so a `{:?}` of a credential or shared provider never reveals the token.

**This boundary covers faucet's own output only.** A third-party connector that
debug-logs its own deserialized config fields — or any library that logs a
`reqwest::Request`, a database row, or a JSON object — operates outside this
boundary. In particular:

- Do **not** enable `RUST_LOG=debug` or `FAUCET_LOG=debug` when running a
  pipeline whose connector configs hold resolved secrets. The connector libraries
  may log intermediate objects that contain the resolved value before faucet's
  scrubber can see it.
- Prometheus metric labels and span attributes set by connectors are also outside
  this boundary.
- The scrubber does not redact values shorter than 4 characters.

## Secrets in the `auth:` catalog and `vars:` block

Secret directives are resolved **everywhere** config interpolation runs:
connector configs (including topology `nodes`, the mirror snapshot source and
table overrides, and the `verify` / `reconcile` read-back connectors),
transforms, state, dlq, matrix rows, `pipeline.masking`, `lineage`,
`notifications`, `catalog`, `observability`, the top-level **`auth:`**
shared-provider catalog, and the top-level **`vars:`** block. A secret
directive anywhere else fails the load, rather than reaching a consumer as the
literal `${vault:…}` text.

Putting a secret in the shared `auth:` catalog is often the cleanest option — a
single bearer token resolved once and shared across every matrix row that
references it via `auth: { ref }` (one token cache, single-flight refresh):

```yaml
# A secret in the shared catalog, resolved once and shared by reference.
auth:
  api:
    type: static
    config:
      token: "${vault:secret/data/app#token}"

pipeline:
  sources:
    orders:  { type: rest, config: { base_url: https://api.example.com/orders,  auth: { ref: api } } }
    refunds: { type: rest, config: { base_url: https://api.example.com/refunds, auth: { ref: api } } }
  sink: { type: file, config: { path: ./out.jsonl } }
```

A secret in the `vars:` block works the same way and can be reused through
`${vars.X}`:

```yaml
vars:
  db_password: "${aws-sm:prod/db#password}"

pipeline:
  source:
    type: postgres
    config:
      connection_url: "postgres://app:${vars.db_password}@db.internal:5432/app"
  sink: { type: file, config: { path: ./rows.jsonl } }
```

The shared `auth:` catalog is a first-class config location in every respect:
its provider specs can also reference `${vars.X}` and `${sources.X.PATH}`, not
just secret directives.

> Inline `auth:` blocks on individual connectors resolve secrets too — use the
> shared catalog when several connectors share one credential, and inline auth
> when a credential belongs to a single connector.
