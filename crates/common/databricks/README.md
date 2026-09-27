# faucet-common-databricks

Shared Databricks SQL warehouse types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) Databricks connectors
— [`faucet-source-databricks`](https://crates.io/crates/faucet-source-databricks)
and [`faucet-sink-databricks`](https://crates.io/crates/faucet-sink-databricks).
End users do not need this crate directly; both connectors re-export what they
use.

## What is in it

- **`DatabricksAuth`** — `{ type: pat | token, config: { token } }`, sent as
  `Authorization: Bearer <token>`. Its `Debug` output masks the token.
- **`resolve_authorization`** — a shared `auth: { ref }` provider wins over the
  inline auth; a dangling `ref` is a typed auth error.
- **`StatementClient`** — the
  [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
  lifecycle: submit (`POST /api/2.0/sql/statements`), poll until terminal,
  follow result chunks, cancel on a client deadline.
- Response types (`StatementResponse`, `StatementStatus`, `ResultColumn`, …),
  `StatementRequest` / `StatementParam` for named `:param` markers, and
  `value_to_param_string`.

## Retry rules

A retried submit that the warehouse had already accepted would run a
non-idempotent statement twice, so retries are narrow:

| Request | Retried on |
|---|---|
| submit (`POST`) | `429`, `503` (the warehouse or gateway refused it — nothing ran); `401` once when a shared provider can mint a fresh token |
| poll / chunk (`GET`) | `429`, any `5xx`, transport errors |

Backoff is exponential from `StatementOptions::retry_backoff` (capped at 30 s)
and honours a numeric `Retry-After`. A statement still `PENDING`/`RUNNING`
past `statement_timeout` is cancelled (best-effort) and the call fails.

## OAuth M2M (service principal)

Databricks OAuth machine-to-machine is a standard client-credentials grant, so
it is configured as a shared provider in the CLI's top-level `auth:` catalog and
referenced from the connector — the provider refreshes the token before expiry
and every connector sharing it reuses one token:

```yaml
auth:
  databricks_sp:
    type: oauth2
    config:
      token_url: https://dbc-xxxx.cloud.databricks.com/oidc/v1/token
      client_id: "${env:DATABRICKS_CLIENT_ID}"
      client_secret: "${env:DATABRICKS_CLIENT_SECRET}"
      scopes: [all-apis]

pipeline:
  sink:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      schema: sales
      table: orders
      auth: { ref: databricks_sp }
```
