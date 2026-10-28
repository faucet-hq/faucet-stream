# faucet-common-redshift

Shared connection, credentials, and connection-pool types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) Amazon Redshift
**source** (`faucet-source-redshift`) and **sink** (`faucet-sink-redshift`)
connectors.

Redshift speaks the PostgreSQL wire protocol, so the `password` and `iam`
methods connect through `sqlx`'s Postgres driver; `redshift_data_api` runs every
statement through the Redshift Data API over HTTPS instead. This crate
centralises all three so TLS, auth, pooling and the Data API behave identically
on both sides.

## Types

- **`RedshiftCredentials`** — adjacently-tagged `{ type, config }` enum:
  - `password` — username/password auth (the user comes from the connection
    block).
  - `iam` — temporary database credentials over the PostgreSQL wire protocol.
    Set exactly one of `cluster_identifier` (provisioned:
    `redshift:GetClusterCredentials` with `db_user` — default the connection's
    `user` — `auto_create`, `db_groups`) or `workgroup_name` (serverless:
    `redshift-serverless:GetCredentials`, the user comes from the IAM
    identity). `duration_secs` (900–3600) sets the lifetime; `region`,
    `aws_credentials` and `endpoint_url` select how AWS is called. The pool
    fetches credentials on first use and again 60 s before they expire (half
    way through a shorter lifetime), so every connection it opens afterwards
    logs in with valid ones; open connections are unaffected. A refresh that
    fails while the current credentials are still valid is logged and retried
    after a backoff (a quarter of the remaining lifetime, 1–60 s), not on
    every use; once they have expired it fails the operation as
    `FaucetError::Auth`. Credentials are never refetched sooner than 1 s after
    the last fetch, even when they look expired already (a skewed clock).
  - `redshift_data_api` — every statement goes through the Data API
    (`ExecuteStatement`, polled with `DescribeStatement` until `FINISHED` /
    `FAILED` / `ABORTED`, rows paged with `GetStatementResult`). Set exactly
    one of `cluster_identifier` (with exactly one of `db_user` or
    `secret_arn`) or `workgroup_name` (optional `secret_arn`). `host`, `port`,
    `user` and the TLS settings are not used and may be left out; `database`
    is required.
- **`RedshiftAwsCredentials`** — the AWS credentials `iam` and
  `redshift_data_api` call AWS with, the same shape as the other AWS
  connectors: `default` (the SDK chain), `profile { name }`,
  `access_key { access_key_id, secret_access_key, session_token? }`,
  `assume_role { role_arn, session_name?, external_id? }`, `web_identity`.
- **`RedshiftConnection`** — `host`, `port` (default `5439`), `database`,
  `user`, `credentials`, a `tls` toggle (default `true`), and the optional
  `tls_mode` (`disable` | `prefer` | `require` | `verify_ca` | `verify_full`,
  overrides `tls`) + `ssl_root_cert` (CA PEM path for the verifying modes;
  without it they verify against the Mozilla root store bundled into the
  binary, not the operating system's).
  Flattened into both end configs.

## Helpers

- `build_connect_options(&RedshiftConnection)` — pure `PgConnectOptions` builder.
  `tls: true` → `sslmode=require` (encrypted, certificate **not** verified);
  `tls: false` → `sslmode=prefer`. Set `tls_mode: verify_full` with
  `ssl_root_cert` (the Redshift CA bundle) to verify the server certificate and
  host name; `ssl_root_cert` with a non-verifying mode is a config error.
- `build_pool_lazy(conn, max)` — lazily-connected pool (no I/O at construction).
- `build_pool(conn, max)` — eagerly validated pool (fails fast on bad creds).
- `resolve_password(&RedshiftCredentials)` — extracts the password (`iam` and
  `redshift_data_api` have none and return `FaucetError::Config`).
- `connect_lazy(conn, max)` → `RedshiftPool` — the wire pool for `password` and
  `iam`; `RedshiftPool::get(&CallContext)` returns the pool, refreshing `iam`
  credentials first when they are due (the call `credentials`).
- `DataApiClient::from_connection(conn, error_kind)` — the Data API client for a
  `redshift_data_api` connection. `execute(ctx, Effect, &Statement)` submits
  (the call `submit`; a `Effect::Write` is a single attempt, never resubmitted)
  and polls (`poll`, waiting through `CallContext::wait`, 50 ms doubling to
  1 s); a run deadline or cancel ends the wait and cancels the statement on the
  service, best effort. A failed poll is retried up to 4 times (250 ms
  doubling); when polling gives up the statement is cancelled as well, and for
  an `Effect::Write` the error names the statement id and says whether it
  committed is unknown (check the destination before re-running). `result_page` (`fetch`) pages the result; `query` reads
  a small one whole. `data_api::named_parameters` turns `$n` placeholders into
  `:pN` parameters (NULL and empty strings are written into the SQL, since a
  parameter value can be neither), and `data_api::field_to_json` renders the
  typed fields through `faucet_core::fidelity` — `longValue` a number,
  `doubleValue` a float (`float4` in single precision), `NUMERIC` text an exact
  decimal string, `timestamptz` RFC 3339 with its offset, `timestamp` without
  one (microsecond precision), `blobValue` / `VARBYTE` hex base64, JSON parsed
  with exact numbers, anything else text.

## Example

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
port: 5439
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
tls_mode: verify_full
ssl_root_cert: /etc/ssl/redshift-ca-bundle.crt
```

Temporary IAM credentials for a provisioned cluster:

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
database: dev
credentials:
  type: iam
  config:
    region: us-east-1
    cluster_identifier: my-cluster
    db_user: loader
    db_groups: [loaders]
    duration_secs: 3600
    aws_credentials: { type: assume_role, config: { role_arn: arn:aws:iam::123456789012:role/faucet } }
```

The Data API against a serverless workgroup:

```yaml
database: dev
credentials:
  type: redshift_data_api
  config:
    region: us-east-1
    workgroup_name: analytics
```

License: MIT OR Apache-2.0
