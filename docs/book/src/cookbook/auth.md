# Authentication

Every connector's `auth:` block uses one consistent shape — a `type:`
discriminator plus a nested `config:` map:

```yaml
auth:
  type: <method>
  config:
    <method-specific fields>
```

Always pull secrets from the environment with `${env:VAR}` (or `${file:PATH}` /
`${secret:VAR}`) rather than hard-coding them.

## API key / header

```yaml
auth:
  type: api_key
  config:
    header: Authorization
    value: "Bearer ${env:API_TOKEN}"
```

## Bearer token

```yaml
auth:
  type: bearer
  config:
    token: ${env:API_TOKEN}
```

## Basic auth

```yaml
auth:
  type: basic
  config:
    username: ${env:API_USER}
    password: ${env:API_PASS}
```

## OAuth2 client credentials

The source fetches and refreshes the token automatically (before expiry):

```yaml
auth:
  type: oauth2
  config:
    token_url: https://auth.example.com/oauth/token
    client_id: ${env:CLIENT_ID}
    client_secret: ${env:CLIENT_SECRET}
    scopes: ["read:events"]
```

## Custom token endpoint

For non-standard token endpoints, `token_endpoint` lets you describe the request
and point at the access-token and expiry fields in the response. See
`faucet schema source rest` for the full field list.

Two knobs cover the less-standard flows:

- **`encoding: form`** sends the token request as
  `application/x-www-form-urlencoded` (OAuth endpoints that expect a `resource=`
  param need this; the default is `json`).
- **`apply_as: { header, template }`** puts the fetched token in an arbitrary
  header instead of `Authorization: Bearer` — e.g. a session cookie. `template`
  is the header value with `{token}` substituted.

```yaml
auth:
  session:                      # SessionId carried as a cookie
    type: token_endpoint
    config:
      url: "https://host:50000/api/v1/Login"
      body: { CompanyDB: "${param.company_db}", UserName: "${param.user}", Password: "${secret:session_pw}" }
      token_path: "$.SessionId"
      apply_as: { header: "Cookie", template: "SESSION={token}; CompanyDB=${param.company_db}" }
```

## Persisting a rotating refresh token

Some providers **rotate the `refresh_token` on every
refresh** — the old one is invalidated. In-memory rotation works for a single
run, but the *next* scheduled run would present the now-stale seed and get a 401.
Set `persist.path` on an `oauth2_refresh` provider to durably store the rotated
token (a file-backed state store) so later runs pick up where the last one left
off. Token files are owner-only (`0600`, in a `0700` directory when faucet
creates it), and a failed write fails the refresh rather than leaving the next
run with a revoked token. The stored token is re-read before every refresh and
written back with a compare-and-set, so overlapping runs that share the grant
never refresh with a token another run already rotated away; a grant rejected
with `invalid_grant` is retried once when the store holds a newer token:

```yaml
auth:
  idp:
    type: oauth2_refresh
    config:
      token_url: "https://login.example.com/${param.tenant}/oauth2/v2.0/token"
      client_id: "${secret:idp_client_id}"
      client_secret: "${secret:idp_client_secret}"
      refresh_token: "${secret:idp_seed_refresh_token}"   # seed; used only until the first rotation
      persist:
        path: "./state/auth"      # rotated refresh_token survives across runs
```

## Mutual TLS (client certificates)

Some enterprise/gov APIs (payroll, banking) require the client to present a certificate
(**mutual TLS**). The `rest`, `xml`, and `graphql` sources accept a `tls:` block
that attaches a client identity to **every** request — data requests *and* any
inline token-endpoint request (they share one HTTP client). Build the CLI with
the `mtls` feature (`cargo install faucet-cli --features mtls`); without it a
`tls:` block is a load-time error rather than being silently ignored.

```yaml
source:
  type: rest
  config:
    base_url: https://api.eu.example.com
    tls:
      client_cert: ${file:./client-cert.pem}   # PEM cert chain (inline / ${file:} / ${secret:})
      client_key:  ${file:./client-key.pem}    # PEM PKCS#8 private key
      # min_version: "1.2"                   # optional: "1.2" | "1.3"
```

Or point at a PKCS#12 (`.p12`/`.pfx`) bundle instead of the PEM pair:

```yaml
    tls:
      client_identity_pkcs12: ./client-identity.p12
      pkcs12_password: ${env:CLIENT_P12_PASSWORD}
```

Supply **either** the PEM pair **or** the PKCS#12 file, not both. Key material is
never written to logs or error messages.

> **Shared providers:** mTLS lives on the *source's* client, so it covers inline
> auth (the token request goes through the same client). A token minted by a
> shared `auth: { ref }` provider uses that provider's own client and does not
> present the source's certificate — use inline auth for mTLS endpoints.

## Google service accounts (JWT-bearer)

Google APIs (Sheets, Drive, BigQuery, …) accept a
**service-account key** instead of a user's refresh token — the right credential
for `faucet schedule`, `faucet serve` and tenants, where no human is around to
re-consent. Declare it once in the top-level `auth:` catalog and reference it:

```yaml
auth:
  google:
    type: google_service_account
    config:
      key_json: "${secret:GOOGLE_SA_KEY}"        # or key_file: /etc/faucet/sa.json
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
      subject: reports@example.com               # optional: domain-wide delegation
      # token_uri: defaults to the key's own token_uri

pipeline:
  source:
    type: rest
    config:
      base_url: https://sheets.googleapis.com
      path: /v4/spreadsheets/SHEET_ID/values/Sheet1
      method: GET
      auth: { ref: google }
      # …
```

Each refresh signs an RS256 assertion with the key (RFC 7523) and exchanges it
at Google's token endpoint; the access token is cached and shared single-flight
by every row that references `google`. A malformed key fails at load time, and
an `invalid_grant` (disabled key, delegation not granted) names the service
account, subject and scopes. The private key is redacted from logs even when
written inline, but prefer a secrets-manager reference. The provider ships in
the default CLI build (`google-sa` feature).

## OAuth1 request signing (HMAC-SHA256)

Some APIs (token-based auth schemes) authenticate by **signing each
request** rather than issuing a bearer token. The `oauth1` provider signs every
request's method + URL + query per RFC 5849. It's a catalog provider used via
`auth: { ref }`, and requires the `oauth1` build feature
(`cargo install faucet-cli --features oauth1`):

```yaml
auth:
  signed:
    type: oauth1
    config:
      consumer_key: ${secret:OAUTH1_CONSUMER_KEY}
      consumer_secret: ${secret:OAUTH1_CONSUMER_SECRET}
      token: ${secret:OAUTH1_TOKEN}
      token_secret: ${secret:OAUTH1_TOKEN_SECRET}
      realm: ${param.account}      # account id
pipeline:
  source:
    type: rest
    config:
      base_url: https://${param.account_lower}.api.example.com
      auth: { ref: signed }
```

## Composable multi-step flows (`type: flow`)

Some APIs make auth a small *program*: log in, capture a session token from the
response, place it in a query param (not a header), and follow a base-URL the
server hands back. The `flow` provider (a catalog provider, always available)
runs a login/pre-flight chain, captures values by JSONPath, applies credential
*placements* (header / query / cookie / body), can HMAC-sign each request, and
overrides the base-URL per session:

```yaml
auth:
  login_chain:
    type: flow
    config:
      steps:
        - request: { method: POST, url: https://auth.example.com/oauth/token,
                     form: { grant_type: refresh_token, refresh_token: ${secret:LOGIN_RT} } }
          capture: { access_token: "$.access_token" }
        - request: { method: GET, url: https://login.example.com/rest-services/login,
                     query: { access_token: "${access_token}" } }
          capture: { session_token: "$.sessionToken", base_url: "$.restUrl" }
      apply:
        - { into: query, name: sessionToken, value: "${session_token}" }
      base_url_from: "${base_url}"   # follow the server-supplied REST host
      reauth_on: [401]               # re-login + retry once when the session expires
pipeline:
  source:
    type: rest
    config:
      base_url: https://placeholder.example.com   # overridden by base_url_from
      path: /entity/Candidate
      auth: { ref: login_chain }
      records_path: "$.data[*]"
```

`${captured}` values from earlier steps are substituted into later steps and into
`apply`; a signer entry is `{ sign: { alg: hmac_sha256, key, template, encoding:
hex|base64, into: { header, format: "${sig}" } } }` (with `${ts}` / `${nonce}`
available in the template). The `rest` and `xml` sources honor header / query /
cookie placement and the dynamic base-URL.

**Captured value into a raw body (`${name}`, #567).** A captured value can also
be substituted directly into a **raw request body** (or a config header/URL) via
`${<capture_name>}` — what an XML/SOAP gateway needs, since no placement can reach
inside a raw body. The `xml` source captures a `sessionid` at login and every data
request carries it:

```yaml
auth:
  xml_gateway:
    type: flow
    config:
      steps:
        - request: { url: "https://xml-gateway.example.com/xml/gateway", method: POST, body: "<request>…getAPISession…</request>" }
          capture: { session_id: { from: xml, path: "operation.result.data.api.sessionid" } }
pipeline:
  source:
    type: xml
    config:
      method: POST
      body: "<request><control>…</control><operation><authentication><sessionid>${session_id}</sessionid></authentication>…</operation></request>"
      auth: { ref: xml_gateway }
```

The capture names declared by a `flow` provider are valid `${name}` tokens
anywhere in a source config. See the [`rest_flow_auth.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_flow_auth.yaml) example.

## Shared auth providers (`auth: { ref }`)

When several connectors authenticate against the **same** system — e.g. four
matrix rows reading four endpoints of one API, or four Snowflake tables — define
the credential **once** in the top-level `auth:` catalog and reference it with
`auth: { ref: <name> }`. faucet builds a single provider and shares it across
every row, so there is **one** token fetch and **one** refresh cycle
(single-flight) instead of each row racing to refresh a single-active / rotating
token:

```yaml
auth:
  api:
    type: oauth2_refresh        # rotating refresh token captured centrally
    config:
      token_url: ${env:API_TOKEN_URL}
      client_id: ${secret:API_CLIENT_ID}
      client_secret: ${secret:API_CLIENT_SECRET}
      refresh_token: ${secret:API_REFRESH_TOKEN}
pipeline:
  sources:
    ep:
      type: rest
      config:
        base_url: ${env:API_BASE_URL}
        auth: { ref: api }      # every row sharing this template shares ONE token
  sink: { type: stdout, config: {} }
matrix:
  - { id: customers, source: { ref: ep, config: { path: /customers } } }
  - { id: orders,    source: { ref: ep, config: { path: /orders } } }
```

Provider `type:` values (catalog only): `static`, `oauth2` (client-credentials),
`oauth2_refresh` (with rotation), `token_endpoint`. A connector's `auth:` is
**either** an inline definition **or** a `{ ref }` — never both. See
`cli/examples/shared_auth_rest.yaml` for a full four-row example.

Shared providers are supported by the bearer/header-based connectors (rest,
graphql, xml, grpc, websocket, http sink, elasticsearch, snowflake-OAuth).

When the server rejects a shared provider's credential — a `401`, or a status a
`flow` provider lists in `reauth_on` — the connector tells the provider the token
is stale and retries the request once with a fresh one (a WebSocket reconnects,
gRPC retries on `UNAUTHENTICATED`). The provider refreshes once even when many
rows hit the same rejection, so a token the server expired or revoked before its
client-side expiry recovers on the next request instead of failing every run
until a restart.

**Library use:** build one `faucet_auth` provider, wrap it in an `Arc`, and pass
it to each source/sink with `.with_auth_provider(provider.clone())`.

## Connector-specific inline auth

Each connector also has its own inline auth methods, all under the `auth:` key
and all in `{ type, config }` form:

- **BigQuery** — `service_account_key_path`, `service_account_key`
  (inline JSON), or `application_default`.
- **Snowflake** — `key_pair` (JWT minted locally; `config: { user, private_key_pem }`,
  both required — `private_key_pem` is the PEM text, typically `${file:./key.pem}`)
  or `oauth` (`config: { token }`).
- **Kafka** — `sasl_plain` / `sasl_scram` / `ssl` / `sasl_ssl`.
- **Elasticsearch** — `basic`, `api_key`, `bearer`, or `none`.
- **GCS** — `service_account_json_file`, `service_account_json_inline`,
  `application_default`, or `anonymous`.

Inspect any connector's auth shape with `faucet schema source <name>` /
`faucet schema sink <name>`.

## Secret interpolation

`${env:VAR}` and `${file:PATH}` are resolved at config-load time, so secrets
never need to appear in the file. A sibling `.env` is loaded automatically (use
`--no-env-file` to disable, or `--env-file PATH` to point elsewhere).
