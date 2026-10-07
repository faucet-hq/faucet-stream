# faucet-auth

[![Crates.io](https://img.shields.io/crates/v/faucet-auth.svg)](https://crates.io/crates/faucet-auth)
[![Docs.rs](https://docs.rs/faucet-auth/badge.svg)](https://docs.rs/faucet-auth)
[![MSRV](https://img.shields.io/crates/msrv/faucet-auth.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-auth.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared, single-flight authentication providers for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Each provider implements [`faucet_core::AuthProvider`](https://docs.rs/faucet-core/latest/faucet_core/trait.AuthProvider.html) — a live entity that owns a token cache and refresh lifecycle.

The point of this crate is **token sharing**: one provider instance, wrapped in an `Arc`, is handed to every connector that authenticates against the same identity provider. So N connectors (or N matrix rows) hitting one IdP share a **single** token with **single-flight** refresh, instead of each racing to mint or rotate its own. That is the difference between one token request per run and one per connector per refresh window — and, for rotating refresh tokens, the difference between working and invalidating each other.

## Feature highlights

- **Four provider types** — a fixed static credential, OAuth2 `client_credentials`, OAuth2 `refresh_token` with **rotation capture**, and a generic JSONPath-extracting token endpoint.
- **Single-flight refresh** — concurrent callers during a refresh await the one in-flight fetch; they don't stampede the token endpoint.
- **Force-refresh on rejection** — a connector that gets a `401` calls `invalidate(stale)` and receives a freshly-fetched token. Concurrent invalidations of the same token collapse into one refresh via compare-and-swap.
- **Refresh-token rotation capture** — `oauth2_refresh` captures a rotated `refresh_token` from each response in place, so a single active access token plus a rotating refresh token can be shared safely across many connectors.
- **Secret-safe `Debug`** — every provider's `Debug` impl renders secrets (`client_secret`, refresh token, request body, cached access token) as `***`; only non-secret identifiers stay visible.
- **Bounded fetch timeout** — providers hold a single-flight mutex across the network call, so the internal HTTP client has a 30 s request timeout: a hung IdP fails and releases the lock instead of wedging every connector that shares the provider.
- **Validated config at load time** — `expiry_ratio` is checked to be a finite number in `(0, 1]`; unknown provider `type`s and missing required fields surface as `FaucetError::Config` before any run starts.

## Installation

```bash
# As a library:
cargo add faucet-auth

# In the CLI, the shared `auth:` catalog is always available — no feature flag needed.
# Library callers who want the umbrella crate to build providers enable the `auth` feature:
cargo add faucet-stream --features auth
```

The `faucet-cli` binary always links `faucet-auth` to power the top-level `auth:` catalog, so config-driven users get every provider type out of the box.

## What it provides

| `type` (config) | Rust type | What it does |
|-----------------|-----------|--------------|
| `static` | [`StaticProvider`] | Returns a fixed, pre-minted credential forever — bearer token, custom header, or HTTP Basic. No network calls. |
| `oauth2` | [`OAuth2ClientCredentialsProvider`] | OAuth2 `client_credentials` grant. Fetches a token from the token endpoint, caches it, refreshes single-flight. |
| `oauth2_refresh` | [`OAuth2RefreshProvider`] | OAuth2 `refresh_token` grant with refresh-token **rotation capture** (a single active access token + a rotating refresh token, shared safely). |
| `token_endpoint` | [`TokenEndpointProvider`] | Fetches a token from any HTTP endpoint and extracts it from the JSON response via JSONPath. The escape hatch for non-standard token APIs. |
| `google_service_account` | `GoogleServiceAccountProvider` | Google service-account key → RS256 JWT-bearer assertion (RFC 7523) → cached bearer token, single-flight refresh. Requires the `google-sa` crate feature. |
| `oauth1` | `OAuth1Provider` | OAuth1 one-legged **request signing** (HMAC-SHA256) — signs each request's method + URL + query per RFC 5849 (no token to fetch). For token-based request-signing APIs. Requires the `oauth1` crate feature. |

`build_provider(&Value)` is the entry point: it reads a `{ type, config }` spec and returns a `SharedAuthProvider` (`Arc<dyn AuthProvider>`).

[`StaticProvider`]: https://docs.rs/faucet-auth/latest/faucet_auth/struct.StaticProvider.html
[`OAuth2ClientCredentialsProvider`]: https://docs.rs/faucet-auth/latest/faucet_auth/struct.OAuth2ClientCredentialsProvider.html
[`OAuth2RefreshProvider`]: https://docs.rs/faucet-auth/latest/faucet_auth/struct.OAuth2RefreshProvider.html
[`TokenEndpointProvider`]: https://docs.rs/faucet-auth/latest/faucet_auth/struct.TokenEndpointProvider.html

## Provider configuration reference

Every provider is built from a `{ type, config }` object — the project-wide adjacently-tagged auth shape. The fields below are the keys inside each provider's `config`.

### `flow` (composable multi-step auth)

A small declarative auth *program* (#511): an optional login / pre-flight
request chain whose responses are captured by JSONPath, credential *placement*
across header / query / cookie / body, a pluggable HMAC request *signer*, and a
dynamic per-session base-URL — on top of the single-flight machinery. It's the
single biggest source-side unlock for session-cookie / multi-step ERP APIs
(cookie sessions, XML-gateway session ids, per-session REST URLs, signed logins, …).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `steps` | array | `[]` | Login/pre-flight chain. Each: `request: { method, url, headers, query, form \| json, sign }` + `capture: { name: <source> }`. Later steps and `apply` see captured values via `${name}`. |
| `apply` | array | `[]` | Per-request placements: `{ into: header\|query\|cookie\|body, name, value: "${captured}" }`, or a signer `{ sign: { alg: hmac_sha256, key, template, encoding: hex\|base64, into: { header, format: "${sig}" } } }`. |
| `base_url_from` | string / null | `null` | Template (over captured values) yielding a per-session base-URL that overrides the connector's configured one. |
| `ttl_secs` | int / null | `null` | Re-run the login chain after this many seconds. |
| `reauth_on` | array<int> | `[]` | HTTP statuses that trigger a re-login + one retry (honored by the REST source). |

#### Signing a login step (#541)

A login/pre-flight `request` can carry its **own** `sign:` block — the same
`SignSpec` shape used in `apply`. It's computed with a fresh `${ts}`/`${nonce}`
clock per step and attached as a header, so APIs whose *login* call is itself
HMAC-signed are expressible from config. On the **first** step
nothing has been captured yet, so its `template` may reference only
`${param.*}` / `${env:*}` / `${ts}` / `${nonce}` (later steps also see values
captured by earlier steps).

```yaml
auth:
  signed_login:
    type: flow
    config:
      steps:
        - request:
            method: POST
            url: "${param.base_url}/auth/login"
            json: { clientId: "${param.client_id}" }
            sign:                                   # sign the login step itself
              alg: hmac_sha256
              key: "${param.secret_key}"
              template: "${param.client_id}:${param.client_secret}:${ts}"
              encoding: base64
              into: { header: "Authorization", format: "SS ${param.access_key}:${sig}" }
          capture: { session: "$.session.token" }
      apply:
        - { sign: { alg: hmac_sha256, key: "${param.secret_key}", template: "${param.client_id}:${param.client_secret}:${ts}", encoding: base64, into: { header: Authorization, format: "SS ${param.access_key}:${sig}" } } }
        - { into: header, name: "Session", value: "${session}" }
```

#### Capture sources (#542)

By default a `capture` entry is a JSONPath **string** into the JSON response
body (`{ session: "$.session.token" }` — unchanged, back-compatible). It can
also be a `{ from, … }` struct to capture from other parts of the response:

| `from` | Selector | Captures |
|--------|----------|----------|
| `json` *(default)* | `path: "$.jsonpath"` | JSONPath into the JSON body (same as the bare-string form). |
| `xml` | `path: "a.b.c"` | Dot-path into an XML body, by element local name (namespace prefixes are ignored). |
| `header` | `name: "Location"` | A response header value (case-insensitive). |
| `set_cookie` | `name: "ASP.NET_SessionId"` | A specific `Set-Cookie` value, selected by cookie name. |

```yaml
steps:
  - request: { method: POST, url: "${param.base_url}/entity/auth/login",
               json: { name: "${param.user}", password: "${secret:pw}" } }
    capture:
      session_cookie: { from: set_cookie, name: "ASP.NET_SessionId" }   # Set-Cookie + 204 empty body
      sess_id:        { from: xml, path: "response.operation.result.data.api.sessionid" }  # XML-gateway session id
      loc:            { from: header, name: "Location" }                # any response header
apply:
  - { into: cookie, name: "ASP.NET_SessionId", value: "${session_cookie}" }
```

> A shared **cookie jar** (auto-forwarding login cookies to data requests
> without an explicit capture) is future work; the `from: set_cookie` →
> `into: cookie` path covers the same case today.

```yaml
auth:
  login_chain:
    type: flow
    config:
      steps:
        - request: { method: POST, url: "https://auth/oauth/token",
                     form: { grant_type: refresh_token, refresh_token: "${env:RT}" } }
          capture: { access_token: "$.access_token" }
        - request: { method: GET, url: "https://login/rest-services/login",
                     query: { access_token: "${access_token}" } }
          capture: { bh_rest_token: "$.BhRestToken", base_url: "$.restUrl" }
      apply:
        - { into: query, name: BhRestToken, value: "${bh_rest_token}" }
      base_url_from: "${base_url}"
      reauth_on: [401]
```

Header placements also apply to `xml` / `graphql` sources (via `credential()`);
query / cookie / body placement and dynamic base-URL are consumed by the `rest`
source (which reads the richer `request_auth`).

### `static`

A fixed credential. Exactly one of the three shapes below must be present.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token` | string | — | Bearer token → `Authorization: Bearer <token>`. |
| `header` + `value` | string + string | — | A custom header credential (e.g. `X-Api-Key`). Both keys required together. |
| `username` + `password` | string + string | — | HTTP Basic credentials. Both keys required together. |

### `oauth2` (client-credentials)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token_url` | string | — *(required)* | OAuth2 token endpoint. |
| `client_id` | string | — *(required)* | OAuth2 client ID. |
| `client_secret` | string | — *(required)* | OAuth2 client secret. |
| `scopes` | array of string | `[]` | Scopes, sent space-joined as the `scope` form field. |
| `expiry_ratio` | number | `0.9` | Refresh after `expires_in × expiry_ratio` seconds. Must be a finite number in `(0, 1]`. |

### `oauth2_refresh`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token_url` | string | — *(required)* | OAuth2 token endpoint. |
| `client_id` | string | — *(required)* | OAuth2 client ID. |
| `client_secret` | string | — *(required)* | OAuth2 client secret. |
| `refresh_token` | string | — *(required)* | Initial (seed) refresh token. A rotated token in the response is captured in place for the next refresh. |
| `scope` | string | *(none)* | Optional `scope` sent on the refresh grant. Some IdPs require it on refresh (e.g. `https://api.example.com/.default offline_access`). Omitted entirely when unset, so the refresh inherits the original grant's scope (RFC 6749 §6). |
| `expiry_ratio` | number | `0.9` | Refresh after `expires_in × expiry_ratio` seconds. Must be a finite number in `(0, 1]`. |
| `persist` | object | *(none)* | Durably persist the rotated `refresh_token` so a **later** run authenticates after the token rotates (some providers rotate on every refresh; without this the second scheduled run 401s). `persist.path` is a file-backed state-store directory; the rotated token is written after each refresh and re-read on startup in preference to the config seed. The auto-derived storage key covers `token_url`, `client_id` and the seed `refresh_token`, so two grants of one app (one app connected to two accounts) never share a slot, and every stored token records the seed it came from — an entry recorded for a different seed is ignored in favour of the configured one. Token files are written owner-only (`0600`), and a directory faucet creates is owner-only (`0700`). A failed write of the rotated token fails the refresh — the old token is already revoked, so carrying on would leave the next run without a usable one. Optional `persist.key` sets the key explicitly. Upgrading from a release that keyed by `token_url` + `client_id` only: the old entry is not adopted automatically (it could hold another grant's token); a warning names the old key — set `persist.key` to it if this provider is the only grant for its app. |

`persist` example:

```yaml
auth:
  graph:
    type: oauth2_refresh
    config:
      token_url: "https://login.microsoftonline.com/${param.tenant}/oauth2/v2.0/token"
      client_id: "${secret:idp_client_id}"
      client_secret: "${secret:idp_client_secret}"
      refresh_token: "${secret:idp_seed_refresh_token}"   # seed; used only until the first rotation
      scope: "https://api.example.com/.default offline_access"  # this IdP requires scope on refresh
      persist:
        path: "./state/auth"        # rotated refresh_token survives across runs
```

### `token_endpoint`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — *(required)* | HTTP endpoint to fetch the token from. |
| `method` | string | `POST` | HTTP method. |
| `encoding` | string | `json` | Request-body encoding: `json` (default) or `form` (`application/x-www-form-urlencoded`, required by OAuth token endpoints that expect a `resource=` param). `form` requires a flat object `body` of string/number/boolean values. |
| `body` | object | *(none)* | Request body (e.g. carrying `client_id` / `client_secret` / `resource`). Omit for a GET. |
| `token_path` | string | — *(required)* | JSONPath selecting the token from the response (e.g. `$.auth.access_token`). String and numeric matches are accepted. |
| `expiry_path` | string | *(none)* | JSONPath selecting `expires_in` (seconds) from the response. When absent, the token is cached without an expiry. |
| `expiry_ratio` | number | `0.9` | Refresh after `expires_in × expiry_ratio` seconds. Must be a finite number in `(0, 1]`. |
| `apply_as` | object | *(Bearer)* | Where the fetched token is placed on each request. Default is `Authorization: Bearer <token>`. Set `apply_as: { header, template }` to place it in an arbitrary header (e.g. a session cookie): `template` is the header value with `{token}` substituted (defaults to the bare token). |

`apply_as` example — a session cookie:

```yaml
auth:
  session:
    type: token_endpoint
    config:
      url: "https://host:50000/api/v1/Login"
      body: { CompanyDB: "${param.company_db}", UserName: "${param.user}", Password: "${secret:session_pw}" }
      token_path: "$.SessionId"
      apply_as:
        header: "Cookie"
        template: "SESSION={token}; CompanyDB=${param.company_db}"
```

### `google_service_account` (RFC 7523 JWT-bearer)

Authenticates as a Google service account — the machine credential for the
`googleapis.com` family (analytics, advertising, Sheets,
Drive, …) when no human-held refresh token is available. Each refresh signs an
RS256 assertion (`kid` = the key's `private_key_id`; claims `iss`, `scope`,
`aud` = token URI, `iat` back-dated 30 s for clock skew, `exp` = `iat` + 1 h,
and `sub` when set) and exchanges it with
`grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`. The access token is
cached and refreshed single-flight. Requires the `google-sa` feature (on in the
CLI default build).

| Field | Required | Description |
|-------|----------|-------------|
| `key_json` | one of | The key file's contents, as a string (e.g. `${secret:GOOGLE_SA_KEY}`) or an inline object. |
| `key_file` | one of | Path to the key file. Set exactly one of `key_json` / `key_file`. |
| `scopes` | yes | OAuth scopes, e.g. `["https://www.googleapis.com/auth/analytics.readonly"]`. |
| `subject` | no | User to impersonate (domain-wide delegation). |
| `token_uri` | no | Overrides the key's `token_uri` (default `https://oauth2.googleapis.com/token`). |
| `expiry_ratio` | no | Fraction of `expires_in` before a proactive refresh (default `0.9`). |

```yaml
auth:
  google:
    type: google_service_account
    config:
      key_json: "${secret:GOOGLE_SA_KEY}"
      scopes: ["https://www.googleapis.com/auth/analytics.readonly"]
      subject: reports@example.com   # optional
```

A malformed key (not a service-account JSON, or an unparseable PEM) fails at
config load. A rejected grant surfaces as `Auth: google_service_account token
request failed (HTTP 400): {"error":"invalid_grant",…}` naming the service
account, subject and scopes — which `faucet serve` tenants treat as a revoked
connection. The private key never appears in `Debug` output or errors, and the
CLI registers it for log redaction even when written inline.

This crate ships the Google preset only; a generic `jwt_bearer` provider for
other IdPs is not exposed.

### `oauth1` (HMAC-SHA256 request signing)

Unlike the token providers, OAuth1 has **no token to fetch** — it signs every
request individually (the signature covers the HTTP method, URL, and query
parameters). Requires the `oauth1` crate feature (`cargo add faucet-auth
--features oauth1`, or `cargo install faucet-cli --features oauth1`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `consumer_key` | string | — *(required)* | OAuth1 consumer (client) key. |
| `consumer_secret` | string | — *(required)* | OAuth1 consumer secret. |
| `token` | string | — *(required)* | OAuth1 access token. |
| `token_secret` | string | — *(required)* | OAuth1 access-token secret. |
| `realm` | string | *(none)* | Optional `realm` (e.g. the account id). |
| `signature_method` | string | `HMAC-SHA256` | Only `HMAC-SHA256` is supported. |

```yaml
auth:
  signed:
    type: oauth1
    config:
      consumer_key: "${secret:ns_consumer_key}"
      consumer_secret: "${secret:ns_consumer_secret}"
      token: "${secret:ns_token}"
      token_secret: "${secret:ns_token_secret}"
      realm: "${param.account}"   # account id
```

## CLI usage — the top-level `auth:` catalog

Define a provider **once** in the top-level `auth:` catalog, then reference it from any connector via `auth: { ref: <name> }`. The CLI builds each named provider a single time and injects the same `Arc` into every connector that references it, so all the rows that share a `ref` share one token.

```yaml
# faucet run pipeline.yaml
version: 1
auth:
  sf:
    type: oauth2_refresh
    config:
      token_url: https://<acct>.snowflakecomputing.com/oauth/token-request
      client_id: ${secret:SF_CLIENT_ID}
      client_secret: ${secret:SF_CLIENT_SECRET}
      refresh_token: ${secret:SF_REFRESH_TOKEN}
pipeline:
  sources:
    sf_table:
      type: snowflake
      config:
        account: ${vars.account}
        auth: { ref: sf }      # every row using this template shares ONE token
  sink:
    type: file
    config:
      path: ./out.jsonl
```

Eight connectors consume shared providers via `auth: { ref }`: `rest`, `graphql`, `xml`, `grpc`, `websocket`, `sink-http`, `elasticsearch`, and `snowflake`. (Kafka / BigQuery / GCS keep the same `{ type, config }` wire shape but are not bearer/token-based, so they don't take `auth: { ref }`.)

Provider configs can hold `${env:…}` / `${file:…}` / `${secret:…}` / `${vault:…}` / `${aws-sm:…}` indirection — the secrets pass walks the `auth:` catalog, so a single shared provider can hold a secret-manager reference and be reused across every row.

### Inline auth vs shared provider

A connector's `auth` field accepts **either** an inline `{ type, config }` block **or** a `{ ref: <name> }` pointer. Use a `ref` (and the `auth:` catalog) whenever more than one connector / matrix row authenticates against the same IdP — that's when token sharing and single-flight refresh actually pay off. A one-off connector can keep its auth inline.

## Library usage

Build a provider, wrap it in an `Arc`, and clone it into every source/sink that should share the token via `with_auth_provider`:

```rust
use std::sync::Arc;
use faucet_auth::{build_provider, OAuth2RefreshProvider};
use faucet_core::SharedAuthProvider;
use serde_json::json;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// Build directly from a typed config object…
let provider: SharedAuthProvider = Arc::new(OAuth2RefreshProvider::from_config(&json!({
    "token_url": "https://idp.example/token",
    "client_id": "my-client",
    "client_secret": "s3cr3t",
    "refresh_token": "initial-rt",
}))?);

// …or from a `{ type, config }` spec, the same shape the CLI catalog uses:
let provider2: SharedAuthProvider = build_provider(&json!({
    "type": "oauth2",
    "config": {
        "token_url": "https://idp.example/token",
        "client_id": "my-client",
        "client_secret": "s3cr3t",
        "scopes": ["read"]
    }
}))?;
# let _ = (provider2,);

// Clone the Arc into every connector that should share the one token:
// let source_a = RestStreamSource::new(cfg_a)?.with_auth_provider(provider.clone());
// let source_b = RestStreamSource::new(cfg_b)?.with_auth_provider(provider.clone());
# let _ = provider;
# Ok(())
# }
```

Asking the provider for a credential is then a one-liner; the cache, refresh, and single-flight coordination are internal:

```rust,no_run
use faucet_core::{AuthProvider, Credential};
# async fn use_provider(provider: &dyn AuthProvider) -> Result<(), Box<dyn std::error::Error>> {
let cred: Credential = provider.credential().await?;     // cached or freshly refreshed
// On a 401, force a single-flight refresh of exactly this stale token:
let fresh = provider.invalidate(&cred).await?;
# let _ = fresh;
# Ok(())
# }
```

## How single-flight refresh works

Each fetching provider (`oauth2`, `oauth2_refresh`, `token_endpoint`) holds a `Mutex`-guarded token cache and performs the token-endpoint call **with the lock held**:

1. **Cache hit** — `credential()` returns the cached token until `expires_in × expiry_ratio` has elapsed.
2. **Cache miss / expiry** — the first caller takes the lock and fetches; concurrent callers block on the same lock and, when it's released, observe the freshly-cached token. Result: **one** network fetch serves an arbitrary number of concurrent callers.
3. **Force-refresh on `401`** — a connector whose request was rejected calls `invalidate(stale)`. This is a compare-and-swap: it refetches **only** if the cache still holds the `stale` token. If a concurrent caller already refreshed (the cache now holds a *different* token), the stale caller gets that new token back without a second fetch.
4. **Rotation capture** (`oauth2_refresh` only) — each refresh response may carry a rotated `refresh_token`; the provider stores it in place, so the next refresh uses the latest rotated token. This is exactly the case that breaks when each connector keeps its own copy of a rotating refresh token.

The internal HTTP client has a bounded 30 s request timeout so a hung or unreachable IdP fails the fetch and releases the single-flight lock, rather than wedging every connector that shares the provider.

`expiry_ratio` (default `0.9`) must be a finite number in `(0, 1]`, validated at construction. A value `≤ 0` (or `NaN`) would expire every token immediately, defeating the cache and single-flight refresh; a value `> 1` would treat the token as valid past its real expiry, causing `401`s mid-use. Both are rejected at config-load time.

## Feature flags

| Feature | Enables |
|---------|---------|
| `google-sa` | The `google_service_account` provider (pulls `jsonwebtoken`). On in the `faucet-cli` default build; `faucet-stream` forwards it as `google-sa`. |
| `oauth1` | The `oauth1` request-signing provider. |

It is pulled in by:

- the **`faucet-cli`** binary (always — for the top-level `auth:` catalog);
- the **`faucet-stream`** umbrella crate's `auth` feature, for library callers who want `build_provider` available alongside the connectors.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: auth provider: unknown type ...` | The `type` isn't one of `static` / `oauth2` / `oauth2_refresh` / `token_endpoint`. Check the spelling. |
| `Config: ... missing 'type'` | The provider spec has no `type` key. Each `auth:` catalog entry needs `{ type, config }`. |
| `Config: oauth2 auth provider: missing 'client_id'` (or `token_url` / `client_secret` / `refresh_token`) | A required OAuth2 field is absent. All of `token_url`, `client_id`, `client_secret` are required; `oauth2_refresh` also requires `refresh_token`. |
| `Config: static auth provider: config must contain ...` | The `static` config didn't match `token`, `header`+`value`, or `username`+`password`. Provide exactly one of those shapes. |
| `Config: ... 'expiry_ratio' must be a finite number in (0, 1]` | `expiry_ratio` is out of range or non-numeric. Use a value like `0.9`; remove the field to take the default. |
| `Auth: OAuth2 token request failed (HTTP 401): ...` | The IdP rejected the client credentials / refresh token. Verify `client_id` / `client_secret`, and that the `refresh_token` hasn't already been rotated/revoked by a stale copy elsewhere — share **one** `oauth2_refresh` provider via `auth: { ref }`. |
| `Auth: token endpoint request failed (HTTP ...)` | The `token_endpoint` `url` returned a non-2xx. Check the URL, `method`, and `body`. |
| `Auth: token_path '...' did not match a string value` | The `token_path` JSONPath didn't select a string/number in the response. Inspect the real response body and fix the path (e.g. `$.auth.access_token`). |
| Token endpoint hangs the whole pipeline | A single-flight fetch is blocked. Providers cap each fetch at 30 s, after which the lock is released and the call fails with `FaucetError`; check IdP reachability and the `token_url`. |
| Every request re-fetches a token | An `expiry_ratio` near 0 (or a missing `expiry_path` on `token_endpoint` combined with no caching expectation) shortens the cache window. Set a sensible `expiry_ratio` and an `expiry_path` that selects the response's TTL. |

## See also

- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — the full `{ type, config }` vs `{ ref }` model, per-connector auth methods, and the shared `auth:` catalog.
- [Secrets cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html) — feeding `${secret:…}` / `${vault:…}` references into provider configs.
- [`faucet-core`](https://crates.io/crates/faucet-core) — defines the [`AuthProvider`](https://docs.rs/faucet-core/latest/faucet_core/trait.AuthProvider.html) trait, [`Credential`](https://docs.rs/faucet-core/latest/faucet_core/enum.Credential.html) enum, and `SharedAuthProvider` / `AuthSpec` types this crate implements against.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
