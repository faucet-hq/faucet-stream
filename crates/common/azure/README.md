# faucet-common-azure

Shared Azure Blob Storage / ADLS Gen2 credential and client types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Both `faucet-source-azure-blob` and `faucet-sink-azure-blob` depend on this
crate for a single, consistent way to authenticate against Azure storage and
build an [`object_store`](https://crates.io/crates/object_store)-backed client.
ADLS Gen2 and classic Blob share the same `MicrosoftAzureBuilder`, so one code
path serves both.

## Credentials

`AzureCredentials` serializes as `{ type, config }` (the project-wide auth wire
shape). Variants:

| `type` | Fields | Notes |
|---|---|---|
| `account_key` | `account_key` | Shared storage-account key |
| `sas_token` | `sas_token` | Shared-access-signature token |
| `connection_string` | `connection_string` | Full storage connection string (`EndpointSuffix`, `DefaultEndpointsProtocol` and `UseDevelopmentStorage=true` are honoured) |
| `managed_identity` | `client_id` (optional) | IMDS; omit `client_id` for the system-assigned identity |
| `service_principal` | `client_id`, `client_secret`, `tenant_id` | Azure AD client credentials |
| `default` | — | Default credential chain (env / workload identity / managed identity / Azure CLI). **Default.** |

## Connection

`AzureConnection` carries `container` (required), `account`, `auth`, `endpoint`,
`allow_http`, and `use_emulator`. `build_store(&AzureConnection)` returns an
`Arc<dyn ObjectStore>` ready for reads and writes. With `auth: default` the
builder starts from `MicrosoftAzureBuilder::from_env()`, so the environment's
credential chain applies. With any explicit `auth` only the non-credential
`AZURE_*` settings (account, endpoint, emulator and HTTP-client options) are
taken from the environment, so a key, secret or workload identity there can
never replace the configured credential.

```rust
use faucet_common_azure::{AzureConnection, AzureCredentials, build_store};

let conn = AzureConnection::new("my-container")
    .account("mystorageacct")
    .auth(AzureCredentials::AccountKey { account_key: "…".into() });
let store = build_store(&conn)?;
# Ok::<(), faucet_core::FaucetError>(())
```

## License

MIT
