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

## Storage backend for the file writer

`AzureBackend` is the `StorageBackend` the shared file writer in
[`faucet-common-file`](https://crates.io/crates/faucet-common-file) publishes
blobs through; the [`faucet-sink-azure-blob`](https://crates.io/crates/faucet-sink-azure-blob)
sink is a `FileSink<AzureBackend>` (RFC 0013, #886). Build it with
`AzureBackend::builder_for(build_azure(&conn)?, container).concurrency(n).scratch_dir(dir).build(base, &template)`.

- Listing (overwrite commits, rollover cleanup) sends the key prefix to the
  server and fetches page by page, one `list` round trip per page. A backend
  built with `builder(store, …)` over a plain `ObjectStore` walks the
  prefix's directory and filters it instead; `paginated_list(store)` opts it in.

- A blob up to 8 MiB (`PART_BYTES`) is one `Put Blob`; a larger one is
  uploaded as blocks and committed with one `Put Block List`, which is what
  makes it visible. JSON Lines and raw text go up block by block while they
  are written.
- A move into place (`write_mode: overwrite`) is a server-side copy and a
  delete.
- Every request runs through the run's call context and reports to the run's
  round-trip recorder; the preflight probe lists the container's first blob.

## License

MIT
