# faucet-common-sftp

Shared SFTP connection configuration and connect helper for the
[`faucet-source-sftp`](https://crates.io/crates/faucet-source-sftp) and
[`faucet-sink-sftp`](https://crates.io/crates/faucet-sink-sftp) connectors in
the [faucet-stream](https://crates.io/crates/faucet-stream) ecosystem.

This crate is a small building block, not a connector itself. It provides:

- `SftpConnectionConfig` — host, port (default `22`), username, auth, host-key
  policy. Its fields are `#[serde(flatten)]`ed into the source/sink configs, so
  end users see one flat config block.
- `SftpAuth` — `password` or `private_key` (with optional passphrase),
  serialized with the faucet `{ type, config }` shape. Its `Debug` impl never
  prints the password or passphrase.
- `HostKeyPolicy` — how the server host key is verified (see below).
- `async fn connect(&SftpConnectionConfig) -> Result<SftpSession, FaucetError>`
  — opens the SSH transport, authenticates, verifies the host key, and opens
  the `sftp` subsystem.

## Storage backend for the file writer

`SftpBackend` is the `StorageBackend` the shared file writer in
[`faucet-common-file`](https://crates.io/crates/faucet-common-file) publishes
files through; the [`faucet-sink-sftp`](https://crates.io/crates/faucet-sink-sftp)
sink is a `FileSink<SftpBackend>` (RFC 0013, #886). Build it with
`SftpBackend::builder(connection).concurrency(n).scratch_dir(dir).build(base, &template)`;
it opens no connection until the first operation.

- Each file is built in a local scratch file, uploaded to a hidden temporary
  name beside its destination and renamed into place (`posix-rename` when the
  server has it, else remove-then-rename), so a reader never sees a partial
  file. Missing directories are created.
- Only the server's typed "no such file" status means missing; every other
  refused stat or remove is an error naming the operation and the path.
- Every operation runs through the run's call context and reports to the
  run's round-trip recorder; the preflight probe opens the SSH session (host,
  host key, credentials) and writes nothing.

## Host-key verification

Man-in-the-middle protection is on by default. The policy is selected via the
`known_hosts` block:

| `mode`        | Behaviour |
|---------------|-----------|
| `accept_new`  | **Default.** Trust-on-first-use: record an unknown host key in `~/.ssh/known_hosts`, reject a key that has *changed*. |
| `strict`      | Reject any key not already present in `known_hosts` (optionally at a custom `known_hosts_path`). |

Both policies honour wildcard host patterns (`*.example`, `!skip.example`) and refuse a key marked `@revoked`. A host trusted only through `@cert-authority` is refused: host certificates are not supported, so list the host key itself.

The TCP connect, SSH handshake, authentication and SFTP subsystem start are bounded by `connect_timeout_secs` (default 30), and a quiet connection sends a keepalive every `keepalive_interval_secs` (default 15, `0` = off), dropping it after three go unanswered.
| `insecure`    | Disable verification entirely. **Vulnerable to MITM** — use only on trusted networks / test servers. |

## Auth config shapes

```yaml
# Password
type: password
config:
  password: ${env:SFTP_PASSWORD}

# Private key
type: private_key
config:
  path: /home/me/.ssh/id_ed25519
  passphrase: ${env:SFTP_KEY_PASSPHRASE}   # optional
```

Licensed under MIT OR Apache-2.0.
