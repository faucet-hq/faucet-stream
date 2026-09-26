# faucet-common-dynamodb

Shared types for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
Amazon DynamoDB connectors — [`faucet-source-dynamodb`](https://crates.io/crates/faucet-source-dynamodb)
and [`faucet-sink-dynamodb`](https://crates.io/crates/faucet-sink-dynamodb).
Both re-export what users need, so you normally depend on the source or sink
crate rather than this one.

## Contents

- **`DynamoDbCredentials`** — the AWS auth enum, in faucet's `{ type, config }`
  wire shape (identical to the Kinesis and SQS connectors):

  | `type` | Fields | Meaning |
  |--------|--------|---------|
  | `default` | — | AWS SDK default provider chain, with automatic refresh |
  | `profile` | `name` | A named profile from the shared AWS config files |
  | `access_key` | `access_key_id`, `secret_access_key`, `session_token?` | Static keys — prefer `${env:…}` / secrets-manager interpolation |
  | `assume_role` | `role_arn`, `session_name?`, `external_id?` | STS AssumeRole on top of the default chain |
  | `web_identity` | — | Web-identity federation (EKS IRSA) |

- **`build_client` / `build_streams_client`** — DynamoDB and DynamoDB Streams
  clients from region / `endpoint_url` (DynamoDB Local, LocalStack, VPC
  endpoints) / credentials.
- **`convert`** — lossless `AttributeValue` ⇄ JSON:

  | DynamoDB | JSON |
  |----------|------|
  | `S` / `BOOL` / `NULL` | string / boolean / `null` |
  | `N` | a number when the decimal is exactly representable (an `i64`/`u64`, or an `f64` whose shortest form equals the stored value), otherwise the original decimal **string** — precision is never lost |
  | `B` | base64 string |
  | `M` / `L` | object / array |
  | `SS` / `NS` / `BS` | arrays (numbers follow the `N` rule, binaries are base64) |

  Writing maps strings → `S`, numbers → `N`, booleans → `BOOL`, `null` →
  `NULL`, arrays → `L`, objects → `M`. `item_to_typed_json` /
  `typed_json_to_item` round-trip DynamoDB typed JSON (used for scan cursors).
  `item_size` estimates item size per DynamoDB's sizing rules.
- **`table`** — `key_schema` (partition key first) from `DescribeTable`.
- **`retry`** — `RetryPolicy { max_retries, initial_backoff_ms, max_backoff_ms }`
  and error classification: throttling (`ProvisionedThroughputExceededException`,
  `ThrottlingException`, `RequestLimitExceeded`, `LimitExceededException`) and
  transient failures retry with jittered exponential backoff; everything else
  fails immediately.

## License

MIT OR Apache-2.0
