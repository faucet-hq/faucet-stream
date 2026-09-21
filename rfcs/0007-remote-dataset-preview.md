# RFC 0007 — Remote dataset preview

*Let the serve console preview a capped page from any source connector — BigQuery, Snowflake, Postgres, an object store — using credentials supplied for that one request and never stored.*

| | |
|---|---|
| **RFC** | 0007 |
| **Title** | Remote dataset preview |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #591 (this RFC is its first acceptance criterion) · #586 (the preview engine) · #642 · #279 (catalog) · epic #38 |
| **Related ADRs** | — |

## Summary

`faucet serve` can already read back a **local** sink's output and render it in
the console (#586). The engine behind it
(`cli/src/serve/preview/engine.rs`) is generic: `read_capped(&PreviewRequest)`
takes any source `kind`, that connector's own `config`, and a row cap. The only
thing that makes today's endpoint local-only is its *input policy* — the caller
names a ledger id, and the path comes from the ledger row. This RFC proposes a
second endpoint over the same engine that accepts a **caller-supplied source
spec plus credentials**, so an operator can answer "did my BigQuery table
actually land correctly?" without leaving the console. It is read-only, capped,
push-down-limited, RBAC-gated, audited, and stores nothing.

## Motivation

faucet already owns both halves and connects neither:

- **The read half exists.** Twenty-odd source connectors, and a capped,
  deadline-bounded, byte-budgeted preview loop that stops early and reports *why*
  it stopped (`Capped::{Rows,Bytes,Time}`). `PreviewRequest.kind`'s own doc
  comment already says "and — for #591 — any other registered source".
- **The question is the common one after a run.** The console shows a run
  succeeded and a catalog dataset with a row count. The next thing an operator
  wants is the rows. Today that means leaving for the BigQuery console, the
  Snowflake UI, or `psql` — with a different set of credentials and no
  connection back to the run that wrote it.
- **Local-only preview is an asymmetry that reads as a bug.** A jsonl sink is
  previewable; the BigQuery table written by the very same pipeline is not,
  purely because one is a file on the server's disk.

Doing nothing leaves #586's engine serving one narrow case, and leaves the
console's dataset pages unable to show the data the catalog describes.

## Guide-level explanation

In the console's **Datasets** view, a non-local dataset gains a **Preview**
button. Clicking it opens a form:

- the **source kind** (pre-filled from the catalog dataset's connector, editable),
- that connector's own config fields (pre-filled from the recorded dataset URI
  where it can be derived — project/dataset/table for BigQuery, the table for a
  SQL source),
- an **auth** block rendered from the connector's existing auth schema (service
  account JSON, a connection URL, a key pair — whatever that connector declares),
- a row count, bounded by the server's cap.

Submitting returns the first N rows in the same table the local preview uses,
with the same honest footer: *"500 of 1,284,993 rows — stopped at the row cap."*

The equivalent HTTP call:

```
POST /v1/preview
{
  "source": {
    "type": "bigquery",
    "config": {
      "project_id": "acme-analytics",
      "query": "SELECT * FROM `acme-analytics.sales.orders`",
      "credentials": { "type": "service_account", "config": { "json": "…" } }
    }
  },
  "row_count_to_load": 500
}
```

```json
{ "kind": "bigquery", "rows": [...], "columns": [...], "row_count": 500,
  "row_limit": 500, "max_rows": 1000, "truncated": true, "capped_by": "rows",
  "elapsed_ms": 812 }
```

And from the CLI, `faucet preview` already does this for a config file — the
endpoint is the console's way to reach the same capability without one.

## Reference-level explanation

### No `faucet-core` changes, and no new preview subsystem

The entire feature is a second handler plus a UI form over
`serve::preview::engine`. `PreviewRequest` is unchanged.

### New handler: `cli/src/serve/handlers/remote_preview.rs`

```rust
pub struct RemotePreviewRequest {
    /// `{ type, config }` — the same shape a `source:` block takes.
    pub source: ConnectorSpec,
    pub row_count_to_load: Option<String>,   // count, or "all"
}
```

The handler is, like #586's, an **input-policy layer**; the engine does the work.
Its policy has six parts.

**1. Off unless the operator opted in.** A new `--preview-remote` flag, separate
from `--preview-local-outputs`. The two grant different things — one discloses
files this server wrote, the other lets a caller point the server at any system
whose credentials they hold — so one flag must not imply the other.

**2. RBAC: `PreviewWrite`, operator+.** A new permission rather than reusing
`CatalogRead`. The request is *not* a read of server state; it makes the server
originate an outbound connection with caller-supplied credentials. Viewers do not
get it.

**3. Audited, always, including failures.** `preview.remote` records the
principal, source IP, connector kind, a redacted config fingerprint, the row cap,
the row count returned, and the outcome. The failure case matters most: a series
of denied previews against varying hosts is the signature this endpoint's abuse
looks like.

**4. Credentials live for one request.** The spec is deserialized, every
`secret: true`-shaped and auth field registered with
`secrets::registry` for redaction before the connector is built, used to build
the source, and dropped with the response. Nothing is written to the history
store, the catalog, the run record, or the audit row (which carries a hash, not
the config). The request body is never logged: the tracing span carries the kind
and the principal only. Load-time directives (`${env:}`, `${vault:}`) are
**not** resolved from a submitted body — the same rule the clustered template
trigger enforces (#456 C5) — so a caller cannot use this endpoint to read the
server's own environment or secret store.

**5. The limit is pushed down, never applied after the fact.** This is the
correctness-and-money requirement. `read_capped` stops consuming after N rows,
which bounds *faucet's* memory but not what the remote does: a BigQuery source
whose `query` has no `LIMIT` scans the table and bills for it, whatever faucet
does with the stream. So the handler resolves the cap and then **pushes it into
the connector config before building the source**, per kind:

| kind | pushdown |
|---|---|
| `bigquery`, `snowflake`, `redshift`, `databricks`, `clickhouse`, `postgres`, `mysql`, `mssql`, `sqlite` | wrap the configured query as `SELECT * FROM (<q>) LIMIT n` in that dialect (`TOP n` for mssql), and set `batch_size = n` |
| `mongodb` | `limit: n` |
| `elasticsearch` | `size: n` |
| `rest`, `graphql` | `max_pages: 1` + the connector's page-size field |
| `s3`, `gcs`, `azure-blob`, `parquet`, `csv` | `batch_size = n`; the engine's early stop is sufficient — these are per-object range reads, not billed scans |
| anything else | **refused**, with a message naming the kind |

An allowlist, not a fallback: a connector whose cost model we have not reasoned
about does not get to run an unbounded query on a caller's behalf. `RowCap::Unlimited`
(`row_count_to_load=all`) is refused outright on this endpoint regardless of
`--preview-max-rows`, because "no limit" and "push a limit down" are
contradictory and the local endpoint's escape hatch was sized for a file on
local disk.

**6. Bounded like the local path, plus a connect timeout.** The engine's byte
budget and deadline apply unchanged; the handler adds a shorter connect/first-row
deadline so an unreachable host fails fast rather than holding a worker.

### Governance: masking applies

A preview is a read of production data through a control plane that many people
can open. If a pipeline's `masking:` policy would have masked a column on the way
to a sink, showing it unmasked in a preview of that same sink defeats the
ordering guarantee masking exists to provide. So: **when the preview targets a
catalog dataset whose pipeline declares a masking policy, that policy is compiled
and applied to the returned rows**, using the same
`masking::compile_for_sink` the executor uses. An ad-hoc preview of a source with
no associated policy is unmasked — it is the caller's own credentials against
their own system — and the response says which of the two it was
(`"masked": true|false`), because a viewer must never have to guess whether what
they are looking at is the real value.

### Why this is not a query tool

The request names a connector config, not SQL the server executes on the
caller's behalf — except that several source connectors *take* a query in their
config, which is the whole of their read surface. That is not a loophole we can
close by validation (a `query` field is how those connectors work), so the
boundary is enforced where it is real: `PreviewWrite` is operator+, the limit is
pushed down, every call is audited, and the endpoint is off by default. What the
scope discipline does buy us is that we add **no** query UI, no result
pagination, no saved queries, and no write verbs — the form collects a connector
config, and the response is one capped page.

## Drawbacks

- **A confused-deputy surface.** The server makes outbound connections with
  credentials a caller supplied. On a server reachable by more people than should
  hold those credentials, that is a data-egress path. Mitigations: off by
  default, operator+, audited, capped. It remains the single largest cost of this
  feature and the reason for the separate flag.
- **Credentials in a request body.** They cross TLS and live in memory for the
  request, but they are typed into a browser and travel through the server. Some
  operators will reasonably not want that; hence a flag they need never set, and
  the follow-on possibility of referencing the secrets catalog instead.
- **Cost is bounded, not free.** A pushed-down `LIMIT` still scans in some
  engines (BigQuery bills bytes read for `SELECT *` over a non-clustered table
  even with a `LIMIT`). The docs must say plainly that a preview costs what the
  same query costs, and the audit log is what makes that attributable.
- **Per-kind pushdown is a table to maintain.** A new source connector is not
  previewable remotely until it is added — deliberately, since the failure mode
  of forgetting is a refusal, not a surprise bill.

## Rationale and alternatives

**Why reuse the #586 engine rather than build a preview service?** Because the
engine is the part that is hard to get right — the three simultaneous bounds, the
early stop, and reporting honestly *which* bound was hit — and it is already
written, tested, and in production use on the local path. A second
implementation would diverge on exactly those details.

**Alternative: credentials only by reference to the secrets catalog.** Strictly
safer: the server resolves `${vault:…}` itself, and no credential ever enters a
request. Rejected as the v1 *requirement* because it makes the feature useless
for the case that motivates it — an operator checking a system the server has no
standing credentials for — and because it moves the trust boundary rather than
removing it (whoever can call the endpoint can then read anything the server's
own vault role can reach, which is worse). Listed under future possibilities as
an *additional* accepted form.

**Alternative: no remote preview; deep-link to the vendor console.** Cheap, zero
new surface, and genuinely adequate for BigQuery and Snowflake. Rejected because
it does nothing for Postgres, MySQL, MSSQL, MongoDB, Elasticsearch, or an object
store, and because it cannot apply the masking policy — a deep link shows the
raw column.

**Alternative: preview only datasets the catalog recorded, never an ad-hoc spec.**
Tighter, and it would let the config come from the catalog rather than the
caller. Rejected as the only mode because it cannot serve "explore a source
before wiring a pipeline", which is half the issue's motivation. The catalog path
is kept as the *pre-filled* case, which is what makes the masking policy
discoverable.

## Prior art

- **Airbyte / Fivetran schema browsers** — connect with stored credentials and
  browse source schemas. They store the credential, which is the design we
  deliberately do not adopt for v1.
- **dbt Cloud IDE previews** — capped `LIMIT`-ed previews against the
  warehouse, with the limit pushed into the compiled SQL. Directly the pushdown
  model here.
- **Jupyter/Hex/Metabase** — full query tools. Named here as the thing this is
  explicitly *not*: they own result pagination, saved queries, and visualization,
  which is where a movement engine's scope would end and a BI product's would
  start.
- **Kafka Connect's REST API** — a cautionary case: connector configs containing
  credentials, POSTed to a control plane, were routinely logged in plaintext.
  Hence the explicit never-log rule and the fingerprint-not-body audit row.

## Unresolved questions

Must resolve before implementation:

- Whether `--preview-remote` should additionally take an **allowlist of
  connector kinds**, so an operator can enable it for `bigquery` only. (Leaning:
  yes — `--preview-remote=bigquery,snowflake`, with a bare flag meaning "every
  kind that has a pushdown rule".)

Can resolve during implementation:

- Whether the response should carry the pushed-down query it actually ran, so the
  cost is inspectable. (Leaning: yes, redacted, for SQL kinds.)
- Whether a preview should record a catalog observation. (Leaning: no — a preview
  is not a movement, and recording one would corrupt volume statistics.)

## Future possibilities

- Accept `${vault:…}`-style **references** to the server's secrets catalog as an
  alternative to inline credentials, for operators who prefer the server to hold
  them.
- Preview a *source* directly from the pipeline-authoring form, so a config is
  checked against real data before it is saved.
- Reuse the pushdown table to give `faucet preview` the same cost guarantee on
  the CLI.

## Related

- [RFC process](./README.md)
- [Data Movement Catalog](../docs/book/src/cookbook/catalog.md)
- [PII masking](../docs/book/src/cookbook/masking.md)
- [Documentation hub](../docs/README.md)
