# Value fidelity

A faucet record is a JSON value. JSON has one number type, which most readers —
and `serde_json` inside faucet — hold as a 64-bit float. That is where silent
corruption comes from: a `DECIMAL(38,9)` rounded through a double, an `id` past
2^53, a timestamp that lost its zone or its microseconds, bytes re-encoded
differently by every connector. None of these fail a run.

So every connector renders a typed value the same way, through the converters
in `faucet_core::fidelity`. A value means the same thing whichever source
produced it, and every sink reads the same rendering back.

## Canonical renderings

| Backend value | JSON | Example |
|---|---|---|
| Exact decimal — `DECIMAL`, `NUMERIC`, `MONEY`, `NUMBER(p,s)`, Arrow `Decimal*`, BigQuery `NUMERIC` | a **string** with the exact digits at the column's declared scale | `"12345678901234567890.123456789"`, `"1.50"` |
| Integer that fits `i64` / `u64` | a JSON number | `9223372036854775807` |
| Integer beyond `i64` / `u64` — 128-bit, `NUMERIC(38,0)`, `UInt128` | a **string** of its digits | `"123456789012345678901234567890"` |
| Number with no declared scale — an unscaled Oracle `NUMBER`, a DynamoDB number, a number inside a JSON document or payload | a JSON number when the double it parses to names the same value, otherwise a string of its exact digits | `1.5`, `"0.12345678901234567890"` |
| `FLOAT` / `DOUBLE` | a JSON number; a single-precision value keeps its shortest form, `-0.0` keeps its sign | `0.1` (not `0.10000000149011612`) |
| `NaN`, `±Infinity` (float or decimal) | **`null`, counted** — see below | `null` |
| Instant — `TIMESTAMPTZ`, a zoned Arrow timestamp | RFC 3339 with an explicit offset (`Z` for UTC) | `"2026-09-18T12:34:56.123456Z"` |
| Local date-time — a timestamp with no zone | ISO 8601 without an offset; an offset the backend never stored is not invented | `"2026-09-18T12:34:56.123456"` |
| Date, time of day | `YYYY-MM-DD`, `HH:MM:SS[.fff…]` | `"2026-02-28"`, `"12:30:00.5"` |
| Binary — `BYTEA`, `BLOB`, `VARBINARY`, Arrow `Binary`, Avro `bytes` | standard **base64** with padding | `"AAEC/f7/"` |

**Exception — MySQL `DATETIME`.** The `mysql` query source and the
`mysql-cdc` source render a zone-less `DATETIME` as RFC 3339 at `+00:00`
(`"2024-01-03T04:05:06.123456+00:00"`), not as a local date-time. The query
source has always read it that way, and the CDC source matches it so a
`faucet mirror` snapshot and its change stream agree (#933 changed the CDC
rendering from `"2024-01-03 04:05:06.123456"`). The `+00:00` is a
convention, not an offset MySQL stored: a column holding wall-clock local
times keeps those wall-clock values, so treat the offset as "as stored" rather
than converting them as UTC instants. `TIMESTAMP` is a real instant and is
rendered in UTC as usual.

Temporal values are never truncated. A converter that knows the column's unit
(Arrow `Timestamp(µs)`, Avro `timestamp-micros`) renders that unit's fixed
number of fractional digits; a driver value whose unit is not known renders as
many digits as the value needs.

A document store whose own typed JSON round-trips its types keeps it:
MongoDB reads as relaxed extended JSON, so a `Decimal128` is
`{"$numberDecimal": "…"}` and a `Date` `{"$date": …}`, and the MongoDB sink
restores them.

## Non-finite values

JSON cannot hold `NaN` or an infinity. A source writes such a value as `null`
and counts it:

- the run summary's row accounts gain `non_finite` (`faucet run` prints
  `non-finite values written as null N`; the serve run detail carries it under
  `rows`);
- the metric `faucet_fidelity_non_finite_total{pipeline,row}` increases;
- the first one in a process logs a warning, and each run that saw any logs how
  many.

On the columnar (Arrow) path a non-finite float is still a float; it becomes a
counted `null` only where a batch is turned into JSON rows.

## How sinks read the renderings

- A string bound into an exact-decimal column is parsed by the server, never
  through a float. A non-integer JSON number is bound as its shortest decimal
  text (`0.1`), so a `DECIMAL(38,20)` column stores `0.1` rather than the
  binary double `0.1000000000000000055511151231257827`.
- A binary column reads base64. The Kafka sink's Avro `bytes_encoding` defaults
  to `base64`; `hex` reads the lowercase hex sources wrote before this model.
- A JSON payload is parsed with every number exact: a literal the parsed value
  would not spell again — an integer past `u64`, a decimal longer than a double
  carries — is kept as a string of its digits. A 17-digit rendering of a double
  (`0.10000000000000001`) still names that double.

## Declared column types

An exact decimal, a date, a timestamp and a UUID all render as JSON strings,
so a sink that creates its destination from the first page's values
(`create_table: true`) would create them as text columns. A source that knows
its column types declares them instead (`Source::declared_columns`), and the
pipeline hands the declaration to the sink (`Sink::set_declared_columns`)
before the first write. The declared types are portable:

| Logical type | Rendering it describes |
|---|---|
| `int64` | a JSON integer |
| `float64` | a JSON number (`NaN` / `±Inf` arrive as `null`) |
| `bool` | a JSON boolean |
| `string` | a JSON string |
| `decimal { precision, scale }` | an exact decimal string; no precision is unbounded |
| `date` | `YYYY-MM-DD` |
| `time` | `HH:MM:SS[.f]` |
| `timestamp { tz }` | RFC 3339 with an offset (`tz: true`), or a date-time without a zone |
| `uuid` | the 36-character hyphenated text |
| `bytes` | standard base64 |
| `json` | any JSON value |

The SQL query sources declare them from the describe of the statement they are
about to run: `postgres` (a `numeric` column read straight from a table column
carries its precision and scale), `mysql`, `mssql`, `redshift` and `duckdb`.
MySQL and Redshift report no decimal precision, so their decimals are declared
unbounded. SQLite is not among them: its columns have affinities, not types,
so no declared type would hold for every row.

Each SQL and warehouse sink maps the declaration through its dialect when it
creates the table — `postgres`, `mysql`, `sqlite`, `mssql`, `duckdb`,
`oracle`, `redshift`, `clickhouse`, `snowflake`, `databricks`, `bigquery` and
`spanner`. BigQuery, for example:

| Declared | BigQuery column |
|---|---|
| `decimal` of at most 29 integer and 9 fractional digits | `NUMERIC` |
| `decimal` up to 38 and 38, or without precision | `BIGNUMERIC` |
| `date` / `time` | `DATE` / `TIME` |
| `timestamp { tz: true }` / `{ tz: false }` | `TIMESTAMP` / `DATETIME` |
| `uuid` | `STRING` |
| `bytes` / `json` | `BYTES` / `JSON` |

A type a dialect cannot hold exactly — a decimal wider than its widest exact
type, a time of day where there is no time type — keeps the inferred column
(text), never a rounding one. The renderings load into the typed columns as
they are: each sink already binds, casts or coerces them per column type.

The rules every sink follows:

- **Only a created table is typed.** An existing table is never retyped; a
  declared type that differs from an existing column is the
  [`schema:` drift policy](../cookbook/schema-drift.md)'s business.
- **The first page decides, column by column.** A declared type is used only
  when every value of the first page is a rendering of it. A transform or a
  masking rule that rewrote a column (a hashed date, a renamed field) therefore
  keeps today's inferred type for that column. A column the page does not
  carry is not created, and an undeclared one (a transform's output, the
  `_faucet_*` metadata columns when no declaration covers them) is inferred.
  Declared columns come first, in the source's order; the rest follow sorted.
- **An empty source creates the empty, typed table** when the declaration is
  complete: nothing between source and sink renamed, added, removed or
  rewrote a column (no transform stage other than a filter, no masking rule).
  The `metadata_columns` decorator adds its own columns to the declaration.
  Under `write_mode: overwrite` the table appears after the commit. Without a
  complete declaration an empty source still creates nothing.
- **Without a declaration nothing changes**: a source that declares nothing, a
  sink that does not type, or a declaration the source could not produce (the
  run logs a warning and infers).

A topology hands the declaration of its one source to each sink node; a
transform, merge or join node anywhere in the graph makes it incomplete.

## The fidelity corpus

`faucet_conformance::fidelity` holds a corpus of the values that have broken a
pipeline somewhere: integers at and past 2^53, `-0.0`, `1e±300`, strings with
quotes, backslashes, tabs and padding, timestamps with offsets and before the
epoch, nested and empty containers, and the canonical renderings above. Every
source and sink runs it, in one of three forms:

| Form | Where | Example |
|---|---|---|
| Pair round trip | `crates/interop-tests/tests/*_fidelity.rs` | Postgres sink → query source and CDC source |
| One-sided against the backend | the connector's `tests/` | the Postgres sink writing typed columns and reading them back with SQL |
| The connector's converter | a `#[cfg(test)]` module of the connector's `src/` | a messaging source decoding the corpus as message payloads |

A destination that genuinely cannot hold a value says so with a
[`Tolerance`](https://docs.rs/faucet-conformance/latest/faucet_conformance/fidelity/struct.Tolerance.html)
in its test, with a comment naming the limitation — SQLite has no boolean,
Oracle no negative zero, an Arrow list one element type.

Two CI checks (the `Doc counts` job) keep this true:

- `scripts/fidelity-coverage.py` fails when a connector crate runs none of
  the corpus assertions. A connector that cannot lists its reason in
  `crates/conformance/fidelity-exempt.txt` (empty today).
- `scripts/decimal-f64.py` fails on a float conversion next to a decimal in
  connector code; a line that converts a float which is not a decimal is listed
  in `scripts/decimal-f64-allow.txt` with the reason.

## For connector authors

Render values through `faucet_core::fidelity` rather than by hand:

| Value | Converter |
|---|---|
| Decimal text from the driver | `decimal_text(text)` |
| Unscaled integer + scale | `decimal_from_i128(unscaled, scale)` |
| PostgreSQL-wire binary `NUMERIC` | `decimal_from_pg_numeric(bytes)` |
| Integer of any width | `integer_i128`, `integer_u128`, `integer_text` |
| Number with no declared scale | `number_text(text)` |
| Float | `float64`, `float32` |
| Bytes | `binary(bytes)` |
| Instant / local date-time / date / time | `timestamp`, `timestamp_naive`, `timestamp_from_epoch`, `date`, `time` |
| JSON payload text | `from_str` / `from_slice` (a drop-in for `serde_json`'s) |
| JSON stored by a database | `parse_json` |
| A non-integer number bound into a SQL column | `non_integer_text` (or `faucet_common_sql::Param::non_integer_text`) |

A converter that runs on a thread of its own (a `spawn_blocking` decoder)
carries the run's counter there with `NonFiniteCounter::current()` and
`fidelity::propagate`, so its non-finite values are counted on the right run.
Then add the corpus to the connector's tests — see
[Authoring connectors](../extending/authoring-connectors.md#values-the-fidelity-model).
