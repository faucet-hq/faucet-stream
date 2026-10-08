# Record transforms

A pipeline's `transforms:` list is a sequence of pure `Fn(Value) -> Value`
steps run on every record between source and sink. Each transform is a
small, declarative reshape — pick the ones you need, list them in the
order you want them to run, and the CLI wires them up for you.

This page is a tour of the standard transforms exposed in YAML. All of
them are listed in `faucet list` and dispatchable as `type:` values.

A `config:` key the transform does not declare is refused at load time
(`faucet validate` reports it, with a `did you mean` hint), so a typo such as
`slat:` for `salt:` never silently runs with the option off. `faucet schema
transform <name>` lists every key.

## At a glance

| Kind | Purpose | Shape |
|---|---|---|
| `flatten` | Collapse nested objects to a flat record | `separator` |
| `rename_keys` | Regex rename of every key, recursively | `pattern`, `replacement` |
| `keys_case` | Re-case every key (snake / camel / pascal / kebab / screaming_snake / dot) | `mode` |
| `spell_symbols` | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) | `extra`, `separator` |
| `select` | Keep only listed top-level fields | `fields: [..]` |
| `drop` | Remove listed top-level fields | `fields: [..]` |
| `set` | Add or overwrite top-level fields with constants | `values: {k: v, ..}` |
| `rename_field` | Exact-name rename (vs. regex) | `fields: {from: to, ..}` |
| `cast` | Coerce per-field types | `fields: {name: type}`, `on_error` |
| `redact` | Replace listed field values with a mask | `fields: [..]`, `mask` |
| `hash` | Hash fields (SHA-256 / BLAKE3) into stable, join-able tokens | `fields: [..]`, `algorithm`, `encoding`, `salt?`, `into?` |
| `json_parse` | Parse a stringified-JSON field into a nested value | `fields: [..]`, `on_error`, `into?` |
| `coalesce` | Fill a missing/null field from a default or first non-null key | `field`, `default` \| `from: [..]`, `treat_empty_string_as_null` |
| `value_case` | Lowercase / uppercase / trim / title / capitalize string values | `fields: [..]`, `mode` |
| `split` | Split a string field into an array on a delimiter | `field`, `delimiter`, `trim`, `into?` |
| `join` | Join an array field into a string with a delimiter | `field`, `delimiter`, `into?` |
| `json_encode` | Serialize a nested field to a JSON string (inverse of `json_parse`) | `fields: [..]` |
| `unpivot` | Reshape wide columns or a map field into long key/value rows (1→N) | `id_fields`, `key_name`, `value_name`, `columns?` \| `from?`, `drop_nulls?` |
| `lookup` | Enrich records by joining an inline / JSONL reference table | `values` \| `jsonl`, `on: {record, ref}`, `add: {out: ref_col}`, `on_missing?` |
| `tree_flatten` | Flatten a recursive report tree / matrix (nested `Rows`) into one row per leaf (1→N) | `children`, `columns: {from, header?, value}` or `groups: [{from, header, header_label?, value?}]`, `root?`, `leaf?`, `ancestors?`, `path_as?` |
| `cross_join` | Cartesian product of two or more sibling array fields → one row per combination (1→N) | `arrays`, `prefix?`, `keep_parent?`, `on_empty?`, `drop_arrays?`, `max_product?` |
| `zip_columns` | Zip a columnar payload (`{columns, rows}`, or several header + cell-array groups such as a `runReport` response) into one object per row (1→N) | `rows_path`, `columns_path` or `groups: [{from, header, header_label?, value?}]` |
| `sql` | Run DuckDB SQL over the whole page; records are the `batch` relation | `query`, `relations?`, `memory_limit?`, `threads?` · page-level (sees the whole batch) · needs `transform-sql` feature · [cookbook](./sql-transform.md) |
| `wasm` | Run a user-provided sandboxed `.wasm` module over each record | `module`, `function?`, `memory_limit_mb?`, `fuel_limit?`, `on_error?`, `reload_on_change?` · per-record · needs `transform-wasm` feature · [cookbook](./wasm-transforms.md) |

The field-targeting transforms (`select`, `drop`, `set`, `rename_field`,
`cast`, `redact`, `value_case`) act on **top-level** fields only —
dotted paths into nested objects are intentionally out of scope. If you
need to reach a nested field, run `flatten` first, then operate on the
flattened key.

Missing fields are silently skipped. None of the field-selection
transforms introduce a `null` for a name that wasn't already on the
record.

## A full example

The runnable file is at `cli/examples/rest_to_stdout_transforms.yaml`:

```yaml
pipeline:
  source:
    type: rest
    config: { ... }

  transforms:
    - type: flatten
      config: { separator: "__" }
    - type: select
      config:
        fields: [id, name, email, address__city, company__name]
    - type: rename_field
      config:
        fields:
          address__city: city
          company__name: company
    - type: value_case
      config:
        fields: [email]
        mode: lower
    - type: cast
      config:
        fields: { id: string }
        on_error: error
    - type: redact
      config:
        fields: [phone]
        mask: "[redacted]"
    - type: set
      config:
        values:
          _source: jsonplaceholder
          _ingested_at: "2026-01-01T00:00:00Z"

  sink:
    type: stdout
    config: { format: json_lines }
```

Run it:

```bash
faucet run cli/examples/rest_to_stdout_transforms.yaml | jq .
```

The order matters: `flatten` runs first so that `select` can reference
`address__city`; `rename_field` runs after `select` so it only has to
rename keys that survived; `cast` runs before `set` so the stamped
`_source` field is left untouched.

## Declaration layers

Transforms can be declared at three layers in a config. The executor
resolves them per matrix row by concatenating contributions in
*lifecycle order* — pipeline first, then source template, then row:

```
final = T_pipeline ++ T_source ++ T_row
```

| Layer | Lives at | Intent |
|---|---|---|
| Pipeline | `pipeline.transforms` | cross-cutting policy (PII redaction, provenance stamp) |
| Source template | `pipeline.sources.<name>.transforms` | cleanup tied to the source's natural emission shape |
| Matrix row | `matrix[i].transforms` | row-specific extras or one-off shaping |

Each layer is optional. Empty layers contribute nothing.

```yaml
pipeline:
  transforms:                                  # T_pipeline (runs first)
    - { type: set, config: { values: { _ingested_at: "${env:NOW}" } } }
  sources:
    users_api:
      type: rest
      transforms:                              # T_source
        - { type: flatten, config: { separator: "__" } }
        - { type: keys_case, config: { mode: snake } }
matrix:
  - id: users_pii
    source: { ref: users_api }
    transforms:                                # T_row (runs last)
      - { type: redact, config: { fields: [email], mask: "[pii]" } }
    # final = [set, flatten, keys_case, redact]
```

## Opting out: `inherit_transforms: false`

Each layer that *introduces* transforms (source template, matrix row) carries
a sibling boolean field `inherit_transforms`, default `true`. Set to `false`,
it drops every layer declared above it.

| `source.inherit_transforms` | `row.inherit_transforms` | Final list |
|---|---|---|
| `true` (default) | `true` (default) | `T_pipeline ++ T_source ++ T_row` |
| `false` | `true` | `T_source ++ T_row` |
| `true` | `false` | `T_row` |
| `false` | `false` | `T_row` |

Use this for debug rows that need raw records, or for a source whose natural
shape is already canonical and shouldn't be touched by global policy:

```yaml
matrix:
  - id: forensic_row
    source: { ref: users_api }
    inherit_transforms: false              # ← drops T_pipeline AND T_source
    transforms:
      - { type: select, config: { fields: [id, raw_payload] } }
    # final = [select]
```

Sinks reject both `transforms:` and `inherit_transforms:`. Destination shaping
belongs at the pipeline or row layer.

## Reusing transform lists across sources

Use YAML anchors:

```yaml
pipeline:
  sources:
    users_api:
      type: rest
      transforms: &user_cleanup
        - { type: flatten, config: { separator: "__" } }
        - { type: keys_case, config: { mode: snake } }
    archived_users_api:
      type: rest
      transforms: *user_cleanup
```

No grammar extension needed — the YAML parser expands anchors before the
config reaches `faucet`.

## `keys_case` — pick the output convention

```yaml
- type: keys_case
  config:
    mode: snake   # | camel | pascal | kebab | screaming_snake | dot
```

The tokeniser splits each key on whitespace, `_`, `-`, dropped
punctuation, and lower→upper transitions (so `"firstName"` and
`"first_name"` and `"first-name"` all tokenise the same), then re-joins
in the requested style:

| Input          | `snake`        | `camel`       | `pascal`     | `kebab`        | `screaming_snake` | `dot`          |
|----------------|----------------|---------------|--------------|----------------|-------------------|----------------|
| `"First Name"` | `first_name`   | `firstName`   | `FirstName`  | `first-name`   | `FIRST_NAME`      | `first.name`   |
| `"last-name"`  | `last_name`    | `lastName`    | `LastName`   | `last-name`    | `LAST_NAME`       | `last.name`    |
| `"camelCase"`  | `camel_case`   | `camelCase`   | `CamelCase`  | `camel-case`   | `CAMEL_CASE`      | `camel.case`   |
| `"ID"`         | `id`           | `id`          | `Id`         | `id`           | `ID`              | `id`           |

`dot` (`dot.case`) is handy for backends that expect dotted field names
(some search / metrics systems). It tokenises identically to the other
modes — only the join separator differs.

Two distinct keys that re-case to the same name error rather than
silently overwriting (same collision rule as `flatten` and
`spell_symbols`). An all-symbol key (`"!@#"`) tokenises to nothing and
is kept as-is to avoid producing a blank key.

Multi-char uppercase runs are left as one token: `"XMLParser"` →
`["XMLParser"]` → `xmlparser` (snake). If you need them split, normalise
with `rename_keys` first.

## `spell_symbols` — symbols → words in keys

```yaml
- type: spell_symbols
  config:
    extra:
      "©": copyright
      "<=": lte
    separator: " "   # default
```

The default map covers the common ASCII symbols:

| `%` → `percent` | `#` → `number` | `$` → `dollar` | `&` → `and` | `@` → `at` |
| `+` → `plus` | `*` → `star` | `=` → `equals` | `<` → `lt` | `>` → `gt` |
| `/` → `slash` | `\` → `backslash` | `|` → `pipe` | `^` → `caret` | `~` → `tilde` |

User entries in `extra` are merged on top of the defaults (an override
with the same key wins). Replacements are sorted longest-first, so
`"<="` beats `"<"` when both are present.

Each replacement is surrounded by `separator` (default `" "`) so a
chained `keys_case` cleanly picks up the word boundary:

```yaml
transforms:
  - type: spell_symbols
  - type: keys_case
    config: { mode: snake }
```

turns `"% sold"` → `" percent sold"` → `"percent_sold"`.

## `select` vs. `drop`

```yaml
- type: select
  config:
    fields: [id, email]
```

Listed fields are kept; everything else is dropped.

```yaml
- type: drop
  config:
    fields: [password, ssn]
```

Listed fields are removed; everything else is kept. Use `select` when
the schema is fixed and you want to defend against the source adding
new fields you don't want; use `drop` for targeted PII / secret
removal.

## `set` — constant stamps

```yaml
- type: set
  config:
    values:
      _source: my-api
      _ingested_at: "2026-05-28T00:00:00Z"
      version: 2
      tags: [pii-free]
```

Any JSON value is accepted (string, number, bool, null, array, object).
Existing fields with the same name are **overwritten** — `set` is the
intentional "I want this value" transform.

## `rename_field` vs. `rename_keys`

Both transforms rename keys, but they're aimed at different jobs:

| `rename_keys` | `rename_field` |
|---|---|
| Single regex substitution applied to every key, recursively (including keys inside nested objects and arrays). | Exact-name match on top-level keys only. |
| Best for systematic patterns: `^_sdc_` → `""`, `([a-z])([A-Z])` → `$1_$2`. | Best for a handful of explicit renames: `address__city` → `city`. |

`rename_field` errors if a target name already exists on the record
(same collision rule as `flatten` and `keys_case`) — to avoid silently
overwriting a real value.

## `cast` — type coercion

```yaml
- type: cast
  config:
    fields:
      age: int
      price: float
      active: bool
      id: string
      created_at: timestamp
    on_error: error
```

Target types: `int` (i64), `float` (f64), `bool`, `string`, `timestamp`
(RFC 3339). `bool` from a string accepts `true|false|1|0|yes|no`
case-insensitively. `timestamp` parses RFC 3339 / ISO 8601 and
normalises the output (so `+00:00` becomes `Z`). Casting a **float to
`int`** only succeeds for a whole number within i64 range — a fractional
value (e.g. `3.9`) or one beyond ±9.2e18 is treated as uncastable (governed
by `on_error`) rather than being silently truncated or saturated.

Failure behaviour is controlled by `on_error`:

| `on_error` | What happens on an uncastable value |
|---|---|
| `error` *(default)* | The transform errors with `FaucetError::Transform`. The pipeline either aborts or routes the record to the DLQ, depending on your DLQ config. |
| `null` | The value is replaced with `null`. Use when the schema must hold and a downstream nullable column is acceptable. |
| `skip` | The value is left as-is (original type). Use when downstream code already handles mixed types. |

Missing fields are always a no-op — `cast` will never insert a `null` for
a field that wasn't already on the record.

Casting epoch seconds / millis to a timestamp is out of scope for the
initial release; file a follow-up issue if you need it.

## `redact`

```yaml
- type: redact
  config:
    fields: [password, ssn, credit_card]
    mask: "***"
```

`mask` is any JSON value (default `"***"` if omitted). Missing fields
are skipped — `redact` will not add `"***"` to a record that didn't
have the field.

> For a policy-driven layer that *detects* PII by value (whatever the column is
> called), reaches into nested paths, hashes/tokenizes for joinable pseudonyms,
> and scopes rules per destination sink, see
> [PII detection & masking](./masking.md).

## `hash`

```yaml
- type: hash
  config:
    fields: [email, user_id]   # one or more fields
    algorithm: sha256          # sha256 (default) | blake3
    encoding: hex              # hex (default) | base64
    salt: "${env:HASH_SALT}"   # optional; prepended before hashing
    into: null                 # optional target key (single field only); null = in place
```

Unlike `redact` (which destroys the value), `hash` produces a **stable,
join-able token**: the same input always maps to the same digest, so
downstream joins still work while the raw PII never reaches a sink.
String values are hashed over their raw UTF-8 bytes; every other JSON
value is hashed over its canonical serialization. Missing fields are
skipped. `into` is only valid with exactly one field (a config error
otherwise); with multiple fields each is replaced in place. Needs the
`transform-hash` feature.

> This is pseudonymization, not a secret — an unsalted digest is
> recomputable by anyone. For keyed, policy-driven hashing see
> [PII detection & masking](./masking.md).

## `json_parse`

```yaml
- type: json_parse
  config:
    fields: [payload, metadata]   # dotted keys holding JSON strings
    on_error: keep                # keep (default) | null | error
    into: null                    # optional target key (single field only); null = in place
```

Expands a stringified-JSON column into a real nested value the rest of
the pipeline (and the sink) can see — pairs naturally with `flatten`
(parse, then flatten). Values that are already objects/arrays (or any
non-string) pass through unchanged (idempotent); missing fields are
skipped. Parse failures follow `on_error`: `keep` leaves the string,
`null` replaces it with `null`, `error` aborts (or routes to the DLQ). A
1→1 transform can't drop a record, so there is no `skip_record` — chain
a `filter` if you need to drop rows whose JSON failed. Needs the
`transform-json-parse` feature.

## `coalesce`

```yaml
- type: coalesce
  config:
    field: status
    # exactly one of:
    default: "unknown"            # a literal JSON value, OR
    from: [status, state]         # first non-null among these keys wins
    treat_empty_string_as_null: false
```

Fills a **missing or null** field — the "set only if absent" primitive
that `set` (which always overwrites) can't express. Exactly one of
`default` / `from` must be set (a config error otherwise). A present,
non-null target is left unchanged (idempotent). With
`treat_empty_string_as_null: true`, an empty string counts as null for
both the target and the `from` keys. If every `from` key is null/absent
and no `default` is given, the target is left as-is. Needs the
`transform-coalesce` feature.

## `value_case`

```yaml
- type: value_case
  config:
    fields: [email, username]
    mode: lower   # | upper | trim | title | capitalize
```

Only string field values are touched; non-string values (numbers, bools,
nulls, nested objects) pass through unchanged.

- `title` upper-cases the first letter of each **whitespace-delimited**
  word and lower-cases the rest (`"new york"` → `"New York"`);
  punctuation and underscores do not start a new word.
- `capitalize` upper-cases only the first character of the whole string
  and lower-cases the rest (`"hELLO wORLD"` → `"Hello world"`).

Both use `char::to_uppercase` semantics (no locale-aware casing).

## `split` / `join`

```yaml
- type: split
  config: { field: tags, delimiter: ",", trim: true, into: null }
- type: join
  config: { field: tags, delimiter: ",", into: null }
```

`split` turns a delimited string into an array; `join` is its inverse.
Both are no-ops on the wrong type (`split` on a non-string, `join` on a
non-array) or a missing field. With `trim`, `split` whitespace-trims each
element but **keeps empty segments**. `join` renders non-string elements
via their JSON scalar form (strings raw, `null` as empty, everything else
as compact JSON). An empty `delimiter` on `split` yields a single-element
array holding the whole string (rather than splitting between every
char). When `into` is set the result is written there, else in place.
Needs the `transform-split-join` feature.

## `json_encode` — nested field → JSON string

```yaml
- type: json_encode
  config: { fields: [address, line_items] }
```

The inverse of `json_parse`: each named field whose value is an object or
array is replaced **in place** with its compact JSON-string form — the
standard step for landing nested data as a flat `STRING` column (e.g. when
matching a warehouse table that stores nested structures as text). Scalar
(already-flat) values and absent fields are left unchanged (idempotent).
Needs the `transform-json-encode` feature.

## `unpivot` — wide/map → long (1→N)

```yaml
# Wide form: monthly columns → one row per month.
- type: unpivot
  config:
    id_fields: [account_id]        # copied onto every output row
    key_name: month                # column name → this field
    value_name: amount             # cell value → this field
    # columns: [jan, feb, mar]     # optional; default = all non-id fields
    drop_nulls: true               # skip null cells

# Map form: expand an object field's entries into rows.
- type: unpivot
  config:
    id_fields: [report_id]
    from: cells                    # the object field to expand
    key_name: column
    value_name: value
```

`unpivot` reshapes each record into **N rows** — one per selected column
(wide form) or per entry of the `from` object (map form) — carrying
`id_fields` onto each. Output rows contain only `id_fields` plus the
key/value pair. When the reshape yields nothing (missing `from`, or no
columns) the original record is **passed through unchanged** unless
`drop_if_empty: true` — records are never silently dropped. This replaces
the SQL you'd otherwise write for gross-to-net / period-report / timeseries
data. Needs the `transform-unpivot` feature.

## `lookup` — enrich from a reference table (no SQL)

```yaml
- type: lookup
  config:
    values:                                  # inline reference rows …
      - { id: "1", name: "North America" }
      - { id: "2", name: "EMEA" }
    # jsonl: ./ref/regions.jsonl             # … or a JSONL file (one object/line)
    on: { record: region_id, ref: id }       # match record.region_id == ref.id
    add: { region_name: name }               # add record.region_name = ref.name
    on_missing: null                         # null (default) | keep | error
```

`lookup` joins each record against a small in-memory reference set by key
(compared by scalar-string form, so `42` matches `"42"`) and writes the
`add` columns onto the record — a code→label enrichment without a SQL
transform. It is 1→1 (never drops rows): on a miss it writes the added
columns as `null` (`null`), leaves the record untouched (`keep`), or fails
the batch (`error`). The reference is resolved once at config-load. Needs
the `transform-lookup` feature.

## `tree_flatten` — recursive report tree / matrix → rows (1→N)

```yaml
- type: tree_flatten
  config:
    root: "Rows.Row"            # path to the top-level node array (omit → the record itself)
    children: "Rows.Row"        # a node's child-array (the recursion key)
    leaf: has_no_children       # has_no_children (default) | has_field:<name>
    columns:
      from: "ColData"           # a leaf's cell array …
      header: "Columns.Column"  # … paired positionally with these header defs …
      header_label: "ColTitle"  # … reading each header's label from this field
      value: "value"            # the cell field to read (ColData[i].value)
    ancestors:
      field: "Header.ColData[0].value"  # each group node's label
      as: [section, subsection]         # column names per depth (extra → ancestor_N)
    path_as: group_path         # optional: the joined path, e.g. "Income > Sales"
    drop_empty: true            # skip leaves whose cells are all empty
    # emit_group_rows: false    # also emit subtotal (group) rows
    # max_depth: 64             # stack-overflow backstop
```

Financial-report APIs (profit-and-loss, balance-sheet and similar endpoints)
return a **self-referential nested-`Rows` matrix** — a tree of section →
subsection → line. `tree_flatten` walks it depth-first, carries the section
labels down, and emits **one flat row per leaf**, naming the value columns from
the report's header row and the group columns from `ancestors.as`. It is the
one reshape that otherwise forced these connectors onto the embedded-DuckDB SQL
transform; `tree_flatten` keeps them inbuilt. Uneven branch depth leaves the
missing ancestor levels null; a header/cell length mismatch zips to the shorter;
a tree deeper than `max_depth` (a malformed or cyclic one) fails the record
rather than overflowing the stack or dropping the deeper rows, and a repeated
header label (or one that collides with an ancestor/path column) fails it too. It also flattens any generic `children` tree (org charts,
category trees, BOM explosions). Column-lineage is opaque (structure-changing).
Needs the `transform-tree-flatten` feature.

### Several column groups per leaf (`groups`)

When each leaf splits its cells into several positional arrays, each named by
its own header list, use `groups` instead of `columns` — the same shape as
[`zip_columns` groups](#several-column-groups-groups), but each group's
`header` is a path within the record and `header` is required:

```yaml
- type: tree_flatten
  config:
    root: rows
    children: rows
    groups:
      - { from: dims, header: dimHeaders, header_label: name }   # value: value (default)
      - { from: mets, header: metHeaders, header_label: name }
    ancestors: { field: label, as: [section] }
```

Every group's columns are merged into one row per leaf. Set exactly one of
`columns` or `groups`. Unlike the lenient single `columns` form, groups are
strict: a leaf missing a group's array, a group whose cell count differs from
its header count, a header path that is not an array, and a column two groups
both name each fail the page, with the group and row named.

## `cross_join` — cartesian product of sibling arrays (1→N)

```yaml
- type: cross_join
  config:
    arrays: [jobs, compensation, employment]  # ≥2 sibling array fields to cross
    prefix: false        # prefix produced columns with the array name (jobs_title)
    keep_parent: true    # carry the record's non-array scalars onto every row
    on_empty: skip       # skip (CROSS JOIN) | one_row (LEFT JOIN … ON true)
    drop_arrays: true    # remove the source array fields after expansion
    max_product: 10000   # fail loudly if a record's product exceeds this
```

Expands one record into the **cartesian product of two or more of its sibling
array fields**, emitting one flat row per combination — e.g. a HCM record's
`jobs[] × compensation[] × employment[]`. Object elements spread their fields
into the row; a field that would overwrite an existing column (a parent field
such as `id`, or a field of an earlier array) fails the record, so set
`prefix: true` to name-prefix them instead. Scalar elements land under the
array's name. A crossed field that is missing or `null` counts as empty; one
that is present but not an array fails the record. This is a different shape from `explode`
(one array → N rows) and `unpivot` (wide → long), and the last per-record
reshape that otherwise forced a connector (e.g. `ukg_pro`) onto the DuckDB SQL
transform. An empty crossed array yields zero rows (`skip`) or a null-filled row
(`one_row`); a record whose product would exceed `max_product` fails the run
rather than risking OOM. Column-lineage is opaque (structure-changing). Needs the
`transform-cross-join` feature.

## Ordering rules of thumb

Transforms run **in the order you list them**, so think about
dependencies:

- `flatten`, `spell_symbols`, and `keys_case` change key names — list
  field-targeting transforms (`select`, `drop`, `cast`, `redact`,
  `value_case`, `rename_field`) **after** them, referencing the
  post-rename keys.
- `cast` runs before downstream consumers see the record, so put it
  after any rename steps but before `set` if you want `set`'s stamped
  values left untouched.
- `set` overwrites by name — put it last when you want it to win.

The "clean keys for a downstream warehouse" pipeline is canonical:

```yaml
transforms:
  - type: spell_symbols     # %sold → percent sold
  - type: keys_case
    config: { mode: snake } # percent sold → percent_sold
  - type: rename_field
    config:
      fields: { legacy_id: id }
```

## Out of scope

- **Dotted-path field selection on the field-list transforms** (`select`,
  `drop`, `cast`, `redact`, `value_case`, `rename_field`) — they still
  operate on bare top-level keys. Run `flatten` first if you need nested
  access. `filter` and `explode` are the exceptions and support the
  JSONPath subset documented in their sections.
- **A general expression / scripting transform (jq, CEL, …)** —
  separate, larger discussion.

## Filter and explode

### Filter — keep records matching a predicate

```yaml
transforms:
  - { type: filter, config: { path: deleted, op: ne, value: true } }
```

Operators: `eq`, `ne`, `exists`, `in`, `not_in`.

- `path:` — JSONPath subset: bare key (`status`), dot path (`$.user.status`), or bracketed string key (`$['order-id']`). Bare keys are auto-prefixed with `$.`. Keys that literally contain `.` require the `$`-rooted bracket form (`"$['foo.bar']"`).
- `value:` — required for `eq` / `ne` / `in` / `not_in`. For `in` / `not_in`, must be an array. Forbidden for `exists`.
- Type semantics: strict JSON equality. `"5" eq 5` is false. Chain `cast` upstream to coerce.
- `ne` and `not_in` **keep records with a missing path** (the predicate is satisfied by absence). All other operators drop missing-path records.

### Explode — expand an array into one record per element

```yaml
transforms:
  - { type: explode, config: { path: items, prefix: item } }
```

- `path:` — same JSONPath subset as filter.
- `prefix:` — prepended to each element field when the element is an object. Defaults to the last segment of `path` (so `path: items` ⇒ `prefix: items`). Empty string opts out of prefixing (pure LATERAL FLATTEN).
- `separator:` — between prefix and element field key. Default `"_"`.
- `on_missing:` — what to do when the path doesn't yield a non-empty array. `passthrough` (default — record flows through unchanged), `drop` (SQL `UNNEST` semantics), or `error`.

**Merge rule (object elements):** the array node at `path` is removed from its parent container and each element field is added as a sibling, prefixed.

| Input | Stage | Output |
|---|---|---|
| `{id: 1, items: [{sku: A, qty: 2}]}` | `explode { path: items }` | `{id: 1, items_sku: A, items_qty: 2}` |
| `{id: 1, items: [{sku: A}, {sku: B}]}` | `explode { path: items, prefix: item }` | `{id: 1, item_sku: A}`, `{id: 1, item_sku: B}` |
| `{id: 1, items: [{sku: A}], prefix: ""}` | `explode { path: items, prefix: "" }` | `{id: 1, sku: A}` |
| `{id: 1, tags: ["rust", "etl"]}` | `explode { path: tags }` | `{id: 1, tags: rust}`, `{id: 1, tags: etl}` |
| `{id: 1, user: {name: A, items: [{x: 1}]}}` | `explode { path: $.user.items }` | `{id: 1, user: {name: A, items_x: 1}}` |

**Collisions** (a prefixed element key would overwrite a sibling) fail loudly with `FaucetError::Transform("explode produced duplicate key 'X'")` — mirroring `flatten` / `keys_case`.

**Carry parent fields down (`carry`).** When the exploded array is nested and the child rows need a parent key to stay joinable, `carry` copies named fields from the parent record onto every child (`{ dest_field: "source.dot.path" }`):

```yaml
- type: explode
  config: { path: values, prefix: "", carry: { employee_id: id } }
```

`{id: 7, values: [{v: a}, {v: b}]}` → `{v: a, employee_id: 7}`, `{v: b, employee_id: 7}`.

## `zip_columns` — columnar payload → one object per row (1→N)

Analytics / report APIs (e.g. a query-language `tableData` payload) return results *positionally*: a list of column descriptors plus a list of value-arrays. `zip_columns` zips each row against the column names.

```yaml
- type: zip_columns
  config: { columns_path: "columns[*].name", rows_path: "rows" }
```

`{columns: [{name: day}, {name: sessions}], rows: [["2026-01-01", 12]]}` → `{day: "2026-01-01", sessions: 12}`. A row whose width differs from the column count fails loudly rather than misaligning fields. Gated on the `transform-zip-columns` feature (in `transforms` / `full`).

### Several column groups (`groups`)

Some report APIs split every row into **several** positional cell arrays, each named by its **own** header list. An analytics `runReport` response is the common case:

```json
{
  "dimensionHeaders": [{"name": "date"}, {"name": "country"}],
  "metricHeaders":    [{"name": "sessions", "type": "TYPE_INTEGER"}, {"name": "bounceRate", "type": "TYPE_FLOAT"}],
  "rows": [
    {"dimensionValues": [{"value": "20260901"}, {"value": "DE"}],
     "metricValues":    [{"value": "1204"},     {"value": "0.41"}]}
  ]
}
```

Use `groups` instead of `columns_path`. Each group is zipped against its own headers, and the groups are merged into one record:

```yaml
- type: zip_columns
  config:
    rows_path: "$.rows[*]"
    groups:
      - { from: dimensionValues, header: "$.dimensionHeaders[*].name", value: value }
      - { from: metricValues, header: "$.metricHeaders[*]", header_label: name, value: value }
```

→ `{date: "20260901", country: "DE", sessions: "1204", bounceRate: "0.41"}`. Add a `cast` transform afterwards to type the metrics.

| Group field | Meaning |
|---|---|
| `from` | Dot path, inside each row, of this group's cell array. A row without it fails the page (a schema change, not "no data"). |
| `header` | JSONPath, evaluated against the **record**, to this group's header list. |
| `header_label` | Field of each header object to use as the column name, when `header` matches objects. |
| `value` | Dot path, inside each cell, of the value. Omit when the cells are the values. A cell without it (or a `null` cell) yields `null`. |

Set exactly one of `columns_path` or `groups`. Two groups naming the same column, a header that is not a string, and a row whose group width differs from that group's header count each fail the page with the group and row named — a value never lands under the wrong column. A record with no rows (the API omits `rows` from an empty report) yields no records. The runnable fixture is `cli/examples/tests/zip_columns_groups_tests.yaml`.

### Ordering: explode early, filter late (usually)

The recommended order is `explode → transform → filter`: each child of the explode gets transforms applied uniformly, and the final filter acts on cleaned shape. Two legitimate deviations:

- **filter before explode**: drop soft-deleted parents *before* exploding, saving the work of expanding children of dead rows.
- **filter both sides**: drop dead parents, explode, then drop archived children.

```yaml
transforms:
  - { type: filter, config: { path: deleted, op: ne, value: true } }
  - { type: explode, config: { path: items, prefix: item } }
  - { type: filter, config: { path: item_status, op: in, value: [active, pending] } }
  - { type: keys_case, config: { mode: snake } }
```

## `cdc_unwrap` — normalize CDC change events into flat rows

The CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) emit change-event
**envelopes** — a wrapper carrying an operation code and the row's before/after
images — not the bare rows themselves. `cdc_unwrap` flattens that envelope into a
single row plus an `__op` marker, so a downstream
[upsert sink](./upsert.md) can mirror the change without understanding CDC at all.
It's the standard first transform in a CDC → mirror pipeline:

```yaml
transforms:
  - type: cdc_unwrap
```

For each change event it:

- **drops** DDL / truncate events (`op` ∈ `drop_ops`) — they have no row to mirror;
- for a **delete** (`op` ∈ `delete_ops`), emits the pre-image (`before`), falling
  back to `key_field` (MongoDB carries the key in `document_key` when there is no
  `before`);
- for an **insert / update**, emits the post-image (`after`);
- **fails the run** on an event it cannot turn into a row (an update with no
  `after`, a delete with no key) unless `on_missing_image: drop`, because
  dropping it would leave the mirror silently out of date;
- stamps every emitted row with a `marker_field` (`__op`) set to the normalized
  value **`"d"`** (delete) or **`"u"`** (upsert) — *not* the raw op code. A
  downstream sink's `delete_marker` should therefore match `"d"`;
- with `key` set (the sink's upsert `key`), turns an update that **changed the
  key** — `before` and `after` disagree on a key column — into a delete of the
  old key followed by the upsert of the new row. Without it a mirror keeps the
  row under its old key forever:

  ```yaml
  transforms:
    - type: cdc_unwrap
      config: { key: [id] }
  ```

  postgres-cdc carries the old key on such an update under the default
  `REPLICA IDENTITY`; mysql-cdc carries it with `include_columns: true`.

It is a 1→0|1|2 stage (an input row becomes zero or one output row, two for a
key-changing update) and runs in declaration order like any other transform.

### Config fields and defaults

| Field | Default | Purpose |
|-------|---------|---------|
| `op_field` | `op` | Envelope field holding the operation code |
| `after_field` | `after` | Envelope field holding the post-image |
| `before_field` | `before` | Envelope field holding the pre-image |
| `key_field` | `document_key` | Fallback key for deletes with no `before` (MongoDB) |
| `marker_field` | `__op` | Field stamped on every emitted row (`"d"` / `"u"`) |
| `delete_ops` | `["d", "delete"]` | `op` values that mean delete |
| `drop_ops` | `["ddl", "truncate"]` | `op` values dropped entirely |
| `on_missing_image` | `fail` | `fail` or `drop` an event with no usable row image |
| `key` | `[]` | Key columns; an update whose `before` key differs from its `after` key also deletes the old key |

The defaults span all three CDC vocabularies seen in the wild — `insert` /
`update` / `delete` / `truncate`, `c` / `u` / `d` / `ddl`, and `c` / `u` / `r` /
`d` / `ddl` — so a bare `- type: cdc_unwrap` works for postgres-cdc, mysql-cdc,
and mongodb-cdc without per-source tuning.

Sources whose updates can lack an `after` image:

- **mongodb-cdc** sends one only with `full_document: update_lookup` (or
  `required` with collection post-images enabled). `faucet validate` refuses
  `cdc_unwrap` over the default `full_document: off` or `when_available`. Under
  `update_lookup` a missing image means the document was deleted before the
  lookup and its delete event follows, so `on_missing_image` defaults to `drop`
  there.
- **DynamoDB streams** carry a new image only with the `NEW_IMAGE` or
  `NEW_AND_OLD_IMAGES` stream view type; `faucet doctor` reports the table's
  view type.

`cdc_unwrap` is a built-in transform gated on the `transform-cdc-unwrap` feature
(included in the `full` build). It is **opaque** for column-lineage analysis (it
reshapes the whole envelope), so faucet emits no column-lineage edges for it.

See the [Upsert / mirror tables](./upsert.md) cookbook for the full
CDC → mirror pipeline.
