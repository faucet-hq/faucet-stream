# Your first pipeline

This walkthrough moves a local CSV file to JSON Lines — no external services
required, so it works immediately after `cargo install faucet-cli`.

## 1. Create some input

```bash
mkdir -p data out
cat > data/input.csv <<'CSV'
id,name,city
1,Ada,London
2,Grace,New York
3,Linus,Helsinki
CSV
```

## 2. Write a config

Create `pipeline.yaml`:

```yaml
version: 1
name: csv_to_jsonl

pipeline:
  source:
    type: file
    config:
      path: ./data/input.csv
  sink:
    type: file
    config:
      path: ./out/records.jsonl
```

`faucet run` auto-discovers a `faucet.yaml` / `faucet.yml` / `faucet.json` in the
current directory (and a sibling `.env`), so you can also name the file
`faucet.yaml` and just run `faucet run`.

## 3. Validate, then run

```bash
faucet validate pipeline.yaml
faucet run pipeline.yaml
```

```bash
$ cat out/records.jsonl
{"id":"1","name":"Ada","city":"London"}
{"id":"2","name":"Grace","city":"New York"}
{"id":"3","name":"Linus","city":"Helsinki"}
```

To look at the output in another format, swap the sink for the general local
[`file` sink](../cookbook/file-formats.md#the-local-file-sink) — the format
follows the extension, and a file only appears once it is complete:

```yaml
  sink:
    type: file
    config:
      path: ./out/records.csv      # or .json, .xml, .xlsx, .avro, .parquet
```

## 4. Preview without writing

To see what a source emits without touching a sink, use `preview` — it runs the
source and prints records to stdout:

```bash
faucet preview pipeline.yaml --limit 5
```

## 5. Scaffold from a connector's schema

`faucet init` generates a commented config skeleton from any connector's JSON
schema, marking required fields and commenting out optional ones:

```bash
faucet init my_pipeline --source rest --sink postgres
```

## Add a transform

Insert a `transforms:` list between source and sink to reshape records. For
example, normalize keys to `snake_case`:

```yaml
pipeline:
  source: { type: file, config: { path: ./data/input.csv } }
  transforms:
    - { type: keys_case, config: { mode: snake } }
  sink: { type: file, config: { path: ./out/records.jsonl } }
```

Config-exposed transforms include `flatten`, `rename_keys`, `keys_case`,
`select`, `cast`, `redact`, and more — see the
[record transforms recipe](../cookbook/transforms.md) for the full list.

Next: [core concepts](./concepts.md).
