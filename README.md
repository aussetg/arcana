# Arcana

Arcana builds a local SQLite + FTS5 search database from Anna's Archive derived metadata.

It is a personal, rebuild-friendly CLI for one Linux machine: ingest shards, search them quickly, link your local files, and optionally download matching files through Anna's fast-download API.

## Status

Usable, but still evolving. Rebuilds are preferred over migration compatibility.

Implemented today:

- streaming ingest from `aarecords__*.json.gz`
- compact SQLite schema with FTS5 search
- keyword search with weighted ranking
- exact lookup by ISBN / DOI / MD5
- optional filters: `--language`, `--extension`, `--year`
- local file linkage into `records.local_path`
- optional local-LLM query expansion with cache + fail-open behavior
- fast-download workflow via Anna's Archive member API
- YAML config at `~/.config/arcana/config.yaml`
- text and structured JSON output for automation

## What Arcana is for

- title / author lookup
- subject / description search
- fast local CLI queries
- simple destructive rebuilds when the schema improves

## Install

```sh
cargo build --release
```

Then use either:

- `cargo run -- ...`
- or `target/release/arcana ...`

## Input data

Arcana reads Anna's Archive derived Elasticsearch shards directly:

- a directory containing `aarecords__*.json.gz`
- or a single `aarecords__*.json.gz` file

It does **not** require Elasticsearch for the local build pipeline.

## Quick start

Initialize a config file:

```sh
cargo run -- config init
```

Build a small test database:

```sh
cargo run -- build \
  --input ~/Datasets/Anna\'s\ Archive/aa_derived_mirror_metadata_20260208/elasticsearch \
  --output data/arcana.sqlite3 \
  --max-shards 1 \
  --max-records 50000
```

Search it:

```sh
cargo run -- search --db data/arcana.sqlite3 "large language models"
```

Link local files:

```sh
cargo run -- link-local --db data/arcana.sqlite3 --scan ~/Books
```

Download by ISBN:

```sh
ANNAS_ARCHIVE_SECRET_KEY=... \
cargo run -- download --db data/arcana.sqlite3 --isbn 9780131103627
```

## Configuration

Default config path:

```text
~/.config/arcana/config.yaml
```

Typical config:

```yaml
db_path: "~/.config/arcana/arcana.sqlite3"
download_dir: "~/Downloads"
secret_key_env: "ANNAS_ARCHIVE_SECRET_KEY"
fast_download_api_url: "https://annas-archive.gl/dyn/api/fast_download.json"

# Optional local query expansion
expand_cache_path: "~/.config/arcana/arcana.expand.sqlite3"
expand_command: "llama-cli"
expand_model_path: "~/Models/your-model.gguf"
expand_timeout_secs: 8
```

Notes:

- command-line flags override config values
- if no config exists, Arcana falls back to sensible defaults
- downloads use the XDG Downloads directory when available

Useful config commands:

```sh
cargo run -- config
cargo run -- config path
cargo run -- config --json
cargo run -- config init --force
```

## Commands

### `build`

Create a database, optionally ingesting shards immediately.

Initialize an empty database:

```sh
cargo run -- build --output data/arcana.sqlite3
```

Build from shards:

```sh
cargo run -- build \
  --input ~/path/to/elasticsearch \
  --output data/arcana.sqlite3
```

Rebuild in place:

```sh
cargo run -- build \
  --input ~/path/to/elasticsearch \
  --output data/arcana.sqlite3 \
  --replace
```

Show ingest phase timings:

```sh
cargo run -- build \
  --input ~/path/to/elasticsearch \
  --output data/arcana.sqlite3 \
  --replace \
  --timings
```

### `search`

Keyword search:

```sh
cargo run -- search --db data/arcana.sqlite3 "transformer interpretability"
```

Exact lookup:

```sh
cargo run -- search --db data/arcana.sqlite3 --isbn 9780131103627
```

Filtered search:

```sh
cargo run -- search \
  --db data/arcana.sqlite3 \
  --language en \
  --extension pdf \
  --year 2024 \
  "rag evaluation"
```

JSON output:

```sh
cargo run -- search --db data/arcana.sqlite3 --json "large language models"
```

Optional local query expansion:

```sh
cargo run -- search --db data/arcana.sqlite3 --expand "llm interpretability"
```

Debug expansion behavior:

```sh
cargo run -- search --db data/arcana.sqlite3 --expand --expand-debug "rag evaluation"
```

### `link-local`

Scan a directory tree and link matching local files into the database.

```sh
cargo run -- link-local --db data/arcana.sqlite3 --scan ~/Books
```

Dry-run with verbose decisions and content hashing:

```sh
cargo run -- link-local \
  --db data/arcana.sqlite3 \
  --scan ~/Books \
  --dry-run \
  --verbose \
  --hash-md5
```

### `download`

Download a matching record by `--aa-id`, `--md5`, `--isbn`, or `--doi`.

```sh
ANNAS_ARCHIVE_SECRET_KEY=... \
cargo run -- download \
  --db data/arcana.sqlite3 \
  --isbn 9780131103627 \
  --verify-md5
```

Use a flat output name and JSON output:

```sh
ANNAS_ARCHIVE_SECRET_KEY=... \
cargo run -- download \
  --db data/arcana.sqlite3 \
  --isbn 9780131103627 \
  --filename-mode flat \
  --json
```

Notes:

- interrupted downloads resume from `.part` files when the server supports range requests
- `--verify-md5` validates the downloaded file when MD5 metadata is available
- by default, successful downloads update `records.local_path`

## JSON output

`search`, `config`, `link-local`, and `download` support `--json`.

JSON reports are versioned and intended to be automation-friendly. In JSON mode, command failures are emitted as structured error reports on stderr.

## Design notes

- reads the Anna dump directly
- keeps only useful flattened fields in SQLite
- builds a cleaner FTS index instead of reusing noisy source search text
- favors small, readable code over heavy infrastructure
- treats rebuildability as more important than backward compatibility

## Limitations

- early-stage project; schema and output may still change
- no migration story by design
- download support is currently centered on Anna's fast-download API