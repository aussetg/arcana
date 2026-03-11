 # Arcana

 Build a fast local SQLite search database from Anna's Archive derived metadata.

 ## Status

 Early implementation stage.

 Currently implemented:

 - CLI wiring
 - SQLite schema creation
 - DB initialization/finalization pragmas
 - empty DB creation
 - streaming ingest from `aarecords__*.json.gz` into `records` and `record_codes`
 - FTS population and weighted BM25 ranking
 - keyword search
 - exact lookup for ISBN / DOI / MD5
 - optional `--language`, `--extension`, and `--year` filters
 - local file linkage into `records.local_path`
 - optional local-LLM query expansion with sidecar caching and fail-open fallback
 - member fast-download workflow via Anna's Archive API
 - YAML config support via `~/.config/arcana/config.yaml`

 Not implemented yet:

 - additional download-provider strategies beyond the fast-download API

 ## Goal

 Produce one local SQLite artifact optimized for:

 - title / author lookup
 - subject and description search
 - fast local CLI queries
 - simple rebuilds

 This is a personal tool for one Linux machine. It is intentionally not designed as a published product.

 ## Planned stack

 - Rust
 - SQLite + FTS5
 - Anna derived Elasticsearch NDJSON shards (`*.json.gz`) as input

 ## Design summary

 - read the Anna dump directly
 - do not run Elasticsearch for the main pipeline
 - flatten only useful fields into SQLite
 - build a clean FTS index instead of reusing Anna's noisy catch-all `search_text`
 - allow destructive rebuilds whenever the schema improves

 ## Current commands

 Config file:

 ```yaml
 # ~/.config/arcana/config.yaml
 db_path: "~/.config/arcana/arcana.sqlite3"
 download_dir: "~/Downloads"
 secret_key_env: "ANNAS_ARCHIVE_SECRET_KEY"
 ```

 Command-line flags override config values.

 Initialize an empty database:

 ```sh
 cargo run -- build --output data/arcana.sqlite3
 ```

 Ingest a sample subset:

 ```sh
 cargo run -- build \
   --input ~/Datasets/Anna\'s\ Archive/aa_derived_mirror_metadata_20260208/elasticsearch \
   --output data/arcana.sqlite3 \
   --max-shards 1 \
   --max-records 50000
 ```

 Keyword search:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 "large language models"
 ```

 Keyword search as JSON:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 --json "large language models"
 ```

 Exact lookup:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 --isbn 9780131103627
 ```

 Download by ISBN:

 ```sh
 ANNAS_ARCHIVE_SECRET_KEY=... \
 cargo run -- download \
   --db data/arcana.sqlite3 \
   --isbn 9780131103627
 ```

 Show resolved config:

 ```sh
 cargo run -- config
 ```

 Show resolved config as JSON:

 ```sh
 cargo run -- config --json
 ```

 Show config file path:

 ```sh
 cargo run -- config path
 ```

 Initialize a starter config file:

 ```sh
 cargo run -- config init
 ```

 Local file linkage:

 ```sh
 cargo run -- link-local --db data/arcana.sqlite3 --scan ~/Books
 ```

 Verbose dry-run local linkage with real MD5 hashing:

 ```sh
 cargo run -- link-local \
   --db data/arcana.sqlite3 \
   --scan ~/Books \
   --dry-run \
   --verbose \
   --hash-md5
 ```

 Optional expanded search:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 --expand "llm interpretability"
 ```

 Expansion debug:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 --expand --expand-debug "rag evaluation"
 ```

 ## Planned commands

 Full build shape:

 ```sh
 cargo run -- build \
   --input ~/Datasets/Anna\'s\ Archive/aa_derived_mirror_metadata_20260208/elasticsearch \
   --output data/arcana.sqlite3
 ```

 Search shape:

 ```sh
 cargo run -- search --db data/arcana.sqlite3 "large language models"
 ```

 ## Notes

 - backward compatibility is not a goal
 - rebuildability is preferred over migration machinery
 - code should stay small, readable, and fast enough to feel good on a Linux workstation
 - set `ANNAS_ARCHIVE_SECRET_KEY` in your environment before using `download`
 - by default, config lives in `~/.config/arcana/config.yaml`, DB defaults to `~/.config/arcana/arcana.sqlite3`, and downloads default to the XDG Downloads directory
 - the repository now contains ingest, local search, local file linkage, opt-in local-LLM query expansion, config-file support, and fast-download support
