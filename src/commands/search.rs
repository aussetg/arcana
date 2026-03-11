use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;

use crate::output::search::SearchReport;

#[derive(Debug, Args)]
pub struct SearchArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub db: Option<PathBuf>,

    #[arg(value_name = "QUERY")]
    pub query: Option<String>,

    #[arg(long)]
    pub isbn: Option<String>,

    #[arg(long)]
    pub doi: Option<String>,

    #[arg(long)]
    pub md5: Option<String>,

    #[arg(
        long,
        help = "Enable optional local-LLM query expansion for keyword search"
    )]
    pub expand: bool,

    #[arg(long, help = "Show query expansion cache/provider decisions on stderr")]
    pub expand_debug: bool,

    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the expansion sidecar cache SQLite DB"
    )]
    pub expand_cache: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the GGUF model used for query expansion"
    )]
    pub expand_model: Option<PathBuf>,

    #[arg(
        long,
        value_name = "COMMAND",
        help = "Command used to invoke the local expansion provider (defaults to config file or llama-cli)"
    )]
    pub expand_command: Option<String>,

    #[arg(
        long,
        help = "Timeout in seconds for the expansion provider (defaults to config file or 8)"
    )]
    pub expand_timeout: Option<u64>,

    #[arg(long)]
    pub language: Option<String>,

    #[arg(long)]
    pub extension: Option<String>,

    #[arg(long)]
    pub year: Option<i32>,

    #[arg(long, default_value_t = 20)]
    pub limit: usize,
}

pub fn run(args: SearchArgs) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit must be greater than zero");
    }

    let query_count = usize::from(args.query.is_some())
        + usize::from(args.isbn.is_some())
        + usize::from(args.doi.is_some())
        + usize::from(args.md5.is_some());

    if query_count == 0 {
        bail!("provide a keyword query or one exact lookup flag")
    }

    if query_count > 1 {
        bail!("use only one of QUERY, --isbn, --doi, or --md5 at a time")
    }

    let config = crate::config::load()?;
    let db_path = args.db.clone().unwrap_or(config.db_path()?);

    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let filters = crate::search::query::SearchFilters {
        language: args.language,
        extension: args.extension,
        year: args.year,
        limit: args.limit,
    };

    let (results, expansion_debug) = if let Some(query) = args.query {
        if !crate::search::has_fts_rows(&conn)? {
            bail!("database has no populated FTS index; rebuild with `build --input ...`")
        }
        let output = crate::search::search_keyword_with_expansion(
            &conn,
            &db_path,
            &query,
            &filters,
            &crate::search::expand::ExpansionOptions {
                enabled: args.expand,
                debug: args.expand_debug,
                cache_path: args
                    .expand_cache
                    .clone()
                    .or_else(|| config.expand_cache_path()),
                provider_command: args
                    .expand_command
                    .clone()
                    .unwrap_or_else(|| config.expand_command().to_string()),
                model_path: Some(
                    args.expand_model
                        .clone()
                        .unwrap_or_else(|| config.expand_model_path()),
                ),
                timeout_secs: args.expand_timeout.unwrap_or(config.expand_timeout_secs()),
                max_candidates: 4,
            },
        )?;

        (output.results, output.expansion)
    } else if let Some(isbn) = args.isbn {
        (
            crate::search::search_exact(&conn, &["isbn10", "isbn13"], &isbn, &filters)?,
            None,
        )
    } else if let Some(doi) = args.doi {
        (
            crate::search::search_exact(&conn, &["doi"], &doi, &filters)?,
            None,
        )
    } else if let Some(md5) = args.md5 {
        (
            crate::search::search_exact(&conn, &["md5"], &md5, &filters)?,
            None,
        )
    } else {
        bail!("unreachable search mode")
    };

    crate::output::search::print_text(&SearchReport {
        results,
        expansion_debug,
    });

    Ok(())
}
