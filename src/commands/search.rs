use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;

#[derive(Debug, Args)]
pub struct SearchArgs {
    #[arg(long, value_name = "PATH", help = "Path to the SQLite database")]
    pub db: PathBuf,

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
        default_value = "llama-cli",
        value_name = "COMMAND",
        help = "Command used to invoke the local expansion provider"
    )]
    pub expand_command: String,

    #[arg(
        long,
        default_value_t = 8,
        help = "Timeout in seconds for the expansion provider"
    )]
    pub expand_timeout: u64,

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

    let conn = Connection::open(&args.db)
        .with_context(|| format!("failed to open {}", args.db.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let filters = crate::search::query::SearchFilters {
        language: args.language,
        extension: args.extension,
        year: args.year,
        limit: args.limit,
    };

    let results = if let Some(query) = args.query {
        if !crate::search::has_fts_rows(&conn)? {
            bail!("database has no populated FTS index; rebuild with `build --input ...`")
        }
        let output = crate::search::search_keyword_with_expansion(
            &conn,
            &args.db,
            &query,
            &filters,
            &crate::search::expand::ExpansionOptions {
                enabled: args.expand,
                debug: args.expand_debug,
                cache_path: args.expand_cache.clone(),
                provider_command: args.expand_command.clone(),
                model_path: args.expand_model.clone(),
                timeout_secs: args.expand_timeout,
                max_candidates: 4,
            },
        )?;

        if let Some(report) = output.expansion.as_ref() {
            print_expansion_debug(report);
        }

        output.results
    } else if let Some(isbn) = args.isbn {
        crate::search::search_exact(&conn, &["isbn10", "isbn13"], &isbn, &filters)?
    } else if let Some(doi) = args.doi {
        crate::search::search_exact(&conn, &["doi"], &doi, &filters)?
    } else if let Some(md5) = args.md5 {
        crate::search::search_exact(&conn, &["md5"], &md5, &filters)?
    } else {
        bail!("unreachable search mode")
    };

    if results.is_empty() {
        println!("no results");
        return Ok(());
    }

    for (index, result) in results.iter().enumerate() {
        print_result(index + 1, result);
    }

    Ok(())
}

fn print_expansion_debug(report: &crate::search::expand::ExpansionDebugReport) {
    eprintln!("[expand] query: {}", report.query_norm);
    eprintln!("[expand] attempted: {}", report.attempted);
    eprintln!("[expand] cache: {}", report.cache_path.display());
    eprintln!("[expand] cache-hit: {}", report.cache_hit);

    if let Some(provider) = &report.provider {
        eprintln!("[expand] provider: {provider}");
    }

    for accepted in &report.accepted {
        eprintln!(
            "[expand] accepted: {:?} kind={:?} confidence={:?} hits={}",
            accepted.text, accepted.kind, accepted.confidence, accepted.corpus_hits
        );
    }

    for rejected in &report.rejected {
        eprintln!(
            "[expand] rejected: {:?} ({})",
            rejected.text, rejected.reason
        );
    }

    if let Some(reason) = &report.fallback_reason {
        eprintln!("[expand] fallback: {reason}");
    }
}

fn print_result(index: usize, result: &crate::model::SearchResult) {
    println!(
        "{index}. {}",
        result.title.as_deref().unwrap_or("(untitled)")
    );

    let mut meta = Vec::new();
    if let Some(author) = &result.author {
        meta.push(author.clone());
    }
    if let Some(year) = result.year {
        meta.push(year.to_string());
    }
    if let Some(language) = &result.language {
        meta.push(language.clone());
    }
    if let Some(extension) = &result.extension {
        meta.push(extension.clone());
    }
    if !meta.is_empty() {
        println!("   {}", meta.join(" · "));
    }

    println!("   aa_id: {}", result.aa_id);

    if let Some(source) = &result.primary_source {
        println!("   source: {source}");
    }

    if let Some(rank) = result.score_base_rank {
        println!("   base-rank: {rank}");
    }

    if let Some(score) = result.bm25_score {
        println!("   bm25: {score:.6}");
    }

    if let Some(path) = &result.local_path {
        println!("   local: {path}");
    }

    println!();
}
