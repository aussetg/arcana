use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;

use crate::link_local::{LinkOptions, discover_local_files, link_local_files};

#[derive(Debug, Args)]
pub struct LinkLocalArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub db: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory tree to scan for local files"
    )]
    pub scan: PathBuf,

    #[arg(long, help = "Replace existing records.local_path values")]
    pub replace_existing: bool,

    #[arg(long, help = "Scan and report matches without updating the database")]
    pub dry_run: bool,

    #[arg(long, help = "Print per-file matching decisions to stderr")]
    pub verbose: bool,

    #[arg(
        long,
        help = "Hash local file contents and try real MD5 matching before filename heuristics"
    )]
    pub hash_md5: bool,

    #[arg(long)]
    pub max_files: Option<usize>,

    #[arg(long, help = "Emit a machine-readable JSON report")]
    pub json: bool,
}

pub fn run(args: LinkLocalArgs) -> Result<()> {
    let json = args.json;
    crate::output::error::run_json("link_local", json, || run_inner(args))
}

fn run_inner(args: LinkLocalArgs) -> Result<()> {
    if !args.scan.is_dir() {
        bail!("scan path is not a directory: {}", args.scan.display());
    }

    let config = crate::config::resolve()?;
    let db_path = args.db.clone().unwrap_or(config.db_path());

    let mut conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let files = discover_local_files(&args.scan, args.max_files)?;
    let report = link_local_files(
        &mut conn,
        &files,
        LinkOptions {
            replace_existing: args.replace_existing,
            dry_run: args.dry_run,
            verbose: args.verbose,
            hash_md5: args.hash_md5,
        },
    )?;

    if args.json {
        crate::output::link_local::print_json(&report)?;
    } else {
        crate::output::link_local::print_summary(&report);
    }
    Ok(())
}
#[cfg(test)]
mod tests;
