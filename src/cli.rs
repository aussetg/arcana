use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "arcana",
    version,
    about = "Build a local SQLite search database from Anna's Archive metadata"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Build(crate::commands::build::BuildArgs),
    Search(crate::commands::search::SearchArgs),
    #[command(name = "link-local")]
    LinkLocal(crate::commands::link_local::LinkLocalArgs),
    Download(crate::commands::download::DownloadArgs),
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Build(args) => crate::commands::build::run(args),
        Command::Search(args) => crate::commands::search::run(args),
        Command::LinkLocal(args) => crate::commands::link_local::run(args),
        Command::Download(args) => crate::commands::download::run(args),
    }
}
