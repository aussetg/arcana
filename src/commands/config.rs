use std::fs;

use anyhow::{Result, bail};
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct ConfigArgs {
    #[arg(long, help = "Emit machine-readable JSON output")]
    pub json: bool,

    #[command(subcommand)]
    pub command: Option<ConfigCommand>,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Path,
    Init(ConfigInitArgs),
}

#[derive(Debug, Args)]
pub struct ConfigInitArgs {
    #[arg(long, help = "Overwrite an existing config file")]
    pub force: bool,
}

pub fn run(args: ConfigArgs) -> Result<()> {
    match args.command {
        Some(ConfigCommand::Path) => {
            if args.json {
                bail!("--json is not supported with `config path`")
            }
            println!("{}", crate::config::config_file_path()?.display());
            Ok(())
        }
        Some(ConfigCommand::Init(init_args)) => {
            if args.json {
                bail!("--json is not supported with `config init`")
            }
            run_init(init_args)
        }
        None => {
            let config = crate::config::resolve()?;
            if args.json {
                crate::output::config::print_json(&config)
            } else {
                crate::output::config::print_text(&config)
            }
        }
    }
}

fn run_init(args: ConfigInitArgs) -> Result<()> {
    let path = crate::config::config_file_path()?;
    if path.exists() && !args.force {
        bail!(
            "config file already exists: {} (pass --force to overwrite)",
            path.display()
        )
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    crate::config::save(&path, &crate::config::starter()?)?;
    println!("wrote config: {}", path.display());
    Ok(())
}
