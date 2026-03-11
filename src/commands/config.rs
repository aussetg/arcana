use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct ConfigArgs {}

pub fn run(_args: ConfigArgs) -> Result<()> {
    let config = crate::config::resolve()?;
    crate::output::config::print_text(&config)
}
