use anyhow::{Result, bail};
use clap::Args;

#[derive(Debug, Args)]
pub struct DownloadArgs {}

pub fn run(_args: DownloadArgs) -> Result<()> {
    bail!("download is not implemented yet")
}
