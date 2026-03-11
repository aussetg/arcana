use anyhow::Result;
use serde::Serialize;

pub const REPORT_VERSION: u32 = 1;

pub fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn eprint_json<T: Serialize>(value: &T) -> Result<()> {
    eprintln!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
