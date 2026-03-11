use std::fmt;

use anyhow::{Error, Result};
use serde::Serialize;

#[derive(Debug)]
pub struct AlreadyReportedError;

impl fmt::Display for AlreadyReportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("error already reported")
    }
}

impl std::error::Error for AlreadyReportedError {}

#[derive(Debug, Serialize)]
struct ErrorReport {
    report_version: u32,
    kind: &'static str,
    command: String,
    message: String,
}

pub fn print_json(command: &str, error: &Error) -> anyhow::Result<()> {
    let report = ErrorReport {
        report_version: 1,
        kind: "error_report",
        command: command.to_string(),
        message: format!("{error:#}"),
    };
    eprintln!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

pub fn already_reported() -> Error {
    AlreadyReportedError.into()
}

pub fn run_json<F>(command: &str, json: bool, f: F) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    match f() {
        Ok(()) => Ok(()),
        Err(error) if json => {
            print_json(command, &error)?;
            Err(already_reported())
        }
        Err(error) => Err(error),
    }
}
