use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::ValueEnum;
use serde::Serialize;

use crate::records::RecordSummary;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FilenameMode {
    Original,
    AaId,
    Md5,
    Flat,
}

impl Default for FilenameMode {
    fn default() -> Self {
        Self::Original
    }
}

pub fn destination_path(
    output_dir: &Path,
    record: &RecordSummary,
    filename_mode: FilenameMode,
    output_name: Option<&str>,
) -> Result<PathBuf> {
    let file_name = if let Some(output_name) = output_name {
        sanitize_file_name(output_name)
    } else {
        default_file_name(record, filename_mode)
    };

    if file_name.is_empty() {
        bail!("could not derive a destination filename")
    }

    Ok(output_dir.join(file_name))
}

pub fn sanitize_file_name(input: &str) -> String {
    input
        .chars()
        .map(|character| match character {
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => character,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

fn default_file_name(record: &RecordSummary, filename_mode: FilenameMode) -> String {
    let extension = record
        .extension
        .as_deref()
        .filter(|extension| !extension.trim().is_empty())
        .unwrap_or("bin");

    match filename_mode {
        FilenameMode::Original => {
            if let Some(original_filename) = &record.original_filename {
                sanitize_file_name(original_filename)
            } else {
                sanitize_file_name(&format!("{}.{extension}", record.aa_id))
            }
        }
        FilenameMode::AaId => sanitize_file_name(&format!("{}.{extension}", record.aa_id)),
        FilenameMode::Md5 => sanitize_file_name(&format!(
            "{}.{}",
            record.md5.as_deref().unwrap_or(&record.aa_id),
            extension
        )),
        FilenameMode::Flat => {
            sanitize_file_name(&format!("{}.{}", flat_base_name(record), extension))
        }
    }
}

fn flat_base_name(record: &RecordSummary) -> String {
    let mut parts = Vec::new();

    if let Some(title) = record
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(title.trim().to_string());
    }

    if let Some(author) = record
        .author
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(author.trim().to_string());
    }

    if parts.is_empty() {
        record.aa_id.clone()
    } else {
        parts.join(" -- ")
    }
}
