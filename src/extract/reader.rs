use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use serde_json::Value;

use crate::model::ExtractedRecord;

use super::flatten::flatten_document;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    pub lines: usize,
    pub records: usize,
}

pub fn stream_records<F>(path: &Path, mut on_record: F) -> Result<FileStats>
where
    F: FnMut(ExtractedRecord) -> Result<bool>,
{
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut stats = FileStats::default();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line
            .with_context(|| format!("failed to read {} line {}", path.display(), line_number))?;

        if line.trim().is_empty() {
            continue;
        }

        stats.lines += 1;

        let record = parse_ndjson_line(&line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), line_number))?;

        stats.records += 1;

        if !on_record(record)? {
            break;
        }
    }

    Ok(stats)
}

pub fn parse_ndjson_line(line: &str) -> Result<ExtractedRecord> {
    let document: Value = serde_json::from_str(line)?;
    flatten_document(&document)
}
