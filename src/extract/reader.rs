use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flate2::read::GzDecoder;

use crate::model::ExtractedRecord;

use super::flatten::parse_document_bytes;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    pub lines: usize,
    pub records: usize,
    pub read_elapsed: Duration,
    pub parse_elapsed: Duration,
}

pub fn stream_records<F>(path: &Path, mut on_record: F) -> Result<FileStats>
where
    F: FnMut(ExtractedRecord) -> Result<bool>,
{
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let decoder = GzDecoder::new(file);
    let mut reader = BufReader::new(decoder);

    let mut stats = FileStats::default();
    let mut line = Vec::new();
    let mut physical_line_number = 0usize;

    loop {
        line.clear();

        let read_started_at = Instant::now();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("failed to read {}", path.display()))?;
        stats.read_elapsed += read_started_at.elapsed();

        if bytes_read == 0 {
            break;
        }

        physical_line_number += 1;
        let Some(trimmed) = trim_ascii_whitespace_mut(&mut line) else {
            continue;
        };

        stats.lines += 1;

        let parse_started_at = Instant::now();
        let record = parse_ndjson_bytes(trimmed).with_context(|| {
            format!(
                "failed to parse {} line {}",
                path.display(),
                physical_line_number
            )
        })?;
        stats.parse_elapsed += parse_started_at.elapsed();

        stats.records += 1;

        if !on_record(record)? {
            break;
        }
    }

    Ok(stats)
}

pub fn parse_ndjson_line(line: &str) -> Result<ExtractedRecord> {
    let mut bytes = line.as_bytes().to_vec();
    let trimmed = trim_ascii_whitespace_mut(&mut bytes).context("line is empty")?;
    parse_ndjson_bytes(trimmed)
}

fn parse_ndjson_bytes(bytes: &mut [u8]) -> Result<ExtractedRecord> {
    parse_document_bytes(bytes)
}

fn trim_ascii_whitespace_mut(bytes: &mut Vec<u8>) -> Option<&mut [u8]> {
    let start = bytes.iter().position(|byte| !byte.is_ascii_whitespace())?;
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)?;
    Some(&mut bytes[start..end])
}
