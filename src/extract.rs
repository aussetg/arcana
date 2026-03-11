use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use serde_json::Value;

use crate::model::{ExactCode, ExtractedRecord, FlatRecord};
use crate::{normalize, subjects};

const EXACT_CODE_KINDS: &[&str] = &["md5", "isbn10", "isbn13", "doi", "oclc", "ol", "zlib"];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    pub lines: usize,
    pub records: usize,
}

pub fn discover_input_shards(input: &Path, max_shards: Option<usize>) -> Result<Vec<PathBuf>> {
    let mut paths = if input.is_file() {
        vec![input.to_path_buf()]
    } else if input.is_dir() {
        let mut paths = Vec::new();

        for entry in std::fs::read_dir(input)
            .with_context(|| format!("failed to read {}", input.display()))?
        {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if file_name.starts_with("aarecords__") && file_name.ends_with(".json.gz") {
                paths.push(path);
            }
        }

        paths.sort_by_key(|path| shard_sort_key(path));
        paths
    } else {
        bail!("input path does not exist: {}", input.display());
    };

    if let Some(limit) = max_shards {
        paths.truncate(limit);
    }

    Ok(paths)
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

fn flatten_document(document: &Value) -> Result<ExtractedRecord> {
    let source = get_path(document, &["_source"]).context("missing _source")?;
    let search = get_path(source, &["search_only_fields"]);
    let unified = get_path(source, &["file_unified_data"]);

    let aa_id =
        normalize::first_trimmed_string(get_path(source, &["id"])).context("missing _source.id")?;

    let identifiers = get_path(unified.unwrap_or(&Value::Null), &["identifiers_unified"]);
    let md5 = first_identifier(identifiers, "md5");
    let isbn13 = first_identifier(identifiers, "isbn13");
    let doi = first_identifier(identifiers, "doi");

    let description = normalize::first_present([
        normalize::first_clean_text(get_path(
            unified.unwrap_or(&Value::Null),
            &["stripped_description_best"],
        )),
        normalize::join_parts(
            normalize::cleaned_texts(get_path(
                unified.unwrap_or(&Value::Null),
                &["stripped_description_additional"],
            )),
            "\n\n",
        ),
        normalize::first_clean_text(get_path(
            search.unwrap_or(&Value::Null),
            &["search_description_comments"],
        )),
    ]);

    let record = FlatRecord {
        aa_id,
        md5,
        isbn13,
        doi,
        title: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_title"],
            )),
            normalize::first_clean_text(get_path(unified.unwrap_or(&Value::Null), &["title_best"])),
        ]),
        author: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_author"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["author_best"],
            )),
        ]),
        publisher: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_publisher"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["publisher_best"],
            )),
        ]),
        edition_varia: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_edition_varia"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["edition_varia_best"],
            )),
        ]),
        subjects: subjects::synthesize(get_path(
            unified.unwrap_or(&Value::Null),
            &["classifications_unified"],
        )),
        description,
        year: normalize::parse_year(get_path(search.unwrap_or(&Value::Null), &["search_year"])),
        language: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_most_likely_language_code"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["most_likely_language_codes"],
            )),
        ]),
        extension: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_extension"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["extension_best"],
            )),
        ]),
        content_type: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_content_type"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["content_type_best"],
            )),
        ]),
        filesize: normalize::parse_i64(get_path(
            search.unwrap_or(&Value::Null),
            &["search_filesize"],
        ))
        .or_else(|| {
            normalize::parse_i64(get_path(
                unified.unwrap_or(&Value::Null),
                &["filesize_best"],
            ))
        }),
        added_date: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_added_date"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["added_date_best"],
            )),
        ]),
        primary_source: normalize::first_clean_text(get_path(
            search.unwrap_or(&Value::Null),
            &["search_record_primary_source"],
        )),
        score_base_rank: normalize::parse_i64(get_path(
            search.unwrap_or(&Value::Null),
            &["search_score_base_rank"],
        )),
        cover_url: normalize::first_clean_text(get_path(
            unified.unwrap_or(&Value::Null),
            &["cover_url_best"],
        )),
        original_filename: normalize::first_present([
            normalize::first_clean_text(get_path(
                search.unwrap_or(&Value::Null),
                &["search_original_filename"],
            )),
            normalize::first_clean_text(get_path(
                unified.unwrap_or(&Value::Null),
                &["original_filename_best"],
            )),
        ]),
        has_aa_downloads: normalize::parse_bool_flag(get_path(
            unified.unwrap_or(&Value::Null),
            &["has_aa_downloads"],
        )),
        has_torrent_paths: normalize::parse_bool_flag(get_path(
            unified.unwrap_or(&Value::Null),
            &["has_torrent_paths"],
        )),
    };

    Ok(ExtractedRecord {
        record,
        codes: extract_codes(identifiers),
    })
}

fn first_identifier(identifiers: Option<&Value>, kind: &str) -> Option<String> {
    let identifiers = identifiers?.as_object()?;
    normalize::first_trimmed_string(identifiers.get(kind))
}

fn extract_codes(identifiers: Option<&Value>) -> Vec<ExactCode> {
    let Some(identifiers) = identifiers.and_then(Value::as_object) else {
        return Vec::new();
    };

    let mut codes = Vec::new();

    for kind in EXACT_CODE_KINDS {
        let Some(value) = identifiers.get(*kind) else {
            continue;
        };

        let mut seen = HashSet::new();

        for value in normalize::trimmed_strings(Some(value)) {
            if seen.insert(value.clone()) {
                codes.push(ExactCode {
                    kind: (*kind).to_string(),
                    value,
                });
            }
        }
    }

    codes
}

fn get_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;

    for part in path {
        current = current.get(*part)?;
    }

    Some(current)
}

fn shard_sort_key(path: &Path) -> (u32, String) {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();

    let shard_number = file_name
        .strip_prefix("aarecords__")
        .and_then(|value| value.strip_suffix(".json.gz"))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(u32::MAX);

    (shard_number, file_name)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::flatten_document;

    #[test]
    fn flattens_selected_fields_and_codes() {
        let document = json!({
            "_source": {
                "id": "aa-record-1",
                "search_only_fields": {
                    "search_title": "  Search Title  ",
                    "search_author": "Search Author",
                    "search_year": "2024",
                    "search_most_likely_language_code": ["en"],
                    "search_extension": "pdf",
                    "search_content_type": "book_nonfiction",
                    "search_filesize": "12345",
                    "search_added_date": "2024-05-01",
                    "search_record_primary_source": "libgen",
                    "search_score_base_rank": 77,
                    "search_original_filename": "example.pdf"
                },
                "file_unified_data": {
                    "publisher_best": "Publisher",
                    "edition_varia_best": "Second edition",
                    "stripped_description_additional": [" First paragraph ", "Second paragraph"],
                    "cover_url_best": "https://example.invalid/cover.jpg",
                    "has_aa_downloads": true,
                    "has_torrent_paths": false,
                    "identifiers_unified": {
                        "md5": ["abcdef0123456789abcdef0123456789"],
                        "isbn13": ["9780131103627", "9780131103627"],
                        "doi": ["10.1000/example"],
                        "zlib": ["12345", "12345"],
                        "ignored": ["skip-me"]
                    },
                    "classifications_unified": {
                        "openlib_subject": [" Machine learning ", "NLP"],
                        "rgb_subject": ["Artificial Intelligence", "machine learning"],
                        "zlib_category_name": ["/srv/path/nope"]
                    }
                }
            }
        });

        let extracted = flatten_document(&document).unwrap();

        assert_eq!(extracted.record.aa_id, "aa-record-1");
        assert_eq!(extracted.record.title.as_deref(), Some("Search Title"));
        assert_eq!(extracted.record.publisher.as_deref(), Some("Publisher"));
        assert_eq!(
            extracted.record.description.as_deref(),
            Some("First paragraph\n\nSecond paragraph")
        );
        assert_eq!(
            extracted.record.subjects.as_deref(),
            Some("Machine learning ; NLP ; Artificial Intelligence")
        );
        assert_eq!(extracted.record.year, Some(2024));
        assert_eq!(extracted.record.filesize, Some(12345));
        assert_eq!(extracted.record.has_aa_downloads, Some(1));
        assert_eq!(extracted.record.has_torrent_paths, Some(0));

        let codes: Vec<(String, String)> = extracted
            .codes
            .into_iter()
            .map(|code| (code.kind, code.value))
            .collect();

        assert_eq!(
            codes,
            vec![
                (
                    "md5".to_string(),
                    "abcdef0123456789abcdef0123456789".to_string()
                ),
                ("isbn13".to_string(), "9780131103627".to_string()),
                ("doi".to_string(), "10.1000/example".to_string()),
                ("zlib".to_string(), "12345".to_string()),
            ]
        );
    }
}
