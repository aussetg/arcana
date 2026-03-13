use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

const EXACT_CODE_KINDS: &[&str] = &["md5", "isbn10", "isbn13", "doi", "oclc", "ol", "zlib"];

#[derive(Debug, Deserialize)]
struct SourceDocument {
    _source: SourceRecord,
}

#[derive(Debug, Deserialize)]
struct SourceRecord {
    id: Option<RawValue>,
    search_only_fields: Option<SearchOnlyFields>,
    file_unified_data: Option<FileUnifiedData>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchOnlyFields {
    search_title: Option<RawValue>,
    search_author: Option<RawValue>,
    search_publisher: Option<RawValue>,
    search_edition_varia: Option<RawValue>,
    search_description_comments: Option<RawValue>,
    search_year: Option<RawValue>,
    search_most_likely_language_code: Option<RawValue>,
    search_extension: Option<RawValue>,
    search_content_type: Option<RawValue>,
    search_filesize: Option<RawValue>,
    search_added_date: Option<RawValue>,
    search_record_primary_source: Option<RawValue>,
    search_score_base_rank: Option<RawValue>,
    search_original_filename: Option<RawValue>,
}

#[derive(Debug, Default, Deserialize)]
struct FileUnifiedData {
    identifiers_unified: Option<IdentifiersUnified>,
    title_best: Option<RawValue>,
    author_best: Option<RawValue>,
    publisher_best: Option<RawValue>,
    edition_varia_best: Option<RawValue>,
    classifications_unified: Option<ClassificationsUnified>,
    stripped_description_best: Option<RawValue>,
    stripped_description_additional: Option<RawValue>,
    most_likely_language_codes: Option<RawValue>,
    extension_best: Option<RawValue>,
    content_type_best: Option<RawValue>,
    filesize_best: Option<RawValue>,
    added_date_best: Option<RawValue>,
    cover_url_best: Option<RawValue>,
    original_filename_best: Option<RawValue>,
    has_aa_downloads: Option<RawValue>,
    has_torrent_paths: Option<RawValue>,
}

#[derive(Debug, Default, Deserialize)]
struct IdentifiersUnified {
    md5: Option<RawValue>,
    isbn10: Option<RawValue>,
    isbn13: Option<RawValue>,
    doi: Option<RawValue>,
    oclc: Option<RawValue>,
    ol: Option<RawValue>,
    zlib: Option<RawValue>,
}

impl IdentifiersUnified {
    fn get(&self, kind: &str) -> Option<&RawValue> {
        match kind {
            "md5" => self.md5.as_ref(),
            "isbn10" => self.isbn10.as_ref(),
            "isbn13" => self.isbn13.as_ref(),
            "doi" => self.doi.as_ref(),
            "oclc" => self.oclc.as_ref(),
            "ol" => self.ol.as_ref(),
            "zlib" => self.zlib.as_ref(),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ClassificationsUnified {
    openlib_subject: Option<RawValue>,
    zlib_category_name: Option<RawValue>,
    rgb_subject: Option<RawValue>,
    sciencereading_category: Option<RawValue>,
    edsebk_subject: Option<RawValue>,
    lgrsnf_topic: Option<RawValue>,
    nexusstc_tag: Option<RawValue>,
}

impl ClassificationsUnified {
    fn subject_values(&self) -> [Option<&RawValue>; 7] {
        [
            self.openlib_subject.as_ref(),
            self.zlib_category_name.as_ref(),
            self.rgb_subject.as_ref(),
            self.sciencereading_category.as_ref(),
            self.edsebk_subject.as_ref(),
            self.lgrsnf_topic.as_ref(),
            self.nexusstc_tag.as_ref(),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawValue {
    String(String),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Array(Vec<RawValue>),
}

pub(super) fn parse_document_bytes(bytes: &mut [u8]) -> Result<ExtractedRecord> {
    let document: SourceDocument = simd_json::serde::from_slice(bytes)?;
    flatten_document(document)
}

fn flatten_document(document: SourceDocument) -> Result<ExtractedRecord> {
    let source = document._source;
    let search = source.search_only_fields.as_ref();
    let unified = source.file_unified_data.as_ref();
    let identifiers = unified.and_then(|data| data.identifiers_unified.as_ref());

    let aa_id = first_trimmed_string(source.id.as_ref()).context("missing _source.id")?;

    let record = FlatRecord {
        aa_id,
        md5: first_identifier(identifiers, "md5"),
        isbn13: first_identifier(identifiers, "isbn13"),
        doi: first_identifier(identifiers, "doi"),
        title: preferred_text(
            search.and_then(|value| value.search_title.as_ref()),
            unified.and_then(|value| value.title_best.as_ref()),
        ),
        author: preferred_text(
            search.and_then(|value| value.search_author.as_ref()),
            unified.and_then(|value| value.author_best.as_ref()),
        ),
        publisher: preferred_text(
            search.and_then(|value| value.search_publisher.as_ref()),
            unified.and_then(|value| value.publisher_best.as_ref()),
        ),
        edition_varia: preferred_text(
            search.and_then(|value| value.search_edition_varia.as_ref()),
            unified.and_then(|value| value.edition_varia_best.as_ref()),
        ),
        subjects: synthesize_subjects(
            unified.and_then(|value| value.classifications_unified.as_ref()),
        ),
        description: first_description(search, unified),
        year: parse_year(search.and_then(|value| value.search_year.as_ref())),
        language: preferred_text(
            search.and_then(|value| value.search_most_likely_language_code.as_ref()),
            unified.and_then(|value| value.most_likely_language_codes.as_ref()),
        ),
        extension: preferred_text(
            search.and_then(|value| value.search_extension.as_ref()),
            unified.and_then(|value| value.extension_best.as_ref()),
        ),
        content_type: preferred_text(
            search.and_then(|value| value.search_content_type.as_ref()),
            unified.and_then(|value| value.content_type_best.as_ref()),
        ),
        filesize: first_filesize(search, unified),
        added_date: preferred_text(
            search.and_then(|value| value.search_added_date.as_ref()),
            unified.and_then(|value| value.added_date_best.as_ref()),
        ),
        primary_source: first_clean_text(
            search.and_then(|value| value.search_record_primary_source.as_ref()),
        ),
        score_base_rank: parse_i64(search.and_then(|value| value.search_score_base_rank.as_ref())),
        cover_url: first_clean_text(unified.and_then(|value| value.cover_url_best.as_ref())),
        original_filename: preferred_text(
            search.and_then(|value| value.search_original_filename.as_ref()),
            unified.and_then(|value| value.original_filename_best.as_ref()),
        ),
        has_aa_downloads: parse_bool_flag(
            unified.and_then(|value| value.has_aa_downloads.as_ref()),
        ),
        has_torrent_paths: parse_bool_flag(
            unified.and_then(|value| value.has_torrent_paths.as_ref()),
        ),
    };

    Ok(ExtractedRecord {
        record,
        codes: extract_codes(identifiers),
    })
}

fn first_description(
    search: Option<&SearchOnlyFields>,
    unified: Option<&FileUnifiedData>,
) -> Option<String> {
    first_present([
        first_clean_text(unified.and_then(|value| value.stripped_description_best.as_ref())),
        join_parts(
            cleaned_texts(unified.and_then(|value| value.stripped_description_additional.as_ref())),
            "\n\n",
        ),
        first_clean_text(search.and_then(|value| value.search_description_comments.as_ref())),
    ])
}

fn first_filesize(
    search: Option<&SearchOnlyFields>,
    unified: Option<&FileUnifiedData>,
) -> Option<i64> {
    parse_i64(search.and_then(|value| value.search_filesize.as_ref()))
        .or_else(|| parse_i64(unified.and_then(|value| value.filesize_best.as_ref())))
}

fn preferred_text(search: Option<&RawValue>, unified: Option<&RawValue>) -> Option<String> {
    first_present([first_clean_text(search), first_clean_text(unified)])
}

fn first_identifier(identifiers: Option<&IdentifiersUnified>, kind: &str) -> Option<String> {
    first_trimmed_string(identifiers?.get(kind))
}

fn extract_codes(identifiers: Option<&IdentifiersUnified>) -> Vec<ExactCode> {
    let Some(identifiers) = identifiers else {
        return Vec::new();
    };

    let mut codes = Vec::new();

    for kind in EXACT_CODE_KINDS {
        let Some(value) = identifiers.get(kind) else {
            continue;
        };

        let mut seen = HashSet::new();

        for value in trimmed_strings(Some(value)) {
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

fn synthesize_subjects(classifications: Option<&ClassificationsUnified>) -> Option<String> {
    let classifications = classifications?;

    let mut seen = HashSet::new();
    let mut subjects = Vec::new();

    for values in classifications.subject_values() {
        for subject in cleaned_texts(values) {
            if !looks_human_meaningful(&subject) {
                continue;
            }

            let normalized = subject.to_ascii_lowercase();
            if seen.insert(normalized) {
                subjects.push(subject);
            }
        }
    }

    join_parts(subjects, " ; ")
}

fn looks_human_meaningful(value: &str) -> bool {
    if value.len() > 300 || !value.chars().any(|ch| ch.is_alphabetic()) {
        return false;
    }

    let lower = value.to_ascii_lowercase();

    if lower.contains("://")
        || lower.contains(".torrent")
        || value.starts_with('/')
        || value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with("~/")
        || value.contains('\\')
    {
        return false;
    }

    !looks_like_hash(value)
}

fn looks_like_hash(value: &str) -> bool {
    value.len() >= 16 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn clean_text(input: &str) -> Option<String> {
    let mut out = String::with_capacity(input.len());

    for part in input.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(part);
    }

    if out.is_empty() { None } else { Some(out) }
}

fn trim_to_option(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn cleaned_texts(value: Option<&RawValue>) -> Vec<String> {
    let mut out = Vec::new();
    collect_cleaned_texts(value, &mut out);
    out
}

fn trimmed_strings(value: Option<&RawValue>) -> Vec<String> {
    let mut out = Vec::new();
    collect_trimmed_strings(value, &mut out);
    out
}

fn first_clean_text(value: Option<&RawValue>) -> Option<String> {
    match value? {
        RawValue::String(text) => clean_text(text),
        RawValue::Int(number) => Some(number.to_string()),
        RawValue::UInt(number) => Some(number.to_string()),
        RawValue::Float(number) => Some(number.to_string()),
        RawValue::Bool(flag) => Some(flag.to_string()),
        RawValue::Array(values) => values
            .iter()
            .find_map(|value| first_clean_text(Some(value))),
    }
}

fn first_trimmed_string(value: Option<&RawValue>) -> Option<String> {
    match value? {
        RawValue::String(text) => trim_to_option(text),
        RawValue::Int(number) => Some(number.to_string()),
        RawValue::UInt(number) => Some(number.to_string()),
        RawValue::Float(number) => Some(number.to_string()),
        RawValue::Bool(flag) => Some(flag.to_string()),
        RawValue::Array(values) => values
            .iter()
            .find_map(|value| first_trimmed_string(Some(value))),
    }
}

fn first_present<I>(values: I) -> Option<String>
where
    I: IntoIterator<Item = Option<String>>,
{
    values.into_iter().flatten().next()
}

fn join_parts(parts: Vec<String>, separator: &str) -> Option<String> {
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(separator))
    }
}

fn parse_i64(value: Option<&RawValue>) -> Option<i64> {
    match value? {
        RawValue::Int(number) => Some(*number),
        RawValue::UInt(number) => i64::try_from(*number).ok(),
        RawValue::Float(number) => {
            if number.fract() == 0.0 {
                Some(*number as i64)
            } else {
                None
            }
        }
        RawValue::String(text) => text.trim().parse().ok(),
        RawValue::Bool(flag) => Some(i64::from(*flag)),
        RawValue::Array(values) => values.iter().find_map(|value| parse_i64(Some(value))),
    }
}

fn parse_year(value: Option<&RawValue>) -> Option<i32> {
    let text = first_trimmed_string(value)?;

    if text.len() != 4 || !text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    text.parse().ok()
}

fn parse_bool_flag(value: Option<&RawValue>) -> Option<i64> {
    match value? {
        RawValue::Bool(flag) => Some(i64::from(*flag)),
        RawValue::Int(number) => Some(i64::from(*number != 0)),
        RawValue::UInt(number) => Some(i64::from(*number != 0)),
        RawValue::Float(number) => Some(i64::from(*number != 0.0)),
        RawValue::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Some(1),
            "0" | "false" | "no" => Some(0),
            _ => None,
        },
        RawValue::Array(values) => values.iter().find_map(|value| parse_bool_flag(Some(value))),
    }
}

fn collect_cleaned_texts(value: Option<&RawValue>, out: &mut Vec<String>) {
    let Some(value) = value else {
        return;
    };

    match value {
        RawValue::String(text) => {
            if let Some(text) = clean_text(text) {
                out.push(text);
            }
        }
        RawValue::Int(number) => out.push(number.to_string()),
        RawValue::UInt(number) => out.push(number.to_string()),
        RawValue::Float(number) => out.push(number.to_string()),
        RawValue::Bool(flag) => out.push(flag.to_string()),
        RawValue::Array(values) => {
            for value in values {
                collect_cleaned_texts(Some(value), out);
            }
        }
    }
}

fn collect_trimmed_strings(value: Option<&RawValue>, out: &mut Vec<String>) {
    let Some(value) = value else {
        return;
    };

    match value {
        RawValue::String(text) => {
            if let Some(text) = trim_to_option(text) {
                out.push(text);
            }
        }
        RawValue::Int(number) => out.push(number.to_string()),
        RawValue::UInt(number) => out.push(number.to_string()),
        RawValue::Float(number) => out.push(number.to_string()),
        RawValue::Bool(flag) => out.push(flag.to_string()),
        RawValue::Array(values) => {
            for value in values {
                collect_trimmed_strings(Some(value), out);
            }
        }
    }
}
