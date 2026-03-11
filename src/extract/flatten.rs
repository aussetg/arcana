use std::collections::HashSet;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::model::{ExactCode, ExtractedRecord, FlatRecord};
use crate::{normalize, subjects};

const EXACT_CODE_KINDS: &[&str] = &["md5", "isbn10", "isbn13", "doi", "oclc", "ol", "zlib"];

pub(super) fn flatten_document(document: &Value) -> Result<ExtractedRecord> {
    let source = get_path(document, &["_source"]).context("missing _source")?;
    let search = get_path(source, &["search_only_fields"]);
    let unified = get_path(source, &["file_unified_data"]);

    let aa_id =
        normalize::first_trimmed_string(get_path(source, &["id"])).context("missing _source.id")?;
    let identifiers = get_path(unified.unwrap_or(&Value::Null), &["identifiers_unified"]);

    let record = FlatRecord {
        aa_id,
        md5: first_identifier(identifiers, "md5"),
        isbn13: first_identifier(identifiers, "isbn13"),
        doi: first_identifier(identifiers, "doi"),
        title: preferred_text(search, unified, &["search_title"], &["title_best"]),
        author: preferred_text(search, unified, &["search_author"], &["author_best"]),
        publisher: preferred_text(search, unified, &["search_publisher"], &["publisher_best"]),
        edition_varia: preferred_text(
            search,
            unified,
            &["search_edition_varia"],
            &["edition_varia_best"],
        ),
        subjects: subjects::synthesize(get_path(
            unified.unwrap_or(&Value::Null),
            &["classifications_unified"],
        )),
        description: first_description(search, unified),
        year: normalize::parse_year(get_search_value(search, &["search_year"])),
        language: preferred_text(
            search,
            unified,
            &["search_most_likely_language_code"],
            &["most_likely_language_codes"],
        ),
        extension: preferred_text(search, unified, &["search_extension"], &["extension_best"]),
        content_type: preferred_text(
            search,
            unified,
            &["search_content_type"],
            &["content_type_best"],
        ),
        filesize: first_filesize(search, unified),
        added_date: preferred_text(
            search,
            unified,
            &["search_added_date"],
            &["added_date_best"],
        ),
        primary_source: normalize::first_clean_text(get_search_value(
            search,
            &["search_record_primary_source"],
        )),
        score_base_rank: normalize::parse_i64(get_search_value(
            search,
            &["search_score_base_rank"],
        )),
        cover_url: normalize::first_clean_text(get_unified_value(unified, &["cover_url_best"])),
        original_filename: preferred_text(
            search,
            unified,
            &["search_original_filename"],
            &["original_filename_best"],
        ),
        has_aa_downloads: normalize::parse_bool_flag(get_unified_value(
            unified,
            &["has_aa_downloads"],
        )),
        has_torrent_paths: normalize::parse_bool_flag(get_unified_value(
            unified,
            &["has_torrent_paths"],
        )),
    };

    Ok(ExtractedRecord {
        record,
        codes: extract_codes(identifiers),
    })
}

fn first_description(search: Option<&Value>, unified: Option<&Value>) -> Option<String> {
    normalize::first_present([
        normalize::first_clean_text(get_unified_value(unified, &["stripped_description_best"])),
        normalize::join_parts(
            normalize::cleaned_texts(get_unified_value(
                unified,
                &["stripped_description_additional"],
            )),
            "\n\n",
        ),
        normalize::first_clean_text(get_search_value(search, &["search_description_comments"])),
    ])
}

fn first_filesize(search: Option<&Value>, unified: Option<&Value>) -> Option<i64> {
    normalize::parse_i64(get_search_value(search, &["search_filesize"]))
        .or_else(|| normalize::parse_i64(get_unified_value(unified, &["filesize_best"])))
}

fn preferred_text(
    search: Option<&Value>,
    unified: Option<&Value>,
    search_path: &[&str],
    unified_path: &[&str],
) -> Option<String> {
    normalize::first_present([
        normalize::first_clean_text(get_search_value(search, search_path)),
        normalize::first_clean_text(get_unified_value(unified, unified_path)),
    ])
}

fn get_search_value<'a>(search: Option<&'a Value>, path: &[&str]) -> Option<&'a Value> {
    get_optional_path(search, path)
}

fn get_unified_value<'a>(unified: Option<&'a Value>, path: &[&str]) -> Option<&'a Value> {
    get_optional_path(unified, path)
}

fn get_optional_path<'a>(value: Option<&'a Value>, path: &[&str]) -> Option<&'a Value> {
    get_path(value.unwrap_or(&Value::Null), path)
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
