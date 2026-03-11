use serde_json::json;

use super::flatten::flatten_document;

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
