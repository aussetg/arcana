use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

use super::{
    has_fts_rows, query::SearchFilters, search_exact, search_keyword, search_keyword_with_expansion,
};

fn sample_record(
    aa_id: &str,
    title: &str,
    author: &str,
    subjects: &str,
    codes: Vec<ExactCode>,
) -> ExtractedRecord {
    ExtractedRecord {
        record: FlatRecord {
            aa_id: aa_id.to_string(),
            md5: None,
            isbn13: None,
            doi: None,
            title: Some(title.to_string()),
            author: Some(author.to_string()),
            publisher: Some("Publisher".into()),
            edition_varia: None,
            subjects: Some(subjects.to_string()),
            description: Some("A book about transformers and retrieval".into()),
            year: Some(2024),
            language: Some("en".into()),
            extension: Some("pdf".into()),
            content_type: Some("book_nonfiction".into()),
            filesize: None,
            added_date: None,
            primary_source: Some("libgen".into()),
            score_base_rank: Some(10),
            cover_url: None,
            original_filename: None,
            has_aa_downloads: Some(1),
            has_torrent_paths: Some(0),
        },
        codes,
    }
}

#[test]
fn searches_fts_and_exact_codes() {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::db::prepare_database(&conn).unwrap();

    let mut batch = vec![
        sample_record(
            "aa-1",
            "Large Language Models",
            "A. Researcher",
            "Machine learning ; NLP",
            vec![ExactCode {
                kind: "isbn13".into(),
                value: "9780131103627".into(),
            }],
        ),
        sample_record(
            "aa-2",
            "Databases Internals",
            "B. Hacker",
            "Databases",
            vec![ExactCode {
                kind: "doi".into(),
                value: "10.1000/example".into(),
            }],
        ),
    ];

    crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
    crate::db::populate_fts(&conn).unwrap();

    assert!(has_fts_rows(&conn).unwrap());

    let filters = SearchFilters {
        language: Some("en".into()),
        extension: Some("pdf".into()),
        year: Some(2024),
        limit: 10,
    };

    let keyword_results = search_keyword(&conn, "language models", &filters).unwrap();
    assert_eq!(keyword_results.len(), 1);
    assert_eq!(keyword_results[0].aa_id, "aa-1");
    assert!(keyword_results[0].bm25_score.is_some());

    let exact_results =
        search_exact(&conn, &["isbn10", "isbn13"], "9780131103627", &filters).unwrap();
    assert_eq!(exact_results.len(), 1);
    assert_eq!(exact_results[0].aa_id, "aa-1");
    assert!(exact_results[0].bm25_score.is_none());
}

#[test]
fn appends_expanded_only_hits_after_literal_hits() {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::db::prepare_database(&conn).unwrap();

    let mut batch = vec![
        sample_record(
            "aa-1",
            "LLM Research",
            "A. Researcher",
            "Machine learning",
            Vec::new(),
        ),
        sample_record(
            "aa-2",
            "Large Language Model Research",
            "B. Researcher",
            "Machine learning",
            Vec::new(),
        ),
    ];

    crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
    crate::db::populate_fts(&conn).unwrap();

    let filters = SearchFilters {
        language: Some("en".into()),
        extension: Some("pdf".into()),
        year: Some(2024),
        limit: 10,
    };

    let search_db_path = std::env::temp_dir().join("arcana-search-expansion-test.sqlite3");
    let cache_path = std::env::temp_dir().join("arcana-search-expansion-cache.sqlite3");
    let options = crate::search::expand::ExpansionOptions {
        enabled: true,
        debug: true,
        cache_path: Some(cache_path.clone()),
        provider_command: "unused".into(),
        model_path: Some(Path::new("/nonexistent").to_path_buf()),
        timeout_secs: 1,
        max_candidates: 4,
    };

    struct TestProvider;

    impl crate::search::expand::ExpansionProvider for TestProvider {
        fn provider_name(&self) -> String {
            "test-provider".into()
        }

        fn generate(
            &self,
            _normalized_query: &str,
            _max_candidates: usize,
        ) -> Result<crate::search::expand::ProviderOutput> {
            Ok(crate::search::expand::ProviderOutput {
                raw_response: "{\"expansions\":[]}".into(),
                candidates: vec![crate::search::expand::ExpansionCandidate {
                    text: "large language model research".into(),
                    kind: Some("abbreviation_expansion".into()),
                    confidence: Some(0.9),
                }],
            })
        }
    }

    let outcome = crate::search::expand::expand_with_provider(
        &conn,
        &search_db_path,
        "llm research",
        &filters,
        &options,
        &TestProvider,
    );
    assert_eq!(
        outcome.accepted_queries,
        vec!["large language model research"]
    );

    let keyword = search_keyword(&conn, "llm research", &filters).unwrap();
    assert_eq!(keyword.len(), 1);
    assert_eq!(keyword[0].aa_id, "aa-1");

    let search_out = search_keyword_with_expansion(
        &conn,
        &search_db_path,
        "llm research",
        &filters,
        &crate::search::expand::ExpansionOptions {
            enabled: false,
            debug: true,
            ..crate::search::expand::ExpansionOptions::default()
        },
    )
    .unwrap();
    assert_eq!(search_out.results.len(), 1);

    let _ = std::fs::remove_file(cache_path);
}
