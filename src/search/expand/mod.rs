use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use super::query::SearchFilters;

mod cache;
mod provider;

pub use cache::default_cache_path;
pub use provider::{ExpansionProvider, LlamaCliProvider, ProviderOutput};

const DEFAULT_EXPAND_COMMAND: &str = "llama-cli";
const DEFAULT_EXPAND_MODEL: &str = "~/Models/models--unsloth--Qwen3.5-0.8B-GGUF/snapshots/6ab461498e2023f6e3c1baea90a8f0fe38ab64d0/Qwen3.5-0.8B-UD-Q4_K_XL.gguf";
const DEFAULT_TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone)]
pub struct ExpansionOptions {
    pub enabled: bool,
    pub debug: bool,
    pub cache_path: Option<PathBuf>,
    pub provider_command: String,
    pub model_path: Option<PathBuf>,
    pub timeout_secs: u64,
    pub max_candidates: usize,
}

impl Default for ExpansionOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            debug: false,
            cache_path: None,
            provider_command: DEFAULT_EXPAND_COMMAND.to_string(),
            model_path: Some(PathBuf::from(DEFAULT_EXPAND_MODEL)),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            max_candidates: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExpansionDebugReport {
    pub attempted: bool,
    pub cache_path: PathBuf,
    pub cache_hit: bool,
    pub provider: Option<String>,
    pub query_norm: String,
    pub accepted: Vec<AcceptedExpansion>,
    pub rejected: Vec<RejectedExpansion>,
    pub fallback_reason: Option<String>,
}

impl ExpansionDebugReport {
    pub fn skipped(search_db_path: &Path, query: &str, reason: &str) -> Self {
        Self {
            attempted: false,
            cache_path: default_cache_path(search_db_path),
            cache_hit: false,
            provider: None,
            query_norm: normalize_query(query),
            accepted: Vec::new(),
            rejected: Vec::new(),
            fallback_reason: Some(reason.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AcceptedExpansion {
    pub text: String,
    pub kind: Option<String>,
    pub confidence: Option<f64>,
    pub corpus_hits: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RejectedExpansion {
    pub text: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpansionOutcome {
    pub accepted_queries: Vec<String>,
    pub debug: ExpansionDebugReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpansionCandidate {
    pub text: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
}

pub fn expand_with_provider<P: ExpansionProvider>(
    conn: &Connection,
    search_db_path: &Path,
    raw_query: &str,
    filters: &SearchFilters,
    options: &ExpansionOptions,
    provider: &P,
) -> ExpansionOutcome {
    let cache_path = options
        .cache_path
        .clone()
        .unwrap_or_else(|| default_cache_path(search_db_path));
    let query_norm = normalize_query(raw_query);

    let mut debug = ExpansionDebugReport {
        attempted: true,
        cache_path: cache_path.clone(),
        cache_hit: false,
        provider: Some(provider.provider_name()),
        query_norm: query_norm.clone(),
        accepted: Vec::new(),
        rejected: Vec::new(),
        fallback_reason: None,
    };

    if let Some(reason) = should_skip_expansion(&query_norm, options) {
        debug.attempted = false;
        debug.fallback_reason = Some(reason.to_string());
        return ExpansionOutcome {
            accepted_queries: Vec::new(),
            debug,
        };
    }

    let metadata = match cache::load_search_db_metadata(conn, search_db_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            debug.fallback_reason = Some(format!("failed to inspect search DB: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    };

    let cache_conn = match cache::open_cache_connection(&cache_path) {
        Ok(conn) => conn,
        Err(error) => {
            debug.fallback_reason = Some(format!("failed to open expansion cache: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    };

    match cache::load_cache_entry(&cache_conn, &query_norm, &metadata) {
        Ok(Some(entry)) => {
            debug.cache_hit = true;
            debug.accepted = entry.accepted.clone();
            let _ = cache::touch_cache_entry(&cache_conn, &query_norm, &metadata);
            return ExpansionOutcome {
                accepted_queries: entry.accepted.into_iter().map(|item| item.text).collect(),
                debug,
            };
        }
        Ok(None) => {}
        Err(error) => {
            debug.fallback_reason = Some(format!("failed to read expansion cache: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    }

    let literal_hits = super::count_keyword_matches(conn, raw_query, filters).unwrap_or_default();

    let provider_output = match provider.generate(&query_norm, options.max_candidates) {
        Ok(output) => output,
        Err(error) => {
            debug.fallback_reason = Some(format!("expansion provider failed: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    };

    let (accepted, rejected) = validate_candidates(
        conn,
        raw_query,
        filters,
        literal_hits,
        provider_output.candidates,
        options.max_candidates,
    );

    debug.accepted = accepted.clone();
    debug.rejected = rejected;

    if let Err(error) = cache::store_cache_entry(
        &cache_conn,
        &query_norm,
        &metadata,
        &provider.provider_name(),
        provider_output.raw_response,
        &accepted,
    ) {
        debug.fallback_reason = Some(format!("failed to store expansion cache: {error:#}"));
    }

    ExpansionOutcome {
        accepted_queries: accepted.into_iter().map(|item| item.text).collect(),
        debug,
    }
}

pub fn expand_with_shell_provider(
    conn: &Connection,
    search_db_path: &Path,
    raw_query: &str,
    filters: &SearchFilters,
    options: &ExpansionOptions,
) -> ExpansionOutcome {
    let provider = LlamaCliProvider::new(
        options.provider_command.clone(),
        options
            .model_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_EXPAND_MODEL)),
        options.timeout_secs,
    );

    expand_with_provider(conn, search_db_path, raw_query, filters, options, &provider)
}

fn validate_candidates(
    conn: &Connection,
    raw_query: &str,
    filters: &SearchFilters,
    literal_hits: i64,
    candidates: Vec<ExpansionCandidate>,
    max_candidates: usize,
) -> (Vec<AcceptedExpansion>, Vec<RejectedExpansion>) {
    let query_norm = normalize_query(raw_query);
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    let mut seen = HashSet::new();

    for candidate in candidates {
        let candidate_norm = normalize_query(&candidate.text);

        if candidate_norm.is_empty() {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: "empty candidate".into(),
            });
            continue;
        }

        if candidate_norm == query_norm {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: "same as literal query".into(),
            });
            continue;
        }

        if !looks_expandable_query(&candidate_norm) {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: "not a safe lexical query variant".into(),
            });
            continue;
        }

        if !seen.insert(candidate_norm.clone()) {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: "duplicate candidate".into(),
            });
            continue;
        }

        let corpus_hits = match super::count_keyword_matches(conn, &candidate_norm, filters) {
            Ok(hits) => hits,
            Err(error) => {
                rejected.push(RejectedExpansion {
                    text: candidate.text,
                    reason: format!("invalid FTS candidate: {error:#}"),
                });
                continue;
            }
        };

        if corpus_hits == 0 {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: "zero corpus hits".into(),
            });
            continue;
        }

        if corpus_hits > broad_hit_limit(literal_hits) {
            rejected.push(RejectedExpansion {
                text: candidate.text,
                reason: format!("too broad with {corpus_hits} corpus hits"),
            });
            continue;
        }

        accepted.push(AcceptedExpansion {
            text: candidate_norm,
            kind: candidate.kind,
            confidence: candidate.confidence,
            corpus_hits,
        });

        if accepted.len() >= max_candidates {
            break;
        }
    }

    (accepted, rejected)
}

fn broad_hit_limit(literal_hits: i64) -> i64 {
    (literal_hits.max(1) * 20).max(2_000)
}

fn should_skip_expansion(query_norm: &str, options: &ExpansionOptions) -> Option<&'static str> {
    if !options.enabled {
        return Some("query expansion disabled");
    }

    if options.max_candidates == 0 {
        return Some("no expansion candidates allowed");
    }

    if query_norm.is_empty() {
        return Some("query is empty after normalization");
    }

    if !looks_expandable_query(query_norm) {
        return Some("query does not look suitable for expansion");
    }

    let term_count = query_norm.split_whitespace().count();
    if term_count > 8 || query_norm.len() > 120 {
        return Some("query already looks specific enough; skipping expansion");
    }

    None
}

fn looks_expandable_query(query: &str) -> bool {
    !query.contains('/')
        && !query.contains('\\')
        && !looks_like_hash(query)
        && query
            .chars()
            .any(|character| character.is_ascii_alphabetic())
}

fn looks_like_hash(query: &str) -> bool {
    let compact: String = query
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    compact.len() >= 16
        && compact
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

pub(crate) fn normalize_query(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::Result;
    use rusqlite::Connection;

    use crate::model::{ExtractedRecord, FlatRecord};

    use super::{
        AcceptedExpansion, ExpansionCandidate, ExpansionOptions, ExpansionProvider, ProviderOutput,
        expand_with_provider, normalize_query,
    };

    #[derive(Clone)]
    struct MockProvider {
        call_count: Arc<Mutex<usize>>,
        candidates: Vec<ExpansionCandidate>,
        fail: bool,
    }

    impl ExpansionProvider for MockProvider {
        fn provider_name(&self) -> String {
            "mock-provider".into()
        }

        fn generate(
            &self,
            _normalized_query: &str,
            _max_candidates: usize,
        ) -> Result<ProviderOutput> {
            *self.call_count.lock().unwrap() += 1;

            if self.fail {
                anyhow::bail!("mock failure");
            }

            Ok(ProviderOutput {
                raw_response: "{\"expansions\":[]}".into(),
                candidates: self.candidates.clone(),
            })
        }
    }

    fn unique_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("arcana-{name}-{nonce}.sqlite3"))
    }

    fn sample_record(aa_id: &str, title: &str) -> ExtractedRecord {
        ExtractedRecord {
            record: FlatRecord {
                aa_id: aa_id.into(),
                md5: None,
                isbn13: None,
                doi: None,
                title: Some(title.into()),
                author: Some("Author".into()),
                publisher: Some("Publisher".into()),
                edition_varia: None,
                subjects: Some("Machine learning".into()),
                description: Some("About language models".into()),
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
            codes: Vec::new(),
        }
    }

    #[test]
    fn normalizes_queries() {
        assert_eq!(
            normalize_query("  LLM   interpretability  "),
            "llm interpretability"
        );
    }

    #[test]
    fn caches_validated_expansions() {
        let search_db_path = unique_path("search-db");
        let cache_path = unique_path("expand-cache");
        let mut conn = Connection::open(&search_db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![sample_record(
            "aa-1",
            "Large Language Model Interpretability",
        )];
        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
        crate::db::populate_fts(&conn).unwrap();

        let calls = Arc::new(Mutex::new(0usize));
        let provider = MockProvider {
            call_count: calls.clone(),
            candidates: vec![ExpansionCandidate {
                text: "large language model interpretability".into(),
                kind: Some("abbreviation_expansion".into()),
                confidence: Some(0.95),
            }],
            fail: false,
        };

        let options = ExpansionOptions {
            enabled: true,
            debug: true,
            cache_path: Some(cache_path.clone()),
            provider_command: "mock".into(),
            model_path: None,
            timeout_secs: 1,
            max_candidates: 4,
        };

        let filters = crate::search::query::SearchFilters {
            language: Some("en".into()),
            extension: Some("pdf".into()),
            year: Some(2024),
            limit: 10,
        };

        let first = expand_with_provider(
            &conn,
            &search_db_path,
            "llm interpretability",
            &filters,
            &options,
            &provider,
        );
        assert!(!first.debug.cache_hit);
        assert_eq!(
            first.debug.accepted,
            vec![AcceptedExpansion {
                text: "large language model interpretability".into(),
                kind: Some("abbreviation_expansion".into()),
                confidence: Some(0.95),
                corpus_hits: 1,
            }]
        );

        let second = expand_with_provider(
            &conn,
            &search_db_path,
            "llm interpretability",
            &filters,
            &options,
            &provider,
        );
        assert!(second.debug.cache_hit);
        assert_eq!(
            second.accepted_queries,
            vec!["large language model interpretability".to_string()]
        );
        assert_eq!(*calls.lock().unwrap(), 1);

        let _ = std::fs::remove_file(search_db_path);
        let _ = std::fs::remove_file(cache_path);
    }

    #[test]
    fn falls_open_when_provider_fails() {
        let search_db_path = unique_path("search-db-fail");
        let cache_path = unique_path("expand-cache-fail");
        let conn = Connection::open(&search_db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let provider = MockProvider {
            call_count: Arc::new(Mutex::new(0)),
            candidates: Vec::new(),
            fail: true,
        };

        let options = ExpansionOptions {
            enabled: true,
            debug: true,
            cache_path: Some(cache_path.clone()),
            provider_command: "mock".into(),
            model_path: None,
            timeout_secs: 1,
            max_candidates: 4,
        };
        let filters = crate::search::query::SearchFilters {
            language: None,
            extension: None,
            year: None,
            limit: 10,
        };

        let outcome = expand_with_provider(
            &conn,
            Path::new(&search_db_path),
            "llm",
            &filters,
            &options,
            &provider,
        );
        assert!(outcome.accepted_queries.is_empty());
        assert!(
            outcome
                .debug
                .fallback_reason
                .unwrap()
                .contains("expansion provider failed")
        );

        let _ = std::fs::remove_file(search_db_path);
        let _ = std::fs::remove_file(cache_path);
    }
}
