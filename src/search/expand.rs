use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use super::query::SearchFilters;

const DEFAULT_EXPAND_COMMAND: &str = "llama-cli";
const DEFAULT_EXPAND_MODEL: &str = "~/Models/models--unsloth--Qwen3.5-0.8B-GGUF/snapshots/6ab461498e2023f6e3c1baea90a8f0fe38ab64d0/Qwen3.5-0.8B-UD-Q4_K_XL.gguf";
const DEFAULT_TIMEOUT_SECS: u64 = 8;
const PROMPT_VERSION: &str = "v1";

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

#[derive(Debug, Clone)]
pub struct ProviderOutput {
    pub raw_response: String,
    pub candidates: Vec<ExpansionCandidate>,
}

pub trait ExpansionProvider {
    fn provider_name(&self) -> String;
    fn generate(&self, normalized_query: &str, max_candidates: usize) -> Result<ProviderOutput>;
}

#[derive(Debug, Clone)]
pub struct LlamaCliProvider {
    command: String,
    model_path: PathBuf,
    timeout_secs: u64,
}

impl LlamaCliProvider {
    pub fn new(command: String, model_path: PathBuf, timeout_secs: u64) -> Self {
        Self {
            command,
            model_path,
            timeout_secs,
        }
    }
}

impl ExpansionProvider for LlamaCliProvider {
    fn provider_name(&self) -> String {
        format!("{}:{}", self.command, self.model_path.display())
    }

    fn generate(&self, normalized_query: &str, max_candidates: usize) -> Result<ProviderOutput> {
        let model_path = expand_tilde_path(&self.model_path);
        if !model_path.exists() {
            bail!("expansion model not found: {}", model_path.display());
        }

        let prompt = build_prompt(normalized_query, max_candidates);

        let mut child = Command::new(&self.command)
            .arg("-m")
            .arg(&model_path)
            .arg("-p")
            .arg(prompt)
            .arg("-n")
            .arg("256")
            .arg("--temp")
            .arg("0")
            .arg("--seed")
            .arg("42")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {}", self.command))?;

        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                let output = child.wait_with_output()?;
                if !output.status.success() {
                    bail!(
                        "{} exited with status {}: {}",
                        self.command,
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }

                let stdout = String::from_utf8(output.stdout)
                    .context("expansion provider output was not valid UTF-8")?;
                let parsed = parse_provider_json(&stdout)?;

                return Ok(ProviderOutput {
                    raw_response: stdout,
                    candidates: parsed.expansions,
                });
            }

            if started.elapsed() >= Duration::from_secs(self.timeout_secs) {
                let _ = child.kill();
                let _ = child.wait_with_output();
                bail!("{} timed out after {}s", self.command, self.timeout_secs);
            }

            thread::sleep(Duration::from_millis(25));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderJson {
    expansions: Vec<ExpansionCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachePayload {
    accepted: Vec<AcceptedExpansion>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    accepted: Vec<AcceptedExpansion>,
}

#[derive(Debug, Clone)]
struct SearchDbMetadata {
    search_db_path: String,
    user_version: i32,
    record_count: i64,
}

pub fn default_cache_path(search_db_path: &Path) -> PathBuf {
    let parent = search_db_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = search_db_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("arcana");

    parent.join(format!("{stem}.expand-cache.sqlite3"))
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

    let metadata = match load_search_db_metadata(conn, search_db_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            debug.fallback_reason = Some(format!("failed to inspect search DB: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    };

    let cache_conn = match open_cache_connection(&cache_path) {
        Ok(conn) => conn,
        Err(error) => {
            debug.fallback_reason = Some(format!("failed to open expansion cache: {error:#}"));
            return ExpansionOutcome {
                accepted_queries: Vec::new(),
                debug,
            };
        }
    };

    match load_cache_entry(&cache_conn, &query_norm, &metadata) {
        Ok(Some(entry)) => {
            debug.cache_hit = true;
            debug.accepted = entry.accepted.clone();
            let _ = touch_cache_entry(&cache_conn, &query_norm, &metadata);
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

    if let Err(error) = store_cache_entry(
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

fn build_prompt(normalized_query: &str, max_candidates: usize) -> String {
    format!(
        "You generate lexical search query variants for a local metadata search engine.\n\
Return JSON only. No markdown. No prose.\n\
Schema: {{\"expansions\":[{{\"text\":\"...\",\"kind\":\"...\",\"confidence\":0.0}}]}}\n\
Rules:\n\
- Output at most {max_candidates} expansions.\n\
- Each expansion must be a full alternative query for the whole input query.\n\
- Prefer close lexical variants likely to appear literally in titles, subjects, descriptions, or publisher metadata.\n\
- Do not output broad topic neighbors or brainstorming terms.\n\
- Keep intent tight.\n\
- If no safe expansion exists, return {{\"expansions\":[]}}.\n\
Input query: {normalized_query:?}\n"
    )
}

fn parse_provider_json(raw: &str) -> Result<ProviderJson> {
    if let Ok(parsed) = serde_json::from_str::<ProviderJson>(raw.trim()) {
        return Ok(parsed);
    }

    let trimmed = raw.trim();
    let start = trimmed
        .find('{')
        .context("provider returned no JSON object")?;
    let end = trimmed
        .rfind('}')
        .context("provider returned no JSON object")?;
    let object = &trimmed[start..=end];
    Ok(serde_json::from_str(object)?)
}

fn normalize_query(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn load_search_db_metadata(conn: &Connection, search_db_path: &Path) -> Result<SearchDbMetadata> {
    let user_version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let record_count: i64 = conn.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;

    Ok(SearchDbMetadata {
        search_db_path: search_db_path.display().to_string(),
        user_version,
        record_count,
    })
}

fn open_cache_connection(path: &Path) -> Result<Connection> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let conn =
        Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS query_expansion_cache (
            query_norm TEXT PRIMARY KEY,
            search_db_path TEXT NOT NULL,
            search_db_user_version INTEGER NOT NULL,
            search_db_record_count INTEGER,
            model_name TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            raw_response TEXT NOT NULL,
            expansions_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            use_count INTEGER NOT NULL DEFAULT 1
        );",
    )?;
    Ok(conn)
}

fn load_cache_entry(
    cache_conn: &Connection,
    query_norm: &str,
    metadata: &SearchDbMetadata,
) -> Result<Option<CacheEntry>> {
    let row = cache_conn
        .query_row(
            "SELECT raw_response, expansions_json, search_db_path, search_db_user_version, search_db_record_count
             FROM query_expansion_cache
             WHERE query_norm = ?1",
            [query_norm],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        )
        .optional()?;

    let Some((
        raw_response,
        expansions_json,
        cached_path,
        cached_user_version,
        cached_record_count,
    )) = row
    else {
        return Ok(None);
    };

    if cached_path != metadata.search_db_path
        || cached_user_version != metadata.user_version
        || cached_record_count != Some(metadata.record_count)
    {
        return Ok(None);
    }

    let payload: CachePayload = serde_json::from_str(&expansions_json)?;

    let _ = raw_response;

    Ok(Some(CacheEntry {
        accepted: payload.accepted,
    }))
}

fn touch_cache_entry(
    cache_conn: &Connection,
    query_norm: &str,
    metadata: &SearchDbMetadata,
) -> Result<()> {
    cache_conn.execute(
        "UPDATE query_expansion_cache
         SET last_used_at = ?2,
             use_count = use_count + 1,
             search_db_path = ?3,
             search_db_user_version = ?4,
             search_db_record_count = ?5
         WHERE query_norm = ?1",
        params![
            query_norm,
            now_string(),
            metadata.search_db_path,
            metadata.user_version,
            metadata.record_count,
        ],
    )?;
    Ok(())
}

fn store_cache_entry(
    cache_conn: &Connection,
    query_norm: &str,
    metadata: &SearchDbMetadata,
    model_name: &str,
    raw_response: String,
    accepted: &[AcceptedExpansion],
) -> Result<()> {
    let payload = serde_json::to_string(&CachePayload {
        accepted: accepted.to_vec(),
    })?;
    let now = now_string();

    cache_conn.execute(
        "INSERT INTO query_expansion_cache (
            query_norm,
            search_db_path,
            search_db_user_version,
            search_db_record_count,
            model_name,
            prompt_version,
            raw_response,
            expansions_json,
            created_at,
            last_used_at,
            use_count
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 1)
        ON CONFLICT(query_norm) DO UPDATE SET
            search_db_path = excluded.search_db_path,
            search_db_user_version = excluded.search_db_user_version,
            search_db_record_count = excluded.search_db_record_count,
            model_name = excluded.model_name,
            prompt_version = excluded.prompt_version,
            raw_response = excluded.raw_response,
            expansions_json = excluded.expansions_json,
            last_used_at = excluded.last_used_at,
            use_count = query_expansion_cache.use_count + 1",
        params![
            query_norm,
            metadata.search_db_path,
            metadata.user_version,
            metadata.record_count,
            model_name,
            PROMPT_VERSION,
            raw_response,
            payload,
            now,
            now,
        ],
    )?;

    Ok(())
}

fn now_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn expand_tilde_path(path: &Path) -> PathBuf {
    let path_text = path.to_string_lossy();
    if !path_text.starts_with("~/") {
        return path.to_path_buf();
    }

    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(&path_text[2..]),
        None => path.to_path_buf(),
    }
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
        AcceptedExpansion, ExpansionCandidate, ExpansionOptions, ExpansionProvider,
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
        ) -> Result<super::ProviderOutput> {
            *self.call_count.lock().unwrap() += 1;

            if self.fail {
                anyhow::bail!("mock failure");
            }

            Ok(super::ProviderOutput {
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
