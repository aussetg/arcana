use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::search::query::SearchFilters;

use super::AcceptedExpansion;

const CACHE_USER_VERSION: i32 = 4;
const PROMPT_VERSION: &str = "v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachePayload {
    accepted: Vec<AcceptedExpansion>,
}

#[derive(Debug, Clone)]
pub(crate) struct CacheEntry {
    pub accepted: Vec<AcceptedExpansion>,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchDbMetadata {
    pub search_db_path: String,
    pub user_version: i32,
    pub record_count: i64,
    pub aa_id_length_sum: i64,
    pub title_length_sum: i64,
    pub author_length_sum: i64,
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

pub(crate) fn load_search_db_metadata(
    conn: &Connection,
    search_db_path: &Path,
) -> Result<SearchDbMetadata> {
    let user_version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let (record_count, aa_id_length_sum, title_length_sum, author_length_sum) = conn.query_row(
        "SELECT COUNT(*),
                COALESCE(SUM(length(aa_id)), 0),
                COALESCE(SUM(length(coalesce(title, ''))), 0),
                COALESCE(SUM(length(coalesce(author, ''))), 0)
         FROM records",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    Ok(SearchDbMetadata {
        search_db_path: search_db_path.display().to_string(),
        user_version,
        record_count,
        aa_id_length_sum,
        title_length_sum,
        author_length_sum,
    })
}

pub(crate) fn open_cache_connection(path: &Path) -> Result<Connection> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let conn =
        Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    ensure_cache_schema(&conn)?;
    Ok(conn)
}

pub(crate) fn load_cache_entry(
    cache_conn: &Connection,
    query_norm: &str,
    metadata: &SearchDbMetadata,
    provider_name: &str,
    filters: &SearchFilters,
) -> Result<Option<CacheEntry>> {
    let filters_json = serialize_filters(filters)?;
    let row = cache_conn
        .query_row(
            "SELECT expansions_json,
                    search_db_path,
                    search_db_user_version,
                    search_db_record_count,
                    model_name,
                    prompt_version,
                    filters_json,
                    search_db_aa_id_length_sum,
                    search_db_title_length_sum,
                    search_db_author_length_sum
             FROM query_expansion_cache
             WHERE query_norm = ?1",
            [query_norm],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .optional()?;

    let Some((
        expansions_json,
        cached_path,
        cached_user_version,
        cached_record_count,
        cached_provider_name,
        cached_prompt_version,
        cached_filters_json,
        cached_aa_id_length_sum,
        cached_title_length_sum,
        cached_author_length_sum,
    )) = row
    else {
        return Ok(None);
    };

    if cached_path != metadata.search_db_path
        || cached_user_version != metadata.user_version
        || cached_record_count != metadata.record_count
        || cached_provider_name != provider_name
        || cached_prompt_version != PROMPT_VERSION
        || cached_filters_json != filters_json
        || cached_aa_id_length_sum != metadata.aa_id_length_sum
        || cached_title_length_sum != metadata.title_length_sum
        || cached_author_length_sum != metadata.author_length_sum
    {
        return Ok(None);
    }

    let payload: CachePayload = serde_json::from_str(&expansions_json)?;

    Ok(Some(CacheEntry {
        accepted: payload.accepted,
    }))
}

pub(crate) fn touch_cache_entry(
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
             search_db_record_count = ?5,
             search_db_aa_id_length_sum = ?6,
             search_db_title_length_sum = ?7,
             search_db_author_length_sum = ?8
         WHERE query_norm = ?1",
        params![
            query_norm,
            now_string(),
            metadata.search_db_path,
            metadata.user_version,
            metadata.record_count,
            metadata.aa_id_length_sum,
            metadata.title_length_sum,
            metadata.author_length_sum,
        ],
    )?;
    Ok(())
}

pub(crate) fn store_cache_entry(
    cache_conn: &Connection,
    query_norm: &str,
    metadata: &SearchDbMetadata,
    model_name: &str,
    raw_response: String,
    filters: &SearchFilters,
    accepted: &[AcceptedExpansion],
) -> Result<()> {
    let filters_json = serialize_filters(filters)?;
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
            search_db_aa_id_length_sum,
            search_db_title_length_sum,
            search_db_author_length_sum,
            model_name,
            prompt_version,
            filters_json,
            raw_response,
            expansions_json,
            created_at,
            last_used_at,
            use_count
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 1)
        ON CONFLICT(query_norm) DO UPDATE SET
            search_db_path = excluded.search_db_path,
            search_db_user_version = excluded.search_db_user_version,
            search_db_record_count = excluded.search_db_record_count,
            search_db_aa_id_length_sum = excluded.search_db_aa_id_length_sum,
            search_db_title_length_sum = excluded.search_db_title_length_sum,
            search_db_author_length_sum = excluded.search_db_author_length_sum,
            model_name = excluded.model_name,
            prompt_version = excluded.prompt_version,
            filters_json = excluded.filters_json,
            raw_response = excluded.raw_response,
            expansions_json = excluded.expansions_json,
            last_used_at = excluded.last_used_at,
            use_count = query_expansion_cache.use_count + 1",
        params![
            query_norm,
            metadata.search_db_path,
            metadata.user_version,
            metadata.record_count,
            metadata.aa_id_length_sum,
            metadata.title_length_sum,
            metadata.author_length_sum,
            model_name,
            PROMPT_VERSION,
            filters_json,
            raw_response,
            payload,
            now,
            now,
        ],
    )?;

    Ok(())
}

fn ensure_cache_schema(conn: &Connection) -> Result<()> {
    let user_version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;

    if user_version != CACHE_USER_VERSION {
        conn.execute_batch("DROP TABLE IF EXISTS query_expansion_cache;")?;
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS query_expansion_cache (
            query_norm TEXT PRIMARY KEY,
            search_db_path TEXT NOT NULL,
            search_db_user_version INTEGER NOT NULL,
            search_db_record_count INTEGER NOT NULL,
            search_db_aa_id_length_sum INTEGER NOT NULL,
            search_db_title_length_sum INTEGER NOT NULL,
            search_db_author_length_sum INTEGER NOT NULL,
            model_name TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            filters_json TEXT NOT NULL,
            raw_response TEXT NOT NULL,
            expansions_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            use_count INTEGER NOT NULL DEFAULT 1
        );",
    )?;
    conn.pragma_update(None, "user_version", CACHE_USER_VERSION)?;
    Ok(())
}

fn serialize_filters(filters: &SearchFilters) -> Result<String> {
    #[derive(Serialize)]
    struct CacheFilters<'a> {
        language: Option<&'a str>,
        extension: Option<&'a str>,
        year: Option<i32>,
    }

    serde_json::to_string(&CacheFilters {
        language: filters.language.as_deref(),
        extension: filters.extension.as_deref(),
        year: filters.year,
    })
    .context("failed to serialize search filters for expansion cache")
}

fn now_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}
