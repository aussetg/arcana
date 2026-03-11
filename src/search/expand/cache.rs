use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use super::AcceptedExpansion;

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
    let record_count: i64 = conn.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;

    Ok(SearchDbMetadata {
        search_db_path: search_db_path.display().to_string(),
        user_version,
        record_count,
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

pub(crate) fn load_cache_entry(
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

pub(crate) fn store_cache_entry(
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
