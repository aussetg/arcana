use anyhow::{Context, Result, bail};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, params_from_iter};

use crate::model::SearchResult;

use super::query;

pub fn search_keyword(
    conn: &Connection,
    raw_query: &str,
    filters: &query::SearchFilters,
) -> Result<Vec<SearchResult>> {
    let match_query = query::build_match_query(raw_query)?;
    search_keyword_match(conn, &match_query, filters)
}

pub(crate) fn count_keyword_matches(
    conn: &Connection,
    raw_query: &str,
    filters: &query::SearchFilters,
) -> Result<i64> {
    let match_query = query::build_match_query(raw_query)?;
    let (sql, mut params) = query::keyword_count_sql(filters);
    params.insert(0, Value::Text(match_query));

    let count = conn.query_row(&sql, params_from_iter(params), |row| row.get(0))?;
    Ok(count)
}

pub fn search_exact(
    conn: &Connection,
    kinds: &[&'static str],
    value: &str,
    filters: &query::SearchFilters,
) -> Result<Vec<SearchResult>> {
    if value.trim().is_empty() {
        bail!("exact lookup value is empty")
    }

    let (sql, mut params) = query::exact_sql(kinds, filters);
    params[0] = Value::Text(value.trim().to_string());

    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(params), map_search_result)?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to read exact search results")
}

pub fn has_fts_rows(conn: &Connection) -> Result<bool> {
    let count = conn
        .query_row("SELECT COUNT(*) FROM records_fts", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()?
        .unwrap_or(0);
    Ok(count > 0)
}

fn search_keyword_match(
    conn: &Connection,
    match_query: &str,
    filters: &query::SearchFilters,
) -> Result<Vec<SearchResult>> {
    let (sql, mut params) = query::keyword_sql(filters);
    params.insert(0, Value::Text(match_query.to_string()));

    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(params), map_search_result)?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to read keyword search results")
}

fn map_search_result(row: &Row<'_>) -> rusqlite::Result<SearchResult> {
    Ok(SearchResult {
        aa_id: row.get(0)?,
        title: row.get(1)?,
        author: row.get(2)?,
        year: row.get(3)?,
        language: row.get(4)?,
        extension: row.get(5)?,
        publisher: row.get(6)?,
        primary_source: row.get(7)?,
        score_base_rank: row.get(8)?,
        local_path: row.get(9)?,
        bm25_score: row.get(10)?,
    })
}
