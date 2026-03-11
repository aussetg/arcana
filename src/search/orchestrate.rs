use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::model::SearchResult;

use super::{execute, expand, query};

#[derive(Debug, Clone)]
pub struct KeywordSearchOutput {
    pub results: Vec<SearchResult>,
    pub expansion: Option<expand::ExpansionDebugReport>,
}

pub fn search_keyword_with_expansion(
    conn: &Connection,
    search_db_path: &Path,
    raw_query: &str,
    filters: &query::SearchFilters,
    options: &expand::ExpansionOptions,
) -> Result<KeywordSearchOutput> {
    let literal_results = execute::search_keyword(conn, raw_query, filters)?;

    if !options.enabled {
        return Ok(skipped_output(
            literal_results,
            options,
            search_db_path,
            raw_query,
            "query expansion disabled",
        ));
    }

    if literal_results.len() >= filters.limit {
        return Ok(skipped_output(
            literal_results,
            options,
            search_db_path,
            raw_query,
            "literal results already filled the requested limit",
        ));
    }

    let outcome =
        expand::expand_with_shell_provider(conn, search_db_path, raw_query, filters, options);
    let debug = options.debug.then_some(outcome.debug.clone());

    if outcome.accepted_queries.is_empty() {
        return Ok(KeywordSearchOutput {
            results: literal_results,
            expansion: debug,
        });
    }

    let results = merge_expanded_results(conn, literal_results, outcome.accepted_queries, filters)?;

    Ok(KeywordSearchOutput {
        results,
        expansion: debug,
    })
}

fn skipped_output(
    results: Vec<SearchResult>,
    options: &expand::ExpansionOptions,
    search_db_path: &Path,
    raw_query: &str,
    reason: &str,
) -> KeywordSearchOutput {
    KeywordSearchOutput {
        results,
        expansion: options
            .debug
            .then(|| expand::ExpansionDebugReport::skipped(search_db_path, raw_query, reason)),
    }
}

fn merge_expanded_results(
    conn: &Connection,
    literal_results: Vec<SearchResult>,
    expanded_queries: Vec<String>,
    filters: &query::SearchFilters,
) -> Result<Vec<SearchResult>> {
    let mut combined = literal_results;
    let mut seen: HashSet<String> = combined.iter().map(|result| result.aa_id.clone()).collect();
    let remaining = filters.limit.saturating_sub(combined.len());

    if remaining == 0 {
        return Ok(combined);
    }

    let expanded_filters = query::SearchFilters {
        language: filters.language.clone(),
        extension: filters.extension.clone(),
        year: filters.year,
        limit: remaining,
    };

    for expanded_query in expanded_queries {
        for result in execute::search_keyword(conn, &expanded_query, &expanded_filters)? {
            if seen.insert(result.aa_id.clone()) {
                combined.push(result);
                if combined.len() >= filters.limit {
                    return Ok(combined);
                }
            }
        }
    }

    Ok(combined)
}
