use anyhow::{Result, bail};
use rusqlite::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchFilters {
    pub language: Option<String>,
    pub extension: Option<String>,
    pub year: Option<i32>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchMode {
    Keyword {
        match_query: String,
    },
    Exact {
        kinds: Vec<&'static str>,
        value: String,
    },
}

pub fn build_match_query(query: &str) -> Result<String> {
    let mut tokens = Vec::new();
    let mut chars = query.chars().peekable();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        if ch == '"' {
            chars.next();
            let mut phrase = String::new();

            for ch in chars.by_ref() {
                if ch == '"' {
                    break;
                }
                phrase.push(ch);
            }

            if !phrase.trim().is_empty() {
                tokens.push(quote_fts_term(phrase.trim()));
            }

            continue;
        }

        let mut term = String::new();
        while let Some(ch) = chars.peek().copied() {
            if ch.is_whitespace() || ch == '"' {
                break;
            }
            term.push(ch);
            chars.next();
        }

        if !term.trim().is_empty() {
            tokens.push(quote_fts_term(term.trim()));
        }
    }

    if tokens.is_empty() {
        bail!("query is empty")
    }

    Ok(tokens.join(" AND "))
}

pub fn keyword_sql(filters: &SearchFilters) -> (String, Vec<Value>) {
    let mut sql = String::from(
        "SELECT \
            r.aa_id, \
            r.title, \
            r.author, \
            r.year, \
            r.language, \
            r.extension, \
            r.publisher, \
            r.primary_source, \
            r.score_base_rank, \
            r.local_path, \
            bm25(records_fts, 10.0, 8.0, 5.0, 1.5, 1.0, 0.5) AS bm25_score \
        FROM records_fts \
        JOIN records r ON r.rid = records_fts.rowid \
        WHERE records_fts MATCH ?",
    );

    let mut params = Vec::new();
    push_filter_clauses(&mut sql, &mut params, filters);
    sql.push_str(
        " ORDER BY bm25_score ASC, COALESCE(r.score_base_rank, 0) DESC, r.rid DESC LIMIT ?",
    );
    params.push(Value::Integer(filters.limit as i64));
    (sql, params)
}

pub fn keyword_count_sql(filters: &SearchFilters) -> (String, Vec<Value>) {
    let mut sql = String::from(
        "SELECT COUNT(*) \
        FROM records_fts \
        JOIN records r ON r.rid = records_fts.rowid \
        WHERE records_fts MATCH ?",
    );

    let mut params = Vec::new();
    push_filter_clauses(&mut sql, &mut params, filters);
    (sql, params)
}

pub fn exact_sql(kinds: &[&str], filters: &SearchFilters) -> (String, Vec<Value>) {
    let mut sql = String::from(
        "SELECT \
            r.aa_id, \
            r.title, \
            r.author, \
            r.year, \
            r.language, \
            r.extension, \
            r.publisher, \
            r.primary_source, \
            r.score_base_rank, \
            r.local_path, \
            NULL AS bm25_score \
        FROM records r \
        WHERE EXISTS (\
            SELECT 1 FROM record_codes rc \
            WHERE rc.rid = r.rid \
              AND lower(rc.value) = lower(?)",
    );

    let mut params = Vec::new();
    if kinds.len() == 1 {
        sql.push_str(" AND rc.kind = ?");
        params.push(Value::Text(kinds[0].to_string()));
    } else {
        sql.push_str(" AND rc.kind IN (");
        for (index, _kind) in kinds.iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');
        for kind in kinds {
            params.push(Value::Text((*kind).to_string()));
        }
    }
    sql.push_str(")");

    let mut final_params = vec![Value::Null];
    final_params.append(&mut params);
    push_filter_clauses(&mut sql, &mut final_params, filters);
    sql.push_str(" ORDER BY COALESCE(r.score_base_rank, 0) DESC, r.rid DESC LIMIT ?");
    final_params.push(Value::Integer(filters.limit as i64));
    (sql, final_params)
}

fn push_filter_clauses(sql: &mut String, params: &mut Vec<Value>, filters: &SearchFilters) {
    if let Some(language) = &filters.language {
        sql.push_str(" AND lower(r.language) = lower(?)");
        params.push(Value::Text(language.clone()));
    }

    if let Some(extension) = &filters.extension {
        sql.push_str(" AND lower(r.extension) = lower(?)");
        params.push(Value::Text(extension.clone()));
    }

    if let Some(year) = filters.year {
        sql.push_str(" AND r.year = ?");
        params.push(Value::Integer(year as i64));
    }
}

fn quote_fts_term(term: &str) -> String {
    format!("\"{}\"", term.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::{SearchFilters, build_match_query, exact_sql, keyword_count_sql, keyword_sql};

    #[test]
    fn builds_match_query_with_terms_and_phrases() {
        assert_eq!(
            build_match_query("llm \"large language model\" rag").unwrap(),
            "\"llm\" AND \"large language model\" AND \"rag\""
        );
    }

    #[test]
    fn builds_keyword_sql_with_filters() {
        let filters = SearchFilters {
            language: Some("en".into()),
            extension: Some("pdf".into()),
            year: Some(2024),
            limit: 20,
        };

        let (sql, params) = keyword_sql(&filters);

        assert!(sql.contains("records_fts MATCH ?"));
        assert!(sql.contains("lower(r.language) = lower(?)"));
        assert!(sql.contains("lower(r.extension) = lower(?)"));
        assert!(sql.contains("r.year = ?"));
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn builds_exact_sql_for_multiple_kinds() {
        let filters = SearchFilters {
            language: None,
            extension: None,
            year: None,
            limit: 10,
        };

        let (sql, params) = exact_sql(&["isbn10", "isbn13"], &filters);

        assert!(sql.contains("lower(rc.value) = lower(?)"));
        assert!(sql.contains("rc.kind IN (?, ?)"));
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn builds_keyword_count_sql_with_filters() {
        let filters = SearchFilters {
            language: Some("en".into()),
            extension: Some("pdf".into()),
            year: Some(2024),
            limit: 20,
        };

        let (sql, params) = keyword_count_sql(&filters);

        assert!(sql.contains("SELECT COUNT(*)"));
        assert!(sql.contains("records_fts MATCH ?"));
        assert!(sql.contains("lower(r.language) = lower(?)"));
        assert!(sql.contains("lower(r.extension) = lower(?)"));
        assert!(sql.contains("r.year = ?"));
        assert_eq!(params.len(), 3);
    }
}
