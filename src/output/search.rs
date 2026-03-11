use crate::model::SearchResult;
use crate::search::expand::ExpansionDebugReport;
use crate::search::query::SearchFilters;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SearchReport {
    pub report_version: u32,
    pub kind: &'static str,
    pub request: SearchRequest,
    pub result_count: usize,
    pub results: Vec<SearchResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expansion: Option<ExpansionDebugReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchRequest {
    pub mode: SearchQueryMode,
    pub value: String,
    pub filters: SearchReportFilters,
    pub expand_requested: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchQueryMode {
    Keyword,
    Isbn,
    Doi,
    Md5,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchReportFilters {
    pub language: Option<String>,
    pub extension: Option<String>,
    pub year: Option<i32>,
    pub limit: usize,
}

impl SearchReport {
    pub fn new(
        mode: SearchQueryMode,
        query: impl Into<String>,
        filters: &SearchFilters,
        expand_requested: bool,
        results: Vec<SearchResult>,
        expansion: Option<ExpansionDebugReport>,
    ) -> Self {
        Self {
            report_version: 1,
            kind: "search_report",
            request: SearchRequest {
                mode,
                value: query.into(),
                filters: SearchReportFilters {
                    language: filters.language.clone(),
                    extension: filters.extension.clone(),
                    year: filters.year,
                    limit: filters.limit,
                },
                expand_requested,
            },
            result_count: results.len(),
            results,
            expansion,
        }
    }
}

pub fn print_text(report: &SearchReport) {
    if let Some(expansion_debug) = report.expansion.as_ref() {
        print_expansion_debug(expansion_debug);
    }

    if report.results.is_empty() {
        println!("no results");
        return;
    }

    for (index, result) in report.results.iter().enumerate() {
        print_result(index + 1, result);
    }
}

pub fn print_json(report: &SearchReport) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(report)?);
    Ok(())
}

fn print_expansion_debug(report: &ExpansionDebugReport) {
    eprintln!("[expand] query: {}", report.query_norm);
    eprintln!("[expand] attempted: {}", report.attempted);
    eprintln!("[expand] cache: {}", report.cache_path.display());
    eprintln!("[expand] cache-hit: {}", report.cache_hit);

    if let Some(provider) = &report.provider {
        eprintln!("[expand] provider: {provider}");
    }

    for accepted in &report.accepted {
        eprintln!(
            "[expand] accepted: {:?} kind={:?} confidence={:?} hits={}",
            accepted.text, accepted.kind, accepted.confidence, accepted.corpus_hits
        );
    }

    for rejected in &report.rejected {
        eprintln!(
            "[expand] rejected: {:?} ({})",
            rejected.text, rejected.reason
        );
    }

    if let Some(reason) = &report.fallback_reason {
        eprintln!("[expand] fallback: {reason}");
    }
}

fn print_result(index: usize, result: &SearchResult) {
    println!(
        "{index}. {}",
        result.title.as_deref().unwrap_or("(untitled)")
    );

    let mut meta = Vec::new();
    if let Some(author) = &result.author {
        meta.push(author.clone());
    }
    if let Some(year) = result.year {
        meta.push(year.to_string());
    }
    if let Some(language) = &result.language {
        meta.push(language.clone());
    }
    if let Some(extension) = &result.extension {
        meta.push(extension.clone());
    }
    if !meta.is_empty() {
        println!("   {}", meta.join(" · "));
    }

    println!("   aa_id: {}", result.aa_id);

    if let Some(source) = &result.primary_source {
        println!("   source: {source}");
    }

    if let Some(rank) = result.score_base_rank {
        println!("   base-rank: {rank}");
    }

    if let Some(score) = result.bm25_score {
        println!("   bm25: {score:.6}");
    }

    if let Some(path) = &result.local_path {
        println!("   local: {path}");
    }

    println!();
}
