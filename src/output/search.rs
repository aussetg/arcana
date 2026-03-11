use crate::model::SearchResult;
use crate::search::expand::ExpansionDebugReport;

#[derive(Debug, Clone)]
pub struct SearchReport {
    pub results: Vec<SearchResult>,
    pub expansion_debug: Option<ExpansionDebugReport>,
}

pub fn print_text(report: &SearchReport) {
    if let Some(expansion_debug) = report.expansion_debug.as_ref() {
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
