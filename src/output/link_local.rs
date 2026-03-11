use crate::output::report::print_json as print_report_json;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalReport {
    pub report_version: u32,
    pub kind: &'static str,
    pub dry_run: bool,
    pub stats: LinkLocalStats,
    pub entries: Vec<LinkLocalEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalStats {
    pub files_scanned: usize,
    pub files_hashed_md5: usize,
    pub linked_total: usize,
    pub linked_by_md5: usize,
    pub linked_by_isbn: usize,
    pub linked_by_original_filename: usize,
    pub linked_by_title_author: usize,
    pub unmatched: usize,
    pub ambiguous: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalEntry {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<LinkLocalIdentifiers>,
    pub status: LinkLocalStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<LinkLocalMatched>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ambiguity: Option<LinkLocalAmbiguity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkLocalStatus {
    Linked,
    WouldLink,
    Ambiguous,
    Unmatched,
    Unreadable,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalIdentifiers {
    pub md5_candidates: Vec<String>,
    pub isbn10_candidates: Vec<String>,
    pub isbn13_candidates: Vec<String>,
    pub search_terms: Vec<String>,
    pub used_content_md5: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalMatched {
    pub method: LinkMatchMethod,
    pub evidence: LinkLocalEvidence,
    pub record: LinkLocalRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalAmbiguity {
    pub method: LinkMatchMethod,
    pub evidence: LinkLocalEvidence,
    pub candidates: Vec<LinkLocalRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkMatchMethod {
    Md5,
    Isbn,
    OriginalFilename,
    TitleAuthor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkEvidenceKind {
    Md5,
    Isbn10,
    Isbn13,
    OriginalFilename,
    TitleAuthor,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalEvidence {
    pub kind: LinkEvidenceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub used_content_md5: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub search_terms: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalRecord {
    pub aa_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

pub fn print_summary(report: &LinkLocalReport) {
    println!(
        "{}local files: scanned={} hashed_md5={} linked={} (md5={}, isbn={}, original_filename={}, title_author={}) unmatched={} ambiguous={}",
        if report.dry_run {
            "dry-run "
        } else {
            "linked "
        },
        report.stats.files_scanned,
        report.stats.files_hashed_md5,
        report.stats.linked_total,
        report.stats.linked_by_md5,
        report.stats.linked_by_isbn,
        report.stats.linked_by_original_filename,
        report.stats.linked_by_title_author,
        report.stats.unmatched,
        report.stats.ambiguous,
    );
}

pub fn print_json(report: &LinkLocalReport) -> anyhow::Result<()> {
    print_report_json(report)
}
