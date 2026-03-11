use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalReport {
    pub dry_run: bool,
    pub stats: LinkLocalStats,
    pub entries: Vec<LinkLocalEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkLocalStats {
    pub files_scanned: usize,
    pub files_hashed_md5: usize,
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
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<LinkLocalRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub candidates: Vec<LinkLocalRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
        report.stats.linked_by_md5
            + report.stats.linked_by_isbn
            + report.stats.linked_by_original_filename
            + report.stats.linked_by_title_author,
        report.stats.linked_by_md5,
        report.stats.linked_by_isbn,
        report.stats.linked_by_original_filename,
        report.stats.linked_by_title_author,
        report.stats.unmatched,
        report.stats.ambiguous,
    );
}

pub fn print_json(report: &LinkLocalReport) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(report)?);
    Ok(())
}
