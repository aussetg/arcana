use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatRecord {
    pub aa_id: String,
    pub md5: Option<String>,
    pub isbn13: Option<String>,
    pub doi: Option<String>,
    pub title: Option<String>,
    pub author: Option<String>,
    pub publisher: Option<String>,
    pub edition_varia: Option<String>,
    pub subjects: Option<String>,
    pub description: Option<String>,
    pub year: Option<i32>,
    pub language: Option<String>,
    pub extension: Option<String>,
    pub content_type: Option<String>,
    pub filesize: Option<i64>,
    pub added_date: Option<String>,
    pub primary_source: Option<String>,
    pub score_base_rank: Option<i64>,
    pub cover_url: Option<String>,
    pub original_filename: Option<String>,
    pub has_aa_downloads: Option<i64>,
    pub has_torrent_paths: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactCode {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedRecord {
    pub record: FlatRecord,
    pub codes: Vec<ExactCode>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchResult {
    pub aa_id: String,
    pub title: Option<String>,
    pub author: Option<String>,
    pub year: Option<i32>,
    pub language: Option<String>,
    pub extension: Option<String>,
    pub publisher: Option<String>,
    pub primary_source: Option<String>,
    pub score_base_rank: Option<i64>,
    pub local_path: Option<String>,
    pub bm25_score: Option<f64>,
}
