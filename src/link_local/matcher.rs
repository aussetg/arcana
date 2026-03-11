use std::collections::HashSet;

use anyhow::Result;
use rusqlite::{Connection, params};

use crate::link_local::{LocalFileInfo, normalize_for_match, significant_terms};
use crate::output::link_local::LinkEvidenceKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMethod {
    Md5,
    Isbn,
    OriginalFilename,
    TitleAuthor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMatch {
    pub record: CandidateRecord,
    pub method: MatchMethod,
    pub evidence: MatchEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRecord {
    pub rid: i64,
    pub aa_id: String,
    pub title: Option<String>,
    pub author: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousMatch {
    pub method: MatchMethod,
    pub evidence: MatchEvidence,
    pub candidates: Vec<CandidateRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchEvidence {
    pub kind: LinkEvidenceKind,
    pub value: Option<String>,
    pub used_content_md5: bool,
    pub search_terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchSearchOutcome {
    Matched(LocalMatch),
    Ambiguous(AmbiguousMatch),
    NoMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UniqueLookup {
    Unique(CandidateRecord),
    Ambiguous(Vec<CandidateRecord>),
    None,
}

pub fn find_local_match(
    conn: &Connection,
    info: &LocalFileInfo,
    replace_existing: bool,
) -> Result<MatchSearchOutcome> {
    for md5 in &info.md5_candidates {
        match find_unique_record_by_code(conn, "md5", md5, replace_existing)? {
            UniqueLookup::Unique(record) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    record,
                    method: MatchMethod::Md5,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Md5,
                        value: Some(md5.clone()),
                        used_content_md5: info.used_content_md5,
                        search_terms: Vec::new(),
                    },
                }));
            }
            UniqueLookup::Ambiguous(candidates) => {
                return Ok(MatchSearchOutcome::Ambiguous(AmbiguousMatch {
                    method: MatchMethod::Md5,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Md5,
                        value: Some(md5.clone()),
                        used_content_md5: info.used_content_md5,
                        search_terms: Vec::new(),
                    },
                    candidates,
                }));
            }
            UniqueLookup::None => {}
        }
    }

    for isbn13 in &info.isbn13_candidates {
        match find_unique_record_by_code(conn, "isbn13", isbn13, replace_existing)? {
            UniqueLookup::Unique(record) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    record,
                    method: MatchMethod::Isbn,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Isbn13,
                        value: Some(isbn13.clone()),
                        used_content_md5: false,
                        search_terms: Vec::new(),
                    },
                }));
            }
            UniqueLookup::Ambiguous(candidates) => {
                return Ok(MatchSearchOutcome::Ambiguous(AmbiguousMatch {
                    method: MatchMethod::Isbn,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Isbn13,
                        value: Some(isbn13.clone()),
                        used_content_md5: false,
                        search_terms: Vec::new(),
                    },
                    candidates,
                }));
            }
            UniqueLookup::None => {}
        }
    }

    for isbn10 in &info.isbn10_candidates {
        match find_unique_record_by_code(conn, "isbn10", isbn10, replace_existing)? {
            UniqueLookup::Unique(record) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    record,
                    method: MatchMethod::Isbn,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Isbn10,
                        value: Some(isbn10.clone()),
                        used_content_md5: false,
                        search_terms: Vec::new(),
                    },
                }));
            }
            UniqueLookup::Ambiguous(candidates) => {
                return Ok(MatchSearchOutcome::Ambiguous(AmbiguousMatch {
                    method: MatchMethod::Isbn,
                    evidence: MatchEvidence {
                        kind: LinkEvidenceKind::Isbn10,
                        value: Some(isbn10.clone()),
                        used_content_md5: false,
                        search_terms: Vec::new(),
                    },
                    candidates,
                }));
            }
            UniqueLookup::None => {}
        }
    }

    match find_unique_record_by_original_filename(conn, &info.file_name, replace_existing)? {
        UniqueLookup::Unique(record) => {
            return Ok(MatchSearchOutcome::Matched(LocalMatch {
                record,
                method: MatchMethod::OriginalFilename,
                evidence: MatchEvidence {
                    kind: LinkEvidenceKind::OriginalFilename,
                    value: Some(info.file_name.clone()),
                    used_content_md5: false,
                    search_terms: Vec::new(),
                },
            }));
        }
        UniqueLookup::Ambiguous(candidates) => {
            return Ok(MatchSearchOutcome::Ambiguous(AmbiguousMatch {
                method: MatchMethod::OriginalFilename,
                evidence: MatchEvidence {
                    kind: LinkEvidenceKind::OriginalFilename,
                    value: Some(info.file_name.clone()),
                    used_content_md5: false,
                    search_terms: Vec::new(),
                },
                candidates,
            }));
        }
        UniqueLookup::None => {}
    }

    match find_unique_record_by_title_author(conn, info, replace_existing)? {
        UniqueLookup::Unique(record) => Ok(MatchSearchOutcome::Matched(LocalMatch {
            record,
            method: MatchMethod::TitleAuthor,
            evidence: MatchEvidence {
                kind: LinkEvidenceKind::TitleAuthor,
                value: None,
                used_content_md5: false,
                search_terms: info.search_terms.clone(),
            },
        })),
        UniqueLookup::Ambiguous(candidates) => Ok(MatchSearchOutcome::Ambiguous(AmbiguousMatch {
            method: MatchMethod::TitleAuthor,
            evidence: MatchEvidence {
                kind: LinkEvidenceKind::TitleAuthor,
                value: None,
                used_content_md5: false,
                search_terms: info.search_terms.clone(),
            },
            candidates,
        })),
        UniqueLookup::None => Ok(MatchSearchOutcome::NoMatch),
    }
}

fn find_unique_record_by_code(
    conn: &Connection,
    kind: &str,
    value: &str,
    replace_existing: bool,
) -> Result<UniqueLookup> {
    let mut statement = conn.prepare(
        "SELECT r.rid, r.aa_id, r.title, r.author
         FROM records r
         JOIN record_codes rc ON rc.rid = r.rid
         WHERE rc.kind = ?1
           AND lower(rc.value) = lower(?2)
           AND (?3 OR r.local_path IS NULL)
         ORDER BY r.rid
         LIMIT 3",
    )?;

    let rows = statement.query_map(params![kind, value, replace_existing], map_candidate_record)?;
    collapse_unique_lookup(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn find_unique_record_by_original_filename(
    conn: &Connection,
    file_name: &str,
    replace_existing: bool,
) -> Result<UniqueLookup> {
    let mut statement = conn.prepare(
        "SELECT rid, aa_id, title, author
         FROM records
         WHERE lower(original_filename) = lower(?1)
           AND (?2 OR local_path IS NULL)
         ORDER BY rid
         LIMIT 3",
    )?;

    let rows = statement.query_map(params![file_name, replace_existing], map_candidate_record)?;
    collapse_unique_lookup(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn find_unique_record_by_title_author(
    conn: &Connection,
    info: &LocalFileInfo,
    replace_existing: bool,
) -> Result<UniqueLookup> {
    if info.search_terms.len() < 2 {
        return Ok(UniqueLookup::None);
    }

    let query = info
        .search_terms
        .iter()
        .take(6)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut statement = conn.prepare(
        "SELECT r.rid, r.aa_id, r.title, r.author
         FROM records_fts
         JOIN records r ON r.rid = records_fts.rowid
         WHERE records_fts MATCH ?1
           AND (?2 OR r.local_path IS NULL)
         ORDER BY bm25(records_fts, 10.0, 8.0, 5.0, 1.5, 1.0, 0.5) ASC,
                  COALESCE(r.score_base_rank, 0) DESC,
                  r.rid DESC
         LIMIT 8",
    )?;

    let candidates = statement
        .query_map(params![query, replace_existing], map_candidate_record)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let matched = candidates
        .into_iter()
        .filter(|candidate| heuristic_accepts(candidate, info))
        .collect::<Vec<_>>();

    collapse_unique_lookup(matched)
}

fn heuristic_accepts(candidate: &CandidateRecord, info: &LocalFileInfo) -> bool {
    let title = candidate.title.as_deref().map(normalize_for_match);
    let Some(title_norm) = title else {
        return false;
    };

    let title_terms = significant_terms(&title_norm);
    if title_terms.len() < 2 {
        return false;
    }

    let file_terms: HashSet<&str> = info.search_terms.iter().map(String::as_str).collect();
    let matched_title_terms = title_terms
        .iter()
        .filter(|term| file_terms.contains(term.as_str()))
        .count();

    let title_ratio = matched_title_terms as f64 / title_terms.len() as f64;
    let title_substring = info.stem_normalized.contains(&title_norm);

    if !(title_substring || title_ratio >= 0.75) {
        return false;
    }

    let Some(author) = candidate.author.as_deref() else {
        return true;
    };

    let author_norm = normalize_for_match(author);
    let author_terms = significant_terms(&author_norm);
    if author_terms.is_empty() {
        return true;
    }

    author_terms
        .iter()
        .any(|term| file_terms.contains(term.as_str()))
}

fn map_candidate_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<CandidateRecord> {
    Ok(CandidateRecord {
        rid: row.get(0)?,
        aa_id: row.get(1)?,
        title: row.get(2)?,
        author: row.get(3)?,
    })
}

fn collapse_unique_lookup(values: Vec<CandidateRecord>) -> Result<UniqueLookup> {
    Ok(match values.as_slice() {
        [] => UniqueLookup::None,
        [record] => UniqueLookup::Unique(record.clone()),
        _ => UniqueLookup::Ambiguous(values),
    })
}
