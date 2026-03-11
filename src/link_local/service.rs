use std::path::PathBuf;

use anyhow::Result;
use rusqlite::{Connection, params};

use crate::link_local::{
    CandidateRecord, LocalFileInfo, MatchEvidence, MatchMethod, MatchSearchOutcome,
    build_local_file_info, find_local_match,
};
use crate::output::link_local::{
    LinkLocalAmbiguity, LinkLocalEntry, LinkLocalEvidence, LinkLocalIdentifiers, LinkLocalMatched,
    LinkLocalRecord, LinkLocalReport, LinkLocalStats, LinkLocalStatus, LinkMatchMethod,
};
use crate::output::report::REPORT_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkOptions {
    pub replace_existing: bool,
    pub dry_run: bool,
    pub verbose: bool,
    pub hash_md5: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LinkStats {
    files_scanned: usize,
    files_hashed_md5: usize,
    linked_by_md5: usize,
    linked_by_isbn: usize,
    linked_by_original_filename: usize,
    linked_by_title_author: usize,
    unmatched: usize,
    ambiguous: usize,
}

pub fn link_local_files(
    conn: &mut Connection,
    paths: &[PathBuf],
    options: LinkOptions,
) -> Result<LinkLocalReport> {
    let tx = conn.transaction()?;
    let mut update = if options.dry_run {
        None
    } else {
        Some(tx.prepare("UPDATE records SET local_path = ?1 WHERE rid = ?2")?)
    };
    let mut stats = LinkStats::default();
    let mut entries = Vec::with_capacity(paths.len());

    for path in paths {
        stats.files_scanned += 1;

        let info = match build_local_file_info(path, options.hash_md5) {
            Ok(info) => info,
            Err(error) => {
                if options.verbose {
                    eprintln!("[link-local] unreadable: {} ({error:#})", path.display());
                }
                stats.unmatched += 1;
                entries.push(LinkLocalEntry {
                    path: path.display().to_string(),
                    file_name: path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .map(str::to_string),
                    identifiers: None,
                    status: LinkLocalStatus::Unreadable,
                    matched: None,
                    ambiguity: None,
                    error: Some(format!("{error:#}")),
                });
                continue;
            }
        };

        if info.used_content_md5 {
            stats.files_hashed_md5 += 1;
        }

        match find_local_match(&tx, &info, options.replace_existing)? {
            MatchSearchOutcome::Matched(local_match) => {
                if let Some(update) = update.as_mut() {
                    update.execute(params![
                        info.canonical_path.display().to_string(),
                        local_match.record.rid
                    ])?;
                }

                if options.verbose {
                    eprintln!(
                        "[link-local] {} [{}{}] {} -> {} ({})",
                        if options.dry_run {
                            "would-link"
                        } else {
                            "linked"
                        },
                        local_match.method.label(),
                        if local_match.method == MatchMethod::Md5 && info.used_content_md5 {
                            ":content"
                        } else {
                            ""
                        },
                        info.canonical_path.display(),
                        local_match.record.aa_id,
                        local_match.evidence.summary()
                    );
                }

                entries.push(LinkLocalEntry {
                    path: info.canonical_path.display().to_string(),
                    file_name: Some(info.file_name.clone()),
                    identifiers: Some(identifiers_from_info(&info)),
                    status: if options.dry_run {
                        LinkLocalStatus::WouldLink
                    } else {
                        LinkLocalStatus::Linked
                    },
                    matched: Some(LinkLocalMatched {
                        method: local_match.method.json_value(),
                        evidence: LinkLocalEvidence::from(&local_match.evidence),
                        record: LinkLocalRecord::from(&local_match.record),
                    }),
                    ambiguity: None,
                    error: None,
                });

                match local_match.method {
                    MatchMethod::Md5 => stats.linked_by_md5 += 1,
                    MatchMethod::Isbn => stats.linked_by_isbn += 1,
                    MatchMethod::OriginalFilename => stats.linked_by_original_filename += 1,
                    MatchMethod::TitleAuthor => stats.linked_by_title_author += 1,
                }
            }
            MatchSearchOutcome::Ambiguous(ambiguous) => {
                if options.verbose {
                    let candidate_ids = ambiguous
                        .candidates
                        .iter()
                        .map(|candidate| candidate.aa_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    eprintln!(
                        "[link-local] ambiguous [{}] {} ({}) candidates: {}",
                        ambiguous.method.label(),
                        info.canonical_path.display(),
                        ambiguous.evidence.summary(),
                        candidate_ids
                    );
                }
                stats.ambiguous += 1;
                entries.push(LinkLocalEntry {
                    path: info.canonical_path.display().to_string(),
                    file_name: Some(info.file_name.clone()),
                    identifiers: Some(identifiers_from_info(&info)),
                    status: LinkLocalStatus::Ambiguous,
                    matched: None,
                    ambiguity: Some(LinkLocalAmbiguity {
                        method: ambiguous.method.json_value(),
                        evidence: LinkLocalEvidence::from(&ambiguous.evidence),
                        candidates: ambiguous
                            .candidates
                            .iter()
                            .map(LinkLocalRecord::from)
                            .collect(),
                    }),
                    error: None,
                });
            }
            MatchSearchOutcome::NoMatch => {
                if options.verbose {
                    eprintln!("[link-local] unmatched: {}", info.canonical_path.display());
                }
                stats.unmatched += 1;
                entries.push(LinkLocalEntry {
                    path: info.canonical_path.display().to_string(),
                    file_name: Some(info.file_name.clone()),
                    identifiers: Some(identifiers_from_info(&info)),
                    status: LinkLocalStatus::Unmatched,
                    matched: None,
                    ambiguity: None,
                    error: None,
                });
            }
        }
    }

    drop(update);
    if options.dry_run {
        tx.rollback()?;
    } else {
        tx.commit()?;
    }
    Ok(LinkLocalReport {
        report_version: REPORT_VERSION,
        kind: "link_local_report",
        dry_run: options.dry_run,
        stats: LinkLocalStats {
            files_scanned: stats.files_scanned,
            files_hashed_md5: stats.files_hashed_md5,
            linked_total: stats.linked_by_md5
                + stats.linked_by_isbn
                + stats.linked_by_original_filename
                + stats.linked_by_title_author,
            linked_by_md5: stats.linked_by_md5,
            linked_by_isbn: stats.linked_by_isbn,
            linked_by_original_filename: stats.linked_by_original_filename,
            linked_by_title_author: stats.linked_by_title_author,
            unmatched: stats.unmatched,
            ambiguous: stats.ambiguous,
        },
        entries,
    })
}

impl MatchMethod {
    fn label(self) -> &'static str {
        match self {
            MatchMethod::Md5 => "md5",
            MatchMethod::Isbn => "isbn",
            MatchMethod::OriginalFilename => "original_filename",
            MatchMethod::TitleAuthor => "title_author",
        }
    }

    fn json_value(self) -> LinkMatchMethod {
        match self {
            MatchMethod::Md5 => LinkMatchMethod::Md5,
            MatchMethod::Isbn => LinkMatchMethod::Isbn,
            MatchMethod::OriginalFilename => LinkMatchMethod::OriginalFilename,
            MatchMethod::TitleAuthor => LinkMatchMethod::TitleAuthor,
        }
    }
}

impl MatchEvidence {
    fn summary(&self) -> String {
        match self.kind {
            crate::output::link_local::LinkEvidenceKind::Md5 => {
                match (&self.value, self.used_content_md5) {
                    (Some(value), true) => format!("matched md5 {value} from file contents"),
                    (Some(value), false) => format!("matched md5 {value} from file name"),
                    (None, true) => "matched md5 from file contents".to_string(),
                    (None, false) => "matched md5".to_string(),
                }
            }
            crate::output::link_local::LinkEvidenceKind::Isbn10 => self
                .value
                .as_ref()
                .map(|value| format!("matched isbn10 {value}"))
                .unwrap_or_else(|| "matched isbn10".to_string()),
            crate::output::link_local::LinkEvidenceKind::Isbn13 => self
                .value
                .as_ref()
                .map(|value| format!("matched isbn13 {value}"))
                .unwrap_or_else(|| "matched isbn13".to_string()),
            crate::output::link_local::LinkEvidenceKind::OriginalFilename => self
                .value
                .as_ref()
                .map(|value| format!("matched original filename {value}"))
                .unwrap_or_else(|| "matched original filename".to_string()),
            crate::output::link_local::LinkEvidenceKind::TitleAuthor => {
                format!("matched title/author terms {:?}", self.search_terms)
            }
        }
    }
}

impl From<&MatchEvidence> for LinkLocalEvidence {
    fn from(value: &MatchEvidence) -> Self {
        Self {
            kind: value.kind,
            value: value.value.clone(),
            used_content_md5: value.used_content_md5,
            search_terms: value.search_terms.clone(),
            summary: value.summary(),
        }
    }
}

impl From<&CandidateRecord> for LinkLocalRecord {
    fn from(value: &CandidateRecord) -> Self {
        Self {
            aa_id: value.aa_id.clone(),
            title: value.title.clone(),
            author: value.author.clone(),
        }
    }
}

fn identifiers_from_info(info: &LocalFileInfo) -> LinkLocalIdentifiers {
    LinkLocalIdentifiers {
        md5_candidates: info.md5_candidates.clone(),
        isbn10_candidates: info.isbn10_candidates.clone(),
        isbn13_candidates: info.isbn13_candidates.clone(),
        search_terms: info.search_terms.clone(),
        used_content_md5: info.used_content_md5,
    }
}
