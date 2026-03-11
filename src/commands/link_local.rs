use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::{Connection, params};

use crate::link_local::{
    CandidateRecord, LocalFileInfo, MatchEvidence, MatchMethod, MatchSearchOutcome,
    build_local_file_info, discover_local_files, find_local_match,
};
use crate::output::link_local::{
    LinkEvidenceKind, LinkLocalAmbiguity, LinkLocalEntry, LinkLocalEvidence, LinkLocalIdentifiers,
    LinkLocalMatched, LinkLocalRecord, LinkLocalReport, LinkLocalStats, LinkLocalStatus,
    LinkMatchMethod,
};

#[derive(Debug, Args)]
pub struct LinkLocalArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub db: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory tree to scan for local files"
    )]
    pub scan: PathBuf,

    #[arg(long, help = "Replace existing records.local_path values")]
    pub replace_existing: bool,

    #[arg(long, help = "Scan and report matches without updating the database")]
    pub dry_run: bool,

    #[arg(long, help = "Print per-file matching decisions to stderr")]
    pub verbose: bool,

    #[arg(
        long,
        help = "Hash local file contents and try real MD5 matching before filename heuristics"
    )]
    pub hash_md5: bool,

    #[arg(long)]
    pub max_files: Option<usize>,

    #[arg(long, help = "Emit a machine-readable JSON report")]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LinkOptions {
    replace_existing: bool,
    dry_run: bool,
    verbose: bool,
    hash_md5: bool,
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

pub fn run(args: LinkLocalArgs) -> Result<()> {
    let json = args.json;
    crate::output::error::run_json("link_local", json, || run_inner(args))
}

fn run_inner(args: LinkLocalArgs) -> Result<()> {
    if !args.scan.is_dir() {
        bail!("scan path is not a directory: {}", args.scan.display());
    }

    let config = crate::config::resolve()?;
    let db_path = args.db.clone().unwrap_or(config.db_path());

    let mut conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let files = discover_local_files(&args.scan, args.max_files)?;
    let report = link_local_files(
        &mut conn,
        &files,
        LinkOptions {
            replace_existing: args.replace_existing,
            dry_run: args.dry_run,
            verbose: args.verbose,
            hash_md5: args.hash_md5,
        },
    )?;

    if args.json {
        crate::output::link_local::print_json(&report)?;
    } else {
        crate::output::link_local::print_summary(&report);
    }
    Ok(())
}

fn link_local_files(
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
        report_version: 1,
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
            LinkEvidenceKind::Md5 => match (&self.value, self.used_content_md5) {
                (Some(value), true) => format!("matched md5 {value} from file contents"),
                (Some(value), false) => format!("matched md5 {value} from file name"),
                (None, true) => "matched md5 from file contents".to_string(),
                (None, false) => "matched md5".to_string(),
            },
            LinkEvidenceKind::Isbn10 => self
                .value
                .as_ref()
                .map(|value| format!("matched isbn10 {value}"))
                .unwrap_or_else(|| "matched isbn10".to_string()),
            LinkEvidenceKind::Isbn13 => self
                .value
                .as_ref()
                .map(|value| format!("matched isbn13 {value}"))
                .unwrap_or_else(|| "matched isbn13".to_string()),
            LinkEvidenceKind::OriginalFilename => self
                .value
                .as_ref()
                .map(|value| format!("matched original filename {value}"))
                .unwrap_or_else(|| "matched original filename".to_string()),
            LinkEvidenceKind::TitleAuthor => {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use crate::link_local::{
        build_local_file_info, compute_file_md5, extract_isbn_candidates, extract_md5_candidates,
        normalize_for_match,
    };
    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};
    use crate::output::link_local::{LinkLocalStatus, LinkMatchMethod};

    use super::{LinkOptions, link_local_files};

    fn unique_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("arcana-{name}-{nonce}"))
    }

    fn sample_record(
        aa_id: &str,
        title: &str,
        author: &str,
        original_filename: Option<&str>,
        codes: Vec<ExactCode>,
    ) -> ExtractedRecord {
        ExtractedRecord {
            record: FlatRecord {
                aa_id: aa_id.into(),
                md5: None,
                isbn13: None,
                doi: None,
                title: Some(title.into()),
                author: Some(author.into()),
                publisher: Some("Publisher".into()),
                edition_varia: None,
                subjects: Some("Machine learning".into()),
                description: Some("Description".into()),
                year: Some(2024),
                language: Some("en".into()),
                extension: Some("pdf".into()),
                content_type: Some("book_nonfiction".into()),
                filesize: None,
                added_date: None,
                primary_source: Some("libgen".into()),
                score_base_rank: Some(10),
                cover_url: None,
                original_filename: original_filename.map(str::to_string),
                has_aa_downloads: Some(1),
                has_torrent_paths: Some(0),
            },
            codes,
        }
    }

    #[test]
    fn extracts_identifiers_from_filenames() {
        assert_eq!(
            extract_md5_candidates("book-abcdef0123456789abcdef0123456789.pdf"),
            vec!["abcdef0123456789abcdef0123456789".to_string()]
        );

        let (isbn10, isbn13) = extract_isbn_candidates("The-Book-978-0131103627-(0131103628).pdf");
        assert_eq!(isbn13, vec!["9780131103627".to_string()]);
        assert_eq!(isbn10, vec!["0131103628".to_string()]);
    }

    #[test]
    fn links_by_md5_then_original_filename_then_title_author() {
        let db_path = unique_path("link-local-db.sqlite3");
        let dir = unique_path("link-local-dir");
        fs::create_dir_all(&dir).unwrap();

        let mut conn = Connection::open(&db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![
            sample_record(
                "aa-1",
                "Hash Book",
                "Alice Author",
                None,
                vec![ExactCode {
                    kind: "md5".into(),
                    value: "abcdef0123456789abcdef0123456789".into(),
                }],
            ),
            sample_record(
                "aa-2",
                "Filename Book",
                "Bob Writer",
                Some("Filename Book.pdf"),
                Vec::new(),
            ),
            sample_record(
                "aa-3",
                "Large Language Models",
                "Carol Smith",
                None,
                Vec::new(),
            ),
        ];

        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
        crate::db::populate_fts(&conn).unwrap();

        let file1 = dir.join("book-abcdef0123456789abcdef0123456789.pdf");
        let file2 = dir.join("Filename Book.pdf");
        let file3 = dir.join("Carol Smith - Large Language Models [2024].pdf");
        fs::write(&file1, b"a").unwrap();
        fs::write(&file2, b"b").unwrap();
        fs::write(&file3, b"c").unwrap();

        let report = link_local_files(
            &mut conn,
            &[file1.clone(), file2.clone(), file3.clone()],
            LinkOptions {
                replace_existing: false,
                dry_run: false,
                verbose: false,
                hash_md5: false,
            },
        )
        .unwrap();

        assert_eq!(report.stats.linked_by_md5, 1);
        assert_eq!(report.stats.linked_by_original_filename, 1);
        assert_eq!(report.stats.linked_by_title_author, 1);
        assert_eq!(report.stats.unmatched, 0);

        let rows = conn
            .prepare("SELECT aa_id, local_path FROM records ORDER BY aa_id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(rows.iter().all(|(_, local_path)| local_path.is_some()));

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(file1);
        let _ = fs::remove_file(file2);
        let _ = fs::remove_file(file3);
        let _ = fs::remove_dir(dir);
    }

    #[test]
    fn normalizes_file_text_for_matching() {
        assert_eq!(
            normalize_for_match("Carol Smith - Large Language Models [2024]"),
            "carol smith large language models 2024"
        );
    }

    #[test]
    fn builds_local_file_info_from_utf8_path() {
        let dir = unique_path("link-local-info-dir");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Example-9780131103627.pdf");
        fs::write(&file, b"x").unwrap();

        let info = build_local_file_info(&file, false).unwrap();
        assert_eq!(info.file_name, "Example-9780131103627.pdf");
        assert_eq!(info.isbn13_candidates, vec!["9780131103627".to_string()]);

        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }

    #[test]
    fn dry_run_reports_matches_without_writing_local_paths() {
        let db_path = unique_path("link-local-dry-run-db.sqlite3");
        let dir = unique_path("link-local-dry-run-dir");
        fs::create_dir_all(&dir).unwrap();

        let mut conn = Connection::open(&db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![sample_record(
            "aa-1",
            "Filename Book",
            "Bob Writer",
            Some("Filename Book.pdf"),
            Vec::new(),
        )];
        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
        crate::db::populate_fts(&conn).unwrap();

        let file = dir.join("Filename Book.pdf");
        fs::write(&file, b"b").unwrap();

        let report = link_local_files(
            &mut conn,
            std::slice::from_ref(&file),
            LinkOptions {
                replace_existing: false,
                dry_run: true,
                verbose: false,
                hash_md5: false,
            },
        )
        .unwrap();

        assert_eq!(report.stats.linked_by_original_filename, 1);

        let local_path: Option<String> = conn
            .query_row(
                "SELECT local_path FROM records WHERE aa_id = 'aa-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(local_path.is_none());

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }

    #[test]
    fn hashes_file_contents_for_md5_matching_when_enabled() {
        let db_path = unique_path("link-local-hash-db.sqlite3");
        let dir = unique_path("link-local-hash-dir");
        fs::create_dir_all(&dir).unwrap();

        let file = dir.join("completely-unrelated-name.pdf");
        fs::write(&file, b"hello").unwrap();
        let content_md5 = compute_file_md5(&file).unwrap();

        let mut conn = Connection::open(&db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![sample_record(
            "aa-1",
            "Hash Book",
            "Alice Author",
            None,
            vec![ExactCode {
                kind: "md5".into(),
                value: content_md5,
            }],
        )];
        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
        crate::db::populate_fts(&conn).unwrap();

        let report = link_local_files(
            &mut conn,
            std::slice::from_ref(&file),
            LinkOptions {
                replace_existing: false,
                dry_run: false,
                verbose: false,
                hash_md5: true,
            },
        )
        .unwrap();

        assert_eq!(report.stats.files_hashed_md5, 1);
        assert_eq!(report.stats.linked_by_md5, 1);

        let entry = &report.entries[0];
        assert!(matches!(entry.status, LinkLocalStatus::Linked));
        assert!(matches!(
            entry.matched.as_ref().map(|matched| matched.method),
            Some(LinkMatchMethod::Md5)
        ));
        assert!(
            entry
                .matched
                .as_ref()
                .unwrap()
                .evidence
                .summary
                .contains("matched md5")
        );

        let local_path: Option<String> = conn
            .query_row(
                "SELECT local_path FROM records WHERE aa_id = 'aa-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(local_path.is_some());

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }

    #[test]
    fn reports_ambiguous_candidates() {
        let db_path = unique_path("link-local-ambiguous-db.sqlite3");
        let dir = unique_path("link-local-ambiguous-dir");
        fs::create_dir_all(&dir).unwrap();

        let mut conn = Connection::open(&db_path).unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![
            sample_record(
                "aa-1",
                "Duplicate Filename One",
                "Alice Author",
                Some("same.pdf"),
                Vec::new(),
            ),
            sample_record(
                "aa-2",
                "Duplicate Filename Two",
                "Bob Writer",
                Some("same.pdf"),
                Vec::new(),
            ),
        ];
        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();
        crate::db::populate_fts(&conn).unwrap();

        let file = dir.join("same.pdf");
        fs::write(&file, b"x").unwrap();

        let report = link_local_files(
            &mut conn,
            std::slice::from_ref(&file),
            LinkOptions {
                replace_existing: false,
                dry_run: true,
                verbose: false,
                hash_md5: false,
            },
        )
        .unwrap();

        assert_eq!(report.stats.ambiguous, 1);
        let entry = &report.entries[0];
        assert!(matches!(entry.status, LinkLocalStatus::Ambiguous));
        assert!(matches!(
            entry.ambiguity.as_ref().map(|ambiguity| ambiguity.method),
            Some(LinkMatchMethod::OriginalFilename)
        ));
        assert_eq!(entry.ambiguity.as_ref().unwrap().candidates.len(), 2);
        assert_eq!(
            entry.ambiguity.as_ref().unwrap().candidates[0].aa_id,
            "aa-1"
        );
        assert_eq!(
            entry.ambiguity.as_ref().unwrap().candidates[1].aa_id,
            "aa-2"
        );

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }
}
