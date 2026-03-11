use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;

use crate::link_local::{LinkOptions, discover_local_files, link_local_files};

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
#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use crate::link_local::{
        LinkOptions, build_local_file_info, compute_file_md5, extract_isbn_candidates,
        extract_md5_candidates, link_local_files, normalize_for_match,
    };
    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};
    use crate::output::link_local::{LinkLocalStatus, LinkMatchMethod};

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
