use std::collections::HashSet;
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::{Connection, params};

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalFileInfo {
    canonical_path: PathBuf,
    file_name: String,
    stem_normalized: String,
    md5_candidates: Vec<String>,
    isbn10_candidates: Vec<String>,
    isbn13_candidates: Vec<String>,
    search_terms: Vec<String>,
    used_content_md5: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchMethod {
    Md5,
    Isbn,
    OriginalFilename,
    TitleAuthor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalMatch {
    rid: i64,
    method: MatchMethod,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeuristicCandidate {
    rid: i64,
    title: Option<String>,
    author: Option<String>,
}

const NOISE_TERMS: &[&str] = &[
    "ebook", "book", "books", "scan", "scanned", "ocr", "libgen", "zlib", "epub", "pdf", "djvu",
    "mobi", "azw3", "azw", "fb2", "edition", "ed", "vol", "volume", "v2", "v3", "retail", "fixed",
    "clean", "text", "author", "title",
];

pub fn run(args: LinkLocalArgs) -> Result<()> {
    if !args.scan.is_dir() {
        bail!("scan path is not a directory: {}", args.scan.display());
    }

    let config = crate::config::resolve()?;
    let db_path = args.db.clone().unwrap_or(config.db_path());

    let mut conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let files = discover_local_files(&args.scan, args.max_files)?;
    let stats = link_local_files(
        &mut conn,
        &files,
        LinkOptions {
            replace_existing: args.replace_existing,
            dry_run: args.dry_run,
            verbose: args.verbose,
            hash_md5: args.hash_md5,
        },
    )?;

    println!(
        "{}local files: scanned={} hashed_md5={} linked={} (md5={}, isbn={}, original_filename={}, title_author={}) unmatched={} ambiguous={}",
        if args.dry_run { "dry-run " } else { "linked " },
        stats.files_scanned,
        stats.files_hashed_md5,
        stats.linked_by_md5
            + stats.linked_by_isbn
            + stats.linked_by_original_filename
            + stats.linked_by_title_author,
        stats.linked_by_md5,
        stats.linked_by_isbn,
        stats.linked_by_original_filename,
        stats.linked_by_title_author,
        stats.unmatched,
        stats.ambiguous,
    );

    Ok(())
}

fn link_local_files(
    conn: &mut Connection,
    paths: &[PathBuf],
    options: LinkOptions,
) -> Result<LinkStats> {
    let tx = conn.transaction()?;
    let mut update = if options.dry_run {
        None
    } else {
        Some(tx.prepare("UPDATE records SET local_path = ?1 WHERE rid = ?2")?)
    };
    let mut stats = LinkStats::default();

    for path in paths {
        stats.files_scanned += 1;

        let info = match build_local_file_info(path, options.hash_md5) {
            Ok(info) => info,
            Err(error) => {
                if options.verbose {
                    eprintln!("[link-local] unreadable: {} ({error:#})", path.display());
                }
                stats.unmatched += 1;
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
                        local_match.rid
                    ])?;
                }

                let aa_id = fetch_aa_id(&tx, local_match.rid)?;

                if options.verbose {
                    eprintln!(
                        "[link-local] {} [{}{}] {} -> {}",
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
                        aa_id
                    );
                }

                match local_match.method {
                    MatchMethod::Md5 => stats.linked_by_md5 += 1,
                    MatchMethod::Isbn => stats.linked_by_isbn += 1,
                    MatchMethod::OriginalFilename => stats.linked_by_original_filename += 1,
                    MatchMethod::TitleAuthor => stats.linked_by_title_author += 1,
                }
            }
            MatchSearchOutcome::Ambiguous => {
                if options.verbose {
                    eprintln!("[link-local] ambiguous: {}", info.canonical_path.display());
                }
                stats.ambiguous += 1;
            }
            MatchSearchOutcome::NoMatch => {
                if options.verbose {
                    eprintln!("[link-local] unmatched: {}", info.canonical_path.display());
                }
                stats.unmatched += 1;
            }
        }
    }

    drop(update);
    if options.dry_run {
        tx.rollback()?;
    } else {
        tx.commit()?;
    }
    Ok(stats)
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
}

fn fetch_aa_id(conn: &Connection, rid: i64) -> Result<String> {
    Ok(
        conn.query_row("SELECT aa_id FROM records WHERE rid = ?1", [rid], |row| {
            row.get(0)
        })?,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchSearchOutcome {
    Matched(LocalMatch),
    Ambiguous,
    NoMatch,
}

fn find_local_match(
    conn: &Connection,
    info: &LocalFileInfo,
    replace_existing: bool,
) -> Result<MatchSearchOutcome> {
    for md5 in &info.md5_candidates {
        match find_unique_record_by_code(conn, "md5", md5, replace_existing)? {
            UniqueLookup::Unique(rid) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    rid,
                    method: MatchMethod::Md5,
                }));
            }
            UniqueLookup::Ambiguous => return Ok(MatchSearchOutcome::Ambiguous),
            UniqueLookup::None => {}
        }
    }

    for isbn13 in &info.isbn13_candidates {
        match find_unique_record_by_code(conn, "isbn13", isbn13, replace_existing)? {
            UniqueLookup::Unique(rid) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    rid,
                    method: MatchMethod::Isbn,
                }));
            }
            UniqueLookup::Ambiguous => return Ok(MatchSearchOutcome::Ambiguous),
            UniqueLookup::None => {}
        }
    }

    for isbn10 in &info.isbn10_candidates {
        match find_unique_record_by_code(conn, "isbn10", isbn10, replace_existing)? {
            UniqueLookup::Unique(rid) => {
                return Ok(MatchSearchOutcome::Matched(LocalMatch {
                    rid,
                    method: MatchMethod::Isbn,
                }));
            }
            UniqueLookup::Ambiguous => return Ok(MatchSearchOutcome::Ambiguous),
            UniqueLookup::None => {}
        }
    }

    match find_unique_record_by_original_filename(conn, &info.file_name, replace_existing)? {
        UniqueLookup::Unique(rid) => {
            return Ok(MatchSearchOutcome::Matched(LocalMatch {
                rid,
                method: MatchMethod::OriginalFilename,
            }));
        }
        UniqueLookup::Ambiguous => return Ok(MatchSearchOutcome::Ambiguous),
        UniqueLookup::None => {}
    }

    match find_unique_record_by_title_author(conn, info, replace_existing)? {
        UniqueLookup::Unique(rid) => Ok(MatchSearchOutcome::Matched(LocalMatch {
            rid,
            method: MatchMethod::TitleAuthor,
        })),
        UniqueLookup::Ambiguous => Ok(MatchSearchOutcome::Ambiguous),
        UniqueLookup::None => Ok(MatchSearchOutcome::NoMatch),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UniqueLookup {
    Unique(i64),
    Ambiguous,
    None,
}

fn find_unique_record_by_code(
    conn: &Connection,
    kind: &str,
    value: &str,
    replace_existing: bool,
) -> Result<UniqueLookup> {
    let mut statement = conn.prepare(
        "SELECT r.rid
         FROM records r
         JOIN record_codes rc ON rc.rid = r.rid
         WHERE rc.kind = ?1
           AND lower(rc.value) = lower(?2)
           AND (?3 OR r.local_path IS NULL)
         ORDER BY r.rid
         LIMIT 2",
    )?;

    let rows = statement.query_map(params![kind, value, replace_existing], |row| {
        row.get::<_, i64>(0)
    })?;
    collapse_unique_lookup(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn find_unique_record_by_original_filename(
    conn: &Connection,
    file_name: &str,
    replace_existing: bool,
) -> Result<UniqueLookup> {
    let mut statement = conn.prepare(
        "SELECT rid
         FROM records
         WHERE lower(original_filename) = lower(?1)
           AND (?2 OR local_path IS NULL)
         ORDER BY rid
         LIMIT 2",
    )?;

    let rows = statement.query_map(params![file_name, replace_existing], |row| {
        row.get::<_, i64>(0)
    })?;
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
        "SELECT r.rid, r.title, r.author
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
        .query_map(params![query, replace_existing], |row| {
            Ok(HeuristicCandidate {
                rid: row.get(0)?,
                title: row.get(1)?,
                author: row.get(2)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let matched = candidates
        .into_iter()
        .filter(|candidate| heuristic_accepts(candidate, info))
        .map(|candidate| candidate.rid)
        .collect::<Vec<_>>();

    collapse_unique_lookup(matched)
}

fn heuristic_accepts(candidate: &HeuristicCandidate, info: &LocalFileInfo) -> bool {
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

fn collapse_unique_lookup(values: Vec<i64>) -> Result<UniqueLookup> {
    Ok(match values.as_slice() {
        [] => UniqueLookup::None,
        [rid] => UniqueLookup::Unique(*rid),
        _ => UniqueLookup::Ambiguous,
    })
}

fn discover_local_files(root: &Path, max_files: Option<usize>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_local_files(root, &mut out, max_files)?;
    Ok(out)
}

fn collect_local_files(
    root: &Path,
    out: &mut Vec<PathBuf>,
    max_files: Option<usize>,
) -> Result<()> {
    if max_files.is_some_and(|limit| out.len() >= limit) {
        return Ok(());
    }

    let mut entries = fs::read_dir(root)
        .with_context(|| format!("failed to read {}", root.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to inspect {}", path.display()))?;

        if metadata.is_dir() {
            collect_local_files(&path, out, max_files)?;
        } else if metadata.is_file() {
            out.push(path);
        }

        if max_files.is_some_and(|limit| out.len() >= limit) {
            break;
        }
    }

    Ok(())
}

fn build_local_file_info(path: &Path, hash_md5: bool) -> Result<LocalFileInfo> {
    let canonical_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("file name is not valid UTF-8")?
        .to_string();
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .context("file stem is not valid UTF-8")?;

    let stem_normalized = normalize_for_match(stem);
    let search_terms = significant_terms(&stem_normalized);
    let mut md5_candidates = Vec::new();
    let mut seen_md5 = HashSet::new();
    let mut used_content_md5 = false;

    if hash_md5 {
        if let Ok(md5) = compute_file_md5(path) {
            if seen_md5.insert(md5.clone()) {
                md5_candidates.push(md5);
            }
            used_content_md5 = true;
        }
    }

    for md5 in extract_md5_candidates(&file_name) {
        if seen_md5.insert(md5.clone()) {
            md5_candidates.push(md5);
        }
    }

    let (isbn10_candidates, isbn13_candidates) = extract_isbn_candidates(&file_name);

    Ok(LocalFileInfo {
        canonical_path,
        file_name,
        stem_normalized,
        md5_candidates,
        isbn10_candidates,
        isbn13_candidates,
        search_terms,
        used_content_md5,
    })
}

fn compute_file_md5(path: &Path) -> Result<String> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut context = md5::Context::new();
    let mut buffer = [0u8; 1024 * 1024];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        context.consume(&buffer[..read]);
    }

    Ok(format!("{:x}", context.compute()))
}

fn extract_md5_candidates(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for token in split_hexdigit_runs(input) {
        if token.len() == 32 && token.chars().all(|character| character.is_ascii_hexdigit()) {
            let lowered = token.to_ascii_lowercase();
            if seen.insert(lowered.clone()) {
                out.push(lowered);
            }
        }
    }

    out
}

fn extract_isbn_candidates(input: &str) -> (Vec<String>, Vec<String>) {
    let mut isbn10 = Vec::new();
    let mut isbn13 = Vec::new();
    let mut seen10 = HashSet::new();
    let mut seen13 = HashSet::new();

    for token in split_isbnish_runs(input) {
        let compact = token
            .chars()
            .filter(|character| {
                character.is_ascii_digit() || *character == 'X' || *character == 'x'
            })
            .collect::<String>();

        if compact.len() == 13 && compact.chars().all(|character| character.is_ascii_digit()) {
            if seen13.insert(compact.clone()) {
                isbn13.push(compact);
            }
        } else if compact.len() == 10
            && compact.chars().enumerate().all(|(index, character)| {
                character.is_ascii_digit() || (index == 9 && matches!(character, 'X' | 'x'))
            })
        {
            let normalized = compact.to_ascii_uppercase();
            if seen10.insert(normalized.clone()) {
                isbn10.push(normalized);
            }
        }
    }

    (isbn10, isbn13)
}

fn split_hexdigit_runs(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for character in input.chars() {
        if character.is_ascii_hexdigit() {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn split_isbnish_runs(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for character in input.chars() {
        if character.is_ascii_digit() || character == 'X' || character == 'x' || character == '-' {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn normalize_for_match(input: &str) -> String {
    input
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn significant_terms(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter(|term| term.len() >= 3)
        .filter(|term| {
            term.chars()
                .any(|character| character.is_ascii_alphabetic())
        })
        .filter(|term| !NOISE_TERMS.contains(term))
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

    use super::{
        LinkOptions, build_local_file_info, compute_file_md5, extract_isbn_candidates,
        extract_md5_candidates, link_local_files, normalize_for_match,
    };

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

        let stats = link_local_files(
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

        assert_eq!(stats.linked_by_md5, 1);
        assert_eq!(stats.linked_by_original_filename, 1);
        assert_eq!(stats.linked_by_title_author, 1);
        assert_eq!(stats.unmatched, 0);

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

        let stats = link_local_files(
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

        assert_eq!(stats.linked_by_original_filename, 1);

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

        let stats = link_local_files(
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

        assert_eq!(stats.files_hashed_md5, 1);
        assert_eq!(stats.linked_by_md5, 1);

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
}
