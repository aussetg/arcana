use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;
use rusqlite::params;

use crate::model::ExtractedRecord;

#[derive(Debug, Args)]
pub struct BuildArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Input shard directory or a single .json.gz shard"
    )]
    pub input: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database to create (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub output: Option<PathBuf>,

    #[arg(long, help = "Replace an existing database file at --output")]
    pub replace: bool,

    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,

    #[arg(long)]
    pub max_shards: Option<usize>,

    #[arg(long)]
    pub max_records: Option<usize>,
}

pub fn run(args: BuildArgs) -> Result<()> {
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than zero");
    }

    let config = crate::config::load()?;
    let output = args.output.unwrap_or(config.db_path()?).to_path_buf();

    prepare_output_path(&output, args.replace)?;

    let mut conn = Connection::open(&output)
        .with_context(|| format!("failed to open {}", output.display()))?;

    crate::db::prepare_database(&conn)
        .with_context(|| format!("failed to initialize {}", output.display()))?;

    let mut total_files = 0usize;
    let mut total_lines = 0usize;
    let mut total_records = 0usize;
    let mut total_codes = 0usize;

    if let Some(input) = args.input.as_deref() {
        let shards = crate::extract::discover_input_shards(input, args.max_shards)?;

        if shards.is_empty() {
            bail!("no input shards found under {}", input.display());
        }

        let mut batch = Vec::with_capacity(args.batch_size);
        let mut stop = false;

        for shard in &shards {
            if stop {
                break;
            }

            total_files += 1;

            let file_stats = crate::extract::stream_records(shard, |record| {
                total_records += 1;
                total_codes += record.codes.len();
                batch.push(record);

                if batch.len() >= args.batch_size {
                    insert_batch(&mut conn, &mut batch)?;
                }

                if args.max_records.is_some_and(|limit| total_records >= limit) {
                    stop = true;
                    return Ok(false);
                }

                Ok(true)
            })?;

            total_lines += file_stats.lines;
        }

        insert_batch(&mut conn, &mut batch)?;
    }

    crate::db::populate_fts(&conn)
        .with_context(|| format!("failed to populate FTS in {}", output.display()))?;

    crate::db::finalize_database(&conn)
        .with_context(|| format!("failed to finalize {}", output.display()))?;

    if args.input.is_some() {
        println!(
            "built database: {} (files: {total_files}, lines: {total_lines}, records: {total_records}, codes: {total_codes})",
            output.display()
        );
    } else {
        println!("initialized empty database: {}", output.display());
    }

    Ok(())
}

pub(crate) fn insert_batch(conn: &mut Connection, batch: &mut Vec<ExtractedRecord>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    let mut insert_record = tx.prepare(
        "INSERT INTO records (
            aa_id,
            md5,
            isbn13,
            doi,
            title,
            author,
            publisher,
            edition_varia,
            subjects,
            description,
            year,
            language,
            extension,
            content_type,
            filesize,
            added_date,
            primary_source,
            score_base_rank,
            cover_url,
            original_filename,
            has_aa_downloads,
            has_torrent_paths
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
            ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22
        )",
    )?;
    let mut insert_code =
        tx.prepare("INSERT INTO record_codes (rid, kind, value) VALUES (?1, ?2, ?3)")?;

    for extracted in batch.drain(..) {
        let record = extracted.record;

        insert_record.execute(params![
            record.aa_id,
            record.md5,
            record.isbn13,
            record.doi,
            record.title,
            record.author,
            record.publisher,
            record.edition_varia,
            record.subjects,
            record.description,
            record.year,
            record.language,
            record.extension,
            record.content_type,
            record.filesize,
            record.added_date,
            record.primary_source,
            record.score_base_rank,
            record.cover_url,
            record.original_filename,
            record.has_aa_downloads,
            record.has_torrent_paths,
        ])?;

        let rid = tx.last_insert_rowid();

        for code in extracted.codes {
            insert_code.execute(params![rid, code.kind, code.value])?;
        }
    }

    drop(insert_code);
    drop(insert_record);
    tx.commit()?;
    Ok(())
}

fn prepare_output_path(path: &Path, replace: bool) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    if !path.exists() {
        return Ok(());
    }

    let metadata =
        fs::metadata(path).with_context(|| format!("failed to inspect {}", path.display()))?;

    if metadata.is_dir() {
        bail!("output path is a directory: {}", path.display());
    }

    if !replace {
        bail!(
            "output already exists: {} (pass --replace to overwrite)",
            path.display()
        );
    }

    fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

    use super::insert_batch;

    #[test]
    fn inserts_records_and_codes() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![ExtractedRecord {
            record: FlatRecord {
                aa_id: "aa-1".into(),
                md5: Some("md5-1".into()),
                isbn13: Some("isbn-1".into()),
                doi: Some("doi-1".into()),
                title: Some("Title".into()),
                author: Some("Author".into()),
                publisher: None,
                edition_varia: None,
                subjects: Some("Subject".into()),
                description: Some("Description".into()),
                year: Some(2024),
                language: Some("en".into()),
                extension: Some("pdf".into()),
                content_type: Some("book".into()),
                filesize: Some(123),
                added_date: Some("2024-01-01".into()),
                primary_source: Some("libgen".into()),
                score_base_rank: Some(10),
                cover_url: None,
                original_filename: Some("file.pdf".into()),
                has_aa_downloads: Some(1),
                has_torrent_paths: Some(0),
            },
            codes: vec![
                ExactCode {
                    kind: "md5".into(),
                    value: "md5-1".into(),
                },
                ExactCode {
                    kind: "isbn13".into(),
                    value: "isbn-1".into(),
                },
            ],
        }];

        insert_batch(&mut conn, &mut batch).unwrap();

        let record_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        let code_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM record_codes", [], |row| row.get(0))
            .unwrap();

        assert_eq!(record_count, 1);
        assert_eq!(code_count, 2);
    }
}
