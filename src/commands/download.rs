use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::blocking::Client;
use rusqlite::{Connection, params};

use crate::download::{destination_path, download_to_path, request_fast_download_url};
use crate::records::{RecordSelector, resolve_unique_record};

#[derive(Debug, Args)]
pub struct DownloadArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub db: Option<PathBuf>,

    #[arg(long, help = "Download the record with this aa_id")]
    pub aa_id: Option<String>,

    #[arg(long, help = "Download the record with this md5")]
    pub md5: Option<String>,

    #[arg(long, help = "Download the record matching this ISBN")]
    pub isbn: Option<String>,

    #[arg(long, help = "Download the record matching this DOI")]
    pub doi: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory where downloaded files will be stored (defaults to config file or the XDG Downloads directory)"
    )]
    pub output_dir: Option<PathBuf>,

    #[arg(long, help = "Fast-download path index override")]
    pub path_index: Option<u32>,

    #[arg(long, help = "Fast-download domain index override")]
    pub domain_index: Option<u32>,

    #[arg(long, help = "Overwrite an existing file at the destination path")]
    pub replace_existing: bool,

    #[arg(long, help = "Do not update records.local_path after download")]
    pub no_link: bool,
}

pub fn run(args: DownloadArgs) -> Result<()> {
    let selector = RecordSelector::from_exact_args(
        args.aa_id.as_deref(),
        args.md5.as_deref(),
        args.isbn.as_deref(),
        args.doi.as_deref(),
    )?;
    let config = crate::config::resolve()?;
    let db_path = args.db.clone().unwrap_or(config.db_path());
    let output_dir = args.output_dir.clone().unwrap_or(config.download_dir());

    let mut conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let record = resolve_unique_record(&conn, &selector)?;
    let record_md5 = record
        .md5
        .as_deref()
        .filter(|md5| !md5.trim().is_empty())
        .context("record has no md5; fast download API requires md5")?;
    let destination = destination_path(&output_dir, &record)?;

    if let Some(existing_local_path) = &record.local_path {
        let existing_path = Path::new(existing_local_path);
        if existing_path.exists() && !args.replace_existing {
            println!("already linked locally: {}", existing_path.display());
            return Ok(());
        }
    }

    if destination.exists() && !args.replace_existing {
        bail!(
            "destination already exists: {} (pass --replace-existing to overwrite)",
            destination.display()
        );
    }

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let secret_key = config.secret_key()?;

    let client = Client::builder()
        .user_agent("arcana/0.1")
        .build()
        .context("failed to build HTTP client")?;

    let download_url = request_fast_download_url(
        &client,
        config.fast_download_api_url(),
        record_md5,
        &secret_key,
        args.path_index,
        args.domain_index,
    )?;

    download_to_path(&client, &download_url, &destination, args.replace_existing)?;

    if !args.no_link {
        update_local_path(&mut conn, record.rid, &destination)?;
    }

    println!("downloaded: {}", destination.display());
    Ok(())
}

fn update_local_path(conn: &mut Connection, rid: i64, destination: &Path) -> Result<()> {
    conn.execute(
        "UPDATE records SET local_path = ?1 WHERE rid = ?2",
        params![destination.display().to_string(), rid],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};
    use crate::records::{RecordSelector, resolve_unique_record};

    fn sample_record(aa_id: &str, md5: &str, isbn13: Option<&str>) -> ExtractedRecord {
        ExtractedRecord {
            record: FlatRecord {
                aa_id: aa_id.into(),
                md5: Some(md5.into()),
                isbn13: isbn13.map(str::to_string),
                doi: None,
                title: Some("Title".into()),
                author: Some("Author".into()),
                publisher: Some("Publisher".into()),
                edition_varia: None,
                subjects: Some("Subject".into()),
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
                original_filename: Some(format!("{aa_id}.pdf")),
                has_aa_downloads: Some(1),
                has_torrent_paths: Some(0),
            },
            codes: {
                let mut codes = vec![ExactCode {
                    kind: "md5".into(),
                    value: md5.into(),
                }];
                if let Some(isbn13) = isbn13 {
                    codes.push(ExactCode {
                        kind: "isbn13".into(),
                        value: isbn13.into(),
                    });
                }
                codes
            },
        }
    }

    #[test]
    fn resolves_record_by_isbn() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![sample_record(
            "aa-1",
            "abcdef0123456789abcdef0123456789",
            Some("9780131103627"),
        )];
        crate::commands::build::insert_batch(&mut conn, &mut batch).unwrap();

        let record =
            resolve_unique_record(&conn, &RecordSelector::Isbn("9780131103627".into())).unwrap();
        assert_eq!(record.aa_id, "aa-1");
        assert_eq!(
            record.md5.as_deref(),
            Some("abcdef0123456789abcdef0123456789")
        );
    }
}
