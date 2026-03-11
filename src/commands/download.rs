use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use rusqlite::Connection;

use crate::download::{DownloadOptions, FilenameMode, execute_download};
use crate::output::download::DownloadReport;
use crate::records::RecordSelector;

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

    #[arg(long, help = "Verify the downloaded file against the expected MD5")]
    pub verify_md5: bool,

    #[arg(long, value_name = "NAME", help = "Override the destination file name")]
    pub output_name: Option<String>,

    #[arg(long, value_enum, default_value_t = FilenameMode::Original)]
    pub filename_mode: FilenameMode,

    #[arg(long, help = "Emit machine-readable JSON output")]
    pub json: bool,

    #[arg(long, help = "Do not update records.local_path after download")]
    pub no_link: bool,
}

pub fn run(args: DownloadArgs) -> Result<()> {
    let json = args.json;
    crate::output::error::run_json("download", json, || run_inner(args))
}

fn run_inner(args: DownloadArgs) -> Result<()> {
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

    let options = DownloadOptions {
        output_dir,
        path_index: args.path_index,
        domain_index: args.domain_index,
        replace_existing: args.replace_existing,
        verify_md5: args.verify_md5,
        output_name: args.output_name.clone(),
        filename_mode: args.filename_mode,
        no_link: args.no_link,
    };
    let execution = execute_download(&mut conn, &config, &selector, &options)?;
    let report = DownloadReport::new(
        &selector,
        &execution.record,
        &options,
        &execution.destination,
        execution.outcome,
    );

    if args.json {
        crate::output::download::print_json(&report)?;
    } else {
        crate::output::download::print_text(&report);
    }
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
