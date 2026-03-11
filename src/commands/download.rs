use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use rusqlite::{Connection, params};
use serde::Deserialize;

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DownloadRecord {
    rid: i64,
    aa_id: String,
    md5: String,
    title: Option<String>,
    original_filename: Option<String>,
    extension: Option<String>,
    local_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DownloadSelector {
    AaId(String),
    Md5(String),
    Isbn(String),
    Doi(String),
}

#[derive(Debug, Deserialize)]
struct FastDownloadResponse {
    download_url: Option<String>,
    error: Option<String>,
}

pub fn run(args: DownloadArgs) -> Result<()> {
    let selector = selector_from_args(&args)?;
    let config = crate::config::load()?;
    let db_path = args.db.clone().unwrap_or(config.db_path()?);
    let output_dir = args.output_dir.clone().unwrap_or(config.download_dir()?);

    let mut conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    crate::db::pragmas::apply_query_pragmas(&conn)?;

    let record = resolve_download_record(&conn, &selector)?;
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
        &record.md5,
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

fn selector_from_args(args: &DownloadArgs) -> Result<DownloadSelector> {
    let selectors = [
        args.aa_id.is_some(),
        args.md5.is_some(),
        args.isbn.is_some(),
        args.doi.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();

    if selectors == 0 {
        bail!("provide exactly one of --aa-id, --md5, --isbn, or --doi")
    }

    if selectors > 1 {
        bail!("use only one of --aa-id, --md5, --isbn, or --doi")
    }

    if let Some(aa_id) = &args.aa_id {
        return Ok(DownloadSelector::AaId(aa_id.trim().to_string()));
    }
    if let Some(md5) = &args.md5 {
        return Ok(DownloadSelector::Md5(md5.trim().to_string()));
    }
    if let Some(isbn) = &args.isbn {
        return Ok(DownloadSelector::Isbn(isbn.trim().to_string()));
    }
    if let Some(doi) = &args.doi {
        return Ok(DownloadSelector::Doi(doi.trim().to_string()));
    }

    bail!("unreachable selector state")
}

fn resolve_download_record(
    conn: &Connection,
    selector: &DownloadSelector,
) -> Result<DownloadRecord> {
    match selector {
        DownloadSelector::AaId(aa_id) => resolve_unique_record(
            conn,
            "SELECT rid, aa_id, md5, title, original_filename, extension, local_path FROM records WHERE aa_id = ?1 LIMIT 2",
            params![aa_id],
        ),
        DownloadSelector::Md5(md5) => resolve_unique_record_by_code(conn, &["md5"], md5),
        DownloadSelector::Isbn(isbn) => {
            resolve_unique_record_by_code(conn, &["isbn10", "isbn13"], isbn)
        }
        DownloadSelector::Doi(doi) => resolve_unique_record_by_code(conn, &["doi"], doi),
    }
}

fn resolve_unique_record_by_code(
    conn: &Connection,
    kinds: &[&str],
    value: &str,
) -> Result<DownloadRecord> {
    let mut sql = String::from(
        "SELECT DISTINCT r.rid, r.aa_id, r.md5, r.title, r.original_filename, r.extension, r.local_path
         FROM records r
         JOIN record_codes rc ON rc.rid = r.rid
         WHERE lower(rc.value) = lower(?1) AND rc.kind IN (",
    );

    for (index, _) in kinds.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push('?');
        sql.push_str(&(index + 2).to_string());
    }
    sql.push_str(") ORDER BY r.rid LIMIT 2");

    let mut params_vec = vec![rusqlite::types::Value::Text(value.to_string())];
    for kind in kinds {
        params_vec.push(rusqlite::types::Value::Text((*kind).to_string()));
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(params_vec),
            map_download_record_row,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    collapse_unique_record(rows)
}

fn resolve_unique_record<P>(conn: &Connection, sql: &str, params: P) -> Result<DownloadRecord>
where
    P: rusqlite::Params,
{
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(params, map_download_record_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    collapse_unique_record(rows)
}

fn map_download_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DownloadRecord> {
    let md5: Option<String> = row.get(2)?;
    Ok(DownloadRecord {
        rid: row.get(0)?,
        aa_id: row.get(1)?,
        md5: md5.unwrap_or_default(),
        title: row.get(3)?,
        original_filename: row.get(4)?,
        extension: row.get(5)?,
        local_path: row.get(6)?,
    })
}

fn collapse_unique_record(rows: Vec<DownloadRecord>) -> Result<DownloadRecord> {
    match rows.as_slice() {
        [] => bail!("no matching record found"),
        [record] => {
            if record.md5.trim().is_empty() {
                bail!("record has no md5; fast download API requires md5")
            }
            Ok(record.clone())
        }
        _ => bail!("multiple records matched; refine the selector"),
    }
}

fn destination_path(output_dir: &Path, record: &DownloadRecord) -> Result<PathBuf> {
    let file_name = if let Some(original_filename) = &record.original_filename {
        sanitize_file_name(original_filename)
    } else {
        let extension = record
            .extension
            .as_deref()
            .filter(|extension| !extension.trim().is_empty())
            .unwrap_or("bin");
        sanitize_file_name(&format!("{}.{extension}", record.aa_id))
    };

    if file_name.is_empty() {
        bail!("could not derive a destination filename")
    }

    Ok(output_dir.join(file_name))
}

fn sanitize_file_name(input: &str) -> String {
    input
        .chars()
        .map(|character| match character {
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => character,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

fn request_fast_download_url(
    client: &Client,
    api_url: &str,
    md5: &str,
    secret_key: &str,
    path_index: Option<u32>,
    domain_index: Option<u32>,
) -> Result<String> {
    let mut request = client
        .get(api_url)
        .query(&[("md5", md5), ("key", secret_key)]);

    if let Some(path_index) = path_index {
        request = request.query(&[("path_index", path_index)]);
    }

    if let Some(domain_index) = domain_index {
        request = request.query(&[("domain_index", domain_index)]);
    }

    let response = request
        .send()
        .context("failed to contact Anna's Archive fast-download API")?;
    let status = response.status();
    let body = response
        .text()
        .context("failed to read fast-download API response")?;

    let parsed: FastDownloadResponse = serde_json::from_str(&body)
        .with_context(|| format!("fast-download API returned invalid JSON with status {status}"))?;

    if !status.is_success() {
        bail!(
            "fast-download API error (status {}): {}",
            status,
            parsed.error.unwrap_or_else(|| "unknown error".to_string())
        );
    }

    parsed
        .download_url
        .filter(|url| !url.trim().is_empty())
        .context("fast-download API returned no download_url")
}

fn download_to_path(
    client: &Client,
    download_url: &str,
    destination: &Path,
    replace_existing: bool,
) -> Result<()> {
    if destination.exists() && replace_existing {
        fs::remove_file(destination)
            .with_context(|| format!("failed to remove {}", destination.display()))?;
    }

    let temporary_path = temporary_download_path(destination);
    if temporary_path.exists() {
        fs::remove_file(&temporary_path)
            .with_context(|| format!("failed to remove {}", temporary_path.display()))?;
    }

    let response = client
        .get(download_url)
        .send()
        .with_context(|| format!("failed to download {download_url}"))?;

    if response.status() != StatusCode::OK {
        bail!("download server returned status {}", response.status())
    }

    let mut response = response;
    let mut file = File::create(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    response
        .copy_to(&mut file)
        .context("failed while streaming download body")?;
    file.flush()?;

    fs::rename(&temporary_path, destination).with_context(|| {
        format!(
            "failed to move {} to {}",
            temporary_path.display(),
            destination.display()
        )
    })?;

    Ok(())
}

fn temporary_download_path(destination: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let extension = destination
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!("{value}.part.{nonce}"))
        .unwrap_or_else(|| format!("part.{nonce}"));

    destination.with_extension(extension)
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
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;

    use reqwest::blocking::Client;
    use rusqlite::Connection;
    use tiny_http::{Method, Response, Server};

    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

    use super::{
        DownloadRecord, DownloadSelector, destination_path, request_fast_download_url,
        resolve_download_record, sanitize_file_name,
    };

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
            resolve_download_record(&conn, &DownloadSelector::Isbn("9780131103627".into()))
                .unwrap();
        assert_eq!(record.aa_id, "aa-1");
        assert_eq!(record.md5, "abcdef0123456789abcdef0123456789");
    }

    #[test]
    fn sanitizes_destination_filenames() {
        assert_eq!(
            sanitize_file_name("bad/name:here?.pdf"),
            "bad_name_here_.pdf"
        );

        let record = DownloadRecord {
            rid: 1,
            aa_id: "aa-1".into(),
            md5: "abcdef0123456789abcdef0123456789".into(),
            title: None,
            original_filename: Some("bad/name.pdf".into()),
            extension: Some("pdf".into()),
            local_path: None,
        };

        let destination = destination_path(Path::new("/tmp/output"), &record).unwrap();
        assert!(destination.ends_with("bad_name.pdf"));
    }

    #[test]
    fn requests_download_url_from_api() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let server = Server::http(address).unwrap();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            assert_eq!(request.method(), &Method::Get);
            assert!(
                request
                    .url()
                    .contains("md5=d6e1dc51a50726f00ec438af21952a45")
            );
            let response = Response::from_string(
                r#"{"download_url":"http://example.invalid/file.pdf","error":null}"#,
            )
            .with_status_code(200);
            request.respond(response).unwrap();
            tx.send(()).unwrap();
        });

        let client = Client::builder().build().unwrap();
        let url = request_fast_download_url(
            &client,
            &format!("http://{address}/dyn/api/fast_download.json"),
            "d6e1dc51a50726f00ec438af21952a45",
            "secret-key",
            None,
            None,
        )
        .unwrap();

        assert_eq!(url, "http://example.invalid/file.pdf");
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}
