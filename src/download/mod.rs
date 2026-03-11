use std::fs::{self, File};
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use md5::Context as Md5Context;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::records::RecordSummary;

#[derive(Debug, Deserialize)]
struct FastDownloadResponse {
    download_url: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FilenameMode {
    Original,
    AaId,
    Md5,
    Flat,
}

impl Default for FilenameMode {
    fn default() -> Self {
        Self::Original
    }
}

pub fn destination_path(
    output_dir: &Path,
    record: &RecordSummary,
    filename_mode: FilenameMode,
    output_name: Option<&str>,
) -> Result<PathBuf> {
    let file_name = if let Some(output_name) = output_name {
        sanitize_file_name(output_name)
    } else {
        default_file_name(record, filename_mode)
    };

    if file_name.is_empty() {
        bail!("could not derive a destination filename")
    }

    Ok(output_dir.join(file_name))
}

pub fn file_matches_md5(path: &Path, expected_md5: &str) -> Result<bool> {
    Ok(file_md5_hex(path)?.eq_ignore_ascii_case(expected_md5))
}

pub fn verify_file_md5(path: &Path, expected_md5: &str) -> Result<()> {
    let actual = file_md5_hex(path)?;
    if actual.eq_ignore_ascii_case(expected_md5) {
        return Ok(());
    }

    bail!(
        "md5 mismatch for {}: expected {}, got {}",
        path.display(),
        expected_md5,
        actual
    )
}

pub fn request_fast_download_url(
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
            "fast-download API error for md5 {} (status {}): {}",
            md5,
            status,
            parsed.error.unwrap_or_else(|| "unknown error".to_string())
        );
    }

    parsed
        .download_url
        .filter(|url| !url.trim().is_empty())
        .with_context(|| format!("fast-download API returned no download_url for md5 {md5}"))
}

pub fn download_to_path(
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

pub fn sanitize_file_name(input: &str) -> String {
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

fn default_file_name(record: &RecordSummary, filename_mode: FilenameMode) -> String {
    let extension = record
        .extension
        .as_deref()
        .filter(|extension| !extension.trim().is_empty())
        .unwrap_or("bin");

    match filename_mode {
        FilenameMode::Original => {
            if let Some(original_filename) = &record.original_filename {
                sanitize_file_name(original_filename)
            } else {
                sanitize_file_name(&format!("{}.{extension}", record.aa_id))
            }
        }
        FilenameMode::AaId => sanitize_file_name(&format!("{}.{extension}", record.aa_id)),
        FilenameMode::Md5 => sanitize_file_name(&format!(
            "{}.{}",
            record.md5.as_deref().unwrap_or(&record.aa_id),
            extension
        )),
        FilenameMode::Flat => {
            sanitize_file_name(&format!("{}.{}", flat_base_name(record), extension))
        }
    }
}

fn flat_base_name(record: &RecordSummary) -> String {
    let mut parts = Vec::new();

    if let Some(title) = record
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(title.trim().to_string());
    }

    if let Some(author) = record
        .author
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(author.trim().to_string());
    }

    if parts.is_empty() {
        record.aa_id.clone()
    } else {
        parts.join(" -- ")
    }
}

fn file_md5_hex(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut context = Md5Context::new();
    let mut buffer = [0u8; 8192];

    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        context.consume(&buffer[..read]);
    }

    Ok(format!("{:x}", context.compute()))
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use reqwest::blocking::Client;
    use tiny_http::{Method, Response, Server};

    use crate::records::RecordSummary;

    use super::{
        FilenameMode, destination_path, file_matches_md5, request_fast_download_url,
        sanitize_file_name, verify_file_md5,
    };

    #[test]
    fn sanitizes_destination_filenames() {
        assert_eq!(
            sanitize_file_name("bad/name:here?.pdf"),
            "bad_name_here_.pdf"
        );

        let record = RecordSummary {
            rid: 1,
            aa_id: "aa-1".into(),
            md5: Some("abcdef0123456789abcdef0123456789".into()),
            title: None,
            author: None,
            original_filename: Some("bad/name.pdf".into()),
            extension: Some("pdf".into()),
            local_path: None,
        };

        let destination = destination_path(
            Path::new("/tmp/output"),
            &record,
            FilenameMode::Original,
            None,
        )
        .unwrap();
        assert!(destination.ends_with("bad_name.pdf"));
    }

    #[test]
    fn supports_output_name_and_filename_modes() {
        let record = RecordSummary {
            rid: 1,
            aa_id: "aa-1".into(),
            md5: Some("abcdef0123456789abcdef0123456789".into()),
            title: Some("Deep Learning".into()),
            author: Some("Ian Goodfellow".into()),
            original_filename: Some("nested/original.pdf".into()),
            extension: Some("pdf".into()),
            local_path: None,
        };

        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::AaId, None,)
                .unwrap()
                .ends_with("aa-1.pdf")
        );
        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::Md5, None,)
                .unwrap()
                .ends_with("abcdef0123456789abcdef0123456789.pdf")
        );
        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::Flat, None,)
                .unwrap()
                .ends_with("Deep Learning -- Ian Goodfellow.pdf")
        );
        assert!(
            destination_path(
                Path::new("/tmp/output"),
                &record,
                FilenameMode::Flat,
                Some("custom/name.pdf"),
            )
            .unwrap()
            .ends_with("custom_name.pdf")
        );
    }

    #[test]
    fn verifies_existing_file_md5() {
        let root = std::env::temp_dir().join(format!(
            "arcana-download-md5-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("file.bin");
        fs::write(&path, b"hello").unwrap();

        let expected = format!("{:x}", md5::compute(b"hello"));
        assert!(file_matches_md5(&path, &expected).unwrap());
        assert!(verify_file_md5(&path, &expected).is_ok());
        assert!(verify_file_md5(&path, "deadbeef").is_err());

        let _ = fs::remove_dir_all(root);
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

    #[test]
    fn surfaces_api_errors_with_md5_context() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let server = Server::http(address).unwrap();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            let response =
                Response::from_string(r#"{"error":"invalid key"}"#).with_status_code(403);
            request.respond(response).unwrap();
            tx.send(()).unwrap();
        });

        let client = Client::builder().build().unwrap();
        let error = request_fast_download_url(
            &client,
            &format!("http://{address}/dyn/api/fast_download.json"),
            "d6e1dc51a50726f00ec438af21952a45",
            "bad-key",
            None,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("d6e1dc51a50726f00ec438af21952a45"));
        assert!(error.contains("invalid key"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}
