use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::records::RecordSummary;

#[derive(Debug, Deserialize)]
struct FastDownloadResponse {
    download_url: Option<String>,
    error: Option<String>,
}

pub fn destination_path(output_dir: &Path, record: &RecordSummary) -> Result<PathBuf> {
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
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;

    use reqwest::blocking::Client;
    use tiny_http::{Method, Response, Server};

    use crate::records::RecordSummary;

    use super::{destination_path, request_fast_download_url, sanitize_file_name};

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
