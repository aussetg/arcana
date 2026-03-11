use std::fs::{self, File};
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use md5::Context as Md5Context;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::RANGE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadTransferResult {
    pub resumed_partial: bool,
    pub restarted_partial: bool,
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

pub fn download_to_path(
    client: &Client,
    download_url: &str,
    destination: &Path,
    replace_existing: bool,
) -> Result<DownloadTransferResult> {
    if destination.exists() && !destination.is_file() {
        bail!(
            "destination exists but is not a regular file: {}",
            destination.display()
        )
    }

    if destination.exists() && replace_existing {
        fs::remove_file(destination)
            .with_context(|| format!("failed to remove {}", destination.display()))?;
    }

    let partial_path = partial_download_path(destination);
    if partial_path.exists() && !partial_path.is_file() {
        bail!(
            "partial download path exists but is not a regular file: {}",
            partial_path.display()
        )
    }
    if replace_existing && partial_path.exists() {
        fs::remove_file(&partial_path)
            .with_context(|| format!("failed to remove {}", partial_path.display()))?;
    }

    let existing_partial_len = partial_path.metadata().map(|meta| meta.len()).unwrap_or(0);

    let mut request = client.get(download_url);
    if existing_partial_len > 0 {
        request = request.header(RANGE, format!("bytes={existing_partial_len}-"));
    }

    let response = request
        .send()
        .with_context(|| format!("failed to download {download_url}"))?;

    let status = response.status();

    let transfer = match status {
        StatusCode::OK => {
            write_response_to_partial(response, &partial_path, false)?;
            DownloadTransferResult {
                resumed_partial: false,
                restarted_partial: existing_partial_len > 0,
            }
        }
        StatusCode::PARTIAL_CONTENT => {
            write_response_to_partial(response, &partial_path, existing_partial_len > 0)?;
            DownloadTransferResult {
                resumed_partial: existing_partial_len > 0,
                restarted_partial: false,
            }
        }
        StatusCode::RANGE_NOT_SATISFIABLE if existing_partial_len > 0 => {
            fs::remove_file(&partial_path)
                .with_context(|| format!("failed to remove {}", partial_path.display()))?;
            let retry = client
                .get(download_url)
                .send()
                .with_context(|| format!("failed to retry download {download_url}"))?;
            if retry.status() != StatusCode::OK {
                bail!("download server returned status {}", retry.status())
            }
            write_response_to_partial(retry, &partial_path, false)?;
            DownloadTransferResult {
                resumed_partial: false,
                restarted_partial: true,
            }
        }
        _ => bail!("download server returned status {}", status),
    };

    fs::rename(&partial_path, destination).with_context(|| {
        format!(
            "failed to move {} to {}",
            partial_path.display(),
            destination.display()
        )
    })?;

    Ok(transfer)
}

fn partial_download_path(destination: &Path) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .map(|value| format!("{value}.part"))
        .unwrap_or_else(|| {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("arcana-download-{nonce}.part")
        });

    destination.with_file_name(file_name)
}

fn write_response_to_partial(
    mut response: reqwest::blocking::Response,
    partial_path: &Path,
    append: bool,
) -> Result<()> {
    let mut file = if append {
        File::options()
            .append(true)
            .open(partial_path)
            .with_context(|| format!("failed to open {}", partial_path.display()))?
    } else {
        File::create(partial_path)
            .with_context(|| format!("failed to create {}", partial_path.display()))?
    };

    response
        .copy_to(&mut file)
        .context("failed while streaming download body")?;
    file.flush()?;
    Ok(())
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
