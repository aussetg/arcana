use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FastDownloadResponse {
    download_url: Option<String>,
    error: Option<String>,
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
