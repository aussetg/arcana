use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use rusqlite::{Connection, params};

use crate::config::ResolvedConfig;
use crate::output::download::{DownloadOutcome, DownloadStatus};
use crate::records::{RecordSelector, RecordSummary, resolve_unique_record};

use super::{
    FilenameMode, destination_path, download_to_path, file_matches_md5, request_fast_download_url,
    verify_file_md5,
};

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub output_dir: PathBuf,
    pub path_index: Option<u32>,
    pub domain_index: Option<u32>,
    pub replace_existing: bool,
    pub verify_md5: bool,
    pub output_name: Option<String>,
    pub filename_mode: FilenameMode,
    pub no_link: bool,
}

#[derive(Debug, Clone)]
pub struct DownloadExecution {
    pub record: RecordSummary,
    pub destination: PathBuf,
    pub outcome: DownloadOutcome,
}

pub fn execute_download(
    conn: &mut Connection,
    config: &ResolvedConfig,
    selector: &RecordSelector,
    options: &DownloadOptions,
) -> Result<DownloadExecution> {
    let record = resolve_unique_record(conn, selector)?;
    let record_md5 = record
        .md5
        .as_deref()
        .filter(|md5| !md5.trim().is_empty())
        .context("record has no md5; fast download API requires md5")?;
    let destination = destination_path(
        &options.output_dir,
        &record,
        options.filename_mode,
        options.output_name.as_deref(),
    )?;

    fs::create_dir_all(&options.output_dir)
        .with_context(|| format!("failed to create {}", options.output_dir.display()))?;

    if let Some(outcome) =
        maybe_use_existing_destination(conn, &record, &destination, record_md5, options)?
    {
        return Ok(DownloadExecution {
            record,
            destination,
            outcome,
        });
    }

    let outcome = download_missing_file(conn, config, &record, &destination, record_md5, options)?;

    Ok(DownloadExecution {
        record,
        destination,
        outcome,
    })
}

fn maybe_use_existing_destination(
    conn: &mut Connection,
    record: &RecordSummary,
    destination: &PathBuf,
    record_md5: &str,
    options: &DownloadOptions,
) -> Result<Option<DownloadOutcome>> {
    if !destination.exists() {
        return Ok(None);
    }

    if file_matches_md5(destination, record_md5)? {
        let local_path_updated =
            maybe_update_local_path(conn, record.rid, destination, options.no_link)?;
        return Ok(Some(DownloadOutcome {
            status: DownloadStatus::AlreadyPresent,
            network_used: false,
            resumed_partial: false,
            restarted_partial: false,
            md5_checked: true,
            md5_ok: Some(true),
            local_path_updated,
        }));
    }

    if !options.replace_existing {
        bail!(
            "destination already exists but md5 does not match expected record: {} (pass --replace-existing to overwrite)",
            destination.display()
        );
    }

    Ok(None)
}

fn download_missing_file(
    conn: &mut Connection,
    config: &ResolvedConfig,
    record: &RecordSummary,
    destination: &PathBuf,
    record_md5: &str,
    options: &DownloadOptions,
) -> Result<DownloadOutcome> {
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
        options.path_index,
        options.domain_index,
    )?;

    let transfer = download_to_path(
        &client,
        &download_url,
        destination,
        options.replace_existing,
    )?;

    if options.verify_md5 {
        if let Err(error) = verify_file_md5(destination, record_md5) {
            let _ = fs::remove_file(destination);
            return Err(error).context("download verification failed; removed downloaded file");
        }
    }

    let local_path_updated =
        maybe_update_local_path(conn, record.rid, destination, options.no_link)?;

    Ok(DownloadOutcome {
        status: DownloadStatus::Downloaded,
        network_used: true,
        resumed_partial: transfer.resumed_partial,
        restarted_partial: transfer.restarted_partial,
        md5_checked: options.verify_md5,
        md5_ok: options.verify_md5.then_some(true),
        local_path_updated,
    })
}

fn maybe_update_local_path(
    conn: &mut Connection,
    rid: i64,
    destination: &PathBuf,
    no_link: bool,
) -> Result<bool> {
    if no_link {
        return Ok(false);
    }

    conn.execute(
        "UPDATE records SET local_path = ?1 WHERE rid = ?2",
        params![destination.display().to_string(), rid],
    )?;
    Ok(true)
}
