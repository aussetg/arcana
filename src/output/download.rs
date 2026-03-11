use anyhow::Result;
use serde::Serialize;

use crate::download::{DownloadOptions, FilenameMode};
use crate::output::report::REPORT_VERSION;
use crate::output::report::print_json as print_report_json;
use crate::records::{RecordSelector, RecordSummary};

#[derive(Debug, Clone, Serialize)]
pub struct DownloadReport {
    pub report_version: u32,
    pub kind: &'static str,
    pub request: DownloadRequest,
    pub record: DownloadRecord,
    pub destination: String,
    pub outcome: DownloadOutcome,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownloadRequest {
    pub selector: DownloadSelector,
    pub output_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_name: Option<String>,
    pub filename_mode: FilenameMode,
    pub replace_existing: bool,
    pub verify_md5: bool,
    pub no_link: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_index: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownloadSelector {
    pub mode: DownloadSelectorMode,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadSelectorMode {
    AaId,
    Md5,
    Isbn,
    Doi,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownloadRecord {
    pub aa_id: String,
    pub md5: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownloadOutcome {
    pub status: DownloadStatus,
    pub network_used: bool,
    pub resumed_partial: bool,
    pub restarted_partial: bool,
    pub md5_checked: bool,
    pub md5_ok: Option<bool>,
    pub local_path_updated: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Downloaded,
    AlreadyPresent,
}

pub fn print_text(report: &DownloadReport) {
    match report.outcome.status {
        DownloadStatus::Downloaded => println!("downloaded: {}", report.destination),
        DownloadStatus::AlreadyPresent => {
            println!("already present and verified: {}", report.destination)
        }
    }
}

pub fn print_json(report: &DownloadReport) -> Result<()> {
    print_report_json(report)
}

impl From<&RecordSelector> for DownloadSelector {
    fn from(value: &RecordSelector) -> Self {
        match value {
            RecordSelector::AaId(text) => Self {
                mode: DownloadSelectorMode::AaId,
                value: text.clone(),
            },
            RecordSelector::Md5(text) => Self {
                mode: DownloadSelectorMode::Md5,
                value: text.clone(),
            },
            RecordSelector::Isbn(text) => Self {
                mode: DownloadSelectorMode::Isbn,
                value: text.clone(),
            },
            RecordSelector::Doi(text) => Self {
                mode: DownloadSelectorMode::Doi,
                value: text.clone(),
            },
        }
    }
}

impl DownloadReport {
    pub fn new(
        selector: &RecordSelector,
        record: &RecordSummary,
        options: &DownloadOptions,
        destination: &std::path::Path,
        outcome: DownloadOutcome,
    ) -> Self {
        Self {
            report_version: REPORT_VERSION,
            kind: "download_report",
            request: DownloadRequest {
                selector: selector.into(),
                output_dir: options.output_dir.display().to_string(),
                output_name: options.output_name.clone(),
                filename_mode: options.filename_mode,
                replace_existing: options.replace_existing,
                verify_md5: options.verify_md5,
                no_link: options.no_link,
                path_index: options.path_index,
                domain_index: options.domain_index,
            },
            record: DownloadRecord {
                aa_id: record.aa_id.clone(),
                md5: record.md5.clone().unwrap_or_default(),
                title: record.title.clone(),
                author: record.author.clone(),
                extension: record.extension.clone(),
                original_filename: record.original_filename.clone(),
            },
            destination: destination.display().to_string(),
            outcome,
        }
    }
}
