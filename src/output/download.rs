use anyhow::Result;
use serde::Serialize;

use crate::download::FilenameMode;
use crate::records::RecordSelector;

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
    pub md5_checked: bool,
    pub md5_ok: bool,
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
    println!("{}", serde_json::to_string_pretty(report)?);
    Ok(())
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
