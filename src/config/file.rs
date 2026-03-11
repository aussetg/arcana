use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FileConfig {
    pub db_path: Option<String>,
    pub download_dir: Option<String>,
    pub secret_key_env: Option<String>,
    pub fast_download_api_url: Option<String>,
    pub expand_cache_path: Option<String>,
    pub expand_model_path: Option<String>,
    pub expand_command: Option<String>,
    pub expand_timeout_secs: Option<u64>,
}

pub fn load(path: &Path) -> Result<FileConfig> {
    if !path.exists() {
        return Ok(FileConfig::default());
    }

    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let config: FileConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(config)
}
