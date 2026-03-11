use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_SECRET_KEY_ENV: &str = "ANNAS_ARCHIVE_SECRET_KEY";
const DEFAULT_FAST_DOWNLOAD_API_URL: &str = "https://annas-archive.gl/dyn/api/fast_download.json";
const DEFAULT_EXPAND_COMMAND: &str = "llama-cli";
const DEFAULT_EXPAND_MODEL: &str = "~/Models/models--unsloth--Qwen3.5-0.8B-GGUF/snapshots/6ab461498e2023f6e3c1baea90a8f0fe38ab64d0/Qwen3.5-0.8B-UD-Q4_K_XL.gguf";
const DEFAULT_EXPAND_TIMEOUT_SECS: u64 = 8;

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

pub fn save(path: &Path, config: &FileConfig) -> Result<()> {
    let serialized = serde_yaml::to_string(config).context("failed to serialize config")?;
    fs::write(path, serialized).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub fn starter() -> Result<FileConfig> {
    let db_path = crate::config::config_dir()?.join("arcana.sqlite3");
    let download_dir = crate::config::path::xdg_user_dir("DOWNLOAD")?
        .or_else(|| crate::config::path::home_dir().map(|home| home.join("Downloads")))
        .unwrap_or_else(|| {
            db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        });
    let expand_cache_path = crate::search::expand::default_cache_path(&db_path);

    Ok(FileConfig {
        db_path: Some(crate::config::compact_tilde_path(db_path)),
        download_dir: Some(crate::config::compact_tilde_path(download_dir)),
        secret_key_env: Some(DEFAULT_SECRET_KEY_ENV.to_string()),
        fast_download_api_url: Some(DEFAULT_FAST_DOWNLOAD_API_URL.to_string()),
        expand_cache_path: Some(crate::config::compact_tilde_path(expand_cache_path)),
        expand_model_path: Some(DEFAULT_EXPAND_MODEL.to_string()),
        expand_command: Some(DEFAULT_EXPAND_COMMAND.to_string()),
        expand_timeout_secs: Some(DEFAULT_EXPAND_TIMEOUT_SECS),
    })
}
