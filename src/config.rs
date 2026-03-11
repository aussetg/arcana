use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_SECRET_KEY_ENV: &str = "ANNAS_ARCHIVE_SECRET_KEY";
const DEFAULT_FAST_DOWNLOAD_API_URL: &str = "https://annas-archive.gl/dyn/api/fast_download.json";
const DEFAULT_EXPAND_COMMAND: &str = "llama-cli";
const DEFAULT_EXPAND_MODEL: &str = "~/Models/models--unsloth--Qwen3.5-0.8B-GGUF/snapshots/6ab461498e2023f6e3c1baea90a8f0fe38ab64d0/Qwen3.5-0.8B-UD-Q4_K_XL.gguf";
const DEFAULT_EXPAND_TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    pub db_path: Option<String>,
    pub download_dir: Option<String>,
    pub secret_key_env: Option<String>,
    pub fast_download_api_url: Option<String>,
    pub expand_cache_path: Option<String>,
    pub expand_model_path: Option<String>,
    pub expand_command: Option<String>,
    pub expand_timeout_secs: Option<u64>,
}

pub fn load() -> Result<AppConfig> {
    let path = config_file_path()?;
    if !path.exists() {
        return Ok(AppConfig::default());
    }

    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let config: AppConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(config)
}

pub fn config_dir() -> Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path).join("arcana"));
    }

    let home = home_dir().context("HOME is not set")?;
    Ok(home.join(".config/arcana"))
}

pub fn config_file_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.yaml"))
}

impl AppConfig {
    pub fn db_path(&self) -> Result<PathBuf> {
        Ok(self
            .db_path
            .as_deref()
            .map(expand_tilde_path)
            .unwrap_or(config_dir()?.join("arcana.sqlite3")))
    }

    pub fn download_dir(&self) -> Result<PathBuf> {
        if let Some(path) = self.download_dir.as_deref() {
            return Ok(expand_tilde_path(path));
        }

        if let Some(path) = xdg_user_dir("DOWNLOAD")? {
            return Ok(path);
        }

        let home = home_dir().context("HOME is not set")?;
        Ok(home.join("Downloads"))
    }

    pub fn secret_key_env(&self) -> &str {
        self.secret_key_env
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(DEFAULT_SECRET_KEY_ENV)
    }

    pub fn fast_download_api_url(&self) -> &str {
        self.fast_download_api_url
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(DEFAULT_FAST_DOWNLOAD_API_URL)
    }

    pub fn expand_cache_path(&self) -> Option<PathBuf> {
        self.expand_cache_path.as_deref().map(expand_tilde_path)
    }

    pub fn expand_model_path(&self) -> PathBuf {
        self.expand_model_path
            .as_deref()
            .map(expand_tilde_path)
            .unwrap_or_else(|| expand_tilde_path(DEFAULT_EXPAND_MODEL))
    }

    pub fn expand_command(&self) -> &str {
        self.expand_command
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(DEFAULT_EXPAND_COMMAND)
    }

    pub fn expand_timeout_secs(&self) -> u64 {
        self.expand_timeout_secs
            .unwrap_or(DEFAULT_EXPAND_TIMEOUT_SECS)
    }

    pub fn secret_key(&self) -> Result<String> {
        let env_name = self.secret_key_env();
        env::var(env_name).with_context(|| format!("{env_name} is not set"))
    }
}

pub fn expand_tilde_path<P>(path: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let path_text = path.to_string_lossy();
    if !path_text.starts_with("~/") {
        return path.to_path_buf();
    }

    match home_dir() {
        Some(home) => home.join(&path_text[2..]),
        None => path.to_path_buf(),
    }
}

fn xdg_user_dir(kind: &str) -> Result<Option<PathBuf>> {
    let env_key = format!("XDG_{kind}_DIR");
    if let Some(value) = env::var_os(&env_key) {
        return Ok(Some(expand_xdg_path(&value.to_string_lossy())));
    }

    let user_dirs_path = if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("user-dirs.dirs")
    } else {
        match home_dir() {
            Some(home) => home.join(".config/user-dirs.dirs"),
            None => return Ok(None),
        }
    };

    if !user_dirs_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&user_dirs_path)
        .with_context(|| format!("failed to read {}", user_dirs_path.display()))?;

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        if let Some(value) = line.strip_prefix(&format!("{env_key}=")) {
            let value = value.trim().trim_matches('"');
            return Ok(Some(expand_xdg_path(value)));
        }
    }

    Ok(None)
}

fn expand_xdg_path(value: &str) -> PathBuf {
    if let Some(home) = home_dir() {
        if let Some(rest) = value.strip_prefix("$HOME/") {
            return home.join(rest);
        }
        if value == "$HOME" {
            return home;
        }
    }

    expand_tilde_path(value)
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, expand_tilde_path, expand_xdg_path};

    #[test]
    fn expands_tilde_paths() {
        let path = expand_tilde_path("~/example/file.txt");
        assert!(path.to_string_lossy().contains("example/file.txt"));
    }

    #[test]
    fn uses_configured_paths() {
        let config = AppConfig {
            db_path: Some("~/db/arcana.sqlite3".into()),
            download_dir: Some("~/downloads/arcana".into()),
            secret_key_env: Some("AA_KEY".into()),
            ..AppConfig::default()
        };

        assert!(
            config
                .db_path()
                .unwrap()
                .to_string_lossy()
                .contains("arcana.sqlite3")
        );
        assert!(
            config
                .download_dir()
                .unwrap()
                .to_string_lossy()
                .contains("downloads/arcana")
        );
        assert_eq!(config.secret_key_env(), "AA_KEY");
    }

    #[test]
    fn expands_xdg_home_token() {
        let path = expand_xdg_path("$HOME/Downloads");
        assert!(path.to_string_lossy().ends_with("Downloads"));
    }
}
