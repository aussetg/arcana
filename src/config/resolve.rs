use std::env;
use std::fmt;
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::config::file::load as load_file;
use crate::config::path::{
    config_dir, config_file_path, expand_tilde_path, home_dir, xdg_user_dir,
};

const DEFAULT_SECRET_KEY_ENV: &str = "ANNAS_ARCHIVE_SECRET_KEY";
const DEFAULT_FAST_DOWNLOAD_API_URL: &str = "https://annas-archive.gl/dyn/api/fast_download.json";
const DEFAULT_EXPAND_COMMAND: &str = "llama-cli";
const DEFAULT_EXPAND_MODEL: &str = "~/Models/models--unsloth--Qwen3.5-0.8B-GGUF/snapshots/6ab461498e2023f6e3c1baea90a8f0fe38ab64d0/Qwen3.5-0.8B-UD-Q4_K_XL.gguf";
const DEFAULT_EXPAND_TIMEOUT_SECS: u64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueSource {
    ConfigFile,
    Xdg,
    Default,
}

impl fmt::Display for ValueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::ConfigFile => "config_file",
            Self::Xdg => "xdg",
            Self::Default => "default",
        };
        f.write_str(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved<T> {
    pub value: T,
    pub source: ValueSource,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub config_file: PathBuf,
    pub config_exists: bool,
    pub db_path: Resolved<PathBuf>,
    pub download_dir: Resolved<PathBuf>,
    pub secret_key_env: Resolved<String>,
    pub fast_download_api_url: Resolved<String>,
    pub expand_cache_path: Resolved<PathBuf>,
    pub expand_model_path: Resolved<PathBuf>,
    pub expand_command: Resolved<String>,
    pub expand_timeout_secs: Resolved<u64>,
}

pub fn resolve() -> Result<ResolvedConfig> {
    let config_file = config_file_path()?;
    let config_exists = config_file.exists();
    let file = load_file(&config_file)?;

    let db_path = file
        .db_path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| Resolved {
            value: expand_tilde_path(value),
            source: ValueSource::ConfigFile,
        })
        .unwrap_or(Resolved {
            value: config_dir()?.join("arcana.sqlite3"),
            source: ValueSource::Default,
        });

    let download_dir = if let Some(value) = file
        .download_dir
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Resolved {
            value: expand_tilde_path(value),
            source: ValueSource::ConfigFile,
        }
    } else if let Some(value) = xdg_user_dir("DOWNLOAD")? {
        Resolved {
            value,
            source: ValueSource::Xdg,
        }
    } else {
        let home = home_dir().context("HOME is not set")?;
        Resolved {
            value: home.join("Downloads"),
            source: ValueSource::Default,
        }
    };

    let secret_key_env = resolve_string(file.secret_key_env.as_deref(), DEFAULT_SECRET_KEY_ENV);
    let fast_download_api_url = resolve_string(
        file.fast_download_api_url.as_deref(),
        DEFAULT_FAST_DOWNLOAD_API_URL,
    );
    let expand_command = resolve_string(file.expand_command.as_deref(), DEFAULT_EXPAND_COMMAND);

    let expand_model_path = file
        .expand_model_path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| Resolved {
            value: expand_tilde_path(value),
            source: ValueSource::ConfigFile,
        })
        .unwrap_or(Resolved {
            value: expand_tilde_path(DEFAULT_EXPAND_MODEL),
            source: ValueSource::Default,
        });

    let expand_cache_path = file
        .expand_cache_path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| Resolved {
            value: expand_tilde_path(value),
            source: ValueSource::ConfigFile,
        })
        .unwrap_or(Resolved {
            value: crate::search::expand::default_cache_path(&db_path.value),
            source: ValueSource::Default,
        });

    let expand_timeout_secs = file
        .expand_timeout_secs
        .map(|value| Resolved {
            value,
            source: ValueSource::ConfigFile,
        })
        .unwrap_or(Resolved {
            value: DEFAULT_EXPAND_TIMEOUT_SECS,
            source: ValueSource::Default,
        });

    Ok(ResolvedConfig {
        config_file,
        config_exists,
        db_path,
        download_dir,
        secret_key_env,
        fast_download_api_url,
        expand_cache_path,
        expand_model_path,
        expand_command,
        expand_timeout_secs,
    })
}

fn resolve_string(config_value: Option<&str>, default: &str) -> Resolved<String> {
    config_value
        .filter(|value| !value.trim().is_empty())
        .map(|value| Resolved {
            value: value.to_string(),
            source: ValueSource::ConfigFile,
        })
        .unwrap_or_else(|| Resolved {
            value: default.to_string(),
            source: ValueSource::Default,
        })
}

impl ResolvedConfig {
    pub fn db_path(&self) -> PathBuf {
        self.db_path.value.clone()
    }

    pub fn download_dir(&self) -> PathBuf {
        self.download_dir.value.clone()
    }

    pub fn secret_key_env(&self) -> &str {
        &self.secret_key_env.value
    }

    pub fn fast_download_api_url(&self) -> &str {
        &self.fast_download_api_url.value
    }

    pub fn expand_cache_path(&self) -> PathBuf {
        self.expand_cache_path.value.clone()
    }

    pub fn expand_model_path(&self) -> PathBuf {
        self.expand_model_path.value.clone()
    }

    pub fn expand_command(&self) -> &str {
        &self.expand_command.value
    }

    pub fn expand_timeout_secs(&self) -> u64 {
        self.expand_timeout_secs.value
    }

    pub fn secret_key(&self) -> Result<String> {
        let env_name = self.secret_key_env();
        env::var(env_name).with_context(|| format!("{env_name} is not set"))
    }

    pub fn secret_key_is_set(&self) -> bool {
        env::var_os(self.secret_key_env()).is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::file::FileConfig;
    use crate::config::resolve::{Resolved, ValueSource, resolve_string};

    #[test]
    fn prefers_non_empty_config_strings() {
        let resolved = resolve_string(Some("AA_KEY"), "ANNAS_ARCHIVE_SECRET_KEY");
        assert_eq!(resolved.value, "AA_KEY");
        assert_eq!(resolved.source, ValueSource::ConfigFile);
    }

    #[test]
    fn falls_back_for_blank_config_strings() {
        let resolved = resolve_string(Some("   "), "ANNAS_ARCHIVE_SECRET_KEY");
        assert_eq!(resolved.value, "ANNAS_ARCHIVE_SECRET_KEY");
        assert_eq!(resolved.source, ValueSource::Default);
    }

    #[test]
    fn file_config_still_deserializes() {
        let config = serde_yaml::from_str::<FileConfig>(
            "db_path: \"~/db/arcana.sqlite3\"\ndownload_dir: \"~/downloads/arcana\"\n",
        )
        .unwrap();
        assert_eq!(config.db_path.as_deref(), Some("~/db/arcana.sqlite3"));
        assert_eq!(config.download_dir.as_deref(), Some("~/downloads/arcana"));
    }

    #[test]
    fn resolved_value_keeps_source() {
        let value = Resolved {
            value: 8u64,
            source: ValueSource::Default,
        };
        assert_eq!(value.value, 8);
        assert_eq!(value.source, ValueSource::Default);
    }
}
