use anyhow::Result;
use serde::Serialize;

use crate::config::{Resolved, ResolvedConfig};

pub fn print_text(config: &ResolvedConfig) -> Result<()> {
    println!("config_file: {}", config.config_file.display());
    println!("config_exists: {}", config.config_exists);
    println!(
        "db_path: {} [{}]",
        config.db_path.value.display(),
        config.db_path.source
    );
    println!(
        "download_dir: {} [{}]",
        config.download_dir.value.display(),
        config.download_dir.source
    );
    println!(
        "secret_key_env: {} [{}]",
        config.secret_key_env.value, config.secret_key_env.source
    );
    println!("secret_key_set: {}", config.secret_key_is_set());
    println!(
        "fast_download_api_url: {} [{}]",
        config.fast_download_api_url.value, config.fast_download_api_url.source
    );
    println!(
        "expand_cache_path: {} [{}]",
        config.expand_cache_path.value.display(),
        config.expand_cache_path.source
    );
    println!(
        "expand_command: {} [{}]",
        config.expand_command.value, config.expand_command.source
    );
    println!(
        "expand_model_path: {} [{}]",
        config.expand_model_path.value.display(),
        config.expand_model_path.source
    );
    println!(
        "expand_timeout_secs: {} [{}]",
        config.expand_timeout_secs.value, config.expand_timeout_secs.source
    );

    Ok(())
}

pub fn print_json(config: &ResolvedConfig) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&ConfigReport::from(config))?
    );
    Ok(())
}

pub fn print_json_path(path: &std::path::Path, exists: bool) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&ConfigPathReport {
            report_version: 1,
            kind: "config_path_report",
            path: path.display().to_string(),
            exists,
        })?
    );
    Ok(())
}

pub fn print_json_init(path: &std::path::Path, overwritten: bool) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&ConfigInitReport {
            report_version: 1,
            kind: "config_init_report",
            path: path.display().to_string(),
            created: true,
            overwritten,
        })?
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct ConfigReport {
    report_version: u32,
    kind: &'static str,
    config_file: String,
    config_exists: bool,
    values: ConfigValues,
}

#[derive(Debug, Serialize)]
struct ConfigValues {
    db_path: ConfigValue<String>,
    download_dir: ConfigValue<String>,
    secret_key_env: ConfigValue<String>,
    secret_key_set: bool,
    fast_download_api_url: ConfigValue<String>,
    expand_cache_path: ConfigValue<String>,
    expand_command: ConfigValue<String>,
    expand_model_path: ConfigValue<String>,
    expand_timeout_secs: ConfigValue<u64>,
}

#[derive(Debug, Serialize)]
struct ConfigValue<T> {
    value: T,
    source: crate::config::ValueSource,
}

#[derive(Debug, Serialize)]
struct ConfigPathReport {
    report_version: u32,
    kind: &'static str,
    path: String,
    exists: bool,
}

#[derive(Debug, Serialize)]
struct ConfigInitReport {
    report_version: u32,
    kind: &'static str,
    path: String,
    created: bool,
    overwritten: bool,
}

impl From<&ResolvedConfig> for ConfigReport {
    fn from(config: &ResolvedConfig) -> Self {
        Self {
            report_version: 1,
            kind: "config_report",
            config_file: config.config_file.display().to_string(),
            config_exists: config.config_exists,
            values: ConfigValues {
                db_path: config_path_value(&config.db_path),
                download_dir: config_path_value(&config.download_dir),
                secret_key_env: config_string_value(&config.secret_key_env),
                secret_key_set: config.secret_key_is_set(),
                fast_download_api_url: config_string_value(&config.fast_download_api_url),
                expand_cache_path: config_path_value(&config.expand_cache_path),
                expand_command: config_string_value(&config.expand_command),
                expand_model_path: config_path_value(&config.expand_model_path),
                expand_timeout_secs: ConfigValue {
                    value: config.expand_timeout_secs.value,
                    source: config.expand_timeout_secs.source,
                },
            },
        }
    }
}

fn config_path_value(value: &Resolved<std::path::PathBuf>) -> ConfigValue<String> {
    ConfigValue {
        value: value.value.display().to_string(),
        source: value.source,
    }
}

fn config_string_value(value: &Resolved<String>) -> ConfigValue<String> {
    ConfigValue {
        value: value.value.clone(),
        source: value.source,
    }
}
