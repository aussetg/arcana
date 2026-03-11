use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct ConfigArgs {}

pub fn run(_args: ConfigArgs) -> Result<()> {
    let config = crate::config::load()?;
    let config_file = crate::config::config_file_path()?;
    let config_exists = config_file.exists();
    let db_path = config.db_path()?;
    let download_dir = config.download_dir()?;
    let expand_cache_path = config
        .expand_cache_path()
        .unwrap_or_else(|| crate::search::expand::default_cache_path(&db_path));
    let expand_model_path = config.expand_model_path();
    let secret_env = config.secret_key_env().to_string();
    let secret_is_set = std::env::var_os(&secret_env).is_some();

    println!("config_file: {}", config_file.display());
    println!("config_exists: {}", config_exists);
    println!("db_path: {}", db_path.display());
    println!("download_dir: {}", download_dir.display());
    println!("secret_key_env: {}", secret_env);
    println!("secret_key_set: {}", secret_is_set);
    println!("fast_download_api_url: {}", config.fast_download_api_url());
    println!("expand_cache_path: {}", expand_cache_path.display());
    println!("expand_command: {}", config.expand_command());
    println!("expand_model_path: {}", expand_model_path.display());
    println!("expand_timeout_secs: {}", config.expand_timeout_secs());

    Ok(())
}
