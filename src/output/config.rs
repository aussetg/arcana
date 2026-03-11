use anyhow::Result;

use crate::config::ResolvedConfig;

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
