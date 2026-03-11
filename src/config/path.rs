use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

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

pub(crate) fn xdg_user_dir(kind: &str) -> Result<Option<PathBuf>> {
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

pub(crate) fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
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

#[cfg(test)]
mod tests {
    use super::{expand_tilde_path, expand_xdg_path};

    #[test]
    fn expands_tilde_paths() {
        let path = expand_tilde_path("~/example/file.txt");
        assert!(path.to_string_lossy().contains("example/file.txt"));
    }

    #[test]
    fn expands_xdg_home_token() {
        let path = expand_xdg_path("$HOME/Downloads");
        assert!(path.to_string_lossy().ends_with("Downloads"));
    }
}
