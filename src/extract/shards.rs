use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

pub fn discover_input_shards(input: &Path, max_shards: Option<usize>) -> Result<Vec<PathBuf>> {
    let mut paths = if input.is_file() {
        vec![input.to_path_buf()]
    } else if input.is_dir() {
        let mut paths = Vec::new();

        for entry in std::fs::read_dir(input)
            .with_context(|| format!("failed to read {}", input.display()))?
        {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if file_name.starts_with("aarecords__") && file_name.ends_with(".json.gz") {
                paths.push(path);
            }
        }

        paths.sort_by_key(|path| shard_sort_key(path));
        paths
    } else {
        bail!("input path does not exist: {}", input.display());
    };

    if let Some(limit) = max_shards {
        paths.truncate(limit);
    }

    Ok(paths)
}

fn shard_sort_key(path: &Path) -> (u32, String) {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();

    let shard_number = file_name
        .strip_prefix("aarecords__")
        .and_then(|value| value.strip_suffix(".json.gz"))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(u32::MAX);

    (shard_number, file_name)
}
