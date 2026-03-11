use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub fn discover_local_files(root: &Path, max_files: Option<usize>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_local_files(root, &mut out, max_files)?;
    Ok(out)
}

fn collect_local_files(
    root: &Path,
    out: &mut Vec<PathBuf>,
    max_files: Option<usize>,
) -> Result<()> {
    if max_files.is_some_and(|limit| out.len() >= limit) {
        return Ok(());
    }

    let mut entries = fs::read_dir(root)
        .with_context(|| format!("failed to read {}", root.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to inspect {}", path.display()))?;

        if metadata.is_dir() {
            collect_local_files(&path, out, max_files)?;
        } else if metadata.is_file() {
            out.push(path);
        }

        if max_files.is_some_and(|limit| out.len() >= limit) {
            break;
        }
    }

    Ok(())
}
