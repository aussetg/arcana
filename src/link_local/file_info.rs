use std::collections::HashSet;
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFileInfo {
    pub canonical_path: PathBuf,
    pub file_name: String,
    pub stem_normalized: String,
    pub md5_candidates: Vec<String>,
    pub isbn10_candidates: Vec<String>,
    pub isbn13_candidates: Vec<String>,
    pub search_terms: Vec<String>,
    pub used_content_md5: bool,
}

const NOISE_TERMS: &[&str] = &[
    "ebook", "book", "books", "scan", "scanned", "ocr", "libgen", "zlib", "epub", "pdf", "djvu",
    "mobi", "azw3", "azw", "fb2", "edition", "ed", "vol", "volume", "v2", "v3", "retail", "fixed",
    "clean", "text", "author", "title",
];

pub fn build_local_file_info(path: &Path, hash_md5: bool) -> Result<LocalFileInfo> {
    let canonical_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("file name is not valid UTF-8")?
        .to_string();
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .context("file stem is not valid UTF-8")?;

    let stem_normalized = normalize_for_match(stem);
    let search_terms = significant_terms(&stem_normalized);
    let mut md5_candidates = Vec::new();
    let mut seen_md5 = HashSet::new();
    let mut used_content_md5 = false;

    if hash_md5 {
        if let Ok(md5) = compute_file_md5(path) {
            if seen_md5.insert(md5.clone()) {
                md5_candidates.push(md5);
            }
            used_content_md5 = true;
        }
    }

    for md5 in extract_md5_candidates(&file_name) {
        if seen_md5.insert(md5.clone()) {
            md5_candidates.push(md5);
        }
    }

    let (isbn10_candidates, isbn13_candidates) = extract_isbn_candidates(&file_name);

    Ok(LocalFileInfo {
        canonical_path,
        file_name,
        stem_normalized,
        md5_candidates,
        isbn10_candidates,
        isbn13_candidates,
        search_terms,
        used_content_md5,
    })
}

pub fn compute_file_md5(path: &Path) -> Result<String> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut context = md5::Context::new();
    let mut buffer = [0u8; 1024 * 1024];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        context.consume(&buffer[..read]);
    }

    Ok(format!("{:x}", context.compute()))
}

pub fn extract_md5_candidates(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for token in split_hexdigit_runs(input) {
        if token.len() == 32 && token.chars().all(|character| character.is_ascii_hexdigit()) {
            let lowered = token.to_ascii_lowercase();
            if seen.insert(lowered.clone()) {
                out.push(lowered);
            }
        }
    }

    out
}

pub fn extract_isbn_candidates(input: &str) -> (Vec<String>, Vec<String>) {
    let mut isbn10 = Vec::new();
    let mut isbn13 = Vec::new();
    let mut seen10 = HashSet::new();
    let mut seen13 = HashSet::new();

    for token in split_isbnish_runs(input) {
        let compact = token
            .chars()
            .filter(|character| {
                character.is_ascii_digit() || *character == 'X' || *character == 'x'
            })
            .collect::<String>();

        if compact.len() == 13 && compact.chars().all(|character| character.is_ascii_digit()) {
            if seen13.insert(compact.clone()) {
                isbn13.push(compact);
            }
        } else if compact.len() == 10
            && compact.chars().enumerate().all(|(index, character)| {
                character.is_ascii_digit() || (index == 9 && matches!(character, 'X' | 'x'))
            })
        {
            let normalized = compact.to_ascii_uppercase();
            if seen10.insert(normalized.clone()) {
                isbn10.push(normalized);
            }
        }
    }

    (isbn10, isbn13)
}

pub fn normalize_for_match(input: &str) -> String {
    input
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn significant_terms(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter(|term| term.len() >= 3)
        .filter(|term| {
            term.chars()
                .any(|character| character.is_ascii_alphabetic())
        })
        .filter(|term| !NOISE_TERMS.contains(term))
        .map(ToString::to_string)
        .collect()
}

fn split_hexdigit_runs(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for character in input.chars() {
        if character.is_ascii_hexdigit() {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn split_isbnish_runs(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for character in input.chars() {
        if character.is_ascii_digit() || character == 'X' || character == 'x' || character == '-' {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        build_local_file_info, compute_file_md5, extract_isbn_candidates, extract_md5_candidates,
        normalize_for_match,
    };

    fn unique_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("arcana-{name}-{nonce}"))
    }

    #[test]
    fn extracts_identifiers_from_filenames() {
        assert_eq!(
            extract_md5_candidates("book-abcdef0123456789abcdef0123456789.pdf"),
            vec!["abcdef0123456789abcdef0123456789".to_string()]
        );

        let (isbn10, isbn13) = extract_isbn_candidates("The-Book-978-0131103627-(0131103628).pdf");
        assert_eq!(isbn13, vec!["9780131103627".to_string()]);
        assert_eq!(isbn10, vec!["0131103628".to_string()]);
    }

    #[test]
    fn normalizes_file_text_for_matching() {
        assert_eq!(
            normalize_for_match("Carol Smith - Large Language Models [2024]"),
            "carol smith large language models 2024"
        );
    }

    #[test]
    fn builds_local_file_info_from_utf8_path() {
        let dir = unique_path("link-local-info-dir");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Example-9780131103627.pdf");
        fs::write(&file, b"x").unwrap();

        let info = build_local_file_info(&file, false).unwrap();
        assert_eq!(info.file_name, "Example-9780131103627.pdf");
        assert_eq!(info.isbn13_candidates, vec!["9780131103627".to_string()]);

        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }

    #[test]
    fn hashes_file_contents_for_md5() {
        let dir = unique_path("link-local-hash-dir");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("hello.bin");
        fs::write(&file, b"hello").unwrap();

        let content_md5 = compute_file_md5(&file).unwrap();
        assert_eq!(content_md5, format!("{:x}", md5::compute(b"hello")));

        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(dir);
    }
}
