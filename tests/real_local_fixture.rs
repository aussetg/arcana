#[test]
#[ignore = "uses a local external file fixture"]
fn link_local_real_fixture_file() {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use rusqlite::Connection;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("arcana-real-{name}-{nonce}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn bin() -> PathBuf {
        std::env::var_os("CARGO_BIN_EXE_arcana")
            .map(PathBuf::from)
            .expect("CARGO_BIN_EXE_arcana is not set")
    }

    fn configured_fixture_path() -> Option<PathBuf> {
        if let Some(path) = std::env::var_os("LOCAL_ANNA_REAL_TEST_FILE") {
            return Some(PathBuf::from(path));
        }

        let default = Path::new("/tmp/aa_test_files");
        if default.is_file() {
            return Some(default.to_path_buf());
        }
        if !default.is_dir() {
            return None;
        }

        let mut entries = fs::read_dir(default)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        entries.sort();
        entries.into_iter().next()
    }

    fn normalize_author_segment(segment: &str) -> String {
        segment
            .replace("(eds_)", "")
            .replace(['(', ')', ','], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn extract_isbn13(file_name: &str) -> Option<String> {
        let mut token = String::new();

        for ch in file_name.chars() {
            if ch.is_ascii_digit() || ch == '-' {
                token.push(ch);
            } else if !token.is_empty() {
                let compact = token
                    .chars()
                    .filter(|c| c.is_ascii_digit())
                    .collect::<String>();
                if compact.len() == 13 {
                    return Some(compact);
                }
                token.clear();
            }
        }

        let compact = token
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();
        (compact.len() == 13).then_some(compact)
    }

    let Some(real_file) = configured_fixture_path() else {
        eprintln!(
            "skipping real fixture test: no LOCAL_ANNA_REAL_TEST_FILE and /tmp/aa_test_files is absent"
        );
        return;
    };

    if !real_file.is_file() {
        eprintln!(
            "skipping real fixture test: not a file: {}",
            real_file.display()
        );
        return;
    }

    let file_name = real_file.file_name().unwrap().to_string_lossy().to_string();
    let md5 = format!("{:x}", md5::compute(fs::read(&real_file).unwrap()));
    let segments = file_name.split(" -- ").collect::<Vec<_>>();
    let title = segments
        .first()
        .copied()
        .unwrap_or("Local Fixture Book")
        .to_string();
    let author = segments
        .get(1)
        .map(|segment| normalize_author_segment(segment))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Unknown Author".to_string());
    let year = segments
        .iter()
        .find_map(|segment| {
            let digits = segment
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .collect::<String>();
            (digits.len() == 4).then_some(digits)
        })
        .unwrap_or_else(|| "2024".to_string());
    let isbn13 = extract_isbn13(&file_name);

    let root = unique_temp_dir("fixture");
    let shard = root.join("aarecords__0.json.gz");
    let db = root.join("arcana.sqlite3");

    let mut identifiers = serde_json::Map::new();
    identifiers.insert("md5".into(), serde_json::json!([md5]));
    if let Some(isbn13) = isbn13 {
        identifiers.insert("isbn13".into(), serde_json::json!([isbn13]));
    }

    let ndjson = format!(
        "{}\n",
        serde_json::json!({
            "_source": {
                "id": "aa-real-fixture-1",
                "search_only_fields": {
                    "search_title": title,
                    "search_author": author,
                    "search_year": year,
                    "search_most_likely_language_code": ["en"],
                    "search_extension": "pdf",
                    "search_original_filename": file_name,
                },
                "file_unified_data": {
                    "title_best": title,
                    "author_best": author,
                    "original_filename_best": file_name,
                    "identifiers_unified": identifiers,
                }
            }
        })
    );

    let file = fs::File::create(&shard).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    use std::io::Write;
    encoder.write_all(ndjson.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let build = Command::new(bin())
        .args([
            "build",
            "--input",
            root.to_str().unwrap(),
            "--output",
            db.to_str().unwrap(),
            "--replace",
        ])
        .output()
        .unwrap();
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );

    let books_dir = real_file.parent().unwrap();
    let link = Command::new(bin())
        .args([
            "link-local",
            "--db",
            db.to_str().unwrap(),
            "--scan",
            books_dir.to_str().unwrap(),
            "--hash-md5",
            "--verbose",
            "--max-files",
            "1000",
        ])
        .output()
        .unwrap();
    assert!(
        link.status.success(),
        "{}",
        String::from_utf8_lossy(&link.stderr)
    );

    let conn = Connection::open(&db).unwrap();
    let local_path: Option<String> = conn
        .query_row(
            "SELECT local_path FROM records WHERE aa_id = 'aa-real-fixture-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(local_path.as_deref(), Some(real_file.to_str().unwrap()));

    let _ = fs::remove_dir_all(root);
}
