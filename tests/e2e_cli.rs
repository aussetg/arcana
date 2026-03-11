use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use rusqlite::Connection;

fn bin() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_arcana")
        .map(PathBuf::from)
        .expect("CARGO_BIN_EXE_arcana is not set")
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("arcana-it-{name}-{nonce}"));
    fs::create_dir_all(&path).unwrap();
    path
}

fn write_gz_shard_from_fixture(fixture_name: &str, output_path: &Path) {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(fixture_name);
    let contents = fs::read(&fixture_path).unwrap();
    let file = fs::File::create(output_path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    use std::io::Write;
    encoder.write_all(&contents).unwrap();
    encoder.finish().unwrap();
}

fn run_ok(args: &[&str]) -> String {
    let output = Command::new(bin()).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn build_search_and_link_local_cli_end_to_end() {
    let root = unique_temp_dir("portable");
    let shard = root.join("aarecords__0.json.gz");
    let db = root.join("arcana.sqlite3");
    let books = root.join("books");
    fs::create_dir_all(&books).unwrap();
    write_gz_shard_from_fixture("sample_records.ndjson", &shard);

    let build_out = run_ok(&[
        "build",
        "--input",
        root.to_str().unwrap(),
        "--output",
        db.to_str().unwrap(),
        "--replace",
    ]);
    assert!(build_out.contains("built database"));

    let search_out = run_ok(&[
        "search",
        "--db",
        db.to_str().unwrap(),
        "large language models",
    ]);
    assert!(search_out.contains("Large Language Models in Practice"));

    let exact_out = run_ok(&[
        "search",
        "--db",
        db.to_str().unwrap(),
        "--isbn",
        "9789401771993",
    ]);
    assert!(exact_out.contains("The Essential Guide to N-of-1 Trials in Health"));

    let portable_file = books.join(
        "The Essential Guide to N-of-1 Trials in Health -- 9789401771993 -- local copy.pdf",
    );
    fs::write(&portable_file, b"portable fixture").unwrap();

    let dry_run = Command::new(bin())
        .args([
            "link-local",
            "--db",
            db.to_str().unwrap(),
            "--scan",
            books.to_str().unwrap(),
            "--dry-run",
            "--verbose",
        ])
        .output()
        .unwrap();
    assert!(dry_run.status.success());
    let dry_stderr = String::from_utf8(dry_run.stderr).unwrap();
    assert!(dry_stderr.contains("would-link"));

    let conn = Connection::open(&db).unwrap();
    let dry_local_path: Option<String> = conn
        .query_row(
            "SELECT local_path FROM records WHERE aa_id = 'aa-nof1-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(dry_local_path.is_none());
    drop(conn);

    let link_out = run_ok(&[
        "link-local",
        "--db",
        db.to_str().unwrap(),
        "--scan",
        books.to_str().unwrap(),
    ]);
    assert!(link_out.contains("linked local files"));

    let conn = Connection::open(&db).unwrap();
    let linked_local_path: Option<String> = conn
        .query_row(
            "SELECT local_path FROM records WHERE aa_id = 'aa-nof1-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(linked_local_path.is_some());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn link_local_hash_md5_cli_end_to_end() {
    let root = unique_temp_dir("hash-md5");
    let shard = root.join("aarecords__0.json.gz");
    let db = root.join("arcana.sqlite3");
    let books = root.join("books");
    fs::create_dir_all(&books).unwrap();

    let local_file = books.join("unrelated-name.pdf");
    fs::write(&local_file, b"hello").unwrap();
    let content_md5 = format!("{:x}", md5::compute(b"hello"));

    let ndjson = format!(
        "{{\"_source\":{{\"id\":\"aa-md5-1\",\"search_only_fields\":{{\"search_title\":\"Hash Verified Book\",\"search_author\":\"Hash Author\",\"search_year\":\"2024\",\"search_most_likely_language_code\":[\"en\"],\"search_extension\":\"pdf\"}},\"file_unified_data\":{{\"identifiers_unified\":{{\"md5\":[\"{content_md5}\"]}}}}}}}}\n"
    );
    let file = fs::File::create(&shard).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    use std::io::Write;
    encoder.write_all(ndjson.as_bytes()).unwrap();
    encoder.finish().unwrap();

    run_ok(&[
        "build",
        "--input",
        root.to_str().unwrap(),
        "--output",
        db.to_str().unwrap(),
        "--replace",
    ]);

    let output = Command::new(bin())
        .args([
            "link-local",
            "--db",
            db.to_str().unwrap(),
            "--scan",
            books.to_str().unwrap(),
            "--hash-md5",
            "--verbose",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("[md5:content]"));

    let conn = Connection::open(&db).unwrap();
    let linked_local_path: Option<String> = conn
        .query_row(
            "SELECT local_path FROM records WHERE aa_id = 'aa-md5-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(linked_local_path.is_some());

    let _ = fs::remove_dir_all(root);
}