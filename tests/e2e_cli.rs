use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use rusqlite::Connection;
use serde_json::Value;

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

fn run_ok_with_env(args: &[&str], envs: &[(&str, &str)], remove_envs: &[&str]) -> String {
    let mut command = Command::new(bin());
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    for key in remove_envs {
        command.env_remove(key);
    }

    let output = command.output().unwrap();
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

    let search_json = run_ok(&[
        "search",
        "--db",
        db.to_str().unwrap(),
        "--json",
        "large language models",
    ]);
    let search_value: Value = serde_json::from_str(&search_json).unwrap();
    assert_eq!(search_value["query_kind"], "keyword");
    assert_eq!(search_value["query"], "large language models");
    assert_eq!(search_value["result_count"], 1);
    assert_eq!(search_value["results"][0]["aa_id"], "aa-llm-1");

    let exact_out = run_ok(&[
        "search",
        "--db",
        db.to_str().unwrap(),
        "--isbn",
        "9789401771993",
    ]);
    assert!(exact_out.contains("The Essential Guide to N-of-1 Trials in Health"));

    let portable_file = books
        .join("The Essential Guide to N-of-1 Trials in Health -- 9789401771993 -- local copy.pdf");
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

    let dry_json = run_ok(&[
        "link-local",
        "--db",
        db.to_str().unwrap(),
        "--scan",
        books.to_str().unwrap(),
        "--dry-run",
        "--json",
    ]);
    let dry_json_value: Value = serde_json::from_str(&dry_json).unwrap();
    assert_eq!(dry_json_value["dry_run"], true);
    assert_eq!(dry_json_value["entries"][0]["status"], "would_link");
    assert_eq!(dry_json_value["entries"][0]["matched_by"], "isbn");
    assert!(
        dry_json_value["entries"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("isbn13")
    );

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

#[test]
fn config_cli_supports_path_init_and_json() {
    let root = unique_temp_dir("config-cli");
    let home = root.join("home");
    fs::create_dir_all(&home).unwrap();

    let path_out = run_ok_with_env(
        &["config", "path"],
        &[("HOME", home.to_str().unwrap())],
        &["XDG_CONFIG_HOME", "XDG_DOWNLOAD_DIR"],
    );
    let expected_path = home.join(".config/arcana/config.yaml");
    assert_eq!(path_out.trim(), expected_path.display().to_string());

    let init_out = run_ok_with_env(
        &["config", "init"],
        &[("HOME", home.to_str().unwrap())],
        &["XDG_CONFIG_HOME", "XDG_DOWNLOAD_DIR"],
    );
    assert!(init_out.contains("wrote config"));
    assert!(expected_path.is_file());

    let config_text = fs::read_to_string(&expected_path).unwrap();
    assert!(config_text.contains("db_path:"));
    assert!(config_text.contains("download_dir:"));
    assert!(config_text.contains("secret_key_env:"));

    let json_out = run_ok_with_env(
        &["config", "--json"],
        &[("HOME", home.to_str().unwrap())],
        &["XDG_CONFIG_HOME", "XDG_DOWNLOAD_DIR"],
    );
    let json: Value = serde_json::from_str(&json_out).unwrap();
    assert_eq!(json["config_exists"], true);
    assert_eq!(json["db_path"]["source"], "config_file");
    assert_eq!(json["secret_key_env"]["value"], "ANNAS_ARCHIVE_SECRET_KEY");

    let duplicate_init = Command::new(bin())
        .args(["config", "init"])
        .env("HOME", home.to_str().unwrap())
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("XDG_DOWNLOAD_DIR")
        .output()
        .unwrap();
    assert!(!duplicate_init.status.success());
    assert!(String::from_utf8_lossy(&duplicate_init.stderr).contains("already exists"));

    let _ = fs::remove_dir_all(root);
}
