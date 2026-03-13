use anyhow::Result;
use rusqlite::Connection;

pub const USER_VERSION: i32 = 1;

const SCHEMA_SQL: &str = r#"
CREATE TABLE records (
    rid INTEGER PRIMARY KEY,

    aa_id TEXT NOT NULL UNIQUE,
    md5 TEXT,
    isbn13 TEXT,
    doi TEXT,

    title TEXT,
    author TEXT,
    publisher TEXT,
    edition_varia TEXT,

    subjects TEXT,
    description TEXT,

    year INTEGER,
    language TEXT,
    extension TEXT,
    content_type TEXT,
    filesize INTEGER,
    added_date TEXT,

    primary_source TEXT,
    score_base_rank INTEGER,

    cover_url TEXT,
    original_filename TEXT,

    has_aa_downloads INTEGER,
    has_torrent_paths INTEGER,

    local_path TEXT
);

CREATE VIRTUAL TABLE records_fts USING fts5(
    title,
    author,
    subjects,
    description,
    publisher,
    edition_varia,
    content='records',
    content_rowid='rid',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TABLE record_codes (
    rid INTEGER NOT NULL,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE(rid, kind, value),
    FOREIGN KEY (rid) REFERENCES records(rid) ON DELETE CASCADE
);
"#;

const SECONDARY_INDEX_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_record_codes_kind_value ON record_codes(kind, value);
CREATE INDEX IF NOT EXISTS idx_record_codes_rid        ON record_codes(rid);

CREATE INDEX IF NOT EXISTS idx_records_md5            ON records(md5);
CREATE INDEX IF NOT EXISTS idx_records_isbn13         ON records(isbn13);
CREATE INDEX IF NOT EXISTS idx_records_doi            ON records(doi);
CREATE INDEX IF NOT EXISTS idx_records_year           ON records(year);
CREATE INDEX IF NOT EXISTS idx_records_language       ON records(language);
CREATE INDEX IF NOT EXISTS idx_records_extension      ON records(extension);
CREATE INDEX IF NOT EXISTS idx_records_content_type   ON records(content_type);
CREATE INDEX IF NOT EXISTS idx_records_primary_source ON records(primary_source);
CREATE INDEX IF NOT EXISTS idx_records_local_path     ON records(local_path);
"#;

pub fn create_all(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA_SQL)?;
    conn.pragma_update(None, "user_version", USER_VERSION)?;
    Ok(())
}

pub fn create_secondary_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(SECONDARY_INDEX_SQL)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{USER_VERSION, create_all, create_secondary_indexes};

    #[test]
    fn creates_expected_schema_objects() {
        let conn = Connection::open_in_memory().unwrap();

        create_all(&conn).unwrap();

        for object_name in ["records", "records_fts", "record_codes"] {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                    [object_name],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(exists, 1, "missing schema object: {object_name}");
        }
    }

    #[test]
    fn creates_secondary_indexes_when_requested() {
        let conn = Connection::open_in_memory().unwrap();

        create_all(&conn).unwrap();
        create_secondary_indexes(&conn).unwrap();

        for object_name in [
            "idx_record_codes_kind_value",
            "idx_record_codes_rid",
            "idx_records_md5",
            "idx_records_isbn13",
            "idx_records_doi",
            "idx_records_year",
            "idx_records_language",
            "idx_records_extension",
            "idx_records_content_type",
            "idx_records_primary_source",
            "idx_records_local_path",
        ] {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                    [object_name],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(exists, 1, "missing schema object: {object_name}");
        }
    }

    #[test]
    fn sets_user_version() {
        let conn = Connection::open_in_memory().unwrap();

        create_all(&conn).unwrap();

        let user_version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();

        assert_eq!(user_version, USER_VERSION);
    }
}
