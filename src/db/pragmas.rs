use anyhow::Result;
use rusqlite::Connection;

const BUILD_PRAGMAS_SQL: &str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;
PRAGMA cache_size = -1000000;
"#;

const QUERY_PRAGMAS_SQL: &str = r#"
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;
PRAGMA cache_size = -200000;
"#;

const FINALIZE_SQL: &str = r#"
ANALYZE;
INSERT INTO records_fts(records_fts) VALUES('optimize');
VACUUM;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA locking_mode = NORMAL;
"#;

pub fn apply_build_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(BUILD_PRAGMAS_SQL)?;
    Ok(())
}

pub fn apply_query_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(QUERY_PRAGMAS_SQL)?;
    Ok(())
}

pub fn finalize_database(conn: &Connection) -> Result<()> {
    conn.execute_batch(FINALIZE_SQL)?;
    Ok(())
}
