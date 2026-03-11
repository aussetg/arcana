use anyhow::Result;
use rusqlite::Connection;

pub mod pragmas;
pub mod schema;

pub fn prepare_database(conn: &Connection) -> Result<()> {
    pragmas::apply_build_pragmas(conn)?;
    schema::create_all(conn)?;
    Ok(())
}

pub fn populate_fts(conn: &Connection) -> Result<()> {
    conn.execute_batch("INSERT INTO records_fts(records_fts) VALUES('rebuild');")?;
    Ok(())
}

pub fn finalize_database(conn: &Connection) -> Result<()> {
    pragmas::finalize_database(conn)?;
    Ok(())
}

pub fn initialize_empty_database(conn: &Connection) -> Result<()> {
    prepare_database(conn)?;
    populate_fts(conn)?;
    finalize_database(conn)?;
    Ok(())
}
