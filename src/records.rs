use anyhow::{Result, bail};
use rusqlite::{Connection, Params, params, params_from_iter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordSelector {
    AaId(String),
    Md5(String),
    Isbn(String),
    Doi(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSummary {
    pub rid: i64,
    pub aa_id: String,
    pub md5: Option<String>,
    pub title: Option<String>,
    pub original_filename: Option<String>,
    pub extension: Option<String>,
    pub local_path: Option<String>,
}

impl RecordSelector {
    pub fn from_exact_args(
        aa_id: Option<&str>,
        md5: Option<&str>,
        isbn: Option<&str>,
        doi: Option<&str>,
    ) -> Result<Self> {
        let selector_count = [
            aa_id.is_some(),
            md5.is_some(),
            isbn.is_some(),
            doi.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();

        if selector_count == 0 {
            bail!("provide exactly one of --aa-id, --md5, --isbn, or --doi")
        }

        if selector_count > 1 {
            bail!("use only one of --aa-id, --md5, --isbn, or --doi")
        }

        if let Some(value) = aa_id {
            return Ok(Self::AaId(value.trim().to_string()));
        }
        if let Some(value) = md5 {
            return Ok(Self::Md5(value.trim().to_string()));
        }
        if let Some(value) = isbn {
            return Ok(Self::Isbn(value.trim().to_string()));
        }
        if let Some(value) = doi {
            return Ok(Self::Doi(value.trim().to_string()));
        }

        bail!("unreachable selector state")
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::AaId(_) => "aa_id",
            Self::Md5(_) => "md5",
            Self::Isbn(_) => "isbn",
            Self::Doi(_) => "doi",
        }
    }

    pub fn value(&self) -> &str {
        match self {
            Self::AaId(value) | Self::Md5(value) | Self::Isbn(value) | Self::Doi(value) => value,
        }
    }
}

pub fn resolve_unique_record(
    conn: &Connection,
    selector: &RecordSelector,
) -> Result<RecordSummary> {
    match selector {
        RecordSelector::AaId(aa_id) => resolve_unique_with_query(
            conn,
            selector,
            "SELECT rid, aa_id, md5, title, original_filename, extension, local_path FROM records WHERE aa_id = ?1 ORDER BY rid LIMIT 2",
            params![aa_id],
        ),
        RecordSelector::Md5(md5) => resolve_unique_by_code(conn, selector, &["md5"], md5),
        RecordSelector::Isbn(isbn) => {
            resolve_unique_by_code(conn, selector, &["isbn10", "isbn13"], isbn)
        }
        RecordSelector::Doi(doi) => resolve_unique_by_code(conn, selector, &["doi"], doi),
    }
}

fn resolve_unique_by_code(
    conn: &Connection,
    selector: &RecordSelector,
    kinds: &[&str],
    value: &str,
) -> Result<RecordSummary> {
    let mut sql = String::from(
        "SELECT DISTINCT r.rid, r.aa_id, r.md5, r.title, r.original_filename, r.extension, r.local_path
         FROM records r
         JOIN record_codes rc ON rc.rid = r.rid
         WHERE lower(rc.value) = lower(?1) AND rc.kind IN (",
    );

    for (index, _) in kinds.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push('?');
        sql.push_str(&(index + 2).to_string());
    }
    sql.push_str(") ORDER BY r.rid LIMIT 2");

    let mut params_vec = vec![rusqlite::types::Value::Text(value.to_string())];
    for kind in kinds {
        params_vec.push(rusqlite::types::Value::Text((*kind).to_string()));
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(params_vec), map_record_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    collapse_unique_rows(selector, rows)
}

fn resolve_unique_with_query<P>(
    conn: &Connection,
    selector: &RecordSelector,
    sql: &str,
    params: P,
) -> Result<RecordSummary>
where
    P: Params,
{
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(params, map_record_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    collapse_unique_rows(selector, rows)
}

fn map_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordSummary> {
    Ok(RecordSummary {
        rid: row.get(0)?,
        aa_id: row.get(1)?,
        md5: row.get(2)?,
        title: row.get(3)?,
        original_filename: row.get(4)?,
        extension: row.get(5)?,
        local_path: row.get(6)?,
    })
}

fn collapse_unique_rows(
    selector: &RecordSelector,
    rows: Vec<RecordSummary>,
) -> Result<RecordSummary> {
    match rows.as_slice() {
        [] => bail!(
            "no record matched {} '{}'",
            selector.label(),
            selector.value()
        ),
        [record] => Ok(record.clone()),
        _ => bail!(
            "multiple records matched {} '{}'; refine the selector",
            selector.label(),
            selector.value()
        ),
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::commands::build::insert_batch;
    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

    use super::{RecordSelector, resolve_unique_record};

    fn sample_record(aa_id: &str, md5: &str, isbn13: Option<&str>) -> ExtractedRecord {
        ExtractedRecord {
            record: FlatRecord {
                aa_id: aa_id.into(),
                md5: Some(md5.into()),
                isbn13: isbn13.map(str::to_string),
                doi: None,
                title: Some("Title".into()),
                author: Some("Author".into()),
                publisher: Some("Publisher".into()),
                edition_varia: None,
                subjects: Some("Subject".into()),
                description: Some("Description".into()),
                year: Some(2024),
                language: Some("en".into()),
                extension: Some("pdf".into()),
                content_type: Some("book_nonfiction".into()),
                filesize: None,
                added_date: None,
                primary_source: Some("libgen".into()),
                score_base_rank: Some(10),
                cover_url: None,
                original_filename: Some(format!("{aa_id}.pdf")),
                has_aa_downloads: Some(1),
                has_torrent_paths: Some(0),
            },
            codes: {
                let mut codes = vec![ExactCode {
                    kind: "md5".into(),
                    value: md5.into(),
                }];
                if let Some(isbn13) = isbn13 {
                    codes.push(ExactCode {
                        kind: "isbn13".into(),
                        value: isbn13.into(),
                    });
                }
                codes
            },
        }
    }

    #[test]
    fn builds_selector_from_single_flag() {
        let selector =
            RecordSelector::from_exact_args(None, None, Some("9780131103627"), None).unwrap();
        assert_eq!(selector, RecordSelector::Isbn("9780131103627".into()));
    }

    #[test]
    fn rejects_missing_or_multiple_selectors() {
        assert!(RecordSelector::from_exact_args(None, None, None, None).is_err());
        assert!(RecordSelector::from_exact_args(Some("a"), Some("b"), None, None).is_err());
    }

    #[test]
    fn resolves_record_by_isbn() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![sample_record(
            "aa-1",
            "abcdef0123456789abcdef0123456789",
            Some("9780131103627"),
        )];
        insert_batch(&mut conn, &mut batch).unwrap();

        let record =
            resolve_unique_record(&conn, &RecordSelector::Isbn("9780131103627".into())).unwrap();
        assert_eq!(record.aa_id, "aa-1");
        assert_eq!(
            record.md5.as_deref(),
            Some("abcdef0123456789abcdef0123456789")
        );
    }
}
