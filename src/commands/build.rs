use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Args;
use rusqlite::Connection;
use rusqlite::{Statement, Transaction, params};

use crate::model::ExtractedRecord;

const INSERT_RECORD_SQL: &str = "INSERT INTO records (
    aa_id,
    md5,
    isbn13,
    doi,
    title,
    author,
    publisher,
    edition_varia,
    subjects,
    description,
    year,
    language,
    extension,
    content_type,
    filesize,
    added_date,
    primary_source,
    score_base_rank,
    cover_url,
    original_filename,
    has_aa_downloads,
    has_torrent_paths
) VALUES (
    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
    ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22
)";

const INSERT_CODE_SQL: &str = "INSERT INTO record_codes (rid, kind, value) VALUES (?1, ?2, ?3)";

#[derive(Debug, Default)]
struct IngestStats {
    total_files: usize,
    total_lines: usize,
    total_records: usize,
    total_codes: usize,
    insert_batches: usize,
    read_elapsed: Duration,
    parse_elapsed: Duration,
    insert_elapsed: Duration,
    ingest_elapsed: Duration,
    jobs: usize,
}

struct WorkerBatch {
    records: Vec<ExtractedRecord>,
    codes: usize,
}

#[derive(Debug, Clone)]
struct ShardPlan {
    path: PathBuf,
    max_records: Option<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
struct WorkerShardStats {
    lines: usize,
    read_elapsed: Duration,
    parse_elapsed: Duration,
}

enum WorkerMessage {
    Batch(WorkerBatch),
    ShardDone(WorkerShardStats),
    Error(String),
}

#[derive(Debug, Args)]
pub struct BuildArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Input shard directory or a single .json.gz shard"
    )]
    pub input: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database to create (defaults to config file or ~/.config/arcana/arcana.sqlite3)"
    )]
    pub output: Option<PathBuf>,

    #[arg(long, help = "Replace an existing database file at --output")]
    pub replace: bool,

    #[arg(long, default_value_t = 10000)]
    pub batch_size: usize,

    #[arg(long)]
    pub max_shards: Option<usize>,

    #[arg(long)]
    pub max_records: Option<usize>,

    #[arg(
        long,
        help = "Distribute --max-records evenly across selected shards instead of stopping globally"
    )]
    pub spread_max_records: bool,

    #[arg(
        long,
        value_name = "N",
        help = "Worker threads for shard ingest (defaults to available CPUs)"
    )]
    pub jobs: Option<usize>,

    #[arg(long, help = "Print phase timings for ingest profiling")]
    pub timings: bool,
}

pub fn run(args: BuildArgs) -> Result<()> {
    let total_started_at = Instant::now();

    if args.batch_size == 0 {
        bail!("--batch-size must be greater than zero");
    }

    let config = crate::config::resolve()?;
    let output = args.output.unwrap_or(config.db_path()).to_path_buf();

    prepare_output_path(&output, args.replace)?;

    let mut conn = Connection::open(&output)
        .with_context(|| format!("failed to open {}", output.display()))?;

    let prepare_started_at = Instant::now();
    crate::db::prepare_database(&conn)
        .with_context(|| format!("failed to initialize {}", output.display()))?;
    let prepare_elapsed = prepare_started_at.elapsed();

    let mut ingest_stats = IngestStats::default();

    if let Some(input) = args.input.as_deref() {
        let shards = crate::extract::discover_input_shards(input, args.max_shards)?;

        if shards.is_empty() {
            bail!("no input shards found under {}", input.display());
        }

        let requested_jobs = args
            .jobs
            .unwrap_or_else(default_ingest_jobs)
            .max(1)
            .min(shards.len());

        let (plans, global_max_records) =
            build_shard_plans(shards, args.max_records, args.spread_max_records);

        ingest_stats = if requested_jobs > 1 && global_max_records.is_none() {
            ingest_shards_parallel(&mut conn, plans, args.batch_size, requested_jobs)?
        } else {
            ingest_shards_sequential(&mut conn, &plans, args.batch_size, global_max_records)?
        };
    }

    let populate_fts_started_at = Instant::now();
    crate::db::populate_fts(&conn)
        .with_context(|| format!("failed to populate FTS in {}", output.display()))?;
    let populate_fts_elapsed = populate_fts_started_at.elapsed();

    let create_indexes_started_at = Instant::now();
    crate::db::create_secondary_indexes(&conn)
        .with_context(|| format!("failed to build secondary indexes in {}", output.display()))?;
    let create_indexes_elapsed = create_indexes_started_at.elapsed();

    let finalize_started_at = Instant::now();
    crate::db::pragmas::finalize_database(&conn)
        .with_context(|| format!("failed to finalize {}", output.display()))?;
    let finalize_elapsed = finalize_started_at.elapsed();

    let total_elapsed = total_started_at.elapsed();

    if args.input.is_some() {
        println!(
            "built database: {} (files: {}, lines: {}, records: {}, codes: {})",
            output.display(),
            ingest_stats.total_files,
            ingest_stats.total_lines,
            ingest_stats.total_records,
            ingest_stats.total_codes,
        );
    } else {
        println!("initialized empty database: {}", output.display());
    }

    if args.timings {
        let unaccounted_ingest_elapsed = ingest_stats
            .ingest_elapsed
            .saturating_sub(ingest_stats.read_elapsed)
            .saturating_sub(ingest_stats.parse_elapsed)
            .saturating_sub(ingest_stats.insert_elapsed);
        println!(
            "timings: total={:.3}s prepare={:.3}s ingest={:.3}s jobs={} read+gzip={:.3}s parse+flatten={:.3}s insert={:.3}s insert_batches={} ingest_other={:.3}s fts_rebuild={:.3}s secondary_indexes={:.3}s finalize={:.3}s",
            total_elapsed.as_secs_f64(),
            prepare_elapsed.as_secs_f64(),
            ingest_stats.ingest_elapsed.as_secs_f64(),
            ingest_stats.jobs,
            ingest_stats.read_elapsed.as_secs_f64(),
            ingest_stats.parse_elapsed.as_secs_f64(),
            ingest_stats.insert_elapsed.as_secs_f64(),
            ingest_stats.insert_batches,
            unaccounted_ingest_elapsed.as_secs_f64(),
            populate_fts_elapsed.as_secs_f64(),
            create_indexes_elapsed.as_secs_f64(),
            finalize_elapsed.as_secs_f64(),
        );
    }

    Ok(())
}

fn build_shard_plans(
    shards: Vec<PathBuf>,
    max_records: Option<usize>,
    spread_max_records: bool,
) -> (Vec<ShardPlan>, Option<usize>) {
    if !spread_max_records {
        let plans = shards
            .into_iter()
            .map(|path| ShardPlan {
                path,
                max_records: None,
            })
            .collect();
        return (plans, max_records);
    }

    let Some(max_records) = max_records else {
        let plans = shards
            .into_iter()
            .map(|path| ShardPlan {
                path,
                max_records: None,
            })
            .collect();
        return (plans, None);
    };

    let shard_count = shards.len().max(1);
    let base = max_records / shard_count;
    let remainder = max_records % shard_count;

    let plans = shards
        .into_iter()
        .enumerate()
        .map(|(index, path)| ShardPlan {
            path,
            max_records: Some(base + usize::from(index < remainder)),
        })
        .collect();

    (plans, None)
}

fn default_ingest_jobs() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
}

fn ingest_shards_sequential(
    conn: &mut Connection,
    plans: &[ShardPlan],
    batch_size: usize,
    global_max_records: Option<usize>,
) -> Result<IngestStats> {
    let mut stats = IngestStats {
        jobs: 1,
        ..IngestStats::default()
    };
    let mut batch = Vec::with_capacity(batch_size);
    let mut stop = false;
    let tx = conn.transaction()?;
    let ingest_started_at = Instant::now();

    {
        let mut insert_record = tx.prepare(INSERT_RECORD_SQL)?;
        let mut insert_code = tx.prepare(INSERT_CODE_SQL)?;

        for plan in plans {
            if stop {
                break;
            }

            stats.total_files += 1;

            if plan.max_records == Some(0) {
                continue;
            }

            let mut shard_records = 0usize;

            let file_stats = crate::extract::stream_records(&plan.path, |record| {
                stats.total_records += 1;
                shard_records += 1;
                stats.total_codes += record.codes.len();
                batch.push(record);

                if batch.len() >= batch_size {
                    let insert_started_at = Instant::now();
                    insert_batch_in_tx(&tx, &mut insert_record, &mut insert_code, &mut batch)?;
                    stats.insert_elapsed += insert_started_at.elapsed();
                    stats.insert_batches += 1;
                }

                if global_max_records.is_some_and(|limit| stats.total_records >= limit) {
                    stop = true;
                    return Ok(false);
                }

                if plan.max_records.is_some_and(|limit| shard_records >= limit) {
                    return Ok(false);
                }

                Ok(true)
            })?;

            stats.total_lines += file_stats.lines;
            stats.read_elapsed += file_stats.read_elapsed;
            stats.parse_elapsed += file_stats.parse_elapsed;
        }

        if !batch.is_empty() {
            let insert_started_at = Instant::now();
            insert_batch_in_tx(&tx, &mut insert_record, &mut insert_code, &mut batch)?;
            stats.insert_elapsed += insert_started_at.elapsed();
            stats.insert_batches += 1;
        }
    }

    tx.commit()?;
    stats.ingest_elapsed = ingest_started_at.elapsed();
    Ok(stats)
}

fn ingest_shards_parallel(
    conn: &mut Connection,
    plans: Vec<ShardPlan>,
    batch_size: usize,
    jobs: usize,
) -> Result<IngestStats> {
    let mut stats = IngestStats {
        jobs,
        ..IngestStats::default()
    };
    let ingest_started_at = Instant::now();
    let queue = Arc::new(Mutex::new(VecDeque::from(plans)));
    let stop = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::sync_channel::<WorkerMessage>(jobs.saturating_mul(2).max(1));
    let mut workers = Vec::with_capacity(jobs);
    let tx = conn.transaction()?;
    let mut insert_record = tx.prepare(INSERT_RECORD_SQL)?;
    let mut insert_code = tx.prepare(INSERT_CODE_SQL)?;

    for _ in 0..jobs {
        let worker_queue = Arc::clone(&queue);
        let worker_stop = Arc::clone(&stop);
        let worker_sender = sender.clone();

        workers.push(thread::spawn(move || {
            worker_ingest_loop(worker_queue, worker_stop, worker_sender, batch_size)
        }));
    }

    drop(sender);

    let mut worker_error = None;

    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Batch(mut batch) => {
                stats.total_records += batch.records.len();
                stats.total_codes += batch.codes;

                let insert_started_at = Instant::now();
                if let Err(error) = insert_batch_in_tx(
                    &tx,
                    &mut insert_record,
                    &mut insert_code,
                    &mut batch.records,
                ) {
                    worker_error = Some(error);
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
                stats.insert_elapsed += insert_started_at.elapsed();
                stats.insert_batches += 1;
            }
            WorkerMessage::ShardDone(shard_stats) => {
                stats.total_files += 1;
                stats.total_lines += shard_stats.lines;
                stats.read_elapsed += shard_stats.read_elapsed;
                stats.parse_elapsed += shard_stats.parse_elapsed;
            }
            WorkerMessage::Error(error) => {
                worker_error = Some(anyhow::anyhow!(error));
                stop.store(true, Ordering::Relaxed);
                break;
            }
        }
    }

    drop(insert_code);
    drop(insert_record);

    drop(receiver);

    for worker in workers {
        match worker.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if worker_error.is_none() {
                    worker_error = Some(error);
                }
            }
            Err(_) => {
                if worker_error.is_none() {
                    worker_error = Some(anyhow::anyhow!("ingest worker panicked"));
                }
            }
        }
    }

    if let Some(error) = worker_error {
        return Err(error);
    }

    tx.commit()?;
    stats.ingest_elapsed = ingest_started_at.elapsed();
    Ok(stats)
}

fn worker_ingest_loop(
    queue: Arc<Mutex<VecDeque<ShardPlan>>>,
    stop: Arc<AtomicBool>,
    sender: mpsc::SyncSender<WorkerMessage>,
    batch_size: usize,
) -> Result<()> {
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }

        let Some(shard) = queue
            .lock()
            .map_err(|_| anyhow::anyhow!("ingest shard queue mutex poisoned"))?
            .pop_front()
        else {
            return Ok(());
        };

        if let Err(error) = worker_ingest_shard(&shard, &sender, batch_size) {
            stop.store(true, Ordering::Relaxed);
            let _ = sender.send(WorkerMessage::Error(error.to_string()));
            return Ok(());
        }
    }
}

fn worker_ingest_shard(
    plan: &ShardPlan,
    sender: &mpsc::SyncSender<WorkerMessage>,
    batch_size: usize,
) -> Result<()> {
    if plan.max_records == Some(0) {
        let _ = sender.send(WorkerMessage::ShardDone(WorkerShardStats::default()));
        return Ok(());
    }

    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_codes = 0usize;
    let mut shard_records = 0usize;

    let file_stats = crate::extract::stream_records(&plan.path, |record| {
        shard_records += 1;
        batch_codes += record.codes.len();
        batch.push(record);

        if batch.len() >= batch_size {
            if sender
                .send(WorkerMessage::Batch(WorkerBatch {
                    records: std::mem::take(&mut batch),
                    codes: batch_codes,
                }))
                .is_err()
            {
                return Ok(false);
            }

            batch_codes = 0;
            batch = Vec::with_capacity(batch_size);
        }

        if plan.max_records.is_some_and(|limit| shard_records >= limit) {
            return Ok(false);
        }

        Ok(true)
    })?;

    if !batch.is_empty()
        && sender
            .send(WorkerMessage::Batch(WorkerBatch {
                records: batch,
                codes: batch_codes,
            }))
            .is_err()
    {
        return Ok(());
    }

    let shard_stats = WorkerShardStats {
        lines: file_stats.lines,
        read_elapsed: file_stats.read_elapsed,
        parse_elapsed: file_stats.parse_elapsed,
    };

    let _ = sender.send(WorkerMessage::ShardDone(shard_stats));
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn insert_batch(conn: &mut Connection, batch: &mut Vec<ExtractedRecord>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    let mut insert_record = tx.prepare(INSERT_RECORD_SQL)?;
    let mut insert_code = tx.prepare(INSERT_CODE_SQL)?;

    insert_batch_in_tx(&tx, &mut insert_record, &mut insert_code, batch)?;

    drop(insert_code);
    drop(insert_record);
    tx.commit()?;
    Ok(())
}

fn insert_batch_in_tx(
    tx: &Transaction<'_>,
    insert_record: &mut Statement<'_>,
    insert_code: &mut Statement<'_>,
    batch: &mut Vec<ExtractedRecord>,
) -> Result<()> {
    for extracted in batch.drain(..) {
        let record = extracted.record;

        insert_record.execute(params![
            record.aa_id,
            record.md5,
            record.isbn13,
            record.doi,
            record.title,
            record.author,
            record.publisher,
            record.edition_varia,
            record.subjects,
            record.description,
            record.year,
            record.language,
            record.extension,
            record.content_type,
            record.filesize,
            record.added_date,
            record.primary_source,
            record.score_base_rank,
            record.cover_url,
            record.original_filename,
            record.has_aa_downloads,
            record.has_torrent_paths,
        ])?;

        let rid = tx.last_insert_rowid();

        for code in extracted.codes {
            insert_code.execute(params![rid, code.kind, code.value])?;
        }
    }

    Ok(())
}

fn prepare_output_path(path: &Path, replace: bool) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    if !path.exists() {
        return Ok(());
    }

    let metadata =
        fs::metadata(path).with_context(|| format!("failed to inspect {}", path.display()))?;

    if metadata.is_dir() {
        bail!("output path is a directory: {}", path.display());
    }

    if !replace {
        bail!(
            "output already exists: {} (pass --replace to overwrite)",
            path.display()
        );
    }

    fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::model::{ExactCode, ExtractedRecord, FlatRecord};

    use super::insert_batch;

    #[test]
    fn inserts_records_and_codes() {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::prepare_database(&conn).unwrap();

        let mut batch = vec![ExtractedRecord {
            record: FlatRecord {
                aa_id: "aa-1".into(),
                md5: Some("md5-1".into()),
                isbn13: Some("isbn-1".into()),
                doi: Some("doi-1".into()),
                title: Some("Title".into()),
                author: Some("Author".into()),
                publisher: None,
                edition_varia: None,
                subjects: Some("Subject".into()),
                description: Some("Description".into()),
                year: Some(2024),
                language: Some("en".into()),
                extension: Some("pdf".into()),
                content_type: Some("book".into()),
                filesize: Some(123),
                added_date: Some("2024-01-01".into()),
                primary_source: Some("libgen".into()),
                score_base_rank: Some(10),
                cover_url: None,
                original_filename: Some("file.pdf".into()),
                has_aa_downloads: Some(1),
                has_torrent_paths: Some(0),
            },
            codes: vec![
                ExactCode {
                    kind: "md5".into(),
                    value: "md5-1".into(),
                },
                ExactCode {
                    kind: "isbn13".into(),
                    value: "isbn-1".into(),
                },
            ],
        }];

        insert_batch(&mut conn, &mut batch).unwrap();

        let record_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        let code_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM record_codes", [], |row| row.get(0))
            .unwrap();

        assert_eq!(record_count, 1);
        assert_eq!(code_count, 2);
    }
}
