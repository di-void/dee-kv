pub mod cache;
pub mod file;
pub mod writer;

use crate::{
    DATA_DIR, LOG_FILE_DELIM, LOG_FILE_FLUSH_LIMIT, LOG_FILE_MAX_SIZE, LogIndex, LogMessage,
    LogTerm, META_BUF_CAPACITY, META_FILE_FLUSH_WRITES, META_FILE_PATH, Op,
    log::file::{
        CheckStatus, check_file_size_or_create, generate_file_name, get_file_size, get_log_files,
        open_append_file, open_or_create_file, replay_log_file, validate_or_create_dir,
    },
    serde::{CustomSerialize, LogEntry, Payload, deserialize_entry},
    state::Types,
    utils::file as file_utils,
};
use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU32, Ordering},
};
use tokio::sync::mpsc;

/// A buffer for meta writes that overwrites its contents on each write,
/// grows dynamically as needed, and periodically flushes to disk by overwriting the file.
pub struct MetaBuffer {
    file: File,
    buffer: Vec<u8>,
    len: usize,
    write_count: u16,
    flush_threshold: u16,
}

impl MetaBuffer {
    pub fn new(file: File, capacity: usize, flush_threshold: u16) -> Self {
        Self {
            file,
            buffer: vec![0u8; capacity],
            len: 0,
            write_count: 0,
            flush_threshold,
        }
    }

    /// Overwrites the buffer with the given payload, resizing if necessary.
    pub fn write(&mut self, payload: &[u8]) -> usize {
        if payload.len() > self.buffer.len() {
            self.buffer.resize(payload.len(), 0);
        }
        let bytes_to_write = payload.len();
        self.buffer[..bytes_to_write].copy_from_slice(payload);
        self.len = bytes_to_write;
        self.write_count += 1;
        bytes_to_write
    }

    /// Returns true if the write count has reached the flush threshold.
    pub fn should_flush(&self) -> bool {
        self.write_count >= self.flush_threshold
    }

    /// Flushes the buffer to disk by seeking to the start and overwriting.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.buffer[..self.len])?;
        self.file.set_len(self.len as u64)?; // Truncate file to current content size
        self.file.sync_all()?;
        self.write_count = 0;
        Ok(())
    }
}

pub struct Log {
    meta_buf: MetaBuffer,
    pub(crate) curr_log_file: BufWriter<File>,
    pub(crate) data_dir: PathBuf,
}

impl Log {
    pub fn append(&mut self, payload: &[u8], should_check: bool) -> Result<usize> {
        if should_check {
            let meta = self.curr_log_file.get_ref().metadata()?;
            if let CheckStatus::Over(fh) =
                check_file_size_or_create(get_file_size(&meta), LOG_FILE_MAX_SIZE, &self.data_dir)?
            {
                self.curr_log_file = BufWriter::new(fh);
            };
        }

        let bytes = self
            .curr_log_file
            .write(payload)
            .with_context(|| format!("Failed to append to log writing payload: {:?}", payload))?;

        Ok(bytes)
    }

    /// Writes metadata by overwriting the in-memory buffer.
    /// Flushes to disk after `META_FILE_FLUSH_WRITES` writes.
    pub fn write_meta(&mut self, payload: &[u8]) -> Result<usize> {
        let bytes = self.meta_buf.write(payload);
        if self.meta_buf.should_flush() {
            self.meta_buf
                .flush()
                .with_context(|| "Failed to flush meta buffer to disk")?;
        }

        Ok(bytes)
    }

    pub fn from_data_dir(dir_path: &str) -> Result<Self> {
        let data_dir_path = Path::new(dir_path);
        let _ = validate_or_create_dir(data_dir_path)?; // parent path
        let mut meta_path = data_dir_path.to_path_buf();
        meta_path.push(Path::new(META_FILE_PATH));
        let meta_file = file_utils::open_or_create_file(meta_path.as_path())?;
        let log_file: File;
        let files = get_log_files(data_dir_path)?;

        if files.len() == 0 {
            let fname = generate_file_name();
            log_file = open_or_create_file(&fname, data_dir_path).with_context(|| {
                format!(
                    "Failed to create new file at: {:?}/{:?}",
                    data_dir_path.to_str().unwrap(),
                    fname
                )
            })?;
        } else {
            let latest = &files[files.len() - 1];
            let res = check_file_size_or_create(
                get_file_size(&latest.meta),
                LOG_FILE_MAX_SIZE,
                data_dir_path,
            )?;
            match res {
                CheckStatus::Good => {
                    let fh = open_append_file(&latest.file_path).with_context(|| {
                        format!("Failed to open file at path: {:?}", &latest.file_path)
                    })?;

                    log_file = fh;
                }
                CheckStatus::Over(fh) => log_file = fh,
            }
        }

        Ok(Self {
            curr_log_file: BufWriter::with_capacity(LOG_FILE_FLUSH_LIMIT.into(), log_file),
            meta_buf: MetaBuffer::new(meta_file, META_BUF_CAPACITY.into(), META_FILE_FLUSH_WRITES),
            data_dir: data_dir_path.to_path_buf(),
        })
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        match self.curr_log_file.flush() {
            Ok(_) => println!("[LOG WRITER]: Flushed buffer successfully!"),
            Err(e) => println!("[LOG WRITER]: An error occurred while flushing: {:?}", e),
        }

        match self.meta_buf.flush() {
            Ok(_) => println!("[META FILE]: Flushed buffer successfully!"),
            Err(e) => println!("[META FILE]: An error occurred while flushing: {:?}", e),
        }
    }
}

/// Rebuilds the in-memory key-value store by replaying persisted log files from a directory.
///
/// Reconstructs and returns a HashMap of keys to `Types` by locating all log files under `path` and replaying their entries in order.
///
/// # Returns
/// A `HashMap<String, Types>` containing the last persisted value for each key found in the logs.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// // Rebuild store from the "./data" log directory
/// let store = crate::log::rebuild(Path::new("./data")).unwrap();
/// assert!(store.is_empty() || store.len() >= 0);
/// ```
pub fn rebuild_map(logs: &Vec<LogEntry>) -> HashMap<String, Types> {
    let mut hash: HashMap<String, Types> = HashMap::new();
    tracing::info!("Rebuilding map from logs");

    for log in logs.iter() {
        use crate::serde::Payload;

        match log.payload {
            Payload::Put { ref key, ref value } => hash.insert(key.clone(), value.clone().into()),
            Payload::Delete { ref key } => hash.remove(key),
        };
    }

    hash
}

pub async fn ensure_sentinel_entry(lw_tx: &mpsc::Sender<LogMessage>) -> Result<()> {
    if get_last_log_index() == 0 && get_entry_term(0).is_none() {
        lw_tx
            .send(LogMessage::Append {
                op: Op::Put("__dee_kv_meta__".to_string(), "sentinel".to_string().into()),
                meta: Some((1, 0)), // (term, idx)
            })
            .await
            .with_context(|| "Failed to append sentinel log entry")?;
    }

    Ok(())
}

pub fn load_or_init_kv_state(
    data_dir: &str,
    end_idx: LogIndex,
) -> Result<(HashMap<String, Types>, Vec<(LogEntry, usize)>)> {
    let mut logs_map = HashMap::new();
    let data_dir_path = Path::new(data_dir);
    tracing::info!("Loading log..");
    let files = get_log_files(data_dir_path)?;
    let files_len = files.len();
    tracing::debug!(file_count = files.len(), "Replaying log files");

    if files_len == 0 {
        tracing::info!("No log files found. Initializing log..");
        // init log file and insert sentinel entry
        let fname = generate_file_name();
        let mut file = open_or_create_file(&fname, data_dir_path).with_context(|| {
            format!(
                "Failed to create new file at: {:?}/{:?}",
                data_dir_path.to_str().unwrap(),
                fname
            )
        })?;

        let entry = LogEntry::with_index(
            Payload::Put {
                key: String::from("__dee_kv_meta__"),
                value: String::from("sentinel").into(),
            },
            1,
            0,
        );

        let payload = entry
            .serialize()
            .with_context(|| format!("Failed to serialize sentinel log entry"))?;

        let bytes = payload.as_bytes();
        let _ = file
            .write_all(bytes)
            .with_context(|| format!("Failed to append sentinel log entry"));

        return Ok((logs_map, vec![(entry, bytes.len())]));
    }

    let mut buf = Vec::new(); // to hold last 2 log files to initialize cache
    let mut buf_ref = None;
    let mut logs_map_ref = Some(&mut logs_map);

    for (i, file) in files.into_iter().enumerate() {
        // last 2 log files
        if i == files_len.saturating_sub(2) || i == files_len.saturating_sub(1) {
            buf_ref = Some(&mut buf);
        }

        replay_log_file(file.clone(), &mut logs_map_ref, &mut buf_ref, end_idx)?;
        tracing::debug!(file_path = ?file, "Replayed log file");
    }

    Ok((logs_map, buf))
}

// Atomics to hold last-known log index and term for fast, lock-free reads
pub static LAST_LOG_INDEX: AtomicU32 = AtomicU32::new(0);
pub static LAST_LOG_TERM: AtomicU32 = AtomicU32::new(1);

/// Returns the highest persisted log index known to this process.
///
/// # Returns
///
/// `LastIdx` containing the last persisted log index; `0` if no log entries have been recorded.
///
/// # Examples
///
/// ```
/// let idx = get_last_log_index();
/// // idx == 0 when no logs are present
/// assert!(idx >= 0);
/// ```
pub fn get_last_log_index() -> LogIndex {
    LAST_LOG_INDEX.load(Ordering::SeqCst)
}

pub fn get_last_log_term() -> LogTerm {
    LAST_LOG_TERM.load(Ordering::SeqCst) as LogTerm
}

pub fn get_entry_from_disk(index: u32, skip: u8) -> Option<(LogEntry, u8)> {
    let mut files = get_log_files(Path::new(DATA_DIR)).ok()?;
    files.reverse(); // search from behind

    let delim = LOG_FILE_DELIM.as_bytes()[0];
    let files_iter = files.into_iter().skip(skip as usize);

    for (i, file) in files_iter.enumerate() {
        let fh = open_append_file(&file.file_path).ok()?;
        let reader = BufReader::new(fh);

        for record in reader.split(delim) {
            let bytes = match record {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.is_empty() {
                continue;
            }
            let log = match deserialize_entry::<LogEntry>(&bytes) {
                Ok(log) => log,
                Err(_) => continue,
            };

            if log.index == index {
                return Some((log, (i + 1) as u8));
            }
            if log.index > index {
                return None;
            }
        }
    }

    None
}

pub fn get_entry_term(index: u32) -> Option<LogTerm> {
    let files = get_log_files(Path::new(DATA_DIR)).ok()?;
    let delim = LOG_FILE_DELIM.as_bytes()[0];

    for file in files {
        let fh = open_append_file(&file.file_path).ok()?;
        let reader = BufReader::new(fh);

        for record in reader.split(delim) {
            let bytes = match record {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.is_empty() {
                continue;
            }
            let log = match deserialize_entry::<LogEntry>(&bytes) {
                Ok(log) => log,
                Err(_) => continue,
            };

            if log.index == index {
                return Some(log.term);
            }
            if log.index > index {
                return None;
            }
        }
    }

    None
}

pub fn find_first_index_of_term(term: LogTerm, _skip_n_pages: u8) -> Option<u32> {
    let files = get_log_files(Path::new(DATA_DIR)).ok()?;
    let delim = LOG_FILE_DELIM.as_bytes()[0];

    for file in files {
        let fh = open_append_file(&file.file_path).ok()?;
        let reader = BufReader::new(fh);

        for record in reader.split(delim) {
            let bytes = match record {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.is_empty() {
                continue;
            }
            let log = match deserialize_entry::<LogEntry>(&bytes) {
                Ok(log) => log,
                Err(_) => continue,
            };

            if log.term == term {
                return Some(log.index);
            }
        }
    }

    None
}

pub fn get_entries_from_idx(start_index: u32, max_entries: usize) -> Vec<LogEntry> {
    if max_entries == 0 {
        return Vec::new();
    }

    let files = match get_log_files(Path::new(DATA_DIR)) {
        Ok(files) => files,
        Err(_) => return Vec::new(),
    };

    let delim = LOG_FILE_DELIM.as_bytes()[0];
    let mut entries = Vec::new();

    for file in files {
        let fh = match open_append_file(&file.file_path) {
            Ok(fh) => fh,
            Err(_) => continue,
        };
        let reader = BufReader::new(fh);

        for record in reader.split(delim) {
            let bytes = match record {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.is_empty() {
                continue;
            }
            let log = match deserialize_entry::<LogEntry>(&bytes) {
                Ok(log) => log,
                Err(_) => continue,
            };

            if log.index == 0 || log.index < start_index {
                continue;
            }

            entries.push(log);
            if entries.len() >= max_entries {
                return entries;
            }
        }
    }

    entries
}

/// Set the global last-log term and index used for recovery and coordination.
///
/// This updates the module-level atomic metadata that other threads read to determine
/// the highest known log term and index.
///
/// # Examples
///
/// ```
/// // Initialize last-log metadata to term 2 and index 42
/// init_last_log_meta(2, 42);
/// assert_eq!(get_last_log_term(), 2);
/// assert_eq!(get_last_log_index(), 42);
/// ```
pub fn init_last_log_meta(term: LogTerm, idx: LogIndex) {
    LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);
    LAST_LOG_INDEX.store(idx, Ordering::SeqCst);
}

/// Determine the last persisted log term and index by scanning existing log files.
///
/// Returns the most-recently written log record's `(term, index)`. If no log files or
/// no valid log records are found, returns `(1, 0)`.
///
/// # Examples
///
/// ```
/// let (_term, _index) = crate::log::get_last_log_meta_from_disk();
/// // Use the returned term and index as needed.
/// ```
pub fn get_last_log_meta_from_disk() -> (LogTerm, LogIndex) {
    use crate::{LOG_FILE_DELIM, serde::deserialize_entry};
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    let mut last_term: LogTerm = 1;
    let mut last_idx: LogIndex = 0;

    let files = match get_log_files(Path::new(DATA_DIR)) {
        Ok(f) => f,
        Err(_) => return (last_term, last_idx),
    };

    if files.is_empty() {
        return (last_term, last_idx);
    }

    const CHUNK: usize = 8 * 1024;
    let delim = LOG_FILE_DELIM.as_bytes()[0];

    // Try newest files first
    for file in files.iter().rev() {
        if let Ok(mut fh) = file::open_append_file(&file.file_path) {
            if let Ok(metadata) = fh.metadata() {
                let mut remaining = metadata.len();
                let mut acc: Vec<u8> = Vec::new();

                while remaining > 0 {
                    let read_size = std::cmp::min(remaining, CHUNK as u64) as usize;
                    let start = remaining - read_size as u64;
                    if fh.seek(SeekFrom::Start(start)).is_err() {
                        break;
                    }

                    let mut chunk = vec![0u8; read_size];
                    if fh.read_exact(&mut chunk).is_err() {
                        break;
                    }

                    // prepend chunk to accumulator
                    if acc.is_empty() {
                        acc = chunk;
                    } else {
                        let mut combined = chunk;
                        combined.extend_from_slice(&acc);
                        acc = combined;
                    }

                    // iterate records from the end using rsplit
                    for record in acc.rsplit(|&b| b == delim) {
                        if record.is_empty() {
                            continue; // trailing delimiter
                        }
                        if let Ok(log) = deserialize_entry::<LogEntry>(record) {
                            last_term = log.term;
                            last_idx = log.index;
                            return (last_term, last_idx);
                        } else {
                            // parsing failed for this candidate, try earlier
                            continue;
                        }
                    }

                    remaining = start;
                }
            }
        }
    }

    // No indexed records found across files
    (last_term, last_idx)
}

pub fn get_last_log_meta(log: &Vec<(LogEntry, usize)>) -> (LogTerm, LogIndex) {
    let mut last_term: LogTerm = 1;
    let mut last_index: LogIndex = 0;

    if let Some(e) = log.last() {
        last_term = e.0.term;
        last_index = e.0.index;
    };

    (last_term, last_index)
}

pub fn truncate_logs(
    parent_path: &Path,
    last_index: u32,
) -> Result<(Option<BufWriter<File>>, LogTerm, LogIndex, Vec<PathBuf>)> {
    use anyhow::Error;

    let mut files = get_log_files(parent_path)?;
    files.reverse();

    let mut old_paths: Vec<PathBuf> = Vec::new();
    let mut new_writer: Option<BufWriter<File>> = None;
    let mut last_term: LogTerm = 1;
    let mut last_idx: LogIndex = 0;
    let delim = LOG_FILE_DELIM.as_bytes()[0];

    let mut buf = Vec::new(); // PERF: pre-allocate buffer with expected log file capacity; they should stay around the same size
    'outer: for file in &files {
        let mut fh = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.file_path)?;

        let _ = fh.read_to_end(&mut buf)?;
        let mut offset: usize = 0;

        for bytes in buf.split(|byte| *byte == delim) {
            if bytes.is_empty() {
                offset += 1;
                continue;
            }

            offset += bytes.len() + 1; // entry + delim
            let log = deserialize_entry::<LogEntry>(bytes)?;
            if log.index == last_index {
                last_term = log.term;
                last_idx = log.index;

                fh.set_len(offset as u64)?; // truncate file
                new_writer = Some(BufWriter::with_capacity(LOG_FILE_FLUSH_LIMIT.into(), fh));

                break 'outer;
            } else if log.index > last_index {
                return Err(Error::msg("[truncate_logs]: gap detected")); // gap detected
            }
        }

        buf.clear();
        old_paths.push(file.file_path.clone());
    }

    Ok((new_writer, last_term, last_idx, old_paths))
}
