mod file;
mod writer;

use crate::{
    LOG_FILE_CHECK_TIMEOUT,
    log::file::{get_log_files, replay_log_file},
    serde::{CustomSerialize, NodeMeta},
    store::Types,
};
use std::{
    collections::HashMap,
    path::Path,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::serde::{Log, LogOperation, Payload};
use crate::{DATA_DIR, LogWriterMsg, Op, Term};
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::mpsc;

/// Starts a dedicated thread that serializes and persists log entries and node metadata.
///
/// The spawned thread listens on the provided `rx` receiver for `LogWriterMsg` commands:
/// - `LogAppend(Op::Put)` and `LogAppend(Op::Delete)`: serialize a `Log` with the current term and next index,
///   append it to the log file, update the atomics `LAST_LOG_INDEX` and `LAST_LOG_TERM`, and increment the next index.
/// - `NodeMeta(current_term, voted_for)`: serialize and write node metadata to the meta file and update the active term.
/// - `ShutDown`: stop the writer thread and return.
///
/// The function panics if the underlying `LogWriter` cannot be initialized. The caller is responsible for joining
/// the returned handle to observe thread termination.
///
/// # Examples
///
/// ```
/// use tokio::sync::mpsc;
/// use crate::log::{init_log_writer, LogWriterMsg, Term};
///
/// // create a channel and start the writer thread
/// let (tx, rx) = mpsc::channel(1);
/// let handle = init_log_writer(1 as Term, rx);
///
/// // request shutdown and wait for the writer to exit
/// let _ = tokio::spawn(async move { let _ = tx.send(LogWriterMsg::ShutDown).await; });
/// handle.join().unwrap();
/// ```
pub fn init_log_writer(curr_term: Term, mut rx: mpsc::Receiver<LogWriterMsg>) -> JoinHandle<()> {
    use writer::LogWriter;

    let handle = thread::spawn(move || {
        let mut term = curr_term;
        // initialize next_index from the atomic last index (should be set at startup)
        let mut next_index: LastIdx = LAST_LOG_INDEX.load(Ordering::SeqCst).saturating_add(1);

        let mut log_w = match LogWriter::with_data_dir(DATA_DIR) {
            Ok(lw) => lw,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    "Failed to initialize log writer, killing writer thread"
                );
                panic!("Writer thread panicked on startup!");
            }
        };

        tracing::info!("Log writer thread started");

        let mut now = Instant::now();
        let mut check_delta = false;
        let timeout = Duration::from_millis(LOG_FILE_CHECK_TIMEOUT as u64);

        loop {
            let msg = rx.blocking_recv().unwrap(); // DANGER
            if now.elapsed() >= timeout {
                now = Instant::now();
                check_delta = true;
            }

            match msg {
                LogWriterMsg::LogAppend(op) => match op {
                    Op::Delete(key) => {
                        let log = Log::with_index(
                            LogOperation::Delete,
                            Payload::Delete { key: key.clone() },
                            term,
                            next_index,
                        );
                        let payload = log
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Delete payload: ({})", &key)
                            })
                            .unwrap();

                        let b = log_w
                            .append_log(payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        LAST_LOG_INDEX.store(next_index, Ordering::SeqCst);
                        LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);

                        // increment index after successful append
                        next_index = next_index.saturating_add(1);

                        tracing::debug!(
                            bytes = b,
                            index = next_index - 1,
                            term = term,
                            "Wrote Delete operation to log"
                        );
                    }
                    Op::Put(key, val) => {
                        let log = Log::with_index(
                            LogOperation::Put,
                            Payload::Put {
                                key: key.clone(),
                                value: val.clone().into(),
                            },
                            term,
                            next_index,
                        );
                        let payload = log
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Put payload: ({}:{:?})", &key, &val)
                            })
                            .unwrap();

                        let b = log_w
                            .append_log(payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        LAST_LOG_INDEX.store(next_index, Ordering::SeqCst);
                        LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);
                        next_index = next_index.saturating_add(1);

                        tracing::debug!(
                            bytes = b,
                            index = next_index - 1,
                            term = term,
                            "Wrote Put operation to log"
                        );
                    }
                },
                LogWriterMsg::NodeMeta(current_term, voted_for) => {
                    let meta = NodeMeta {
                        current_term,
                        voted_for,
                    };

                    if current_term != term {
                        tracing::info!(
                            prev_term = term,
                            term = current_term,
                            voted_for = ?voted_for,
                            "Persisting updated term to meta store"
                        );
                    }
                    term = current_term;

                    let payload = meta
                        .serialize()
                        .with_context(|| format!("Failed to serliaze meta object: {:?}", meta))
                        .unwrap();

                    let b = log_w
                        .write_meta(payload.as_bytes())
                        .with_context(|| format!("Failed to write to meta file"))
                        .unwrap();

                    tracing::debug!(bytes = b, "Wrote node metadata to meta file");
                }
                LogWriterMsg::ShutDown => break,
            }
        }

        tracing::info!("Log writer thread shutting down");
    });

    handle
}

// replay file to rebuild in-mem map
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
/// let store = crate::log::load_store(Path::new("./data")).unwrap();
/// assert!(store.is_empty() || store.len() >= 0);
/// ```
pub fn load_store(path: &Path) -> Result<HashMap<String, Types>> {
    let mut hash: HashMap<String, Types> = HashMap::new();
    tracing::info!("Rebuilding store from log files");
    let files = get_log_files(path)?;
    tracing::debug!(file_count = files.len(), "Replaying log files");
    for file in files {
        replay_log_file(file.clone(), &mut hash)?;
        tracing::debug!(file_path = ?file, "Replayed log file");
    }

    Ok(hash)
}

type LastIdx = u32;
type LastTerm = Term;
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
pub fn get_last_log_index() -> LastIdx {
    LAST_LOG_INDEX.load(Ordering::SeqCst)
}

pub fn get_last_log_term() -> LastTerm {
    LAST_LOG_TERM.load(Ordering::SeqCst) as LastTerm
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
pub fn init_last_log_meta(term: LastTerm, idx: LastIdx) {
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
/// let (_term, _index) = crate::log::get_log_meta();
/// // Use the returned term and index as needed.
/// ```
pub fn get_log_meta() -> (LastTerm, LastIdx) {
    use crate::{LOG_FILE_DELIM, serde::deserialize_entry};
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    let mut last_term: LastTerm = 1;
    let mut last_idx: LastIdx = 0;

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
        if let Ok(mut fh) = file::open_file(&file.file_path) {
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
                        if let Ok(log) = deserialize_entry::<Log>(record) {
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
