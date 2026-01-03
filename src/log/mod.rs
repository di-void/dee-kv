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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::sync::mpsc;

/// Runs on a separate thread
pub fn init_log_writer(curr_term: Term, mut rx: mpsc::Receiver<LogWriterMsg>) -> JoinHandle<()> {
    use writer::LogWriter;

    let handle = thread::spawn(move || {
        let mut term = curr_term;
        // initialize next_index from the atomic last index (should be set at startup)
        let mut next_index: LastIdx = LAST_LOG_INDEX.load(Ordering::SeqCst).saturating_add(1);

        let mut log_w = match LogWriter::with_data_dir(DATA_DIR) {
            Ok(lw) => lw,
            Err(e) => {
                println!(
                    "Failed to initalize Log writer!.\n Error: {:#?}\n\n Killing writer thread!",
                    e
                );
                panic!("Writer thread panicked on startup!");
            }
        };

        println!("Writer thread has started!");

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
                LogWriterMsg::LogAppend(m) => match m {
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
                        // update atomics to reflect the newly appended entry
                        LAST_LOG_INDEX.store(next_index, Ordering::SeqCst);
                        LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);
                        // increment index after successful append
                        next_index = next_index.saturating_add(1);

                        println!("LogWriter wrote {b} bytes for {:?}", Op::Delete(key));
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
                        // update atomics to reflect the newly appended entry
                        LAST_LOG_INDEX.store(next_index, Ordering::SeqCst);
                        LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);
                        // increment index after successful append
                        next_index = next_index.saturating_add(1);

                        println!(
                            "LogWriter wrote {b} bytes for {:?}",
                            Op::Put(key, val.into())
                        );
                    }
                },
                LogWriterMsg::NodeMeta(current_term, voted_for) => {
                    let meta = NodeMeta {
                        current_term,
                        voted_for,
                    };
                    term = current_term;

                    let payload = meta
                        .serialize()
                        .with_context(|| format!("Failed to serliaze meta object: {:?}", meta))
                        .unwrap();
                    let b = log_w
                        .write_meta(payload.as_bytes())
                        .with_context(|| format!("Failed to write to meta file"))
                        .unwrap();

                    println!(
                        "LogWriter wrote {b} bytes for {{ current_term: {:?}, voted_for: {:?} }} to meta file",
                        current_term, voted_for
                    );
                }
                LogWriterMsg::ShutDown => break,
            }
        }

        println!("Shutting down log writer..");
    });

    handle
}

// replay file to rebuild in-mem map
pub fn load_store(path: &Path) -> Result<HashMap<String, Types>> {
    let mut hash: HashMap<String, Types> = HashMap::new();
    println!("Rebuilding map..");
    let files = get_log_files(path)?;
    println!("Replaying logs..");
    for file in files {
        replay_log_file(file.clone(), &mut hash)?;
        println!("Done replaying log file: {:?}", file);
    }

    Ok(hash)
}

type LastIdx = u64;
type LastTerm = Term;
// Atomics to hold last-known log index and term for fast, lock-free reads
pub static LAST_LOG_INDEX: AtomicU64 = AtomicU64::new(0);
pub static LAST_LOG_TERM: AtomicU32 = AtomicU32::new(1);

pub fn get_last_log_index() -> LastIdx {
    LAST_LOG_INDEX.load(Ordering::SeqCst)
}

pub fn get_last_log_term() -> LastTerm {
    LAST_LOG_TERM.load(Ordering::SeqCst) as LastTerm
}

pub fn init_last_log_meta(term: LastTerm, idx: LastIdx) {
    LAST_LOG_TERM.store(term as u32, Ordering::SeqCst);
    LAST_LOG_INDEX.store(idx, Ordering::SeqCst);
}
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

                    // iterate records from the end using rsplit (zero-copy)
                    for record in acc.rsplit(|&b| b == delim) {
                        if record.is_empty() {
                            continue;
                        }
                        if let Ok(log) = deserialize_entry::<Log>(record) {
                            last_term = log.term;
                            last_idx = log.index as LastIdx;
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
