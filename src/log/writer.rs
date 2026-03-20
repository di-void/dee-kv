use crate::{
    DATA_DIR, LOG_FILE_CHECK_TIMEOUT, LogIndex, LogMessage, LogTerm, Op,
    log::{LAST_LOG_INDEX, LAST_LOG_TERM, cache::LogCache, truncate_logs},
    serde::{CustomSerialize, LogEntry, NodeMeta, Payload},
};
use anyhow::Context;
use std::{
    io::Write,
    sync::{Arc, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc};

/// Starts a dedicated thread that serializes and persists log entries and node metadata.
///
/// The spawned thread listens on the provided `rx` receiver for `LogWriterMsg` commands:
/// - `LogAppend(Op::Put)` and `LogAppend(Op::Delete)`: serialize a `Log` with the current term and next index,
///   append it to the log file, update the atomics `LAST_LOG_INDEX` and `LAST_LOG_TERM`, and increment the next index.
/// - `AppendEntry { .. }`: serialize a `Log` with provided term and index, append it, and update atomics.
/// - `Truncate { last_index }`: rebuild log files up to `last_index` and update atomics.
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
pub fn init_log_writer(
    curr_term: LogTerm,
    mut rx: mpsc::Receiver<LogMessage>,
    log_cache: Arc<RwLock<LogCache>>,
) -> JoinHandle<()> {
    use super::Log;

    let handle = thread::spawn(move || {
        let mut term = curr_term;
        // initialize next_index from the atomic last index (should be set at startup)
        let mut next_index: LogIndex = LAST_LOG_INDEX.load(Ordering::SeqCst).saturating_add(1);

        let mut log = match Log::from_data_dir(DATA_DIR) {
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
            let msg = match rx.blocking_recv() {
                Some(msg) => msg,
                _ => break,
            };

            if now.elapsed() >= timeout {
                now = Instant::now();
                check_delta = true;
            }

            match msg {
                LogMessage::Append { op, meta } => match op {
                    Op::Delete(key) => {
                        if meta.is_some() {
                            let (trm, idx) = meta.unwrap();
                            if term != trm || next_index != idx {
                                term = trm;
                                next_index = idx;
                            };
                        }

                        let log_entry = LogEntry::with_index(
                            Payload::Delete { key: key.clone() },
                            term,
                            next_index,
                        );
                        let payload = log_entry
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Delete payload: ({})", &key)
                            })
                            .unwrap();

                        let mut guard = log_cache.blocking_write();
                        let b = log
                            .append(payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();
                        guard.push(log_entry, payload.as_bytes().len());
                        drop(guard);

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
                        if meta.is_some() {
                            let (trm, idx) = meta.unwrap();
                            if term != trm || next_index != idx {
                                term = trm;
                                next_index = idx;
                            };
                        }

                        let log_entry = LogEntry::with_index(
                            Payload::Put {
                                key: key.clone(),
                                value: val.clone().into(),
                            },
                            term,
                            next_index,
                        );
                        let payload = log_entry
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Put payload: ({}:{:?})", &key, &val)
                            })
                            .unwrap();

                        let mut guard = log_cache.blocking_write();
                        let b = log
                            .append(payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();
                        guard.push(log_entry, payload.as_bytes().len());
                        drop(guard);

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
                LogMessage::Truncate { last_index } => {
                    if let Err(e) = log.curr_log_file.flush() {
                        tracing::error!(error = ?e, "Failed to flush log file before truncation");
                    }

                    let (new_writer, last_term, last_idx, old_paths) =
                        truncate_logs(&log.data_dir, last_index)
                            .with_context(|| {
                                format!("Failed to truncate log to index: {}", last_index)
                            })
                            .unwrap();

                    let old_writer = std::mem::replace(&mut log.curr_log_file, new_writer);
                    drop(old_writer);

                    for path in old_paths {
                        if let Err(e) = std::fs::remove_file(&path) {
                            tracing::error!(error = ?e, file = ?path, "Failed to remove old log file");
                        }
                    }

                    LAST_LOG_INDEX.store(last_idx, Ordering::SeqCst);
                    LAST_LOG_TERM.store(last_term as u32, Ordering::SeqCst);
                    next_index = last_idx.saturating_add(1);

                    tracing::info!(
                        last_index = last_idx,
                        last_term = last_term,
                        "Truncated log"
                    );
                }
                LogMessage::NodeMeta(current_term, voted_for) => {
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

                    let b = log
                        .write_meta(payload.as_bytes())
                        .with_context(|| format!("Failed to write to meta file"))
                        .unwrap();

                    tracing::debug!(bytes = b, "Wrote node metadata to meta file");
                }
                LogMessage::ShutDown => break,
            }
        }

        tracing::info!("Log writer thread shutting down");
    });

    handle
}
