mod file;
mod writer;

use crate::{
    LOG_FILE_CHECK_TIMEOUT,
    log::file::{get_log_files, replay_log_file},
    store::Types,
};
use std::{
    collections::HashMap,
    path::Path,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::serde::{Log, LogOperation, Payload};
use crate::{DATA_DIR, LogWriterMessage, Op};
use anyhow::{Context, Result};
use tokio::sync::mpsc;

/// Runs on a separate thread
pub fn init_log_writer(mut rx: mpsc::Receiver<LogWriterMessage>) -> JoinHandle<()> {
    use writer::LogWriter;

    let handle = thread::spawn(move || {
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
            let term = 1;
            if now.elapsed() >= timeout {
                now = Instant::now();
                check_delta = true;
            }

            match msg {
                LogWriterMessage::LogAppend(m) => match m {
                    Op::Delete(key) => {
                        let log = Log {
                            operation: LogOperation::Delete,
                            payload: Payload::Delete { key: key.clone() },
                            term,
                        };
                        let serialized_payload = log
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Delete payload: ({})", &key)
                            })
                            .unwrap();

                        let b = log_w
                            .append_log(serialized_payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        println!("LogWriter wrote {b} bytes for {:?}", Op::Delete(key));
                    }
                    Op::Put(key, val) => {
                        let log = Log {
                            operation: LogOperation::Put,
                            payload: Payload::Put {
                                key: key.clone(),
                                value: val.clone().into(),
                            },
                            term,
                        };
                        let serialized_payload = log
                            .serialize()
                            .with_context(|| {
                                format!("Failed to serialize Put payload: ({}:{:?})", &key, &val)
                            })
                            .unwrap();

                        let b = log_w
                            .append_log(serialized_payload.as_bytes(), check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        println!(
                            "LogWriter wrote {b} bytes for {:?}",
                            Op::Put(key, val.into())
                        );
                    }
                },
                LogWriterMessage::NodeMeta(curr_term, voted_for) => {
                    // construct serializable meta object from passed args
                    // serialize the object and then call the function to write
                    // to the meta file
                }
                LogWriterMessage::ShutDown => break,
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
