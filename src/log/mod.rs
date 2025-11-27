mod file;
mod writer;

use crate::{LOG_FILE_CHECK_TIMEOUT, store::Types};
use std::{
    collections::HashMap,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::serde::{Log, LogOperation, Payload, serialize_entry};
use crate::{ChannelMessage, DATA_DIR, Op};
use anyhow::Context;
use tokio::sync::mpsc::Receiver;

pub fn setup_log_writer(mut rx: Receiver<ChannelMessage>) -> JoinHandle<()> {
    use writer::LogWriter;

    let handle = thread::spawn(move || {
        let mut log_w = match LogWriter::from_data_dir(DATA_DIR) {
            Ok(lw) => lw,
            Err(e) => {
                println!(
                    "Failed to initalize Log writer!.\n Error: {:#?}\n\n Killing writer thread!",
                    e
                );
                panic!("Writer thread panicked on startup!");
            }
        };

        let mut now = Instant::now();
        let mut check_delta = false;
        let timeout = Duration::from_millis(LOG_FILE_CHECK_TIMEOUT as u64);

        loop {
            let msg = rx.blocking_recv().unwrap(); // DANGER
            let has_timed_out = now.elapsed() >= timeout;
            if has_timed_out {
                now = Instant::now();
                check_delta = true;
            }

            match msg {
                ChannelMessage::Append(m) => match m {
                    Op::Delete(key) => {
                        let serialized_payload = serialize_entry(Log {
                            operation: LogOperation::Delete,
                            payload: Payload::Delete { key: key.clone() },
                        })
                        .with_context(|| format!("Failed to serialize Delete payload: ({})", &key))
                        .unwrap();

                        let b = log_w
                            .append(serialized_payload, check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        println!("LogWriter wrote {b} bytes for {:?}", Op::Delete(key));
                    }
                    Op::Put(key, val) => {
                        let serialized_payload = serialize_entry(Log {
                            operation: LogOperation::Put,
                            payload: Payload::Put {
                                key: key.clone(),
                                value: val.clone().into(),
                            },
                        })
                        .with_context(|| {
                            format!("Failed to serialize Put payload: ({}:{:?})", &key, &val)
                        })
                        .unwrap();

                        let b = log_w
                            .append(serialized_payload, check_delta)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();

                        check_delta = false;

                        println!(
                            "LogWriter wrote {b} bytes for {:?}",
                            Op::Put(key, val.into())
                        );
                    }
                },
                ChannelMessage::ShutDown => {
                    // cleanup
                    break;
                }
            }
        }
    });

    handle
}

// replay file to rebuild in-mem map
pub fn load_store() -> HashMap<String, Types> {
    // TODO: replay log file

    let test_kv = [("foo", "bar"), ("boo", "baz"), ("dee", "kv")];
    let test_hash = test_kv
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string().into()))
        .collect::<HashMap<_, _>>();

    test_hash
}

// compact the log
