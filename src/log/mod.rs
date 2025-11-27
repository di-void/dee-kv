mod file;
mod writer;

use crate::store::Types;
use std::{
    collections::HashMap,
    thread::{self, JoinHandle},
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

        loop {
            let msg = rx.blocking_recv().unwrap(); // DANGER
            match msg {
                ChannelMessage::Append(m) => match m {
                    Op::Delete(key) => {
                        let serialized_payload = serialize_entry(Log {
                            operation: LogOperation::Delete,
                            payload: Payload::Delete { key: key.clone() },
                        })
                        .with_context(|| format!("Failed to serialize Delete payload: ({key})"))
                        .unwrap();

                        let _ = log_w
                            .append(serialized_payload)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();
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
                            format!("Failed to serialize Put payload: ({key}:{:?})", val)
                        })
                        .unwrap();

                        let _ = log_w
                            .append(serialized_payload)
                            .with_context(|| format!("Failed to append to log file"))
                            .unwrap();
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
