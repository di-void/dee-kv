mod file;
mod writer;
use crate::store::Types;
use std::{
    collections::HashMap,
    thread::{self, JoinHandle},
};

use crate::{ChannelMessage, DATA_DIR};
use tokio::sync::mpsc::Receiver;

pub enum Operation {
    Put(String, Types), // (key, value)
    Delete(String),     // (key)
}

pub fn setup_writer(mut rx: Receiver<ChannelMessage>) -> JoinHandle<()> {
    use anyhow::Result;
    use writer::LogWriter;

    let handle = thread::spawn(move || {
        let log_w = match LogWriter::from_data_dir(DATA_DIR) {
            Result::Ok(lw) => lw,
            Result::Err(e) => {
                println!(
                    "Failed to initalize Log writer!.\n Error: {:#?}\n\n Killing writer thread!",
                    e
                );
                panic!("Writer thread panicked on startup!");
            }
        };

        loop {
            let msg = rx.blocking_recv().unwrap(); // dangerous
            match msg {
                ChannelMessage::Append(m) => {}
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
