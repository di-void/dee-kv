use crate::store::Types;
use std::{
    collections::HashMap,
    thread::{self, JoinHandle},
};

use crate::ChannelMessage;
use tokio::sync::mpsc::Receiver;

enum Operation {
    Put(String, Types), // (key, value)
    Delete(String),     // (key)
}

pub fn setup_writer(mut rx: Receiver<ChannelMessage>) -> JoinHandle<()> {
    let handle = thread::spawn(move || {
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

// add entry to append-only log
pub fn append(op: Operation) {
    todo!("write to the log")
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
