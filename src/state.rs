use crate::{
    log,
    serde::{LogEntry, Payload},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Types {
    #[serde(untagged)]
    String(String),
}

pub struct Store {
    inner: HashMap<String, Types>,
}

impl Store {
    pub fn from_logs(logs: &Vec<LogEntry>) -> Self {
        Self {
            inner: log::rebuild_map(logs),
        }
    }

    pub fn get(&self, k: &str) -> Option<Types> {
        self.inner.get(k).map(|v| v.to_owned())
    }

    pub fn set(&mut self, kv: (&str, Types)) {
        self.inner.insert(kv.0.to_string(), kv.1);
    }

    pub fn delete(&mut self, k: &str) -> Option<Types> {
        self.inner.remove(k)
    }

    pub fn apply_log(&mut self, log: &LogEntry) {
        match &log.payload {
            Payload::Put { key, value } => {
                self.set((key, value.clone().into()));
            }
            Payload::Delete { key } => {
                let _ = self.delete(key);
            }
        }
    }
}

impl From<String> for Types {
    fn from(value: String) -> Self {
        Types::String(value)
    }
}

impl From<Types> for String {
    fn from(value: Types) -> Self {
        match value {
            Types::String(s) => s,
        }
    }
}
