use serde::{Deserialize, Serialize};
use serde_json::Result;

use crate::LOG_FILE_DELIM;

#[derive(Serialize, Deserialize)]
pub enum LogOperation {
    Put,
    Delete,
}

#[derive(Serialize, Deserialize)]
pub enum Payload {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Serialize, Deserialize)]
pub struct Log {
    pub operation: LogOperation,
    pub payload: Payload,
    pub term: u16,
}

#[derive(Serialize, Deserialize)]
pub struct NodeMeta {
    current_term: u16,
    voted_for: Option<u8>,
}

impl Log {
    pub fn serialize(&self) -> Result<String> {
        let mut s = serialize_entry(self)?;
        s.push_str(LOG_FILE_DELIM);
        Ok(s)
    }
}

// serialize entry
fn serialize_entry<T: Serialize>(entry: T) -> Result<String> {
    Ok(serde_json::to_string(&entry)?)
}

// deserialize entry
pub fn deserialize_entry<'de, T: Deserialize<'de>>(entry: &'de [u8]) -> Result<T> {
    let le = serde_json::from_slice::<T>(entry)?;
    Ok(le)
}

// https://docs.rs/serde_json/latest/serde_json/
// https://docs.rs/serde/latest/serde/
