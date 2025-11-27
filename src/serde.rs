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
}

// serialize entry
pub fn serialize_entry(entry: Log) -> Result<String> {
    let mut se = serde_json::to_string(&entry)?;
    se.push_str(LOG_FILE_DELIM);
    Ok(se)
}

// deserialize entry
pub fn deserialize_entry(entry: &[u8]) -> Result<Log> {
    let le = serde_json::from_slice::<Log>(entry)?;
    Ok(le)
}

// https://docs.rs/serde_json/latest/serde_json/
// https://docs.rs/serde/latest/serde/
