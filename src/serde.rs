use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Serialize, Deserialize)]
pub enum Operation {
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
    pub operation: Operation,
    pub payload: Payload,
}

// serialize entry
pub fn serialize_entry(entry: Log) -> Result<String> {
    let mut s = serde_json::to_string(&entry)?;
    s.push_str("\n");
    Ok(s)
}

// deserialize entry
pub fn deserialize_entry(entry: String) -> Result<Log> {
    let s = entry.trim();
    let l = serde_json::from_str::<Log>(s)?;
    Ok(l)
}

// https://docs.rs/serde_json/latest/serde_json/
// https://docs.rs/serde/latest/serde/
