use serde::{Deserialize, Serialize};
use serde_json::Result;

use crate::{LOG_FILE_DELIM, Term};

pub trait CustomSerialize {
    fn serialize(&self) -> Result<String>;
}

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
    pub term: Term,
    pub index: u32,
}

// Each log entry now includes a monotonic `index` for fast lookups.
impl Log {
    /// Creates a `Log` record using the provided operation, payload, term, and index.
    ///
    /// The `index` is the record's monotonic position used for lookups and ordering.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::{Log, LogOperation, Payload, Term};
    ///
    /// let op = LogOperation::Put;
    /// let payload = Payload::Put { key: "k".into(), value: "v".into() };
    /// let term: Term = 1;
    /// let log = Log::with_index(op, payload, term, 42);
    ///
    /// assert_eq!(log.term, 1);
    /// assert_eq!(log.index, 42);
    /// if let Payload::Put { key, value } = log.payload {
    ///     assert_eq!(key, "k");
    ///     assert_eq!(value, "v");
    /// }
    /// ```
    pub fn with_index(operation: LogOperation, payload: Payload, term: Term, index: u32) -> Self {
        Log {
            operation,
            payload,
            term,
            index,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeMeta {
    pub current_term: Term,
    pub voted_for: Option<u8>,
}

impl CustomSerialize for NodeMeta {
    fn serialize(&self) -> Result<String> {
        Ok(serialize_entry(self)?)
    }
}

impl CustomSerialize for Log {
    fn serialize(&self) -> Result<String> {
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