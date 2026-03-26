pub mod cluster;
pub mod log;
mod serde;
pub mod server;
pub mod services;
pub mod state;
pub mod utils;

#[derive(Debug)]
pub enum Op {
    Put(String, state::Types), // (key, value)
    Delete(String),            // (key)
}

pub enum LogMessage {
    Append {
        op: Op,
        meta: Option<(LogTerm, LogIndex)>,
    },
    Truncate {
        last_index: LogIndex,
    },
    // NodeMeta(LogTerm, Option<u8>, u32, u32), // (currentTerm, votedFor, commitIndex, lastAppliedIndex)
    NodeMeta {
        curr_term: LogTerm,
        voted_for: Option<u8>,
        last_applied_idx: LogIndex,
    },
    ShutDown,
}

pub enum ConsensusMessage {
    ResetTimer,
    Init,
}

pub mod store_proto {
    tonic::include_proto!("store");
}
pub mod health_proto {
    tonic::include_proto!("health");
}
pub mod consensus_proto {
    tonic::include_proto!("consensus");
}

pub type LogTerm = u16;
pub type LogIndex = u32;

pub const DATA_DIR: &str = "./DATA";
pub const LOOPBACK_NET_INT_STRING: &str = "loopback";
pub const WILDCARD_NET_INT_STRING: &str = "wildcard";
pub const LOCAL_HOST_IPV6: &str = "[::1]";
pub const LOCAL_HOST_IPV4: &str = "127.0.0.1";
pub const WILDCARD_IPV4: &str = "0.0.0.0";
pub const META_FILE_PATH: &str = "./meta.json";
pub const META_BUF_CAPACITY: u8 = 100; // 100 bytes (buffer capacity)
pub const META_FILE_FLUSH_WRITES: u16 = 5; // flush to disk after this many writes
pub const LOG_FILE_EXT: &str = "aof";
pub const LOG_FILE_MAX_SIZE: u64 = 3_000_000; // 3MB
pub const LOG_FILE_MAX_DELTA: u8 = 90; // 90%
pub const LOG_FILE_CHECK_TIMEOUT: u32 = 5 * 60 * 1000; // 5 mins
pub const LOG_FILE_DELIM: &str = "\0";
pub const LOG_FILE_FLUSH_LIMIT: u16 = 8000; // rust default: 8KB
pub const LOG_CACHE_LIMIT: u32 = 6_000_000; // 6MB
