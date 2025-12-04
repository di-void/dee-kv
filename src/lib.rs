pub mod cluster;
mod log;
mod serde;
pub mod server;
mod store;
pub mod utils;

#[derive(Debug)]
pub enum Op {
    Put(String, store::Types), // (key, value)
    Delete(String),            // (key)
}

pub enum ChannelMessage {
    Append(Op),
    ShutDown,
}

pub mod store_proto {
    tonic::include_proto!("store");
}

pub mod health_proto {
    tonic::include_proto!("health");
}

pub const DATA_DIR: &str = "./DATA";
pub const LOOPBACK_NET_INT_STRING: &str = "loopback";
pub const WILDCARD_NET_INT_STRING: &str = "wildcard";
pub const LOCAL_HOST_IPV6: &str = "[::1]";
pub const LOCAL_HOST_IPV4: &str = "127.0.0.1";
pub const WILDCARD_IPV4: &str = "0.0.0.0";
pub const MAX_LOG_FILE_SIZE: u64 = 5_000_000; // 5mb
pub const LOG_FILE_EXT: &str = "aof";
pub const LOG_FILE_DELTA_THRESH: u8 = 90; // 90%
pub const LOG_FILE_CHECK_TIMEOUT: u32 = 5 * 60 * 1000; // 5 mins
pub const LOG_FILE_DELIM: &str = "\0";
pub const LOG_FILE_BUF_MAX: u16 = 8000; // rust default: 8kb
