mod log;
mod serde;
pub mod server;
mod store;

pub enum ChannelMessage {
    Append(()),
    ShutDown,
}

pub const DATA_DIR: &str = "./DATA";
pub const MAX_LOG_FILE_SIZE: u64 = 5_000_000; // 5mb
pub const LOG_FILE_EXT: &str = "aof";
