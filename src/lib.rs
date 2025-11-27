mod log;
mod serde;
pub mod server;
mod store;

pub enum Op {
    Put(String, store::Types), // (key, value)
    Delete(String),            // (key)
}

pub enum ChannelMessage {
    Append(Op),
    ShutDown,
}

pub const DATA_DIR: &str = "./DATA";
pub const MAX_LOG_FILE_SIZE: u64 = 5_000_000; // 5mb
pub const LOG_FILE_EXT: &str = "aof";
