mod log;
mod serde;
pub mod server;
mod store;

pub enum ChannelMessage {
    Append(()),
    ShutDown,
}
