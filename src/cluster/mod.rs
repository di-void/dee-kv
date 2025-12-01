pub mod config;

#[derive(Debug)]
pub struct Cluster {
    name: String,
    self_id: u8,
    self_address: String,
    peers: Vec<Peer>,
}

#[derive(Debug)]
pub enum PeerStatus {
    Alive,
    Dead,
}

#[derive(Debug)]
pub struct Peer {
    id: u8,
    address: String,
    status: PeerStatus,
}
