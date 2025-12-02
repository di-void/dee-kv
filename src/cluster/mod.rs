pub mod config;
pub mod health;
use serde::Deserialize;
use tonic::transport::Channel;

use crate::health_proto::health_check_client::HealthCheckClient;

#[derive(Deserialize, Clone, Debug)]
pub struct Node {
    id: u8,
    address: String,
}

#[derive(Deserialize)]
struct ClusterConfig {
    cluster_name: String,
    nodes: Vec<Node>,
}

#[derive(Debug)]
pub struct Cluster {
    pub name: String,
    pub self_id: u8,
    pub self_address: String,
    pub peers: Vec<Node>,
}

#[derive(Debug)]
pub enum PeerStatus {
    Alive,
    Dead,
}

#[derive(Debug)]
pub struct Peer {
    id: u8,
    status: PeerStatus,
    last_ping: std::time::Instant,
    client: HealthCheckClient<Channel>,
}

pub const HEARTBEAT_INTERVAL_MS: u16 = 1000;
pub const PEER_FAILURE_TIMEOUT_MS: u16 = 5000; // 5 secs
