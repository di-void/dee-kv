pub mod config;
pub mod consensus;
pub mod hearbeats;

use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tonic::transport::Channel;

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
    pub self_address: SocketAddr,
    pub peers: Vec<Node>,
    pub quorom: u8,
}

#[derive(Debug)]
pub enum PeerStatus {
    Alive,
    Dead,
}

pub struct CurrentNode {
    pub id: u8,
    pub term: u16,
    pub role: NodeRole,
    pub votes: u8,
    pub has_voted: bool,
}

impl CurrentNode {
    // pub fn is_leader(&self) -> bool {
    //     self.role == NodeRole::Leader
    // }
    // pub fn is_candidate(&self) -> bool {
    //     self.role == NodeRole::Candidate
    // }
    pub fn reset_role(&mut self) {
        self.role = Default::default();
    }
}

#[derive(Debug)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Follower
    }
}

#[derive(Debug)]
pub struct Peer {
    pub id: u8,
    pub role: NodeRole,
    pub status: PeerStatus,
    pub last_ping: std::time::Instant,
    pub client: Channel,
}

pub type PeersTable = Vec<Arc<Mutex<Peer>>>;

pub const HEARTBEAT_INTERVAL_MS: u16 = 1000;
pub const PEER_FAILURE_TIMEOUT_MS: u16 = 5000; // 5 secs
pub const LEADER_HEARTBEAT_INTERVAL_MS: u8 = 50;
