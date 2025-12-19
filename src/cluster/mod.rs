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
    pub votes: (u16, u8), // (term, nVotes)
    pub has_voted: bool,
}

impl CurrentNode {
    pub fn new() {
        //
    }
    pub fn is_follower(&self) -> bool {
        self.role == NodeRole::Follower
    }
    pub fn step_down(&mut self, term: u16) {
        self.term = term;
        self.role = Default::default();
    }
    pub fn promote(&mut self) {
        // check the current node's role
        match self.role {
            NodeRole::Follower => {
                self.role = NodeRole::Candidate;
                self.term += 1;
                self.votes = (self.term, 1);
                self.has_voted = true;
            }
            NodeRole::Candidate => {
                self.role = NodeRole::Leader;
                self.votes = (self.term, 0); // reset votes
                self.has_voted = false;
            }
            _ => {
                dbg!("Cannot promote a Leader! Current node is already a Leader");
            }
        };
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
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
