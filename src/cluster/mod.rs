pub mod config;
pub mod consensus;
pub mod hearbeats;

use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::Term;

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

#[derive(Debug)]
pub struct CurrentNode {
    pub id: u8,
    pub term: Term,
    pub role: NodeRole,
    pub voted_for: Option<u8>,
    pub votes: u8,
    pub commit_index: u32,
}

impl CurrentNode {
    pub fn is_follower(&self) -> bool {
        self.role == NodeRole::Follower
    }
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }
    pub fn step_down(&mut self, term: Term) {
        // in case of racing calls
        if term > self.term {
            self.term = term;
        }
        self.votes = 0;
        self.voted_for = None;
        self.role = Default::default();
    }
    pub fn from_meta(node_id: u8) -> anyhow::Result<Self> {
        use crate::serde::{CustomSerialize, NodeMeta, deserialize_entry};
        use crate::utils::file;
        use std::path::PathBuf;

        // Build full path
        let mut meta_path = PathBuf::from(crate::DATA_DIR);
        meta_path.push(crate::META_FILE_PATH);

        let node_meta = if file::file_exists(&meta_path) {
            let content = file::read_file(&meta_path)?;
            deserialize_entry::<NodeMeta>(&content)?
        } else {
            let meta = NodeMeta {
                current_term: 1,
                voted_for: None,
            };
            std::fs::write(&meta_path, meta.serialize()?.as_bytes())?;
            meta
        };

        // Set votes to 1 if voted_for equals node_id, otherwise 0
        let votes = if node_meta.voted_for == Some(node_id) {
            1
        } else {
            0
        };

        Ok(Self {
            id: node_id,
            term: node_meta.current_term,
            role: Default::default(),
            voted_for: node_meta.voted_for,
            votes,
            commit_index: 0,
        })
    }
    pub fn promote(&mut self) {
        match self.role {
            NodeRole::Follower => {
                self.role = NodeRole::Candidate;
                let prev_term = self.term;
                self.term += 1;
                self.votes = 1;
                self.voted_for = Some(self.id);
                tracing::info!(
                    node_id = self.id,
                    term = self.term,
                    prev_term = prev_term,
                    "Promoted from Follower to Candidate"
                );
            }
            NodeRole::Candidate => {
                self.role = NodeRole::Leader;
                tracing::info!(
                    node_id = self.id,
                    term = self.term,
                    "Promoted from Candidate to Leader"
                );
            }
            _ => {
                tracing::warn!(
                    node_id = self.id,
                    "Cannot promote a Leader, already in Leader role"
                );
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

struct ChannelBuilder {
    endpoint: Endpoint,
}

impl ChannelBuilder {
    fn from_addr(addr: &str) -> anyhow::Result<Self> {
        let mut parts = http::uri::Parts::default();
        parts.scheme = Some("http".parse().unwrap());
        parts.authority = Some(addr.parse().unwrap());
        parts.path_and_query = Some("/".parse().unwrap());
        let uri = Uri::from_parts(parts).unwrap();

        let endpoint = Endpoint::from_shared(uri.to_string())?;
        Ok(Self { endpoint })
    }

    pub async fn create_channel(&self) -> anyhow::Result<Channel> {
        Ok(self
            .endpoint
            .clone()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?)
    }

    pub fn create_lazy_channel(&self) -> Channel {
        self.endpoint
            .clone()
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy()
    }
}

#[derive(Debug)]
pub struct Peer {
    pub id: u8,
    pub role: NodeRole,
    pub status: PeerStatus,
    pub last_ping: std::time::Instant,
    pub channel: Channel,
    pub next_index: u32,
    pub match_index: u32,
}

pub type PeersTable = Vec<Arc<Mutex<Peer>>>;

pub const HEARTBEAT_INTERVAL_MS: u16 = 1000;
pub const PEER_FAILURE_TIMEOUT_MS: u16 = 5000; // 5 secs
pub const LEADER_HEARTBEAT_INTERVAL_MS: u8 = 50;
