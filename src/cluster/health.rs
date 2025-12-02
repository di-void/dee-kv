use super::{Node, Peer, PeerStatus};
use crate::{
    cluster::{HEARTBEAT_INTERVAL_MS, PEER_FAILURE_TIMEOUT_MS},
    health_proto::{PingRequest, health_check_client::HealthCheckClient},
};
use anyhow::Result;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::{Code, Request, transport::Channel};

pub async fn start_heartbeat_loop(peers_table: Arc<Vec<Arc<Mutex<Peer>>>>) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            for p in &*peers_table {
                let peer = Arc::clone(p);

                tokio::spawn(async move {
                    // handler
                    let mut p = peer.lock().await;
                    let mut ping_req = Request::new(PingRequest {});
                    ping_req.set_timeout(Duration::from_millis(500));

                    // ping the node
                    let res = p.client.ping(ping_req).await;
                    let now = Instant::now();

                    match res {
                        Ok(_) => {
                            p.last_ping = now;
                            p.status = PeerStatus::Alive;
                        }
                        Err(e) => {
                            if let Code::DeadlineExceeded = e.code() {
                                if now.duration_since(p.last_ping)
                                    > Duration::from_millis(PEER_FAILURE_TIMEOUT_MS.into())
                                {
                                    p.status = PeerStatus::Dead;
                                }
                            }
                        }
                    }
                });
            }

            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS.into()));
        }
    })
}

pub async fn init_peers(p_nodes: &Vec<Node>) -> Result<Vec<Arc<Mutex<Peer>>>> {
    let mut peers = vec![];

    for n in p_nodes.iter() {
        let client = create_client(n.address.clone()).await?;
        let peer = Peer {
            client,
            id: n.id,
            last_ping: Instant::now(),
            status: PeerStatus::Alive,
        };
        let peer = Arc::new(Mutex::new(peer));

        peers.push(peer);
    }

    Ok(peers)
}

async fn create_client(addr: String) -> Result<HealthCheckClient<Channel>> {
    let c = HealthCheckClient::connect(addr).await?;
    Ok(c)
}
