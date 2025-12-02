// create a table of node peers with instantiated channels
// each entry will have it's own lock so we can lock individually
// start a heartbeat thread taking a reference to this table
// in an infinite loop, we go through each peer
// and for each peer we lock and ping it
// then we spawn an async task where the returned future of the ping and reference to the entry will be sent
// then we release the lock on the entry and move on to the next one
// on the entry 'handler' task, we poll the future setting a request timeout
// if the response comes before the request timeout
// we try to acquire a lock on the entry
// then we set the last_ping to now()
// if the request times out without a response, we ignore the response
// then we first check if the dead_or_alive timeout has elapsed
// if it has, we mark the peer status as dead
// if it has not, we just ignore and move on
// at the end of this infinite loop, we make the thread sleep for a period of time
// before we loop again

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
