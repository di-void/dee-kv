use super::{Node, Peer, PeerStatus};
use crate::{
    cluster::{HEARTBEAT_INTERVAL_MS, PEER_FAILURE_TIMEOUT_MS},
    health_proto::{PingRequest, health_check_client::HealthCheckClient},
};
use anyhow::{Error, Result};
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex, watch},
    time::{sleep, timeout},
};
use tonic::{
    Request,
    transport::{Channel, Uri},
};

pub async fn start_heartbeat_loop(
    peers_table: Arc<Vec<Arc<Mutex<Peer>>>>,
    rt: Handle,
    shutdown_rx: watch::Receiver<Option<()>>,
) -> Option<JoinHandle<()>> {
    println!("Starting heartbeat loop...");

    if peers_table.len() == 0 {
        println!("Heartbeat loop aborted. No peers found in peers_table");
        return None;
    }

    let h = thread::spawn(move || {
        let _guard = rt.enter();

        loop {
            let sd = shutdown_rx.borrow();
            if sd.is_some() {
                break;
            }

            for p in peers_table.iter() {
                let peer = Arc::clone(p);

                // only spawn with lock
                if let Ok(guard) = peer.try_lock_owned() {
                    tokio::spawn(async move {
                        // handler
                        let mut p = guard;
                        let p_ref = &*p;
                        let p_info = format!(
                            "{{ id: {}, status: {:?}, last_ping: {:?} }}",
                            p_ref.id, p_ref.status, p_ref.last_ping
                        );

                        // ping the node
                        println!("Pinging peer: {}", &p_info);
                        let now = Instant::now();

                        match timed_ping_request(&mut *p, Duration::from_millis(500)).await {
                            Ok(_) => {
                                println!("Received OK response. Peer: {}, is healthy", &p_info);
                                p.last_ping = now;
                                p.status = PeerStatus::Alive;
                            }
                            Err(e) => {
                                println!(
                                    "Received Err response. Peer: {}, returned error: {e}",
                                    &p_info
                                );
                                if now.duration_since(p.last_ping)
                                    > Duration::from_millis(PEER_FAILURE_TIMEOUT_MS.into())
                                {
                                    println!(
                                        "Peer: {} is no longer healthy. Marking as Dead",
                                        &p_info
                                    );
                                    p.status = PeerStatus::Dead;
                                }
                            }
                        };
                    });
                };
            }

            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS.into()));
        }

        println!("Shutting down hearbeat loop..");
    });

    Some(h)
}

async fn timed_ping_request(peer: &mut Peer, period: Duration) -> Result<()> {
    let req = Request::new(PingRequest {});
    let _r = timeout(period, peer.client.ping(req)).await??;
    Ok(())
}

pub async fn init_peers(p_nodes: &Vec<Node>) -> Result<Vec<Arc<Mutex<Peer>>>> {
    let mut peers = vec![];

    for n in p_nodes.iter() {
        let peer: Peer;

        match create_client(&n.address).await {
            Ok(client) => {
                peer = Peer {
                    client,
                    id: n.id,
                    last_ping: Instant::now(),
                    status: PeerStatus::Alive,
                };

                println!(
                    "Succesfully initialized peer: {{ id: {}, last_ping: {:?}, status: {:?} }}",
                    peer.id, &peer.last_ping, &peer.status
                );

                let peer = Arc::new(Mutex::new(peer));
                peers.push(peer);
            }
            Err(e) => {
                println!(
                    "Failed to initialize client with id: {}. Error: {:?}\n Retrying...",
                    n.id, e
                );

                if let Ok(client) = retry_create_client(&n.address).await {
                    peer = Peer {
                        client,
                        id: n.id,
                        last_ping: Instant::now(),
                        status: PeerStatus::Alive,
                    };

                    println!(
                        "Succesfully initialized peer: {{ id: {}, last_ping: {:?}, status: {:?} }}",
                        peer.id, &peer.last_ping, &peer.status
                    );

                    let peer = Arc::new(Mutex::new(peer));
                    peers.push(peer);
                } else {
                    println!("Failed to initialize client after 4 attempts. Skipping init step")
                }
            }
        };
    }

    Ok(peers)
}

async fn retry_create_client(addr: &str) -> Result<HealthCheckClient<Channel>> {
    for i in 1..=3 {
        println!("Retry attempt: {i}");
        if let Ok(client) = create_client(addr).await {
            return Ok(client);
        }

        println!("Failed to init client. Trying again in 2 secs.");
        sleep(Duration::from_millis(2000)).await
    }

    Err(Error::msg("Failed to init client with address: {addr}"))
}

async fn create_client(addr: &str) -> Result<HealthCheckClient<Channel>> {
    let mut parts = http::uri::Parts::default();
    parts.scheme = Some("http".parse().unwrap());
    parts.authority = Some(addr.parse().unwrap());
    parts.path_and_query = Some("/".parse().unwrap());

    let uri = Uri::from_parts(parts).unwrap();
    let c = HealthCheckClient::connect(uri).await?;
    Ok(c)
}
