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
use tokio::{runtime::Handle, sync::Mutex, time::sleep};
use tonic::{
    Request,
    transport::{Channel, Uri},
};

pub async fn start_heartbeat_loop(
    peers_table: Arc<Vec<Arc<Mutex<Peer>>>>,
    rt: Handle,
) -> Option<JoinHandle<()>> {
    println!("Starting heartbeat loop.");

    if peers_table.len() == 0 {
        println!("Heartbeat loop aborted. No peers found in peers_table");
        return None;
    }

    let h = thread::spawn(move || {
        let _guard = rt.enter();

        loop {
            for (i, p) in peers_table.iter().enumerate() {
                let peer = Arc::clone(p);

                tokio::spawn(async move {
                    println!("Spawned handler task for peer {i} successfully");
                    // handler
                    let mut p = peer.lock().await;
                    let p_ref = &*p;
                    let p_info = format!(
                        "{{ id: {}, status: {:?}, last_ping: {:?} }}",
                        p_ref.id, p_ref.status, p_ref.last_ping
                    );

                    println!("Task acquired lock for peer: {}", &p_info);

                    let mut ping_req = Request::new(PingRequest {});
                    ping_req.set_timeout(Duration::from_millis(500));

                    // ping the node
                    println!("Pinging peer: {}", &p_info);
                    let res = p.client.ping(ping_req).await;
                    let now = Instant::now();

                    match res {
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
                                println!("Peer: {} is no longer healthy. Marking as Dead", &p_info);
                                p.status = PeerStatus::Dead;
                            }
                        }
                    }
                });
            }

            println!("Heartbeat loop is pausing for a period");
            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS.into()));
        }
    });

    Some(h)
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

                println!("Succesfully initialized peer: {:?}", &peer);

                let peer = Arc::new(Mutex::new(peer));
                peers.push(peer);
            }
            Err(e) => {
                println!(
                    "Failed to initialize client for node: {:?}. Error: {:?}\n Retrying...",
                    n, e
                );

                if let Ok(client) = retry_create_client(&n.address).await {
                    peer = Peer {
                        client,
                        id: n.id,
                        last_ping: Instant::now(),
                        status: PeerStatus::Alive,
                    };

                    println!("Succesfully initialized peer: {:?}", &peer);

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
    println!("Retrying client init");

    for i in 1..=3 {
        println!("Pass {i}");
        if let Ok(client) = create_client(addr).await {
            return Ok(client);
        }

        println!("Failed to init client. Trying again after 2 seconds.");
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
