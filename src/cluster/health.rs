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
use tokio::{sync::Mutex, time::sleep};
use tonic::{Code, Request, transport::Channel};

pub async fn start_heartbeat_loop(peers_table: Arc<Vec<Arc<Mutex<Peer>>>>) -> JoinHandle<()> {
    println!("Starting heartbeat loop. Spawning thread..");

    thread::spawn(move || {
        loop {
            println!("Heartbeat loop has started..");
            for (i, p) in peers_table.iter().enumerate() {
                let peer = Arc::clone(p);

                println!("Spawning handler task for peer {i}");

                tokio::spawn(async move {
                    println!("Spawned handler task for peer {i} successfully");
                    // handler
                    let mut p = peer.lock().await;
                    println!("Task acquired lock for peer {i}: {:?}", &*p);

                    let mut ping_req = Request::new(PingRequest {});
                    ping_req.set_timeout(Duration::from_millis(500));

                    // ping the node
                    println!("Pinging peer node {i}: {:?}", &*p);
                    let res = p.client.ping(ping_req).await;
                    let now = Instant::now();

                    match res {
                        Ok(_) => {
                            println!("Received OK response. Peer node {i}: {:?}, is healthy", &*p);
                            p.last_ping = now;
                            p.status = PeerStatus::Alive;
                        }
                        Err(e) => {
                            println!(
                                "Received Err response. Peer node {i}: {:?}, returned an error",
                                &*p
                            );
                            if let Code::DeadlineExceeded = e.code() {
                                println!(
                                    "Request timed out. Peer node {i}: {:?}, didn't respond in time",
                                    &*p
                                );
                                if now.duration_since(p.last_ping)
                                    > Duration::from_millis(PEER_FAILURE_TIMEOUT_MS.into())
                                {
                                    println!(
                                        "Peer node {i}: {:?} is no longer healthy. Marking as Dead",
                                        &*p
                                    );
                                    p.status = PeerStatus::Dead;
                                }
                            }
                        }
                    }
                });
            }

            println!("Heartbeat loop is pausing for a period");
            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS.into()));
        }
    })
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
            Err(_) => {
                println!(
                    "Failed to initialize client for node: {:?}.\n Retrying...",
                    n
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
        sleep(Duration::from_millis(1700)).await
    }

    Err(Error::msg("Failed to init client with address: {addr}"))
}

async fn create_client(addr: &str) -> Result<HealthCheckClient<Channel>> {
    let c = HealthCheckClient::connect(addr.to_owned()).await?;
    Ok(c)
}
