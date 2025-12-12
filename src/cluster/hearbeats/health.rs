use crate::{
    cluster::{HEARTBEAT_INTERVAL_MS, PEER_FAILURE_TIMEOUT_MS, Peer, PeerStatus, PeersTable},
    health_proto::{PingRequest, health_check_client::HealthCheckClient},
    services::health::init_health_clients,
};
use anyhow::Result;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, sync::watch, time::timeout};
use tonic::Request;

pub async fn start_heartbeat_loop(
    pt: Arc<PeersTable>,
    rt: Handle,
    shutdown_rx: watch::Receiver<Option<()>>,
) -> Option<JoinHandle<()>> {
    println!("Starting heartbeat loop...");

    if pt.len() == 0 {
        println!("Heartbeat loop aborted. No peers found in peers_table");
        return None;
    }

    let _clients = init_health_clients(Arc::clone(&pt));

    let h = thread::spawn(move || {
        let _guard = rt.enter();

        loop {
            let sd = shutdown_rx.borrow();
            if sd.is_some() {
                break;
            }

            for p in pt.iter() {
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
    let mut client = HealthCheckClient::new(peer.client.clone());
    let _r = timeout(period, client.ping(req)).await??;
    Ok(())
}
