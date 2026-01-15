use crate::{
    cluster::{HEARTBEAT_INTERVAL_MS, PEER_FAILURE_TIMEOUT_MS, Peer, PeerStatus, PeersTable},
    health_proto::{PingRequest, health_check_service_client::HealthCheckServiceClient},
};
use anyhow::Result;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, sync::watch, time::timeout};
use tonic::Request;

/// Starts a background heartbeat loop that periodically pings peers in the provided peers table.
///
/// The loop runs on a new thread and enters the provided Tokio runtime's context to spawn per-peer
/// async ping tasks. The loop checks `shutdown_rx` each iteration and exits when it contains
/// `Some(())`. If the peers table is empty at call time, the function returns `None` and no thread
/// is spawned.
///
/// Arguments:
/// - `pt` — shared `PeersTable` used by the heartbeat loop; an `Arc` is cloned for use by the thread.
/// - `rt` — a `tokio::runtime::Handle` whose context is entered on the spawned thread to run async tasks.
/// - `shutdown_rx` — a `watch::Receiver<Option<()>>` watched each iteration; writing `Some(())` to the
///   corresponding sender signals the loop to shut down.
///
/// # Returns
///
/// `Some(JoinHandle)` for the spawned thread when the loop was started, `None` if no peers were present.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tokio::runtime::Runtime;
/// use tokio::sync::watch;
///
/// // Assume `PeersTable` and `start_heartbeat_loop` are defined in the current crate.
/// // let pt: Arc<PeersTable> = Arc::new(PeersTable::new());
/// // let rt = Runtime::new().unwrap();
/// // let (tx, rx) = watch::channel(None);
/// // let handle = start_heartbeat_loop(pt.clone(), rt.handle().clone(), rx);
/// // // ... later signal shutdown:
/// // let _ = tx.send(Some(()));
/// ```
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
                } else {
                    println!("Peer {:?} is currently locked! Skipping ping.", p);
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
    let mut client = HealthCheckServiceClient::new(peer.channel.clone());
    let _r = timeout(period, client.ping(req)).await??;
    Ok(())
}