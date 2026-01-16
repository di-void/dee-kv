use crate::{
    ConsensusMessage, LogWriterMsg,
    cluster::{Cluster, CurrentNode, PeersTable, config::init_peers_table},
    consensus_proto::{RequestVoteRequest, consensus_service_client::ConsensusServiceClient},
    services::create_custom_clients,
};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use tokio::{
    runtime::Handle,
    sync::{RwLock, mpsc, watch},
    task,
    time::timeout,
};
use tonic::{Request, transport::Channel};

/// Runs the continuous election loop for the local node and attempts to acquire leadership.
///
/// When an election timeout elapses, a follower is promoted to candidate (votes for itself),
/// the node's metadata is persisted via the provided log-writer, and RequestVote RPCs are
/// dispatched concurrently to all peers. Incoming consensus messages observed on `csus_rx`
/// that are `LeaderAssert` or `VoteGranted` reset the election timer. If the collected votes
/// reach `quorom`, the node is promoted to leader.
///
/// # Parameters
///
/// - `current_node`: shared mutable state for the local node.
/// - `quorom`: number of votes required to become leader.
/// - `p_table`: table of peer nodes used to create RPC clients.
/// - `csus_rx`: watch receiver for consensus messages that can reset the election timer.
/// - `lw`: sender to persist node metadata to the log writer.
///
/// # Examples
///
/// ```
/// // Spawn the election loop (arguments omitted for brevity).
/// // tokio::spawn(start_election(current_node, quorom, p_table, csus_rx, lw));
/// ```
pub async fn start_election(
    current_node: Arc<RwLock<CurrentNode>>,
    quorum: u8,
    p_table: Arc<PeersTable>,
    mut csus_rx: watch::Receiver<ConsensusMessage>,
    mut shutdown_rx: watch::Receiver<Option<()>>,
    lw: mpsc::Sender<LogWriterMsg>, // log-writer
) -> Result<()> {
    use crate::utils::cluster::get_random_election_timeout;

    'outer: loop {
        use tokio::time::{Instant, sleep};
        tokio::pin! { let sleep_fut = sleep(get_random_election_timeout()); }

        loop {
            tokio::select! {
                _ = &mut sleep_fut => {
                    break; // start election
                }
                res = csus_rx.changed() => {
                    if res.is_ok() {
                        match *csus_rx.borrow_and_update() {
                            ConsensusMessage::LeaderAssert | ConsensusMessage::VoteGranted => {
                                let next_deadline = Instant::now() + get_random_election_timeout();
                                sleep_fut.as_mut().reset(next_deadline); // reset to a fresh deadline
                            }
                            _ => {}
                        }
                    } else {
                        dbg!("Consensus Message Channel closed prematurely!");
                        break 'outer; // abort election loop
                    }
                }
                _ = shutdown_rx.changed() => {
                    break 'outer; // graceful shutdown requested
                }
            }
        }

        let mut cw = current_node.write().await;
        if cw.is_follower() {
            // transition to candidate
            cw.promote(); // +1 vote
            lw.send(LogWriterMsg::NodeMeta(cw.term, cw.voted_for.clone()))
                .await
                .unwrap();
        };
        let (curr_term, candidate_id) = (cw.term, cw.id);
        drop(cw);

        let pt = Arc::clone(&p_table);
        let clients = task::spawn_blocking(move || {
            let p_table = pt;
            create_custom_clients::<ConsensusServiceClient<Channel>>(&p_table)
        })
        .await
        .with_context(|| format!("Failed to generate custom clients"))?;

        let last_log_index = crate::log::LAST_LOG_INDEX.load(Ordering::SeqCst);
        let last_log_term = crate::log::LAST_LOG_TERM.load(Ordering::SeqCst);

        let mut futs = FuturesUnordered::new();

        for (mut client, _) in clients {
            futs.push(tokio::spawn(async move {
                let req = Request::new(RequestVoteRequest {
                    term: curr_term.into(),
                    candidate_id: candidate_id.into(),
                    last_log_index,
                    last_log_term,
                });

                match timeout(Duration::from_secs(2), client.request_vote(req)).await {
                    Ok(Ok(res)) => {
                        let vote_response = res.into_inner();
                        if vote_response.vote_granted {
                            Ok(())
                        } else {
                            Err(Some(vote_response.term))
                        }
                    }
                    // rpc returned error
                    Ok(Err(_status)) => {
                        // treat as non-fatal failure (None)
                        Err(None)
                    }
                    // timeout elapsed
                    Err(_) => Err(None),
                }
            }));
        }

        while let Some(res) = futs.next().await {
            if let Ok(r) = res {
                match r {
                    Ok(_) => {
                        let mut cw = current_node.write().await;
                        cw.votes += 1;
                        if cw.votes >= quorum {
                            cw.promote();
                            lw.send(LogWriterMsg::NodeMeta(cw.term, cw.voted_for.clone()))
                                .await
                                .unwrap();
                            break 'outer;
                        }
                    }
                    Err(Some(peer_term)) => {
                        let mut cw = current_node.write().await;
                        if (peer_term as u16) > cw.term {
                            cw.step_down(peer_term as u16);
                            lw.send(LogWriterMsg::NodeMeta(cw.term, cw.voted_for.clone()))
                                .await
                                .unwrap();
                            break;
                        }
                    }
                    Err(None) => {
                        // RPC failure or timeout from a peer; ignore for now.
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn begin(
    cc: &Cluster,
    current_node: Arc<RwLock<CurrentNode>>,
    tx_rx: (
        mpsc::Sender<LogWriterMsg>,  // log-writer
        watch::Receiver<Option<()>>, // shutdown
        watch::Receiver<ConsensusMessage>,
    ),
    _rt: Handle,
) -> Result<()> {
    let (lw_tx, sd_rx, csus_rx) = tx_rx;
    println!("Current Node: {:?}", &current_node);

    let p_table = init_peers_table(&cc.peers).await?;
    let p_table = Arc::new(p_table);

    let quorom = cc.quorom;
    tokio::spawn(async move {
        loop {
            let _ = start_election(
                Arc::clone(&current_node),
                quorom,
                Arc::clone(&p_table),
                csus_rx.clone(),
                sd_rx.clone(),
                lw_tx.clone(),
            )
            .await;

            if sd_rx.borrow().is_some() {
                break;
            }

            // start sending out leader heartbeats
            // while listening for step down notifications
            // if we get a step down notification
            // break out of the heartbeat loop
            // then restart the election process
        }
    });

    Ok(())
}

// https://docs.rs/futures/latest/futures/index.html
