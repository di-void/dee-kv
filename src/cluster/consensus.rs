use crate::{
    ConsensusMessage, LogWriterMsg,
    cluster::{Cluster, CurrentNode, PeersTable, config::init_peers_table},
    consensus_proto::{RequestVoteRequest, consensus_service_client::ConsensusServiceClient},
    services::create_custom_clients,
};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::{Arc, atomic::Ordering};
use tokio::{
    runtime::Handle,
    sync::{RwLock, mpsc, watch},
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
    quorom: u8,
    p_table: Arc<PeersTable>,
    mut csus_rx: watch::Receiver<ConsensusMessage>,
    lw: mpsc::Sender<LogWriterMsg>, // log-writer
) {
    use crate::utils::cluster::get_random_election_timeout;

    loop {
        if let Err(_) = timeout(get_random_election_timeout(), async {
            if csus_rx.changed().await.is_ok() {
                // reset timer conditions
                if let ConsensusMessage::LeaderAssert | ConsensusMessage::VoteGranted =
                    *csus_rx.borrow_and_update()
                {
                    return;
                };
            };
        })
        .await
        {
            // transition to candidate
            let mut cw = current_node.write().await;
            if cw.is_follower() {
                cw.promote(); // +1 vote
                lw.send(LogWriterMsg::NodeMeta(cw.term, cw.voted_for.clone()))
                    .await
                    .unwrap();
            };

            let clients = create_custom_clients::<ConsensusServiceClient<Channel>>(&p_table);

            use crate::log::{LAST_LOG_INDEX, LAST_LOG_TERM};
            let last_log_index = LAST_LOG_INDEX.load(Ordering::SeqCst);
            let last_log_term = LAST_LOG_TERM.load(Ordering::SeqCst);
            let curr_term = cw.term;
            let candidate_id = cw.id;

            let mut futs = FuturesUnordered::new();

            for (cs, _) in clients {
                let mut client = cs.lock_owned().await;

                let handle = tokio::spawn(async move {
                    let req = Request::new(RequestVoteRequest {
                        term: curr_term.into(),
                        candidate_id: candidate_id.into(),
                        last_log_index,
                        last_log_term,
                    });

                    let res = client
                        .request_vote(req)
                        .await
                        .with_context(|| format!("[FAILED REQUEST]: `request_vote`")) // TODO: include peer node id
                        .unwrap();
                    let vote_response = res.into_inner();
                    if vote_response.vote_granted {
                        true
                    } else {
                        panic!("VOTE DENIED!"); // TODO: include peer node id
                    }
                });

                futs.push(handle);
            }

            while let Some(r) = futs.next().await {
                if r.is_ok() {
                    cw.votes += 1;
                }
            }

            if cw.votes < quorom {
                break;
            } else {
                cw.promote(); // promote to Leader
                // start sending hearbeats for dominance
            }
        };
    }
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
    let (lw_tx, _sd_tx, csus_rx) = tx_rx;
    println!("Current Node: {:?}", &current_node);

    let p_table = init_peers_table(&cc.peers).await?;
    let p_table = Arc::new(p_table);

    let quorom = cc.quorom;
    tokio::spawn(async move {
        start_election(
            Arc::clone(&current_node),
            quorom,
            Arc::clone(&p_table),
            csus_rx.clone(),
            lw_tx.clone(),
        )
        .await;
    });

    Ok(())
}