use crate::{
    ConsensusMessage, LogWriterMsg,
    cluster::{Cluster, CurrentNode, Peer, config::init_peers_table},
};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{Mutex, RwLock, mpsc, watch},
    time::timeout,
};

pub async fn start_election(
    current_node: Arc<RwLock<CurrentNode>>,
    _quorom: u8,
    _p_table: Arc<Vec<Arc<Mutex<Peer>>>>,
    mut csus_rx: watch::Receiver<ConsensusMessage>,
    lw: mpsc::Sender<LogWriterMsg>, // log-writer
) {
    use crate::utils::cluster::get_random_election_timeout;

    loop {
        if let Err(_) = timeout(get_random_election_timeout(), async {
            if csus_rx.changed().await.is_ok() {
                if let ConsensusMessage::LeaderAssert | ConsensusMessage::RequestVote =
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
                cw.promote();
                lw.send(LogWriterMsg::NodeMeta(cw.term, cw.voted_for.clone()))
                    .await
                    .unwrap();
            };
            // we start sending out request for votes
            // collate vote responses and if it doesn't meet the quorom
            // we cancel and retry from the top
            // if it meets the quorom, we promote current node to leader
            // and spawn a task for leader heartbeats so that it begins sending them out
            // to assert dominance
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
