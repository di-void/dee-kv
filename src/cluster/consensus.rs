use crate::{
    ChannelMessage,
    cluster::{
        Cluster,
        CurrentNode,
        Peer,
        config::init_peers_table,
        // hearbeats::health::start_heartbeat_loop,
    },
};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{Mutex, mpsc, watch},
};

pub fn _start_election(
    _current_node: Arc<Mutex<CurrentNode>>,
    _quorom: u8,
    _p_table: Arc<Vec<Arc<Mutex<Peer>>>>,
    _rt: Handle,
) {
    // start randomized timeout
    // once timeout expires
    // if we receive a leader heartbeat before the timeout expires
    // we reset the timeout
    // if we don't receive a leader heartbeat before it expires
    // we transition the current node to candidate and start sending out request for votes
    // collate vote responses and if it doesn't meet the quorom
    // we cancel and retry from the top
    // if it meets the quorom, we promote current node to leader
    // and spawn a task for leader heartbeats so that it begins sending them out
    // to assert dominance
}

pub async fn begin(
    cc: &Cluster,
    _lw_tx: mpsc::Sender<ChannelMessage>,
    _sd_tx: watch::Sender<Option<()>>,
    _rt: Handle,
) -> Result<()> {
    let _current_node = CurrentNode {
        id: cc.self_id,
        role: super::NodeRole::Follower,
        term: 1,
        votes: 0,
        has_voted: false,
    };

    let p_table = init_peers_table(&cc.peers).await?;
    let _p_table = Arc::new(p_table);
    // let hb_handle = start_heartbeat_loop(Arc::clone(&p_table), rt.clone(), shutdown.clone()).await;

    // if hb_handle.is_some() {
    //     hb_handle.unwrap().join().unwrap();
    // }

    Ok(())
}
