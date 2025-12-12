use crate::cluster::{CurrentNode, Peer};
use std::sync::Arc;
use tokio::{runtime::Handle, sync::Mutex};

pub fn _start_election(
    current_node: Arc<Mutex<CurrentNode>>,
    quorom: u8,
    p_table: Arc<Vec<Arc<Mutex<Peer>>>>,
    rt: Handle,
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
