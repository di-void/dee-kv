use crate::{cluster::CurrentNode, store::Store};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, watch};

#[derive(Debug, Clone, Copy)]
pub enum ApplyMsg {
    Apply,
}

pub async fn run_apply_worker(
    current_node: Arc<RwLock<CurrentNode>>,
    store: Arc<RwLock<Store>>,
    mut rx: mpsc::Receiver<ApplyMsg>,
    mut shutdown_rx: watch::Receiver<Option<()>>,
) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                if msg.is_none() {
                    break;
                }
                while rx.try_recv().is_ok() {} // drain spurious messages
                apply_committed_entries(&current_node, &store, "Worker").await;
            }
            _ = shutdown_rx.changed() => {
                if shutdown_rx.borrow().is_some() {
                    break;
                }
            }
        }
    }
}

pub async fn apply_committed_entries(
    current_node: &Arc<RwLock<CurrentNode>>,
    store: &Arc<RwLock<Store>>,
    role: &'static str,
) {
    const MAX_APPLY_ENTRIES: usize = 128;

    let (commit_index, last_applied) = {
        let node = current_node.read().await;
        (node.commit_index, node.last_applied_idx)
    };

    if commit_index <= last_applied {
        return;
    }

    let mut next_index = last_applied.saturating_add(1);
    let mut new_last_applied = last_applied;

    while next_index <= commit_index {
        let entries = crate::log::get_entries_from(next_index, MAX_APPLY_ENTRIES);
        if entries.is_empty() {
            break;
        }

        let mut store = store.write().await;
        let mut gap = false;
        for entry in entries {
            if entry.index != next_index {
                gap = true;
                break;
            }
            store.apply_log(&entry);
            new_last_applied = entry.index;
            next_index = entry.index.saturating_add(1);
            if next_index > commit_index {
                break;
            }
        }
        drop(store);

        if gap {
            break;
        }
    }

    if new_last_applied > last_applied {
        let mut node = current_node.write().await;
        if new_last_applied > node.last_applied_idx {
            tracing::debug!(
                prev_last_applied = node.last_applied_idx,
                new_last_applied = new_last_applied,
                role = role,
                "Applied committed entries"
            );
            node.last_applied_idx = new_last_applied;
        }
    }
}
