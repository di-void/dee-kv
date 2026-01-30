use crate::{
    ConsensusMessage, LogWriterMsg, Op,
    cluster::CurrentNode,
    consensus_proto::{
        AppendEntriesRequest, AppendEntriesResponse, Command, LeaderAssertRequest,
        LeaderAssertResponse, RequestVoteRequest, RequestVoteResponse,
        consensus_service_client::ConsensusServiceClient,
        consensus_service_server::ConsensusService as ConsensusSvc,
    },
    services::GrpcClientWrapper,
};

use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, watch};
use tonic::{Request, Response, Status, transport::Channel};

#[derive(Clone)]
pub struct ConsensusService {
    current_node: Arc<RwLock<CurrentNode>>,
    lw_tx: mpsc::Sender<LogWriterMsg>,
    csus_tx: watch::Sender<ConsensusMessage>,
}

impl ConsensusService {
    pub fn with_state(
        current_node: Arc<RwLock<CurrentNode>>,
        lw_tx: mpsc::Sender<LogWriterMsg>,
        csus_tx: watch::Sender<ConsensusMessage>,
    ) -> Self {
        Self {
            current_node,
            lw_tx,
            csus_tx,
        }
    }
}

#[tonic::async_trait]
impl ConsensusSvc for ConsensusService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let candidate_term = req.term as crate::Term;
        let candidate_id = req.candidate_id as u8;
        let candidate_last_term = req.last_log_term;
        let candidate_last_index = req.last_log_index;

        tracing::debug!(
            candidate_id = candidate_id,
            candidate_term = candidate_term,
            "Received vote request"
        );

        {
            let node = self.current_node.read().await;
            if candidate_term < node.term {
                return Ok(Response::new(RequestVoteResponse {
                    term: node.term.into(),
                    vote_granted: false,
                }));
            }
            if candidate_term == node.term {
                if let Some(v) = node.voted_for {
                    if v != candidate_id {
                        return Ok(Response::new(RequestVoteResponse {
                            term: node.term.into(),
                            vote_granted: false,
                        }));
                    }
                    // already voted this candidate: continue to log check
                }
            }
        }

        // log up-to-date check (compare term then index)
        let local_last_term = crate::log::get_last_log_term() as u32;
        let local_last_index = crate::log::get_last_log_index();
        let _up_to_date = if candidate_last_term > local_last_term {
            true
        } else if candidate_last_term < local_last_term {
            false
        } else {
            candidate_last_index >= local_last_index
        };

        let mut vote_granted = false;
        {
            let mut node = self.current_node.write().await;
            if candidate_term > node.term {
                node.step_down(candidate_term);
            }
            if candidate_term >= node.term {
                if node.voted_for.is_none() || node.voted_for == Some(candidate_id) {
                    // if up_to_date {
                    // }
                    node.voted_for = Some(candidate_id);
                    vote_granted = true;
                }
            }
        }

        if vote_granted {
            tracing::info!(
                candidate_id = candidate_id,
                candidate_term = candidate_term,
                "Vote granted to candidate"
            );
            // persist node meta
            let node = self.current_node.read().await;
            let persist_term = node.term;
            let persist_voted_for = node.voted_for.clone();
            drop(node);

            let _ = self
                .lw_tx
                .send(LogWriterMsg::NodeMeta(persist_term, persist_voted_for))
                .await;

            // reset election timer
            let _ = self.csus_tx.send(ConsensusMessage::ResetTimer);
        }

        let cur_term = { self.current_node.read().await.term };

        if !vote_granted {
            tracing::debug!(
                candidate_id = candidate_id,
                candidate_term = candidate_term,
                "Vote denied to candidate"
            );
        }

        Ok(Response::new(RequestVoteResponse {
            term: cur_term.into(),
            vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let leader_term = req.term as crate::Term;
        let leader_id = req.leader_id as u8;
        let prev_log_idx = req.prev_log_idx;
        let prev_log_term = req.prev_log_term;
        let entries = req.entires;

        tracing::debug!(
            leader_id = leader_id,
            leader_term = leader_term,
            prev_log_idx = prev_log_idx,
            prev_log_term = prev_log_term,
            entry_count = entries.len(),
            "Received append entries"
        );

        let _ = self.csus_tx.send(ConsensusMessage::ResetTimer);

        let mut need_persist = false;
        let persist_term: crate::Term;
        let persist_voted_for: Option<u8>;

        {
            let mut node = self.current_node.write().await;
            if node.term > leader_term {
                let cur = node.term;
                return Ok(Response::new(AppendEntriesResponse {
                    term: cur.into(),
                    success: false,
                    conflict_term: None,
                    conflict_index: 0,
                }));
            }

            if leader_term > node.term || !node.is_follower() {
                node.step_down(leader_term);
                need_persist = true;
            }

            persist_term = node.term;
            persist_voted_for = node.voted_for.clone();
        }

        if need_persist {
            let _ = self
                .lw_tx
                .send(LogWriterMsg::NodeMeta(persist_term, persist_voted_for))
                .await;
        }

        let local_last_index = crate::log::get_last_log_index();
        if prev_log_idx > local_last_index {
            return Ok(Response::new(AppendEntriesResponse {
                term: persist_term.into(),
                success: false,
                conflict_term: None,
                conflict_index: local_last_index.saturating_add(1),
            }));
        }

        if prev_log_idx > 0 {
            match crate::log::get_entry_term(prev_log_idx) {
                Some(local_term) if (local_term as u32) != prev_log_term => {
                    let conflict_index =
                        crate::log::find_first_index_of_term(local_term).unwrap_or(prev_log_idx);
                    return Ok(Response::new(AppendEntriesResponse {
                        term: persist_term.into(),
                        success: false,
                        conflict_term: Some(local_term as u32),
                        conflict_index,
                    }));
                }
                None => {
                    return Ok(Response::new(AppendEntriesResponse {
                        term: persist_term.into(),
                        success: false,
                        conflict_term: None,
                        conflict_index: local_last_index.saturating_add(1),
                    }));
                }
                _ => {}
            }
        }

        if entries.is_empty() {
            // heartbeat
            return Ok(Response::new(AppendEntriesResponse {
                term: persist_term.into(),
                success: true,
                conflict_term: None,
                conflict_index: 0,
            }));
        }

        let _ = self
            .lw_tx
            .send(LogWriterMsg::Truncate {
                last_index: prev_log_idx,
            })
            .await;

        for entry in entries {
            let command = entry.command();
            let payload = &entry.payload;
            let key = match payload.get("key") {
                Some(k) => k.clone(),
                None => {
                    return Err(Status::invalid_argument("append_entries entry missing key"));
                }
            };

            let op = match command {
                Command::Put => {
                    let value = match payload.get("value") {
                        Some(v) => v.clone(),
                        None => {
                            return Err(Status::invalid_argument(
                                "append_entries put missing value",
                            ));
                        }
                    };
                    Op::Put(key, value.into())
                }
                Command::Del => Op::Delete(key),
                Command::Unspecified => {
                    return Err(Status::invalid_argument(
                        "append_entries command unspecified",
                    ));
                }
            };

            let entry_term = entry.term as crate::Term;
            let entry_index = entry.idx;

            let _ = self
                .lw_tx
                .send(LogWriterMsg::AppendEntry {
                    op,
                    term: entry_term,
                    index: entry_index,
                })
                .await;
        }

        Ok(Response::new(AppendEntriesResponse {
            term: persist_term.into(),
            success: true,
            conflict_term: None,
            conflict_index: 0,
        }))
    }

    async fn leader_assert(
        &self,
        request: Request<LeaderAssertRequest>,
    ) -> Result<Response<LeaderAssertResponse>, Status> {
        let req = request.into_inner();
        let leader_term = req.term as crate::Term;

        tracing::debug!(leader_term = leader_term, "Received leader heartbeat");

        // Reset election timer immediately to avoid unnecessary elections.
        let _ = self.csus_tx.send(ConsensusMessage::ResetTimer);

        let mut need_persist = false;
        let persist_term: crate::Term;
        let persist_voted_for: Option<u8>;

        {
            let mut node = self.current_node.write().await;
            if node.term > leader_term {
                let cur = node.term;
                drop(node);
                return Ok(Response::new(LeaderAssertResponse {
                    term: cur.into(),
                    success: false,
                }));
            }

            // If leader's term is greater or equal, step down to follower.
            if leader_term >= node.term && !node.is_follower() {
                node.step_down(leader_term);
                need_persist = true;
            }

            persist_term = node.term;
            persist_voted_for = node.voted_for.clone();
        }

        if need_persist {
            let _ = self
                .lw_tx
                .send(LogWriterMsg::NodeMeta(persist_term, persist_voted_for))
                .await;
        }

        // reset timer again for good measure.
        let _ = self.csus_tx.send(ConsensusMessage::ResetTimer);

        Ok(Response::new(LeaderAssertResponse {
            term: persist_term.into(),
            success: true,
        }))
    }
}

impl GrpcClientWrapper for ConsensusServiceClient<Channel> {
    fn new_client(inner: Channel) -> Self {
        Self::new(inner)
    }
}
