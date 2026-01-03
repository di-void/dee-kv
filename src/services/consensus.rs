use crate::consensus_proto::{
    AppendEntriesRequest, AppendEntriesResponse, LeaderAssertRequest, LeaderAssertResponse,
    RequestVoteRequest, RequestVoteResponse,
    consensus_service_server::ConsensusService as ConsensusSvc,
};

use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct ConsensusService {}

#[tonic::async_trait]
impl ConsensusSvc for ConsensusService {
    async fn request_vote(
        &self,
        _request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        todo!("request vote")
    }

    async fn append_entries(
        &self,
        _request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        todo!("append entries");
    }

    async fn leader_assert(
        &self,
        _request: Request<LeaderAssertRequest>,
    ) -> Result<Response<LeaderAssertResponse>, Status> {
        todo!("leader assert");
    }
}
