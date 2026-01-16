use crate::{
    consensus_proto::{
        AppendEntriesRequest, AppendEntriesResponse, LeaderAssertRequest, LeaderAssertResponse,
        RequestVoteRequest, RequestVoteResponse, consensus_service_client::ConsensusServiceClient,
        consensus_service_server::ConsensusService as ConsensusSvc,
    },
    services::GrpcClientWrapper,
};

use tonic::{Request, Response, Status, transport::Channel};

#[derive(Default)]
pub struct ConsensusService {}

#[tonic::async_trait]
impl ConsensusSvc for ConsensusService {
    async fn request_vote(
        &self,
        _request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        // check the candidate's term against current node
        // if the term is greater than the current node's term
        // step down the current node then
        // grant vote to candidate node
        // if the terms are equal, check if the current node has voted
        // if it has voted and it wasn't this candidate, reject the request
        // if it has voted and it *was* this candidate, grant vote again
        // if the candidate's term is lower than current node's
        // reject the request while setting the term in response to the current node's
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
        // reset the election timer
        // check the term if it is greater than current node's
        // if yes, step down this node and
        // and return the "term echo response"
        // if the terms are equal and current node is already a follower
        // reset the election timer and return "term echo response"
        // if the current node is not a follower and the terms are equal
        // step the current node down, reset the election timer and return the response
        // if the leader term is less than current node's term
        // return a response with the term included
        todo!("leader assert");
    }
}

impl GrpcClientWrapper for ConsensusServiceClient<Channel> {
    fn new_client(inner: Channel) -> Self {
        Self::new(inner)
    }
}
