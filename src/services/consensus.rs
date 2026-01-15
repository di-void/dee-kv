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
        todo!("request vote")
    }

    async fn append_entries(
        &self,
        _request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        todo!("append entries");
    }

    /// Handles an incoming leader assertion RPC.
    ///
    /// Processes a `LeaderAssertRequest` and returns a gRPC `Response` containing a `LeaderAssertResponse` that indicates whether the sender's leadership assertion is accepted. On failure, returns a gRPC `Status`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tonic::Request;
    /// use consensus_proto::LeaderAssertRequest;
    ///
    /// // Build service and request (fields omitted for brevity)
    /// let svc = ConsensusService::default();
    /// let req = Request::new(LeaderAssertRequest { /* ... */ });
    ///
    /// // Invoke handler (requires an async runtime)
    /// let _ = tokio::runtime::Runtime::new().unwrap().block_on(async {
    ///     let _ = svc.leader_assert(req).await;
    /// });
    /// ```
    async fn leader_assert(
        &self,
        _request: Request<LeaderAssertRequest>,
    ) -> Result<Response<LeaderAssertResponse>, Status> {
        todo!("leader assert");
    }
}

impl GrpcClientWrapper for ConsensusServiceClient<Channel> {
    /// Creates a ConsensusServiceClient that uses the provided gRPC Channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use tonic::transport::Channel;
    /// use consensus_proto::consensus_service_client::ConsensusServiceClient;
    ///
    /// // construct a Channel (in real code this is usually awaited)
    /// let channel = Channel::from_static("http://127.0.0.1:50051");
    /// let _client = ConsensusServiceClient::new_client(channel);
    /// ```
    fn new_client(inner: Channel) -> Self {
        Self::new(inner)
    }
}