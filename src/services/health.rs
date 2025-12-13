use crate::{
    health_proto::{
        PingReply, PingRequest, health_check_client::HealthCheckClient,
        health_check_server::HealthCheck,
    },
    services::GrpcClientWrapper,
};
use tonic::{Request, Response, Status, transport::Channel};

#[derive(Default)]
pub struct HealthService {}

#[tonic::async_trait]
impl HealthCheck for HealthService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        println!("Received Ping Request. Sending reply..");
        Ok(Response::new(PingReply {}))
    }
}

impl GrpcClientWrapper for HealthCheckClient<Channel> {
    fn new_client(inner: Channel) -> Self {
        Self::new(inner)
    }
}
