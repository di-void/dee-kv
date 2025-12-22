use crate::{
    health_proto::{
        PingRequest, PingResponse, health_check_service_client::HealthCheckServiceClient,
        health_check_service_server::HealthCheckService as HealthCheckSvc,
    },
    // health_proto::{
    //     PingReply, PingRequest, health_check_client::HealthCheckClient,
    //     health_check_server::HealthCheck,
    // },
    services::GrpcClientWrapper,
};
use tonic::{Request, Response, Status, transport::Channel};

#[derive(Default)]
pub struct HealthCheckService {}

#[tonic::async_trait]
impl HealthCheckSvc for HealthCheckService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        println!("Received Ping Request. Sending reply..");
        Ok(Response::new(PingResponse {}))
    }
}

impl GrpcClientWrapper for HealthCheckServiceClient<Channel> {
    fn new_client(inner: Channel) -> Self {
        Self::new(inner)
    }
}
